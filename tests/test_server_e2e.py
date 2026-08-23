from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request

from rapp_virtual_as400.server import RAPPServer
from rapp_virtual_as400.storage import MAX_PERSISTED_STATE_BYTES, empty_state

from .support import EngineTestCase


class ServerE2ETests(EngineTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.server = RAPPServer(("127.0.0.1", 0), self.work / "http-state.json", self.work / "stop.capability")
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        if self.thread.is_alive():
            self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        super().tearDown()

    def request(self, path: str, payload: dict | None = None, token: str | None = None) -> tuple[int, dict]:
        data = None if payload is None else json.dumps(payload).encode()
        headers = {} if payload is None else {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        request = urllib.request.Request(self.base + path, data=data, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=2) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as error:
            try:
                return error.code, json.loads(error.read())
            finally:
                error.close()

    def test_health_is_typed(self) -> None:
        status, body = self.request("/health")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ok")
        self.assertEqual(body["protocol"], "RAPP/1")
        self.assertIsInstance(body["version"], str)

    def test_exact_chat_success_shape(self) -> None:
        status, body = self.request(
            "/chat",
            {"user_input": "CRTLIB LIB(WEB)", "session_id": "web", "idempotency_key": "one"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(set(body), {"response", "agent_logs", "session_id"})
        self.assertEqual(body["session_id"], "web")

    def test_live_chat_colon_identities_repeat_and_conflict_independently(self) -> None:
        left = {
            "user_input": "CRTLIB LIB(LEFT)",
            "session_id": "a:b",
            "idempotency_key": "c",
        }
        right = {
            "user_input": "CRTLIB LIB(RIGHT)",
            "session_id": "a",
            "idempotency_key": "b:c",
        }
        self.assertEqual(self.request("/chat", left)[0], 200)
        self.assertEqual(self.request("/chat", right)[0], 200)
        self.assertEqual(self.request("/chat", left)[1]["session_id"], "a:b")
        self.assertEqual(self.request("/chat", right)[1]["session_id"], "a")
        for payload in (left, right):
            conflict = {**payload, "user_input": "DSPLIB"}
            status, body = self.request("/chat", conflict)
            self.assertEqual(status, 422)
            self.assertEqual(body["error"]["code"], "IDEMPOTENCY_CONFLICT")
            self.assertEqual(body["session_id"], payload["session_id"])

    def test_exact_422_refusal_envelope(self) -> None:
        status, body = self.request("/chat", {})
        self.assertEqual(status, 422)
        self.assertEqual(set(body), {"error", "agent_logs", "session_id"})
        self.assertEqual(body["error"]["type"], "refusal")
        self.assertEqual(body["error"]["code"], "INVALID_REQUEST")
        self.assertEqual(body["agent_logs"], [])

    def test_decimal_precision_28_29_and_38_through_live_chat(self) -> None:
        values = {
            28: "9" * 28,
            29: "9" * 29,
            38: "9" * 38,
        }
        commands = ["CRTLIB LIB(PRECISION)"]
        for precision, value in values.items():
            commands.extend(
                [
                    f"CRTPF FILE(PRECISION/P{precision}) FIELDS(V:DECIMAL({precision},0))",
                    f"INSERT FILE(PRECISION/P{precision}) VALUES(V='{value}')",
                    f"SELECT FILE(PRECISION/P{precision}) WHERE(V='{value}')",
                ]
            )
        status, body = self.request("/chat", {"user_input": "; ".join(commands), "session_id": "decimal"})
        self.assertEqual(status, 200)
        for value in values.values():
            self.assertIn(f'"V":"{value}"', body["response"])

    def test_decimal_error_is_stable_live_refusal(self) -> None:
        status, _ = self.request(
            "/chat",
            {
                "user_input": (
                    "CRTLIB LIB(PREC); CRTPF FILE(PREC/F) FIELDS(V:DECIMAL(38,0)); "
                    f"INSERT FILE(PREC/F) VALUES(V='{'9' * 39}')"
                ),
                "session_id": "decimal-error",
            },
        )
        self.assertEqual(status, 422)
        status, body = self.request(
            "/chat",
            {
                "user_input": (
                    "CRTLIB LIB(PREC); CRTPF FILE(PREC/F) FIELDS(V:DECIMAL(38,0)); "
                    f"INSERT FILE(PREC/F) VALUES(V='{'9' * 39}')"
                ),
                "session_id": "decimal-error",
            },
        )
        self.assertEqual(status, 422)
        self.assertEqual(body["error"]["code"], "INVALID_RECORD")
        self.assertEqual(body["error"]["message"], "V exceeds declared precision.")
        self.assertEqual(set(body), {"error", "agent_logs", "session_id"})

    def test_unpaired_surrogate_is_exact_raw_http_422_refusal(self) -> None:
        request = urllib.request.Request(
            self.base + "/chat",
            data=b'{"user_input":"DSPLIB \\ud800","session_id":"unicode"}',
            headers={"Content-Type": "application/json"},
        )
        with self.assertRaises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(request, timeout=2)
        error = caught.exception
        try:
            body = json.loads(error.read())
        finally:
            error.close()
        self.assertEqual(error.code, 422)
        self.assertEqual(
            body,
            {
                "error": {
                    "type": "refusal",
                    "code": "INVALID_REQUEST",
                    "message": "Request contains malformed Unicode.",
                },
                "agent_logs": [],
                "session_id": "",
            },
        )

    def test_invalid_utf8_is_exact_raw_http_422_refusal(self) -> None:
        request = urllib.request.Request(
            self.base + "/chat",
            data=b'{"user_input":"\xff"}',
            headers={"Content-Type": "application/json"},
        )
        with self.assertRaises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(request, timeout=2)
        error = caught.exception
        try:
            body = json.loads(error.read())
        finally:
            error.close()
        self.assertEqual(error.code, 422)
        self.assertEqual(body["error"]["code"], "INVALID_REQUEST")
        self.assertEqual(body["error"]["message"], "Request contains malformed Unicode.")
        self.assertEqual(set(body), {"error", "agent_logs", "session_id"})

    def test_persisted_byte_limit_refuses_select_atomically_and_restarts(self) -> None:
        fields = [
            {"name": f"F{index:02d}", "type": "CHAR", "precision": 256, "scale": 0}
            for index in range(15)
        ]
        record = {f"F{index:02d}": "x" * 256 for index in range(15)}
        insert = "INSERT FILE(BIG/ROWS) VALUES(" + ",".join(
            f"F{index:02d}='{'x' * 256}'" for index in range(15)
        ) + ")"
        state = empty_state()
        state["revision"] = 1
        state["libraries"]["BIG"] = {
            "files": {
                "ROWS": {
                    "fields": fields,
                    "records": [record.copy() for _ in range(900)],
                }
            }
        }
        state["sessions"]["bulk"] = {
            "turns": [
                {
                    "at": "2000-01-01T00:00:00+00:00",
                    "input": insert,
                    "response": "1 record inserted into BIG/ROWS.",
                }
                for _ in range(100)
            ]
        }
        self.server.engine.store.restore(state)
        state_path = self.work / "http-state.json"
        before_bytes = state_path.read_bytes()
        before_state = self.server.engine.store.snapshot()
        self.assertLess(len(before_bytes), MAX_PERSISTED_STATE_BYTES)
        self.assertGreater(len(before_bytes), MAX_PERSISTED_STATE_BYTES - 250_000)

        status, body = self.request(
            "/chat",
            {"user_input": "SELECT FILE(BIG/ROWS)", "session_id": "select"},
        )
        self.assertEqual(status, 422)
        self.assertEqual(body["error"]["code"], "LIMIT_EXCEEDED")
        self.assertEqual(body["session_id"], "select")
        self.assertEqual(state_path.read_bytes(), before_bytes)
        self.assertEqual(self.server.engine.store.snapshot(), before_state)

        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.server = RAPPServer(
            ("127.0.0.1", 0),
            state_path,
            self.work / "stop.capability",
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"
        self.assertEqual(self.request("/health")[0], 200)
        self.assertEqual(self.server.engine.store.snapshot(), before_state)

    def test_stop_requires_capability_not_pid(self) -> None:
        status, _ = self.request("/admin/stop", {})
        self.assertEqual(status, 403)
        token = (self.work / "stop.capability").read_text()
        status, body = self.request("/admin/stop", {}, token)
        self.assertEqual((status, body), (200, {"status": "stopping"}))
        self.thread.join(timeout=2)
        self.assertFalse(self.thread.is_alive())
