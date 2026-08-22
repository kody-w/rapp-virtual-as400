from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request

from rapp_virtual_as400.server import RAPPServer

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

    def test_exact_422_refusal_envelope(self) -> None:
        status, body = self.request("/chat", {})
        self.assertEqual(status, 422)
        self.assertEqual(set(body), {"error", "agent_logs", "session_id"})
        self.assertEqual(body["error"]["type"], "refusal")
        self.assertEqual(body["error"]["code"], "INVALID_REQUEST")
        self.assertEqual(body["agent_logs"], [])

    def test_stop_requires_capability_not_pid(self) -> None:
        status, _ = self.request("/admin/stop", {})
        self.assertEqual(status, 403)
        token = (self.work / "stop.capability").read_text()
        status, body = self.request("/admin/stop", {}, token)
        self.assertEqual((status, body), (200, {"status": "stopping"}))
        self.thread.join(timeout=2)
        self.assertFalse(self.thread.is_alive())
