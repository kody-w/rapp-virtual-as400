"""Exact local RAPP/1 HTTP transport using only the Python standard library."""

from __future__ import annotations

import json
import os
import secrets
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from . import __version__
from .engine import VirtualAS400
from .errors import Refusal
from .storage import enforce_private_mode, fsync_directory
from .unicode_safe import canonical_json_strings

MAX_REQUEST_BYTES = 8192


class RAPPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], state_path: str | Path, capability_path: str | Path) -> None:
        super().__init__(address, RAPPHandler)
        self.engine: VirtualAS400 | None = None
        self.storage_error: Refusal | None = None
        try:
            self.engine = VirtualAS400(state_path)
        except Refusal as error:
            if error.code != "RECOVERY_REQUIRED":
                raise
            self.storage_error = error
        self.stop_capability = secrets.token_urlsafe(32)
        capability = Path(capability_path).expanduser().resolve()
        capability.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        enforce_private_mode(capability.parent, 0o700)
        temporary = capability.with_name(
            f".{capability.name}.{secrets.token_hex(8)}.new"
        )
        try:
            descriptor = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(self.stop_capability)
                handle.flush()
                os.fsync(handle.fileno())
            enforce_private_mode(temporary, 0o600)
            os.replace(temporary, capability)
            enforce_private_mode(capability, 0o600)
            fsync_directory(capability.parent)
        finally:
            if temporary.exists():
                temporary.unlink()
        self.capability_path = capability

    def server_close(self) -> None:
        try:
            if self.capability_path.exists():
                self.capability_path.unlink()
        finally:
            super().server_close()


class RAPPHandler(BaseHTTPRequestHandler):
    server: RAPPServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:
        return

    def _json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/health":
            storage_error = self.server.storage_error
            self._json(
                HTTPStatus.OK,
                {
                    "status": "degraded" if storage_error else "ok",
                    "service": "rapp-virtual-as400",
                    "version": __version__,
                    "protocol": "RAPP/1",
                    **(
                        {"storage_error": storage_error.code}
                        if storage_error is not None
                        else {}
                    ),
                },
            )
            return
        self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def do_POST(self) -> None:
        if self.path == "/chat":
            self._chat()
            return
        if self.path == "/admin/stop":
            self._stop()
            return
        self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def _read_json(self) -> dict:
        content_type = self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        if content_type != "application/json":
            raise Refusal("Content-Type must be application/json.", "INVALID_REQUEST")
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            raise Refusal("Invalid Content-Length.", "INVALID_REQUEST") from None
        if length < 1 or length > MAX_REQUEST_BYTES:
            raise Refusal("Request body must contain 1 to 8192 bytes.", "LIMIT_EXCEEDED")
        raw = self.rfile.read(length)
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            raise Refusal("Request contains malformed Unicode.", "INVALID_REQUEST") from None
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            raise Refusal("Request body must be valid JSON.", "INVALID_REQUEST") from None
        payload = canonical_json_strings(payload)
        if not isinstance(payload, dict):
            raise Refusal("Request body must be a JSON object.", "INVALID_REQUEST")
        extra = set(payload) - {"user_input", "session_id", "idempotency_key"}
        if extra:
            raise Refusal(f"Unsupported request field(s): {', '.join(sorted(extra))}.", "INVALID_REQUEST")
        return payload

    def _chat(self) -> None:
        session_id = ""
        try:
            payload = self._read_json()
            session_id = payload.get("session_id") if isinstance(payload.get("session_id"), str) else ""
            if "user_input" not in payload:
                raise Refusal("user_input is required.", "INVALID_REQUEST")
            if self.server.storage_error is not None:
                raise self.server.storage_error
            if self.server.engine is None:
                raise Refusal(
                    "State recovery is required before this store can accept requests.",
                    "RECOVERY_REQUIRED",
                )
            result = self.server.engine.chat(
                payload["user_input"],
                payload.get("session_id"),
                payload.get("idempotency_key"),
            )
            self._json(HTTPStatus.OK, result)
        except Refusal as error:
            self._json(HTTPStatus.UNPROCESSABLE_ENTITY, error.envelope(session_id))

    def _stop(self) -> None:
        supplied = self.headers.get("Authorization", "")
        expected = f"Bearer {self.server.stop_capability}"
        if not secrets.compare_digest(supplied, expected):
            self._json(
                HTTPStatus.FORBIDDEN,
                Refusal("A valid stop capability is required.", "CAPABILITY_REQUIRED").envelope(""),
            )
            return
        self._json(HTTPStatus.OK, {"status": "stopping"})
        threading.Thread(target=self.server.shutdown, daemon=True).start()


def serve(
    host: str,
    port: int,
    state_path: str | Path,
    capability_path: str | Path,
) -> None:
    if host not in {"127.0.0.1", "::1", "localhost"}:
        raise Refusal("This prototype binds only to loopback.", "NETWORK_NOT_ALLOWED")
    server = RAPPServer((host, port), state_path, capability_path)
    try:
        server.serve_forever()
    finally:
        server.server_close()
