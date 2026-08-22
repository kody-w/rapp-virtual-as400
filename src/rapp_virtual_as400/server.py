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

MAX_REQUEST_BYTES = 8192


class RAPPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], state_path: str | Path, capability_path: str | Path) -> None:
        super().__init__(address, RAPPHandler)
        self.engine = VirtualAS400(state_path)
        self.stop_capability = secrets.token_urlsafe(32)
        capability = Path(capability_path).expanduser().resolve()
        capability.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(capability.parent, 0o700)
        descriptor = os.open(capability, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(self.stop_capability)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(capability, 0o600)
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
            self._json(
                HTTPStatus.OK,
                {
                    "status": "ok",
                    "service": "rapp-virtual-as400",
                    "version": __version__,
                    "protocol": "RAPP/1",
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
        try:
            payload = json.loads(self.rfile.read(length))
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise Refusal("Request body must be valid JSON.", "INVALID_REQUEST") from None
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
