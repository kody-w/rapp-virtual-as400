"""Fixed-protocol worker for one isolated virtual AS400-style node."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

from .engine import VirtualAS400
from .errors import Refusal
from .storage import MAX_RESTORE_SNAPSHOT_BYTES

MAX_WORKER_MESSAGE_BYTES = 8192
MAX_RESTORE_MESSAGE_BYTES = MAX_RESTORE_SNAPSHOT_BYTES + 1024


def _canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _control(control: str, **values: object) -> dict:
    return {"protocol": "RAPP/1", "control": control, "status": "ok", **values}


def _handle(engine: VirtualAS400, message: object) -> tuple[dict, bool]:
    if not isinstance(message, dict) or message.get("protocol") != "RAPP/1":
        raise Refusal("Worker messages require protocol RAPP/1.", "INVALID_REQUEST")
    kind = message.get("kind")
    if kind == "chat":
        allowed = {"protocol", "kind", "user_input", "session_id", "idempotency_key", "event_at"}
        if set(message) - allowed:
            raise Refusal("Unsupported worker chat field.", "INVALID_REQUEST")
        result = engine.chat(
            message.get("user_input"),  # type: ignore[arg-type]
            message.get("session_id"),  # type: ignore[arg-type]
            message.get("idempotency_key"),  # type: ignore[arg-type]
            event_at=message.get("event_at"),  # type: ignore[arg-type]
        )
        return result, False
    if kind != "control":
        raise Refusal("Worker kind must be chat or control.", "INVALID_REQUEST")
    operation = message.get("operation")
    if operation == "snapshot":
        if set(message) != {"protocol", "kind", "operation"}:
            raise Refusal("Snapshot control has unsupported fields.", "INVALID_REQUEST")
        return _control("snapshot", state=engine.store.snapshot()), False
    if operation == "reset":
        if set(message) != {"protocol", "kind", "operation"}:
            raise Refusal("Reset control has unsupported fields.", "INVALID_REQUEST")
        engine.store.reset()
        return _control("reset"), False
    if operation == "restore":
        if set(message) != {"protocol", "kind", "operation", "state"}:
            raise Refusal("Restore control has unsupported fields.", "INVALID_REQUEST")
        engine.store.restore(message["state"])
        return _control("restore", state_hash=hashlib.sha256(_canonical(engine.store.snapshot())).hexdigest()), False
    if operation == "simulate":
        allowed = {"protocol", "kind", "operation", "job", "replica", "mode", "expected"}
        if set(message) - allowed:
            raise Refusal("Simulation control has unsupported fields.", "INVALID_REQUEST")
        job = message.get("job")
        replica = message.get("replica")
        mode = message.get("mode")
        expected = message.get("expected")
        if not isinstance(job, dict) or not isinstance(replica, int) or isinstance(replica, bool):
            raise Refusal("Simulation job and replica are invalid.", "INVALID_REQUEST")
        if mode not in {"deterministic", "stochastic"} or not isinstance(expected, bool):
            raise Refusal("Simulation mode is invalid.", "INVALID_REQUEST")
        base = hashlib.sha256(_canonical(job)).hexdigest()
        if mode == "deterministic" or expected:
            outcome = f"COMPLETE:{base}"
        else:
            outlier = hashlib.sha256(_canonical({"job": job, "replica": replica})).hexdigest()
            outcome = f"OUTLIER:{outlier}"
        return _control("simulate", replica=replica, outcome=outcome), False
    if operation == "stop":
        if set(message) != {"protocol", "kind", "operation"}:
            raise Refusal("Stop control has unsupported fields.", "INVALID_REQUEST")
        return _control("stop"), True
    raise Refusal("Control operation is not allowlisted.", "COMMAND_NOT_ALLOWED")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    args = parser.parse_args(argv)
    engine: VirtualAS400 | None = None
    storage_error: Refusal | None = None
    try:
        engine = VirtualAS400(args.root.expanduser().resolve() / "state.json")
    except Refusal as error:
        if error.code != "RECOVERY_REQUIRED":
            raise
        storage_error = error
    for line in sys.stdin:
        stop = False
        session_id = ""
        try:
            message_size = len(line.encode("utf-8"))
            if message_size > MAX_RESTORE_MESSAGE_BYTES:
                raise Refusal("Worker message exceeds the bounded restore limit.", "LIMIT_EXCEEDED")
            message = json.loads(line)
            is_restore = (
                isinstance(message, dict)
                and message.get("kind") == "control"
                and message.get("operation") == "restore"
            )
            if message_size > MAX_WORKER_MESSAGE_BYTES and not is_restore:
                raise Refusal("Worker message exceeds 8192 bytes.", "LIMIT_EXCEEDED")
            if isinstance(message, dict) and isinstance(message.get("session_id"), str):
                session_id = message["session_id"]
            if storage_error is not None:
                raise storage_error
            if engine is None:
                raise Refusal(
                    "State recovery is required before this store can accept requests.",
                    "RECOVERY_REQUIRED",
                )
            response, stop = _handle(engine, message)
        except (json.JSONDecodeError, UnicodeError):
            response = Refusal("Worker message must be valid JSON.", "INVALID_REQUEST").envelope("")
        except Refusal as error:
            response = error.envelope(session_id)
        except Exception:
            response = Refusal(
                "Worker could not safely process the request.",
                "WORKER_ERROR",
            ).envelope(session_id)
        sys.stdout.write(json.dumps(response, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
        sys.stdout.flush()
        if stop:
            break
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
