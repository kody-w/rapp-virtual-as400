"""Private, atomic JSON persistence with process and thread serialization."""

from __future__ import annotations

import copy
import json
import os
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .errors import Refusal

try:
    import fcntl
except ImportError:  # pragma: no cover - project currently targets local POSIX hosts
    fcntl = None

MAX_RESTORE_SNAPSHOT_BYTES = 4 * 1024 * 1024
MAX_SNAPSHOT_DEPTH = 32


def empty_state() -> dict:
    return {
        "format": 1,
        "revision": 0,
        "libraries": {},
        "data_queues": {},
        "job_queues": {},
        "jobs": {},
        "spool": [],
        "sessions": {},
        "idempotency": {},
        "next_job": 1,
        "next_spool": 1,
    }


class AtomicStore:
    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.path.parent, 0o700)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self._thread_lock = threading.RLock()
        if not self.path.exists():
            self._write(empty_state())

    def _read(self) -> dict:
        with self.path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def _write(self, state: dict) -> None:
        temp = self.path.with_suffix(self.path.suffix + ".new")
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        descriptor = os.open(temp, flags, 0o600)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(state, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temp, 0o600)
            os.replace(temp, self.path)
            os.chmod(self.path, 0o600)
            directory_fd = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if temp.exists():
                temp.unlink()

    @staticmethod
    def validate_snapshot(snapshot: object) -> dict:
        expected = {
            "format",
            "revision",
            "libraries",
            "data_queues",
            "job_queues",
            "jobs",
            "spool",
            "sessions",
            "idempotency",
            "next_job",
            "next_spool",
        }
        if not isinstance(snapshot, dict) or set(snapshot) != expected:
            raise Refusal("Restore snapshot has an invalid schema.", "INVALID_SNAPSHOT")
        integer_fields = ("revision", "next_job", "next_spool")
        if snapshot["format"] != 1:
            raise Refusal("Restore snapshot has an invalid format.", "INVALID_SNAPSHOT")
        for field in integer_fields:
            value = snapshot[field]
            minimum = 0 if field == "revision" else 1
            if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
                raise Refusal("Restore snapshot has an invalid counter.", "INVALID_SNAPSHOT")
        for field in ("libraries", "data_queues", "job_queues", "jobs", "sessions", "idempotency"):
            if not isinstance(snapshot[field], dict):
                raise Refusal("Restore snapshot has an invalid mapping.", "INVALID_SNAPSHOT")
        if not isinstance(snapshot["spool"], list):
            raise Refusal("Restore snapshot has an invalid spool.", "INVALID_SNAPSHOT")

        def exact_mapping(value: object, keys: set[str]) -> bool:
            return isinstance(value, dict) and set(value) == keys

        for library in snapshot["libraries"].values():
            if not exact_mapping(library, {"files"}) or not isinstance(library["files"], dict):
                raise Refusal("Restore snapshot has an invalid library.", "INVALID_SNAPSHOT")
            for file in library["files"].values():
                if not exact_mapping(file, {"fields", "records"}):
                    raise Refusal("Restore snapshot has an invalid physical file.", "INVALID_SNAPSHOT")
                if not isinstance(file["fields"], list) or not isinstance(file["records"], list):
                    raise Refusal("Restore snapshot has an invalid physical file.", "INVALID_SNAPSHOT")
                field_names: set[str] = set()
                for field in file["fields"]:
                    if not exact_mapping(field, {"name", "type", "precision", "scale"}):
                        raise Refusal("Restore snapshot has an invalid field.", "INVALID_SNAPSHOT")
                    name, kind = field["name"], field["type"]
                    precision, scale = field["precision"], field["scale"]
                    if (
                        not isinstance(name, str)
                        or not name
                        or name in field_names
                        or kind not in {"CHAR", "INT", "DECIMAL"}
                        or not isinstance(precision, int)
                        or isinstance(precision, bool)
                        or not isinstance(scale, int)
                        or isinstance(scale, bool)
                    ):
                        raise Refusal("Restore snapshot has an invalid field.", "INVALID_SNAPSHOT")
                    if (
                        (kind == "CHAR" and not (1 <= precision <= 256 and scale == 0))
                        or (kind == "INT" and (precision != 0 or scale != 0))
                        or (kind == "DECIMAL" and not (1 <= precision <= 38 and 0 <= scale < precision))
                    ):
                        raise Refusal("Restore snapshot has an invalid field.", "INVALID_SNAPSHOT")
                    field_names.add(name)
                for record in file["records"]:
                    if (
                        not isinstance(record, dict)
                        or set(record) != field_names
                        or any(not isinstance(value, str) for value in record.values())
                    ):
                        raise Refusal("Restore snapshot has an invalid record.", "INVALID_SNAPSHOT")

        for queues in (snapshot["data_queues"], snapshot["job_queues"]):
            if any(
                not isinstance(queue, list) or any(not isinstance(item, str) for item in queue)
                for queue in queues.values()
            ):
                raise Refusal("Restore snapshot has an invalid queue.", "INVALID_SNAPSHOT")
        for job in snapshot["jobs"].values():
            if not exact_mapping(job, {"queue", "command", "status", "result"}) or any(
                not isinstance(value, str) for value in job.values()
            ):
                raise Refusal("Restore snapshot has an invalid job.", "INVALID_SNAPSHOT")
        for spool in snapshot["spool"]:
            if not exact_mapping(spool, {"id", "title", "created_at", "report"}) or any(
                not isinstance(value, str) for value in spool.values()
            ):
                raise Refusal("Restore snapshot has an invalid spool entry.", "INVALID_SNAPSHOT")
        for session in snapshot["sessions"].values():
            if not exact_mapping(session, {"turns"}) or not isinstance(session["turns"], list):
                raise Refusal("Restore snapshot has an invalid session.", "INVALID_SNAPSHOT")
            for turn in session["turns"]:
                if not exact_mapping(turn, {"at", "input", "response"}) or any(
                    not isinstance(value, str) for value in turn.values()
                ):
                    raise Refusal("Restore snapshot has an invalid session turn.", "INVALID_SNAPSHOT")
        for cached in snapshot["idempotency"].values():
            if (
                not exact_mapping(cached, {"request_hash", "result"})
                or not isinstance(cached["request_hash"], str)
                or not exact_mapping(cached["result"], {"response", "agent_logs", "session_id"})
                or not isinstance(cached["result"]["response"], str)
                or not isinstance(cached["result"]["session_id"], str)
                or not isinstance(cached["result"]["agent_logs"], list)
            ):
                raise Refusal("Restore snapshot has invalid idempotency evidence.", "INVALID_SNAPSHOT")
            for log in cached["result"]["agent_logs"]:
                if not exact_mapping(log, {"command", "status"}) or any(
                    not isinstance(value, str) for value in log.values()
                ):
                    raise Refusal("Restore snapshot has invalid agent logs.", "INVALID_SNAPSHOT")

        stack: list[tuple[object, int]] = [(snapshot, 0)]
        while stack:
            value, depth = stack.pop()
            if depth > MAX_SNAPSHOT_DEPTH:
                raise Refusal("Restore snapshot exceeds the depth limit.", "LIMIT_EXCEEDED")
            if isinstance(value, dict):
                if any(not isinstance(key, str) for key in value):
                    raise Refusal("Restore snapshot keys must be strings.", "INVALID_SNAPSHOT")
                stack.extend((item, depth + 1) for item in value.values())
            elif isinstance(value, list):
                stack.extend((item, depth + 1) for item in value)
            elif value is not None and not isinstance(value, (str, int, bool)):
                raise Refusal("Restore snapshot contains a non-JSON value.", "INVALID_SNAPSHOT")
        try:
            encoded = json.dumps(
                snapshot,
                allow_nan=False,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        except (TypeError, ValueError, UnicodeError):
            raise Refusal("Restore snapshot must be canonical JSON.", "INVALID_SNAPSHOT") from None
        if len(encoded) > MAX_RESTORE_SNAPSHOT_BYTES:
            raise Refusal("Restore snapshot exceeds the bounded restore limit.", "LIMIT_EXCEEDED")
        return copy.deepcopy(snapshot)

    @contextmanager
    def transaction(self) -> Iterator[dict]:
        with self._thread_lock:
            lock_descriptor = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o600)
            try:
                os.chmod(self.lock_path, 0o600)
                if fcntl:
                    fcntl.flock(lock_descriptor, fcntl.LOCK_EX)
                original = self._read()
                working = copy.deepcopy(original)
                yield working
                working["revision"] = original.get("revision", 0) + 1
                self._write(working)
            finally:
                if fcntl:
                    fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
                os.close(lock_descriptor)

    def snapshot(self) -> dict:
        with self._thread_lock:
            return copy.deepcopy(self._read())

    def restore(self, snapshot: object) -> None:
        restored = self.validate_snapshot(snapshot)
        with self._thread_lock:
            lock_descriptor = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o600)
            try:
                os.chmod(self.lock_path, 0o600)
                if fcntl:
                    fcntl.flock(lock_descriptor, fcntl.LOCK_EX)
                self._write(restored)
            finally:
                if fcntl:
                    fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
                os.close(lock_descriptor)

    def reset(self) -> None:
        with self._thread_lock:
            lock_descriptor = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o600)
            try:
                os.chmod(self.lock_path, 0o600)
                if fcntl:
                    fcntl.flock(lock_descriptor, fcntl.LOCK_EX)
                self._write(empty_state())
            finally:
                if fcntl:
                    fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
                os.close(lock_descriptor)
