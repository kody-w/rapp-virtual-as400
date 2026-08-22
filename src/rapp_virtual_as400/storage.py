"""Private, atomic JSON persistence with process and thread serialization."""

from __future__ import annotations

import copy
import json
import os
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

try:
    import fcntl
except ImportError:  # pragma: no cover - project currently targets local POSIX hosts
    fcntl = None


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
