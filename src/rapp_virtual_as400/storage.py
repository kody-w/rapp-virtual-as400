"""Private, atomic JSON persistence with process and thread serialization."""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import re
import stat
import threading
import time
from contextlib import contextmanager
from decimal import Decimal, DecimalException, localcontext
from pathlib import Path
from typing import Iterator

from .errors import Refusal
from .parser import MAX_COMMAND_BYTES, NAME_RE, parse_batch

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows
    fcntl = None

try:
    import msvcrt
except ImportError:  # pragma: no cover - POSIX
    msvcrt = None

MAX_PERSISTED_STATE_BYTES = 4 * 1024 * 1024
MAX_RESTORE_SNAPSHOT_BYTES = MAX_PERSISTED_STATE_BYTES
MAX_RECOVERY_JOURNAL_BYTES = 12 * 1024 * 1024
MAX_SNAPSHOT_DEPTH = 32
MAX_SIX_DIGIT_ID = 999_999
SESSION_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_LOCKS_GUARD = threading.Lock()
_ROOT_LOCKS: dict[Path, "PortableRootLock"] = {}

PUBLICATION_FAILED_MESSAGE = "State publication failed; the prior state remains active."
RECOVERY_REQUIRED_MESSAGE = (
    "State recovery is required before this store can accept requests."
)


def enforce_private_mode(path: str | os.PathLike[str], mode: int) -> None:
    """Apply an exact private POSIX mode where mode bits are authoritative.

    Windows ``chmod`` only controls the read-only attribute and ``stat`` reports
    synthetic POSIX bits. Security there comes from the ACL inherited from the
    caller-selected private root, so pretending to enforce 0600/0700 would be
    misleading.
    """
    if os.name != "nt":
        os.chmod(path, mode)


def private_mode_mismatch(metadata_mode: int, expected: int) -> bool:
    """Report an authoritative POSIX mismatch without interpreting Windows bits."""
    return os.name != "nt" and stat.S_IMODE(metadata_mode) != expected


def fsync_directory(path: Path) -> None:
    """Persist directory entries where the platform exposes a safe primitive.

    Python does not support opening directory handles with ``os.open`` on
    Windows. File contents are still flushed before atomic publication there,
    but the directory entry cannot be flushed with the standard library.
    """
    if os.name == "nt":
        return
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class PortableRootLock:
    """Reentrant thread/process file lock shared by every instance for one root."""

    def __init__(self, root: Path) -> None:
        self.root = root.expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        enforce_private_mode(self.root, 0o700)
        self.path = self.root / ".neighborhood.lock"
        self._thread_lock = threading.RLock()
        self._local = threading.local()

    def __enter__(self) -> "PortableRootLock":
        self._thread_lock.acquire()
        depth = getattr(self._local, "depth", 0)
        if depth:
            self._local.depth = depth + 1
            return self
        descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            enforce_private_mode(self.path, 0o600)
            if os.fstat(descriptor).st_size == 0:
                os.write(descriptor, b"\0")
                os.fsync(descriptor)
            if fcntl is not None:
                fcntl.flock(descriptor, fcntl.LOCK_EX)
            elif msvcrt is not None:  # pragma: no cover - Windows
                while True:
                    try:
                        os.lseek(descriptor, 0, os.SEEK_SET)
                        msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
                        break
                    except OSError:
                        time.sleep(0.05)
            else:  # pragma: no cover - unsupported Python platform
                raise OSError("No supported interprocess file-lock backend.")
        except Exception:
            os.close(descriptor)
            self._thread_lock.release()
            raise
        self._local.descriptor = descriptor
        self._local.depth = 1
        return self

    def __exit__(self, *_: object) -> None:
        depth = self._local.depth
        if depth > 1:
            self._local.depth = depth - 1
            self._thread_lock.release()
            return
        descriptor = self._local.descriptor
        try:
            if fcntl is not None:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            elif msvcrt is not None:  # pragma: no cover - Windows
                os.lseek(descriptor, 0, os.SEEK_SET)
                msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
        finally:
            try:
                os.close(descriptor)
            except OSError:
                pass
            finally:
                del self._local.descriptor
                self._local.depth = 0
                self._thread_lock.release()


def root_lock(root: Path) -> PortableRootLock:
    resolved = root.expanduser().resolve()
    with _LOCKS_GUARD:
        lock = _ROOT_LOCKS.get(resolved)
        if lock is None:
            lock = PortableRootLock(resolved)
            _ROOT_LOCKS[resolved] = lock
        return lock


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


def encode_idempotency_identity(session_id: str, idempotency_key: str) -> str:
    """Return the canonical, reversible JSON tuple used as a cache mapping key."""
    return json.dumps(
        [session_id, idempotency_key],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def decode_idempotency_identity(value: object) -> tuple[str, str]:
    if not isinstance(value, str):
        raise Refusal("Idempotency cache identity is invalid.", "INVALID_SNAPSHOT")
    try:
        decoded = json.loads(value)
    except (json.JSONDecodeError, UnicodeError):
        raise Refusal("Idempotency cache identity is invalid.", "INVALID_SNAPSHOT") from None
    if (
        not isinstance(decoded, list)
        or len(decoded) != 2
        or any(not isinstance(item, str) for item in decoded)
        or any(SESSION_ID_RE.fullmatch(item) is None for item in decoded)
        or encode_idempotency_identity(decoded[0], decoded[1]) != value
    ):
        raise Refusal("Idempotency cache identity is invalid.", "INVALID_SNAPSHOT")
    return decoded[0], decoded[1]


def _serialized_state_bytes(state: object) -> bytes:
    try:
        encoded = json.dumps(
            state,
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError):
        raise Refusal("Persisted state must be canonical JSON.", "INVALID_SNAPSHOT") from None
    if len(encoded) > MAX_PERSISTED_STATE_BYTES:
        raise Refusal(
            "Persisted state exceeds the serialized byte limit.",
            "LIMIT_EXCEEDED",
        )
    return encoded


class AtomicStore:
    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        recover: bool = False,
    ) -> None:
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        enforce_private_mode(self.path.parent, 0o700)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.recovery_path = self.path.with_suffix(self.path.suffix + ".recovery")
        self._thread_lock = threading.RLock()
        lock_descriptor = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        os.close(lock_descriptor)
        enforce_private_mode(self.lock_path, 0o600)
        with root_lock(self.path.parent):
            self._recover_if_needed(force_prior=recover)
            if not self.path.exists():
                self._write(empty_state())
            else:
                persisted = self._read()
                validated = self.validate_snapshot(persisted)
                if validated != persisted:
                    self._write(validated)

    def _read(self) -> dict:
        with self.path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    @staticmethod
    def _hash(value: bytes) -> str:
        return hashlib.sha256(value).hexdigest()

    @staticmethod
    def _revision(value: bytes) -> int | None:
        try:
            decoded = json.loads(value)
        except (json.JSONDecodeError, UnicodeError):
            return None
        revision = decoded.get("revision") if isinstance(decoded, dict) else None
        return revision if isinstance(revision, int) and not isinstance(revision, bool) else None

    def _journal_bytes(
        self,
        phase: str,
        old: bytes,
        new: bytes,
        old_exists: bool,
    ) -> bytes:
        journal = {
            "format": 1,
            "phase": phase,
            "old_exists": old_exists,
            "old_hash": self._hash(old),
            "new_hash": self._hash(new),
            "old_revision": self._revision(old) if old_exists else None,
            "new_revision": self._revision(new),
            "old_bytes": base64.b64encode(old).decode("ascii"),
            "new_bytes": base64.b64encode(new).decode("ascii"),
        }
        encoded = json.dumps(
            journal,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
        if len(encoded) > MAX_RECOVERY_JOURNAL_BYTES:
            raise Refusal(RECOVERY_REQUIRED_MESSAGE, "RECOVERY_REQUIRED")
        return encoded

    def _load_journal(self) -> dict:
        try:
            metadata = self.recovery_path.stat()
            if metadata.st_size > MAX_RECOVERY_JOURNAL_BYTES:
                raise ValueError("oversized recovery journal")
            if private_mode_mismatch(metadata.st_mode, 0o600):
                raise ValueError("non-private recovery journal")
            encoded = self.recovery_path.read_bytes()
            if len(encoded) > MAX_RECOVERY_JOURNAL_BYTES:
                raise ValueError("oversized recovery journal")
            journal = json.loads(encoded)
            expected = {
                "format",
                "phase",
                "old_exists",
                "old_hash",
                "new_hash",
                "old_revision",
                "new_revision",
                "old_bytes",
                "new_bytes",
            }
            if (
                not isinstance(journal, dict)
                or set(journal) != expected
                or journal["format"] != 1
                or journal["phase"] not in {"prepared", "committed"}
                or not isinstance(journal["old_exists"], bool)
                or not isinstance(journal["old_bytes"], str)
                or not isinstance(journal["new_bytes"], str)
                or (
                    not journal["old_exists"]
                    and journal["old_revision"] is not None
                )
                or any(
                    not isinstance(journal[key], str)
                    or re.fullmatch(r"[0-9a-f]{64}", journal[key]) is None
                    for key in ("old_hash", "new_hash")
                )
                or any(
                    value is not None
                    and (not isinstance(value, int) or isinstance(value, bool) or value < 0)
                    for value in (journal["old_revision"], journal["new_revision"])
                )
            ):
                raise ValueError("invalid recovery journal")
            old = base64.b64decode(journal["old_bytes"], validate=True)
            new = base64.b64decode(journal["new_bytes"], validate=True)
            if (
                self._hash(old) != journal["old_hash"]
                or self._hash(new) != journal["new_hash"]
                or (not journal["old_exists"] and old != b"")
                or (
                    journal["old_exists"]
                    and self._revision(old) != journal["old_revision"]
                )
                or self._revision(new) != journal["new_revision"]
            ):
                raise ValueError("invalid recovery journal payload")
            journal["decoded_old_bytes"] = old
            journal["decoded_new_bytes"] = new
            return journal
        except (OSError, ValueError, TypeError, json.JSONDecodeError, UnicodeError):
            raise Refusal(RECOVERY_REQUIRED_MESSAGE, "RECOVERY_REQUIRED") from None

    def _publish_file(self, destination: Path, encoded: bytes) -> None:
        temp = destination.with_suffix(destination.suffix + ".new")
        descriptor = os.open(
            temp,
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
            0o600,
        )
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            enforce_private_mode(temp, 0o600)
            os.replace(temp, destination)
            fsync_directory(destination.parent)
            actual = destination.read_bytes()
            if actual != encoded or self._hash(actual) != self._hash(encoded):
                raise OSError("atomic publication verification failed")
        finally:
            if temp.exists():
                temp.unlink()

    def _remove_journal(self) -> None:
        try:
            self.recovery_path.unlink()
        except FileNotFoundError:
            return
        fsync_directory(self.path.parent)

    def _restore_prior(self, old: bytes, old_exists: bool) -> bool:
        try:
            if old_exists:
                self._publish_file(self.path, old)
            else:
                try:
                    self.path.unlink()
                except FileNotFoundError:
                    pass
                fsync_directory(self.path.parent)
            if old_exists:
                return (
                    self.path.exists()
                    and self.path.read_bytes() == old
                    and self._hash(self.path.read_bytes()) == self._hash(old)
                )
            return not self.path.exists()
        except OSError:
            try:
                if old_exists and self.path.exists() and self.path.read_bytes() == old:
                    fsync_directory(self.path.parent)
                    return self.path.read_bytes() == old
                if not old_exists and not self.path.exists():
                    fsync_directory(self.path.parent)
                    return not self.path.exists()
            except OSError:
                pass
            return False

    def _recover_if_needed(self, *, force_prior: bool = False) -> None:
        try:
            self.recovery_path.stat()
        except FileNotFoundError:
            return
        except OSError:
            raise Refusal(RECOVERY_REQUIRED_MESSAGE, "RECOVERY_REQUIRED") from None
        journal = self._load_journal()
        old = journal["decoded_old_bytes"]
        new = journal["decoded_new_bytes"]
        old_exists = journal["old_exists"]
        try:
            current = self.path.read_bytes() if self.path.exists() else None
        except OSError:
            raise Refusal(RECOVERY_REQUIRED_MESSAGE, "RECOVERY_REQUIRED") from None
        old_matches = current == old if old_exists else current is None
        new_matches = current == new

        if force_prior:
            if not self._restore_prior(old, old_exists):
                raise Refusal(RECOVERY_REQUIRED_MESSAGE, "RECOVERY_REQUIRED")
        elif journal["phase"] == "prepared":
            if new_matches:
                if not self._restore_prior(old, old_exists):
                    raise Refusal(RECOVERY_REQUIRED_MESSAGE, "RECOVERY_REQUIRED")
            elif not old_matches:
                raise Refusal(RECOVERY_REQUIRED_MESSAGE, "RECOVERY_REQUIRED")
        elif not (new_matches or old_matches):
            raise Refusal(RECOVERY_REQUIRED_MESSAGE, "RECOVERY_REQUIRED")

        try:
            self._remove_journal()
        except OSError:
            raise Refusal(RECOVERY_REQUIRED_MESSAGE, "RECOVERY_REQUIRED") from None

    def _write(self, state: dict) -> None:
        encoded = _serialized_state_bytes(state)
        self._recover_if_needed()
        try:
            self.path.stat()
            old_exists = True
        except FileNotFoundError:
            old_exists = False
        except OSError:
            raise Refusal(RECOVERY_REQUIRED_MESSAGE, "RECOVERY_REQUIRED") from None
        try:
            old = self.path.read_bytes() if old_exists else b""
        except OSError:
            raise Refusal(RECOVERY_REQUIRED_MESSAGE, "RECOVERY_REQUIRED") from None
        prepared = self._journal_bytes("prepared", old, encoded, old_exists)
        committed = self._journal_bytes("committed", old, encoded, old_exists)
        try:
            self._publish_file(self.recovery_path, prepared)
        except OSError:
            raise Refusal(PUBLICATION_FAILED_MESSAGE, "STORAGE_PUBLICATION_FAILED") from None

        try:
            self._publish_file(self.path, encoded)
        except OSError:
            try:
                current = self.path.read_bytes() if self.path.exists() else None
            except OSError:
                current = None
            old_matches = current == old if old_exists else current is None
            new_matches = current == encoded
            if new_matches and not self._restore_prior(old, old_exists):
                raise Refusal(RECOVERY_REQUIRED_MESSAGE, "RECOVERY_REQUIRED") from None
            if not old_matches and not new_matches:
                raise Refusal(RECOVERY_REQUIRED_MESSAGE, "RECOVERY_REQUIRED") from None
            try:
                self._remove_journal()
            except OSError:
                pass
            raise Refusal(
                PUBLICATION_FAILED_MESSAGE,
                "STORAGE_PUBLICATION_FAILED",
            ) from None

        try:
            self._publish_file(self.recovery_path, committed)
        except OSError:
            try:
                durable_commit = (
                    self.recovery_path.read_bytes() == committed
                    and self.path.read_bytes() == encoded
                )
                if durable_commit:
                    fsync_directory(self.path.parent)
            except OSError:
                durable_commit = False
            if not durable_commit:
                if not self._restore_prior(old, old_exists):
                    raise Refusal(RECOVERY_REQUIRED_MESSAGE, "RECOVERY_REQUIRED") from None
                try:
                    self._remove_journal()
                except OSError:
                    pass
                raise Refusal(
                    PUBLICATION_FAILED_MESSAGE,
                    "STORAGE_PUBLICATION_FAILED",
                ) from None

        try:
            self._remove_journal()
        except OSError:
            pass

    def recover(self) -> None:
        """Explicitly restore the exact pre-publication state from the journal."""
        with self._thread_lock, root_lock(self.path.parent):
            self._recover_if_needed(force_prior=True)

    @staticmethod
    def validate_snapshot(snapshot: object) -> dict:
        from . import engine as engine_module

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
        snapshot = copy.deepcopy(snapshot)
        integer_fields = ("revision", "next_job", "next_spool")
        if (
            not isinstance(snapshot["format"], int)
            or isinstance(snapshot["format"], bool)
            or snapshot["format"] != 1
        ):
            raise Refusal("Restore snapshot has an invalid format.", "INVALID_SNAPSHOT")
        for field in integer_fields:
            value = snapshot[field]
            minimum = 0 if field == "revision" else 1
            if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
                raise Refusal("Restore snapshot has an invalid counter.", "INVALID_SNAPSHOT")
            if field != "revision" and value > MAX_SIX_DIGIT_ID + 1:
                raise Refusal("Restore snapshot has an exhausted counter.", "INVALID_SNAPSHOT")
        for field in ("libraries", "data_queues", "job_queues", "jobs", "sessions", "idempotency"):
            if not isinstance(snapshot[field], dict):
                raise Refusal("Restore snapshot has an invalid mapping.", "INVALID_SNAPSHOT")
        if not isinstance(snapshot["spool"], list):
            raise Refusal("Restore snapshot has an invalid spool.", "INVALID_SNAPSHOT")

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
            encoded = _serialized_state_bytes(snapshot)
        except Refusal as error:
            if error.code == "LIMIT_EXCEEDED":
                raise Refusal(
                    "Restore snapshot exceeds the bounded restore limit.",
                    "LIMIT_EXCEEDED",
                ) from None
            raise Refusal("Restore snapshot must be canonical JSON.", "INVALID_SNAPSHOT") from None
        if len(encoded) > MAX_RESTORE_SNAPSHOT_BYTES:
            raise Refusal(
                "Restore snapshot exceeds the bounded restore limit.",
                "LIMIT_EXCEEDED",
            )

        def exact_mapping(value: object, keys: set[str]) -> bool:
            return isinstance(value, dict) and set(value) == keys

        def valid_name(value: object) -> bool:
            return isinstance(value, str) and NAME_RE.fullmatch(value) is not None

        def qualified(value: object) -> tuple[str, str] | None:
            if not isinstance(value, str):
                return None
            parts = value.split("/")
            if len(parts) != 2 or not all(valid_name(part) for part in parts):
                return None
            return parts[0], parts[1]

        def invalid(message: str) -> None:
            raise Refusal(message, "INVALID_SNAPSHOT")

        if len(snapshot["libraries"]) > engine_module.MAX_LIBRARIES:
            invalid("Restore snapshot exceeds the library limit.")
        if len(snapshot["data_queues"]) > engine_module.MAX_DATA_QUEUES:
            invalid("Restore snapshot exceeds the data queue limit.")
        if len(snapshot["job_queues"]) > engine_module.MAX_JOB_QUEUES:
            invalid("Restore snapshot exceeds the job queue limit.")
        if len(snapshot["jobs"]) > engine_module.MAX_JOBS:
            invalid("Restore snapshot exceeds the job limit.")
        if len(snapshot["spool"]) > engine_module.MAX_SPOOL:
            invalid("Restore snapshot exceeds the spool limit.")
        if len(snapshot["sessions"]) > engine_module.MAX_SESSIONS:
            invalid("Restore snapshot exceeds the session limit.")
        if len(snapshot["idempotency"]) > 2000:
            invalid("Restore snapshot exceeds the idempotency limit.")

        total_files = 0
        for library_name, library in snapshot["libraries"].items():
            if not valid_name(library_name):
                invalid("Restore snapshot has an invalid library name.")
            if not exact_mapping(library, {"files"}) or not isinstance(library["files"], dict):
                invalid("Restore snapshot has an invalid library.")
            total_files += len(library["files"])
            for file_name, file in library["files"].items():
                if not valid_name(file_name):
                    invalid("Restore snapshot has an invalid file name.")
                if not exact_mapping(file, {"fields", "records"}):
                    invalid("Restore snapshot has an invalid physical file.")
                if not isinstance(file["fields"], list) or not isinstance(file["records"], list):
                    invalid("Restore snapshot has an invalid physical file.")
                if not 1 <= len(file["fields"]) <= engine_module.MAX_FIELDS:
                    invalid("Restore snapshot has an invalid field count.")
                if len(file["records"]) > engine_module.MAX_RECORDS_PER_FILE:
                    invalid("Restore snapshot exceeds the record limit.")
                field_names: set[str] = set()
                fields: dict[str, dict] = {}
                for field in file["fields"]:
                    if not exact_mapping(field, {"name", "type", "precision", "scale"}):
                        invalid("Restore snapshot has an invalid field.")
                    name, kind = field["name"], field["type"]
                    precision, scale = field["precision"], field["scale"]
                    if (
                        not valid_name(name)
                        or name in field_names
                        or not isinstance(kind, str)
                        or kind not in {"CHAR", "INT", "DECIMAL"}
                        or not isinstance(precision, int)
                        or isinstance(precision, bool)
                        or not isinstance(scale, int)
                        or isinstance(scale, bool)
                    ):
                        invalid("Restore snapshot has an invalid field.")
                    if (
                        (kind == "CHAR" and not (1 <= precision <= 256 and scale == 0))
                        or (kind == "INT" and (precision != 0 or scale != 0))
                        or (kind == "DECIMAL" and not (1 <= precision <= 38 and 0 <= scale < precision))
                    ):
                        invalid("Restore snapshot has an invalid field.")
                    field_names.add(name)
                    fields[name] = field
                for record in file["records"]:
                    if (
                        not isinstance(record, dict)
                        or set(record) != field_names
                        or any(not isinstance(value, str) for value in record.values())
                    ):
                        invalid("Restore snapshot has an invalid record.")
                    for name, value in record.items():
                        field = fields[name]
                        if len(value) > engine_module.MAX_TEXT:
                            invalid("Restore snapshot record value exceeds its limit.")
                        if field["type"] == "CHAR" and len(value) > field["precision"]:
                            invalid("Restore snapshot has an invalid CHAR value.")
                        if field["type"] == "INT":
                            if not re.fullmatch(r"-?\d+", value):
                                invalid("Restore snapshot has an invalid INT value.")
                            number = int(value)
                            if not -(2**63) <= number < 2**63 or str(number) != value:
                                invalid("Restore snapshot has a noncanonical INT value.")
                        if field["type"] == "DECIMAL":
                            scale = field["scale"]
                            if not re.fullmatch(r"-?\d+(?:\.\d+)?", value):
                                invalid("Restore snapshot has an invalid DECIMAL value.")
                            try:
                                with localcontext() as context:
                                    context.prec = field["precision"]
                                    number = Decimal(value)
                                    quantum = Decimal(1).scaleb(-scale)
                                    quantized = number.quantize(quantum)
                            except DecimalException:
                                invalid("Restore snapshot has an invalid DECIMAL value.")
                            canonical = f"{quantized:.{scale}f}"
                            digits = len(canonical.replace("-", "").replace(".", ""))
                            if (
                                number != quantized
                                or canonical != value
                                or digits > field["precision"]
                            ):
                                invalid("Restore snapshot has a noncanonical DECIMAL value.")
        if total_files > engine_module.MAX_FILES:
            invalid("Restore snapshot exceeds the physical file limit.")

        for key, queue in snapshot["data_queues"].items():
            parsed = qualified(key)
            if parsed is None or parsed[0] not in snapshot["libraries"]:
                invalid("Restore snapshot has an invalid data queue name.")
            if (
                not isinstance(queue, list)
                or len(queue) > engine_module.MAX_QUEUE_ITEMS
                or any(
                    not isinstance(item, str) or len(item) > engine_module.MAX_TEXT
                    for item in queue
                )
            ):
                invalid("Restore snapshot has an invalid data queue.")

        queued_ids: list[str] = []
        for key, queue in snapshot["job_queues"].items():
            parsed = qualified(key)
            if parsed is None or parsed[0] not in snapshot["libraries"]:
                invalid("Restore snapshot has an invalid job queue name.")
            if (
                not isinstance(queue, list)
                or len(queue) > engine_module.MAX_QUEUE_ITEMS
                or any(not isinstance(item, str) for item in queue)
            ):
                invalid("Restore snapshot has an invalid job queue.")
            queued_ids.extend(queue)
        if len(queued_ids) != len(set(queued_ids)):
            invalid("Restore snapshot queues contain duplicate jobs.")

        max_job = 0
        for job_id, job in snapshot["jobs"].items():
            match = re.fullmatch(r"J([0-9]{6})", job_id) if isinstance(job_id, str) else None
            if match is None or int(match.group(1)) < 1:
                invalid("Restore snapshot has an invalid job identifier.")
            max_job = max(max_job, int(match.group(1)))
            if not exact_mapping(job, {"queue", "command", "status", "result"}) or any(
                not isinstance(value, str) for value in job.values()
            ):
                invalid("Restore snapshot has an invalid job.")
            if job["queue"] not in snapshot["job_queues"]:
                invalid("Restore snapshot job references a missing queue.")
            if job["status"] not in {"QUEUED", "READY", "COMPLETE"}:
                invalid("Restore snapshot has an invalid job status.")
            in_queue = job_id in queued_ids
            if (job["status"] == "QUEUED") != in_queue:
                invalid("Restore snapshot job and queue status diverge.")
            if in_queue and job_id not in snapshot["job_queues"][job["queue"]]:
                invalid("Restore snapshot job is in the wrong queue.")
            if job["status"] != "COMPLETE" and job["result"] != "":
                invalid("Restore snapshot has a premature job result.")
            try:
                engine_module.VirtualAS400._validated_submitted_command(job["command"])
            except Refusal:
                invalid("Restore snapshot has an invalid job command.")
        if set(queued_ids) - set(snapshot["jobs"]):
            invalid("Restore snapshot queue references a missing job.")
        if snapshot["next_job"] != max_job + 1:
            invalid("Restore snapshot has an incoherent next-job counter.")

        spool_ids: set[str] = set()
        max_spool = 0
        for spool in snapshot["spool"]:
            if not exact_mapping(spool, {"id", "title", "created_at", "report"}) or any(
                not isinstance(value, str) for value in spool.values()
            ):
                invalid("Restore snapshot has an invalid spool entry.")
            match = re.fullmatch(r"S([0-9]{6})", spool["id"])
            if (
                match is None
                or int(match.group(1)) < 1
                or spool["id"] in spool_ids
                or not 1 <= len(spool["title"]) <= 120
                or not 1 <= len(spool["created_at"]) <= 64
            ):
                invalid("Restore snapshot has an invalid spool entry.")
            spool_ids.add(spool["id"])
            max_spool = max(max_spool, int(match.group(1)))
        if snapshot["next_spool"] != max_spool + 1:
            invalid("Restore snapshot has an incoherent next-spool counter.")

        for session_id, session in snapshot["sessions"].items():
            if not isinstance(session_id, str) or SESSION_ID_RE.fullmatch(session_id) is None:
                invalid("Restore snapshot has an invalid session identifier.")
            if not exact_mapping(session, {"turns"}) or not isinstance(session["turns"], list):
                invalid("Restore snapshot has an invalid session.")
            if len(session["turns"]) > 100:
                invalid("Restore snapshot exceeds the session turn limit.")
            for turn in session["turns"]:
                if not exact_mapping(turn, {"at", "input", "response"}) or any(
                    not isinstance(value, str) for value in turn.values()
                ):
                    invalid("Restore snapshot has an invalid session turn.")
                if (
                    not 1 <= len(turn["at"]) <= 64
                    or len(turn["input"].encode("utf-8")) > MAX_COMMAND_BYTES
                ):
                    invalid("Restore snapshot has an invalid session timestamp.")
                try:
                    parse_batch(turn["input"])
                except Refusal:
                    invalid("Restore snapshot has an invalid session input.")
        migrated_idempotency: dict[str, dict] = {}
        for cache_key, cached in snapshot["idempotency"].items():
            if (
                not isinstance(cache_key, str)
                or not exact_mapping(cached, {"request_hash", "result"})
                or not isinstance(cached["request_hash"], str)
                or re.fullmatch(r"[0-9a-f]{64}", cached["request_hash"]) is None
                or not exact_mapping(cached["result"], {"response", "agent_logs", "session_id"})
                or not isinstance(cached["result"]["response"], str)
                or not isinstance(cached["result"]["session_id"], str)
                or not isinstance(cached["result"]["agent_logs"], list)
            ):
                invalid("Restore snapshot has invalid idempotency evidence.")
            result_session = cached["result"]["session_id"]
            if (
                SESSION_ID_RE.fullmatch(result_session) is None
                or len(cached["result"]["agent_logs"]) > 16
            ):
                invalid("Restore snapshot has invalid idempotency evidence.")
            if cache_key.startswith("["):
                try:
                    identity_session, identity_key = decode_idempotency_identity(cache_key)
                except Refusal:
                    invalid("Restore snapshot has invalid idempotency evidence.")
            else:
                prefix = f"{result_session}:"
                if (
                    not cache_key.startswith(prefix)
                    or SESSION_ID_RE.fullmatch(cache_key[len(prefix) :]) is None
                ):
                    invalid("Restore snapshot has ambiguous legacy idempotency evidence.")
                identity_session = result_session
                identity_key = cache_key[len(prefix) :]
            if identity_session != result_session:
                invalid("Restore snapshot idempotency session identity diverges.")
            canonical_key = encode_idempotency_identity(identity_session, identity_key)
            existing = migrated_idempotency.get(canonical_key)
            if existing is not None and existing != cached:
                invalid("Restore snapshot has conflicting idempotency evidence.")
            migrated_idempotency[canonical_key] = cached
            for log in cached["result"]["agent_logs"]:
                if not exact_mapping(log, {"command", "status"}) or any(
                    not isinstance(value, str) for value in log.values()
                ):
                    invalid("Restore snapshot has invalid agent logs.")
                if log["command"] not in engine_module.ALLOWED_CLAUSES or log["status"] != "ok":
                    invalid("Restore snapshot has invalid agent logs.")
        snapshot["idempotency"] = migrated_idempotency

        if snapshot["revision"] == 0 and any(
            snapshot[field]
            for field in (
                "libraries",
                "data_queues",
                "job_queues",
                "jobs",
                "spool",
                "sessions",
                "idempotency",
            )
        ):
            invalid("Restore snapshot has an incoherent revision.")

        migrated = _serialized_state_bytes(snapshot)
        if len(migrated) > MAX_RESTORE_SNAPSHOT_BYTES:
            raise Refusal(
                "Restore snapshot exceeds the bounded restore limit.",
                "LIMIT_EXCEEDED",
            )
        return copy.deepcopy(snapshot)

    @contextmanager
    def transaction(self) -> Iterator[dict]:
        with self._thread_lock:
            with root_lock(self.path.parent):
                self._recover_if_needed()
                original = self._read()
                working = copy.deepcopy(original)
                yield working
                working["revision"] = original.get("revision", 0) + 1
                self._write(working)

    def snapshot(self) -> dict:
        with self._thread_lock, root_lock(self.path.parent):
            self._recover_if_needed()
            return copy.deepcopy(self._read())

    def restore(self, snapshot: object) -> None:
        restored = self.validate_snapshot(snapshot)
        with self._thread_lock, root_lock(self.path.parent):
            self._recover_if_needed()
            self._write(restored)

    def reset(self) -> None:
        with self._thread_lock, root_lock(self.path.parent):
            self._recover_if_needed()
            self._write(empty_state())
