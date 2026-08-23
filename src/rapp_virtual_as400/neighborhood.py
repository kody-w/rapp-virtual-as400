"""Provider-neutral private-vNet simulator with isolated local node processes."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Iterator

from .errors import Refusal
from .storage import (
    AtomicStore,
    MAX_RESTORE_SNAPSHOT_BYTES,
    PortableRootLock,
    enforce_private_mode,
    fsync_directory as _fsync_directory,
    private_mode_mismatch,
    root_lock,
)
from .unicode_safe import canonical_json_strings

MAX_NODES = 8
MAX_REPLICAS = 100
MAX_JOB_BYTES = 2048
MAX_EVIDENCE_EVENTS = 10_000
MAX_EVIDENCE_BYTES = 32 * 1024 * 1024
MAX_EVIDENCE_RECORD_BYTES = 512 * 1024
MAX_SNAPSHOT_BUNDLE_BYTES = MAX_EVIDENCE_BYTES - MAX_EVIDENCE_RECORD_BYTES
MAX_RESTORE_MESSAGE_BYTES = MAX_RESTORE_SNAPSHOT_BYTES + 1024
NODE_RE = re.compile(r"^[A-Z][A-Z0-9-]{0,31}$")
BUNDLE_NAME_RE = re.compile(r"intent-[1-9][0-9]*\.json")
BUNDLE_TEMP_RE = re.compile(r"\.intent-[1-9][0-9]*\.json\.[0-9a-f]{32}\.tmp")
REPLAY_ROOT_RE = re.compile(r"\.replay-[0-9a-f]{32}")
MAX_REPLAY_ROOT_ENTRIES = 8
PROCESS_CLOSE_TIMEOUT_SECONDS = 2


def _json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _digest(value: object) -> str:
    return hashlib.sha256(_json_bytes(value)).hexdigest()


class EvidenceLedger:
    """Private append-only, hash-chained JSON Lines evidence."""

    def __init__(self, path: Path, transaction_lock: PortableRootLock | None = None) -> None:
        self.path = path.resolve()
        lock_root = self.path.parent.parent if self.path.parent.name == "evidence" else self.path.parent
        self.transaction_lock = transaction_lock or root_lock(lock_root)
        with self.transaction_lock:
            self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            enforce_private_mode(self.path.parent, 0o700)
            self._bundle_bytes_path = self.path.parent / ".bundle-bytes"
            directory_changed = False
            if not self.path.exists():
                descriptor = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
                directory_changed = True
            enforce_private_mode(self.path, 0o600)
            self._snapshots_path = self.path.parent / "snapshots"
            snapshots_created = not self._snapshots_path.exists()
            self._snapshots_path.mkdir(parents=False, exist_ok=True, mode=0o700)
            if self._snapshots_path.is_symlink() or not self._snapshots_path.is_dir():
                raise Refusal("Snapshot evidence directory is unsafe.", "EVIDENCE_INVALID")
            enforce_private_mode(self._snapshots_path, 0o700)
            if snapshots_created or directory_changed:
                _fsync_directory(self.path.parent)
            self._cleanup_stale_bundle_temps()
            self._write_bundle_bytes(self._scan_bundle_bytes())
            self._sequence, self._previous = self._refresh_tail()

    @staticmethod
    def _validate_entry(entry: object, sequence: int, previous: str) -> dict:
        if not isinstance(entry, dict) or set(entry) != {
            "sequence",
            "previous_hash",
            "record",
            "event_hash",
        }:
            raise Refusal("Evidence entry schema is invalid.", "EVIDENCE_INVALID")
        if (
            not isinstance(entry["sequence"], int)
            or isinstance(entry["sequence"], bool)
            or entry["sequence"] != sequence
            or not isinstance(entry["previous_hash"], str)
            or re.fullmatch(r"[0-9a-f]{64}", entry["previous_hash"]) is None
            or entry["previous_hash"] != previous
            or not isinstance(entry["record"], dict)
            or not isinstance(entry["event_hash"], str)
            or re.fullmatch(r"[0-9a-f]{64}", entry["event_hash"]) is None
        ):
            raise Refusal("Evidence sequence or hash link is invalid.", "EVIDENCE_INVALID")
        unsigned = {
            "sequence": entry["sequence"],
            "previous_hash": entry["previous_hash"],
            "record": entry["record"],
        }
        if entry["event_hash"] != _digest(unsigned):
            raise Refusal("Evidence event hash is invalid.", "EVIDENCE_INVALID")
        return entry

    def _refresh_tail(self) -> tuple[int, str]:
        size = self.path.stat().st_size
        if size == 0:
            self._sequence, self._previous = 0, "0" * 64
            return self._sequence, self._previous
        window = min(size, (MAX_EVIDENCE_RECORD_BYTES * 2) + 4096)
        with self.path.open("rb") as handle:
            handle.seek(size - window)
            chunk = handle.read(window)
        if not chunk.endswith(b"\n"):
            raise Refusal("Evidence tail is incomplete.", "EVIDENCE_INVALID")
        lines = chunk[:-1].split(b"\n")
        complete = lines[1:] if size > window else lines
        if not complete or (size > window and len(complete) < 2):
            raise Refusal("Evidence tail exceeds its record bound.", "EVIDENCE_INVALID")

        def decode(encoded: bytes) -> dict:
            if len(encoded) > MAX_EVIDENCE_RECORD_BYTES:
                raise Refusal("Evidence record exceeds its byte limit.", "EVIDENCE_INVALID")
            try:
                entry = json.loads(encoded)
            except (json.JSONDecodeError, UnicodeError):
                raise Refusal("Evidence tail is invalid JSON.", "EVIDENCE_INVALID") from None
            if (
                not isinstance(entry, dict)
                or not isinstance(entry.get("sequence"), int)
                or isinstance(entry.get("sequence"), bool)
                or entry["sequence"] < 1
                or not isinstance(entry.get("previous_hash"), str)
            ):
                raise Refusal("Evidence tail schema is invalid.", "EVIDENCE_INVALID")
            return self._validate_entry(entry, entry["sequence"], entry["previous_hash"])

        checked = decode(complete[-1])
        if len(complete) == 1:
            if checked["sequence"] != 1 or checked["previous_hash"] != "0" * 64:
                raise Refusal("Evidence first event link is invalid.", "EVIDENCE_INVALID")
        else:
            penultimate = decode(complete[-2])
            if (
                checked["sequence"] != penultimate["sequence"] + 1
                or checked["previous_hash"] != penultimate["event_hash"]
            ):
                raise Refusal("Evidence tail hash link is invalid.", "EVIDENCE_INVALID")
        self._sequence, self._previous = checked["sequence"], checked["event_hash"]
        return self._sequence, self._previous

    def _scan_bundle_bytes(self) -> int:
        total = 0
        for child in self._snapshots_path.iterdir():
            if (
                BUNDLE_NAME_RE.fullmatch(child.name) is None
                or child.is_symlink()
                or not child.is_file()
            ):
                raise Refusal("Snapshot evidence contains an unsafe entry.", "EVIDENCE_INVALID")
            total += child.stat().st_size
        return total

    def _cleanup_stale_bundle_temps(self) -> None:
        removed = False
        for child in self._snapshots_path.iterdir():
            if BUNDLE_TEMP_RE.fullmatch(child.name) is None:
                continue
            metadata = child.lstat()
            if not stat.S_ISREG(metadata.st_mode):
                raise Refusal("Snapshot evidence contains an unsafe temporary.", "EVIDENCE_INVALID")
            child.unlink()
            removed = True
        if removed:
            _fsync_directory(self._snapshots_path)

    def _write_bundle_bytes(self, value: int) -> None:
        temporary = self._bundle_bytes_path.with_suffix(".new")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with os.fdopen(descriptor, "w", encoding="ascii") as handle:
                handle.write(str(value))
                handle.flush()
                os.fsync(handle.fileno())
            enforce_private_mode(temporary, 0o600)
            os.replace(temporary, self._bundle_bytes_path)
            enforce_private_mode(self._bundle_bytes_path, 0o600)
            _fsync_directory(self.path.parent)
        finally:
            if temporary.exists():
                temporary.unlink()

    def _read_bundle_bytes(self) -> int:
        try:
            text = self._bundle_bytes_path.read_text(encoding="ascii")
            value = int(text)
        except (OSError, UnicodeError, ValueError):
            raise Refusal("Snapshot evidence byte metadata is invalid.", "EVIDENCE_INVALID") from None
        if value < 0 or str(value) != text:
            raise Refusal("Snapshot evidence byte metadata is invalid.", "EVIDENCE_INVALID")
        return value

    def _evidence_bytes(self) -> int:
        return self.path.stat().st_size + self._read_bundle_bytes()

    def next_sequence(self) -> int:
        with self.transaction_lock:
            sequence, _ = self._refresh_tail()
            return sequence + 1

    def read(self) -> list[dict]:
        with self.transaction_lock:
            entries: list[dict] = []
            previous = "0" * 64
            with self.path.open("rb") as handle:
                for sequence, line in enumerate(handle, 1):
                    if len(line) > MAX_EVIDENCE_RECORD_BYTES + 1:
                        raise Refusal("Evidence record exceeds its byte limit.", "EVIDENCE_INVALID")
                    try:
                        entry = json.loads(line)
                    except (json.JSONDecodeError, UnicodeError):
                        raise Refusal("Evidence contains invalid JSON.", "EVIDENCE_INVALID") from None
                    checked = self._validate_entry(entry, sequence, previous)
                    previous = checked["event_hash"]
                    entries.append(checked)
            self._sequence = len(entries)
            self._previous = previous
            return entries

    def _audit_transactions(self, allow_unmatched: bool) -> tuple[list[dict], list[dict]]:
        entries = self.read()
        with self.transaction_lock:
            if self._scan_bundle_bytes() != self._read_bundle_bytes():
                raise Refusal("Snapshot evidence byte metadata diverged.", "EVIDENCE_INVALID")
        by_sequence = {entry["sequence"]: entry for entry in entries}
        intents: dict[int, dict] = {}
        terminals: dict[int, list[dict]] = {}
        for entry in entries:
            record = entry["record"]
            record_type = record.get("type")
            if record_type == "replicated_chat_intent":
                reference = record.get("snapshot_bundle")
                nodes = record.get("nodes")
                pre_state_hashes = record.get("pre_state_hashes")
                message = record.get("message")
                if (
                    not isinstance(reference, dict)
                    or reference.get("path")
                    != f"snapshots/intent-{entry['sequence']}.json"
                    or not isinstance(message, dict)
                    or set(message)
                    != {
                        "protocol",
                        "kind",
                        "user_input",
                        "session_id",
                        "idempotency_key",
                        "event_at",
                    }
                    or message.get("protocol") != "RAPP/1"
                    or message.get("kind") != "chat"
                    or any(
                        not isinstance(message.get(field), str)
                        for field in (
                            "user_input",
                            "session_id",
                            "idempotency_key",
                            "event_at",
                        )
                    )
                    or not isinstance(nodes, list)
                    or not 2 <= len(nodes) <= MAX_NODES
                    or len(set(nodes)) != len(nodes)
                    or any(
                        not isinstance(node_id, str) or NODE_RE.fullmatch(node_id) is None
                        for node_id in nodes
                    )
                    or not isinstance(pre_state_hashes, dict)
                    or set(pre_state_hashes) != set(nodes)
                    or any(
                        not isinstance(digest, str)
                        or re.fullmatch(r"[0-9a-f]{64}", digest) is None
                        for digest in pre_state_hashes.values()
                    )
                    or record.get("terminal_sequence") != entry["sequence"] + 1
                    or record.get("snapshot_bundle_path")
                    != reference.get("path")
                ):
                    raise Refusal("Replication intent evidence is invalid.", "EVIDENCE_INVALID")
                bundle = self.read_snapshot_bundle(reference)
                if (
                    set(bundle["pre_snapshots"]) != set(nodes)
                    or bundle["pre_state_hashes"] != pre_state_hashes
                    or len(set(pre_state_hashes.values())) != 1
                ):
                    raise Refusal("Replication intent snapshot evidence diverges.", "EVIDENCE_INVALID")
                intents[entry["sequence"]] = entry
                continue
            if record_type in {
                "replicated_chat_commit",
                "replicated_chat_failure",
                "replicated_chat_recovery",
            }:
                if "pre_snapshots" in record:
                    raise Refusal("Terminal evidence duplicates private snapshots.", "EVIDENCE_INVALID")
                intent = by_sequence.get(record.get("intent_sequence"))
                if (
                    intent is None
                    or intent["event_hash"] != record.get("intent_event_hash")
                    or intent["record"].get("type") != "replicated_chat_intent"
                    or entry["sequence"] != intent["record"].get("terminal_sequence")
                    or intent["record"].get("snapshot_bundle") != record.get("snapshot_bundle")
                    or intent["record"].get("pre_state_hashes")
                    != record.get("pre_state_hashes")
                    or intent["record"].get("message") != record.get("message")
                ):
                    raise Refusal("Terminal evidence intent link is invalid.", "EVIDENCE_INVALID")
                terminals.setdefault(intent["sequence"], []).append(entry)
                bundle = self.read_snapshot_bundle(record.get("snapshot_bundle"))
                if bundle["pre_state_hashes"] != record.get("pre_state_hashes"):
                    raise Refusal("Terminal evidence pre-state hashes diverge.", "EVIDENCE_INVALID")
                runtime_hashes = {
                    node_id: _digest(snapshot)
                    for node_id, snapshot in bundle["pre_snapshots"].items()
                }
                valid_restore_hashes = (
                    bundle["pre_state_hashes"],
                    runtime_hashes,
                )
                restore = record.get("restore")
                if (
                    not isinstance(restore, dict)
                    or set(restore) != {"status", "state_hashes", "failures"}
                    or restore["status"] not in {"not_required", "verified", "failed"}
                    or not isinstance(restore["state_hashes"], dict)
                    or any(
                        not isinstance(node_id, str)
                        or NODE_RE.fullmatch(node_id) is None
                        or not isinstance(digest, str)
                        or re.fullmatch(r"[0-9a-f]{64}", digest) is None
                        for node_id, digest in restore["state_hashes"].items()
                    )
                    or not isinstance(restore["failures"], list)
                    or any(not isinstance(failure, str) for failure in restore["failures"])
                ):
                    raise Refusal("Terminal restore evidence is invalid.", "EVIDENCE_INVALID")
                if record["type"] == "replicated_chat_commit" and restore != {
                    "status": "not_required",
                    "state_hashes": {},
                    "failures": [],
                }:
                    raise Refusal("Commit restore evidence is invalid.", "EVIDENCE_INVALID")
                if record_type == "replicated_chat_commit":
                    state_hashes = record.get("state_hashes")
                    results = record.get("results")
                    if (
                        record.get("converged") is not True
                        or not isinstance(results, dict)
                        or set(results) != set(intent["record"]["nodes"])
                        or any(
                            not isinstance(result, dict)
                            or set(result) != {"response", "agent_logs", "session_id"}
                            or result["session_id"] != intent["record"]["message"]["session_id"]
                            for result in results.values()
                        )
                        or not isinstance(state_hashes, dict)
                        or set(state_hashes) != set(intent["record"]["nodes"])
                        or any(
                            not isinstance(digest, str)
                            or re.fullmatch(r"[0-9a-f]{64}", digest) is None
                            for digest in state_hashes.values()
                        )
                        or len(set(state_hashes.values())) != 1
                    ):
                        raise Refusal("Commit convergence evidence is invalid.", "EVIDENCE_INVALID")
                elif record_type == "replicated_chat_recovery":
                    restored_state_hashes = record.get("restored_state_hashes")
                    if (
                        restore["status"] != "verified"
                        or restore["failures"] != []
                        or restore["state_hashes"] != restored_state_hashes
                        or restored_state_hashes not in valid_restore_hashes
                        or record.get("rollback_verified") is not True
                        or record.get("rollback_failures") != []
                        or record.get("converged") is not True
                        or len(set(restored_state_hashes.values())) != 1
                    ):
                        raise Refusal("Recovery terminal evidence is invalid.", "EVIDENCE_INVALID")
                else:
                    rollback_required = record.get("rollback_required")
                    rollback_verified = record.get("rollback_verified")
                    failure = record.get("failure")
                    if (
                        not isinstance(rollback_required, bool)
                        or not isinstance(rollback_verified, bool)
                        or not isinstance(failure, dict)
                        or set(failure) != {"code", "message"}
                        or any(not isinstance(value, str) for value in failure.values())
                        or not isinstance(record.get("rollback_failures"), list)
                        or not isinstance(record.get("restored_state_hashes"), dict)
                    ):
                        raise Refusal("Failure terminal evidence is invalid.", "EVIDENCE_INVALID")
                    expected_restore = (
                        "not_required"
                        if not rollback_required
                        else "verified"
                        if rollback_verified
                        else "failed"
                    )
                    if (
                        restore["status"] != expected_restore
                        or (
                            rollback_required
                            and rollback_verified
                            and (
                                record["restored_state_hashes"]
                                not in valid_restore_hashes
                                or restore["state_hashes"]
                                != record["restored_state_hashes"]
                                or record["rollback_failures"]
                                or restore["failures"]
                            )
                        )
                    ):
                        raise Refusal("Failure restore evidence is invalid.", "EVIDENCE_INVALID")

        for linked_sequence in terminals:
            if linked_sequence not in intents:
                raise Refusal("Terminal evidence is orphaned.", "EVIDENCE_INVALID")
        unmatched: list[dict] = []
        for sequence, intent in intents.items():
            linked = terminals.get(sequence, [])
            if len(linked) > 1:
                raise Refusal("Replication intent has duplicate terminals.", "EVIDENCE_INVALID")
            if not linked:
                unmatched.append(intent)
        if unmatched:
            if (
                not allow_unmatched
                or len(unmatched) != 1
                or unmatched[0]["sequence"] != len(entries)
                or unmatched[0]["record"]["terminal_sequence"] != len(entries) + 1
            ):
                raise Refusal("Replication intent has no valid terminal.", "EVIDENCE_INVALID")
        return entries, unmatched

    def recovery_audit(self) -> tuple[list[dict], list[dict]]:
        return self._audit_transactions(allow_unmatched=True)

    def audit(self) -> list[dict]:
        entries, _ = self._audit_transactions(allow_unmatched=False)
        return entries

    @contextmanager
    def reserve(self, event_count: int, byte_count: int = 0) -> Iterator[None]:
        if not isinstance(event_count, int) or isinstance(event_count, bool) or event_count < 1:
            raise Refusal("Evidence reservation must be positive.", "INVALID_REQUEST")
        if not isinstance(byte_count, int) or isinstance(byte_count, bool) or byte_count < 0:
            raise Refusal("Evidence byte reservation is invalid.", "INVALID_REQUEST")
        with self.transaction_lock:
            sequence, _ = self._refresh_tail()
            if sequence + event_count > MAX_EVIDENCE_EVENTS:
                raise Refusal("Evidence event limit reached.", "LIMIT_EXCEEDED")
            if self._evidence_bytes() + byte_count > MAX_EVIDENCE_BYTES:
                raise Refusal("Evidence byte limit reached.", "LIMIT_EXCEEDED")
            yield

    def preflight_transaction(
        self,
        bundle_bytes: int,
        filename: str | None = None,
        digest: str | None = None,
    ) -> None:
        if (
            not isinstance(bundle_bytes, int)
            or isinstance(bundle_bytes, bool)
            or not 1 <= bundle_bytes <= MAX_SNAPSHOT_BUNDLE_BYTES
        ):
            raise Refusal("Snapshot bundle exceeds its byte limit.", "LIMIT_EXCEEDED")
        with self.transaction_lock:
            self._refresh_tail()
            additional = bundle_bytes
            if filename is not None or digest is not None:
                if (
                    not isinstance(filename, str)
                    or BUNDLE_NAME_RE.fullmatch(filename) is None
                    or not isinstance(digest, str)
                    or re.fullmatch(r"[0-9a-f]{64}", digest) is None
                ):
                    raise Refusal("Snapshot bundle preflight is invalid.", "INVALID_REQUEST")
                destination = self._snapshots_path / filename
                if destination.exists():
                    metadata = destination.lstat()
                    if (
                        not stat.S_ISREG(metadata.st_mode)
                        or metadata.st_size != bundle_bytes
                        or hashlib.sha256(destination.read_bytes()).hexdigest() != digest
                    ):
                        raise Refusal(
                            "Existing snapshot bundle is immutable.",
                            "EVIDENCE_INVALID",
                        )
                    additional = 0
            required = additional + (2 * MAX_EVIDENCE_RECORD_BYTES)
            if self._evidence_bytes() + required > MAX_EVIDENCE_BYTES:
                raise Refusal("Evidence byte limit reached.", "LIMIT_EXCEEDED")

    def _exact_append_is_durable(
        self,
        entry: dict,
        encoded: bytes,
        original_size: int,
        synchronized: bool,
    ) -> bool:
        if not synchronized:
            descriptor = -1
            try:
                descriptor = os.open(
                    self.path,
                    os.O_RDWR | getattr(os, "O_BINARY", 0),
                )
                os.fsync(descriptor)
                synchronized = True
            except OSError:
                return False
            finally:
                if descriptor >= 0:
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
        if not synchronized:
            return False
        try:
            sequence, event_hash = self._refresh_tail()
            if (
                sequence != entry["sequence"]
                or event_hash != entry["event_hash"]
                or self.path.stat().st_size != original_size + len(encoded)
            ):
                return False
            with self.path.open("rb") as handle:
                handle.seek(original_size)
                durable = handle.read(len(encoded) + 1)
            return durable == encoded and json.loads(durable) == entry
        except (OSError, Refusal, json.JSONDecodeError, UnicodeError):
            return False

    def append(self, record: dict) -> dict:
        if not isinstance(record, dict):
            raise Refusal("Evidence record must be an object.", "INVALID_REQUEST")
        with self.transaction_lock:
            current, previous = self._refresh_tail()
            sequence = current + 1
            if sequence > MAX_EVIDENCE_EVENTS:
                raise Refusal("Evidence event limit reached.", "LIMIT_EXCEEDED")
            unsigned = {"sequence": sequence, "previous_hash": previous, "record": record}
            entry = {**unsigned, "event_hash": _digest(unsigned)}
            encoded = _json_bytes(entry) + b"\n"
            if len(encoded) > MAX_EVIDENCE_RECORD_BYTES:
                raise Refusal("Evidence record exceeds its byte limit.", "LIMIT_EXCEEDED")
            if self._evidence_bytes() + len(encoded) > MAX_EVIDENCE_BYTES:
                raise Refusal("Evidence byte limit reached.", "LIMIT_EXCEEDED")
            metadata = self.path.lstat()
            if not stat.S_ISREG(metadata.st_mode):
                raise Refusal("Evidence file is unsafe.", "EVIDENCE_INVALID")
            enforce_private_mode(self.path, 0o600)
            metadata = self.path.lstat()
            if (
                not stat.S_ISREG(metadata.st_mode)
                or private_mode_mismatch(metadata.st_mode, 0o600)
            ):
                raise Refusal("Evidence file permissions are unsafe.", "EVIDENCE_INVALID")
            descriptor = os.open(
                self.path,
                os.O_WRONLY | os.O_APPEND | getattr(os, "O_BINARY", 0),
                0o600,
            )
            original_size = os.fstat(descriptor).st_size
            publication_attempted = False
            synchronized = False
            try:
                publication_attempted = True
                written = os.write(descriptor, encoded)
                if written != len(encoded):
                    raise OSError("Evidence append was incomplete.")
                os.fsync(descriptor)
                synchronized = True
            except Exception as error:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
                if publication_attempted and self._exact_append_is_durable(
                    entry,
                    encoded,
                    original_size,
                    synchronized,
                ):
                    self._sequence = sequence
                    self._previous = entry["event_hash"]
                    return entry
                raise Refusal(
                    "Evidence append outcome is not durably exact.",
                    "EVIDENCE_INVALID",
                ) from error
            try:
                os.close(descriptor)
            except OSError:
                pass
            self._sequence = sequence
            self._previous = entry["event_hash"]
            return entry

    def reserved_terminal(self, intent: dict) -> dict | None:
        if (
            not isinstance(intent, dict)
            or not isinstance(intent.get("sequence"), int)
            or not isinstance(intent.get("event_hash"), str)
            or not isinstance(intent.get("record"), dict)
            or intent["record"].get("type") != "replicated_chat_intent"
        ):
            raise Refusal("Reserved terminal intent is invalid.", "EVIDENCE_INVALID")
        with self.transaction_lock:
            entries, unmatched = self.recovery_audit()
            terminal_sequence = intent["record"].get("terminal_sequence")
            if unmatched:
                if unmatched != [intent] or entries[-1] != intent:
                    raise Refusal("Reserved terminal evidence is ambiguous.", "EVIDENCE_INVALID")
                return None
            if not isinstance(terminal_sequence, int) or terminal_sequence != intent["sequence"] + 1:
                raise Refusal("Reserved terminal sequence is invalid.", "EVIDENCE_INVALID")
            terminal = next(
                (entry for entry in entries if entry["sequence"] == terminal_sequence),
                None,
            )
            if (
                terminal is None
                or terminal["record"].get("type")
                not in {
                    "replicated_chat_commit",
                    "replicated_chat_failure",
                    "replicated_chat_recovery",
                }
                or terminal["record"].get("intent_sequence") != intent["sequence"]
                or terminal["record"].get("intent_event_hash") != intent["event_hash"]
                or entries[-1] != terminal
            ):
                raise Refusal("Reserved terminal evidence is invalid.", "EVIDENCE_INVALID")
            return terminal

    def write_snapshot_bundle(self, filename: str, bundle: dict) -> dict:
        if BUNDLE_NAME_RE.fullmatch(filename) is None:
            raise Refusal("Snapshot evidence filename is invalid.", "INVALID_REQUEST")
        encoded = _json_bytes(bundle)
        if not 1 <= len(encoded) <= MAX_SNAPSHOT_BUNDLE_BYTES:
            raise Refusal("Snapshot bundle exceeds its byte limit.", "LIMIT_EXCEEDED")
        reference = {
            "path": f"snapshots/{filename}",
            "sha256": hashlib.sha256(encoded).hexdigest(),
            "bytes": len(encoded),
        }
        with self.transaction_lock:
            current_bundle_bytes = self._read_bundle_bytes()
            destination = self._snapshots_path / filename
            if destination.exists():
                metadata = destination.lstat()
                if (
                    not stat.S_ISREG(metadata.st_mode)
                    or metadata.st_size != len(encoded)
                    or destination.read_bytes() != encoded
                ):
                    raise Refusal("Existing snapshot bundle is immutable.", "EVIDENCE_INVALID")
                _fsync_directory(self._snapshots_path)
                return reference
            if self.path.stat().st_size + current_bundle_bytes + len(encoded) > MAX_EVIDENCE_BYTES:
                raise Refusal("Evidence byte limit reached.", "LIMIT_EXCEEDED")

            temporary = self._snapshots_path / f".{filename}.{uuid.uuid4().hex}.tmp"
            flags = (
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_BINARY", 0)
            )
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            descriptor = -1
            published = False
            try:
                descriptor = os.open(temporary, flags, 0o600)
                offset = 0
                while offset < len(encoded):
                    written = os.write(descriptor, encoded[offset:])
                    if written <= 0:
                        raise OSError("Snapshot bundle write was incomplete.")
                    offset += written
                os.fsync(descriptor)
                enforce_private_mode(temporary, 0o600)
                os.close(descriptor)
                descriptor = -1
                os.link(temporary, destination, follow_symlinks=False)
                published = True
                temporary.unlink()
                _fsync_directory(self._snapshots_path)
                self._write_bundle_bytes(current_bundle_bytes + len(encoded))
            except Exception:
                if descriptor >= 0:
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
                try:
                    temporary.unlink()
                except OSError:
                    pass
                if published:
                    try:
                        destination.unlink()
                        _fsync_directory(self._snapshots_path)
                    except OSError:
                        pass
                raise
            return reference

    def read_snapshot_bundle(self, reference: object) -> dict:
        if not isinstance(reference, dict) or set(reference) != {"path", "sha256", "bytes"}:
            raise Refusal("Snapshot bundle reference is invalid.", "EVIDENCE_INVALID")
        relative = reference["path"]
        digest = reference["sha256"]
        byte_count = reference["bytes"]
        if (
            not isinstance(relative, str)
            or re.fullmatch(r"snapshots/intent-[1-9][0-9]*\.json", relative) is None
            or not isinstance(digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", digest) is None
            or not isinstance(byte_count, int)
            or isinstance(byte_count, bool)
            or byte_count < 1
        ):
            raise Refusal("Snapshot bundle reference is invalid.", "EVIDENCE_INVALID")
        with self.transaction_lock:
            root = self.path.parent.resolve()
            candidate = root / relative
            try:
                resolved = candidate.resolve(strict=True)
            except OSError:
                raise Refusal("Snapshot bundle is missing.", "EVIDENCE_INVALID") from None
            if not resolved.is_relative_to(root) or candidate != resolved or not resolved.is_file():
                raise Refusal("Snapshot bundle path escapes the evidence root.", "EVIDENCE_INVALID")
            encoded = resolved.read_bytes()
            if len(encoded) != byte_count or hashlib.sha256(encoded).hexdigest() != digest:
                raise Refusal("Snapshot bundle digest is invalid.", "EVIDENCE_INVALID")
            try:
                bundle = json.loads(encoded)
            except (json.JSONDecodeError, UnicodeError):
                raise Refusal("Snapshot bundle is invalid JSON.", "EVIDENCE_INVALID") from None
            if (
                not isinstance(bundle, dict)
                or set(bundle) != {"pre_snapshots", "pre_state_hashes"}
                or not isinstance(bundle["pre_snapshots"], dict)
                or not isinstance(bundle["pre_state_hashes"], dict)
                or set(bundle["pre_snapshots"]) != set(bundle["pre_state_hashes"])
                or any(
                    not isinstance(node_id, str) or NODE_RE.fullmatch(node_id) is None
                    for node_id in bundle["pre_snapshots"]
                )
            ):
                raise Refusal("Snapshot bundle schema is invalid.", "EVIDENCE_INVALID")
            for node_id, snapshot in bundle["pre_snapshots"].items():
                validated = AtomicStore.validate_snapshot(copy.deepcopy(snapshot))
                if bundle["pre_state_hashes"].get(node_id) != _digest(snapshot):
                    raise Refusal("Snapshot bundle state hash is invalid.", "EVIDENCE_INVALID")
                bundle["pre_snapshots"][node_id] = validated
            return bundle


class NodeProcess:
    def __init__(self, node_id: str, root: Path) -> None:
        self.node_id = node_id
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        enforce_private_mode(self.root, 0o700)
        environment = {
            key: value
            for key, value in os.environ.items()
            if key in {"HOME", "PATH", "PYTHONPATH", "VIRTUAL_ENV", "SYSTEMROOT"}
        }
        environment["PYTHONUTF8"] = "1"
        self._lock = threading.Lock()
        self._process = subprocess.Popen(
            [sys.executable, "-m", "rapp_virtual_as400.node_worker", "--root", str(self.root)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            env=environment,
        )
        try:
            ready = self.request(
                {"protocol": "RAPP/1", "kind": "control", "operation": "snapshot"}
            )
            if (
                ready.get("protocol") != "RAPP/1"
                or ready.get("control") != "snapshot"
                or ready.get("status") != "ok"
            ):
                raise Refusal(f"Node {self.node_id} did not become ready.", "NODE_UNAVAILABLE")
        except Exception:
            self.close()
            raise

    @property
    def pid(self) -> int:
        return self._process.pid

    def request(self, message: dict) -> dict:
        encoded = json.dumps(message, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        is_restore = message.get("kind") == "control" and message.get("operation") == "restore"
        limit = MAX_RESTORE_MESSAGE_BYTES if is_restore else 8192
        if len(encoded.encode("utf-8")) > limit:
            raise Refusal("Node request exceeds its bounded message limit.", "LIMIT_EXCEEDED")
        with self._lock:
            if self._process.poll() is not None or not self._process.stdin or not self._process.stdout:
                raise Refusal(f"Node {self.node_id} is not running.", "NODE_UNAVAILABLE")
            self._process.stdin.write(encoded + "\n")
            self._process.stdin.flush()
            line = self._process.stdout.readline()
        if not line:
            raise Refusal(f"Node {self.node_id} returned no typed response.", "NODE_UNAVAILABLE")
        response = json.loads(line)
        if not isinstance(response, dict):
            raise Refusal(f"Node {self.node_id} returned an invalid response.", "NODE_UNAVAILABLE")
        return response

    def close(self) -> None:
        failure: Exception | None = None
        if self._process.poll() is None:
            try:
                self.request({"protocol": "RAPP/1", "kind": "control", "operation": "stop"})
                self._process.wait(timeout=PROCESS_CLOSE_TIMEOUT_SECONDS)
            except Exception as error:
                failure = error
                if self._process.poll() is None:
                    try:
                        self._process.terminate()
                        self._process.wait(timeout=PROCESS_CLOSE_TIMEOUT_SECONDS)
                    except subprocess.TimeoutExpired:
                        self._process.kill()
                        self._process.wait(timeout=PROCESS_CLOSE_TIMEOUT_SECONDS)
                    except Exception as terminate_error:
                        failure = terminate_error
        for stream in (self._process.stdin, self._process.stdout, self._process.stderr):
            if stream:
                try:
                    stream.close()
                except OSError as error:
                    failure = failure or error
        if self._process.poll() is None:
            raise Refusal(f"Node {self.node_id} could not be stopped.", "NODE_UNAVAILABLE")
        if failure is not None and not isinstance(failure, (Refusal, subprocess.TimeoutExpired)):
            raise Refusal(
                f"Node {self.node_id} close was not clean ({type(failure).__name__}).",
                "NODE_UNAVAILABLE",
            ) from failure


class PrivateVNetNeighborhood:
    """Local simulation of a private-vNet trust topology, never a LAN listener."""

    def __init__(self, root: str | Path, node_ids: Iterable[str] = ("AS400-A", "AS400-B")) -> None:
        ids = tuple(node_ids)
        if not 2 <= len(ids) <= MAX_NODES or len(set(ids)) != len(ids):
            raise Refusal("A neighborhood requires 2 through 8 unique nodes.", "INVALID_TOPOLOGY")
        if any(not isinstance(node, str) or not NODE_RE.fullmatch(node) for node in ids):
            raise Refusal("Node IDs must use bounded uppercase provider-neutral names.", "INVALID_TOPOLOGY")
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        enforce_private_mode(self.root, 0o700)
        self._root_lock = root_lock(self.root)
        self._replication_lock = threading.RLock()
        self._operable = False
        self.nodes: dict[str, NodeProcess] = {}
        try:
            with self._root_lock:
                self.ledger = EvidenceLedger(
                    self.root / "evidence" / "events.jsonl",
                    transaction_lock=self._root_lock,
                )
                self.nodes = {
                    node: NodeProcess(node, self.root / "nodes" / node) for node in ids
                }
                self._recover_unmatched_intent()
                self._operable = True
        except BaseException:
            self.close()
            raise

    def __enter__(self) -> "PrivateVNetNeighborhood":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        self._operable = False
        for node in self.nodes.values():
            node.close()

    def _ensure_operable(self) -> None:
        if not self._operable:
            raise Refusal(
                "Neighborhood is closed pending proven durable recovery.",
                "RECOVERY_REQUIRED",
            )

    def _recover_unmatched_intent(self) -> None:
        entries, unmatched = self.ledger.recovery_audit()
        for entry in entries:
            record = entry["record"]
            if (
                record.get("type") == "replicated_chat_failure"
                and record.get("restore", {}).get("status") == "failed"
            ):
                raise Refusal(
                    "Evidence records an unverified rollback.",
                    "RECOVERY_FAILED",
                )
        if not unmatched:
            return
        intent = unmatched[0]
        record = intent["record"]
        if set(record["nodes"]) != set(self.nodes):
            raise Refusal(
                "Recovery topology does not match the durable intent.",
                "RECOVERY_FAILED",
            )
        bundle = self.ledger.read_snapshot_bundle(record["snapshot_bundle"])
        snapshots = bundle["pre_snapshots"]
        evidence_hashes = bundle["pre_state_hashes"]
        expected_hashes = {
            node_id: _digest(snapshot) for node_id, snapshot in snapshots.items()
        }
        if (
            set(snapshots) != set(self.nodes)
            or evidence_hashes != record["pre_state_hashes"]
            or len(set(evidence_hashes.values())) != 1
            or len(set(expected_hashes.values())) != 1
        ):
            raise Refusal(
                "Recovery snapshot evidence cannot prove convergence.",
                "RECOVERY_FAILED",
            )
        restored_hashes, failures = self._restore_and_verify(snapshots)
        if failures or restored_hashes != expected_hashes or len(set(restored_hashes.values())) != 1:
            raise Refusal(
                "Durable intent recovery could not restore every node exactly.",
                "RECOVERY_FAILED",
            )
        recovery_record = {
            "type": "replicated_chat_recovery",
            "intent_sequence": intent["sequence"],
            "intent_event_hash": intent["event_hash"],
            "message": record["message"],
            "snapshot_bundle": record["snapshot_bundle"],
            "pre_state_hashes": evidence_hashes,
            "restored_state_hashes": restored_hashes,
            "rollback_verified": True,
            "rollback_failures": [],
            "restore": {
                "status": "verified",
                "state_hashes": restored_hashes,
                "failures": [],
            },
            "converged": True,
        }
        try:
            terminal = self.ledger.append(recovery_record)
        except Exception as error:
            try:
                terminal = self.ledger.reserved_terminal(intent)
            except Exception as verification_error:
                raise Refusal(
                    "Recovery terminal evidence is ambiguous or invalid.",
                    "RECOVERY_FAILED",
                ) from verification_error
            if terminal is None or terminal["record"] != recovery_record:
                raise Refusal(
                    f"Recovery terminal evidence append failed ({type(error).__name__}).",
                    "RECOVERY_FAILED",
                ) from error
        if terminal["sequence"] != record["terminal_sequence"]:
            raise Refusal("Recovery did not use its reserved terminal slot.", "RECOVERY_FAILED")
        self.ledger.audit()

    @staticmethod
    def _event_time(sequence: int) -> str:
        return (datetime(2000, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=sequence)).isoformat()

    def topology(self) -> dict:
        self._ensure_operable()
        return {
            "schema": "rapp.private-vnet/v1",
            "provider": "provider-neutral",
            "network_exposure": "none",
            "transport": "parent-child-stdio",
            "loopback_http_optional": True,
            "lan_listener": False,
            "privileged_sibling_route": False,
            "node_count": len(self.nodes),
            "nodes": [
                {"node_id": node.node_id, "pid": node.pid, "state_root": str(node.root)}
                for node in self.nodes.values()
            ],
        }

    def chat(
        self,
        node_id: str,
        user_input: str,
        session_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict:
        user_input = canonical_json_strings(user_input)  # type: ignore[assignment]
        session_id = canonical_json_strings(session_id)  # type: ignore[assignment]
        idempotency_key = canonical_json_strings(idempotency_key)  # type: ignore[assignment]
        try:
            node = self.nodes[node_id]
        except KeyError:
            raise Refusal(f"Node {node_id} is not in this neighborhood.", "OBJECT_NOT_FOUND") from None
        with self._replication_lock, self._root_lock:
            self._ensure_operable()
            return node.request(
                {
                    "protocol": "RAPP/1",
                    "kind": "chat",
                    "user_input": user_input,
                    "session_id": session_id,
                    "idempotency_key": idempotency_key,
                }
            )

    @staticmethod
    def _checked_response(node_id: str, response: dict, control: str | None = None) -> dict:
        error = response.get("error")
        if isinstance(error, dict):
            message = error.get("message")
            code = error.get("code")
            raise Refusal(
                f"Node {node_id} failed: {message if isinstance(message, str) else 'unknown refusal'}.",
                code if isinstance(code, str) else "NODE_FAILED",
            )
        if control is not None and (
            response.get("protocol") != "RAPP/1"
            or response.get("control") != control
            or response.get("status") != "ok"
        ):
            raise Refusal(f"Node {node_id} returned an invalid {control} response.", "NODE_FAILED")
        return response

    def _snapshots(self) -> dict[str, dict]:
        snapshots: dict[str, dict] = {}
        for node_id, node in self.nodes.items():
            response = self._checked_response(
                node_id,
                node.request({"protocol": "RAPP/1", "kind": "control", "operation": "snapshot"}),
                "snapshot",
            )
            snapshots[node_id] = AtomicStore.validate_snapshot(response.get("state"))
        return snapshots

    def _restore_and_verify(self, snapshots: dict[str, dict]) -> tuple[dict[str, str], list[str]]:
        expected_hashes = {node_id: _digest(state) for node_id, state in snapshots.items()}
        failures: list[str] = []
        for node_id, node in self.nodes.items():
            try:
                response = self._checked_response(
                    node_id,
                    node.request(
                        {
                            "protocol": "RAPP/1",
                            "kind": "control",
                            "operation": "restore",
                            "state": snapshots[node_id],
                        }
                    ),
                    "restore",
                )
                if response.get("state_hash") != expected_hashes[node_id]:
                    failures.append(f"{node_id}: restore acknowledgement hash diverged")
            except Exception as error:
                failures.append(f"{node_id}: restore failed ({type(error).__name__})")

        restored_hashes: dict[str, str] = {}
        for node_id, node in self.nodes.items():
            try:
                response = self._checked_response(
                    node_id,
                    node.request({"protocol": "RAPP/1", "kind": "control", "operation": "snapshot"}),
                    "snapshot",
                )
                restored = AtomicStore.validate_snapshot(response.get("state"))
                restored_hashes[node_id] = _digest(restored)
                if restored != snapshots[node_id] or restored_hashes[node_id] != expected_hashes[node_id]:
                    failures.append(f"{node_id}: restored snapshot hash diverged")
            except Exception as error:
                failures.append(f"{node_id}: restore verification failed ({type(error).__name__})")
        return restored_hashes, failures

    def _live_node_fingerprint(self) -> dict[str, dict[str, tuple[int, str]]]:
        fingerprints: dict[str, dict[str, tuple[int, str]]] = {}
        for node_id, node in self.nodes.items():
            files: dict[str, tuple[int, str]] = {}
            entries = list(node.root.iterdir())
            if len(entries) > MAX_REPLAY_ROOT_ENTRIES:
                raise Refusal("Live node root exceeds its replay audit bound.", "REPLAY_UNSAFE")
            for child in sorted(entries, key=lambda item: item.name):
                metadata = child.lstat()
                if not stat.S_ISREG(metadata.st_mode):
                    raise Refusal("Live node state contains an unsafe entry.", "REPLAY_UNSAFE")
                encoded = child.read_bytes()
                files[child.name] = (len(encoded), hashlib.sha256(encoded).hexdigest())
            fingerprints[node_id] = files
        return fingerprints

    def _evidence_fingerprint(self) -> tuple[tuple[str, int, str], ...]:
        paths = [self.ledger.path, self.ledger._bundle_bytes_path]
        paths.extend(sorted(self.ledger._snapshots_path.iterdir(), key=lambda item: item.name))
        fingerprint: list[tuple[str, int, str]] = []
        for path in paths:
            metadata = path.lstat()
            if not stat.S_ISREG(metadata.st_mode):
                raise Refusal("Replay evidence contains an unsafe entry.", "EVIDENCE_INVALID")
            encoded = path.read_bytes()
            fingerprint.append(
                (
                    path.relative_to(self.ledger.path.parent).as_posix(),
                    len(encoded),
                    hashlib.sha256(encoded).hexdigest(),
                )
            )
        return tuple(fingerprint)

    def _create_disposable_replay_root(self) -> Path:
        stale = [
            child
            for child in self.root.iterdir()
            if child.name.startswith(".replay-")
        ]
        if stale:
            raise Refusal(
                "A prior disposable replay root was not proven erased.",
                "REPLAY_CLEANUP_REQUIRED",
            )
        for _ in range(MAX_REPLAY_ROOT_ENTRIES):
            candidate = self.root / f".replay-{uuid.uuid4().hex}"
            try:
                candidate.mkdir(mode=0o700)
            except FileExistsError:
                continue
            try:
                enforce_private_mode(candidate, 0o700)
                metadata = candidate.lstat()
                if (
                    candidate.parent != self.root
                    or not stat.S_ISDIR(metadata.st_mode)
                    or private_mode_mismatch(metadata.st_mode, 0o700)
                ):
                    raise Refusal("Disposable replay root is unsafe.", "REPLAY_UNSAFE")
                _fsync_directory(self.root)
            except Exception as error:
                try:
                    cleanup_metadata = candidate.lstat()
                    if stat.S_ISDIR(cleanup_metadata.st_mode) and not candidate.is_symlink():
                        if any(candidate.iterdir()):
                            raise OSError("replay setup root was not empty")
                        candidate.rmdir()
                    else:
                        candidate.unlink()
                    _fsync_directory(self.root)
                except Exception as cleanup_error:
                    raise Refusal(
                        "Disposable replay setup cleanup was not proven.",
                        "REPLAY_CLEANUP_FAILED",
                    ) from cleanup_error
                if isinstance(error, Refusal):
                    raise
                raise Refusal(
                    "Disposable replay root publication was not proven.",
                    "REPLAY_UNSAFE",
                ) from error
            return candidate
        raise Refusal("Could not allocate a unique disposable replay root.", "REPLAY_UNSAFE")

    def _erase_disposable_replay_root(self, replay_root: Path) -> None:
        if (
            replay_root.parent != self.root
            or REPLAY_ROOT_RE.fullmatch(replay_root.name) is None
            or replay_root.is_symlink()
        ):
            raise Refusal("Disposable replay cleanup path is unsafe.", "REPLAY_CLEANUP_FAILED")
        entries = list(replay_root.iterdir())
        if len(entries) > MAX_REPLAY_ROOT_ENTRIES:
            raise Refusal("Disposable replay cleanup exceeds its bound.", "REPLAY_CLEANUP_FAILED")
        for child in entries:
            metadata = child.lstat()
            if not stat.S_ISREG(metadata.st_mode):
                raise Refusal("Disposable replay cleanup found an unsafe entry.", "REPLAY_CLEANUP_FAILED")
            child.unlink()
        replay_root.rmdir()
        _fsync_directory(self.root)

    @staticmethod
    def _failure_details(error: Exception) -> dict[str, str]:
        if isinstance(error, Refusal):
            return {"code": error.code, "message": error.message}
        return {"code": "NODE_FAILED", "message": f"{type(error).__name__}: {error}"}

    def replicate_chat(
        self,
        user_input: str,
        session_id: str = "replicated",
        idempotency_key: str | None = None,
    ) -> dict:
        user_input = canonical_json_strings(user_input)  # type: ignore[assignment]
        session_id = canonical_json_strings(session_id)  # type: ignore[assignment]
        idempotency_key = canonical_json_strings(idempotency_key)  # type: ignore[assignment]
        if (
            not isinstance(user_input, str)
            or not isinstance(session_id, str)
            or (idempotency_key is not None and not isinstance(idempotency_key, str))
        ):
            raise Refusal("Replicated chat fields must be strings.", "INVALID_REQUEST")
        with self._replication_lock, self.ledger.reserve(2):
            self._ensure_operable()
            sequence = self.ledger.next_sequence()
            event_at = self._event_time(sequence)
            key = idempotency_key or f"replicated-{sequence}"
            message = {
                "protocol": "RAPP/1",
                "kind": "chat",
                "user_input": user_input,
                "session_id": session_id,
                "idempotency_key": key,
                "event_at": event_at,
            }
            snapshot_file = f"intent-{sequence}.json"
            try:
                pre_snapshots = self._snapshots()
                pre_state_hashes = {
                    node_id: _digest(state) for node_id, state in pre_snapshots.items()
                }
                if len(set(pre_state_hashes.values())) != 1:
                    raise Refusal(
                        "Replicated node pre-states diverged.",
                        "REPLICATION_DIVERGED",
                    )
                bundle = {
                    "pre_snapshots": pre_snapshots,
                    "pre_state_hashes": pre_state_hashes,
                }
                bundle_bytes = _json_bytes(bundle)
                bundle_reference = {
                    "path": f"snapshots/{snapshot_file}",
                    "sha256": hashlib.sha256(bundle_bytes).hexdigest(),
                    "bytes": len(bundle_bytes),
                }
                self.ledger.preflight_transaction(
                    len(bundle_bytes),
                    snapshot_file,
                    bundle_reference["sha256"],
                )
                written_reference = self.ledger.write_snapshot_bundle(snapshot_file, bundle)
                if written_reference != bundle_reference:
                    raise Refusal("Snapshot bundle evidence diverged.", "EVIDENCE_IO_FAILED")
            except Refusal:
                raise
            except Exception as error:
                raise Refusal(
                    f"Snapshot bundle publication failed ({type(error).__name__}).",
                    "EVIDENCE_IO_FAILED",
                ) from error

            intent_record = {
                "type": "replicated_chat_intent",
                "message": message,
                "nodes": list(self.nodes),
                "snapshot_bundle_path": bundle_reference["path"],
                "snapshot_bundle": bundle_reference,
                "pre_state_hashes": pre_state_hashes,
                "terminal_sequence": sequence + 1,
            }
            self._operable = False
            try:
                intent = self.ledger.append(intent_record)
            except Exception as error:
                try:
                    entries, unmatched = self.ledger.recovery_audit()
                except Exception as verification_error:
                    raise Refusal(
                        "Intent evidence is ambiguous or invalid.",
                        "RECOVERY_REQUIRED",
                    ) from verification_error
                if (
                    len(unmatched) == 1
                    and unmatched[0]["sequence"] == sequence
                    and unmatched[0]["record"] == intent_record
                ):
                    intent = unmatched[0]
                elif not unmatched and all(entry["sequence"] < sequence for entry in entries):
                    self._operable = True
                    raise Refusal(
                        f"Intent evidence append failed ({type(error).__name__}).",
                        "EVIDENCE_IO_FAILED",
                    ) from error
                else:
                    raise Refusal(
                        "Intent evidence outcome could not be proven.",
                        "RECOVERY_REQUIRED",
                    ) from error
            intent_link = {
                "intent_sequence": intent["sequence"],
                "intent_event_hash": intent["event_hash"],
            }
            mutation_started = False
            results: dict[str, dict] = {}
            state_hashes: dict[str, str] = {}
            entry: dict | None = None
            operation_error: Exception | None = None
            try:
                mutation_started = True
                for node_id, node in self.nodes.items():
                    results[node_id] = self._checked_response(node_id, node.request(message))
                if len({_digest(result) for result in results.values()}) != 1:
                    raise Refusal("Replicated chat results diverged.", "REPLICATION_DIVERGED")
                post_snapshots = self._snapshots()
                state_hashes = {
                    node_id: _digest(state) for node_id, state in post_snapshots.items()
                }
                if len(set(state_hashes.values())) != 1:
                    raise Refusal("Replicated node states diverged.", "REPLICATION_DIVERGED")
            except Exception as error:
                operation_error = error

            commit_record: dict | None = None
            if operation_error is None:
                commit_record = {
                    "type": "replicated_chat_commit",
                    **intent_link,
                    "message": message,
                    "snapshot_bundle": bundle_reference,
                    "pre_state_hashes": pre_state_hashes,
                    "results": results,
                    "state_hashes": state_hashes,
                    "restore": {
                        "status": "not_required",
                        "state_hashes": {},
                        "failures": [],
                    },
                    "converged": True,
                }
                try:
                    entry = self.ledger.append(commit_record)
                except Exception as error:
                    try:
                        occupied = self.ledger.reserved_terminal(intent)
                    except Exception as verification_error:
                        self._operable = False
                        raise Refusal(
                            "Terminal evidence is ambiguous or invalid.",
                            "RECOVERY_REQUIRED",
                        ) from verification_error
                    if occupied is not None:
                        if occupied["record"] != commit_record:
                            self._operable = False
                            raise Refusal(
                                "Reserved terminal slot was consumed by a different outcome.",
                                "RECOVERY_REQUIRED",
                            ) from error
                        entry = occupied
                    else:
                        operation_error = Refusal(
                            f"Terminal evidence append failed ({type(error).__name__}).",
                            "EVIDENCE_IO_FAILED",
                        )

            if operation_error is not None:
                try:
                    occupied = self.ledger.reserved_terminal(intent)
                except Exception as verification_error:
                    self._operable = False
                    raise Refusal(
                        "Terminal evidence is ambiguous or invalid.",
                        "RECOVERY_REQUIRED",
                    ) from verification_error
                if occupied is not None:
                    if commit_record is not None and occupied["record"] == commit_record:
                        entry = occupied
                        operation_error = None
                    else:
                        self._operable = False
                        raise Refusal(
                            "Reserved terminal slot was already consumed.",
                            "RECOVERY_REQUIRED",
                        ) from operation_error

            if operation_error is not None:
                restored_hashes: dict[str, str] = {}
                rollback_failures: list[str] = []
                if mutation_started and len(pre_snapshots) == len(self.nodes):
                    restored_hashes, rollback_failures = self._restore_and_verify(pre_snapshots)
                failure_record = {
                    "type": "replicated_chat_failure",
                    **intent_link,
                    "message": message,
                    "failure": self._failure_details(operation_error),
                    "snapshot_bundle": bundle_reference,
                    "pre_state_hashes": pre_state_hashes,
                    "rollback_required": mutation_started,
                    "restored_state_hashes": restored_hashes,
                    "rollback_verified": not mutation_started or not rollback_failures,
                    "rollback_failures": rollback_failures,
                    "restore": {
                        "status": (
                            "not_required"
                            if not mutation_started
                            else "verified"
                            if not rollback_failures
                            else "failed"
                        ),
                        "state_hashes": restored_hashes,
                        "failures": rollback_failures,
                    },
                }
                terminal_recorded = False
                try:
                    self.ledger.append(failure_record)
                    terminal_recorded = True
                except Exception as terminal_error:
                    try:
                        occupied = self.ledger.reserved_terminal(intent)
                    except Exception as verification_error:
                        self._operable = False
                        raise Refusal(
                            "Failure terminal evidence is ambiguous or invalid.",
                            "RECOVERY_REQUIRED",
                        ) from verification_error
                    if occupied is not None and occupied["record"] == failure_record:
                        terminal_recorded = True
                    elif occupied is not None:
                        self._operable = False
                        raise Refusal(
                            "Reserved terminal slot was consumed by a different outcome.",
                            "RECOVERY_REQUIRED",
                        ) from terminal_error
                if rollback_failures:
                    self._operable = False
                    raise Refusal(
                        "Replicated chat rollback could not be verified for every node.",
                        "ROLLBACK_FAILED",
                    ) from operation_error
                if not terminal_recorded:
                    self._operable = False
                    raise Refusal(
                        "Replicated chat is closed pending durable recovery.",
                        "RECOVERY_REQUIRED",
                    ) from operation_error
                try:
                    self.ledger.audit()
                except Exception as audit_error:
                    self._operable = False
                    raise Refusal(
                        "Failure terminal evidence did not pass audit.",
                        "RECOVERY_REQUIRED",
                    ) from audit_error
                self._operable = True
                if isinstance(operation_error, Refusal):
                    raise operation_error
                raise Refusal(
                    f"Replicated chat failed ({type(operation_error).__name__}).",
                    "NODE_FAILED",
                ) from operation_error

            if entry is None:
                self._operable = False
                raise Refusal("Commit evidence is missing.", "RECOVERY_REQUIRED")
            try:
                self.ledger.audit()
            except Exception as audit_error:
                self._operable = False
                raise Refusal(
                    "Commit evidence did not pass audit.",
                    "RECOVERY_REQUIRED",
                ) from audit_error
            self._operable = True
            return {
                "protocol": "RAPP/1",
                "control": "replicate_chat",
                "chat_result": next(iter(results.values())),
                "nodes": list(self.nodes),
                "converged": True,
                "state_hash": next(iter(state_hashes.values())),
                "evidence": {
                    "intent_sequence": intent["sequence"],
                    "intent_event_hash": intent["event_hash"],
                    "sequence": entry["sequence"],
                    "event_hash": entry["event_hash"],
                },
            }

    def replay_and_verify(self, node_id: str) -> dict:
        with self._replication_lock, self._root_lock:
            self._ensure_operable()
            if node_id not in self.nodes:
                raise Refusal(f"Node {node_id} is not in this neighborhood.", "OBJECT_NOT_FOUND")
            entries = self.ledger.audit()
            live_snapshots = self._snapshots()
            live_state_hashes = {
                name: _digest(snapshot) for name, snapshot in live_snapshots.items()
            }
            if (
                len(set(live_state_hashes.values())) != 1
                or any(snapshot != live_snapshots[node_id] for snapshot in live_snapshots.values())
            ):
                raise Refusal("Live nodes are not converged for replay.", "REPLAY_DIVERGED")
            live_fingerprint = self._live_node_fingerprint()
            evidence_fingerprint = self._evidence_fingerprint()
            commits = [
                entry for entry in entries
                if entry["record"].get("type") == "replicated_chat_commit"
            ]
            replay_root: Path | None = None
            disposable: NodeProcess | None = None
            result: dict | None = None
            replay_error: BaseException | None = None
            cleanup_errors: list[str] = []
            try:
                replay_root = self._create_disposable_replay_root()
                disposable = NodeProcess(
                    f"REPLAY-{uuid.uuid4().hex[:12].upper()}",
                    replay_root,
                )
                replayed = 0
                if commits:
                    first_record = commits[0]["record"]
                    first_bundle = self.ledger.read_snapshot_bundle(
                        first_record["snapshot_bundle"]
                    )
                    initial = first_bundle["pre_snapshots"][node_id]
                    restore = self._checked_response(
                        disposable.node_id,
                        disposable.request(
                            {
                                "protocol": "RAPP/1",
                                "kind": "control",
                                "operation": "restore",
                                "state": initial,
                            }
                        ),
                        "restore",
                    )
                    if restore.get("state_hash") != _digest(initial):
                        raise Refusal(
                            "Disposable replay restore hash diverged.",
                            "REPLAY_DIVERGED",
                        )
                for entry in commits:
                    record = entry["record"]
                    bundle = self.ledger.read_snapshot_bundle(record["snapshot_bundle"])
                    expected_pre = bundle["pre_snapshots"][node_id]
                    pre_response = self._checked_response(
                        disposable.node_id,
                        disposable.request(
                            {"protocol": "RAPP/1", "kind": "control", "operation": "snapshot"}
                        ),
                        "snapshot",
                    )
                    actual_pre = AtomicStore.validate_snapshot(pre_response.get("state"))
                    if actual_pre != expected_pre:
                        raise Refusal(
                            "Replay pre-state diverged from append-only evidence.",
                            "REPLAY_DIVERGED",
                        )
                    expected_results = list(record["results"].values())
                    if any(expected != expected_results[0] for expected in expected_results[1:]):
                        raise Refusal(
                            "Recorded replay results are not converged.",
                            "EVIDENCE_INVALID",
                        )
                    actual_result = self._checked_response(
                        disposable.node_id,
                        disposable.request(record["message"]),
                    )
                    if any(actual_result != expected for expected in expected_results):
                        raise Refusal(
                            "Replay result diverged from append-only evidence.",
                            "REPLAY_DIVERGED",
                        )
                    post_response = self._checked_response(
                        disposable.node_id,
                        disposable.request(
                            {"protocol": "RAPP/1", "kind": "control", "operation": "snapshot"}
                        ),
                        "snapshot",
                    )
                    post_state = AtomicStore.validate_snapshot(post_response.get("state"))
                    if any(
                        _digest(post_state) != expected_hash
                        for expected_hash in record["state_hashes"].values()
                    ):
                        raise Refusal(
                            "Replay event state diverged from append-only evidence.",
                            "REPLAY_DIVERGED",
                        )
                    replayed += 1
                final_response = self._checked_response(
                    disposable.node_id,
                    disposable.request(
                        {"protocol": "RAPP/1", "kind": "control", "operation": "snapshot"}
                    ),
                    "snapshot",
                )
                final_state = AtomicStore.validate_snapshot(final_response.get("state"))
                if (
                    final_state != live_snapshots[node_id]
                    or _digest(final_state) != live_state_hashes[node_id]
                ):
                    raise Refusal(
                        "Disposable replay did not converge with live state.",
                        "REPLAY_DIVERGED",
                    )
                result = {
                    "protocol": "RAPP/1",
                    "control": "replay",
                    "node_id": node_id,
                    "events_replayed": replayed,
                    "converged": True,
                    "state_hash": live_state_hashes[node_id],
                }
            except BaseException as error:
                replay_error = error
            finally:
                if disposable is not None:
                    try:
                        disposable.close()
                    except Exception as error:
                        cleanup_errors.append(f"close failed ({type(error).__name__})")
                if replay_root is not None:
                    if disposable is None:
                        cleanup_errors.append("disposable setup status was not proven")
                    elif disposable._process.poll() is not None:
                        try:
                            self._erase_disposable_replay_root(replay_root)
                        except Exception as error:
                            cleanup_errors.append(f"erase failed ({type(error).__name__})")
                    else:
                        cleanup_errors.append("disposable process remained running")

            try:
                current_snapshots = self._snapshots()
                if (
                    current_snapshots != live_snapshots
                    or self._live_node_fingerprint() != live_fingerprint
                    or self._evidence_fingerprint() != evidence_fingerprint
                ):
                    raise Refusal(
                        "Replay isolation invariant could not be verified.",
                        "REPLAY_ISOLATION_FAILED",
                    )
            except Exception as error:
                if isinstance(error, Refusal) and error.code == "REPLAY_ISOLATION_FAILED":
                    raise
                raise Refusal(
                    "Replay isolation invariant could not be verified.",
                    "REPLAY_ISOLATION_FAILED",
                ) from error
            if cleanup_errors:
                raise Refusal(
                    "Disposable replay cleanup was not proven: " + "; ".join(cleanup_errors) + ".",
                    "REPLAY_CLEANUP_FAILED",
                ) from replay_error
            if replay_error is not None:
                raise replay_error
            if result is None:
                raise Refusal("Disposable replay produced no result.", "REPLAY_DIVERGED")
            return result

    def run_replicated_job(
        self,
        job: dict,
        *,
        replicas: int = MAX_REPLICAS,
        mode: str = "deterministic",
        quorum: int | None = None,
    ) -> dict:
        job = canonical_json_strings(job)  # type: ignore[assignment]
        if not isinstance(job, dict) or set(job) != {"name", "payload"}:
            raise Refusal("Job must contain exactly name and payload.", "INVALID_REQUEST")
        if not isinstance(job["name"], str) or not NODE_RE.fullmatch(job["name"]):
            raise Refusal("Job name must use a bounded uppercase name.", "INVALID_REQUEST")
        try:
            job_bytes = _json_bytes(job)
        except (TypeError, ValueError, UnicodeError):
            raise Refusal("Job payload must contain bounded JSON values.", "INVALID_REQUEST") from None
        if len(job_bytes) > MAX_JOB_BYTES:
            raise Refusal("Job exceeds 2048 bytes.", "LIMIT_EXCEEDED")
        if not isinstance(replicas, int) or isinstance(replicas, bool) or not 1 <= replicas <= MAX_REPLICAS:
            raise Refusal("Replicas must be an integer from 1 through 100.", "LIMIT_EXCEEDED")
        if mode == "deterministic":
            if quorum not in {None, replicas}:
                raise Refusal("Deterministic runs require all replicas.", "INVALID_QUORUM")
            quorum = replicas
        elif mode == "stochastic":
            if not isinstance(quorum, int) or isinstance(quorum, bool) or not 1 <= quorum <= replicas:
                raise Refusal("Stochastic runs require an exact predeclared quorum.", "INVALID_QUORUM")
        else:
            raise Refusal("Mode must be deterministic or stochastic.", "INVALID_REQUEST")

        with self._replication_lock, self.ledger.reserve(1, MAX_EVIDENCE_RECORD_BYTES):
            self._ensure_operable()
            attempts: list[dict] = []
            node_items = list(self.nodes.items())
            for replica in range(replicas):
                node_id, node = node_items[replica % len(node_items)]
                expected = mode == "deterministic" or replica < quorum
                response = node.request(
                    {
                        "protocol": "RAPP/1",
                        "kind": "control",
                        "operation": "simulate",
                        "job": job,
                        "replica": replica,
                        "mode": mode,
                        "expected": expected,
                    }
                )
                attempts.append(
                    {
                        "replica": replica,
                        "node_id": node_id,
                        "outcome": response["outcome"],
                        "outlier": not expected,
                    }
                )
            expected_outcome = f"COMPLETE:{_digest(job)}"
            expected_count = sum(item["outcome"] == expected_outcome for item in attempts)
            all_identical = len({item["outcome"] for item in attempts}) == 1
            accepted = all_identical if mode == "deterministic" else expected_count == quorum
            if not accepted:
                raise Refusal(
                    "Replicated job failed its predeclared convergence rule.",
                    "REPLICATION_DIVERGED",
                )
            outliers = [item for item in attempts if item["outcome"] != expected_outcome]
            try:
                entry = self.ledger.append(
                    {
                        "type": "replicated_run",
                        "job": job,
                        "mode": mode,
                        "replicas": replicas,
                        "predeclared_quorum": quorum,
                        "expected_outcome": expected_outcome,
                        "attempts": attempts,
                        "outliers": outliers,
                        "accepted": True,
                    }
                )
            except Exception as error:
                self._operable = False
                raise Refusal(
                    "Replicated run evidence is ambiguous; recovery is required.",
                    "RECOVERY_REQUIRED",
                ) from error
        return {
            "protocol": "RAPP/1",
            "control": "replicated_run",
            "mode": mode,
            "replicas": replicas,
            "predeclared_quorum": quorum,
            "expected_count": expected_count,
            "all_identical": all_identical,
            "accepted": True,
            "attempts": attempts,
            "outliers": outliers,
            "evidence": {"sequence": entry["sequence"], "event_hash": entry["event_hash"]},
        }
