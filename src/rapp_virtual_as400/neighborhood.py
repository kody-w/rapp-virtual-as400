"""Provider-neutral private-vNet simulator with isolated local node processes."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from .errors import Refusal
from .unicode_safe import canonical_json_strings

MAX_NODES = 8
MAX_REPLICAS = 100
MAX_JOB_BYTES = 2048
MAX_EVIDENCE_EVENTS = 10_000
NODE_RE = re.compile(r"^[A-Z][A-Z0-9-]{0,31}$")


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

    def __init__(self, path: Path) -> None:
        self.path = path.resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.path.parent, 0o700)
        self._lock = threading.Lock()
        if not self.path.exists():
            descriptor = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            os.close(descriptor)
        os.chmod(self.path, 0o600)
        entries = self.read()
        self._previous = entries[-1]["event_hash"] if entries else "0" * 64

    def read(self) -> list[dict]:
        entries: list[dict] = []
        previous = "0" * 64
        with self.path.open("r", encoding="utf-8") as handle:
            for sequence, line in enumerate(handle, 1):
                entry = json.loads(line)
                if entry.get("sequence") != sequence or entry.get("previous_hash") != previous:
                    raise Refusal("Evidence sequence or hash link is invalid.", "EVIDENCE_INVALID")
                unsigned = {
                    "sequence": entry["sequence"],
                    "previous_hash": entry["previous_hash"],
                    "record": entry["record"],
                }
                if entry.get("event_hash") != _digest(unsigned):
                    raise Refusal("Evidence event hash is invalid.", "EVIDENCE_INVALID")
                previous = entry["event_hash"]
                entries.append(entry)
        return entries

    def append(self, record: dict) -> dict:
        with self._lock:
            sequence = len(self.read()) + 1
            if sequence > MAX_EVIDENCE_EVENTS:
                raise Refusal("Evidence event limit reached.", "LIMIT_EXCEEDED")
            unsigned = {"sequence": sequence, "previous_hash": self._previous, "record": record}
            entry = {**unsigned, "event_hash": _digest(unsigned)}
            descriptor = os.open(self.path, os.O_WRONLY | os.O_APPEND, 0o600)
            try:
                with os.fdopen(descriptor, "ab", closefd=True) as handle:
                    handle.write(_json_bytes(entry) + b"\n")
                    handle.flush()
                    os.fsync(handle.fileno())
            finally:
                os.chmod(self.path, 0o600)
            self._previous = entry["event_hash"]
            return entry


class NodeProcess:
    def __init__(self, node_id: str, root: Path) -> None:
        self.node_id = node_id
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.root, 0o700)
        environment = {
            key: value
            for key, value in os.environ.items()
            if key in {"HOME", "PATH", "PYTHONPATH", "VIRTUAL_ENV", "SYSTEMROOT"}
        }
        environment["PYTHONUTF8"] = "1"
        self._process = subprocess.Popen(
            [sys.executable, "-m", "rapp_virtual_as400.node_worker", "--root", str(self.root)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            env=environment,
        )
        self._lock = threading.Lock()

    @property
    def pid(self) -> int:
        return self._process.pid

    def request(self, message: dict) -> dict:
        encoded = json.dumps(message, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if len(encoded.encode("utf-8")) > 8192:
            raise Refusal("Node request exceeds 8192 bytes.", "LIMIT_EXCEEDED")
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
        if self._process.poll() is None:
            try:
                self.request({"protocol": "RAPP/1", "kind": "control", "operation": "stop"})
                self._process.wait(timeout=2)
            except (Refusal, subprocess.TimeoutExpired):
                self._process.terminate()
                self._process.wait(timeout=2)
        if self._process.stdin:
            self._process.stdin.close()
        if self._process.stdout:
            self._process.stdout.close()
        if self._process.stderr:
            self._process.stderr.close()


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
        os.chmod(self.root, 0o700)
        self.ledger = EvidenceLedger(self.root / "evidence" / "events.jsonl")
        self.nodes = {node: NodeProcess(node, self.root / "nodes" / node) for node in ids}

    def __enter__(self) -> "PrivateVNetNeighborhood":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        for node in self.nodes.values():
            node.close()

    @staticmethod
    def _event_time(sequence: int) -> str:
        return (datetime(2000, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=sequence)).isoformat()

    def topology(self) -> dict:
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
        return node.request(
            {
                "protocol": "RAPP/1",
                "kind": "chat",
                "user_input": user_input,
                "session_id": session_id,
                "idempotency_key": idempotency_key,
            }
        )

    def _snapshots(self) -> dict[str, dict]:
        return {
            node_id: node.request({"protocol": "RAPP/1", "kind": "control", "operation": "snapshot"})["state"]
            for node_id, node in self.nodes.items()
        }

    def replicate_chat(
        self,
        user_input: str,
        session_id: str = "replicated",
        idempotency_key: str | None = None,
    ) -> dict:
        user_input = canonical_json_strings(user_input)  # type: ignore[assignment]
        session_id = canonical_json_strings(session_id)  # type: ignore[assignment]
        idempotency_key = canonical_json_strings(idempotency_key)  # type: ignore[assignment]
        sequence = len(self.ledger.read()) + 1
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
        results = {node_id: node.request(message) for node_id, node in self.nodes.items()}
        if len({_digest(result) for result in results.values()}) != 1:
            raise Refusal("Replicated chat results diverged.", "REPLICATION_DIVERGED")
        snapshots = self._snapshots()
        state_hashes = {node_id: _digest(state) for node_id, state in snapshots.items()}
        if len(set(state_hashes.values())) != 1:
            raise Refusal("Replicated node states diverged.", "REPLICATION_DIVERGED")
        entry = self.ledger.append(
            {
                "type": "replicated_chat",
                "message": message,
                "results": results,
                "state_hashes": state_hashes,
                "converged": True,
            }
        )
        return {
            "protocol": "RAPP/1",
            "control": "replicate_chat",
            "chat_result": next(iter(results.values())),
            "nodes": list(self.nodes),
            "converged": True,
            "state_hash": next(iter(state_hashes.values())),
            "evidence": {"sequence": entry["sequence"], "event_hash": entry["event_hash"]},
        }

    def replay_and_verify(self, node_id: str) -> dict:
        if node_id not in self.nodes:
            raise Refusal(f"Node {node_id} is not in this neighborhood.", "OBJECT_NOT_FOUND")
        node = self.nodes[node_id]
        node.request({"protocol": "RAPP/1", "kind": "control", "operation": "reset"})
        replayed = 0
        for entry in self.ledger.read():
            record = entry["record"]
            if record["type"] == "replicated_chat":
                result = node.request(record["message"])
                expected = record["results"][node_id]
                if result != expected:
                    raise Refusal("Replay result diverged from append-only evidence.", "REPLAY_DIVERGED")
                replayed += 1
        state_hashes = {name: _digest(state) for name, state in self._snapshots().items()}
        if len(set(state_hashes.values())) != 1:
            raise Refusal("Replayed node did not converge.", "REPLAY_DIVERGED")
        return {
            "protocol": "RAPP/1",
            "control": "replay",
            "node_id": node_id,
            "events_replayed": replayed,
            "converged": True,
            "state_hash": next(iter(state_hashes.values())),
        }

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
            raise Refusal("Replicated job failed its predeclared convergence rule.", "REPLICATION_DIVERGED")
        outliers = [item for item in attempts if item["outcome"] != expected_outcome]
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
