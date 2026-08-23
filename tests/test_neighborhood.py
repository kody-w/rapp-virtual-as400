from __future__ import annotations

import copy
import hashlib
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

from rapp_virtual_as400 import PrivateVNetNeighborhood, Refusal, VirtualAS400
from rapp_virtual_as400.storage import AtomicStore, empty_state
import rapp_virtual_as400.neighborhood as neighborhood_module

from .support import EngineTestCase


class NeighborhoodTests(EngineTestCase):
    def test_two_isolated_nodes_replicate_replay_and_run_100_identical(self) -> None:
        with PrivateVNetNeighborhood(self.work / "vnet") as neighborhood:
            topology = neighborhood.topology()
            self.assertEqual(topology["schema"], "rapp.private-vnet/v1")
            self.assertEqual(topology["node_count"], 2)
            self.assertFalse(topology["lan_listener"])
            self.assertFalse(topology["privileged_sibling_route"])
            self.assertEqual(len({node["pid"] for node in topology["nodes"]}), 2)
            self.assertEqual(len({node["state_root"] for node in topology["nodes"]}), 2)

            receipt = neighborhood.replicate_chat(
                "CRTLIB LIB(NET); CRTPF FILE(NET/JOBS) FIELDS(ID:INT,STATE:CHAR(8)); "
                "INSERT FILE(NET/JOBS) VALUES(ID='1',STATE='READY'); "
                "PRINT FILE(NET/JOBS) TITLE('Replicated Synthetic Jobs')",
                "network",
                "event-1",
            )
            self.assertTrue(receipt["converged"])
            self.assertEqual(
                set(receipt["chat_result"]),
                {"response", "agent_logs", "session_id"},
            )

            run = neighborhood.run_replicated_job(
                {"name": "DAILY-RUN", "payload": {"file": "NET/JOBS", "synthetic": True}},
                replicas=100,
                mode="deterministic",
            )
            self.assertEqual(run["replicas"], 100)
            self.assertEqual(run["predeclared_quorum"], 100)
            self.assertTrue(run["all_identical"])
            self.assertEqual(run["outliers"], [])
            self.assertEqual(len(run["attempts"]), 100)

            replay = neighborhood.replay_and_verify("AS400-B")
            self.assertTrue(replay["converged"])
            self.assertEqual(replay["events_replayed"], 1)
            self.assertEqual(len(neighborhood.ledger.read()), 3)

            evidence = self.work / "vnet" / "evidence" / "events.jsonl"
            self.assertEqual(os.stat(evidence).st_mode & 0o777, 0o600)

    def test_stochastic_exact_quorum_is_predeclared_and_outliers_retained(self) -> None:
        with PrivateVNetNeighborhood(self.work / "vnet") as neighborhood:
            with self.assertRaisesRegex(Refusal, "predeclared quorum"):
                neighborhood.run_replicated_job(
                    {"name": "FORECAST", "payload": {"synthetic": True}},
                    replicas=10,
                    mode="stochastic",
                )
            run = neighborhood.run_replicated_job(
                {"name": "FORECAST", "payload": {"synthetic": True}},
                replicas=10,
                mode="stochastic",
                quorum=7,
            )
            self.assertEqual(run["expected_count"], 7)
            self.assertEqual(len(run["attempts"]), 10)
            self.assertEqual(len(run["outliers"]), 3)
            recorded = neighborhood.ledger.read()[0]["record"]
            self.assertEqual(recorded["attempts"], run["attempts"])
            self.assertEqual(recorded["outliers"], run["outliers"])

    def test_replica_and_node_bounds_are_refused(self) -> None:
        with self.assertRaises(Refusal):
            PrivateVNetNeighborhood(self.work / "one", ("ONLY",))
        with PrivateVNetNeighborhood(self.work / "vnet") as neighborhood:
            with self.assertRaises(Refusal):
                neighborhood.run_replicated_job(
                    {"name": "TOO-MANY", "payload": {}},
                    replicas=101,
                )

    def test_later_node_failure_restores_earlier_mutation_and_records_failure(self) -> None:
        with PrivateVNetNeighborhood(self.work / "vnet") as neighborhood:
            before = neighborhood._snapshots()
            second = neighborhood.nodes["AS400-B"]
            original = second.request
            failed = False

            def fail_chat(message: dict) -> dict:
                nonlocal failed
                if message.get("kind") == "chat" and not failed:
                    failed = True
                    raise Refusal("injected later-node failure", "NODE_UNAVAILABLE")
                return original(message)

            second.request = fail_chat  # type: ignore[method-assign]
            with self.assertRaisesRegex(Refusal, "injected later-node failure"):
                neighborhood.replicate_chat("CRTLIB LIB(ROLLBACK)", "rollback", "later-failure")
            self.assertEqual(neighborhood._snapshots(), before)
            entries = neighborhood.ledger.read()
            self.assertEqual(
                [entry["record"]["type"] for entry in entries],
                ["replicated_chat_intent", "replicated_chat_failure"],
            )
            failure = entries[1]["record"]
            self.assertEqual(failure["intent_event_hash"], entries[0]["event_hash"])
            self.assertNotIn("pre_snapshots", failure)
            self.assertTrue(failure["rollback_verified"])
            self.assertEqual(
                failure["snapshot_bundle"]["path"],
                entries[0]["record"]["snapshot_bundle_path"],
            )
            snapshot_file = neighborhood.ledger.path.parent / failure["snapshot_bundle"]["path"]
            bundle = neighborhood.ledger.read_snapshot_bundle(failure["snapshot_bundle"])
            self.assertEqual(bundle["pre_snapshots"], before)
            encoded = snapshot_file.read_bytes()
            self.assertEqual(failure["snapshot_bundle"]["bytes"], len(encoded))
            self.assertEqual(
                failure["snapshot_bundle"]["sha256"],
                hashlib.sha256(encoded).hexdigest(),
            )
            self.assertEqual(os.stat(snapshot_file).st_mode & 0o777, 0o600)

    def test_result_divergence_rolls_back_every_node(self) -> None:
        with PrivateVNetNeighborhood(self.work / "vnet") as neighborhood:
            before = neighborhood._snapshots()
            second = neighborhood.nodes["AS400-B"]
            original = second.request

            def diverge_result(message: dict) -> dict:
                response = original(message)
                if message.get("kind") == "chat":
                    response = copy.deepcopy(response)
                    response["response"] += "\nDIVERGED"
                return response

            second.request = diverge_result  # type: ignore[method-assign]
            with self.assertRaisesRegex(Refusal, "results diverged"):
                neighborhood.replicate_chat("CRTLIB LIB(RESULTS)", "results", "diverge-results")
            self.assertEqual(neighborhood._snapshots(), before)
            self.assertTrue(neighborhood.ledger.read()[-1]["record"]["rollback_verified"])

    def test_state_divergence_rolls_back_every_node(self) -> None:
        with PrivateVNetNeighborhood(self.work / "vnet") as neighborhood:
            before = neighborhood._snapshots()
            second = neighborhood.nodes["AS400-B"]
            original = second.request
            diverged = False

            def diverge_state(message: dict) -> dict:
                nonlocal diverged
                response = original(message)
                if message.get("kind") == "chat" and not diverged:
                    diverged = True
                    original(
                        {
                            **message,
                            "user_input": "CRTLIB LIB(EXTRA)",
                            "idempotency_key": "extra-state",
                        }
                    )
                return response

            second.request = diverge_state  # type: ignore[method-assign]
            with self.assertRaisesRegex(Refusal, "states diverged"):
                neighborhood.replicate_chat("CRTLIB LIB(STATES)", "states", "diverge-states")
            self.assertEqual(neighborhood._snapshots(), before)

    def test_evidence_limit_preflight_contacts_no_node(self) -> None:
        with PrivateVNetNeighborhood(self.work / "vnet") as neighborhood:
            contacts = 0
            originals = {node_id: node.request for node_id, node in neighborhood.nodes.items()}

            def counted(node_id: str, message: dict) -> dict:
                nonlocal contacts
                contacts += 1
                return originals[node_id](message)

            for node_id, node in neighborhood.nodes.items():
                node.request = lambda message, node_id=node_id: counted(node_id, message)  # type: ignore[method-assign]
            with mock.patch.object(neighborhood_module, "MAX_EVIDENCE_EVENTS", 1):
                with self.assertRaisesRegex(Refusal, "Evidence event limit"):
                    neighborhood.replicate_chat("CRTLIB LIB(FULL)", "full", "full")
            self.assertEqual(contacts, 0)
            self.assertEqual(neighborhood.ledger.read(), [])

    def test_terminal_append_failure_rolls_back_and_leaves_linked_failure(self) -> None:
        with PrivateVNetNeighborhood(self.work / "vnet") as neighborhood:
            before = neighborhood._snapshots()
            original_append = neighborhood.ledger.append
            commit_failed = False

            def fail_commit(record: dict) -> dict:
                nonlocal commit_failed
                if record.get("type") == "replicated_chat_commit" and not commit_failed:
                    commit_failed = True
                    raise OSError("injected terminal append failure")
                return original_append(record)

            neighborhood.ledger.append = fail_commit  # type: ignore[method-assign]
            with self.assertRaisesRegex(Refusal, "Terminal evidence append failed"):
                neighborhood.replicate_chat("CRTLIB LIB(EVIDENCE)", "evidence", "append-failure")
            self.assertEqual(neighborhood._snapshots(), before)
            entries = neighborhood.ledger.read()
            self.assertEqual(
                [entry["record"]["type"] for entry in entries],
                ["replicated_chat_intent", "replicated_chat_failure"],
            )
            self.assertEqual(entries[1]["record"]["failure"]["code"], "EVIDENCE_IO_FAILED")
            self.assertTrue(entries[1]["record"]["rollback_verified"])
            self.assertNotIn("pre_snapshots", entries[1]["record"])
            neighborhood.ledger.audit()

    def test_rollback_acknowledgement_hash_is_verified(self) -> None:
        with PrivateVNetNeighborhood(self.work / "vnet") as neighborhood:
            before = neighborhood._snapshots()
            first = neighborhood.nodes["AS400-A"]
            first_original = first.request
            second = neighborhood.nodes["AS400-B"]
            second_original = second.request
            failed = False

            def false_restore_ack(message: dict) -> dict:
                response = first_original(message)
                if message.get("operation") == "restore":
                    response = {**response, "state_hash": "0" * 64}
                return response

            def fail_second_chat(message: dict) -> dict:
                nonlocal failed
                if message.get("kind") == "chat" and not failed:
                    failed = True
                    raise Refusal("injected node failure", "NODE_UNAVAILABLE")
                return second_original(message)

            first.request = false_restore_ack  # type: ignore[method-assign]
            second.request = fail_second_chat  # type: ignore[method-assign]
            with self.assertRaisesRegex(Refusal, "rollback could not be verified"):
                neighborhood.replicate_chat("CRTLIB LIB(VERIFY)", "verify", "verify-rollback")
            self.assertEqual(neighborhood._snapshots(), before)
            failure = neighborhood.ledger.read()[-1]["record"]
            self.assertFalse(failure["rollback_verified"])
            self.assertRegex(failure["rollback_failures"][0], "acknowledgement hash")

    def test_replay_ignores_intent_and_failure_records(self) -> None:
        with PrivateVNetNeighborhood(self.work / "vnet") as neighborhood:
            second = neighborhood.nodes["AS400-B"]
            original = second.request
            failed = False

            def fail_once(message: dict) -> dict:
                nonlocal failed
                if message.get("kind") == "chat" and not failed:
                    failed = True
                    raise Refusal("injected replay prelude failure", "NODE_UNAVAILABLE")
                return original(message)

            second.request = fail_once  # type: ignore[method-assign]
            with self.assertRaises(Refusal):
                neighborhood.replicate_chat("CRTLIB LIB(IGNORED)", "ignored", "ignored")
            neighborhood.replicate_chat("CRTLIB LIB(COMMITTED)", "committed", "committed")
            replay = neighborhood.replay_and_verify("AS400-B")
            self.assertEqual(replay["events_replayed"], 1)
            self.assertTrue(replay["converged"])

    def test_restore_is_strict_atomic_and_private(self) -> None:
        store = AtomicStore(self.work / "restore" / "state.json")
        snapshot = store.snapshot()
        with self.assertRaisesRegex(Refusal, "invalid schema"):
            store.restore({**snapshot, "unexpected": True})
        with mock.patch("rapp_virtual_as400.storage.MAX_RESTORE_SNAPSHOT_BYTES", 100):
            with self.assertRaisesRegex(Refusal, "bounded restore limit"):
                store.restore(snapshot)
        changed = copy.deepcopy(snapshot)
        changed["revision"] = 7
        store.restore(changed)
        self.assertEqual(store.snapshot(), changed)
        self.assertEqual(os.stat(store.path).st_mode & 0o777, 0o600)
        self.assertEqual(os.stat(store.lock_path).st_mode & 0o777, 0o600)

        with PrivateVNetNeighborhood(self.work / "vnet") as neighborhood:
            node = neighborhood.nodes["AS400-A"]
            response = node.request(
                {
                    "protocol": "RAPP/1",
                    "kind": "control",
                    "operation": "restore",
                    "state": {"format": 1},
                }
            )
            self.assertEqual(response["error"]["code"], "INVALID_SNAPSHOT")
            neighborhood.replicate_chat("CRTLIB LIB(PRIVATE)", "private", "private")
            snapshots = neighborhood.ledger.path.parent / "snapshots"
            self.assertEqual(os.stat(snapshots).st_mode & 0o777, 0o700)
            self.assertEqual(os.stat(next(snapshots.iterdir())).st_mode & 0o777, 0o600)
            for child in neighborhood.nodes.values():
                self.assertEqual(os.stat(child.root).st_mode & 0o777, 0o700)
                self.assertEqual(os.stat(child.root / "state.json").st_mode & 0o777, 0o600)
                self.assertEqual(os.stat(child.root / "state.json.lock").st_mode & 0o777, 0o600)

    def test_two_instances_serialize_complete_replication_transactions(self) -> None:
        root = self.work / "shared"
        first = PrivateVNetNeighborhood(root)
        second = PrivateVNetNeighborhood(root)
        entered = threading.Event()
        release = threading.Event()
        original = first.nodes["AS400-A"].request

        def pause_first_chat(message: dict) -> dict:
            if message.get("kind") == "chat":
                entered.set()
                if not release.wait(3):
                    raise AssertionError("concurrency test release timed out")
            return original(message)

        first.nodes["AS400-A"].request = pause_first_chat  # type: ignore[method-assign]
        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                one = executor.submit(
                    first.replicate_chat,
                    "CRTLIB LIB(FIRST)",
                    "first",
                    "first",
                )
                self.assertTrue(entered.wait(3))
                two = executor.submit(
                    second.replicate_chat,
                    "CRTLIB LIB(SECOND)",
                    "second",
                    "second",
                )
                time.sleep(0.1)
                self.assertFalse(two.done())
                release.set()
                one.result(timeout=5)
                two.result(timeout=5)
            entries = first.ledger.audit()
            self.assertEqual([entry["sequence"] for entry in entries], [1, 2, 3, 4])
            self.assertEqual(len({entry["event_hash"] for entry in entries}), 4)
            snapshots = second._snapshots()
            for state in snapshots.values():
                self.assertEqual(set(state["libraries"]), {"FIRST", "SECOND"})
        finally:
            release.set()
            first.close()
            second.close()

    def test_stale_ledgers_refresh_bounded_tail_without_full_read(self) -> None:
        path = self.work / "evidence" / "events.jsonl"
        first = neighborhood_module.EvidenceLedger(path)
        stale = neighborhood_module.EvidenceLedger(path)
        first.append({"type": "one"})
        with mock.patch.object(stale, "read", side_effect=AssertionError("full read used")):
            second_entry = stale.append({"type": "two"})
        third_entry = first.append({"type": "three"})
        self.assertEqual((second_entry["sequence"], third_entry["sequence"]), (2, 3))
        self.assertEqual(len(first.read()), 3)

    def test_large_snapshot_exists_once_and_bundle_tampering_is_refused(self) -> None:
        root = self.work / "large"
        for node_id in ("AS400-A", "AS400-B"):
            store = AtomicStore(root / "nodes" / node_id / "state.json")
            state = empty_state()
            state["revision"] = 1
            state["libraries"]["BIG"] = {"files": {}}
            state["data_queues"]["BIG/QUEUE"] = ["x" * 2048 for _ in range(100)]
            store.restore(state)
        with PrivateVNetNeighborhood(root) as neighborhood:
            neighborhood.replicate_chat("DSPLIB", "large", "large")
            entries = neighborhood.ledger.audit()
            terminal = entries[-1]["record"]
            self.assertNotIn("pre_snapshots", terminal)
            reference = terminal["snapshot_bundle"]
            self.assertGreater(reference["bytes"], 400_000)
            self.assertLess(neighborhood.ledger.path.stat().st_size, reference["bytes"])
            bundle_path = neighborhood.ledger.path.parent / reference["path"]
            with bundle_path.open("r+b") as handle:
                handle.seek(0)
                handle.write(b" ")
                handle.flush()
                os.fsync(handle.fileno())
            before = neighborhood._snapshots()["AS400-B"]
            with self.assertRaisesRegex(Refusal, "digest"):
                neighborhood.replay_and_verify("AS400-B")
            self.assertEqual(neighborhood._snapshots()["AS400-B"], before)

    def test_snapshot_bundle_rejects_digest_and_escape_references(self) -> None:
        path = self.work / "evidence" / "events.jsonl"
        ledger = neighborhood_module.EvidenceLedger(path)
        bundle = {"pre_snapshots": {}, "pre_state_hashes": {}}
        reference = ledger.write_snapshot_bundle("intent-1.json", bundle)
        with self.assertRaisesRegex(Refusal, "digest"):
            ledger.read_snapshot_bundle({**reference, "sha256": "0" * 64})
        with self.assertRaisesRegex(Refusal, "reference"):
            ledger.read_snapshot_bundle({**reference, "path": "../state.json"})

    def test_snapshot_bundle_publication_failures_precede_intent_and_mutation(self) -> None:
        stages = ("write", "rename", "directory-fsync")
        for stage in stages:
            with self.subTest(stage=stage):
                with PrivateVNetNeighborhood(self.work / stage) as neighborhood:
                    before = neighborhood._snapshots()
                    original_write = os.write
                    partial_write_done = False

                    def fail_partial_write(descriptor: int, data: bytes) -> int:
                        nonlocal partial_write_done
                        if not partial_write_done:
                            partial_write_done = True
                            return original_write(descriptor, data[:3])
                        raise OSError("injected bundle write failure")

                    if stage == "write":
                        patcher = mock.patch.object(
                            neighborhood_module.os,
                            "write",
                            side_effect=fail_partial_write,
                        )
                    elif stage == "rename":
                        patcher = mock.patch.object(
                            neighborhood_module.os,
                            "link",
                            side_effect=OSError("injected bundle publication failure"),
                        )
                    else:
                        patcher = mock.patch.object(
                            neighborhood_module,
                            "_fsync_directory",
                            side_effect=OSError("injected snapshot directory fsync failure"),
                        )
                    with patcher, self.assertRaises(Refusal):
                        neighborhood.replicate_chat(
                            "CRTLIB LIB(NEVER)",
                            stage,
                            stage,
                        )
                    self.assertEqual(neighborhood._snapshots(), before)
                    self.assertEqual(neighborhood.ledger.read(), [])
                    children = list((neighborhood.ledger.path.parent / "snapshots").iterdir())
                    self.assertFalse(
                        any(neighborhood_module.BUNDLE_TEMP_RE.fullmatch(child.name) for child in children)
                    )
                    self.assertFalse(
                        any(
                            entry["record"].get("type") == "replicated_chat_commit"
                            for entry in neighborhood.ledger.read()
                        )
                    )

    def test_snapshot_bundle_is_immutable_and_crash_artifacts_recover_safely(self) -> None:
        path = self.work / "durable" / "events.jsonl"
        ledger = neighborhood_module.EvidenceLedger(path)
        first = {"pre_snapshots": {}, "pre_state_hashes": {}}
        reference = ledger.write_snapshot_bundle("intent-1.json", first)
        destination = ledger.path.parent / reference["path"]
        original = destination.read_bytes()
        with self.assertRaisesRegex(Refusal, "immutable"):
            ledger.write_snapshot_bundle(
                "intent-1.json",
                {"pre_snapshots": {"AS400-A": empty_state()}, "pre_state_hashes": {}},
            )
        self.assertEqual(destination.read_bytes(), original)

        stale = destination.parent / ".intent-2.json.0123456789abcdef0123456789abcdef.tmp"
        stale.write_bytes(b'{"partial":')
        os.chmod(stale, 0o600)
        reopened = neighborhood_module.EvidenceLedger(path)
        self.assertFalse(stale.exists())
        self.assertEqual(reopened.write_snapshot_bundle("intent-1.json", first), reference)
        self.assertEqual(reopened.read(), [])
        self.assertEqual(reopened.read_snapshot_bundle(reference), first)

    def test_directory_durability_precedes_intent_append(self) -> None:
        with PrivateVNetNeighborhood(self.work / "ordering") as neighborhood:
            durable = False
            original_fsync = neighborhood_module._fsync_directory
            original_append = neighborhood.ledger.append
            snapshots = neighborhood.ledger.path.parent / "snapshots"

            def observed_fsync(path) -> None:
                nonlocal durable
                original_fsync(path)
                if path == snapshots:
                    durable = True

            def guarded_append(record: dict) -> dict:
                if record.get("type") in {
                    "replicated_chat_intent",
                    "replicated_chat_commit",
                    "replicated_chat_failure",
                }:
                    self.assertTrue(durable)
                return original_append(record)

            neighborhood.ledger.append = guarded_append  # type: ignore[method-assign]
            with mock.patch.object(
                neighborhood_module,
                "_fsync_directory",
                side_effect=observed_fsync,
            ):
                neighborhood.replicate_chat("CRTLIB LIB(DURABLE)", "durable", "durable")
            neighborhood.ledger.audit()

    def test_exhausted_identifier_state_is_valid_snapshot_evidence(self) -> None:
        state = empty_state()
        state["revision"] = 1
        state["libraries"]["TEST"] = {
            "files": {
                "ITEMS": {
                    "fields": [
                        {"name": "ID", "type": "CHAR", "precision": 1, "scale": 0}
                    ],
                    "records": [],
                }
            }
        }
        state["job_queues"]["TEST/BATCH"] = []
        state["jobs"]["J999999"] = {
            "queue": "TEST/BATCH",
            "command": "DSPLIB",
            "status": "COMPLETE",
            "result": "complete",
        }
        state["next_job"] = 1000000
        state["spool"] = [
            {
                "id": "S999999",
                "title": "Terminal",
                "created_at": "2000-01-01T00:00:00+00:00",
                "report": "terminal",
            }
        ]
        state["next_spool"] = 1000000
        with PrivateVNetNeighborhood(self.work / "exhausted-evidence") as neighborhood:
            for node_id, node in neighborhood.nodes.items():
                neighborhood._checked_response(
                    node_id,
                    node.request(
                        {
                            "protocol": "RAPP/1",
                            "kind": "control",
                            "operation": "restore",
                            "state": state,
                        }
                    ),
                    "restore",
                )
            neighborhood.replicate_chat("DSPLIB", "terminal", "terminal")
            entries = neighborhood.ledger.audit()
            bundle = neighborhood.ledger.read_snapshot_bundle(
                entries[-1]["record"]["snapshot_bundle"]
            )
            for snapshot in bundle["pre_snapshots"].values():
                self.assertEqual(
                    (snapshot["next_job"], snapshot["next_spool"]),
                    (1000000, 1000000),
                )

    def test_byte_capacity_preflight_happens_before_chat_mutation(self) -> None:
        with PrivateVNetNeighborhood(self.work / "capacity") as neighborhood:
            chat_contacts = 0
            originals = {name: node.request for name, node in neighborhood.nodes.items()}

            def counted(node_id: str, message: dict) -> dict:
                nonlocal chat_contacts
                if message.get("kind") == "chat":
                    chat_contacts += 1
                return originals[node_id](message)

            for node_id, node in neighborhood.nodes.items():
                node.request = lambda message, node_id=node_id: counted(node_id, message)  # type: ignore[method-assign]
            with mock.patch.object(
                neighborhood_module,
                "MAX_EVIDENCE_BYTES",
                neighborhood_module.MAX_EVIDENCE_RECORD_BYTES,
            ):
                with self.assertRaisesRegex(Refusal, "Evidence byte limit"):
                    neighborhood.replicate_chat("CRTLIB LIB(FULL)", "full", "full")
            self.assertEqual(chat_contacts, 0)
            self.assertEqual(neighborhood._snapshots()["AS400-A"]["libraries"], {})
