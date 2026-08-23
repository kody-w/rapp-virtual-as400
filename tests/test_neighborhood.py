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
from rapp_virtual_as400.storage import (
    AtomicStore,
    empty_state,
    encode_idempotency_identity,
)
import rapp_virtual_as400.neighborhood as neighborhood_module

from .support import EngineTestCase


class NeighborhoodTests(EngineTestCase):
    def _replay_protected_artifacts(self, neighborhood) -> tuple[dict, dict, dict]:
        snapshots = neighborhood._snapshots()
        node_files = {
            node_id: {
                child.name: child.read_bytes()
                for child in sorted(node.root.iterdir(), key=lambda item: item.name)
            }
            for node_id, node in neighborhood.nodes.items()
        }
        evidence_files = {
            child.relative_to(neighborhood.ledger.path.parent).as_posix(): child.read_bytes()
            for child in sorted(
                (
                    path
                    for path in neighborhood.ledger.path.parent.rglob("*")
                    if path.is_file()
                ),
                key=lambda item: item.as_posix(),
            )
        }
        return snapshots, node_files, evidence_files

    def _assert_replay_protected_artifacts(self, neighborhood, expected) -> None:
        self.assertEqual(self._replay_protected_artifacts(neighborhood), expected)
        self.assertFalse(any(child.name.startswith(".replay-") for child in neighborhood.root.iterdir()))
        self.assertEqual(neighborhood.topology()["node_count"], len(neighborhood.nodes))

    def _leave_unmatched_after_first_mutation(self, root) -> dict:
        neighborhood = PrivateVNetNeighborhood(root)
        first = neighborhood.nodes["AS400-A"]
        original = first.request

        def crash_after_mutation(message: dict) -> dict:
            response = original(message)
            if message.get("kind") == "chat":
                raise SystemExit("simulated parent crash")
            return response

        first.request = crash_after_mutation  # type: ignore[method-assign]
        try:
            with self.assertRaisesRegex(SystemExit, "simulated parent crash"):
                neighborhood.replicate_chat(
                    "CRTLIB LIB(CRASHED)",
                    "crash",
                    "first-node",
                )
            entries = neighborhood.ledger.read()
            self.assertEqual(
                [entry["record"]["type"] for entry in entries],
                ["replicated_chat_intent"],
            )
            with self.assertRaises(Refusal) as blocked:
                neighborhood.topology()
            self.assertEqual(blocked.exception.code, "RECOVERY_REQUIRED")
            return entries[0]
        finally:
            neighborhood.close()

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
            self.assert_private_mode(evidence, 0o600)

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
            self.assert_private_mode(snapshot_file, 0o600)

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

    def test_unrecordable_terminal_closes_operations_until_reopen_recovery(self) -> None:
        root = self.work / "unrecordable"
        neighborhood = PrivateVNetNeighborhood(root)
        original_append = neighborhood.ledger.append

        def fail_all_terminals(record: dict) -> dict:
            if record.get("type") in {
                "replicated_chat_commit",
                "replicated_chat_failure",
            }:
                raise OSError("injected terminal outage")
            return original_append(record)

        neighborhood.ledger.append = fail_all_terminals  # type: ignore[method-assign]
        try:
            with self.assertRaisesRegex(Refusal, "closed pending durable recovery") as caught:
                neighborhood.replicate_chat("CRTLIB LIB(CLOSED)", "closed", "closed")
            self.assertEqual(caught.exception.code, "RECOVERY_REQUIRED")
            with self.assertRaises(Refusal) as blocked:
                neighborhood.chat("AS400-A", "DSPLIB", "blocked")
            self.assertEqual(blocked.exception.code, "RECOVERY_REQUIRED")
        finally:
            neighborhood.close()

        with PrivateVNetNeighborhood(root) as recovered:
            self.assertEqual(
                [entry["record"]["type"] for entry in recovered.ledger.audit()],
                ["replicated_chat_intent", "replicated_chat_recovery"],
            )
            self.assertEqual(recovered._snapshots()["AS400-A"]["libraries"], {})

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

    def test_replay_uses_disposable_node_from_recorded_initial_state(self) -> None:
        root = self.work / "preloaded"
        seeded = empty_state()
        seeded["revision"] = 1
        seeded["libraries"]["SEED"] = {"files": {}}
        for node_id in ("AS400-A", "AS400-B"):
            AtomicStore(root / "nodes" / node_id / "state.json").restore(seeded)
        with PrivateVNetNeighborhood(root) as neighborhood:
            neighborhood.replicate_chat(
                "CRTPF FILE(SEED/ITEMS) FIELDS(ID:INT)",
                "seeded",
                "create-items",
            )
            before = self._replay_protected_artifacts(neighborhood)
            replay = neighborhood.replay_and_verify("AS400-B")
            self.assertEqual(replay["events_replayed"], 1)
            self.assertEqual(replay["state_hash"], neighborhood.ledger.audit()[-1]["record"]["state_hashes"]["AS400-B"])
            self._assert_replay_protected_artifacts(neighborhood, before)

    def test_replay_mid_event_failure_never_touches_live_nodes_or_evidence(self) -> None:
        with PrivateVNetNeighborhood(self.work / "mid-event") as neighborhood:
            neighborhood.replicate_chat("CRTLIB LIB(ONE)", "replay", "one")
            neighborhood.replicate_chat("CRTLIB LIB(TWO)", "replay", "two")
            before = self._replay_protected_artifacts(neighborhood)
            original = neighborhood_module.NodeProcess.request
            replay_events = 0

            def fail_second_replay_event(node, message):
                nonlocal replay_events
                if node.node_id.startswith("REPLAY-") and message.get("kind") == "chat":
                    replay_events += 1
                    if replay_events == 2:
                        raise Refusal("injected disposable event failure", "NODE_UNAVAILABLE")
                return original(node, message)

            with mock.patch.object(
                neighborhood_module.NodeProcess,
                "request",
                fail_second_replay_event,
            ):
                with self.assertRaisesRegex(Refusal, "injected disposable event failure"):
                    neighborhood.replay_and_verify("AS400-B")
            self._assert_replay_protected_artifacts(neighborhood, before)

    def test_replay_disposable_restore_close_and_cleanup_faults_preserve_live_state(self) -> None:
        stages = ("restore", "close", "cleanup")
        for stage in stages:
            with self.subTest(stage=stage):
                with PrivateVNetNeighborhood(self.work / f"replay-{stage}") as neighborhood:
                    neighborhood.replicate_chat("CRTLIB LIB(SAFE)", "replay", stage)
                    before = self._replay_protected_artifacts(neighborhood)
                    original_request = neighborhood_module.NodeProcess.request
                    original_close = neighborhood_module.NodeProcess.close
                    original_erase = neighborhood._erase_disposable_replay_root

                    def fault_restore(node, message):
                        if (
                            node.node_id.startswith("REPLAY-")
                            and message.get("operation") == "restore"
                        ):
                            raise Refusal("injected disposable restore failure", "NODE_UNAVAILABLE")
                        return original_request(node, message)

                    def close_then_fault(node):
                        original_close(node)
                        if node.node_id.startswith("REPLAY-"):
                            raise OSError("injected disposable close failure")

                    def fault_erase(path):
                        raise OSError("injected disposable cleanup failure")

                    request_patch = (
                        mock.patch.object(neighborhood_module.NodeProcess, "request", fault_restore)
                        if stage == "restore"
                        else mock.patch.object(neighborhood_module.NodeProcess, "request", original_request)
                    )
                    close_patch = (
                        mock.patch.object(neighborhood_module.NodeProcess, "close", close_then_fault)
                        if stage == "close"
                        else mock.patch.object(neighborhood_module.NodeProcess, "close", original_close)
                    )
                    erase_patch = (
                        mock.patch.object(neighborhood, "_erase_disposable_replay_root", fault_erase)
                        if stage == "cleanup"
                        else mock.patch.object(
                            neighborhood,
                            "_erase_disposable_replay_root",
                            original_erase,
                        )
                    )
                    with request_patch, close_patch, erase_patch:
                        with self.assertRaises(Refusal):
                            neighborhood.replay_and_verify("AS400-A")
                    if stage == "cleanup":
                        self.assertEqual(self._replay_protected_artifacts(neighborhood), before)
                        stale = [
                            child
                            for child in neighborhood.root.iterdir()
                            if child.name.startswith(".replay-")
                        ]
                        self.assertEqual(len(stale), 1)
                        self.assertLessEqual(
                            len(list(stale[0].iterdir())),
                            neighborhood_module.MAX_REPLAY_ROOT_ENTRIES,
                        )
                        with self.assertRaisesRegex(Refusal, "not proven erased"):
                            neighborhood.replay_and_verify("AS400-A")
                        self.assertEqual(neighborhood.topology()["node_count"], 2)
                        original_erase(stale[0])
                    self._assert_replay_protected_artifacts(neighborhood, before)

    def test_replay_uncertain_disposable_setup_fails_closed_without_live_change(self) -> None:
        with PrivateVNetNeighborhood(self.work / "replay-setup") as neighborhood:
            neighborhood.replicate_chat("CRTLIB LIB(SAFE)", "replay", "setup")
            before = self._replay_protected_artifacts(neighborhood)
            with mock.patch.object(
                neighborhood_module.NodeProcess,
                "__init__",
                side_effect=OSError("injected constructor failure"),
            ):
                with self.assertRaisesRegex(Refusal, "setup status was not proven"):
                    neighborhood.replay_and_verify("AS400-A")
            self.assertEqual(self._replay_protected_artifacts(neighborhood), before)
            stale = [
                child
                for child in neighborhood.root.iterdir()
                if child.name.startswith(".replay-")
            ]
            self.assertEqual(len(stale), 1)
            self.assertEqual(list(stale[0].iterdir()), [])
            with self.assertRaisesRegex(Refusal, "not proven erased"):
                neighborhood.replay_and_verify("AS400-A")
            self.assertEqual(neighborhood.topology()["node_count"], 2)
            neighborhood._erase_disposable_replay_root(stale[0])
            self._assert_replay_protected_artifacts(neighborhood, before)

    def test_replay_tampered_evidence_fails_before_disposable_setup(self) -> None:
        with PrivateVNetNeighborhood(self.work / "tampered-replay") as neighborhood:
            neighborhood.replicate_chat("CRTLIB LIB(SAFE)", "tamper", "tamper")
            reference = neighborhood.ledger.read()[-1]["record"]["snapshot_bundle"]
            bundle = neighborhood.ledger.path.parent / reference["path"]
            encoded = bundle.read_bytes()
            bundle.write_bytes(bytes([encoded[0] ^ 1]) + encoded[1:])
            before = self._replay_protected_artifacts(neighborhood)
            with mock.patch.object(
                neighborhood,
                "_create_disposable_replay_root",
                side_effect=AssertionError("disposable setup must not start"),
            ):
                with self.assertRaisesRegex(Refusal, "digest"):
                    neighborhood.replay_and_verify("AS400-B")
            self._assert_replay_protected_artifacts(neighborhood, before)

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
        self.assert_private_mode(store.path, 0o600)
        self.assert_private_mode(store.lock_path, 0o600)

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
            self.assert_private_mode(snapshots, 0o700)
            self.assert_private_mode(next(snapshots.iterdir()), 0o600)
            for child in neighborhood.nodes.values():
                self.assert_private_mode(child.root, 0o700)
                self.assert_private_mode(child.root / "state.json", 0o600)
                self.assert_private_mode(child.root / "state.json.lock", 0o600)

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

    def test_append_checks_permissions_before_publication(self) -> None:
        if os.name == "nt":
            self.skipTest("Windows privacy is enforced by inherited ACLs, not chmod")
        ledger = neighborhood_module.EvidenceLedger(
            self.work / "permissions" / "evidence" / "events.jsonl"
        )
        original_chmod = os.chmod
        original_write = os.write
        writes = 0

        def fail_evidence_chmod(path, mode) -> None:
            if os.fspath(path) == os.fspath(ledger.path):
                raise OSError("injected permission failure")
            original_chmod(path, mode)

        def count_write(descriptor: int, data: bytes) -> int:
            nonlocal writes
            writes += 1
            return original_write(descriptor, data)

        with (
            mock.patch.object(neighborhood_module.os, "chmod", side_effect=fail_evidence_chmod),
            mock.patch.object(neighborhood_module.os, "write", side_effect=count_write),
            self.assertRaisesRegex(OSError, "permission"),
        ):
            ledger.append({"type": "never-published"})
        self.assertEqual(writes, 0)
        self.assertEqual(ledger.read(), [])

    def test_append_recovers_exact_write_and_fsync_exceptions(self) -> None:
        for stage in ("write", "fsync"):
            with self.subTest(stage=stage):
                ledger = neighborhood_module.EvidenceLedger(
                    self.work / f"exact-{stage}" / "evidence" / "events.jsonl"
                )
                original_write = os.write
                original_fsync = os.fsync
                injected = False

                def write_then_raise(descriptor: int, data: bytes) -> int:
                    nonlocal injected
                    written = original_write(descriptor, data)
                    if not injected:
                        injected = True
                        raise OSError("injected post-write exception")
                    return written

                def fsync_then_raise(descriptor: int) -> None:
                    nonlocal injected
                    original_fsync(descriptor)
                    if not injected:
                        injected = True
                        raise OSError("injected post-fsync exception")

                patcher = (
                    mock.patch.object(neighborhood_module.os, "write", side_effect=write_then_raise)
                    if stage == "write"
                    else mock.patch.object(
                        neighborhood_module.os,
                        "fsync",
                        side_effect=fsync_then_raise,
                    )
                )
                with ledger.transaction_lock, patcher:
                    entry = ledger.append({"type": f"exact-{stage}"})
                self.assertEqual(entry["sequence"], 1)
                self.assertEqual(ledger.read(), [entry])

    def test_append_ignores_close_failure_after_fsync(self) -> None:
        ledger = neighborhood_module.EvidenceLedger(
            self.work / "close" / "evidence" / "events.jsonl"
        )
        with mock.patch.object(
                neighborhood_module.os,
                "close",
                side_effect=OSError("injected cosmetic close failure"),
            ):
            entry = ledger.append({"type": "close-is-cosmetic"})
        self.assertEqual(ledger.read(), [entry])

    def test_commit_published_then_exception_is_exact_success_not_failure(self) -> None:
        with PrivateVNetNeighborhood(self.work / "commit-exact") as neighborhood:
            original_append = neighborhood.ledger.append

            def publish_then_raise(record: dict) -> dict:
                entry = original_append(record)
                if record.get("type") == "replicated_chat_commit":
                    raise OSError("injected exception after commit publication")
                return entry

            neighborhood.ledger.append = publish_then_raise  # type: ignore[method-assign]
            receipt = neighborhood.replicate_chat(
                "CRTLIB LIB(EXACT)",
                "exact",
                "exact",
            )
            self.assertTrue(receipt["converged"])
            entries = neighborhood.ledger.audit()
            self.assertEqual(
                [entry["record"]["type"] for entry in entries],
                ["replicated_chat_intent", "replicated_chat_commit"],
            )
            self.assertTrue(neighborhood.topology()["node_count"])

    def test_failure_published_then_exception_is_not_duplicated(self) -> None:
        with PrivateVNetNeighborhood(self.work / "failure-exact") as neighborhood:
            second = neighborhood.nodes["AS400-B"]
            original_request = second.request
            original_append = neighborhood.ledger.append
            failed = False

            def fail_node(message: dict) -> dict:
                nonlocal failed
                if message.get("kind") == "chat" and not failed:
                    failed = True
                    raise Refusal("injected node failure", "NODE_UNAVAILABLE")
                return original_request(message)

            def publish_then_raise(record: dict) -> dict:
                entry = original_append(record)
                if record.get("type") == "replicated_chat_failure":
                    raise OSError("injected exception after failure publication")
                return entry

            second.request = fail_node  # type: ignore[method-assign]
            neighborhood.ledger.append = publish_then_raise  # type: ignore[method-assign]
            with self.assertRaisesRegex(Refusal, "injected node failure"):
                neighborhood.replicate_chat("CRTLIB LIB(FAIL)", "fail", "fail")
            entries = neighborhood.ledger.audit()
            self.assertEqual(
                [entry["record"]["type"] for entry in entries],
                ["replicated_chat_intent", "replicated_chat_failure"],
            )
            self.assertTrue(neighborhood.topology()["node_count"])

    def test_partial_commit_append_fails_closed_without_failure_terminal(self) -> None:
        with PrivateVNetNeighborhood(self.work / "partial-commit") as neighborhood:
            original_append = neighborhood.ledger.append
            original_write = os.write

            def partial_then_raise(descriptor: int, data: bytes) -> int:
                original_write(descriptor, data[:3])
                raise OSError("injected partial commit")

            def fail_commit(record: dict) -> dict:
                if record.get("type") == "replicated_chat_commit":
                    with mock.patch.object(
                        neighborhood_module.os,
                        "write",
                        side_effect=partial_then_raise,
                    ):
                        return original_append(record)
                return original_append(record)

            neighborhood.ledger.append = fail_commit  # type: ignore[method-assign]
            with self.assertRaises(Refusal) as caught:
                neighborhood.replicate_chat("CRTLIB LIB(PARTIAL)", "partial", "partial")
            self.assertEqual(caught.exception.code, "RECOVERY_REQUIRED")
            with self.assertRaises(Refusal) as blocked:
                neighborhood.topology()
            self.assertEqual(blocked.exception.code, "RECOVERY_REQUIRED")
            with self.assertRaisesRegex(Refusal, "incomplete|invalid"):
                neighborhood.ledger.audit()

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

    def test_legacy_bundle_keeps_raw_evidence_and_recovers_migrated_state(self) -> None:
        root = self.work / "legacy-evidence"
        events_path = root / "evidence" / "events.jsonl"
        ledger = neighborhood_module.EvidenceLedger(events_path)
        legacy = empty_state()
        legacy["revision"] = 1
        legacy["sessions"]["a:b"] = {
            "turns": [
                {
                    "at": "2000-01-01T00:00:00+00:00",
                    "input": "DSPLIB",
                    "response": "Libraries: none",
                }
            ]
        }
        legacy["idempotency"]["a:b:c"] = {
            "request_hash": hashlib.sha256(b"DSPLIB").hexdigest(),
            "result": {
                "response": "Libraries: none",
                "agent_logs": [{"command": "DSPLIB", "status": "ok"}],
                "session_id": "a:b",
            },
        }
        raw_hash = neighborhood_module._digest(legacy)
        bundle = {
            "pre_snapshots": {
                "AS400-A": copy.deepcopy(legacy),
                "AS400-B": copy.deepcopy(legacy),
            },
            "pre_state_hashes": {
                "AS400-A": raw_hash,
                "AS400-B": raw_hash,
            },
        }
        reference = ledger.write_snapshot_bundle("intent-1.json", bundle)
        bundle_path = events_path.parent / reference["path"]
        raw_bytes = bundle_path.read_bytes()
        self.assertEqual(hashlib.sha256(raw_bytes).hexdigest(), reference["sha256"])
        self.assertEqual(len(raw_bytes), reference["bytes"])
        message = {
            "protocol": "RAPP/1",
            "kind": "chat",
            "user_input": "DSPLIB",
            "session_id": "a:b",
            "idempotency_key": "c",
            "event_at": "2000-01-01T00:00:00.000001+00:00",
        }
        ledger.append(
            {
                "type": "replicated_chat_intent",
                "message": message,
                "nodes": ["AS400-A", "AS400-B"],
                "snapshot_bundle_path": reference["path"],
                "snapshot_bundle": reference,
                "pre_state_hashes": bundle["pre_state_hashes"],
                "terminal_sequence": 2,
            }
        )
        for node_id in ("AS400-A", "AS400-B"):
            state_path = root / "nodes" / node_id / "state.json"
            state_path.parent.mkdir(parents=True)
            state_path.write_text(
                json.dumps(empty_state(), sort_keys=True, separators=(",", ":")),
                encoding="utf-8",
            )

        canonical = encode_idempotency_identity("a:b", "c")
        with PrivateVNetNeighborhood(root) as recovered:
            states = recovered._snapshots()
            for state in states.values():
                self.assertNotIn("a:b:c", state["idempotency"])
                self.assertIn(canonical, state["idempotency"])
            terminal = recovered.ledger.audit()[-1]["record"]
            migrated_hash = neighborhood_module._digest(states["AS400-A"])
            self.assertEqual(terminal["pre_state_hashes"], bundle["pre_state_hashes"])
            self.assertEqual(
                terminal["restored_state_hashes"],
                {"AS400-A": migrated_hash, "AS400-B": migrated_hash},
            )

        self.assertEqual(bundle_path.read_bytes(), raw_bytes)
        self.assertEqual(
            hashlib.sha256(bundle_path.read_bytes()).hexdigest(),
            reference["sha256"],
        )
        persisted_bundle = json.loads(bundle_path.read_bytes())
        self.assertIn("a:b:c", persisted_bundle["pre_snapshots"]["AS400-A"]["idempotency"])
        self.assertNotIn(
            canonical,
            persisted_bundle["pre_snapshots"]["AS400-A"]["idempotency"],
        )
        with PrivateVNetNeighborhood(root) as reopened:
            self.assertEqual(len(reopened.ledger.audit()), 2)
            self.assertIn(canonical, reopened._snapshots()["AS400-A"]["idempotency"])

        tampered = raw_bytes.replace(b"a:b:c", b"a:b:d", 1)
        self.assertNotEqual(tampered, raw_bytes)
        bundle_path.write_bytes(tampered)
        with self.assertRaisesRegex(Refusal, "digest"):
            neighborhood_module.EvidenceLedger(events_path).read_snapshot_bundle(reference)

    def test_unmatched_intent_recovers_on_open_then_converges_and_replays(self) -> None:
        root = self.work / "crash-recovery"
        intent = self._leave_unmatched_after_first_mutation(root)
        first_state = AtomicStore(root / "nodes" / "AS400-A" / "state.json").snapshot()
        second_state = AtomicStore(root / "nodes" / "AS400-B" / "state.json").snapshot()
        self.assertIn("CRASHED", first_state["libraries"])
        self.assertNotIn("CRASHED", second_state["libraries"])

        with PrivateVNetNeighborhood(root) as recovered:
            snapshots = recovered._snapshots()
            self.assertEqual(snapshots["AS400-A"], snapshots["AS400-B"])
            self.assertEqual(snapshots["AS400-A"]["libraries"], {})
            entries = recovered.ledger.audit()
            self.assertEqual(
                [entry["record"]["type"] for entry in entries],
                ["replicated_chat_intent", "replicated_chat_recovery"],
            )
            terminal = entries[1]
            self.assertEqual(terminal["sequence"], intent["record"]["terminal_sequence"])
            self.assertEqual(terminal["record"]["intent_event_hash"], intent["event_hash"])
            self.assertTrue(terminal["record"]["rollback_verified"])

            receipt = recovered.replicate_chat(
                "CRTLIB LIB(CRASHED)",
                "crash",
                "first-node",
            )
            self.assertTrue(receipt["converged"])
            replay = recovered.replay_and_verify("AS400-B")
            self.assertEqual(replay["events_replayed"], 1)
            self.assertTrue(replay["converged"])

    def test_unmatched_intent_missing_or_tampered_bundle_fails_open_closed(self) -> None:
        for damage in ("missing", "tampered"):
            with self.subTest(damage=damage):
                root = self.work / f"recovery-{damage}"
                intent = self._leave_unmatched_after_first_mutation(root)
                bundle_path = root / "evidence" / intent["record"]["snapshot_bundle"]["path"]
                if damage == "missing":
                    bundle_path.unlink()
                else:
                    encoded = bundle_path.read_bytes()
                    bundle_path.write_bytes(bytes([encoded[0] ^ 1]) + encoded[1:])
                with self.assertRaisesRegex(Refusal, "Snapshot bundle"):
                    PrivateVNetNeighborhood(root)
                ledger = neighborhood_module.EvidenceLedger(root / "evidence" / "events.jsonl")
                self.assertEqual(
                    [entry["record"]["type"] for entry in ledger.read()],
                    ["replicated_chat_intent"],
                )

    def test_unmatched_intent_terminal_capacity_failure_fails_closed_and_retries(self) -> None:
        root = self.work / "recovery-capacity"
        self._leave_unmatched_after_first_mutation(root)
        with mock.patch.object(neighborhood_module, "MAX_EVIDENCE_EVENTS", 1):
            with self.assertRaisesRegex(Refusal, "Recovery terminal evidence append failed"):
                PrivateVNetNeighborhood(root)
        ledger = neighborhood_module.EvidenceLedger(root / "evidence" / "events.jsonl")
        self.assertEqual(len(ledger.read()), 1)
        with PrivateVNetNeighborhood(root) as recovered:
            self.assertEqual(
                [entry["record"]["type"] for entry in recovered.ledger.audit()],
                ["replicated_chat_intent", "replicated_chat_recovery"],
            )

    def test_audit_rejects_orphan_mismatched_and_duplicate_terminals(self) -> None:
        orphan = neighborhood_module.EvidenceLedger(
            self.work / "orphan" / "evidence" / "events.jsonl"
        )
        orphan.append(
            {
                "type": "replicated_chat_commit",
                "intent_sequence": 99,
                "intent_event_hash": "0" * 64,
            }
        )
        with self.assertRaisesRegex(Refusal, "intent link"):
            orphan.audit()

        mismatch_root = self.work / "mismatched"
        intent = self._leave_unmatched_after_first_mutation(mismatch_root)
        mismatch = neighborhood_module.EvidenceLedger(
            mismatch_root / "evidence" / "events.jsonl"
        )
        mismatch.append(
            {
                "type": "replicated_chat_recovery",
                "intent_sequence": intent["sequence"],
                "intent_event_hash": "0" * 64,
            }
        )
        with self.assertRaisesRegex(Refusal, "intent link"):
            mismatch.audit()

        duplicate_root = self.work / "duplicate"
        with PrivateVNetNeighborhood(duplicate_root) as neighborhood:
            neighborhood.replicate_chat("CRTLIB LIB(ONE)", "one", "one")
            duplicate_record = copy.deepcopy(neighborhood.ledger.read()[-1]["record"])
            neighborhood.ledger.append(duplicate_record)
            with self.assertRaisesRegex(Refusal, "intent link"):
                neighborhood.ledger.audit()

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
