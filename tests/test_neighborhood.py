from __future__ import annotations

import copy
import json
import os
from unittest import mock

from rapp_virtual_as400 import PrivateVNetNeighborhood, Refusal
from rapp_virtual_as400.storage import AtomicStore
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
            self.assertEqual(failure["pre_snapshots"], before)
            self.assertTrue(failure["rollback_verified"])
            snapshot_file = (
                neighborhood.ledger.path.parent
                / entries[0]["record"]["pre_event_snapshot_file"]
            )
            bundle = json.loads(snapshot_file.read_text(encoding="utf-8"))
            self.assertEqual(bundle["pre_snapshots"], before)
            self.assertEqual(bundle["intent_event_hash"], entries[0]["event_hash"])
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
