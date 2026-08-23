from __future__ import annotations

import os

from rapp_virtual_as400 import PrivateVNetNeighborhood, Refusal

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
            self.assertEqual(len(neighborhood.ledger.read()), 2)

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
