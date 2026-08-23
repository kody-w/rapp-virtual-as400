from __future__ import annotations

import copy
import json

from rapp_virtual_as400 import PrivateVNetNeighborhood, Refusal, VirtualAS400
from rapp_virtual_as400.storage import (
    AtomicStore,
    empty_state,
    encode_idempotency_identity,
)

from .support import EngineTestCase


class RestoreValidationTests(EngineTestCase):
    def _rich_snapshot(self) -> dict:
        self.engine.chat(
            "CRTLIB LIB(TEST); "
            "CRTPF FILE(TEST/ITEMS) FIELDS(ID:CHAR(8),QTY:INT,PRICE:DECIMAL(10,2)); "
            "INSERT FILE(TEST/ITEMS) VALUES(ID='A1',QTY='2',PRICE='10.20'); "
            "CRTDTAQ DTAQ(TEST/EVENTS); ENQUEUE DTAQ(TEST/EVENTS) DATA('ready'); "
            "CRTJOBQ JOBQ(TEST/BATCH); "
            "SUBMIT JOBQ(TEST/BATCH) CMD(\"DISPLAY FILE(TEST/ITEMS)\"); "
            "PRINT FILE(TEST/ITEMS) TITLE('Restore Corpus')",
            "restore",
            "restore-1",
        )
        return self.engine.store.snapshot()

    def test_malformed_snapshot_corpus_is_refused(self) -> None:
        base = self._rich_snapshot()
        cases: dict[str, dict] = {}

        missing_job = copy.deepcopy(base)
        missing_job["jobs"] = {}
        missing_job["next_job"] = 1
        cases["queue references missing job"] = missing_job

        missing_queue_entry = copy.deepcopy(base)
        missing_queue_entry["job_queues"]["TEST/BATCH"] = []
        cases["queued job absent from queue"] = missing_queue_entry

        wrong_status = copy.deepcopy(base)
        wrong_status["jobs"]["J000001"]["status"] = "READY"
        cases["ready job remains queued"] = wrong_status

        missing_object_library = copy.deepcopy(base)
        missing_object_library["data_queues"]["MISSING/QUEUE"] = []
        cases["queue references missing library"] = missing_object_library

        bad_counter = copy.deepcopy(base)
        bad_counter["next_job"] = 1
        cases["reused job counter"] = bad_counter

        bad_name = copy.deepcopy(base)
        bad_name["libraries"]["TEST"]["files"]["bad-name"] = bad_name["libraries"]["TEST"][
            "files"
        ].pop("ITEMS")
        cases["invalid object name"] = bad_name

        bad_record = copy.deepcopy(base)
        bad_record["libraries"]["TEST"]["files"]["ITEMS"]["records"][0]["QTY"] = "02"
        cases["noncanonical record value"] = bad_record

        bad_record_shape = copy.deepcopy(base)
        del bad_record_shape["libraries"]["TEST"]["files"]["ITEMS"]["records"][0]["PRICE"]
        cases["missing record field"] = bad_record_shape

        bad_char_scale = copy.deepcopy(base)
        bad_char_scale["libraries"]["TEST"]["files"]["ITEMS"]["fields"][0]["scale"] = 2
        cases["CHAR field with scale"] = bad_char_scale

        bad_job_missing_clause = copy.deepcopy(base)
        bad_job_missing_clause["jobs"]["J000001"]["command"] = "CRTLIB"
        cases["job command missing required clause"] = bad_job_missing_clause

        bad_job_extra_clause = copy.deepcopy(base)
        bad_job_extra_clause["jobs"]["J000001"]["command"] = "CRTLIB LIB(NEVER) EXTRA(x)"
        cases["job command with unsupported clause"] = bad_job_extra_clause

        too_many_records = copy.deepcopy(base)
        record = too_many_records["libraries"]["TEST"]["files"]["ITEMS"]["records"][0]
        too_many_records["libraries"]["TEST"]["files"]["ITEMS"]["records"] = [
            copy.deepcopy(record) for _ in range(1001)
        ]
        cases["record limit"] = too_many_records

        bad_revision = copy.deepcopy(base)
        bad_revision["revision"] = 0
        cases["active zero revision"] = bad_revision

        bad_spool_counter = copy.deepcopy(base)
        bad_spool_counter["next_spool"] = 1
        cases["reused spool counter"] = bad_spool_counter

        exhausted_job_counter = copy.deepcopy(base)
        exhausted_job_counter["next_job"] = 1000001
        cases["job counter beyond exhausted state"] = exhausted_job_counter

        exhausted_spool_counter = copy.deepcopy(base)
        exhausted_spool_counter["next_spool"] = 1000001
        cases["spool counter beyond exhausted state"] = exhausted_spool_counter

        bad_cache = copy.deepcopy(base)
        cached = next(iter(bad_cache["idempotency"].values()))
        cached["request_hash"] = "not-a-digest"
        cases["invalid idempotency digest"] = bad_cache

        for name, snapshot in cases.items():
            with self.subTest(name=name), self.assertRaises(Refusal):
                AtomicStore.validate_snapshot(snapshot)

    def test_generated_snapshot_restores_and_allowlisted_job_flow_stays_safe(self) -> None:
        snapshot = self._rich_snapshot()
        self.assertEqual(AtomicStore.validate_snapshot(snapshot), snapshot)
        target = VirtualAS400(self.work / "restored" / "state.json")
        target.store.restore(snapshot)
        worked = target.chat("WORK JOBQ(TEST/BATCH)", "after")
        self.assertIn("READY", worked["response"])
        ran = target.chat("RUN JOB(J000001)", "after")
        self.assertIn("COMPLETE", ran["response"])
        for command in (
            "DSPLIB",
            "SELECT FILE(TEST/ITEMS)",
            "DISPLAY FILE(TEST/ITEMS)",
            "DEQUEUE DTAQ(TEST/EVENTS)",
            "PRINT FILE(TEST/ITEMS)",
        ):
            with self.subTest(command=command):
                response = target.chat(command, "after")
                self.assertEqual(set(response), {"response", "agent_logs", "session_id"})

    def test_legacy_idempotency_identity_migrates_or_fails_closed(self) -> None:
        self.engine.chat("CRTLIB LIB(LEGACY)", "a:b", "c")
        state = self.engine.store.snapshot()
        canonical = encode_idempotency_identity("a:b", "c")
        cached = state["idempotency"].pop(canonical)
        state["idempotency"]["a:b:c"] = cached

        migrated = AtomicStore.validate_snapshot(state)
        self.assertNotIn("a:b:c", migrated["idempotency"])
        self.assertEqual(migrated["idempotency"][canonical], cached)
        restored_path = self.work / "legacy" / "state.json"
        restored_path.parent.mkdir()
        restored_path.write_text(
            json.dumps(state, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        restored = VirtualAS400(restored_path)
        self.assertEqual(
            restored.chat("CRTLIB LIB(LEGACY)", "a:b", "c")["session_id"],
            "a:b",
        )
        self.assertIn(canonical, json.loads(restored_path.read_text())["idempotency"])

        irreconcilable = copy.deepcopy(state)
        irreconcilable["idempotency"]["a:b:c"]["result"]["session_id"] = "z"
        with self.assertRaisesRegex(Refusal, "ambiguous legacy"):
            AtomicStore.validate_snapshot(irreconcilable)

        conflicting = copy.deepcopy(state)
        conflicting["idempotency"][canonical] = copy.deepcopy(cached)
        conflicting["idempotency"][canonical]["request_hash"] = "0" * 64
        with self.assertRaisesRegex(Refusal, "conflicting"):
            AtomicStore.validate_snapshot(conflicting)

    def test_worker_converts_unexpected_engine_error_to_stable_refusal_and_survives(self) -> None:
        with PrivateVNetNeighborhood(self.work / "worker") as neighborhood:
            node = neighborhood.nodes["AS400-A"]
            corrupt = empty_state()
            corrupt["revision"] = 1
            corrupt["libraries"]["TEST"] = {"files": {}}
            corrupt["job_queues"]["TEST/BATCH"] = ["J999999"]
            state_path = node.root / "state.json"
            state_path.write_text(
                json.dumps(corrupt, sort_keys=True, separators=(",", ":")),
                encoding="utf-8",
            )
            response = node.request(
                {
                    "protocol": "RAPP/1",
                    "kind": "chat",
                    "user_input": "WORK JOBQ(TEST/BATCH)",
                    "session_id": "worker",
                    "idempotency_key": "worker",
                }
            )
            self.assertEqual(response["error"]["code"], "WORKER_ERROR")
            self.assertEqual(
                response["error"]["message"],
                "Worker could not safely process the request.",
            )
            restored = node.request(
                {
                    "protocol": "RAPP/1",
                    "kind": "control",
                    "operation": "restore",
                    "state": empty_state(),
                }
            )
            self.assertEqual(restored["status"], "ok")
            self.assertIsNone(node._process.poll())
