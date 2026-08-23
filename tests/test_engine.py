from __future__ import annotations

import json
import os

from rapp_virtual_as400 import Refusal, VirtualAS400
from rapp_virtual_as400.storage import AtomicStore

from .support import EngineTestCase


class EngineTests(EngineTestCase):
    def test_library_file_records_update_select_and_display(self) -> None:
        self.bootstrap()
        self.engine.chat(
            "INSERT FILE(TEST/ITEMS) VALUES(ID='A1',QTY='2',PRICE='10.20',NOTE='synthetic')",
            "s",
        )
        result = self.engine.chat(
            "UPDATE FILE(TEST/ITEMS) SET(QTY='3') WHERE(ID='A1'); "
            "SELECT FILE(TEST/ITEMS) WHERE(ID='A1'); DISPLAY FILE(TEST/ITEMS)",
            "s",
        )
        self.assertIn('"QTY":"3"', result["response"])
        self.assertIn("DISPLAY TEST/ITEMS", result["response"])
        self.assertEqual(set(result), {"response", "agent_logs", "session_id"})

    def test_decimal_is_stored_as_exact_string(self) -> None:
        self.bootstrap()
        self.engine.chat(
            "INSERT FILE(TEST/ITEMS) VALUES(ID='A1',QTY='1',PRICE='0.10',NOTE='safe')",
            "s",
        )
        state = self.engine.store.snapshot()
        self.assertEqual(state["libraries"]["TEST"]["files"]["ITEMS"]["records"][0]["PRICE"], "0.10")
        json.dumps(state)

    def test_where_values_are_schema_canonicalized_for_select_update_delete(self) -> None:
        self.bootstrap()
        self.engine.chat(
            "INSERT FILE(TEST/ITEMS) VALUES(ID='A1',QTY='3',PRICE='10.20',NOTE='safe'); "
            "INSERT FILE(TEST/ITEMS) VALUES(ID='A2',QTY='4',PRICE='20.00',NOTE='keep')",
            "s",
        )
        selected = self.engine.chat("SELECT FILE(TEST/ITEMS) WHERE(QTY='03',PRICE='10.2')", "s")
        self.assertIn('"ID":"A1"', selected["response"])
        updated = self.engine.chat(
            "UPDATE FILE(TEST/ITEMS) SET(NOTE='updated') WHERE(QTY='003')", "s"
        )
        self.assertIn("1 record(s) updated", updated["response"])
        deleted = self.engine.chat("DELETE FILE(TEST/ITEMS) WHERE(PRICE='10.20')", "s")
        self.assertIn("1 record(s) deleted", deleted["response"])
        remaining = self.engine.chat("SELECT FILE(TEST/ITEMS)", "s")["response"]
        self.assertNotIn('"ID":"A1"', remaining)
        self.assertIn('"ID":"A2"', remaining)

    def test_unknown_where_fields_are_refused_in_select_update_delete(self) -> None:
        self.bootstrap()
        for command in [
            "SELECT FILE(TEST/ITEMS) WHERE(UNKNOWN='x')",
            "UPDATE FILE(TEST/ITEMS) SET(QTY='1') WHERE(UNKNOWN='x')",
            "DELETE FILE(TEST/ITEMS) WHERE(UNKNOWN='x')",
        ]:
            with self.subTest(command=command), self.assertRaisesRegex(Refusal, "Unknown field"):
                self.engine.chat(command, "s")

    def test_batch_rolls_back_all_mutations(self) -> None:
        with self.assertRaises(Refusal):
            self.engine.chat("CRTLIB LIB(ROLLBACK); INSERT FILE(ROLLBACK/MISSING) VALUES(A='x')", "s")
        self.assertNotIn("ROLLBACK", self.engine.store.snapshot()["libraries"])

    def test_idempotency_and_sessions_persist(self) -> None:
        first = self.engine.chat("CRTLIB LIB(ONCE)", "session-a", "key-1")
        second = self.engine.chat("CRTLIB LIB(ONCE)", "session-a", "key-1")
        self.assertEqual(first, second)
        with self.assertRaisesRegex(Refusal, "different input"):
            self.engine.chat("CRTLIB LIB(OTHER)", "session-a", "key-1")
        self.assertEqual(len(self.engine.store.snapshot()["sessions"]["session-a"]["turns"]), 1)

    def test_private_file_modes(self) -> None:
        mode = os.stat(self.work / "state.json").st_mode & 0o777
        directory_mode = os.stat(self.work).st_mode & 0o777
        self.assertEqual(mode, 0o600)
        self.assertEqual(directory_mode, 0o700)

    def test_data_queue_job_queue_and_spool_report(self) -> None:
        self.bootstrap()
        output = self.engine.chat(
            "CRTDTAQ DTAQ(TEST/EVENTS); ENQUEUE DTAQ(TEST/EVENTS) DATA('hello'); "
            "DEQUEUE DTAQ(TEST/EVENTS); CRTJOBQ JOBQ(TEST/BATCH); "
            "SUBMIT JOBQ(TEST/BATCH) CMD(\"INSERT FILE(TEST/ITEMS) "
            "VALUES(ID='J1',QTY='1',PRICE='9.99',NOTE='job')\"); "
            "WORK JOBQ(TEST/BATCH); RUN JOB(J000001); "
            "PRINT FILE(TEST/ITEMS) TITLE('Synthetic Inventory')",
            "s",
        )["response"]
        self.assertIn("TEST/EVENTS: hello", output)
        self.assertIn("Job J000001 COMPLETE", output)
        self.assertIn("Spool report S000001", output)
        self.assertIn("Synthetic Inventory", output)

    def test_six_digit_job_and_spool_identifier_exhaustion_is_stable(self) -> None:
        self.engine.chat(
            "CRTLIB LIB(TEST); CRTPF FILE(TEST/ITEMS) FIELDS(ID:CHAR(1)); "
            "CRTJOBQ JOBQ(TEST/BATCH)",
            "setup",
        )
        boundary = self.engine.store.snapshot()
        boundary["jobs"]["J999997"] = {
            "queue": "TEST/BATCH",
            "command": "DSPLIB",
            "status": "COMPLETE",
            "result": "complete",
        }
        boundary["next_job"] = 999998
        boundary["spool"] = [
            {
                "id": "S999997",
                "title": "Boundary",
                "created_at": "2000-01-01T00:00:00+00:00",
                "report": "boundary",
            }
        ]
        boundary["next_spool"] = 999998
        self.engine.store.restore(boundary)

        at_999998 = self.engine.chat(
            'SUBMIT JOBQ(TEST/BATCH) CMD("DSPLIB"); PRINT FILE(TEST/ITEMS)',
            "boundary",
        )
        self.assertIn("J999998", at_999998["response"])
        self.assertIn("S999998", at_999998["response"])
        at_999999 = self.engine.chat(
            'SUBMIT JOBQ(TEST/BATCH) CMD("DSPLIB"); PRINT FILE(TEST/ITEMS)',
            "boundary",
        )
        self.assertIn("J999999", at_999999["response"])
        self.assertIn("S999999", at_999999["response"])

        terminal = self.engine.store.snapshot()
        self.assertEqual((terminal["next_job"], terminal["next_spool"]), (1000000, 1000000))
        self.assertEqual(AtomicStore.validate_snapshot(terminal), terminal)
        restored = VirtualAS400(self.work / "terminal" / "state.json")
        restored.store.restore(terminal)
        for command, message in (
            ('SUBMIT JOBQ(TEST/BATCH) CMD("DSPLIB")', "Job identifier space exhausted"),
            ("PRINT FILE(TEST/ITEMS)", "Spool identifier space exhausted"),
        ):
            before = restored.store.snapshot()
            with self.subTest(command=command), self.assertRaisesRegex(Refusal, message) as caught:
                restored.chat(command, "exhausted")
            self.assertEqual(caught.exception.code, "LIMIT_EXCEEDED")
            self.assertEqual(restored.store.snapshot(), before)

    def test_display_empty_file(self) -> None:
        self.bootstrap()
        self.assertIn("Records: 0", self.engine.chat("DISPLAY FILE(TEST/ITEMS)", "s")["response"])
