from __future__ import annotations

import json
import os

from rapp_virtual_as400 import Refusal

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

    def test_display_empty_file(self) -> None:
        self.bootstrap()
        self.assertIn("Records: 0", self.engine.chat("DISPLAY FILE(TEST/ITEMS)", "s")["response"])
