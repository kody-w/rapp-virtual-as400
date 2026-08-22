from __future__ import annotations

from rapp_virtual_as400 import Refusal

from .support import EngineTestCase


class AdversarialTests(EngineTestCase):
    def assert_refused(self, command: object) -> None:
        with self.assertRaises(Refusal):
            self.engine.chat(command, "adversarial")  # type: ignore[arg-type]

    def test_arbitrary_execution_and_non_allowlisted_commands_are_refused(self) -> None:
        for command in [
            "CALL PGM(QCMD)",
            "SQL(DELETE FROM USERS)",
            "PYTHON EVAL('__import__(\"os\")')",
            "SH CMD('rm -rf /')",
            "SELECT * FROM users",
        ]:
            with self.subTest(command=command):
                self.assert_refused(command)

    def test_traversal_network_and_control_sequences_are_refused(self) -> None:
        for command in [
            "CRTLIB LIB(../ESCAPE)",
            "ENQUEUE DTAQ(TEST/Q) DATA('https://example.com/..')",
            "CRTLIB LIB(A\\B)",
            "CRTLIB LIB(A\x00B)",
        ]:
            with self.subTest(command=command):
                self.assert_refused(command)

    def test_malformed_inputs_are_refused(self) -> None:
        for command in ["", " ", "CRTLIB", "CRTLIB LIB(A", "CRTLIB LIB(A) LIB(B)", 42, []]:
            with self.subTest(command=command):
                self.assert_refused(command)

    def test_schema_and_record_limits_are_enforced(self) -> None:
        self.engine.chat("CRTLIB LIB(TEST)", "s")
        self.assert_refused("CRTPF FILE(TEST/BAD) FIELDS(V:CHAR(999))")
        self.engine.chat("CRTPF FILE(TEST/GOOD) FIELDS(V:DECIMAL(3,1))", "s")
        self.assert_refused("INSERT FILE(TEST/GOOD) VALUES(V='100.0')")
        self.assert_refused("INSERT FILE(TEST/GOOD) VALUES(V='NaN')")

    def test_request_identifiers_are_restricted(self) -> None:
        with self.assertRaises(Refusal):
            self.engine.chat("DSPLIB", "../../session", None)
        with self.assertRaises(Refusal):
            self.engine.chat("DSPLIB", "s", "contains spaces")
