from __future__ import annotations

import os
import shutil
import unittest
from pathlib import Path

from rapp_virtual_as400 import VirtualAS400


class EngineTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.work = Path(__file__).resolve().parent / ".work" / self.id().replace(".", "_")
        shutil.rmtree(self.work, ignore_errors=True)
        self.work.mkdir(parents=True)
        self.engine = VirtualAS400(self.work / "state.json")

    def tearDown(self) -> None:
        shutil.rmtree(self.work, ignore_errors=True)

    def bootstrap(self) -> None:
        self.engine.chat(
            "CRTLIB LIB(TEST); "
            "CRTPF FILE(TEST/ITEMS) FIELDS(ID:CHAR(8),QTY:INT,PRICE:DECIMAL(10,2),NOTE:CHAR(32))",
            "bootstrap",
        )

    def assert_private_mode(self, path: Path, expected: int) -> None:
        if os.name == "nt":
            self.assertTrue(path.exists())
            return
        self.assertEqual(path.stat().st_mode & 0o777, expected)
