from __future__ import annotations

import os
from unittest import mock

from rapp_virtual_as400.neighborhood import EvidenceLedger
from rapp_virtual_as400.server import RAPPServer
from rapp_virtual_as400.storage import AtomicStore
import rapp_virtual_as400.storage as storage_module

from .support import EngineTestCase


class PlatformPermissionTests(EngineTestCase):
    def test_windows_private_publication_relies_on_inherited_acls(self) -> None:
        if os.name != "nt":
            self.skipTest("Windows ACL contract")

        with mock.patch.object(
            storage_module.os,
            "chmod",
            side_effect=AssertionError("Windows chmod must not be used as an ACL guarantee"),
        ):
            store = AtomicStore(self.work / "windows-state" / "state.json")
            ledger = EvidenceLedger(
                self.work / "windows-evidence" / "evidence" / "events.jsonl"
            )
            reference = ledger.write_snapshot_bundle(
                "intent-1.json",
                {"platform": "windows"},
            )
            server = RAPPServer(
                ("127.0.0.1", 0),
                self.work / "windows-http" / "state.json",
                self.work / "windows-http" / "stop.capability",
            )
            try:
                self.assertTrue(store.path.is_file())
                self.assertTrue(ledger.path.is_file())
                self.assertTrue(
                    (ledger.path.parent / reference["path"]).is_file()
                )
                self.assertTrue(server.capability_path.is_file())
                self.assertFalse(
                    any(server.capability_path.parent.glob(".stop.capability.*.new"))
                )
            finally:
                server.server_close()
