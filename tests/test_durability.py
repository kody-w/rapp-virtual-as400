from __future__ import annotations

import os
import shutil
import unittest
from pathlib import Path
from unittest import mock

import rapp_virtual_as400.neighborhood as neighborhood_module
import rapp_virtual_as400.storage as storage_module
from rapp_virtual_as400 import Refusal
from rapp_virtual_as400.storage import AtomicStore, empty_state


class DirectoryDurabilityContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.work = Path(__file__).resolve().parent / ".work" / self.id().replace(".", "_")
        shutil.rmtree(self.work, ignore_errors=True)
        self.work.mkdir(parents=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.work, ignore_errors=True)

    def test_posix_directory_sync_opens_fsyncs_and_closes_directory(self) -> None:
        with (
            mock.patch.object(storage_module.os, "open", return_value=73) as opened,
            mock.patch.object(storage_module.os, "fsync") as fsynced,
            mock.patch.object(storage_module.os, "close") as closed,
            mock.patch.object(storage_module.os, "name", "posix"),
        ):
            storage_module.fsync_directory(self.work)

        opened.assert_called_once_with(
            self.work,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        fsynced.assert_called_once_with(73)
        closed.assert_called_once_with(73)

    def test_simulated_windows_directory_sync_never_opens_directory(self) -> None:
        with (
            mock.patch.object(storage_module.os, "open") as opened,
            mock.patch.object(storage_module.os, "fsync") as fsynced,
            mock.patch.object(storage_module.os, "name", "nt"),
        ):
            storage_module.fsync_directory(self.work)

        opened.assert_not_called()
        fsynced.assert_not_called()

    def test_simulated_windows_never_treats_posix_modes_as_acl_guarantees(self) -> None:
        with (
            mock.patch.object(storage_module.os, "chmod") as chmod,
            mock.patch.object(storage_module.os, "name", "nt"),
        ):
            storage_module.enforce_private_mode(self.work, 0o700)
            mismatch = storage_module.private_mode_mismatch(0o777, 0o600)

        chmod.assert_not_called()
        self.assertFalse(mismatch)

    def test_simulated_windows_atomic_store_flushes_file_and_replaces(self) -> None:
        store = AtomicStore(self.work / "store" / "state.json")
        state = empty_state()
        state["revision"] = 7
        real_open = os.open
        real_fsync = os.fsync
        real_replace = os.replace
        opened_paths: list[str] = []

        def guarded_open(path, flags, mode=0o777, *, dir_fd=None):
            opened_paths.append(os.fspath(path))
            self.assertNotEqual(os.fspath(path), os.fspath(store.path.parent))
            return real_open(path, flags, mode, dir_fd=dir_fd)

        with (
            mock.patch.object(storage_module.os, "open", side_effect=guarded_open),
            mock.patch.object(storage_module.os, "fsync", wraps=real_fsync) as fsynced,
            mock.patch.object(storage_module.os, "replace", wraps=real_replace) as replaced,
            mock.patch.object(storage_module.os, "name", "nt"),
        ):
            store._write(state)

        self.assertEqual(store.snapshot()["revision"], 7)
        self.assertTrue(any(path.endswith(".new") for path in opened_paths))
        fsynced.assert_called()
        replaced.assert_called_once()

    def test_simulated_windows_surfaces_file_flush_and_replace_errors(self) -> None:
        store = AtomicStore(self.work / "errors" / "state.json")
        original = store.path.read_bytes()
        state = empty_state()
        state["revision"] = 9

        with (
            mock.patch.object(storage_module.os, "name", "nt"),
            mock.patch.object(
                storage_module.os,
                "fsync",
                side_effect=OSError("injected file flush failure"),
            ),
            self.assertRaisesRegex(OSError, "file flush failure"),
        ):
            store._write(state)
        self.assertEqual(store.path.read_bytes(), original)

        with (
            mock.patch.object(storage_module.os, "name", "nt"),
            mock.patch.object(
                storage_module.os,
                "replace",
                side_effect=OSError("injected replace failure"),
            ),
            self.assertRaisesRegex(OSError, "replace failure"),
        ):
            store._write(state)
        self.assertEqual(store.path.read_bytes(), original)

    def test_simulated_windows_snapshot_publication_is_flush_first_and_no_clobber(self) -> None:
        ledger = neighborhood_module.EvidenceLedger(self.work / "ledger" / "events.jsonl")
        snapshots = os.fspath(ledger.path.parent / "snapshots")
        evidence = os.fspath(ledger.path.parent)
        real_open = os.open
        real_fsync = os.fsync
        real_link = os.link
        opened_paths: list[str] = []

        def guarded_open(path, flags, mode=0o777, *, dir_fd=None):
            opened_paths.append(os.fspath(path))
            self.assertNotIn(os.fspath(path), {snapshots, evidence})
            return real_open(path, flags, mode, dir_fd=dir_fd)

        first = {"pre_snapshots": {}, "pre_state_hashes": {}}
        with (
            mock.patch.object(neighborhood_module.os, "open", side_effect=guarded_open),
            mock.patch.object(neighborhood_module.os, "fsync", wraps=real_fsync) as fsynced,
            mock.patch.object(neighborhood_module.os, "link", wraps=real_link) as linked,
            mock.patch.object(neighborhood_module.os, "name", "nt"),
        ):
            reference = ledger.write_snapshot_bundle("intent-1.json", first)

        destination = ledger.path.parent / reference["path"]
        original = destination.read_bytes()
        self.assertTrue(any(path.endswith(".tmp") for path in opened_paths))
        fsynced.assert_called()
        linked.assert_called_once()

        with self.assertRaisesRegex(Refusal, "immutable"):
            ledger.write_snapshot_bundle(
                "intent-1.json",
                {"pre_snapshots": {"CHANGED": empty_state()}, "pre_state_hashes": {}},
            )
        self.assertEqual(destination.read_bytes(), original)

    def test_simulated_windows_evidence_uses_binary_descriptors(self) -> None:
        ledger = neighborhood_module.EvidenceLedger(
            self.work / "binary-ledger" / "events.jsonl"
        )
        binary_flag = 0x8000
        real_open = os.open
        evidence_flags: list[int] = []

        def windows_open(path, flags, mode=0o777, *, dir_fd=None):
            if os.fspath(path) == os.fspath(ledger.path):
                evidence_flags.append(flags)
            return real_open(path, flags & ~binary_flag, mode, dir_fd=dir_fd)

        with (
            mock.patch.object(
                neighborhood_module.os,
                "O_BINARY",
                binary_flag,
                create=True,
            ),
            mock.patch.object(
                neighborhood_module.os,
                "open",
                side_effect=windows_open,
            ),
        ):
            ledger.append({"type": "binary"})

        self.assertTrue(evidence_flags)
        self.assertTrue(all(flags & binary_flag for flags in evidence_flags))


if __name__ == "__main__":
    unittest.main()
