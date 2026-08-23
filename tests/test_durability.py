from __future__ import annotations

import io
import json
import os
import shutil
import unittest
from pathlib import Path
from unittest import mock

import rapp_virtual_as400.neighborhood as neighborhood_module
import rapp_virtual_as400.node_worker as worker_module
import rapp_virtual_as400.storage as storage_module
from rapp_virtual_as400 import Refusal
from rapp_virtual_as400.engine import VirtualAS400
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
        self.assertEqual(replaced.call_count, 3)

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
            self.assertRaises(Refusal) as raised,
        ):
            store._write(state)
        self.assertEqual(raised.exception.code, "STORAGE_PUBLICATION_FAILED")
        self.assertEqual(store.path.read_bytes(), original)

        with (
            mock.patch.object(storage_module.os, "name", "nt"),
            mock.patch.object(
                storage_module.os,
                "replace",
                side_effect=OSError("injected replace failure"),
            ),
            self.assertRaises(Refusal) as raised,
        ):
            store._write(state)
        self.assertEqual(raised.exception.code, "STORAGE_PUBLICATION_FAILED")
        self.assertEqual(store.path.read_bytes(), original)

    def test_chmod_failure_precedes_state_publication(self) -> None:
        store = AtomicStore(self.work / "chmod" / "state.json")
        original = store.path.read_bytes()
        state = empty_state()
        state["revision"] = 9
        real_enforce = storage_module.enforce_private_mode
        injected = False

        def fail_after_state_temp_chmod(path, mode):
            nonlocal injected
            real_enforce(path, mode)
            if Path(path) == store.path.with_suffix(store.path.suffix + ".new") and not injected:
                injected = True
                raise OSError("injected chmod failure")

        with (
            mock.patch.object(
                storage_module,
                "enforce_private_mode",
                side_effect=fail_after_state_temp_chmod,
            ),
            self.assertRaises(Refusal) as raised,
        ):
            store._write(state)

        self.assertEqual(raised.exception.code, "STORAGE_PUBLICATION_FAILED")
        self.assertEqual(store.path.read_bytes(), original)
        self.assertFalse(store.recovery_path.exists())

    def test_after_replace_failure_rolls_back_exact_bytes_and_retry_once(self) -> None:
        store = AtomicStore(self.work / "replace" / "state.json")
        original = store.path.read_bytes()
        state = json.loads(original)
        state["libraries"]["ONCE"] = {"files": {}}
        state["revision"] += 1
        real_replace = os.replace
        injected = False

        def fail_after_state_replace(source, destination):
            nonlocal injected
            real_replace(source, destination)
            if Path(destination) == store.path and not injected:
                injected = True
                raise OSError("injected after replace")

        with (
            mock.patch.object(
                storage_module.os,
                "replace",
                side_effect=fail_after_state_replace,
            ),
            self.assertRaises(Refusal) as raised,
        ):
            store._write(state)

        self.assertEqual(raised.exception.code, "STORAGE_PUBLICATION_FAILED")
        self.assertEqual(store.path.read_bytes(), original)
        store._write(state)
        self.assertEqual(store.snapshot()["revision"], 1)
        self.assertEqual(set(store.snapshot()["libraries"]), {"ONCE"})

    def test_after_directory_fsync_failure_rolls_back_exact_bytes(self) -> None:
        store = AtomicStore(self.work / "dirsync" / "state.json")
        original = store.path.read_bytes()
        state = json.loads(original)
        state["revision"] = 1
        real_sync = storage_module.fsync_directory
        calls = 0

        def fail_after_state_directory_sync(path):
            nonlocal calls
            calls += 1
            real_sync(path)
            if calls == 2:
                raise OSError("injected after state directory fsync")

        with (
            mock.patch.object(
                storage_module,
                "fsync_directory",
                side_effect=fail_after_state_directory_sync,
            ),
            self.assertRaises(Refusal) as raised,
        ):
            store._write(state)

        self.assertEqual(raised.exception.code, "STORAGE_PUBLICATION_FAILED")
        self.assertEqual(store.path.read_bytes(), original)
        self.assertFalse(store.recovery_path.exists())

    def test_post_publish_verification_mismatch_fails_closed(self) -> None:
        store = AtomicStore(self.work / "verify" / "state.json")
        original = store.path.read_bytes()
        state = json.loads(original)
        state["revision"] = 1
        real_replace = os.replace

        def corrupt_after_state_replace(source, destination):
            real_replace(source, destination)
            if Path(destination) == store.path:
                store.path.write_bytes(b"not-the-published-bytes")

        with (
            mock.patch.object(
                storage_module.os,
                "replace",
                side_effect=corrupt_after_state_replace,
            ),
            self.assertRaises(Refusal) as raised,
        ):
            store._write(state)

        self.assertEqual(raised.exception.code, "RECOVERY_REQUIRED")
        self.assertTrue(store.recovery_path.exists())
        with self.assertRaises(Refusal) as reopened:
            AtomicStore(store.path)
        self.assertEqual(reopened.exception.code, "RECOVERY_REQUIRED")
        recovered = AtomicStore(store.path, recover=True)
        self.assertEqual(recovered.path.read_bytes(), original)

    def test_rollback_failure_fails_closed_and_restart_recovers_prior(self) -> None:
        store = AtomicStore(self.work / "recovery" / "state.json")
        original = store.path.read_bytes()
        state = json.loads(original)
        state["revision"] = 1
        real_replace = os.replace
        state_replaces = 0

        def fail_publish_and_rollback(source, destination):
            nonlocal state_replaces
            if Path(destination) == store.path:
                state_replaces += 1
                if state_replaces == 1:
                    real_replace(source, destination)
                    raise OSError("injected after replace")
                raise OSError("injected rollback failure")
            return real_replace(source, destination)

        with (
            mock.patch.object(
                storage_module.os,
                "replace",
                side_effect=fail_publish_and_rollback,
            ),
            self.assertRaises(Refusal) as raised,
        ):
            store._write(state)

        self.assertEqual(raised.exception.code, "RECOVERY_REQUIRED")
        self.assertTrue(store.recovery_path.exists())
        self.assertEqual(json.loads(store.recovery_path.read_text())["phase"], "prepared")
        reopened = AtomicStore(store.path)
        self.assertEqual(reopened.path.read_bytes(), original)
        self.assertFalse(reopened.recovery_path.exists())

    def test_restart_rolls_back_unacknowledged_crash_then_retry_is_single_mutation(self) -> None:
        store = AtomicStore(self.work / "crash" / "state.json")
        original = store.path.read_bytes()
        state = json.loads(original)
        state["libraries"]["RETRY"] = {"files": {}}
        state["revision"] = 1
        real_publish = store._publish_file

        def crash_after_state_publish(destination, encoded):
            real_publish(destination, encoded)
            if destination == store.path:
                raise KeyboardInterrupt("simulated process loss")

        with (
            mock.patch.object(store, "_publish_file", side_effect=crash_after_state_publish),
            self.assertRaises(KeyboardInterrupt),
        ):
            store._write(state)

        self.assertTrue(store.recovery_path.exists())
        reopened = AtomicStore(store.path)
        self.assertEqual(reopened.path.read_bytes(), original)
        reopened._write(state)
        self.assertEqual(reopened.snapshot()["revision"], 1)
        self.assertEqual(set(reopened.snapshot()["libraries"]), {"RETRY"})

    def test_unrecognized_live_bytes_refuse_until_explicit_recovery(self) -> None:
        store = AtomicStore(self.work / "explicit" / "state.json")
        original = store.path.read_bytes()
        state = json.loads(original)
        state["revision"] = 1
        prepared = store._journal_bytes("prepared", original, storage_module._serialized_state_bytes(state), True)
        store._publish_file(store.recovery_path, prepared)
        store.path.write_bytes(b"unrecognized")

        with self.assertRaises(Refusal) as raised:
            AtomicStore(store.path)
        self.assertEqual(raised.exception.code, "RECOVERY_REQUIRED")
        recovered = AtomicStore(store.path, recover=True)
        self.assertEqual(recovered.path.read_bytes(), original)
        self.assertFalse(recovered.recovery_path.exists())

    def test_idempotent_retry_after_publication_failure_has_one_mutation(self) -> None:
        engine = VirtualAS400(self.work / "idempotent" / "state.json")
        real_replace = os.replace
        injected = False

        def fail_after_state_replace(source, destination):
            nonlocal injected
            real_replace(source, destination)
            if Path(destination) == engine.store.path and not injected:
                injected = True
                raise OSError("injected after state replace")

        with (
            mock.patch.object(
                storage_module.os,
                "replace",
                side_effect=fail_after_state_replace,
            ),
            self.assertRaises(Refusal) as raised,
        ):
            engine.chat("CRTLIB LIB(ONCE)", "session", "request")

        self.assertEqual(raised.exception.code, "STORAGE_PUBLICATION_FAILED")
        first = engine.chat("CRTLIB LIB(ONCE)", "session", "request")
        second = engine.chat("CRTLIB LIB(ONCE)", "session", "request")
        self.assertEqual(first, second)
        snapshot = engine.store.snapshot()
        self.assertEqual(snapshot["revision"], 2)
        self.assertEqual(set(snapshot["libraries"]), {"ONCE"})
        self.assertEqual(len(snapshot["sessions"]["session"]["turns"]), 1)

    def test_worker_returns_recovery_refusal_when_store_cannot_open(self) -> None:
        output = io.StringIO()
        request = (
            '{"protocol":"RAPP/1","kind":"chat","user_input":"DSPLIB",'
            '"session_id":"worker"}\n'
        )
        unavailable = Refusal(
            storage_module.RECOVERY_REQUIRED_MESSAGE,
            "RECOVERY_REQUIRED",
        )
        with (
            mock.patch.object(worker_module, "VirtualAS400", side_effect=unavailable),
            mock.patch.object(worker_module.sys, "stdin", io.StringIO(request)),
            mock.patch.object(worker_module.sys, "stdout", output),
        ):
            self.assertEqual(worker_module.main(["--root", os.fspath(self.work)]), 0)

        response = json.loads(output.getvalue())
        self.assertEqual(response["error"]["code"], "RECOVERY_REQUIRED")
        self.assertEqual(response["session_id"], "worker")

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
