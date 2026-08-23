from __future__ import annotations

import importlib.resources
import importlib.util
import json
import os
from pathlib import Path

from rapp_virtual_as400.manifest import build_manifest

from .support import EngineTestCase


ROOT = Path(__file__).resolve().parents[1]


class AgentAndManifestTests(EngineTestCase):
    def test_single_file_agent_is_basic_agent_compatible(self) -> None:
        path = ROOT / "src" / "rapp_virtual_as400" / "zoo" / "rapp_virtual_as400_agent.py"
        spec = importlib.util.spec_from_file_location("virtual_agent", path)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        previous = os.environ.get("RAPP_VIRTUAL_AS400_HOME")
        os.environ["RAPP_VIRTUAL_AS400_HOME"] = str(self.work / "agent-home")
        try:
            agent = module.RAPPVirtualAS400Agent()
            self.assertEqual(agent.metadata["name"], "RAPPVirtualAS400")
            self.assertIn("Library AGENT created", agent.perform("CRTLIB LIB(AGENT)"))
            self.assertIn("REFUSED", agent.perform("CALL PGM(BAD)"))
            self.assertEqual(agent.to_tool()["type"], "function")
        finally:
            if previous is None:
                os.environ.pop("RAPP_VIRTUAL_AS400_HOME", None)
            else:
                os.environ["RAPP_VIRTUAL_AS400_HOME"] = previous

    def test_manifest_is_deterministic_and_store_v2_metadata_is_valid(self) -> None:
        first = build_manifest(ROOT).read_text()
        second = build_manifest(ROOT).read_text()
        self.assertEqual(first, second)
        manifest = json.loads(first)
        store = json.loads((ROOT / "store.v2.json").read_text())
        self.assertEqual(manifest["license_dimension"], "MIT")
        self.assertTrue(manifest["summon_chant"]["ready"])
        self.assertEqual(store["schema_version"], 2)
        package = ROOT / "src" / "rapp_virtual_as400" / "zoo"
        self.assertEqual(
            (ROOT / "agents" / "rapp_virtual_as400_agent.py").read_bytes(),
            (package / "rapp_virtual_as400_agent.py").read_bytes(),
        )
        self.assertEqual((ROOT / "store.v2.json").read_bytes(), (package / "store.v2.json").read_bytes())
        self.assertEqual(
            (ROOT / "global-objects.manifest.json").read_bytes(),
            (package / "global-objects.manifest.json").read_bytes(),
        )
        resources = importlib.resources.files("rapp_virtual_as400.zoo")
        self.assertTrue(resources.joinpath("store.v2.json").is_file())
        self.assertTrue(resources.joinpath("global-objects.manifest.json").is_file())
