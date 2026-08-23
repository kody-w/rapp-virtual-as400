from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class WheelInstallTests(unittest.TestCase):
    def test_clean_venv_loads_adapter_and_metadata_from_installed_bytes(self) -> None:
        work = ROOT / "tests" / ".work" / "wheel-install"
        shutil.rmtree(work, ignore_errors=True)
        work.mkdir(parents=True)
        try:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "wheel",
                    str(ROOT),
                    "--no-deps",
                    "--wheel-dir",
                    str(work / "dist"),
                ],
                check=True,
                cwd=work,
                stdout=subprocess.DEVNULL,
            )
            wheel = next((work / "dist").glob("rapp_virtual_as400-*.whl"))
            subprocess.run([sys.executable, "-m", "venv", str(work / "venv")], check=True)
            python = work / "venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
            subprocess.run(
                [str(python), "-m", "pip", "install", "--no-deps", str(wheel)],
                check=True,
                cwd=work,
                stdout=subprocess.DEVNULL,
            )
            authority = ROOT / "src" / "rapp_virtual_as400" / "zoo" / "rapp_virtual_as400_agent.py"
            expected_hash = hashlib.sha256(authority.read_bytes()).hexdigest()
            script = f"""
import hashlib
import importlib
import importlib.resources
import inspect
import json
import os
from pathlib import Path

resources = importlib.resources.files("rapp_virtual_as400.zoo")
store = json.loads(resources.joinpath("store.v2.json").read_text())
manifest = json.loads(resources.joinpath("global-objects.manifest.json").read_text())
module_name, class_name = store["agent"].split(":", 1)
RAPPVirtualAS400Agent = getattr(importlib.import_module(module_name), class_name)
source = Path(inspect.getsourcefile(RAPPVirtualAS400Agent))
assert hashlib.sha256(source.read_bytes()).hexdigest() == {expected_hash!r}
assert manifest["license_dimension"] == "MIT"
os.environ["RAPP_VIRTUAL_AS400_HOME"] = {str(work / "installed-home")!r}
agent = RAPPVirtualAS400Agent()
assert "Library WHEEL created." in agent.perform("CRTLIB LIB(WHEEL)")
assert agent.to_tool()["type"] == "function"
"""
            environment = os.environ.copy()
            environment.pop("PYTHONPATH", None)
            subprocess.run([str(python), "-c", script], check=True, cwd=work, env=environment)
        finally:
            shutil.rmtree(work, ignore_errors=True)
