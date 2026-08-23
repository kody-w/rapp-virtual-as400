"""Deterministic global-object manifest builder."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


def build_manifest(root: Path) -> Path:
    root = root.resolve()
    package = root / "src" / "rapp_virtual_as400" / "zoo"
    sources = [
        (
            package / "rapp_virtual_as400_agent.py",
            "rapp_virtual_as400/zoo/rapp_virtual_as400_agent.py",
        ),
        (package / "store.v2.json", "rapp_virtual_as400/zoo/store.v2.json"),
        (root / "LICENSE", "rapp_virtual_as400-0.2.0.dist-info/licenses/LICENSE"),
    ]
    artifacts = []
    for path, install_path in sources:
        if not path.is_file() or root not in path.resolve().parents:
            raise ValueError(f"Required manifest input is missing: {path.name}")
        artifacts.append(
            {
                "source_path": path.relative_to(root).as_posix(),
                "install_path": install_path,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "bytes": path.stat().st_size,
            }
        )
    mirrors = [
        (root / "agents" / "rapp_virtual_as400_agent.py", package / "rapp_virtual_as400_agent.py"),
        (root / "store.v2.json", package / "store.v2.json"),
    ]
    for mirror, authority in mirrors:
        if not mirror.is_file() or mirror.read_bytes() != authority.read_bytes():
            raise ValueError(f"Documented source mirror differs from packaged authority: {mirror}")
    manifest = {
        "schema": "rapp.global-objects/v1",
        "name": "rapp-virtual-as400",
        "license_dimension": "MIT",
        "protocol": "RAPP/1",
        "global_objects": artifacts,
        "summon_chant": {
            "ready": True,
            "phrase": "Summon the virtual operations neighborhood.",
            "entrypoint": "rapp-virtual-as400 chat",
        },
    }
    output = root / "global-objects.manifest.json"
    rendered = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    output.write_text(rendered, encoding="utf-8")
    (package / "global-objects.manifest.json").write_text(rendered, encoding="utf-8")
    return output
