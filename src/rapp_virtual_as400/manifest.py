"""Deterministic global-object manifest builder."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


def build_manifest(root: Path) -> Path:
    root = root.resolve()
    paths = [
        root / "agents" / "rapp_virtual_as400_agent.py",
        root / "store.v2.json",
        root / "LICENSE",
    ]
    artifacts = []
    for path in paths:
        if not path.is_file() or root not in path.resolve().parents:
            raise ValueError(f"Required manifest input is missing: {path.name}")
        artifacts.append(
            {
                "path": path.relative_to(root).as_posix(),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "bytes": path.stat().st_size,
            }
        )
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
    output.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return output
