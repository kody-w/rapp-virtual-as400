#!/usr/bin/env python3
"""Small dependency-free mutation gate for safety and transactional invariants."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / ".mutation-work"


@dataclass(frozen=True)
class Mutation:
    name: str
    file: str
    old: str
    new: str
    probe: str


MUTATIONS = [
    Mutation(
        "traversal-guard",
        "parser.py",
        'if "\\x00" in text or ".." in text or "\\\\" in text:',
        'if "\\x00" in text:',
        (
            "from rapp_virtual_as400.parser import parse_batch\n"
            "from rapp_virtual_as400 import Refusal\n"
            "try: parse_batch(\"DSPLIB LIB('..')\")\n"
            "except Refusal: raise SystemExit(0)\n"
            "raise SystemExit(1)\n"
        ),
    ),
    Mutation(
        "idempotency-conflict",
        "engine.py",
        'if cached["request_hash"] != request_hash:',
        'if cached["request_hash"] == request_hash:',
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400 import VirtualAS400\n"
            "e=VirtualAS400(Path('state.json'))\n"
            "e.chat('CRTLIB LIB(ONCE)','s','k')\n"
            "e.chat('CRTLIB LIB(ONCE)','s','k')\n"
        ),
    ),
    Mutation(
        "record-limit",
        "engine.py",
        'if len(file["records"]) >= MAX_RECORDS_PER_FILE:',
        'if len(file["records"]) > MAX_RECORDS_PER_FILE:',
        (
            "from pathlib import Path\n"
            "import rapp_virtual_as400.engine as m\n"
            "from rapp_virtual_as400 import VirtualAS400, Refusal\n"
            "m.MAX_RECORDS_PER_FILE=0\n"
            "e=VirtualAS400(Path('state.json'))\n"
            "e.chat('CRTLIB LIB(T); CRTPF FILE(T/F) FIELDS(A:CHAR(1))','s')\n"
            "try: e.chat(\"INSERT FILE(T/F) VALUES(A='x')\",'s')\n"
            "except Refusal: raise SystemExit(0)\n"
            "raise SystemExit(1)\n"
        ),
    ),
]


def run_probe(package_root: Path, probe: str) -> int:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(package_root)
    return subprocess.run(
        [sys.executable, "-c", probe],
        cwd=package_root,
        env=environment,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ).returncode


def main() -> int:
    shutil.rmtree(WORK, ignore_errors=True)
    failures: list[str] = []
    try:
        for mutation in MUTATIONS:
            case = WORK / mutation.name
            package = case / "rapp_virtual_as400"
            shutil.copytree(ROOT / "src" / "rapp_virtual_as400", package)
            source = package / mutation.file
            text = source.read_text(encoding="utf-8")
            if text.count(mutation.old) != 1:
                failures.append(f"{mutation.name}: mutation target not unique")
                continue
            if run_probe(case, mutation.probe) != 0:
                failures.append(f"{mutation.name}: baseline probe failed")
                continue
            source.write_text(text.replace(mutation.old, mutation.new), encoding="utf-8")
            if run_probe(case, mutation.probe) == 0:
                failures.append(f"{mutation.name}: mutant survived")
            else:
                print(f"KILLED {mutation.name}")
    finally:
        shutil.rmtree(WORK, ignore_errors=True)
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print(f"Mutation gate passed: {len(MUTATIONS)}/{len(MUTATIONS)} killed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
