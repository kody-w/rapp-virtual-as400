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
    Mutation(
        "decimal-local-context",
        "engine.py",
        "context.prec = precision",
        "context.prec = 28",
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400 import VirtualAS400\n"
            "e=VirtualAS400(Path('state.json'))\n"
            "v='9'*38\n"
            "e.chat('CRTLIB LIB(T); CRTPF FILE(T/F) FIELDS(V:DECIMAL(38,0))','s')\n"
            "e.chat(f\"INSERT FILE(T/F) VALUES(V='{v}')\",'s')\n"
        ),
    ),
    Mutation(
        "where-schema-canonicalization",
        "engine.py",
        "return self._coerce_record(file, parse_pairs(where), partial=True)",
        "return parse_pairs(where)",
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400 import VirtualAS400\n"
            "e=VirtualAS400(Path('state.json'))\n"
            "e.chat('CRTLIB LIB(T); CRTPF FILE(T/F) FIELDS(V:INT)','s')\n"
            "e.chat(\"INSERT FILE(T/F) VALUES(V='3')\",'s')\n"
            "r=e.chat(\"SELECT FILE(T/F) WHERE(V='03')\",'s')\n"
            "raise SystemExit(0 if '\\\"V\\\":\\\"3\\\"' in r['response'] else 1)\n"
        ),
    ),
    Mutation(
        "unicode-surrogate-guard",
        "unicode_safe.py",
        'decode("utf-16-le")',
        'decode("utf-16-le", "surrogatepass")',
        (
            "from rapp_virtual_as400 import Refusal\n"
            "from rapp_virtual_as400.unicode_safe import canonical_unicode\n"
            "try: canonical_unicode(chr(0xd800))\n"
            "except Refusal: raise SystemExit(0)\n"
            "raise SystemExit(1)\n"
        ),
    ),
    Mutation(
        "replica-upper-bound",
        "neighborhood.py",
        "not 1 <= replicas <= MAX_REPLICAS",
        "not 1 <= replicas < MAX_REPLICAS",
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400 import PrivateVNetNeighborhood\n"
            "with PrivateVNetNeighborhood(Path('vnet')) as n:\n"
            " n.run_replicated_job({'name':'BOUND','payload':{}},replicas=100)\n"
        ),
    ),
    Mutation(
        "stochastic-positive-quorum",
        "neighborhood.py",
        "not 1 <= quorum <= replicas",
        "not 0 <= quorum <= replicas",
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400 import PrivateVNetNeighborhood, Refusal\n"
            "with PrivateVNetNeighborhood(Path('vnet')) as n:\n"
            " try: n.run_replicated_job({'name':'QUORUM','payload':{}},replicas=2,mode='stochastic',quorum=0)\n"
            " except Refusal: raise SystemExit(0)\n"
            "raise SystemExit(1)\n"
        ),
    ),
    Mutation(
        "deterministic-convergence-gate",
        "neighborhood.py",
        "if not accepted:",
        "if accepted:",
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400 import PrivateVNetNeighborhood\n"
            "with PrivateVNetNeighborhood(Path('vnet')) as n:\n"
            " n.run_replicated_job({'name':'SAME','payload':{}},replicas=2)\n"
        ),
    ),
    Mutation(
        "append-only-evidence",
        "neighborhood.py",
        "os.O_WRONLY | os.O_APPEND",
        "os.O_WRONLY | os.O_TRUNC",
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400.neighborhood import EvidenceLedger\n"
            "p=Path('evidence.jsonl'); ledger=EvidenceLedger(p)\n"
            "ledger.append({'type':'one'}); ledger.append({'type':'two'})\n"
            "raise SystemExit(0 if len(ledger.read()) == 2 else 1)\n"
        ),
    ),
    Mutation(
        "replication-evidence-capacity",
        "neighborhood.py",
        "with self._replication_lock, self.ledger.reserve(2):",
        "with self._replication_lock:",
        (
            "from pathlib import Path\n"
            "import rapp_virtual_as400.neighborhood as m\n"
            "from rapp_virtual_as400 import PrivateVNetNeighborhood, Refusal\n"
            "m.MAX_EVIDENCE_EVENTS=1\n"
            "with PrivateVNetNeighborhood(Path('vnet')) as n:\n"
            " for node in n.nodes.values():\n"
            "  original=node.request\n"
            "  def guarded(message, original=original):\n"
            "   if message.get('operation')=='stop': return original(message)\n"
            "   raise SystemExit(2)\n"
            "  node.request=guarded\n"
            " try: n.replicate_chat('CRTLIB LIB(FULL)','full','full')\n"
            " except Refusal: raise SystemExit(0)\n"
            "raise SystemExit(1)\n"
        ),
    ),
    Mutation(
        "replication-durable-intent",
        "neighborhood.py",
        '"type": "replicated_chat_intent",',
        '"type": "replicated_chat_missing",',
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400 import PrivateVNetNeighborhood\n"
            "with PrivateVNetNeighborhood(Path('vnet')) as n:\n"
            " for node in n.nodes.values():\n"
            "  original=node.request\n"
            "  def guarded(message, original=original):\n"
            "   if message.get('operation')=='stop': return original(message)\n"
            "   entries=n.ledger.read()\n"
            "   if not entries or entries[-1]['record']['type']!='replicated_chat_intent': raise SystemExit(2)\n"
            "   return original(message)\n"
            "  node.request=guarded\n"
            " n.replicate_chat('CRTLIB LIB(INTENT)','intent','intent')\n"
        ),
    ),
    Mutation(
        "replication-rollback",
        "neighborhood.py",
        "restored_hashes, rollback_failures = self._restore_and_verify(pre_snapshots)",
        "restored_hashes, rollback_failures = {}, []",
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400 import PrivateVNetNeighborhood, Refusal\n"
            "with PrivateVNetNeighborhood(Path('vnet')) as n:\n"
            " before=n._snapshots()\n"
            " second=n.nodes['AS400-B']; original=second.request; failed=[False]\n"
            " def fail(message):\n"
            "  if message.get('kind')=='chat' and not failed[0]: failed[0]=True; raise Refusal('injected','NODE_UNAVAILABLE')\n"
            "  return original(message)\n"
            " second.request=fail\n"
            " try: n.replicate_chat('CRTLIB LIB(ROLLBACK)','rollback','rollback')\n"
            " except Refusal: pass\n"
            " raise SystemExit(0 if n._snapshots()==before else 1)\n"
        ),
    ),
    Mutation(
        "replay-commits-only",
        "neighborhood.py",
        'if record.get("type") == "replicated_chat_commit":',
        'if record.get("type") == "replicated_chat_intent":',
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400 import PrivateVNetNeighborhood\n"
            "with PrivateVNetNeighborhood(Path('vnet')) as n:\n"
            " n.replicate_chat('CRTLIB LIB(REPLAY)','replay','replay')\n"
            " result=n.replay_and_verify('AS400-B')\n"
            " raise SystemExit(0 if result['events_replayed']==1 else 1)\n"
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
