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
        "structured-idempotency-identity",
        "engine.py",
        "encode_idempotency_identity(session_id, idempotency_key)",
        'f"{session_id}:{idempotency_key}"',
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400 import VirtualAS400\n"
            "e=VirtualAS400(Path('state.json'))\n"
            "a=e.chat('CRTLIB LIB(LEFT)','a:b','c')\n"
            "b=e.chat('CRTLIB LIB(RIGHT)','a','b:c')\n"
            "raise SystemExit(0 if a['session_id']=='a:b' and b['session_id']=='a' else 1)\n"
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
        "char-second-argument",
        "engine.py",
        '            if kind == "CHAR" and type_match.group(3) is not None:',
        "            if False:",
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400 import Refusal,VirtualAS400\n"
            "e=VirtualAS400(Path('state.json')); e.chat('CRTLIB LIB(T)','s'); before=e.store.snapshot()\n"
            "try: e.chat('CRTPF FILE(T/F) FIELDS(V:CHAR(10,0))','s')\n"
            "except Refusal as error: raise SystemExit(0 if error.code=='INVALID_SCHEMA' and e.store.snapshot()==before else 2)\n"
            "raise SystemExit(1)\n"
        ),
    ),
    Mutation(
        "submit-embedded-clause-validation",
        "engine.py",
        "        cls._validate_clauses(command)",
        "        pass",
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400 import Refusal,VirtualAS400\n"
            "e=VirtualAS400(Path('state.json')); e.chat('CRTLIB LIB(T); CRTJOBQ JOBQ(T/Q)','s'); before=e.store.snapshot()\n"
            "try: e.chat('SUBMIT JOBQ(T/Q) CMD(\"CRTLIB\")','s')\n"
            "except Refusal: raise SystemExit(0 if e.store.snapshot()==before else 2)\n"
            "raise SystemExit(1)\n"
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
            "   if message.get('kind')=='chat':\n"
            "    entries=n.ledger.read()\n"
            "    if not entries or entries[-1]['record']['type']!='replicated_chat_intent': raise SystemExit(2)\n"
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
        'if entry["record"].get("type") == "replicated_chat_commit"',
        'if entry["record"].get("type") == "replicated_chat_intent"',
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400 import PrivateVNetNeighborhood\n"
            "with PrivateVNetNeighborhood(Path('vnet')) as n:\n"
            " n.replicate_chat('CRTLIB LIB(REPLAY)','replay','replay')\n"
            " result=n.replay_and_verify('AS400-B')\n"
            " raise SystemExit(0 if result['events_replayed']==1 else 1)\n"
        ),
    ),
    Mutation(
        "replay-disposable-node",
        "neighborhood.py",
        (
            "                disposable = NodeProcess(\n"
            '                    f"REPLAY-{uuid.uuid4().hex[:12].upper()}",\n'
            "                    replay_root,\n"
            "                )"
        ),
        "                disposable = self.nodes[node_id]",
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400 import PrivateVNetNeighborhood\n"
            "with PrivateVNetNeighborhood(Path('vnet')) as n:\n"
            " n.replicate_chat('CRTLIB LIB(ISOLATED)','replay','isolated')\n"
            " before={name:(node.root/'state.json').read_bytes() for name,node in n.nodes.items()}\n"
            " result=n.replay_and_verify('AS400-B')\n"
            " after={name:(node.root/'state.json').read_bytes() for name,node in n.nodes.items()}\n"
            " good=result['converged'] and before==after and n.topology()['node_count']==2\n"
            " raise SystemExit(0 if good else 1)\n"
        ),
    ),
    Mutation(
        "replay-every-recorded-result",
        "neighborhood.py",
        "                    if any(actual_result != expected for expected in expected_results):",
        "                    if False:",
        (
            "from pathlib import Path\n"
            "import copy\n"
            "from rapp_virtual_as400 import PrivateVNetNeighborhood,Refusal\n"
            "with PrivateVNetNeighborhood(Path('vnet')) as n:\n"
            " n.replicate_chat('CRTLIB LIB(RESULT)','replay','result')\n"
            " audit=n.ledger.audit\n"
            " def altered():\n"
            "  entries=copy.deepcopy(audit())\n"
            "  record=entries[-1]['record']\n"
            "  for value in record['results'].values(): value['response']='tampered'\n"
            "  return entries\n"
            " n.ledger.audit=altered\n"
            " try: n.replay_and_verify('AS400-A')\n"
            " except Refusal: raise SystemExit(0)\n"
            "raise SystemExit(1)\n"
        ),
    ),
    Mutation(
        "replay-every-event-state-hash",
        "neighborhood.py",
        (
            "                    if any(\n"
            "                        _digest(post_state) != expected_hash\n"
            '                        for expected_hash in record["state_hashes"].values()\n'
            "                    ):"
        ),
        "                    if False:",
        (
            "from pathlib import Path\n"
            "import copy\n"
            "from rapp_virtual_as400 import PrivateVNetNeighborhood,Refusal\n"
            "with PrivateVNetNeighborhood(Path('vnet')) as n:\n"
            " n.replicate_chat('CRTLIB LIB(STATE)','replay','state')\n"
            " audit=n.ledger.audit\n"
            " def altered():\n"
            "  entries=copy.deepcopy(audit())\n"
            "  record=entries[-1]['record']\n"
            "  record['state_hashes']={key:'0'*64 for key in record['state_hashes']}\n"
            "  return entries\n"
            " n.ledger.audit=altered\n"
            " try: n.replay_and_verify('AS400-A')\n"
            " except Refusal: raise SystemExit(0)\n"
            "raise SystemExit(1)\n"
        ),
    ),
    Mutation(
        "replay-disposable-cleanup",
        "neighborhood.py",
        "                            self._erase_disposable_replay_root(replay_root)",
        "                            pass",
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400 import PrivateVNetNeighborhood\n"
            "with PrivateVNetNeighborhood(Path('vnet')) as n:\n"
            " n.replicate_chat('CRTLIB LIB(CLEAN)','replay','clean')\n"
            " n.replay_and_verify('AS400-A')\n"
            " stale=any(path.name.startswith('.replay-') for path in n.root.iterdir())\n"
            " raise SystemExit(1 if stale else 0)\n"
        ),
    ),
    Mutation(
        "root-interprocess-lock",
        "storage.py",
        "                fcntl.flock(descriptor, fcntl.LOCK_EX)",
        "                pass",
        (
            "from pathlib import Path\n"
            "import subprocess,sys,time\n"
            "worker=\"\"\"from pathlib import Path\n"
            "import sys,time\n"
            "from rapp_virtual_as400.storage import root_lock\n"
            "with root_lock(Path('shared')):\n"
            " Path(sys.argv[1]).write_text('entered')\n"
            " if sys.argv[1]=='first':\n"
            "  deadline=time.time()+5\n"
            "  while not Path('release').exists() and time.time()<deadline: time.sleep(.01)\n"
            "\"\"\"\n"
            "one=subprocess.Popen([sys.executable,'-c',worker,'first'])\n"
            "deadline=time.time()+5\n"
            "while not Path('first').exists() and time.time()<deadline: time.sleep(.01)\n"
            "two=subprocess.Popen([sys.executable,'-c',worker,'second'])\n"
            "time.sleep(.2); early=Path('second').exists(); Path('release').write_text('go')\n"
            "one.wait(timeout=5); two.wait(timeout=5)\n"
            "raise SystemExit(1 if early or one.returncode or two.returncode else 0)\n"
        ),
    ),
    Mutation(
        "restore-queue-job-reference",
        "storage.py",
        '        if set(queued_ids) - set(snapshot["jobs"]):',
        "        if False:",
        (
            "from rapp_virtual_as400.storage import empty_state,AtomicStore\n"
            "from rapp_virtual_as400 import Refusal\n"
            "s=empty_state(); s['revision']=1; s['libraries']['T']={'files':{}}; "
            "s['job_queues']['T/Q']=['J000001']\n"
            "try: AtomicStore.validate_snapshot(s)\n"
            "except Refusal: raise SystemExit(0)\n"
            "raise SystemExit(1)\n"
        ),
    ),
    Mutation(
        "bundle-only-terminal-record",
        "neighborhood.py",
        (
            '                    "pre_state_hashes": pre_state_hashes,\n'
            '                    "results": results,'
        ),
        (
            '                    "pre_snapshots": pre_snapshots,\n'
            '                    "pre_state_hashes": pre_state_hashes,\n'
            '                    "results": results,'
        ),
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400 import PrivateVNetNeighborhood\n"
            "with PrivateVNetNeighborhood(Path('vnet')) as n:\n"
            " n.replicate_chat('CRTLIB LIB(BUNDLE)','bundle','bundle')\n"
            " terminal=n.ledger.read()[-1]['record']\n"
            " raise SystemExit(0 if 'pre_snapshots' not in terminal else 1)\n"
        ),
    ),
    Mutation(
        "evidence-byte-preflight",
        "neighborhood.py",
        "            if self._evidence_bytes() + required > MAX_EVIDENCE_BYTES:",
        "            if False:",
        (
            "from pathlib import Path\n"
            "import rapp_virtual_as400.neighborhood as m\n"
            "from rapp_virtual_as400 import PrivateVNetNeighborhood,Refusal\n"
            "m.MAX_EVIDENCE_BYTES=m.MAX_EVIDENCE_RECORD_BYTES\n"
            "with PrivateVNetNeighborhood(Path('vnet')) as n:\n"
            " try: n.replicate_chat('CRTLIB LIB(FULL)','full','full')\n"
            " except Refusal: raise SystemExit(0 if not n._snapshots()['AS400-A']['libraries'] else 2)\n"
            "raise SystemExit(1)\n"
        ),
    ),
    Mutation(
        "stale-ledger-tail-refresh",
        "neighborhood.py",
        "            current, previous = self._refresh_tail()",
        "            current, previous = self._sequence, self._previous",
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400.neighborhood import EvidenceLedger\n"
            "p=Path('evidence/events.jsonl'); a=EvidenceLedger(p); b=EvidenceLedger(p)\n"
            "a.append({'type':'one'}); b.append({'type':'two'})\n"
            "entries=a.read()\n"
            "raise SystemExit(0 if [e['sequence'] for e in entries]==[1,2] else 1)\n"
        ),
    ),
    Mutation(
        "append-permission-preflight",
        "neighborhood.py",
        "            enforce_private_mode(self.path, 0o600)\n            metadata = self.path.lstat()",
        "            metadata = self.path.lstat()",
        (
            "from pathlib import Path\n"
            "import os\n"
            "from rapp_virtual_as400.neighborhood import EvidenceLedger\n"
            "ledger=EvidenceLedger(Path('evidence/events.jsonl')); os.chmod(ledger.path,0o644)\n"
            "entry=ledger.append({'type':'private'})\n"
            "raise SystemExit(0 if entry['sequence']==1 and (ledger.path.stat().st_mode & 0o777)==0o600 else 1)\n"
        ),
    ),
    Mutation(
        "append-post-fsync-close",
        "neighborhood.py",
        (
            "            try:\n"
            "                os.close(descriptor)\n"
            "            except OSError:\n"
            "                pass\n"
            "            self._sequence = sequence"
        ),
        (
            "            os.close(descriptor)\n"
            "            self._sequence = sequence"
        ),
        (
            "from pathlib import Path\n"
            "from unittest import mock\n"
            "import rapp_virtual_as400.neighborhood as m\n"
            "ledger=m.EvidenceLedger(Path('evidence/events.jsonl'))\n"
            "with ledger.transaction_lock, mock.patch.object(m.os,'close',side_effect=OSError('close')):\n"
            " entry=ledger.append({'type':'durable'})\n"
            "raise SystemExit(0 if entry['sequence']==1 and len(ledger.read())==1 else 1)\n"
        ),
    ),
    Mutation(
        "append-exact-durable-recovery",
        "neighborhood.py",
        "                if publication_attempted and self._exact_append_is_durable(",
        "                if False and self._exact_append_is_durable(",
        (
            "from pathlib import Path\n"
            "from unittest import mock\n"
            "import os\n"
            "import rapp_virtual_as400.neighborhood as m\n"
            "ledger=m.EvidenceLedger(Path('evidence/events.jsonl')); original=os.write; injected=[False]\n"
            "def write_then_raise(fd,data):\n"
            " written=original(fd,data)\n"
            " if not injected[0]: injected[0]=True; raise OSError('after write')\n"
            " return written\n"
            "with ledger.transaction_lock, mock.patch.object(m.os,'write',side_effect=write_then_raise):\n"
            " entry=ledger.append({'type':'exact'})\n"
            "raise SystemExit(0 if entry['sequence']==1 and len(ledger.read())==1 else 1)\n"
        ),
    ),
    Mutation(
        "reserved-terminal-detection",
        "neighborhood.py",
        "            return terminal\n\n    def write_snapshot_bundle",
        "            return None\n\n    def write_snapshot_bundle",
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400 import PrivateVNetNeighborhood\n"
            "with PrivateVNetNeighborhood(Path('vnet')) as n:\n"
            " original=n.ledger.append\n"
            " def publish_then_raise(record):\n"
            "  entry=original(record)\n"
            "  if record.get('type')=='replicated_chat_commit': raise OSError('after commit')\n"
            "  return entry\n"
            " n.ledger.append=publish_then_raise\n"
            " result=n.replicate_chat('CRTLIB LIB(EXACT)','exact','exact')\n"
            " types=[entry['record']['type'] for entry in n.ledger.audit()]\n"
            " raise SystemExit(0 if result['converged'] and types==['replicated_chat_intent','replicated_chat_commit'] else 1)\n"
        ),
    ),
    Mutation(
        "failure-terminal-exact-detection",
        "neighborhood.py",
        '                    if occupied is not None and occupied["record"] == failure_record:',
        "                    if False:",
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400 import PrivateVNetNeighborhood,Refusal\n"
            "with PrivateVNetNeighborhood(Path('vnet')) as n:\n"
            " node=n.nodes['AS400-B']; request=node.request; append=n.ledger.append; failed=[False]\n"
            " def fail(message):\n"
            "  if message.get('kind')=='chat' and not failed[0]: failed[0]=True; raise Refusal('node','NODE_UNAVAILABLE')\n"
            "  return request(message)\n"
            " def publish_then_raise(record):\n"
            "  entry=append(record)\n"
            "  if record.get('type')=='replicated_chat_failure': raise OSError('after failure')\n"
            "  return entry\n"
            " node.request=fail; n.ledger.append=publish_then_raise\n"
            " try: n.replicate_chat('CRTLIB LIB(FAIL)','fail','fail')\n"
            " except Refusal: pass\n"
            " types=[entry['record']['type'] for entry in n.ledger.audit()]\n"
            " raise SystemExit(0 if types==['replicated_chat_intent','replicated_chat_failure'] and n.topology()['node_count']==2 else 1)\n"
        ),
    ),
    Mutation(
        "job-identifier-exhaustion",
        "engine.py",
        '        if state["next_job"] > MAX_SIX_DIGIT_ID:',
        "        if False:",
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400 import VirtualAS400,Refusal\n"
            "from rapp_virtual_as400.storage import empty_state\n"
            "s=empty_state(); s['revision']=1; s['libraries']['T']={'files':{}}; "
            "s['job_queues']['T/Q']=[]; s['jobs']['J999999']="
            "{'queue':'T/Q','command':'DSPLIB','status':'COMPLETE','result':'done'}; "
            "s['next_job']=1000000\n"
            "e=VirtualAS400(Path('state.json')); e.store.restore(s); before=e.store.snapshot()\n"
            "try: e.chat('SUBMIT JOBQ(T/Q) CMD(\"DSPLIB\")','s')\n"
            "except Refusal as error: raise SystemExit(0 if error.code=='LIMIT_EXCEEDED' and e.store.snapshot()==before else 2)\n"
            "raise SystemExit(1)\n"
        ),
    ),
    Mutation(
        "spool-identifier-exhaustion",
        "engine.py",
        '        if state["next_spool"] > MAX_SIX_DIGIT_ID:',
        "        if False:",
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400 import VirtualAS400,Refusal\n"
            "from rapp_virtual_as400.storage import empty_state\n"
            "s=empty_state(); s['revision']=1; s['libraries']['T']={'files':{'F':"
            "{'fields':[{'name':'A','type':'CHAR','precision':1,'scale':0}],'records':[]}}}; "
            "s['spool']=[{'id':'S999999','title':'x','created_at':'x','report':'x'}]; "
            "s['next_spool']=1000000\n"
            "e=VirtualAS400(Path('state.json')); e.store.restore(s); before=e.store.snapshot()\n"
            "try: e.chat('PRINT FILE(T/F)','s')\n"
            "except Refusal as error: raise SystemExit(0 if error.code=='LIMIT_EXCEEDED' and e.store.snapshot()==before else 2)\n"
            "raise SystemExit(1)\n"
        ),
    ),
    Mutation(
        "snapshot-directory-durability",
        "neighborhood.py",
        (
            "                _fsync_directory(self._snapshots_path)\n"
            "                self._write_bundle_bytes(current_bundle_bytes + len(encoded))"
        ),
        "                self._write_bundle_bytes(current_bundle_bytes + len(encoded))",
        (
            "from pathlib import Path\n"
            "import shutil\n"
            "import rapp_virtual_as400.neighborhood as m\n"
            "shutil.rmtree('evidence',ignore_errors=True)\n"
            "ledger=m.EvidenceLedger(Path('evidence/events.jsonl')); called=[]\n"
            "m._fsync_directory=lambda path: called.append(path)\n"
            "ledger.write_snapshot_bundle('intent-1.json',{'pre_snapshots':{},'pre_state_hashes':{}})\n"
            "raise SystemExit(0 if ledger._snapshots_path in called else 1)\n"
        ),
    ),
    Mutation(
        "open-time-intent-recovery",
        "neighborhood.py",
        "                self._recover_unmatched_intent()",
        "                pass",
        (
            "from pathlib import Path\n"
            "from rapp_virtual_as400 import PrivateVNetNeighborhood\n"
            "root=Path('vnet'); n=PrivateVNetNeighborhood(root)\n"
            "node=n.nodes['AS400-A']; original=node.request\n"
            "def crash(message):\n"
            " response=original(message)\n"
            " if message.get('kind')=='chat': raise SystemExit()\n"
            " return response\n"
            "node.request=crash\n"
            "try: n.replicate_chat('CRTLIB LIB(CRASHED)','crash','crash')\n"
            "except SystemExit: pass\n"
            "n.close()\n"
            "with PrivateVNetNeighborhood(root) as recovered:\n"
            " states=recovered._snapshots(); types=[e['record']['type'] for e in recovered.ledger.read()]\n"
            " good=states['AS400-A']==states['AS400-B'] and not states['AS400-A']['libraries'] and types==['replicated_chat_intent','replicated_chat_recovery']\n"
            "raise SystemExit(0 if good else 1)\n"
        ),
    ),
    Mutation(
        "raw-hash-before-legacy-migration",
        "neighborhood.py",
        'if bundle["pre_state_hashes"].get(node_id) != _digest(snapshot):',
        'if bundle["pre_state_hashes"].get(node_id) != _digest(validated):',
        (
            "from pathlib import Path\n"
            "import hashlib,json\n"
            "from rapp_virtual_as400.neighborhood import EvidenceLedger\n"
            "from rapp_virtual_as400.storage import empty_state,encode_idempotency_identity\n"
            "s=empty_state(); s['revision']=1; s['idempotency']['a:b:c']={"
            "'request_hash':hashlib.sha256(b'DSPLIB').hexdigest(),"
            "'result':{'response':'ok','agent_logs':[{'command':'DSPLIB','status':'ok'}],"
            "'session_id':'a:b'}}\n"
            "raw=json.dumps(s,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()\n"
            "bundle={'pre_snapshots':{'AS400-A':s},"
            "'pre_state_hashes':{'AS400-A':hashlib.sha256(raw).hexdigest()}}\n"
            "ledger=EvidenceLedger(Path('evidence/events.jsonl'))\n"
            "ref=ledger.write_snapshot_bundle('intent-1.json',bundle)\n"
            "read=ledger.read_snapshot_bundle(ref)\n"
            "key=encode_idempotency_identity('a:b','c')\n"
            "raise SystemExit(0 if key in read['pre_snapshots']['AS400-A']['idempotency'] "
            "and ledger.path.parent.joinpath(ref['path']).read_bytes()=="
            "json.dumps(bundle,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode() else 1)\n"
        ),
    ),
    Mutation(
        "persisted-byte-write-preflight",
        "storage.py",
        "    if len(encoded) > MAX_PERSISTED_STATE_BYTES:",
        "    if False:",
        (
            "from pathlib import Path\n"
            "import rapp_virtual_as400.storage as m\n"
            "from rapp_virtual_as400 import Refusal,VirtualAS400\n"
            "path=Path('state.json'); engine=VirtualAS400(path); before=path.read_bytes()\n"
            "m.MAX_PERSISTED_STATE_BYTES=len(before)+10\n"
            "try:\n"
            " with engine.store.transaction() as state: state['sessions']['growth']={'turns':[]}\n"
            "except Refusal as error:\n"
            " raise SystemExit(0 if error.code=='LIMIT_EXCEEDED' and path.read_bytes()==before else 2)\n"
            "raise SystemExit(1)\n"
        ),
    ),
    Mutation(
        "state-permissions-before-publication",
        "storage.py",
        (
            "            enforce_private_mode(temp, 0o600)\n"
            "            os.replace(temp, destination)"
        ),
        (
            "            os.replace(temp, destination)\n"
            "            enforce_private_mode(destination, 0o600)"
        ),
        (
            "from pathlib import Path\n"
            "from unittest import mock\n"
            "import os\n"
            "import rapp_virtual_as400.storage as m\n"
            "s=m.AtomicStore(Path('state.json')); state=m.empty_state(); state['revision']=1\n"
            "events=[]; real_enforce=m.enforce_private_mode; real_replace=os.replace\n"
            "def enforce(path,mode):\n"
            " if Path(path) in {s.path,s.path.with_suffix(s.path.suffix+'.new')}: "
            "events.append(('chmod',Path(path)))\n"
            " return real_enforce(path,mode)\n"
            "def replace(source,destination):\n"
            " if Path(destination)==s.path: events.append(('replace',Path(destination)))\n"
            " return real_replace(source,destination)\n"
            "with mock.patch.object(m,'enforce_private_mode',side_effect=enforce),"
            "mock.patch.object(m.os,'replace',side_effect=replace): s._write(state)\n"
            "raise SystemExit(0 if events==[('chmod',s.path.with_suffix(s.path.suffix+'.new')),"
            "('replace',s.path)] else 1)\n"
        ),
    ),
    Mutation(
        "published-byte-verification",
        "storage.py",
        (
            "            if actual != encoded or self._hash(actual) != self._hash(encoded):"
        ),
        "            if False:",
        (
            "from pathlib import Path\n"
            "from unittest import mock\n"
            "import os\n"
            "import rapp_virtual_as400.storage as m\n"
            "from rapp_virtual_as400 import Refusal\n"
            "s=m.AtomicStore(Path('state.json')); state=m.empty_state(); state['revision']=1\n"
            "real_replace=os.replace\n"
            "def corrupt(source,destination):\n"
            " real_replace(source,destination)\n"
            " if Path(destination)==s.path: s.path.write_bytes(b'corrupt')\n"
            "try:\n"
            " with mock.patch.object(m.os,'replace',side_effect=corrupt): s._write(state)\n"
            "except Refusal as error:\n"
            " raise SystemExit(0 if error.code=='RECOVERY_REQUIRED' else 2)\n"
            "raise SystemExit(1)\n"
        ),
    ),
    Mutation(
        "prepared-restart-rolls-back-new-state",
        "storage.py",
        (
            "        elif journal[\"phase\"] == \"prepared\":\n"
            "            if new_matches:"
        ),
        (
            "        elif journal[\"phase\"] == \"prepared\":\n"
            "            if False:"
        ),
        (
            "from pathlib import Path\n"
            "import json\n"
            "import rapp_virtual_as400.storage as m\n"
            "s=m.AtomicStore(Path('state.json')); old=s.path.read_bytes()\n"
            "state=m.empty_state(); state['revision']=1\n"
            "new=json.dumps(state,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()\n"
            "s._publish_file(s.recovery_path,s._journal_bytes('prepared',old,new,True))\n"
            "s._publish_file(s.path,new)\n"
            "reopened=m.AtomicStore(s.path)\n"
            "raise SystemExit(0 if reopened.path.read_bytes()==old and "
            "not reopened.recovery_path.exists() else 1)\n"
        ),
    ),
    Mutation(
        "windows-directory-open-fallback",
        "storage.py",
        '    if os.name == "nt":\n        return',
        "    if False:\n        return",
        (
            "from pathlib import Path\n"
            "from unittest import mock\n"
            "import rapp_virtual_as400.storage as m\n"
            "path=Path('.')\n"
            "with mock.patch.object(m.os,'name','nt'), "
            "mock.patch.object(m.os,'open',side_effect=AssertionError('directory opened')):\n"
            " m.fsync_directory(path)\n"
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
