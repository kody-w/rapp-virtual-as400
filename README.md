# RAPP Virtual AS400

A **clean-room, local, educational prototype** of an operations neighborhood
inspired by general IBM i / AS/400-era concepts: libraries, declared physical
files, records, queues, jobs, and spool-like reports.

This project is not IBM software, an IBM i emulator, or an implementation of a
licensed operating system. It contains no IBM binaries, proprietary code,
branding claim, or licensed OS artifact. **Do not enter real-system
credentials or production data. It is not production software.**

## Properties

- Python 3.11+ standard-library-only runtime.
- Exact local RAPP/1 `POST /chat` response:
  `{response, agent_logs, session_id}`.
- Typed `GET /health`; stable HTTP 422 refusal envelope.
- Atomic JSON persistence in a private state root. On POSIX, exact `0700`
  directory and `0600` file modes are enforced and validated. Windows POSIX
  mode bits are synthetic and `chmod` does not manage ACLs, so the default
  root stays under the user's profile and state, evidence, snapshots, and
  capability files inherit that root's user-scoped ACL. A custom Windows
  `--home` must likewise have a private ACL; no unsupported `0600`/`0700`
  guarantee is claimed there. File contents are flushed before atomic
  publication; POSIX also opens and flushes the containing directory. Python
  cannot safely open directory handles on Windows, so Windows deliberately
  stops after the file flush and atomic replace/link rather than calling
  unsupported `os.open(directory)`.
- Capability-token shutdown; no PID files or PID-signaling authority.
- Strict allowlist parser. No shell, SQL, `eval`, filesystem commands,
  traversal, or outbound network feature exists.
- Batch transactions roll back on refusal; exact decimals remain strings.
- Idempotency uses reversible canonical JSON tuple identities, so allowed
  colons in session IDs and keys cannot alias; legacy cache entries migrate
  deterministically from their bound response session or fail closed.
- Durable sessions, concurrency serialization, and bounded data.
- A provider-neutral private-vNet simulator runs isolated local node
  processes over typed parent/child pipes, without LAN or sibling routes.
- A root-scoped POSIX/Windows interprocess lock serializes complete
  neighborhood mutation, rollback, reset/replay, and evidence transactions.
- Byte-capped, hash-chained evidence keeps exact pre-state snapshots once in
  immutable private bundles; terminal records contain verified references,
  hashes, byte counts, and restore status rather than duplicate snapshots.
- Opening a neighborhood under its root lock audits intent/terminal
  cardinality and automatically restores every node for an unmatched durable
  intent before accepting operations, then appends recovery evidence in the
  terminal slot reserved by that intent.
- Strict restore validation covers object grammar, schema/value limits,
  counters, revisions, and queue/job referential integrity before atomic
  replacement. The same 4 MiB canonical serialized-state cap is checked before
  every atomic write and transaction commit; rejected growth leaves the prior
  revision and bytes intact. A bounded private recovery journal makes
  post-replace failures roll back exact prior bytes or fail closed with stable
  `RECOVERY_REQUIRED`; restart resolves prepared publications before accepting
  work. An operator can explicitly select the journal's exact prior bytes with
  `AtomicStore(path, recover=True)`. Expected publication failures are stable
  HTTP and worker refusals.
- Evidence proves deterministic replication, disposable replay, and
  convergence, including a 100-replica release proof. Replay never resets or
  restores a live neighborhood node.

## Quick start

No installation is required:

```bash
PYTHONPATH=src python3 -m rapp_virtual_as400 --home .rapp-virtual-as400 demo
PYTHONPATH=src python3 -m rapp_virtual_as400 --home .rapp-virtual-as400 \
  chat "DSPLIB" --session-id cli-1
```

Install an isolated command if desired:

```bash
python3 -m venv .venv
.venv/bin/pip install .
rapp-virtual-as400 --home .rapp-virtual-as400 demo
```

## Exact local RAPP/1 server

```bash
PYTHONPATH=src python3 -m rapp_virtual_as400 --home .rapp-virtual-as400 serve --port 7084

curl -s http://127.0.0.1:7084/health
curl -s -X POST http://127.0.0.1:7084/chat \
  -H 'Content-Type: application/json' \
  -d '{"user_input":"DSPLIB","session_id":"demo","idempotency_key":"turn-1"}'
```

`user_input` is required. `session_id` and `idempotency_key` are optional.
Their allowed strings may contain colons; the persisted cache identity is the
canonical JSON tuple `[session_id,idempotency_key]`, never delimiter joining.
Successful responses have exactly three top-level fields. Refused requests:

```json
{
  "error": {"type": "refusal", "code": "COMMAND_NOT_ALLOWED", "message": "..."},
  "agent_logs": [],
  "session_id": "demo"
}
```

To stop, read the private capability and present it:

```bash
TOKEN="$(cat .rapp-virtual-as400/stop.capability)"
curl -s -X POST http://127.0.0.1:7084/admin/stop \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{}'
```

## Command neighborhood

```text
CRTLIB LIB(DEMO)
CRTPF FILE(DEMO/ITEMS) FIELDS(ID:CHAR(8),QTY:INT,PRICE:DECIMAL(10,2))
INSERT FILE(DEMO/ITEMS) VALUES(ID='A1',QTY='2',PRICE='10.20')
UPDATE FILE(DEMO/ITEMS) SET(QTY='3') WHERE(ID='A1')
SELECT FILE(DEMO/ITEMS) WHERE(ID='A1')
DELETE FILE(DEMO/ITEMS) WHERE(ID='A1')
DISPLAY FILE(DEMO/ITEMS)
CRTDTAQ DTAQ(DEMO/EVENTS)
ENQUEUE DTAQ(DEMO/EVENTS) DATA('ready')
DEQUEUE DTAQ(DEMO/EVENTS)
CRTJOBQ JOBQ(DEMO/BATCH)
SUBMIT JOBQ(DEMO/BATCH) CMD("DISPLAY FILE(DEMO/ITEMS)")
WORK JOBQ(DEMO/BATCH)
RUN JOB(J000001)
PRINT FILE(DEMO/ITEMS) TITLE('Synthetic Inventory')
DSPLIB LIB(DEMO)
```

Semicolon-separated commands are one transaction. See
[`docs/COMMANDS.md`](docs/COMMANDS.md) for types and limits.

## Multi-node private-vNet proof

Here “private vNet” means the provider-neutral, local trust topology defined in
[`docs/PRIVATE_VNET_TOPOLOGY.md`](docs/PRIVATE_VNET_TOPOLOGY.md), not a real
cloud network. At least two isolated node processes have separate private state
roots. Their only interconnection is bounded, typed RAPP/1 control through the
parent process; no node opens a LAN listener or has a privileged sibling route.

```bash
PYTHONPATH=src python3 -m rapp_virtual_as400 \
  --home .rapp-virtual-as400 neighborhood-proof
```

The proof converges replicated state, executes 100 bounded deterministic job
simulations with all results identical, replays committed chats on a private
unique disposable node from append-only hash-chained evidence, and verifies
convergence without mutating the selected live node. Stochastic runs require
an exact quorum declared before execution and retain every outlier.

## RAPP Zoo v2

The wheel installs
`rapp_virtual_as400.zoo.rapp_virtual_as400_agent:RAPPVirtualAS400Agent` and
package-data copies of Store v2 and global-object metadata. The documented
`agents/rapp_virtual_as400_agent.py` and `store.v2.json` source mirrors are
required to be byte-identical to the packaged authorities. Build both
deterministic manifest copies:

```bash
PYTHONPATH=src python3 -m rapp_virtual_as400 manifest --root .
```

The metadata marks **Summon Chant ready** with the phrase “Summon the virtual
operations neighborhood.” See [`docs/RAPP_ZOO.md`](docs/RAPP_ZOO.md).

## Test and mutation gates

```bash
PYTHONPATH=src python3 -m unittest discover -v
PYTHONPATH=src python3 tools/mutation_gate.py
PYTHONPATH=src python3 -m rapp_virtual_as400 --home .rapp-virtual-as400 neighborhood-proof
```

## License

MIT. IBM, IBM i, and AS/400 are identifiers associated with IBM. Their mention
describes historical inspiration only and does not imply affiliation,
endorsement, compatibility certification, or use of IBM materials.
