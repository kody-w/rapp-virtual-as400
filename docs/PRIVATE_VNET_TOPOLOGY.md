# Provider-neutral private-vNet topology

“Private vNet” in this clean-room simulator is a **trust and addressing
model**, not a claim that a cloud virtual network, VPN, VLAN, or IBM network
has been provisioned. The topology is provider-neutral
`rapp.private-vnet/v1`.

```text
                  typed RAPP/1 control
              +-------------------------+
              | local parent orchestrator|
              +-------------+-----------+
                            |
              private parent/child stdio only
                    /                 \
        +----------+-------+   +------+-----------+
        | AS400-A process  |   | AS400-B process  |
        | nodes/AS400-A/   |   | nodes/AS400-B/   |
        | private state    |   | private state    |
        +------------------+   +------------------+
```

Each node is a distinct local process with its own `0700` state root and
`0600` atomic state files. Nodes are addressed by typed IDs such as
`AS400-A`; they cannot address one another. The parent sends bounded JSON
messages over inherited standard-input/output pipes. There is no LAN
listener, outbound connector, arbitrary shell, credential field, proprietary
image, or privileged sibling route. Optional RAPP HTTP remains loopback-only
and is not used for inter-node traffic.

## Replication and evidence

Every neighborhood root owns one private lock file. A reentrant in-process
lock plus `flock` on POSIX or one-byte `msvcrt` locking on Windows serializes
the whole reserve, intent, snapshot bundle, mutation, rollback, and terminal
evidence transaction. Separate neighborhood or `EvidenceLedger` instances
using the same root refresh the on-disk tail while holding this lock. Direct
node chat, disposable replay, and replicated-run evidence use the same
authority, so one instance cannot append a duplicate sequence or roll back
another instance's completed write.

State, snapshot, bundle-accounting, and new-ledger publications share one
directory-durability primitive. Every platform flushes file contents before
atomic `os.replace` or no-clobber hard-link publication. POSIX then opens,
flushes, and closes the containing directory, and propagates any failure.
Python's Windows standard library cannot safely open a directory with
`os.open`, so Windows explicitly skips only that final directory flush; it
still propagates real file-flush, replace, and link errors. Root serialization
continues to use the existing one-byte `msvcrt` lock on Windows.

`PrivateVNetNeighborhood.replicate_chat()` reserves adjacent intent and
terminal event slots and durably appends an intent before contacting any node.
The intent binds its terminal sequence, complete immutable bundle reference
(relative path, SHA-256, and byte count), named nodes, message, and pre-state
hashes. It captures and validates each exact converged pre-event snapshot and
preflights the remaining maximum terminal record + bundle byte budget before
mutation. It writes the snapshots exactly once as an immutable `0600` JSON
bundle before sending the same typed RAPP/1 chat event, idempotency key, and
deterministic event timestamp to every node. A linked commit is appended only
when response hashes and complete persisted-state hashes agree.

Any node failure, result/state divergence, or terminal evidence failure
restores every node through the bounded restore control and verifies each
restored hash against its exact pre-event snapshot. A linked failure/rollback
record is appended when evidence I/O permits. If no terminal can be recorded,
that live neighborhood fails operations closed.

On every later open, before operations are accepted and while holding the root
interprocess lock, the full ledger and bundle accounting are validated. One
trailing unmatched intent is recoverable: its bound bundle path, digest, size,
node set, snapshots, and converged pre-state hashes are validated; every named
node is restored exactly and resnapshotted; then a linked
`replicated_chat_recovery` terminal is durably appended in the intent's
reserved slot. Missing/tampered evidence, topology mismatch, failed restore,
or unavailable terminal capacity fails initialization closed. Full audit
requires exactly one valid adjacent commit, failure, or recovery terminal for
every intent and rejects orphan, duplicate, or mismatched terminals.

Restore snapshots use strict schema, type/value/limit, object-name,
counter/revision, queue/job referential, depth, and size validation before
atomic private writes. Their 4 MiB canonical serialized-state limit is the
same cap enforced before every normal state write and transaction commit; the
restore pipe admits that state plus only its bounded control envelope.
Over-limit growth returns `LIMIT_EXCEEDED` without changing the prior state
revision or file bytes. Unexpected engine failures are returned as stable
`WORKER_ERROR` refusals without turning failures into successes.

Terminal records do not duplicate snapshots. They retain only an immutable
relative bundle reference, SHA-256, byte count, pre-state hashes, and restore
hashes/status alongside the event result. Replay and full evidence audit
reject absolute/traversing/symlinked bundle paths, missing bundles, digest or
size changes, and malformed bundle snapshots. The configured evidence byte
cap covers both JSONL and bundles, each JSONL record has its own bound, and
capacity failure occurs before mutation.

For pre-upgrade bundles, audit first validates the exact raw snapshot and
checks its recorded hash against that unmodified representation. Legacy
idempotency identities are migrated only in a separate deep copy used for
restore/runtime. The immutable bundle bytes, path, size, digest, and recorded
raw state hashes are never rewritten; current-format bundles still read back
exactly.

`replay_and_verify()` verifies the complete evidence hash chain and every
referenced bundle under the root lock, then captures every live node snapshot,
state-file fingerprint, and the evidence fingerprint. It refuses a divergent
live neighborhood before setup. Replay creates a private `0700`, uniquely
named disposable root alongside (never inside) the live node roots, restores
the selected node's first committed pre-state there, and replays committed
chat events only in ledger order. Every event's recorded result, pre-state,
and post-state hash must agree, and the disposable final snapshot and hash
must equal the selected and converged live state.

The disposable process is stopped and its bounded flat state root is erased
on success or failure. Setup refuses when an earlier replay root was not
proven erased. Close, erase, or directory-durability uncertainty fails replay
closed, while live nodes remain operable and their state bytes/hashes and all
evidence bytes are rechecked unchanged. No replay path sends reset, restore,
or chat controls to a live node.

Normal appends refresh sequence and hash from one bounded tail read instead of
parsing all historical JSONL. Permissions and byte/event capacity are checked
before publication. If write or flush reports an error after publication may
have begun, the ledger flushes again and accepts success only when a bounded
tail refresh proves the exact expected sequence, hash, record, and file size.
Close errors after that proof are cosmetic. Replication inspects the reserved
terminal before recording failure, so a durably published commit cannot gain
a second failure terminal. Any partial, mismatched, or otherwise ambiguous
tail keeps the live neighborhood in recovery-required fail-closed state. Full
`read()`/`audit()` still validates every sequence and hash link.

`run_replicated_job()` runs 1–100 bounded simulations across the node
processes:

- `deterministic`: quorum is necessarily every replica and every result must
  be identical;
- `stochastic`: an exact quorum must be declared before execution; the
  expected outcome must occur exactly that many times.

All attempts and all stochastic outliers are retained in the append-only
event. Nothing is silently discarded. These are synthetic job simulations,
not production workload execution.

Run the release proof:

```bash
PYTHONPATH=src python3 -m rapp_virtual_as400 \
  --home .rapp-virtual-as400 neighborhood-proof
```

The proof starts at least two isolated processes, converges a replicated chat,
runs 100 deterministic simulations with all-identical results, replays the
selected history on a disposable node, and prints the typed proof JSON.
