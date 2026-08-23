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

`PrivateVNetNeighborhood.replicate_chat()` first reserves evidence capacity
and durably appends an intent before contacting a node. It then captures each
exact pre-event snapshot and hash before sending the same typed RAPP/1 chat
event, idempotency key, and deterministic event timestamp to every node. A
linked commit is appended only when response hashes and complete
persisted-state hashes agree.

Any node failure, result/state divergence, or terminal evidence failure
restores every node through the bounded restore control and verifies each
restored hash against its exact pre-event snapshot. A linked failure/rollback
record is appended when evidence I/O permits; an unpaired durable intent makes
terminal evidence I/O failure visible. Restore snapshots use strict schema,
depth, and size validation and atomic private writes.

`replay_and_verify()` resets one selected node through its fixed typed control
operation, verifies the evidence hash chain, replays committed chat events
only, ignores intents/failures, and requires byte-canonical state convergence
with its peers.

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
runs 100 deterministic simulations with all-identical results, replays one
node from evidence, and prints the typed proof JSON.
