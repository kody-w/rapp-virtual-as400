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

`PrivateVNetNeighborhood.replicate_chat()` sends the same typed RAPP/1 chat
event, idempotency key, and deterministic event timestamp to every node. It
accepts the event only when response hashes and complete persisted-state
hashes agree. Every attempt and state hash is added to private, append-only,
hash-chained JSONL evidence.

`replay_and_verify()` resets one selected node through its fixed typed control
operation, replays accepted chat events, and requires byte-canonical state
convergence with its peers.

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
