# RAPP Zoo v2 integration

Copy or hardlink `agents/rapp_virtual_as400_agent.py` into a RAPP agents
directory and make this package importable. The class exposes OpenAI-compatible
function metadata, a `perform(..., **kwargs)` method, and a `to_tool()` fallback
for standalone inspection.

The adapter uses `RAPP_VIRTUAL_AS400_HOME` (default
`~/.rapp-virtual-as400`) and drives the same `VirtualAS400` engine used by the
CLI and local HTTP server. It does not bypass the RAPP/1 command grammar.

`store.v2.json` is the public catalog record. The global-object manifest hashes
the agent, Store v2 record, and MIT license dimension. Its Summon Chant block is
ready for discovery using:

> Summon the virtual operations neighborhood.

This is a local prototype boundary. Hosts must not solicit, store, or forward
real IBM i / AS/400 credentials or production data to this capability.
