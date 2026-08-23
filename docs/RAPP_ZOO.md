# RAPP Zoo v2 integration

The wheel installs the adapter at
`rapp_virtual_as400.zoo.rapp_virtual_as400_agent:RAPPVirtualAS400Agent`.
The class exposes OpenAI-compatible function metadata, a
`perform(..., **kwargs)` method, and a `to_tool()` fallback for standalone
inspection. A host may copy the documented
`agents/rapp_virtual_as400_agent.py` mirror; tests require that mirror to be
byte-identical to the installed source authority.

The adapter uses `RAPP_VIRTUAL_AS400_HOME` (default
`~/.rapp-virtual-as400`) and drives the same `VirtualAS400` engine used by the
CLI and local HTTP server. It does not bypass the RAPP/1 command grammar.

Packaged `store.v2.json` is the public catalog record, and packaged
`global-objects.manifest.json` hashes the adapter, Store v2 record, and MIT
license dimension. Root copies are documented byte-identical source mirrors.
Its Summon Chant block is ready for discovery using:

> Summon the virtual operations neighborhood.

This is a local prototype boundary. Hosts must not solicit, store, or forward
real IBM i / AS/400 credentials or production data to this capability.
