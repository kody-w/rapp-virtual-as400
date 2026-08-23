# Clean-room boundary

This repository was authored as a new educational model from general,
publicly understood operational concepts. It does not derive from, bundle, or
require IBM binaries, IBM i / OS/400 source, licensed manuals, command text,
APIs, object formats, data, screenshots, or reverse-engineered artifacts.

The CL-like language is intentionally tiny and project-defined. Its behavior,
storage format, HTTP interface, queue semantics, and job lifecycle are original
prototype choices, not compatibility claims. The project does not connect to
real systems and has no credential fields, host connectors, terminal emulation,
database driver, shell escape, SQL bridge, or proprietary extension point.

The multi-node “private vNet” is a provider-neutral local simulation: isolated
child processes communicate only with their parent over bounded typed pipes.
It does not create a cloud network or expose a LAN listener, sibling route,
credential mechanism, proprietary binary, or machine connector. See
`PRIVATE_VNET_TOPOLOGY.md`.

IBM, IBM i, and AS/400 are identifiers associated with IBM. References explain
historical inspiration only. This project is unaffiliated with and not endorsed
or certified by IBM.
