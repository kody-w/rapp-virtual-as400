"""Headless command-line interface and server launcher."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from .engine import VirtualAS400
from .errors import Refusal
from .manifest import build_manifest
from .neighborhood import PrivateVNetNeighborhood
from .server import serve
from .storage import enforce_private_mode


def default_home() -> Path:
    configured = os.environ.get("RAPP_VIRTUAL_AS400_HOME")
    return Path(configured).expanduser() if configured else Path.home() / ".rapp-virtual-as400"


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        prog="rapp-virtual-as400",
        description="Clean-room educational operations prototype; not an IBM system or emulator.",
    )
    result.add_argument("--home", type=Path, default=default_home(), help="Private local state directory")
    commands = result.add_subparsers(dest="action", required=True)
    chat = commands.add_parser("chat", help="Run one CL-like RAPP command batch")
    chat.add_argument("user_input")
    chat.add_argument("--session-id")
    chat.add_argument("--idempotency-key")
    server = commands.add_parser("serve", help="Serve exact local RAPP/1 HTTP")
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=7084)
    commands.add_parser("demo", help="Load synthetic sample operations")
    commands.add_parser("neighborhood-proof", help="Prove two-node replay and 100-replica convergence")
    manifest = commands.add_parser("manifest", help="Build the global-object manifest")
    manifest.add_argument("--root", type=Path, default=Path.cwd())
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    home = args.home.expanduser().resolve()
    home.mkdir(parents=True, exist_ok=True, mode=0o700)
    enforce_private_mode(home, 0o700)
    try:
        if args.action == "serve":
            print(
                f"RAPP/1 listening on http://{args.host}:{args.port}; "
                f"stop capability: {home / 'stop.capability'}",
                flush=True,
            )
            serve(args.host, args.port, home / "state.json", home / "stop.capability")
            return 0
        if args.action == "manifest":
            output = build_manifest(args.root)
            print(output)
            return 0
        if args.action == "neighborhood-proof":
            with PrivateVNetNeighborhood(home / "private-vnet") as neighborhood:
                replicated = neighborhood.replicate_chat(
                    "CRTLIB LIB(PROOF); CRTJOBQ JOBQ(PROOF/BATCH)",
                    "proof",
                    "proof-bootstrap-v1",
                )
                run = neighborhood.run_replicated_job(
                    {"name": "BOUNDED-JOB", "payload": {"command": "DISPLAY", "synthetic": True}},
                    replicas=100,
                    mode="deterministic",
                )
                replay = neighborhood.replay_and_verify("AS400-B")
                proof = {
                    "protocol": "RAPP/1",
                    "proof": "private-vnet-neighborhood",
                    "topology": neighborhood.topology(),
                    "replicated_chat": replicated,
                    "replicated_run": run,
                    "replay": replay,
                    "evidence_events": len(neighborhood.ledger.read()),
                }
                print(json.dumps(proof, ensure_ascii=False, indent=2, sort_keys=True))
            return 0
        engine = VirtualAS400(home / "state.json")
        if args.action == "demo":
            sample = (
                "CRTLIB LIB(DEMO); "
                "CRTPF FILE(DEMO/ORDERS) FIELDS(ID:CHAR(8),CUSTOMER:CHAR(24),TOTAL:DECIMAL(10,2),STATUS:CHAR(12)); "
                "INSERT FILE(DEMO/ORDERS) VALUES(ID='A100',CUSTOMER='Northwind',TOTAL='145.90',STATUS='READY'); "
                "INSERT FILE(DEMO/ORDERS) VALUES(ID='A101',CUSTOMER='Contoso',TOTAL='82.15',STATUS='HOLD'); "
                "PRINT FILE(DEMO/ORDERS) TITLE('Synthetic Order Operations')"
            )
            print(json.dumps(engine.chat(sample, "demo", "sample-v1"), indent=2))
            return 0
        print(
            json.dumps(
                engine.chat(args.user_input, args.session_id, args.idempotency_key),
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    except Refusal as error:
        print(json.dumps(error.envelope(getattr(args, "session_id", "") or ""), indent=2), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
