"""Single-file BasicAgent-compatible adapter for the virtual operations engine."""

from __future__ import annotations

import os
from pathlib import Path

try:
    from agents.basic_agent import BasicAgent
except ImportError:
    class BasicAgent:  # type: ignore[no-redef]
        def __init__(self) -> None:
            pass

        def to_tool(self) -> dict:
            return {"type": "function", "function": self.metadata}


class RAPPVirtualAS400Agent(BasicAgent):
    def __init__(self) -> None:
        self.name = "RAPPVirtualAS400"
        self.metadata = {
            "name": self.name,
            "description": (
                "Drive a clean-room local educational operations neighborhood with safe CL-like commands. "
                "It is not an IBM system or emulator and must never receive real-system credentials."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "user_input": {
                        "type": "string",
                        "description": "One or more allowlisted CL-like commands.",
                    },
                    "session_id": {"type": "string"},
                    "idempotency_key": {"type": "string"},
                },
                "required": ["user_input"],
            },
        }
        super().__init__()

    def perform(
        self,
        user_input: str = "",
        session_id: str = "",
        idempotency_key: str = "",
        **kwargs: object,
    ) -> str:
        from rapp_virtual_as400 import Refusal, VirtualAS400

        home = Path(os.environ.get("RAPP_VIRTUAL_AS400_HOME", "~/.rapp-virtual-as400")).expanduser()
        try:
            result = VirtualAS400(home / "state.json").chat(
                user_input,
                session_id or None,
                idempotency_key or None,
            )
            return result["response"]
        except Refusal as error:
            return f"REFUSED [{error.code}]: {error.message}"


VirtualAS400Agent = RAPPVirtualAS400Agent
