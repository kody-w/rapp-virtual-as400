"""Public error types."""


class Refusal(ValueError):
    """A safe refusal for malformed, disallowed, or over-limit input."""

    def __init__(self, message: str, code: str = "COMMAND_REFUSED") -> None:
        super().__init__(message)
        self.message = message
        self.code = code

    def envelope(self, session_id: str) -> dict:
        return {
            "error": {
                "type": "refusal",
                "code": self.code,
                "message": self.message,
            },
            "agent_logs": [],
            "session_id": session_id,
        }
