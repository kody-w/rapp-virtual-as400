"""Strict, allowlisted parser for the small CL-like command language."""

from __future__ import annotations

import re
from dataclasses import dataclass

from .errors import Refusal

MAX_COMMAND_BYTES = 4096
MAX_BATCH_COMMANDS = 16
NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]{0,9}$")
VERBS = {
    "CRTLIB",
    "CRTPF",
    "CRTDTAQ",
    "CRTJOBQ",
    "INSERT",
    "UPDATE",
    "DELETE",
    "SELECT",
    "DISPLAY",
    "DSPLIB",
    "ENQUEUE",
    "DEQUEUE",
    "SUBMIT",
    "WORK",
    "RUN",
    "PRINT",
}


@dataclass(frozen=True)
class Command:
    verb: str
    clauses: dict[str, str]
    raw: str


def _split_top_level(text: str, delimiter: str) -> list[str]:
    parts: list[str] = []
    start = 0
    depth = 0
    quote: str | None = None
    escaped = False
    for index, char in enumerate(text):
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
        elif char in {"'", '"'}:
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth < 0:
                raise Refusal("Unbalanced parentheses.", "MALFORMED_COMMAND")
        elif char == delimiter and depth == 0:
            parts.append(text[start:index].strip())
            start = index + 1
    if quote or depth:
        raise Refusal("Unclosed quote or parenthesis.", "MALFORMED_COMMAND")
    parts.append(text[start:].strip())
    return parts


def split_batch(text: str) -> list[str]:
    if not isinstance(text, str) or not text.strip():
        raise Refusal("user_input must be a non-empty string.", "INVALID_REQUEST")
    if len(text.encode("utf-8")) > MAX_COMMAND_BYTES:
        raise Refusal("Command batch exceeds 4096 bytes.", "LIMIT_EXCEEDED")
    if "\x00" in text or ".." in text or "\\" in text:
        raise Refusal("Traversal and control sequences are not allowed.", "UNSAFE_INPUT")
    commands = [item for item in _split_top_level(text, ";") if item]
    if len(commands) > MAX_BATCH_COMMANDS:
        raise Refusal("A batch may contain at most 16 commands.", "LIMIT_EXCEEDED")
    return commands


def parse_command(raw: str) -> Command:
    match = re.match(r"^\s*([A-Za-z]+)(?:\s+(.*))?\s*$", raw, re.DOTALL)
    if not match:
        raise Refusal("Malformed command.", "MALFORMED_COMMAND")
    verb = match.group(1).upper()
    if verb not in VERBS:
        raise Refusal(f"Command {verb!r} is not allowlisted.", "COMMAND_NOT_ALLOWED")
    rest = (match.group(2) or "").strip()
    clauses: dict[str, str] = {}
    index = 0
    while index < len(rest):
        key_match = re.match(r"([A-Za-z][A-Za-z0-9_]*)\s*\(", rest[index:])
        if not key_match:
            raise Refusal("Expected a KEY(value) clause.", "MALFORMED_COMMAND")
        key = key_match.group(1).upper()
        index += key_match.end()
        start = index
        depth = 1
        quote: str | None = None
        escaped = False
        while index < len(rest) and depth:
            char = rest[index]
            if quote:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == quote:
                    quote = None
            elif char in {"'", '"'}:
                quote = char
            elif char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            index += 1
        if depth or quote:
            raise Refusal("Unclosed clause.", "MALFORMED_COMMAND")
        if key in clauses:
            raise Refusal(f"Duplicate clause {key}.", "MALFORMED_COMMAND")
        clauses[key] = rest[start : index - 1].strip()
        while index < len(rest) and rest[index].isspace():
            index += 1
    return Command(verb=verb, clauses=clauses, raw=raw.strip())


def parse_batch(text: str) -> list[Command]:
    return [parse_command(item) for item in split_batch(text)]


def require_name(value: str, label: str = "name") -> str:
    name = unquote(value).upper()
    if not NAME_RE.fullmatch(name):
        raise Refusal(f"{label} must match {NAME_RE.pattern}.", "INVALID_NAME")
    return name


def require_qualified(value: str) -> tuple[str, str]:
    parts = unquote(value).split("/")
    if len(parts) != 2:
        raise Refusal("Object must be qualified as LIBRARY/OBJECT.", "INVALID_NAME")
    return require_name(parts[0], "library"), require_name(parts[1], "object")


def unquote(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        body = value[1:-1]
        if "\\" in body:
            raise Refusal("Escape sequences are not supported.", "UNSAFE_INPUT")
        return body
    return value


def parse_pairs(value: str) -> dict[str, str]:
    pairs: dict[str, str] = {}
    for item in _split_top_level(value, ","):
        if not item or "=" not in item:
            raise Refusal("Expected comma-separated FIELD=value pairs.", "MALFORMED_COMMAND")
        key, raw_value = item.split("=", 1)
        name = require_name(key.strip(), "field")
        if name in pairs:
            raise Refusal(f"Duplicate field {name}.", "MALFORMED_COMMAND")
        pairs[name] = unquote(raw_value)
    return pairs
