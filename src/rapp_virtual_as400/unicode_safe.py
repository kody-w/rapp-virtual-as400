"""Unicode scalar-value validation shared by transports and the engine."""

from __future__ import annotations

from .errors import Refusal


def canonical_unicode(value: str) -> str:
    """Combine valid JSON surrogate pairs and refuse every unpaired surrogate."""
    try:
        return value.encode("utf-16-le", "surrogatepass").decode("utf-16-le")
    except UnicodeDecodeError:
        raise Refusal("Request contains malformed Unicode.", "INVALID_REQUEST") from None


def canonical_json_strings(value: object) -> object:
    if isinstance(value, str):
        return canonical_unicode(value)
    if isinstance(value, list):
        return [canonical_json_strings(item) for item in value]
    if isinstance(value, dict):
        normalized: dict[object, object] = {}
        for key, item in value.items():
            normalized[canonical_json_strings(key)] = canonical_json_strings(item)
        return normalized
    return value
