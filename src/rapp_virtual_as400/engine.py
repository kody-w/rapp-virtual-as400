"""Transactional virtual operations engine."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone
from decimal import Decimal, DecimalException, localcontext
from pathlib import Path

from .errors import Refusal
from .parser import Command, parse_batch, parse_pairs, require_name, require_qualified, unquote
from .storage import AtomicStore, MAX_SIX_DIGIT_ID, encode_idempotency_identity
from .unicode_safe import canonical_unicode

MAX_LIBRARIES = 64
MAX_FILES = 128
MAX_FIELDS = 32
MAX_RECORDS_PER_FILE = 1000
MAX_DATA_QUEUES = 128
MAX_JOB_QUEUES = 128
MAX_QUEUE_ITEMS = 1000
MAX_JOBS = 1000
MAX_SPOOL = 500
MAX_SESSIONS = 1000
MAX_TEXT = 2048
_EVENT_AT: ContextVar[str | None] = ContextVar("rapp_virtual_as400_event_at", default=None)
ALLOWED_CLAUSES = {
    "CRTLIB": {"LIB"},
    "CRTPF": {"FILE", "FIELDS"},
    "CRTDTAQ": {"DTAQ"},
    "CRTJOBQ": {"JOBQ"},
    "INSERT": {"FILE", "VALUES"},
    "UPDATE": {"FILE", "SET", "WHERE"},
    "DELETE": {"FILE", "WHERE"},
    "SELECT": {"FILE", "WHERE"},
    "DISPLAY": {"FILE", "WHERE"},
    "DSPLIB": {"LIB"},
    "ENQUEUE": {"DTAQ", "DATA"},
    "DEQUEUE": {"DTAQ"},
    "SUBMIT": {"JOBQ", "CMD"},
    "WORK": {"JOBQ"},
    "RUN": {"JOB"},
    "PRINT": {"FILE", "WHERE", "TITLE"},
}
REQUIRED_CLAUSES = {
    "CRTLIB": {"LIB"},
    "CRTPF": {"FILE", "FIELDS"},
    "CRTDTAQ": {"DTAQ"},
    "CRTJOBQ": {"JOBQ"},
    "INSERT": {"FILE", "VALUES"},
    "UPDATE": {"FILE", "SET", "WHERE"},
    "DELETE": {"FILE", "WHERE"},
    "SELECT": {"FILE"},
    "DISPLAY": {"FILE"},
    "DSPLIB": set(),
    "ENQUEUE": {"DTAQ", "DATA"},
    "DEQUEUE": {"DTAQ"},
    "SUBMIT": {"JOBQ", "CMD"},
    "WORK": {"JOBQ"},
    "RUN": {"JOB"},
    "PRINT": {"FILE"},
}


class VirtualAS400:
    """A clean-room educational model, not an IBM system or emulator."""

    def __init__(self, state_path: str | Path) -> None:
        self.store = AtomicStore(state_path)

    def chat(
        self,
        user_input: str,
        session_id: str | None = None,
        idempotency_key: str | None = None,
        *,
        event_at: str | None = None,
    ) -> dict:
        if not isinstance(user_input, str):
            raise Refusal("user_input must be a non-empty string.", "INVALID_REQUEST")
        user_input = canonical_unicode(user_input)
        if session_id is not None and isinstance(session_id, str):
            session_id = canonical_unicode(session_id)
        if idempotency_key is not None and isinstance(idempotency_key, str):
            idempotency_key = canonical_unicode(idempotency_key)
        if event_at is not None:
            if not isinstance(event_at, str) or not 1 <= len(event_at) <= 64:
                raise Refusal("event_at has an invalid format.", "INVALID_REQUEST")
            event_at = canonical_unicode(event_at)
        session_id = self._session_id(session_id)
        commands = parse_batch(user_input)
        request_hash = hashlib.sha256(user_input.encode("utf-8")).hexdigest()
        if idempotency_key is not None:
            if not isinstance(idempotency_key, str) or not re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", idempotency_key):
                raise Refusal("idempotency_key has an invalid format.", "INVALID_REQUEST")
        cache_key = (
            encode_idempotency_identity(session_id, idempotency_key)
            if idempotency_key
            else None
        )

        with self.store.transaction() as state:
            if cache_key and cache_key in state["idempotency"]:
                cached = state["idempotency"][cache_key]
                if cached.get("result", {}).get("session_id") != session_id:
                    raise Refusal(
                        "Idempotency cache session identity diverged.",
                        "IDEMPOTENCY_CONFLICT",
                    )
                if cached["request_hash"] != request_hash:
                    raise Refusal("Idempotency key was already used for different input.", "IDEMPOTENCY_CONFLICT")
                return cached["result"]

            outputs: list[str] = []
            logs: list[dict] = []
            for command in commands:
                self._validate_clauses(command)
                event_token = _EVENT_AT.set(event_at)
                try:
                    output = self._execute(state, command)
                finally:
                    _EVENT_AT.reset(event_token)
                outputs.append(output)
                logs.append({"command": command.verb, "status": "ok"})
            result = {
                "response": "\n\n".join(outputs),
                "agent_logs": logs,
                "session_id": session_id,
            }
            session = state["sessions"].setdefault(session_id, {"turns": []})
            session["turns"].append(
                {
                    "at": event_at or datetime.now(timezone.utc).isoformat(),
                    "input": user_input,
                    "response": result["response"],
                }
            )
            session["turns"] = session["turns"][-100:]
            self._trim_mapping(state["sessions"], MAX_SESSIONS)
            if cache_key:
                state["idempotency"][cache_key] = {
                    "request_hash": request_hash,
                    "result": result,
                }
                self._trim_mapping(state["idempotency"], 2000)
            return result

    @staticmethod
    def _session_id(value: str | None) -> str:
        if value is None:
            return str(uuid.uuid4())
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", value):
            raise Refusal("session_id has an invalid format.", "INVALID_REQUEST")
        return value

    @staticmethod
    def _trim_mapping(mapping: dict, limit: int) -> None:
        while len(mapping) > limit:
            del mapping[next(iter(mapping))]

    @staticmethod
    def _validate_clauses(command: Command) -> None:
        keys = set(command.clauses)
        unexpected = keys - ALLOWED_CLAUSES[command.verb]
        missing = REQUIRED_CLAUSES[command.verb] - keys
        if unexpected:
            raise Refusal(f"Unsupported clause(s): {', '.join(sorted(unexpected))}.", "COMMAND_NOT_ALLOWED")
        if missing:
            raise Refusal(f"Missing clause(s): {', '.join(sorted(missing))}.", "MALFORMED_COMMAND")

    def _execute(self, state: dict, command: Command) -> str:
        handler = getattr(self, f"_do_{command.verb.lower()}")
        return handler(state, command.clauses)

    @staticmethod
    def _library(state: dict, name: str) -> dict:
        try:
            return state["libraries"][name]
        except KeyError:
            raise Refusal(f"Library {name} does not exist.", "OBJECT_NOT_FOUND") from None

    def _file(self, state: dict, value: str) -> tuple[str, str, dict]:
        library_name, file_name = require_qualified(value)
        library = self._library(state, library_name)
        try:
            return library_name, file_name, library["files"][file_name]
        except KeyError:
            raise Refusal(f"File {library_name}/{file_name} does not exist.", "OBJECT_NOT_FOUND") from None

    def _do_crtlib(self, state: dict, clauses: dict) -> str:
        name = require_name(clauses["LIB"], "library")
        if name in state["libraries"]:
            raise Refusal(f"Library {name} already exists.", "OBJECT_EXISTS")
        if len(state["libraries"]) >= MAX_LIBRARIES:
            raise Refusal("Library limit reached.", "LIMIT_EXCEEDED")
        state["libraries"][name] = {"files": {}}
        return f"Library {name} created."

    def _do_crtpf(self, state: dict, clauses: dict) -> str:
        library_name, file_name = require_qualified(clauses["FILE"])
        library = self._library(state, library_name)
        if file_name in library["files"]:
            raise Refusal(f"File {library_name}/{file_name} already exists.", "OBJECT_EXISTS")
        if sum(len(item["files"]) for item in state["libraries"].values()) >= MAX_FILES:
            raise Refusal("Physical file limit reached.", "LIMIT_EXCEEDED")
        fields = self._parse_fields(clauses["FIELDS"])
        library["files"][file_name] = {"fields": fields, "records": []}
        return f"Physical file {library_name}/{file_name} created with {len(fields)} fields."

    @staticmethod
    def _parse_fields(value: str) -> list[dict]:
        chunks: list[str] = []
        current = ""
        depth = 0
        for char in value:
            if char == "," and depth == 0:
                chunks.append(current)
                current = ""
                continue
            current += char
            depth += char == "("
            depth -= char == ")"
        chunks.append(current)
        if not 1 <= len(chunks) <= MAX_FIELDS:
            raise Refusal("A file requires 1 to 32 fields.", "LIMIT_EXCEEDED")
        fields: list[dict] = []
        names: set[str] = set()
        for chunk in chunks:
            if ":" not in chunk:
                raise Refusal("Field declarations use NAME:TYPE.", "MALFORMED_COMMAND")
            raw_name, raw_type = chunk.split(":", 1)
            name = require_name(raw_name.strip(), "field")
            if name in names:
                raise Refusal(f"Duplicate field {name}.", "MALFORMED_COMMAND")
            type_match = re.fullmatch(r"\s*(CHAR|INT|DECIMAL)(?:\((\d+)(?:,(\d+))?\))?\s*", raw_type, re.I)
            if not type_match:
                raise Refusal(f"Unsupported type for field {name}.", "INVALID_SCHEMA")
            kind = type_match.group(1).upper()
            first = int(type_match.group(2) or (10 if kind == "DECIMAL" else 0))
            second = int(type_match.group(3) or 0)
            if kind == "CHAR" and type_match.group(3) is not None:
                raise Refusal("CHAR takes exactly one length.", "INVALID_SCHEMA")
            if kind == "CHAR" and not 1 <= first <= 256:
                raise Refusal("CHAR length must be 1 through 256.", "INVALID_SCHEMA")
            if kind == "INT" and (type_match.group(2) or type_match.group(3)):
                raise Refusal("INT takes no size.", "INVALID_SCHEMA")
            if kind == "DECIMAL" and not (1 <= first <= 38 and 0 <= second < first):
                raise Refusal("DECIMAL requires precision 1-38 and scale below precision.", "INVALID_SCHEMA")
            fields.append({"name": name, "type": kind, "precision": first, "scale": second})
            names.add(name)
        return fields

    def _coerce_record(self, file: dict, values: dict[str, str], partial: bool = False) -> dict[str, str]:
        schema = {field["name"]: field for field in file["fields"]}
        unknown = set(values) - set(schema)
        if unknown:
            raise Refusal(f"Unknown field(s): {', '.join(sorted(unknown))}.", "INVALID_RECORD")
        if not partial and set(values) != set(schema):
            missing = set(schema) - set(values)
            raise Refusal(f"Missing field(s): {', '.join(sorted(missing))}.", "INVALID_RECORD")
        return {name: self._coerce_value(schema[name], value) for name, value in values.items()}

    @staticmethod
    def _coerce_value(field: dict, value: str) -> str:
        if len(value) > MAX_TEXT:
            raise Refusal("Value exceeds 2048 characters.", "LIMIT_EXCEEDED")
        if field["type"] == "CHAR":
            if len(value) > field["precision"]:
                raise Refusal(f"{field['name']} exceeds CHAR length.", "INVALID_RECORD")
            return value
        if field["type"] == "INT":
            if not re.fullmatch(r"-?\d+", value):
                raise Refusal(f"{field['name']} requires an integer.", "INVALID_RECORD")
            number = int(value)
            if not -(2**63) <= number < 2**63:
                raise Refusal(f"{field['name']} exceeds signed 64-bit range.", "INVALID_RECORD")
            return str(number)
        if not re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)", value):
            raise Refusal(f"{field['name']} requires a decimal.", "INVALID_RECORD") from None
        scale = field["scale"]
        precision = field["precision"]
        try:
            with localcontext() as context:
                context.prec = precision
                number = Decimal(value)
                quantum = Decimal(1).scaleb(-scale)
                quantized = number.quantize(quantum)
        except DecimalException:
            raise Refusal(f"{field['name']} exceeds declared precision.", "INVALID_RECORD") from None
        if number != quantized:
            raise Refusal(f"{field['name']} exceeds declared scale.", "INVALID_RECORD")
        normalized = f"{quantized:.{scale}f}"
        digits = len(normalized.replace("-", "").replace(".", ""))
        if digits > precision:
            raise Refusal(f"{field['name']} exceeds declared precision.", "INVALID_RECORD")
        return normalized

    def _do_insert(self, state: dict, clauses: dict) -> str:
        library_name, file_name, file = self._file(state, clauses["FILE"])
        if len(file["records"]) >= MAX_RECORDS_PER_FILE:
            raise Refusal("Record limit reached.", "LIMIT_EXCEEDED")
        record = self._coerce_record(file, parse_pairs(clauses["VALUES"]))
        file["records"].append(record)
        return f"1 record inserted into {library_name}/{file_name}."

    def _where(self, file: dict, where: str | None) -> dict[str, str]:
        if where is None:
            return {}
        return self._coerce_record(file, parse_pairs(where), partial=True)

    @staticmethod
    def _matches(record: dict, where: dict[str, str]) -> bool:
        if not where:
            return True
        return all(record[key] == value for key, value in where.items())

    def _do_update(self, state: dict, clauses: dict) -> str:
        library_name, file_name, file = self._file(state, clauses["FILE"])
        updates = self._coerce_record(file, parse_pairs(clauses["SET"]), partial=True)
        where = self._where(file, clauses["WHERE"])
        count = 0
        for record in file["records"]:
            if self._matches(record, where):
                record.update(updates)
                count += 1
        return f"{count} record(s) updated in {library_name}/{file_name}."

    def _do_delete(self, state: dict, clauses: dict) -> str:
        library_name, file_name, file = self._file(state, clauses["FILE"])
        where = self._where(file, clauses["WHERE"])
        retained = [record for record in file["records"] if not self._matches(record, where)]
        count = len(file["records"]) - len(retained)
        file["records"] = retained
        return f"{count} record(s) deleted from {library_name}/{file_name}."

    def _select_records(self, state: dict, clauses: dict) -> tuple[str, list[dict], list[str]]:
        library_name, file_name, file = self._file(state, clauses["FILE"])
        where = self._where(file, clauses.get("WHERE"))
        records = [record for record in file["records"] if self._matches(record, where)]
        fields = [field["name"] for field in file["fields"]]
        return f"{library_name}/{file_name}", records, fields

    def _do_select(self, state: dict, clauses: dict) -> str:
        qualified, records, _ = self._select_records(state, clauses)
        return f"{qualified}: {json.dumps(records, ensure_ascii=False, separators=(',', ':'))}"

    def _do_display(self, state: dict, clauses: dict) -> str:
        qualified, records, fields = self._select_records(state, clauses)
        return self._table(f"DISPLAY {qualified}", fields, records)

    def _do_dsplib(self, state: dict, clauses: dict) -> str:
        if "LIB" in clauses:
            names = [require_name(clauses["LIB"], "library")]
            self._library(state, names[0])
        else:
            names = sorted(state["libraries"])
        lines = ["LIBRARY     FILES"]
        lines.extend(f"{name:<10}  {len(state['libraries'][name]['files']):>5}" for name in names)
        return "\n".join(lines)

    def _do_crtdtaq(self, state: dict, clauses: dict) -> str:
        library, name = require_qualified(clauses["DTAQ"])
        self._library(state, library)
        key = f"{library}/{name}"
        if key in state["data_queues"]:
            raise Refusal(f"Data queue {key} already exists.", "OBJECT_EXISTS")
        if len(state["data_queues"]) >= MAX_DATA_QUEUES:
            raise Refusal("Data queue object limit reached.", "LIMIT_EXCEEDED")
        state["data_queues"][key] = []
        return f"Data queue {key} created."

    def _do_enqueue(self, state: dict, clauses: dict) -> str:
        key = "/".join(require_qualified(clauses["DTAQ"]))
        if key not in state["data_queues"]:
            raise Refusal(f"Data queue {key} does not exist.", "OBJECT_NOT_FOUND")
        queue = state["data_queues"][key]
        if len(queue) >= MAX_QUEUE_ITEMS:
            raise Refusal("Data queue limit reached.", "LIMIT_EXCEEDED")
        data = unquote(clauses["DATA"])
        if len(data) > MAX_TEXT:
            raise Refusal("Queue data exceeds 2048 characters.", "LIMIT_EXCEEDED")
        queue.append(data)
        return f"Enqueued on {key}; depth={len(queue)}."

    def _do_dequeue(self, state: dict, clauses: dict) -> str:
        key = "/".join(require_qualified(clauses["DTAQ"]))
        if key not in state["data_queues"]:
            raise Refusal(f"Data queue {key} does not exist.", "OBJECT_NOT_FOUND")
        queue = state["data_queues"][key]
        if not queue:
            return f"{key}: EMPTY"
        data = queue.pop(0)
        return f"{key}: {data}"

    def _do_crtjobq(self, state: dict, clauses: dict) -> str:
        library, name = require_qualified(clauses["JOBQ"])
        self._library(state, library)
        key = f"{library}/{name}"
        if key in state["job_queues"]:
            raise Refusal(f"Job queue {key} already exists.", "OBJECT_EXISTS")
        if len(state["job_queues"]) >= MAX_JOB_QUEUES:
            raise Refusal("Job queue object limit reached.", "LIMIT_EXCEEDED")
        state["job_queues"][key] = []
        return f"Job queue {key} created."

    def _do_submit(self, state: dict, clauses: dict) -> str:
        key = "/".join(require_qualified(clauses["JOBQ"]))
        if key not in state["job_queues"]:
            raise Refusal(f"Job queue {key} does not exist.", "OBJECT_NOT_FOUND")
        embedded = unquote(clauses["CMD"])
        self._validated_submitted_command(embedded)
        if state["next_job"] > MAX_SIX_DIGIT_ID:
            raise Refusal("Job identifier space exhausted.", "LIMIT_EXCEEDED")
        if len(state["jobs"]) >= MAX_JOBS:
            raise Refusal("Job limit reached.", "LIMIT_EXCEEDED")
        job_id = f"J{state['next_job']:06d}"
        state["next_job"] += 1
        state["jobs"][job_id] = {"queue": key, "command": embedded, "status": "QUEUED", "result": ""}
        state["job_queues"][key].append(job_id)
        return f"Job {job_id} submitted to {key}."

    @classmethod
    def _validated_submitted_command(cls, embedded: str) -> Command:
        parsed = parse_batch(embedded)
        if len(parsed) != 1 or parsed[0].verb in {"SUBMIT", "WORK", "RUN"}:
            raise Refusal("Submitted jobs require one non-job command.", "COMMAND_NOT_ALLOWED")
        command = parsed[0]
        cls._validate_clauses(command)
        return command

    def _do_work(self, state: dict, clauses: dict) -> str:
        key = "/".join(require_qualified(clauses["JOBQ"]))
        if key not in state["job_queues"]:
            raise Refusal(f"Job queue {key} does not exist.", "OBJECT_NOT_FOUND")
        queue = state["job_queues"][key]
        if not queue:
            return f"{key}: EMPTY"
        job_id = queue.pop(0)
        state["jobs"][job_id]["status"] = "READY"
        return f"Job {job_id} is READY."

    def _do_run(self, state: dict, clauses: dict) -> str:
        job_id = require_name(clauses["JOB"], "job")
        if job_id not in state["jobs"]:
            raise Refusal(f"Job {job_id} does not exist.", "OBJECT_NOT_FOUND")
        job = state["jobs"][job_id]
        if job["status"] != "READY":
            raise Refusal(f"Job {job_id} is {job['status']}, not READY.", "INVALID_STATE")
        command = self._validated_submitted_command(job["command"])
        result = self._execute(state, command)
        job["status"] = "COMPLETE"
        job["result"] = result
        return f"Job {job_id} COMPLETE: {result}"

    def _do_print(self, state: dict, clauses: dict) -> str:
        qualified, records, fields = self._select_records(state, clauses)
        if state["next_spool"] > MAX_SIX_DIGIT_ID:
            raise Refusal("Spool identifier space exhausted.", "LIMIT_EXCEEDED")
        title = unquote(clauses.get("TITLE", f"REPORT {qualified}"))
        if not 1 <= len(title) <= 120:
            raise Refusal("Report title must contain 1 to 120 characters.", "LIMIT_EXCEEDED")
        report = self._table(title, fields, records)
        spool_id = f"S{state['next_spool']:06d}"
        state["next_spool"] += 1
        state["spool"].append(
            {
                "id": spool_id,
                "title": title,
                "created_at": _EVENT_AT.get() or datetime.now(timezone.utc).isoformat(),
                "report": report,
            }
        )
        state["spool"] = state["spool"][-MAX_SPOOL:]
        return f"Spool report {spool_id} created.\n{report}"

    @staticmethod
    def _table(title: str, fields: list[str], records: list[dict]) -> str:
        widths = {
            field: min(40, max([len(field), *(len(str(record.get(field, ""))) for record in records)]))
            for field in fields
        }
        line = "+-" + "-+-".join("-" * widths[field] for field in fields) + "-+"
        header = "| " + " | ".join(f"{field:<{widths[field]}}" for field in fields) + " |"
        rows = [
            "| " + " | ".join(f"{str(record.get(field, '')):<{widths[field]}}" for field in fields) + " |"
            for record in records
        ]
        return "\n".join([title, line, header, line, *rows, line, f"Records: {len(records)}"])
