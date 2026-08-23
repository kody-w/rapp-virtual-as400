# Command and safety reference

Commands are case-insensitive; names are normalized uppercase and must match
`[A-Z][A-Z0-9_]{0,9}`. Objects use `LIBRARY/OBJECT`. Values can be single- or
double-quoted and are never evaluated.

| Command | Purpose |
|---|---|
| `CRTLIB LIB(name)` | Create a library |
| `CRTPF FILE(lib/file) FIELDS(name:type,...)` | Create a physical file |
| `INSERT FILE(...) VALUES(field=value,...)` | Append one complete record |
| `UPDATE FILE(...) SET(...) WHERE(...)` | Update exact-match records |
| `DELETE FILE(...) WHERE(...)` | Delete exact-match records |
| `SELECT FILE(...) [WHERE(...)]` | Return JSON-safe records |
| `DISPLAY FILE(...) [WHERE(...)]` | Return a fixed-width table |
| `DSPLIB [LIB(name)]` | Display library inventory |
| `CRTDTAQ`, `ENQUEUE`, `DEQUEUE` | Create and operate a FIFO data queue |
| `CRTJOBQ`, `SUBMIT`, `WORK`, `RUN` | Create and operate a job queue |
| `PRINT FILE(...) [WHERE(...)] [TITLE(...)]` | Save and return a report |

Supported fields are `CHAR(1..256)` (exactly one length argument), signed 64-bit `INT`, and
`DECIMAL(precision,scale)` with precision 1–38. Decimal and integer values are
persisted canonically as strings, avoiding JSON floating-point loss.
`WHERE` values use the same canonicalization as inserted and updated values:
for example, integer `03` matches stored `3`. Unknown `WHERE` fields are
refused for `SELECT`, `UPDATE`, `DELETE`, `DISPLAY`, and `PRINT`.

Limits include 4,096 input bytes, 16 commands per transaction, 64 libraries,
128 files, 128 data queues, 128 job queues, 32 fields per file, 1,000 records
per file, 1,000 queue entries, 1,000 jobs, 1,000 sessions, 500 retained spool
entries, and 2,048 characters per value. Job and spool identifiers are fixed
six-digit values (`J000001`..`J999999` and `S000001`..`S999999`). Their final
values are valid durable state; later `SUBMIT` or `PRINT` requests are refused
with `LIMIT_EXCEEDED` without changing that state.

The complete persisted state has a 4 MiB canonical UTF-8 JSON limit. That is
also the maximum restore snapshot size; the fixed worker restore transport
allows that state plus its bounded RAPP/1 control envelope. Every atomic
write, including each chat transaction commit, checks the serialized bytes
before touching the state file. Growth beyond the cap returns
`LIMIT_EXCEEDED` and preserves the prior file bytes and revision.

The parser only dispatches named handlers. There is no general interpreter,
shell subprocess, SQL parser, Python evaluation, path argument, socket client,
or arbitrary network operation. Submitted jobs store one already-allowlisted
non-job command and execute through the same parser and transaction.
