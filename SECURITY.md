# Security

This prototype must remain loopback-only and must never receive real-system
credentials or production data. Please report vulnerabilities privately
through GitHub's repository security advisory feature rather than a public
issue. Do not include secrets, customer data, or exploit data from real systems.

Supported security fixes target the latest `main` revision only.

Private persistence uses exact `0700` directories and `0600` files on POSIX.
Windows does not expose authoritative POSIX mode bits through Python: private
files instead inherit the ACL of the user-profile state root and are published
atomically after their contents are flushed. If `--home` is overridden on
Windows, its ACL must grant access only to the intended user and administrators.
