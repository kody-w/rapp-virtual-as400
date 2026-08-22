# Contributing

Contributions must be clean-room, MIT-compatible, and free of IBM binaries,
licensed OS artifacts, proprietary code, copied command help, credentials, and
production data. Keep the runtime standard-library-only and the server
loopback-only.

Run both gates before opening a pull request:

```bash
PYTHONPATH=src python3 -m unittest discover -v
PYTHONPATH=src python3 tools/mutation_gate.py
```

By contributing, you agree that your contribution is licensed under MIT.
