# discrete-event-systems-test/contract-conformance-tests

Deterministic state-model, idempotency, serialization, and protocol contract conformance tests.

This repository is the `contract` deep-test suite for `discrete-event-systems`. It is intentionally dependency-light and deterministic so failures can be reproduced locally without production credentials or customer data.

## Run

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
python scripts/verify_repository.py
```

The initial model is executable rather than a placeholder. Product adapters should be added through focused pull requests while preserving the reference-model tests as an oracle.

The [FEL evidence gates](docs/fel-evidence-gates.md) add versioned benchmark
fixtures, classified legacy-trace observations, bounded failure artifacts and
telemetry, plus a shared read-only web/MCP projection contract. This is
synthetic reference evidence; the production Rust and live transport adapters
remain intentionally separate follow-up work.

Tracking: https://github.com/ORESoftware/ai-agent-coordinator.rs/issues/139
