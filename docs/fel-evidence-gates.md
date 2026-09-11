# FEL evidence gates

This repository contains an independent, dependency-free future-event-list
(FEL) oracle and a versioned evidence contract. The evidence is synthetic and
safe to publish. It is not a production scheduler implementation, a production
load test, or a live probe of `des-web.rs` or `des-mcp-server.rs`.

## Versioned source of truth

`fixtures/fel/evidence-cases-v1.json` owns the workload inputs and every public
limit used by the evidence gate. Its canonical SHA-256 digest is included in
each report so two reports can prove that they used the same contract.

The six scenarios isolate queue depth, event rate per simulated tick, fan-out,
cancellation density, long simulation horizons, and same-timestamp bursts. A
seed, semantic trace digest, deterministic counters, and comparison digest make
results comparable without asserting unstable wall-clock thresholds.

The same fixture also records observed legacy traces. An identical trace is
`equivalent`; a mismatch needs a named policy to be an
`intentional_policy_difference`; absent observations are `not_observed`; and
an undocumented mismatch remains `unclassified_mismatch`. The loader verifies
the recorded classification instead of normalizing a difference.

## Bounded artifacts and telemetry

Semantic reports include only generated event identifiers and integer ticks.
The complete trace is hashed, while the publishable preview is capped by
`max_trace_events`. Failure artifacts include the exception class, never the
exception message or event payload. Body size, output size, generated event
count, operation count, concurrency, label length, and allowed telemetry keys
are all fail-closed fixture limits.

Telemetry projections use these fixed names:

- span: `des.fel.run`
- structured-log event: `des.fel.run.completed`
- metrics: `des.fel.events.executed`, `des.fel.operations`,
  `des.fel.queue.peak`, `des.fel.storage.peak`,
  `des.fel.work.cancelled`, and `des.fel.work.rejected`
- attributes: `simulation_id`, `run_id`, `model_id`, and `outcome`

No model content or event payload is a telemetry attribute. The run comparison
digest supplies a bounded correlation identifier.

## Web/MCP projection boundary

`ConformanceGate` is a transport-neutral adapter contract. Its web and MCP
projections accept only `describe_fixture` and `run_fixture`; both return the
same semantic body for the same versioned request. Mutation verbs, source code,
inline models, unknown scenarios, unknown fields, and oversized inputs fail
closed. Saturated admission returns an explicit overload error, releases all
slots, and permits a subsequent healthy request.

This projection is deliberately not wired to a network listener or MCP
process. A production adapter must invoke the same fixture through each real
surface and compare its response to this semantic body before live web/MCP
parity can be claimed.

## Reproduce

Run exactly the checks used by hosted CI:

```console
python -m compileall -q src tests scripts
python scripts/verify_repository.py
PYTHONPATH=src DEEP_TEST_SEEDS=0-49 python -m unittest discover -s tests -v
```

The test suite also collects one informational Python-oracle runtime sample:
duration, events per second, operation-latency p50/p95/p99/max, and peak traced
memory. Tests verify the schema and non-negative measurements but never impose
absolute thresholds. Those numbers describe only the reference oracle.

## Remaining production evidence

The following work remains explicit and must not be inferred from this suite:

- run the same scenarios against the production Rust scheduler;
- execute the legacy TypeScript kernel and replace recorded observations with
  adapter-produced evidence;
- exercise live authenticated read-only web and MCP endpoints and compare their
  schemas, errors, limits, cancellation, and backpressure behavior;
- collect wall-clock, process-memory, and OpenTelemetry evidence from the Rust
  runtime rather than the Python reference oracle.
