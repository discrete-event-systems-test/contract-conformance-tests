from __future__ import annotations

import hashlib
import json
import random
import re
import threading
import time
import tracemalloc
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping

from deep_tests.fel_model import Event, ExecutedEvent, Scheduler


_IDENTIFIER = re.compile(r"^[A-Za-z0-9._:-]+$")
_TRANSPORTS = frozenset({"mcp", "web"})
_OPERATIONS = frozenset({"describe_fixture", "run_fixture"})
_CLASSIFICATIONS = frozenset(
    {
        "equivalent",
        "intentional_policy_difference",
        "not_observed",
        "unclassified_mismatch",
    }
)


class ContractViolation(ValueError):
    """A versioned evidence contract or request failed closed."""


class Overloaded(RuntimeError):
    """The bounded admission gate has no free request slot."""


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _integer(value: object, name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ContractViolation(f"{name} must be an integer >= {minimum}")
    return value


def _identifier(value: object, name: str, *, max_chars: int = 64) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > max_chars
        or _IDENTIFIER.fullmatch(value) is None
    ):
        raise ContractViolation(
            f"{name} must be 1-{max_chars} safe identifier characters"
        )
    return value


def _require_keys(value: Mapping[str, object], expected: set[str], name: str) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise ContractViolation(f"{name} keys differ: missing={missing}, extra={extra}")


@dataclass(frozen=True)
class Limits:
    max_attribute_value_chars: int
    max_body_bytes: int
    max_concurrency: int
    max_events: int
    max_output_bytes: int
    max_trace_events: int
    operation_budget: int
    allowed_attribute_keys: tuple[str, ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> Limits:
        _require_keys(
            value,
            {
                "allowed_attribute_keys",
                "max_attribute_value_chars",
                "max_body_bytes",
                "max_concurrency",
                "max_events",
                "max_output_bytes",
                "max_trace_events",
                "operation_budget",
            },
            "limits",
        )
        raw_keys = value["allowed_attribute_keys"]
        if not isinstance(raw_keys, list) or not raw_keys:
            raise ContractViolation("allowed_attribute_keys must be a non-empty list")
        keys = tuple(
            _identifier(item, "allowed_attribute_keys item") for item in raw_keys
        )
        if len(keys) != len(set(keys)):
            raise ContractViolation("allowed_attribute_keys must be unique")
        required_keys = {"model_id", "outcome", "run_id", "simulation_id"}
        if set(keys) != required_keys:
            raise ContractViolation("allowed_attribute_keys must use the bounded v1 set")
        return cls(
            max_attribute_value_chars=_integer(
                value["max_attribute_value_chars"],
                "max_attribute_value_chars",
                minimum=1,
            ),
            max_body_bytes=_integer(
                value["max_body_bytes"], "max_body_bytes", minimum=1
            ),
            max_concurrency=_integer(
                value["max_concurrency"], "max_concurrency", minimum=1
            ),
            max_events=_integer(value["max_events"], "max_events", minimum=1),
            max_output_bytes=_integer(
                value["max_output_bytes"], "max_output_bytes", minimum=1
            ),
            max_trace_events=_integer(
                value["max_trace_events"], "max_trace_events", minimum=1
            ),
            operation_budget=_integer(
                value["operation_budget"], "operation_budget", minimum=1
            ),
            allowed_attribute_keys=keys,
        )


@dataclass(frozen=True)
class Scenario:
    id: str
    seed: int
    initial_events: int
    horizon: int
    same_timestamp_burst: int
    fan_out: int
    fan_out_depth: int
    cancel_every: int
    reschedule_every: int

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> Scenario:
        _require_keys(
            value,
            {
                "cancel_every",
                "fan_out",
                "fan_out_depth",
                "horizon",
                "id",
                "initial_events",
                "reschedule_every",
                "same_timestamp_burst",
                "seed",
            },
            "scenario",
        )
        return cls(
            id=_identifier(value["id"], "scenario.id"),
            seed=_integer(value["seed"], "scenario.seed"),
            initial_events=_integer(
                value["initial_events"], "scenario.initial_events", minimum=1
            ),
            horizon=_integer(value["horizon"], "scenario.horizon"),
            same_timestamp_burst=_integer(
                value["same_timestamp_burst"],
                "scenario.same_timestamp_burst",
                minimum=1,
            ),
            fan_out=_integer(value["fan_out"], "scenario.fan_out"),
            fan_out_depth=_integer(
                value["fan_out_depth"], "scenario.fan_out_depth"
            ),
            cancel_every=_integer(
                value["cancel_every"], "scenario.cancel_every"
            ),
            reschedule_every=_integer(
                value["reschedule_every"], "scenario.reschedule_every"
            ),
        )

    @property
    def maximum_generated_events(self) -> int:
        multiplier = 1
        generation_size = 1
        for _ in range(self.fan_out_depth):
            generation_size *= self.fan_out
            multiplier += generation_size
        return self.initial_events * multiplier


@dataclass(frozen=True)
class DifferentialCase:
    id: str
    oracle_trace: tuple[Mapping[str, object], ...]
    observed_legacy_trace: tuple[Mapping[str, object], ...] | None
    documented_policy_id: str | None
    expected_classification: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> DifferentialCase:
        _require_keys(
            value,
            {
                "documented_policy_id",
                "expected_classification",
                "id",
                "observed_legacy_trace",
                "oracle_trace",
            },
            "differential case",
        )
        oracle_trace = _trace(value["oracle_trace"], "oracle_trace")
        raw_legacy = value["observed_legacy_trace"]
        legacy_trace = (
            None
            if raw_legacy is None
            else _trace(raw_legacy, "observed_legacy_trace")
        )
        policy = value["documented_policy_id"]
        if policy is not None:
            policy = _identifier(policy, "documented_policy_id")
        classification = value["expected_classification"]
        if classification not in _CLASSIFICATIONS:
            raise ContractViolation("unknown differential classification")
        return cls(
            id=_identifier(value["id"], "differential_case.id"),
            oracle_trace=oracle_trace,
            observed_legacy_trace=legacy_trace,
            documented_policy_id=policy,
            expected_classification=str(classification),
        )


def _trace(value: object, name: str) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list):
        raise ContractViolation(f"{name} must be a list")
    trace: list[Mapping[str, object]] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise ContractViolation(f"{name}[{index}] must be an object")
        _require_keys(item, {"at", "event_id"}, f"{name}[{index}]")
        trace.append(
            {
                "at": _integer(item["at"], f"{name}[{index}].at", minimum=-1),
                "event_id": _identifier(
                    item["event_id"], f"{name}[{index}].event_id"
                ),
            }
        )
    return tuple(trace)


@dataclass(frozen=True)
class EvidenceContract:
    contract_id: str
    schema_version: int
    limits: Limits
    scenarios: tuple[Scenario, ...]
    differential_cases: tuple[DifferentialCase, ...]
    fixture_digest: str

    @classmethod
    def load(cls, path: Path) -> EvidenceContract:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ContractViolation("evidence fixture is not readable JSON") from error
        if not isinstance(raw, dict):
            raise ContractViolation("evidence fixture must be an object")
        _require_keys(
            raw,
            {
                "contract_id",
                "differential_cases",
                "limits",
                "scenarios",
                "schema_version",
            },
            "evidence fixture",
        )
        if not isinstance(raw["limits"], dict):
            raise ContractViolation("limits must be an object")
        raw_scenarios = raw["scenarios"]
        raw_cases = raw["differential_cases"]
        if not isinstance(raw_scenarios, list) or not raw_scenarios:
            raise ContractViolation("scenarios must be a non-empty list")
        if not isinstance(raw_cases, list) or not raw_cases:
            raise ContractViolation("differential_cases must be a non-empty list")
        scenarios = tuple(
            Scenario.from_mapping(_mapping(item, "scenario"))
            for item in raw_scenarios
        )
        cases = tuple(
            DifferentialCase.from_mapping(_mapping(item, "differential case"))
            for item in raw_cases
        )
        limits = Limits.from_mapping(raw["limits"])
        _unique((item.id for item in scenarios), name="scenario ids")
        _unique((item.id for item in cases), name="differential case ids")
        for scenario in scenarios:
            if scenario.maximum_generated_events > limits.max_events:
                raise ContractViolation(
                    f"scenario {scenario.id} can exceed max_events"
                )
        contract = cls(
            contract_id=_identifier(raw["contract_id"], "contract_id"),
            schema_version=_integer(
                raw["schema_version"], "schema_version", minimum=1
            ),
            limits=limits,
            scenarios=scenarios,
            differential_cases=cases,
            fixture_digest=_digest(raw),
        )
        if contract.schema_version != 1:
            raise ContractViolation("unsupported evidence schema_version")
        for case in contract.differential_cases:
            if classify_differential(case) != case.expected_classification:
                raise ContractViolation(
                    f"differential case {case.id} has stale classification"
                )
        return contract

    def scenario(self, scenario_id: str) -> Scenario:
        for scenario in self.scenarios:
            if scenario.id == scenario_id:
                return scenario
        raise ContractViolation("unknown versioned scenario")


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise ContractViolation(f"{name} must be an object")
    return value


def _unique(values: Iterator[str], *, name: str) -> None:
    materialized = tuple(values)
    if len(materialized) != len(set(materialized)):
        raise ContractViolation(f"{name} must be unique")


def classify_differential(case: DifferentialCase) -> str:
    """Classify observed legacy evidence without manufacturing an equivalence."""

    if case.observed_legacy_trace is None:
        return "not_observed"
    if case.oracle_trace == case.observed_legacy_trace:
        return "equivalent"
    if case.documented_policy_id is not None:
        return "intentional_policy_difference"
    return "unclassified_mismatch"


@dataclass(frozen=True)
class BenchmarkReport:
    contract_id: str
    fixture_digest: str
    scenario_id: str
    seed: int
    input_digest: str
    comparison_digest: str
    scheduled_count: int
    executed_count: int
    cancelled_count: int
    rescheduled_count: int
    rejected_count: int
    operation_count: int
    peak_queue_occupancy: int
    peak_storage_count: int
    simulated_end_tick: int
    termination_reason: str
    trace_digest: str
    trace_preview: tuple[Mapping[str, object], ...]
    runtime_measurements: Mapping[str, object]

    def semantic_manifest(self) -> Mapping[str, object]:
        return asdict(self)


def run_scenario(
    contract: EvidenceContract,
    scenario: Scenario,
    *,
    measure_runtime: bool = False,
) -> BenchmarkReport:
    """Execute a bounded synthetic workload against the independent FEL oracle."""

    if scenario not in contract.scenarios:
        raise ContractViolation("scenario is not part of this evidence contract")
    scheduler = Scheduler()
    rng = random.Random(scenario.seed)
    operations = 0
    scheduled = 0
    cancelled = 0
    rescheduled = 0
    rejected = 0
    peak_queue = 0
    peak_storage = 0
    latencies: list[int] = []
    generations: dict[str, int] = {}
    original_times: dict[str, int] = {}
    tracing_before = tracemalloc.is_tracing()
    if measure_runtime and not tracing_before:
        tracemalloc.start()
    runtime_start = time.perf_counter_ns() if measure_runtime else 0

    def call(function: Callable[[], Any]) -> Any:
        nonlocal operations, peak_queue, peak_storage
        if operations >= contract.limits.operation_budget:
            raise ContractViolation("operation budget exhausted")
        started = time.perf_counter_ns() if measure_runtime else 0
        result = function()
        if measure_runtime:
            latencies.append(max(0, time.perf_counter_ns() - started))
        operations += 1
        peak_queue = max(peak_queue, scheduler.pending_count)
        peak_storage = max(peak_storage, scheduler.storage_count)
        return result

    try:
        for index in range(scenario.initial_events):
            event_id = f"event-{index}"
            at = _initial_time(scenario, index, rng)
            call(
                lambda event_id=event_id, at=at: scheduler.schedule(
                    Event(event_id, at, "synthetic-generation-0")
                )
            )
            scheduled += 1
            generations[event_id] = 0
            original_times[event_id] = at

        cancelled_ids: set[str] = set()
        if scenario.cancel_every:
            for index in range(scenario.cancel_every - 1, scenario.initial_events, scenario.cancel_every):
                event_id = f"event-{index}"
                if call(lambda event_id=event_id: scheduler.cancel(event_id)):
                    cancelled += 1
                    cancelled_ids.add(event_id)

        if scenario.reschedule_every:
            for index in range(
                scenario.reschedule_every - 1,
                scenario.initial_events,
                scenario.reschedule_every,
            ):
                event_id = f"event-{index}"
                if event_id in cancelled_ids:
                    continue
                at = original_times[event_id]
                call(
                    lambda event_id=event_id, at=at: scheduler.reschedule(
                        event_id, at
                    )
                )
                rescheduled += 1

        executed: list[Mapping[str, object]] = []
        while scheduler.pending_count:
            event = call(scheduler.pop_next)
            if event is None:
                raise AssertionError("pending oracle event became unreachable")
            executed.append(_safe_event(event))
            generation = generations[event.event_id]
            if generation >= scenario.fan_out_depth:
                continue
            for child_index in range(scenario.fan_out):
                if scheduled >= contract.limits.max_events:
                    rejected += 1
                    continue
                child_id = f"{event.event_id}.c{child_index}"
                child_generation = generation + 1
                child_at = event.at + 1 + rng.randrange(0, 2)
                call(
                    lambda child_id=child_id,
                    child_at=child_at,
                    child_generation=child_generation: scheduler.schedule(
                        Event(
                            child_id,
                            child_at,
                            f"synthetic-generation-{child_generation}",
                        )
                    )
                )
                scheduled += 1
                generations[child_id] = child_generation

        duration_ns = max(0, time.perf_counter_ns() - runtime_start)
        runtime = _runtime_measurements(
            measure_runtime,
            duration_ns,
            latencies,
            len(executed),
        )
        if measure_runtime:
            _current, peak_memory = tracemalloc.get_traced_memory()
            runtime = {**runtime, "peak_traced_memory_bytes": peak_memory}
    finally:
        if measure_runtime and not tracing_before and tracemalloc.is_tracing():
            tracemalloc.stop()

    trace_digest = _digest(executed)
    scenario_input = asdict(scenario)
    comparison_fields = {
        "cancelled_count": cancelled,
        "executed_count": len(executed),
        "input_digest": _digest(scenario_input),
        "operation_count": operations,
        "peak_queue_occupancy": peak_queue,
        "peak_storage_count": peak_storage,
        "rejected_count": rejected,
        "rescheduled_count": rescheduled,
        "scheduled_count": scheduled,
        "simulated_end_tick": scheduler.current_time,
        "termination_reason": "drained",
        "trace_digest": trace_digest,
    }
    return BenchmarkReport(
        contract_id=contract.contract_id,
        fixture_digest=contract.fixture_digest,
        scenario_id=scenario.id,
        seed=scenario.seed,
        input_digest=comparison_fields["input_digest"],
        comparison_digest=_digest(comparison_fields),
        scheduled_count=scheduled,
        executed_count=len(executed),
        cancelled_count=cancelled,
        rescheduled_count=rescheduled,
        rejected_count=rejected,
        operation_count=operations,
        peak_queue_occupancy=peak_queue,
        peak_storage_count=peak_storage,
        simulated_end_tick=scheduler.current_time,
        termination_reason="drained",
        trace_digest=trace_digest,
        trace_preview=tuple(executed[: contract.limits.max_trace_events]),
        runtime_measurements=runtime,
    )


def _initial_time(scenario: Scenario, index: int, rng: random.Random) -> int:
    if scenario.horizon == 0:
        return 0
    if scenario.same_timestamp_burst == 1:
        return rng.randrange(0, scenario.horizon + 1)
    group = index // scenario.same_timestamp_burst
    group_count = max(
        1,
        (scenario.initial_events + scenario.same_timestamp_burst - 1)
        // scenario.same_timestamp_burst,
    )
    return round(group * scenario.horizon / max(1, group_count - 1))


def _safe_event(event: ExecutedEvent) -> Mapping[str, object]:
    return {"at": event.at, "event_id": event.event_id}


def _runtime_measurements(
    enabled: bool,
    duration_ns: int,
    latencies: list[int],
    executed_count: int,
) -> Mapping[str, object]:
    if not enabled:
        return {
            "reason": "deterministic contract projection",
            "status": "not_collected",
        }
    ordered = sorted(latencies)
    return {
        "duration_ns": duration_ns,
        "operation_latency_ns": {
            "max": ordered[-1] if ordered else 0,
            "p50": _percentile(ordered, 50),
            "p95": _percentile(ordered, 95),
            "p99": _percentile(ordered, 99),
            "sample_count": len(ordered),
        },
        "peak_traced_memory_bytes": 0,
        "status": "collected_reference_oracle_only",
        "throughput_events_per_second": (
            0 if duration_ns == 0 else executed_count * 1_000_000_000 // duration_ns
        ),
    }


def _percentile(ordered: list[int], percentile: int) -> int:
    if not ordered:
        return 0
    index = max(0, (len(ordered) * percentile + 99) // 100 - 1)
    return ordered[min(index, len(ordered) - 1)]


def bounded_failure_artifact(
    contract: EvidenceContract,
    error: BaseException,
    trace: list[Mapping[str, object]],
) -> Mapping[str, object]:
    """Return a payload-free, bounded artifact while hashing the complete trace."""

    safe_trace = [
        {
            "at": _integer(item.get("at"), "failure trace at"),
            "event_id": _identifier(item.get("event_id"), "failure trace event_id"),
        }
        for item in trace
    ]
    return {
        "error_code": type(error).__name__[:64],
        "omitted_event_count": max(
            0, len(safe_trace) - contract.limits.max_trace_events
        ),
        "trace_digest": _digest(safe_trace),
        "trace_preview": safe_trace[: contract.limits.max_trace_events],
    }


def telemetry_envelope(
    contract: EvidenceContract,
    report: BenchmarkReport,
    *,
    simulation_id: str,
    run_id: str,
    model_id: str,
) -> Mapping[str, object]:
    attributes = {
        "model_id": model_id,
        "outcome": report.termination_reason,
        "run_id": run_id,
        "simulation_id": simulation_id,
    }
    if set(attributes) != set(contract.limits.allowed_attribute_keys):
        raise ContractViolation("telemetry attribute keys drifted from the v1 contract")
    safe_attributes = {
        key: _identifier(
            value,
            f"telemetry.{key}",
            max_chars=contract.limits.max_attribute_value_chars,
        )
        for key, value in attributes.items()
    }
    metrics = {
        "des.fel.events.executed": report.executed_count,
        "des.fel.operations": report.operation_count,
        "des.fel.queue.peak": report.peak_queue_occupancy,
        "des.fel.storage.peak": report.peak_storage_count,
        "des.fel.work.cancelled": report.cancelled_count,
        "des.fel.work.rejected": report.rejected_count,
    }
    return {
        "log": {
            "attributes": safe_attributes,
            "event": "des.fel.run.completed",
            "manifest_digest": report.comparison_digest,
        },
        "metrics": metrics,
        "span": {
            "attributes": safe_attributes,
            "name": "des.fel.run",
            "trace_id": report.comparison_digest[:32],
        },
    }


@dataclass(frozen=True)
class SurfaceResponse:
    transport: str
    semantic_body: Mapping[str, object]


class ConformanceGate:
    """Transport-neutral, read-only projection for future web/MCP adapters."""

    def __init__(self, contract: EvidenceContract) -> None:
        self.contract = contract
        self._active_requests = 0
        self._rejected_requests = 0
        self._lock = threading.Lock()

    @property
    def active_requests(self) -> int:
        with self._lock:
            return self._active_requests

    @property
    def rejected_requests(self) -> int:
        with self._lock:
            return self._rejected_requests

    @contextmanager
    def hold_slot(self) -> Iterator[None]:
        with self._lock:
            if self._active_requests >= self.contract.limits.max_concurrency:
                self._rejected_requests += 1
                raise Overloaded("conformance request concurrency limit reached")
            self._active_requests += 1
        try:
            yield
        finally:
            with self._lock:
                self._active_requests -= 1

    def invoke(
        self,
        transport: str,
        operation: str,
        request_body: Mapping[str, object],
    ) -> SurfaceResponse:
        if transport not in _TRANSPORTS:
            raise ContractViolation("unknown conformance transport")
        if operation not in _OPERATIONS:
            raise ContractViolation("operation is not in the read-only allowlist")
        if not isinstance(request_body, Mapping):
            raise ContractViolation("request body must be an object")
        try:
            encoded_body = _canonical_json(request_body).encode("utf-8")
        except (TypeError, ValueError) as error:
            raise ContractViolation("request body is not canonical JSON") from error
        if len(encoded_body) > self.contract.limits.max_body_bytes:
            raise ContractViolation("request body exceeds max_body_bytes")
        with self.hold_slot():
            semantic_body = self._execute(operation, request_body)
            output_size = len(_canonical_json(semantic_body).encode("utf-8"))
            if output_size > self.contract.limits.max_output_bytes:
                raise ContractViolation("response exceeds max_output_bytes")
            return SurfaceResponse(transport=transport, semantic_body=semantic_body)

    def _execute(
        self, operation: str, request_body: Mapping[str, object]
    ) -> Mapping[str, object]:
        if operation == "describe_fixture":
            _require_keys(request_body, set(), "describe_fixture request")
            return {
                "contract_id": self.contract.contract_id,
                "fixture_digest": self.contract.fixture_digest,
                "limits": asdict(self.contract.limits),
                "scenario_ids": [item.id for item in self.contract.scenarios],
                "schema_version": self.contract.schema_version,
            }
        _require_keys(request_body, {"scenario_id"}, "run_fixture request")
        scenario_id = _identifier(request_body["scenario_id"], "scenario_id")
        report = run_scenario(self.contract, self.contract.scenario(scenario_id))
        return report.semantic_manifest()
