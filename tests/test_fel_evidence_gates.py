from __future__ import annotations

import json
import unittest
from dataclasses import replace
from pathlib import Path

from deep_tests.fel_evidence import (
    ConformanceGate,
    ContractViolation,
    DifferentialCase,
    EvidenceContract,
    Overloaded,
    bounded_failure_artifact,
    classify_differential,
    run_scenario,
    telemetry_envelope,
)


FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "fel"
    / "evidence-cases-v1.json"
)


class FelEvidenceGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.contract = EvidenceContract.load(FIXTURE)

    def test_fixture_covers_each_bounded_workload_dimension(self) -> None:
        self.assertEqual(
            {scenario.id for scenario in self.contract.scenarios},
            {
                "cancellation-density",
                "event-rate",
                "fan-out",
                "long-horizon",
                "queue-depth",
                "same-time-storm",
            },
        )
        self.assertTrue(
            all(
                scenario.maximum_generated_events <= self.contract.limits.max_events
                for scenario in self.contract.scenarios
            )
        )

    def test_all_semantic_manifests_replay_byte_for_byte(self) -> None:
        for scenario in self.contract.scenarios:
            with self.subTest(scenario=scenario.id):
                first = run_scenario(self.contract, scenario).semantic_manifest()
                second = run_scenario(self.contract, scenario).semantic_manifest()
                self.assertEqual(first, second)
                self.assertEqual(first["termination_reason"], "drained")
                self.assertLessEqual(
                    len(first["trace_preview"]),
                    self.contract.limits.max_trace_events,
                )

    def test_dimension_specific_counts_are_comparable_without_thresholds(self) -> None:
        reports = {
            scenario.id: run_scenario(self.contract, scenario)
            for scenario in self.contract.scenarios
        }
        self.assertEqual(reports["queue-depth"].peak_queue_occupancy, 192)
        self.assertEqual(reports["event-rate"].simulated_end_tick, 8)
        self.assertEqual(reports["fan-out"].scheduled_count, 112)
        self.assertEqual(reports["fan-out"].executed_count, 112)
        self.assertEqual(reports["cancellation-density"].cancelled_count, 100)
        self.assertEqual(reports["cancellation-density"].executed_count, 100)
        self.assertGreaterEqual(reports["long-horizon"].simulated_end_tick, 900_000)
        self.assertEqual(reports["same-time-storm"].simulated_end_tick, 0)
        self.assertEqual(reports["same-time-storm"].rescheduled_count, 51)
        self.assertTrue(
            all(report.rejected_count == 0 for report in reports.values())
        )

    def test_reference_runtime_measurements_are_informational_only(self) -> None:
        scenario = self.contract.scenario("queue-depth")
        deterministic = run_scenario(self.contract, scenario)
        measured = run_scenario(self.contract, scenario, measure_runtime=True)
        runtime = measured.runtime_measurements
        self.assertEqual(
            runtime["status"], "collected_reference_oracle_only"
        )
        self.assertGreaterEqual(runtime["duration_ns"], 0)
        self.assertGreater(runtime["peak_traced_memory_bytes"], 0)
        latency = runtime["operation_latency_ns"]
        self.assertEqual(latency["sample_count"], measured.operation_count)
        self.assertGreaterEqual(latency["max"], latency["p99"])
        self.assertEqual(
            measured.comparison_digest, deterministic.comparison_digest
        )

    def test_differential_mismatches_are_never_silently_normalized(self) -> None:
        observed = {
            case.id: classify_differential(case)
            for case in self.contract.differential_cases
        }
        self.assertEqual(observed["stable-same-time-order"], "equivalent")
        self.assertEqual(
            observed["rescheduled-peer-order"],
            "intentional_policy_difference",
        )
        self.assertEqual(
            observed["legacy-cancellation-race-unavailable"], "not_observed"
        )
        unclassified = DifferentialCase(
            id="new-mismatch",
            oracle_trace=({"at": 1, "event_id": "a"},),
            observed_legacy_trace=({"at": 1, "event_id": "b"},),
            documented_policy_id=None,
            expected_classification="unclassified_mismatch",
        )
        self.assertEqual(
            classify_differential(unclassified), "unclassified_mismatch"
        )

    def test_failure_artifact_is_capped_hashed_and_payload_free(self) -> None:
        trace = [
            {"at": index, "event_id": f"event-{index}"} for index in range(200)
        ]
        artifact = bounded_failure_artifact(
            self.contract,
            RuntimeError("sensitive-event-payload"),
            trace,
        )
        self.assertEqual(
            len(artifact["trace_preview"]),
            self.contract.limits.max_trace_events,
        )
        self.assertEqual(artifact["omitted_event_count"], 152)
        self.assertEqual(len(artifact["trace_digest"]), 64)
        encoded = json.dumps(artifact, sort_keys=True)
        self.assertNotIn("sensitive-event-payload", encoded)
        self.assertLess(len(encoded), self.contract.limits.max_output_bytes)

    def test_telemetry_uses_only_fixed_bounded_labels_and_safe_metrics(self) -> None:
        report = run_scenario(
            self.contract, self.contract.scenario("cancellation-density")
        )
        envelope = telemetry_envelope(
            self.contract,
            report,
            simulation_id="simulation-1",
            run_id=report.comparison_digest,
            model_id="reference-oracle",
        )
        expected_attributes = set(self.contract.limits.allowed_attribute_keys)
        self.assertEqual(
            set(envelope["span"]["attributes"]), expected_attributes
        )
        self.assertEqual(
            envelope["span"]["attributes"], envelope["log"]["attributes"]
        )
        self.assertEqual(
            set(envelope["metrics"]),
            {
                "des.fel.events.executed",
                "des.fel.operations",
                "des.fel.queue.peak",
                "des.fel.storage.peak",
                "des.fel.work.cancelled",
                "des.fel.work.rejected",
            },
        )
        with self.assertRaises(ContractViolation):
            telemetry_envelope(
                self.contract,
                report,
                simulation_id="x" * 65,
                run_id=report.comparison_digest,
                model_id="reference-oracle",
            )

    def test_web_and_mcp_projections_have_identical_read_only_semantics(self) -> None:
        gate = ConformanceGate(self.contract)
        for operation, body in (
            ("describe_fixture", {}),
            ("run_fixture", {"scenario_id": "fan-out"}),
        ):
            with self.subTest(operation=operation):
                web = gate.invoke("web", operation, body)
                mcp = gate.invoke("mcp", operation, body)
                self.assertEqual(web.semantic_body, mcp.semantic_body)
                self.assertEqual(web.transport, "web")
                self.assertEqual(mcp.transport, "mcp")

    def test_surface_rejects_mutation_code_and_unversioned_inputs(self) -> None:
        gate = ConformanceGate(self.contract)
        for operation in ("delete_model", "execute_code", "schedule_event"):
            with self.subTest(operation=operation):
                with self.assertRaises(ContractViolation):
                    gate.invoke("web", operation, {})
        with self.assertRaises(ContractViolation):
            gate.invoke("mcp", "run_fixture", {"scenario_id": "arbitrary"})
        with self.assertRaises(ContractViolation):
            gate.invoke(
                "web",
                "run_fixture",
                {"scenario_id": "fan-out", "model_source": "untrusted"},
            )
        with self.assertRaises(ContractViolation):
            gate.invoke(
                "mcp",
                "run_fixture",
                {"scenario_id": "x" * self.contract.limits.max_body_bytes},
            )

    def test_backpressure_is_explicit_and_process_remains_healthy(self) -> None:
        gate = ConformanceGate(self.contract)
        with gate.hold_slot():
            with gate.hold_slot():
                self.assertEqual(
                    gate.active_requests, self.contract.limits.max_concurrency
                )
                with self.assertRaises(Overloaded):
                    with gate.hold_slot():
                        self.fail("over-capacity slot must not be admitted")
        self.assertEqual(gate.active_requests, 0)
        self.assertEqual(gate.rejected_requests, 1)
        response = gate.invoke("web", "describe_fixture", {})
        self.assertEqual(response.semantic_body["contract_id"], "fel-evidence-v1")

    def test_operation_and_output_budgets_fail_closed(self) -> None:
        scenario = self.contract.scenario("queue-depth")
        small_operation_contract = replace(
            self.contract,
            limits=replace(self.contract.limits, operation_budget=1),
        )
        with self.assertRaisesRegex(ContractViolation, "operation budget"):
            run_scenario(small_operation_contract, scenario)

        small_output_contract = replace(
            self.contract,
            limits=replace(self.contract.limits, max_output_bytes=16),
        )
        with self.assertRaisesRegex(ContractViolation, "max_output_bytes"):
            ConformanceGate(small_output_contract).invoke(
                "mcp", "describe_fixture", {}
            )


if __name__ == "__main__":
    unittest.main()
