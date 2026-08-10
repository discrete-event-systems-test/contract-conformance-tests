from __future__ import annotations

import random
import unittest

from deep_tests.fel_model import Event, Scheduler


class FelSchedulerContractTests(unittest.TestCase):
    def test_equal_time_events_are_stable_and_exactly_once(self) -> None:
        scheduler = Scheduler()
        for event_id in ("alpha", "beta", "gamma"):
            scheduler.schedule(Event(event_id, 10, event_id.upper()))
        trace = scheduler.drain()
        self.assertEqual([event.event_id for event in trace], ["alpha", "beta", "gamma"])
        self.assertEqual([event.at for event in trace], [10, 10, 10])
        self.assertEqual(len({event.event_id for event in trace}), len(trace))
        self.assertEqual(scheduler.pending_count, 0)
        self.assertIsNone(scheduler.pop_next())

    def test_cancellation_and_reschedule_fail_closed(self) -> None:
        scheduler = Scheduler()
        scheduler.schedule(Event("cancel-me", 4, "x"))
        scheduler.schedule(Event("peer", 8, "y"))
        scheduler.schedule(Event("move-me", 5, "z"))
        self.assertTrue(scheduler.cancel("cancel-me"))
        self.assertFalse(scheduler.cancel("cancel-me"))
        scheduler.reschedule("move-me", 8)
        trace = scheduler.drain()
        self.assertEqual([event.event_id for event in trace], ["peer", "move-me"])
        self.assertEqual([event.at for event in trace], [8, 8])

    def test_time_never_regresses_and_invalid_time_is_rejected(self) -> None:
        scheduler = Scheduler()
        with self.assertRaises(ValueError):
            scheduler.schedule(Event("negative", -1, "x"))
        scheduler.schedule(Event("now", 2, "x"))
        self.assertEqual(scheduler.pop_next().at, 2)  # type: ignore[union-attr]
        with self.assertRaises(ValueError):
            scheduler.schedule(Event("past", 1, "x"))
        with self.assertRaises(ValueError):
            scheduler.reschedule("missing", 1)

    def test_seeded_runs_reproduce_identical_trace_and_snapshot(self) -> None:
        def run(seed: int) -> tuple[str, tuple[tuple[str, int, str], ...]]:
            rng = random.Random(seed)
            scheduler = Scheduler()
            for index in range(250):
                scheduler.schedule(
                    Event(
                        event_id=f"event-{index}",
                        at=rng.randrange(0, 40),
                        payload=f"payload-{rng.randrange(0, 1000)}",
                    )
                )
            snapshot = scheduler.snapshot()
            trace = tuple(
                (event.event_id, event.at, event.payload)
                for event in scheduler.drain()
            )
            return snapshot, trace

        first = run(17)
        second = run(17)
        third = run(18)
        self.assertEqual(first, second)
        self.assertNotEqual(first, third)
        self.assertEqual([at for _, at, _ in first[1]], sorted(at for _, at, _ in first[1]))

    def test_same_time_storm_and_reschedule_churn_keep_storage_bounded(self) -> None:
        scheduler = Scheduler()
        for index in range(500):
            scheduler.schedule(Event(f"storm-{index}", 50, "payload"))
        for index in range(400):
            scheduler.reschedule(f"storm-{index}", 50)
        # The oracle may temporarily retain stale heap entries, but compaction
        # bounds hidden storage to a small multiple of live state.
        self.assertLessEqual(scheduler.storage_count, scheduler.pending_count * 3 + 16)
        trace = scheduler.drain(limit=600)
        self.assertEqual(len(trace), 500)
        self.assertEqual(len({event.event_id for event in trace}), 500)
        self.assertTrue(all(event.at == 50 for event in trace))

    def test_duplicate_pending_identifier_is_rejected(self) -> None:
        scheduler = Scheduler()
        scheduler.schedule(Event("one", 1, "a"))
        with self.assertRaises(ValueError):
            scheduler.schedule(Event("one", 2, "b"))


if __name__ == "__main__":
    unittest.main()
