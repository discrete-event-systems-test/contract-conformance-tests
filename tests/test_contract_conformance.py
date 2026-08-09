import unittest

from deep_tests.contract_model import (
    Command,
    IdempotencyConflict,
    ReferenceStore,
    generate_valid_trace,
    replay,
)


class ContractConformanceTests(unittest.TestCase):
    def test_stateful_model_replays_deterministically_across_many_seeds(self) -> None:
        for seed in range(32):
            commands = generate_valid_trace(seed, steps=240)
            first = replay(commands, duplicate_every=7)
            second = replay(commands, duplicate_every=5)
            self.assertEqual(first.snapshot(), second.snapshot(), f"seed={seed}")
            revisions = [outcome.revision for outcome in first.history]
            self.assertEqual(revisions, list(range(1, len(revisions) + 1)))

    def test_idempotency_key_reuse_with_different_intent_fails_closed(self) -> None:
        store = ReferenceStore()
        store.apply(Command("create", "a", "one", "same-key"))
        with self.assertRaises(IdempotencyConflict):
            store.apply(Command("update", "a", "two", "same-key"))

    def test_snapshot_is_canonical_independent_of_insertion_order(self) -> None:
        left = ReferenceStore()
        right = ReferenceStore()
        for entity_id in ("b", "a", "c"):
            left.apply(Command("create", entity_id, entity_id.upper(), f"l-{entity_id}"))
        for entity_id in ("c", "b", "a"):
            right.apply(Command("create", entity_id, entity_id.upper(), f"r-{entity_id}"))
        self.assertEqual(left.snapshot(), right.snapshot())

    def test_delete_creates_tombstone_and_duplicate_is_side_effect_free(self) -> None:
        store = ReferenceStore()
        store.apply(Command("create", "a", "one", "create-a"))
        delete = Command("delete", "a", None, "delete-a")
        first = store.apply(delete)
        duplicate = store.apply(delete)
        self.assertEqual(first, duplicate)
        self.assertEqual(store.revision, 2)
        self.assertIn('"tombstones":["a"]', store.snapshot())


if __name__ == "__main__":
    unittest.main()
