import tomllib
import unittest
from pathlib import Path
from deep_tests.contract_model import Command, IdempotencyConflict, ReferenceStore, generate_valid_trace, replay
class CounterpartTipHardeningTests(unittest.TestCase):
    def test_duplicate_create_is_exactly_once_and_does_not_advance_revision(self):
        store=ReferenceStore(); command=Command("create","alpha","one","create-alpha"); first=store.apply(command); duplicate=store.apply(command); self.assertEqual(first,duplicate); self.assertEqual(store.revision,1)
    def test_idempotency_key_remains_bound_to_intent_after_intervening_writes(self):
        store=ReferenceStore(); store.apply(Command("create","alpha","one","stable-key")); store.apply(Command("create","beta","two","create-beta"));
        with self.assertRaises(IdempotencyConflict): store.apply(Command("update","alpha","changed","stable-key"))
    def test_large_trace_is_convergent_across_multiple_duplicate_schedules(self):
        commands=generate_valid_trace(20260914,steps=600); self.assertEqual(len({replay(commands,duplicate_every=n).snapshot() for n in (2,3,7,11)}),1)
    def test_zed_pkg_test_script_runs_suite_and_repository_verifier(self):
        config=tomllib.loads(Path(".zpkg.toml").read_text()); script=config["scripts"]["test"]; self.assertIn("unittest discover",script); self.assertIn("scripts/verify_repository.py",script); self.assertEqual(config["install"]["dir"],".vendor/.zed")
if __name__ == "__main__": unittest.main()
