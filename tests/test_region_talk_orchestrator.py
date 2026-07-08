from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "region_talk_orchestrator.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_orchestrator", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class RegionTalkOrchestratorTests(unittest.TestCase):
    def test_decision_prioritizes_notifier_finalizer_bge_image(self) -> None:
        mod = load_module()
        actions = mod.build_decision_plan(
            {
                "publication_sent_total": 0,
                "publication_confirmed_total": 2,
                "publication_unsent_confirmed_total": 1,
                "image_actual_scored_total": 5,
                "publication_candidate_total": 3,
                "bge_pending_sample_total": 4,
                "image_pending_total": 7,
            },
            target_confirmed=20,
            bge_threshold=1,
            image_threshold=1,
        )
        self.assertEqual([a["action"] for a in actions[:4]], ["notify_confirmed", "run_finalizer", "launch_bge_m3", "launch_image_diagnostic"])

    def test_decision_stops_at_target(self) -> None:
        mod = load_module()
        actions = mod.build_decision_plan({"publication_confirmed_total": 20, "publication_sent_total": 0}, target_confirmed=20, bge_threshold=1, image_threshold=1)
        self.assertEqual(actions, [{"action": "stop", "reason": "target_confirmed_reached"}])


if __name__ == "__main__":
    unittest.main()
