from __future__ import annotations

import contextlib
import importlib.util
import io
import os
import sys
import unittest
from pathlib import Path
from unittest import mock

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

    def test_main_missing_config_is_noninteractive_by_default(self) -> None:
        mod = load_module()
        with mock.patch.dict(os.environ, {}, clear=True), \
            mock.patch.object(mod, "load_env", lambda path: None), \
            mock.patch.object(sys, "argv", ["region_talk_orchestrator.py"]):
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = mod.main()
        self.assertEqual(rc, 2)
        payload = out.getvalue()
        self.assertIn("missing_ydb_config", payload)
        self.assertIn("REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON", payload)
        self.assertIn("--allow-yc-fallback", payload)

    def test_main_accepts_service_account_key_as_direct_credential(self) -> None:
        mod = load_module()
        metrics = {
            "publication_sent_total": 0,
            "publication_confirmed_total": 0,
            "publication_unsent_confirmed_total": 0,
            "image_actual_scored_total": 0,
            "publication_candidate_total": 0,
            "bge_pending_sample_total": 0,
            "image_pending_total": 0,
        }
        env = {
            "REGION_TALK_YDB_ENDPOINT": "grpcs://ydb.example:2135",
            "REGION_TALK_YDB_DATABASE": "/ru-central1/example/db",
            "REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON": "{}",
        }
        with mock.patch.dict(os.environ, env, clear=True), \
            mock.patch.object(mod, "load_env", lambda path: None), \
            mock.patch.object(mod, "read_region_talk_queue_metrics", return_value=metrics) as read_metrics, \
            mock.patch.object(sys, "argv", ["region_talk_orchestrator.py"]):
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = mod.main()
        self.assertEqual(rc, 0)
        self.assertIn('"ok": true', out.getvalue())
        read_metrics.assert_called_once()
        self.assertFalse(read_metrics.call_args.kwargs["allow_yc_fallback"])


if __name__ == "__main__":
    unittest.main()
