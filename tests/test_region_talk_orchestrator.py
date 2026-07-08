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
        self.assertEqual([a["action"] for a in actions[:5]], ["notify_confirmed", "launch_bge_m3", "launch_image_diagnostic", "launch_candidate_report", "run_finalizer"])
        self.assertTrue(actions[1]["parallel_safe"])
        self.assertTrue(actions[2]["parallel_safe"])
        self.assertTrue(actions[3]["parallel_safe"])
        self.assertIn("--batch-limit", actions[1]["cmd"])
        self.assertIn("12", actions[1]["cmd"])
        self.assertIn("--batch-size", actions[1]["cmd"])
        self.assertIn("4", actions[1]["cmd"])
        self.assertEqual(actions[3]["env"]["REGION_TALK_STATE_BACKEND"], "ydb")
        self.assertEqual(actions[3]["env"]["REGION_TALK_REQUIRE_YDB_STATE"], "1")
        self.assertEqual(actions[3]["env"]["REGION_TALK_TEXT_EMBEDDING_MODEL_IDS"], "intfloat/multilingual-e5-base")
        self.assertEqual(actions[3]["env"]["REGION_TALK_REQUIRE_EXTERNAL_BGE_M3_FOR_IMAGE_QUEUE"], "1")
        self.assertIn("--max-sources", actions[3]["cmd"])
        self.assertIn("12", actions[3]["cmd"])
        self.assertEqual(actions[3]["env"]["REGION_TALK_TG_SIMILAR_ENABLED"], "1")
        self.assertEqual(actions[3]["env"]["REGION_TALK_TG_SIMILAR_MAX_SEED_CHANNELS_PER_RUN"], "5")
        self.assertEqual(actions[3]["env"]["REGION_TALK_MAX_TELEGRAM_KEYWORD_QUERIES"], "2")
        self.assertEqual(actions[3]["env"]["REGION_TALK_RUNTIME_RESERVE_BEFORE_DISCOVERY_TAIL_SECONDS"], "240")

    def test_progress_signature_uses_all_numeric_metrics_without_classes(self) -> None:
        mod = load_module()
        base = {
            "publics_total": 10,
            "processed_posts_unique_total": 15,
            "publication_sent_total": 0,
            "some_future_numeric_metric": 1,
            "kaggle_kernel_statuses": {"region-talk-candidate-report": "RUNNING"},
            "non_numeric_status": "RUNNING",
        }
        more_publics = dict(base, publics_total=11)
        more_future_metric = dict(base, some_future_numeric_metric=2)
        self.assertIn(("publics_total", 10), mod._progress_signature(base))
        self.assertIn(("some_future_numeric_metric", 1), mod._progress_signature(base))
        self.assertNotIn(("non_numeric_status", 0), mod._progress_signature(base))
        self.assertNotEqual(mod._progress_signature(base), mod._progress_signature(more_publics))
        self.assertNotEqual(mod._progress_signature(base), mod._progress_signature(more_future_metric))

    def test_execute_ready_selects_non_conflicting_parallel_launches(self) -> None:
        mod = load_module()
        actions = mod.build_decision_plan(
            {
                "publication_sent_total": 0,
                "publication_confirmed_total": 0,
                "publication_unsent_confirmed_total": 0,
                "image_actual_scored_total": 5,
                "publication_candidate_total": 3,
                "bge_pending_sample_total": 4,
                "image_pending_total": 7,
            },
            target_confirmed=20,
            bge_threshold=1,
            image_threshold=1,
        )
        selected = mod.select_actions_for_execution(actions, execute_ready=True, max_actions=3)
        self.assertEqual([a["action"] for a in selected], ["launch_bge_m3", "launch_image_diagnostic", "launch_candidate_report"])
        self.assertEqual([a["resource"] for a in selected], ["kaggle:bge_m3", "telegram:DISCOVERY2", "telegram:DISCOVERY1"])

    def test_execute_ready_can_include_local_finalizer_after_parallel_launches(self) -> None:
        mod = load_module()
        actions = mod.build_decision_plan(
            {
                "publication_sent_total": 0,
                "publication_confirmed_total": 0,
                "publication_unsent_confirmed_total": 0,
                "image_actual_scored_total": 5,
                "publication_candidate_total": 3,
                "bge_pending_sample_total": 4,
                "image_pending_total": 7,
            },
            target_confirmed=20,
            bge_threshold=1,
            image_threshold=1,
        )
        selected = mod.select_actions_for_execution(actions, execute_ready=True, max_actions=4)
        self.assertEqual([a["action"] for a in selected], ["launch_bge_m3", "launch_image_diagnostic", "launch_candidate_report", "run_finalizer"])

    def test_decision_stops_at_target(self) -> None:
        mod = load_module()
        actions = mod.build_decision_plan({"publication_confirmed_total": 20, "publication_sent_total": 0}, target_confirmed=20, bge_threshold=1, image_threshold=1)
        self.assertEqual(actions, [{"action": "stop", "reason": "target_confirmed_reached"}])

    def test_filter_actions_skips_active_kaggle_kernels(self) -> None:
        mod = load_module()
        actions = mod.build_decision_plan(
            {
                "publication_sent_total": 0,
                "publication_confirmed_total": 0,
                "publication_unsent_confirmed_total": 0,
                "image_actual_scored_total": 5,
                "publication_candidate_total": 3,
                "bge_pending_sample_total": 4,
                "image_pending_total": 7,
            },
            target_confirmed=20,
            bge_threshold=1,
            image_threshold=1,
        )
        kept, skipped = mod.filter_actions_for_active_kernels(actions, {"region-talk-candidate-report": "RUNNING"})
        self.assertNotIn("launch_candidate_report", [a["action"] for a in kept])
        self.assertIn({"action": "launch_candidate_report", "kernel_slug": "region-talk-candidate-report", "status": "RUNNING", "reason": "kernel_already_active"}, skipped)

    def test_filter_actions_blocks_unverified_kaggle_status_by_default(self) -> None:
        mod = load_module()
        actions = mod.build_decision_plan(
            {
                "publication_sent_total": 0,
                "publication_confirmed_total": 0,
                "publication_unsent_confirmed_total": 0,
                "image_actual_scored_total": 0,
                "publication_candidate_total": 0,
                "bge_pending_sample_total": 4,
                "image_pending_total": 0,
            },
            target_confirmed=20,
            bge_threshold=1,
            image_threshold=1,
        )
        kept, skipped = mod.filter_actions_for_active_kernels(actions, {"region-talk-bge-m3-enrichment": "UNVERIFIED:ApiException"})
        self.assertNotIn("launch_bge_m3", [a["action"] for a in kept])
        self.assertIn({"action": "launch_bge_m3", "kernel_slug": "region-talk-bge-m3-enrichment", "status": "UNVERIFIED:APIEXCEPTION", "reason": "kernel_status_unverified"}, skipped)

    def test_filter_actions_can_allow_unverified_after_manual_audit(self) -> None:
        mod = load_module()
        actions = [{"action": "launch_bge_m3", "cmd": [], "parallel_safe": True}]
        kept, skipped = mod.filter_actions_for_active_kernels(
            actions,
            {"region-talk-bge-m3-enrichment": "UNVERIFIED:ApiException"},
            block_unverified=False,
        )
        self.assertEqual(kept, actions)
        self.assertEqual(skipped, [])

    def test_readiness_selection_after_active_filter_still_launches_bge_and_image(self) -> None:
        mod = load_module()
        actions = mod.build_decision_plan(
            {
                "publication_sent_total": 0,
                "publication_confirmed_total": 0,
                "publication_unsent_confirmed_total": 0,
                "image_actual_scored_total": 5,
                "publication_candidate_total": 3,
                "bge_pending_sample_total": 4,
                "image_pending_total": 7,
            },
            target_confirmed=20,
            bge_threshold=1,
            image_threshold=1,
        )
        kept, _ = mod.filter_actions_for_active_kernels(actions, {"region-talk-candidate-report": "RUNNING"})
        selected = mod.select_actions_for_execution(kept, execute_ready=True, max_actions=3)
        self.assertEqual([a["action"] for a in selected], ["launch_bge_m3", "launch_image_diagnostic", "run_finalizer"])

    def test_active_bge_still_allows_image_and_main_cycling(self) -> None:
        mod = load_module()
        actions = mod.build_decision_plan(
            {
                "publication_sent_total": 0,
                "publication_confirmed_total": 0,
                "publication_unsent_confirmed_total": 0,
                "image_actual_scored_total": 5,
                "publication_candidate_total": 3,
                "bge_pending_sample_total": 4,
                "image_pending_total": 7,
            },
            target_confirmed=20,
            bge_threshold=1,
            image_threshold=1,
        )
        kept, skipped = mod.filter_actions_for_active_kernels(actions, {"region-talk-bge-m3-enrichment": "RUNNING"})
        self.assertIn({"action": "launch_bge_m3", "kernel_slug": "region-talk-bge-m3-enrichment", "status": "RUNNING", "reason": "kernel_already_active"}, skipped)
        selected = mod.select_actions_for_execution(kept, execute_ready=True, max_actions=3)
        self.assertEqual([a["action"] for a in selected], ["launch_image_diagnostic", "launch_candidate_report", "run_finalizer"])

    def test_prepare_action_command_injects_env_file_and_run_id(self) -> None:
        mod = load_module()
        action = {
            "action": "launch_bge_m3",
            "cmd": ["python3", "kaggle/execute_region_talk_bge_m3_enrichment.py", "--batch-size", "12", "--no-wait"],
            "run_id": "rt-test-run",
        }
        cmd, run_id = mod.prepare_action_command(action, env_file="/tmp/region.env")
        self.assertEqual(run_id, "rt-test-run")
        self.assertIn("--env-file", cmd)
        self.assertIn("/tmp/region.env", cmd)
        self.assertIn("--run-id", cmd)
        self.assertIn("rt-test-run", cmd)

    def test_prepare_action_command_does_not_add_run_id_to_notifier(self) -> None:
        mod = load_module()
        action = {"action": "notify_confirmed", "cmd": ["python3", "scripts/region_talk_goal_notify.py", "--limit", "20"], "run_id": "ignored"}
        cmd, _ = mod.prepare_action_command(action, env_file="/tmp/region.env")
        self.assertIn("--env-file", cmd)
        self.assertNotIn("--run-id", cmd)

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
