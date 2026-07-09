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
        self.assertEqual(actions[1]["env"]["REGION_TALK_BGE_E5_ONLY"], "1")
        self.assertEqual(actions[1]["env"]["REGION_TALK_BGE_INPUT_KINDS"], "text_vector_enrichment_item")
        self.assertEqual(actions[1]["env"]["REGION_TALK_BGE_YDB_SCAN_LIMIT"], "6000")
        self.assertEqual(actions[3]["env"]["REGION_TALK_STATE_BACKEND"], "ydb")
        self.assertEqual(actions[3]["env"]["REGION_TALK_REQUIRE_YDB_STATE"], "1")
        self.assertEqual(actions[3]["env"]["REGION_TALK_TEXT_EMBEDDING_MODEL_IDS"], "intfloat/multilingual-e5-base")
        self.assertEqual(actions[3]["env"]["REGION_TALK_REQUIRE_EXTERNAL_BGE_M3_FOR_IMAGE_QUEUE"], "1")
        self.assertEqual(actions[3]["env"]["REGION_TALK_SKIP_REPORT_TAIL_AFTER_IMAGE_QUEUE_HANDOFF"], "0")
        self.assertEqual(actions[3]["env"]["REGION_TALK_YDB_MAX_SOURCE_ROWS"], "6000")
        self.assertEqual(actions[3]["env"]["REGION_TALK_YDB_MAX_TEXT_VECTOR_ROWS"], "6000")
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

    def test_source_merge_preserves_max_counter_values(self) -> None:
        mod = load_module()
        rows = mod._merge_source_rows(
            [{"canonical_source_key": "telegram:x", "posts_scanned": 31, "ko_posts_found": 2}],
            [{"canonical_source_key": "telegram:x", "posts_scanned": 17, "ko_posts_found": 1, "fetch_status": "ok"}],
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["posts_scanned"], 31)
        self.assertEqual(rows[0]["ko_posts_found"], 2)
        self.assertEqual(rows[0]["fetch_status"], "ok")

    def test_numeric_aggregate_helpers_for_history_depth(self) -> None:
        mod = load_module()
        rows = [{"age": "1.5"}, {"age": 2}, {"age": ""}]
        self.assertEqual(mod._avg_numeric(rows, "age"), 1.75)
        self.assertEqual(mod._min_numeric(rows, "age"), 1.5)
        self.assertEqual(mod._max_numeric(rows, "age"), 2.0)

    def test_text_vector_pair_metrics_separate_raw_and_paired_backlog(self) -> None:
        mod = load_module()
        e5 = [
            {"model_short": "e5", "post_url": "https://t.me/a/1", "text_hash": "h1"},
            {"model_short": "e5", "post_url": "https://t.me/b/2", "text_hash": "h2"},
        ]
        bge = [
            {"model_short": "bge_m3", "post_url": "https://t.me/a/1", "paired_e5_text_hash": "h1", "text_hash": "h1"},
            {"model_short": "bge_m3", "post_url": "https://t.me/legacy/9", "text_hash": "old"},
        ]
        metrics = mod._text_vector_pair_metrics(e5, bge)
        self.assertEqual(metrics["text_vector_e5_unique_posts_total"], 2)
        self.assertEqual(metrics["text_vector_bge_m3_unique_posts_total"], 2)
        self.assertEqual(metrics["text_vector_dual_post_paired_total"], 1)
        self.assertEqual(metrics["text_vector_e5_without_bge_exact_text_total"], 1)
        self.assertEqual(metrics["text_vector_bge_without_e5_exact_text_total"], 1)
        self.assertEqual(metrics["text_vector_dual_exact_text_coverage_percent"], 50)

    def test_decision_launches_bge_from_e5_pair_backlog_even_when_sample_metric_missing(self) -> None:
        mod = load_module()
        actions = mod.build_decision_plan(
            {
                "publication_sent_total": 0,
                "publication_confirmed_total": 0,
                "publication_unsent_confirmed_total": 0,
                "image_actual_scored_total": 0,
                "publication_candidate_total": 0,
                "bge_pending_sample_total": 0,
                "text_vector_e5_without_bge_exact_text_total": 3,
                "image_pending_total": 0,
            },
            target_confirmed=20,
            bge_threshold=1,
            image_threshold=1,
        )
        self.assertEqual(actions[0]["action"], "launch_bge_m3")

    def test_regex_ko_diagnostic_keeps_filtered_ko_but_rejects_multiregion(self) -> None:
        mod = load_module()
        good = mod._regex_ko_diagnostic("Ездили в Зеленоградск и на Куршскую косу: дюны, море, особенно запомнилась прогулка.")
        self.assertTrue(good["regex_ko_raw"])
        self.assertTrue(good["regex_ko_filtered"])

        roundup = mod._regex_ko_diagnostic("Куда поехать летом: Байкал, Сочи, Калининград и Дагестан — топ мест России.")
        self.assertTrue(roundup["regex_ko_raw"])
        self.assertFalse(roundup["regex_ko_filtered"])
        self.assertTrue(roundup["regex_is_multi_region"])

    def test_regex_vector_comparison_metrics_expose_delta(self) -> None:
        mod = load_module()
        rows = [
            {"post_id": "r1", "text": "Ездили в Зеленоградск: красивое море, особенно запомнилась прогулка."},
            {"post_id": "v1", "text": "Личная история про Куршскую косу и маршрут.", "vector_gate_status": "vector_accept_candidate"},
            {"post_id": "m1", "text": "Куда поехать: Байкал и Калининград, подборка мест России."},
        ]
        metrics = mod._regex_vector_comparison_metrics(rows)
        self.assertEqual(metrics["regex_ko_raw_posts_total"], 3)
        self.assertEqual(metrics["regex_ko_filtered_posts_total"], 2)
        self.assertEqual(metrics["regex_ko_external_geo_filtered_posts_total"], 1)
        self.assertEqual(metrics["regex_ko_multiregion_filtered_posts_total"], 1)
        self.assertEqual(metrics["vector_ko_candidate_posts_total"], 1)
        self.assertEqual(metrics["regex_filtered_without_vector_posts_total"], 1)
        self.assertEqual(metrics["vector_without_regex_filtered_posts_total"], 0)

    def test_post_source_merge_key_uses_processed_post_source_url(self) -> None:
        mod = load_module()
        row = {"source_url": "https://t.me/source/", "post_url": "https://t.me/source/1"}
        self.assertEqual(mod._post_source_merge_key(row), "https://t.me/source")

    def test_ko_candidate_source_keys_use_row_level_evidence(self) -> None:
        mod = load_module()
        rows = [
            {"source_url": "https://t.me/ko", "vector_gate_status": "vector_accept_candidate"},
            {"source_url": "https://t.me/text", "text_region_confirmation_status": "text_confirmed_ko_only_for_image_analysis"},
            {"source_url": "https://t.me/plain", "vector_gate_status": "vector_reject"},
        ]
        self.assertEqual(mod._ko_candidate_source_keys(rows), {"https://t.me/ko", "https://t.me/text"})

    def test_image_queue_status_metrics_include_terminal_unsupported(self) -> None:
        mod = load_module()
        metrics = mod._image_queue_status_metrics([
            {"image_queue_status": "actual_scored"},
            {"image_queue_status": "not_reviewable_no_media"},
            {"image_queue_status": "not_reviewable_unsupported_media"},
            {"image_queue_status": "rejected_text_gate"},
        ])
        self.assertEqual(metrics["image_not_reviewable_no_media_total"], 1)
        self.assertEqual(metrics["image_not_reviewable_unsupported_media_total"], 1)
        self.assertEqual(metrics["image_rejected_text_gate_total"], 1)

    def test_keyword_source_metrics_show_expected_keyword_yield(self) -> None:
        mod = load_module()
        rows = [
            {"canonical_source_key": "telegram:k1", "added_from": "telegram_keyword_search", "queue_order": 11, "source_queue_status": "processed_found_ko_candidate", "posts_scanned": 20, "ko_posts_found": 1},
            {"canonical_source_key": "telegram:k2", "added_from": "telegram_keyword_search", "queue_order": 12, "source_queue_status": "processed_no_ko", "posts_scanned": 20, "ko_posts_found": 0},
            {"canonical_source_key": "telegram:k3", "added_from": "telegram_keyword_search", "queue_order": 13, "source_queue_status": "pending_scan", "posts_scanned": 0},
            {"canonical_source_key": "telegram:s1", "added_from": "telegram_similar", "queue_order": 14, "source_queue_status": "processed_found_ko_candidate", "posts_scanned": 20, "ko_posts_found": 1},
        ]
        metrics = mod._keyword_source_metrics(rows, cursor_position=10)
        self.assertEqual(metrics["publics_keyword_discovered_total"], 3)
        self.assertEqual(metrics["publics_keyword_queue_rows_total"], 3)
        self.assertEqual(metrics["publics_keyword_edge_targets_total"], 0)
        self.assertEqual(metrics["publics_keyword_scanned_with_posts_total"], 2)
        self.assertEqual(metrics["publics_keyword_with_ko_candidates_total"], 1)
        self.assertEqual(metrics["publics_keyword_pending_after_cursor_total"], 1)
        self.assertEqual(metrics["publics_keyword_ko_yield_percent"], 50)

    def test_keyword_source_metrics_include_keyword_edge_targets(self) -> None:
        mod = load_module()
        source_rows = [
            {"canonical_source_key": "telegram:edgehit", "queue_order": 11, "source_queue_status": "pending_scan", "posts_scanned": 0},
            {"canonical_source_key": "telegram:scanned", "queue_order": 9, "source_queue_status": "processed_found_ko_candidate", "posts_scanned": 20, "ko_posts_found": 1},
            {"canonical_source_key": "telegram:fake", "queue_order": 8, "source_queue_status": "processed_no_ko", "posts_scanned": 0},
        ]
        candidates = [
            {"source_candidate_id": "cand1", "canonical_source_key": "telegram:edgehit"},
            {"source_candidate_id": "cand2", "canonical_source_key": "telegram:scanned"},
            {"source_candidate_id": "cand3", "canonical_source_key": "telegram:missing"},
            {"source_candidate_id": "cand4", "canonical_source_key": "telegram:fake"},
        ]
        edges = [
            {"edge_type": "telegram_keyword_search", "to_source_candidate_id": "cand1"},
            {"edge_type": "telegram_keyword_search", "to_source_candidate_id": "cand2"},
            {"edge_type": "telegram_keyword_search", "to_source_candidate_id": "cand3"},
            {"edge_type": "telegram_keyword_search", "to_source_candidate_id": "cand4"},
        ]
        metrics = mod._keyword_source_metrics(source_rows, cursor_position=10, source_candidates=candidates, source_edges=edges)
        self.assertEqual(metrics["publics_keyword_discovered_total"], 4)
        self.assertEqual(metrics["publics_keyword_queue_rows_total"], 0)
        self.assertEqual(metrics["publics_keyword_edge_targets_total"], 4)
        self.assertEqual(metrics["publics_keyword_queue_missing_total"], 1)
        self.assertEqual(metrics["publics_keyword_fake_processed_without_scan_evidence_total"], 1)
        self.assertEqual(metrics["publics_keyword_scanned_with_posts_total"], 1)
        self.assertEqual(metrics["publics_keyword_with_ko_candidates_total"], 1)
        self.assertEqual(metrics["publics_keyword_pending_after_cursor_total"], 1)

    def test_keyword_post_regex_metrics_compare_keyword_scan_posts(self) -> None:
        mod = load_module()
        source_rows = [
            {"canonical_source_key": "telegram:travelhit", "source_url": "https://t.me/travelhit", "added_from": "telegram_keyword_search"},
            {"canonical_source_key": "telegram:quiet", "source_url": "https://t.me/quiet"},
        ]
        posts = [
            {
                "post_id": "p1",
                "source_url": "https://t.me/travelhit",
                "post_url": "https://t.me/travelhit/1",
                "text": "Ездили в Зеленоградск: красиво, особенно запомнилась прогулка.",
            },
            {
                "post_id": "p2",
                "post_url": "https://t.me/travelhit/2",
                "text": "Личная история про Куршскую косу и маршрут.",
                "vector_gate_status": "vector_accept_candidate",
            },
            {
                "post_id": "p3",
                "source_url": "https://t.me/quiet",
                "post_url": "https://t.me/quiet/1",
                "text": "Ездили в Зеленоградск: красиво, особенно запомнилась прогулка.",
            },
        ]
        metrics = mod._keyword_source_post_regex_metrics(source_rows, posts)
        self.assertEqual(metrics["publics_keyword_post_rows_total"], 2)
        self.assertEqual(metrics["publics_keyword_post_rows_with_text_total"], 2)
        self.assertEqual(metrics["publics_keyword_sources_with_post_rows_total"], 1)
        self.assertEqual(metrics["publics_keyword_regex_ko_raw_posts_total"], 2)
        self.assertEqual(metrics["publics_keyword_regex_ko_filtered_posts_total"], 2)
        self.assertEqual(metrics["publics_keyword_vector_ko_candidate_posts_total"], 1)
        self.assertEqual(metrics["publics_keyword_regex_sources_with_ko_filtered_total"], 1)
        self.assertEqual(metrics["publics_keyword_regex_filtered_without_vector_posts_total"], 1)

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
