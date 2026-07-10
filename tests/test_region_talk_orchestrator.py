from __future__ import annotations

import contextlib
import importlib.util
import io
import os
import sys
import unittest
from datetime import datetime, timezone
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
                "finalizer_pending_url_total": 2,
            },
            target_confirmed=20,
            bge_threshold=1,
            image_threshold=1,
        )
        self.assertEqual([a["action"] for a in actions[:5]], ["notify_confirmed", "launch_candidate_report", "launch_bge_m3", "launch_image_diagnostic", "run_finalizer"])
        by_name = {action["action"]: action for action in actions}
        main = by_name["launch_candidate_report"]
        bge = by_name["launch_bge_m3"]
        self.assertTrue(main["parallel_safe"])
        self.assertTrue(bge["parallel_safe"])
        self.assertTrue(by_name["launch_image_diagnostic"]["parallel_safe"])
        self.assertIn("--batch-limit", bge["cmd"])
        self.assertIn("48", bge["cmd"])
        self.assertIn("--batch-size", bge["cmd"])
        self.assertIn("4", bge["cmd"])
        self.assertEqual(bge["env"]["REGION_TALK_BGE_E5_ONLY"], "1")
        self.assertEqual(bge["env"]["REGION_TALK_BGE_INPUT_KINDS"], "text_vector_enrichment_item")
        self.assertEqual(bge["env"]["REGION_TALK_BGE_BATCH_SIZE"], "4")
        self.assertEqual(bge["env"]["REGION_TALK_BGE_YDB_SCAN_LIMIT"], "6000")
        self.assertEqual(main["env"]["REGION_TALK_STATE_BACKEND"], "ydb")
        self.assertEqual(main["env"]["REGION_TALK_REQUIRE_YDB_STATE"], "1")
        self.assertEqual(main["env"]["REGION_TALK_TEXT_EMBEDDING_MODEL_IDS"], "intfloat/multilingual-e5-base")
        self.assertEqual(main["env"]["REGION_TALK_REQUIRE_EXTERNAL_BGE_M3_FOR_IMAGE_QUEUE"], "1")
        self.assertEqual(main["env"]["REGION_TALK_SKIP_REPORT_TAIL_AFTER_IMAGE_QUEUE_HANDOFF"], "0")
        self.assertEqual(main["env"]["REGION_TALK_SKIP_REPORT_TAIL_AFTER_SOURCE_QUEUE_HANDOFF"], "0")
        self.assertEqual(main["env"]["REGION_TALK_NOTEBOOK_MAX_RUNTIME_SECONDS"], "1200")
        self.assertEqual(main["env"]["REGION_TALK_SOURCE_SELECTION_YDB_QUEUE_ONLY"], "1")
        self.assertEqual(main["env"]["REGION_TALK_MAX_POSTS_PER_SOURCE"], "20")
        self.assertEqual(main["env"]["REGION_TALK_TG_MAX_HISTORY_SOURCES_PER_RUN"], "6")
        self.assertEqual(main["env"]["REGION_TALK_TG_PUBLIC_WEB_FETCH_FIRST"], "0")
        self.assertEqual(main["env"]["REGION_TALK_TG_PUBLIC_WEB_FALLBACK"], "0")
        self.assertEqual(main["env"]["REGION_TALK_FETCH_POST_LINK_QUEUE_FIRST"], "1")
        self.assertEqual(main["env"]["REGION_TALK_POST_LINK_QUEUE_FETCH_LIMIT"], "3")
        self.assertEqual(main["env"]["REGION_TALK_TG_CACHED_ENTITY_ONLY"], "1")
        self.assertEqual(main["env"]["REGION_TALK_SOURCE_QUEUE_UNCACHED_RESOLVE_LANE_PER_RUN"], "1")
        self.assertEqual(main["env"]["REGION_TALK_TG_MAX_NETWORK_RESOLVES_PER_RUN"], "1")
        self.assertEqual(main["env"]["REGION_TALK_TG_EXACT_POST_NETWORK_RESOLVE_BUDGET_PER_RUN"], "1")
        self.assertEqual(main["env"]["REGION_TALK_YDB_MAX_SOURCE_ROWS"], "20000")
        self.assertEqual(main["env"]["REGION_TALK_YDB_SOURCE_QUEUE_FULL_READ_LIMIT"], "20000")
        self.assertEqual(main["env"]["REGION_TALK_YDB_MAX_TEXT_VECTOR_ROWS"], "6000")
        self.assertIn("--max-sources", main["cmd"])
        self.assertIn("6", main["cmd"])
        self.assertEqual(main["env"]["REGION_TALK_TG_SIMILAR_ENABLED"], "1")
        self.assertEqual(main["env"]["REGION_TALK_TG_SIMILAR_MAX_SEED_CHANNELS_PER_RUN"], "3")
        self.assertEqual(main["env"]["REGION_TALK_TELEGRAM_QUERY_SOURCE"], "place_lexicon")
        self.assertEqual(main["env"]["REGION_TALK_MAX_TELEGRAM_KEYWORD_QUERIES"], "4")
        self.assertEqual(main["env"]["REGION_TALK_MAX_TELEGRAM_KEYWORD_PHRASE_QUERIES"], "2")
        self.assertEqual(main["env"]["REGION_TALK_MAX_TELEGRAM_HASHTAG_QUERIES_PER_RUN"], "2")
        self.assertEqual(main["env"]["REGION_TALK_RUNTIME_RESERVE_BEFORE_DISCOVERY_TAIL_SECONDS"], "300")


    def test_bge_batch_limit_is_configurable_for_backlog_catchup(self) -> None:
        old_limit = os.environ.get("REGION_TALK_ORCHESTRATOR_BGE_BATCH_LIMIT")
        old_size = os.environ.get("REGION_TALK_ORCHESTRATOR_BGE_BATCH_SIZE")
        try:
            os.environ["REGION_TALK_ORCHESTRATOR_BGE_BATCH_LIMIT"] = "36"
            os.environ["REGION_TALK_ORCHESTRATOR_BGE_BATCH_SIZE"] = "6"
            mod = load_module()
            actions = mod.build_decision_plan(
                {
                    "publication_sent_total": 0,
                    "publication_confirmed_total": 0,
                    "publication_unsent_confirmed_total": 0,
                    "image_actual_scored_total": 0,
                    "publication_candidate_total": 0,
                    "text_vector_e5_without_bge_exact_text_total": 600,
                    "bge_pending_sample_total": 100,
                    "image_pending_total": 0,
                },
                target_confirmed=20,
                bge_threshold=1,
                image_threshold=1,
            )
            bge = next(a for a in actions if a["action"] == "launch_bge_m3")
            self.assertIn("36", bge["cmd"])
            batch_size_index = bge["cmd"].index("--batch-size") + 1
            self.assertEqual(bge["cmd"][batch_size_index], "4")
        finally:
            if old_limit is None:
                os.environ.pop("REGION_TALK_ORCHESTRATOR_BGE_BATCH_LIMIT", None)
            else:
                os.environ["REGION_TALK_ORCHESTRATOR_BGE_BATCH_LIMIT"] = old_limit
            if old_size is None:
                os.environ.pop("REGION_TALK_ORCHESTRATOR_BGE_BATCH_SIZE", None)
            else:
                os.environ["REGION_TALK_ORCHESTRATOR_BGE_BATCH_SIZE"] = old_size

    def test_bge_batch_limit_respects_external_cpu_runtime_capacity(self) -> None:
        mod = load_module()
        with mock.patch.dict(os.environ, {
            "REGION_TALK_EXTERNAL_CPU_BGE_CAPACITY_ROWS": "24",
            "REGION_TALK_ORCHESTRATOR_BGE_BATCH_LIMIT": "60",
            "REGION_TALK_ORCHESTRATOR_BGE_BATCH_SIZE": "9",
        }):
            actions = mod.build_decision_plan(
                {
                    "text_vector_current_version_e5_without_bge_total": 100,
                    "bge_pending_sample_total": 100,
                },
                target_confirmed=20,
                bge_threshold=1,
                image_threshold=1,
            )
        bge = next(action for action in actions if action["action"] == "launch_bge_m3")
        limit_index = bge["cmd"].index("--batch-limit") + 1
        size_index = bge["cmd"].index("--batch-size") + 1
        self.assertEqual(bge["cmd"][limit_index], "24")
        self.assertEqual(bge["cmd"][size_index], "4")

    def test_processed_post_metric_limit_is_not_capped_by_debug_source_limit(self) -> None:
        mod = load_module()
        self.assertGreaterEqual(mod._orchestrator_kind_limit("processed_post_item", 6000), 20000)
        self.assertGreaterEqual(mod._orchestrator_kind_limit("post_live_item", 6000), 20000)
        self.assertEqual(mod._orchestrator_kind_limit("source_queue_item", 6000), 20000)

    def test_cursor_metric_prefers_highest_position_over_stale_history(self) -> None:
        mod = load_module()
        current = {"_ydb_pk": "queue_cursor:source", "queue_name": "unified_source_queue", "cursor_position": 475, "_ydb_updated_at": "2026-07-09T15:40:00Z"}
        stale = {"_ydb_pk": "queue_cursor:source:old-run", "queue_name": "unified_source_queue", "cursor_position": 1957, "_ydb_updated_at": "2026-07-09T15:50:00Z"}
        self.assertFalse(mod._cursor_row_is_better(current, stale, "unified_source_queue"))
        self.assertTrue(mod._cursor_row_is_better(stale, current, "unified_source_queue"))

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

    def test_heartbeat_metrics_keep_run_identity_and_stage(self) -> None:
        mod = load_module()
        metrics = mod._heartbeat_metric_fields("candidate", {
            "run_id": "candidate-run-2",
            "event_name": "image_queue_build_done",
            "phase": "queue_assembly",
            "status": "running",
            "created_at": "2026-07-10T10:00:00Z",
            "event_seq": 52,
        })
        self.assertEqual(metrics["candidate_heartbeat_run_id"], "candidate-run-2")
        self.assertEqual(metrics["candidate_heartbeat_event_name"], "image_queue_build_done")
        self.assertEqual(metrics["candidate_heartbeat_event_seq"], 52)

    def test_canonical_metric_aliases_include_snapshot_names(self) -> None:
        mod = load_module()
        metrics = mod.with_canonical_metric_aliases({
            "publics_primary_unscanned_pending_total": 7,
            "source_queue_posts_scanned_total": 101,
            "processed_posts_unique_total": 55,
            "candidate_memory_total": 9,
            "publication_candidate_total": 3,
        })
        self.assertEqual(metrics["pending_scan"], 7)
        self.assertEqual(metrics["source_posts_scanned_sum"], 101)
        self.assertEqual(metrics["processed_post_rows"], 55)
        self.assertEqual(metrics["candidate_memory_rows"], 9)
        self.assertEqual(metrics["publication_queue_total"], 3)

    def test_loop_goal_progress_tracks_delta_targets(self) -> None:
        mod = load_module()
        baseline = {"publics_total": 10, "processed_posts_unique_total": 20, "publics_with_ko_candidates_total": 2}
        current = {"publics_total": 12, "processed_posts_unique_total": 23, "publics_with_ko_candidates_total": 3}
        progress = mod.loop_goal_progress(current, baseline, {"new_publics": 2, "processed_posts": 4, "ko_sources": 1})
        self.assertTrue(progress["active"])
        self.assertFalse(progress["reached"])
        self.assertTrue(progress["items"]["new_publics"]["reached"])
        self.assertFalse(progress["items"]["processed_posts"]["reached"])

    def test_image_queue_goal_tracks_eligible_rows_not_retained_raw_rows(self) -> None:
        mod = load_module()
        baseline = {"image_queue_total": 77, "image_product_eligible_total": 0}
        current = {"image_queue_total": 77, "image_product_eligible_total": 4}
        progress = mod.loop_goal_progress(current, baseline, {"image_queue": 1})
        self.assertTrue(progress["reached"])
        self.assertEqual(progress["items"]["image_queue"]["metric"], "image_product_eligible_total")
        self.assertEqual(progress["items"]["image_queue"]["delta"], 4)

    def test_publication_goal_ignores_historical_and_tombstone_rows(self) -> None:
        mod = load_module()
        baseline = {"publication_candidate_total": 19, "publication_active_candidate_total": 0}
        current = {"publication_candidate_total": 22, "publication_active_candidate_total": 0}
        progress = mod.loop_goal_progress(current, baseline, {"publication_candidates": 1})
        self.assertFalse(progress["reached"])
        self.assertEqual(
            progress["items"]["publication_candidates"]["metric"],
            "publication_active_candidate_total",
        )
        self.assertEqual(progress["items"]["publication_candidates"]["delta"], 0)

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

    def test_current_vector_pairing_and_lag_ignore_stale_hashes_and_contracts(self) -> None:
        mod = load_module()
        now = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)
        e5 = [
            {"post_url": "https://t.me/a/1", "text_hash": "old", "encoder_contract": mod.CURRENT_E5_ENCODER_CONTRACT, "semantic_bank_version": "v1", "created_at": "2026-07-10T08:00:00Z"},
            {"post_url": "https://t.me/a/1", "text_hash": "h1", "encoder_contract": mod.CURRENT_E5_ENCODER_CONTRACT, "semantic_bank_version": "v1", "created_at": "2026-07-10T10:00:00Z"},
            {"post_url": "https://t.me/b/2", "text_hash": "h2", "encoder_contract": mod.CURRENT_E5_ENCODER_CONTRACT, "semantic_bank_version": "v2", "created_at": "2026-07-10T09:00:00Z"},
            {"post_url": "https://t.me/stale/3", "text_hash": "h3", "encoder_contract": "e5_legacy", "semantic_bank_version": "v1", "created_at": "2026-07-10T09:00:00Z"},
        ]
        bge = [
            {"post_url": "https://t.me/a/1", "text_hash": "h1", "paired_e5_text_hash": "h1", "encoder_contract": mod.CURRENT_BGE_M3_ENCODER_CONTRACT, "semantic_bank_version": "v1", "created_at": "2026-07-10T10:02:00Z"},
            {"post_url": "https://t.me/b/2", "text_hash": "h2", "paired_e5_text_hash": "h2", "encoder_contract": mod.CURRENT_BGE_M3_ENCODER_CONTRACT, "semantic_bank_version": "v1", "created_at": "2026-07-10T09:05:00Z"},
        ]
        metrics = mod._text_vector_pair_metrics(e5, bge, now=now)
        self.assertEqual(metrics["text_vector_current_version_e5_unique_posts_total"], 2)
        self.assertEqual(metrics["text_vector_current_version_dual_paired_total"], 1)
        self.assertEqual(metrics["text_vector_current_version_e5_without_bge_total"], 1)
        self.assertEqual(metrics["text_vector_current_version_semantic_bank_mismatch_total"], 1)
        self.assertEqual(metrics["text_vector_current_version_dual_coverage_percent"], 50)
        self.assertEqual(metrics["text_vector_current_version_bge_pair_lag_seconds_max"], 120)
        self.assertEqual(metrics["text_vector_current_version_bge_pending_lag_seconds_max"], 10800)
        self.assertEqual(metrics["text_vector_stale_version_e5_rows_total"], 1)

    def test_post_link_queue_states_head_blocking_integrity_and_entity_cache(self) -> None:
        mod = load_module()
        now = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)
        rows = [
            {"post_url": "https://t.me/cool/1", "post_link_status": "retry_fetch", "post_link_priority": 0, "next_attempt_after": "2026-07-10T13:00:00Z"},
            {"post_url": "https://t.me/wait/2", "post_link_status": "retry_wait_entity_cache", "post_link_priority": 1, "handle": "@wait"},
            {"post_url": "https://t.me/ready/3", "post_link_status": "pending_fetch", "post_link_priority": 2},
            {"post_url": "https://t.me/ready/3?single=1", "post_link_status": "fetch_error", "post_link_priority": 3},
            {"post_url": "https://t.me/done/4", "post_link_status": "fetched", "post_link_priority": 0},
            {"post_url": "not-a-post", "post_link_status": "pending_fetch", "post_link_priority": 4},
        ]
        cache = [{"entity_cache_key": "telegram:username:wait", "username": "wait", "channel_id_private": "1", "access_hash_private": "2"}]
        metrics = mod._post_link_queue_metrics(rows, cache, now=now)
        self.assertEqual(metrics["post_link_queue_exact_ready_total"], 2)
        self.assertEqual(metrics["post_link_queue_cooldown_total"], 1)
        self.assertEqual(metrics["post_link_queue_entity_wait_total"], 1)
        self.assertEqual(metrics["post_link_queue_terminal_total"], 1)
        self.assertEqual(metrics["post_link_queue_unknown_status_total"], 1)
        self.assertEqual(metrics["post_link_queue_head_blocked_total"], 2)
        self.assertEqual(metrics["post_link_queue_head_blocked_cooldown_total"], 1)
        self.assertEqual(metrics["post_link_queue_head_blocked_entity_wait_total"], 1)
        self.assertEqual(metrics["post_link_queue_integrity_duplicate_url_rows_total"], 1)
        self.assertEqual(metrics["post_link_queue_integrity_invalid_url_total"], 1)
        self.assertEqual(metrics["telegram_entity_cache_valid_rows_total"], 1)
        self.assertEqual(metrics["post_link_queue_entity_wait_cache_now_available_total"], 1)

    def test_manual_keyword_hashtag_similar_inflow_metrics_are_distinct(self) -> None:
        mod = load_module()
        metrics = mod._discovery_inflow_metrics([
            {"added_from": "manual_seed"},
            {"discovery_type": "telegram_keyword_search"},
            {"discovery_type": "telegram_hashtag_search", "matched_hashtag": "#kaliningrad"},
            {"edge_type": "telegram_similar_channel"},
            {"added_from": "source_history"},
        ])
        self.assertEqual(metrics["discovery_inflow_manual_total"], 1)
        self.assertEqual(metrics["discovery_inflow_keyword_total"], 1)
        self.assertEqual(metrics["discovery_inflow_hashtag_total"], 1)
        self.assertEqual(metrics["discovery_inflow_similar_total"], 1)
        self.assertEqual(metrics["discovery_inflow_source_total"], 1)

    def test_source_queue_integrity_reports_unordered_duplicate_and_bad_cursor(self) -> None:
        mod = load_module()
        metrics = mod._source_queue_integrity_metrics(
            [
                {"queue_seq": 1, "queue_order": 1},
                {"queue_seq": 1, "queue_order": 1},
                {"queue_seq": 0, "queue_order": 0},
            ],
            cursor_position=5,
        )
        self.assertEqual(metrics["source_queue_integrity_unordered_total"], 1)
        self.assertEqual(metrics["source_queue_integrity_duplicate_order_values_total"], 1)
        self.assertEqual(metrics["source_queue_integrity_duplicate_order_rows_total"], 1)
        self.assertEqual(metrics["source_queue_integrity_legacy_order_duplicate_rows_total"], 1)
        self.assertEqual(metrics["source_queue_integrity_cursor_past_max_total"], 1)

    def test_publication_taxonomy_and_finalizer_are_url_level(self) -> None:
        mod = load_module()
        images = [
            {"post_url": "https://t.me/a/1", "image_queue_status": "actual_scored", "image_model_input_type": "actual_image", "image_publication_ready": "true"},
            {"post_url": "https://t.me/b/2", "image_queue_status": "actual_scored", "image_model_input_type": "actual_image", "image_publication_ready": "true"},
        ]
        publications = [
            {
                "post_url": "https://t.me/a/1",
                "publication_candidate_status": "llm_confirmed",
                "sent_to_chat": "false",
                "publication_eligibility_verdict": "eligible",
                "publication_eligibility_gate_version": mod.CURRENT_PUBLICATION_ELIGIBILITY_GATE_VERSION,
                "authoritative_source_fingerprint_version": mod.AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION,
            },
            {
                "post_url": "https://t.me/c/3",
                "publication_candidate_status": "filtered_before_llm",
                "publication_eligibility_verdict": "reject",
                "publication_eligibility_gate_version": mod.CURRENT_PUBLICATION_ELIGIBILITY_GATE_VERSION,
            },
        ]
        sources = [{
            "canonical_source_key": "telegram:a",
            "source_url": "https://t.me/a",
            "source_scope": "external",
            "source_geo_class": "nonlocal_russia",
            "source_queue_status": "processed_found_ko_candidate",
        }]
        publications[0]["canonical_source_key"] = "telegram:a"
        publications[0]["authoritative_source_fingerprint"] = mod.authoritative_source_fingerprint(sources[0])
        metrics = mod._publication_handoff_metrics(images, publications, sources)
        self.assertEqual(metrics["publication_candidate_total"], 2)
        self.assertEqual(metrics["publication_active_candidate_total"], 1)
        self.assertEqual(metrics["publication_ready_total"], 1)
        self.assertEqual(metrics["publication_confirmed_total"], 1)
        self.assertEqual(metrics["publication_rejected_total"], 1)
        self.assertEqual(metrics["finalizer_pending_url_total"], 1)
        self.assertEqual(metrics["finalizer_pending_urls"], ["https://t.me/b/2"])
        actions = mod.build_decision_plan(metrics, target_confirmed=20, bge_threshold=1, image_threshold=1)
        self.assertIn("run_finalizer", [action["action"] for action in actions])

        unsigned = mod._publication_handoff_metrics(
            [images[0]],
            [{"post_url": "https://t.me/a/1", "publication_candidate_status": "llm_confirmed"}],
            sources,
        )
        self.assertEqual(unsigned["publication_confirmed_total"], 0)
        self.assertEqual(unsigned["finalizer_pending_url_total"], 1)

    def test_current_vector_backlog_drives_bge_instead_of_stale_aggregate(self) -> None:
        mod = load_module()
        metrics = {
            "text_vector_current_version_e5_without_bge_total": 0,
            "text_vector_e5_without_bge_exact_text_total": 99,
            "bge_pending_sample_total": 0,
        }
        actions = mod.build_decision_plan(metrics, target_confirmed=20, bge_threshold=1, image_threshold=1)
        self.assertNotIn("launch_bge_m3", [action["action"] for action in actions])
        metrics["text_vector_current_version_e5_without_bge_total"] = 1
        actions = mod.build_decision_plan(metrics, target_confirmed=20, bge_threshold=1, image_threshold=1)
        self.assertNotIn("launch_bge_m3", [action["action"] for action in actions])
        metrics["bge_pending_sample_total"] = 1
        actions = mod.build_decision_plan(metrics, target_confirmed=20, bge_threshold=1, image_threshold=1)
        self.assertIn("launch_bge_m3", [action["action"] for action in actions])

    def test_decision_does_not_launch_bge_when_worker_sample_is_empty(self) -> None:
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
        self.assertNotIn("launch_bge_m3", [action["action"] for action in actions])

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
                "finalizer_pending_url_total": 2,
            },
            target_confirmed=20,
            bge_threshold=1,
            image_threshold=1,
        )
        selected = mod.select_actions_for_execution(actions, execute_ready=True, max_actions=3)
        self.assertEqual([a["action"] for a in selected], ["launch_candidate_report", "launch_bge_m3", "launch_image_diagnostic"])
        self.assertEqual([a["resource"] for a in selected], ["telegram:DISCOVERY1", "kaggle:bge_m3", "telegram:DISCOVERY2"])

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
                "finalizer_pending_url_total": 2,
            },
            target_confirmed=20,
            bge_threshold=1,
            image_threshold=1,
        )
        selected = mod.select_actions_for_execution(actions, execute_ready=True, max_actions=4)
        self.assertEqual([a["action"] for a in selected], ["launch_candidate_report", "launch_bge_m3", "launch_image_diagnostic", "run_finalizer"])

    def test_decision_keeps_discovery_enabled_at_target(self) -> None:
        mod = load_module()
        actions = mod.build_decision_plan({"publication_confirmed_total": 20, "publication_sent_total": 0}, target_confirmed=20, bge_threshold=1, image_threshold=1)
        self.assertEqual([action["action"] for action in actions], ["launch_candidate_report"])
        self.assertIn("after publication goal", actions[0]["reason"])

    def test_include_main_false_cannot_disable_discovery_or_manual_intake(self) -> None:
        mod = load_module()
        actions = mod.build_decision_plan({}, target_confirmed=20, bge_threshold=1, image_threshold=1, include_main=False)
        main = next(action for action in actions if action["action"] == "launch_candidate_report")
        self.assertEqual(main["env"]["REGION_TALK_FETCH_POST_LINK_QUEUE_FIRST"], "1")
        self.assertEqual(main["env"]["REGION_TALK_POST_LINK_QUEUE_FETCH_LIMIT"], "3")
        self.assertEqual(main["env"]["REGION_TALK_ENABLE_TELEGRAM_KEYWORD_DISCOVERY"], "1")
        self.assertEqual(main["env"]["REGION_TALK_TG_SIMILAR_ENABLED"], "1")

    def test_single_action_execution_reserves_discovery_slot(self) -> None:
        mod = load_module()
        actions = mod.build_decision_plan(
            {"publication_unsent_confirmed_total": 1},
            target_confirmed=20,
            bge_threshold=1,
            image_threshold=1,
        )
        selected = mod.select_actions_for_execution(actions, execute_ready=False, max_actions=1)
        self.assertEqual([action["action"] for action in selected], ["launch_candidate_report"])

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
                "finalizer_pending_url_total": 2,
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
                "finalizer_pending_url_total": 2,
            },
            target_confirmed=20,
            bge_threshold=1,
            image_threshold=1,
        )
        kept, skipped = mod.filter_actions_for_active_kernels(actions, {"region-talk-bge-m3-enrichment": "RUNNING"})
        self.assertIn({"action": "launch_bge_m3", "kernel_slug": "region-talk-bge-m3-enrichment", "status": "RUNNING", "reason": "kernel_already_active"}, skipped)
        selected = mod.select_actions_for_execution(kept, execute_ready=True, max_actions=3)
        self.assertEqual([a["action"] for a in selected], ["launch_candidate_report", "launch_image_diagnostic", "run_finalizer"])

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

    def test_run_cmd_treats_launcher_active_kernel_refusal_as_skip(self) -> None:
        mod = load_module()
        refusal = (
            "RuntimeError: Region Talk Kaggle launch refused: active kernel(s) "
            "detected zigomaro/region-talk-candidate-report(QUEUED); auth bundle "
            "TELEGRAM_AUTH_BUNDLE_DISCOVERY1 must not be used concurrently."
        )
        proc = mock.Mock(returncode=1, stdout=refusal)
        with mock.patch.object(mod.subprocess, "run", return_value=proc):
            result = mod._run_cmd(
                ["python3", "kaggle/execute_region_talk_candidate_report.py"],
                dry_run=False,
                action={"action": "launch_candidate_report", "resource": "telegram:DISCOVERY1"},
                run_id="rt-race",
            )
        self.assertEqual(result["status"], "skipped_active_kernel_race")
        self.assertEqual(result["reason"], "launcher_detected_active_kernel_after_status_snapshot")
        self.assertEqual(result["returncode"], 1)

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

    def test_source_metric_populations_ignore_small_debug_limit(self) -> None:
        mod = load_module()
        self.assertEqual(mod._orchestrator_kind_limit("source_queue_item", 100), 20000)
        self.assertEqual(mod._orchestrator_kind_limit("source_candidate_item", 100), 20000)
        self.assertEqual(mod._orchestrator_kind_limit("processed_post_item", 100), 20000)

    def test_stats_message_is_rendered_from_same_metric_snapshot(self) -> None:
        mod = load_module()
        text = mod.build_orchestrator_stats_message({
            "publics_total": 7055,
            "publics_primary_unscanned_pending_total": 6393,
            "publics_with_ko_candidates_total": 91,
            "post_link_queue_exact_ready_total": 67,
            "post_link_queue_fetched_total": 0,
            "processed_posts_unique_total": 10756,
            "text_vector_current_version_dual_coverage_percent": 87,
            "text_vector_current_version_e5_without_bge_total": 290,
            "publication_candidate_total": 19,
            "publication_confirmed_total": 7,
            "publication_sent_total": 7,
        })
        self.assertIn("canonical population: 7055", text)
        self.assertIn("С KO evidence: 91", text)
        self.assertIn("Exact ready/cooldown/entity-wait/fetched: 67/0/0/0", text)
        self.assertIn("Publication total/confirmed/sent/ready: 19/7/7/0", text)


if __name__ == "__main__":
    unittest.main()
