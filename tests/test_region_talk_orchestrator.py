from __future__ import annotations

import argparse
import contextlib
import importlib.util
import io
import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
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
    def test_image_contract_rescore_metric_survives_temporary_migration_terminal_status(self) -> None:
        mod = load_module()
        row = {
            "image_queue_status": "rejected_publication_eligibility",
            "image_model_input_type": "actual_image",
            "overall_media_score": 0.5,
            "image_decision_contract_version": "legacy-v1",
            "publication_eligibility_decision": "reject",
            "publication_eligibility_gate_version": "region_talk_publication_eligibility_v5",
            "publication_eligibility_reason": "image_queue_not_actual_scored",
            "image_eligibility_decision": "accept",
            "image_eligibility_gate_version": "region_talk_publication_eligibility_v4",
        }
        self.assertTrue(mod._image_contract_rescore_candidate(row))

        row["image_decision_contract_version"] = "region_talk_image_album_guard_v2"
        self.assertFalse(mod._image_contract_rescore_candidate(row))

    def test_image_contract_rescore_metric_does_not_reopen_current_semantic_reject(self) -> None:
        mod = load_module()
        row = {
            "image_model_input_type": "actual_image",
            "overall_media_score": 0.5,
            "image_decision_contract_version": "legacy-v1",
            "publication_eligibility_decision": "reject",
            "publication_eligibility_gate_version": "region_talk_publication_eligibility_v5",
            "image_eligibility_decision": "reject",
            "image_eligibility_gate_version": "region_talk_publication_eligibility_v5",
        }
        self.assertFalse(mod._image_contract_rescore_candidate(row))

    def test_text_vector_metric_read_projects_scalars_without_dense_embedding(self) -> None:
        mod = load_module()
        captured: dict[str, object] = {}
        row = SimpleNamespace(
            pk="text_vector_enrichment_item:e5_1",
            _row_updated_at="2026-07-14T00:00:00+00:00",
            model_short="e5",
            model_id="intfloat/multilingual-e5-base",
            encoder_contract=mod.CURRENT_E5_ENCODER_CONTRACT,
            post_url="https://t.me/example/1",
            post_id="tg:example:1",
            text_hash="abc",
            text="Калининград",
        )

        class Transaction:
            def execute(self, query, params, commit_tx=True):
                captured["query"] = query
                captured["params"] = params
                captured["commit_tx"] = commit_tx
                return [SimpleNamespace(rows=[row])]

        class Session:
            def prepare(self, query_text):
                captured["query_text"] = query_text
                return query_text

            def transaction(self, _mode):
                return Transaction()

        class Pool:
            def retry_operation_sync(self, op):
                return op(Session())

        fake_ydb = SimpleNamespace(StaleReadOnly=lambda: object())
        rows = mod.read_text_vector_metric_rows(Pool(), fake_ydb, "/db/state", 10)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["post_url"], "https://t.me/example/1")
        self.assertEqual(rows[0]["_ydb_pk"], "text_vector_enrichment_item:e5_1")
        self.assertNotIn("vector", rows[0])
        self.assertNotIn("embedding", rows[0])
        query_text = str(captured["query_text"])
        self.assertIn('JSON_VALUE(payload_json, "$.model_short")', query_text)
        self.assertNotIn('"$.vector"', query_text)
        self.assertNotIn('"$.embedding"', query_text)

    def test_exact_post_probe_is_not_source_history_scan_evidence(self) -> None:
        mod = load_module()
        exact = {
            "source_queue_status": "pending_scan",
            "fetch_status": "ok",
            "fetch_attempted": "true",
            "posts_scanned": 1,
            "history_fetch_mode": "exact_post_link_fetch",
            "online_update_stage": "post_link_queue_exact_fetch",
        }
        self.assertTrue(mod._source_is_post_probe_only(exact))
        self.assertFalse(mod._source_has_scan_evidence(exact))

        completed_history = {**exact, "last_history_fetch_at": "2026-07-14T00:00:00+00:00"}
        self.assertTrue(mod._source_has_scan_evidence(completed_history))

    def test_primary_history_fetch_attempt_without_posts_is_not_real_scan_evidence(self) -> None:
        mod = load_module()
        self.assertFalse(mod._source_has_scan_evidence({
            "fetch_attempted": "true",
            "history_fetch_mode": "delta_scan_active",
            "posts_scanned": 0,
        }))
        self.assertTrue(mod._source_has_scan_evidence({
            "fetch_attempted": "true",
            "source_history_scan_ever_completed": "true",
            "source_history_posts_scanned_max": 7,
            "posts_scanned": 0,
        }))

    def test_confirmed_blogger_funnel_metrics_deduplicate_posts_and_separate_ledger(self) -> None:
        mod = load_module()
        sources = [
            {
                "canonical_source_key": "telegram:blogger",
                "platform": "telegram",
                "source_url": "https://t.me/blogger",
                "external_blogger_evidence_status": "confirmed_external",
                "source_queue_status": "processed_found_ko_candidate",
                "posts_scanned": 4,
                "ko_posts_found": 1,
            },
            {
                "canonical_source_key": "vk:writer",
                "platform": "vk",
                "source_url": "https://vk.com/writer",
                "external_blogger_evidence_status": "confirmed_external",
                "source_queue_status": "pending_scan",
            },
        ]
        processed_post = {
            "platform_post_key": "tg:blogger:1",
            "post_url": "https://t.me/blogger/1",
            "source_url": "https://t.me/blogger",
            "platform": "telegram",
            "vector_gate_status": "vector_defer_wait_bge_m3",
        }
        fused_candidate = {
            **processed_post,
            "vector_gate_status": "vector_accept_candidate",
            "text_vector_fusion_status": "fused_e5_bge_m3",
        }
        metrics = mod._confirmed_external_blogger_funnel_metrics(
            sources,
            [processed_post, dict(processed_post), fused_candidate],
            [{"post_url": "https://t.me/blogger/1"}, {"post_url": "https://t.me/blogger/1"}],
            [{"post_url": "https://t.me/blogger/1", "publication_candidate_status": "tombstoned_reject"}],
            [],
        )
        self.assertEqual(metrics["confirmed_external_blogger_sources_total"], 2)
        self.assertEqual(metrics["confirmed_external_blogger_scanned_total"], 1)
        self.assertEqual(metrics["confirmed_external_blogger_unscanned_total"], 1)
        self.assertEqual(metrics["confirmed_external_blogger_posts_processed_total"], 1)
        self.assertEqual(metrics["confirmed_external_blogger_sources_with_processed_posts_total"], 1)
        self.assertEqual(metrics["confirmed_external_blogger_with_ko_total"], 1)
        self.assertEqual(metrics["confirmed_external_blogger_vector_accepted_posts_total"], 1)
        self.assertEqual(metrics["confirmed_external_blogger_sources_with_vector_accepted_posts_total"], 1)
        self.assertEqual(metrics["confirmed_external_blogger_image_queue_posts_total"], 1)
        self.assertEqual(metrics["confirmed_external_blogger_sources_with_image_queue_posts_total"], 1)
        self.assertEqual(metrics["confirmed_external_blogger_publication_posts_total"], 1)
        self.assertEqual(metrics["confirmed_external_blogger_sources_with_publication_posts_total"], 1)
        self.assertEqual(metrics["confirmed_external_blogger_publication_confirmed_posts_total"], 0)
        self.assertEqual(metrics["confirmed_external_blogger_sources_with_publication_confirmed_posts_total"], 0)
        self.assertEqual(metrics["confirmed_external_blogger_sources_with_delivery_completed_posts_total"], 0)

    def test_external_blogger_registry_metrics_expose_raw_and_canonical_denominators(self) -> None:
        mod = load_module()
        evidence = [
            {
                "record_id": "1",
                "confirmation_status": "confirmed_external",
                "region_relation_status": "external visitor",
                "telegram_url": "https://t.me/blogger",
                "vk_public_url": "https://vk.com/writer",
            },
            {
                "record_id": "2",
                "confirmation_status": "confirmed_external",
                "region_relation_status": "местный блогер",
                "telegram_url": "https://t.me/local",
            },
            {
                "record_id": "3",
                "confirmation_status": "needs_externality_review",
                "telegram_url": "https://t.me/review",
            },
            {
                "record_id": "4",
                "confirmation_status": "confirmed_external",
                "region_relation_status": "external visitor",
                "vk_video_url": "https://vkvideo.ru/@videoauthor",
                "pipeline_status": "stored_only",
            },
            {
                "record_id": "5",
                "confirmation_status": "confirmed_external",
                "region_relation_status": "external visitor",
                "rutube_url": "https://rutube.ru/channel/123/",
                "pipeline_status": "stored_only",
            },
        ]
        source_rows = [
            {
                "canonical_source_key": "telegram:blogger",
                "platform": "telegram",
                "source_url": "https://t.me/blogger",
                "source_history_scan_ever_completed": True,
                "ko_posts_found": 1,
            },
            {
                "canonical_source_key": "vk:videoauthor",
                "platform": "vk",
                "source_url": "https://vk.com/videoauthor",
            },
        ]
        metrics = mod._external_blogger_registry_metrics(evidence, source_rows)
        self.assertEqual(metrics["external_blogger_registry_records_total"], 5)
        self.assertEqual(metrics["external_blogger_registry_confirmed_records_total"], 4)
        self.assertEqual(metrics["external_blogger_registry_needs_review_records_total"], 1)
        self.assertEqual(metrics["external_blogger_registry_confirmed_local_excluded_records_total"], 1)
        self.assertEqual(metrics["external_blogger_registry_eligible_records_total"], 3)
        self.assertEqual(metrics["external_blogger_registry_records_with_supported_tg_vk_source_total"], 2)
        self.assertEqual(metrics["external_blogger_registry_records_without_supported_tg_vk_source_total"], 1)
        self.assertEqual(metrics["external_blogger_registry_supported_records_in_queue_total"], 2)
        self.assertEqual(metrics["external_blogger_registry_supported_records_with_scanned_source_total"], 1)
        self.assertEqual(metrics["external_blogger_registry_supported_records_without_scanned_source_total"], 1)
        self.assertEqual(metrics["external_blogger_registry_supported_records_with_ko_source_total"], 1)
        self.assertEqual(metrics["external_blogger_registry_pipeline_stored_only_records_total"], 2)
        self.assertEqual(metrics["external_blogger_registry_canonical_tg_vk_sources_total"], 3)
        self.assertEqual(metrics["external_blogger_registry_canonical_sources_in_queue_total"], 2)
        self.assertEqual(metrics["external_blogger_registry_canonical_sources_missing_from_queue_total"], 1)
        self.assertEqual(metrics["external_blogger_registry_canonical_sources_scanned_total"], 1)
        self.assertEqual(metrics["external_blogger_registry_canonical_sources_with_ko_total"], 1)

    def test_confirmed_blogger_metrics_count_terminal_missing_telegram_username(self) -> None:
        mod = load_module()
        metrics = mod._confirmed_external_blogger_funnel_metrics(
            [{
                "canonical_source_key": "telegram:missing",
                "platform": "telegram",
                "source_url": "https://t.me/missing",
                "external_blogger_evidence_status": "confirmed_external",
                "source_queue_status": "rejected_unresolvable_telegram_source",
            }],
            [], [], [], [],
        )
        self.assertEqual(metrics["confirmed_external_blogger_terminal_total"], 1)
        self.assertEqual(metrics["confirmed_external_blogger_rejected_unresolvable_telegram_total"], 1)
        self.assertEqual(metrics["confirmed_external_blogger_unscanned_total"], 0)

    def test_heuristic_ko_funnel_uses_exclusive_product_outcomes(self) -> None:
        mod = load_module()
        run_id = "region-talk-orchestrator-candidate-report-unit"
        common = {
            "text": "Побывали в Калининграде, особенно запомнилась прогулка и красивый маршрут",
            "run_id": run_id,
        }
        rows = [
            {**common, "post_url": "https://t.me/local/1", "canonical_source_key": "telegram:local"},
            {**common, "post_url": "https://t.me/ad/2", "canonical_source_key": "telegram:ad", "vector_gate_status": "vector_reject_ad_promo"},
            {**common, "post_url": "https://t.me/pending/3", "canonical_source_key": "telegram:pending", "vector_gate_status": "vector_defer_wait_bge_m3"},
            {**common, "post_url": "https://t.me/sent/4", "canonical_source_key": "telegram:sent", "vector_gate_status": "vector_accept_candidate", "publication_candidate_status": "sent_to_chat"},
        ]
        metrics = mod._heuristic_ko_funnel_metrics(
            rows,
            [{"canonical_source_key": "telegram:local", "source_queue_status": "rejected_local_region_source"}],
            latest_candidate_run_id=run_id,
        )
        self.assertEqual(metrics["heuristic_ko_raw_posts_total"], 4)
        self.assertEqual(metrics["heuristic_ko_classified_total"], 4)
        self.assertEqual(metrics["heuristic_ko_outcome_counts"], {
            "dual_vector_pending": 1,
            "publication_sent": 1,
            "source_local": 1,
            "vector_ad_promo": 1,
        })
        self.assertEqual(metrics["heuristic_ko_latest_run_raw_posts_total"], 4)
        self.assertEqual(metrics["heuristic_ko_latest_run_sent_total"], 1)

    def test_heuristic_latest_run_ignores_downstream_overlay_run_stamp(self) -> None:
        mod = load_module()
        run_id = "region-talk-orchestrator-candidate-report-unit"
        rows = [
            {
                "text": "Побывали в Калининграде, запомнилась прогулка",
                "post_url": "https://t.me/current/1",
                "run_id": run_id,
                "vector_gate_status": "vector_reject_multi_region_roundup",
            },
            {
                "text": "Побывали в Калининграде, запомнилась прогулка",
                "post_url": "https://t.me/stale_overlay/2",
                # A CandidateReport reconciliation touched this historical
                # publication row, but the post was not processed in the run.
                "last_seen_run_id": run_id,
                "vector_gate_status": "vector_accept_candidate",
                "publication_candidate_status": "visual_review_pending",
            },
        ]
        metrics = mod._heuristic_ko_funnel_metrics(
            rows,
            [],
            latest_candidate_run_id=run_id,
            latest_processed_post_keys={"url:https://t.me/current/1"},
        )
        self.assertEqual(metrics["heuristic_ko_raw_posts_total"], 2)
        self.assertEqual(metrics["heuristic_ko_latest_run_raw_posts_total"], 1)
        self.assertEqual(metrics["heuristic_ko_latest_run_text_accepted_total"], 0)
        self.assertEqual(metrics["heuristic_ko_latest_run_publication_total"], 0)

    def test_ko_scope_conversion_is_unique_and_pre_content_filter(self) -> None:
        mod = load_module()
        metrics = mod._ko_scope_conversion_metrics([
            {
                "post_url": "https://t.me/travel/1",
                "vector_gate_status": "vector_accept_candidate",
                "kaliningrad_oblast_only_scope": True,
            },
            {
                "post_url": "https://t.me/travel/1",
                "vector_gate_status": "vector_reject_ad_promo",
                "kaliningrad_oblast_only_scope": "true",
            },
            {
                "post_url": "https://t.me/travel/2",
                "vector_gate_status": "vector_reject_not_kaliningrad_oblast",
                "kaliningrad_oblast_only_scope": False,
            },
            {"post_url": "https://t.me/legacy/3"},
            {"post_url": "https://t.me/legacy/4"},
        ])
        self.assertEqual(metrics["ko_scope_detected_posts_unique_total"], 1)
        self.assertEqual(metrics["ko_scope_evaluated_posts_unique_total"], 2)
        self.assertEqual(metrics["processed_to_ko_scope_conversion_percent"], 25.0)
        self.assertEqual(metrics["processed_to_ko_scope_detected_per_1000"], 250.0)
        self.assertEqual(metrics["ko_scope_evaluation_coverage_percent"], 50.0)
        self.assertEqual(metrics["evaluated_to_ko_scope_conversion_percent"], 50.0)

    def test_post_source_merge_key_prefers_canonical_source_over_synthetic_id(self) -> None:
        mod = load_module()
        self.assertEqual(
            mod._post_source_merge_key({
                "source_id": "src_synthetic",
                "canonical_source_key": "telegram:travel",
            }),
            "telegram:travel",
        )

    def test_confirmed_blogger_source_metrics_do_not_treat_vk_handle_as_telegram_alias(self) -> None:
        mod = load_module()
        sources = [
            {
                "canonical_source_key": "telegram:figarotravel",
                "platform": "telegram",
                "handle": "@figarotravel",
                "source_url": "https://t.me/figarotravel",
                "external_blogger_evidence_status": "confirmed_external",
            },
            {
                "canonical_source_key": "vk:figarotravel",
                "platform": "vk",
                "handle": "@figarotravel",
                "source_url": "https://vk.com/figarotravel",
                "external_blogger_evidence_status": "confirmed_external",
            },
        ]
        post = {
            "platform_post_key": "tg:figarotravel:7346",
            "post_url": "https://t.me/figarotravel/7346",
            "source_url": "https://t.me/figarotravel",
            "platform": "telegram",
            "vector_gate_status": "vector_accept_candidate",
        }
        metrics = mod._confirmed_external_blogger_funnel_metrics(
            sources,
            [post],
            [{"post_url": post["post_url"]}],
            [{"post_url": post["post_url"], "publication_candidate_status": "sent_to_chat"}],
            [{"post_url": post["post_url"], "delivery_status": "completed"}],
        )
        self.assertEqual(metrics["confirmed_external_blogger_sources_with_processed_posts_total"], 1)
        self.assertEqual(metrics["confirmed_external_blogger_sources_with_delivery_completed_posts_total"], 1)

    def test_terminal_unresolvable_vk_is_not_reported_as_active_unscanned_backlog(self) -> None:
        mod = load_module()
        metrics = mod._confirmed_external_blogger_funnel_metrics(
            [{
                "canonical_source_key": "vk:missing",
                "platform": "vk",
                "source_url": "https://vk.com/missing",
                "external_blogger_evidence_status": "confirmed_external",
                "source_queue_status": "rejected_unresolvable_vk_source",
                "fetch_status": "rejected_unresolvable_vk_source",
            }],
            [], [], [], [],
        )
        self.assertEqual(metrics["confirmed_external_blogger_scanned_total"], 0)
        self.assertEqual(metrics["confirmed_external_blogger_pending_total"], 0)
        self.assertEqual(metrics["confirmed_external_blogger_unscanned_total"], 0)
        self.assertEqual(metrics["confirmed_external_blogger_terminal_total"], 1)
        self.assertEqual(metrics["confirmed_external_blogger_rejected_unresolvable_vk_total"], 1)

    def test_latest_llm_budget_is_not_summed_across_daily_rows(self) -> None:
        mod = load_module()
        latest = mod._latest_llm_budget_row([
            {"budget_id": "region-talk-debug-20260710", "reserved_total": 8, "remaining": 92, "updated_at": "2026-07-10T23:44:58+00:00"},
            {"budget_id": "region-talk-debug-20260711", "reserved_total": 1, "remaining": 99, "updated_at": "2026-07-11T09:34:41+00:00"},
        ])
        self.assertEqual(latest["budget_id"], "region-talk-debug-20260711")
        self.assertEqual(latest["remaining"], 99)

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
        self.assertEqual(by_name["notify_confirmed"]["resource"], "telegram:DISCOVERY2")
        self.assertIn("telethon_discovery2", by_name["notify_confirmed"]["cmd"])
        main = by_name["launch_candidate_report"]
        bge = by_name["launch_bge_m3"]
        self.assertTrue(main["parallel_safe"])
        self.assertTrue(bge["parallel_safe"])
        self.assertTrue(by_name["launch_image_diagnostic"]["parallel_safe"])
        self.assertIn("120", by_name["launch_image_diagnostic"]["cmd"])
        self.assertIn("--wait-after-drain-seconds", by_name["launch_image_diagnostic"]["cmd"])
        image_cmd = by_name["launch_image_diagnostic"]["cmd"]
        self.assertEqual(image_cmd[image_cmd.index("--max-items-per-run") + 1], "10")
        self.assertEqual(image_cmd[image_cmd.index("--batch-size") + 1], "5")

        self.assertIn("--batch-limit", bge["cmd"])
        self.assertIn("48", bge["cmd"])
        self.assertIn("--batch-size", bge["cmd"])
        self.assertIn("4", bge["cmd"])
        self.assertEqual(bge["env"]["REGION_TALK_BGE_E5_ONLY"], "1")
        self.assertEqual(bge["env"]["REGION_TALK_BGE_INPUT_KINDS"], "text_vector_enrichment_item")
        self.assertEqual(bge["env"]["REGION_TALK_BGE_BATCH_SIZE"], "4")
        self.assertEqual(bge["env"]["REGION_TALK_BGE_YDB_SCAN_LIMIT"], "20000")
        self.assertEqual(main["env"]["REGION_TALK_STATE_BACKEND"], "ydb")
        self.assertEqual(main["env"]["REGION_TALK_REQUIRE_YDB_STATE"], "1")
        self.assertEqual(main["env"]["REGION_TALK_TEXT_EMBEDDING_MODEL_IDS"], "intfloat/multilingual-e5-base")
        self.assertEqual(main["env"]["REGION_TALK_REQUIRE_EXTERNAL_BGE_M3_FOR_IMAGE_QUEUE"], "1")
        self.assertEqual(main["env"]["REGION_TALK_SKIP_REPORT_TAIL_AFTER_IMAGE_QUEUE_HANDOFF"], "0")
        self.assertEqual(main["env"]["REGION_TALK_SKIP_REPORT_TAIL_AFTER_SOURCE_QUEUE_HANDOFF"], "0")
        self.assertEqual(main["env"]["REGION_TALK_NOTEBOOK_MAX_RUNTIME_SECONDS"], "1200")
        self.assertEqual(main["env"]["REGION_TALK_SOURCE_SELECTION_YDB_QUEUE_ONLY"], "1")
        self.assertEqual(main["env"]["REGION_TALK_MAX_POSTS_PER_SOURCE"], "10")
        self.assertEqual(main["env"]["REGION_TALK_TG_MAX_HISTORY_SOURCES_PER_RUN"], "4")
        self.assertEqual(main["env"]["REGION_TALK_TG_PUBLIC_WEB_FETCH_FIRST"], "0")
        self.assertEqual(main["env"]["REGION_TALK_TG_PUBLIC_WEB_FALLBACK"], "0")
        self.assertEqual(main["env"]["REGION_TALK_FETCH_POST_LINK_QUEUE_FIRST"], "1")
        self.assertEqual(main["env"]["REGION_TALK_POST_LINK_QUEUE_FETCH_LIMIT"], "5")
        self.assertEqual(main["env"]["REGION_TALK_TG_CACHED_ENTITY_ONLY"], "1")
        self.assertEqual(main["env"]["REGION_TALK_SOURCE_QUEUE_UNCACHED_RESOLVE_LANE_PER_RUN"], "1")
        self.assertEqual(main["env"]["REGION_TALK_TG_MAX_NETWORK_RESOLVES_PER_RUN"], "1")
        self.assertEqual(main["env"]["REGION_TALK_TG_EXACT_POST_NETWORK_RESOLVE_BUDGET_PER_RUN"], "1")
        self.assertEqual(main["env"]["REGION_TALK_YDB_MAX_SOURCE_ROWS"], "20000")
        self.assertEqual(main["env"]["REGION_TALK_YDB_SOURCE_QUEUE_FULL_READ_LIMIT"], "20000")
        self.assertEqual(main["env"]["REGION_TALK_YDB_MAX_TEXT_VECTOR_ROWS"], "20000")
        self.assertIn("--max-sources", main["cmd"])
        self.assertIn("4", main["cmd"])
        self.assertEqual(main["env"]["REGION_TALK_HISTORY_SOURCES_TARGET"], "4")
        self.assertEqual(main["env"]["REGION_TALK_TG_MAX_HISTORY_SOURCES_PER_RUN"], "4")
        self.assertEqual(main["env"]["REGION_TALK_CONFIRMED_BLOGGER_HISTORY_SLOTS_PER_RUN"], "4")
        self.assertEqual(main["env"]["REGION_TALK_FAST_CHECK_KO_SOURCES_PER_RUN"], "10")
        self.assertEqual(main["env"]["REGION_TALK_FAST_CHECK_QUERY_STRATEGY"], "adaptive_cursor_v1")
        self.assertEqual(main["env"]["REGION_TALK_FAST_CHECK_ADAPTIVE_PREFER_CONTINUATIONS"], "0")
        self.assertEqual(main["env"]["REGION_TALK_FAST_CHECK_KO_QUERIES_PER_SOURCE"], "2")
        self.assertEqual(main["env"]["REGION_TALK_CONFIRMED_BLOGGER_FAST_CHECK_QUERIES_PER_SOURCE"], "8")
        self.assertEqual(main["env"]["REGION_TALK_FAST_CHECK_STAGE_MAX_SECONDS"], "180")
        self.assertEqual(main["env"]["REGION_TALK_TG_SIMILAR_ENABLED"], "1")
        self.assertEqual(main["env"]["REGION_TALK_TG_SIMILAR_MAX_SEED_CHANNELS_PER_RUN"], "3")
        self.assertEqual(main["env"]["REGION_TALK_TELEGRAM_QUERY_SOURCE"], "place_lexicon")
        self.assertEqual(main["env"]["REGION_TALK_MAX_TELEGRAM_KEYWORD_QUERIES"], "6")
        self.assertEqual(main["env"]["REGION_TALK_MAX_TELEGRAM_KEYWORD_PHRASE_QUERIES"], "4")
        self.assertEqual(main["env"]["REGION_TALK_MAX_TELEGRAM_HASHTAG_QUERIES_PER_RUN"], "2")
        self.assertEqual(main["env"]["REGION_TALK_RUNTIME_RESERVE_BEFORE_DISCOVERY_TAIL_SECONDS"], "300")
        self.assertEqual(main["env"]["REGION_TALK_RUNTIME_FIXED_TAIL_SECONDS"], "300")
        self.assertEqual(main["env"]["REGION_TALK_RUNTIME_SECONDS_PER_SCORED_POST"], "5")
        self.assertEqual(main["env"]["REGION_TALK_YDB_ONLINE_QUEUE_BULK_UPSERT"], "1")
        self.assertEqual(main["env"]["REGION_TALK_YDB_STATE_WRITE_REQUEST_TIMEOUT_SECONDS"], "20")
        self.assertEqual(main["env"]["REGION_TALK_YDB_RETENTION_PRUNE"], "0")

    def test_image_action_batch_is_explicitly_tunable_and_bounded(self) -> None:
        mod = load_module()
        metrics = {
            "publication_sent_total": 0,
            "publication_confirmed_total": 0,
            "publication_unsent_confirmed_total": 0,
            "image_actual_scored_total": 0,
            "publication_candidate_total": 0,
            "bge_pending_sample_total": 0,
            "image_pending_total": 7,
        }
        with mock.patch.dict(
            os.environ,
            {
                "REGION_TALK_ORCHESTRATOR_IMAGE_MAX_ITEMS_PER_RUN": "8",
                "REGION_TALK_ORCHESTRATOR_IMAGE_BATCH_SIZE": "20",
            },
            clear=False,
        ):
            actions = mod.build_decision_plan(
                metrics,
                target_confirmed=20,
                bge_threshold=1,
                image_threshold=1,
            )
        image_cmd = next(a for a in actions if a["action"] == "launch_image_diagnostic")["cmd"]
        self.assertEqual(image_cmd[image_cmd.index("--max-items-per-run") + 1], "8")
        self.assertEqual(image_cmd[image_cmd.index("--batch-size") + 1], "8")

    def test_candidate_adaptive_budget_keeps_generic_history_small_and_fast_check_wide(self) -> None:
        mod = load_module()
        for metrics in [
            {"candidate_heartbeat_runtime_elapsed_seconds": 800},
            {"candidate_heartbeat_runtime_elapsed_seconds": 960},
            {"candidate_heartbeat_runtime_elapsed_seconds": 1100},
            {"candidate_heartbeat_runtime_elapsed_seconds": 800, "bge_pending_sample_total": 36},
            {"candidate_heartbeat_runtime_elapsed_seconds": 800, "bge_pending_sample_total": 60},
        ]:
            budget = mod.candidate_adaptive_budget(metrics)
            self.assertEqual(budget["history_sources"], 4)
            self.assertEqual(budget["confirmed_blogger_slots"], 4)
            self.assertEqual(budget["fast_check_sources"], 10)
        failed_tail = mod.candidate_adaptive_budget({
            "candidate_heartbeat_runtime_elapsed_seconds": 0,
            "candidate_heartbeat_event_name": "state_write_started",
            "candidate_heartbeat_phase": "state_write",
            "candidate_heartbeat_status": "running",
        })
        self.assertEqual(failed_tail["history_sources"], 4)
        self.assertEqual(failed_tail["incomplete_late_tail_observed"], 1)

        evidence_headroom = mod.candidate_adaptive_budget({
            "candidate_heartbeat_runtime_elapsed_seconds": 401,
            "confirmed_external_blogger_pending_total": 26,
            "bge_pending_sample_total": 17,
            "bge_capacity_rows": 48,
        })
        self.assertEqual(evidence_headroom["history_sources"], 6)
        self.assertEqual(evidence_headroom["confirmed_blogger_slots"], 5)
        self.assertEqual(evidence_headroom["confirmed_blogger_headroom_used"], 1)

        stale_only_headroom = mod.candidate_adaptive_budget({
            "candidate_heartbeat_runtime_elapsed_seconds": 401,
            "confirmed_external_blogger_pending_total": 26,
            "bge_pending_sample_total": 100,
            "bge_missing_current_sample_total": 0,
            "bge_existing_stale_rescore_sample_total": 700,
            "bge_capacity_rows": 48,
        })
        self.assertEqual(stale_only_headroom["history_sources"], 6)
        self.assertEqual(stale_only_headroom["confirmed_blogger_headroom_used"], 1)

        old_sources = os.environ.get("REGION_TALK_ORCHESTRATOR_FAST_CHECK_SOURCES")
        try:
            os.environ["REGION_TALK_ORCHESTRATOR_FAST_CHECK_SOURCES"] = "1"
            self.assertEqual(mod.candidate_adaptive_budget({})["fast_check_sources"], 1)
        finally:
            if old_sources is None:
                os.environ.pop("REGION_TALK_ORCHESTRATOR_FAST_CHECK_SOURCES", None)
            else:
                os.environ["REGION_TALK_ORCHESTRATOR_FAST_CHECK_SOURCES"] = old_sources

    def test_ydb_endpoint_discovery_retries_with_fresh_closed_drivers(self) -> None:
        mod = load_module()
        drivers = []

        class FakeDriver:
            def __init__(self, attempt: int) -> None:
                self.attempt = attempt
                self.stopped = False

            def wait(self, *, timeout: int, fail_fast: bool) -> None:
                self.timeout = timeout
                self.fail_fast = fail_fast
                if self.attempt < 3:
                    raise RuntimeError("Failed to resolve endpoints: Deadline exceeded")

            def stop(self) -> None:
                self.stopped = True

        class FakeYdb:
            @staticmethod
            def Driver(**_kwargs):
                driver = FakeDriver(len(drivers) + 1)
                drivers.append(driver)
                return driver

        with mock.patch.dict(os.environ, {
            "REGION_TALK_ORCHESTRATOR_YDB_CONNECT_ATTEMPTS": "3",
            "REGION_TALK_ORCHESTRATOR_YDB_CONNECT_BACKOFF_SECONDS": "0",
            "REGION_TALK_ORCHESTRATOR_YDB_CONNECT_TIMEOUT_SECONDS": "7",
        }):
            driver = mod._open_ydb_driver(
                FakeYdb,
                endpoint="grpcs://example",
                database="/db",
                credentials="token",
            )

        self.assertIs(driver, drivers[2])
        self.assertEqual([item.stopped for item in drivers], [True, True, False])
        self.assertTrue(all(item.timeout == 7 for item in drivers))


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

    def test_cached_exact_backlog_expands_safe_candidate_batch(self) -> None:
        mod = load_module()
        actions = mod.build_decision_plan(
            {
                "post_link_queue_exact_ready_total": 12,
                "post_link_queue_entity_cache_hit_total": 9,
            },
            target_confirmed=20,
            bge_threshold=1,
            image_threshold=1,
        )
        main = next(action for action in actions if action["action"] == "launch_candidate_report")
        self.assertEqual(main["env"]["REGION_TALK_POST_LINK_QUEUE_FETCH_LIMIT"], "8")
        self.assertIn("drain up to 8 exact KO links first", main["reason"])
        self.assertEqual(main["env"]["REGION_TALK_TG_EXACT_POST_NETWORK_RESOLVE_BUDGET_PER_RUN"], "1")

    def test_source_terminal_exact_cleanup_gets_one_bounded_drain_wave(self) -> None:
        mod = load_module()
        actions = mod.build_decision_plan(
            {
                "post_link_queue_exact_ready_total": 0,
                "post_link_queue_bge_ready_rescore_total": 0,
                "post_link_queue_source_terminal_cleanup_total": 3,
                "post_link_queue_bge_ready_rescore_source_terminal_cleanup_total": 12,
            },
            target_confirmed=20,
            bge_threshold=1,
            image_threshold=1,
        )
        main = next(action for action in actions if action["action"] == "launch_candidate_report")
        self.assertEqual(main["env"]["REGION_TALK_POST_LINK_QUEUE_FETCH_LIMIT"], "8")
        self.assertIn("drain up to 8 source-terminal exact cleanup rows", main["reason"])

    def test_orchestrator_polls_faster_while_downstream_backlog_exists(self) -> None:
        mod = load_module()
        self.assertEqual(
            mod.orchestrator_poll_sleep_seconds({}, normal_seconds=180, downstream_seconds=60),
            180,
        )
        self.assertEqual(
            mod.orchestrator_poll_sleep_seconds(
                {"bge_pending_sample_total": 33}, normal_seconds=180, downstream_seconds=60,
            ),
            60,
        )

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
        self.assertEqual(mod._orchestrator_kind_limit("text_vector_enrichment_item", 100), 20000)

    def test_checkpoint_v4_candidate_config_reads_complete_row_level_state(self) -> None:
        mod = load_module()
        self.assertGreaterEqual(int(mod.MAIN_DISCOVERY_YDB_BUDGET_ENV["REGION_TALK_YDB_MAX_POST_ROWS"]), 20000)
        self.assertGreaterEqual(int(mod.MAIN_DISCOVERY_YDB_BUDGET_ENV["REGION_TALK_YDB_MAX_TEXT_VECTOR_ROWS"]), 20000)
        self.assertGreaterEqual(int(mod.MAIN_DISCOVERY_YDB_BUDGET_ENV["REGION_TALK_YDB_MAX_CANDIDATE_ROWS"]), 5000)
        self.assertEqual(mod.MAIN_DISCOVERY_YDB_BUDGET_ENV["REGION_TALK_WRITE_REPORT_ARTIFACTS"], "0")
        self.assertEqual(mod.MAIN_DISCOVERY_YDB_BUDGET_ENV["REGION_TALK_DEFER_DISCOVERY_ON_CRITICAL_WORK"], "1")

    def test_cursor_metric_prefers_highest_position_over_stale_history(self) -> None:
        mod = load_module()
        current = {"_ydb_pk": "queue_cursor:source", "queue_name": "unified_source_queue", "cursor_position": 475, "_ydb_updated_at": "2026-07-09T15:40:00Z"}
        stale = {"_ydb_pk": "queue_cursor:source:old-run", "queue_name": "unified_source_queue", "cursor_position": 1957, "_ydb_updated_at": "2026-07-09T15:50:00Z"}
        self.assertFalse(mod._cursor_row_is_better(current, stale, "unified_source_queue"))
        self.assertTrue(mod._cursor_row_is_better(stale, current, "unified_source_queue"))

    def test_progress_signature_uses_only_durable_product_milestones(self) -> None:
        mod = load_module()
        base = {
            "publics_total": 10,
            "processed_posts_unique_total": 15,
            "publication_delivery_completed_total": 0,
            "some_future_numeric_metric": 1,
            "kaggle_kernel_statuses": {"region-talk-candidate-report": "RUNNING"},
            "non_numeric_status": "RUNNING",
        }
        more_publics = dict(base, publics_total=11)
        more_future_metric = dict(base, some_future_numeric_metric=2)
        more_posts = dict(base, processed_posts_unique_total=16)
        self.assertIn(("processed_posts_unique_total", 15), mod._progress_signature(base))
        self.assertNotIn(("publics_total", 10), mod._progress_signature(base))
        self.assertNotIn(("some_future_numeric_metric", 1), mod._progress_signature(base))
        self.assertNotIn(("non_numeric_status", 0), mod._progress_signature(base))
        self.assertEqual(mod._progress_signature(base), mod._progress_signature(more_publics))
        self.assertEqual(mod._progress_signature(base), mod._progress_signature(more_future_metric))
        self.assertNotEqual(mod._progress_signature(base), mod._progress_signature(more_posts))
        self.assertTrue(mod._product_progress_increased(None, mod._progress_signature(base)))
        self.assertFalse(mod._product_progress_increased(mod._progress_signature(base), mod._progress_signature(more_publics)))
        self.assertTrue(mod._product_progress_increased(mod._progress_signature(base), mod._progress_signature(more_posts)))

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

    def test_stats_message_exposes_candidate_audit_and_bge_capacity(self) -> None:
        mod = load_module()
        message = mod.build_orchestrator_stats_message({
            "candidate_memory_total": 100,
            "candidate_memory_operational_total": 70,
            "candidate_memory_terminal_local_audit_total": 25,
            "candidate_memory_terminal_spam_audit_total": 5,
            "candidate_memory_dual_pending_total": 12,
            "candidate_memory_image_wait_total": 8,
            "bge_pending_sample_total": 36,
            "bge_immediate_pair_backlog_total": 4,
            "bge_immediate_pair_backlog_capacity_percent": 8,
            "bge_stale_maintenance_backlog_total": 32,
            "bge_stale_maintenance_selected_sample_total": 32,
            "bge_capacity_rows": 48,
            "bge_backlog_capacity_percent": 75,
            "bge_source_terminal_skipped_sample_total": 9,
        })
        self.assertIn("100/70/25/5/12/8", message)
        self.assertIn("4/48/8%; 32/32/9", message)

    def test_stats_message_exposes_processed_to_pre_filter_ko_conversion(self) -> None:
        mod = load_module()
        message = mod.build_orchestrator_stats_message({
            "processed_posts_unique_total": 12075,
            "ko_scope_detected_posts_unique_total": 525,
            "ko_scope_evaluated_posts_unique_total": 3563,
            "processed_to_ko_scope_conversion_percent": 4.35,
            "processed_to_ko_scope_detected_per_1000": 43.5,
            "ko_scope_evaluation_coverage_percent": 29.51,
            "evaluated_to_ko_scope_conversion_percent": 14.74,
        })
        self.assertIn("525/12075/4.35%/43.5", message)
        self.assertIn("3563/12075/29.51%; 14.74%", message)

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

    def test_latest_processed_metrics_separate_new_posts_from_refreshed_rows(self) -> None:
        mod = load_module()
        run_id = "region-talk-candidate-20260713T160229Z"
        metrics, rows, unique_keys = mod._latest_processed_post_metrics([
            {
                "post_url": "https://t.me/new/1",
                "run_id": run_id,
                "first_seen_run_id": run_id,
            },
            {
                "post_url": "https://t.me/known/2",
                "last_seen_run_id": run_id,
                "first_seen_run_id": "older-run",
            },
            {
                "post_url": "https://t.me/known/2",
                "current_run_id": run_id,
                "first_seen_run_id": "older-run",
            },
            {
                "post_url": "https://t.me/unrelated/3",
                "run_id": "older-run",
                "first_seen_run_id": "older-run",
            },
        ], run_id)
        self.assertEqual(len(rows), 3)
        self.assertEqual(unique_keys, {"url:https://t.me/new/1", "url:https://t.me/known/2"})
        self.assertEqual(metrics["processed_posts_unique_latest_candidate_run_total"], 2)
        self.assertEqual(metrics["processed_posts_new_latest_candidate_run_total"], 1)
        self.assertEqual(metrics["processed_posts_reprocessed_latest_candidate_run_total"], 1)
        self.assertEqual(metrics["processed_post_duplicate_identity_rows_latest_candidate_run_total"], 1)

    def test_source_merge_does_not_resurrect_terminal_local_queue_from_online_overlay(self) -> None:
        mod = load_module()
        rows = mod._merge_source_rows(
            [{
                "canonical_source_key": "telegram:goneagain_russia",
                "source_queue_status": "rejected_local_region_source",
                "source_scope": "local_region",
                "source_geo_class": "kaliningrad_local",
                "source_quick_class": "local_region_source",
                "source_locality_reconciliation_status": "local_repeated_ko_over_time",
                "next_action": "do_not_rescan_rejected_source",
                "posts_scanned": 13,
                "ko_posts_found": 9,
            }],
            [{
                "canonical_source_key": "telegram:goneagain_russia",
                "source_queue_status": "processed_found_ko_candidate",
                "source_scope": "unknown",
                "source_geo_class": "unknown",
                "source_quick_class": "candidate_keep",
                "fetch_status": "ok",
                "posts_scanned": 7,
                "ko_posts_found": 2,
            }],
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["source_queue_status"], "rejected_local_region_source")
        self.assertEqual(rows[0]["source_scope"], "local_region")
        self.assertEqual(rows[0]["source_geo_class"], "kaliningrad_local")
        self.assertEqual(rows[0]["source_quick_class"], "local_region_source")
        self.assertEqual(rows[0]["source_locality_reconciliation_status"], "local_repeated_ko_over_time")
        self.assertEqual(rows[0]["next_action"], "do_not_rescan_rejected_source")
        self.assertEqual(rows[0]["posts_scanned"], 13)
        self.assertEqual(rows[0]["ko_posts_found"], 9)
        self.assertEqual(rows[0]["fetch_status"], "ok")

        images = [{
            "post_url": "https://t.me/goneagain_russia/7599",
            "source_url": "https://t.me/goneagain_russia",
            "canonical_source_key": "telegram:goneagain_russia",
            "image_queue_status": "actual_scored",
            "image_model_input_type": "actual_image",
        }]
        publications = [{
            "post_url": "https://t.me/goneagain_russia/7599",
            "canonical_source_key": "telegram:goneagain_russia",
            "publication_status": "eligibility_reject_tombstone",
            "publication_candidate_status": "tombstoned_reject",
            "publication_eligibility_verdict": "reject",
            "publication_eligibility_gate_version": mod.CURRENT_PUBLICATION_ELIGIBILITY_GATE_VERSION,
            "authoritative_source_fingerprint": "older-counter-fingerprint",
        }]
        metrics = mod._publication_handoff_metrics(images, publications, rows)
        self.assertEqual(metrics["finalizer_pending_url_total"], 0)

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
            {"post_url": "https://t.me/a/1", "text_hash": "old", "text_excerpt": "Старый длинный текст о поездке в Калининградскую область", "encoder_contract": mod.CURRENT_E5_ENCODER_CONTRACT, "semantic_bank_version": "v1", "created_at": "2026-07-10T08:00:00Z"},
            {"post_url": "https://t.me/a/1", "text_hash": "h1", "text_excerpt": "Личный подробный рассказ о поездке в Калининградскую область", "encoder_contract": mod.CURRENT_E5_ENCODER_CONTRACT, "semantic_bank_version": "v1", "created_at": "2026-07-10T10:00:00Z"},
            {"post_url": "https://t.me/b/2", "text_hash": "h2", "text_excerpt": "Привет!", "encoder_contract": mod.CURRENT_E5_ENCODER_CONTRACT, "semantic_bank_version": "v2", "created_at": "2026-07-10T09:00:00Z"},
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
        self.assertEqual(metrics["text_vector_current_version_e5_below_bge_min_text_total"], 1)
        self.assertEqual(metrics["text_vector_current_version_e5_without_bge_actionable_total"], 0)
        self.assertEqual(metrics["text_vector_current_version_dual_actionable_coverage_percent"], 100)
        self.assertEqual(metrics["text_vector_current_version_bge_pair_lag_seconds_max"], 120)
        self.assertEqual(metrics["text_vector_current_version_bge_pending_lag_seconds_max"], 10800)
        self.assertEqual(metrics["text_vector_stale_version_e5_rows_total"], 1)

    def test_current_vector_actionable_backlog_excludes_terminal_sources(self) -> None:
        mod = load_module()
        e5 = [{
            "post_url": "https://t.me/local/1",
            "text_hash": "h1",
            "text_excerpt": "Подробный локальный пост о Калининградской области и музее",
            "encoder_contract": mod.CURRENT_E5_ENCODER_CONTRACT,
            "semantic_bank_version": "v1",
            "created_at": "2026-07-10T10:00:00Z",
            "source_queue_status": "rejected_local_region_source",
            "source_terminal_excluded": True,
        }]
        metrics = mod._text_vector_pair_metrics(e5, [])
        self.assertEqual(metrics["text_vector_current_version_e5_without_bge_total"], 1)
        self.assertEqual(metrics["text_vector_current_version_e5_without_bge_source_terminal_total"], 1)
        self.assertEqual(metrics["text_vector_current_version_e5_without_bge_actionable_total"], 0)

    def test_post_link_queue_states_head_blocking_integrity_and_entity_cache(self) -> None:
        mod = load_module()
        now = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)
        rows = [
            {"post_url": "https://t.me/cool/1", "post_link_status": "retry_fetch", "post_link_priority": 0, "next_attempt_after": "2026-07-10T13:00:00Z"},
            {"post_url": "https://t.me/wait/2", "post_link_status": "retry_wait_entity_cache", "post_link_priority": 1, "handle": "@wait"},
            {"post_url": "https://t.me/ready/3", "post_link_status": "pending_fetch", "post_link_priority": 2},
            {"post_url": "https://t.me/ready/3?single=1", "post_link_status": "fetch_error", "post_link_priority": 3},
            {"post_url": "https://t.me/done/4", "post_link_status": "fetched", "post_link_priority": 0},
            {"post_url": "https://t.me/rejected/5", "post_link_status": "operator_rejected", "post_link_priority": 0},
            {"post_url": "https://t.me/local/6", "post_link_status": "pending_fetch", "post_link_priority": 0, "canonical_source_key": "telegram:local"},
            {"post_url": "not-a-post", "post_link_status": "pending_fetch", "post_link_priority": 4},
        ]
        cache = [{"entity_cache_key": "telegram:username:wait", "username": "wait", "channel_id_private": "1", "access_hash_private": "2"}]
        sources = [{"canonical_source_key": "telegram:local", "source_queue_status": "rejected_local_region_source"}]
        metrics = mod._post_link_queue_metrics(rows, cache, sources, now=now)
        self.assertEqual(metrics["post_link_queue_exact_ready_total"], 2)
        self.assertEqual(metrics["post_link_queue_cooldown_total"], 1)
        self.assertEqual(metrics["post_link_queue_entity_wait_total"], 1)
        self.assertEqual(metrics["post_link_queue_terminal_total"], 3)
        self.assertEqual(metrics["post_link_queue_source_terminal_cleanup_total"], 1)
        self.assertEqual(metrics["post_link_queue_unknown_status_total"], 1)
        self.assertEqual(metrics["post_link_queue_head_blocked_total"], 2)
        self.assertEqual(metrics["post_link_queue_head_blocked_cooldown_total"], 1)
        self.assertEqual(metrics["post_link_queue_head_blocked_entity_wait_total"], 1)
        self.assertEqual(metrics["post_link_queue_integrity_duplicate_url_rows_total"], 1)
        self.assertEqual(metrics["post_link_queue_integrity_invalid_url_total"], 1)
        self.assertEqual(metrics["telegram_entity_cache_valid_rows_total"], 1)
        self.assertEqual(metrics["post_link_queue_entity_wait_cache_now_available_total"], 1)

    def test_bge_ready_exact_rescore_is_visible_and_excludes_terminal_publications(self) -> None:
        mod = load_module()
        links = [
            {"post_url": "https://t.me/a/1", "post_link_status": "fetched"},
            {"post_url": "https://t.me/b/2", "post_link_status": "fetched"},
            {"post_url": "https://t.me/local/3", "post_link_status": "fetched", "canonical_source_key": "telegram:local"},
        ]
        processed = [
            {"post_url": "https://t.me/a/1", "vector_gate_status": "vector_defer_wait_bge_m3"},
            {"post_url": "https://t.me/b/2", "current_stage": "dual_model_vector_enrichment_pending"},
            {"post_url": "https://t.me/local/3", "current_stage": "dual_model_vector_enrichment_pending"},
        ]
        vectors = [
            {"post_url": "https://t.me/a/1", "model_short": "bge_m3"},
            {"post_url": "https://t.me/b/2", "model_id": "BAAI/bge-m3"},
            {"post_url": "https://t.me/local/3", "model_id": "BAAI/bge-m3"},
        ]
        publications = [{"post_url": "https://t.me/b/2", "publication_status": "gemini_reject"}]
        sources = [{"canonical_source_key": "telegram:local", "source_queue_status": "rejected_local_region_source"}]

        metrics = mod._bge_ready_exact_rescore_metrics(links, processed, vectors, publications, sources)

        self.assertEqual(metrics["post_link_queue_bge_ready_rescore_total"], 1)
        self.assertEqual(metrics["post_link_queue_bge_ready_rescore_urls"], ["https://t.me/a/1"])
        self.assertEqual(metrics["post_link_queue_bge_ready_rescore_source_terminal_cleanup_total"], 1)

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

    def test_source_merge_does_not_let_stale_terminal_status_override_newer_queue_repair(self) -> None:
        mod = load_module()
        queue = {
            "_ydb_pk": "source_queue_item:telegram:travel",
            "canonical_source_key": "telegram:travel",
            "source_queue_status": "processed_found_ko_candidate",
            "source_scope": "external",
            "source_quick_class": "candidate_keep",
            "updated_at": "2026-07-17T09:00:00+00:00",
        }
        stale_status = {
            "_ydb_pk": "source_status_item:telegram:travel",
            "canonical_source_key": "telegram:travel",
            "source_queue_status": "rejected_spam_source",
            "source_quick_class": "spam_source_reject",
            "updated_at": "2026-07-17T08:00:00+00:00",
        }
        merged = mod._merge_source_rows([queue], [stale_status])[0]
        self.assertEqual(merged["source_queue_status"], "processed_found_ko_candidate")
        self.assertEqual(merged["source_quick_class"], "candidate_keep")

        fresh_status = {**stale_status, "updated_at": "2026-07-17T10:00:00+00:00"}
        merged_fresh = mod._merge_source_rows([queue], [fresh_status])[0]
        self.assertEqual(merged_fresh["source_queue_status"], "rejected_spam_source")

        stale_online = {
            **stale_status,
            "_ydb_pk": "online_source_item:telegram:travel",
            "updated_at": "2026-07-17T08:30:00+00:00",
        }
        merged_two_overlays = mod._merge_source_rows([queue], [stale_status], [stale_online])[0]
        self.assertEqual(merged_two_overlays["source_queue_status"], "processed_found_ko_candidate")

    def test_publication_taxonomy_and_finalizer_are_url_level(self) -> None:
        mod = load_module()
        images = [
            {"post_url": "https://t.me/a/1", "image_queue_status": "actual_scored", "image_model_input_type": "actual_image", "image_publication_ready": "true"},
            {"post_url": "https://t.me/b/2", "image_queue_status": "actual_scored", "image_model_input_type": "actual_image", "image_publication_ready": "true"},
            {"post_url": "https://t.me/sent/3", "image_queue_status": "actual_scored", "image_model_input_type": "actual_image", "image_publication_ready": "true"},
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
            {
                "post_url": "https://t.me/sent/3",
                "publication_status": "eligibility_revoked",
                "publication_candidate_status": "revoked",
                "sent_to_chat": "true",
                "publication_eligibility_verdict": "review",
                "publication_eligibility_gate_version": "stale-gate",
                "authoritative_source_fingerprint": "stale-fingerprint",
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
        self.assertEqual(metrics["publication_candidate_total"], 3)
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

        stale_review = mod._publication_handoff_metrics(
            [images[0]],
            [{
                "post_url": "https://t.me/a/1",
                "canonical_source_key": "telegram:a",
                "publication_candidate_status": "tombstoned_review",
                "publication_eligibility_verdict": "review",
                "publication_eligibility_gate_version": mod.CURRENT_PUBLICATION_ELIGIBILITY_GATE_VERSION,
                "authoritative_source_fingerprint": "stale-source-fingerprint",
            }],
            sources,
        )
        self.assertEqual(stale_review["finalizer_pending_urls"], ["https://t.me/a/1"])

        terminal_reject = mod._publication_handoff_metrics(
            [images[1]],
            [{
                "post_url": "https://t.me/b/2",
                "publication_candidate_status": "llm_rejected",
                "publication_status": "gemini_reject",
            }],
            [],
        )
        self.assertEqual(terminal_reject["publication_source_evidence_backlog_total"], 0)
        self.assertEqual(terminal_reject["finalizer_pending_url_total"], 0)

        terminal_review_with_changed_source = mod._publication_handoff_metrics(
            [images[1]],
            [{
                "post_url": "https://t.me/b/2",
                "canonical_source_key": "telegram:b",
                "publication_candidate_status": "llm_needs_review",
                "publication_status": "gemini_needs_review",
                "authoritative_source_fingerprint": "old-source-fingerprint",
            }],
            [{
                "canonical_source_key": "telegram:b",
                "source_url": "https://t.me/b",
                "source_scope": "external",
                "source_geo_class": "nonlocal_russia",
                "source_queue_status": "processed_found_ko_candidate",
                "posts_scanned": 25,
            }],
        )
        self.assertEqual(terminal_review_with_changed_source["finalizer_pending_url_total"], 0)

        local_source = {
            "canonical_source_key": "telegram:kenig01",
            "source_url": "https://t.me/kenig01",
            "source_scope": "local_region",
            "source_geo_class": "kaliningrad_local",
            "source_queue_status": "rejected_local_region_source",
            "posts_scanned": 9,
        }
        local_terminal = mod._publication_handoff_metrics(
            [{
                "post_url": "https://t.me/kenig01/10",
                "image_queue_status": "not_reviewable_unsupported_media",
                "media_kind": "video",
            }],
            [{
                "post_url": "https://t.me/kenig01/10",
                "canonical_source_key": "telegram:kenig01",
                "publication_status": "eligibility_reject_tombstone",
                "publication_candidate_status": "tombstoned_reject",
                "publication_eligibility_verdict": "reject",
                "publication_eligibility_gate_version": mod.CURRENT_PUBLICATION_ELIGIBILITY_GATE_VERSION,
                "authoritative_source_fingerprint": "older-counter-fingerprint",
            }],
            [local_source],
        )
        self.assertEqual(local_terminal["finalizer_pending_url_total"], 0)

    def test_text_restore_is_visible_but_does_not_hot_loop_finalizer(self) -> None:
        mod = load_module()
        metrics = mod._publication_handoff_metrics(
            [{
                "post_url": "https://t.me/travel/10",
                "image_queue_status": "actual_scored",
                "image_model_input_type": "actual_image",
            }],
            [{
                "post_url": "https://t.me/travel/10",
                "publication_status": "text_restore_pending",
                "publication_candidate_status": "awaiting_text_restore",
                "publication_eligibility_verdict": "eligible",
                "publication_eligibility_gate_version": mod.CURRENT_PUBLICATION_ELIGIBILITY_GATE_VERSION,
            }],
            [],
        )
        self.assertEqual(metrics["publication_text_restore_pending_raw_total"], 1)
        self.assertEqual(metrics["publication_text_restore_pending_total"], 1)
        self.assertEqual(metrics["publication_text_restore_tombstoned_total"], 0)
        self.assertEqual(metrics["publication_active_candidate_total"], 1)
        self.assertEqual(metrics["finalizer_pending_url_total"], 0)
        self.assertEqual(metrics["publication_text_restore_ready_for_finalizer_total"], 0)

    def test_restored_candidate_text_reopens_finalizer_without_another_fetch(self) -> None:
        mod = load_module()
        url = "https://t.me/travel/10"
        metrics = mod._publication_handoff_metrics(
            [{
                "post_url": url,
                "image_queue_status": "actual_scored",
                "image_model_input_type": "actual_image",
            }],
            [{
                "post_url": url,
                "publication_status": "text_restore_pending",
                "publication_candidate_status": "awaiting_text_restore",
                "publication_eligibility_verdict": "eligible",
                "publication_eligibility_gate_version": mod.CURRENT_PUBLICATION_ELIGIBILITY_GATE_VERSION,
            }],
            [],
            [{
                "post_url": url,
                "full_text": "Полный восстановленный рассказ о поездке в Калининград.",
                "vector_gate_status": "vector_accept_candidate",
                "text_vector_fusion_status": "fused_e5_bge_m3",
                "kaliningrad_oblast_only_scope": True,
            }],
        )
        self.assertEqual(metrics["publication_text_restore_pending_total"], 1)
        self.assertEqual(metrics["publication_text_restore_ready_for_finalizer_total"], 1)
        self.assertEqual(metrics["publication_text_restore_ready_for_finalizer_urls"], [url])
        self.assertEqual(metrics["finalizer_pending_url_total"], 1)
        self.assertEqual(metrics["finalizer_pending_urls"], [url])

    def test_stale_text_restore_is_not_active_after_current_dual_text_reject(self) -> None:
        mod = load_module()
        url = "https://t.me/travel/12"
        metrics = mod._publication_handoff_metrics(
            [{
                "post_url": url,
                "image_queue_status": "actual_scored",
                "image_model_input_type": "actual_image",
                "image_quality_decision": "vlm_visual_accept",
            }],
            [{
                "post_url": url,
                "publication_status": "text_restore_pending",
                "publication_candidate_status": "awaiting_text_restore",
                "publication_eligibility_verdict": "eligible",
                "publication_eligibility_gate_version": mod.CURRENT_PUBLICATION_ELIGIBILITY_GATE_VERSION,
            }],
            [],
            [{
                "post_url": url,
                "vector_gate_status": "vector_reject_not_kaliningrad_oblast",
                "text_vector_fusion_status": "fused_e5_bge_m3",
                "kaliningrad_oblast_only_scope": False,
            }],
        )
        self.assertEqual(metrics["publication_text_restore_pending_raw_total"], 1)
        self.assertEqual(metrics["publication_text_restore_pending_total"], 0)
        self.assertEqual(metrics["publication_text_restore_tombstoned_total"], 1)
        self.assertEqual(metrics["publication_active_candidate_total"], 0)
        self.assertEqual(metrics["publication_text_restore_ready_for_finalizer_total"], 0)
        self.assertEqual(metrics["finalizer_pending_url_total"], 0)

    def test_current_vlm_accept_reopens_stale_visual_review_publication(self) -> None:
        mod = load_module()
        url = "https://t.me/travel/11"
        image = {
            "post_url": url,
            "image_queue_status": "actual_scored",
            "image_model_input_type": "actual_image",
            "image_quality_decision": "vlm_visual_accept",
            "image_vlm_status": "completed",
            "image_vlm_decision": "accept",
            "image_vlm_prompt_version": "region_talk_visual_adjudicator_v1",
            "image_vlm_decision_version": "region_talk_visual_decision_v1",
            "image_vlm_request_fingerprint": "current-fingerprint",
            "image_vlm_media_manifest_hash": "album-hash",
            "input_media_manifest_hash": "album-hash",
            "expected_image_count": 2,
            "fetched_image_count": 2,
            "image_component_bundle_complete": "true",
            "publication_eligibility_decision": "accept",
            "publication_eligibility_gate_version": mod.CURRENT_PUBLICATION_ELIGIBILITY_GATE_VERSION,
            "vector_gate_status": "vector_accept_candidate",
            "text_vector_fusion_status": "fused_e5_bge_m3",
            "kaliningrad_oblast_only_scope": "true",
            "kaliningrad_mention_role": "main_subject",
            "image_acquisition_status": "complete",
        }
        with mock.patch.object(mod, "_image_vlm_verdict_is_current", return_value=True):
            metrics = mod._publication_handoff_metrics(
                [image],
                [{
                    "post_url": url,
                    "publication_status": "needs_visual_review",
                    "publication_candidate_status": "visual_review_pending",
                    "publication_eligibility_verdict": "review",
                    "publication_eligibility_gate_version": mod.CURRENT_PUBLICATION_ELIGIBILITY_GATE_VERSION,
                }],
                [],
                [{
                    "post_url": url,
                    "vector_gate_status": "vector_accept_candidate",
                    "text_vector_fusion_status": "fused_e5_bge_m3",
                    "kaliningrad_oblast_only_scope": True,
                    "is_ad_or_promo": False,
                    "is_multi_region_roundup": False,
                }],
            )
        self.assertEqual(metrics["publication_visual_review_pending_total"], 1)
        self.assertEqual(metrics["publication_visual_review_resolved_ready_for_finalizer_total"], 1)
        self.assertEqual(metrics["publication_visual_review_resolved_ready_for_finalizer_urls"], [url])
        self.assertEqual(metrics["finalizer_pending_urls"], [url])

        rejected = mod._publication_handoff_metrics(
            [image],
            [{
                "post_url": url,
                "publication_status": "needs_visual_review",
                "publication_candidate_status": "visual_review_pending",
            }],
            [],
            [{
                "post_url": url,
                "vector_gate_status": "vector_reject_multi_region_roundup",
                "text_vector_fusion_status": "fused_e5_bge_m3",
                "kaliningrad_oblast_only_scope": False,
            }],
        )
        self.assertEqual(rejected["publication_visual_review_resolved_ready_for_finalizer_total"], 0)
        self.assertEqual(rejected["finalizer_pending_url_total"], 0)

    def test_image_review_metrics_separate_active_work_from_historical_ledger(self) -> None:
        mod = load_module()
        images = [
            {
                "post_url": "https://t.me/active/1",
                "image_quality_decision": "needs_visual_review",
                "image_acquisition_status": "partial",
            },
            {
                "post_url": "https://t.me/rejected/2",
                "image_quality_decision": "needs_visual_review",
                "image_acquisition_status": "partial",
            },
            {
                "post_url": "https://t.me/sent/3",
                "image_quality_decision": "needs_visual_review",
            },
        ]
        publications = [
            {
                "post_url": "https://t.me/active/1",
                "publication_candidate_status": "visual_review_pending",
                "llm_decision": "accept",
            },
            {
                "post_url": "https://t.me/rejected/2",
                "publication_candidate_status": "llm_rejected",
                "llm_decision": "reject",
            },
            {
                "post_url": "https://t.me/sent/3",
                "publication_candidate_status": "visual_review_pending",
                "sent_to_chat": "true",
                "llm_decision": "accept",
            },
            {
                "post_url": "https://t.me/restore/4",
                "publication_candidate_status": "awaiting_text_restore",
                "llm_decision": "reject",
            },
        ]
        metrics = mod._image_review_lifecycle_metrics(images, publications)
        self.assertEqual(metrics["image_visual_review_raw_urls_total"], 3)
        self.assertEqual(metrics["image_visual_review_active_total"], 2)
        self.assertEqual(metrics["image_visual_review_tombstoned_total"], 1)
        self.assertEqual(metrics["image_partial_album_active_total"], 1)
        self.assertEqual(metrics["publication_lifecycle_contradiction_total"], 2)

    def test_vlm_backlog_requires_strict_dual_complete_album_and_current_gate(self) -> None:
        mod = load_module()
        self.assertEqual(mod.IMAGE_VLM_PROMPT_VERSION, "region_talk_visual_adjudicator_v2")
        self.assertEqual(mod.IMAGE_VLM_DECISION_VERSION, "region_talk_visual_decision_v2")
        row = {
            "image_quality_decision": "needs_visual_review",
            "image_quality_reason": "uncalibrated_legacy_low_score_requires_visual_review",
            "publication_eligibility_decision": "accept",
            "publication_eligibility_gate_version": mod.CURRENT_PUBLICATION_ELIGIBILITY_GATE_VERSION,
            "vector_gate_status": "vector_accept_candidate",
            "text_vector_fusion_status": "fused_e5_bge_m3",
            "image_model_input_type": "actual_image",
            "image_acquisition_status": "complete",
            "expected_image_count": 4,
            "fetched_image_count": 4,
            "image_component_bundle_complete": "true",
            "input_media_manifest_hash": "album-hash",
            "overall_media_score": 0.60,
            "postcardness_score": 0.90,
        }
        self.assertTrue(mod._image_vlm_backlog_candidate(row))
        self.assertFalse(mod._image_vlm_backlog_candidate({**row, "text_vector_fusion_status": "missing_bge"}))
        self.assertFalse(mod._image_vlm_backlog_candidate({**row, "fetched_image_count": 3}))
        self.assertFalse(mod._image_vlm_backlog_candidate({**row, "overall_media_score": 0.4, "postcardness_score": 0.4}))
        completed = {
            **row,
            "image_vlm_status": "completed",
            "image_vlm_decision": "reject",
            "image_vlm_prompt_version": mod.IMAGE_VLM_PROMPT_VERSION,
            "image_vlm_decision_version": mod.IMAGE_VLM_DECISION_VERSION,
            "image_vlm_request_fingerprint": "fingerprint",
            "image_vlm_media_manifest_hash": "album-hash",
        }
        self.assertTrue(mod._image_vlm_verdict_is_current(completed))
        self.assertFalse(mod._image_vlm_backlog_candidate(completed))

    def test_vlm_backlog_launches_image_worker_with_two_call_cap(self) -> None:
        mod = load_module()
        actions = mod.build_decision_plan(
            {
                "publication_sent_total": 0,
                "publication_confirmed_total": 0,
                "image_vlm_backlog_total": 3,
                "image_actionable_work_total": 3,
            },
            target_confirmed=20,
            bge_threshold=1,
            image_threshold=1,
        )
        image_action = next(action for action in actions if action["action"] == "launch_image_diagnostic")
        self.assertEqual(image_action["env"]["REGION_TALK_IMAGE_VLM_ENABLED"], "1")
        self.assertEqual(image_action["env"]["REGION_TALK_IMAGE_VLM_MAX_CALLS_PER_RUN"], "2")

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

    def test_stale_only_bge_worker_sample_does_not_launch_product_bge(self) -> None:
        mod = load_module()
        metrics = {
            "text_vector_current_version_e5_without_bge_total": 268,
            "text_vector_current_version_e5_without_bge_actionable_total": 0,
            "bge_pending_sample_total": 100,
            "bge_missing_current_sample_total": 0,
            "bge_existing_stale_rescore_sample_total": 711,
        }
        actions = mod.build_decision_plan(metrics, target_confirmed=20, bge_threshold=1, image_threshold=1)
        self.assertNotIn("launch_bge_m3", [action["action"] for action in actions])
        self.assertEqual(
            mod.orchestrator_poll_sleep_seconds(metrics, normal_seconds=180, downstream_seconds=60),
            180,
        )

        metrics["bge_missing_current_sample_total"] = 1
        actions = mod.build_decision_plan(metrics, target_confirmed=20, bge_threshold=1, image_threshold=1)
        self.assertIn("launch_bge_m3", [action["action"] for action in actions])
        self.assertEqual(
            mod.orchestrator_poll_sleep_seconds(metrics, normal_seconds=180, downstream_seconds=60),
            60,
        )

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
            {"image_queue_status": "deferred_text_gate"},
        ])
        self.assertEqual(metrics["image_not_reviewable_no_media_total"], 1)
        self.assertEqual(metrics["image_not_reviewable_unsupported_media_total"], 1)
        self.assertEqual(metrics["image_rejected_text_gate_total"], 1)
        self.assertEqual(metrics["image_deferred_text_gate_total"], 1)
        self.assertEqual(metrics["image_ledger_terminal_rows_total"], 4)

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
        self.assertEqual(metrics["keyword_sources_with_preliminary_candidates_total"], 0)
        self.assertEqual(metrics["keyword_sources_with_confirmed_ko_posts_total"], 1)
        self.assertEqual(metrics["keyword_external_sources_with_confirmed_ko_posts_total"], 1)

    def test_fast_check_exact_metrics_separate_keyword_match_from_strict_accept(self) -> None:
        mod = load_module()
        sources = [
            {"fast_check_status": "ko_hit", "fast_check_hit_post_url": "https://t.me/a/1", "source_queue_status": "processed_found_ko_candidate"},
            {"fast_check_status": "ko_hit", "fast_check_hit_post_url": "https://t.me/b/2", "source_queue_status": "processed_found_ko_candidate"},
        ]
        processed = [
            {"post_url": "https://t.me/a/1", "updated_at": "2", "current_stage": "needs_image_review", "fresh_enough": True, "kaliningrad_oblast_only_scope": True, "vector_gate_status": "vector_accept_candidate"},
            {"post_url": "https://t.me/b/2", "updated_at": "2", "fresh_enough": True, "kaliningrad_oblast_only_scope": False, "vector_gate_status": "vector_reject_multi_region_roundup"},
        ]
        candidates = [{"post_url": "https://t.me/a/1", "updated_at": "3", "kaliningrad_oblast_only_scope": True, "vector_gate_status": "vector_accept_candidate", "text_vector_fusion_status": "fused_e5_bge_m3"}]
        images = [{"post_url": "https://t.me/a/1", "media_fetch_error": "telegram media is not an image: .mp4"}]
        vectors = [
            {"post_url": "https://t.me/a/1", "model_short": "e5"}, {"post_url": "https://t.me/a/1", "model_short": "bge_m3"},
            {"post_url": "https://t.me/b/2", "model_short": "e5"}, {"post_url": "https://t.me/b/2", "model_short": "bge_m3"},
        ]
        metrics = mod._fast_check_exact_post_metrics(sources, processed, candidates, images, [], vectors)
        self.assertEqual(metrics["fast_check_keyword_match_sources_total"], 2)
        self.assertEqual(metrics["fast_check_exact_posts_processed_unique_total"], 2)
        self.assertEqual(metrics["fast_check_exact_posts_dual_vectorized_total"], 2)
        self.assertEqual(metrics["fast_check_exact_posts_dual_semantic_accept_total"], 1)
        self.assertEqual(metrics["fast_check_exact_posts_strict_text_accepted_total"], 1)
        self.assertEqual(metrics["fast_check_exact_posts_video_manual_review_total"], 1)
        self.assertEqual(metrics["fast_check_exact_posts_text_rejection_reasons"], {"vector_reject_multi_region_roundup": 1})
        self.assertEqual(metrics["fast_check_exact_posts_text_pending_total"], 0)

    def test_latest_fast_check_rows_use_dedicated_run_id_not_generic_overlay(self) -> None:
        mod = load_module()
        rows = [
            {
                "canonical_source_key": "telegram:actually-checked",
                "fast_check_status": "no_hit_partial",
                "last_fast_check_run_id": "candidate-run-2",
                "run_id": "candidate-run-2",
            },
            {
                "canonical_source_key": "telegram:old-hit-touched-by-history",
                "fast_check_status": "ko_hit",
                "last_fast_check_run_id": "candidate-run-1",
                "run_id": "candidate-run-2",
                "last_seen_run_id": "candidate-run-2",
            },
            {
                "canonical_source_key": "telegram:old-no-hit-touched-by-keyword",
                "fast_check_status": "no_hit",
                "last_fast_check_run_id": "candidate-run-1",
                "last_seen_run_id": "candidate-run-2",
            },
        ]

        latest = mod._latest_fast_check_rows(rows, "candidate-run-2")

        self.assertEqual(
            [row["canonical_source_key"] for row in latest],
            ["telegram:actually-checked"],
        )

    def test_fast_check_exact_metrics_count_image_fetch_pending_as_text_passed(self) -> None:
        mod = load_module()
        sources = [
            {
                "fast_check_status": "ko_hit",
                "fast_check_hit_post_url": "https://t.me/krasivoorussia/6168",
                "source_queue_status": "processed_found_ko_candidate",
            }
        ]
        processed = [
            {
                "post_url": "https://t.me/krasivoorussia/6168",
                "updated_at": "2",
                "current_stage": "image_fetch_retry_needed",
                "drop_gate": "image_fetch_gate",
                "rejection_reason": "needs_actual_image_fetch",
                "fresh_enough": True,
                "kaliningrad_oblast_only_scope": True,
                "vector_gate_status": "vector_accept_candidate",
                "text_vector_fusion_status": "fused_e5_bge_m3",
            }
        ]
        vectors = [
            {"post_url": "https://t.me/krasivoorussia/6168", "model_short": "e5"},
            {"post_url": "https://t.me/krasivoorussia/6168", "model_short": "bge_m3"},
        ]

        metrics = mod._fast_check_exact_post_metrics(sources, processed, [], [], [], vectors)

        self.assertEqual(metrics["fast_check_exact_posts_dual_semantic_accept_total"], 1)
        self.assertEqual(metrics["fast_check_exact_posts_strict_text_accepted_total"], 1)
        self.assertEqual(metrics["fast_check_exact_posts_text_rejected_total"], 0)
        self.assertEqual(metrics["fast_check_exact_posts_text_pending_total"], 0)

    def test_fast_check_exact_metrics_do_not_let_stale_candidate_override_current_defer(self) -> None:
        mod = load_module()
        sources = [
            {
                "fast_check_status": "ko_hit",
                "fast_check_hit_post_url": "https://t.me/uniteclub/15347",
                "source_queue_status": "processed_found_ko_candidate",
            }
        ]
        processed = [
            {
                "post_url": "https://t.me/uniteclub/15347",
                "updated_at": "3",
                "current_stage": "dual_model_vector_enrichment_pending",
                "drop_gate": "external_bge_m3_enrichment_gate",
                "fresh_enough": True,
                "kaliningrad_oblast_only_scope": True,
                "vector_gate_status": "vector_defer_wait_bge_m3",
                "text_vector_fusion_status": "missing_bge_m3_enrichment",
            }
        ]
        candidates = [
            {
                "post_url": "https://t.me/uniteclub/15347",
                "updated_at": "2",
                "current_stage": "image_fetch_retry_needed",
                "vector_gate_status": "vector_accept_candidate",
                "text_vector_fusion_status": "fused_e5_bge_m3",
            }
        ]
        vectors = [
            {"post_url": "https://t.me/uniteclub/15347", "model_short": "e5"},
            {"post_url": "https://t.me/uniteclub/15347", "model_short": "bge_m3"},
        ]

        metrics = mod._fast_check_exact_post_metrics(sources, processed, candidates, [], [], vectors)

        self.assertEqual(metrics["fast_check_exact_posts_dual_semantic_accept_total"], 0)
        self.assertEqual(metrics["fast_check_exact_posts_strict_text_accepted_total"], 0)
        self.assertEqual(metrics["fast_check_exact_posts_text_rejected_total"], 0)
        self.assertEqual(metrics["fast_check_exact_posts_text_pending_reasons"], {"dual_vector_pending": 1})

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
        self.assertEqual(main["env"]["REGION_TALK_POST_LINK_QUEUE_FETCH_LIMIT"], "5")
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

    def test_active_image_kernel_blocks_discovery2_notifier(self) -> None:
        mod = load_module()
        actions = mod.build_decision_plan(
            {
                "publication_unsent_confirmed_total": 1,
                "image_pending_total": 1,
            },
            target_confirmed=20,
            bge_threshold=1,
            image_threshold=1,
        )
        kept, skipped = mod.filter_actions_for_active_kernels(
            actions,
            {"region-talk-image-diagnostic": "RUNNING", "region-talk-candidate-report": "COMPLETE"},
        )
        self.assertNotIn("notify_confirmed", [action["action"] for action in kept])
        self.assertIn(
            {
                "action": "notify_confirmed",
                "resource": "telegram:DISCOVERY2",
                "reason": "telegram_auth_bundle_in_use_by_active_kernel",
            },
            skipped,
        )

    def test_selector_never_runs_notifier_with_same_bundle_as_candidate(self) -> None:
        mod = load_module()
        actions = [
            {"action": "launch_candidate_report", "resource": "telegram:DISCOVERY1", "parallel_safe": True},
            {"action": "notify_confirmed", "resource": "telegram:DISCOVERY1", "parallel_safe": False},
        ]
        selected = mod.select_actions_for_execution(actions, execute_ready=True, max_actions=4)
        self.assertEqual([action["action"] for action in selected], ["launch_candidate_report"])

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
        action = {"action": "notify_confirmed", "cmd": ["python3", "scripts/region_talk_goal_notify.py", "--limit", "20", "--transport", "bot_api"], "run_id": "ignored"}
        cmd, _ = mod.prepare_action_command(action, env_file="/tmp/region.env")
        self.assertIn("--env-file", cmd)
        self.assertNotIn("--run-id", cmd)

    def test_prepare_action_command_omits_empty_default_env_file(self) -> None:
        mod = load_module()
        action = {
            "action": "launch_candidate_report",
            "cmd": ["python3", "kaggle/execute_region_talk_candidate_report.py", "--no-wait"],
        }
        cmd, _ = mod.prepare_action_command(action, env_file="")
        self.assertNotIn("--env-file", cmd)
        self.assertIn("--run-id", cmd)

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

    def test_loop_cycle_retries_transient_ydb_endpoint_failure(self) -> None:
        mod = load_module()
        args = argparse.Namespace()
        success = {"ok": True, "cycle": 4, "metrics": {}, "actions": []}
        sleeps: list[float] = []
        with mock.patch.object(
            mod,
            "run_orchestrator_cycle",
            side_effect=[
                RuntimeError("ConnectionFailure: Failed to resolve endpoints; Deadline exceeded on request"),
                success,
            ],
        ) as run_cycle:
            result = mod.run_orchestrator_cycle_with_retries(
                args,
                allow_yc_fallback=True,
                cycle_index=4,
                retry_limit=3,
                backoff_seconds=2.0,
                sleep_fn=sleeps.append,
            )
        self.assertEqual(run_cycle.call_count, 2)
        self.assertEqual(sleeps, [2.0])
        self.assertEqual(result["cycle_transient_retries"][0]["attempt"], 1)
        self.assertIn("Failed to resolve endpoints", result["cycle_transient_retries"][0]["error"])

    def test_loop_cycle_does_not_retry_configuration_error(self) -> None:
        mod = load_module()
        args = argparse.Namespace()
        sleeps: list[float] = []
        with mock.patch.object(
            mod,
            "run_orchestrator_cycle",
            side_effect=RuntimeError("missing_ydb_config"),
        ) as run_cycle:
            with self.assertRaisesRegex(RuntimeError, "missing_ydb_config"):
                mod.run_orchestrator_cycle_with_retries(
                    args,
                    allow_yc_fallback=False,
                    cycle_index=1,
                    retry_limit=3,
                    backoff_seconds=2.0,
                    sleep_fn=sleeps.append,
                )
        run_cycle.assert_called_once()
        self.assertEqual(sleeps, [])

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

    def test_main_rejects_explicit_missing_env_file_before_live_reads(self) -> None:
        mod = load_module()
        missing = "/tmp/region-talk-definitely-missing.env"
        with mock.patch.dict(os.environ, {}, clear=True), \
            mock.patch.object(mod, "load_env") as load_env, \
            mock.patch.object(mod, "read_region_talk_queue_metrics") as read_metrics, \
            mock.patch.object(sys, "argv", ["region_talk_orchestrator.py", "--env-file", missing, "--execute"]):
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = mod.main()
        self.assertEqual(rc, 2)
        self.assertIn('"error": "missing_env_file"', out.getvalue())
        self.assertIn(missing, out.getvalue())
        load_env.assert_not_called()
        read_metrics.assert_not_called()

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
            "publics_scanned_with_posts_total": 662,
            "publics_with_ko_candidates_total": 91,
            "confirmed_external_blogger_sources_total": 53,
            "confirmed_external_blogger_scanned_total": 8,
            "confirmed_external_blogger_with_ko_total": 5,
            "confirmed_external_blogger_fast_check_hit_total": 3,
            "confirmed_external_blogger_vk_search_checked_total": 7,
            "confirmed_external_blogger_vk_search_hit_total": 4,
            "confirmed_external_blogger_pending_total": 45,
            "confirmed_external_blogger_rejected_local_total": 0,
            "confirmed_external_blogger_rejected_spam_total": 0,
            "confirmed_external_blogger_posts_processed_total": 12,
            "confirmed_external_blogger_vector_accepted_posts_total": 5,
            "confirmed_external_blogger_image_queue_posts_total": 3,
            "confirmed_external_blogger_publication_posts_total": 2,
            "confirmed_external_blogger_delivery_completed_posts_total": 1,
            "confirmed_external_blogger_sources_with_processed_posts_total": 8,
            "confirmed_external_blogger_sources_with_vector_accepted_posts_total": 3,
            "confirmed_external_blogger_sources_with_image_queue_posts_total": 2,
            "confirmed_external_blogger_sources_with_publication_posts_total": 2,
            "confirmed_external_blogger_sources_with_publication_confirmed_posts_total": 1,
            "confirmed_external_blogger_sources_with_delivery_completed_posts_total": 1,
            "post_link_queue_exact_ready_total": 67,
            "post_link_queue_fetched_total": 0,
            "processed_posts_unique_total": 10756,
            "text_vector_current_version_dual_coverage_percent": 87,
            "text_vector_current_version_e5_without_bge_total": 290,
            "publication_candidate_total": 19,
            "publication_confirmed_total": 7,
            "publication_sent_total": 7,
            "publication_delivery_completed_total": 6,
        })
        self.assertIn("всего / хотя бы раз реально просмотрены / ещё ни разу не просмотрены: 7055/662/6393", text)
        self.assertIn("Технический backlog", text)
        self.assertIn("Источники, где уже найден хотя бы один возможный пост о КО: 91", text)
        self.assertIn("Подтверждённые внешние блогеры", text)
        self.assertIn("53/8/5/3/7/4/45/0/0", text)
        self.assertIn("Конверсия подтверждённых блогеров по уникальным источникам", text)
        self.assertIn("8/3/2/2/1/1", text)
        self.assertIn("новые тексты к чтению / готовы к повторному решению после BGE", text)
        self.assertIn("67/0/0/0/0", text)
        self.assertIn("Публикационный отбор", text)
        self.assertIn("19/7/7/0/6", text)

    def test_strong_source_attestation_backlog_schedules_bounded_priority_pass(self) -> None:
        mod = load_module()
        actions = mod.build_decision_plan(
            {
                "publication_source_evidence_backlog_total": 1,
                "publication_confirmed_total": 0,
                "publication_sent_total": 0,
                "bge_pending_sample_total": 0,
                "image_pending_total": 0,
                "finalizer_pending_url_total": 0,
            },
            target_confirmed=20,
            bge_threshold=1,
            image_threshold=1,
        )
        by_name = {action["action"]: action for action in actions}
        self.assertIn("launch_candidate_report", by_name)
        self.assertIn("prioritize_source_evidence", by_name)
        self.assertIn("--prioritize-source-evidence-only", by_name["prioritize_source_evidence"]["cmd"])

    def test_candidate_profile_uses_lightweight_report_tail(self) -> None:
        mod = load_module()
        self.assertEqual(mod.MAIN_DISCOVERY_YDB_BUDGET_ENV["REGION_TALK_LIGHTWEIGHT_REPORT"], "1")
        self.assertEqual(mod.MAIN_DISCOVERY_YDB_BUDGET_ENV["REGION_TALK_ENABLE_POST_WORK_IDEMPOTENCY"], "1")

    def test_accepted_unsent_candidate_missing_onboarding_schedules_finalizer_without_reverification_hint(self) -> None:
        mod = load_module()
        actions = mod.build_decision_plan(
            {
                "publication_confirmed_total": 1,
                "publication_sent_total": 0,
                "publication_unsent_confirmed_total": 0,
                "publication_onboarding_pending_unsent_total": 1,
                "bge_pending_sample_total": 0,
                "image_pending_total": 0,
                "finalizer_pending_url_total": 0,
            },
            target_confirmed=20,
            bge_threshold=1,
            image_threshold=1,
        )
        finalizer = next(action for action in actions if action["action"] == "run_finalizer")
        self.assertIn("1 accepted unsent rows need source onboarding", finalizer["reason"])
        self.assertNotIn("--reverify-existing", finalizer["cmd"])

    def test_vk_image_without_direct_url_schedules_server_prefetch(self) -> None:
        mod = load_module()
        actions = mod.build_decision_plan(
            {
                "image_pending_vk_without_url_total": 1,
                "image_pending_total": 1,
                "publication_confirmed_total": 0,
                "publication_sent_total": 0,
                "bge_pending_sample_total": 0,
                "finalizer_pending_url_total": 0,
            },
            target_confirmed=20,
            bge_threshold=1,
            image_threshold=1,
        )
        by_name = {action["action"]: action for action in actions}
        self.assertIn("prefetch_vk_media", by_name)
        self.assertIn("--allow-fly-fallback", by_name["prefetch_vk_media"]["cmd"])
        self.assertIn("launch_image_diagnostic", by_name)


if __name__ == "__main__":
    unittest.main()
