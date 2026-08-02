from __future__ import annotations

import importlib.util
import json
import os
import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "region_talk_publication_finalizer.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_publication_finalizer", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def external_source(handle: str = "travelcase") -> dict[str, str]:
    return {
        "canonical_source_key": f"telegram:{handle}",
        "platform": "telegram",
        "handle": handle,
        "source_url": f"https://t.me/{handle}",
        "source_title": "Travel notes",
        "source_scope": "external",
        "source_geo_class": "nonlocal_russia",
        "source_topic_class": "travel_blogger",
    }


def candidate_row(url: str = "https://t.me/travelcase/10", **overrides):
    row = {
        "post_url": url,
        "source_title": "Travel notes",
        "source_url": "https://t.me/travelcase",
        "canonical_source_key": "telegram:travelcase",
        "source_class_guess": "nonlocal_travel_or_general_source",
        "_authoritative_source": external_source(),
        "_previous_publication": {},
        "finalization_trigger": "never_finalized",
        "text": "Личный рассказ о поездке в Калининградскую область.",
        "attempt_count": 0,
    }
    row.update(overrides)
    return row


def ready_social_capture(fingerprint: str = "stable-capture") -> dict[str, object]:
    topics = ("route", "history", "practical", "architecture", "nature", "culture", "food", "city_life")
    return {
        "capture_status": "ready",
        "capture_version": "region_talk_source_profile_capture.v1",
        "capture_fingerprint": fingerprint,
        "scanned_count": 50,
        "authored_count": 24,
        "selected_count": 8,
        "description_evidence": {"status": "available", "text": "Авторский канал о самостоятельных поездках."},
        "pinned_evidence": {
            "status": "available",
            "text": "Здесь собраны маршруты, полевые заметки и практические советы.",
            "post_url": "https://t.me/travelcase/1",
        },
        "representative_excerpts": [
            {
                "excerpt": f"Разнообразный авторский фрагмент {index} о поездке и наблюдениях.",
                "post_url": f"https://t.me/travelcase/{10 + index}",
                "published_at": f"2026-07-{index:02d}",
                "topic": topic,
            }
            for index, topic in enumerate(topics, 1)
        ],
    }


def eligibility(verdict: str = "eligible") -> dict[str, object]:
    return {
        "verdict": verdict,
        "evidence": {"source_policy": verdict, "authoritative": True},
        "gate_version": "publication-source-gate-v3",
    }


class RegionTalkPublicationFinalizerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.mod = load_module()

    def test_gemini_fingerprint_changes_with_final_verifier_prompt_version(self) -> None:
        row = {
            "post_url": "https://t.me/travel/10",
            "text": "Личное впечатление от поездки в Калининград",
            "authoritative_source_fingerprint": "source-v2-value",
            "publication_eligibility_gate_version": "region_talk_publication_eligibility_v1",
        }
        first = self.mod.gemini_request_fingerprint(row, model="fake")
        old = self.mod.rt.REGION_TALK_FINAL_VERIFIER_PROMPT_VERSION
        try:
            self.mod.rt.REGION_TALK_FINAL_VERIFIER_PROMPT_VERSION = "changed-prompt-version"
            second = self.mod.gemini_request_fingerprint(row, model="fake")
        finally:
            self.mod.rt.REGION_TALK_FINAL_VERIFIER_PROMPT_VERSION = old
        self.assertNotEqual(first, second)

    def test_gemini_fingerprint_ignores_source_counter_growth(self) -> None:
        source = external_source()
        source.update({"posts_scanned": 5, "ko_posts_found": 1, "candidate_posts_found": 1})
        row = candidate_row(_authoritative_source=source)
        first = self.mod.gemini_request_fingerprint(row, model="fake")
        source.update({"posts_scanned": 25, "ko_posts_found": 4, "candidate_posts_found": 3})
        second = self.mod.gemini_request_fingerprint(row, model="fake")
        self.assertEqual(first, second)
        source["source_scope"] = "local_region"
        self.assertNotEqual(second, self.mod.gemini_request_fingerprint(row, model="fake"))

    def test_only_terminal_provider_results_are_replayed(self) -> None:
        self.assertTrue(self.mod._completed_llm_result_is_replayable({
            "llm_gate_status": "ok", "llm_decision": "reject",
        }))
        self.assertFalse(self.mod._completed_llm_result_is_replayable({
            "llm_gate_status": "error", "llm_reason": "local ImportError",
        }))
        self.assertFalse(self.mod._completed_llm_result_is_replayable({
            "llm_gate_status": "rate_limited", "llm_reason": "429",
        }))
        self.assertTrue(self.mod._completed_llm_result_is_replayable({
            "vlm_gate_status": "ok", "vlm_decision": "accept",
        }))

    def test_provider_preflight_fails_before_budget_reservation(self) -> None:
        with mock.patch.object(self.mod.importlib.util, "find_spec", return_value=None):
            with self.assertRaisesRegex(RuntimeError, "official google-genai runtime"):
                self.mod.require_google_genai_runtime()

    def test_source_class_guess_uses_region_talk_local_source_filter(self) -> None:
        self.assertEqual(
            self.mod.source_class_guess("Дом китобоя", "https://t.me/domkitoboya", {}),
            "local_region_source",
        )

    def test_newer_external_image_scope_overrides_stale_candidate_memory(self) -> None:
        memory = {
            "content_origin_type": "editorial_publication",
            "updated_at": "2026-07-19T23:10:00+00:00",
            "kaliningrad_oblast_only_scope": False,
            "kaliningrad_mention_role": "unclear",
            "vector_gate_status": "vector_accept_candidate",
        }
        image = {
            "content_origin_type": "editorial_publication",
            "updated_at": "2026-07-19T23:53:00+00:00",
            "kaliningrad_oblast_only_scope": True,
            "kaliningrad_mention_role": "main_subject",
            "vector_gate_status": "vector_accept_candidate",
        }

        merged = self.mod.merge_image_and_memory_for_finalizer(image, memory)
        self.assertTrue(merged["kaliningrad_oblast_only_scope"])
        self.assertEqual(merged["kaliningrad_mention_role"], "main_subject")

        memory["updated_at"] = "2026-07-20T00:00:00+00:00"
        newer_memory = self.mod.merge_image_and_memory_for_finalizer(image, memory)
        self.assertFalse(newer_memory["kaliningrad_oblast_only_scope"])

    def test_ineligible_tombstone_is_current_only_with_current_source_fingerprint(self) -> None:
        row = {
            "publication_eligibility_verdict": "review",
            "publication_eligibility_gate_version": "gate-v1",
            "publication_eligibility_evidence": "same-evidence",
            "authoritative_source_fingerprint": "source-v2-value",
            "authoritative_source_fingerprint_version": self.mod.AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION,
            "_previous_publication": {
                "publication_status": "eligibility_review_tombstone",
                "publication_candidate_status": "tombstoned_review",
                "publication_eligibility_verdict": "review",
                "publication_eligibility_gate_version": "gate-v1",
                "publication_eligibility_evidence": "same-evidence",
                "authoritative_source_fingerprint": "source-v2-value",
                "authoritative_source_fingerprint_version": self.mod.AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION,
            },
        }
        self.assertTrue(self.mod._ineligible_state_is_current(row, "review"))
        row["_previous_publication"]["authoritative_source_fingerprint_version"] = "region_talk_source_fingerprint_v1"
        self.assertFalse(self.mod._ineligible_state_is_current(row, "review"))
        self.assertEqual(
            self.mod.source_class_guess("Travel notes", "https://t.me/example_travel", {}),
            "nonlocal_travel_or_general_source",
        )

    def test_ineligible_tombstone_accepts_legacy_truncated_evidence_prefix(self) -> None:
        full_evidence = "e" * 700 + "changed-only-after-storage-cap"
        row = {
            "publication_eligibility_verdict": "reject",
            "publication_eligibility_gate_version": "gate-v1",
            "publication_eligibility_evidence": full_evidence,
            "publication_eligibility_evidence_fingerprint": self.mod.publication_eligibility_evidence_fingerprint(full_evidence),
            "authoritative_source_fingerprint": "source-v2-value",
            "authoritative_source_fingerprint_version": self.mod.AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION,
            "_previous_publication": {
                "publication_status": "eligibility_reject_tombstone",
                "publication_candidate_status": "tombstoned_reject",
                "publication_eligibility_verdict": "reject",
                "publication_eligibility_gate_version": "gate-v1",
                "publication_eligibility_evidence": full_evidence[:700],
                "authoritative_source_fingerprint": "source-v2-value",
                "authoritative_source_fingerprint_version": self.mod.AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION,
            },
        }
        self.assertTrue(self.mod._ineligible_state_is_current(row, "reject"))

    def test_ineligible_tombstone_fingerprint_detects_change_beyond_stored_prefix(self) -> None:
        old_full_evidence = "e" * 700 + "old-tail"
        new_full_evidence = "e" * 700 + "new-tail"
        row = {
            "publication_eligibility_verdict": "reject",
            "publication_eligibility_gate_version": "gate-v1",
            "publication_eligibility_evidence": new_full_evidence,
            "publication_eligibility_evidence_fingerprint": self.mod.publication_eligibility_evidence_fingerprint(new_full_evidence),
            "authoritative_source_fingerprint": "source-v2-value",
            "authoritative_source_fingerprint_version": self.mod.AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION,
            "_previous_publication": {
                "publication_status": "eligibility_reject_tombstone",
                "publication_candidate_status": "tombstoned_reject",
                "publication_eligibility_verdict": "reject",
                "publication_eligibility_gate_version": "gate-v1",
                "publication_eligibility_evidence": old_full_evidence[:700],
                "publication_eligibility_evidence_fingerprint": self.mod.publication_eligibility_evidence_fingerprint(old_full_evidence),
                "authoritative_source_fingerprint": "source-v2-value",
                "authoritative_source_fingerprint_version": self.mod.AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION,
            },
        }
        self.assertFalse(self.mod._ineligible_state_is_current(row, "reject"))

    def test_changed_eligibility_tombstone_reopens_without_reopening_operator_reject(self) -> None:
        mod = self.mod
        previous = candidate_row(
            publication_status="eligibility_reject_tombstone",
            publication_candidate_status="tombstoned_reject",
            publication_eligibility_verdict="reject",
            publication_tombstone="true",
        )
        row = candidate_row(
            finalization_trigger="never_finalized",
            _previous_publication=previous,
        )
        with (
            mock.patch.object(mod, "_eligibility_fields", return_value=("eligible", {})),
            mock.patch.object(mod.rt, "call_region_talk_semantic_llm", return_value={
                "llm_gate_status": "ok",
                "llm_decision": "accept",
                "llm_reason": "verified",
            }) as llm,
        ):
            result = mod.verify_rows(
                [row],
                max_llm=1,
                model="gemini-test",
                default_env_var_name="KEY",
                now_iso="2026-07-31T19:40:00+00:00",
            )
        self.assertEqual(result[0]["publication_status"], "gemini_accept")
        llm.assert_called_once()

        operator_previous = {
            **previous,
            "publication_status": "operator_rejected",
        }
        operator_row = candidate_row(
            finalization_trigger="never_finalized",
            _previous_publication=operator_previous,
        )
        with (
            mock.patch.object(mod, "_eligibility_fields", return_value=("eligible", {})),
            mock.patch.object(mod.rt, "call_region_talk_semantic_llm") as operator_llm,
        ):
            operator_result = mod.verify_rows(
                [operator_row],
                max_llm=1,
                model="gemini-test",
                default_env_var_name="KEY",
                now_iso="2026-07-31T19:40:00+00:00",
            )
        self.assertEqual(operator_result[0]["publication_status"], "operator_rejected")
        operator_llm.assert_not_called()

    def test_visual_review_state_is_nonterminal_and_idempotent(self) -> None:
        mod = self.mod
        row = candidate_row()
        with (
            mock.patch.object(mod.rt, "publication_eligibility", return_value=eligibility("needs_visual_review")),
            mock.patch.object(mod.rt, "call_region_talk_semantic_llm") as llm,
        ):
            result = mod.verify_rows(
                [row],
                max_llm=10,
                model="gemini-test",
                default_env_var_name="KEY",
                now_iso="2026-07-14T18:00:00+00:00",
            )
        llm.assert_not_called()
        self.assertEqual(len(result), 1)
        self.assertEqual(row["publication_status"], "needs_visual_review")
        self.assertEqual(row["publication_candidate_status"], "visual_review_pending")
        self.assertEqual(row["publication_tombstone"], "false")
        self.assertEqual(row["finalization_status"], "review_pending")

        previous = dict(row)
        repeated = candidate_row(_previous_publication=previous)
        with mock.patch.object(mod.rt, "publication_eligibility", return_value=eligibility("needs_visual_review")):
            self.assertEqual(
                mod.verify_rows(
                    [repeated],
                    max_llm=10,
                    model="gemini-test",
                    default_env_var_name="KEY",
                    now_iso="2026-07-14T19:00:00+00:00",
                ),
                [],
            )

    def test_external_visual_review_rewrites_missing_rights_projection_once(self) -> None:
        mod = self.mod
        row = candidate_row(
            content_origin_type="editorial_publication",
            external_publication_id="extpub-1",
            external_research_quality_score=0.857,
            rights_policy="link_only",
            media_use_policy="score_only_no_reuse",
            media_reuse_allowed=False,
            image_quality_decision="needs_visual_review",
            image_quality_reason="low_score_requires_review",
            kaliningrad_oblast_only_scope=True,
            kaliningrad_mention_role="main_subject",
            publication_eligibility_verdict="review",
            publication_eligibility_gate_version="gate-v1",
            publication_eligibility_evidence_fingerprint="evidence-v1",
            authoritative_source_fingerprint="source-v1",
            _previous_publication={
                "publication_status": "needs_visual_review",
                "publication_eligibility_verdict": "review",
                "publication_eligibility_gate_version": "gate-v1",
                "publication_eligibility_evidence_fingerprint": "evidence-v1",
                "authoritative_source_fingerprint": "source-v1",
            },
        )
        self.assertFalse(mod._review_state_is_current(row))
        row["_previous_publication"].update({
            field: row.get(field)
            for field in (
                "content_origin_type", "external_publication_id", "external_research_quality_score",
                "rights_policy", "media_use_policy", "media_reuse_allowed",
                "image_quality_decision", "image_quality_reason",
                "kaliningrad_oblast_only_scope", "kaliningrad_mention_role",
            )
        })
        self.assertTrue(mod._review_state_is_current(row))

    def test_normalize_post_url_collapses_public_telegram_variants(self) -> None:
        variants = [
            "http://T.ME/TravelCase/10/",
            "https://telegram.me/travelcase/10?single=1#x",
            "https://t.me/s/TravelCase/10",
            "t.me/travelcase/10/",
        ]
        self.assertEqual(
            {self.mod.normalize_post_url(url) for url in variants},
            {"https://t.me/travelcase/10"},
        )

    def test_authoritative_source_index_and_live_rows_join_by_canonical_key(self) -> None:
        mod = self.mod
        source = external_source("travelcase")
        kinds = {
            "image_queue_item": {
                "image:10": {
                    "post_url": "https://telegram.me/TravelCase/10?single=1",
                    "source_url": "https://t.me/TravelCase/",
                    "source_title": "Travel notes",
                    "platform": "telegram",
                    "image_queue_status": "actual_scored",
                    "image_model_input_type": "actual_image",
                    "overall_media_score": 0.8,
                    "updated_at": "2026-07-10T08:00:00+00:00",
                },
                "image:11": {
                    "post_url": "https://t.me/travelcase/11",
                    "source_url": "https://t.me/travelcase",
                    "source_title": "Travel notes",
                    "platform": "telegram",
                    "image_queue_status": "actual_scored",
                    "image_model_input_type": "actual_image",
                    "overall_media_score": 0.7,
                    "updated_at": "2026-07-10T08:00:00+00:00",
                },
            },
            "candidate_memory_item": {
                "memory:10": {
                    "post_url": "https://t.me/s/travelcase/10/",
                    "text_excerpt": "Exact YDB text",
                }
            },
            "publication_candidate_item": {
                "publication:10": {
                    "post_url": "http://t.me/TRAVELCASE/10",
                    "publication_status": "gemini_accept",
                    "publication_candidate_status": "llm_confirmed",
                    "updated_at": "2026-07-10T07:00:00+00:00",
                }
            },
            "source_queue_item": {"source:travelcase": source},
            "source_status_item": {},
            "online_source_item": {},
        }

        class Pool:
            def retry_operation_sync(self, operation):
                return operation(object())

        pool = Pool()

        class Ydb:
            def SessionPool(self, _driver):
                return pool

        ydb = Ydb()
        with (
            mock.patch.object(mod.rt, "ydb_connect", return_value=(ydb, object(), object())),
            mock.patch.object(mod.rt, "ydb_kv_table_path", return_value="table"),
            mock.patch.object(mod.rt, "ydb_select_kind_items", side_effect=lambda _s, _y, _t, kind, limit, **_kwargs: kinds.get(kind, {})),
            mock.patch.object(mod.rt, "utc_now_iso", return_value="2026-07-10T09:00:00+00:00"),
        ):
            _ydb, _driver, _pool, _table, rows, _strict_priority = mod.read_live_rows(100, 100)

        by_url = {row["post_url"]: row for row in rows}
        finalized = by_url["https://t.me/travelcase/10"]
        never_finalized = by_url["https://t.me/travelcase/11"]
        self.assertEqual(finalized["_authoritative_source"], source)
        self.assertEqual(finalized["canonical_source_key"], "telegram:travelcase")
        self.assertEqual(finalized["text"], "Exact YDB text")
        self.assertEqual(finalized["finalization_trigger"], "")
        self.assertEqual(never_finalized["finalization_trigger"], "never_finalized")

    def test_read_live_rows_includes_video_for_manual_operator_review(self) -> None:
        mod = self.mod
        source = external_source("videotravel")
        kinds = {
            "image_queue_item": {
                "image:video": {
                    "post_url": "https://t.me/videotravel/7",
                    "source_url": "https://t.me/videotravel",
                    "source_title": "Video travel",
                    "platform": "telegram",
                    "image_queue_status": "not_reviewable_unsupported_media",
                    "image_model_input_type": "metadata_only",
                    "media_fetch_error": "telegram media is not an image: .mp4",
                    "updated_at": "2026-07-10T08:00:00+00:00",
                },
            },
            "candidate_memory_item": {
                "memory:video": {"post_url": "https://t.me/videotravel/7", "text_excerpt": "Поездка в Калининград"},
            },
            "publication_candidate_item": {},
            "source_queue_item": {"source:videotravel": source},
            "source_status_item": {},
            "online_source_item": {},
        }

        class Pool:
            def retry_operation_sync(self, operation):
                return operation(object())

        pool = Pool()

        class Ydb:
            def SessionPool(self, _driver):
                return pool

        with (
            mock.patch.object(mod.rt, "ydb_connect", return_value=(Ydb(), object(), object())),
            mock.patch.object(mod.rt, "ydb_kv_table_path", return_value="table"),
            mock.patch.object(mod.rt, "ydb_select_kind_items", side_effect=lambda _s, _y, _t, kind, limit, **_kwargs: kinds.get(kind, {})),
            mock.patch.object(mod.rt, "utc_now_iso", return_value="2026-07-10T09:00:00+00:00"),
        ):
            _ydb, _driver, _pool, _table, rows, _strict_priority = mod.read_live_rows(100, 100)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["media_kind"], "video")
        self.assertEqual(rows[0]["media_review_mode"], "operator_video_review")
        self.assertEqual(rows[0]["manual_media_review_required"], "true")

    def test_read_live_rows_uses_terminal_article_link_preview_fallback(self) -> None:
        mod = self.mod
        source = {
            "canonical_source_key": "web:publisher.example",
            "platform": "web",
            "source_url": "https://publisher.example",
            "source_title": "Publisher Example",
            "source_scope": "external",
            "source_geo_class": "nonlocal_russia",
            "source_topic_class": "editorial_publication",
            "source_quick_class": "candidate_keep",
            "source_queue_status": "confirmed_external_publication_research",
        }
        kinds = {
            "image_queue_item": {
                "image:article": {
                    "post_url": "https://publisher.example/kaliningrad",
                    "platform": "web",
                    "external_publication_id": "extpub-1",
                    "content_origin_type": "editorial_publication",
                    "canonical_source_key": "web:publisher.example",
                    "image_queue_status": "not_reviewable_no_media",
                    "browser_materialization_status": "terminal_no_associated_images",
                    "image_quality_terminality": "terminal",
                    "presentation_recommendation": "system_link_preview",
                }
            },
            "candidate_memory_item": {
                "memory:article": {
                    "post_url": "https://publisher.example/kaliningrad",
                    "platform": "web",
                    "external_publication_id": "extpub-1",
                    "content_origin_type": "editorial_publication",
                    "source_url": "https://publisher.example",
                    "source_title": "Publisher Example",
                    "canonical_source_key": "web:publisher.example",
                    "source_scope": "external",
                    "source_geo_class": "nonlocal_russia",
                    "source_topic_class": "editorial_publication",
                    "kaliningrad_oblast_only_scope": True,
                    "kaliningrad_mention_role": "main_subject",
                    "is_ad_or_promo": False,
                    "vector_gate_status": "vector_accept_candidate",
                    "vector_content_type": "editorial_publication_candidate",
                    "text_vector_fusion_status": "fused_e5_bge_m3",
                    "current_stage": "good_text_weak_media",
                    "rights_policy": "link_only",
                    "media_use_policy": "score_only_no_reuse",
                    "media_reuse_allowed": False,
                    "external_research_quality_score": 0.86,
                    "full_text": "Проверенная статья о Калининградской области.",
                }
            },
            "publication_candidate_item": {},
            "source_queue_item": {},
            "source_status_item": {},
            "online_source_item": {},
            "external_publication_source_item": {"source:publisher": source},
            "source_onboarding_profile_item": {},
        }

        class Pool:
            def retry_operation_sync(self, operation):
                return operation(object())

        class Ydb:
            def SessionPool(self, _driver):
                return Pool()

        self.assertFalse(
            mod.rt.is_external_link_article_candidate(kinds["candidate_memory_item"]["memory:article"])
        )
        with (
            mock.patch.object(mod.rt, "ydb_connect", return_value=(Ydb(), object(), object())),
            mock.patch.object(mod.rt, "ydb_kv_table_path", return_value="table"),
            mock.patch.object(mod.rt, "ydb_select_kind_items", side_effect=lambda _s, _y, _t, kind, limit, **_kwargs: kinds.get(kind, {})),
            mock.patch.object(mod.rt, "utc_now_iso", return_value="2026-07-31T18:00:00+00:00"),
        ):
            _ydb, _driver, _pool, _table, rows, _strict_priority = mod.read_live_rows(100, 100)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["post_url"], "https://publisher.example/kaliningrad")
        self.assertEqual(rows[0]["media_kind"], "external_article_link_preview")
        self.assertEqual(rows[0]["media_review_mode"], "system_link_preview_terminal_fallback")
        self.assertEqual(rows[0]["image_queue_status"], "not_reviewable_no_media")
        self.assertEqual(rows[0]["_authoritative_source"], source)
        self.assertGreater(rows[0]["publication_pre_score"], 0.8)

    def test_read_live_rows_projects_archived_external_publication_for_attestation_refresh(self) -> None:
        mod = self.mod
        post_url = "https://publisher.example/archived-article"
        intake = {
            "external_publication_id": "extpub-archived",
            "intake_status": "new_intake",
            "review_status": "unreviewed",
            "publication_permission": "none",
            "intake_revision": "legacy-attestation-v1",
            "legacy_provenance_attestation": {"status": "attested"},
        }
        publication = {
            "post_url": post_url,
            "external_publication_id": "extpub-archived",
            "content_origin_type": "editorial_publication",
            "canonical_source_key": "web:publisher.example",
            "publication_status": "gemini_accept",
            "publication_candidate_status": "sent_to_chat",
            "sent_to_chat": "true",
            "publication_eligibility_verdict": "eligible",
            "publication_eligibility_gate_version": mod.rt.PUBLICATION_ELIGIBILITY_GATE_VERSION,
        }
        kinds = {
            # A compact image row can remain after the media lane stopped at
            # manual review. It must not mask the terminal publication row,
            # because that stale image is not itself a finalizer input.
            "image_queue_item": {
                "image:archived-stale": {
                    "post_url": post_url,
                    "external_publication_id": "extpub-archived",
                    "image_queue_status": "needs_visual_review",
                    "image_model_input_type": "actual_image",
                }
            },
            "candidate_memory_item": {},
            "publication_candidate_item": {"publication:archived": publication},
            "source_queue_item": {},
            "source_status_item": {},
            "online_source_item": {},
            "external_publication_source_item": {
                "source:publisher": {
                    "canonical_source_key": "web:publisher.example",
                    "source_url": "https://publisher.example",
                    "source_title": "Publisher Example",
                    "source_scope": "external",
                    "source_geo_class": "nonlocal_russia",
                    "source_queue_status": "confirmed_external_publication_research",
                }
            },
            "external_publication_intake_item": {"intake:archived": intake},
            "source_onboarding_profile_item": {},
        }

        class Pool:
            def retry_operation_sync(self, operation):
                return operation(object())

        class Ydb:
            def SessionPool(self, _driver):
                return Pool()

        with (
            mock.patch.object(mod.rt, "ydb_connect", return_value=(Ydb(), object(), object())),
            mock.patch.object(mod.rt, "ydb_kv_table_path", return_value="table"),
            mock.patch.object(
                mod.rt,
                "ydb_select_kind_items",
                side_effect=lambda _s, _y, _t, kind, limit, **_kwargs: kinds.get(kind, {}),
            ),
            mock.patch.object(mod.rt, "utc_now_iso", return_value="2026-08-02T02:00:00+00:00"),
        ):
            _ydb, _driver, _pool, _table, rows, _strict_priority = mod.read_live_rows(100, 100)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["post_url"], post_url)
        self.assertEqual(rows[0]["_image_ydb_pk"], "")
        self.assertEqual(rows[0]["_previous_publication"], publication)
        self.assertEqual(
            rows[0]["external_intake_fingerprint"],
            mod.rt.external_publication_intake_fingerprint(intake),
        )

    def test_read_live_rows_prefers_current_memory_text_reject_over_stale_image_snapshot(self) -> None:
        mod = self.mod
        source = external_source("kseniacalm")
        post_url = "https://t.me/kseniacalm/3678"
        kinds = {
            "image_queue_item": {
                "image:stale": {
                    "post_url": post_url,
                    "source_url": "https://t.me/kseniacalm",
                    "source_title": "KSENIA CALM",
                    "platform": "telegram",
                    "image_queue_status": "actual_scored",
                    "image_model_input_type": "actual_image",
                    "vector_gate_status": "vector_accept_candidate",
                    "text_vector_fusion_status": "fused_e5_bge_m3",
                    "kaliningrad_oblast_only_scope": True,
                    "overall_media_score": 0.8,
                },
            },
            "candidate_memory_item": {
                "memory:current": {
                    "post_url": post_url,
                    "text_excerpt": "Любить светлый диван букле",
                    "vector_gate_status": "vector_reject_not_kaliningrad_oblast",
                    "text_vector_fusion_status": "fused_e5_bge_m3",
                    "kaliningrad_oblast_only_scope": False,
                    "current_stage": "dropped_text_gate",
                    "current_lifecycle_status": "vector_reject_not_kaliningrad_oblast",
                    "processing_policy_version": "region_talk_post_processing_v3_ambiguous_place_context",
                },
            },
            "publication_candidate_item": {},
            "source_queue_item": {"source:kseniacalm": source},
            "source_status_item": {},
            "online_source_item": {},
            "source_onboarding_profile_item": {},
        }

        class Pool:
            def retry_operation_sync(self, operation):
                return operation(object())

        class Ydb:
            def SessionPool(self, _driver):
                return Pool()

        with (
            mock.patch.object(mod.rt, "ydb_connect", return_value=(Ydb(), object(), object())),
            mock.patch.object(mod.rt, "ydb_kv_table_path", return_value="table"),
            mock.patch.object(mod.rt, "ydb_select_kind_items", side_effect=lambda _s, _y, _t, kind, limit, **_kwargs: kinds.get(kind, {})),
            mock.patch.object(mod.rt, "utc_now_iso", return_value="2026-07-15T06:00:00+00:00"),
        ):
            _ydb, _driver, _pool, _table, rows, _strict_priority = mod.read_live_rows(100, 100)

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["vector_gate_status"], "vector_reject_not_kaliningrad_oblast")
        self.assertFalse(row["kaliningrad_oblast_only_scope"])
        verdict = mod.rt.publication_eligibility(row, row["_authoritative_source"])
        self.assertFalse(verdict["eligible"])
        self.assertEqual(verdict["primary_reason"], "vector_reject_not_kaliningrad_oblast")

    def test_read_live_rows_reuses_snapshot_for_pre_image_source_priority(self) -> None:
        mod = self.mod
        source = {
            "canonical_source_key": "telegram:prioritycase",
            "platform": "telegram",
            "handle": "prioritycase",
            "source_url": "https://t.me/prioritycase",
            "source_queue_status": "processed_found_ko_candidate",
            "posts_scanned": 1,
            "ko_posts_found": 1,
        }
        kinds = {
            "image_queue_item": {},
            "candidate_memory_item": {
                "memory:priority": {
                    "post_url": "https://t.me/prioritycase/10",
                    "canonical_source_key": "telegram:prioritycase",
                    "current_stage": "image_fetch_retry_needed",
                    "vector_gate_status": "vector_accept_candidate",
                    "text_vector_fusion_status": "fused_e5_bge_m3",
                    "kaliningrad_oblast_only_scope": True,
                },
            },
            "publication_candidate_item": {},
            "source_queue_item": {"source:prioritycase": source},
            "source_status_item": {},
            "online_source_item": {},
            "source_onboarding_profile_item": {},
        }

        class Pool:
            def retry_operation_sync(self, operation):
                return operation(object())

        class Ydb:
            def SessionPool(self, _driver):
                return Pool()

        select_mock = mock.Mock(side_effect=lambda _s, _y, _t, kind, limit, **_kwargs: kinds.get(kind, {}))
        with (
            mock.patch.object(mod.rt, "ydb_connect", return_value=(Ydb(), object(), object())),
            mock.patch.object(mod.rt, "ydb_kv_table_path", return_value="table"),
            mock.patch.object(mod.rt, "ydb_select_kind_items", select_mock),
            mock.patch.object(mod.rt, "utc_now_iso", return_value="2026-07-17T18:00:00+00:00"),
        ):
            _ydb, _driver, _pool, _table, rows, strict_priority = mod.read_live_rows(100, 100)

        self.assertEqual(rows, [])
        self.assertEqual(len(strict_priority), 1)
        self.assertEqual(strict_priority[0]["priority_reason"], "strict_text_candidate_needs_source_attestation")
        # One read per required kind. The removed post-snapshot pass used to
        # read candidate/source/status a second time and caused the live YDB
        # deadline failure.
        self.assertEqual(select_mock.call_count, 12)

    def test_external_publication_source_is_authoritative_by_web_key(self) -> None:
        mod = self.mod
        source = {
            "canonical_source_key": "web:archi.ru",
            "platform": "web",
            "source_title": "Архи.ру",
            "source_url": "https://archi.ru",
            "source_scope": "external",
            "source_geo_class": "nonlocal_russia",
            "source_topic_class": "editorial_publication",
            "source_queue_status": "confirmed_external_publication_research",
            "source_externality_basis": "Федеральное профессиональное издание.",
            "publisher_source_overview": "Профессиональная платформа об архитектуре для архитекторов, урбанистов и читателей, которые следят за проектами и устройством городской среды.",
            "publisher_source_overview_evidence_refs_json": '["publisher-about"]',
            "publisher_profile_evidence_json": json.dumps([{
                "evidence_id": "publisher-about",
                "paraphrase": "Навигация и редакционное описание охватывают архитектурные проекты, практику и профессиональную критику.",
                "url": "https://archi.ru/about",
            }], ensure_ascii=False),
        }
        indexed = mod.authoritative_source_index({}, {}, {"external:archi": source})
        self.assertEqual(indexed["web:archi.ru"]["source_title"], "Архи.ру")
        evidence = mod.build_source_onboarding_evidence(source, [], {
            "canonical_source_key": "web:archi.ru",
            "post_url": "https://archi.ru/russia/101203/vsya-mudrost-okeana",
            "content_origin_type": "editorial_publication",
        })
        evidence_pack = json.loads(evidence["evidence_pack_json"])
        self.assertTrue(any(row["kind"] == "external_publication_source" for row in evidence_pack))
        self.assertEqual(evidence["evidence_status"], "sufficient")
        self.assertGreaterEqual(evidence["publisher_evidence_total"], 2)

    def test_publisher_profile_requires_all_three_reader_brief_dimensions(self) -> None:
        mod = self.mod
        source = {
            "canonical_source_key": "web:publisher.example",
            "source_title": "Publisher",
            "source_url": "https://publisher.example",
            "source_topic_class": "editorial_publication",
            "publisher_source_overview": "Профессиональное издание об архитектуре и городской среде.",
            "publisher_source_overview_evidence_refs_json": '["about"]',
            "publisher_profile_evidence_json": json.dumps([{
                "evidence_id": "about", "paraphrase": "Издание публикует проектные разборы для архитекторов и интересующихся городской средой."
            }], ensure_ascii=False),
        }
        evidence = mod.build_source_onboarding_evidence(
            source, [], {"content_origin_type": "editorial_publication", "post_url": "https://publisher.example/a"},
        )
        evidence_ids = [item["evidence_id"] for item in json.loads(evidence["evidence_pack_json"])]
        result = {
            "llm_gate_status": "ok",
            "data": {
                "status": "ready", "entity_type": "media_brand", "profile_summary": "Профильное издание.",
                "claims": [{"claim_id": "C1", "text": "Пишет об архитектуре", "evidence_ids": [evidence_ids[0]]}],
                "candidate_angles": [],
                "publisher_dimensions": {
                    key: {"text": key, "basis": "editorial_inference", "evidence_ids": [evidence_ids[-1]]}
                    for key in ("outlet_identity", "intended_audience", "distinctive_value")
                },
            },
        }
        profile = mod.normalize_source_onboarding_profile(
            result, evidence, model="gemini-test", profile_fingerprint="fp",
        )
        self.assertEqual(profile["profile_status"], "ready")
        dimensions = json.loads(profile["publisher_dimensions_json"])
        self.assertEqual(set(dimensions), {"outlet_identity", "intended_audience", "distinctive_value"})
        del result["data"]["publisher_dimensions"]["intended_audience"]
        incomplete = mod.normalize_source_onboarding_profile(
            result, evidence, model="gemini-test", profile_fingerprint="fp2",
        )
        self.assertEqual(incomplete["profile_status"], "needs_review")

    def test_trusted_publisher_profile_is_reused_without_profile_llm(self) -> None:
        mod = self.mod
        source = {
            "canonical_source_key": "web:archi.ru",
            "source_title": "Архи.ру",
            "source_url": "https://archi.ru/",
            "source_topic_class": "editorial_publication",
            "publisher_source_overview": "Профессиональное издание об архитектуре.",
            "source_externality_basis": "Общероссийская профессиональная редакция.",
        }
        publisher = {
            "publisher_profile_id": "rtpublisher_archi",
            "canonical_source_key": "web:archi.ru",
            "source_name": "Архи.ру",
            "canonical_url": "https://archi.ru/",
            "entity_type": "professional_platform",
            "scope": "external",
            "profile_status": "ready",
            "usable_without_profile_llm": True,
            "profile_hash": "publisher-profile-hash",
            "public_copy_eligibility": "allowed",
            "profile_dimensions": {
                "outlet_identity": "Профессиональное издание об архитектуре и городской среде.",
                "intended_audience": [{"label": "архитекторы и читатели городской культуры", "evidence_refs": ["about"]}],
                "distinctive_value": [{"text": "Соединяет авторские разборы с каталогом проектов.", "evidence_refs": ["about"]}],
            },
            "copy_projection": {
                "short_reader_brief": "Архи.ру объясняет архитектурные проекты через профессиональный замысел и устройство среды.",
                "cta_label": "Подробнее — в статье на Архи.ру",
                "public_copy_eligibility": "allowed",
            },
            "evidence": [{
                "evidence_id": "about",
                "paraphrase": "Официальная страница описывает тематику, аудиторию и форматы издания.",
                "url": "https://archi.ru/about",
            }],
        }
        row = candidate_row(
            url="https://archi.ru/russia/example",
            publication_status="gemini_accept",
            content_origin_type="editorial_publication",
            canonical_source_key="web:archi.ru",
            source_title="Архи.ру",
            source_url="https://archi.ru/",
            _authoritative_source=source,
        )
        evidence = mod.build_source_onboarding_evidence(
            source, [], row, publisher_profile=publisher,
        )
        row["_source_onboarding_evidence"] = evidence
        row["_publisher_profile"] = publisher
        paragraph = (
            "Архи.ру работает как профессиональное издание об архитектуре и городской среде, "
            "адресованное архитекторам и читателям городской культуры. Его авторские разборы "
            "связаны с каталогом проектов и помогают видеть замысел здания в контексте среды. "
            "Этот материал показывает такой подход на конкретном калининградском объекте."
        )
        with mock.patch.object(mod, "_call_structured_with_budget", return_value=({
            "llm_gate_status": "ok",
            "data": {
                "status": "ready",
                "onboarding_paragraph": paragraph,
                "claim_ids": ["PC1", "PC2", "PC3"],
                "evidence_ids": ["E1"],
                "selected_angle_id": "PA1",
                "covered_publisher_dimensions": ["outlet_identity", "intended_audience", "distinctive_value"],
            },
        }, True)) as call:
            enriched, profiles, stats = mod.enrich_accepted_rows_with_onboarding(
                [row], max_llm=0, max_profile_llm=1, max_writer_llm=1,
                model="gemini-test", default_env_var_name="KEY", durable_budget=None,
            )
        self.assertEqual(call.call_count, 1)
        self.assertEqual(stats["profile_calls"], 0)
        self.assertEqual(stats["profiles_reused"], 1)
        self.assertEqual(profiles, [])
        self.assertEqual(enriched[0]["source_profile_fingerprint"], "publisher-profile-hash")
        self.assertEqual(enriched[0]["source_profile_cta_label"], "Подробнее — в статье на Архи.ру")

    def test_external_publication_onboarding_prompt_uses_publisher_language(self) -> None:
        mod = self.mod
        prompt = mod._candidate_onboarding_prompt(
            {
                "content_origin_type": "editorial_publication",
                "post_url": "https://archi.ru/article",
                "source_title": "Архи.ру",
                "short_summary": "Обзор музейного комплекса.",
            },
            {"profile_summary": "Профессиональное архитектурное издание."},
            {"evidence_pack": []},
        )
        self.assertIn("издании/журнале", prompt)
        self.assertIn("чем интересна эта публикация", prompt)
        self.assertNotIn("кто автор", prompt)

    def test_one_current_social_post_is_not_a_ready_reusable_profile(self) -> None:
        mod = self.mod
        row = candidate_row(text="Восстановленный личный рассказ о зимней поездке в Калининград.")
        evidence = mod.build_source_onboarding_evidence(
            row["_authoritative_source"],
            [{"post_url": row["post_url"], "text": row["text"]}],
            row,
        )

        self.assertEqual(evidence["evidence_status"], "insufficient")
        self.assertEqual(evidence["profile_readiness_reason"], "source_capture_missing")

    def test_ready_social_capture_supplies_description_pinned_and_diverse_excerpts(self) -> None:
        mod = self.mod
        capture = ready_social_capture("capture-fingerprint")
        evidence = mod.build_source_onboarding_evidence(
            external_source("travelcase"), [], candidate_row(), source_capture=capture,
        )
        items = json.loads(evidence["evidence_pack_json"])

        self.assertEqual(evidence["evidence_status"], "sufficient")
        self.assertEqual(evidence["source_capture_fingerprint"], "capture-fingerprint")
        self.assertEqual(evidence["authored_posts_captured_total"], 24)
        self.assertEqual(evidence["representative_excerpts_total"], 8)
        self.assertGreaterEqual(evidence["representative_topics_total"], 3)
        self.assertTrue(any(item["kind"] == "source_description" for item in items))
        self.assertTrue(any(item["kind"] == "pinned_post_excerpt" for item in items))

    def test_source_onboarding_evidence_is_compact_deduplicated_and_traceable(self) -> None:
        mod = self.mod
        source = external_source("travelcase")
        source.update({
            "external_blogger_name": "Анна",
            "external_blogger_segment": "travel blogger",
            "external_blogger_visit_period": "2026-06",
            "external_blogger_evidence_url": "https://example.test/anna",
        })
        memory = [
            {
                "canonical_source_key": "telegram:travelcase",
                "post_url": "https://t.me/travelcase/10",
                "post_date": "2026-06-10T10:00:00+00:00",
                "full_text": "Личный рассказ о прогулке по Балтийской косе. " * 80,
            },
            {
                "canonical_source_key": "telegram:travelcase",
                "post_url": "https://t.me/travelcase/10",
                "post_date": "2026-06-10T10:00:00+00:00",
                "full_text": "Личный рассказ о прогулке по Балтийской косе. " * 80,
            },
        ]
        capture = ready_social_capture("capture-fp")
        evidence = mod.build_source_onboarding_evidence(source, memory, candidate_row(), source_capture=capture)
        items = json.loads(evidence["evidence_pack_json"])
        self.assertEqual(evidence["evidence_status"], "sufficient")
        self.assertLessEqual(len(items), 20)
        self.assertEqual(len({item["evidence_id"] for item in items}), len(items))
        self.assertTrue(all(len(item["excerpt"]) <= 500 for item in items))
        self.assertEqual(sum(1 for item in items if item["url"] == "https://t.me/travelcase/11"), 1)

    def test_current_restored_text_does_not_substitute_for_source_capture(self) -> None:
        mod = self.mod
        row = candidate_row(text="Восстановленный личный рассказ о зимней поездке в Калининград.")
        compacted_memory = [{
            "canonical_source_key": "telegram:travelcase",
            "post_url": row["post_url"],
            "post_date": "2026-06-10T10:00:00+00:00",
            "full_text": "",
            "text": "",
            "text_excerpt": "",
            "short_summary": "",
        }]

        evidence = mod.build_source_onboarding_evidence(
            row["_authoritative_source"],
            compacted_memory,
            row,
        )
        items = json.loads(evidence["evidence_pack_json"])
        authored = [item for item in items if item["kind"] == "authored_post_excerpt"]

        self.assertEqual(evidence["evidence_status"], "insufficient")
        self.assertEqual(evidence["authored_post_evidence_total"], 0)
        self.assertEqual(authored, [])

    def test_source_onboarding_profile_and_writer_fail_closed_on_unsupported_references(self) -> None:
        mod = self.mod
        evidence = mod.build_source_onboarding_evidence(
            external_source("travelcase"),
            [{"post_url": "https://t.me/travelcase/10", "text": "Путешествие по Калининграду"}],
            candidate_row(),
        )
        bad_profile = mod.normalize_source_onboarding_profile(
            {
                "llm_gate_status": "ok",
                "data": {
                    "status": "ready",
                    "entity_type": "person",
                    "profile_summary": "Автор путешествует.",
                    "claims": [{"claim_id": "C1", "text": "Живёт в Москве", "evidence_ids": ["E999"]}],
                    "candidate_angles": [],
                },
            },
            evidence,
            model="gemini-test",
            profile_fingerprint="profile-fp",
        )
        self.assertEqual(bad_profile["profile_status"], "needs_review")
        self.assertEqual(json.loads(bad_profile["claims_json"]), [])

        good_profile = dict(bad_profile)
        good_profile.update({
            "profile_status": "ready",
            "entity_type": "person",
            "claims_json": json.dumps([{"claim_id": "C1", "text": "Автор пишет о поездке", "evidence_ids": ["E1"]}]),
            "candidate_angles_json": json.dumps([{"angle_id": "A1", "text": "личный взгляд", "claim_ids": ["C1"], "evidence_ids": ["E1"]}]),
        })
        paragraph = "Автор тревел-канала делится проверяемым личным опытом поездок и в этом материале показывает Калининградскую область через конкретные наблюдения и детали маршрута. Вводный ракурс основан на опубликованном рассказе автора и помогает понять, почему его взгляд на регион может быть полезен читателю перед переходом к оригинальному посту."
        ready = mod.normalize_candidate_onboarding(
            {"llm_gate_status": "ok", "data": {
                "status": "ready", "onboarding_paragraph": paragraph,
                "claim_ids": ["C1"], "evidence_ids": ["E1"], "selected_angle_id": "A1",
            }},
            profile=good_profile,
            evidence_row=evidence,
            writer_fingerprint="writer-fp",
        )
        self.assertEqual(ready["source_onboarding_status"], "ready")
        self.assertGreaterEqual(len(ready["source_onboarding_paragraph"]), 300)

        cliched = mod.normalize_candidate_onboarding(
            {"llm_gate_status": "ok", "data": {
                "status": "ready",
                "onboarding_paragraph": (
                    "Автор тревел-канала представляет поездку не как перечень мест, а как личный "
                    "маршрут по Калининградской области с наблюдениями о городах, побережье и дороге. "
                    "Текст опирается на опубликованный опыт и помогает читателю понять авторский "
                    "ракурс перед переходом к оригиналу. Дополнительный контекст раскрывает тему "
                    "источника и его последовательный интерес к путешествиям."
                ),
                "claim_ids": ["C1"], "evidence_ids": ["E1"], "selected_angle_id": "A1",
            }},
            profile=good_profile,
            evidence_row=evidence,
            writer_fingerprint="writer-cliched-fp",
        )
        self.assertEqual(cliched["source_onboarding_status"], "needs_review")

    def test_social_profile_requires_grounded_reader_value_dimensions(self) -> None:
        mod = self.mod
        evidence = mod.build_source_onboarding_evidence(
            external_source("travelcase"), [], candidate_row(),
            source_capture=ready_social_capture(),
        )
        evidence_ids = [item["evidence_id"] for item in json.loads(evidence["evidence_pack_json"])]
        result = {
            "llm_gate_status": "ok",
            "data": {
                "status": "ready",
                "entity_type": "thematic_channel",
                "profile_summary": "Авторский канал о самостоятельных поездках.",
                "claims": [{"claim_id": "C1", "text": "Публикует маршруты", "evidence_ids": [evidence_ids[-1]]}],
                "candidate_angles": [],
                "social_dimensions": {
                    "stable_topics": [{"text": "самостоятельные поездки", "evidence_ids": [evidence_ids[-1]]}],
                    "recurring_formats": [{"text": "маршруты", "evidence_ids": [evidence_ids[-1]]}],
                    "intended_audience": {"text": "читатели, планирующие поездки", "evidence_ids": [evidence_ids[-1]]},
                    "distinctive_value": {"text": "личные наблюдения и дорожные детали", "evidence_ids": [evidence_ids[-1]]},
                    "reader_brief": {"text": "Канал полезен личными маршрутами и наблюдениями из дороги.", "evidence_ids": [evidence_ids[-1]]},
                },
                "do_not_say": ["ведущий тревел-канал"],
            },
        }
        profile = mod.normalize_source_onboarding_profile(
            result, evidence, model="gemini-test", profile_fingerprint="social-fp",
        )
        self.assertEqual(profile["profile_status"], "ready")
        self.assertEqual(profile["social_dimensions_status"], "ready")
        self.assertEqual(profile["reader_brief"], "Канал полезен личными маршрутами и наблюдениями из дороги.")
        del result["data"]["social_dimensions"]["reader_brief"]
        incomplete = mod.normalize_source_onboarding_profile(
            result, evidence, model="gemini-test", profile_fingerprint="social-fp-2",
        )
        self.assertEqual(incomplete["profile_status"], "needs_review")

    def test_missing_social_profile_projects_explicit_capture_request(self) -> None:
        mod = self.mod
        source = external_source("travelcase") | {"_ydb_pk": "source_queue_item:telegram:travelcase"}
        row = candidate_row(
            publication_status="gemini_accept",
            source_onboarding_status="needs_source_profile",
            source_onboarding_llm_reason="source_capture_missing",
            _authoritative_source=source,
        )
        updates = mod.source_profile_capture_request_updates(
            [row], now_iso="2026-08-02T10:00:00+00:00",
        )
        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0]["source_profile_capture_requested"], "true")
        self.assertEqual(updates[0]["needs_source_profile"], "true")
        self.assertEqual(updates[0]["next_action"], "capture_bounded_source_profile")

        source.update({
            "source_profile_capture_requested": "true",
            "needs_source_profile": "true",
            "priority_lane": "source_profile_capture",
            "priority_reason": "accepted_candidate_needs_source_profile",
        })
        row.update({
            "_authoritative_source": source,
            "source_onboarding_status": "ready",
            "source_onboarding_profile_fingerprint": "profile-fp",
        })
        cleared = mod.source_profile_capture_request_updates(
            [row], now_iso="2026-08-02T11:00:00+00:00",
        )
        self.assertEqual(cleared[0]["source_profile_capture_requested"], "false")
        self.assertEqual(cleared[0]["needs_source_profile"], "false")
        self.assertEqual(cleared[0]["priority_lane"], "")
        self.assertEqual(cleared[0]["priority_reason"], "")

    def test_processed_nonready_capture_clears_only_acquisition_request(self) -> None:
        mod = self.mod
        source = external_source("travelcase") | {
            "_ydb_pk": "source_queue_item:telegram:travelcase",
            "source_profile_capture_requested": "true",
            "needs_source_profile": "true",
            "priority_lane": "source_profile_capture",
            "priority_reason": "accepted_candidate_needs_source_profile",
        }
        row = candidate_row(
            publication_status="gemini_accept",
            source_onboarding_status="needs_source_profile",
            source_onboarding_llm_reason="insufficient_grounded_reader_brief",
            _authoritative_source=source,
        )
        row["_source_onboarding_evidence"] = {
            "source_capture_status": "ready",
            "source_capture_fingerprint": "capture-fp",
        }

        updates = mod.source_profile_capture_request_updates(
            [row], now_iso="2026-08-02T12:00:00+00:00",
        )

        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0]["source_profile_capture_requested"], "false")
        self.assertEqual(updates[0]["needs_source_profile"], "true")
        self.assertEqual(updates[0]["priority_lane"], "")
        self.assertEqual(updates[0]["priority_reason"], "")
        self.assertEqual(updates[0]["next_action"], "review_nonready_source_profile")

        row["publication_status"] = "gemini_needs_review"
        deferred_updates = mod.source_profile_capture_request_updates(
            [row], now_iso="2026-08-02T12:01:00+00:00",
        )
        self.assertEqual(len(deferred_updates), 1)
        self.assertEqual(deferred_updates[0]["source_profile_capture_requested"], "false")
        self.assertEqual(deferred_updates[0]["needs_source_profile"], "true")

    def test_onboarding_reuses_current_profile_and_spends_only_writer_call(self) -> None:
        mod = self.mod
        row = candidate_row(publication_status="gemini_accept", sent_to_chat="false")
        evidence = mod.build_source_onboarding_evidence(
            row["_authoritative_source"],
            [{"post_url": row["post_url"], "text": row["text"]}],
            row,
            source_capture=ready_social_capture(),
        )
        profile = {
            "source_profile_id": evidence["source_profile_id"],
            "profile_status": "ready",
            "entity_type": "thematic_channel",
            "profile_summary": "Канал публикует личные заметки о поездках.",
            "claims_json": json.dumps([{"claim_id": "C1", "text": "Пишет о поездках", "evidence_ids": ["E1"]}]),
            "candidate_angles_json": json.dumps([{"angle_id": "A1", "text": "личный опыт", "claim_ids": ["C1"], "evidence_ids": ["E1"]}]),
            "evidence_fingerprint": evidence["evidence_fingerprint"],
            "profile_prompt_version": mod.SOURCE_ONBOARDING_PROFILE_PROMPT_VERSION,
            "profile_fingerprint": "current-profile",
        }
        row["_source_onboarding_evidence"] = evidence
        row["_source_onboarding_profile"] = profile
        paragraph = "Тревел-канал публикует личные заметки о поездках и в этом посте показывает Калининградскую область через конкретные впечатления автора. Такой ракурс помогает заранее увидеть личный опыт посещения с деталями маршрута, наблюдениями и собственным отношением автора к увиденному в регионе. Профиль полезен читателю как контекст перед переходом к исходной публикации."
        with mock.patch.object(mod, "_call_structured_with_budget", return_value=({
            "llm_gate_status": "ok",
            "data": {"status": "ready", "onboarding_paragraph": paragraph, "claim_ids": ["C1"], "evidence_ids": ["E1"], "selected_angle_id": "A1"},
        }, True)) as call:
            enriched, profiles, stats = mod.enrich_accepted_rows_with_onboarding(
                [row], max_llm=1, model="gemini-test", default_env_var_name="KEY", durable_budget=None,
            )
        self.assertEqual(call.call_count, 1)
        self.assertEqual(profiles, [])
        self.assertEqual(stats["profiles_reused"], 1)
        self.assertEqual(stats["writer_calls"], 1)
        self.assertEqual(enriched[0]["source_onboarding_status"], "ready")

    def test_unchanged_nonready_profile_spends_zero_profile_llm_calls(self) -> None:
        mod = self.mod
        row = candidate_row(publication_status="gemini_accept", sent_to_chat="false")
        evidence = mod.build_source_onboarding_evidence(
            row["_authoritative_source"],
            [{"post_url": row["post_url"], "text": row["text"]}],
            row,
            source_capture=ready_social_capture(),
        )
        row["_source_onboarding_evidence"] = evidence
        row["_source_onboarding_profile"] = {
            "source_profile_id": evidence["source_profile_id"],
            "profile_status": "needs_review",
            "profile_reason": "insufficient_grounded_reader_brief",
            "evidence_fingerprint": evidence["evidence_fingerprint"],
            "profile_prompt_version": mod.SOURCE_ONBOARDING_PROFILE_PROMPT_VERSION,
            "profile_fingerprint": "current-nonready-profile-attempt",
        }

        with mock.patch.object(
            mod,
            "_call_structured_with_budget",
            side_effect=AssertionError("unchanged failed profile must not call provider again"),
        ) as call:
            enriched, profiles, stats = mod.enrich_accepted_rows_with_onboarding(
                [row], max_llm=10, max_profile_llm=10, max_writer_llm=10,
                model="gemini-test", default_env_var_name="KEY", durable_budget=None,
            )

        self.assertEqual(call.call_count, 0)
        self.assertEqual(profiles, [])
        self.assertEqual(stats["profile_calls"], 0)
        self.assertEqual(stats["profiles_reused"], 1)
        self.assertEqual(enriched[0]["source_onboarding_status"], "needs_source_profile")

    def test_eligibility_helper_receives_authoritative_source_and_fields_persist(self) -> None:
        mod = self.mod
        row = candidate_row()
        authoritative = row["_authoritative_source"]

        def gate(actual_row, actual_source):
            self.assertIs(actual_row, row)
            self.assertIs(actual_source, authoritative)
            return eligibility("eligible")

        with (
            mock.patch.object(mod.rt, "publication_eligibility", create=True, side_effect=gate) as gate_mock,
            mock.patch.object(
                mod.rt,
                "call_region_talk_semantic_llm",
                return_value={"llm_gate_status": "ok", "llm_decision": "accept", "llm_reason": "good"},
            ),
        ):
            result = mod.verify_rows(
                [row],
                max_llm=1,
                model="gemini-test",
                default_env_var_name="KEY",
                now_iso="2026-07-10T10:00:00+00:00",
            )

        self.assertEqual(gate_mock.call_count, 1)
        self.assertEqual(result[0]["publication_eligibility_verdict"], "eligible")
        self.assertEqual(result[0]["publication_eligibility_gate_version"], "publication-source-gate-v3")
        self.assertEqual(json.loads(result[0]["publication_eligibility_evidence"])["source_policy"], "eligible")

        result[0]["text"] = "x" * 2000
        result[0].update({
            "content_origin_type": "editorial_publication",
            "external_publication_id": "extpub-1",
            "external_research_quality_score": 0.857,
            "rights_policy": "link_only",
            "media_use_policy": "score_only_no_reuse",
            "media_reuse_allowed": False,
            "image_quality_decision": "needs_visual_review",
            "image_quality_reason": "low_score_requires_review",
            "kaliningrad_oblast_only_scope": True,
            "publication_draft_status": "ready_for_operator_review",
            "publication_draft_title": "Музей и город",
            "publication_draft_source_attribution": "Архи.ру",
            "publication_draft_telegram_text": "Фактический черновик.\n\nОригинал: https://example.test/post/1",
            "publication_draft_vk_text": "Фактический черновик для VK.\n\nОригинал: https://example.test/post/1",
            "publication_draft_fact_points_json": '[{"claim":"Факт","support_excerpt":"Опора"}]',
            "publication_draft_prompt_version": "region_talk_final_verifier_v7_grounded_draft",
        })
        captured = {}

        class Pool:
            def retry_operation_sync(self, operation):
                return operation(object())

        def capture(_session, _ydb, _table, items, _now, **_kwargs):
            captured["items"] = items
            return len(items)

        with (
            mock.patch.object(mod.rt, "utc_now_iso", return_value="2026-07-10T10:01:00+00:00"),
            mock.patch.object(mod.rt, "ensure_ydb_kv_table"),
            mock.patch.object(mod.rt, "ydb_upsert_json_many", side_effect=capture),
        ):
            self.assertEqual(mod.write_publication_rows(Pool(), object(), "table", result, "run-1"), 1)
        payload = captured["items"][0][2]
        self.assertEqual(payload["publication_eligibility_verdict"], "eligible")
        self.assertEqual(payload["publication_eligibility_gate_version"], "publication-source-gate-v3")
        self.assertEqual(payload["attempt_count"], 1)
        self.assertEqual(payload["finalizer_state_version"], mod.PUBLICATION_FINALIZER_STATE_VERSION)
        self.assertEqual(payload["llm_prompt_version"], mod.rt.REGION_TALK_FINAL_VERIFIER_PROMPT_VERSION)
        self.assertTrue(payload["llm_request_fingerprint"])
        self.assertEqual(payload["content_origin_type"], "editorial_publication")
        self.assertEqual(payload["external_publication_id"], "extpub-1")
        self.assertEqual(payload["rights_policy"], "link_only")
        self.assertEqual(payload["media_use_policy"], "score_only_no_reuse")
        self.assertEqual(payload["image_quality_decision"], "needs_visual_review")
        self.assertEqual(payload["publication_draft_status"], "ready_for_operator_review")
        self.assertIn("Оригинал:", payload["publication_draft_telegram_text"])
        self.assertTrue(payload["kaliningrad_oblast_only_scope"])
        self.assertNotIn("text", payload)

    def test_retryable_publication_keeps_only_bounded_text_for_next_attempt(self) -> None:
        mod = self.mod
        row = candidate_row(
            text="x" * 2000,
            publication_status="gemini_error",
            publication_candidate_status="llm_error",
            finalization_status="retryable",
            llm_gate_status="error",
            llm_decision="",
        )
        captured = {}

        class Pool:
            def retry_operation_sync(self, operation):
                return operation(object())

        def capture(_session, _ydb, _table, items, _now, **_kwargs):
            captured["items"] = items
            return len(items)

        with (
            mock.patch.object(mod.rt, "utc_now_iso", return_value="2026-07-10T10:01:00+00:00"),
            mock.patch.object(mod.rt, "ensure_ydb_kv_table"),
            mock.patch.object(mod.rt, "ydb_upsert_json_many", side_effect=capture),
        ):
            self.assertEqual(mod.write_publication_rows(Pool(), object(), "table", [row], "run-1"), 1)
        self.assertEqual(len(captured["items"][0][2]["text"]), 2000)

    def test_unknown_local_and_spam_fail_closed_without_gemini_and_revoke_prior_accept(self) -> None:
        mod = self.mod
        unknown = candidate_row(
            "https://t.me/unknown/1",
            canonical_source_key="telegram:unknown",
            _authoritative_source=None,
        )
        local_source = {
            "canonical_source_key": "telegram:domkitoboya",
            "platform": "telegram",
            "handle": "domkitoboya",
            "source_url": "https://t.me/domkitoboya",
            "source_title": "Дом китобоя",
        }
        local = candidate_row(
            "https://t.me/domkitoboya/2",
            canonical_source_key="telegram:domkitoboya",
            _authoritative_source=local_source,
            _previous_publication={
                "publication_status": "gemini_accept",
                "publication_candidate_status": "llm_confirmed",
            },
        )
        spam = candidate_row("https://t.me/travelcase/3")

        gate_results = [eligibility("eligible"), eligibility("eligible"), eligibility("spam")]
        with (
            mock.patch.object(mod.rt, "publication_eligibility", create=True, side_effect=gate_results) as gate_mock,
            mock.patch.object(mod.rt, "call_region_talk_semantic_llm") as llm_mock,
        ):
            result = mod.verify_rows(
                [unknown, local, spam],
                max_llm=10,
                model="gemini-test",
                default_env_var_name="KEY",
                now_iso="2026-07-10T10:00:00+00:00",
            )

        self.assertEqual(gate_mock.call_count, 3)
        llm_mock.assert_not_called()
        self.assertEqual([row["publication_eligibility_verdict"] for row in result], ["review", "reject", "reject"])
        self.assertEqual(unknown["publication_status"], "needs_visual_review")
        self.assertEqual(unknown["publication_candidate_status"], "visual_review_pending")
        self.assertEqual(unknown["publication_tombstone"], "false")
        self.assertEqual(unknown["finalization_status"], "review_pending")
        self.assertEqual(local["publication_status"], "eligibility_revoked")
        self.assertEqual(local["publication_candidate_status"], "revoked")
        self.assertEqual(local["publication_revoked"], "true")
        self.assertEqual(spam["publication_candidate_status"], "tombstoned_reject")

        prior_revoke = dict(local)
        repeated = candidate_row(
            "https://t.me/domkitoboya/2",
            canonical_source_key="telegram:domkitoboya",
            _authoritative_source=local_source,
            _previous_publication=prior_revoke,
        )
        with (
            mock.patch.object(mod.rt, "publication_eligibility", create=True, return_value=eligibility("eligible")),
            mock.patch.object(mod.rt, "call_region_talk_semantic_llm") as llm_again,
        ):
            self.assertEqual(
                mod.verify_rows(
                    [repeated],
                    max_llm=10,
                    model="gemini-test",
                    default_env_var_name="KEY",
                    now_iso="2026-07-11T10:00:00+00:00",
                ),
                [],
            )
        llm_again.assert_not_called()

    def test_terminal_and_retryable_gemini_statuses_track_attempts_and_retry_due(self) -> None:
        mod = self.mod
        retry = candidate_row("https://t.me/travelcase/20", attempt_count=2)
        accepted = candidate_row("https://t.me/travelcase/21", attempt_count=0)
        with (
            mock.patch.object(mod.rt, "publication_eligibility", create=True, side_effect=[eligibility(), eligibility()]),
            mock.patch.object(
                mod.rt,
                "call_region_talk_semantic_llm",
                side_effect=[
                    {"llm_gate_status": "rate_limited", "llm_reason": "429"},
                    {"llm_gate_status": "ok", "llm_decision": "accept", "llm_reason": "good"},
                ],
            ),
            mock.patch.dict(os.environ, {"REGION_TALK_FINALIZER_RATE_LIMIT_RETRY_SECONDS": "60"}),
        ):
            result = mod.verify_rows(
                [retry, accepted],
                max_llm=2,
                model="gemini-test",
                default_env_var_name="KEY",
                now_iso="2026-07-10T10:00:00+00:00",
            )

        self.assertEqual(len(result), 2)
        self.assertEqual(retry["publication_status"], "gemini_rate_limited")
        self.assertEqual(retry["finalization_status"], "retryable")
        self.assertEqual(retry["attempt_count"], 3)
        self.assertEqual(retry["next_attempt_after"], "2026-07-10T10:01:00+00:00")
        self.assertEqual(
            mod.finalization_trigger(retry, now_iso="2026-07-10T10:00:30+00:00"),
            "",
        )
        self.assertEqual(
            mod.finalization_trigger(retry, now_iso="2026-07-10T10:01:01+00:00"),
            "retry_due",
        )
        self.assertEqual(accepted["publication_status"], "gemini_accept")
        self.assertEqual(accepted["finalization_status"], "terminal")
        self.assertEqual(accepted["attempt_count"], 1)
        self.assertEqual(mod.finalization_trigger(accepted, now_iso="2026-07-11T10:00:00+00:00"), "")

    def test_sent_candidate_never_reuses_gemini_or_counts_as_new_accept(self) -> None:
        mod = self.mod
        previous = {
            "publication_status": "eligibility_revoked",
            "publication_candidate_status": "revoked",
            "sent_to_chat": "true",
        }
        self.assertEqual(
            mod.finalization_trigger(previous, now_iso="2026-07-11T10:00:00+00:00"),
            "",
        )
        self.assertEqual(
            mod.finalization_trigger(previous, now_iso="2026-07-11T10:00:00+00:00", reverify_existing=True),
            "",
        )
        replayed = {
            "publication_status": "gemini_accept",
            "_previous_publication": previous,
        }
        self.assertFalse(mod.is_newly_accepted_in_run(replayed))
        self.assertTrue(mod.is_newly_accepted_in_run({"publication_status": "gemini_accept"}))

    def test_text_restore_never_reopens_delivered_or_rejected_terminal_rows(self) -> None:
        mod = self.mod
        rows = [
            candidate_row(
                publication_status="text_restore_pending",
                publication_candidate_status="awaiting_text_restore",
                sent_to_chat="true",
            ),
            candidate_row(
                publication_status="text_restore_pending",
                publication_candidate_status="llm_rejected",
            ),
            candidate_row(
                publication_status="text_restore_pending",
                publication_candidate_status="awaiting_text_restore",
                publication_tombstone="true",
            ),
            candidate_row(
                publication_status="operator_rejected",
                publication_candidate_status="filtered_before_llm",
            ),
            candidate_row(
                publication_status="text_restore_pending",
                publication_candidate_status="tombstoned_reject",
            ),
            candidate_row(
                publication_status="text_restore_pending",
                publication_candidate_status="revoked",
            ),
            candidate_row(
                publication_status="text_restore_pending",
                publication_candidate_status="awaiting_text_restore",
                llm_decision="reject",
            ),
        ]
        for row in rows:
            with self.subTest(row=row):
                self.assertEqual(mod.finalization_trigger(row, now_iso="2026-07-14T20:00:00+00:00"), "")

        with mock.patch.object(mod.rt, "ydb_select_kind_items") as select_mock:
            self.assertEqual(
                mod.write_text_restore_post_link_rows(object(), object(), "table", rows, run_id="run-terminal"),
                0,
            )
        select_mock.assert_not_called()

    def test_verify_rows_reconciles_terminal_provider_evidence_before_restore_or_review(self) -> None:
        mod = self.mod
        rejected = candidate_row(
            text="",
            short_summary="",
            _previous_publication={
                "publication_status": "text_restore_pending",
                "publication_candidate_status": "awaiting_text_restore",
                "llm_decision": "reject",
            },
        )
        sent = candidate_row(
            "https://t.me/travelcase/20",
            _authoritative_source=None,
            _previous_publication={
                "publication_status": "gemini_accept",
                "publication_candidate_status": "sent_to_chat",
                "sent_to_chat": "true",
            },
        )
        with (
            mock.patch.object(mod.rt, "publication_eligibility", create=True, return_value=eligibility("eligible")),
            mock.patch.object(mod, "telegram_public_text", return_value="") as fallback_mock,
            mock.patch.object(mod.rt, "call_region_talk_semantic_llm") as llm_mock,
        ):
            result = mod.verify_rows(
                [rejected, sent],
                max_llm=2,
                model="gemini-test",
                default_env_var_name="KEY",
                now_iso="2026-07-15T10:00:00+00:00",
            )
        self.assertEqual(len(result), 2)
        self.assertEqual(rejected["publication_status"], "gemini_reject")
        self.assertEqual(rejected["publication_candidate_status"], "llm_rejected")
        self.assertEqual(rejected["next_action"], "")
        self.assertEqual(sent["publication_status"], "gemini_accept")
        self.assertEqual(sent["publication_candidate_status"], "sent_to_chat")
        fallback_mock.assert_not_called()
        llm_mock.assert_not_called()

    def test_terminal_accept_refreshes_new_media_materialization_without_provider_call(self) -> None:
        mod = self.mod
        row = candidate_row(
            selected_media_materialization_fingerprint="new-media",
            selected_media_materialization_json='[{"media_id":"frame:1"}]',
            _previous_publication={
                "publication_status": "gemini_accept",
                "publication_candidate_status": "llm_confirmed",
                "llm_decision": "accept",
                "selected_media_materialization_fingerprint": "old-media",
            },
        )
        with (
            mock.patch.object(mod, "_eligibility_fields", return_value=("eligible", {})),
            mock.patch.object(mod.rt, "call_region_talk_semantic_llm") as llm_mock,
        ):
            result = mod.verify_rows(
                [row], max_llm=1, model="gemini-test", default_env_var_name="KEY",
                now_iso="2026-08-01T12:00:00+00:00",
            )
        self.assertEqual(result, [row])
        self.assertEqual(row["publication_status"], "gemini_accept")
        self.assertEqual(row["selected_media_materialization_fingerprint"], "new-media")
        llm_mock.assert_not_called()

    def test_no_text_enters_exact_restore_queue_without_consuming_llm_budget(self) -> None:
        mod = self.mod
        first = candidate_row(text="", short_summary="")
        with (
            mock.patch.object(mod.rt, "publication_eligibility", create=True, return_value=eligibility()),
            mock.patch.object(mod, "telegram_public_text", return_value="") as fallback_mock,
            mock.patch.object(mod.rt, "call_region_talk_semantic_llm") as llm_mock,
        ):
            result = mod.verify_rows(
                [first],
                max_llm=1,
                model="gemini-test",
                default_env_var_name="KEY",
                now_iso="2026-07-10T10:00:00+00:00",
            )
        self.assertEqual(result[0]["publication_status"], "text_restore_pending")
        self.assertEqual(result[0]["publication_candidate_status"], "awaiting_text_restore")
        self.assertEqual(result[0]["finalization_status"], "retryable")
        self.assertEqual(result[0]["attempt_count"], 0)
        fallback_mock.assert_called_once()
        llm_mock.assert_not_called()

        second = candidate_row(
            text="",
            short_summary="",
            finalization_trigger=mod.finalization_trigger(result[0], now_iso="2026-07-11T10:00:00+00:00"),
        )
        self.assertEqual(second["finalization_trigger"], "retry_due")
        with (
            mock.patch.object(mod.rt, "publication_eligibility", create=True, return_value=eligibility()),
            mock.patch.object(mod, "telegram_public_text", return_value="") as fallback_again,
            mock.patch.object(mod.rt, "call_region_talk_semantic_llm") as llm_again,
        ):
            retried = mod.verify_rows(
                [second],
                max_llm=1,
                model="gemini-test",
                default_env_var_name="KEY",
                now_iso="2026-07-11T10:00:00+00:00",
            )
        self.assertEqual(retried[0]["publication_status"], "text_restore_pending")
        fallback_again.assert_called_once()
        llm_again.assert_not_called()

    def test_legacy_no_text_terminal_row_is_reopened_for_restore(self) -> None:
        self.assertEqual(
            self.mod.finalization_trigger(
                {
                    "publication_status": "no_text_for_gemini",
                    "publication_candidate_status": "filtered_before_llm",
                    "finalization_status": "terminal",
                },
                now_iso="2026-07-14T20:00:00+00:00",
            ),
            "retry_due",
        )

    def test_text_restore_handoff_preserves_active_telegram_cooldown(self) -> None:
        mod = self.mod
        captured = {}
        existing = {
            "post_link_queue_item:postlink_existing": {
                "post_link_queue_id": "postlink_existing",
                "post_url": "https://t.me/travelcase/10",
                "post_link_status": "retry_fetch",
                "attempt_count": 3,
                "fetch_error_code": "FloodWaitError",
                "next_attempt_after": "2026-07-15T02:00:00+00:00",
            }
        }

        class Pool:
            def retry_operation_sync(self, operation):
                return operation(object())

        def capture(_session, _ydb, _table, items, _now, **_kwargs):
            captured["items"] = items
            return len(items)

        row = candidate_row(
            publication_status="text_restore_pending",
            publication_candidate_status="awaiting_text_restore",
        )
        with (
            mock.patch.object(mod.rt, "utc_now_iso", return_value="2026-07-14T20:00:00+00:00"),
            mock.patch.object(mod.rt, "ydb_select_kind_items", return_value=existing),
            mock.patch.object(mod.rt, "ensure_ydb_kv_table"),
            mock.patch.object(mod.rt, "ydb_upsert_json_many", side_effect=capture),
        ):
            self.assertEqual(
                mod.write_text_restore_post_link_rows(Pool(), object(), "table", [row], run_id="run-restore"),
                1,
            )
        pk, kind, payload = captured["items"][0]
        self.assertEqual(pk, "post_link_queue_item:postlink_existing")
        self.assertEqual(kind, "post_link_queue_item")
        self.assertEqual(payload["post_link_status"], "retry_fetch")
        self.assertEqual(payload["attempt_count"], 3)
        self.assertEqual(payload["fetch_error_code"], "FloodWaitError")
        self.assertEqual(payload["next_attempt_after"], "2026-07-15T02:00:00+00:00")
        self.assertEqual(payload["priority_reason"], "publication_text_restore_after_active_payload_prune")
        self.assertEqual(payload["publication_text_restore_requested"], "true")
        self.assertEqual(payload["publication_text_restore_request_run_id"], "run-restore")

    def test_public_tme_fallback_is_default_off_and_requires_explicit_opt_in(self) -> None:
        mod = self.mod
        response = mock.Mock()
        response.text = (
            '<div class="tgme_widget_message_wrap">'
            '<div data-post="travelcase/10"></div>'
            '<div class="tgme_widget_message_text js-message_text">Exact<br>public text</div>'
        )
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(mod.PUBLIC_TME_FALLBACK_ENV, None)
            with mock.patch.object(mod.requests, "get") as get_mock:
                self.assertEqual(mod.telegram_public_text("https://t.me/travelcase/10"), "")
                get_mock.assert_not_called()

        with (
            mock.patch.dict(os.environ, {mod.PUBLIC_TME_FALLBACK_ENV: "true"}),
            mock.patch.object(mod.requests, "get", return_value=response) as get_mock,
        ):
            self.assertEqual(
                mod.telegram_public_text("https://t.me/s/TravelCase/10?single=1"),
                "Exact\npublic text",
            )
            self.assertEqual(get_mock.call_args.args[0], "https://t.me/s/travelcase/10")

    def test_existing_confirmed_row_gets_current_eligibility_attestation_without_llm(self) -> None:
        mod = self.mod
        previous = {
            "publication_status": "gemini_accept",
            "publication_candidate_status": "llm_confirmed",
        }
        row = candidate_row(finalization_trigger="", _previous_publication=previous, **previous)
        with (
            mock.patch.object(mod.rt, "publication_eligibility", create=True, return_value=eligibility("eligible")),
            mock.patch.object(mod.rt, "call_region_talk_semantic_llm") as llm_mock,
        ):
            result = mod.verify_rows(
                [row], max_llm=3, model="gemini-test", default_env_var_name="KEY",
                now_iso="2026-07-11T10:00:00+00:00",
            )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["publication_status"], "gemini_accept")
        self.assertEqual(result[0]["publication_eligibility_verdict"], "eligible")
        self.assertEqual(result[0]["llm_attempted_this_run"], "false")
        llm_mock.assert_not_called()

    def test_sent_row_refreshes_changed_source_attestation_without_llm(self) -> None:
        mod = self.mod
        source = external_source()
        previous = {
            "publication_status": "gemini_accept",
            "publication_candidate_status": "sent_to_chat",
            "sent_to_chat": "true",
            "llm_decision": "accept",
            "publication_tombstone": "false",
            "publication_revoked": "false",
            "revoked_at": "",
            "finalization_status": "terminal",
            "llm_attempted_this_run": "false",
            "next_attempt_after": "",
            "next_action": "",
            "text_restore_reason": "",
            "publication_eligibility_verdict": "eligible",
            "publication_eligibility_gate_version": "old-gate",
            "publication_eligibility_evidence": "old-evidence",
            "publication_eligibility_evidence_fingerprint": "old-evidence-fingerprint",
            "authoritative_source_fingerprint": "stale-source-fingerprint",
            "authoritative_source_fingerprint_version": mod.AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION,
            "authoritative_source_found": "true",
        }
        row = candidate_row(
            finalization_trigger="",
            _previous_publication=previous,
            _authoritative_source=source,
            **previous,
        )
        with (
            mock.patch.object(mod.rt, "publication_eligibility", create=True, return_value=eligibility("eligible")),
            mock.patch.object(mod.rt, "call_region_talk_semantic_llm") as llm_mock,
        ):
            result = mod.verify_rows(
                [row], max_llm=3, model="gemini-test", default_env_var_name="KEY",
                now_iso="2026-07-17T09:00:00+00:00",
            )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["publication_candidate_status"], "sent_to_chat")
        self.assertEqual(
            result[0]["authoritative_source_fingerprint"],
            mod.authoritative_source_fingerprint(source),
        )
        self.assertEqual(result[0]["llm_attempted_this_run"], "false")
        llm_mock.assert_not_called()

    def test_sent_row_refreshes_changed_external_intake_attestation_without_llm(self) -> None:
        mod = self.mod
        previous = {
            "publication_status": "gemini_accept",
            "publication_candidate_status": "sent_to_chat",
            "sent_to_chat": "true",
            "llm_decision": "accept",
            "finalization_status": "terminal",
            "llm_attempted_this_run": "false",
            "external_intake_fingerprint": "",
            "external_intake_revision": "",
            "external_intake_status": "new_intake",
            "external_intake_review_status": "unreviewed",
            "external_intake_publication_permission": "none",
            "external_intake_manual_review_required": "false",
        }
        row = {
            **previous,
            "external_intake_fingerprint": "current-intake-fingerprint",
            "external_intake_revision": "legacy-attestation-v1",
        }

        changed = mod._reconcile_terminal_provider_decision(
            row,
            previous,
            eligibility_verdict="eligible",
            now_iso="2026-08-02T02:00:00+00:00",
        )

        self.assertTrue(changed)
        self.assertEqual(row["publication_candidate_status"], "sent_to_chat")
        self.assertEqual(row["external_intake_fingerprint"], "current-intake-fingerprint")
        self.assertEqual(row["llm_attempted_this_run"], "false")

    def test_existing_unsent_accept_finishes_onboarding_without_repeating_gemini(self) -> None:
        mod = self.mod
        source = external_source()
        row = candidate_row(_authoritative_source=source)
        with mock.patch.object(mod.rt, "publication_eligibility", create=True, return_value=eligibility("eligible")):
            mod._eligibility_fields(row)
        previous = {
            "publication_status": "gemini_accept",
            "publication_candidate_status": "llm_confirmed",
            "sent_to_chat": "false",
            "publication_eligibility_verdict": "eligible",
            "publication_eligibility_gate_version": row["publication_eligibility_gate_version"],
            "publication_eligibility_evidence": row["publication_eligibility_evidence"],
            "authoritative_source_fingerprint": row["authoritative_source_fingerprint"],
            "authoritative_source_fingerprint_version": mod.AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION,
        }
        row.update(previous)
        row["_previous_publication"] = previous
        row["finalization_trigger"] = ""
        with (
            mock.patch.object(mod.rt, "publication_eligibility", create=True, return_value=eligibility("eligible")),
            mock.patch.object(mod.rt, "call_region_talk_semantic_llm") as llm_mock,
        ):
            result = mod.verify_rows(
                [row], max_llm=3, model="gemini-test", default_env_var_name="KEY",
                now_iso="2026-07-13T10:00:00+00:00",
            )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["publication_status"], "gemini_accept")
        self.assertEqual(result[0]["llm_attempted_this_run"], "false")
        llm_mock.assert_not_called()

    def test_strong_finalist_blocked_only_by_source_is_promoted_for_attestation(self) -> None:
        mod = self.mod
        source = {
            "_ydb_pk": "source_queue_item:telegram:travelcase",
            "canonical_source_key": "telegram:travelcase",
            "source_url": "https://t.me/travelcase",
            "source_queue_status": "processed_found_ko_candidate",
            "source_quick_class": "candidate_keep",
            "posts_scanned": 1,
            "ko_posts_found": 1,
            "candidate_posts_found": 1,
        }
        row = candidate_row(
            _authoritative_source=source,
            overall_media_score=0.78,
            postcardness_score=0.84,
            image_queue_status="actual_scored",
            image_model_input_type="actual_image",
        )
        def fake_gate(_row, authoritative_source):
            if str((authoritative_source or {}).get("source_scope") or "") == "external":
                return {"eligible": True, "primary_reason": "eligible_for_publication_verification"}
            return {"eligible": False, "primary_reason": "source_verdict_unknown"}
        with mock.patch.object(mod.rt, "publication_eligibility", side_effect=fake_gate):
            updates = mod.source_evidence_priority_updates([row], now_iso="2026-07-10T12:00:00+00:00")
        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0]["publication_source_evidence_priority"], "true")
        self.assertEqual(updates[0]["publication_source_evidence_target_posts"], 5)

        source.update(updates[0])
        with mock.patch.object(mod.rt, "publication_eligibility", side_effect=fake_gate):
            self.assertEqual(
                mod.source_evidence_priority_updates([row], now_iso="2026-07-10T13:00:00+00:00"),
                [],
            )

    def test_terminal_reject_clears_and_does_not_repromote_source_attestation(self) -> None:
        mod = self.mod
        source = {
            "_ydb_pk": "source_queue_item:telegram:hotel",
            "canonical_source_key": "telegram:hotel",
            "source_url": "https://t.me/hotel",
            "publication_source_evidence_priority": "true",
            "publication_source_evidence_post_url": "https://t.me/hotel/10",
            "posts_scanned": 1,
        }
        row = candidate_row(
            post_url="https://t.me/hotel/10",
            _authoritative_source=source,
            publication_candidate_status="llm_rejected",
            publication_status="gemini_reject",
        )
        self.assertEqual(mod.source_evidence_priority_updates([row], now_iso="2026-07-10T12:00:00+00:00"), [])
        cleared = mod.source_evidence_priority_clear_updates([row], now_iso="2026-07-10T12:00:00+00:00")
        self.assertEqual(len(cleared), 1)
        self.assertEqual(cleared[0]["publication_source_evidence_priority"], "false")
        self.assertEqual(cleared[0]["publication_source_evidence_clear_reason"], "publication_terminal_non_candidate")
        row["publication_candidate_status"] = "tombstoned_review"
        row["publication_status"] = "eligibility_review_tombstone"
        self.assertEqual(mod.source_evidence_priority_updates([row], now_iso="2026-07-10T12:00:00+00:00"), [])
        self.assertEqual(len(mod.source_evidence_priority_clear_updates([row], now_iso="2026-07-10T12:00:00+00:00")), 1)

    def test_strict_text_candidate_prioritizes_source_before_image_handoff(self) -> None:
        mod = self.mod
        source = {
            "canonical_source_key": "telegram:umka_blog",
            "source_url": "https://t.me/umka_blog",
            "source_title": "География и факты",
            "source_queue_status": "processed_found_ko_candidate",
            "posts_scanned": 1,
            "ko_posts_found": 1,
        }
        candidate = candidate_row(
            post_url="https://t.me/umka_blog/2118",
            canonical_source_key="telegram:umka_blog",
            current_stage="image_fetch_retry_needed",
            vector_gate_status="vector_accept_candidate",
            text_vector_fusion_status="fused_e5_bge_m3",
            kaliningrad_oblast_only_scope=True,
        )

        updates = mod.strict_text_candidate_source_priority_updates(
            [candidate],
            {"telegram:umka_blog": source},
            now_iso="2026-07-12T11:00:00+00:00",
        )

        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0]["publication_source_evidence_priority"], "true")
        self.assertEqual(updates[0]["publication_source_evidence_target_posts"], 5)
        self.assertEqual(updates[0]["priority_reason"], "strict_text_candidate_needs_source_attestation")

        source.update(updates[0])
        # Attestation is source-level work. Another qualifying post from the
        # same source must not rotate the priority URL and rewrite the row.
        candidate["post_url"] = "https://t.me/umka_blog/2120"
        self.assertEqual(
            mod.strict_text_candidate_source_priority_updates(
                [candidate],
                {"telegram:umka_blog": source},
                now_iso="2026-07-12T12:00:00+00:00",
            ),
            [],
        )

    def test_durable_budget_exhaustion_defers_without_gemini_call(self) -> None:
        mod = self.mod
        budget = mock.Mock()
        budget.budget_id = "unit-budget"
        budget.reserve.return_value = {"status": "exhausted"}
        row = candidate_row()
        with (
            mock.patch.object(mod, "_eligibility_fields", return_value=("eligible", {})),
            mock.patch.object(mod.rt, "call_region_talk_semantic_llm") as llm_mock,
        ):
            result = mod.verify_rows(
                [row], max_llm=100, model="gemini-test", default_env_var_name="KEY",
                now_iso="2026-07-10T12:00:00+00:00", durable_budget=budget,
            )
        self.assertEqual(result[0]["publication_candidate_status"], "llm_budget_deferred")
        self.assertEqual(result[0]["llm_budget_status"], "exhausted")
        llm_mock.assert_not_called()

    def test_completed_budget_request_replays_without_gemini_call(self) -> None:
        mod = self.mod
        budget = mock.Mock()
        budget.budget_id = "unit-budget"
        budget.reserve.return_value = {"status": "replay", "result": {"llm_gate_status": "ok", "llm_decision": "accept"}}
        row = candidate_row()
        with (
            mock.patch.object(mod, "_eligibility_fields", return_value=("eligible", {})),
            mock.patch.object(mod.rt, "call_region_talk_semantic_llm") as llm_mock,
        ):
            result = mod.verify_rows(
                [row], max_llm=100, model="gemini-test", default_env_var_name="KEY",
                now_iso="2026-07-10T12:00:00+00:00", durable_budget=budget,
            )
        self.assertEqual(result[0]["publication_candidate_status"], "llm_confirmed")
        self.assertEqual(result[0]["llm_attempted_this_run"], "false")
        llm_mock.assert_not_called()

    def test_external_manual_review_intake_never_reaches_provider_or_confirmation(self) -> None:
        mod = self.mod
        intake = {
            "external_publication_id": "extpub-manual",
            "canonical_url": "https://publisher.example/manual",
            "intake_status": "new_intake",
            "review_status": "unreviewed",
            "publication_permission": "not_granted",
            "decision": {
                "import_status": "manual_review_required",
                "downstream_readiness": "manual_review_required",
            },
        }
        row = candidate_row(
            post_url=intake["canonical_url"],
            external_publication_id="extpub-manual",
            content_origin_type="editorial_publication",
            _external_intake=intake,
            external_intake_manual_review_required="true",
        )
        with mock.patch.object(mod.rt, "call_region_talk_semantic_llm") as llm_mock:
            result = mod.verify_rows(
                [row], max_llm=1, model="gemini-test", default_env_var_name="KEY",
                now_iso="2026-08-02T10:00:00+00:00",
            )
        self.assertEqual(result[0]["publication_status"], "gemini_needs_review")
        self.assertEqual(result[0]["publication_candidate_status"], "llm_needs_review")
        self.assertNotEqual(result[0]["publication_candidate_status"], "llm_confirmed")
        llm_mock.assert_not_called()

    def test_state_change_during_mocked_llm_writes_zero_stale_accept_rows(self) -> None:
        mod = self.mod
        url = "https://t.me/travelcase/99"
        source = external_source("travelcase") | {"_ydb_pk": "source_queue_item:telegram:travelcase"}
        image = {
            "_ydb_pk": "image_queue_item:99",
            "post_url": url,
            "image_queue_status": "actual_scored",
            "image_model_input_type": "actual_image",
            "overall_media_score": 0.8,
        }
        memory = {
            "_ydb_pk": "candidate_memory_item:99",
            "post_url": url,
            "canonical_source_key": "telegram:travelcase",
            "vector_gate_status": "vector_accept_candidate",
            "text": "Проверенный рассказ о Калининградской области.",
        }
        row = candidate_row(
            post_url=url,
            _image_ydb_pk=image["_ydb_pk"],
            _memory_ydb_pk=memory["_ydb_pk"],
            _image_payload=image,
            _memory_payload=memory,
            _authoritative_source=source,
        )
        row["_live_decision_fingerprint"] = mod.live_finalization_fingerprint(
            image=image, memory=memory, intake=None, source=source, publication=None
        )
        current_memory = dict(memory)

        def provider(*_args, **_kwargs):
            current_memory["vector_gate_status"] = "vector_reject_other_region"
            return {"llm_gate_status": "ok", "llm_decision": "accept", "llm_reason": "accept"}

        with (
            mock.patch.object(mod, "_eligibility_fields", return_value=("eligible", {})),
            mock.patch.object(mod.rt, "call_region_talk_semantic_llm", side_effect=provider),
        ):
            verified = mod.verify_rows(
                [row], max_llm=1, model="gemini-test", default_env_var_name="KEY",
                now_iso="2026-08-02T10:00:00+00:00",
            )
        self.assertEqual(verified[0]["publication_status"], "gemini_accept")

        kinds = {
            "image_queue_item": {image["_ydb_pk"]: image},
            "candidate_memory_item": {memory["_ydb_pk"]: current_memory},
            "publication_candidate_item": {},
            "source_queue_item": {source["_ydb_pk"]: source},
            "source_status_item": {},
            "online_source_item": {},
            "external_publication_source_item": {},
            "external_publication_intake_item": {},
            "source_profile_capture_item": {},
            "publisher_profile_item": {},
            "publisher_profile_candidate_correction_item": {},
        }

        class Pool:
            def retry_operation_sync(self, operation):
                return operation(object())

        captured = {}
        def upsert(_session, _ydb, _table, items, _now, **_kwargs):
            captured["items"] = items
            return len(items)
        with (
            mock.patch.object(mod, "_current_complete_kind", side_effect=lambda _s, _y, _t, kind, _limit: kinds[kind]),
            mock.patch.object(mod.rt, "ensure_ydb_kv_table"),
            mock.patch.object(mod.rt, "ydb_upsert_json_many", side_effect=upsert),
        ):
            written = mod.write_publication_rows(Pool(), object(), "table", verified, "run-stale")
        self.assertEqual(written, 1)
        payload = captured["items"][0][2]
        self.assertEqual(payload["publication_status"], "gemini_needs_review")
        self.assertEqual(payload["publication_candidate_status"], "llm_needs_review")
        self.assertEqual(payload["final_decision_guard_status"], "deferred_live_state_changed")
        self.assertNotEqual(payload["publication_candidate_status"], "llm_confirmed")

    def test_final_decision_refresh_failure_raises_and_writes_nothing(self) -> None:
        mod = self.mod
        row = candidate_row(_live_decision_fingerprint="expected")

        class Pool:
            def retry_operation_sync(self, operation):
                return operation(object())

        with (
            mock.patch.object(mod, "_current_complete_kind", side_effect=RuntimeError("read timeout")),
            mock.patch.object(mod.rt, "ydb_upsert_json_many") as upsert,
        ):
            with self.assertRaisesRegex(mod.FinalDecisionRefreshError, "failed closed"):
                mod.write_publication_rows(Pool(), object(), "table", [row], "run-read-error")
        upsert.assert_not_called()

    def test_published_row_stays_immutable_and_writes_durable_conflict_audit(self) -> None:
        mod = self.mod
        url = "https://t.me/travelcase/100"
        row = candidate_row(
            post_url=url,
            _live_decision_fingerprint="old-fingerprint",
            _previous_publication={"target_publication_status": "published", "plan_status": "completed"},
        )
        kinds = {
            "image_queue_item": {},
            "candidate_memory_item": {},
            "publication_candidate_item": {},
            "source_queue_item": {},
            "source_status_item": {},
            "online_source_item": {},
            "external_publication_source_item": {},
            "external_publication_intake_item": {},
            "source_profile_capture_item": {},
            "publisher_profile_item": {},
            "publisher_profile_candidate_correction_item": {},
        }

        class Pool:
            def retry_operation_sync(self, operation):
                return operation(object())

        captured = {}

        def upsert(_session, _ydb, _table, items, _now, **_kwargs):
            captured["items"] = items
            return len(items)

        with (
            mock.patch.object(mod, "_current_complete_kind", side_effect=lambda _s, _y, _t, kind, _limit: kinds[kind]),
            mock.patch.object(mod.rt, "ensure_ydb_kv_table"),
            mock.patch.object(mod.rt, "ydb_upsert_json_many", side_effect=upsert),
        ):
            written = mod.write_publication_rows(Pool(), object(), "table", [row], "run-audit")

        self.assertEqual(written, 1)
        self.assertEqual(len(captured["items"]), 1)
        pk, kind, payload = captured["items"][0]
        self.assertTrue(pk.startswith("publication_final_decision_audit_item:rtfinalaudit_"))
        self.assertEqual(kind, "publication_final_decision_audit_item")
        self.assertEqual(payload["audit_status"], "manual_review_required")
        self.assertEqual(payload["post_url"], url)
        self.assertEqual(payload["previous_target_publication_status"], "published")

    def test_pending_candidate_correction_blocks_copy_without_downgrading_acceptance(self) -> None:
        mod = self.mod
        url = "https://rg.ru/2025/09/16/reg-szfo/example.html"
        source = {
            **external_source("unused"),
            "_ydb_pk": "external_publication_source_item:rg",
            "canonical_source_key": "web:rg.ru",
            "source_url": "https://rg.ru/",
            "source_title": "Российская газета",
        }
        row = candidate_row(
            url=url,
            canonical_source_key="web:rg.ru",
            content_origin_type="editorial_publication",
            publication_status="gemini_accept",
            publication_candidate_status="llm_confirmed",
            _authoritative_source=source,
            _previous_publication={},
        )
        row["_live_decision_fingerprint"] = mod.live_finalization_fingerprint(
            image=None, memory=None, intake=None, source=source, publication=None,
        )
        correction = {
            "_ydb_pk": "publisher_profile_candidate_correction_item:rg",
            "canonical_url": url,
            "recommended_action": "re_adjudicate_externality",
            "reason_codes": ["regional_local_edition", "local_correspondent"],
            "review_status": "unreviewed",
            "live_revalidation_status": "pending_live_revalidation",
            "candidate_mutation_allowed": False,
            "regeneration_allowed": False,
        }
        kinds = {
            "image_queue_item": {},
            "candidate_memory_item": {},
            "publication_candidate_item": {},
            "source_queue_item": {},
            "source_status_item": {},
            "online_source_item": {},
            "external_publication_source_item": {source["_ydb_pk"]: source},
            "external_publication_intake_item": {},
            "source_profile_capture_item": {},
            "publisher_profile_item": {},
            "publisher_profile_candidate_correction_item": {correction["_ydb_pk"]: correction},
        }

        class Pool:
            def retry_operation_sync(self, operation):
                return operation(object())

        captured = {}

        def upsert(_session, _ydb, _table, items, _now, **_kwargs):
            captured["items"] = items
            return len(items)

        with (
            mock.patch.object(mod, "_current_complete_kind", side_effect=lambda _s, _y, _t, kind, _limit: kinds[kind]),
            mock.patch.object(mod.rt, "ensure_ydb_kv_table"),
            mock.patch.object(mod.rt, "ydb_upsert_json_many", side_effect=upsert),
        ):
            written = mod.write_publication_rows(Pool(), object(), "table", [row], "run-correction")

        self.assertEqual(written, 1)
        _pk, kind, payload = captured["items"][0]
        self.assertEqual(kind, "publication_candidate_item")
        self.assertEqual(payload["publication_status"], "gemini_accept")
        self.assertEqual(payload["source_onboarding_status"], "needs_source_profile")
        self.assertEqual(payload["final_decision_guard_status"], "blocked_candidate_correction_review")
        self.assertEqual(payload["candidate_correction_regeneration_allowed"], "false")

    def test_explicit_fresh_evidence_override_resolves_re_adjudication_action(self) -> None:
        correction = {
            "recommended_action": "re_adjudicate_externality",
            "reason_codes": ["regional_local_edition", "local_correspondent"],
            "review_status": "retained_external",
            "live_revalidation_status": "resolved_external",
            "candidate_mutation_allowed": True,
            "regeneration_allowed": True,
            "review_id": "explicit-fresh-evidence-review",
        }
        self.assertFalse(self.mod.candidate_correction_requires_explicit_review(correction))
        correction["regeneration_allowed"] = False
        self.assertTrue(self.mod.candidate_correction_requires_explicit_review(correction))


if __name__ == "__main__":
    unittest.main()
