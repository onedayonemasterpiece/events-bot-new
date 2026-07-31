from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import unittest
import argparse
import asyncio
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "region_talk_goal_notify.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_goal_notify", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class RegionTalkGoalNotifyTests(unittest.TestCase):
    def test_yc_fallback_is_bounded_when_interactive_auth_is_required(self) -> None:
        mod = load_module()
        env = {
            "REGION_TALK_YDB_ENDPOINT": "",
            "REGION_TALK_YDB_DATABASE": "",
            "REGION_TALK_YC_CLI_TIMEOUT_SECONDS": "7",
        }
        with (
            mock.patch.dict(os.environ, env, clear=False),
            mock.patch.object(mod.Path, "exists", return_value=True),
            mock.patch.object(
                mod.subprocess,
                "check_output",
                side_effect=subprocess.TimeoutExpired(cmd=["yc"], timeout=7),
            ) as check_output,
        ):
            with self.assertRaisesRegex(RuntimeError, "CLI timed out"):
                mod.ydb_endpoint_database(allow_yc_fallback=True)
            self.assertEqual(check_output.call_args.kwargs["timeout"], 7)

    def test_yc_token_mint_is_bounded(self) -> None:
        mod = load_module()
        env = {
            "REGION_TALK_YDB_IAM_TOKEN": "",
            "YC_IAM_TOKEN": "",
            "YDB_ACCESS_TOKEN": "",
            "REGION_TALK_YC_CLI_TIMEOUT_SECONDS": "9",
        }
        with (
            mock.patch.dict(os.environ, env, clear=False),
            mock.patch.object(mod.Path, "exists", return_value=True),
            mock.patch.object(
                mod.subprocess,
                "check_output",
                side_effect=subprocess.TimeoutExpired(cmd=["yc"], timeout=9),
            ) as check_output,
        ):
            with self.assertRaisesRegex(RuntimeError, "interactive browser authentication"):
                mod.ydb_token(allow_yc_fallback=True)
            self.assertEqual(check_output.call_args.kwargs["timeout"], 9)

    def test_delivery_batch_limit_does_not_truncate_publication_ledger_scan(self) -> None:
        mod = load_module()
        self.assertEqual(mod.publication_scan_limit(1), 5000)
        self.assertEqual(mod.publication_scan_limit(20), 5000)
        self.assertEqual(mod.publication_scan_limit(2000), 10000)

    def test_only_current_eligibility_attested_confirmed_rows_are_sendable(self) -> None:
        mod = load_module()
        base = {
            "publication_candidate_status": "llm_confirmed",
            "publication_status": "gemini_accept",
            "sent_to_chat": "false",
        }
        signed = {
            **base,
            "publication_eligibility_verdict": "eligible",
            "publication_eligibility_gate_version": mod.PUBLICATION_ELIGIBILITY_GATE_VERSION,
            "authoritative_source_fingerprint_version": mod.AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION,
            "authoritative_source_fingerprint": "current-source-fingerprint",
            "_live_authoritative_source_fingerprint": "current-source-fingerprint",
        }
        self.assertTrue(mod.is_confirmed_publication(signed))
        self.assertTrue(mod.is_unsent_confirmed_publication(signed))
        self.assertFalse(mod.is_confirmed_publication(base))
        self.assertFalse(mod.is_confirmed_publication({**signed, "publication_eligibility_verdict": "review"}))
        self.assertFalse(mod.is_confirmed_publication({**signed, "publication_revoked": "true"}))
        self.assertFalse(mod.is_confirmed_publication({**signed, "_live_authoritative_source_fingerprint": "source-became-local"}))
        self.assertFalse(mod.is_confirmed_publication({**signed, "_live_authoritative_source_fingerprint": ""}))
        self.assertFalse(mod.is_unsent_confirmed_publication({**signed, "sent_to_chat": "true"}))

    def test_source_fingerprint_changes_when_source_classification_changes(self) -> None:
        mod = load_module()
        external = {
            "canonical_source_key": "telegram:travelcase",
            "source_queue_status": "processed_found_ko_candidate",
            "source_scope": "external",
            "source_geo_class": "nonlocal_russia",
            "posts_scanned": 1,
            "ko_posts_found": 1,
            "candidate_posts_found": 1,
            "queue_item_updated_at": "2026-07-10T00:00:00+00:00",
        }
        local = {
            **external,
            "source_queue_status": "rejected_local_region_source",
            "source_scope": "local_region",
            "source_geo_class": "kaliningrad_local",
            "queue_item_updated_at": "2026-07-10T01:00:00+00:00",
        }
        self.assertNotEqual(
            mod.authoritative_source_fingerprint(external),
            mod.authoritative_source_fingerprint(local),
        )
        self.assertEqual(
            mod.authoritative_source_fingerprint(external),
            mod.authoritative_source_fingerprint({**external, "queue_item_updated_at": "2026-07-11T00:00:00+00:00"}),
        )
        self.assertEqual(
            mod.authoritative_source_fingerprint(external),
            mod.authoritative_source_fingerprint({**external, "posts_scanned": 9, "ko_posts_found": 2}),
        )

    def test_live_source_merge_does_not_erase_queue_counters_with_status_overlay(self) -> None:
        mod = load_module()
        publication = {"canonical_source_key": "telegram:twodaystrip"}
        queue = {
            "canonical_source_key": "telegram:twodaystrip",
            "source_queue_status": "processed_found_ko_candidate",
            "posts_scanned": 13,
            "ko_posts_found": 3,
            "candidate_posts_found": 7,
        }
        expected = mod.authoritative_source_fingerprint(queue)
        mod.attach_live_source_fingerprints([publication], [
            queue,
            {
                "canonical_source_key": "telegram:twodaystrip",
                "fetch_status": "ok",
                "posts_scanned": 13,
                "ko_posts_found": 0,
                "candidate_posts_found": 0,
            },
            {
                "canonical_source_key": "telegram:twodaystrip",
                "posts_scanned": 0,
                "ko_posts_found": 0,
                "candidate_posts_found": 0,
            },
        ])
        self.assertEqual(publication["_live_authoritative_source_fingerprint"], expected)

    def test_stale_terminal_status_cannot_override_newer_queue_repair(self) -> None:
        mod = load_module()
        publication = {"canonical_source_key": "telegram:figarotravel"}
        queue = {
            "_ydb_pk": "source_queue_item:telegram:figarotravel",
            "canonical_source_key": "telegram:figarotravel",
            "source_queue_status": "processed_found_ko_candidate",
            "source_scope": "external",
            "source_geo_class": "external",
            "source_quick_class": "candidate_keep",
            "source_surface_filter_version": "source_surface_v2026_07_17_spam_evidence_v3",
            "updated_at": "2026-07-17T09:00:00+00:00",
        }
        stale_status = {
            "_ydb_pk": "source_status_item:telegram:figarotravel",
            "canonical_source_key": "telegram:figarotravel",
            "source_queue_status": "rejected_spam_source",
            "source_quick_class": "spam_source_reject",
            "updated_at": "2026-07-17T08:00:00+00:00",
        }
        stale_online = {
            **stale_status,
            "_ydb_pk": "online_source_item:telegram:figarotravel",
            "updated_at": "2026-07-17T08:30:00+00:00",
        }
        merged = mod.merge_live_source_rows([queue, stale_status, stale_online])
        self.assertEqual(merged[0]["source_queue_status"], "processed_found_ko_candidate")
        mod.attach_live_source_fingerprints([publication], [queue, stale_status, stale_online])
        self.assertEqual(
            publication["_live_authoritative_source_fingerprint"],
            mod.authoritative_source_fingerprint(queue),
        )

    def test_delivery_identity_is_stable_per_canonical_post_and_chat(self) -> None:
        mod = load_module()
        first = {"post_url": "https://telegram.me/TravelCase/10?single=1"}
        second = {"post_url": "https://t.me/travelcase/10/"}
        key1 = mod.publication_delivery_key(first, "-100123")
        key2 = mod.publication_delivery_key(second, "-100123")
        self.assertEqual(key1, key2)
        self.assertEqual(mod.delivery_random_id(key1), mod.delivery_random_id(key1))
        self.assertNotEqual(key1, mod.publication_delivery_key(second, "-100999"))

    def test_video_candidate_message_does_not_claim_visual_score(self) -> None:
        mod = load_module()
        message = mod.candidate_message({
            "publication_rank": 1,
            "post_url": "https://t.me/travel/1",
            "media_kind": "video",
            "media_review_mode": "operator_video_review",
            "llm_reason": "Текст подходит",
        })
        self.assertIn("требуется ручной просмотр", message)
        self.assertIn("текст прошёл строгую E5+BGE", message)
        self.assertNotIn("визуальному score", message)

    def test_candidate_message_includes_only_ready_source_onboarding(self) -> None:
        mod = load_module()
        ready = mod.candidate_message({
            "publication_rank": 2,
            "post_url": "https://t.me/travel/2",
            "source_onboarding_status": "ready",
            "source_onboarding_paragraph": "Проверенный вводный абзац о тревел-блогере.",
        })
        self.assertIn("О блогере: Проверенный вводный абзац", ready)
        review = mod.candidate_message({
            "publication_rank": 3,
            "post_url": "https://t.me/travel/3",
            "source_onboarding_status": "needs_review",
            "source_onboarding_paragraph": "Непроверенный текст",
        })
        self.assertNotIn("О блогере:", review)

    def test_editorial_candidate_message_includes_scores_and_publication_label(self) -> None:
        mod = load_module()
        message = mod.candidate_message({
            "publication_rank": 4,
            "post_url": "https://example.org/article",
            "source_url": "https://example.org",
            "content_origin_type": "editorial_publication",
            "publication_pre_score": 0.81,
            "overall_media_score": 0.74,
            "postcardness_score": 0.77,
            "source_onboarding_status": "ready",
            "source_onboarding_paragraph": "Федеральное издание о культуре и науке.",
        })
        self.assertIn("Оценка: итог 0.81", message)
        self.assertIn("О публикации:", message)
        self.assertIn("Источник: https://example.org", message)
        self.assertNotIn("О блогере:", message)

    def test_publication_reader_joins_external_publisher_attestation(self) -> None:
        mod = load_module()
        publication = {
            "post_url": "https://publisher.example/article",
            "canonical_source_key": "web:publisher.example",
            "publication_status": "gemini_accept",
            "publication_candidate_status": "llm_confirmed",
        }
        publisher = {
            "canonical_source_key": "web:publisher.example",
            "source_url": "https://publisher.example",
            "source_scope": "external",
            "source_geo_class": "nonlocal_russia",
            "source_queue_status": "confirmed_external_publication_research",
        }
        calls: list[str] = []

        def fake_read(_pool, _ydb, _table, kind, _limit):
            calls.append(kind)
            if kind == "publication_candidate_item":
                return [dict(publication)]
            if kind == "external_publication_source_item":
                return [dict(publisher)]
            return []

        mod.ensure_ydb_module = lambda: object()
        mod.ydb_endpoint_database = lambda: ("endpoint", "database")
        mod.ydb_credentials = lambda _ydb: object()
        mod.ydb_table_path = lambda _database: "table"
        mod.read_kind_rows = fake_read

        class Driver:
            def wait(self, **_kwargs):
                return None

        class Ydb:
            Driver = staticmethod(lambda **_kwargs: Driver())
            SessionPool = staticmethod(lambda _driver: object())

        mod.ensure_ydb_module = lambda: Ydb
        _ydb, _driver, _pool, _table, rows = mod.read_publication_rows(3)

        self.assertIn("external_publication_source_item", calls)
        self.assertEqual(
            rows[0]["_live_authoritative_source_fingerprint"],
            mod.authoritative_source_fingerprint(publisher),
        )

    def test_candidate_message_preserves_zero_scores(self) -> None:
        mod = load_module()
        message = mod.candidate_message({
            "post_url": "https://example.org/article",
            "publication_score": 0,
            "overall_media_score": 0,
            "postcardness_score": 0,
        })
        self.assertIn("Оценка: итог 0 · изображение 0 · открыточность 0", message)

    def test_candidate_message_accepts_nested_external_intake_fields(self) -> None:
        mod = load_module()
        message = mod.candidate_message({
            "canonical_url": "https://example.org/article",
            "content_origin_type": "academic_publication",
            "publication": {"source_domain": "example.org"},
            "quality_assessment": {"track": "scholarly", "normalized_score": 0.79},
            "editorial_pack": {
                "source_overview": "Нерегиональный научный журнал.",
                "teaser": "Исследование объясняет заметный природный процесс.",
                "why_selected": "Есть понятное научно-популярное зерно.",
            },
            "decision": {"reason_short": "Подтверждено первичной страницей."},
        })
        self.assertIn("https://example.org/article", message)
        self.assertIn("Источник: https://example.org", message)
        self.assertIn("Оценка: итог 0.79", message)
        self.assertIn("О публикации: Нерегиональный научный журнал.", message)
        self.assertIn("Кратко: Исследование объясняет", message)

    def test_candidate_message_includes_ready_grounded_telegram_draft(self) -> None:
        mod = load_module()
        message = mod.candidate_message({
            "post_url": "https://example.org/article",
            "publication_draft_status": "ready_for_operator_review",
            "publication_draft_telegram_text": "Короткий фактический текст.\n\nОригинал: https://example.org/article",
        })
        self.assertIn("📝 Черновик для Telegram", message)
        self.assertIn("Короткий фактический текст", message)

    def test_latest_bge_vector_is_attached_by_canonical_url(self) -> None:
        mod = load_module()
        publications = [{"post_url": "https://example.org/a/"}]
        vectors = [
            {
                "post_url": "https://example.org/a",
                "model_id": "BAAI/bge-m3",
                "model_short": "bge_m3",
                "created_at": "2026-07-18T00:00:00Z",
                "embedding_vector_f16_b64": "old",
                "embedding_vector_encoding": "f16_le_base64",
                "embedding_dim": 2,
                "encoder_contract": "v1",
            },
            {
                "post_url": "https://example.org/a",
                "model_id": "BAAI/bge-m3",
                "model_short": "bge_m3",
                "created_at": "2026-07-19T00:00:00Z",
                "embedding_vector_f16_b64": "new",
                "embedding_vector_encoding": "f16_le_base64",
                "embedding_dim": 2,
                "encoder_contract": "v1",
            },
        ]
        mod.attach_latest_bge_vectors(publications, vectors)
        self.assertEqual(publications[0]["embedding_vector_f16_b64"], "new")

    def test_dry_run_never_decodes_or_connects_human_session(self) -> None:
        mod = load_module()
        args = argparse.Namespace(
            stats=False,
            message="diagnostic",
            queue=False,
            limit=20,
            vector_scan_limit=100,
            history_limit=100,
            diversity_weight=0.28,
            adjacency_threshold=0.86,
            dry_run=True,
            expected_chat_id="-100123",
            transport="telethon",
        )
        with mock.patch.object(mod, "decode_e2e_bundle", side_effect=AssertionError("must not connect")):
            result = asyncio.run(mod.send_rows(args))
        self.assertTrue(result["ok"])
        self.assertTrue(result["dry_run"])
        self.assertEqual(result["transport"], "telethon")

    def test_bot_api_delivery_uses_bot_and_persists_candidate(self) -> None:
        mod = load_module()
        args = argparse.Namespace(expected_chat_id="-100123", chat="", transport="bot_api")
        calls = []
        persisted = []

        def fake_call(_token, method, payload):
            calls.append((method, payload))
            if method == "getMe":
                return {"id": 99, "username": "region_bot"}
            if method == "getChat":
                return {"id": -100123, "title": "Region Talk"}
            return {"message_id": 777}

        mod._bot_api_call = fake_call
        mod.read_delivery = lambda *_args: {}
        mod.upsert_delivery = lambda *_args: persisted.append(("delivery", _args[-1]))
        mod.upsert_sent = lambda *_args, **_kwargs: persisted.append(("sent", _kwargs))
        row = {"post_url": "https://t.me/example/1"}
        with mock.patch.dict(os.environ, {"TELEGRAM_BOT_TOKEN": "token"}, clear=False):
            result = asyncio.run(mod.send_rows_bot_api(
                args,
                messages=["candidate"],
                rows=[row],
                ydb=object(),
                driver=object(),
                pool=object(),
                table="table",
            ))

        self.assertEqual(result["sent_count"], 1)
        self.assertEqual(result["transport"], "bot_api")
        self.assertEqual([method for method, _payload in calls], ["getMe", "getChat", "sendMessage"])
        self.assertEqual(calls[-1][1], {"chat_id": "-100123", "text": "candidate"})
        self.assertEqual([item[1]["status"] for item in persisted if item[0] == "delivery"], ["sending", "delivered"])


if __name__ == "__main__":
    unittest.main()
