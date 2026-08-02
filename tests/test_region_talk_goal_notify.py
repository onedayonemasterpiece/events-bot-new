from __future__ import annotations

import importlib.util
import base64
import hashlib
import json
import os
import subprocess
import sys
import unittest
import argparse
import asyncio
import contextlib
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


def _vnext_paragraphs() -> tuple[str, str]:
    return (
        "В Гусеве строгая геометрия улиц выводит прогулку к восстановленным фрескам. "
        "Авторский канал собирает маршруты из личных наблюдений и точных деталей дороги.",
        "Автор связывает фрески с повседневным ритмом города и порядком остановок. "
        "Маршрут продолжается за центральной площадью и сохраняет ясные ориентиры прогулки.",
    )


def _vnext_profile_fields() -> dict[str, object]:
    return {
        "source_profile_fingerprint": "profile-fp",
        "source_onboarding_profile_fingerprint": "profile-fp",
        "source_onboarding_status": "ready",
        "source_onboarding_paragraph": (
            "Авторский канал собирает маршруты из личных наблюдений и точных деталей дороги."
        ),
    }


class RegionTalkGoalNotifyTests(unittest.TestCase):
    def test_functional_notifier_uses_only_role_scoped_discovery_sessions(self) -> None:
        mod = load_module()
        self.assertFalse(hasattr(mod, "decode_e2e_bundle"))
        source = MODULE_PATH.read_text(encoding="utf-8")
        self.assertNotIn("TELEGRAM_AUTH_BUNDLE_E2E", source)
        self.assertNotIn("TELEGRAM_SESSION", source)
        self.assertIn("TELEGRAM_AUTH_BUNDLE_DISCOVERY1", source)
        self.assertIn("TELEGRAM_AUTH_BUNDLE_DISCOVERY2", source)

    def test_discovery_bundle_decoder_accepts_only_base64_json_with_session(self) -> None:
        mod = load_module()
        encoded = base64.urlsafe_b64encode(json.dumps({"session": "abc"}).encode()).decode().rstrip("=")
        self.assertEqual(mod.decode_discovery_bundle(encoded)["session"], "abc")
        with self.assertRaisesRegex(RuntimeError, "no StringSession"):
            mod.decode_discovery_bundle(base64.urlsafe_b64encode(b"{}").decode())

    def test_telethon_guard_rejects_bundle_while_its_notebook_is_active(self) -> None:
        mod = load_module()
        with (
            mock.patch.dict(os.environ, {"KAGGLE_USERNAME": "operator"}, clear=False),
            mock.patch(
                "scripts.region_talk_orchestrator.read_kaggle_kernel_statuses",
                return_value={"region-talk-image-diagnostic": "RUNNING"},
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "refusing concurrent use"):
                mod.assert_telethon_transport_idle("telethon_discovery2")

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
        p1, p2 = _vnext_paragraphs()
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
            "publication_draft_status": "ready_for_operator_review",
            "publication_draft_title": "Маршрут",
            "publication_draft_source_attribution": "Авторский канал",
            "publication_draft_telegram_text": f"{p1}\n\n{p2}\n\nИсточник: Авторский канал\nОригинал: https://t.me/a/1",
            "publication_draft_vk_text": f"{p1}\n\n{p2}\n\nИсточник: Авторский канал\nОригинал: https://t.me/a/1",
            "publication_draft_fact_points_json": '[{"claim":"Факт","support_excerpt":"Опора"}]',
            "publication_draft_prompt_version": mod.EDITORIAL_WRITER_VERSION,
            "publication_draft_contract_version": mod.EDITORIAL_OUTPUT_CONTRACT,
            "publication_media_materialization_status": "fallback",
            "publication_media_materialization_contract_version": mod.MEDIA_MATERIALIZATION_CONTRACT_VERSION,
            "post_url": "https://t.me/a/1",
            **_vnext_profile_fields(),
        }
        self.assertTrue(mod.is_confirmed_publication(signed))
        self.assertTrue(mod.is_unsent_confirmed_publication(signed))
        self.assertFalse(mod.is_confirmed_publication(base))
        self.assertFalse(mod.is_confirmed_publication({**signed, "publication_eligibility_verdict": "review"}))
        self.assertFalse(mod.is_confirmed_publication({**signed, "publication_revoked": "true"}))
        self.assertFalse(mod.is_confirmed_publication({**signed, "_live_authoritative_source_fingerprint": "source-became-local"}))
        self.assertFalse(mod.is_confirmed_publication({**signed, "_live_authoritative_source_fingerprint": ""}))
        # A legacy delivery flag without a ready-draft fingerprint must not
        # hide the first actionable copy from the operator chat.
        legacy_sent = {**signed, "sent_to_chat": "true"}
        self.assertTrue(mod.is_unsent_confirmed_publication(legacy_sent))
        acknowledged = {
            **legacy_sent,
            "sent_publication_draft_fingerprint": mod.publication_draft_fingerprint(signed),
        }
        self.assertTrue(mod.is_unsent_confirmed_publication(acknowledged))
        acknowledged["sent_operator_review_fingerprint"] = mod.publication_operator_review_fingerprint(signed)
        self.assertFalse(mod.is_unsent_confirmed_publication(acknowledged))
        self.assertTrue(mod.is_unsent_confirmed_publication({
            **acknowledged,
            "publication_draft_telegram_text": acknowledged["publication_draft_telegram_text"].replace(
                "ясные ориентиры", "обновлённые ясные ориентиры"
            ),
        }))
        self.assertFalse(mod.is_unsent_confirmed_publication({**signed, "publication_draft_vk_text": ""}))

    def test_discovery_session_lease_blocks_a_second_local_owner(self) -> None:
        mod = load_module()
        import tempfile

        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch.dict(os.environ, {"REGION_TALK_TELETHON_LOCK_DIR": tmp}, clear=False),
            mock.patch.object(mod, "assert_telethon_transport_idle", return_value={}),
        ):
            with mod.discovery_session_lease("telethon_discovery2"):
                with self.assertRaisesRegex(RuntimeError, "another local Region Talk process"):
                    with mod.discovery_session_lease("telethon_discovery2"):
                        pass

    def test_v8_caption_and_media_are_one_exact_review_revision(self) -> None:
        mod = load_module()
        p1, p2 = _vnext_paragraphs()
        manifest = {
            "contract_version": mod.MEDIA_MATERIALIZATION_CONTRACT_VERSION,
            "mode": "article_hero", "status": "ready", "reason": "associated",
            "items": [{"media_id": "hero", "ordinal": 1, "kind": "image", "ref": "https://cdn.example.org/hero.jpg"}],
        }
        row = {
            "post_url": "https://example.org/article", "source_url": "https://example.org",
            "publication_draft_status": "ready_for_operator_review",
            "publication_draft_title": "Городская прогулка",
            "publication_draft_source_attribution": "Внешнее издание",
            "publication_draft_telegram_text": f"{p1}\n\n{p2}\n\nИсточник: Внешнее издание\nОригинал: https://example.org/article",
            "publication_draft_vk_text": f"{p1}\n\n{p2}\n\nИсточник: Внешнее издание\nОригинал: https://example.org/article",
            "publication_draft_fact_points_json": '[{"claim":"Факт","evidence_ids":["E1"]}]',
            "publication_draft_prompt_version": mod.EDITORIAL_WRITER_VERSION,
            "publication_draft_contract_version": mod.EDITORIAL_OUTPUT_CONTRACT,
            "publication_media_materialization_status": "ready",
            "publication_media_materialization_contract_version": mod.MEDIA_MATERIALIZATION_CONTRACT_VERSION,
            "publication_presentation_mode": "article_hero",
            "publication_presentation_manifest_json": json.dumps(manifest, ensure_ascii=False),
            **_vnext_profile_fields(),
        }
        self.assertTrue(mod.is_publication_draft_ready(row))
        caption = mod.public_caption(row, html_mode=True)
        self.assertEqual(caption.count("\n\n"), 3)
        self.assertIn('<b><a href="https://example.org/article">Подробнее — в оригинальной публикации</a></b>', caption)
        self.assertIn(
            '<b><a href="https://t.me/kalinigrad_visit">О Калининграде говорят</a></b>',
            caption,
        )
        self.assertNotIn("\nОригинал", caption)
        self.assertNotIn("https://example.org\"", caption)
        original = mod.publication_operator_review_fingerprint(row)
        self.assertEqual(
            original,
            mod.publication_operator_review_fingerprint({
                **row,
                "media_manifest_items": [{
                    "media_id": "raw-cache-only",
                    "content_sha256": "a" * 64,
                }],
                "input_media_manifest_hash": "b" * 64,
            }),
        )
        changed_media = {**manifest, "items": [{**manifest["items"][0], "ref": "https://cdn.example.org/other.jpg"}]}
        self.assertNotEqual(original, mod.publication_operator_review_fingerprint({
            **row, "publication_presentation_manifest_json": json.dumps(changed_media, ensure_ascii=False)
        }))

    def test_unversioned_draft_never_satisfies_production_readiness(self) -> None:
        mod = load_module()
        self.assertFalse(mod.is_publication_draft_ready({
            "publication_draft_status": "ready_for_operator_review",
            "publication_draft_title": "Старый черновик",
            "publication_draft_source_attribution": "Источник",
            "publication_draft_telegram_text": "Первый абзац.\n\nВторой абзац.",
            "publication_draft_vk_text": "Первый абзац.\n\nВторой абзац.",
            "publication_draft_fact_points_json": '[{"claim":"Факт"}]',
        }))

    def test_v8_media_manifest_must_be_materializable_before_review(self) -> None:
        mod = load_module()
        row = {
            "publication_presentation_mode": "social_album",
            "publication_media_materialization_status": "pending",
            "publication_presentation_manifest_json": json.dumps({
                "mode": "social_album", "status": "pending", "items": []
            }),
        }
        with self.assertRaisesRegex(RuntimeError, "exact ordered materialization manifest"):
            mod.publication_delivery_mode(row)

    def test_v8_candidate_message_does_not_count_long_hidden_href(self) -> None:
        mod = load_module()
        p1, p2 = _vnext_paragraphs()
        long_url = "https://publisher.example/article?" + "tracking=" + "x" * 240
        manifest = {
            "mode": "article_hero", "status": "ready",
            "items": [{"media_id": "hero", "ordinal": 1, "kind": "image", "ref": "https://cdn.example.org/hero.jpg"}],
        }
        row = {
            "post_url": long_url,
            "source_url": "https://publisher.example",
            "publication_draft_source_attribution": "Внешнее издание",
            "publication_draft_telegram_text": f"{p1}\n\n{p2}",
            "publication_draft_prompt_version": mod.EDITORIAL_WRITER_VERSION,
            "publication_media_materialization_status": "ready",
            "publication_presentation_mode": "article_hero",
            "publication_presentation_manifest_json": json.dumps(manifest, ensure_ascii=False),
        }
        message = mod.candidate_message(row)
        self.assertIn('<b><a href="https://publisher.example/article">', message)
        self.assertIn(">Подробнее — в оригинальной публикации</a></b>", message)
        self.assertIn('href="https://t.me/kalinigrad_visit"', message)
        self.assertNotIn("https://publisher.example\">", message)

    def test_footer_replacement_keeps_two_paragraphs_and_one_original_link(self) -> None:
        mod = load_module()
        p1, p2 = _vnext_paragraphs()
        old = (
            f"{p1}\n\n{p2}\n\n"
            "Источник: Авторский канал\nОригинал: https://t.me/a/1"
        )
        repaired = mod.replace_publication_draft_footer(old, "https://t.me/a/1")
        self.assertEqual(repaired.count("https://t.me/a/1"), 1)
        self.assertNotIn("Источник: Авторский канал", repaired)
        self.assertNotIn("Оригинал", repaired)
        self.assertTrue(repaired.endswith(
            "Подробнее — в оригинальной публикации: https://t.me/a/1\n\n"
            "О Калининграде говорят: https://t.me/kalinigrad_visit"
        ))

    def test_vnext_renderer_selects_one_source_aware_cta_and_one_channel_footer(self) -> None:
        mod = load_module()
        cases = [
            ({"content_origin_type": "social", "source_onboarding_entity_type": "person", "source_title": "Анна"}, "Подробнее — у автора Анны"),
            ({"content_origin_type": "social", "source_onboarding_entity_type": "thematic_channel", "source_title": "Море рядом"}, "Подробнее — в канале «Море рядом»"),
            ({"content_origin_type": "social", "source_profile_kind": "blog", "source_title": "Umka Blog"}, "Подробнее — в блоге Umka Blog"),
            ({"content_origin_type": "editorial_publication", "source_onboarding_entity_type": "media_brand", "source_title": "Архи.ру"}, "Подробнее — в статье на Архи.ру"),
            ({"content_origin_type": "academic_publication", "source_onboarding_entity_type": "journal", "source_title": "Крестьяноведение"}, "Подробнее — в статье журнала «Крестьяноведение»"),
            ({"content_origin_type": "social", "source_title": ""}, "Подробнее — в оригинальной публикации"),
        ]
        for fields, expected in cases:
            row = {"post_url": "https://example.org/original", **fields}
            label, url, _kind = mod.publication_source_cta(row)
            self.assertEqual(label, expected)
            self.assertEqual(url, row["post_url"])
            footer = mod.publication_footer_plain(row)
            self.assertEqual(footer.count(row["post_url"]), 1)
            self.assertEqual(footer.count(mod.REGION_TALK_PUBLIC_CHANNEL_URL), 1)
            self.assertTrue(footer.startswith(expected + ": "))

    def test_vnext_readiness_requires_profile_fingerprint_and_rejects_correction_block(self) -> None:
        mod = load_module()
        self.assertEqual(len(mod.editorial_sentences(
            "В Балтийске гавань выводит прогулку к старому маяку и длинному молу. "
            "Archi.ru объясняет архитектуру через устройство проектов."
        )), 2)
        self.assertTrue(mod.candidate_has_pending_correction({
            "externality_re_adjudication_status": "pending",
        }))
        self.assertFalse(mod.candidate_has_pending_correction({
            "externality_re_adjudication_status": "approved_external",
        }))
        resolved = {
            "externality_re_adjudication_status": "resolved_external",
            "candidate_correction_status": "retained_external",
            "candidate_correction_recommended_action": "re_adjudicate_externality",
            "candidate_correction_regeneration_allowed": "true",
            "candidate_correction_mutation_allowed": "true",
        }
        self.assertFalse(mod.candidate_has_pending_correction(resolved))

        publication = {
            "post_url": "https://rg.ru/region/example",
            **resolved,
            "externality_re_adjudication_status": "pending",
        }
        mod.attach_live_profile_and_corrections(
            [publication],
            [],
            [{
                "canonical_url": publication["post_url"],
                "review_status": "retained_external",
                "live_revalidation_status": "resolved_external",
                "recommended_action": "re_adjudicate_externality",
                "regeneration_allowed": True,
                "candidate_mutation_allowed": True,
            }],
        )
        self.assertEqual(publication["externality_re_adjudication_status"], "resolved_external")
        self.assertFalse(mod.candidate_has_pending_correction(publication))

    def test_v9_style_guard_detects_not_a_family_without_crossing_sentences(self) -> None:
        mod = load_module()
        banned = [
            "Автор видит здание не как памятник, а как живой городской маршрут.",
            "Редакция читает проект не как каталог — а как разговор о городе.",
            "Материал устроен не как справка; а как последовательность наблюдений.",
            "Текст показывает объект не как декорацию,\nа как рабочее пространство.",
            "Автор видит здание не как музей а как маршрут.",
            "Материал расположен не в г. Калининграде, а в области.",
            "Автор видит здание не как музей… а как маршрут.",
            "Автор воспринимает объект не как " + "очень подробное наблюдение " * 14 + ", а как маршрут.",
        ]
        for text in banned:
            self.assertTrue(mod.contains_contrastive_not_a_cliche(text), text)
        allowed = [
            "Автор не скрывает ограничений исследования.",
            "Это не только маршрут, но и дневник наблюдений.",
            "Автор не даёт готового ответа. А читателю оставляет исходные данные.",
            "Автор не скрывает ограничений. Исследование подробно описывает выборку, а результаты оставляет читателю.",
            "В слове «нега» нет запрещённой конструкции, а эта фраза проверяет границу слова.",
        ]
        for text in allowed:
            self.assertFalse(mod.contains_contrastive_not_a_cliche(text), text)

    def test_v9_readiness_and_caption_fail_closed_on_banned_style(self) -> None:
        mod = load_module()
        p1 = (
            "Федеральное архитектурное издание рассматривает музейный комплекс через решения "
            "проектировщиков и показывает, как посетитель проходит сквозь разные пространства здания."
        )
        p2 = (
            "Автор описывает экспозицию не как набор залов, а как цельный маршрут вокруг океана. "
            "Оригинал стоит открыть ради подробного разбора конструкций и фотографий интерьера."
        )
        manifest = {
            "contract_version": mod.MEDIA_MATERIALIZATION_CONTRACT_VERSION,
            "mode": "article_hero", "status": "ready", "reason": "associated",
            "items": [{"media_id": "hero", "ordinal": 1, "kind": "image", "ref": "https://cdn.example.org/hero.jpg"}],
        }
        row = {
            "post_url": "https://example.org/article", "source_url": "https://example.org",
            "publication_draft_status": "ready_for_operator_review",
            "publication_draft_title": "Архитектурный маршрут",
            "publication_draft_source_attribution": "Внешнее издание",
            "publication_draft_telegram_text": f"{p1}\n\n{p2}\n\nИсточник: Внешнее издание\nОригинал: https://example.org/article",
            "publication_draft_vk_text": f"{p1}\n\n{p2}\n\nИсточник: Внешнее издание\nОригинал: https://example.org/article",
            "publication_draft_fact_points_json": '[{"claim":"Факт","evidence_ids":["E1"]}]',
            "publication_draft_prompt_version": mod.EDITORIAL_WRITER_VERSION,
            "publication_draft_contract_version": mod.EDITORIAL_OUTPUT_CONTRACT,
            "publication_media_materialization_status": "ready",
            "publication_media_materialization_contract_version": mod.MEDIA_MATERIALIZATION_CONTRACT_VERSION,
            "publication_presentation_mode": "article_hero",
            "publication_presentation_manifest_json": json.dumps(manifest, ensure_ascii=False),
        }
        self.assertFalse(mod.is_publication_draft_ready(row))
        with self.assertRaisesRegex(RuntimeError, "contrastive_not_a_cliche"):
            mod.public_caption(row)

    def test_article_readiness_requires_grounded_publisher_reader_brief(self) -> None:
        mod = load_module()
        p1, p2 = _vnext_paragraphs()
        dimensions = {
            key: {"text": key, "evidence_ids": ["E1"]}
            for key in mod.PUBLISHER_READER_BRIEF_DIMENSIONS
        }
        row = {
            "content_origin_type": "editorial_publication",
            "post_url": "https://example.org/article",
            "source_url": "https://example.org",
            "source_title": "Архитектурное издание",
            "publication_draft_status": "ready_for_operator_review",
            "publication_draft_title": "Музейный маршрут",
            "publication_draft_source_attribution": "Архитектурное издание",
            "publication_draft_telegram_text": f"{p1}\n\n{p2}\n\nИсточник: Архитектурное издание\nОригинал: https://example.org/article",
            "publication_draft_vk_text": f"{p1}\n\n{p2}\n\nИсточник: Архитектурное издание\nОригинал: https://example.org/article",
            "publication_draft_fact_points_json": '[{"claim":"Факт","evidence_ids":["E1"]}]',
            "publication_draft_prompt_version": mod.EDITORIAL_WRITER_VERSION,
            "publication_draft_contract_version": mod.EDITORIAL_OUTPUT_CONTRACT,
            "publication_media_materialization_status": "ready",
            "publication_media_materialization_contract_version": mod.MEDIA_MATERIALIZATION_CONTRACT_VERSION,
            "source_onboarding_status": "ready",
            "source_onboarding_paragraph": "Краткая доказательная сводка об издании.",
            "source_onboarding_publisher_dimensions_status": "ready",
            "source_onboarding_publisher_dimensions_json": json.dumps(dimensions, ensure_ascii=False),
            "source_onboarding_summary_kind": mod.PUBLISHER_READER_BRIEF_KIND,
            "source_profile_fingerprint": "profile-fp",
            "source_onboarding_profile_fingerprint": "profile-fp",
        }
        self.assertTrue(mod.is_publication_draft_ready(row))
        row.pop("source_onboarding_publisher_dimensions_json")
        self.assertFalse(mod.is_publication_draft_ready(row))

    def test_reviewed_media_digest_is_verified_before_delivery(self) -> None:
        mod = load_module()
        data = b"exact-reviewed-source-media"
        item = {"reviewed_content_sha256": hashlib.sha256(data).hexdigest()}
        mod.verify_reviewed_media_digest(data, item)
        with self.assertRaisesRegex(RuntimeError, "differs from reviewed_content_sha256"):
            mod.verify_reviewed_media_digest(b"changed-source-media", item)
        with self.assertRaisesRegex(RuntimeError, "invalid reviewed_content_sha256"):
            mod.verify_reviewed_media_digest(data, {"reviewed_content_sha256": "not-a-sha"})

    def test_direct_image_refetch_reproduces_image_diagnostic_negotiation(self) -> None:
        mod = load_module()

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def read(self, _limit):
                return b"reviewed-webp-bytes"

        with mock.patch.object(mod.urllib.request, "urlopen", return_value=Response()) as urlopen:
            result = mod.download_reviewed_direct_media(
                "https://publisher.example/hero.jpg", kind="image"
            )

        self.assertEqual(result, b"reviewed-webp-bytes")
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "https://publisher.example/hero.jpg")
        self.assertEqual(
            request.get_header("Accept"),
            "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        )
        self.assertEqual(
            request.get_header("User-agent"),
            "Mozilla/5.0 RegionTalkImageDiagnostic/1.0",
        )

    def test_manifest_message_id_uses_exact_numeric_suffix(self) -> None:
        mod = load_module()
        self.assertEqual(mod.manifest_item_message_id({"media_id": "telegram:110"}), 110)
        self.assertEqual(mod.manifest_item_message_id({"media_id": "tg:10"}), 10)
        self.assertIsNone(mod.manifest_item_message_id({"media_id": "frame:hero"}))
        self.assertIsNone(mod.manifest_item_message_id({"media_id": "hero:1"}))

    def test_source_album_locator_is_bounded_to_six_in_original_order(self) -> None:
        mod = load_module()

        class Message:
            def __init__(self, message_id: int, grouped_id: int = 77):
                self.id = message_id
                self.grouped_id = grouped_id
                self.media = object()

        class Client:
            async def get_messages(self, _handle, ids):
                if isinstance(ids, int):
                    return Message(ids)
                return [Message(message_id) for message_id in ids]

        result = asyncio.run(mod._telegram_source_media(
            Client(),
            "https://t.me/example/100",
            [],
            max_items=6,
        ))
        self.assertEqual([message.id for message in result], [90, 91, 92, 93, 94, 95])

    def test_grouped_source_video_locator_is_bounded_to_anchor_item(self) -> None:
        mod = load_module()

        class Message:
            def __init__(self, message_id: int, grouped_id: int = 77):
                self.id = message_id
                self.grouped_id = grouped_id
                self.media = object()

        class Client:
            async def get_messages(self, _handle, ids):
                if isinstance(ids, int):
                    return Message(ids)
                return [Message(message_id) for message_id in ids]

        result = asyncio.run(mod._telegram_source_media(
            Client(), "https://t.me/example/100", [], max_items=1,
        ))
        self.assertEqual([message.id for message in result], [100])

    def test_reviewed_album_ids_keep_their_selected_order(self) -> None:
        mod = load_module()

        class Message:
            def __init__(self, message_id: int):
                self.id = message_id
                self.media = object()

        class Client:
            async def get_messages(self, _handle, ids):
                return [Message(message_id) for message_id in ids]

        result = asyncio.run(mod._telegram_source_media(
            Client(),
            "https://t.me/example/100",
            ["telegram:100", "telegram:109", "telegram:102"],
            max_items=6,
        ))
        self.assertEqual([message.id for message in result], [100, 109, 102])

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
        draft = {
            "publication_draft_status": "ready_for_operator_review",
            "publication_draft_title": "Маршрут",
            "publication_draft_source_attribution": "TravelCase",
            "publication_draft_telegram_text": "Текст для Telegram",
            "publication_draft_vk_text": "Текст для VK",
            "publication_draft_fact_points_json": '[{"claim":"Факт"}]',
            "publication_draft_prompt_version": "draft-v1",
        }
        first = {**draft, "post_url": "https://telegram.me/TravelCase/10?single=1"}
        second = {**draft, "post_url": "https://t.me/travelcase/10/"}
        key1 = mod.publication_delivery_key(first, "-100123")
        key2 = mod.publication_delivery_key(second, "-100123")
        self.assertEqual(key1, key2)
        self.assertEqual(mod.delivery_random_id(key1), mod.delivery_random_id(key1))
        self.assertNotEqual(key1, mod.publication_delivery_key(second, "-100999"))
        self.assertNotEqual(key1, mod.publication_delivery_key({
            **second,
            "publication_draft_telegram_text": "Исправленный текст",
        }, "-100123"))

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

    def test_dry_run_uses_bot_api_contract_without_connecting(self) -> None:
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
            transport="bot_api",
        )
        result = asyncio.run(mod.send_rows(args))
        self.assertTrue(result["ok"])
        self.assertTrue(result["dry_run"])
        self.assertEqual(result["transport"], "bot_api")

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

    def test_bot_api_existing_review_revision_fails_closed_before_send(self) -> None:
        mod = load_module()
        args = argparse.Namespace(expected_chat_id="-100123", chat="", transport="bot_api")
        calls = []

        def fake_call(_token, method, payload):
            calls.append((method, payload))
            if method == "getMe":
                return {"id": 99, "username": "region_bot"}
            if method == "getChat":
                return {"id": -100123, "title": "Region Talk"}
            raise AssertionError("a revised existing candidate must not be sent again")

        mod._bot_api_call = fake_call
        mod.read_delivery = lambda *_args: {}
        mod.upsert_delivery = lambda *_args: (_ for _ in ()).throw(
            AssertionError("blocked revision must not create a delivery row")
        )
        row = {
            "post_url": "https://t.me/example/1",
            "sent_message_id": "777",
            "sent_chat_id": "-100123",
            "delivery_key": "prior-revision-key",
        }
        with (
            mock.patch.dict(os.environ, {"TELEGRAM_BOT_TOKEN": "token"}, clear=False),
            self.assertRaisesRegex(RuntimeError, "cannot safely replace"),
        ):
            asyncio.run(mod.send_rows_bot_api(
                args,
                messages=["candidate"],
                rows=[row],
                ydb=object(),
                driver=object(),
                pool=object(),
                table="table",
            ))

        self.assertEqual([method for method, _payload in calls], ["getMe", "getChat"])

    def test_telethon_delivery_reuses_stable_random_id_and_persists_candidate(self) -> None:
        mod = load_module()
        args = argparse.Namespace(expected_chat_id="-100123", chat="", transport="telethon_discovery2")
        persisted = []
        requests = []

        class Update:
            random_id = 4242
            id = 777

        class Result:
            updates = [Update()]

        class Client:
            async def __call__(self, request):
                requests.append(request)
                return Result()

            async def disconnect(self):
                return None

        async def fake_client_and_chat(_args):
            return Client(), object(), "-100123", "55"

        mod._telethon_client_and_chat = fake_client_and_chat
        mod.discovery_session_lease = lambda _transport: contextlib.nullcontext({})
        mod.read_delivery = lambda *_args: {"status": "sending", "random_id": "4242"}
        mod.upsert_delivery = lambda *_args: persisted.append(("delivery", _args[-1]))
        mod.upsert_sent = lambda *_args, **_kwargs: persisted.append(("sent", _kwargs))
        row = {"post_url": "https://t.me/example/1"}
        result = asyncio.run(mod.send_rows_telethon(
            args,
            messages=["candidate"],
            rows=[row],
            ydb=object(),
            driver=object(),
            pool=object(),
            table="table",
        ))

        self.assertEqual(result["sent_count"], 1)
        self.assertEqual(result["transport"], "telethon_discovery2")
        self.assertEqual(requests[0].random_id, 4242)
        self.assertFalse(requests[0].no_webpage)
        self.assertEqual([item[1]["status"] for item in persisted if item[0] == "delivery"], ["sending", "delivered"])

    def test_telethon_pending_editorial_revision_edits_prior_message_in_place(self) -> None:
        mod = load_module()
        args = argparse.Namespace(expected_chat_id="-100123", chat="", transport="telethon_discovery2")
        persisted = []
        edits = []
        sent_over_network = []
        row = {
            "post_url": "https://example.org/article",
            "publication_draft_prompt_version": mod.EDITORIAL_WRITER_VERSION,
            "publication_draft_title": "Заголовок",
            "publication_draft_source_attribution": "Издание",
            "publication_draft_telegram_text": "Первый абзац.\n\nВторой абзац.",
            "publication_draft_vk_text": "Первый абзац.\n\nВторой абзац.",
            "publication_presentation_mode": "article_hero",
            "publication_presentation_manifest_json": json.dumps({
                "status": "ready", "mode": "article_hero", "items": [{"media_id": "hero:1"}],
            }),
            "sent_message_id": "321",
            "sent_chat_id": "-100123",
            "delivery_key": "prior-key",
        }
        current_manifest = mod.publication_delivery_review_fields(row)["operator_review_media_manifest_json"]

        class Message:
            message = "verified"
            entities = []

        class Client:
            async def edit_message(self, _peer, message_id, caption, **kwargs):
                edits.append((message_id, caption, kwargs))

            async def get_messages(self, _peer, ids):
                return Message()

            async def __call__(self, request):
                sent_over_network.append(request)
                raise AssertionError("a pending revision must not create a new message")

            async def disconnect(self):
                return None

        async def fake_client_and_chat(_args):
            return Client(), object(), "-100123", "55"

        async def fake_reactions(_client, _peer, _message_id):
            return {
                "operator_review_decision": "pending",
                "operator_review_rewrite_status": "clean",
            }

        def fake_read_delivery(_pool, _ydb, _table, key):
            if key == "prior-key":
                legacy_manifest = json.loads(current_manifest)
                legacy_manifest.update({
                    "media_manifest_items": [{
                        "media_id": "raw-cache-only",
                        "content_sha256": "a" * 64,
                    }],
                    "input_media_manifest_hash": "b" * 64,
                })
                return {
                    "status": "delivered",
                    "post_url": "https://example.org/article",
                    "message_id": "321",
                    "delivered_at": "2026-08-02T10:00:00+00:00",
                    "operator_review_media_manifest_json": json.dumps(
                        legacy_manifest, sort_keys=True, separators=(",", ":")
                    ),
                }
            return {}

        mod._telethon_client_and_chat = fake_client_and_chat
        mod.fetch_pending_revision_reactions = fake_reactions
        mod.telegram_message_matches_public_caption = lambda _message, _row: True
        mod.public_caption = lambda _row, html_mode=False: "current caption"
        mod.discovery_session_lease = lambda _transport: contextlib.nullcontext({})
        mod.read_delivery = fake_read_delivery
        mod.upsert_delivery = lambda *_args: persisted.append(("delivery", _args[-2], _args[-1]))
        mod.upsert_sent = lambda *_args, **kwargs: persisted.append(("sent", kwargs))

        result = asyncio.run(mod.send_rows_telethon(
            args,
            messages=["unused"],
            rows=[row],
            ydb=object(),
            driver=object(),
            pool=object(),
            table="table",
        ))

        self.assertEqual(result["sent_count"], 1)
        self.assertTrue(result["sent"][0]["edited_in_place"])
        self.assertEqual(result["sent"][0]["message_id"], 321)
        self.assertEqual(len(edits), 1)
        self.assertEqual(sent_over_network, [])
        self.assertEqual([item[0] for item in persisted], ["delivery", "delivery", "sent"])

    def test_editorial_media_failure_is_recorded_and_later_rows_continue(self) -> None:
        mod = load_module()
        args = argparse.Namespace(expected_chat_id="-100123", chat="", transport="telethon_discovery2")
        persisted = []

        class Client:
            async def __call__(self, request):
                class Update:
                    random_id = request.random_id
                    id = 778
                class Result:
                    updates = [Update()]
                return Result()

            async def disconnect(self):
                return None

        async def fake_client_and_chat(_args):
            return Client(), object(), "-100123", "55"

        async def fail_materialization(_client, _row):
            raise RuntimeError("source album unavailable")

        mod._telethon_client_and_chat = fake_client_and_chat
        mod.materialize_telethon_media = fail_materialization
        mod.public_caption = lambda _row, html_mode=False: "validated caption"
        mod.discovery_session_lease = lambda _transport: contextlib.nullcontext({})
        mod.read_delivery = lambda *_args: {}
        mod.upsert_delivery = lambda *_args: persisted.append(_args[-1])
        mod.upsert_sent = lambda *_args, **_kwargs: None
        failed_row = {
            "post_url": "https://t.me/example/100",
            "publication_draft_prompt_version": mod.EDITORIAL_WRITER_VERSION,
            "publication_presentation_mode": "social_album",
            "publication_media_materialization_status": "ready",
            "publication_presentation_manifest_json": json.dumps({
                "mode": "social_album",
                "status": "ready",
                "items": [{
                    "media_id": "source:album",
                    "ordinal": 1,
                    "kind": "image",
                    "ref": "https://t.me/example/100",
                }],
            }),
        }
        plain_row = {"post_url": "https://t.me/example/101"}
        result = asyncio.run(mod.send_rows_telethon(
            args,
            messages=["candidate", "next candidate"],
            rows=[failed_row, plain_row],
            ydb=object(),
            driver=object(),
            pool=object(),
            table="table",
        ))
        self.assertFalse(result["ok"])
        self.assertTrue(result["partial"])
        self.assertEqual(result["sent_count"], 1)
        self.assertEqual(result["failed_count"], 1)
        self.assertIn("source album unavailable", result["failed"][0]["reason"])
        self.assertEqual(persisted[0]["status"], "materialization_failed")
        self.assertEqual(persisted[0]["delivery_stage"], "pre_send_media_materialization")


if __name__ == "__main__":
    unittest.main()
