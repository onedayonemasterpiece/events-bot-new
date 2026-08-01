from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "region_talk_publication_draft_backfill.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_publication_draft_backfill", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_backfill_source_never_mentions_local_e2e_sessions() -> None:
    source = MODULE_PATH.read_text(encoding="utf-8")
    assert "TELEGRAM_AUTH_BUNDLE_E2E" not in source
    assert "TELEGRAM_SESSION" not in source


def test_telegram_post_ref_is_exact_and_public() -> None:
    mod = load_module()
    assert mod.telegram_post_ref("https://t.me/s/TravelCase/123?single=1") == ("travelcase", 123)
    assert mod.telegram_post_ref("https://t.me/travelcase") is None
    assert mod.telegram_post_ref("https://vk.com/wall-1_2") is None
    assert mod.social_post_surface("https://vk.com/wall-1_2") == "vk"


def test_selection_skips_ready_terminal_and_future_retry_rows() -> None:
    mod = load_module()
    now = datetime(2026, 7, 31, 20, 0, tzinfo=timezone.utc)
    rows = [
        {"post_url": "https://t.me/source/1", "publication_rank": 2},
        {
            "post_url": "https://t.me/source/2",
            "publication_rank": 1,
            "publication_draft_backfill_status": "llm_not_accepted",
        },
        {
            "post_url": "https://t.me/source/3",
            "publication_rank": 3,
            "publication_draft_backfill_status": "retry_due",
            "publication_draft_backfill_next_attempt_after": (now + timedelta(hours=1)).isoformat(),
        },
    ]
    with (
        mock.patch.object(mod.notify, "is_confirmed_publication", return_value=True),
        mock.patch.object(mod.notify, "is_publication_draft_ready", return_value=False),
    ):
        selected = mod.select_rows(rows, limit=10, now=now, surface="telegram")
    assert [row["post_url"] for row in selected] == ["https://t.me/source/1"]


def test_request_fingerprint_changes_with_exact_source_text() -> None:
    mod = load_module()
    row = {"post_url": "https://t.me/source/1", "llm_decision": "accept"}
    first = mod.draft_request_fingerprint(
        row,
        "Первый исходный текст",
        model="gemini-3.1-flash-lite",
    )
    second = mod.draft_request_fingerprint(
        row,
        "Другой исходный текст",
        model="gemini-3.1-flash-lite",
    )
    assert first != second


def test_vk_fetch_requires_the_exact_wall_identity() -> None:
    mod = load_module()
    row = {"post_url": "https://vk.com/wall-10_20"}
    text, fields = mod.fetch_vk_text(
        row,
        {"-10_20": {"owner_id": -10, "id": 20, "text": "Исходный текст VK", "date": 1785530000}},
        "",
    )
    assert text == "Исходный текст VK"
    assert fields["platform"] == "vk"
    try:
        mod.fetch_vk_text(
            row,
            {"-10_20": {"owner_id": -10, "id": 21, "text": "Другой пост"}},
            "",
        )
    except RuntimeError as exc:
        assert "different post" in str(exc)
    else:
        raise AssertionError("mismatched VK post must fail closed")


def _valid_writer_output(mod):
    p1 = (
        "Петербургский автор смотрит на восток Калининградской области без привычного набора "
        "открыточных остановок: его интересует, как повседневный городской ритм считывается "
        "человеком с другим опытом путешествий и наблюдений за малыми городами."
    )
    p2 = (
        "В публикации автор отмечает строгую геометрию улиц и восстановленные фрески, которые "
        "меняют впечатление от обычной прогулки по Гусеву. Оригинал стоит открыть ради конкретных "
        "деталей маршрута и последовательного взгляда, не сводящего город к центральной площади."
    )
    return {
        "status": "draft_ready",
        "public_copy": {"paragraph_1": p1, "paragraph_2": p2},
        "grounding_map": [
            {"sentence_index": 1, "sentence_text": p1, "claim_type": "source_profile_fact", "evidence_ids": ["source.name"], "third_person_maintained": True},
            {"sentence_index": 2, "sentence_text": p2, "claim_type": "content_fact", "evidence_ids": ["content.exact_text"], "third_person_maintained": True},
        ],
    }


def test_v8_validator_enforces_two_grounded_russian_paragraphs() -> None:
    mod = load_module()
    output = _valid_writer_output(mod)
    assert mod.validate_editorial_output(output, {"source.name", "content.exact_text"}) == []
    output["grounding_map"][1]["evidence_ids"] = ["invented"]
    assert "unknown_or_empty_evidence_id" in mod.validate_editorial_output(
        output, {"source.name", "content.exact_text"}
    )


def test_v8_validator_requires_at_least_95_percent_cyrillic() -> None:
    mod = load_module()
    output = _valid_writer_output(mod)
    output["public_copy"]["paragraph_2"] += " English English English English."
    assert "russian_language" in mod.validate_editorial_output(
        output, {"source.name", "content.exact_text"}
    )


def test_history_uses_only_published_or_clean_approved_rows() -> None:
    mod = load_module()
    approved = {
        "post_url": "https://t.me/a/3",
        "operator_review_decision": "approved",
        "operator_review_rewrite_status": "clean",
        "updated_at": "2026-08-01",
        "publication_draft_telegram_text": "Один.\n\nДва.",
    }
    approved["operator_review_fingerprint"] = mod.notify.publication_operator_review_fingerprint(approved)
    rows = [
        {"post_url": "https://t.me/a/1", "publication_draft_telegram_text": "Один.\n\nДва."},
        {"post_url": "https://t.me/a/2", "operator_review_decision": "approved", "operator_review_rewrite_status": "rewrite_requested", "publication_draft_telegram_text": "Один.\n\nДва."},
        approved,
        {"post_url": "https://t.me/a/stale", "operator_review_decision": "approved", "operator_review_rewrite_status": "clean", "operator_review_fingerprint": "stale", "updated_at": "2026-08-03", "publication_draft_telegram_text": "Старое.\n\nРешение."},
        {"post_url": "https://t.me/a/4", "status": "published", "published_at": "2026-08-02", "publication_draft_telegram_text": "Три.\n\nЧетыре."},
    ]
    assert [item["candidate_url"] for item in mod.publication_history(rows)] == [
        "https://t.me/a/4", "https://t.me/a/3"
    ]


def test_recent_history_forces_fresh_start_after_two_transitions() -> None:
    mod = load_module()
    assert mod.recent_history_requires_fresh_start([
        {"throughline_mode": "explicit_transition"},
        {"throughline_mode": "fresh_start"},
        {"throughline_mode": "contrast_or_scale_shift"},
    ]) is True
    assert mod.recent_history_requires_fresh_start([
        {"throughline_mode": "explicit_transition"},
        {"throughline_mode": "fresh_start"},
        {"throughline_mode": "fresh_start"},
    ]) is False


def test_media_plan_is_media_first_and_fails_article_without_hero() -> None:
    mod = load_module()
    missing = mod.publication_media_plan({
        "post_url": "https://example.org/a", "content_origin_type": "editorial_publication"
    })
    assert (missing["mode"], missing["status"]) == ("article_hero", "pending")
    hero = mod.publication_media_plan({
        "post_url": "https://example.org/a", "content_origin_type": "editorial_publication",
        "publication_primary_image_url": "https://cdn.example.org/hero.jpg",
    })
    assert hero["status"] == "ready"
    assert hero["items"][0]["ref"].endswith("hero.jpg")
    album = mod.publication_media_plan({
        "post_url": "https://t.me/travel/100", "expected_image_count": 4,
        "selected_media_ids": '["tg:100","tg:101","tg:102","tg:103"]',
        "original_photo_evidence": "true",
    })
    assert album["mode"] == "social_album"
    assert len(album["items"]) == 4
    associated = mod.publication_media_plan({
        "post_url": "https://example.org/a", "content_origin_type": "editorial_publication",
        "selected_media_materialization_json": '[{"media_id":"frame:1","ordinal":1,"source_ref":"https://cdn.example.org/associated.jpg","reviewed_content_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","materialization_fingerprint":"fp1","refetch_locator":{"method":"article_page_image_evidence","association_reason":"publisher_declared_article_role"}}]',
    })
    assert associated["mode"] == "article_hero"
    assert associated["items"][0]["ref"].endswith("associated.jpg")
    assert associated["items"][0]["reviewed_content_sha256"] == "a" * 64
    assert associated["items"][0]["materialization_fingerprint"] == "fp1"
    assert associated["items"][0]["refetch_locator"]["method"] == "article_page_image_evidence"
    fallback = mod.publication_media_plan({
        "post_url": "https://example.org/a",
        "platform": "web",
        "external_publication_id": "extpub-1",
        "content_origin_type": "editorial_publication",
        "image_queue_status": "not_reviewable_no_media",
        "browser_materialization_status": "terminal_no_associated_images",
        "presentation_recommendation": "system_link_preview",
        "image_quality_terminality": "terminal",
    })
    assert (fallback["mode"], fallback["status"]) == ("link_preview_fallback", "fallback")


def test_legacy_review_is_archived_but_never_approves_v8_revision(monkeypatch) -> None:
    mod = load_module()
    generated = {
        "publication_draft_status": "ready_for_operator_review",
        "publication_draft_prompt_version": mod.EDITORIAL_WRITER_VERSION,
        "publication_draft_contract_version": mod.EDITORIAL_OUTPUT_CONTRACT,
        "publication_draft_backfill_status": "ready",
        "publication_draft_input_fingerprint": "new-v8",
    }
    monkeypatch.setattr(mod, "generate_editorial_draft", lambda *_args, **_kwargs: (generated, 0))
    row = {
        "post_url": "https://t.me/a/1", "publication_draft_prompt_version": "region_talk_final_verifier_v7_grounded_draft",
        "operator_review_fingerprint": "old-fp", "operator_review_decision": "approved",
        "operator_review_rewrite_status": "rewrite_requested",
    }
    updates, _called = mod.build_draft_updates(
        row, text="Исходный текст", fetched={}, source_transport="telethon_discovery2",
        intake=None, history=[], model="test", default_env="KEY", budget=object(),
    )
    assert updates["legacy_principle_status"] == "approved"
    assert updates["legacy_copy_status"] == "rewrite_requested"
    assert updates["legacy_operator_review_fingerprint"] == "old-fp"
    assert updates["operator_review_fingerprint"] == ""
    assert updates["operator_review_decision"] == "pending"


def test_stage_calls_use_controlled_gateway_and_durable_budget() -> None:
    source = MODULE_PATH.read_text(encoding="utf-8")
    assert "rt.get_region_talk_llm_gateway" in source
    assert "budget.reserve(stage_fingerprint)" in source
    assert "budget.complete(stage_fingerprint, result)" in source
    assert "from google import genai" not in source
    assert "requests.post(" not in source


def test_provider_budget_exhaustion_defers_without_uncontrolled_call(monkeypatch) -> None:
    mod = load_module()

    class Budget:
        def reserve(self, _fingerprint):
            return {"status": "exhausted"}

        def complete(self, *_args):
            raise AssertionError("an exhausted reservation cannot be completed")

    monkeypatch.setattr(
        mod.rt,
        "get_region_talk_llm_gateway",
        lambda *_args: (_ for _ in ()).throw(AssertionError("provider must not be called")),
    )
    evidence = mod.build_editorial_evidence(
        {"post_url": "https://t.me/a/1", "source_title": "Автор"},
        source_text="Автор подробно описывает прогулку по Гусеву.",
    )
    updates, calls = mod.generate_editorial_draft(
        {"post_url": "https://t.me/a/1", "source_title": "Автор"},
        evidence_pack=evidence,
        history=[],
        model="test-model",
        default_env="KEY",
        budget=Budget(),
    )
    assert calls == 0
    assert updates["publication_draft_backfill_status"] == "retry_due"
    assert updates["publication_draft_backfill_next_attempt_after"]
