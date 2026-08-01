from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest


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


def test_selection_migrates_unversioned_terminal_and_skips_future_current_retry_rows() -> None:
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
            "publication_draft_backfill_version": mod.DRAFT_BACKFILL_VERSION,
            "publication_draft_backfill_next_attempt_after": (now + timedelta(hours=1)).isoformat(),
        },
    ]
    with (
        mock.patch.object(mod.notify, "is_confirmed_publication", return_value=True),
        mock.patch.object(mod.notify, "is_publication_draft_ready", return_value=False),
    ):
        selected = mod.select_rows(rows, limit=10, now=now, surface="telegram")
    assert [row["post_url"] for row in selected] == [
        "https://t.me/source/2", "https://t.me/source/1",
    ]


def test_current_v3_ready_with_unversioned_draft_is_corrective_actionable() -> None:
    mod = load_module()
    row = {
        "post_url": "https://t.me/source/4",
        "publication_draft_backfill_version": mod.DRAFT_BACKFILL_VERSION,
        "publication_draft_backfill_status": "ready",
    }
    with (
        mock.patch.object(mod.notify, "is_confirmed_publication", return_value=True),
        mock.patch.object(mod.notify, "is_publication_draft_ready", return_value=False),
    ):
        assert mod.backfill_is_actionable(row, surface="telegram") is True


def test_execute_reads_supporting_kinds_through_notifier_namespace() -> None:
    mod = load_module()

    class Driver:
        stopped = False

        def stop(self) -> None:
            self.stopped = True

    driver = Driver()
    kinds: list[str] = []

    def read_kind_rows(_pool, _ydb, _table, kind: str, _limit: int):
        kinds.append(kind)
        return []

    args = argparse.Namespace(
        scan_limit=5000,
        history_limit=5000,
        limit=2,
        surface="all",
        transport="telethon_discovery2",
        dry_run=True,
    )
    with (
        mock.patch.object(
            mod.notify,
            "read_publication_rows",
            return_value=(object(), driver, object(), "table", []),
        ),
        mock.patch.object(mod.notify, "read_kind_rows", side_effect=read_kind_rows),
    ):
        result = asyncio.run(mod.execute(args))

    assert result["selected_total"] == 0
    assert driver.stopped is True
    assert kinds == [
        "external_publication_intake_item",
        "external_publication_source_item",
        "source_onboarding_profile_item",
        "image_queue_item",
        "publication_schedule_item",
        "publication_log_item",
        "region_talk_publication_log",
    ]


def test_article_media_evidence_is_filled_from_latest_image_ledger_row() -> None:
    mod = load_module()
    publication = {
        "post_url": "https://archi.ru/russia/example",
        "content_origin_type": "editorial_publication",
    }
    mod.attach_latest_article_media_evidence(
        [publication],
        [
            {
                "post_url": publication["post_url"],
                "updated_at": "2026-07-01T10:00:00+00:00",
                "image_url_or_local_path": "https://cdn.example.org/old.jpg",
            },
            {
                "post_url": publication["post_url"],
                "updated_at": "2026-07-01T11:00:00+00:00",
                "image_url_or_local_path": "https://cdn.example.org/hero.jpg",
                "selected_media_ids": '["web_direct:1"]',
                "image_queue_status": "actual_scored",
            },
        ],
    )
    assert publication["image_url_or_local_path"].endswith("hero.jpg")
    assert publication["selected_media_ids"] == '["web_direct:1"]'
    assert mod.publication_media_plan(publication)["status"] == "ready"


def test_execute_article_uses_retained_intake_without_social_fetch_unpack(monkeypatch) -> None:
    mod = load_module()

    class Driver:
        def stop(self) -> None:
            pass

    row = {
        "_ydb_pk": "publication_candidate_item:article",
        "post_url": "https://archi.ru/russia/example",
        "content_origin_type": "editorial_publication",
    }
    intake = {
        "external_publication_id": "ext-1",
        "canonical_url": row["post_url"],
        "article_text": "Сохранённый текст статьи",
    }
    row["external_publication_id"] = intake["external_publication_id"]
    args = argparse.Namespace(
        scan_limit=5000,
        history_limit=5000,
        limit=1,
        surface="article",
        transport="telethon_discovery2",
        dry_run=False,
        model="gemini-3.1-flash-lite",
        default_env_var_name="GOOGLE_API_KEY3",
        llm_budget_id="unit-budget",
        llm_budget_max=20,
        delay_min=0,
        delay_max=0,
    )
    written: list[dict] = []
    monkeypatch.setattr(
        mod.notify,
        "read_publication_rows",
        lambda _limit: (object(), Driver(), object(), "table", [row]),
    )
    monkeypatch.setattr(
        mod.notify,
        "read_kind_rows",
        lambda _pool, _ydb, _table, kind, _limit: [intake] if kind == "external_publication_intake_item" else [],
    )
    monkeypatch.setattr(mod.notify, "is_confirmed_publication", lambda _row: True)
    monkeypatch.setattr(mod.notify, "is_publication_draft_ready", lambda _row: False)
    monkeypatch.setattr(mod, "DurableGeminiBudget", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(mod, "collect_source_texts", mock.AsyncMock(return_value=({}, {})))
    monkeypatch.setattr(
        mod.finalizer,
        "build_source_onboarding_evidence",
        lambda *_args, **_kwargs: {"source_profile_id": "profile-1", "evidence_status": "sufficient"},
    )
    def enrich(rows, **_kwargs):
        rows[0].update({
            "source_onboarding_status": "ready",
            "source_onboarding_paragraph": "Проверенная сводка об издании для читателя.",
            "source_onboarding_publisher_dimensions_status": "ready",
            "source_onboarding_publisher_dimensions_json": json.dumps({
                key: {"text": key, "evidence_ids": ["E1"]}
                for key in ("outlet_identity", "intended_audience", "distinctive_value")
            }),
            "source_onboarding_summary_kind": mod.notify.PUBLISHER_READER_BRIEF_KIND,
        })
        return rows, [], {}
    monkeypatch.setattr(mod.finalizer, "enrich_accepted_rows_with_onboarding", enrich)
    monkeypatch.setattr(mod.finalizer, "write_source_onboarding_rows", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        mod,
        "build_draft_updates",
        lambda _row, **kwargs: ({"publication_draft_backfill_status": "ready"}, False),
    )
    monkeypatch.setattr(
        mod,
        "upsert_publication_row",
        lambda _pool, _ydb, _table, _row, updates: written.append(updates),
    )

    result = asyncio.run(mod.execute(args))

    assert result["ready_total"] == 1
    assert written[0]["publication_draft_backfill_status"] == "ready"
    assert written[0]["source_onboarding_publisher_dimensions_status"] == "ready"


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


def test_validator_checks_exact_rendered_caption_length_when_row_is_available() -> None:
    mod = load_module()
    output = _valid_writer_output(mod)
    row = {"post_url": "https://t.me/example/1", "source_title": "Внешнее издание"}
    violations = mod.validate_editorial_output(
        output, {"source.name", "content.exact_text"}, row=row,
    )
    assert "caption_visible_length:536" in violations

    output["public_copy"]["paragraph_2"] += (
        " Автор также объясняет, как эти наблюдения складываются в цельный маршрут прогулки."
    )
    assert not any(
        item.startswith("caption_visible_length:")
        for item in mod.validate_editorial_output(
            output, {"source.name", "content.exact_text"}, row=row,
        )
    )


def test_short_rendered_caption_gets_writer_retry_before_critic(monkeypatch) -> None:
    mod = load_module()
    short = _valid_writer_output(mod)
    short["_stage_status"] = "ok"
    expanded = json.loads(json.dumps(short, ensure_ascii=False))
    expanded["public_copy"]["paragraph_2"] += (
        " Автор также объясняет, как эти наблюдения складываются в цельный маршрут прогулки."
    )
    strategy = {
        "_stage_status": "ok", "status": "ready", "throughline_mode": "fresh_start",
        "used_history_urls": [], "visual_hook_evidence_ids": [],
    }
    critic = {"_stage_status": "ok", "status": "pass", "reason_codes": []}
    row = {
        "post_url": "https://t.me/example/1", "source_title": "Внешнее издание",
        "publication_primary_image_url": "https://cdn.example.org/hero.jpg",
    }
    evidence = mod.build_editorial_evidence(
        row, source_text="Подробный текст о прогулке по Гусеву."
    )
    sequence = iter([(strategy, True), (short, True), (expanded, True), (critic, True)])
    calls_seen = []

    def fake_call(**kwargs):
        calls_seen.append(kwargs)
        return next(sequence)

    monkeypatch.setattr(mod, "call_editorial_stage", fake_call)
    updates, calls = mod.generate_editorial_draft(
        row, evidence_pack=evidence, history=[], model="test", default_env="KEY", budget=object(),
    )

    assert calls == 4
    assert updates["publication_draft_backfill_status"] == "ready"
    assert updates["publication_draft_generation_attempts"] == 2
    assert calls_seen[2]["stage"] == "writer"
    feedback = calls_seen[2]["payload"]["deterministic_feedback"]
    assert "caption_visible_length:536" in feedback
    repair = calls_seen[2]["payload"]["length_repair"]
    assert repair["actual_visible_chars"] == 536
    assert repair["target_visible_min_chars"] == 620
    assert repair["required_added_editorial_chars"] == 84


def test_second_short_writer_failure_preserves_stage_audit(monkeypatch) -> None:
    mod = load_module()
    short = _valid_writer_output(mod)
    short["_stage_status"] = "ok"
    strategy = {
        "_stage_status": "ok", "status": "ready", "throughline_mode": "fresh_start",
        "used_history_urls": [], "visual_hook_evidence_ids": [],
    }
    row = {
        "post_url": "https://t.me/example/1", "source_title": "Внешнее издание",
        "publication_primary_image_url": "https://cdn.example.org/hero.jpg",
    }
    evidence = mod.build_editorial_evidence(
        row, source_text="Подробный текст о прогулке по Гусеву."
    )
    sequence = iter([(strategy, True), (short, True), (short, True)])
    monkeypatch.setattr(mod, "call_editorial_stage", lambda **_kwargs: next(sequence))

    updates, calls = mod.generate_editorial_draft(
        row, evidence_pack=evidence, history=[], model="test", default_env="KEY", budget=object(),
    )

    assert calls == 3
    assert updates["publication_draft_backfill_status"] == "needs_grounding_review"
    audit = json.loads(updates["publication_draft_stage_audit_json"])
    assert audit["strategy"]["status"] == "ready"
    assert audit["writer"]["status"] == "draft_ready"


def test_editorial_stage_pacing_waits_between_physical_provider_calls(monkeypatch) -> None:
    mod = load_module()
    monotonic_values = iter([100.0, 100.0, 101.0, 105.5])
    sleeps = []
    monkeypatch.setattr(mod.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(mod.time, "sleep", sleeps.append)
    mod._EDITORIAL_PROVIDER_STAGE_DELAY_SECONDS = 5.5
    mod._EDITORIAL_PROVIDER_LAST_CALL = 0.0

    mod.pace_editorial_provider_call()
    mod.pace_editorial_provider_call()

    assert sleeps == [4.5]


def test_article_writer_must_ground_all_publisher_reader_brief_dimensions_in_first_paragraph() -> None:
    mod = load_module()
    output = _valid_writer_output(mod)
    required = {
        "source.publisher.outlet_identity",
        "source.publisher.intended_audience",
        "source.publisher.distinctive_value",
    }
    evidence_ids = {"source.name", "content.exact_text", *required}
    assert "missing_publisher_reader_brief" in mod.validate_editorial_output(
        output, evidence_ids, required_publisher_evidence_ids=required,
    )
    output["grounding_map"][0]["paragraph_index"] = 1
    output["grounding_map"][0]["evidence_ids"] = ["source.name", *sorted(required)]
    output["grounding_map"][1]["paragraph_index"] = 2
    assert mod.validate_editorial_output(
        output, evidence_ids, required_publisher_evidence_ids=required,
    ) == []


def test_v9_validator_rejects_banned_not_a_construction() -> None:
    mod = load_module()
    output = _valid_writer_output(mod)
    output["public_copy"]["paragraph_2"] = (
        "В публикации автор показывает музей не как цепочку отдельных залов, а как цельный "
        "маршрут вокруг океана. Оригинал стоит открыть ради деталей конструкций, экспозиции "
        "и последовательного объяснения архитектурного замысла здания."
    )
    assert "contrastive_not_a_cliche" in mod.validate_editorial_output(
        output, {"source.name", "content.exact_text"}
    )
    with mock.patch.object(mod.notify, "contains_contrastive_not_a_cliche", return_value=True):
        with mock.patch.object(mod, "_source_name", return_value="Источник"):
            with mock.patch.object(mod, "_canonical_url", return_value="https://example.org/a"):
                with mock.patch.object(mod, "render_public_copy", wraps=mod.render_public_copy):
                    try:
                        mod.render_public_copy({}, output)
                    except ValueError as exc:
                        assert str(exc) == "contrastive_not_a_cliche"
                    else:
                        raise AssertionError("render must fail closed on banned style")


def test_v9_writer_gets_one_style_retry_then_fails_closed(monkeypatch) -> None:
    mod = load_module()
    clean = _valid_writer_output(mod)
    clean["_stage_status"] = "ok"
    clean["public_copy"]["paragraph_2"] += (
        " Также автор объясняет, какие детали связывают отдельные точки прогулки в общий маршрут."
    )
    banned = json.loads(json.dumps(clean, ensure_ascii=False))
    banned["public_copy"]["paragraph_2"] = (
        "В публикации автор показывает музей не как цепочку отдельных залов, а как цельный "
        "маршрут вокруг океана. Оригинал стоит открыть ради деталей конструкций, экспозиции "
        "и последовательного объяснения архитектурного замысла здания."
    )
    strategy = {
        "_stage_status": "ok", "status": "ready", "throughline_mode": "fresh_start",
        "used_history_urls": [], "visual_hook_evidence_ids": [],
    }
    critic = {"_stage_status": "ok", "status": "pass", "reason_codes": []}
    row = {
        "post_url": "https://example.org/article", "source_title": "Внешнее издание",
        "content_origin_type": "editorial_publication",
        "publication_primary_image_url": "https://cdn.example.org/hero.jpg",
    }
    evidence = mod.build_editorial_evidence(row, source_text="Подробный текст о музейном маршруте.")

    sequence = iter([(strategy, True), (banned, True), (clean, True), (critic, True)])
    monkeypatch.setattr(mod, "call_editorial_stage", lambda **_kwargs: next(sequence))
    updates, calls = mod.generate_editorial_draft(
        row, evidence_pack=evidence, history=[], model="test", default_env="KEY", budget=object(),
    )
    assert calls == 4
    assert updates["publication_draft_backfill_status"] == "ready"
    assert updates["publication_draft_generation_attempts"] == 2
    assert not mod.notify.contains_contrastive_not_a_cliche(
        updates["publication_draft_telegram_text"]
    )

    sequence = iter([(strategy, True), (banned, True), (banned, True)])
    monkeypatch.setattr(mod, "call_editorial_stage", lambda **_kwargs: next(sequence))
    updates, calls = mod.generate_editorial_draft(
        row, evidence_pack=evidence, history=[], model="test", default_env="KEY", budget=object(),
    )
    assert calls == 3
    assert updates["publication_draft_backfill_status"] == "needs_grounding_review"
    assert "contrastive_not_a_cliche" in updates["publication_draft_backfill_reason"]


def test_v9_prompts_tell_writer_and_critic_to_reject_the_style_family() -> None:
    mod = load_module()
    writer = mod._stage_prompt("writer", {})
    critic = mod._stage_prompt("critic", {})
    assert "negation-plus-adversative contrast template is forbidden" in writer
    assert "contrastive_not_a_cliche" in critic


def test_writer_prompt_uses_hard_numeric_caption_contract() -> None:
    mod = load_module()
    writer = json.loads(mod._stage_prompt("writer", {"visible_caption_contract": {"min_chars": 550}}))
    assert writer["output"]["public_copy"] == {
        "paragraph_1": "260-420 chars", "paragraph_2": "260-420 chars",
    }
    assert any("hard output schema" in rule for rule in writer["rules"])
    assert any("required_added_editorial_chars" in rule for rule in writer["rules"])


def test_line_break_variant_is_rejected_before_whitespace_normalization() -> None:
    mod = load_module()
    paragraph = (
        "Автор рассматривает пространство не как набор отдельных залов\n"
        "а как цельный маршрут, который постепенно раскрывает устройство здания и "
        "помогает читателю проследить все основные решения проекта без лишних обобщений."
    )
    output = {
        "public_copy": {"paragraph_1": paragraph, "paragraph_2": "Я" * 160},
        "grounding_map": [{
            "sentence_index": 1, "claim_type": "content_fact",
            "evidence_ids": ["E1"], "third_person_maintained": True,
        }],
    }
    assert "contrastive_not_a_cliche" in mod.validate_editorial_output(output, {"E1"})
    with pytest.raises(ValueError, match="contrastive_not_a_cliche"):
        mod.render_public_copy(
            {"post_url": "https://example.org/a", "source_title": "Источник"}, output
        )


def test_selection_can_force_one_candidate_but_excludes_public_history() -> None:
    mod = load_module()
    target = "https://archi.ru/russia/101203/vsya-mudrost-okeana"
    rows = [
        {
            "post_url": target,
            "content_origin_type": "editorial_publication",
            "publication_draft_backfill_version": mod.DRAFT_BACKFILL_VERSION,
            "publication_draft_backfill_status": "retry_due",
            "publication_draft_backfill_next_attempt_after": "2099-01-01T00:00:00+00:00",
        },
        {"post_url": "https://example.org/other", "content_origin_type": "editorial_publication"},
    ]
    with mock.patch.object(mod.notify, "is_confirmed_publication", return_value=True):
        selected = mod.select_rows(
            rows, limit=10, surface="article", force_regenerate=True,
            candidate_urls={target}, published_urls=set(),
        )
        assert [row["post_url"] for row in selected] == [target]
        assert mod.select_rows(
            rows, limit=10, surface="article", force_regenerate=True,
            candidate_urls={target}, published_urls={target},
        ) == []
        assert mod.select_rows(
            [{**rows[0], "publication_candidate_id": "pubcand-1"}],
            limit=10, surface="article", force_regenerate=True,
            candidate_urls={target}, published_candidate_ids={"pubcand-1"},
        ) == []


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
    source_album = mod.publication_media_plan({
        "post_url": "https://t.me/travel/200", "expected_image_count": 5,
        "original_photo_evidence": "true",
    })
    assert source_album["mode"] == "social_album"
    assert source_album["status"] == "ready"
    assert source_album["reason"] == "exact_source_album_ref"
    assert source_album["items"] == [{
        "media_id": "source:album", "ordinal": 1, "kind": "image",
        "ref": "https://t.me/travel/200",
    }]
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


def test_social_media_selection_is_joined_from_image_diagnostic() -> None:
    mod = load_module()
    publications = [{
        "post_url": "https://t.me/travel/100",
        "content_origin_type": "social_post",
        "expected_image_count": 10,
    }]
    images = [{
        "post_url": "https://t.me/travel/100",
        "updated_at": "2026-08-01T12:00:00Z",
        "selected_media_ids": '["telegram:100","telegram:109","telegram:102"]',
        "expected_image_count": 10,
        "fetched_image_count": 10,
    }]
    mod.attach_latest_media_evidence(publications, images)
    assert publications[0]["selected_media_ids"] == images[0]["selected_media_ids"]
    plan = mod.publication_media_plan(publications[0])
    assert plan["mode"] == "social_album"
    assert [item["media_id"] for item in plan["items"]] == [
        "telegram:100", "telegram:109", "telegram:102",
    ]


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
