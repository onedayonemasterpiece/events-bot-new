from __future__ import annotations

import tomllib
import importlib.util
import json
import sys
from types import SimpleNamespace

from scripts import region_talk_publication_plan as plan
from scripts import region_talk_goal_notify as notify


def load_candidate_report_module():
    path = plan.ROOT / "kaggle" / "RegionTalkCandidateReport" / "region_talk_candidate_report.py"
    spec = importlib.util.spec_from_file_location("region_talk_candidate_report_fingerprint_parity", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_external_intake_fingerprint_matches_candidate_report_legacy_contract() -> None:
    report = load_candidate_report_module()
    row = {
        "external_publication_id": "extpub_legacy",
        "canonical_url": "https://publisher.example/article",
        "decision": {"import_status": "ready_for_region_talk_scoring"},
        "legacy_provenance_attestation": {
            "attestation_version": "region_talk_legacy_external_provenance_v1",
            "legacy_row_sha256": "a" * 64,
        },
    }

    assert plan.external_intake_fingerprint(row) == report.external_publication_intake_fingerprint(row)


def test_production_enables_exact_current_reaction_gate() -> None:
    config = tomllib.loads((plan.ROOT / "fly.toml").read_text(encoding="utf-8"))
    assert config["env"]["REGION_TALK_REACTION_SYNC_ENABLED"] == "1"
    assert config["env"]["REGION_TALK_REACTION_GATE_ENABLED"] == "1"


def external_article_fixture() -> tuple[dict, dict]:
    publication = {
        "_ydb_pk": "publication_candidate_item:https://example.org/article",
        "post_url": "https://example.org/article",
        "external_publication_id": "extpub_test",
        "content_origin_type": "editorial_publication",
        "source_title": "Внешнее издание",
        "llm_gate_status": "ok",
        "llm_decision": "accept",
        "publication_status": "gemini_accept",
        "publication_candidate_status": "sent_to_chat",
        "publication_score": 0.9,
    }
    intake = {
        "external_publication_id": "extpub_test",
        "canonical_url": "https://example.org/article",
        "publication": {
            "title": "Большой материал",
            "authors": ["Автор"],
            "source_name": "Внешнее издание",
        },
        "source_assessment": {"scope": "external"},
        "policy_classification": {
            "product_policy_match": True,
            "hard_exclusion_codes": [],
        },
        "decision": {
            "import_status": "ready_for_region_talk_scoring",
            "downstream_readiness": "candidate_report",
        },
        "editorial_pack": {
            "title_short": "Архитектура как маршрут",
            "teaser": "Издание разбирает устройство нового музейного корпуса.",
            "reader_takeaway": "Читатель увидит связь формы здания и экспозиции.",
            "caveat": "Часть параметров атрибутирована проектировщикам.",
            "copy_support": [
                {"surface": "teaser", "evidence_refs": ["E1"]},
                {"surface": "reader_takeaway", "evidence_refs": ["E2"]},
                {"surface": "caveat", "evidence_refs": ["E3"]},
            ],
        },
        "evidence": [
            {"evidence_id": "E1", "paraphrase": "Материал посвящён музейному корпусу."},
            {"evidence_id": "E2", "paraphrase": "Текст объясняет маршрут и экспозицию."},
            {"evidence_id": "E3", "paraphrase": "Источники параметров названы на странице."},
        ],
    }
    publication["external_intake_fingerprint"] = plan.external_intake_fingerprint(intake)
    return publication, intake


def ready_social(url: str) -> dict:
    p1 = (
        "Петербургский автор внимательно исследует повседневный ритм города и собирает маршрут "
        "из наблюдений за улицами, площадями и привычками жителей. Такой внешний взгляд помогает "
        "увидеть знакомое пространство с новой точки и сохраняет авторскую интонацию источника."
    )
    p2 = (
        "В публикации подробно описаны геометрия улиц, восстановленные фрески и последовательность "
        "прогулки. Оригинал стоит открыть ради конкретных деталей и цельной фотосерии, которая "
        "показывает обычную городскую жизнь далеко за пределами центральной площади и соседних районов."
    )
    return {
        "_ydb_pk": "publication_candidate_item:" + url,
        "post_url": url,
        "publication_score": 0.8,
        "publication_status": "gemini_accept",
        "publication_candidate_status": "sent_to_chat",
        "publication_draft_status": "ready_for_operator_review",
        "publication_draft_title": "Личный маршрут",
        "publication_draft_source_attribution": "Авторский канал",
        "publication_draft_telegram_text": f"{p1}\n\n{p2}\n\nИсточник: Авторский канал\nОригинал: {url}",
        "publication_draft_vk_text": f"{p1}\n\n{p2}\n\nИсточник: Авторский канал\nОригинал: {url}",
        "publication_draft_fact_points_json": '[{"claim":"Факт","support_excerpt":"Опора"}]',
        "publication_draft_prompt_version": notify.EDITORIAL_WRITER_VERSION,
        "publication_draft_contract_version": notify.EDITORIAL_OUTPUT_CONTRACT,
        "publication_media_materialization_status": "fallback",
        "publication_media_materialization_contract_version": notify.MEDIA_MATERIALIZATION_CONTRACT_VERSION,
    }


def test_planner_uses_same_strict_ready_contract_as_operator_delivery() -> None:
    row = ready_social("https://t.me/source/strict")
    assert plan.publication_draft_ready(row) is True

    # A syntactically non-empty JSON string is not grounded evidence. This was
    # previously enough for the planner even though the notifier rejected it.
    row["publication_draft_fact_points_json"] = "[]"
    assert plan.publication_draft_ready(row) is False

    row["publication_draft_fact_points_json"] = "not-json"
    assert plan.publication_draft_ready(row) is False


def test_external_article_evidence_projection_is_complete_and_verbatim() -> None:
    publication, intake = external_article_fixture()

    draft = plan.evidence_projected_article_draft(publication, intake)

    assert draft is not None
    assert draft["publication_draft_status"] == "ready_for_operator_review"
    assert draft["publication_draft_prompt_version"] == plan.EVIDENCE_PROJECTION_DRAFT_VERSION
    assert "Издание разбирает устройство нового музейного корпуса." in draft["publication_draft_telegram_text"]
    assert "Источник: Внешнее издание · Автор" in draft["publication_draft_telegram_text"]
    assert draft["publication_draft_telegram_text"].endswith("Оригинал: https://example.org/article")
    points = json.loads(draft["publication_draft_fact_points_json"])
    assert [item["evidence_ids"] for item in points] == [["E1"], ["E2"], ["E3"]]


def test_external_article_projection_fails_closed_for_broken_copy_support() -> None:
    publication, intake = external_article_fixture()
    intake["editorial_pack"]["copy_support"][0]["evidence_refs"] = ["missing"]

    assert plan.evidence_projected_article_draft(publication, intake) is None


def test_evidence_projection_never_fabricates_social_draft() -> None:
    publication, intake = external_article_fixture()
    publication.pop("external_publication_id")
    publication["content_origin_type"] = "external_social"
    publication["post_url"] = "https://t.me/source/1"

    assert plan.evidence_projected_article_draft(publication, intake) is None


def test_build_plan_waits_for_v8_article_writer_and_excludes_missing_social_draft(monkeypatch) -> None:
    monkeypatch.delenv("REGION_TALK_REACTION_GATE_ENABLED", raising=False)
    article, intake = external_article_fixture()
    social_ready = ready_social("https://t.me/source/1")
    social_missing = {
        "_ydb_pk": "publication_candidate_item:https://t.me/source/2",
        "post_url": "https://t.me/source/2",
        "publication_score": 0.99,
        "publication_status": "gemini_accept",
        "publication_candidate_status": "sent_to_chat",
    }
    publications = [article, social_missing, social_ready]

    class Driver:
        def stop(self, timeout=5):  # noqa: ARG002
            return None

    monkeypatch.setattr(
        plan,
        "read_publication_rows",
        lambda _limit: (object(), Driver(), object(), "/db/table", publications),
    )
    monkeypatch.setattr(
        plan,
        "read_kind_rows",
        lambda _pool, _ydb, _table, kind, _limit: [intake]
        if kind == "external_publication_intake_item"
        else [],
    )
    monkeypatch.setattr(plan, "attach_latest_bge_vectors", lambda *_args: None)
    monkeypatch.setattr(plan, "is_confirmed_publication", lambda _row: True)
    writes = []
    monkeypatch.setattr(
        plan,
        "_upsert_rows",
        lambda _pool, _ydb, _table, rows: writes.extend(rows) or len(rows),
    )
    args = SimpleNamespace(
        scan_limit=5000,
        vector_scan_limit=5000,
        history_limit=5000,
        timezone="Europe/Kaliningrad",
        article_time="12:00",
        social_time="18:00",
        start_date="2026-08-01",
        days=1,
        diversity_weight=0.35,
        pair_similarity_threshold=0.82,
        execute=True,
    )

    result = plan.build_plan(args)

    assert result["counts"]["confirmed_article"] == 1
    assert result["counts"]["confirmed_social"] == 2
    assert result["counts"]["draft_projected_article"] == 0
    assert result["counts"]["draft_missing_article"] == 1
    assert result["counts"]["draft_missing_social"] == 1
    assert result["counts"]["eligible_article"] == 0
    assert result["counts"]["eligible_social"] == 1
    assert result["counts"]["planned_article"] == 0
    assert result["counts"]["planned_social"] == 1
    article_slot = next(row for row in result["rows"] if row["content_lane"] == "article")
    assert article_slot["plan_status"] == "vacant"
    assert not any(pk == article["_ydb_pk"] and kind == "publication_candidate_item" for pk, kind, _payload in writes)


def test_planner_reaction_rollout_gate_defaults_off_and_requires_approved_clean(monkeypatch) -> None:
    social = ready_social("https://t.me/source/reviewed")

    class Driver:
        def stop(self, timeout=5):  # noqa: ARG002
            return None

    monkeypatch.setattr(
        plan,
        "read_publication_rows",
        lambda _limit: (object(), Driver(), object(), "/db/table", [social]),
    )
    monkeypatch.setattr(plan, "read_kind_rows", lambda *_args: [])
    monkeypatch.setattr(plan, "attach_latest_bge_vectors", lambda *_args: None)
    monkeypatch.setattr(plan, "is_confirmed_publication", lambda _row: True)
    args = SimpleNamespace(
        scan_limit=5000,
        vector_scan_limit=5000,
        history_limit=5000,
        timezone="Europe/Kaliningrad",
        article_time="12:00",
        social_time="18:00",
        start_date="2026-08-02",
        days=1,
        diversity_weight=0.35,
        pair_similarity_threshold=0.82,
        execute=False,
    )

    monkeypatch.delenv("REGION_TALK_REACTION_GATE_ENABLED", raising=False)
    legacy = plan.build_plan(args)
    assert legacy["counts"]["reaction_gate_enabled"] is False
    assert legacy["counts"]["eligible_social"] == 1

    monkeypatch.setenv("REGION_TALK_REACTION_GATE_ENABLED", "1")
    blocked = plan.build_plan(args)
    assert blocked["counts"]["reaction_gate_enabled"] is True
    assert blocked["counts"]["reaction_gate_blocked_social"] == 1
    assert blocked["counts"]["eligible_social"] == 0

    social.update({
        "operator_review_fingerprint": plan.publication_operator_review_fingerprint(social),
        "operator_review_decision": "approved",
        "operator_review_rewrite_status": "clean",
    })
    approved = plan.build_plan(args)
    assert approved["counts"]["reaction_gate_blocked_social"] == 0
    assert approved["counts"]["eligible_social"] == 1

    social["operator_review_rewrite_status"] = "rewrite_requested"
    rewrite = plan.build_plan(args)
    assert rewrite["counts"]["eligible_social"] == 0


def test_late_candidate_cannot_replace_prepared_future_slot(monkeypatch) -> None:
    prepared = ready_social("https://t.me/source/prepared")
    prepared["publication_score"] = 0.2
    late = ready_social("https://t.me/source/late")
    late["publication_score"] = 0.99
    schedule = [{
        "plan_date": "2026-08-05",
        "content_lane": "social",
        "plan_status": "prepared",
        "scheduled_for": "2026-08-05T18:00:00+02:00",
        "candidate_url": prepared["post_url"],
        "post_url": prepared["post_url"],
    }]

    class Driver:
        def stop(self, timeout=5):  # noqa: ARG002
            return None

    monkeypatch.setattr(
        plan, "read_publication_rows",
        lambda _limit: (object(), Driver(), object(), "/db/table", [prepared, late]),
    )
    monkeypatch.setattr(
        plan, "read_kind_rows",
        lambda _pool, _ydb, _table, kind, _limit: schedule
        if kind == "publication_schedule_item" else [],
    )
    monkeypatch.setattr(plan, "attach_latest_bge_vectors", lambda *_args: None)
    monkeypatch.setattr(plan, "is_confirmed_publication", lambda _row: True)
    args = SimpleNamespace(
        scan_limit=5000, vector_scan_limit=5000, history_limit=5000,
        timezone="Europe/Kaliningrad", article_time="12:00", social_time="18:00",
        start_date="2026-08-05", days=1, diversity_weight=0.35,
        pair_similarity_threshold=0.82, execute=False,
    )

    first = plan.build_plan(args)
    second = plan.build_plan(args)

    slot = next(row for row in first["rows"] if row["content_lane"] == "social")
    assert slot["candidate_url"] == prepared["post_url"]
    assert slot["prepared_identity_frozen"] is True
    assert slot["slot_locked"] is True
    assert [(r["content_lane"], r["candidate_url"]) for r in first["rows"]] == [
        (r["content_lane"], r["candidate_url"]) for r in second["rows"]
    ]


def test_strong_publication_refresh_reapplies_live_source_fingerprint(monkeypatch) -> None:
    publication = ready_social("https://t.me/source/strong-refresh")
    publication.update({
        "platform": "telegram",
        "canonical_source_key": "telegram:source",
        "source_id": "telegram:source",
        "publication_eligibility_verdict": "eligible",
        "publication_eligibility_gate_version": notify.PUBLICATION_ELIGIBILITY_GATE_VERSION,
        "authoritative_source_fingerprint_version": notify.AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION,
    })
    source = {
        "_ydb_pk": "source_queue_item:telegram:source",
        "platform": "telegram",
        "canonical_source_key": "telegram:source",
        "source_id": "telegram:source",
        "source_title": "Source",
        "source_url": "https://t.me/source",
        "source_geo_class": "nonlocal_russia",
        "source_topic_class": "travel_blogger",
        "source_quick_class": "candidate_keep",
        "source_queue_status": "ready",
    }
    publication["authoritative_source_fingerprint"] = notify.authoritative_source_fingerprint(source)

    class Driver:
        def stop(self, timeout=5):  # noqa: ARG002
            return None

    class Ydb:
        @staticmethod
        def SnapshotReadOnly():
            return object()

    monkeypatch.setattr(
        plan, "read_publication_rows",
        lambda _limit: (Ydb(), Driver(), object(), "/db/table", []),
    )

    def current(_pool, _ydb, _table, kind, _limit):
        if kind == "publication_candidate_item":
            return [dict(publication)]
        if kind == "source_queue_item":
            return [source]
        return []

    monkeypatch.setattr(plan, "_read_current_kind_rows_complete", current)
    monkeypatch.setattr(plan, "attach_latest_bge_vectors", lambda *_args: None)
    args = SimpleNamespace(
        scan_limit=5000, vector_scan_limit=5000, history_limit=5000,
        timezone="Europe/Kaliningrad", article_time="12:00", social_time="18:00",
        start_date="2026-08-05", days=1, diversity_weight=0.35,
        pair_similarity_threshold=0.82, execute=False,
    )

    result = plan.build_plan(args)

    assert result["counts"]["confirmed_social"] == 1
    assert result["counts"]["eligible_social"] == 1


def test_changed_prepared_external_revision_is_preserved_and_opened_for_review(monkeypatch) -> None:
    publication, intake = external_article_fixture()
    # Mark the external article as prepared under an older intake fingerprint.
    publication.update(ready_social(publication["post_url"]))
    publication.update({
        "external_publication_id": intake["external_publication_id"],
        "content_origin_type": "editorial_publication",
        "external_intake_fingerprint": plan.external_intake_fingerprint(intake),
    })
    schedule = [{
        "plan_date": "2026-08-06",
        "content_lane": "article",
        "plan_status": "prepared",
        "scheduled_for": "2026-08-06T12:00:00+02:00",
        "candidate_url": publication["post_url"],
        "post_url": publication["post_url"],
        "external_publication_id": intake["external_publication_id"],
        "external_intake_fingerprint": "older-intake-fingerprint",
    }]

    class Driver:
        def stop(self, timeout=5):  # noqa: ARG002
            return None

    monkeypatch.setattr(
        plan, "read_publication_rows",
        lambda _limit: (object(), Driver(), object(), "/db/table", [publication]),
    )
    monkeypatch.setattr(
        plan, "read_kind_rows",
        lambda _pool, _ydb, _table, kind, _limit: schedule
        if kind == "publication_schedule_item" else [intake]
        if kind == "external_publication_intake_item" else [],
    )
    monkeypatch.setattr(plan, "attach_latest_bge_vectors", lambda *_args: None)
    monkeypatch.setattr(plan, "is_confirmed_publication", lambda _row: True)
    args = SimpleNamespace(
        scan_limit=5000, vector_scan_limit=5000, history_limit=5000,
        timezone="Europe/Kaliningrad", article_time="12:00", social_time="18:00",
        start_date="2026-08-06", days=1, diversity_weight=0.35,
        pair_similarity_threshold=0.82, execute=False,
    )

    result = plan.build_plan(args)

    slot = next(row for row in result["rows"] if row["content_lane"] == "article")
    assert slot["candidate_url"] == publication["post_url"]
    assert slot["plan_status"] == "manual_review_required"
    assert slot["plan_reevaluation_required"] is True
    assert slot["plan_reevaluation_reason"] == "prepared_external_intake_revision_changed_or_missing"


def test_final_planner_revision_change_defers_without_writes(monkeypatch) -> None:
    social = ready_social("https://t.me/source/current")

    class Driver:
        def stop(self, timeout=5):  # noqa: ARG002
            return None

    class Ydb:
        @staticmethod
        def SnapshotReadOnly():
            return object()

    monkeypatch.setattr(
        plan, "read_publication_rows",
        lambda _limit: (Ydb(), Driver(), object(), "/db/table", [social]),
    )
    calls = {"publication_schedule_item": 0}

    def current(_pool, _ydb, _table, kind, _limit):
        if kind == "publication_candidate_item":
            return [social]
        if kind == "publication_schedule_item":
            calls[kind] += 1
            return [] if calls[kind] == 1 else [{"plan_slot_id": "late-change", "plan_status": "planned"}]
        return []

    monkeypatch.setattr(plan, "_read_current_kind_rows_complete", current)
    monkeypatch.setattr(plan, "attach_latest_bge_vectors", lambda *_args: None)
    monkeypatch.setattr(plan, "is_confirmed_publication", lambda _row: True)
    upsert = []
    monkeypatch.setattr(plan, "_upsert_rows", lambda *_args: upsert.append(True) or 1)
    args = SimpleNamespace(
        scan_limit=5000, vector_scan_limit=5000, history_limit=5000,
        timezone="Europe/Kaliningrad", article_time="12:00", social_time="18:00",
        start_date="2026-08-07", days=1, diversity_weight=0.35,
        pair_similarity_threshold=0.82, execute=True,
    )

    result = plan.build_plan(args)

    assert result["ok"] is False
    assert result["status"] == "deferred_live_state_changed"
    assert result["written_ydb_rows"] == 0
    assert upsert == []
