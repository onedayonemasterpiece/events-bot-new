from __future__ import annotations

import json
from types import SimpleNamespace

from scripts import region_talk_publication_plan as plan


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
    return publication, intake


def ready_social(url: str) -> dict:
    return {
        "_ydb_pk": "publication_candidate_item:" + url,
        "post_url": url,
        "publication_score": 0.8,
        "publication_status": "gemini_accept",
        "publication_candidate_status": "sent_to_chat",
        "publication_draft_status": "ready_for_operator_review",
        "publication_draft_title": "Личный маршрут",
        "publication_draft_source_attribution": "Авторский канал",
        "publication_draft_telegram_text": "Фактический черновик\n\nОригинал: " + url,
        "publication_draft_vk_text": "Фактический черновик\n\nОригинал: " + url,
        "publication_draft_fact_points_json": '[{"claim":"Факт","support_excerpt":"Опора"}]',
    }


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


def test_build_plan_projects_legacy_article_and_excludes_social_without_draft(monkeypatch) -> None:
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
    assert result["counts"]["draft_projected_article"] == 1
    assert result["counts"]["draft_missing_social"] == 1
    assert result["counts"]["eligible_article"] == 1
    assert result["counts"]["eligible_social"] == 1
    assert result["counts"]["planned_article"] == 1
    assert result["counts"]["planned_social"] == 1
    article_slot = next(row for row in result["rows"] if row["content_lane"] == "article")
    assert article_slot["publication_title"] == "Архитектура как маршрут"
    assert article_slot["publication_draft_status"] == "ready_for_operator_review"
    projected = next(payload for pk, kind, payload in writes if pk == article["_ydb_pk"] and kind == "publication_candidate_item")
    assert projected["publication_draft_status"] == "ready_for_operator_review"
    assert all(not key.startswith("_") for key in projected)
