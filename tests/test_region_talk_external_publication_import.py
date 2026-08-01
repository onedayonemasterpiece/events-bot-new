from __future__ import annotations

import copy
import importlib
import importlib.util
import json
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "region_talk_external_publication_import.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_external_publication_import", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def valid_candidate() -> dict:
    score = {"score": 3, "reason": "Проверено по первичной странице", "evidence_refs": ["ev-1"]}
    return {
        "canonical_url": "https://example.org/articles/kaliningrad?utm_source=test",
        "doi": None,
        "publication": {
            "title": "Исследование побережья Калининградской области",
            "authors": ["И. Автор"],
            "source_name": "Внешний журнал",
            "source_domain": "example.org",
            "published_at": "2026-06-10",
            "modified_at": None,
            "date_precision": "day",
            "date_basis": "page_metadata",
            "language": "ru",
            "content_type": "research_article",
            "access_status": "full_text",
        },
        "source_assessment": {
            "entity_type": "journal",
            "scope": "external",
            "externality_basis": "Федеральный тематический журнал",
            "externality_evidence_refs": ["ev-1"],
        },
        "region_relevance": {
            "centrality": "central",
            "topics": ["Балтийское море", "побережье"],
            "summary": "Регион является объектом исследования.",
            "evidence_refs": ["ev-1"],
        },
        "policy_classification": {
            "newsiness": "non_news",
            "commerciality": "independent",
            "research_match": True,
            "product_policy_match": True,
            "language_policy_match": True,
            "hard_exclusion_codes": [],
            "boundary_flags": [],
        },
        "quality_assessment": {
            "track": "scholarly",
            "source_authority": copy.deepcopy(score),
            "evidence_depth": copy.deepcopy(score),
            "editorial_independence": copy.deepcopy(score),
            "originality": copy.deepcopy(score),
            "kaliningrad_centrality": copy.deepcopy(score),
            "public_interest": copy.deepcopy(score),
            "accessibility": copy.deepcopy(score),
            "quality_tier": "credible",
            "scholarly_details": {
                "publication_status": "peer_reviewed",
                "peer_review_basis": "Политика журнала и карточка статьи",
                "study_type": "empirical",
                "methods_visible": True,
                "data_or_sample_scope": "Побережье Калининградской области",
                "limitations_visible": True,
                "funding_disclosed": "yes",
                "conflicts_disclosed": "yes",
                "correction_status": "none_found",
                "scientific_caveat": "Выводы относятся к обследованным участкам.",
            },
            "editorial_details": None,
        },
        "editorial_pack": {
            "title_short": "Что происходит с побережьем",
            "teaser": "Исследование объясняет изменения побережья и их значение для жителей региона.",
            "source_overview": "Внешний научный журнал публикует исследования морской среды.",
            "reader_takeaway": "Какие процессы меняют берег.",
            "why_selected": "Есть понятное научно-популярное зерно.",
            "caveat": "Выводы относятся к обследованным участкам.",
            "copy_support": [
                {"surface": surface, "sentence_index": 0, "evidence_refs": ["ev-1"]}
                for surface in ("teaser", "source_overview", "reader_takeaway", "why_selected", "caveat")
            ],
        },
        "media_and_rights": {
            "media_gate_status": "not_evaluated",
            "rights_policy": "link_only",
            "license": None,
            "media_reuse_allowed": False,
            "candidate_urls": [],
            "evidence_refs": [],
        },
        "evidence": [{
            "evidence_id": "ev-1",
            "supports": ["publication_date", "region_relevance", "editorial_copy"],
            "url": "https://example.org/articles/kaliningrad",
            "page_role": "primary_article",
            "retrieved_at": "2026-07-19T10:00:00Z",
            "location_hint": "заголовок и аннотация",
            "paraphrase": "Страница подтверждает название, дату и предмет исследования.",
            "quote_short": None,
        }],
        "uncertainties": [],
        "decision": {
            "research_decision": "candidate",
            "downstream_readiness": "candidate_report",
            "reason_codes": [],
            "reason_short": "Проходит исследовательские критерии.",
        },
        "related_items": {
            "duplicate_of": None,
            "primary_source_url": "https://example.org/articles/kaliningrad",
            "syndicated_or_commentary_urls": [],
        },
    }


def valid_payload() -> dict:
    return {
        "schema_version": "region_talk_external_research.v1",
        "run": {
            "request_id": "research-2026-07-19",
            "executed_at": "2026-07-19T10:00:00Z",
            "window_start": "2025-07-19",
            "window_end": "2026-07-19",
            "research_languages": ["ru", "en"],
            "product_language_policy": "ru_or_mostly_ru",
            "scope_note": "bounded qualitative web research, not exhaustive",
        },
        "coverage": [],
        "candidates": [valid_candidate()],
        "excluded": [],
        "unresolved": [],
        "run_uncertainties": [],
    }


def test_valid_candidate_is_normalized_to_fail_closed_staging_row() -> None:
    mod = load_module()
    result = mod.prepare_import(valid_payload(), imported_at="2026-07-19T11:00:00+00:00")
    assert not result["rejected"]
    row = result["valid"][0]
    assert row["canonical_url"] == "https://example.org/articles/kaliningrad"
    assert row["content_origin_type"] == "academic_publication"
    assert row["decision"]["import_status"] == "ready_for_region_talk_scoring"
    assert row["media_and_rights"]["media_use_policy"] == "score_only_no_reuse"
    assert row["next_action"] == "run_region_talk_text_vector_and_image_scoring"
    assert all(kind != "publication_candidate_item" for _, kind, _ in result["ydb_rows"])
    source_rows = [payload for _, kind, payload in result["ydb_rows"] if kind == "external_publication_source_item"]
    assert len(source_rows) == 1
    assert source_rows[0]["canonical_source_key"] == "web:example.org"
    assert source_rows[0]["source_topic_class"] == "academic_publication"
    assert source_rows[0]["publisher_source_overview"] == valid_candidate()["editorial_pack"]["source_overview"]
    assert json.loads(source_rows[0]["publisher_source_overview_evidence_refs_json"]) == ["ev-1"]
    assert json.loads(source_rows[0]["publisher_profile_evidence_json"])[0]["evidence_id"] == "ev-1"
    assert result["batch"]["external_sources_staged"] == 1


def test_one_invalid_candidate_does_not_abort_batch() -> None:
    mod = load_module()
    payload = valid_payload()
    invalid = valid_candidate()
    invalid["canonical_url"] = "http://127.0.0.1/private"
    payload["candidates"].append(invalid)
    result = mod.prepare_import(payload)
    assert len(result["valid"]) == 1
    assert len(result["rejected"]) == 1
    assert "private" in " ".join(result["rejected"][0]["errors"])
    assert any(kind == "external_publication_import_error_item" for _, kind, _ in result["ydb_rows"])


def test_candidate_schema_error_is_row_local_but_batch_schema_error_aborts() -> None:
    mod = load_module()
    payload = valid_payload()
    invalid = valid_candidate()
    del invalid["related_items"]
    payload["candidates"].append(invalid)
    result = mod.prepare_import(payload)
    assert len(result["valid"]) == 1
    assert "related_items" in " ".join(result["rejected"][0]["errors"])

    del payload["run"]["scope_note"]
    with pytest.raises(mod.ContractError, match="scope_note"):
        mod.prepare_import(payload)


def test_doi_identity_deduplicates_same_research_batch() -> None:
    mod = load_module()
    payload = valid_payload()
    payload["candidates"][0]["doi"] = "https://doi.org/10.1234/ABC.9"
    duplicate = copy.deepcopy(payload["candidates"][0])
    duplicate["canonical_url"] = "https://mirror.example.net/same-paper"
    duplicate["doi"] = "doi:10.1234/abc.9"
    duplicate["evidence"][0]["url"] = "https://mirror.example.net/same-paper"
    payload["candidates"].append(duplicate)
    result = mod.prepare_import(payload)
    assert len(result["valid"]) == 1
    assert result["valid"][0]["doi"] == "10.1234/abc.9"
    assert result["rejected"][0]["errors"][0].startswith("duplicate of")


def test_hard_exclusion_cannot_masquerade_as_candidate() -> None:
    mod = load_module()
    payload = valid_payload()
    payload["candidates"][0]["policy_classification"]["hard_exclusion_codes"] = ["war_military"]
    result = mod.prepare_import(payload)
    assert not result["valid"]
    assert "hard exclusion" in " ".join(result["rejected"][0]["errors"])


@pytest.mark.parametrize(
    ("field", "value", "expected"),
    [
        ("newsiness", "news", "newsiness=non_news"),
        ("commerciality", "sales", "noncommercial"),
    ],
)
def test_news_or_sales_cannot_masquerade_as_candidate(field: str, value: str, expected: str) -> None:
    mod = load_module()
    payload = valid_payload()
    payload["candidates"][0]["policy_classification"][field] = value
    result = mod.prepare_import(payload)
    assert not result["valid"]
    assert expected in " ".join(result["rejected"][0]["errors"])


def test_clean_candidate_requires_public_interest_and_grounded_copy() -> None:
    mod = load_module()
    payload = valid_payload()
    payload["candidates"][0]["quality_assessment"]["public_interest"]["score"] = 0
    payload["candidates"][0]["editorial_pack"]["copy_support"][0]["evidence_refs"] = []
    result = mod.prepare_import(payload)
    errors = " ".join(result["rejected"][0]["errors"])
    assert "public_interest.score>=2" in errors
    assert "evidence_refs required" in errors


def test_every_nonempty_editorial_surface_requires_copy_support() -> None:
    mod = load_module()
    payload = valid_payload()
    payload["candidates"][0]["editorial_pack"]["copy_support"] = [
        item for item in payload["candidates"][0]["editorial_pack"]["copy_support"]
        if item["surface"] != "source_overview"
    ]
    result = mod.prepare_import(payload)
    assert not result["valid"]
    assert "source_overview: missing copy_support coverage" in " ".join(result["rejected"][0]["errors"])


def test_private_media_url_is_rejected_before_future_image_handoff() -> None:
    mod = load_module()
    payload = valid_payload()
    payload["candidates"][0]["media_and_rights"]["candidate_urls"] = ["http://127.0.0.1/secret.jpg"]
    result = mod.prepare_import(payload)
    assert not result["valid"]
    assert "candidate_urls[0]" in " ".join(result["rejected"][0]["errors"])


def test_media_reuse_requires_verified_rights() -> None:
    mod = load_module()
    payload = valid_payload()
    payload["candidates"][0]["media_and_rights"]["media_reuse_allowed"] = True
    result = mod.prepare_import(payload)
    assert not result["valid"]
    assert "reuse_verified" in " ".join(result["rejected"][0]["errors"])


def test_wrong_schema_is_rejected_before_row_processing() -> None:
    mod = load_module()
    payload = valid_payload()
    payload["schema_version"] = "unknown"
    with pytest.raises(mod.ContractError, match="schema_version"):
        mod.prepare_import(payload)


def test_generated_duplicate_guard_rejects_seen_candidate_before_staging() -> None:
    mod = load_module()
    payload = valid_payload()
    guard = {
        "snapshot_id": "rtseen_1234567890abcdef12345678",
        "request": dict(payload["run"]),
        "urls": {"https://example.org/articles/kaliningrad"},
        "dois": set(),
    }
    result = mod.prepare_import(payload, duplicate_guard=guard)

    assert not result["valid"]
    assert result["batch"]["duplicate_seen_rejected"] == 1
    assert "already seen" in " ".join(result["rejected"][0]["errors"])
    assert all(kind != "external_publication_intake_item" for _, kind, _ in result["ydb_rows"])


def test_live_seen_guard_needs_no_run_specific_request_sidecar() -> None:
    mod = load_module()
    payload = valid_payload()
    live = mod.duplicate_guard_from_seen_publications([{
        "canonical_url": "https://example.org/articles/kaliningrad",
        "doi": None,
        "title": "Исследование побережья Калининградской области",
        "source_name": "Внешний журнал",
        "disposition": "candidate",
    }])

    result = mod.prepare_import(payload, duplicate_guard=live)

    assert not result["valid"]
    assert result["batch"]["duplicate_seen_rejected"] == 1
    assert live["request"] == {}


def test_live_and_legacy_guards_merge_without_losing_identities() -> None:
    mod = load_module()
    payload = valid_payload()
    legacy = {
        "snapshot_id": "rtseen_legacy",
        "request": dict(payload["run"]),
        "urls": {"https://legacy.example.org/item"},
        "dois": set(),
    }
    live = {
        "snapshot_id": "rtseen_live",
        "request": {},
        "urls": {"https://live.example.org/item"},
        "dois": {"10.1234/live"},
    }

    merged = mod.merge_duplicate_guards(legacy, live)

    assert merged is not None
    assert merged["request"] == payload["run"]
    assert merged["urls"] == {"https://legacy.example.org/item", "https://live.example.org/item"}
    assert merged["dois"] == {"10.1234/live"}


def test_execute_without_sidecar_uses_live_ydb_guard_and_refreshes_registry(
    tmp_path: Path,
    monkeypatch,
) -> None:
    mod = load_module()
    payload = valid_payload()
    payload["candidates"] = []
    input_path = tmp_path / "result.json"
    report_path = tmp_path / "report.json"
    input_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    request_mod = importlib.import_module("scripts.region_talk_external_research_request")
    registry_mod = importlib.import_module("scripts.region_talk_external_research_registry")
    monkeypatch.setattr(request_mod, "read_seen_from_ydb", lambda _limit: [])
    monkeypatch.setattr(mod, "write_ydb", lambda rows: len(rows))
    monkeypatch.setattr(
        registry_mod,
        "publish_current_registry",
        lambda *, seen_limit: {"seen_publication_count": 0, "seen_limit": seen_limit},
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(MODULE_PATH),
            str(input_path),
            "--report",
            str(report_path),
            "--execute",
        ],
    )

    assert mod.main() == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["live_duplicate_guard_applied"] is True
    assert report["registry_publication"]["seen_limit"] == 20000
    assert report["registry_publication_error"] is None


def test_duplicate_guard_request_must_match_research_result() -> None:
    mod = load_module()
    payload = valid_payload()
    guard = {
        "snapshot_id": "rtseen_1234567890abcdef12345678",
        "request": {**payload["run"], "request_id": "different-run"},
        "urls": set(),
        "dois": set(),
    }
    with pytest.raises(mod.ContractError, match="do not match"):
        mod.prepare_import(payload, duplicate_guard=guard)


def test_import_stages_seen_ledger_for_candidates_exclusions_and_unresolved() -> None:
    mod = load_module()
    payload = valid_payload()
    payload["excluded"] = [{
        "canonical_url": "https://example.net/local-story?utm_source=search",
        "title": "Локальная заметка",
        "source_name": "Местное издание",
        "reason_codes": ["regional_local_outlet"],
        "reason_short": "Региональный источник",
        "checked_at": "2026-07-19T10:00:00Z",
        "evidence": [],
    }]
    payload["unresolved"] = [{
        "url": "https://example.com/uncertain#abstract",
        "title_guess": "Неясная публикация",
        "blocking_unknowns": ["publication_date"],
        "next_check": "Открыть выпуск журнала",
    }]

    result = mod.prepare_import(payload, imported_at="2026-07-19T11:00:00+00:00")
    seen = [row for _, kind, row in result["ydb_rows"] if kind == "external_publication_seen_item"]

    assert result["batch"]["seen_publication_rows_staged"] == 3
    assert {row["seen_disposition"] for row in seen} == {"candidate", "excluded", "unresolved"}
    assert any(row["canonical_url"] == "https://example.net/local-story" for row in seen)
    assert any(row["canonical_url"] == "https://example.com/uncertain" for row in seen)
