from __future__ import annotations

import copy
import hashlib
import importlib
import importlib.util
import json
import sys
import types
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
    result = mod.prepare_import(
        valid_payload(),
        imported_at="2026-07-19T11:00:00+00:00",
        input_json_sha256="a" * 64,
    )
    assert not result["rejected"]
    row = result["valid"][0]
    assert row["canonical_url"] == "https://example.org/articles/kaliningrad"
    assert row["content_origin_type"] == "academic_publication"
    assert row["decision"]["import_status"] == "ready_for_region_talk_scoring"
    assert row["intake_status"] == "new_intake"
    assert row["review_status"] == "unreviewed"
    assert row["publication_permission"] == "not_granted"
    assert row["input_json_sha256"] == "a" * 64
    assert row["intake_at"] == "2026-07-19T11:00:00+00:00"
    assert row["canonical_evidence_urls"] == ["https://example.org/articles/kaliningrad"]
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


def test_normal_import_stages_separate_publisher_seed_from_publisher_evidence() -> None:
    mod = load_module()
    payload = valid_payload()
    candidate = payload["candidates"][0]
    candidate["evidence"].append({
        "evidence_id": "publisher-about",
        "supports": [
            "publisher.identity", "publisher.audience", "publisher.distinctive_value",
            "publisher.editorial_scope", "publisher.formats", "publisher.locality",
        ],
        "url": "https://example.org/about",
        "page_role": "source_about",
        "retrieved_at": "2026-07-19T10:00:00Z",
        "location_hint": "publisher about page",
        "paraphrase": "The official page describes the journal, audience, scope and format.",
        "quote_short": None,
    })
    source_support = next(
        row for row in candidate["editorial_pack"]["copy_support"]
        if row["surface"] == "source_overview"
    )
    source_support["evidence_refs"] = ["publisher-about"]

    result = mod.prepare_import(payload, raw_input_sha256="c" * 64)

    publisher_rows = [
        row for _pk, kind, row in result["ydb_rows"]
        if kind == "publisher_profile_item"
    ]
    assert len(publisher_rows) == 1
    publisher = publisher_rows[0]
    assert publisher["canonical_source_key"] == "web:example.org"
    assert publisher["publisher_profile_id"].startswith("rtpublisher_")
    assert publisher["profile_origin"] == "external_research_seed"
    assert publisher["profile_status"] == "needs_review"
    assert publisher["usable_without_profile_llm"] is False
    assert publisher["publication_permission"] == "not_granted"
    assert [row["evidence_id"] for row in publisher["evidence"]] == ["publisher-about"]
    assert publisher["evidence_fingerprint"].startswith("rtpublisher_evidence_")
    assert all(kind != "publication_candidate_item" for _pk, kind, _row in result["ydb_rows"])


def test_existing_candidate_replay_can_still_stage_publisher_evidence() -> None:
    mod = load_module()
    payload = valid_payload()
    candidate = payload["candidates"][0]
    url_key = "url:" + mod.canonical_url_identity(candidate["canonical_url"])
    title_key = mod.title_authors_identity(
        candidate["publication"]["title"], candidate["publication"]["authors"]
    )
    external_id = "extpub_" + "e" * 24
    guard = {
        "snapshot_id": "rtseen_live",
        "request": {},
        "urls": set(),
        "dois": set(),
        "titles_authors": set(),
        "identity_map": {url_key: external_id, title_key: external_id},
    }

    result = mod.prepare_import(payload, duplicate_guard=guard, raw_input_sha256="d" * 64)

    assert not result["valid"]
    assert result["replayed"][0]["external_publication_id"] == external_id
    assert any(kind == "publisher_profile_item" for _pk, kind, _row in result["ydb_rows"])
    assert result["batch"]["publisher_profiles_staged"] == 1


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
    assert not result["rejected"]
    assert result["replayed"][0]["external_publication_id"] == result["valid"][0]["external_publication_id"]


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


def test_anonymous_duplicate_guard_fails_closed_without_staging() -> None:
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
    assert result["batch"]["duplicate_seen_rejected"] == 0
    assert not result["rejected"]
    assert not result["replayed"]
    assert result["batch"]["execution_blocked"] is True
    assert result["conflicts"][0]["errors"] == [
        "identity keys resolve to different or unverifiable existing publications"
    ]
    assert all(kind != "external_publication_intake_item" for _, kind, _ in result["ydb_rows"])


def test_live_seen_guard_without_owner_fails_closed() -> None:
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
    assert result["batch"]["duplicate_seen_rejected"] == 0
    assert result["batch"]["execution_blocked"] is True
    assert len(result["conflicts"]) == 1
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
    registry_mod = types.ModuleType("scripts.region_talk_external_research_registry")
    registry_mod.publish_current_registry = lambda *, seen_limit: {
        "seen_publication_count": 0,
        "seen_limit": seen_limit,
    }
    monkeypatch.setattr(request_mod, "read_seen_from_ydb", lambda _limit: [])
    monkeypatch.setattr(mod, "execute_import", lambda prepared: {
        "status": "committed",
        "written_ydb_rows": len(prepared["ydb_rows"]),
        "new_intake_count": 0,
        "new_intake_ids": [],
        "replay_count": 0,
        "replay_ids": [],
        "conflict_count": 0,
    })
    monkeypatch.setitem(sys.modules, registry_mod.__name__, registry_mod)
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
    assert report["registry_publication_enabled"] is True
    assert report["registry_publication"]["seen_limit"] == 20000
    assert report["registry_publication_error"] is None


def test_execute_can_skip_object_storage_registry_for_ydb_only_service_account(
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
    registry_mod = types.ModuleType("scripts.region_talk_external_research_registry")

    def registry_publish_must_not_run(*, seen_limit: int) -> None:
        raise AssertionError(f"registry publish unexpectedly called with {seen_limit=}")

    registry_mod.publish_current_registry = registry_publish_must_not_run
    monkeypatch.setattr(request_mod, "read_seen_from_ydb", lambda _limit: [])
    monkeypatch.setattr(mod, "execute_import", lambda prepared: {
        "status": "committed",
        "written_ydb_rows": len(prepared["ydb_rows"]),
        "new_intake_count": 0,
        "new_intake_ids": [],
        "replay_count": 0,
        "replay_ids": [],
        "conflict_count": 0,
    })
    monkeypatch.setitem(sys.modules, registry_mod.__name__, registry_mod)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(MODULE_PATH),
            str(input_path),
            "--report",
            str(report_path),
            "--execute",
            "--no-publish-registry",
        ],
    )

    assert mod.main() == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["executed"] is True
    assert report["live_duplicate_guard_applied"] is True
    assert report["registry_publication_enabled"] is False
    assert report["registry_publication"] is None
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


def test_manual_review_required_is_not_promoted_on_arrival() -> None:
    mod = load_module()
    payload = valid_payload()
    payload["candidates"][0]["decision"].update({
        "research_decision": "needs_review",
        "downstream_readiness": "manual_review_required",
    })

    row = mod.prepare_import(payload)["valid"][0]

    assert row["decision"]["import_status"] == "manual_review_required"
    assert row["review_status"] == "unreviewed"
    assert row["publication_permission"] == "not_granted"
    assert row["next_action"] == "operator_review_external_research"


def test_url_transport_tracking_query_order_and_www_variants_dedupe() -> None:
    mod = load_module()
    payload = valid_payload()
    payload["candidates"][0]["canonical_url"] = "https://example.org/articles/kaliningrad?a=1&b=2"
    duplicate = copy.deepcopy(payload["candidates"][0])
    duplicate["canonical_url"] = "http://www.example.org/articles//kaliningrad/?b=2&utm_source=x&a=1#top"
    payload["candidates"].append(duplicate)

    result = mod.prepare_import(payload)

    assert len(result["valid"]) == 1
    assert len(result["replayed"]) == 1


def test_exact_normalized_title_authors_dedupe_but_different_authors_do_not() -> None:
    mod = load_module()
    payload = valid_payload()
    duplicate = copy.deepcopy(payload["candidates"][0])
    duplicate["canonical_url"] = "https://mirror.example.net/other-url"
    duplicate["publication"]["title"] = "  ИССЛЕДОВАНИЕ   ПОБЕРЕЖЬЯ Калининградской области "
    duplicate["publication"]["authors"] = [" и. автор "]
    payload["candidates"].append(duplicate)

    result = mod.prepare_import(payload)

    assert len(result["valid"]) == 1
    assert len(result["replayed"]) == 1

    different = copy.deepcopy(duplicate)
    different["canonical_url"] = "https://mirror.example.net/different-author"
    different["publication"]["authors"] = ["Другой Автор"]
    payload["candidates"] = [payload["candidates"][0], different]
    result = mod.prepare_import(payload)
    assert len(result["valid"]) == 2
    assert not result["replayed"]


def test_mixed_identity_keys_resolving_to_different_rows_fail_closed() -> None:
    mod = load_module()
    payload = valid_payload()
    row = payload["candidates"][0]
    title_key = mod.title_authors_identity(row["publication"]["title"], row["publication"]["authors"])
    url_key = "url:" + mod.canonical_url_identity(row["canonical_url"])
    guard = {
        "snapshot_id": "rtseen_conflict",
        "request": {},
        "urls": set(),
        "dois": set(),
        "titles_authors": set(),
        "identity_map": {url_key: "extpub_" + "a" * 24, title_key: "extpub_" + "b" * 24},
    }

    result = mod.prepare_import(payload, duplicate_guard=guard)

    assert not result["valid"]
    assert result["batch"]["execution_blocked"] is True
    assert result["batch"]["identity_conflict_count"] == 1
    assert result["conflicts"][0]["external_publication_ids"] == [
        "extpub_" + "a" * 24,
        "extpub_" + "b" * 24,
    ]


def _install_fake_ydb(mod, monkeypatch):
    durable: dict[str, str] = {}

    class Transaction:
        def __init__(self) -> None:
            self.staged: dict[str, str] = {}

        def execute(self, query, params, commit_tx=False):
            if "SELECT payload_json" in query:
                value = self.staged.get(params["$pk"], durable.get(params["$pk"]))
                rows = [] if value is None else [types.SimpleNamespace(payload_json=value)]
                return [types.SimpleNamespace(rows=rows)]
            assert "UPSERT INTO" in query
            self.staged[params["$pk"]] = params["$payload_json"]
            if commit_tx:
                durable.update(self.staged)
            return []

        def rollback(self):
            self.staged.clear()

    class Session:
        @staticmethod
        def prepare(query):
            return query

        @staticmethod
        def transaction(_mode):
            return Transaction()

    class Pool:
        def __init__(self, _driver) -> None:
            pass

        @staticmethod
        def retry_operation_sync(operation):
            return operation(Session())

    class Driver:
        def __init__(self, **_kwargs) -> None:
            pass

        @staticmethod
        def wait(**_kwargs):
            pass

        @staticmethod
        def stop(**_kwargs):
            pass

    fake_ydb = types.SimpleNamespace(
        Driver=Driver,
        SessionPool=Pool,
        SerializableReadWrite=lambda: object(),
    )
    monkeypatch.setattr(mod, "ensure_ydb_module", lambda: fake_ydb)
    monkeypatch.setattr(mod, "ydb_endpoint_database", lambda: ("grpc://example", "/db"))
    monkeypatch.setattr(mod, "ydb_credentials", lambda _ydb: None)
    monkeypatch.setattr(mod, "ydb_table_path", lambda _database: "/db/state")
    return durable


def test_atomic_execute_is_idempotent_and_request_sha_conflicts_write_nothing(monkeypatch) -> None:
    mod = load_module()
    durable = _install_fake_ydb(mod, monkeypatch)
    first = mod.prepare_import(valid_payload(), raw_input_sha256="a" * 64)

    committed = mod.execute_import(first)
    durable_after_first = dict(durable)
    replay = mod.execute_import(first)

    assert committed["status"] == "committed"
    assert committed["new_intake_count"] == 1
    assert committed["new_intake_ids"] == sorted(committed["new_intake_ids"])
    assert replay == {
        "status": "identical_replay",
        "written_ydb_rows": 0,
        "new_intake_count": 0,
        "new_intake_ids": [],
        "replay_count": 1,
        "replay_ids": committed["new_intake_ids"],
        "conflict_count": 0,
    }
    assert durable == durable_after_first

    changed = valid_payload()
    changed["coverage"] = [{
        "contour": "other", "queries": [], "domains_opened": [],
        "verified_candidate_count": 1, "notes": "changed bytes",
    }]
    conflicting = mod.prepare_import(changed, raw_input_sha256="b" * 64)
    with pytest.raises(mod.ContractError, match="different raw input SHA-256"):
        mod.execute_import(conflicting)
    assert durable == durable_after_first


def test_normal_import_monotonically_enriches_full_profile_without_downgrade(monkeypatch) -> None:
    mod = load_module()
    durable = _install_fake_ydb(mod, monkeypatch)
    prepared = mod.prepare_import(valid_payload(), raw_input_sha256="a" * 64)
    seed_pk, _kind, seed = next(
        row for row in prepared["ydb_rows"] if row[1] == "publisher_profile_item"
    )
    full = copy.deepcopy(seed)
    full.update({
        "profile_origin": "publisher_profile_sidecar",
        "profile_status": "ready",
        "usable_without_profile_llm": True,
        "profile_hash": "f" * 64,
        "profile_hashes": ["f" * 64],
        "profile_dimensions": {
            "outlet_identity": "Verified full publisher identity",
            "intended_audience": [{"label": "readers", "basis": "explicit", "evidence_refs": ["full"]}],
            "distinctive_value": [{"text": "method", "evidence_refs": ["full"]}],
            "editorial_scope": ["research"],
            "recurring_formats": ["articles"],
            "locality_guard": {"brand_scope_basis": "external"},
        },
        "evidence": [{"evidence_id": "full", "supports": ["publisher.identity"], "url": "https://example.org/about"}],
        "evidence_fingerprint": "rtpublisher_evidence_full",
    })
    durable[seed_pk] = json.dumps(full, ensure_ascii=False)

    mod.execute_import(prepared)

    stored = json.loads(durable[seed_pk])
    assert stored["profile_origin"] == "publisher_profile_sidecar"
    assert stored["profile_status"] == "ready"
    assert stored["usable_without_profile_llm"] is True
    assert stored["profile_dimensions"] == full["profile_dimensions"]
    assert {row["evidence_id"] for row in stored["evidence"]} == {"full", "ev-1"}
    intake_pk = next(pk for pk in durable if pk.startswith("external_publication_intake_item:"))
    intake = json.loads(durable[intake_pk])
    assert intake["review_status"] == "unreviewed"
    assert intake["publication_permission"] == "not_granted"


def test_conflicting_publisher_scope_fails_atomic_execute_without_partial_write(monkeypatch) -> None:
    mod = load_module()
    durable = _install_fake_ydb(mod, monkeypatch)
    prepared = mod.prepare_import(valid_payload(), raw_input_sha256="a" * 64)
    seed_pk, _kind, seed = next(
        row for row in prepared["ydb_rows"] if row[1] == "publisher_profile_item"
    )
    conflicting = copy.deepcopy(seed)
    conflicting["scope"] = "regional"
    durable[seed_pk] = json.dumps(conflicting, ensure_ascii=False)
    before = dict(durable)

    with pytest.raises(mod.ContractError, match="scope/locality"):
        mod.execute_import(prepared)

    assert durable == before

def test_identity_reservation_race_fails_closed_without_partial_write(monkeypatch) -> None:
    mod = load_module()
    durable = _install_fake_ydb(mod, monkeypatch)
    first = mod.prepare_import(valid_payload(), raw_input_sha256="a" * 64)
    mod.execute_import(first)
    durable_after_first = dict(durable)
    second_payload = valid_payload()
    second_payload["run"]["request_id"] = "different-request"
    second = mod.prepare_import(second_payload, raw_input_sha256="b" * 64)

    with pytest.raises(mod.ContractError, match="identity reservation conflict"):
        mod.execute_import(second)

    assert durable == durable_after_first


def test_later_replay_persists_observation_and_new_identity_alias(monkeypatch) -> None:
    mod = load_module()
    durable = _install_fake_ydb(mod, monkeypatch)
    first = mod.prepare_import(valid_payload(), raw_input_sha256="a" * 64)
    committed = mod.execute_import(first)
    external_id = committed["new_intake_ids"][0]

    second_payload = valid_payload()
    second_payload["run"]["request_id"] = "later-observation"
    second_payload["candidates"][0]["doi"] = "10.1234/KALININGRAD"
    candidate = second_payload["candidates"][0]
    guard = {
        "snapshot_id": "rtseen_live",
        "request": {},
        "urls": {mod.canonical_url_identity(candidate["canonical_url"])},
        "dois": set(),
        "titles_authors": set(),
        "identity_map": {
            "url:" + mod.canonical_url_identity(candidate["canonical_url"]): external_id,
            mod.title_authors_identity(
                candidate["publication"]["title"], candidate["publication"]["authors"]
            ): external_id,
        },
    }
    second = mod.prepare_import(
        second_payload,
        duplicate_guard=guard,
        raw_input_sha256="b" * 64,
        imported_at="2026-08-02T12:00:00+00:00",
    )

    assert second["batch"]["new_intake_count"] == 0
    assert second["batch"]["replay_count"] == 1
    assert second["batch"]["replay_observation_rows_staged"] == 1
    observation = second["replay_observations"][0]
    assert observation["request_id"] == "later-observation"
    assert observation["input_json_sha256"] == "b" * 64
    assert observation["canonical_evidence_urls"]
    assert "doi:10.1234/kaliningrad" in observation["identity_keys"]

    result = mod.execute_import(second)

    assert result["status"] == "committed"
    assert result["new_intake_count"] == 0
    assert result["replay_ids"] == [external_id]
    doi_sha = hashlib.sha256("doi:10.1234/kaliningrad".encode()).hexdigest()
    doi_pk = "external_publication_identity_item:" + doi_sha
    assert json.loads(durable[doi_pk])["external_publication_id"] == external_id
    observations = [pk for pk in durable if pk.startswith("external_publication_intake_observation_item:")]
    assert len(observations) == 1
    intake_pk = "external_publication_intake_item:" + external_id
    enriched = json.loads(durable[intake_pk])
    assert enriched["request_id"] == valid_payload()["run"]["request_id"]
    assert enriched["input_json_sha256"] == "a" * 64
    assert enriched["review_status"] == "unreviewed"
    assert enriched["publication_permission"] == "not_granted"
    assert "doi:10.1234/kaliningrad" in enriched["identity_keys"]
    assert enriched["provenance_observations"][0]["external_publication_observation_id"].startswith("extpubobs_")
    assert enriched["provenance_observations"][0]["input_json_sha256"] == "b" * 64


def test_execute_with_rejected_row_does_not_call_writer_or_registry(tmp_path: Path, monkeypatch) -> None:
    mod = load_module()
    payload = valid_payload()
    invalid = copy.deepcopy(payload["candidates"][0])
    invalid["canonical_url"] = "http://127.0.0.1/private"
    payload["candidates"].append(invalid)
    input_path = tmp_path / "result.json"
    report_path = tmp_path / "report.json"
    input_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    request_mod = importlib.import_module("scripts.region_talk_external_research_request")
    monkeypatch.setattr(request_mod, "read_seen_from_ydb", lambda _limit: [])
    monkeypatch.setattr(mod, "execute_import", lambda _prepared: pytest.fail("writer must not run"))
    monkeypatch.setattr(sys, "argv", [
        str(MODULE_PATH), str(input_path), "--execute", "--report", str(report_path),
    ])

    assert mod.main() == 2
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["execution_status"] == "rejected_no_write"
    assert report["written_ydb_rows"] == 0
    assert report["registry_publication_enabled"] is False
    assert report["new_intake_ids"] == []
    assert report["batch"]["candidate_rows_valid"] == 1
    assert report["batch"]["candidate_rows_rejected"] == 1


def test_github_workflow_passes_exact_sha_and_publishes_intake_receipt_fields() -> None:
    workflow = (ROOT / ".github" / "workflows" / "region-talk-external-publication-import.yml").read_text(
        encoding="utf-8"
    )

    assert workflow.count("--expected-input-sha256") == 2
    for field in ("new_intake_count", "new_intake_ids", "replay_count", "replay_ids", "conflict_count"):
        assert field in workflow
    assert "### Import receipt" in workflow
