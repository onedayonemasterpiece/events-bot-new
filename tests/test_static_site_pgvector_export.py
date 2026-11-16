from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile

import pytest


def _load_exporter_module():
    path = Path(__file__).resolve().parents[1] / "site" / "scripts" / "export-production-preview-data.py"
    spec = importlib.util.spec_from_file_location("export_production_preview_data", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_kaggle_builder_module():
    path = Path(__file__).resolve().parents[1] / "kaggle" / "StaticSiteBuilder" / "static_site_builder.py"
    spec = importlib.util.spec_from_file_location("static_site_builder_compact_related", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_parse_gemma_json_object_accepts_duplicated_structured_parts() -> None:
    exporter = _load_exporter_module()
    parsed = exporter.parse_gemma_json_object(
        '{"ranked":[{"event_id":1,"llm_semantic_score":0.9,"similarity_class":"same","confidence":0.8,"reject":false}]}\n'
        '{"ranked":[{"event_id":2,"llm_semantic_score":0.1,"similarity_class":"no","confidence":0.7,"reject":true}]}'
    )
    assert parsed["ranked"][0]["event_id"] == 1


def test_parse_gemma_json_object_rescues_truncated_complete_items_only() -> None:
    exporter = _load_exporter_module()
    parsed = exporter.parse_gemma_json_object(
        '{"ranked":['
        '{"event_id":1,"llm_semantic_score":0.91,"reject":false},'
        '{"event_id":2,"llm_semantic_score":0.42,"reject":true},'
        '{"event_id":3,"llm_semantic_score":'
    )
    assert parsed == {
        "ranked": [
            {"event_id": 1, "llm_semantic_score": 0.91, "reject": False},
            {"event_id": 2, "llm_semantic_score": 0.42, "reject": True},
        ]
    }


def test_build_gemma_related_audit_prompt_is_compact_and_xml_escaped() -> None:
    exporter = _load_exporter_module()
    anchor = {
        "id": 10,
        "title": "A < B",
        "event_type": "лекция",
        "summary": "Про город & людей",
        "venue_name": "Сигнал",
        "city": "Калининград",
        "start_date": "2026-07-01",
    }
    candidate = {
        "id": 11,
        "title": "Похожее",
        "event_type": "лекция",
        "summary": "Другая лекция",
        "venue_name": "Сигнал",
        "city": "Калининград",
        "start_date": "2026-07-02",
    }
    prompt = exporter.build_gemma_related_audit_prompt(
        anchor=anchor,
        candidates=[candidate],
        fact_max_chars=80,
    )
    assert '<candidate id="11">' in prompt
    assert "A &lt; B" in prompt
    assert "Про город &amp; людей" in prompt
    assert "similarity_class" in prompt
    assert "confidence" in prompt
    assert "reason_codes" not in prompt
    assert len(prompt) < 2100


def test_city_level_fallback_is_not_exported_as_a_venue() -> None:
    exporter = _load_exporter_module()

    assert exporter.drop_city_only_venue("Янтарный", "Янтарный") is None
    assert exporter.drop_city_only_venue("  янтарный  ", "Янтарный") is None
    assert exporter.drop_city_only_venue("Площадь Мастеров", "Янтарный") == "Площадь Мастеров"


def _related_event(event_id: int, title: str, *, day: int) -> dict:
    return {
        "id": event_id,
        "title": title,
        "start_date": f"2026-08-{day:02d}",
        "lifecycle_status": "active",
        "ticket": {},
        "other_date_ids": [],
    }


def test_pgvector_graph_reciprocity_links_equal_titles_without_merging() -> None:
    exporter = _load_exporter_module()
    events = [
        _related_event(4327, "Отдыха не знали, Из руин подняли", day=2),
        _related_event(5382, "🖼️ Выставка «Отдыха не знали, Из руин подняли»", day=4),
        _related_event(7000, "Другая выставка", day=8),
    ]
    chains = {
        "4327": [{
            "event_id": 7000,
            "related_score": 0.7,
            "vector_similarity": 0.76,
            "slot_type": "adjacent_discovery",
            "reason_codes": [],
            "retrieval_sources": ["supabase_pgvector"],
        }],
        "5382": [],
        "7000": [],
    }

    meta = exporter.apply_pgvector_graph_reciprocity(events, chains)

    assert any(item["event_id"] == 5382 for item in chains["4327"])
    assert any(item["event_id"] == 4327 for item in chains["5382"])
    assert meta["exact_title_pairs_missing"] == []
    assert meta["exact_title_links_added"] == 2


def test_pgvector_graph_reciprocity_restores_only_strong_reverse_edges() -> None:
    exporter = _load_exporter_module()
    events = [
        _related_event(1, "Первое", day=1),
        _related_event(2, "Второе", day=2),
        _related_event(3, "Третье", day=3),
    ]
    chains = {
        "1": [
            {
                "event_id": 2,
                "related_score": 0.9,
                "vector_similarity": 0.91,
                "slot_type": "pure_related",
                "reason_codes": [],
                "retrieval_sources": ["supabase_pgvector"],
            },
            {
                "event_id": 3,
                "related_score": 0.7,
                "vector_similarity": 0.79,
                "slot_type": "adjacent_discovery",
                "reason_codes": [],
                "retrieval_sources": ["supabase_pgvector"],
            },
        ],
        "2": [],
        "3": [],
    }

    exporter.apply_pgvector_graph_reciprocity(events, chains)

    assert any(item["event_id"] == 1 for item in chains["2"])
    # The weak pair is not mislabeled as a semantic reciprocal match. A dark
    # node may still be rescued as adjacent discovery, but never pure_related.
    weak_reverse = next((item for item in chains["3"] if item["event_id"] == 1), None)
    assert weak_reverse is None or weak_reverse["slot_type"] == "adjacent_discovery"


def test_pgvector_revision_only_change_does_not_rebuild_unchanged_anchors() -> None:
    exporter = _load_exporter_module()
    events = [
        _related_event(1, "Первое", day=1),
        _related_event(2, "Второе", day=2),
    ]
    calls: list[str] = []

    def fake_build(items, **kwargs):
        calls.append(kwargs.get("embedding_model"))
        chains = {
            "1": [{
                "event_id": 2,
                "related_score": 0.9,
                "vector_similarity": 0.9,
                "slot_type": "pure_related",
                "reason_codes": [],
                "retrieval_sources": ["supabase_pgvector"],
            }],
            "2": [],
        }
        meta_out = kwargs.get("graph_meta_out")
        if meta_out is not None:
            meta_out.update({"policy": "test"})
        return chains

    exporter.build_pgvector_related_chain = fake_build
    with tempfile.TemporaryDirectory() as directory:
        cache_path = Path(directory) / "related.json"
        first = exporter.build_related(
            events,
            current_date="2026-07-20",
            related_mode="pgvector",
            cache_path=cache_path,
            related_corpus_revision="a" * 64,
        )
        second = exporter.build_related(
            events,
            current_date="2026-07-20",
            related_mode="pgvector",
            cache_path=cache_path,
            related_corpus_revision="a" * 64,
        )
        third = exporter.build_related(
            events,
            current_date="2026-07-20",
            related_mode="pgvector",
            cache_path=cache_path,
            related_corpus_revision="b" * 64,
        )

    assert first["cache"]["state"] == "incremental"
    assert second["cache"]["state"] == "hit"
    assert third["cache"]["state"] == "hit"
    assert third["related_corpus_revision"] == "b" * 64
    assert calls == ["gemini-embedding-2"]


def test_pgvector_release_gate_rejects_dark_or_underfilled_graph() -> None:
    exporter = _load_exporter_module()
    try:
        exporter.validate_pgvector_graph_release({
            "zero_incoming_rate": 0.1,
            "exact_title_pairs_missing": [[4327, 5382]],
            "underfilled_event_ids": [99],
        })
    except RuntimeError as exc:
        message = str(exc)
    else:
        raise AssertionError("unhealthy graph unexpectedly passed")
    assert "zero_incoming_rate" in message
    assert "exact_title_pairs_missing" in message
    assert "underfilled_event_ids" in message


def test_pgvector_release_gate_accepts_healthy_graph() -> None:
    exporter = _load_exporter_module()
    exporter.validate_pgvector_graph_release({
        "zero_incoming_rate": 0.0,
        "exact_title_pairs_missing": [],
        "underfilled_event_ids": [],
    })


class _RpcResponse:
    def __init__(self, body: bytes, *, declared_length: int | None = None):
        self.body = body
        self.headers = {}
        if declared_length is not None:
            self.headers["Content-Length"] = str(declared_length)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, limit: int | None = None) -> bytes:
        return self.body if limit is None else self.body[:limit]


def _configure_personalization(monkeypatch) -> None:
    monkeypatch.setenv("PERSONALIZATION_SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("PERSONALIZATION_SUPABASE_SECRET_KEY", "service-role-test")


def test_compact_rpc_rejects_oversized_body_before_json_decode(monkeypatch) -> None:
    exporter = _load_exporter_module()
    _configure_personalization(monkeypatch)
    body = b"[" + b" " * 2048
    monkeypatch.setattr(
        exporter.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _RpcResponse(body),
    )

    with pytest.raises(RuntimeError, match="response exceeds 1024 bytes"):
        exporter.personalization_supabase_request(
            exporter.COMPACT_RELATED_RPC,
            {"p_anchor_event_id": 1},
            response_max_bytes=1024,
            expected_row_fields=exporter.COMPACT_RELATED_FIELDS,
        )


def test_compact_rpc_enforces_exact_narrow_projection(monkeypatch) -> None:
    exporter = _load_exporter_module()
    _configure_personalization(monkeypatch)
    wide = [{
        "event_id": 2,
        "vector_similarity": 0.9,
        "card_snapshot": {"unexpected": "egress"},
    }]
    monkeypatch.setattr(
        exporter.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _RpcResponse(json.dumps(wide).encode()),
    )

    with pytest.raises(RuntimeError, match="violates compact projection"):
        exporter.personalization_supabase_request(
            exporter.COMPACT_RELATED_RPC,
            {"p_anchor_event_id": 1},
            response_max_bytes=4096,
            expected_row_fields=exporter.COMPACT_RELATED_FIELDS,
        )


def test_compact_rpc_enforces_aggregate_full_rebuild_ceiling(monkeypatch) -> None:
    exporter = _load_exporter_module()
    _configure_personalization(monkeypatch)
    row = [{"event_id": 2, "vector_similarity": 0.9}]
    body = json.dumps(row).encode() + b" " * 600
    monkeypatch.setattr(
        exporter.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _RpcResponse(body),
    )
    metrics = {"request_count": 0, "row_count": 0, "response_bytes": 0}
    exporter.personalization_supabase_request(
        exporter.COMPACT_RELATED_RPC,
        {"p_anchor_event_id": 1},
        response_max_bytes=2048,
        total_response_max_bytes=1000,
        metrics=metrics,
        expected_row_fields=exporter.COMPACT_RELATED_FIELDS,
    )
    with pytest.raises(RuntimeError, match="compact response total .* exceeds 1000 bytes"):
        exporter.personalization_supabase_request(
            exporter.COMPACT_RELATED_RPC,
            {"p_anchor_event_id": 1},
            response_max_bytes=2048,
            total_response_max_bytes=1000,
            metrics=metrics,
            expected_row_fields=exporter.COMPACT_RELATED_FIELDS,
        )
    assert metrics["request_count"] == 2
    assert metrics["response_bytes"] == len(body) * 2


def test_compact_pgvector_graph_matches_legacy_candidate_shape(monkeypatch) -> None:
    exporter = _load_exporter_module()
    events = [
        _related_event(1, "Архитектура города", day=1),
        _related_event(2, "Городская архитектура", day=2),
        _related_event(3, "Камерный концерт", day=3),
    ]
    golden = {
        1: [(2, 0.94), (3, 0.61)],
        2: [(1, 0.94), (3, 0.60)],
        3: [(2, 0.60), (1, 0.59)],
    }

    def build_rows(*, legacy: bool):
        def fake_request(_name, payload, **_kwargs):
            anchor = int(payload["p_anchor_event_id"])
            rows = [
                {
                    "event_id": event_id,
                    "vector_similarity": similarity,
                }
                for event_id, similarity in golden[anchor]
            ]
            if legacy:
                for row in rows:
                    row.update({"anchor_event_id": anchor, "distance": 1 - row["vector_similarity"], "title": "wide", "card_snapshot": {}})
            return rows
        return fake_request

    monkeypatch.setattr(exporter, "personalization_supabase_request", build_rows(legacy=True))
    legacy_graph = exporter.build_pgvector_related_chain(events, current_date="2026-07-20")
    monkeypatch.setattr(exporter, "personalization_supabase_request", build_rows(legacy=False))
    compact_graph = exporter.build_pgvector_related_chain(events, current_date="2026-07-20")

    assert compact_graph == legacy_graph


def test_pgvector_cache_hit_makes_zero_related_rpc_calls_and_records_receipt(monkeypatch) -> None:
    exporter = _load_exporter_module()
    events = [
        _related_event(1, "Первое", day=1),
        _related_event(2, "Второе", day=2),
    ]
    calls: list[int] = []

    def fake_request(name, payload, **kwargs):
        assert name == exporter.COMPACT_RELATED_RPC
        anchor = int(payload["p_anchor_event_id"])
        calls.append(anchor)
        metrics = kwargs["metrics"]
        metrics["request_count"] += 1
        metrics["row_count"] += 1
        metrics["response_bytes"] += 72
        metrics["max_single_response_bytes"] = 72
        return [{
            "event_id": 2 if anchor == 1 else 1,
            "vector_similarity": 0.9,
        }]

    monkeypatch.setattr(exporter, "personalization_supabase_request", fake_request)
    with tempfile.TemporaryDirectory() as directory:
        cache_path = Path(directory) / "related.json"
        first = exporter.build_related(
            events,
            current_date="2026-07-20",
            related_mode="pgvector",
            cache_path=cache_path,
            related_corpus_revision="a" * 64,
        )
        first_calls = list(calls)
        calls.clear()
        second = exporter.build_related(
            events,
            current_date="2026-07-20",
            related_mode="pgvector",
            cache_path=cache_path,
            related_corpus_revision="a" * 64,
        )

    assert first_calls == [1, 2]
    assert first["retrieval_receipt"] == {
        "schema_version": "static_related_retrieval_receipt_v1",
        "rpc": exporter.COMPACT_RELATED_RPC,
        "projection": ["event_id", "vector_similarity"],
        "request_count": 2,
        "row_count": 2,
        "response_bytes": 144,
        "max_single_response_bytes": 72,
        "response_max_bytes": exporter.DEFAULT_RELATED_RESPONSE_MAX_BYTES,
        "total_response_max_bytes": None,
        "source": "compact_rpc",
    }
    assert calls == []
    assert second["cache"]["state"] == "hit"
    assert second["retrieval_receipt"]["request_count"] == 0
    assert second["retrieval_receipt"]["response_bytes"] == 0
    assert second["retrieval_receipt"]["source"] == "cache"


def test_scoped_pgvector_ranks_programmes_before_repeated_dates(monkeypatch):
    exporter = _load_exporter_module()
    events = [_related_event(i, f"Programme {i}", day=1) for i in range(1, 58)]
    for event in events:
        if 2 <= event["id"] <= 42:
            # Reciprocal links may form a chain, not a clique.
            event["other_date_ids"] = [i for i in (event["id"] - 1, event["id"] + 1) if 2 <= i <= 42]
    events[42]["other_date_ids"] = [2]  # One-way, so 43 is independent.
    matches = list(range(2, 58))
    calls = []

    def rpc(_name, payload, **_kwargs):
        calls.append(payload["p_anchor_event_id"])
        return [{"event_id": i, "vector_similarity": 1 - i / 100} for i in matches]

    monkeypatch.setattr(exporter, "personalization_supabase_request", rpc)
    graph = exporter.build_pgvector_related_chain(events, current_date="2026-07-20", anchor_ids={1})
    assert calls == [1]
    selected = [row["event_id"] for row in graph["1"]]
    assert selected == [2, *range(43, 58), *range(3, 27)]
    assert graph["1"][1]["vector_similarity"] == .57
    assert set(graph) == {"1"}


def test_legacy_programme_order_migrates_requested_anchors_lazily(tmp_path, monkeypatch):
    exporter = _load_exporter_module()
    events = [_related_event(i, f"Programme {i}", day=i) for i in range(1, 7)]
    events[1]["other_date_ids"], events[2]["other_date_ids"] = [3], [2]
    replies, audits = [], []

    def rpc(_name, payload, **_kwargs):
        anchor = payload["p_anchor_event_id"]
        replies.append(anchor)
        return [{"event_id": i, "vector_similarity": .91 - i / 100}
                for i in range(1, 7) if i != anchor]

    def audit(**kwargs):
        audits.append(kwargs["anchor"]["id"])
        return {e["id"]: {"llm_semantic_score": .9, "confidence": .9, "reject": False}
                for e in kwargs["candidates"]}

    monkeypatch.setattr(exporter, "personalization_supabase_request", rpc)
    monkeypatch.setattr(exporter, "call_gemma_related_audit", audit)
    monkeypatch.setenv("GOOGLE_API_KEY4", "test-only")
    cache_path = tmp_path / "cache.json"

    def run(anchors):
        return exporter.build_related(events, current_date="2026-07-20", related_mode="pgvector",
                                      cache_path=cache_path, gemma_verify=True, anchor_ids=anchors)

    run(None)
    legacy = json.loads(cache_path.read_text())
    legacy["schema_version"] = "event_pgvector_related_chain_v2_cache_20261114_content_vector_only"
    legacy["event_fingerprints"] = {str(e["id"]): exporter.event_fingerprint({**e, "other_date_ids": []}) for e in events}
    cache_path.write_text(json.dumps(legacy))
    replies.clear()
    scoped = run({1})
    assert replies == [1]
    assert scoped["related"]["1"]["strict_verified"]
    assert not scoped["related"]["6"]["strict_verified"]
    assert scoped["related"]["6"]["pure_related"] == [item["event_id"] for item in legacy["chains"]["6"]]
    run({1})
    assert replies == [1]
    run({6})
    assert replies == [1, 6]
    full = run(None)
    assert replies == [1, 6]
    assert full["related"]["1"]["strict_verified"]
    assert full["related"]["6"]["strict_verified"]


def test_pgvector_rejects_duplicate_anchor_fetch_before_rpc(monkeypatch) -> None:
    exporter = _load_exporter_module()
    calls = 0

    def fake_request(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        return []

    monkeypatch.setattr(exporter, "personalization_supabase_request", fake_request)
    duplicate = _related_event(1, "Один", day=1)
    with pytest.raises(RuntimeError, match="unique anchor event ids"):
        exporter.build_pgvector_related_chain(
            [duplicate, dict(duplicate)], current_date="2026-07-20"
        )
    assert calls == 0


def test_compact_rpc_migration_is_service_role_only_and_narrow() -> None:
    root = Path(__file__).resolve().parents[1]
    migration = next(
        (root / "supabase" / "migrations").glob(
            "*_compact_static_related_candidates_v1.sql"
        )
    ).read_text(encoding="utf-8").lower()
    returns = migration.split("returns table", 1)[1].split("language plpgsql", 1)[0]
    assert "event_id bigint" in returns
    assert "vector_similarity double precision" in returns
    assert all(field not in returns for field in ("anchor_event_id", "card_snapshot", "title text", "tags text[]", "distance"))
    assert "from public, anon, authenticated" in migration
    assert "to service_role" in migration


def test_kaggle_build_receipt_reads_compact_related_counters(monkeypatch, tmp_path) -> None:
    builder = _load_kaggle_builder_module()
    site = tmp_path / "site"
    data = site / "src" / "data"
    data.mkdir(parents=True)
    receipt = {
        "schema_version": "static_related_retrieval_receipt_v1",
        "rpc": "event_related_candidates_compact_by_event_id_v1",
        "request_count": 9,
        "row_count": 42,
        "response_bytes": 1234,
    }
    (data / "preview-related.json").write_text(
        json.dumps({"retrieval_receipt": receipt}), encoding="utf-8"
    )
    monkeypatch.setattr(builder, "SITE_DIR", site)

    assert builder.read_related_retrieval_receipt() == {"status": "recorded", **receipt}


def test_kaggle_runner_and_kernel_forward_compact_response_budgets() -> None:
    root = Path(__file__).resolve().parents[1]
    runner = (root / "scripts" / "run_static_site_builder_kaggle.py").read_text(encoding="utf-8")
    kernel = (root / "kaggle" / "StaticSiteBuilder" / "static_site_builder.py").read_text(encoding="utf-8")
    for source in (runner, kernel):
        assert "--related-response-max-bytes" in source
        assert "--related-total-response-max-bytes" in source
    assert "'related_retrieval': related_retrieval" in kernel


def test_pgvector_similarity_is_not_promoted_by_category_or_facets(monkeypatch) -> None:
    exporter = _load_exporter_module()
    events = [_related_event(i, title, day=i) for i, title in (
        (1, "Матч женской лиги"), (2, "Дни литературы"),
        (3, "Финал женской лиги"), (4, "Лига КВН"))]
    for event in events:
        event.update(event_type="краеведение", topics=["KRAEVEDENIE_KALININGRAD_OBLAST"],
                     city="Калининград", venue_name="Общая площадка")
    events[2].update(event_type="спорт", topics=["ACTIVE"], city="Другой город")
    scores = {2: 0.689028663, 3: 0.832861571, 4: 0.776931994}
    monkeypatch.setattr(exporter, "personalization_supabase_request",
                        lambda _rpc, payload, **_kw: [
                            {"event_id": i, "vector_similarity": score}
                            for i, score in scores.items()
                        ] if payload["p_anchor_event_id"] == 1 else [])
    chain = exporter.build_pgvector_related_chain(events, current_date="2026-08-01")["1"]
    assert [item["event_id"] for item in chain] == [3, 4, 2]
    assert [item["event_id"] for item in chain if item["slot_type"] == "pure_related"] == [3]
    assert all(item["related_score"] == round(scores[item["event_id"]], 4) for item in chain)
    # Equal titles may remain discoverable, but cannot manufacture a vector match.
    events[1]["title"] = events[0]["title"]
    chain = exporter.build_pgvector_related_chain(events, current_date="2026-08-01")["1"]
    literature = next(item for item in chain if item["event_id"] == 2)
    assert literature["slot_type"] == "adjacent_discovery"
    assert literature["related_score"] == round(scores[2], 4)


def test_related_audit_completes_uncached_unchanged_anchors_during_import(monkeypatch) -> None:
    exporter = _load_exporter_module()
    monkeypatch.setenv('GOOGLE_API_KEY4', 'unit-test-only')
    monkeypatch.setenv('STATIC_SITE_GEMMA_RELATED_PASSES', '1')
    calls = []

    def audit(**kwargs):
        calls.append(kwargs['anchor']['id'])
        return {event['id']: {'llm_semantic_score': .8, 'confidence': .9,
                'reject': False, 'similarity_class': 'highly_similar'}
                for event in kwargs['candidates']}

    monkeypatch.setattr(exporter, 'call_gemma_related_audit', audit)
    events = [{'id': 1, 'title': 'Открытый микрофон'},
              {'id': 2, 'title': 'Сольный стендап'},
              {'id': 3, 'title': 'Новая выставка'}]
    cache = {}

    def run():
        chains = {str(i): [{'event_id': 3 - i, 'vector_similarity': .8,
                           'related_score': .8, 'slot_type': 'pure_related'}]
                  for i in (1, 2)}
        return exporter.maybe_apply_gemma_audit(events, chains, enabled=True,
            model='models/gemma-4-26b-a4b-it', key_env='GOOGLE_API_KEY4',
            cache=cache, max_anchors=0, changed_event_ids={3})[1]

    cold = run()
    assert cold['verified_event_ids'] == [1, 2, 3]
    assert cold['status'] == 'ok'
    assert calls == [1, 2]
    warm = run()
    assert warm['status'] == 'ok' and warm['cache_hits'] == 2
    assert calls == [1, 2]


def test_related_audit_uses_semantics_without_generated_category() -> None:
    exporter = _load_exporter_module()
    event = {'id': 1, 'title': 'Матч женской баскетбольной лиги',
             'summary': 'Баскетбольный матч', 'topics': ['kraevedenie_kaliningrad_oblast']}
    prompt = exporter.build_gemma_related_audit_prompt(anchor=event, candidates=[event], fact_max_chars=360)
    assert '<category>' not in prompt
    assert 'Баскетбольный матч' in prompt


def test_related_audit_retry_respects_shared_minute_bucket(monkeypatch) -> None:
    from google_ai.exceptions import RateLimitError
    exporter = _load_exporter_module()
    monkeypatch.setenv('GOOGLE_API_KEY4', 'unit-test-only')
    monkeypatch.setenv('STATIC_SITE_GEMMA_RELATED_MAX_ATTEMPTS', '2')
    monkeypatch.setenv('STATIC_SITE_GEMMA_RELATED_RETRY_BACKOFF_SEC', '10')
    monkeypatch.setenv('STATIC_SITE_GEMMA_RELATED_PASSES', '1')
    calls = []
    waits = []
    def audit(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise RateLimitError(blocked_reason='tpm', retry_after_ms=42000)
        return {2: {'llm_semantic_score': .8, 'confidence': .9, 'reject': False,
                    'similarity_class': 'highly_similar'}}
    monkeypatch.setattr(exporter, 'call_gemma_related_audit', audit)
    monkeypatch.setattr(exporter.time, 'sleep', waits.append)
    chains = {'1': [{'event_id': 2, 'vector_similarity': .78, 'related_score': .78,
                     'slot_type': 'adjacent_discovery'}]}
    result, meta = exporter.maybe_apply_gemma_audit(
        [{'id': 1, 'title': 'Открытый микрофон'}, {'id': 2, 'title': 'Сольный стендап'}],
        chains, enabled=True, model='models/gemma-4-26b-a4b-it', key_env='GOOGLE_API_KEY4',
        cache={}, max_anchors=0)
    assert waits == [42.0]
    assert len(calls) == 2
    assert result['1'][0]['slot_type'] == 'pure_related'
    assert meta['status'] == 'ok'


@pytest.mark.asyncio
async def test_related_audit_inherits_operator_normal_pool(monkeypatch) -> None:
    import google_ai.client
    exporter = _load_exporter_module()
    monkeypatch.setenv('GOOGLE_API_KEY4', 'unit-test-only')
    monkeypatch.setenv('GOOGLE_AI_NORMAL_KEY_ENVS', 'GOOGLE_API_KEY,GOOGLE_API_KEY4')
    captured = {}
    class Client:
        def __init__(self, **kwargs):
            captured.update(kwargs)
        async def generate_content_async(self, **kwargs):
            return json.dumps({'ranked': [{'event_id': 2, 'llm_semantic_score': .8,
                   'similarity_class': 'highly_similar', 'confidence': .9, 'reject': False}]}), None
    monkeypatch.setattr(google_ai.client, 'GoogleAIClient', Client)
    monkeypatch.setattr(exporter, 'supabase_limiter_client', object)
    result = await exporter.call_gemma_related_audit_async(
        model='models/gemma-4-26b-a4b-it', key_env='GOOGLE_API_KEY4',
        anchor={'id': 1, 'title': 'Standup'}, candidates=[{'id': 2, 'title': 'Comedy'}])
    assert captured['reserve_key_envs'] == 'GOOGLE_API_KEY,GOOGLE_API_KEY4'
    assert result[2]['llm_semantic_score'] == .8


def test_occurrence_and_duplicate_verdicts_never_become_semantic_recommendations(monkeypatch) -> None:
    exporter = _load_exporter_module()
    monkeypatch.setenv('GOOGLE_API_KEY4', 'unit-test-only')
    monkeypatch.setenv('STATIC_SITE_GEMMA_RELATED_PASSES', '1')
    monkeypatch.setattr(exporter.time, 'sleep', lambda _: None)
    monkeypatch.setattr(exporter, 'call_gemma_related_audit', lambda **_: {
        2: {'llm_semantic_score': .99, 'confidence': .95, 'reject': False,
            'similarity_class': 'same_occurrence_group'},
        3: {'llm_semantic_score': .99, 'confidence': .95, 'reject': False,
            'similarity_class': 'duplicate_same_slot'},
        4: {'llm_semantic_score': .8, 'confidence': .9, 'reject': False,
            'similarity_class': 'highly_similar'},
    })
    events = [{'id': i, 'title': f'Event {i}'} for i in range(1, 5)]
    chains = {'1': [{'event_id': i, 'vector_similarity': .95, 'related_score': .95,
                     'slot_type': 'pure_related'} for i in (2, 3, 4)]}
    result, _ = exporter.maybe_apply_gemma_audit(events, chains, enabled=True,
        model='models/gemma-4-26b-a4b-it', key_env='GOOGLE_API_KEY4', cache={}, max_anchors=0)
    assert [item['event_id'] for item in result['1']] == [4]


def test_related_verifier_keeps_description_beyond_short_caption() -> None:
    exporter = _load_exporter_module()
    event = {'id': 1, 'title': 'Лекция', 'summary': 'Краткая подпись',
             'description_html': '<p>История фотографии и документальные снимки</p>'}
    block = exporter._related_audit_event_block(event, tag='event', fact_max_chars=360)
    assert 'История фотографии и документальные снимки' in block
    assert '<p>' not in block and '&lt;p&gt;' not in block


def test_strict_anchor_flag_requires_completed_audit_and_accepts_empty_candidates(monkeypatch):
    exporter = _load_exporter_module()
    monkeypatch.setenv('GOOGLE_API_KEY4', 'unit-test-only')
    monkeypatch.setenv('STATIC_SITE_GEMMA_RELATED_PASSES', '2')
    monkeypatch.setenv('STATIC_SITE_GEMMA_RELATED_CANDIDATE_LIMIT', '6')
    monkeypatch.setenv('STATIC_SITE_GEMMA_RELATED_MAX_ATTEMPTS', '1')
    events = [_related_event(i, f'Event {i}', day=i) for i in range(1, 9)]
    def audit(**kwargs):
        if kwargs['candidates'][0]['id'] == 8:
            raise RuntimeError('second audit pass failed')
        return {e['id']: {'llm_semantic_score': .8, 'confidence': .9, 'reject': False,
                         'similarity_class': 'highly_similar'} for e in kwargs['candidates']}
    monkeypatch.setattr(exporter, 'call_gemma_related_audit', audit)
    monkeypatch.setattr(exporter, 'build_pgvector_related_chain', lambda *a, **k: {
        str(i): ([{'event_id': j, 'vector_similarity': .9, 'related_score': .9,
                   'slot_type': 'pure_related'} for j in range(2, 9)] if i == 1 else [])
        for i in range(1, 9)})
    result = exporter.build_related(events, current_date='2026-07-20', related_mode='pgvector',
                                    gemma_verify=True)
    assert result['related']['1']['pure_related']
    assert not result['related']['1']['strict_verified']
    assert result['related']['2']['strict_verified']  # No candidates is a complete empty result.
    assert not result['strict_verified_related']
    assert result['gemma_verification']['status'] == 'partial'


def test_related_cold_budget_preserves_later_cache_and_empty_anchors(monkeypatch):
    exporter = _load_exporter_module()
    monkeypatch.setenv('GOOGLE_API_KEY4', 'unit-test-only')
    monkeypatch.setenv('STATIC_SITE_GEMMA_RELATED_PASSES', '1')
    calls = []

    def audit(**kwargs):
        calls.append(kwargs['anchor']['id'])
        return {candidate['id']: {'llm_semantic_score': .8, 'confidence': .9,
                'reject': False, 'similarity_class': 'highly_similar'}
                for candidate in kwargs['candidates']}

    monkeypatch.setattr(exporter, 'call_gemma_related_audit', audit)
    cache = {}
    events = [{'id': i, 'title': f'Event {i}'} for i in range(1, 5)]

    def run(selected, limit, only=None):
        chains = {str(event['id']): ([{'event_id': 2 if event['id'] == 1 else 1,
            'vector_similarity': .8, 'related_score': .8, 'slot_type': 'pure_related'}]
            if event['id'] != 4 and (only is None or event['id'] in only) else [])
            for event in selected}
        return exporter.maybe_apply_gemma_audit(selected, chains, enabled=True,
            model='models/gemma-4-26b-a4b-it', key_env='GOOGLE_API_KEY4',
            cache=cache, max_anchors=limit)[1]

    run(events, 0, {1})  # An older anchor already has a strict semantic verdict.
    calls.clear()
    first = run(list(reversed(events)), 1)
    assert calls == [3]  # Newest uncached ID first, not old catalog order.
    assert first['verified_event_ids'] == [1, 3, 4]
    assert first['cache_hits'] == 1 and first['attempted_anchors'] == 1
    assert first['status'] == 'partial'  # Unreviewed anchor2 stays unverified.
    second = run(events, 1)
    assert calls == [3, 2]
    assert second['verified_event_ids'] == [1, 2, 3, 4]
    assert second['status'] == 'ok'
    warm = run(events, 1)
    assert calls == [3, 2]  # Repeated warm progress spends no new calls.
    assert warm['cache_hits'] == 3 and warm['attempted_anchors'] == 0
    assert warm['verified_event_ids'] == [1, 2, 3, 4]


@pytest.mark.parametrize('limit, changed, expected_calls', [
    (1, {2}, [2]), (1, set(), [3]), (0, {2}, [1, 2, 3])])
def test_related_cold_budget_prioritizes_changed_but_zero_remains_unbounded(
        monkeypatch, limit, changed, expected_calls):
    exporter = _load_exporter_module()
    monkeypatch.setenv('GOOGLE_API_KEY4', 'unit-test-only')
    monkeypatch.setenv('STATIC_SITE_GEMMA_RELATED_PASSES', '1')
    calls = []

    def audit(**kwargs):
        calls.append(kwargs['anchor']['id'])
        return {candidate['id']: {'llm_semantic_score': .8, 'confidence': .9,
                'reject': False, 'similarity_class': 'highly_similar'}
                for candidate in kwargs['candidates']}

    monkeypatch.setattr(exporter, 'call_gemma_related_audit', audit)
    events = [{'id': i, 'title': f'Event {i}'} for i in range(1, 4)]
    chains = {str(i): [{'event_id': 2 if i == 1 else 1, 'vector_similarity': .8,
              'related_score': .8, 'slot_type': 'pure_related'}] for i in (1, 2, 3)}
    _, meta = exporter.maybe_apply_gemma_audit(events, chains, enabled=True,
        model='models/gemma-4-26b-a4b-it', key_env='GOOGLE_API_KEY4',
        cache={}, max_anchors=limit, changed_event_ids=changed)
    assert calls == expected_calls
    assert meta['verified_event_ids'] == sorted(expected_calls)


def test_shrinking_catalog_persists_bounded_audit_progress(tmp_path, monkeypatch):
    exporter = _load_exporter_module()
    monkeypatch.setenv('GOOGLE_API_KEY4', 'unit-test-only')
    monkeypatch.setenv('STATIC_SITE_GEMMA_RELATED_PASSES', '1')
    monkeypatch.delenv('STATIC_SITE_ALLOW_RELATED_CACHE_SHRINK', raising=False)
    cache_path = tmp_path / 'related-cache.json'
    old_audit = {'99': {'llm_semantic_score': .8}}
    cache_path.write_text(json.dumps({'event_ids': [1, 2, 3, 4],
        'gemma_audit_cache': {'previous-verdict': old_audit}}))
    events = [{'id': i, 'title': f'Event {i}'} for i in (1, 2, 3)]
    calls = []

    def audit(**kwargs):
        calls.append(kwargs['anchor']['id'])
        return {candidate['id']: {'llm_semantic_score': .8, 'confidence': .9,
                'reject': False, 'similarity_class': 'highly_similar'}
                for candidate in kwargs['candidates']}

    monkeypatch.setattr(exporter, 'call_gemma_related_audit', audit)
    monkeypatch.setattr(exporter, 'build_pgvector_related_chain', lambda *a, **k: {
        str(i): [{'event_id': 2 if i == 1 else 1, 'vector_similarity': .8,
                  'related_score': .8, 'slot_type': 'pure_related'}] for i in (1, 2, 3)})
    retained_keys = set()
    for expected_ready in (1, 2, 3, 3):
        result = exporter.build_related(events, current_date='2026-11-15',
            related_mode='pgvector', cache_path=cache_path, gemma_verify=True,
            gemma_max_anchors=1, anchor_ids={1, 2, 3})
        assert result['cache']['written']
        assert len(result['gemma_verification']['verified_event_ids']) == expected_ready
        current_keys = set(json.loads(cache_path.read_text())['gemma_audit_cache'])
        assert retained_keys.issubset(current_keys), 'successful progress must persist'
        retained_keys = current_keys
    assert calls == [3, 2, 1]
    assert result['strict_verified_related']
    assert result['gemma_verification']['provider_calls'] == 0
    saved = json.loads(cache_path.read_text())
    assert saved['event_ids'] == [1, 2, 3]
    assert 'previous-verdict' not in saved['gemma_audit_cache']
    assert len(saved['gemma_audit_cache']) == 3


@pytest.mark.parametrize('empty_first_batch', [False, True])
def test_partial_related_batches_survive_save_and_resume(tmp_path, monkeypatch, empty_first_batch):
    exporter = _load_exporter_module()
    monkeypatch.setenv('GOOGLE_API_KEY4', 'unit-test-only')
    monkeypatch.setenv('STATIC_SITE_GEMMA_RELATED_PASSES', '2')
    monkeypatch.setenv('STATIC_SITE_GEMMA_RELATED_CANDIDATE_LIMIT', '6')
    monkeypatch.setenv('STATIC_SITE_GEMMA_RELATED_MAX_ATTEMPTS', '1')
    events = [{'id': i, 'title': f'Programme {i}', 'date': '2026-12-01'} for i in range(1, 14)]
    calls = []
    cycle = 1
    def retrieve(*args, **kwargs):
        return {'1': [{'event_id': i, 'vector_similarity': .8, 'related_score': .8,
                        'slot_type': 'adjacent_discovery'} for i in range(2, 14)]}
    def audit(**kwargs):
        ids = [event['id'] for event in kwargs['candidates']]
        calls.append(ids)
        if cycle == 1 and ids[0] == 8:
            raise RuntimeError('provider minute bucket exhausted')
        if cycle == 2 and ids[0] == 2:
            pytest.fail('completed batch was repeated after saving the cache')
        if empty_first_batch and ids[0] == 2:
            return {}
        return {ids[0]: {'llm_semantic_score': .8, 'confidence': .9,
                        'reject': False, 'similarity_class': 'highly_similar'}}
    monkeypatch.setattr(exporter, 'build_pgvector_related_chain', retrieve)
    monkeypatch.setattr(exporter, 'call_gemma_related_audit', audit)
    path = tmp_path / 'cache.json'
    def run():
        return exporter.build_related(events, current_date='2026-11-15',
            related_mode='pgvector', cache_path=path, gemma_verify=True,
            gemma_max_anchors=20, anchor_ids={1})
    first = run()
    assert not first['related']['1']['strict_verified']
    cycle = 2
    second = run()
    assert second['related']['1']['strict_verified']
    assert calls == [list(range(2, 8)), list(range(8, 14)), list(range(8, 14))]
    cycle = 3
    events[1]['title'] = 'Changed source programme'
    third = run()
    assert third['related']['1']['strict_verified']
    assert calls[-1] == list(range(2, 8))
    assert len(calls) == 4, 'changed peer invalidates its batch, while the other stays reusable'


@pytest.mark.parametrize("change_programme, cap, failure", [
    (False, 0, False), (False, 1, False), (True, 0, False), (True, 1, False), (True, 1, True)])
def test_warm_pgvector_targets_keep_partial_cache_and_rebuild_dependents(tmp_path, monkeypatch, change_programme, cap, failure):
    exporter = _load_exporter_module()
    events = [_related_event(i, f"Programme {i}", day=i) for i in range(1, 7)]
    rows = {1: [2, 3, 4], 2: [1], 3: [1], 4: [2, 3, 1], 5: [6], 6: [5]}
    fetched, audited, failures = [], [], set()

    def rpc(_name, payload, **_kwargs):
        anchor = payload["p_anchor_event_id"]
        fetched.append(anchor)
        return [{"event_id": peer, "vector_similarity": .85 - rank / 100}
                for rank, peer in enumerate(rows[anchor])]

    def audit(**kwargs):
        audited.append(kwargs["anchor"]["id"])
        if kwargs["anchor"]["id"] in failures:
            raise RuntimeError("offline provider failure")
        return {e["id"]: {"llm_semantic_score": .9, "confidence": .9, "reject": False}
                for e in kwargs["candidates"]}

    monkeypatch.setattr(exporter, "personalization_supabase_request", rpc)
    monkeypatch.setattr(exporter, "call_gemma_related_audit", audit)
    monkeypatch.setenv("GOOGLE_API_KEY4", "test-only")
    monkeypatch.setenv("STATIC_SITE_GEMMA_RELATED_MAX_ATTEMPTS", "1")
    monkeypatch.setenv("STATIC_SITE_GEMMA_RELATED_RETRY_BACKOFF_SEC", "0")
    cache_path = tmp_path / "cache.json"

    def run(anchors, limit=0):
        return exporter.build_related(events, current_date="2026-07-20", related_mode="pgvector",
                                      cache_path=cache_path, gemma_verify=True, anchor_ids=anchors, gemma_max_anchors=limit)

    initial = run(None if change_programme else {1})
    before = json.loads(cache_path.read_text())
    assert initial["related"]["1"]["strict_verified"]
    fetched.clear()
    audited.clear()
    if change_programme:
        events[1]["other_date_ids"], events[2]["other_date_ids"] = [3], [2]
        failures.update({2} if failure else ())
        result = run({6}, cap)
        for _ in range(4):
            result = run(None, cap)
        assert 1 in fetched, "unrequested dependent must be rebuilt after candidate programme changes"
        assert 1 in audited
        assert set(fetched) == {1, 2, 3, 4}
        assert result["related"]["1"]["strict_verified"]
        assert result["related"]["4"]["strict_verified"]
        assert 4 in audited
        completed_calls = len(fetched), len(audited)
        run(None, cap)
        if failure:
            assert not result["related"]["2"]["strict_verified"]
            assert audited[completed_calls[1]:] == [2]
            assert fetched[completed_calls[0]:] == [2]
        else:
            assert (len(fetched), len(audited)) == completed_calls
        after = json.loads(cache_path.read_text())
        assert [item["event_id"] for item in after["raw_chains"]["1"]] == [2, 4, 3]
        assert after["chains"]["5"] == before["chains"]["5"]
        assert 5 not in audited and 6 not in audited
    else:
        assert set(before["gemma_verified_event_ids"]) == {1}, "seed must be genuinely partial"
        result = run(None)
        assert fetched == [], "unchanged warm full export must not fetch an old unverified tail"
        assert audited == []
        assert result["related"]["1"]["strict_verified"]
        if cap == 0:
            incompatible = json.loads(cache_path.read_text())
            incompatible["schema_version"] = "unknown-cache-contract"
            cache_path.write_text(json.dumps(incompatible))
            rebuilt = run(None)
            assert set(fetched) == set(range(1, 7)), "incompatible full cache must bootstrap instead of erasing output"
            assert all(rebuilt["related"][str(i)]["strict_verified"] for i in range(1, 7))


def test_full_cli_reuses_incremental_graph_without_validating_partial_metrics(tmp_path, monkeypatch):
    import sqlite3
    import sys
    from types import SimpleNamespace

    exporter = _load_exporter_module()
    events = [_related_event(i, f"Distinct programme {i}", day=i) for i in range(1, 7)]
    db = tmp_path / "catalog.sqlite"
    sqlite3.connect(db).close()
    cache = tmp_path / "related-cache.json"
    fetched, validated = [], []
    empty_rpc = False

    def rpc(_name, payload, **_kwargs):
        anchor = payload["p_anchor_event_id"]
        fetched.append(anchor)
        return [] if empty_rpc else [{"event_id": event["id"], "vector_similarity": .85}
                                     for event in events if event["id"] != anchor]

    validator = exporter.validate_pgvector_graph_release
    def validate(meta):
        validated.append(dict(meta))
        validator(meta)

    monkeypatch.setattr(exporter, "personalization_supabase_request", rpc)
    monkeypatch.setattr(exporter, "validate_pgvector_graph_release", validate)
    monkeypatch.setattr(exporter, "fetch_rows", lambda *a, **k: events)
    monkeypatch.setattr(exporter, "fetch_recent_event_detail_archive_rows", lambda *a, **k: [])
    monkeypatch.setattr(exporter, "build_event", lambda _con, row, *a, **k: dict(row))
    monkeypatch.setattr(exporter, "build_catalog_ledger", lambda *a, **k: {})
    monkeypatch.setattr(exporter, "collect_source_records", lambda *a, **k: [])
    for name in ("event_participants_for_events", "event_video_assets_for_events",
                 "build_interest_clubs_projection", "build_interest_clubs_projection_v2",
                 "build_festival_timeline_projection"):
        monkeypatch.setattr(exporter, name, lambda *a, **k: {})
    monkeypatch.setitem(sys.modules, "static_collection_export", SimpleNamespace(
        stable_hash=lambda *a: "offline", build_registry_projection=lambda *a, **k: ({}, [])))
    monkeypatch.setitem(sys.modules, "static_place_org_registry", SimpleNamespace(
        load_registry=lambda: {}, registry_hash=lambda *a: "offline"))
    monkeypatch.delenv("STATIC_SITE_SYNC_PGVECTOR_VECTORS", raising=False)
    monkeypatch.delenv("STATIC_SITE_COLLECTION_SEMANTIC_COMPUTE", raising=False)
    monkeypatch.setattr(sys, "argv", ["exporter", "--db", str(db), "--output-dir", str(tmp_path / "output"),
        "--catalog-mode", "full", "--current-date", "2026-07-20", "--related-mode", "pgvector",
        "--related-cache", str(cache), "--related-corpus-revision", "c" * 64,
        "--repo-sha", "a" * 40, "--run-id", "offline", "--build-id", "offline",
        "--snapshot-id", "offline", "--snapshot-sha256", "b" * 64])
    assert exporter.main() == 0
    assert len(fetched) == 6 and len(validated) == 1
    fetched.clear()
    events.append(_related_event(7, "New independent programme", day=7))
    assert exporter.main() == 0
    assert fetched == [7], "warm full export must fetch only the new anchor"
    assert len(validated) == 1, "partial metrics must not be interpreted as a full graph"
    fetched.clear()
    assert exporter.main() == 0
    assert fetched == [] and len(validated) == 1, "empty scope is not a measured full graph"
    cache.unlink()
    empty_rpc = True
    with pytest.raises(RuntimeError, match="graph release gate failed"):
        exporter.main()
    assert len(validated) == 2, "a genuinely cold full graph must retain its release validation"
    with pytest.raises(RuntimeError, match="graph release gate failed"):
        exporter.main()
    assert len(validated) == 3, "cached failed full graph must not become unmeasured scoped success"
    fetched.clear()
    sys.argv[sys.argv.index("--related-corpus-revision") + 1] = "d" * 64
    with pytest.raises(RuntimeError, match="graph release gate failed"):
        exporter.main()
    assert fetched == [] and len(validated) == 4, "revision-only change must not hide a cached bad full graph"
