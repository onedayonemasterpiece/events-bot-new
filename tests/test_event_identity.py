from __future__ import annotations

import time
from dataclasses import dataclass
from types import SimpleNamespace

from event_identity import (
    EVENT_IDENTITY_RPC_NAME,
    EVENT_RELATED_DOC_KIND,
    EVENT_SEARCH_DOC_KIND,
    IDENTITY_CANDIDATE_DOC_KIND,
    EventIdentityRecallConfig,
    build_identity_candidate_document,
    recall_identity_candidates_across_doc_kinds,
    recall_identity_candidates_by_embedding,
)


@dataclass
class Poster:
    ocr_title: str | None = None
    ocr_text: str | None = None
    phash: str | None = None


def test_identity_candidate_document_has_hash_truncation_and_provenance_labels():
    candidate = {
        "title": "Большой концерт",
        "date": "2026-07-10",
        "time": "19:00",
        "location_name": "Дом искусств",
        "city": "Калининград",
        "event_type": "concert",
        "source_type": "telegram",
        "source_url": "https://t.me/example/42",
        "source_text": "афиша " + ("очень длинный текст " * 80),
        "posters": [Poster(ocr_title="Большой концерт", ocr_text="10 июля 19:00", phash="abc123")],
    }

    doc = build_identity_candidate_document(candidate, max_chars=520, source_text_max_chars=120)
    doc_again = build_identity_candidate_document(candidate, max_chars=520, source_text_max_chars=120)

    assert doc.kind == IDENTITY_CANDIDATE_DOC_KIND
    assert doc.sha256 == doc_again.sha256
    assert len(doc.sha256) == 64
    assert doc.truncated is True
    assert doc.char_count <= 520
    assert doc.embedding_model == "gemini-embedding-2"
    assert doc.embedding_dim == 768
    assert "[candidate.title] Большой концерт" in doc.text
    assert "[candidate.source_text]" in doc.text
    assert "[candidate.posters.ocr_title]" in doc.text
    assert "candidate.location_name" in doc.provenance_labels
    assert "candidate.posters.phash" in doc.provenance_labels


class _RpcResult:
    def __init__(self, data):
        self.data = data


class _RpcBuilder:
    def __init__(self, client, result=None, exc=None, delay=0.0):
        self.client = client
        self.result = result
        self.exc = exc
        self.delay = delay

    def execute(self):
        if self.delay:
            time.sleep(self.delay)
        if self.exc:
            raise self.exc
        return _RpcResult(self.result)


class _Client:
    def __init__(self, result=None, exc=None, delay=0.0):
        self.result = result or []
        self.exc = exc
        self.delay = delay
        self.calls = []

    def rpc(self, name, payload):
        self.calls.append((name, payload))
        return _RpcBuilder(self, self.result, self.exc, self.delay)


def test_recall_identity_candidates_calls_service_role_rpc_with_configured_payload():
    client = _Client(
        result=[
            {
                "event_id": 123,
                "document_id": 7,
                "embedding_id": 9,
                "similarity": 0.91,
                "title": "Большой концерт",
                "city": "Калининград",
                "evidence": {"source_url": "https://t.me/example/42"},
            }
        ]
    )
    cfg = EventIdentityRecallConfig(top_k=5, min_similarity=0.82, timeout_seconds=1.0)

    result = recall_identity_candidates_by_embedding(
        client,
        [0.1, 0.2, 0.3],
        city="Калининград",
        event_type="concert",
        config=cfg,
    )

    assert result.ok is True
    assert len(result.candidates) == 1
    assert result.candidates[0].event_id == 123
    assert result.candidates[0].similarity == 0.91
    assert result.candidates[0].evidence["source_url"] == "https://t.me/example/42"
    assert client.calls[0][0] == EVENT_IDENTITY_RPC_NAME
    assert client.calls[0][1] == {
        "p_embedding": [0.1, 0.2, 0.3],
        "p_embedding_doc_kind": EVENT_RELATED_DOC_KIND,
        "p_city": "Калининград",
        "p_event_type": "concert",
        "p_limit": 5,
        "p_min_similarity": 0.82,
    }


def test_recall_identity_candidates_across_related_and_search_dedupes_by_best_score():
    class _KindClient(_Client):
        def rpc(self, name, payload):
            self.calls.append((name, payload))
            kind = payload["p_embedding_doc_kind"]
            rows = {
                EVENT_RELATED_DOC_KIND: [
                    {"event_id": 1, "similarity": 0.88, "embedding_doc_kind": EVENT_RELATED_DOC_KIND},
                    {"event_id": 2, "similarity": 0.84, "embedding_doc_kind": EVENT_RELATED_DOC_KIND},
                ],
                EVENT_SEARCH_DOC_KIND: [
                    {"event_id": 1, "similarity": 0.92, "embedding_doc_kind": EVENT_SEARCH_DOC_KIND},
                    {"event_id": 3, "similarity": 0.81, "embedding_doc_kind": EVENT_SEARCH_DOC_KIND},
                ],
            }[kind]
            return _RpcBuilder(self, rows)

    client = _KindClient()
    result = recall_identity_candidates_across_doc_kinds(client, [0.1, 0.2], top_k=4)

    assert result.ok is True
    assert [c.event_id for c in result.candidates] == [1, 2, 3]
    assert result.candidates[0].similarity == 0.92
    assert [call[1]["p_embedding_doc_kind"] for call in client.calls] == [
        EVENT_RELATED_DOC_KIND,
        EVENT_SEARCH_DOC_KIND,
    ]


def test_recall_identity_candidates_returns_safe_failure_on_exception_and_bad_embedding():
    failed = recall_identity_candidates_by_embedding(_Client(exc=RuntimeError("boom")), [0.1])
    assert failed.ok is False
    assert failed.candidates == ()
    assert failed.error_type == "RuntimeError"

    bad = recall_identity_candidates_by_embedding(_Client(), [])
    assert bad.ok is False
    assert bad.error_type == "ValueError"


def test_recall_identity_candidates_times_out_safely():
    result = recall_identity_candidates_by_embedding(
        _Client(delay=0.08),
        [0.1],
        config=EventIdentityRecallConfig(timeout_seconds=0.01),
    )

    assert result.ok is False
    assert result.timed_out is True
    assert result.candidates == ()


def test_supabase_migration_is_service_role_only():
    sql = open(
        "supabase/migrations/20260702131500_event_identity_candidates_by_embedding_v1.sql",
        encoding="utf-8",
    ).read()

    assert "event_identity_candidates_by_embedding_v1" in sql
    assert "p_embedding vector" in sql
    assert "p_embedding_doc_kind text" in sql
    assert "related_v1" in sql
    assert "p_city text" in sql
    assert "p_event_type text" in sql
    assert "p_limit integer" in sql
    assert "p_min_similarity double precision" in sql
    assert "FROM public.event_embeddings" in sql
    assert "JOIN public.event_search_documents" in sql
    assert "REVOKE ALL ON FUNCTION public.event_identity_candidates_by_embedding_v1" in sql
    assert "GRANT EXECUTE ON FUNCTION public.event_identity_candidates_by_embedding_v1" in sql
    assert "TO service_role" in sql
