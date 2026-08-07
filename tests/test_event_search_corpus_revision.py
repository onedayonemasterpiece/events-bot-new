from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path

from scripts import sync_event_search_vectors_to_supabase as sync


def _load_exporter_module():
    path = (
        Path(__file__).resolve().parents[1]
        / "site"
        / "scripts"
        / "export-production-preview-data.py"
    )
    spec = importlib.util.spec_from_file_location(
        "export_production_preview_data_corpus_revision", path
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _event(event_id: int, *, title: str | None = None) -> dict:
    return {
        "id": event_id,
        "title": title or f"Событие {event_id}",
        "slug": f"event-{event_id}",
        "start_date": "2026-08-08",
        "updated_at": "2026-08-07T12:00:00+00:00",
        "lifecycle_status": "active",
        "ticket": {},
    }


def _doc(event_id: int, *, search_hash: str, related_hash: str) -> sync.SearchDoc:
    return sync.SearchDoc(
        event_id=event_id,
        document={
            "event_id": event_id,
            "text_hash": search_hash,
            "related_text_hash": related_hash,
            "search_doc_version": "event-search-doc-v3-search-facets",
            "related_doc_version": "event-related-doc-v1",
            "metadata": {},
        },
        search_embedding_input="",
        related_embedding_input="",
    )


def test_export_and_sync_use_the_same_exact_catalog_revision_contract() -> None:
    exporter = _load_exporter_module()
    events = [_event(2), _event(1)]

    export_revision = exporter.exported_search_catalog_revision(events)
    sync_revision = sync.exported_search_catalog_revision(list(reversed(events)))

    assert re.fullmatch(r"[0-9a-f]{64}", export_revision)
    assert sync_revision == export_revision
    assert exporter.exported_search_catalog_revision(events) == export_revision
    assert (
        exporter.exported_search_catalog_revision([_event(2), _event(1, title="Исправлено")])
        != export_revision
    )


def test_corpus_revision_binds_catalog_and_full_embedding_contract() -> None:
    docs = [_doc(1, search_hash="s1", related_hash="r1")]
    first = sync.vector_corpus_hash(
        docs,
        document_kind="search_v3",
        embedding_model="gemini-embedding-2",
        embedding_dim=768,
        catalog_revision="a" * 64,
    )

    assert first != sync.vector_corpus_hash(
        docs,
        document_kind="search_v3",
        embedding_model="gemini-embedding-2",
        embedding_dim=768,
        catalog_revision="b" * 64,
    )
    changed_version = [_doc(1, search_hash="s1", related_hash="r1")]
    changed_version[0].document["search_doc_version"] = "event-search-doc-v4"
    assert first != sync.vector_corpus_hash(
        changed_version,
        document_kind="search_v3",
        embedding_model="gemini-embedding-2",
        embedding_dim=768,
        catalog_revision="a" * 64,
    )


def test_revision_metadata_is_published_on_documents() -> None:
    docs = [_doc(1, search_hash="s1", related_hash="r1")]
    revisions = sync.build_revision_contract(
        docs,
        catalog_revision="c" * 64,
        embedding_model="gemini-embedding-2",
        embedding_dim=768,
    )

    sync.annotate_search_documents(docs, revisions)

    metadata = docs[0].document["metadata"]
    assert metadata["catalog_revision"] == "c" * 64
    assert metadata["corpus_revision"] == revisions["corpus_revision"]
    assert metadata["search_document_revision"] == revisions["search_document_revision"]
    assert metadata["search_doc_version"] == "event-search-doc-v3-search-facets"
    assert revisions["corpora"]["search_v3"] == {
        "revision": revisions["corpus_revision"],
        "document_version": "event-search-doc-v3-search-facets",
        "embedding_model": "gemini-embedding-2",
        "embedding_dim": 768,
        "embedding_doc_kind": "search_v3",
        "document_count": 1,
    }
    embedding_metadata = sync.embedding_revision_metadata(
        docs[0],
        document_kind="search_v3",
        catalog_revision="c" * 64,
        revision_contract=revisions,
    )
    assert embedding_metadata["catalog_revision"] == "c" * 64
    assert embedding_metadata["corpus_revision"] == revisions["corpus_revision"]
    assert embedding_metadata["doc_version"] == "event-search-doc-v3-search-facets"


def test_coverage_receipt_proves_exact_eligible_projection() -> None:
    docs = [
        _doc(1, search_hash="s1", related_hash="r1"),
        _doc(2, search_hash="s2", related_hash="r2"),
    ]
    document_rows = [
        {
            "event_id": doc.event_id,
            "text_hash": doc.document["text_hash"],
            "related_text_hash": doc.document["related_text_hash"],
            "search_doc_version": doc.document["search_doc_version"],
            "related_doc_version": doc.document["related_doc_version"],
        }
        for doc in docs
    ]
    embedding_rows = [
        {
            "event_id": doc.event_id,
            "embedding_model": "gemini-embedding-2",
            "embedding_dim": 768,
            "embedding_doc_kind": kind,
            "text_hash": doc.document[
                "text_hash" if kind == "search_v3" else "related_text_hash"
            ],
            "metadata": {
                "doc_version": doc.document[
                    "search_doc_version"
                    if kind == "search_v3"
                    else "related_doc_version"
                ]
            },
        }
        for doc in docs
        for kind in ("search_v3", "related_v1")
    ]

    receipt = sync.build_projection_coverage(
        docs,
        document_rows=document_rows,
        embedding_rows=embedding_rows,
        document_kinds=["search_v3", "related_v1"],
        embedding_model="gemini-embedding-2",
        embedding_dim=768,
    )

    assert receipt["status"] == "complete"
    assert receipt["eligible_event_count"] == 2
    assert receipt["document_coverage_percent"] == 100.0
    assert receipt["embedding_coverage_percent"] == 100.0
    assert receipt["stale_document_count"] == 0
    assert receipt["orphan_document_count"] == 0
    assert receipt["missing_embedding_count"] == 0
    assert receipt["stale_embedding_count"] == 0
    assert receipt["orphan_embedding_count"] == 0
    assert receipt["wrong_model_or_dimension_count"] == 0
    assert receipt["wrong_document_kind_count"] == 0


def test_coverage_receipt_exposes_every_release_blocker() -> None:
    docs = [_doc(1, search_hash="s1", related_hash="r1")]
    receipt = sync.build_projection_coverage(
        docs,
        document_rows=[
            {
                "event_id": 1,
                "text_hash": "stale",
                "related_text_hash": "r1",
                "search_doc_version": "old",
                "related_doc_version": "event-related-doc-v1",
            },
            {"event_id": 99, "text_hash": "orphan"},
        ],
        embedding_rows=[
            {
                "event_id": 1,
                "embedding_model": "gemini-embedding-2",
                "embedding_dim": 768,
                "embedding_doc_kind": "search_v3",
                "text_hash": "stale",
                "metadata": {"doc_version": "old"},
            },
            {
                "event_id": 1,
                "embedding_model": "wrong-model",
                "embedding_dim": 384,
                "embedding_doc_kind": "related_v1",
                "text_hash": "r1",
                "metadata": {"doc_version": "event-related-doc-v1"},
            },
            {
                "event_id": 1,
                "embedding_model": "gemini-embedding-2",
                "embedding_dim": 768,
                "embedding_doc_kind": "legacy_v0",
                "text_hash": "x",
                "metadata": {},
            },
            {
                "event_id": 99,
                "embedding_model": "gemini-embedding-2",
                "embedding_dim": 768,
                "embedding_doc_kind": "search_v3",
                "text_hash": "orphan",
                "metadata": {},
            },
        ],
        document_kinds=["search_v3", "related_v1"],
        embedding_model="gemini-embedding-2",
        embedding_dim=768,
    )

    assert receipt["status"] == "incomplete"
    assert receipt["stale_document_count"] == 1
    assert receipt["orphan_document_count"] == 1
    assert receipt["missing_embedding_count"] == 1
    assert receipt["stale_embedding_count"] == 1
    assert receipt["orphan_embedding_count"] == 1
    assert receipt["wrong_model_or_dimension_count"] == 1
    assert receipt["wrong_document_kind_count"] == 1


def test_authoritative_reconciliation_removes_stale_contract_rows_only() -> None:
    docs = [_doc(1, search_hash="s1", related_hash="r1")]
    rows = [
        {
            "event_id": 1,
            "embedding_model": "old-model",
            "embedding_dim": 768,
            "embedding_doc_kind": "search_v3",
            "text_hash": "s1",
            "metadata": {"doc_version": "event-search-doc-v3-search-facets"},
        },
        {
            "event_id": 1,
            "embedding_model": "gemini-embedding-2",
            "embedding_dim": 768,
            "embedding_doc_kind": "search_v3",
            "text_hash": "s1",
            "metadata": {"doc_version": "event-search-doc-v3-search-facets"},
        },
        {
            # A search-only reconciliation must not destroy the independently
            # maintained related corpus.
            "event_id": 1,
            "embedding_model": "old-related-model",
            "embedding_dim": 768,
            "embedding_doc_kind": "related_v1",
            "text_hash": "r1",
            "metadata": {"doc_version": "event-related-doc-v1"},
        },
        {
            "event_id": 99,
            "embedding_model": "gemini-embedding-2",
            "embedding_dim": 768,
            "embedding_doc_kind": "search_v3",
            "text_hash": "orphan",
            "metadata": {},
        },
    ]

    rejected = sync.noncanonical_embedding_rows(
        docs,
        rows,
        document_kinds=["search_v3"],
        embedding_model="gemini-embedding-2",
        embedding_dim=768,
    )

    assert {(row["event_id"], row["embedding_model"]) for row in rejected} == {
        (1, "old-model"),
        (99, "gemini-embedding-2"),
    }


def test_authoritative_main_report_publishes_terminal_zero_gap_coverage(
    monkeypatch, tmp_path: Path
) -> None:
    event = _event(1)
    catalog_revision = sync.exported_search_catalog_revision([event])
    fixture = tmp_path / "preview-events.json"
    fixture.write_text(
        json.dumps(
            {
                "build": {"catalog_revision": catalog_revision},
                "events": [event],
            }
        ),
        encoding="utf-8",
    )
    report_path = tmp_path / "report.json"
    projected: list[sync.SearchDoc] = []

    def remember_documents(docs):
        projected[:] = docs
        return len(docs)

    def existing(_event_ids, *, doc_kind, **_kwargs):
        hash_key = "text_hash" if doc_kind == "search_v3" else "related_text_hash"
        return {doc.event_id: doc.document[hash_key] for doc in projected}

    def inventory():
        documents = [
            {
                key: doc.document[key]
                for key in (
                    "event_id",
                    "text_hash",
                    "related_text_hash",
                    "search_doc_version",
                    "related_doc_version",
                )
            }
            for doc in projected
        ]
        embeddings = [
            {
                "event_id": doc.event_id,
                "embedding_model": "gemini-embedding-2",
                "embedding_dim": 768,
                "embedding_doc_kind": kind,
                "text_hash": doc.document[
                    "text_hash" if kind == "search_v3" else "related_text_hash"
                ],
                "metadata": {
                    "doc_version": doc.document[
                        "search_doc_version"
                        if kind == "search_v3"
                        else "related_doc_version"
                    ]
                },
            }
            for doc in projected
            for kind in ("search_v3", "related_v1")
        ]
        return documents, embeddings

    monkeypatch.setattr(sync, "upsert_documents", remember_documents)
    monkeypatch.setattr(sync, "fetch_existing_embeddings", existing)
    monkeypatch.setattr(sync, "fetch_indexed_event_ids", lambda: {1})
    monkeypatch.setattr(sync, "fetch_projection_inventory", inventory)
    monkeypatch.setattr(
        sync,
        "patch_embedding_revision_metadata",
        lambda event_ids, **_kwargs: len(list(event_ids)),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "sync_event_search_vectors_to_supabase.py",
            "--preview-events-json",
            str(fixture),
            "--apply",
            "--prune-missing",
            "--require-complete",
            "--max-provider-calls",
            "0",
            "--report-json",
            str(report_path),
        ],
    )

    assert sync.main() == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["catalog_revision"] == catalog_revision
    assert report["corpus_revision"] == report["search_v3_hash"]
    assert report["coverage"]["status"] == "complete"
    assert report["embedding_revision_metadata_patched"] == 2
    assert report["embedding_coverage_percent"] == 100.0
    assert report["stale_embedding_count"] == 0
    assert report["orphan_embedding_count"] == 0
    assert report["wrong_model_or_dimension_count"] == 0
