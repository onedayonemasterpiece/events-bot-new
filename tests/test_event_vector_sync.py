from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import event_vector_sync as evs
from google_ai.exceptions import RateLimitError
from scripts import sync_event_search_vectors_to_supabase as sync


@pytest.mark.asyncio
async def test_full_catalog_sync_persists_structured_ops_metrics(monkeypatch, tmp_path):
    monkeypatch.setenv("ENABLE_EVENT_VECTOR_SYNC", "1")
    monkeypatch.setenv("EVENT_VECTOR_SYNC_CATALOG_LIMIT", "5000")
    receipt_path = tmp_path / "event-vector-receipt.json"
    monkeypatch.setenv("EVENT_VECTOR_SYNC_RECEIPT_PATH", str(receipt_path))
    calls: list[tuple[str, dict]] = []
    search_hash = "a" * 64
    related_hash = "b" * 64

    async def fake_start(*_args, **_kwargs):
        return 77

    async def fake_finish(*_args, **kwargs):
        calls.append((kwargs["status"], kwargs))

    async def fake_process(cmd, *, stage, run_id):
        if stage == "export":
            output_dir = Path(cmd[cmd.index("--output-dir") + 1])
            (output_dir / "preview-events.json").write_text('{"events": []}', encoding="utf-8")
        else:
            report_path = Path(cmd[cmd.index("--report-json") + 1])
            report_path.write_text(
                json.dumps(
                    {
                        "events": 334,
                        "documents_upserted": 334,
                        "embeddings_upserted": 12,
                        "embeddings_skipped_unchanged": 656,
                        "embeddings_skipped_by_kind": {"search_v3": 328, "related_v1": 328},
                        "provider_calls": 12,
                        "not_embedded_due_call_cap": 0,
                        "stale_events_deleted": 3,
                        "stale_event_ids": [1, 2, 3],
                        "complete": True,
                        "embedding_model": "gemini-embedding-2",
                        "embedding_dim": 768,
                        "document_kinds": ["search_v3", "related_v1"],
                        "catalog_revision": "c" * 64,
                        "corpus_revision": search_hash,
                        "search_document_revision": search_hash,
                        "search_v3_hash": search_hash,
                        "related_v1_hash": related_hash,
                        "coverage": {"status": "complete", "eligible_event_count": 334},
                    }
                ),
                encoding="utf-8",
            )
        return []

    monkeypatch.setattr(evs, "start_ops_run", fake_start)
    monkeypatch.setattr(evs, "finish_ops_run", fake_finish)
    monkeypatch.setattr(evs, "_run_process", fake_process)
    monkeypatch.setattr(
        evs,
        "_create_sqlite_snapshot",
        lambda _source, target: target.write_bytes(b"snapshot"),
    )
    monkeypatch.setattr(evs, "_snapshot_event_revisions", lambda _path: {"42": "revision-42"})

    result = await evs.run_event_vector_sync(
        SimpleNamespace(path=str(tmp_path / "db.sqlite")),
        trigger="scheduled",
        scheduler_run_id="sched-1",
    )

    assert result["status"] == "success"
    assert result["events"] == 334
    assert result["embeddings_upserted"] == 12
    assert calls[-1][0] == "success"
    assert calls[-1][1]["metrics"]["stale_events_deleted"] == 3
    assert calls[-1][1]["details"]["document_kinds"] == ["search_v3", "related_v1"]
    assert calls[-1][1]["details"]["related_v1_hash"] == related_hash
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt == {
        "complete": True,
        "document_kinds": ["search_v3", "related_v1"],
        "embedding_dim": 768,
        "embedding_model": "gemini-embedding-2",
        "event_revisions": {"42": "revision-42"},
        "events": 334,
        "catalog_revision": "c" * 64,
        "corpus_revision": search_hash,
        "search_document_revision": search_hash,
        "coverage": {"status": "complete", "eligible_event_count": 334},
        "projected_at": receipt["projected_at"],
        "projection_run_id": 77,
        "related_v1_hash": related_hash,
        "run_id": 77,
        "schema_version": "event_vector_sync_receipt_v2",
        "search_v3_hash": search_hash,
        "status": "complete",
    }


def test_vector_corpus_hash_is_order_independent_and_contract_bound() -> None:
    def doc(event_id: int, search_hash: str, related_hash: str) -> sync.SearchDoc:
        return sync.SearchDoc(
            event_id=event_id,
            document={"text_hash": search_hash, "related_text_hash": related_hash},
            search_embedding_input="",
            related_embedding_input="",
        )

    docs = [doc(2, "s2", "r2"), doc(1, "s1", "r1")]
    reverse = list(reversed(docs))
    related = sync.vector_corpus_hash(
        docs,
        document_kind="related_v1",
        embedding_model="gemini-embedding-2",
        embedding_dim=768,
    )

    assert related == sync.vector_corpus_hash(
        reverse,
        document_kind="related_v1",
        embedding_model="gemini-embedding-2",
        embedding_dim=768,
    )
    assert related != sync.vector_corpus_hash(
        [doc(2, "s2", "changed"), doc(1, "s1", "r1")],
        document_kind="related_v1",
        embedding_model="gemini-embedding-2",
        embedding_dim=768,
    )
    assert related != sync.vector_corpus_hash(
        docs,
        document_kind="related_v1",
        embedding_model="gemini-embedding-3",
        embedding_dim=768,
    )


def test_atomic_receipt_replaces_complete_json(tmp_path: Path) -> None:
    receipt_path = tmp_path / "receipt.json"
    receipt_path.write_text('{"status":"old"}\n', encoding="utf-8")

    evs._atomic_write_json(receipt_path, {"status": "complete", "revision": "a" * 64})

    assert json.loads(receipt_path.read_text(encoding="utf-8")) == {
        "revision": "a" * 64,
        "status": "complete",
    }
    assert not list(tmp_path.glob(".receipt.json.*.tmp"))


@pytest.mark.asyncio
async def test_snapshot_revisions_match_static_request_revision(tmp_path: Path) -> None:
    import main

    database = main.Database(str(tmp_path / "source.sqlite"))
    await database.init()
    async with database.get_session() as session:
        event = main.Event(
            title="Revision snapshot",
            description="Canonical description",
            date="2026-08-01",
            time="19:00",
            location_name="Дом искусств",
            city="Калининград",
            source_text="source",
        )
        session.add(event)
        await session.commit()
        await session.refresh(event)
        expected = main.event_public_revision(event)
        event_id = int(event.id)

    snapshot = tmp_path / "snapshot.sqlite"
    evs._create_sqlite_snapshot(database.path, snapshot)

    assert evs._snapshot_event_revisions(snapshot)[str(event_id)] == expected
    await database.close()


def test_sync_require_complete_returns_nonzero_when_cap_leaves_gap(monkeypatch, tmp_path):
    fixture = tmp_path / "preview-events.json"
    fixture.write_text(
        json.dumps(
            {
                "events": [
                    {
                        "id": 42,
                        "title": "Тестовое событие",
                        "slug": "test-event",
                        "start_date": "2026-07-20",
                        "lifecycle_status": "active",
                        "ticket": {},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    report_path = tmp_path / "report.json"
    monkeypatch.setattr(sync, "upsert_documents", lambda docs: len(docs))
    monkeypatch.setattr(sync, "fetch_existing_embeddings", lambda *args, **kwargs: {})
    monkeypatch.setattr(sync, "fetch_indexed_event_ids", lambda: set())
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "sync_event_search_vectors_to_supabase.py",
            "--preview-events-json",
            str(fixture),
            "--apply",
            "--max-provider-calls",
            "0",
            "--require-complete",
            "--report-json",
            str(report_path),
        ],
    )

    assert sync.main() == 2
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["not_embedded_due_call_cap"] == 2
    assert report["complete"] is False


def test_zero_call_verification_scans_all_matching_embeddings(monkeypatch, tmp_path):
    fixture = tmp_path / "preview-events.json"
    fixture.write_text(
        json.dumps(
            {
                "events": [
                    {
                        "id": event_id,
                        "title": f"Событие {event_id}",
                        "slug": f"event-{event_id}",
                        "start_date": "2026-07-20",
                        "lifecycle_status": "active",
                        "ticket": {},
                    }
                    for event_id in (41, 42, 43)
                ]
            }
        ),
        encoding="utf-8",
    )
    report_path = tmp_path / "report.json"
    expected_hashes: dict[int, str] = {}

    def remember_documents(docs):
        expected_hashes.update({doc.event_id: doc.document["text_hash"] for doc in docs})
        return len(docs)

    monkeypatch.setattr(sync, "upsert_documents", remember_documents)
    monkeypatch.setattr(sync, "fetch_existing_embeddings", lambda *args, **kwargs: dict(expected_hashes))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "sync_event_search_vectors_to_supabase.py",
            "--preview-events-json",
            str(fixture),
            "--document-kinds",
            "search_v3",
            "--apply",
            "--max-provider-calls",
            "0",
            "--require-complete",
            "--report-json",
            str(report_path),
        ],
    )

    assert sync.main() == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["embeddings_skipped_unchanged"] == 3
    assert report["provider_calls"] == 0
    assert report["not_embedded_due_call_cap"] == 0
    assert report["complete"] is True


def test_delete_stale_events_removes_embeddings_before_documents(monkeypatch):
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        sync,
        "supabase_request",
        lambda method, path, **kwargs: calls.append((method, path)),
    )

    assert sync.delete_stale_events([3, 2, 3], chunk_size=1) == 2
    assert calls[0] == ("DELETE", "event_embeddings?event_id=in.(2)")
    assert calls[1] == ("DELETE", "event_search_documents?event_id=in.(2)")


def test_gemini_embed_uses_shared_gateway_instead_of_direct_http() -> None:
    calls: list[dict] = []

    class FakeGateway:
        async def embed_content_async(self, **kwargs):
            calls.append(kwargs)
            return ((0.1, 0.2, 0.3), SimpleNamespace(total_tokens=7))

    values = sync.gemini_embed(
        "semantic query",
        model="gemini-embedding-2",
        dim=3,
        client=FakeGateway(),
    )

    assert values == [0.1, 0.2, 0.3]
    assert calls == [
        {
            "model": "gemini-embedding-2",
            "text": "semantic query",
            "output_dimensionality": 3,
        }
    ]


def test_gemini_embed_waits_for_bounded_minute_bucket_then_retries() -> None:
    calls: list[dict] = []
    sleeps: list[float] = []

    class FakeGateway:
        async def embed_content_async(self, **kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                raise RateLimitError(blocked_reason="rpm", retry_after_ms=2_000)
            return ((0.4, 0.5), SimpleNamespace(total_tokens=5))

    values = sync.gemini_embed(
        "semantic query",
        model="gemini-embedding-2",
        dim=2,
        client=FakeGateway(),
        rate_limit_retries=2,
        rate_limit_max_wait_seconds=5,
        sleep_fn=sleeps.append,
        jitter_fn=lambda: 0.1,
    )

    assert values == [0.4, 0.5]
    assert len(calls) == 2
    assert sleeps == [2.1]


def test_gemini_embed_never_waits_for_day_level_exhaustion() -> None:
    sleeps: list[float] = []

    class FakeGateway:
        async def embed_content_async(self, **_kwargs):
            raise RateLimitError(blocked_reason="rpd", retry_after_ms=60_000)

    with pytest.raises(RateLimitError, match="rpd"):
        sync.gemini_embed(
            "semantic query",
            model="gemini-embedding-2",
            dim=2,
            client=FakeGateway(),
            rate_limit_retries=3,
            sleep_fn=sleeps.append,
        )

    assert sleeps == []


def test_gemini_embed_rejects_wait_beyond_batch_budget() -> None:
    sleeps: list[float] = []

    class FakeGateway:
        async def embed_content_async(self, **_kwargs):
            raise RateLimitError(blocked_reason="tpm", retry_after_ms=66_000)

    with pytest.raises(RateLimitError, match="tpm"):
        sync.gemini_embed(
            "semantic query",
            model="gemini-embedding-2",
            dim=2,
            client=FakeGateway(),
            rate_limit_retries=3,
            rate_limit_max_wait_seconds=65,
            sleep_fn=sleeps.append,
        )

    assert sleeps == []


def test_unknown_admission_is_not_tagged_as_ticketed():
    event = {
        "id": 6767,
        "title": "Летний Экодвор",
        "event_type": "фестиваль",
        "city": "Калининград",
        "ticket": {"kind": "status", "label": "Условия уточняются", "is_free": False},
    }

    tags = sync.event_tags(event, "festival")

    assert "ticketed" not in tags
    assert "free" not in tags


def test_explicit_registration_is_tagged_as_ticketed():
    event = {
        "id": 42,
        "title": "Событие по регистрации",
        "ticket": {"kind": "registration", "href": "https://example.org/register"},
    }

    assert "ticketed" in sync.event_tags(event, "event")


def test_search_snapshot_projects_only_reciprocal_explicit_occurrence_families():
    first = {
        "id": 1,
        "title": "Одна программа",
        "start_date": "2026-11-02",
        "start_time": "19:00",
        "display_time": "19:00",
        "end_date": "2026-11-02",
        "lifecycle_status": "active",
        "other_date_ids": [2, 3],
    }
    second = {
        **first,
        "id": 2,
        "start_date": "2026-11-09",
        "end_date": "2026-11-09",
        "other_date_ids": [1],
    }
    lookalike_without_reverse_link = {
        **first,
        "id": 3,
        "start_date": "2026-11-16",
        "end_date": "2026-11-16",
        "other_date_ids": [],
    }

    projection = sync.build_occurrence_projections(
        [first, second, lookalike_without_reverse_link],
        current_year=2026,
    )

    assert projection[1] == {
        "occurrence_member_ids": [1, 2],
        "occurrence_compact_label": "2, 9 ноября 19:00",
        "occurrence_aria_label": "2 и 9 ноября в 19:00",
    }
    assert projection[2] == projection[1]
    assert projection[3]["occurrence_member_ids"] == [3]


def test_search_card_snapshot_carries_family_identity_and_exact_compact_label():
    event = {
        "id": 1,
        "title": "Одна прграмма",
        "slug": "one-program",
        "start_date": "2026-11-02",
        "display_date": "2 ноября",
        "display_time": "19:00",
        "occurrence_member_ids": [1, 2],
        "occurrence_compact_label": "2, 9 ноября 19:00",
        "occurrence_aria_label": "2 и 9 ноября в 19:00",
        "ticket": {},
    }

    card = sync.build_card_snapshot(
        event,
        site_origin="https://kenigevents.ru",
        base_path="",
        ics_base_url="https://static.kenigevents.ru/ics",
    )

    assert card["occurrence_member_ids"] == [1, 2]
    assert card["display"]["occurrence_member_ids"] == [1, 2]
    assert card["display"]["display_date_time"] == "2, 9 ноября 19:00"
    assert card["display"]["occurrence_aria_label"] == "2 и 9 ноября в 19:00"


def test_search_card_snapshot_uses_matching_primary_asset_media_geometry():
    event = {
        "id": 7,
        "title": "Фото события",
        "slug": "photo-event",
        "start_date": "2026-11-02",
        "display_date": "2 ноября",
        "image_url": "/primary.jpg",
        "image_media_role": "unknown_document",
        "image_assets": [
            {
                "src": "/alternate.jpg",
                "width": 400,
                "height": 800,
                "media_role": "event_identity_poster",
                "focal_point": {"y": 0.9},
            },
            {
                "src": "/primary.jpg",
                "width": 1600,
                "height": 1000,
                "media_role": "event_photo",
                "image_text_mode": "visual_only",
                "focal_point": {"x": 0.45, "y": 0.32},
            },
        ],
        "ticket": {},
    }

    card = sync.build_card_snapshot(
        event,
        site_origin="https://kenigevents.ru",
        base_path="",
        ics_base_url="https://static.kenigevents.ru/ics",
    )

    assert card["display"]["image_media_role"] == "event_photo"
    assert card["display"]["image_width"] == 1600
    assert card["display"]["image_height"] == 1000
    assert card["display"]["focal_y"] == 0.32
    assert card["display"]["image_text_mode"] == "visual_only"


def test_search_card_snapshot_unknown_asset_geometry_stays_unknown():
    event = {
        "id": 8,
        "title": "Афиша без геометрии",
        "slug": "poster-without-geometry",
        "start_date": "2026-11-03",
        "display_date": "3 ноября",
        "image_url": "/poster.jpg",
        "image_assets": [{"src": "/poster.jpg", "media_role": "event_identity_poster"}],
        "ticket": {},
    }

    card = sync.build_card_snapshot(
        event,
        site_origin="https://kenigevents.ru",
        base_path="",
        ics_base_url="https://static.kenigevents.ru/ics",
    )

    assert card["display"]["image_width"] is None
    assert card["display"]["image_height"] is None
    assert card["display"]["focal_y"] is None


def test_search_snapshot_formats_same_day_sessions_and_ignores_dangling_links():
    first = {
        "id": 4,
        "title": "Один спектакль",
        "start_date": "2026-11-04",
        "start_time": "17:00",
        "end_date": "2026-11-04",
        "lifecycle_status": "active",
        "other_date_ids": [5, 999],
    }
    second = {
        **first,
        "id": 5,
        "start_time": "19:00",
        "other_date_ids": [4],
    }

    projection = sync.build_occurrence_projections(
        [first, second],
        current_year=2026,
    )

    assert projection[4] == {
        "occurrence_member_ids": [4, 5],
        "occurrence_compact_label": "4 ноября 17:00, 19:00",
        "occurrence_aria_label": "4 ноября в 17:00 и 19:00",
    }
    assert projection[5] == projection[4]
    assert 999 not in projection[4]["occurrence_member_ids"]


@pytest.mark.asyncio
async def test_smart_update_fanout_enqueues_coalesced_vector_projection(monkeypatch, tmp_path):
    import main

    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = main.Event(
            title="Vector projection event",
            description="Canonical LLM-written description",
            date="2026-08-01",
            time="19:00",
            location_name="Дом искусств",
            city="Калининград",
            source_text="source",
        )
        session.add(event)
        await session.commit()
        await session.refresh(event)

    calls: list[tuple] = []

    async def fake_enqueue(_db, event_id, task, **kwargs):
        calls.append((event_id, task, kwargs))
        return kwargs.get("coalesce_key") or f"{task.value}:{event_id}"

    monkeypatch.setenv("ENABLE_EVENT_VECTOR_SYNC", "1")
    monkeypatch.setattr(main, "enqueue_job", fake_enqueue)
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)

    await main.schedule_event_update_tasks(db, event, skip_vk_sync=True)

    vector_calls = [item for item in calls if item[1] == main.JobTask.event_vector_sync]
    assert len(vector_calls) == 1
    assert vector_calls[0][2]["coalesce_key"] == "event_vector_sync:prod"
    assert vector_calls[0][2]["payload"] == {"reason": "smart_update", "event_id": event.id}
    assert main.JOB_HANDLERS["event_vector_sync"] is evs.job_event_vector_sync
