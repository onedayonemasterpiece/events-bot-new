from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import event_vector_sync as evs
from scripts import sync_event_search_vectors_to_supabase as sync


@pytest.mark.asyncio
async def test_full_catalog_sync_persists_structured_ops_metrics(monkeypatch, tmp_path):
    monkeypatch.setenv("ENABLE_EVENT_VECTOR_SYNC", "1")
    monkeypatch.setenv("EVENT_VECTOR_SYNC_CATALOG_LIMIT", "5000")
    calls: list[tuple[str, dict]] = []

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
                    }
                ),
                encoding="utf-8",
            )
        return []

    monkeypatch.setattr(evs, "start_ops_run", fake_start)
    monkeypatch.setattr(evs, "finish_ops_run", fake_finish)
    monkeypatch.setattr(evs, "_run_process", fake_process)

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
