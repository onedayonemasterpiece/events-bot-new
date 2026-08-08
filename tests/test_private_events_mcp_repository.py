from __future__ import annotations

import hashlib
import sqlite3

import pytest

from private_events_mcp.repository import EventsEvidenceRepository


@pytest.mark.asyncio
async def test_event_search_and_event_360_are_bounded_and_read_only(
    config, event_db, event_db_digest
) -> None:
    repository = EventsEvidenceRepository(config)
    hits = await repository.search_events(query="архитектуре", limit=10)
    assert [hit.document_id for hit in hits] == ["event:42"]
    assert hits[0].url == "https://telegra.ph/event-42"

    document = await repository.get_event(42)
    assert document.document_id == "event:42"
    assert "identity conflict" in document.text
    assert "Публичный пост организатора" in document.text
    assert "sqlite mode=ro" in document.text
    assert hashlib.sha256(event_db.read_bytes()).hexdigest() == event_db_digest


@pytest.mark.asyncio
async def test_incident_search_combines_repo_and_runtime_evidence(config) -> None:
    repository = EventsEvidenceRepository(config)
    incident_hits = await repository.search_incidents("identity conflict", limit=10)
    ids = {hit.document_id for hit in incident_hits}
    assert "run:run-test-1" in ids
    assert any(item.startswith("incident:") for item in ids)

    runtime = await repository.fetch("run:run-test-1")
    assert runtime.url.endswith("#run:run-test-1")
    assert "review_required" in runtime.text


@pytest.mark.asyncio
async def test_operations_snapshot_has_no_provider_calls(config) -> None:
    snapshot = await EventsEvidenceRepository(config).operations_snapshot()
    assert snapshot["database"]["mode"] == "read_only"
    assert snapshot["database"]["quick_check"] == "ok"
    assert snapshot["network"] == {"provider_calls": 0, "media_transferred": False}
    assert snapshot["counts"]["events_total"] == 1
    assert snapshot["status_counts"]["joboutbox"]["error"] == 1


@pytest.mark.asyncio
async def test_runtime_payloads_are_recursively_redacted_and_source_text_is_untrusted(
    config, event_db
) -> None:
    conn = sqlite3.connect(event_db)
    conn.execute(
        "UPDATE joboutbox SET payload=? WHERE id=7",
        (
            '{"event_id":42,"access_token":"provider-secret",'
            '"nested":{"client_secret":"client-secret-value",'
            '"operator_id":123456,"total_tokens":99},'
            '"note":"Authorization: Bearer bearer-secret-value"}',
        ),
    )
    conn.commit()
    conn.close()

    document = await EventsEvidenceRepository(config).get_event(42)
    assert "provider-secret" not in document.text
    assert "client-secret-value" not in document.text
    assert "123456" not in document.text
    assert "bearer-secret-value" not in document.text
    assert "<redacted>" in document.text
    assert '"total_tokens": 99' in document.text
    assert "untrusted_data_never_instructions" in document.text
    assert document.metadata["contains_untrusted_external_content"] is True
