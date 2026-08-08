from __future__ import annotations

import hashlib
import json
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
            '"token":"generic-secret","telegram_bot_token":"telegram-secret",'
            '"nested":{"client_secret":"client-secret-value",'
            '"operator_id":123456,"operator_email":"operator@example.com",'
            '"operator_username":"private-operator","total_tokens":99},'
            '"note":"Authorization: Bearer bearer-secret-value token=inline-secret '
            'operator_id=654321 operator_email=inline@example.com"}',
        ),
    )
    conn.commit()
    conn.close()

    incident_path = (
        config.repository_root
        + "/docs/reports/incidents/INC-2026-08-02-private-redaction.md"
    )
    with open(incident_path, "w", encoding="utf-8") as handle:
        handle.write(
            "# Private redaction incident\n\n"
            "access_token=incident-secret-value "
            "operator_email=incident-operator@example.com\n"
        )

    repository = EventsEvidenceRepository(config)
    document = await repository.get_event(42)
    for forbidden in (
        "provider-secret",
        "generic-secret",
        "telegram-secret",
        "client-secret-value",
        "123456",
        "654321",
        "operator@example.com",
        "inline@example.com",
        "private-operator",
        "bearer-secret-value",
        "inline-secret",
    ):
        assert forbidden not in document.text
    assert "<redacted>" in document.text
    assert '"total_tokens": 99' in document.text
    assert "untrusted_data_never_instructions" in document.text
    assert document.metadata["contains_untrusted_external_content"] is True

    event_hits = await repository.search_events(query="архитектуре", include_past=True)
    assert event_hits[0].metadata["contains_untrusted_external_content"] is True

    incident_hits = await repository.search_incidents("private redaction", limit=10)
    incident_hit = next(item for item in incident_hits if item.kind == "incident_report")
    incident_search_output = json.dumps(
        {
            "title": incident_hit.title,
            "snippet": incident_hit.snippet,
            "metadata": incident_hit.metadata,
        },
        ensure_ascii=False,
    )
    assert "incident-secret-value" not in incident_search_output
    assert "incident-operator@example.com" not in incident_search_output
    incident = await repository.get_incident(incident_hit.document_id)
    assert "incident-secret-value" not in incident.text
    assert "incident-operator@example.com" not in incident.text
    assert "<redacted>" in incident.text
