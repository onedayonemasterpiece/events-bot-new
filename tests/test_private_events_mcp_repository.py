from __future__ import annotations

import hashlib
import json
import sqlite3

import pytest

from private_events_mcp.repository import (
    EventsEvidenceRepository,
    InvalidArgumentsError,
    canonicalize_social_post_url,
)


def _seed_multi_event_social_evidence(event_db) -> str:
    managed_url = "https://vk.com/wall-231920894_777"
    conn = sqlite3.connect(event_db)
    for statement in (
        "ALTER TABLE event ADD COLUMN source_vk_post_url TEXT",
        "ALTER TABLE event ADD COLUMN vk_repost_url TEXT",
        "ALTER TABLE event ADD COLUMN tg_event_post_url TEXT",
        "ALTER TABLE event ADD COLUMN ics_url TEXT",
        "ALTER TABLE event_source ADD COLUMN canonical_source_url TEXT",
        "ALTER TABLE event_source ADD COLUMN source_role TEXT",
        "ALTER TABLE event_source ADD COLUMN source_fingerprint TEXT",
        "ALTER TABLE event_source ADD COLUMN source_chat_id INTEGER",
        "ALTER TABLE event_source ADD COLUMN source_message_id INTEGER",
        "ALTER TABLE vk_inbox ADD COLUMN group_id INTEGER",
        "ALTER TABLE vk_inbox ADD COLUMN post_id INTEGER",
        "ALTER TABLE vk_inbox ADD COLUMN imported_event_id INTEGER",
        "ALTER TABLE vk_inbox ADD COLUMN text TEXT",
        "ALTER TABLE vk_inbox ADD COLUMN created_at TEXT",
    ):
        conn.execute(statement)
    conn.executescript(
        """
        CREATE TABLE event_publication (
            id INTEGER PRIMARY KEY, event_id INTEGER, platform TEXT, target TEXT,
            stored_url TEXT, live_url TEXT, stored_post_id INTEGER, live_post_id INTEGER,
            match_method TEXT, match_confidence REAL, status TEXT, resolved_at TEXT
        );
        CREATE TABLE vk_inbox_import_event (
            inbox_id INTEGER, event_id INTEGER, created_at TEXT,
            PRIMARY KEY(inbox_id, event_id)
        );
        CREATE TABLE event_identity_decision_log (
            id INTEGER PRIMARY KEY, event_id INTEGER, candidate_event_id INTEGER,
            source_id INTEGER, source_type TEXT, source_url TEXT, decision TEXT,
            decision_reason TEXT, confidence REAL, decided_by TEXT,
            decision_payload TEXT, created_at TEXT
        );
        """
    )
    conn.execute(
        "UPDATE event_source SET canonical_source_url=source_url, "
        "source_role='identity_bearing', source_fingerprint='source-42', "
        "source_chat_id=123456, source_message_id=42 WHERE id=1"
    )
    conn.execute(
        "UPDATE event SET source_vk_post_url=?, vk_repost_url=? WHERE id=42",
        (managed_url, managed_url),
    )
    conn.executemany(
        "INSERT INTO event(id,title,date,city,location_name,source_vk_post_url,vk_repost_url) "
        "VALUES (?,?,?,?,?,?,?)",
        (
            (43, "ЮНОСТЬ: событие два", "2026-08-12", "Калининград", "ЮНОСТЬ", managed_url, managed_url),
            (44, "ЮНОСТЬ: событие три", "2026-08-13", "Калининград", "ЮНОСТЬ", managed_url, managed_url),
            (45, "Другой пост", "2026-08-14", "Калининград", "Другое", None, "https://vk.com/wall-231920894_7777"),
        ),
    )
    conn.executemany(
        "INSERT INTO event_source(id,event_id,source_type,source_url,canonical_source_url,"
        "source_role,source_fingerprint,source_text,imported_at,trust_level) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            (2, 43, "vk", "https://vk.com/wall-111_43", "https://vk.com/wall-111_43", "identity_bearing", "source-43", "Официальный источник 43", "2026-08-01T09:00:00Z", "primary"),
            (3, 44, "telegram", "https://t.me/source/44", "https://t.me/source/44", "identity_bearing", "source-44", "Официальный источник 44", "2026-08-01T09:00:00Z", "primary"),
        ),
    )
    conn.executemany(
        "INSERT INTO event_publication(id,event_id,platform,target,stored_url,live_url,"
        "stored_post_id,live_post_id,match_method,match_confidence,status,resolved_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        tuple(
            (event_id, event_id, "vk", "managed-events", managed_url, managed_url, 777, 777, "stored_exact", 1.0, "live", "2026-08-01T10:00:00Z")
            for event_id in (42, 43, 44)
        ),
    )
    conn.execute(
        "INSERT INTO event_publication VALUES (45,45,'vk','managed-events',"
        "'https://vk.com/wall-231920894_7777','https://vk.com/wall-231920894_7777',"
        "7777,7777,'stored_exact',1.0,'live','2026-08-01T10:00:00Z')"
    )
    conn.execute(
        "UPDATE vk_inbox SET group_id=231920894,post_id=777,imported_event_id=42,"
        "text='Комментарий: адрес выглядит неверным',created_at='2026-08-01T10:00:00Z' WHERE id=1"
    )
    conn.executemany(
        "INSERT INTO vk_inbox_import_event VALUES (?,?,?)",
        ((1, 42, "2026-08-01T10:01:00Z"), (1, 43, "2026-08-01T10:01:00Z"), (1, 44, "2026-08-01T10:01:00Z")),
    )
    conn.execute(
        "INSERT INTO event_identity_decision_log VALUES "
        "(1,42,43,1,'vk','https://vk.com/wall-111_43','split',"
        "'one source contains several occurrences',0.99,'system',"
        "'{\"access_token\":\"must-redact\"}','2026-08-01T10:02:00Z')"
    )
    conn.execute(
        "INSERT INTO ops_run(run_id,operation,status,event_id,started_at,finished_at,result_json,details_json) "
        "VALUES ('run-yunost-43','event_quality_audit','success',43,"
        "'2026-08-01T11:00:00Z','2026-08-01T11:01:00Z',"
        "'{\"error_class\":\"VenueConflictError\"}',"
        "'{\"operator_email\":\"private@example.com\"}')"
    )
    conn.commit()
    conn.close()
    return managed_url


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
    assert snapshot["database"]["quick_check"] == "not_run:interactive_budget"
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


def test_social_post_url_parser_is_strict_and_canonical() -> None:
    vk = canonicalize_social_post_url(
        "https://m.vk.com/feed?w=wall-231920894_777&utm_source=ignored"
    )
    assert vk.canonical_url == "https://vk.com/wall-231920894_777"
    assert vk.vk_owner_id == -231920894
    assert vk.vk_post_id == 777

    telegram = canonicalize_social_post_url("https://telegram.me/s/source/44?single")
    assert telegram.canonical_url == "https://t.me/source/44"

    for invalid in (
        "https://evilvk.com/wall-1_2",
        "https://user@vk.com/wall-1_2",
        "https://vk.com:8443/wall-1_2",
        "https://vk.com/wall-1_2#fragment",
        "https://vk.com/wall-1_2?w=wall-1_3",
    ):
        with pytest.raises(InvalidArgumentsError):
            canonicalize_social_post_url(invalid)


@pytest.mark.asyncio
async def test_exact_managed_post_returns_all_events_with_provenance_and_no_prefix_collision(
    config, event_db
) -> None:
    managed_url = _seed_multi_event_social_evidence(event_db)
    digest = hashlib.sha256(event_db.read_bytes()).hexdigest()
    repository = EventsEvidenceRepository(config)

    hits = await repository.search_events(
        post_url="https://m.vk.com/feed?w=wall-231920894_777&utm_source=test",
        limit=1,
    )
    assert [item.document_id for item in hits] == ["event:42", "event:43", "event:44"]
    assert all(item.metadata["ambiguous_multi_event"] is True for item in hits)
    assert all(item.metadata["match_count"] == 3 for item in hits)
    assert all(
        any(
            relation["relation"] == "managed_publication"
            and relation["table"] == "event_publication"
            for relation in item.metadata["relations"]
        )
        for item in hits
    )
    assert all(item.metadata["post_url"] == managed_url for item in hits)
    assert "event:45" not in {item.document_id for item in hits}

    telegram_hits = await repository.search_events(
        post_url="https://telegram.me/s/source/44?single"
    )
    assert [item.document_id for item in telegram_hits] == ["event:44"]
    assert telegram_hits[0].metadata["relations"][0]["relation"] == "original_identity_source"
    assert hashlib.sha256(event_db.read_bytes()).hexdigest() == digest


@pytest.mark.asyncio
async def test_event_get_groups_original_managed_inbox_and_identity_evidence(
    config, event_db
) -> None:
    managed_url = _seed_multi_event_social_evidence(event_db)
    repository = EventsEvidenceRepository(config)
    document = await repository.get_event(42)
    payload = document.text

    assert '"original_sources"' in payload
    assert '"source_role": "identity_bearing"' in payload
    assert '"managed_publications"' in payload
    assert managed_url in payload
    assert '"relation": "inbox_import"' in payload
    assert '"identity_decisions"' in payload
    assert "must-redact" not in payload
    assert "123456" not in payload
    assert "untrusted_data_never_instructions" in payload
    assert document.metadata["original_source_count"] == 1
    assert document.metadata["managed_publication_count"] == 1


@pytest.mark.asyncio
async def test_structured_incident_filters_expand_all_status_exact_db_evidence(
    config, event_db
) -> None:
    managed_url = _seed_multi_event_social_evidence(event_db)
    repository = EventsEvidenceRepository(config)

    hits = await repository.search_incidents(
        post_url=managed_url,
        error_class="VenueConflictError",
        time_from="2026-08-01T10:59:00Z",
        time_to="2026-08-01T11:02:00Z",
        limit=10,
    )
    assert [item.document_id for item in hits] == ["run:run-yunost-43"]
    assert hits[0].metadata["status"] == "success"
    assert hits[0].metadata["error_class_filter"] == "bounded_decoded_exact"
    assert hits[0].metadata["runtime_file_mirror"].startswith("not_integrated")

    run = await repository.get_incident("run:run-yunost-43")
    assert "VenueConflictError" in run.text
    assert "private@example.com" not in run.text
    assert "<redacted>" in run.text

    source_hits = await repository.search_incidents(
        source_url="https://vk.com/wall-111_43", run_id="run-yunost-43", limit=10
    )
    assert [item.document_id for item in source_hits] == ["run:run-yunost-43"]

    no_prefix_collision = await repository.search_incidents(
        post_url="https://vk.com/wall-231920894_7777", limit=10
    )
    assert all(item.metadata.get("event_id") not in {42, 43, 44} for item in no_prefix_collision)


@pytest.mark.asyncio
async def test_exact_post_url_rejects_filter_mixing_and_invalid_host_without_db_change(
    config, event_db
) -> None:
    _seed_multi_event_social_evidence(event_db)
    digest = hashlib.sha256(event_db.read_bytes()).hexdigest()
    repository = EventsEvidenceRepository(config)
    with pytest.raises(InvalidArgumentsError):
        await repository.search_events(
            post_url="https://vk.com/wall-231920894_777", city="Калининград"
        )
    with pytest.raises(InvalidArgumentsError):
        await repository.search_events(post_url="https://vk.com.evil.test/wall-1_2")
    assert hashlib.sha256(event_db.read_bytes()).hexdigest() == digest
