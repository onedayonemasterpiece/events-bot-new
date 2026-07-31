import json

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import IntegrityError

from db import Database
from models import FestivalQueueItem, create_all


TABLE_COLUMNS = {
    "festival_web_research_run": {
        "id",
        "run_uid",
        "target_key",
        "series_candidate",
        "edition_candidate",
        "state",
        "mode",
        "review_status",
        "input_fingerprint",
        "orchestration_version",
        "contract_version",
        "taxonomy_version",
        "taxonomy_sha256",
        "primary_queue_item_id",
        "candidate_sha256",
        "candidate_json",
        "quality_json",
        "artifact_manifest_json",
        "lease_owner",
        "lease_expires_at",
        "reviewed_by",
        "reviewed_at",
        "review_reason",
        "started_at",
        "completed_at",
        "created_at",
        "updated_at",
    },
    "festival_web_research_lane_run": {
        "id",
        "run_id",
        "lane",
        "attempt_no",
        "request_uid",
        "provider_state",
        "semantic_state",
        "interaction_ids_json",
        "model_id",
        "prompt_version",
        "contract_version",
        "taxonomy_version",
        "taxonomy_sha256",
        "input_fingerprint",
        "artifact_manifest_json",
        "usage_json",
        "validation_json",
        "candidate_sha256",
        "candidate_json",
        "provider_error_code",
        "semantic_error_code",
        "last_error",
        "started_at",
        "completed_at",
        "created_at",
        "updated_at",
    },
    "festival_web_research_item": {
        "id",
        "run_id",
        "queue_item_id",
        "original_status",
        "source_role",
        "decision",
        "decision_reason",
        "created_at",
        "updated_at",
    },
    "festival_web_research_source": {
        "id",
        "lane_run_id",
        "source_id",
        "requested_url",
        "resolved_url",
        "canonical_url",
        "source_role",
        "edition_status",
        "content_sha256",
        "snapshot_ref",
        "normalizer_version",
        "quote_index_ref",
        "fetched_at",
        "decision",
        "exclusion_reason",
        "created_at",
        "updated_at",
    },
}

TABLE_INDEXES = {
    "festival_web_research_run": {
        "ix_festival_web_research_run_state_updated",
        "ix_festival_web_research_run_target_created",
        "ix_festival_web_research_run_review_updated",
    },
    "festival_web_research_lane_run": {
        "ix_festival_web_research_lane_provider_updated",
        "ix_festival_web_research_lane_semantic_updated",
        "ix_festival_web_research_lane_input_fingerprint",
    },
    "festival_web_research_item": {
        "ix_festival_web_research_item_queue",
        "ix_festival_web_research_item_decision",
    },
    "festival_web_research_source": {
        "ix_festival_web_research_source_canonical_url",
        "ix_festival_web_research_source_content_hash",
        "ix_festival_web_research_source_lane_decision",
    },
}

TABLE_UNIQUES = {
    "festival_web_research_run": {
        "ux_festival_web_research_run_uid",
        "ux_festival_web_research_run_input_fingerprint",
    },
    "festival_web_research_lane_run": {
        "ux_festival_web_research_lane_attempt",
        "ux_festival_web_research_lane_request_uid",
    },
    "festival_web_research_item": {
        "ux_festival_web_research_item_run_queue"
    },
    "festival_web_research_source": {
        "ux_festival_web_research_source_lane_source"
    },
}


def _decode_json(value):
    return json.loads(value) if isinstance(value, str) else value


def _assert_inspected_schema(engine) -> None:
    inspector = inspect(engine)
    assert TABLE_COLUMNS.keys() <= set(inspector.get_table_names())
    for table, expected_columns in TABLE_COLUMNS.items():
        columns = {column["name"] for column in inspector.get_columns(table)}
        assert columns == expected_columns
        indexes = {index["name"] for index in inspector.get_indexes(table)}
        assert TABLE_INDEXES[table] <= indexes
        uniques = {
            constraint["name"]
            for constraint in inspector.get_unique_constraints(table)
        }
        assert TABLE_UNIQUES[table] <= uniques


def _insert_and_assert_defaults(conn) -> tuple[int, int]:
    queue_item_id = conn.execute(
        FestivalQueueItem.__table__.insert().values(
            source_kind="url",
            source_url="https://festival.example/current",
        )
    ).inserted_primary_key[0]
    run_result = conn.execute(
        text(
            """
            INSERT INTO festival_web_research_run(
                run_uid, target_key, input_fingerprint,
                orchestration_version, contract_version,
                taxonomy_version, taxonomy_sha256, primary_queue_item_id
            ) VALUES (
                'run-1', 'festival:2026', 'input-sha-1',
                'orch-v1', 'contract-v1', 'taxonomy-v2', 'taxonomy-sha', :queue_id
            )
            """
        ),
        {"queue_id": queue_item_id},
    )
    run_id = run_result.lastrowid
    lane_result = conn.execute(
        text(
            """
            INSERT INTO festival_web_research_lane_run(
                run_id, request_uid, prompt_version, contract_version,
                taxonomy_version, taxonomy_sha256, input_fingerprint
            ) VALUES (
                :run_id, 'request-1', 'prompt-v1', 'contract-v1',
                'taxonomy-v2', 'taxonomy-sha', 'lane-input-sha-1'
            )
            """
        ),
        {"run_id": run_id},
    )
    lane_run_id = lane_result.lastrowid
    conn.execute(
        text(
            """
            INSERT INTO festival_web_research_item(
                run_id, queue_item_id, original_status, source_role
            ) VALUES (:run_id, :queue_id, 'pending', 'seed')
            """
        ),
        {"run_id": run_id, "queue_id": queue_item_id},
    )
    conn.execute(
        text(
            """
            INSERT INTO festival_web_research_source(
                lane_run_id, source_id, requested_url, source_role
            ) VALUES (
                :lane_run_id, 'source-1', 'https://festival.example/current', 'official'
            )
            """
        ),
        {"lane_run_id": lane_run_id},
    )

    run = conn.execute(
        text(
            """
            SELECT state, mode, review_status, candidate_json, quality_json,
                   artifact_manifest_json, created_at, updated_at
            FROM festival_web_research_run WHERE id=:run_id
            """
        ),
        {"run_id": run_id},
    ).mappings().one()
    assert (run["state"], run["mode"], run["review_status"]) == (
        "pending",
        "collect_only",
        "pending",
    )
    assert _decode_json(run["candidate_json"]) == {}
    assert _decode_json(run["quality_json"]) == {}
    assert _decode_json(run["artifact_manifest_json"]) == {}
    assert run["created_at"] is not None
    assert run["updated_at"] is not None

    lane = conn.execute(
        text(
            """
            SELECT lane, attempt_no, provider_state, semantic_state,
                   interaction_ids_json, artifact_manifest_json, usage_json,
                   validation_json, candidate_json, started_at
            FROM festival_web_research_lane_run WHERE id=:lane_run_id
            """
        ),
        {"lane_run_id": lane_run_id},
    ).mappings().one()
    assert (lane["lane"], lane["attempt_no"]) == ("antigravity", 1)
    assert (lane["provider_state"], lane["semantic_state"]) == (
        "pending",
        "pending",
    )
    assert _decode_json(lane["interaction_ids_json"]) == []
    for field in (
        "artifact_manifest_json",
        "usage_json",
        "validation_json",
        "candidate_json",
    ):
        assert _decode_json(lane[field]) == {}
    assert lane["started_at"] is not None

    item_decision = conn.execute(
        text("SELECT decision FROM festival_web_research_item")
    ).scalar_one()
    source = conn.execute(
        text(
            "SELECT edition_status, decision FROM festival_web_research_source"
        )
    ).one()
    assert item_decision == "pending"
    assert source == ("unknown", "pending")
    return run_id, lane_run_id


def test_create_all_has_research_schema_indexes_defaults_and_idempotency(
    tmp_path,
):
    engine = create_engine(f"sqlite:///{tmp_path / 'create-all.sqlite'}")
    try:
        create_all(engine)
        _assert_inspected_schema(engine)
        with engine.begin() as conn:
            _insert_and_assert_defaults(conn)
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO festival_web_research_run(
                            run_uid, target_key, input_fingerprint,
                            orchestration_version, contract_version,
                            taxonomy_version, taxonomy_sha256
                        ) VALUES (
                            'run-2', 'other:2026', 'input-sha-1',
                            'orch-v1', 'contract-v1', 'taxonomy-v2', 'taxonomy-sha'
                        )
                        """
                    )
                )
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_sqlite_bootstrap_has_research_schema_indexes_and_defaults(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("DB_JOURNAL_MODE", "DELETE")
    db = Database(str(tmp_path / "bootstrap.sqlite"))
    await db.init()
    try:
        async with db.engine.begin() as conn:
            await conn.run_sync(_assert_inspected_schema)
            await conn.run_sync(_insert_and_assert_defaults)
    finally:
        await db.close()
