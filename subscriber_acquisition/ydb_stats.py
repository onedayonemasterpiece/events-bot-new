from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

RUNS_TABLE = "acq_discovery_runs"
SURFACES_TABLE = "acq_discovery_surfaces"
OPPORTUNITIES_TABLE = "acq_discovery_opportunities"


def ydb_stats_enabled() -> bool:
    return (os.getenv("ACQ_YDB_STATS_ENABLED") or "").strip().lower() in {"1", "true", "yes", "on"}


def _ydb_config() -> tuple[str, str]:
    endpoint = (os.getenv("ACQ_YDB_ENDPOINT") or os.getenv("YDB_ENDPOINT") or "").strip()
    database = (os.getenv("ACQ_YDB_DATABASE") or os.getenv("YDB_DATABASE") or "").strip()
    if not endpoint or not database:
        raise RuntimeError("ACQ_YDB_ENDPOINT/YDB_ENDPOINT and ACQ_YDB_DATABASE/YDB_DATABASE are required")
    return endpoint, database


def _credentials(ydb: Any) -> Any:
    token = (os.getenv("ACQ_YDB_ACCESS_TOKEN") or os.getenv("YDB_ACCESS_TOKEN") or os.getenv("YC_IAM_TOKEN") or "").strip()
    if token:
        return ydb.AccessTokenCredentials(token)
    static_token = (os.getenv("ACQ_YDB_AUTH_TOKEN") or os.getenv("YDB_AUTH_TOKEN") or "").strip()
    if static_token:
        return ydb.AuthTokenCredentials(static_token)
    # Let the SDK try its documented environment/default credential chain.
    return None


def _json(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, sort_keys=True)


def _as_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_tables(pool: Any) -> None:
    def op(session: Any) -> None:
        session.execute_scheme(f"""
            CREATE TABLE IF NOT EXISTS {RUNS_TABLE} (
                run_uid Utf8 NOT NULL,
                imported_db_run_id Int64,
                generated_at Utf8,
                imported_at Utf8,
                stats_json Utf8,
                diagnostics_json Utf8,
                PRIMARY KEY (run_uid)
            );
        """)
        session.execute_scheme(f"""
            CREATE TABLE IF NOT EXISTS {SURFACES_TABLE} (
                external_id Utf8 NOT NULL,
                platform Utf8,
                status Utf8,
                source Utf8,
                url Utf8,
                title Utf8,
                last_seen_run_uid Utf8,
                updated_at Utf8,
                payload_json Utf8,
                PRIMARY KEY (external_id)
            );
        """)
        session.execute_scheme(f"""
            CREATE TABLE IF NOT EXISTS {OPPORTUNITIES_TABLE} (
                dedupe_key Utf8 NOT NULL,
                run_uid Utf8,
                platform Utf8,
                surface_external_id Utf8,
                context_url Utf8,
                relevance Double,
                confidence Utf8,
                created_at Utf8,
                payload_json Utf8,
                PRIMARY KEY (dedupe_key)
            );
        """)
    pool.retry_operation_sync(op)


def _execute(pool: Any, query: str, params: dict[str, Any]) -> None:
    def op(session: Any) -> None:
        prepared = session.prepare(query)
        session.transaction().execute(prepared, params, commit_tx=True)
    pool.retry_operation_sync(op)


def export_discovery_payload_to_ydb_sync(payload: dict[str, Any], *, run_db_id: int | None = None) -> dict[str, int | str]:
    import hashlib
    import ydb

    endpoint, database = _ydb_config()
    credentials = _credentials(ydb)
    kwargs = {"endpoint": endpoint, "database": database}
    if credentials is not None:
        kwargs["credentials"] = credentials
    driver = ydb.Driver(**kwargs)
    try:
        driver.wait(fail_fast=True, timeout=float(os.getenv("ACQ_YDB_CONNECT_TIMEOUT_SECONDS") or "10"))
        pool = ydb.SessionPool(driver)
        _ensure_tables(pool)
        run_uid = _as_str(payload.get("run_id") or f"acq-{int(datetime.now(timezone.utc).timestamp())}")
        _execute(pool, f"""
            DECLARE $run_uid AS Utf8;
            DECLARE $imported_db_run_id AS Int64;
            DECLARE $generated_at AS Utf8;
            DECLARE $imported_at AS Utf8;
            DECLARE $stats_json AS Utf8;
            DECLARE $diagnostics_json AS Utf8;
            UPSERT INTO {RUNS_TABLE} (run_uid, imported_db_run_id, generated_at, imported_at, stats_json, diagnostics_json)
            VALUES ($run_uid, $imported_db_run_id, $generated_at, $imported_at, $stats_json, $diagnostics_json);
        """, {
            "$run_uid": run_uid,
            "$imported_db_run_id": int(run_db_id or 0),
            "$generated_at": _as_str(payload.get("generated_at")),
            "$imported_at": _now(),
            "$stats_json": _json(payload.get("stats") or {}),
            "$diagnostics_json": _json(payload.get("diagnostics") or []),
        })
        surfaces = list(payload.get("surfaces") or [])
        for item in surfaces:
            if not isinstance(item, dict):
                continue
            external_id = _as_str(item.get("external_id") or item.get("url"))
            if not external_id:
                continue
            _execute(pool, f"""
                DECLARE $external_id AS Utf8;
                DECLARE $platform AS Utf8;
                DECLARE $status AS Utf8;
                DECLARE $source AS Utf8;
                DECLARE $url AS Utf8;
                DECLARE $title AS Utf8;
                DECLARE $last_seen_run_uid AS Utf8;
                DECLARE $updated_at AS Utf8;
                DECLARE $payload_json AS Utf8;
                UPSERT INTO {SURFACES_TABLE} (external_id, platform, status, source, url, title, last_seen_run_uid, updated_at, payload_json)
                VALUES ($external_id, $platform, $status, $source, $url, $title, $last_seen_run_uid, $updated_at, $payload_json);
            """, {
                "$external_id": external_id,
                "$platform": _as_str(item.get("platform")),
                "$status": _as_str(item.get("status")),
                "$source": _as_str(item.get("source")),
                "$url": _as_str(item.get("url")),
                "$title": _as_str(item.get("title") or item.get("handle")),
                "$last_seen_run_uid": run_uid,
                "$updated_at": _now(),
                "$payload_json": _json(item),
            })
        opportunities = list(payload.get("opportunities") or [])
        for item in opportunities:
            if not isinstance(item, dict):
                continue
            raw_key = "|".join([_as_str(item.get("platform")), _as_str(item.get("context_url")), _as_str(item.get("context_text_snippet"))[:120]])
            dedupe_key = _as_str(item.get("dedupe_key")) or hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
            scores = item.get("scores") or {}
            evidence = item.get("evidence") or {}
            gate = evidence.get("llm_gate") if isinstance(evidence, dict) else {}
            confidence = _as_str((gate or {}).get("confidence") or "")
            _execute(pool, f"""
                DECLARE $dedupe_key AS Utf8;
                DECLARE $run_uid AS Utf8;
                DECLARE $platform AS Utf8;
                DECLARE $surface_external_id AS Utf8;
                DECLARE $context_url AS Utf8;
                DECLARE $relevance AS Double;
                DECLARE $confidence AS Utf8;
                DECLARE $created_at AS Utf8;
                DECLARE $payload_json AS Utf8;
                UPSERT INTO {OPPORTUNITIES_TABLE} (dedupe_key, run_uid, platform, surface_external_id, context_url, relevance, confidence, created_at, payload_json)
                VALUES ($dedupe_key, $run_uid, $platform, $surface_external_id, $context_url, $relevance, $confidence, $created_at, $payload_json);
            """, {
                "$dedupe_key": dedupe_key,
                "$run_uid": run_uid,
                "$platform": _as_str(item.get("platform")),
                "$surface_external_id": _as_str(item.get("surface_external_id")),
                "$context_url": _as_str(item.get("context_url")),
                "$relevance": float(scores.get("relevance") or 0.0),
                "$confidence": confidence,
                "$created_at": _now(),
                "$payload_json": _json(item),
            })
        return {"run_uid": run_uid, "surfaces": len(surfaces), "opportunities": len(opportunities)}
    finally:
        driver.stop()


async def export_discovery_payload_to_ydb(payload: dict[str, Any], *, run_db_id: int | None = None) -> dict[str, int | str] | None:
    if not ydb_stats_enabled():
        return None
    try:
        return await asyncio.to_thread(export_discovery_payload_to_ydb_sync, payload, run_db_id=run_db_id)
    except Exception:
        logger.warning("acq YDB stats export failed", exc_info=True)
        return None
