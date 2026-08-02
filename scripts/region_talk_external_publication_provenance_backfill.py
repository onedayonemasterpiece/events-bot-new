#!/usr/bin/env python3
"""Attest provenance for pre-ledger Region Talk external-publication intake rows.

Modern imports keep the exact research JSON SHA. Historical YDB rows created
before that contract cannot truthfully reconstruct that SHA. This migration
therefore hashes the immutable embedded research row, preserves its public
evidence and identity keys, and records the exception explicitly. It never
changes the semantic research decision or grants publication permission.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.region_talk_external_publication_import import (  # noqa: E402
    canonicalize_http_url,
    publication_identity_keys,
    stable_hash,
)
from scripts.region_talk_external_research_request import _read_kinds_current_complete  # noqa: E402
from scripts.region_talk_goal_notify import (  # noqa: E402
    ensure_ydb_module,
    load_env,
    ydb_credentials,
    ydb_endpoint_database,
    ydb_table_path,
)

ATTESTATION_VERSION = "region_talk_legacy_external_provenance_v1"
EXCLUDED_FROM_LEGACY_HASH = {
    "_ydb_pk", "_ydb_updated_at", "updated_at",
    "legacy_provenance_attestation", "provenance_attested_at",
    "canonical_evidence_urls", "identity_keys", "request_id",
    "intake_at", "intake_received_at", "review_status",
    "publication_permission", "intake_status",
}


class BackfillError(ValueError):
    pass


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def legacy_source_sha256(row: dict[str, Any]) -> str:
    source = {
        key: value for key, value in row.items()
        if key not in EXCLUDED_FROM_LEGACY_HASH and not str(key).startswith("_ydb_")
    }
    raw = json.dumps(source, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _evidence_urls(row: dict[str, Any]) -> list[str]:
    values: list[Any] = list(row.get("canonical_evidence_urls") or [])
    values.append(row.get("canonical_url"))
    for item in row.get("evidence") or []:
        if isinstance(item, dict):
            values.append(item.get("url"))
    out: set[str] = set()
    for value in values:
        if not value:
            continue
        try:
            out.add(canonicalize_http_url(value))
        except Exception:
            continue
    return sorted(out)


def prepare_attestation(row: dict[str, Any], *, attested_at: str) -> dict[str, Any]:
    external_id = str(row.get("external_publication_id") or "").strip()
    request_id = str(row.get("request_id") or row.get("research_request_id") or "").strip()
    canonical_url = str(row.get("canonical_url") or "").strip()
    publication = row.get("publication") if isinstance(row.get("publication"), dict) else {}
    if not external_id or not request_id or not canonical_url:
        raise BackfillError("legacy row needs external_publication_id, request id and canonical_url")
    evidence_urls = _evidence_urls(row)
    identity_keys = publication_identity_keys(
        canonical_url=canonical_url,
        doi=row.get("doi"),
        title=publication.get("title"),
        authors=publication.get("authors"),
    )
    intake_at = str(
        row.get("intake_at") or row.get("intake_received_at")
        or row.get("imported_at") or row.get("research_imported_at") or attested_at
    ).strip()
    if not evidence_urls or not identity_keys or not intake_at:
        raise BackfillError("legacy row lacks public evidence, exact identity or intake time")
    row_sha256 = legacy_source_sha256(row)
    attestation_id = "extpubprov_" + stable_hash(external_id, row_sha256, ATTESTATION_VERSION)
    attestation = {
        "attestation_id": attestation_id,
        "attestation_version": ATTESTATION_VERSION,
        "attested_at": attested_at,
        "request_id": request_id,
        "legacy_row_sha256": row_sha256,
        "canonical_evidence_urls": evidence_urls,
        "identity_keys": identity_keys,
        "intake_at": intake_at,
        "basis": "immutable_legacy_ydb_research_row_with_embedded_public_evidence",
        "input_json_sha256_available": False,
    }
    updated = {
        **{key: value for key, value in row.items() if not str(key).startswith("_ydb_")},
        "request_id": row.get("request_id") or request_id,
        "canonical_evidence_urls": evidence_urls,
        "identity_keys": identity_keys,
        "intake_at": row.get("intake_at") or intake_at,
        "intake_received_at": row.get("intake_received_at") or intake_at,
        "intake_status": row.get("intake_status") or "legacy_intake_attested",
        "review_status": row.get("review_status") or "unreviewed",
        "publication_permission": row.get("publication_permission") or "not_granted",
        "legacy_provenance_attestation": attestation,
        "provenance_attested_at": attested_at,
        "updated_at": attested_at,
    }
    return updated


def build_backfill(
    rows: list[dict[str, Any]],
    *,
    selected_ids: set[str] | None = None,
    attested_at: str,
) -> dict[str, Any]:
    selected = {value for value in (selected_ids or set()) if value}
    updates: list[dict[str, Any]] = []
    skipped_modern: list[str] = []
    skipped_attested: list[str] = []
    blocked: list[dict[str, str]] = []
    found_ids: set[str] = set()
    for row in rows:
        external_id = str(row.get("external_publication_id") or "").strip()
        if selected and external_id not in selected:
            continue
        if external_id:
            found_ids.add(external_id)
        if row.get("input_json_sha256") or row.get("raw_input_json_sha256"):
            skipped_modern.append(external_id)
            continue
        existing = row.get("legacy_provenance_attestation")
        if isinstance(existing, dict) and existing.get("attestation_version") == ATTESTATION_VERSION:
            skipped_attested.append(external_id)
            continue
        try:
            updates.append(prepare_attestation(row, attested_at=attested_at))
        except BackfillError as exc:
            blocked.append({"external_publication_id": external_id, "reason": str(exc)})
    missing = sorted(selected - found_ids)
    blocked.extend({"external_publication_id": value, "reason": "selected intake row not found"} for value in missing)
    return {
        "attestation_version": ATTESTATION_VERSION,
        "attested_at": attested_at,
        "updates": updates,
        "update_ids": sorted(str(row.get("external_publication_id") or "") for row in updates),
        "skipped_modern_ids": sorted(skipped_modern),
        "skipped_attested_ids": sorted(skipped_attested),
        "blocked": blocked,
        "execution_blocked": bool(blocked),
    }


def execute_backfill(pool: Any, ydb: Any, table: str, prepared: dict[str, Any]) -> int:
    if prepared.get("execution_blocked"):
        raise BackfillError("legacy provenance backfill contains blocked rows")
    updates = list(prepared.get("updates") or [])
    if not updates:
        return 0
    select_text = f"DECLARE $pk AS Utf8; SELECT payload_json FROM `{table}` WHERE pk = $pk;"
    upsert_text = f"""
DECLARE $pk AS Utf8; DECLARE $kind AS Utf8; DECLARE $payload_json AS Json; DECLARE $updated_at AS Utf8;
UPSERT INTO `{table}` (pk, kind, payload_json, updated_at)
VALUES ($pk, $kind, $payload_json, $updated_at);
"""

    def op(session: Any) -> int:
        select = session.prepare(select_text)
        upsert = session.prepare(upsert_text)
        tx = session.transaction(ydb.SerializableReadWrite())
        pending: list[tuple[str, dict[str, Any]]] = []
        for intended in updates:
            external_id = str(intended.get("external_publication_id") or "")
            pk = "external_publication_intake_item:" + external_id
            response = tx.execute(select, {"$pk": pk}, commit_tx=False)
            current_rows = response[0].rows if response else []
            if not current_rows:
                tx.rollback()
                raise BackfillError(f"intake disappeared before write: {external_id}")
            raw = current_rows[0].payload_json
            current = json.loads(raw) if isinstance(raw, str) else dict(raw or {})
            expected_sha = str((intended.get("legacy_provenance_attestation") or {}).get("legacy_row_sha256") or "")
            if legacy_source_sha256(current) != expected_sha:
                tx.rollback()
                raise BackfillError(f"intake changed before write: {external_id}")
            pending.append((pk, intended))
        for index, (pk, payload) in enumerate(pending):
            tx.execute(
                upsert,
                {
                    "$pk": pk,
                    "$kind": "external_publication_intake_item",
                    "$payload_json": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                    "$updated_at": str(payload.get("updated_at") or prepared.get("attested_at") or utc_now_iso()),
                },
                commit_tx=index == len(pending) - 1,
            )
        return len(pending)

    return int(pool.retry_operation_sync(op) or 0)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill explicit provenance attestations for legacy Region Talk intake")
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--external-publication-id", action="append", default=[])
    parser.add_argument("--limit", type=int, default=2000)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--report", type=Path, default=ROOT / "artifacts" / "codex" / "region-talk-legacy-provenance-backfill.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env(args.env_file)
    ydb = ensure_ydb_module()
    endpoint, database = ydb_endpoint_database()
    driver = ydb.Driver(endpoint=endpoint, database=database, credentials=ydb_credentials(ydb))
    driver.wait(timeout=20, fail_fast=True)
    pool = ydb.SessionPool(driver)
    table = ydb_table_path(database)
    try:
        current = _read_kinds_current_complete(
            pool, ydb, table, ["external_publication_intake_item"], max(1, args.limit)
        )["external_publication_intake_item"]
        prepared = build_backfill(
            current,
            selected_ids=set(args.external_publication_id),
            attested_at=utc_now_iso(),
        )
        written = 0
        status = "dry_run"
        if args.execute:
            written = execute_backfill(pool, ydb, table, prepared)
            status = "committed"
        report = {
            **{key: value for key, value in prepared.items() if key != "updates"},
            "status": status,
            "intake_rows_read": len(current),
            "planned_updates": len(prepared["updates"]),
            "written_updates": written,
        }
    finally:
        driver.stop(timeout=5)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 2 if report.get("execution_blocked") else 0


if __name__ == "__main__":
    raise SystemExit(main())
