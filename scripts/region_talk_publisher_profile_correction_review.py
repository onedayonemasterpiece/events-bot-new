#!/usr/bin/env python3
"""Explicitly review one queued publisher-profile candidate correction.

The profile importer never mutates a live intake/candidate.  This command is
the narrow review bridge: it strongly rereads the queued correction, identity
mapping and exact intake row in one serializable transaction, verifies the
operator-provided intake snapshot hash, and writes only the correction verdict
plus an immutable audit row.  Candidate/publication mutation remains the
responsibility of the ordinary fail-closed finalizer and Writer CAS paths.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.region_talk_external_publication_import import canonicalize_http_url, stable_hash  # noqa: E402
from scripts.region_talk_goal_notify import (  # noqa: E402
    ensure_ydb_module,
    load_env,
    ydb_credentials,
    ydb_endpoint_database,
    ydb_table_path,
)


REVIEW_SCHEMA_VERSION = "region_talk_publisher_profile_candidate_correction_review.v1"
REVIEW_VERSION = "region_talk_publisher_profile_candidate_correction_review.v1"
HARD_LOCALITY_REASONS = {
    "regional_local_edition",
    "local_correspondent",
    "federal_brand_not_sufficient",
}


class CorrectionReviewError(ValueError):
    pass


def _parse_timestamp(value: Any) -> str:
    raw = str(value or "").strip()
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CorrectionReviewError("reviewed_at must be an ISO-8601 timestamp") from exc
    if not raw or parsed.tzinfo is None:
        raise CorrectionReviewError("reviewed_at must include a timezone")
    return parsed.isoformat()


def _payload(raw: Any) -> dict[str, Any]:
    if isinstance(raw, str):
        value = json.loads(raw)
    elif isinstance(raw, dict):
        value = dict(raw)
    else:
        raise CorrectionReviewError("YDB payload_json has an unsupported type")
    if not isinstance(value, dict):
        raise CorrectionReviewError("YDB payload_json is not an object")
    return value


def _raw_sha256(raw: Any) -> str:
    if isinstance(raw, str):
        data = raw.encode("utf-8")
    else:
        data = json.dumps(raw, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def validate_review(review: Any) -> dict[str, Any]:
    if not isinstance(review, dict):
        raise CorrectionReviewError("review document must be an object")
    if review.get("schema_version") != REVIEW_SCHEMA_VERSION:
        raise CorrectionReviewError(f"schema_version must be {REVIEW_SCHEMA_VERSION}")
    correction_id = str(review.get("publisher_profile_correction_id") or "").strip()
    correction_hash = str(review.get("expected_correction_hash") or "").strip().lower()
    intake_hash = str(review.get("expected_live_intake_payload_sha256") or "").strip().lower()
    if not re.fullmatch(r"rtpublishercorr_[a-f0-9]+", correction_id):
        raise CorrectionReviewError("invalid publisher_profile_correction_id")
    if not re.fullmatch(r"[a-f0-9]{64}", correction_hash):
        raise CorrectionReviewError("expected_correction_hash must be SHA-256")
    if not re.fullmatch(r"[a-f0-9]{64}", intake_hash):
        raise CorrectionReviewError("expected_live_intake_payload_sha256 must be SHA-256")
    canonical_url = canonicalize_http_url(review.get("canonical_url"))
    if not canonical_url:
        raise CorrectionReviewError("canonical_url is required")
    decision = str(review.get("decision") or "").strip().lower()
    if decision not in {"retain_external", "block_regional"}:
        raise CorrectionReviewError("decision must be retain_external or block_regional")
    reviewer = str(review.get("reviewer") or "").strip()
    reason = str(review.get("reason") or "").strip()
    if not reviewer:
        raise CorrectionReviewError("reviewer is required")
    if len(reason) < 20:
        raise CorrectionReviewError("reason must be at least 20 characters")
    evidence = review.get("evidence")
    if not isinstance(evidence, list) or not evidence:
        raise CorrectionReviewError("at least one public evidence item is required")
    normalized_evidence: list[dict[str, Any]] = []
    for index, item in enumerate(evidence):
        if not isinstance(item, dict):
            raise CorrectionReviewError(f"evidence[{index}] must be an object")
        url = canonicalize_http_url(item.get("url"))
        supports = sorted({str(value).strip() for value in (item.get("supports") or []) if str(value).strip()})
        note = str(item.get("note") or "").strip()
        if not url or not supports or len(note) < 10:
            raise CorrectionReviewError(f"evidence[{index}] requires public url, supports and note")
        normalized_evidence.append({"url": url, "supports": supports, "note": note})
    return {
        **review,
        "publisher_profile_correction_id": correction_id,
        "expected_correction_hash": correction_hash,
        "expected_live_intake_payload_sha256": intake_hash,
        "canonical_url": canonical_url,
        "decision": decision,
        "reviewer": reviewer,
        "reason": reason,
        "reviewed_at": _parse_timestamp(review.get("reviewed_at")),
        "evidence": normalized_evidence,
    }


def reviewed_correction(
    correction: dict[str, Any], review: dict[str, Any], *, live_intake_hash: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    reason_codes = {str(value).strip().lower() for value in correction.get("reason_codes") or [] if str(value).strip()}
    decision = review["decision"]
    hard_locality = bool(reason_codes & HARD_LOCALITY_REASONS)
    if decision == "block_regional" and not hard_locality:
        raise CorrectionReviewError("block_regional requires durable locality reason codes")
    if decision == "retain_external" and hard_locality:
        supports = {
            value
            for item in review["evidence"]
            for value in item.get("supports") or []
        }
        if review.get("fresh_evidence_reverses_locality") is not True or "externality_override" not in supports:
            raise CorrectionReviewError(
                "hard locality may be reversed only by explicit fresh evidence supporting externality_override"
            )

    review_id = "rtpublishercorrreview_" + stable_hash(
        correction["publisher_profile_correction_id"],
        correction["correction_hash"],
        review["decision"],
        review["reviewer"],
        review["reviewed_at"],
    )
    retained = decision == "retain_external"
    attestation = {
        "publisher_profile_candidate_correction_review_id": review_id,
        "review_schema_version": REVIEW_SCHEMA_VERSION,
        "review_version": REVIEW_VERSION,
        "publisher_profile_correction_id": correction["publisher_profile_correction_id"],
        "correction_hash": correction["correction_hash"],
        "canonical_url": correction["canonical_url"],
        "decision": decision,
        "reviewer": review["reviewer"],
        "reviewed_at": review["reviewed_at"],
        "reason": review["reason"],
        "evidence": deepcopy(review["evidence"]),
        "live_intake_payload_sha256": live_intake_hash,
        "candidate_mutated": False,
        "publication_permission": "not_granted",
        "publication_effect": "none",
    }
    updated = {
        **correction,
        "review_status": "retained_external" if retained else "reviewed_regional",
        "live_revalidation_status": "resolved_external" if retained else "blocked_regional",
        "revalidation_status": "resolved_external" if retained else "blocked_regional",
        "candidate_mutation_allowed": retained,
        "regeneration_allowed": retained,
        "publication_permission": "not_granted",
        "next_action": "resume_profile_aware_writer" if retained else "exclude_from_external_candidate_regeneration",
        "review_decision": decision,
        "review_id": review_id,
        "reviewed_at": review["reviewed_at"],
        "review_reason": review["reason"],
        "review_evidence": deepcopy(review["evidence"]),
        "reviewed_live_intake_payload_sha256": live_intake_hash,
        "candidate_mutated_by_review": False,
        "updated_at": review["reviewed_at"],
    }
    return updated, attestation


def execute_review(review_input: dict[str, Any], *, execute: bool) -> dict[str, Any]:
    review = validate_review(review_input)
    correction_pk = "publisher_profile_candidate_correction_item:" + review["publisher_profile_correction_id"]
    ydb = ensure_ydb_module()
    endpoint, database = ydb_endpoint_database()
    driver = ydb.Driver(endpoint=endpoint, database=database, credentials=ydb_credentials(ydb))
    driver.wait(timeout=20, fail_fast=True)
    pool = ydb.SessionPool(driver)
    table = ydb_table_path(database)
    select = f"DECLARE $pk AS Utf8; SELECT payload_json FROM `{table}` WHERE pk = $pk;"
    upsert = f"""
DECLARE $pk AS Utf8; DECLARE $kind AS Utf8; DECLARE $payload_json AS Json; DECLARE $updated_at AS Utf8;
UPSERT INTO `{table}` (pk, kind, payload_json, updated_at)
VALUES ($pk, $kind, $payload_json, $updated_at);
"""

    def op(session: Any) -> dict[str, Any]:
        tx = session.transaction(ydb.SerializableReadWrite())

        def read(pk: str) -> tuple[dict[str, Any] | None, str]:
            result = tx.execute(session.prepare(select), {"$pk": pk}, commit_tx=False)
            if len(result or []) != 1:
                tx.rollback()
                raise CorrectionReviewError(f"incomplete strong YDB read for {pk}")
            rows = result[0].rows
            if len(rows) > 1:
                tx.rollback()
                raise CorrectionReviewError(f"non-unique strong YDB read for {pk}")
            if not rows:
                return None, ""
            return _payload(rows[0].payload_json), _raw_sha256(rows[0].payload_json)

        correction, _ = read(correction_pk)
        if not correction:
            tx.rollback()
            raise CorrectionReviewError("queued candidate correction was not found")
        if str(correction.get("correction_hash") or "") != review["expected_correction_hash"]:
            tx.rollback()
            raise CorrectionReviewError("queued correction changed since review document was prepared")
        if canonicalize_http_url(correction.get("canonical_url")) != review["canonical_url"]:
            tx.rollback()
            raise CorrectionReviewError("review canonical_url does not match queued correction")

        identity_pk = str(correction.get("live_identity_pk") or "")
        identity, _ = read(identity_pk)
        external_id = str((identity or {}).get("external_publication_id") or correction.get("live_external_publication_id") or "")
        if not external_id:
            tx.rollback()
            raise CorrectionReviewError("strong identity read did not resolve a live external publication intake")
        intake_pk = "external_publication_intake_item:" + external_id
        intake, intake_hash = read(intake_pk)
        if not intake:
            tx.rollback()
            raise CorrectionReviewError("strong live intake read found no row")
        if canonicalize_http_url(intake.get("canonical_url")) != review["canonical_url"]:
            tx.rollback()
            raise CorrectionReviewError("live intake canonical_url differs from correction")
        if intake_hash != review["expected_live_intake_payload_sha256"]:
            tx.rollback()
            raise CorrectionReviewError("live intake changed since explicit review was prepared")

        updated, attestation = reviewed_correction(correction, review, live_intake_hash=intake_hash)
        review_pk = (
            "publisher_profile_candidate_correction_review_item:"
            + attestation["publisher_profile_candidate_correction_review_id"]
        )
        current_review, _ = read(review_pk)
        if current_review:
            if current_review != attestation:
                tx.rollback()
                raise CorrectionReviewError("review id conflict")
            tx.rollback()
            return {
                "execution_status": "identical_replay",
                "written_ydb_rows": 0,
                "correction": updated,
                "attestation": attestation,
                "live_external_publication_id": external_id,
            }
        if str(correction.get("review_status") or "unreviewed") != "unreviewed":
            tx.rollback()
            raise CorrectionReviewError("candidate correction was already reviewed")
        if not execute:
            tx.rollback()
            return {
                "execution_status": "validated_live_no_write",
                "written_ydb_rows": 0,
                "correction": updated,
                "attestation": attestation,
                "live_external_publication_id": external_id,
            }
        writes = [
            (correction_pk, "publisher_profile_candidate_correction_item", updated),
            (review_pk, "publisher_profile_candidate_correction_review_item", attestation),
        ]
        for index, (pk, kind, row) in enumerate(writes):
            tx.execute(
                session.prepare(upsert),
                {
                    "$pk": pk,
                    "$kind": kind,
                    "$payload_json": json.dumps(row, ensure_ascii=False, separators=(",", ":")),
                    "$updated_at": review["reviewed_at"],
                },
                commit_tx=index == len(writes) - 1,
            )
        return {
            "execution_status": "committed",
            "written_ydb_rows": len(writes),
            "correction": updated,
            "attestation": attestation,
            "live_external_publication_id": external_id,
        }

    try:
        return dict(pool.retry_operation_sync(op) or {})
    finally:
        driver.stop(timeout=5)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Review one queued Region Talk publisher correction")
    parser.add_argument("review_file", type=Path)
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--execute", action="store_true", help="Write reviewed correction/audit; default is live dry-run")
    parser.add_argument(
        "--report",
        type=Path,
        default=ROOT / "artifacts" / "codex" / "region-talk-publisher-profile-correction-review.json",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env(args.env_file)
    review = json.loads(args.review_file.read_text(encoding="utf-8"))
    result = execute_review(review, execute=bool(args.execute))
    report = {
        "ok": True,
        "executed": bool(args.execute),
        "execution_status": result["execution_status"],
        "written_ydb_rows": result["written_ydb_rows"],
        "publisher_profile_correction_id": result["correction"]["publisher_profile_correction_id"],
        "canonical_url": result["correction"]["canonical_url"],
        "decision": result["attestation"]["decision"],
        "live_external_publication_id": result["live_external_publication_id"],
        "live_intake_payload_sha256": result["attestation"]["live_intake_payload_sha256"],
        "candidate_mutated": False,
        "publication_permission": "not_granted",
        "publication_effect": "none",
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
