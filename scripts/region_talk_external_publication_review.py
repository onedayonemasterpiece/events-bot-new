#!/usr/bin/env python3
"""Apply an auditable operator decision to one external-publication intake row.

The research importer is deliberately fail-closed: uncertain rows remain in
manual review.  This command is the narrow, explicit bridge from that queue to
CandidateReport after an operator has resolved every blocking reason with
public evidence.  Dry-run is the default; ``--execute`` writes the intake row,
seen-ledger disposition and a separate immutable review record.
"""
from __future__ import annotations

import argparse
import copy
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.region_talk_external_publication_import import (  # noqa: E402
    canonicalize_http_url,
    stable_hash,
    utc_now_iso,
    write_ydb,
)
from scripts.region_talk_goal_notify import (  # noqa: E402
    ensure_ydb_module,
    load_env,
    read_kind_rows,
    ydb_credentials,
    ydb_endpoint_database,
    ydb_table_path,
)


REVIEW_SCHEMA_VERSION = "region_talk_external_publication_review.v1"
ALLOWED_UPDATES = {
    "publication.published_at",
    "publication.date_precision",
    "publication.date_basis",
    "publication.access_status",
    "quality_assessment.scholarly_details.correction_status",
    "quality_assessment.scholarly_details.funding_disclosed",
    "quality_assessment.scholarly_details.conflicts_disclosed",
    "quality_assessment.scholarly_details.limitations_visible",
}


class ReviewError(ValueError):
    pass


def _parse_timestamp(value: Any, field: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise ReviewError(f"{field} is required")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ReviewError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ReviewError(f"{field} must include a timezone")
    return parsed.isoformat()


def _set_nested(row: dict[str, Any], path: str, value: Any) -> None:
    target: dict[str, Any] = row
    parts = path.split(".")
    for part in parts[:-1]:
        child = target.get(part)
        if not isinstance(child, dict):
            raise ReviewError(f"cannot update missing object {'.'.join(parts[:-1])}")
        target = child
    target[parts[-1]] = value


def _validate_update(path: str, value: Any) -> None:
    if path not in ALLOWED_UPDATES:
        raise ReviewError(f"unsupported update field: {path}")
    if path == "publication.published_at":
        try:
            date.fromisoformat(str(value))
        except ValueError as exc:
            raise ReviewError("publication.published_at must be YYYY-MM-DD") from exc
    elif path == "publication.date_precision" and value not in {"day", "month", "year", "unknown"}:
        raise ReviewError("invalid publication.date_precision")
    elif path == "publication.date_basis" and value not in {
        "page_metadata", "issue_metadata", "doi_metadata", "search_snippet", "unknown"
    }:
        raise ReviewError("invalid publication.date_basis")
    elif path == "publication.access_status" and value not in {
        "full_text", "abstract_only", "partial", "paywalled", "unavailable"
    }:
        raise ReviewError("invalid publication.access_status")
    elif path.endswith("correction_status") and value not in {
        "none_found", "corrected", "expression_of_concern", "retracted", "not_checked"
    }:
        raise ReviewError("invalid correction_status")
    elif path.endswith(("funding_disclosed", "conflicts_disclosed")) and value not in {"yes", "no", "unknown"}:
        raise ReviewError(f"invalid value for {path}")
    elif path.endswith("limitations_visible") and not isinstance(value, bool):
        raise ReviewError("limitations_visible must be boolean")


def _score(quality: dict[str, Any], name: str) -> int:
    item = quality.get(name)
    if not isinstance(item, dict):
        return -1
    try:
        return int(item.get("score"))
    except (TypeError, ValueError):
        return -1


def _validate_approval_eligibility(row: dict[str, Any]) -> None:
    publication = row.get("publication") if isinstance(row.get("publication"), dict) else {}
    source = row.get("source_assessment") if isinstance(row.get("source_assessment"), dict) else {}
    policy = row.get("policy_classification") if isinstance(row.get("policy_classification"), dict) else {}
    quality = row.get("quality_assessment") if isinstance(row.get("quality_assessment"), dict) else {}
    if publication.get("access_status") != "full_text":
        raise ReviewError("approval requires publication.access_status=full_text")
    try:
        published = date.fromisoformat(str(publication.get("published_at") or ""))
        start = date.fromisoformat(str(row.get("research_window_start") or ""))
        end = date.fromisoformat(str(row.get("research_window_end") or ""))
    except ValueError as exc:
        raise ReviewError("approval requires an exact publication date and valid research window") from exc
    if not start <= published <= end:
        raise ReviewError("publication date is outside the research window")
    if source.get("scope") != "external":
        raise ReviewError("approval requires an external source scope")
    if not policy.get("product_policy_match") or not policy.get("language_policy_match"):
        raise ReviewError("approval requires product and language policy matches")
    if policy.get("hard_exclusion_codes"):
        raise ReviewError("approval cannot override hard exclusions")
    if quality.get("quality_tier") not in {"strong", "credible"}:
        raise ReviewError("approval requires strong or credible quality")
    for name in ("kaliningrad_centrality", "public_interest", "accessibility"):
        if _score(quality, name) < 2:
            raise ReviewError(f"approval requires {name} score >= 2")
    if quality.get("track") == "scholarly":
        scholarly = quality.get("scholarly_details") if isinstance(quality.get("scholarly_details"), dict) else {}
        if scholarly.get("publication_status") != "peer_reviewed":
            raise ReviewError("scholarly approval requires peer_reviewed status")
        if scholarly.get("correction_status") != "none_found":
            raise ReviewError("scholarly approval requires correction_status=none_found")
        if scholarly.get("funding_disclosed") == "unknown" or scholarly.get("conflicts_disclosed") == "unknown":
            raise ReviewError("scholarly approval requires checked funding/conflict disclosures")


def apply_review(row: dict[str, Any], review: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    if review.get("schema_version") != REVIEW_SCHEMA_VERSION:
        raise ReviewError(f"schema_version must be {REVIEW_SCHEMA_VERSION}")
    external_id = str(review.get("external_publication_id") or "").strip()
    if external_id != str(row.get("external_publication_id") or ""):
        raise ReviewError("review external_publication_id does not match intake row")
    decision = str(review.get("decision") or "").strip().lower()
    if decision not in {"approve", "block"}:
        raise ReviewError("decision must be approve or block")
    reviewer = str(review.get("reviewer") or "").strip()
    reason = str(review.get("reason") or "").strip()
    if not reviewer:
        raise ReviewError("reviewer is required")
    if len(reason) < 20:
        raise ReviewError("reason must be at least 20 characters")
    reviewed_at = _parse_timestamp(review.get("reviewed_at"), "reviewed_at")
    evidence = review.get("evidence")
    if not isinstance(evidence, list) or not evidence:
        raise ReviewError("at least one evidence item is required")
    normalized_evidence: list[dict[str, Any]] = []
    for index, item in enumerate(evidence):
        if not isinstance(item, dict):
            raise ReviewError(f"evidence[{index}] must be an object")
        url = canonicalize_http_url(item.get("url"))
        supports = [str(value).strip() for value in (item.get("supports") or []) if str(value).strip()]
        note = str(item.get("note") or "").strip()
        if not url or not supports or len(note) < 10:
            raise ReviewError(f"evidence[{index}] requires public url, supports and note")
        normalized_evidence.append({"url": url, "supports": supports, "note": note})

    current_decision = row.get("decision") if isinstance(row.get("decision"), dict) else {}
    current_reasons = {str(value) for value in (current_decision.get("reason_codes") or []) if str(value)}
    resolved_reasons = {str(value) for value in (review.get("resolved_reason_codes") or []) if str(value)}
    if decision == "approve" and current_reasons - resolved_reasons:
        missing = ", ".join(sorted(current_reasons - resolved_reasons))
        raise ReviewError(f"approval must resolve every current reason code; missing: {missing}")

    updated = copy.deepcopy(row)
    updates = review.get("updates") or {}
    if not isinstance(updates, dict):
        raise ReviewError("updates must be an object")
    for path, value in updates.items():
        _validate_update(str(path), value)
        _set_nested(updated, str(path), value)

    review_id = "extpubreview_" + stable_hash(external_id, reviewed_at, reviewer, decision)
    attestation = {
        "review_id": review_id,
        "schema_version": REVIEW_SCHEMA_VERSION,
        "decision": "approved" if decision == "approve" else "blocked",
        "reviewer": reviewer,
        "reviewed_at": reviewed_at,
        "reason": reason,
        "resolved_reason_codes": sorted(resolved_reasons),
        "evidence": normalized_evidence,
        "updates": updates,
    }
    if decision == "approve":
        _validate_approval_eligibility(updated)
        updated["decision"] = {
            **current_decision,
            "research_decision": "candidate",
            "downstream_readiness": "candidate_report",
            "reason_codes": ["operator_evidence_review_approved"],
            "reason_short": reason,
            "import_status": "ready_for_region_talk_scoring",
        }
        updated["next_action"] = "run_region_talk_text_vector_and_image_scoring"
        updated["review_status"] = "reviewed"
        updated["review_decision"] = "approved"
    else:
        updated["decision"] = {
            **current_decision,
            "research_decision": "exclude",
            "downstream_readiness": "blocked",
            "reason_codes": ["operator_evidence_review_blocked"],
            "reason_short": reason,
            "import_status": "research_only_blocked",
        }
        updated["next_action"] = "none_operator_blocked"
        updated["review_status"] = "reviewed"
        updated["review_decision"] = "blocked"
    # This review resolves the intake routing question only. Publication still
    # requires the ordinary text/vector/image/LLM/operator release gates.
    updated["publication_permission"] = "not_granted"
    updated["operator_policy_override"] = attestation
    updated["updated_at"] = reviewed_at
    return updated, attestation


def _read_rows(kind: str, limit: int = 20000) -> list[dict[str, Any]]:
    ydb = ensure_ydb_module()
    endpoint, database = ydb_endpoint_database()
    driver = ydb.Driver(endpoint=endpoint, database=database, credentials=ydb_credentials(ydb))
    driver.wait(timeout=20, fail_fast=True)
    pool = ydb.SessionPool(driver)
    try:
        return read_kind_rows(pool, ydb, ydb_table_path(database), kind, limit)
    finally:
        pool.stop()
        driver.stop(timeout=5)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Review one Region Talk external-publication intake row")
    parser.add_argument("review_file", type=Path)
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument(
        "--report",
        type=Path,
        default=ROOT / "artifacts" / "codex" / "region-talk-external-publication-review.json",
    )
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env(args.env_file)
    review = json.loads(args.review_file.read_text(encoding="utf-8"))
    external_id = str(review.get("external_publication_id") or "").strip()
    intake_rows = _read_rows("external_publication_intake_item", 2000)
    matches = [row for row in intake_rows if str(row.get("external_publication_id") or "") == external_id]
    if len(matches) != 1:
        raise ReviewError(f"expected exactly one intake row for {external_id}, found {len(matches)}")
    updated, attestation = apply_review(matches[0], review)

    rows_to_write: list[tuple[str, str, dict[str, Any]]] = [
        (
            "external_publication_intake_item:" + external_id,
            "external_publication_intake_item",
            updated,
        ),
        (
            "external_publication_review_item:" + attestation["review_id"],
            "external_publication_review_item",
            {**attestation, "external_publication_id": external_id, "canonical_url": updated.get("canonical_url")},
        ),
    ]
    seen_rows = _read_rows("external_publication_seen_item", 20000)
    for seen in seen_rows:
        if (
            str(seen.get("canonical_url") or "") == str(updated.get("canonical_url") or "")
            or (updated.get("doi") and str(seen.get("doi") or "") == str(updated.get("doi") or ""))
        ):
            seen_updated = {
                **seen,
                "seen_disposition": (
                    "candidate"
                    if updated["decision"]["import_status"] == "ready_for_region_talk_scoring"
                    else "excluded"
                ),
                "last_review_id": attestation["review_id"],
                "last_seen_at": attestation["reviewed_at"],
                "updated_at": attestation["reviewed_at"],
            }
            seen_id = str(seen_updated.get("external_publication_seen_id") or "")
            if seen_id:
                rows_to_write.append((
                    "external_publication_seen_item:" + seen_id,
                    "external_publication_seen_item",
                    seen_updated,
                ))
            break

    report = {
        "ok": True,
        "executed": bool(args.execute),
        "external_publication_id": external_id,
        "canonical_url": updated.get("canonical_url"),
        "decision": updated.get("decision"),
        "review_id": attestation["review_id"],
        "rows_planned": len(rows_to_write),
        "rows_written": write_ydb(rows_to_write) if args.execute else 0,
        "generated_at": utc_now_iso(),
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
