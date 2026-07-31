#!/usr/bin/env python3
"""Generate a run-specific external-research sidecar from the durable YDB seen ledger."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.region_talk_external_publication_import import canonicalize_http_url, normalize_doi  # noqa: E402
from scripts.region_talk_goal_notify import (  # noqa: E402
    ensure_ydb_module,
    load_env,
    read_kind_rows,
    ydb_credentials,
    ydb_endpoint_database,
    ydb_table_path,
)


REQUEST_SCHEMA_VERSION = "region_talk_external_research_request.v1"
SEEN_GUARD_VERSION = "region_talk_external_seen_guard_v1"
SCHEMA_PATH = ROOT / "docs" / "features" / "region-talk-channel" / "external-publication-research-request.schema.json"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _safe_url(value: Any) -> str:
    try:
        return canonicalize_http_url(value)
    except Exception:
        return ""


def _safe_doi(value: Any) -> str:
    try:
        return normalize_doi(value)
    except Exception:
        return ""


def _seen_projection(row: dict[str, Any], *, fallback_disposition: str) -> dict[str, Any] | None:
    publication = row.get("publication") if isinstance(row.get("publication"), dict) else {}
    decision = row.get("decision") if isinstance(row.get("decision"), dict) else {}
    url = _safe_url(row.get("canonical_url") or row.get("url") or row.get("post_url"))
    doi = _safe_doi(row.get("doi"))
    if not url and not doi:
        return None
    raw_disposition = str(row.get("seen_disposition") or decision.get("import_status") or fallback_disposition)
    disposition = {
        "ready_for_region_talk_scoring": "candidate",
        "candidate_report": "candidate",
        "manual_review_required": "manual_review",
        "research_only_blocked": "excluded",
        "blocked": "excluded",
    }.get(raw_disposition, raw_disposition)
    if disposition not in {"candidate", "manual_review", "excluded", "unresolved"}:
        disposition = fallback_disposition
    return {
        "canonical_url": url or None,
        "doi": doi or None,
        "title": str(row.get("title") or publication.get("title") or row.get("title_guess") or "")[:500],
        "source_name": str(row.get("source_name") or publication.get("source_name") or "")[:300],
        "disposition": disposition,
    }


def build_seen_publications(*, seen_rows: list[dict[str, Any]], intake_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge the v1 seen ledger with pre-ledger intake rows for backward compatibility."""
    by_identity: dict[str, dict[str, Any]] = {}
    for row, fallback in [(row, "candidate") for row in intake_rows] + [(row, "unresolved") for row in seen_rows]:
        item = _seen_projection(row, fallback_disposition=fallback)
        if not item:
            continue
        identity = "doi:" + str(item["doi"]) if item.get("doi") else "url:" + str(item["canonical_url"])
        current = by_identity.get(identity)
        if current is None or current.get("disposition") == "unresolved":
            by_identity[identity] = item
    return [by_identity[key] for key in sorted(by_identity)]


def build_request(
    *,
    request_id: str,
    as_of_date: str,
    window_start: str,
    window_end: str,
    research_languages: list[str],
    product_language_policy: str,
    maximum_candidates: int,
    maximum_candidates_per_contour: int,
    blocked_domains: list[str],
    seen_publications: list[dict[str, Any]],
    generated_at: str | None = None,
) -> dict[str, Any]:
    for value in (as_of_date, window_start, window_end):
        date.fromisoformat(value)
    if window_start > window_end or window_end > as_of_date:
        raise ValueError("require window_start <= window_end <= as_of_date")
    canonical_seen = sorted(seen_publications, key=lambda item: (str(item.get("doi") or ""), str(item.get("canonical_url") or "")))
    snapshot_payload = json.dumps(canonical_seen, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    snapshot_id = "rtseen_" + hashlib.sha256(snapshot_payload.encode("utf-8")).hexdigest()[:24]
    payload = {
        "schema_version": REQUEST_SCHEMA_VERSION,
        "generated_at": generated_at or utc_now_iso(),
        "request": {
            "request_id": request_id,
            "as_of_date": as_of_date,
            "window_start": window_start,
            "window_end": window_end,
            "target_region": "Калининградская область, Россия",
            "output_language": "ru",
            "research_languages": list(dict.fromkeys(research_languages)),
            "product_language_policy": product_language_policy,
            "maximum_candidates": maximum_candidates,
            "maximum_candidates_per_contour": maximum_candidates_per_contour,
        },
        "duplicate_guard": {
            "policy_version": SEEN_GUARD_VERSION,
            "snapshot_id": snapshot_id,
            "seen_publication_count": len(canonical_seen),
            "seen_publications": canonical_seen,
        },
        "blocked_domains": sorted({domain.strip().lower() for domain in blocked_domains if domain.strip()}),
    }
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(payload)
    return payload


def read_seen_from_ydb(limit: int) -> list[dict[str, Any]]:
    ydb = ensure_ydb_module()
    endpoint, database = ydb_endpoint_database()
    driver = ydb.Driver(endpoint=endpoint, database=database, credentials=ydb_credentials(ydb))
    driver.wait(timeout=20, fail_fast=True)
    pool = ydb.SessionPool(driver)
    table = ydb_table_path(database)
    try:
        seen = read_kind_rows(pool, ydb, table, "external_publication_seen_item", limit)
        intake = read_kind_rows(pool, ydb, table, "external_publication_intake_item", limit)
        return build_seen_publications(seen_rows=seen, intake_rows=intake)
    finally:
        driver.stop(timeout=5)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a Region Talk external-research request with a YDB-backed duplicate guard")
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--as-of-date", default=date.today().isoformat())
    parser.add_argument("--window-start", required=True)
    parser.add_argument("--window-end", required=True)
    parser.add_argument("--research-language", action="append", dest="research_languages")
    parser.add_argument("--product-language-policy", choices=["ru_only", "ru_or_mostly_ru", "multilingual_manual_review"], default="ru_or_mostly_ru")
    parser.add_argument("--maximum-candidates", type=int, default=30)
    parser.add_argument("--maximum-candidates-per-contour", type=int, default=5)
    parser.add_argument("--blocked-domain", action="append", default=[])
    parser.add_argument("--seen-limit", type=int, default=20000)
    parser.add_argument("--output", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env(args.env_file)
    seen = read_seen_from_ydb(max(1, args.seen_limit))
    payload = build_request(
        request_id=args.request_id,
        as_of_date=args.as_of_date,
        window_start=args.window_start,
        window_end=args.window_end,
        research_languages=args.research_languages or ["ru", "en"],
        product_language_policy=args.product_language_policy,
        maximum_candidates=args.maximum_candidates,
        maximum_candidates_per_contour=args.maximum_candidates_per_contour,
        blocked_domains=args.blocked_domain,
        seen_publications=seen,
    )
    output = args.output or ROOT / "artifacts" / "codex" / f"region-talk-external-research-input-{args.request_id}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "output": str(output),
        "request_id": args.request_id,
        "snapshot_id": payload["duplicate_guard"]["snapshot_id"],
        "seen_publication_count": payload["duplicate_guard"]["seen_publication_count"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
