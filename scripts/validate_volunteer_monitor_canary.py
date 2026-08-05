#!/usr/bin/env python3
"""Validate a read-only Volunteer Monitor canary result.

A source canary is allowed to finish PARTIAL when every selected parent source
has an explicit terminal disposition: extracted opportunity, rejected
outside-region row, or one recorded fetch/parser error. A partial result is a
warning, not a false green or an empty success.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from urllib.parse import urlsplit


def _warning_source(warning: str) -> tuple[str, str] | None:
    if warning.startswith("outside_target_region:"):
        return "outside", warning.split(":", 1)[1]
    if warning.startswith("https://"):
        # Runtime errors are rendered as `<url>: <type>: <message>`.
        parts = warning.split(": ", 1)
        return "error", parts[0]
    return None


def validate(path: Path) -> dict[str, object]:
    result = json.loads(path.read_text(encoding="utf-8"))
    if result.get("schema_version") != "volunteer-monitor-result-v1":
        raise ValueError("unexpected Volunteer Monitor schema")
    run_status = str(result.get("run_status") or "")
    if run_status not in {"PASS", "PARTIAL", "WARN_NO_LIVE_SUPPLY"}:
        raise ValueError(f"unexpected run_status: {run_status}")

    source_pages = int(result.get("source_pages_seen") or 0)
    opportunity_count = int(result.get("opportunity_count") or 0)
    outside_count = int(result.get("outside_region_count") or 0)
    warnings = [str(value) for value in result.get("warnings") or []]
    status_counts = result.get("status_counts") or {}

    if run_status == "WARN_NO_LIVE_SUPPLY":
        if source_pages or opportunity_count:
            raise ValueError("no-supply result contains source/opportunity rows")
        return {"status": run_status, "accounted": 0, "warning_count": len(warnings)}

    if source_pages <= 0 or opportunity_count <= 0:
        raise ValueError("live canary requires at least one source and opportunity")
    if int(status_counts.get("OPEN") or 0) <= 0:
        raise ValueError("live canary did not prove any open vacancy-backed opportunity")

    outside_sources: set[str] = set()
    error_sources: set[str] = set()
    unclassified: list[str] = []
    for warning in warnings:
        parsed = _warning_source(warning)
        if parsed is None:
            unclassified.append(warning)
            continue
        kind, url = parsed
        if urlsplit(url).hostname not in {"dobro.ru", "www.dobro.ru"}:
            raise ValueError(f"warning contains non-Dobro source URL: {url}")
        (outside_sources if kind == "outside" else error_sources).add(url)

    if unclassified:
        raise ValueError(f"unclassified canary warnings: {unclassified[:3]}")
    if len(outside_sources) != outside_count:
        raise ValueError(
            f"outside-region count mismatch: receipt={outside_count}, warnings={len(outside_sources)}"
        )
    overlap = outside_sources & error_sources
    if overlap:
        raise ValueError(f"source received two terminal dispositions: {sorted(overlap)[:3]}")

    accounted = opportunity_count + len(outside_sources) + len(error_sources)
    if accounted != source_pages:
        raise ValueError(
            f"source accounting mismatch: selected={source_pages}, accounted={accounted}"
        )
    if run_status == "PASS" and warnings:
        raise ValueError("PASS result contains warnings")
    if run_status == "PARTIAL" and not warnings:
        raise ValueError("PARTIAL result has no recorded warning")

    return {
        "status": run_status,
        "source_pages_seen": source_pages,
        "opportunity_count": opportunity_count,
        "outside_region_count": outside_count,
        "error_source_count": len(error_sources),
        "accounted": accounted,
    }


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: validate_volunteer_monitor_canary.py RESULT.json", file=sys.stderr)
        return 2
    try:
        summary = validate(Path(sys.argv[1]))
    except Exception as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
