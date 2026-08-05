#!/usr/bin/env python3
"""Read-only lifecycle probe for reviewed historical Dobro.ru source pages.

The active vacancy inventory proves OPEN. This separate bounded probe verifies
source-backed non-public lifecycle states with the same detail parser. Callers
may require an exact state such as CLOSED instead of weakening acceptance to any
non-open result. The URL list is rotating and remains source-reviewed.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

from volunteer_monitor.dobro_page import parse_event_page
from volunteer_monitor.types import AvailabilityStatus, canonical_json_hash


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument(
        "--require-status",
        action="append",
        choices=[status.value for status in AvailabilityStatus],
        default=[],
        help="Require at least one parsed source row for each exact status",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    checked_at = datetime.now(timezone.utc)
    requested_urls = tuple(dict.fromkeys(args.url))
    required_statuses = tuple(dict.fromkeys(args.require_status))
    rows: list[dict[str, object]] = []
    errors: list[dict[str, str]] = []

    for url in requested_urls:
        try:
            response = requests.get(
                url,
                timeout=max(5.0, min(float(args.timeout_seconds), 60.0)),
                headers={
                    "User-Agent": (
                        "KenigEventsVolunteerMonitorLifecycleProbe/0.1 "
                        "(+https://kenigevents.ru; read-only canary)"
                    )
                },
            )
            response.raise_for_status()
            item = parse_event_page(response.text, source_url=url, checked_at=checked_at)
            rows.append(item.to_dict())
        except Exception as exc:
            errors.append(
                {
                    "url": url,
                    "error_type": type(exc).__name__,
                    "error": str(exc)[:500],
                }
            )

    counts = {status.value: 0 for status in AvailabilityStatus}
    for row in rows:
        counts[str(row["availability_status"])] += 1
    non_open_count = counts["CLOSED"] + counts["EXPIRED"]
    missing_required = [status for status in required_statuses if counts[status] < 1]

    payload: dict[str, object] = {
        "schema_version": "volunteer-lifecycle-probe-v1",
        "checked_at": checked_at.isoformat(),
        "requested_count": len(requested_urls),
        "parsed_count": len(rows),
        "status_counts": counts,
        "non_open_count": non_open_count,
        "required_statuses": list(required_statuses),
        "missing_required_statuses": missing_required,
        "errors": errors,
        "rows": rows,
    }
    payload["result_sha256"] = canonical_json_hash(payload)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "output": str(args.output),
                "parsed_count": len(rows),
                "CLOSED": counts["CLOSED"],
                "EXPIRED": counts["EXPIRED"],
                "non_open_count": non_open_count,
                "required_statuses": list(required_statuses),
                "missing_required_statuses": missing_required,
                "errors": len(errors),
            },
            ensure_ascii=False,
        )
    )

    if missing_required:
        print(
            "reviewed sources did not prove required status: "
            + ", ".join(missing_required),
            file=sys.stderr,
        )
        return 1
    if not required_statuses and non_open_count < 1:
        print("no reviewed source currently proves CLOSED or EXPIRED", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
