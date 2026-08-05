#!/usr/bin/env python3
"""Read-only lifecycle probe for reviewed historical Dobro.ru source pages.

The active vacancy inventory proves OPEN. This separate bounded probe proves
that the same detail parser still recognises CLOSED/EXPIRED source states. It
accepts a rotating list rather than treating one historical page as a permanent
fixture.
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
    return parser


def main() -> int:
    args = build_parser().parse_args()
    checked_at = datetime.now(timezone.utc)
    rows: list[dict[str, object]] = []
    errors: list[dict[str, str]] = []
    for url in dict.fromkeys(args.url):
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
    payload: dict[str, object] = {
        "schema_version": "volunteer-lifecycle-probe-v1",
        "checked_at": checked_at.isoformat(),
        "requested_count": len(dict.fromkeys(args.url)),
        "parsed_count": len(rows),
        "status_counts": counts,
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
                "errors": len(errors),
            },
            ensure_ascii=False,
        )
    )
    if counts["CLOSED"] < 1:
        print("no reviewed source currently proves CLOSED", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
