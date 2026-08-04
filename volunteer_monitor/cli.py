from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import date
from pathlib import Path
from typing import Sequence

from .service import read_event_map, run_fixture_monitor, run_live_monitor
from .source_config import DobroSourceConfig
from .types import MonitorRunStatus


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only bounded Dobro.ru volunteer monitor")
    parser.add_argument("--mode", choices=("fixture", "live_canary"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--search-html", type=Path)
    parser.add_argument("--event-map", type=Path)
    parser.add_argument("--today", type=date.fromisoformat)
    parser.add_argument(
        "--search-url",
        default=os.getenv(
            "VOLUNTEER_MONITOR_SEARCH_URL",
            "https://dobro.ru/search?d_c=1&d_s=1&t=e",
        ),
    )
    parser.add_argument(
        "--region",
        default=os.getenv("VOLUNTEER_MONITOR_REGION", "Калининградская область"),
    )
    parser.add_argument(
        "--max-items",
        type=_positive_int,
        default=int(os.getenv("VOLUNTEER_MONITOR_MAX_ITEMS", "120")),
    )
    parser.add_argument(
        "--max-more-clicks",
        type=int,
        default=int(os.getenv("VOLUNTEER_MONITOR_MAX_MORE_CLICKS", "40")),
    )
    parser.add_argument(
        "--playwright-timeout-ms",
        type=_positive_int,
        default=int(os.getenv("VOLUNTEER_MONITOR_PLAYWRIGHT_TIMEOUT_MS", "30000")),
    )
    parser.add_argument(
        "--detail-timeout-seconds",
        type=float,
        default=float(os.getenv("VOLUNTEER_MONITOR_DETAIL_TIMEOUT_SECONDS", "30")),
    )
    parser.add_argument(
        "--permission-reference",
        default=os.getenv(
            "VOLUNTEER_MONITOR_PERMISSION_REFERENCE",
            "pending-volunteer-centre-approval",
        ),
    )
    parser.add_argument("--evidence-dir", type=Path)
    parser.add_argument("--headed", action="store_true")
    return parser


async def _run(args: argparse.Namespace):
    config = DobroSourceConfig(
        search_url=args.search_url,
        region_name=args.region,
        max_more_clicks=args.max_more_clicks,
        max_items=args.max_items,
        playwright_timeout_ms=args.playwright_timeout_ms,
        detail_timeout_seconds=args.detail_timeout_seconds,
        headless=not args.headed,
        permission_reference=args.permission_reference,
        evidence_dir=args.evidence_dir,
    )
    config.validate()
    if args.mode == "fixture":
        if not args.search_html or not args.event_map:
            raise ValueError("fixture mode requires --search-html and --event-map")
        return run_fixture_monitor(
            search_html=args.search_html.read_text(encoding="utf-8"),
            event_html_by_url=read_event_map(args.event_map),
            config=config,
            today=args.today,
        )
    return await run_live_monitor(config=config)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = asyncio.run(_run(args))
        payload = result.to_dict()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(
            json.dumps(
                {
                    "status": payload["run_status"],
                    "output": str(args.output),
                    "source_pages_seen": payload["source_pages_seen"],
                    "opportunity_count": payload["opportunity_count"],
                    **payload["status_counts"],
                },
                ensure_ascii=False,
            )
        )
        return 2 if result.run_status is MonitorRunStatus.PARTIAL else 0
    except Exception as exc:
        print(
            json.dumps(
                {"status": "error", "error_type": type(exc).__name__, "error": str(exc)},
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
