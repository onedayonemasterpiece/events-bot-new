#!/usr/bin/env python3
"""Validate and materialize the VK observer envelope without logging raw text."""
from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

ALLOWED_ROW_KEYS = {
    "platform", "post_id", "source_public_id", "source_id", "source_title",
    "resolved_group_id", "url", "published_at", "text", "views", "likes",
    "comments", "shares", "is_repost", "is_ad", "has_media", "features",
}
FORBIDDEN_PATTERNS = (
    re.compile(r"(?i)(?:access_token|authorization|bearer|api[_-]?token)\s*[:=]"),
    re.compile(r"(?i)https?://[^\s]+[?&](?:access_token|token|key)=",
    ),
)


def safe_error(value: Any) -> str:
    text = str(value or "")[:200]
    text = re.sub(r"https?://\S+", "[url-redacted]", text)
    return text


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    raw_text = args.input.read_text(encoding="utf-8", errors="strict")
    if len(raw_text.encode("utf-8")) > 32 * 1024 * 1024:
        raise SystemExit("observer_output_too_large")
    if any(pattern.search(raw_text) for pattern in FORBIDDEN_PATTERNS):
        raise SystemExit("observer_output_failed_secret_scan")
    envelope = json.loads(raw_text)
    if not isinstance(envelope, Mapping) or int(envelope.get("schema_version") or 0) != 1:
        raise SystemExit("invalid_observer_envelope")

    rows = envelope.get("rows")
    summaries = envelope.get("summaries")
    if not isinstance(rows, list) or not isinstance(summaries, list):
        raise SystemExit("invalid_observer_collections")

    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping) or not set(row).issubset(ALLOWED_ROW_KEYS):
            raise SystemExit("invalid_observer_row")
        if row.get("platform") != "vk":
            raise SystemExit("unexpected_platform")
        text = str(row.get("text") or "")
        url = str(row.get("url") or "")
        if not re.fullmatch(r"https://vk\.com/wall-\d+_\d+", url):
            raise SystemExit("invalid_post_url")
        normalized_rows.append(dict(row))

    normalized_summaries: list[dict[str, Any]] = []
    for summary in summaries:
        if not isinstance(summary, Mapping):
            raise SystemExit("invalid_source_summary")
        normalized = {str(key): value for key, value in summary.items()}
        normalized["error"] = safe_error(normalized.get("error"))
        normalized_summaries.append(normalized)

    out = args.out
    raw_dir = out / "raw"
    persist = out / "persist"
    raw_dir.mkdir(parents=True, exist_ok=True)
    persist.mkdir(parents=True, exist_ok=True)

    with (raw_dir / "full_vk_corpus.jsonl").open("w", encoding="utf-8") as stream:
        for row in normalized_rows:
            stream.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")

    with (persist / "vk_post_manifest.jsonl").open("w", encoding="utf-8") as stream:
        for row in normalized_rows:
            durable = {key: value for key, value in row.items() if key != "text"}
            stream.write(json.dumps(durable, ensure_ascii=False, sort_keys=True) + "\n")

    fields = sorted({key for row in normalized_summaries for key in row})
    with (persist / "vk_source_summary.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(normalized_summaries)

    run_summary = {
        "schema_version": 1,
        "materialized_at": datetime.now(timezone.utc).isoformat(),
        "observer_collected_at": envelope.get("collected_at"),
        "target_posts_per_source": envelope.get("target_posts_per_source"),
        "token_present_inside_runtime": bool(envelope.get("token_present")),
        "token_name_inside_runtime": envelope.get("token_name"),
        "selected_posts": len(normalized_rows),
        "successful_sources": sum(int(row.get("selected") or 0) > 0 for row in normalized_summaries),
        "complete_sources": sum(int(row.get("selected") or 0) >= int(envelope.get("target_posts_per_source") or 100) for row in normalized_summaries),
        "errors": [
            {"source_id": row.get("source_id"), "error": row.get("error")}
            for row in normalized_summaries if row.get("error")
        ],
        "policy": {
            "transport": "script and public source list over stdin; credential remains inside Fly runtime",
            "provider": "VK official API v5.199",
            "copyright": "full text retained only in one-day workflow artifact",
            "anti_abuse": "bounded requests, jitter/backoff, no bypass or simulated interaction",
        },
    }
    (persist / "vk_run_summary.json").write_text(
        json.dumps(run_summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "selected_posts": len(normalized_rows),
        "successful_sources": run_summary["successful_sources"],
        "complete_sources": run_summary["complete_sources"],
        "errors": len(run_summary["errors"]),
        "token_present_inside_runtime": run_summary["token_present_inside_runtime"],
        "token_name_inside_runtime": run_summary["token_name_inside_runtime"],
    }, ensure_ascii=False))
    return 0 if normalized_rows else 2


if __name__ == "__main__":
    raise SystemExit(main())
