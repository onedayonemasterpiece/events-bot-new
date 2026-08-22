#!/usr/bin/env python3
"""Create or verify one immutable UI reference-event corpus.

The input is the public ``PreviewEvent`` export produced by
``site/scripts/export-production-preview-data.py``.  This adapter deliberately
does not read arbitrary database rows and does not own a second editable corpus:
it writes the canonical versioned package into lovekgd-design-system.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import re
import shutil
import subprocess
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


HEX64 = re.compile(r"(?<![0-9a-f])[0-9a-f]{64}(?![0-9a-f])")


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True).encode() + b"\n")


def classify_admission(event: dict[str, Any]) -> str:
    ticket = event.get("ticket") or {}
    if ticket.get("is_free") or ticket.get("kind") == "free":
        return "free"
    if ticket.get("price_label"):
        return "price"
    return "unavailable"


def image_dimensions(path: Path) -> tuple[int | None, int | None]:
    result = subprocess.run(
        ["identify", "-format", "%w %h", str(path)],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None, None
    try:
        width, height = result.stdout.strip().split()
        return int(width), int(height)
    except (ValueError, TypeError):
        return None, None


def fetch_asset(url: str, cache_path: Path) -> tuple[bytes, dict[str, str]]:
    request = urllib.request.Request(url, headers={"User-Agent": "LoveKGD-UI-Corpus/1.0"})
    with urllib.request.urlopen(request, timeout=45) as response:
        body = response.read()
        headers = {key.lower(): value for key, value in response.headers.items()}
        resolved_url = response.geturl()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(body)
    headers["resolved-url"] = resolved_url
    return body, headers


def coverage_tags(event: dict[str, Any], current_date: str) -> list[str]:
    from datetime import date, timedelta

    reference = date.fromisoformat(current_date)
    start = date.fromisoformat(event["start_date"])
    tags: list[str] = []
    if start == reference:
        tags.append("reference-day")
    if start == reference + timedelta(days=1):
        tags.append("next-day")
    saturday = reference + timedelta(days=(5 - reference.weekday()) % 7)
    if saturday <= start <= saturday + timedelta(days=1):
        tags.append("nearest-weekend")
    if reference + timedelta(days=45) <= start <= reference + timedelta(days=75):
        tags.append("future-45-75d")
    if event.get("end_date") and date.fromisoformat(event["end_date"]) - start >= timedelta(days=7):
        tags.append("long-running")
    assets = event.get("image_assets") or []
    primary = assets[0] if assets else None
    if primary:
        width, height = int(primary.get("width") or 0), int(primary.get("height") or 0)
        mode = primary.get("image_text_mode") or event.get("image_text_mode")
        if width > height and mode == "visual_only" and primary.get("safe_crop") is True:
            tags.append("landscape-crop-safe")
        if height > width and mode != "visual_only":
            tags.append("portrait-poster")
        if mode == "ocr_text":
            tags.append("ocr-protected")
    if len(assets) >= 2:
        tags.append("multi-image")
    if not assets and not event.get("image_url"):
        tags.append("no-image")
    place = " · ".join(filter(None, [event.get("city"), event.get("venue_name")]))
    if len(event.get("title") or "") >= 70 or len(place) >= 55:
        tags.append("long-copy")
    tags.append(f"admission-{classify_admission(event)}")
    return tags


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preview-events", required=True)
    parser.add_argument("--design-system-root", required=True)
    parser.add_argument("--version", default="v1")
    parser.add_argument("--event-ids", required=True, help="Comma-separated stable production event ids")
    parser.add_argument("--current-date", required=True)
    parser.add_argument("--reference-iso", required=True)
    parser.add_argument("--timezone", default="Europe/Kaliningrad")
    parser.add_argument("--source-repository-sha", required=True)
    parser.add_argument("--source-static-build-id", required=True)
    parser.add_argument("--source-database-snapshot-fingerprint", required=True)
    parser.add_argument("--extracted-at", required=True)
    parser.add_argument("--asset-cache", required=True)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()

    preview_path = Path(args.preview_events).resolve()
    target = Path(args.design_system_root).resolve() / "catalog/fixtures/ui-reference-events" / args.version
    cache = Path(args.asset_cache).resolve()
    # Fail before reading, hashing, downloading, or writing any event/asset.
    # Verification is the only operation allowed against an existing version.
    if target.exists() and not args.verify:
        raise SystemExit(f"Refusing to rewrite immutable corpus: {target}")
    source = json.loads(preview_path.read_text())
    requested = [int(value) for value in args.event_ids.split(",") if value.strip()]
    by_id = {int(event["id"]): event for event in source["events"]}
    missing = [event_id for event_id in requested if event_id not in by_id]
    if missing:
        raise SystemExit(f"PreviewEvent ids missing from exact export: {missing}")

    fixtures: list[dict[str, Any]] = []
    asset_rows: list[dict[str, Any]] = []
    for event_id in requested:
        event = by_id[event_id]
        fixture_id = f"event.real.{event_id}"
        payload_sha = sha256_bytes(canonical_bytes(event))
        fixture = {
            "schema_version": "ui-reference-event.v1",
            "fixture_id": fixture_id,
            "event_id": event_id,
            "source_prod_id": int(event["source_prod_id"]),
            "slug": event["slug"],
            "source_repository_sha": args.source_repository_sha,
            "source_static_build_id": args.source_static_build_id,
            "source_database_snapshot_fingerprint": args.source_database_snapshot_fingerprint,
            "extracted_at": args.extracted_at,
            "preview_event_sha256": payload_sha,
            "payload_path": f"events/{fixture_id}.json",
            "coverage_tags": coverage_tags(event, args.current_date),
            "preview_event": event,
        }
        fixtures.append({key: fixture[key] for key in fixture if key != "preview_event"})
        if not args.verify:
            write_json(target / fixture["payload_path"], fixture)

        seen_urls: set[str] = set()
        for index, asset in enumerate(event.get("image_assets") or []):
            url = str(asset.get("src") or "")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            url_key = hashlib.sha256(url.encode()).hexdigest()
            parsed_url = urllib.parse.urlparse(url)
            ext = Path(parsed_url.path).suffix or ".bin"
            cache_path = cache / f"{url_key}{ext}"
            if cache_path.exists():
                body = cache_path.read_bytes()
                headers: dict[str, str] = {}
                resolved_url = url
            else:
                body, headers = fetch_asset(url, cache_path)
                resolved_url = headers.pop("resolved-url")
            byte_sha = sha256_bytes(body)
            width, height = image_dimensions(cache_path)
            expected_width, expected_height = asset.get("width"), asset.get("height")
            if width is not None and (width != expected_width or height != expected_height):
                raise SystemExit(
                    f"Asset geometry mismatch for {fixture_id}[{index}]: "
                    f"payload={expected_width}x{expected_height}, bytes={width}x{height}"
                )
            path_hash = next(iter(HEX64.findall(parsed_url.path)), None)
            cache_control = headers.get("cache-control", "")
            immutable = bool(path_hash == byte_sha and "/p/image/v2/" in parsed_url.path)
            asset_id = f"asset.sha256.{byte_sha}"
            storage_mode = "immutable-cdn" if immutable else "git-content-addressed-bundle"
            bundle_relpath = None
            if not immutable:
                bundle_relpath = f"assets/{byte_sha}{ext}"
                if not args.verify:
                    bundle_path = target / bundle_relpath
                    bundle_path.parent.mkdir(parents=True, exist_ok=True)
                    if bundle_path.exists() and sha256_bytes(bundle_path.read_bytes()) != byte_sha:
                        raise SystemExit(f"Refusing to overwrite mismatched fixture asset: {bundle_path}")
                    shutil.copyfile(cache_path, bundle_path)
            asset_rows.append({
                "asset_id": asset_id,
                "fixture_id": fixture_id,
                "event_id": event_id,
                "role": "primary" if index == 0 else "gallery",
                "source_url": url,
                "resolved_url": resolved_url,
                "mime_type": headers.get("content-type", "").split(";")[0] or mimetypes.guess_type(url)[0] or "application/octet-stream",
                "width": expected_width,
                "height": expected_height,
                "byte_length": len(body),
                "sha256": byte_sha,
                "storage_mode": storage_mode,
                "cdn_path_content_key": path_hash,
                "cache_control": cache_control or None,
                "local_cache_relpath": cache_path.name,
                "bundle_relpath": bundle_relpath,
            })

    asset_rows.sort(key=lambda row: (row["event_id"], row["role"], row["source_url"]))
    assets_manifest = {
        "schema_version": "ui-reference-assets-manifest.v1",
        "corpus_id": f"ui-reference-events.{args.version}",
        "verification_policy": "download-exact-bytes-and-sha256-before-render",
        "assets": asset_rows,
    }
    assets_manifest["assets_manifest_sha256"] = sha256_bytes(canonical_bytes(assets_manifest))
    corpus = {
        "schema_version": "ui-reference-event-corpus.v1",
        "corpus_id": f"ui-reference-events.{args.version}",
        "version": args.version,
        "immutable": True,
        "reference_clock": {
            "current_date": args.current_date,
            "reference_iso": args.reference_iso,
            "timezone": args.timezone,
        },
        "source": {
            "repository": "onedayonemasterpiece/events-bot-new",
            "repository_sha": args.source_repository_sha,
            "static_build_id": args.source_static_build_id,
            "database_snapshot_fingerprint": args.source_database_snapshot_fingerprint,
            "preview_export_sha256": hashlib.sha256(preview_path.read_bytes()).hexdigest(),
            "preview_contract": "PreviewEvent",
        },
        "fixtures": fixtures,
        "assets_manifest_path": "assets-manifest.json",
        "surface_expectations_path": "surface-expectations.json",
    }
    corpus["corpus_sha256"] = sha256_bytes(canonical_bytes(corpus))

    expected = {
        target / "corpus.json": corpus,
        target / "assets-manifest.json": assets_manifest,
    }
    if args.verify:
        errors = []
        for path, value in expected.items():
            if not path.exists() or json.loads(path.read_text()) != value:
                errors.append(str(path))
        for fixture in fixtures:
            path = target / fixture["payload_path"]
            if not path.exists():
                errors.append(str(path))
                continue
            wrapper = json.loads(path.read_text())
            if sha256_bytes(canonical_bytes(wrapper["preview_event"])) != wrapper["preview_event_sha256"]:
                errors.append(f"payload-hash:{path}")
        for asset in asset_rows:
            if asset["storage_mode"] != "git-content-addressed-bundle":
                continue
            path = target / asset["bundle_relpath"]
            if not path.exists() or sha256_bytes(path.read_bytes()) != asset["sha256"]:
                errors.append(f"asset-hash:{path}")
        if errors:
            raise SystemExit("Immutable corpus verification failed: " + ", ".join(errors))
    else:
        for path, value in expected.items():
            write_json(path, value)

    print(json.dumps({
        "corpus": str(target),
        "corpus_sha256": corpus["corpus_sha256"],
        "fixtures": len(fixtures),
        "assets": len(asset_rows),
        "asset_storage_modes": sorted({row["storage_mode"] for row in asset_rows}),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
