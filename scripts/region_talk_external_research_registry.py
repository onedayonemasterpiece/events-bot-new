#!/usr/bin/env python3
"""Build and publish the live Region Talk external-research registry.

The registry is intentionally public and contains only public publication
identities plus non-secret search policy. A saved external-agent prompt reads
this stable URL at execution time, so operators never have to regenerate or
attach a per-run duplicate sidecar.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.region_talk_external_research_request import (  # noqa: E402
    build_request,
    read_seen_from_ydb,
)
from scripts.region_talk_goal_notify import load_env  # noqa: E402
from yandex_storage import get_yandex_storage_client, upload_yandex_public_bytes  # noqa: E402


REGISTRY_SCHEMA_VERSION = "region_talk_external_research_registry.v1"
REGISTRY_OBJECT_PATH = "region-talk/external-publications/research-registry.json"
REGISTRY_SCHEMA_OBJECT_PATH = "region-talk/external-publications/research-registry.schema.json"
RESULT_SCHEMA_OBJECT_PATH = "region-talk/external-publications/result.schema.json"
PROMPT_OBJECT_PATH = "region-talk/external-publications/research.prompt.txt"
DEFAULT_PUBLIC_BASE_URL = "https://static.kenigevents.ru"
DEFAULT_STORAGE_ENDPOINT = "https://storage.yandexcloud.net"
DEFAULT_BUCKET = "kenigevents.ru"
REGISTRY_SCHEMA_PATH = ROOT / "docs" / "features" / "region-talk-channel" / "external-publication-research-registry.schema.json"
RESULT_SCHEMA_PATH = ROOT / "docs" / "features" / "region-talk-channel" / "external-publication-research.schema.json"
PROMPT_PATH = ROOT / "docs" / "features" / "region-talk-channel" / "external-publication-research.prompt.txt"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _first_env(*names: str) -> str:
    for name in names:
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return ""


def _public_base_url() -> str:
    return (os.getenv("REGION_TALK_EXTERNAL_RESEARCH_PUBLIC_BASE_URL") or DEFAULT_PUBLIC_BASE_URL).rstrip("/")


def _storage_endpoint() -> str:
    return (os.getenv("YC_STORAGE_ENDPOINT") or DEFAULT_STORAGE_ENDPOINT).rstrip("/")


def _bucket() -> str:
    return (
        os.getenv("REGION_TALK_EXTERNAL_RESEARCH_BUCKET")
        or os.getenv("KENIGEVENTS_SITE_YC_BUCKET")
        or DEFAULT_BUCKET
    ).strip()


def _iam_token() -> str:
    """Return an already-issued short-lived token without creating credentials."""

    return _first_env("REGION_TALK_YDB_IAM_TOKEN", "YC_IAM_TOKEN", "YANDEX_IAM_TOKEN")


def _url(object_path: str) -> str:
    return f"{_public_base_url()}/{object_path.lstrip('/')}"


def build_registry(
    seen_publications: list[dict[str, Any]],
    *,
    generated_at: str | None = None,
    earliest_publication_date: str = "2025-01-01",
    research_languages: list[str] | None = None,
    product_language_policy: str = "ru_or_mostly_ru",
    maximum_candidates: int = 30,
    maximum_candidates_per_contour: int = 5,
    blocked_domains: list[str] | None = None,
) -> dict[str, Any]:
    # Reuse the canonical sorting and snapshot algorithm from the legacy
    # sidecar builder, but expose no run-specific request values.
    snapshot = build_request(
        request_id="registry-build-only",
        as_of_date=earliest_publication_date,
        window_start=earliest_publication_date,
        window_end=earliest_publication_date,
        research_languages=research_languages or ["ru", "en"],
        product_language_policy=product_language_policy,
        maximum_candidates=maximum_candidates,
        maximum_candidates_per_contour=maximum_candidates_per_contour,
        blocked_domains=blocked_domains or [],
        seen_publications=seen_publications,
        generated_at=generated_at,
    )
    result_schema_bytes = RESULT_SCHEMA_PATH.read_bytes()
    payload = {
        "schema_version": REGISTRY_SCHEMA_VERSION,
        "generated_at": generated_at or utc_now_iso(),
        "public_registry_url": _url(REGISTRY_OBJECT_PATH),
        "result_contract": {
            "schema_version": "region_talk_external_research.v1",
            "schema_url": _url(RESULT_SCHEMA_OBJECT_PATH),
            "sha256": hashlib.sha256(result_schema_bytes).hexdigest(),
        },
        "search_policy": {
            "target_region": "Калининградская область, Россия",
            "output_language": "ru",
            "earliest_publication_date": earliest_publication_date,
            "research_languages": research_languages or ["ru", "en"],
            "product_language_policy": product_language_policy,
            "maximum_candidates": maximum_candidates,
            "maximum_candidates_per_contour": maximum_candidates_per_contour,
        },
        "duplicate_guard": snapshot["duplicate_guard"],
        "blocked_domains": sorted({str(value).strip().lower() for value in (blocked_domains or []) if str(value).strip()}),
    }
    schema = json.loads(REGISTRY_SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(payload)
    return payload


def _upload_yandex_public_bytes_with_iam(
    data: bytes,
    *,
    object_path: str,
    content_type: str,
    bucket: str,
    cache_control: str,
    iam_token: str,
) -> str:
    """PUT an Object Storage object using a short-lived IAM bearer token.

    Yandex Object Storage accepts IAM-token authentication for the S3 HTTP API
    without AWS request signing. The caller must supply a token for a principal
    with write access to the target bucket.
    """

    if not data:
        raise RuntimeError("refusing to upload an empty registry object")
    clean_path = str(object_path or "").strip().lstrip("/")
    if not bucket or not clean_path or not iam_token:
        raise RuntimeError("bucket, object path and IAM token are required")
    endpoint = _storage_endpoint()
    target = f"{endpoint}/{quote(bucket, safe='')}/{quote(clean_path, safe='/')}"
    request = Request(
        target,
        data=data,
        method="PUT",
        headers={
            "Authorization": f"Bearer {iam_token}",
            "Content-Type": str(content_type or "application/octet-stream"),
            "Cache-Control": str(cache_control or "public, no-cache, no-store"),
        },
    )
    try:
        with urlopen(request, timeout=30) as response:  # noqa: S310 - fixed HTTPS endpoint
            status = int(getattr(response, "status", 0) or response.getcode() or 0)
    except HTTPError as exc:
        try:
            detail = exc.read(800).decode("utf-8", errors="replace")
        except Exception:
            detail = ""
        raise RuntimeError(
            f"Object Storage IAM PUT failed for {clean_path}: HTTP {exc.code} {detail[:500]}"
        ) from exc
    except URLError as exc:
        raise RuntimeError(f"Object Storage IAM PUT failed for {clean_path}: {exc.reason}") from exc
    if status < 200 or status >= 300:
        raise RuntimeError(f"Object Storage IAM PUT returned HTTP {status} for {clean_path}")
    return _url(clean_path)


def publish_registry(payload: dict[str, Any]) -> dict[str, Any]:
    iam_token = _iam_token()
    client = None if iam_token else get_yandex_storage_client()
    if not iam_token and client is None:
        raise RuntimeError(
            "Yandex Object Storage IAM token or static storage credentials are required "
            "to publish the research registry"
        )
    auth_mode = "iam_token" if iam_token else "static_access_key"
    bucket = _bucket()
    objects = {
        REGISTRY_OBJECT_PATH: (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8"),
        REGISTRY_SCHEMA_OBJECT_PATH: REGISTRY_SCHEMA_PATH.read_bytes(),
        RESULT_SCHEMA_OBJECT_PATH: RESULT_SCHEMA_PATH.read_bytes(),
        PROMPT_OBJECT_PATH: PROMPT_PATH.read_bytes(),
    }
    urls: dict[str, str] = {}
    cache_control = "public, no-cache, no-store, must-revalidate, max-age=0"
    for object_path, data in objects.items():
        content_type = "application/json; charset=utf-8" if object_path.endswith(".json") else "text/plain; charset=utf-8"
        if iam_token:
            uploaded = _upload_yandex_public_bytes_with_iam(
                data,
                object_path=object_path,
                content_type=content_type,
                bucket=bucket,
                cache_control=cache_control,
                iam_token=iam_token,
            )
        else:
            uploaded = upload_yandex_public_bytes(
                data,
                object_path=object_path,
                content_type=content_type,
                bucket=bucket,
                cache_control=cache_control,
                client=client,
            )
        if not uploaded:
            raise RuntimeError(f"failed to publish s3://{bucket}/{object_path}")
        urls[object_path] = _url(object_path)
    return {
        "bucket": bucket,
        "auth_mode": auth_mode,
        "snapshot_id": payload["duplicate_guard"]["snapshot_id"],
        "seen_publication_count": payload["duplicate_guard"]["seen_publication_count"],
        "urls": urls,
    }


def publish_current_registry(*, seen_limit: int = 20000) -> dict[str, Any]:
    seen = read_seen_from_ydb(max(1, seen_limit))
    payload = build_registry(seen)
    return publish_registry(payload)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build/publish the live Region Talk external-research registry")
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--seen-limit", type=int, default=20000)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--publish", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env(args.env_file)
    seen = read_seen_from_ydb(max(1, args.seen_limit))
    payload = build_registry(seen)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report = {
        "snapshot_id": payload["duplicate_guard"]["snapshot_id"],
        "seen_publication_count": payload["duplicate_guard"]["seen_publication_count"],
        "output": str(args.output) if args.output else None,
        "published": publish_registry(payload) if args.publish else None,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
