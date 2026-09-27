#!/usr/bin/env python3
"""Publish one already-built named static-site preview to Object Storage.

This is an operator/runtime helper, not a site builder. It never deletes or
syncs bucket objects and can write only below one validated preview-* prefix.
Credentials remain in the trusted runtime environment.
"""
from __future__ import annotations

import argparse
import json
import mimetypes
import os
from pathlib import Path
import re
from typing import Any, Iterable

OBJECT_ORIGIN = "https://storage.yandexcloud.net"
BUILD_RE = re.compile(r"preview-[a-z0-9][a-z0-9._-]*\Z")
RESULT_MARKER = "STATIC_PREVIEW_PUBLISH_JSON="


class PreviewPublishError(RuntimeError):
    pass


def validate_build_id(value: str) -> str:
    build_id = str(value or "").strip()
    if not BUILD_RE.fullmatch(build_id):
        raise PreviewPublishError("build_id must be a validated preview-* prefix")
    return build_id


def validate_source(source_dir: Path, build_id: str) -> Path:
    source = source_dir.resolve()
    if not source.is_dir():
        raise PreviewPublishError("preview source directory does not exist")
    if source.name != build_id:
        raise PreviewPublishError("preview source directory basename must equal build_id")
    manifest_path = source / "preview-build.json"
    if not manifest_path.is_file():
        raise PreviewPublishError("preview-build.json is required")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PreviewPublishError("preview-build.json is invalid") from exc
    if manifest.get("buildId") != build_id:
        raise PreviewPublishError("preview manifest buildId mismatch")
    base_path = manifest.get("basePath")
    if base_path not in (None, f"/{build_id}"):
        raise PreviewPublishError("preview manifest basePath mismatch")
    required = (source / "index.html", source / "poisk" / "index.html")
    if any(not path.is_file() for path in required):
        raise PreviewPublishError("preview review/search entrypoint is missing")
    return source


def iter_files(source: Path, *, owner_review: bool = False) -> Iterable[Path]:
    service_roots = {"__preview", "lab", "_review"}
    for path in sorted(source.rglob("*")):
        if path.is_symlink():
            raise PreviewPublishError("symlinks are not allowed in preview artifact")
        if not path.is_file():
            continue
        rel = path.relative_to(source)
        if owner_review and rel.parts and rel.parts[0] in service_roots:
            continue
        yield path


def object_metadata(rel: str) -> dict[str, str]:
    lower = rel.lower()
    content_type, _ = mimetypes.guess_type(rel)
    extra: dict[str, str] = {"CacheControl": "public, max-age=300"}
    if content_type:
        if content_type in {"text/html", "text/css", "application/javascript", "application/json"}:
            content_type += "; charset=utf-8"
        extra["ContentType"] = content_type
    if rel.startswith("_astro/") or rel.startswith("service-share/versions/"):
        extra["CacheControl"] = "public, max-age=31536000, immutable"
    if rel == "service-share/current/manifest.json":
        extra["ContentType"] = "application/json; charset=utf-8"
        extra["CacheControl"] = "no-cache, max-age=0"
    if rel == "manifest.webmanifest":
        extra["ContentType"] = "application/manifest+json; charset=utf-8"
        extra["CacheControl"] = "public, max-age=300, must-revalidate"
    if lower.endswith(".ics"):
        extra["ContentType"] = "text/calendar; charset=utf-8"
        extra["ContentDisposition"] = 'inline; filename="event.ics"'
    return extra


def create_client():
    import boto3

    bucket = (os.getenv("KENIGEVENTS_SITE_YC_BUCKET") or "").strip()
    access_key = (os.getenv("KENIGEVENTS_SITE_YC_ACCESS_KEY_ID") or "").strip()
    secret_key = (os.getenv("KENIGEVENTS_SITE_YC_SECRET_ACCESS_KEY") or "").strip()
    if not bucket or not access_key or not secret_key:
        raise PreviewPublishError("Object Storage runtime configuration is incomplete")
    client = boto3.client(
        "s3",
        endpoint_url=(os.getenv("KENIGEVENTS_SITE_YC_ENDPOINT") or OBJECT_ORIGIN).strip(),
        region_name=(os.getenv("KENIGEVENTS_SITE_YC_REGION") or "ru-central1").strip(),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    return client, bucket


def publish_preview(
    source_dir: Path,
    build_id: str,
    *,
    client: Any | None = None,
    bucket: str | None = None,
    owner_review: bool = False,
) -> dict[str, Any]:
    build_id = validate_build_id(build_id)
    source = validate_source(source_dir, build_id)
    if client is None:
        client, runtime_bucket = create_client()
        bucket = runtime_bucket
    if not bucket:
        raise PreviewPublishError("bucket is required")

    uploaded = 0
    for path in iter_files(source, owner_review=owner_review):
        rel = path.relative_to(source).as_posix()
        if rel.startswith("../") or rel.startswith("/"):
            raise PreviewPublishError("invalid artifact path")
        key = f"{build_id}/{rel}"
        client.upload_file(str(path), bucket, key, ExtraArgs=object_metadata(rel))
        uploaded += 1
    if uploaded == 0:
        raise PreviewPublishError("preview artifact is empty")
    return {
        "build_id": build_id,
        "prefix": f"{build_id}/",
        "objects": uploaded,
        "entry_path": "/",
        "owner_review": owner_review,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--build-id", required=True)
    parser.add_argument(
        "--owner-review",
        action="store_true",
        help="Publish only user-facing routes; omit __preview/lab/_review service surfaces.",
    )
    args = parser.parse_args()
    result = publish_preview(
        Path(args.source_dir),
        args.build_id,
        owner_review=args.owner_review,
    )
    print(RESULT_MARKER + json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
