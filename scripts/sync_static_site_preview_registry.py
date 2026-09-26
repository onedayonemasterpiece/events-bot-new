#!/usr/bin/env python3
"""Inventory public KenigEvents preview prefixes into one review registry.

The bucket inventory is read-only. With --fly-app the inventory code runs on the
app machine, where the site's existing Object Storage read credentials live;
credentials and bearer review tokens are never returned to this process.
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import datetime as dt
import json
import os
from pathlib import Path
import re
import subprocess
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import yaml

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "docs/features/static-site-pages/review-builds.yaml"
PUBLIC_ORIGIN = "https://kenigevents.ru"
OBJECT_ORIGIN = "https://storage.yandexcloud.net"
MARKER = "STATIC_PREVIEW_INVENTORY_JSON="


def bucket_inventory(build_id: str | None = None) -> list[dict]:
    """Run on the machine with site bucket credentials; return no credentials."""
    import boto3

    bucket = os.environ["KENIGEVENTS_SITE_YC_BUCKET"]
    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("KENIGEVENTS_SITE_YC_ENDPOINT") or OBJECT_ORIGIN,
        region_name=os.getenv("KENIGEVENTS_SITE_YC_REGION") or "ru-central1",
        aws_access_key_id=os.environ["KENIGEVENTS_SITE_YC_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["KENIGEVENTS_SITE_YC_SECRET_ACCESS_KEY"],
    )
    if build_id:
        if not re.fullmatch(r"preview-[a-z0-9][a-z0-9._-]*", build_id):
            raise ValueError("--build-id must be a public preview-* prefix")
        prefixes = [build_id + "/"]
    else:
        pages = client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Delimiter="/")
        prefixes = [
            row["Prefix"]
            for page in pages
            for row in page.get("CommonPrefixes", [])
            if row["Prefix"].startswith("preview-")
        ]

    def exists(key: str) -> bool:
        try:
            client.head_object(Bucket=bucket, Key=key)
            return True
        except client.exceptions.ClientError as exc:
            if exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode") in (403, 404):
                return False
            raise

    result = []
    for prefix in prefixes:
        root = client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
        if build_id and not root.get("Contents") and not root.get("CommonPrefixes"):
            raise ValueError(f"Build prefix does not exist: {build_id}")
        root_keys = {row["Key"] for row in root.get("Contents", [])}
        dirs = [row["Prefix"] for row in root.get("CommonPrefixes", [])]
        candidates = [prefix + "index.html", prefix + "__preview/index.html"]
        for preferred in ("poisk/", "populyarnoe/", "segodnya/", "vyhodnye/"):
            if prefix + preferred in dirs:
                candidates.append(prefix + preferred + "index.html")
        candidates.extend(row + "index.html" for row in dirs if row.rstrip("/").split("/")[-1].startswith("date-"))
        candidates.extend(row + "index.html" for row in dirs if row not in {prefix + "_astro/", prefix + "assets/"})
        entry = next((key for key in dict.fromkeys(candidates) if key in root_keys or exists(key)), None)
        if entry is None:
            # Older partial builds sometimes contain only nested event/collection routes.
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
                entry = next((row["Key"] for row in page.get("Contents", []) if row["Key"].endswith("/index.html")), None)
                if entry:
                    break
        if not entry:
            raise ValueError(f"No HTML entry found under {prefix}")
        entry_head = client.head_object(Bucket=bucket, Key=entry)
        manifest = None
        if prefix + "preview-build.json" in root_keys:
            payload = client.get_object(Bucket=bucket, Key=prefix + "preview-build.json")["Body"].read(100_001)
            if len(payload) > 100_000:
                raise ValueError(f"Oversize preview-build.json under {prefix}")
            manifest = json.loads(payload)
        result.append(
            {
                "build_id": prefix.rstrip("/"),
                "entry_key": entry,
                "entry_last_modified": entry_head["LastModified"].isoformat(),
                "entry_size": entry_head["ContentLength"],
                "entry_etag": entry_head["ETag"].strip('"'),
                "manifest": manifest,
            }
        )
    return result


def inventory_from_fly(app: str, build_id: str | None) -> list[dict]:
    flyctl = next((p for p in (Path("/home/dev/.fly/bin/flyctl"), Path("/home/dev/.local/bin/flyctl")) if p.exists()), None)
    if flyctl is None:
        raise RuntimeError("flyctl not found")
    # The deployed app may precede this file, so send the exact local inventory
    # implementation rather than relying on a production code deployment.
    import inspect

    source = "\n".join(
        (
            "import os,json,re",
            "from urllib.request import Request,urlopen",
            "from urllib.error import HTTPError,URLError",
            f"OBJECT_ORIGIN={OBJECT_ORIGIN!r}",
            inspect.getsource(bucket_inventory),
            f"print({MARKER!r}+json.dumps(bucket_inventory({build_id!r}),ensure_ascii=False))",
        )
    )
    encoded = base64.b64encode(source.encode()).decode()
    remote = f"python -c 'import base64; exec(base64.b64decode(\"{encoded}\"))'"
    proc = subprocess.run(
        [str(flyctl), "ssh", "console", "-a", app, "-C", remote],
        text=True,
        capture_output=True,
        timeout=180,
        check=False,
    )
    if proc.returncode:
        raise RuntimeError(f"Fly inventory failed ({proc.returncode}): {proc.stderr.strip()[-500:]}")
    line = next((part[len(MARKER) :] for part in proc.stdout.splitlines() if part.startswith(MARKER)), None)
    if line is None:
        raise RuntimeError(f"Fly inventory returned no registry payload: {proc.stdout[-500:]}")
    return json.loads(line)


def current_candidate_from_fly(app: str) -> dict | None:
    flyctl = next((p for p in (Path("/home/dev/.fly/bin/flyctl"), Path("/home/dev/.local/bin/flyctl")) if p.exists()), None)
    if flyctl is None:
        return None
    proc = subprocess.run(
        [str(flyctl), "ssh", "console", "-a", app, "-C", "python scripts/request_static_site_build.py --db /data/db.sqlite --show-current-review"],
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    if proc.returncode:
        return None
    try:
        record = json.loads(proc.stdout.splitlines()[-1])
    except (IndexError, json.JSONDecodeError):
        return None
    # This repository is public; the public_url is a bearer token and must
    # remain in production state and the owner's private review channel.
    return {key: record.get(key) for key in ("status", "build_id", "repo_sha", "snapshot_id", "verified_at")}


def public_status(url: str) -> int | None:
    try:
        with urlopen(Request(url, method="HEAD"), timeout=15) as response:
            return response.status
    except HTTPError as exc:
        return exc.code
    except (URLError, TimeoutError):
        return None


def git_subject(sha: str | None) -> str | None:
    if not sha or not re.fullmatch(r"[0-9a-f]{40}", sha):
        return None
    result = subprocess.run(["git", "show", "-s", "--format=%s", sha], cwd=ROOT, text=True, capture_output=True, check=False)
    return result.stdout.strip() if result.returncode == 0 else None


def repo_catalog_generated_at(sha: str | None) -> str | None:
    if not sha or not re.fullmatch(r"[0-9a-f]{40}", sha):
        return None
    result = subprocess.run(
        ["git", "show", f"{sha}:site/src/data/preview-events.json"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode:
        return None
    try:
        return json.loads(result.stdout).get("build", {}).get("generated_at")
    except json.JSONDecodeError:
        return None


def build_record(raw: dict, old: dict | None) -> dict:
    build_id = raw["build_id"]
    key = raw["entry_key"]
    path = key[len(build_id) + 1 :]
    entry_route = "/" if path == "index.html" else "/" + path.removesuffix("index.html")
    site_url = f"{PUBLIC_ORIGIN}/{build_id}{entry_route}"
    object_url = f"{OBJECT_ORIGIN}/kenigevents.ru/{key}"
    manifest = raw.get("manifest") or {}
    sha = manifest.get("repo_sha")
    record = {
        "build_id": build_id,
        "built_at": manifest.get("generatedAt"),
        "bucket_entry_modified_at": raw["entry_last_modified"],
        "source_sha": sha,
        "source_commit": git_subject(sha),
        "reference_date": manifest.get("currentDate"),
        "repo_catalog_generated_at": repo_catalog_generated_at(sha),
        "public_url": site_url,
        "bucket_url": object_url,
        "entry_etag": raw["entry_etag"],
        "entry_bytes": raw["entry_size"],
        "manifest_url": f"{PUBLIC_ORIGIN}/{build_id}/preview-build.json" if manifest else None,
        "present_in_bucket": True,
    }
    for field in ("changes", "review_url", "catalog_snapshot_at", "notes"):
        if old and old.get(field):
            record[field] = old[field]
    if "changes" not in record:
        record["changes"] = [record["source_commit"]] if record["source_commit"] else []
    return record


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fly-app", default="", help="Read bucket using the app's existing credentials")
    parser.add_argument("--build-id", default="", help="Update only one public preview prefix")
    parser.add_argument("--check", action="store_true", help="Validate the committed registry without reading the bucket")
    args = parser.parse_args()
    if args.check:
        raw_text = REGISTRY.read_text()
        assert not re.search(r"/_review/[A-Za-z0-9_-]{20,}", raw_text), "bearer review URL leaked into public registry"
        data = yaml.safe_load(raw_text)
        rows = data["builds"]
        ids = [row["build_id"] for row in rows]
        assert len(ids) == len(set(ids))
        assert all(row["changes"] and row["public_url"].startswith(f"{PUBLIC_ORIGIN}/{row['build_id']}/") for row in rows)
        assert all(row["bucket_url"].startswith(f"{OBJECT_ORIGIN}/{data['bucket']}/{row['build_id']}/") for row in rows)
        assert all(row["http_status_at_sync"] in (200, 404) for row in rows)
        assert all(row["bucket_http_status_at_sync"] in (200, 404) for row in rows)
        assert data["latest_named_preview"] == next(
            (row["build_id"] for row in rows if row["present_in_bucket"] and row["http_status_at_sync"] == 200 and row["bucket_http_status_at_sync"] == 200), None
        )
        print(f"registry valid: {len(rows)} public named previews")
        return 0
    existing = yaml.safe_load(REGISTRY.read_text()) if REGISTRY.exists() else {}
    if args.build_id and not existing:
        parser.error("--build-id needs an existing registry; run a full bucket inventory first")
    old = {row["build_id"]: row for row in existing.get("builds", [])}
    rows = inventory_from_fly(args.fly_app, args.build_id or None) if args.fly_app else bucket_inventory(args.build_id or None)
    records = {**old, **{raw["build_id"]: build_record(raw, old.get(raw["build_id"])) for raw in rows}}
    if not args.build_id:
        found = {raw["build_id"] for raw in rows}
        for build_name in records.keys() - found:
            records[build_name]["present_in_bucket"] = False
    ordered = sorted(records.values(), key=lambda row: (row["bucket_entry_modified_at"], row["build_id"]), reverse=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        site_statuses = list(pool.map(public_status, [row["public_url"] for row in ordered]))
        bucket_statuses = list(pool.map(public_status, [row["bucket_url"] for row in ordered]))
    for row, site_status, bucket_status in zip(ordered, site_statuses, bucket_statuses):
        row["http_status_at_sync"] = site_status
        row["bucket_http_status_at_sync"] = bucket_status
    if args.build_id and (records[args.build_id]["http_status_at_sync"] != 200 or records[args.build_id]["bucket_http_status_at_sync"] != 200):
        raise RuntimeError(f"Published entry does not return HTTP 200: {args.build_id}")
    document = {
        "version": 1,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "bucket": "kenigevents.ru",
        "scope": "Observed public named preview-* builds; deleted builds remain as history after their first inventory. Secret _review tokens are excluded.",
        "latest_named_preview": next(
            (row["build_id"] for row in ordered if row["present_in_bucket"] and row["http_status_at_sync"] == 200 and row["bucket_http_status_at_sync"] == 200), None
        ),
        "canonical_full_candidate_lookup": "python scripts/request_static_site_build.py --db /data/db.sqlite --show-current-review (on Fly)",
        "canonical_full_candidate": current_candidate_from_fly(args.fly_app) if args.fly_app else existing.get("canonical_full_candidate"),
        "builds": ordered,
    }
    REGISTRY.parent.mkdir(parents=True, exist_ok=True)
    REGISTRY.write_text(yaml.safe_dump(document, allow_unicode=True, sort_keys=False, width=120), encoding="utf-8")
    print(f"wrote {REGISTRY}: {len(ordered)} builds, latest {document['latest_named_preview']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
