#!/usr/bin/env python3
"""Copy currently referenced event media from legacy bucket to the CDN bucket.

The bot historically persisted poster URLs as:

    https://storage.yandexcloud.net/kenigevents/p/...

The static site CDN fronts the `kenigevents.ru` bucket, so the same object keys
must exist there before `PUBLIC_ASSET_BASE_URL=https://static.kenigevents.ru` is
enabled. This script performs an idempotent S3 server-side copy for referenced
objects; it does not mutate SQLite rows because the Astro layer rewrites legacy
raw URLs to CDN URLs at render time.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
from pathlib import Path
from urllib.parse import quote, urlparse

ROOT = Path(__file__).resolve().parent.parent
LEGACY_BUCKET = "kenigevents"
DEFAULT_TARGET_BUCKET = "kenigevents.ru"
DEFAULT_ENDPOINT = "https://storage.yandexcloud.net"


def load_dotenv(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        out[key.strip()] = value
    return out


def merged_env() -> dict[str, str]:
    file_env = load_dotenv(ROOT / ".env")
    return {**file_env, **os.environ}


def first_env(env: dict[str, str], *names: str) -> str:
    for name in names:
        value = str(env.get(name) or "").strip()
        if value:
            return value
    return ""


def parse_yandex_url(url: str | None) -> tuple[str, str] | None:
    raw = str(url or "").strip()
    if not raw:
        return None
    try:
        parsed = urlparse(raw)
    except Exception:
        return None
    host = (parsed.netloc or "").lower()
    parts = [p for p in (parsed.path or "").split("/") if p]
    if host == "storage.yandexcloud.net" and len(parts) >= 2:
        return parts[0], "/".join(parts[1:])
    suffix = ".storage.yandexcloud.net"
    if host.endswith(suffix) and parts:
        return host[: -len(suffix)].strip("."), "/".join(parts)
    if host == "static.kenigevents.ru" and parts:
        return DEFAULT_TARGET_BUCKET, "/".join(parts)
    return None


def iter_json_urls(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v or "").strip()]
    raw = str(value or "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    if isinstance(parsed, list):
        return [str(v).strip() for v in parsed if str(v or "").strip()]
    return []


def collect_from_preview_json(path: Path, *, source_bucket: str, prefix: str) -> set[str]:
    if not path.exists():
        return set()
    data = json.loads(path.read_text(encoding="utf-8"))
    keys: set[str] = set()
    for event in data.get("events") or []:
        candidates = [event.get("image_url")]
        for image in event.get("images") or []:
            if isinstance(image, dict):
                candidates.append(image.get("src"))
        for url in candidates:
            parsed = parse_yandex_url(str(url or ""))
            if parsed and parsed[0] == source_bucket and parsed[1].startswith(prefix):
                keys.add(parsed[1])
    return keys


def collect_from_db(db_path: Path, *, source_bucket: str, prefix: str, active_on: str | None) -> set[str]:
    if not db_path.exists():
        raise FileNotFoundError(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    keys: set[str] = set()

    params: list[object] = []
    where = ""
    if active_on:
        where = " WHERE COALESCE(end_date, date) >= ?"
        params.append(active_on)
    event_ids: list[int] = []
    for row in cur.execute(f"SELECT id, photo_urls FROM event{where}", params):
        event_ids.append(int(row["id"]))
        for url in iter_json_urls(row["photo_urls"]):
            parsed = parse_yandex_url(url)
            if parsed and parsed[0] == source_bucket and parsed[1].startswith(prefix):
                keys.add(parsed[1])

    def add_table_urls(table: str) -> None:
        try:
            cur.execute(f"SELECT 1 FROM {table} LIMIT 1")
        except Exception:
            return
        if event_ids:
            chunk_size = 800
            for start in range(0, len(event_ids), chunk_size):
                chunk = event_ids[start : start + chunk_size]
                placeholders = ",".join("?" for _ in chunk)
                query = f"SELECT supabase_url FROM {table} WHERE event_id IN ({placeholders}) AND supabase_url IS NOT NULL"
                rows = cur.execute(query, chunk)
                for media_row in rows:
                    parsed = parse_yandex_url(media_row["supabase_url"])
                    if parsed and parsed[0] == source_bucket and parsed[1].startswith(prefix):
                        keys.add(parsed[1])
        elif not active_on:
            for media_row in cur.execute(f"SELECT supabase_url FROM {table} WHERE supabase_url IS NOT NULL"):
                parsed = parse_yandex_url(media_row["supabase_url"])
                if parsed and parsed[0] == source_bucket and parsed[1].startswith(prefix):
                    keys.add(parsed[1])

    add_table_urls("eventposter")
    add_table_urls("event_media_asset")
    conn.close()
    return keys


def aws_base_cmd(env: dict[str, str], endpoint: str) -> tuple[list[str], dict[str, str]]:
    aws = first_env(env, "AWS_CLI_BIN") or "aws"
    access = first_env(env, "KENIGEVENTS_SITE_YC_ACCESS_KEY_ID", "YC_SA_BOT_STORAGE", "AWS_ACCESS_KEY_ID")
    secret = first_env(env, "KENIGEVENTS_SITE_YC_SECRET_ACCESS_KEY", "YC_SA_BOT_STORAGE_KEY", "AWS_SECRET_ACCESS_KEY")
    region = first_env(env, "KENIGEVENTS_SITE_YC_REGION", "YC_STORAGE_REGION", "AWS_DEFAULT_REGION") or "ru-central1"
    if not access or not secret:
        raise RuntimeError("Missing Yandex S3 credentials: set KENIGEVENTS_SITE_YC_* or YC_SA_BOT_STORAGE*")
    proc_env = {**os.environ, "AWS_ACCESS_KEY_ID": access, "AWS_SECRET_ACCESS_KEY": secret, "AWS_DEFAULT_REGION": region}
    return [aws, "--endpoint-url", endpoint], proc_env


def aws_head(base: list[str], proc_env: dict[str, str], *, bucket: str, key: str) -> bool | None:
    result = subprocess.run(
        [*base, "s3api", "head-object", "--bucket", bucket, "--key", key],
        env=proc_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode == 0:
        return True
    err = result.stderr or ""
    if re.search(r"(404|Not Found|NotFound|NoSuchKey)", err, re.I):
        return False
    return None


def aws_copy(base: list[str], proc_env: dict[str, str], *, source_bucket: str, target_bucket: str, key: str) -> bool:
    copy_source = quote(f"{source_bucket}/{key}", safe="/")
    result = subprocess.run(
        [
            *base,
            "s3api",
            "copy-object",
            "--bucket",
            target_bucket,
            "--key",
            key,
            "--copy-source",
            copy_source,
            "--metadata-directive",
            "COPY",
        ],
        env=proc_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode == 0:
        return True
    sys.stderr.write(result.stderr[-1200:] + "\n")
    return False


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, help="SQLite DB snapshot to scan")
    parser.add_argument("--preview-json", type=Path, default=ROOT / "site/src/data/preview-events.json")
    parser.add_argument("--active-on", default="", help="Only scan events active on/after YYYY-MM-DD")
    parser.add_argument("--source-bucket", default=LEGACY_BUCKET)
    parser.add_argument("--target-bucket", default="")
    parser.add_argument("--prefix", default="p/")
    parser.add_argument("--endpoint", default="")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--report", type=Path, default=ROOT / "artifacts/codex/static-site-media-cdn-migration/report.json")
    args = parser.parse_args()

    env = merged_env()
    endpoint = (args.endpoint or first_env(env, "KENIGEVENTS_SITE_YC_ENDPOINT", "YC_STORAGE_ENDPOINT") or DEFAULT_ENDPOINT).rstrip("/")
    target_bucket = (args.target_bucket or first_env(env, "KENIGEVENTS_SITE_YC_BUCKET", "YC_STORAGE_BUCKET") or DEFAULT_TARGET_BUCKET).strip()
    prefix = args.prefix.strip().lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    keys: set[str] = set()
    if args.db:
        keys.update(collect_from_db(args.db, source_bucket=args.source_bucket, prefix=prefix, active_on=args.active_on or None))
    if args.preview_json:
        keys.update(collect_from_preview_json(args.preview_json, source_bucket=args.source_bucket, prefix=prefix))
    ordered = sorted(keys)
    if args.limit:
        ordered = ordered[: max(0, int(args.limit))]

    report = {
        "source_bucket": args.source_bucket,
        "target_bucket": target_bucket,
        "prefix": prefix,
        "active_on": args.active_on or None,
        "apply": bool(args.apply),
        "found": len(keys),
        "selected": len(ordered),
        "already_present": 0,
        "copied": 0,
        "missing_source": 0,
        "failed": 0,
        "sample_cdn_urls": [],
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)

    if not args.apply:
        report["sample_keys"] = ordered[:20]
        args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    base, proc_env = aws_base_cmd(env, endpoint)
    for idx, key in enumerate(ordered, start=1):
        exists_target = aws_head(base, proc_env, bucket=target_bucket, key=key)
        if exists_target is True:
            report["already_present"] += 1
        else:
            exists_source = aws_head(base, proc_env, bucket=args.source_bucket, key=key)
            if exists_source is False:
                report["missing_source"] += 1
                continue
            if exists_source is None:
                report["failed"] += 1
                continue
            if aws_copy(base, proc_env, source_bucket=args.source_bucket, target_bucket=target_bucket, key=key):
                report["copied"] += 1
            else:
                report["failed"] += 1
        if len(report["sample_cdn_urls"]) < 10:
            report["sample_cdn_urls"].append(f"https://static.kenigevents.ru/{key}")
        if idx % 100 == 0:
            print(f"processed={idx}/{len(ordered)} copied={report['copied']} already={report['already_present']} failed={report['failed']}", flush=True)

    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["failed"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
