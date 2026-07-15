#!/usr/bin/env python3
"""Resolve VK photo CDN URLs for Region Talk image-queue rows before Kaggle.

The future server-side orchestrator runs this where the production VK user token
is valid. Local debugging may use the existing Fly app as a read-only token
proxy; tokens never leave that machine and only public CDN URLs are persisted.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.region_talk_goal_notify import (
    ensure_ydb_module,
    load_env,
    read_kind_rows,
    ydb_credentials,
    ydb_endpoint_database,
    ydb_table_path,
)


def parse_vk_post(url: str) -> tuple[int, int] | None:
    match = re.search(r"vk\.com/wall(-?\d+)_(\d+)", str(url or ""), re.I)
    return (int(match.group(1)), int(match.group(2))) if match else None


def photo_urls_from_post(post: dict[str, Any]) -> list[str]:
    photos: list[str] = []
    for attachment in post.get("attachments") or []:
        if attachment.get("type") != "photo" or not attachment.get("photo"):
            continue
        sizes = attachment["photo"].get("sizes") or []
        best = max(sizes, key=lambda item: int(item.get("width") or 0) * int(item.get("height") or 0), default={})
        url = str(best.get("url") or "")
        if url.startswith("https://"):
            photos.append(url)
    return photos


def vk_read_token_names() -> list[str]:
    service_first = str(os.getenv("REGION_TALK_VK_READ_SERVICE_FIRST") or "1").strip().lower() in {"1", "true", "yes", "on"}
    service = ["VK_SERVICE_TOKEN", "VK_SERVICE_KEY", "VK_TOKEN"]
    user = ["VK_USER_TOKEN", "VK_ACCESS_TOKEN4", "VK_ACCESS_TOKEN5", "VK_ACCESS_TOKEN"]
    return service + user if service_first else user + service


def local_vk_posts(post_ids: list[str]) -> tuple[dict[str, dict[str, Any]], str]:
    token = next((os.getenv(name) for name in vk_read_token_names() if os.getenv(name)), "")
    if not token:
        return {}, "token_unavailable"
    try:
        response = requests.get(
            "https://api.vk.com/method/wall.getById",
            params={"posts": ",".join(post_ids), "access_token": token, "v": "5.199"},
            timeout=25,
        )
        data = response.json()
    except Exception as exc:
        return {}, f"{type(exc).__name__}: {str(exc)[:200]}"
    items = data.get("response")
    if isinstance(items, dict):
        items = items.get("items") or []
    if not isinstance(items, list):
        return {}, str(data.get("error") or "invalid_vk_response")[:300]
    return {f"{item.get('owner_id')}_{item.get('id')}": item for item in items}, ""


def _release_env() -> dict[str, str]:
    env = dict(os.environ)
    path = Path("/home/dev/.config/fly/release.env")
    if not path.exists():
        return env
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env.setdefault(key.strip().removeprefix("export ").strip(), value.strip().strip('"').strip("'"))
    return env


def fly_vk_posts(post_ids: list[str], app: str) -> tuple[dict[str, dict[str, Any]], str]:
    flyctl = str(Path.home() / ".fly" / "bin" / "flyctl")
    if not Path(flyctl).exists():
        return {}, "flyctl_unavailable"
    code = """
import os,json,urllib.parse,urllib.request
ids=POST_IDS
names=['VK_SERVICE_TOKEN','VK_SERVICE_KEY','VK_TOKEN','VK_USER_TOKEN','VK_ACCESS_TOKEN4','VK_ACCESS_TOKEN5','VK_ACCESS_TOKEN']
token=next((os.getenv(name) for name in names if os.getenv(name)),None)
if not token: raise SystemExit('VK_TOKEN_UNAVAILABLE')
q=urllib.parse.urlencode({'posts':','.join(ids),'access_token':token,'v':'5.199'})
with urllib.request.urlopen('https://api.vk.com/method/wall.getById?'+q,timeout=25) as response: data=json.load(response)
items=data.get('response',{}); items=items.get('items',items) if isinstance(items,dict) else items
print(json.dumps({'items':items or []},ensure_ascii=False))
""".replace("POST_IDS", repr(post_ids))
    encoded = base64.b64encode(code.encode("utf-8")).decode("ascii")
    command = [flyctl, "ssh", "console", "-a", app, "-C", f"python -c \"import base64;exec(base64.b64decode('{encoded}'))\""]
    try:
        proc = subprocess.run(command, env=_release_env(), text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=90)
    except Exception as exc:
        return {}, f"{type(exc).__name__}: {str(exc)[:200]}"
    if proc.returncode != 0:
        return {}, (proc.stderr or proc.stdout or "fly_vk_proxy_failed")[-300:]
    lines = [line for line in proc.stdout.splitlines() if line.lstrip().startswith("{")]
    if not lines:
        return {}, "fly_vk_proxy_empty"
    try:
        items = json.loads(lines[-1]).get("items") or []
    except Exception as exc:
        return {}, f"fly_vk_proxy_json_{type(exc).__name__}"
    return {f"{item.get('owner_id')}_{item.get('id')}": item for item in items}, ""


def write_rows(pool: Any, ydb: Any, table: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    now = datetime.now(timezone.utc).isoformat()
    query_text = f"""
DECLARE $pk AS Utf8; DECLARE $kind AS Utf8; DECLARE $payload_json AS Json; DECLARE $updated_at AS Utf8;
UPSERT INTO `{table}` (pk, kind, payload_json, updated_at) VALUES ($pk, $kind, $payload_json, $updated_at);
"""
    def op(session: Any) -> int:
        query = session.prepare(query_text)
        tx = session.transaction(ydb.SerializableReadWrite())
        for row in rows:
            pk = str(row.get("_ydb_pk") or "") or "image_queue_item:" + str(row.get("image_queue_id") or "")
            payload = {key: value for key, value in row.items() if not str(key).startswith("_")}
            tx.execute(query, {"$pk": pk, "$kind": "image_queue_item", "$payload_json": json.dumps(payload, ensure_ascii=False), "$updated_at": now}, commit_tx=False)
        tx.commit()
        return len(rows)
    return int(pool.retry_operation_sync(op) or 0)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--max-items", type=int, default=10)
    parser.add_argument("--fly-app", default=os.getenv("REGION_TALK_VK_MEDIA_PREFETCH_FLY_APP") or "events-bot-new-wngqia")
    parser.add_argument("--allow-fly-fallback", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    load_env(args.env_file)
    ydb = ensure_ydb_module()
    endpoint, database = ydb_endpoint_database()
    driver = ydb.Driver(endpoint=endpoint, database=database, credentials=ydb_credentials(ydb))
    driver.wait(timeout=20, fail_fast=True)
    pool = ydb.SessionPool(driver)
    table = ydb_table_path(database)
    rows = read_kind_rows(pool, ydb, table, "image_queue_item", 20000)
    pending = []
    for row in rows:
        parsed = parse_vk_post(str(row.get("post_url") or ""))
        direct = str(row.get("image_url_or_local_path") or row.get("primary_media_path") or "")
        if parsed and not direct.startswith("http") and str(row.get("image_queue_status") or "") in {"needs_actual_image_fetch", "image_analysis_in_progress"}:
            pending.append(row)
    pending = pending[: max(0, int(args.max_items))]
    post_ids = [f"{parse_vk_post(str(row.get('post_url')))[0]}_{parse_vk_post(str(row.get('post_url')))[1]}" for row in pending]
    posts, error = local_vk_posts(post_ids) if post_ids else ({}, "")
    source = "local_vk_api"
    if post_ids and not posts and args.allow_fly_fallback:
        posts, fly_error = fly_vk_posts(post_ids, args.fly_app)
        error = fly_error or error
        source = "fly_vk_api_proxy"
    now = datetime.now(timezone.utc).isoformat()
    updates = []
    for row, post_id in zip(pending, post_ids):
        photos = photo_urls_from_post(posts.get(post_id, {}))
        if not photos:
            continue
        updates.append({
            **row,
            "image_url_or_local_path": photos[0],
            "vk_media_photo_urls": photos,
            "media_count": len(photos),
            "vk_media_prefetch_status": "ready",
            "vk_media_prefetch_source": source,
            "vk_media_prefetch_at": now,
            "media_fetch_status": "vk_public_url_ready",
            "media_acquisition_status": "vk_public_url_ready",
            "media_fetch_error": "",
            "next_action": "score_prefetched_vk_actual_image",
        })
    written = 0 if args.dry_run else write_rows(pool, ydb, table, updates)
    driver.stop()
    print(json.dumps({
        "ok": True,
        "pending_vk_without_url": len(pending),
        "resolved": len(updates),
        "written": written,
        "source": source if updates else "",
        "error": error if not updates and pending else "",
        "post_urls": [row.get("post_url") for row in updates],
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
