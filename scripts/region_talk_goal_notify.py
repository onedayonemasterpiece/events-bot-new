#!/usr/bin/env python3
"""Local Region Talk goal notifier.

Reads Gemini-confirmed `publication_candidate_item` rows from YDB and sends
unsent links to the operator Telegram chat through the local E2E Telethon
session. This script is local-only: never run it on Kaggle and never use S22.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip(); v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def decode_e2e_bundle() -> dict[str, Any]:
    api_id = os.getenv("TG_API_ID") or os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TG_API_HASH") or os.getenv("TELEGRAM_API_HASH")
    if not api_id or not api_hash:
        raise RuntimeError("TG_API_ID/TG_API_HASH or TELEGRAM_API_ID/TELEGRAM_API_HASH are required")
    bundle_b64 = (os.getenv("TELEGRAM_AUTH_BUNDLE_E2E") or "").strip()
    if bundle_b64:
        bundle = json.loads(base64.urlsafe_b64decode(bundle_b64.encode("ascii")).decode("utf-8"))
        session = str(bundle.get("session") or "").strip()
        if not session:
            raise RuntimeError("TELEGRAM_AUTH_BUNDLE_E2E has no session")
        device = {k: bundle[k] for k in ["device_model", "system_version", "app_version", "lang_code", "system_lang_code"] if bundle.get(k)}
        return {"api_id": int(api_id), "api_hash": api_hash, "session": session, "device": device}
    session = (os.getenv("TELEGRAM_SESSION") or "").strip()
    if session:
        return {"api_id": int(api_id), "api_hash": api_hash, "session": session, "device": {}}
    raise RuntimeError("TELEGRAM_AUTH_BUNDLE_E2E or TELEGRAM_SESSION is required for local notification")


def ydb_endpoint_database() -> tuple[str, str]:
    endpoint = (os.getenv("REGION_TALK_YDB_ENDPOINT") or "").strip()
    database = (os.getenv("REGION_TALK_YDB_DATABASE") or "").strip()
    if endpoint and database:
        return endpoint.split("?")[0].rstrip("/"), database
    yc = "/home/dev/yandex-cloud/bin/yc"
    if Path(yc).exists():
        try:
            raw = subprocess.check_output([yc, "ydb", "database", "get", "events-bot-acq-discovery", "--format", "json"], text=True)
            data = json.loads(raw)
            import urllib.parse
            url = data["endpoint"]
            return url.split("?")[0].rstrip("/"), urllib.parse.parse_qs(urllib.parse.urlparse(url).query)["database"][0]
        except Exception:
            pass
    raise RuntimeError("REGION_TALK_YDB_ENDPOINT and REGION_TALK_YDB_DATABASE are required")


def ydb_token() -> str:
    token = (os.getenv("REGION_TALK_YDB_IAM_TOKEN") or os.getenv("YC_IAM_TOKEN") or os.getenv("YDB_ACCESS_TOKEN") or "").strip()
    if token:
        return token
    yc = "/home/dev/yandex-cloud/bin/yc"
    if Path(yc).exists():
        return subprocess.check_output([yc, "iam", "create-token"], text=True).strip()
    raise RuntimeError("REGION_TALK_YDB_IAM_TOKEN/YC_IAM_TOKEN is required")


def ydb_table_path(database: str) -> str:
    namespace = re.sub(r"[^A-Za-z0-9_]+", "_", (os.getenv("REGION_TALK_YDB_NAMESPACE") or "region_talk").strip() or "region_talk").strip("_") or "region_talk"
    return database.rstrip("/") + f"/{namespace}_state_kv"


def read_publication_rows(limit: int) -> tuple[Any, Any, Any, str, list[dict[str, Any]]]:
    import ydb  # type: ignore
    endpoint, database = ydb_endpoint_database()
    driver = ydb.Driver(endpoint=endpoint, database=database, credentials=ydb.AccessTokenCredentials(ydb_token()))
    driver.wait(timeout=20, fail_fast=True)
    pool = ydb.SessionPool(driver)
    table = ydb_table_path(database)
    q = f"SELECT pk, payload_json FROM `{table}` WHERE kind='publication_candidate_item' LIMIT {max(1, int(limit) * 5)};"
    rows = pool.retry_operation_sync(lambda s: s.transaction(ydb.StaleReadOnly()).execute(q, commit_tx=True))[0].rows
    out: list[dict[str, Any]] = []
    for row in rows:
        payload = row.payload_json
        item = json.loads(payload) if isinstance(payload, str) else dict(payload or {})
        item["_ydb_pk"] = str(row.pk)
        out.append(item)
    out.sort(key=lambda r: (int(r.get("publication_rank") or 999999), -float(r.get("publication_score") or 0)))
    return ydb, driver, pool, table, out


def _json_row_payload(row: Any) -> dict[str, Any]:
    payload = row.payload_json
    return json.loads(payload) if isinstance(payload, str) else dict(payload or {})


def read_kind_rows(pool: Any, ydb: Any, table: str, kind: str, limit: int) -> list[dict[str, Any]]:
    if not re.fullmatch(r"[A-Za-z0-9_:-]+", kind):
        raise ValueError(f"unsafe YDB kind: {kind!r}")
    out = []
    max_items = max(1, int(limit))
    page_size = max(1, min(200, max_items))
    after = ""
    while len(out) < max_items:
        q = (
            f"DECLARE $after AS Utf8; "
            f"SELECT pk, payload_json FROM `{table}` WHERE kind='{kind}' AND pk > $after "
            f"ORDER BY pk LIMIT {min(page_size, max_items - len(out))};"
        )
        def op(session: Any):
            query = session.prepare(q)
            return session.transaction(ydb.StaleReadOnly()).execute(query, {"$after": after}, commit_tx=True)
        rows = pool.retry_operation_sync(op)[0].rows
        if not rows:
            break
        for row in rows:
            after = str(row.pk)
            item = _json_row_payload(row)
            item["_ydb_pk"] = after
            out.append(item)
        if len(rows) < page_size:
            break
    return out


def build_stats_message(limit: int = 20000) -> str:
    import ydb  # type: ignore
    endpoint, database = ydb_endpoint_database()
    driver = ydb.Driver(endpoint=endpoint, database=database, credentials=ydb.AccessTokenCredentials(ydb_token()))
    driver.wait(timeout=20, fail_fast=True)
    pool = ydb.SessionPool(driver)
    table = ydb_table_path(database)
    try:
        source_queue = read_kind_rows(pool, ydb, table, "source_queue_item", limit)
        source_status = read_kind_rows(pool, ydb, table, "source_status_item", limit)
        by_source: dict[str, dict[str, Any]] = {}
        for row in source_queue + source_status:
            key = str(row.get("canonical_source_key") or row.get("source_queue_id") or row.get("source_id") or row.get("source_url") or row.get("_ydb_pk") or "")
            if key:
                by_source[key] = {**by_source.get(key, {}), **row}
        sources = list(by_source.values())
        posts = read_kind_rows(pool, ydb, table, "processed_post_item", limit)
        candidates = read_kind_rows(pool, ydb, table, "candidate_memory_item", limit)
        images = read_kind_rows(pool, ydb, table, "image_queue_item", limit)
        publications = read_kind_rows(pool, ydb, table, "publication_candidate_item", limit)
    finally:
        driver.stop()
    rejected_status_prefixes = ("skipped", "error", "reject", "rejected", "debug_self_loop_rejected")
    rejected_sources = [
        r for r in sources
        if str(r.get("fetch_status") or r.get("source_queue_status") or r.get("queue_status") or r.get("frontier_action") or "").startswith(rejected_status_prefixes)
        or bool(str(r.get("monitoring_exclusion_reason") or "").strip())
    ]
    ko_sources = [r for r in sources if int(float(r.get("ko_posts_found") or 0)) > 0]
    actual_images = [r for r in images if str(r.get("image_model_input_type") or "") == "actual_image" or str(r.get("image_queue_status") or "") == "actual_scored"]
    strong_images = [r for r in actual_images if float(r.get("overall_media_score") or r.get("final_visual_score") or 0) >= 0.66]
    confirmed = [r for r in publications if str(r.get("publication_candidate_status") or "") in {"llm_confirmed", "sent_to_chat", "accepted_for_publication"}]
    ready_to_send = [r for r in publications if str(r.get("publication_candidate_status") or "") == "llm_confirmed" and str(r.get("sent_to_chat") or "").lower() != "true"]
    return "\n".join([
        "📊 Region Talk live YDB stats",
        f"Каналов/пабликов в базе: {len(sources)}",
        f"Каналов отброшено/скрыто/ошибка: {len(rejected_sources)}",
        f"Каналов с постами о Калининградской области: {len(ko_sources)}",
        f"Постов-кандидатов про Калининградскую область: {len(candidates)}",
        f"Постов compact processed: {len(posts)}",
        f"Картинок actual-scored: {len(actual_images)}",
        f"Сильных картинок: {len(strong_images)}",
        f"Gemini-confirmed publication candidates: {len(confirmed)}",
        f"Готово к отправке ссылок: {len(ready_to_send)}",
        f"updated_at: {datetime.now(timezone.utc).isoformat()}",
    ])


def upsert_sent(pool: Any, ydb: Any, table: str, row: dict[str, Any], message_id: int) -> None:
    now = datetime.now(timezone.utc).isoformat()
    item = dict(row)
    pk = str(item.pop("_ydb_pk", "")) or "publication_candidate_item:" + str(item.get("publication_candidate_id") or item.get("post_url"))
    item.update({"sent_to_chat": "true", "sent_message_id": str(message_id), "sent_at": now, "publication_candidate_status": "sent_to_chat"})
    query = f"DECLARE $pk AS Utf8; DECLARE $payload_json AS Json; DECLARE $updated_at AS Utf8; UPSERT INTO `{table}` (pk, kind, payload_json, updated_at) VALUES ($pk, 'publication_candidate_item', $payload_json, $updated_at);"
    def op(session: Any) -> None:
        session.transaction(ydb.SerializableReadWrite()).execute(query, {"$pk": pk, "$payload_json": json.dumps(item, ensure_ascii=False), "$updated_at": now}, commit_tx=True)
    pool.retry_operation_sync(op)


def candidate_message(row: dict[str, Any]) -> str:
    rank = row.get("publication_rank") or "?"
    url = row.get("post_url") or ""
    why = row.get("why_selected") or "выбран по тексту, визуальному score и Gemini-проверке"
    summary = row.get("short_summary") or ""
    reason = str(row.get("publication_llm_reason") or "")[:280]
    return "\n".join([
        f"✅ Region Talk candidate #{rank}",
        str(url),
        f"Почему: {why}",
        f"Кратко: {summary}" if summary else "",
        f"Gemini: {reason}" if reason else "",
    ]).strip()


async def resolve_peer(client: Any, target: str) -> Any:
    raw = (target or "").strip()
    invite = re.search(r"t\.me/(?:\+|joinchat/)([A-Za-z0-9_-]+)", raw)
    if invite:
        from telethon import functions, errors  # type: ignore
        code = invite.group(1)
        try:
            result = await client(functions.messages.ImportChatInviteRequest(code))
            chats = getattr(result, "chats", None) or []
            if chats:
                return chats[0]
        except errors.UserAlreadyParticipantError:
            checked = await client(functions.messages.CheckChatInviteRequest(code))
            chat = getattr(checked, "chat", None)
            if chat is not None:
                return chat
        except Exception:
            checked = await client(functions.messages.CheckChatInviteRequest(code))
            chat = getattr(checked, "chat", None)
            if chat is not None:
                return chat
    return await client.get_entity(raw)


async def send_rows(args: argparse.Namespace) -> dict[str, Any]:
    from telethon import TelegramClient  # type: ignore
    from telethon.sessions import StringSession  # type: ignore

    auth = decode_e2e_bundle()
    ydb = driver = pool = table = None
    rows: list[dict[str, Any]] = []
    if args.stats:
        messages = [build_stats_message(args.stats_limit)]
    elif args.message:
        messages = [args.message]
    else:
        ydb, driver, pool, table, rows = read_publication_rows(args.limit)
        rows = [r for r in rows if str(r.get("publication_candidate_status") or "") == "llm_confirmed" and str(r.get("sent_to_chat") or "").lower() != "true"][: args.limit]
        messages = [candidate_message(r) for r in rows]
    client = TelegramClient(StringSession(auth["session"]), auth["api_id"], auth["api_hash"], **auth.get("device", {}))
    await client.connect()
    sent: list[dict[str, Any]] = []
    try:
        if not await client.is_user_authorized():
            raise RuntimeError("local E2E Telegram session is not authorized")
        peer = await resolve_peer(client, args.chat)
        for idx, text in enumerate(messages):
            if args.dry_run:
                sent.append({"dry_run": True, "text": text[:120], "post_url": rows[idx].get("post_url") if idx < len(rows) else ""})
                continue
            msg = await client.send_message(peer, text, link_preview=True)
            mid = int(getattr(msg, "id", 0) or 0)
            if idx < len(rows) and ydb is not None and pool is not None and table is not None:
                upsert_sent(pool, ydb, table, rows[idx], mid)
            sent.append({"message_id": mid, "post_url": rows[idx].get("post_url") if idx < len(rows) else ""})
    finally:
        await client.disconnect()
        if driver is not None:
            driver.stop()
    return {"ok": True, "sent": sent, "sent_count": len(sent), "dry_run": bool(args.dry_run)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", type=Path, default=Path(".env"))
    ap.add_argument("--chat", default=os.getenv("REGION_TALK_NOTIFY_CHAT") or "https://t.me/+kfaIRh98oHVkYWFi")
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--message", default="", help="Send a single status message instead of YDB publication candidates")
    ap.add_argument("--stats", action="store_true", help="Send live Region Talk YDB statistics instead of candidate links")
    ap.add_argument("--stats-limit", type=int, default=20000)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    load_env(args.env_file)
    result = asyncio.run(send_rows(args))
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
