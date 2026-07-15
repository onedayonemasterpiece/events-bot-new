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
import hashlib
import json
import os
import random
import re
import fcntl
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PUBLICATION_ELIGIBILITY_GATE_VERSION = "region_talk_publication_eligibility_v5"
AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION = "region_talk_source_fingerprint_v3"
DEFAULT_NOTIFY_CHAT = "https://t.me/+kfaIRh98oHVkYWFi"
DEFAULT_NOTIFY_CHAT_ID = "-5563945596"
DEFAULT_PUBLICATION_SCAN_LIMIT = 5000


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


def getenv_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return default


def ydb_database_name() -> str:
    return (os.getenv("REGION_TALK_YDB_DATABASE_NAME") or "events-bot-acq-discovery").strip() or "events-bot-acq-discovery"


def ydb_endpoint_database(*, allow_yc_fallback: bool = True) -> tuple[str, str]:
    endpoint = (os.getenv("REGION_TALK_YDB_ENDPOINT") or "").strip()
    database = (os.getenv("REGION_TALK_YDB_DATABASE") or "").strip()
    if endpoint and database:
        return endpoint.split("?")[0].rstrip("/"), database
    yc = "/home/dev/yandex-cloud/bin/yc"
    if allow_yc_fallback and Path(yc).exists():
        try:
            raw = subprocess.check_output([yc, "ydb", "database", "get", ydb_database_name(), "--format", "json"], text=True, stderr=subprocess.DEVNULL)
            data = json.loads(raw)
            import urllib.parse
            url = data["endpoint"]
            return url.split("?")[0].rstrip("/"), urllib.parse.parse_qs(urllib.parse.urlparse(url).query)["database"][0]
        except Exception:
            pass
    raise RuntimeError("REGION_TALK_YDB_ENDPOINT and REGION_TALK_YDB_DATABASE are required")


def ydb_service_account_key_json() -> str:
    return (os.getenv("REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON") or "").strip()


def ydb_access_token() -> str:
    return (os.getenv("REGION_TALK_YDB_IAM_TOKEN") or os.getenv("YC_IAM_TOKEN") or os.getenv("YDB_ACCESS_TOKEN") or "").strip()


def ydb_token(*, allow_yc_fallback: bool = True) -> str:
    token = ydb_access_token()
    if token:
        return token
    yc = "/home/dev/yandex-cloud/bin/yc"
    if allow_yc_fallback and Path(yc).exists():
        return subprocess.check_output([yc, "iam", "create-token"], text=True, stderr=subprocess.DEVNULL).strip()
    raise RuntimeError("REGION_TALK_YDB_IAM_TOKEN/YC_IAM_TOKEN/YDB_ACCESS_TOKEN is required")


def ydb_credentials(ydb: Any, *, allow_yc_fallback: bool = True) -> Any:
    token = ydb_access_token()
    if token:
        return ydb.AccessTokenCredentials(token)
    key_json = ydb_service_account_key_json()
    if key_json:
        import tempfile
        import ydb.iam  # type: ignore
        fd, path = tempfile.mkstemp(prefix="region-talk-local-ydb-sa-", suffix=".json")
        os.close(fd)
        try:
            Path(path).write_text(key_json, encoding="utf-8")
            return ydb.iam.ServiceAccountCredentials.from_file(path)
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass
    if os.getenv("YDB_USER"):
        return ydb.StaticCredentials.from_user_password(os.getenv("YDB_USER"), os.getenv("YDB_PASSWORD", ""))
    if allow_yc_fallback:
        return ydb.AccessTokenCredentials(ydb_token(allow_yc_fallback=True))
    return ydb.credentials_from_env_variables()


def ydb_has_direct_credential() -> bool:
    return bool(ydb_access_token() or ydb_service_account_key_json() or os.getenv("YDB_USER"))


def ensure_ydb_module() -> Any:
    try:
        import ydb  # type: ignore
        return ydb
    except Exception as import_exc:
        if getenv_bool("REGION_TALK_AUTO_INSTALL", True):
            proc = subprocess.run(
                [sys.executable, "-m", "pip", "install", "-q", "ydb[yc]"],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            if proc.returncode != 0:
                tail = (proc.stdout or "").strip().splitlines()[-8:]
                raise RuntimeError(
                    "Python package ydb is missing and auto-install failed; "
                    "run the local orchestrator/notifier from a virtualenv with `pip install ydb[yc]`, "
                    "or set REGION_TALK_AUTO_INSTALL=0 after installing dependencies. "
                    "pip_tail=" + " | ".join(tail)[:700]
                ) from import_exc
            import ydb  # type: ignore
            return ydb
        raise


def ydb_table_path(database: str) -> str:
    namespace = re.sub(r"[^A-Za-z0-9_]+", "_", (os.getenv("REGION_TALK_YDB_NAMESPACE") or "region_talk_compact").strip() or "region_talk_compact").strip("_") or "region_talk_compact"
    return database.rstrip("/") + f"/{namespace}_state_kv"


def canonical_source_key_for_row(row: dict[str, Any]) -> str:
    key = str(row.get("canonical_source_key") or "").strip().lower().rstrip("/")
    for prefix in ("source_queue_item:", "source_status_item:", "online_source_item:"):
        if key.startswith(prefix):
            key = key[len(prefix):]
    if key:
        return key
    raw = str(row.get("source_url") or row.get("canonical_url") or row.get("post_url") or "").strip().lower()
    match = re.search(r"(?:https?://)?t\.me/(?:s/)?@?([^/?#]+)", raw)
    return "telegram:" + match.group(1).rstrip("/") if match else raw.rstrip("/")


def authoritative_source_fingerprint(source: dict[str, Any] | None) -> str:
    if not isinstance(source, dict) or not source:
        return ""
    payload = {
        "version": AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION,
        "canonical_source_key": canonical_source_key_for_row(source),
        "source_queue_status": source.get("source_queue_status") or "",
        "source_scope": source.get("source_scope") or "",
        "source_geo_class": source.get("source_geo_class") or "",
        "source_topic_class": source.get("source_topic_class") or "",
        "source_quick_class": source.get("source_quick_class") or "",
        "monitoring_exclusion_reason": source.get("monitoring_exclusion_reason") or "",
        "source_surface_filter_version": source.get("source_surface_filter_version") or "",
        "source_surface_filter_reason": source.get("source_surface_filter_reason") or "",
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def attach_live_source_fingerprints(publications: list[dict[str, Any]], source_rows: list[dict[str, Any]]) -> None:
    sources: dict[str, dict[str, Any]] = {}
    for source in source_rows:
        key = canonical_source_key_for_row(source)
        if not key:
            continue
        current = dict(sources.get(key) or {})
        for field, value in source.items():
            if value in (None, ""):
                continue
            if field in {"posts_scanned", "ko_posts_found", "candidate_posts_found"}:
                try:
                    current[field] = max(int(float(current.get(field) or 0)), int(float(value or 0)))
                except (TypeError, ValueError):
                    current[field] = value
                continue
            if field == "source_queue_status":
                existing = str(current.get(field) or "")
                incoming = str(value or "")
                if existing.startswith("rejected_") and not incoming.startswith("rejected_"):
                    continue
            if field in {"source_scope", "source_geo_class", "source_quick_class"}:
                local_values = {"local_region", "kaliningrad_local", "local_region_source"}
                if str(current.get(field) or "") in local_values and str(value or "") not in local_values:
                    continue
            current[field] = value
        sources[key] = current
    for row in publications:
        source = sources.get(canonical_source_key_for_row(row))
        row["_live_authoritative_source_fingerprint"] = authoritative_source_fingerprint(source)
        row["_live_authoritative_source_found"] = str(bool(source)).lower()


def publication_scan_limit(send_limit: int) -> int:
    """Scan the ledger before applying the much smaller delivery batch limit.

    Publication rows are ordered by YDB primary key, not by readiness or
    recency. Reading only ``send_limit * 5`` rows can hide a confirmed unsent
    candidate behind older tombstones after the ledger grows.
    """
    configured = int(os.getenv("REGION_TALK_NOTIFY_PUBLICATION_SCAN_LIMIT") or DEFAULT_PUBLICATION_SCAN_LIMIT)
    return max(DEFAULT_PUBLICATION_SCAN_LIMIT, configured, max(1, int(send_limit)) * 5)


def read_publication_rows(limit: int) -> tuple[Any, Any, Any, str, list[dict[str, Any]]]:
    ydb = ensure_ydb_module()
    endpoint, database = ydb_endpoint_database()
    driver = ydb.Driver(endpoint=endpoint, database=database, credentials=ydb_credentials(ydb))
    driver.wait(timeout=20, fail_fast=True)
    pool = ydb.SessionPool(driver)
    table = ydb_table_path(database)
    out = read_kind_rows(pool, ydb, table, "publication_candidate_item", publication_scan_limit(limit))
    source_limit = max(5000, int(os.getenv("REGION_TALK_NOTIFY_SOURCE_SCAN_LIMIT") or "20000"))
    source_rows = read_kind_rows(pool, ydb, table, "source_queue_item", source_limit)
    source_rows += read_kind_rows(pool, ydb, table, "source_status_item", source_limit)
    source_rows += read_kind_rows(pool, ydb, table, "online_source_item", source_limit)
    attach_live_source_fingerprints(out, source_rows)
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
    try:
        configured_page = int(os.getenv("REGION_TALK_YDB_SELECT_PAGE_SIZE", "200") or "200")
    except Exception:
        configured_page = 200
    page_size = max(1, min(500, configured_page, max_items))
    prefix = kind + ":"
    prefix_upper = kind + ";"
    after = prefix
    while len(out) < max_items:
        q = (
            f"DECLARE $prefix AS Utf8; DECLARE $prefix_upper AS Utf8; DECLARE $after AS Utf8; "
            f"SELECT pk, payload_json FROM `{table}` "
            f"WHERE pk >= $prefix AND pk < $prefix_upper AND pk > $after "
            f"ORDER BY pk LIMIT {min(page_size, max_items - len(out))};"
        )
        def op(session: Any):
            query = session.prepare(q)
            return session.transaction(ydb.StaleReadOnly()).execute(
                query,
                {"$prefix": prefix, "$prefix_upper": prefix_upper, "$after": after},
                commit_tx=True,
            )
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


def is_confirmed_publication(row: dict[str, Any]) -> bool:
    """Backward-compatible live-YDB confirmed marker.

    Older finalizer rows used `publication_status=gemini_accept`; newer rows use
    the more explicit `publication_candidate_status=llm_confirmed|sent_to_chat`.
    Treat both as confirmed so notification stats and sending do not depend on
    an operator XLSX/report-tail rewrite.
    """
    if str(row.get("publication_tombstone") or "").lower() == "true" or str(row.get("publication_revoked") or "").lower() == "true":
        return False
    if str(row.get("publication_eligibility_verdict") or "").lower() != "eligible":
        return False
    if str(row.get("publication_eligibility_gate_version") or "") != PUBLICATION_ELIGIBILITY_GATE_VERSION:
        return False
    if str(row.get("authoritative_source_fingerprint_version") or "") != AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION:
        return False
    stored_fingerprint = str(row.get("authoritative_source_fingerprint") or "")
    live_fingerprint = str(row.get("_live_authoritative_source_fingerprint") or "")
    if not stored_fingerprint or not live_fingerprint or stored_fingerprint != live_fingerprint:
        return False
    candidate_status = str(row.get("publication_candidate_status") or "")
    publication_status = str(row.get("publication_status") or "")
    return candidate_status in {"llm_confirmed", "sent_to_chat", "accepted_for_publication"} or publication_status == "gemini_accept"


def is_unsent_confirmed_publication(row: dict[str, Any]) -> bool:
    if not is_confirmed_publication(row):
        return False
    if str(row.get("publication_candidate_status") or "") == "sent_to_chat":
        return False
    return str(row.get("sent_to_chat") or "").lower() != "true"


def build_stats_message(limit: int = 20000) -> str:
    ydb = ensure_ydb_module()
    endpoint, database = ydb_endpoint_database()
    driver = ydb.Driver(endpoint=endpoint, database=database, credentials=ydb_credentials(ydb))
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
        source_candidates = read_kind_rows(pool, ydb, table, "source_candidate_item", limit)
        source_edges = read_kind_rows(pool, ydb, table, "source_edge_item", limit)
        comment_links = read_kind_rows(pool, ydb, table, "comment_link_item", limit)
        posts = read_kind_rows(pool, ydb, table, "processed_post_item", limit)
        candidates = read_kind_rows(pool, ydb, table, "candidate_memory_item", limit)
        images = read_kind_rows(pool, ydb, table, "image_queue_item", limit)
        publications = read_kind_rows(pool, ydb, table, "publication_candidate_item", limit)
        cursors = read_kind_rows(pool, ydb, table, "queue_cursor", 200)
    finally:
        driver.stop()
    rejected_status_prefixes = ("skipped", "error", "reject", "rejected", "debug_self_loop_rejected")
    rejected_sources = [
        r for r in sources
        if str(r.get("fetch_status") or r.get("source_queue_status") or r.get("queue_status") or r.get("frontier_action") or "").startswith(rejected_status_prefixes)
        or bool(str(r.get("monitoring_exclusion_reason") or "").strip())
    ]
    ko_sources = [r for r in sources if int(float(r.get("ko_posts_found") or 0)) > 0]
    attach_live_source_fingerprints(publications, sources)
    actual_images = [r for r in images if str(r.get("image_model_input_type") or "") == "actual_image" or str(r.get("image_queue_status") or "") == "actual_scored"]
    strong_images = [r for r in actual_images if float(r.get("overall_media_score") or r.get("final_visual_score") or 0) >= 0.66]
    confirmed = [r for r in publications if is_confirmed_publication(r)]
    ready_to_send = [r for r in publications if is_unsent_confirmed_publication(r)]
    cursor_by_name: dict[str, dict[str, Any]] = {}
    for row in cursors:
        name = str(row.get("queue_name") or row.get("_ydb_pk") or "").replace("queue_cursor:", "")
        if name and ":" not in name:
            cursor_by_name[name] = row
    cursor_lines = []
    for name in ["source_scan", "unified_source_queue", "source", "image_candidate_queue", "image", "image_diagnostic"]:
        row = cursor_by_name.get(name)
        if not row:
            continue
        pos = row.get("cursor_position") or row.get("done") or 0
        total = row.get("total") or ""
        label = row.get("progress_label") or f"{name}: {pos}" + (f"/{total}" if total else "")
        cursor_lines.append(f"Курсор {name}: {label}")
    return "\n".join([
        "📊 Region Talk live YDB stats",
        f"Каналов/пабликов в базе: {len(sources)}",
        f"Дискавери-кандидатов пабликов: {len(source_candidates)}",
        f"Граф discovery-связей: {len(source_edges)}",
        f"Comment-link discovery rows: {len(comment_links)}",
        f"Каналов отброшено/скрыто/ошибка: {len(rejected_sources)}",
        f"Каналов с постами о Калининградской области: {len(ko_sources)}",
        f"Постов-кандидатов про Калининградскую область: {len(candidates)}",
        f"Постов compact processed: {len(posts)}",
        f"Картинок actual-scored: {len(actual_images)}",
        f"Сильных картинок: {len(strong_images)}",
        f"Gemini-confirmed publication candidates: {len(confirmed)}",
        f"Готово к отправке ссылок: {len(ready_to_send)}",
        *cursor_lines,
        f"updated_at: {datetime.now(timezone.utc).isoformat()}",
    ])


def canonical_post_url(row: dict[str, Any]) -> str:
    raw = str(row.get("post_url") or "").strip().lower().rstrip("/")
    raw = re.sub(r"^https?://(?:www\.)?(?:telegram\.me|t\.me)/s/", "https://t.me/", raw)
    raw = re.sub(r"^https?://(?:www\.)?telegram\.me/", "https://t.me/", raw)
    return raw.split("?", 1)[0].split("#", 1)[0].rstrip("/")


def delivery_random_id(delivery_key: str) -> int:
    value = int.from_bytes(hashlib.sha256(delivery_key.encode("utf-8")).digest()[:8], "big")
    if value >= 2**63:
        value -= 2**64
    return value or 1


def publication_delivery_key(row: dict[str, Any], chat_id: str) -> str:
    return hashlib.sha256(f"{chat_id}|{canonical_post_url(row)}".encode("utf-8")).hexdigest()


def read_delivery(pool: Any, ydb: Any, table: str, delivery_key: str) -> dict[str, Any]:
    pk = "publication_delivery_item:" + delivery_key
    query_text = f"DECLARE $pk AS Utf8; SELECT payload_json FROM `{table}` WHERE pk = $pk;"
    def op(session: Any) -> dict[str, Any]:
        query = session.prepare(query_text)
        result = session.transaction(ydb.StaleReadOnly()).execute(query, {"$pk": pk}, commit_tx=True)
        rows = result[0].rows if result else []
        if not rows:
            return {}
        value = rows[0].payload_json
        return json.loads(value) if isinstance(value, str) else dict(value or {})
    return dict(pool.retry_operation_sync(op) or {})


def upsert_delivery(pool: Any, ydb: Any, table: str, delivery_key: str, payload: dict[str, Any]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    item = {**payload, "delivery_key": delivery_key, "updated_at": now}
    query_text = f"""
DECLARE $pk AS Utf8; DECLARE $kind AS Utf8; DECLARE $payload_json AS Json; DECLARE $updated_at AS Utf8;
UPSERT INTO `{table}` (pk, kind, payload_json, updated_at) VALUES ($pk, $kind, $payload_json, $updated_at);
"""
    def op(session: Any) -> None:
        query = session.prepare(query_text)
        session.transaction(ydb.SerializableReadWrite()).execute(
            query,
            {"$pk": "publication_delivery_item:" + delivery_key, "$kind": "publication_delivery_item", "$payload_json": json.dumps(item, ensure_ascii=False), "$updated_at": now},
            commit_tx=True,
        )
    pool.retry_operation_sync(op)


def upsert_sent(
    pool: Any,
    ydb: Any,
    table: str,
    row: dict[str, Any],
    message_id: int,
    *,
    chat_id: str = "",
    delivery_key: str = "",
    random_id: int = 0,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    item = dict(row)
    pk = str(item.pop("_ydb_pk", "")) or "publication_candidate_item:" + str(item.get("publication_candidate_id") or item.get("post_url"))
    item.update({
        "sent_to_chat": "true",
        "sent_message_id": str(message_id),
        "sent_at": now,
        "sent_chat_id": chat_id,
        "delivery_key": delivery_key,
        "delivery_random_id": str(random_id or ""),
        "publication_candidate_status": "sent_to_chat",
    })
    query_text = f"""
DECLARE $pk AS Utf8;
DECLARE $kind AS Utf8;
DECLARE $payload_json AS Json;
DECLARE $updated_at AS Utf8;
UPSERT INTO `{table}` (pk, kind, payload_json, updated_at)
VALUES ($pk, $kind, $payload_json, $updated_at);
"""
    def op(session: Any) -> None:
        query = session.prepare(query_text)
        session.transaction(ydb.SerializableReadWrite()).execute(
            query,
            {"$pk": pk, "$kind": "publication_candidate_item", "$payload_json": json.dumps(item, ensure_ascii=False), "$updated_at": now},
            commit_tx=True,
        )
    pool.retry_operation_sync(op)


def candidate_message(row: dict[str, Any]) -> str:
    rank = row.get("publication_rank") or "?"
    url = row.get("post_url") or ""
    video_manual = str(row.get("media_review_mode") or "") == "operator_video_review" or str(row.get("media_kind") or "") == "video"
    why = row.get("why_selected") or (
        "текст прошёл строгую E5+BGE и Gemini-проверку; качество видео нужно оценить вручную"
        if video_manual
        else "выбран по тексту, визуальному score и Gemini-проверке"
    )
    summary = row.get("short_summary") or ""
    reason = str(row.get("publication_llm_reason") or row.get("llm_reason") or row.get("final_verifier_reason") or "")[:280]
    onboarding = (
        str(row.get("source_onboarding_paragraph") or "").strip()
        if str(row.get("source_onboarding_status") or "") == "ready"
        else ""
    )
    return "\n".join([
        f"✅ Region Talk candidate #{rank}",
        str(url),
        f"Почему: {why}",
        f"О блогере: {onboarding}" if onboarding else "",
        "🎬 Видео: требуется ручной просмотр" if video_manual else "",
        f"Кратко: {summary}" if summary else "",
        f"Gemini: {reason}" if reason else "",
    ]).strip()


async def resolve_peer(client: Any, target: str, *, allow_join_chat: bool = False) -> Any:
    raw = (target or "").strip()
    invite = re.search(r"t\.me/(?:\+|joinchat/)([A-Za-z0-9_-]+)", raw)
    if invite:
        from telethon import functions, errors  # type: ignore
        code = invite.group(1)
        checked = await client(functions.messages.CheckChatInviteRequest(code))
        chat = getattr(checked, "chat", None)
        if chat is not None:
            return chat
        if not allow_join_chat:
            raise RuntimeError("E2E delivery account is not a member of the prepared Region Talk chat; pass --allow-join-chat once after operator approval")
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
    return await client.get_entity(raw)


def _message_id_from_updates(result: Any, random_id: int) -> int:
    for update in getattr(result, "updates", None) or []:
        if int(getattr(update, "random_id", 0) or 0) == int(random_id):
            mid = int(getattr(update, "id", 0) or 0)
            if mid:
                return mid
    for update in getattr(result, "updates", None) or []:
        message = getattr(update, "message", None)
        mid = int(getattr(message, "id", 0) or 0)
        if mid:
            return mid
    return 0


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
        deduped: dict[str, dict[str, Any]] = {}
        for row in rows:
            if not is_unsent_confirmed_publication(row):
                continue
            key = canonical_post_url(row)
            if key and key not in deduped:
                deduped[key] = row
        rows = list(deduped.values())[: args.limit]
        messages = [candidate_message(r) for r in rows]
    client = TelegramClient(StringSession(auth["session"]), auth["api_id"], auth["api_hash"], **auth.get("device", {}))
    await client.connect()
    sent: list[dict[str, Any]] = []
    chat_id = ""
    actual_account_id = ""
    try:
        if not await client.is_user_authorized():
            raise RuntimeError("local E2E Telegram session is not authorized")
        me = await client.get_me()
        expected_account_id = str(os.getenv("REGION_TALK_NOTIFY_ACCOUNT_ID") or "").strip()
        actual_account_id = str(getattr(me, "id", "") or "")
        if expected_account_id and actual_account_id != expected_account_id:
            raise RuntimeError(f"E2E delivery account id {actual_account_id} does not match expected {expected_account_id}")
        peer = await resolve_peer(client, args.chat, allow_join_chat=bool(args.allow_join_chat))
        from telethon import functions, utils  # type: ignore
        input_peer = await client.get_input_entity(peer)
        chat_id = str(utils.get_peer_id(peer))
        expected_chat_id = str(args.expected_chat_id or "").strip()
        if expected_chat_id and chat_id != expected_chat_id:
            raise RuntimeError(f"resolved Region Talk chat id {chat_id} does not match expected {expected_chat_id}")
        for idx, text in enumerate(messages):
            if args.dry_run:
                sent.append({"dry_run": True, "text": text[:120], "post_url": rows[idx].get("post_url") if idx < len(rows) else ""})
                continue
            if idx < len(rows) and ydb is not None and pool is not None and table is not None:
                row = rows[idx]
                delivery_key = publication_delivery_key(row, chat_id)
                existing = read_delivery(pool, ydb, table, delivery_key)
                random_id = int(existing.get("random_id") or delivery_random_id(delivery_key))
                if str(existing.get("status") or "") == "delivered":
                    mid = int(existing.get("message_id") or 0)
                    upsert_sent(pool, ydb, table, row, mid, chat_id=chat_id, delivery_key=delivery_key, random_id=random_id)
                    sent.append({"message_id": mid, "post_url": row.get("post_url"), "delivery_key": delivery_key, "replayed": True})
                    continue
                upsert_delivery(pool, ydb, table, delivery_key, {
                    **existing,
                    "status": "sending",
                    "post_url": canonical_post_url(row),
                    "chat_id": chat_id,
                    "random_id": str(random_id),
                    "sending_started_at": existing.get("sending_started_at") or datetime.now(timezone.utc).isoformat(),
                })
                try:
                    result = await client(functions.messages.SendMessageRequest(
                        peer=input_peer,
                        message=text,
                        random_id=random_id,
                        no_webpage=False,
                    ))
                    mid = _message_id_from_updates(result, random_id)
                except Exception as exc:
                    if type(exc).__name__ != "RandomIdDuplicateError":
                        raise
                    # Telegram has already accepted this deterministic delivery.
                    mid = int(existing.get("message_id") or 0)
                upsert_delivery(pool, ydb, table, delivery_key, {
                    **existing,
                    "status": "delivered",
                    "post_url": canonical_post_url(row),
                    "chat_id": chat_id,
                    "random_id": str(random_id),
                    "message_id": str(mid),
                    "delivered_at": datetime.now(timezone.utc).isoformat(),
                })
                upsert_sent(pool, ydb, table, row, mid, chat_id=chat_id, delivery_key=delivery_key, random_id=random_id)
            else:
                msg = await client.send_message(peer, text, link_preview=True)
                mid = int(getattr(msg, "id", 0) or 0)
            sent.append({"message_id": mid, "post_url": rows[idx].get("post_url") if idx < len(rows) else ""})
            if idx + 1 < len(messages) and not args.dry_run:
                await asyncio.sleep(random.uniform(
                    float(os.getenv("REGION_TALK_NOTIFY_DELAY_MIN_SECONDS") or "2"),
                    float(os.getenv("REGION_TALK_NOTIFY_DELAY_MAX_SECONDS") or "5"),
                ))
    finally:
        await client.disconnect()
        if driver is not None:
            driver.stop()
    return {
        "ok": True,
        "sent": sent,
        "sent_count": len(sent),
        "dry_run": bool(args.dry_run),
        "resolved_chat_id": chat_id,
        "delivery_account_id": actual_account_id,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", type=Path, default=Path(".env"))
    ap.add_argument("--chat", default="")
    ap.add_argument("--expected-chat-id", default="")
    ap.add_argument("--allow-join-chat", action="store_true")
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--message", default="", help="Send a single status message instead of YDB publication candidates")
    ap.add_argument("--stats", action="store_true", help="Send live Region Talk YDB statistics instead of candidate links")
    ap.add_argument("--stats-limit", type=int, default=20000)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    load_env(args.env_file)
    args.chat = args.chat or os.getenv("REGION_TALK_NOTIFY_CHAT") or DEFAULT_NOTIFY_CHAT
    args.expected_chat_id = args.expected_chat_id or os.getenv("REGION_TALK_NOTIFY_CHAT_ID") or DEFAULT_NOTIFY_CHAT_ID
    lock_path = Path(os.getenv("REGION_TALK_NOTIFY_LOCK_FILE") or "/tmp/events-bot-region-talk-notify.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("another Region Talk notifier already owns the E2E delivery session") from exc
        result = asyncio.run(send_rows(args))
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
