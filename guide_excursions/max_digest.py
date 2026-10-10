"""MAX fan-out and backfill driven by the existing visual-digest scheduler."""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from .max_delivery import ensure_delivery_schema, prepare_delivery, drive_delivery
from .vibepublish_client import VibePublishClient, SnapshotUnavailable, html_content

logger = logging.getLogger(__name__)


def max_enabled():
    return (os.getenv("ENABLE_GUIDE_VISUAL_DIGEST_MAX") or "").lower() in {"1", "true", "yes", "on"}


def max_targets():
    raw = os.getenv("GUIDE_VISUAL_DIGEST_MAX_TARGETS")
    targets = json.loads(raw) if raw else [{
        "alias": (os.getenv("GUIDE_VISUAL_DIGEST_MAX_ALIAS") or "").strip(),
        "native_id": (os.getenv("GUIDE_VISUAL_DIGEST_MAX_NATIVE_ID") or "").strip(),
        "binding_revision": int(os.getenv("GUIDE_VISUAL_DIGEST_MAX_BINDING_REVISION") or "0"),
    }]
    if not isinstance(targets, list) or not targets or len(targets) > 20:
        raise SnapshotUnavailable("max_verified_binding_missing")
    result, seen = [], set()
    for target in targets:
        if not isinstance(target, dict) or not target.get("alias") or not target.get("native_id") or int(target.get("binding_revision") or 0) < 1:
            raise SnapshotUnavailable("max_verified_binding_missing")
        native_id = str(target["native_id"])
        if native_id not in seen:
            result.append({"alias": str(target["alias"]), "native_id": native_id, "binding_revision": int(target["binding_revision"])})
            seen.add(native_id)
    return result


async def ensure_snapshot_schema(conn):
    await conn.execute("""CREATE TABLE IF NOT EXISTS guide_visual_snapshot (
        issue_id INTEGER PRIMARY KEY, payload_json TEXT NOT NULL, sha256 TEXT NOT NULL,
        source TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""")


async def save_snapshot(db, *, issue_id, payload, source):
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    digest = hashlib.sha256(encoded.encode()).hexdigest()
    async with db.raw_conn() as conn:
        await ensure_snapshot_schema(conn)
        await conn.execute("INSERT OR IGNORE INTO guide_visual_snapshot "
                           "(issue_id,payload_json,sha256,source) VALUES(?,?,?,?)",
                           (issue_id, encoded, digest, source))
        await conn.commit()
    return await get_snapshot(db, issue_id)


async def get_snapshot(db, issue_id):
    async with db.raw_conn() as conn:
        await ensure_snapshot_schema(conn)
        cur = await conn.execute("SELECT payload_json FROM guide_visual_snapshot WHERE issue_id=?", (issue_id,))
        row = await cur.fetchone()
    return json.loads(row[0]) if row else None


async def freeze_visual_snapshot(db, *, issue_id, caption_html, card):
    """Save exact new-edition caption+JPEG before any provider side effect."""
    return await save_snapshot(db, issue_id=issue_id, payload={
        "content": html_content(caption_html),
        "image_b64": base64.b64encode(card).decode("ascii"),
        "image_sha256": hashlib.sha256(card).hexdigest(),
    }, source="visual_digest_renderer")


def _day_bounds(now=None):
    now = now or datetime.now(timezone.utc)
    tz = ZoneInfo(os.getenv("GUIDE_VISUAL_DIGEST_TZ") or "Europe/Kaliningrad")
    day = now.astimezone(tz).date()
    start = datetime.combine(day, time(), tzinfo=tz).astimezone(timezone.utc)
    end = datetime.combine(day + timedelta(days=1), time(), tzinfo=tz).astimezone(timezone.utc)
    return start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")


async def existing_daily_issue_id(db, *, now=None):
    """Resume today's existing edition; never select fresh occurrences on retry."""
    start, end = _day_bounds(now)
    async with db.raw_conn() as conn:
        await ensure_snapshot_schema(conn)
        await ensure_delivery_schema(conn)
        cur = await conn.execute("SELECT id FROM guide_digest_issue WHERE family='visual_schedule' "
                                 "AND created_at>=? AND created_at<? "
                                 "AND (status IN ('published','partial') OR id IN (SELECT issue_id FROM guide_visual_snapshot) "
                                 "OR id IN (SELECT issue_id FROM guide_visual_delivery)) "
                                 "ORDER BY CASE WHEN status IN ('published','partial') THEN 0 ELSE 1 END, id DESC LIMIT 1",
                                 (start, end))
        row = await cur.fetchone()
    return int(row[0]) if row else None


async def _publish_max_target(db, *, issue_id, target, snapshot, client_factory):
    alias, native_id = target["alias"], target["native_id"]
    payload = dict(snapshot, alias=alias, native_id=native_id, binding_revision=target["binding_revision"])
    row = await prepare_delivery(db, issue_id=issue_id, provider="max", target_id=native_id, payload=payload)
    client = client_factory(row["payload"])
    result = await drive_delivery(db, row, client)
    if result["state"] == "published":
        receipt = dict(result["receipt"], request_key=result["request_key"], operation_id=result["operation_id"])
        path = '$.' + json.dumps(f"max:{native_id}:visual")
        async with db.raw_conn() as conn:
            await conn.execute("UPDATE guide_digest_issue SET published_targets_json="
                               "json_set(COALESCE(published_targets_json,'{}'),?,json(?)) WHERE id=?",
                               (path, json.dumps(receipt), issue_id))
            await conn.commit()
    return {"published": result["state"] == "published", "state": result["state"],
            "issue_id": issue_id, "target": native_id, "request_key": result["request_key"],
            "operation_id": result["operation_id"], "receipt": result["receipt"],
            "reason": result.get("error_code")}


async def publish_visual_digest_to_max(db, *, issue_id, client_factory=None):
    """Extend native generation+publication from the existing backend edition.

    The DB edition and canonical renderer are authoritative. This never reads
    Telegram/VK content, selects another set of excursions or creates a new issue.
    The first generated MAX payload is frozen for exact admission replay.
    """
    if not max_enabled():
        return {"published": False, "reason": "disabled", "issue_id": issue_id}
    client_factory = client_factory or VibePublishClient
    try:
        targets = max_targets()
        snapshot = await get_snapshot(db, issue_id)
        if snapshot is None:
            from .visual_digest import (load_visual_digest_issue, build_visual_digest_telegram_text,
                                        render_visual_digest_cards, VISUAL_DIGEST_CARD_LIMIT)
            issue = await load_visual_digest_issue(db, issue_id)
            rows = list((issue or {}).get("items") or [])[:VISUAL_DIGEST_CARD_LIMIT]
            if not rows:
                raise SnapshotUnavailable("backend_edition_items_missing")
            caption = await build_visual_digest_telegram_text(rows, issue_id=issue_id)
            snapshot = await freeze_visual_snapshot(db, issue_id=issue_id, caption_html=caption,
                                                     card=render_visual_digest_cards(rows, issue_id=issue_id)[0])
        results = []
        for target in targets:
            try:
                results.append(await _publish_max_target(db, issue_id=issue_id, target=target,
                                                          snapshot=snapshot, client_factory=client_factory))
            except Exception as exc:
                reason = str(exc) if isinstance(exc, SnapshotUnavailable) else "adapter_failure"
                logger.error("guide_visual_max_blocked issue_id=%s target_id=%s reason=%s",
                             issue_id, target["native_id"], reason)
                results.append({"published": False, "state": "blocked", "target": target["native_id"], "reason": reason})
        complete = bool(results) and all(r["published"] for r in results)
        return {"published": complete, "state": "published" if complete else "pending",
                "issue_id": issue_id, "deliveries": results}
    except SnapshotUnavailable as exc:
        logger.warning("guide_visual_max_blocked issue_id=%s reason=%s", issue_id, str(exc))
        return {"published": False, "state": "blocked", "issue_id": issue_id, "reason": str(exc)}
    except Exception:
        logger.error("guide_visual_max_blocked issue_id=%s reason=adapter_failure", issue_id)
        return {"published": False, "state": "blocked", "issue_id": issue_id, "reason": "adapter_failure"}


async def resume_current_visual_digest(db, bot):
    """The maintenance tick re-enters the SAME native generator/fan-out.

    No independent backfill publication path. No edition is created before its
    normal daily slot. Completed TG/VK targets are skipped by native receipts.
    """
    if not max_enabled():
        return {"reason": "disabled"}
    current = await existing_daily_issue_id(db)
    async with db.raw_conn() as conn:
        await ensure_delivery_schema(conn)
        cur = await conn.execute("SELECT DISTINCT issue_id FROM guide_visual_delivery "
                                 "WHERE provider='max' AND state!='published' ORDER BY issue_id DESC LIMIT 20")
        pending = [int(row[0]) for row in await cur.fetchall()]
    ids = list(dict.fromkeys(([current] if current else []) + pending))
    if not ids:
        return {"reason": "no_pending_daily_edition"}
    from .visual_digest import publish_visual_digest_daily
    results = [await publish_visual_digest_daily(db, bot, resume_issue_id=issue_id) for issue_id in ids]
    return {"editions": [{"issue_id": r.get("issue_id"), "state": r.get("state"), "max": r.get("max")} for r in results]}
