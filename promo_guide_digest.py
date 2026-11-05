"""Guide-digest sources for the existing promo VK runner and delivery ledger."""
from __future__ import annotations

import json
import logging
import random
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from guide_excursions.max_delivery import prepare_delivery, drive_delivery, ensure_delivery_schema

logger = logging.getLogger(__name__)


async def run_digest_repost(db, bot, *, campaign, activity, target, now_utc):
    from promo import (
        PromoVkActionResult, _activity_config, _resolve_vk_group_id,
        _vk_owner_post_from_url, _vk_post_datetime, _publish_vk_repost,
    )
    from guide_excursions.visual_digest import _resolve_live_vk_wall_post_id

    def result(status, *, reason=None, source_url=None, target_url=None):
        return PromoVkActionResult(int(campaign.id), int(activity.id), "vk_repost", 0,
                                   status, source_url, target_url, reason)

    cfg = _activity_config(activity)
    source_group = await _resolve_vk_group_id(cfg.get("source_group"))
    destination = await _resolve_vk_group_id(cfg.get("target_group"))
    if not source_group or not destination or source_group == destination:
        return result("skipped", reason="source_or_target_group_missing")
    # This is a digest campaign: do not invent an Event or attribute its reach
    # to an unrelated excursion. The existing per-edition ledger owns receipts.
    target_key = f"campaign:{campaign.id}:activity:{activity.id}:group:{destination}"
    tz = ZoneInfo("Europe/Kaliningrad")
    today = now_utc.astimezone(tz).date()
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT id, published_targets_json FROM guide_digest_issue "
            "WHERE family=? AND status IN ('published','partial') "
            "AND date(published_at,'+2 hours')=? ORDER BY id DESC LIMIT 1",
            (target.query_text, today.isoformat()),
        )
        issue = await cur.fetchone()
    if not issue:
        return result("skipped", reason="source_unavailable")
    issue_id = int(issue[0])
    targets = json.loads(issue[1] or "{}")
    source_url = next((p["post_urls"][0] for p in targets.values()
                       if isinstance(p, dict) and int(p.get("group_id") or 0) == source_group
                       and p.get("post_urls")), None)
    ids = _vk_owner_post_from_url(source_url)
    if not ids:
        return result("skipped", reason="source_unavailable")
    owner, post = ids
    live_id = await _resolve_live_vk_wall_post_id(db, bot, owner_id=owner, post_id=post)
    if not live_id:
        return result("skipped", reason="source_unavailable")
    source_url = f"https://vk.com/wall{owner}_{live_id}"
    source_at = await _vk_post_datetime(source_url)
    if source_at is None or source_at > now_utc:
        return result("skipped", reason="source_unavailable")
    async with db.raw_conn() as conn:
        await ensure_delivery_schema(conn)
        cur = await conn.execute(
            "SELECT payload_json, receipt_json FROM guide_visual_delivery "
            "WHERE provider='promo_vk_repost' AND issue_id=? "
            "AND json_extract(payload_json,'$.campaign_id')=? "
            "AND json_extract(payload_json,'$.activity_id')<>?",
            (issue_id, int(campaign.id), int(activity.id)))
        siblings = [(json.loads(p), json.loads(r or '{}')) for p, r in await cur.fetchall()]
    # Thirty minutes of actual separation plus one existing five-minute tick.
    choices = [source_at + timedelta(minutes=m) for m in range(30, 176)
               if all(abs(source_at + timedelta(minutes=m) - datetime.fromisoformat(p['due_at']))
                      >= timedelta(minutes=35) for p, _ in siblings)]
    row = await prepare_delivery(
        db, issue_id=issue_id, provider="promo_vk_repost", target_id=target_key,
        payload={"campaign_id": int(campaign.id), "activity_id": int(activity.id),
                 "source_url": source_url, "source_at": source_at.isoformat(),
                 "target_group_id": destination,
                 # Leave one five-minute promo tick inside the owner's 180m bound.
                 "due_at": random.SystemRandom().choice(choices).isoformat()},
    )
    if row["state"] == "published":
        return result("published", source_url=row["payload"]["source_url"],
                      target_url=row["receipt"]["post_urls"][0])
    every_days = max(1, int(cfg.get("every_days", 1)))
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT receipt_json FROM guide_visual_delivery WHERE provider='promo_vk_repost' "
            "AND target_id=? AND state='published' AND issue_id<>? ORDER BY issue_id DESC LIMIT 1",
            (target_key, issue_id),
        )
        previous = await cur.fetchone()
    # Daily cadence permits each edition; explicit daily caps still apply.
    if previous and every_days > 1:
        last_day = datetime.fromisoformat(json.loads(previous[0])["published_at"]).astimezone(tz).date()
        if (today - last_day).days < every_days:
            return result("skipped", reason="dedup_window")
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT payload_json, receipt_json FROM guide_visual_delivery "
            "WHERE provider='promo_vk_repost' AND state='published' "
            "AND json_extract(payload_json,'$.campaign_id')=?", (int(campaign.id),))
        delivered = [(json.loads(p), json.loads(r)) for p, r in await cur.fetchall()]
    if campaign.total_exposure_goal is not None and len(delivered) >= campaign.total_exposure_goal:
        return result("skipped", reason="target_goal_reached")
    daily = [p for p, r in delivered
             if datetime.fromisoformat(r["published_at"]).astimezone(tz).date() == today]
    if ((campaign.daily_exposure_cap is not None and len(daily) >= campaign.daily_exposure_cap)
            or (activity.daily_cap is not None and
                sum(p["activity_id"] == activity.id for p in daily) >= activity.daily_cap)):
        return result("skipped", reason="daily_cap_reached")
    if datetime.fromisoformat(row["payload"]["due_at"]) > now_utc:
        return result("scheduled", source_url=source_url)
    # Overdue schedules must retain spacing too. A lost provider reply retains
    # its attempt time in the same receipt, without blocking another target forever.
    for _, receipt in siblings:
        last_send = receipt.get('published_at') or receipt.get('attempted_at')
        if last_send and now_utc < datetime.fromisoformat(last_send) + timedelta(minutes=30):
            return result("scheduled", source_url=source_url)

    class Transport:
        async def submit(self, payload, key):
            try:
                url = await _publish_vk_repost(db, bot, source_url=payload["source_url"],
                                             target_group_id=payload["target_group_id"], message="")
                if not url:
                    raise RuntimeError("repost_receipt_missing")
            except Exception:
                return {"state": "outcome_unknown", "error_code": "transport_unavailable",
                        "receipt": {"attempted_at": now_utc.isoformat()}}
            return self.receipt(payload, url)

        def receipt(self, payload, url, published_at=None):
            return {"state": "published", "receipt": {
                "post_urls": [url], "source_url": payload["source_url"],
                "campaign_id": payload["campaign_id"], "activity_id": payload["activity_id"],
                "published_at": (published_at or now_utc).isoformat(), "due_at": payload["due_at"]}}

        async def observe(self, key, operation_id):
            # Reply loss/restart: inspect VK's actual repost identity, never send
            # a second wall.repost just because its response was lost.
            from main import _vk_api, _vk_user_token
            payload = row["payload"]
            response = await _vk_api("wall.get", {"owner_id": -payload["target_group_id"],
                                     "filter": "owner", "count": 100}, db, bot,
                                     token=_vk_user_token(), token_kind="user")
            body = response.get("response", {})
            owner_id, post_id = _vk_owner_post_from_url(payload["source_url"])
            for item in body.get("items", []):
                if any(int(p.get("owner_id") or 0) == owner_id and int(p.get("id") or 0) == post_id
                       for p in item.get("copy_history", [])):
                    url = f"https://vk.com/wall{-payload['target_group_id']}_{item['id']}"
                    return self.receipt(payload, url, datetime.fromtimestamp(item['date'], timezone.utc))
            return {"state": "outcome_unknown", "error_code": "repost_not_observed",
                    "receipt": row['receipt']}

    row = await drive_delivery(db, row, Transport())
    logger.info("promo.digest_repost campaign_id=%s activity_id=%s issue_id=%s state=%s",
                campaign.id, activity.id, issue_id, row["state"])
    return result("published" if row["state"] == "published" else "failed",
                  source_url=row["payload"]["source_url"],
                  target_url=next(iter(row["receipt"].get("post_urls", [])), None),
                  reason=row.get("error_code"))
