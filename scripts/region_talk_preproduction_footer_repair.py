#!/usr/bin/env python3
"""Repair pending Region Talk review captions without creating new messages.

The command first synchronizes exact operator reactions, re-reads live YDB,
and then checks reactions again immediately before every Telegram edit.  Only
the current, delivered, still-pending review revision is eligible.  The edit
and its new review fingerprint are reconciled idempotently in YDB.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import region_talk_goal_notify as notify  # noqa: E402
from scripts import region_talk_reaction_sync as reactions  # noqa: E402


FOOTER_REPAIR_VERSION = "region_talk_preproduction_footer_repair_v1"
TRANSPORT = "telethon_discovery2"


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def pending_current_review(row: dict[str, Any]) -> bool:
    current = notify.publication_operator_review_fingerprint(row)
    return bool(
        notify.is_confirmed_publication(row)
        and notify.is_publication_draft_ready(row)
        and str(row.get("sent_operator_review_fingerprint") or "") == current
        and str(row.get("operator_review_fingerprint") or "") == current
        and str(row.get("operator_review_decision") or "") == "pending"
        and str(row.get("operator_review_rewrite_status") or "") == "clean"
        and not _truthy(row.get("operator_review_positive"))
        and not _truthy(row.get("operator_review_negative"))
        and not _truthy(row.get("operator_review_rewrite_requested"))
        and int(row.get("sent_message_id") or 0) > 0
    )


def repaired_candidate(row: dict[str, Any], *, chat_id: str) -> dict[str, Any]:
    original = notify.canonical_post_url(row)
    if not original:
        raise RuntimeError("candidate has no canonical original URL")
    updated = {
        key: value for key, value in row.items() if not str(key).startswith("_")
    }
    updated["publication_draft_telegram_text"] = notify.replace_publication_draft_footer(
        str(row.get("publication_draft_telegram_text") or ""), original
    )
    updated["publication_draft_vk_text"] = notify.replace_publication_draft_footer(
        str(row.get("publication_draft_vk_text") or ""), original
    )
    updated["publication_draft_link_metadata_json"] = json.dumps(
        {
            "original_url": original,
            "channel_label": notify.REGION_TALK_PUBLIC_CHANNEL_LABEL,
            "channel_url": notify.REGION_TALK_PUBLIC_CHANNEL_URL,
            "footer_repair_version": FOOTER_REPAIR_VERSION,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    review_fingerprint = notify.publication_operator_review_fingerprint(updated)
    delivery_key = notify.publication_delivery_key(updated, chat_id)
    now = datetime.now(timezone.utc).isoformat()
    updated.update({
        "publication_footer_version": FOOTER_REPAIR_VERSION,
        "operator_review_fingerprint": review_fingerprint,
        "operator_review_decision": "pending",
        "operator_review_rewrite_status": "clean",
        "operator_review_positive": False,
        "operator_review_negative": False,
        "operator_review_rewrite_requested": False,
        "operator_review_reactions_json": "{}",
        "operator_review_observation_hash": "",
        "operator_review_observed_at": "",
        "sent_publication_draft_fingerprint": notify.publication_draft_fingerprint(updated),
        "sent_operator_review_fingerprint": review_fingerprint,
        "sent_operator_review_payload_version": notify.OPERATOR_REVIEW_PAYLOAD_VERSION,
        "delivery_key": delivery_key,
        "footer_repaired_at": now,
        "footer_repair_version": FOOTER_REPAIR_VERSION,
        "updated_at": now,
    })
    return updated


def message_text_urls(message: Any) -> list[str]:
    return [
        str(getattr(entity, "url", "") or "")
        for entity in (getattr(message, "entities", None) or [])
        if type(entity).__name__ == "MessageEntityTextUrl"
    ]


def message_matches(message: Any, row: dict[str, Any]) -> bool:
    p1, p2 = notify._draft_two_paragraphs(row)
    expected_text = notify.public_caption_visible_text(p1, p2)
    expected_urls = [
        notify.canonical_post_url(row),
        notify.REGION_TALK_PUBLIC_CHANNEL_URL,
    ]
    return bool(
        str(getattr(message, "message", "") or "") == expected_text
        and message_text_urls(message) == expected_urls
    )


def repair_identity_current(row: dict[str, Any], updated: dict[str, Any]) -> bool:
    """Return true when Telegram/YDB already bind the repaired exact revision."""

    return bool(
        str(row.get("delivery_key") or "") == str(updated.get("delivery_key") or "")
        and str(row.get("operator_review_fingerprint") or "")
        == str(updated.get("operator_review_fingerprint") or "")
        and str(row.get("sent_operator_review_fingerprint") or "")
        == str(updated.get("sent_operator_review_fingerprint") or "")
        and str(row.get("sent_publication_draft_fingerprint") or "")
        == str(updated.get("sent_publication_draft_fingerprint") or "")
        and str(row.get("footer_repair_version") or "") == FOOTER_REPAIR_VERSION
    )


def _persist_candidate(
    pool: Any, ydb: Any, table: str, pk: str, payload: dict[str, Any]
) -> None:
    reactions._upsert(pool, ydb, table, pk, "publication_candidate_item", payload)


async def repair(args: argparse.Namespace) -> dict[str, Any]:
    sync_args = argparse.Namespace(
        reviewer_ids=args.reviewer_ids,
        limit=args.scan_limit,
        scan_limit=args.scan_limit,
        page_limit=args.page_limit,
        expected_chat_id=args.expected_chat_id,
        chat=args.chat,
        transport=TRANSPORT,
        execute=bool(args.execute),
    )
    sync_result = await reactions.synchronize(sync_args)
    if not sync_result.get("ok"):
        raise RuntimeError("operator reaction synchronization failed")

    ydb, driver, pool, table, rows = notify.read_publication_rows(args.scan_limit)
    try:
        selected = [row for row in rows if pending_current_review(row)]
        selected.sort(key=lambda row: int(row.get("sent_message_id") or 0))
        if args.limit:
            selected = selected[: max(0, int(args.limit))]
        report: dict[str, Any] = {
            "ok": True,
            "stage": "region_talk_preproduction_footer_repair",
            "executed": bool(args.execute),
            "reaction_sync": sync_result,
            "selected_total": len(selected),
            "selected_message_ids": [int(row["sent_message_id"]) for row in selected],
            "edited": [],
            "already_current": [],
            "skipped_after_live_recheck": [],
        }
        if not selected or not args.execute:
            return report

        reviewer_ids = reactions.parse_reviewer_ids(args.reviewer_ids)
        with notify.discovery_session_lease(TRANSPORT):
            client, peer, chat_id, account_id = await notify._telethon_client_and_chat(args)
            report["resolved_chat_id"] = chat_id
            report["delivery_account_id"] = account_id
            try:
                for row in selected:
                    message_id = int(row["sent_message_id"])
                    live = await reactions.fetch_exact_reactions(
                        client, peer, message_id, reviewer_ids,
                        page_limit=args.page_limit,
                    )
                    if (
                        live["operator_review_decision"] != "pending"
                        or live["operator_review_rewrite_status"] != "clean"
                    ):
                        report["skipped_after_live_recheck"].append({
                            "message_id": message_id,
                            "post_url": notify.canonical_post_url(row),
                            "decision": live["operator_review_decision"],
                            "rewrite_status": live["operator_review_rewrite_status"],
                        })
                        continue

                    updated = repaired_candidate(row, chat_id=chat_id)
                    target_html = notify.public_caption(updated, html_mode=True)
                    message = await client.get_messages(peer, ids=message_id)
                    changed = not message_matches(message, updated)
                    if changed:
                        await client.edit_message(
                            peer, message_id, target_html,
                            parse_mode="html", link_preview=False,
                        )
                    verified = await client.get_messages(peer, ids=message_id)
                    if not message_matches(verified, updated):
                        raise RuntimeError(
                            f"Telegram footer verification failed for message {message_id}"
                        )
                    if not changed and repair_identity_current(row, updated):
                        report["already_current"].append({
                            "message_id": message_id,
                            "post_url": notify.canonical_post_url(updated),
                            "delivery_key": str(updated["delivery_key"]),
                        })
                        continue

                    old_delivery_key = str(row.get("delivery_key") or "")
                    new_delivery_key = str(updated["delivery_key"])
                    old_delivery = (
                        notify.read_delivery(pool, ydb, table, old_delivery_key)
                        if old_delivery_key else {}
                    )
                    now = datetime.now(timezone.utc).isoformat()
                    notify.upsert_delivery(pool, ydb, table, new_delivery_key, {
                        **notify.publication_delivery_review_fields({
                            **updated, "_ydb_pk": row.get("_ydb_pk")
                        }),
                        "status": "delivered",
                        "transport": TRANSPORT,
                        "post_url": notify.canonical_post_url(updated),
                        "chat_id": chat_id,
                        "message_id": str(message_id),
                        "random_id": str(old_delivery.get("random_id") or ""),
                        "delivered_at": str(old_delivery.get("delivered_at") or now),
                        "footer_edited_at": now,
                        "footer_repair_version": FOOTER_REPAIR_VERSION,
                        "edited_from_delivery_key": old_delivery_key,
                    })
                    pk = str(row.get("_ydb_pk") or "")
                    if not pk:
                        raise RuntimeError("candidate has no YDB primary key")
                    _persist_candidate(pool, ydb, table, pk, updated)
                    if old_delivery_key and old_delivery_key != new_delivery_key:
                        notify.upsert_delivery(pool, ydb, table, old_delivery_key, {
                            **old_delivery,
                            "status": "superseded",
                            "superseded_at": now,
                            "superseded_by_delivery_key": new_delivery_key,
                            "supersede_reason": "preproduction_footer_repair",
                        })
                    report["edited" if changed else "already_current"].append({
                        "message_id": message_id,
                        "post_url": notify.canonical_post_url(updated),
                        "delivery_key": new_delivery_key,
                    })
            finally:
                await client.disconnect()
        report["edited_total"] = len(report["edited"])
        report["already_current_total"] = len(report["already_current"])
        report["skipped_total"] = len(report["skipped_after_live_recheck"])
        return report
    finally:
        driver.stop(timeout=5)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Edit footer in still-pending Region Talk review messages"
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--scan-limit", type=int, default=5000)
    parser.add_argument("--page-limit", type=int, default=100)
    parser.add_argument("--chat", default="")
    parser.add_argument("--expected-chat-id", default="")
    parser.add_argument("--reviewer-ids", default="")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    notify.load_env(args.env_file)
    args.transport = TRANSPORT
    args.chat = args.chat or os.getenv("REGION_TALK_NOTIFY_CHAT") or notify.DEFAULT_NOTIFY_CHAT
    args.expected_chat_id = (
        args.expected_chat_id
        or os.getenv("REGION_TALK_NOTIFY_CHAT_ID")
        or notify.DEFAULT_NOTIFY_CHAT_ID
    )
    args.reviewer_ids = (
        args.reviewer_ids or os.getenv("REGION_TALK_OPERATOR_REVIEWER_IDS") or ""
    )
    try:
        result = asyncio.run(repair(args))
    except Exception as exc:
        result = {
            "ok": False,
            "stage": "region_talk_preproduction_footer_repair",
            "error": f"{type(exc).__name__}: {str(exc)[:800]}",
        }
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
