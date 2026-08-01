#!/usr/bin/env python3
"""Synchronize exact operator reactions for Region Talk review deliveries.

Only the role-scoped ImageDiagnostic discovery identity may read the private
review chat.  The sync observes every reaction-list page before it mutates YDB;
Telegram aggregate counters are never approval evidence.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.region_talk_goal_notify import (  # noqa: E402
    DEFAULT_NOTIFY_CHAT,
    DEFAULT_NOTIFY_CHAT_ID,
    OPERATOR_REVIEW_PAYLOAD_VERSION,
    _telethon_client_and_chat,
    canonical_post_url,
    discovery_session_lease,
    load_env,
    publication_operator_review_fingerprint,
    read_kind_rows,
    read_publication_rows,
)


TRANSPORT = "telethon_discovery2"
REVIEW_STATE_VERSION = "region_talk_operator_reaction_state_v1"
REVIEW_EVENT_VERSION = "region_talk_operator_reaction_event_v1"


def normalize_reaction(value: Any) -> str:
    """Normalize supported Telegram emoji aliases; unknown reactions are inert."""

    emoticon = getattr(value, "emoticon", value)
    emoji = str(emoticon or "").replace("\ufe0f", "").strip()
    if emoji == "❤":
        return "positive_heart"
    if emoji == "👍":
        return "positive_like"
    if emoji == "👎":
        return "negative_dislike"
    if emoji == "✍":
        return "rewrite_pen"
    return ""


def parse_reviewer_ids(raw: str) -> set[str]:
    values = {part.strip() for part in re.split(r"[,;\s]+", raw or "") if part.strip()}
    if not values or any(not re.fullmatch(r"[1-9][0-9]*", value) for value in values):
        raise RuntimeError(
            "REGION_TALK_OPERATOR_REVIEWER_IDS must be a non-empty allowlist of exact Telegram user ids"
        )
    return values


def reaction_sender_id(peer_id: Any) -> str:
    for field in ("user_id", "channel_id", "chat_id"):
        value = getattr(peer_id, field, None)
        if value is not None:
            return str(value)
    if isinstance(peer_id, int):
        return str(peer_id)
    return ""


def classify_operator_reactions(reactions_by_reviewer: dict[str, list[str]]) -> dict[str, Any]:
    kinds = {
        reaction
        for reactions in reactions_by_reviewer.values()
        for reaction in reactions
        if reaction
    }
    positive = bool(kinds & {"positive_heart", "positive_like"})
    negative = "negative_dislike" in kinds
    rewrite = "rewrite_pen" in kinds
    if positive and negative:
        decision = "conflict"
    elif negative:
        decision = "rejected"
    elif positive:
        decision = "approved"
    else:
        decision = "pending"
    return {
        "operator_review_decision": decision,
        "operator_review_rewrite_status": "rewrite_requested" if rewrite else "clean",
        "operator_review_positive": positive,
        "operator_review_negative": negative,
        "operator_review_rewrite_requested": rewrite,
    }


def empty_reaction_observation() -> dict[str, Any]:
    """Return a complete empty snapshot without calling Telegram's list RPC.

    Telegram documents ``messages.getMessageReactionsList`` for messages whose
    ``Message.reactions`` field is present.  On a valid message with no
    reactions the list RPC can return ``MSG_ID_INVALID`` instead of an empty
    vector, so the message snapshot is the authority for the empty case.
    """

    exact: dict[str, list[str]] = {}
    return {
        "reactions_by_reviewer": exact,
        "binding_reactor_count": 0,
        "ignored_reaction_count": 0,
        "observed_reaction_count": 0,
        **classify_operator_reactions(exact),
    }


async def _reaction_message_snapshot(client: Any, peer: Any, message_id: int) -> Any:
    message = await client.get_messages(peer, ids=int(message_id))
    if not message or type(message).__name__ == "MessageEmpty":
        raise RuntimeError(f"review delivery message {message_id} is missing")
    return message


async def fetch_exact_reactions(
    client: Any,
    peer: Any,
    message_id: int,
    reviewer_ids: set[str],
    *,
    page_limit: int = 100,
) -> dict[str, Any]:
    """Read the complete per-reactor list or raise without returning a revision."""

    from telethon import functions  # type: ignore

    message = await _reaction_message_snapshot(client, peer, message_id)
    if getattr(message, "reactions", None) is None:
        return empty_reaction_observation()

    offset: str | None = None
    seen_offsets: set[str] = set()
    all_rows: list[Any] = []
    expected_count: int | None = None
    while True:
        try:
            result = await client(functions.messages.GetMessageReactionsListRequest(
                peer=peer,
                id=int(message_id),
                reaction=None,
                offset=offset,
                limit=max(1, min(100, int(page_limit))),
            ))
        except Exception as exc:
            # A last reaction may be removed between the message snapshot and
            # the exact-list call.  Telegram then drops Message.reactions and
            # may answer MSG_ID_INVALID.  Re-read once: only a still-existing
            # message with no reactions is a complete empty observation.
            if type(exc).__name__ != "MsgIdInvalidError":
                raise
            refreshed = await _reaction_message_snapshot(client, peer, message_id)
            if getattr(refreshed, "reactions", None) is None:
                return empty_reaction_observation()
            raise
        if expected_count is None:
            expected_count = int(getattr(result, "count", 0) or 0)
        rows = list(getattr(result, "reactions", None) or [])
        all_rows.extend(rows)
        next_offset = str(getattr(result, "next_offset", None) or "").strip()
        if not next_offset:
            break
        if next_offset in seen_offsets:
            raise RuntimeError(f"reaction pagination loop for message {message_id}")
        seen_offsets.add(next_offset)
        offset = next_offset

    if expected_count is None or len(all_rows) != expected_count:
        raise RuntimeError(
            f"incomplete exact reaction observation for message {message_id}: "
            f"expected={expected_count} fetched={len(all_rows)}"
        )

    binding: dict[str, set[str]] = {}
    ignored = 0
    for item in all_rows:
        sender_id = reaction_sender_id(getattr(item, "peer_id", None))
        normalized = normalize_reaction(getattr(item, "reaction", None))
        if sender_id not in reviewer_ids or not normalized:
            ignored += 1
            continue
        binding.setdefault(sender_id, set()).add(normalized)
    exact = {sender: sorted(values) for sender, values in sorted(binding.items())}
    return {
        "reactions_by_reviewer": exact,
        "binding_reactor_count": len(exact),
        "ignored_reaction_count": ignored,
        "observed_reaction_count": len(all_rows),
        **classify_operator_reactions(exact),
    }


def _read_pk(pool: Any, ydb: Any, table: str, pk: str) -> dict[str, Any]:
    query_text = f"DECLARE $pk AS Utf8; SELECT payload_json FROM `{table}` WHERE pk = $pk;"

    def op(session: Any) -> dict[str, Any]:
        query = session.prepare(query_text)
        result = session.transaction(ydb.StaleReadOnly()).execute(
            query, {"$pk": pk}, commit_tx=True
        )
        rows = result[0].rows if result else []
        if not rows:
            return {}
        raw = rows[0].payload_json
        return json.loads(raw) if isinstance(raw, str) else dict(raw or {})

    return dict(pool.retry_operation_sync(op) or {})


def _upsert(pool: Any, ydb: Any, table: str, pk: str, kind: str, payload: dict[str, Any]) -> None:
    query_text = f"""
DECLARE $pk AS Utf8; DECLARE $kind AS Utf8; DECLARE $payload_json AS Json; DECLARE $updated_at AS Utf8;
UPSERT INTO `{table}` (pk, kind, payload_json, updated_at)
VALUES ($pk, $kind, $payload_json, $updated_at);
"""

    def op(session: Any) -> None:
        query = session.prepare(query_text)
        session.transaction(ydb.SerializableReadWrite()).execute(
            query,
            {
                "$pk": pk,
                "$kind": kind,
                "$payload_json": json.dumps(payload, ensure_ascii=False),
                "$updated_at": str(payload["updated_at"]),
            },
            commit_tx=True,
        )

    pool.retry_operation_sync(op)


def observation_hash(payload: dict[str, Any]) -> str:
    exact = {
        "fingerprint": payload.get("operator_review_fingerprint"),
        "message_id": str(payload.get("message_id") or ""),
        "chat_id": str(payload.get("chat_id") or ""),
        "reactions_by_reviewer": payload.get("reactions_by_reviewer") or {},
    }
    raw = json.dumps(exact, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def reaction_revision_changed(previous: dict[str, Any], current: dict[str, Any]) -> bool:
    return str(previous.get("observation_hash") or "") != observation_hash(current)


def reaction_event_id(previous_hash: str, current_hash: str) -> str:
    """Stable transition id; A→B and B→A are distinct reversible events."""

    return hashlib.sha256(f"{previous_hash}|{current_hash}".encode("utf-8")).hexdigest()[:24]


def _candidate_for_delivery(
    delivery: dict[str, Any],
    by_pk: dict[str, dict[str, Any]],
    by_url: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    pk = str(delivery.get("publication_candidate_pk") or "")
    return by_pk.get(pk) or by_url.get(canonical_post_url(delivery))


async def synchronize(args: argparse.Namespace) -> dict[str, Any]:
    reviewer_ids = parse_reviewer_ids(args.reviewer_ids)
    ydb, driver, pool, table, publications = read_publication_rows(max(args.limit, 5000))
    try:
        deliveries = read_kind_rows(pool, ydb, table, "publication_delivery_item", args.scan_limit)
        deliveries = [
            row for row in deliveries
            if str(row.get("status") or "") == "delivered"
            and str(row.get("transport") or "") == TRANSPORT
            and str(row.get("operator_review_payload_version") or "") == OPERATOR_REVIEW_PAYLOAD_VERSION
            and str(row.get("operator_review_fingerprint") or "")
            and int(row.get("message_id") or 0) > 0
        ]
        deliveries.sort(key=lambda row: str(row.get("delivered_at") or ""), reverse=True)
        deliveries = deliveries[: max(1, args.limit)]

        by_pk = {str(row.get("_ydb_pk") or ""): row for row in publications}
        by_url = {canonical_post_url(row): row for row in publications if canonical_post_url(row)}
        observed: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]] = []
        chat_ids = {str(row.get("chat_id") or "") for row in deliveries}
        expected_chat_id = str(args.expected_chat_id or "")
        if chat_ids - {expected_chat_id}:
            raise RuntimeError("delivery ledger contains a different chat id; refusing reaction sync")

        if deliveries:
            with discovery_session_lease(TRANSPORT):
                client, peer, resolved_chat_id, account_id = await _telethon_client_and_chat(args)
                try:
                    for delivery in deliveries:
                        reaction_state = await fetch_exact_reactions(
                            client,
                            peer,
                            int(delivery["message_id"]),
                            reviewer_ids,
                            page_limit=args.page_limit,
                        )
                        observed.append((delivery, reaction_state, _candidate_for_delivery(delivery, by_pk, by_url)))
                finally:
                    await client.disconnect()
        else:
            resolved_chat_id = expected_chat_id
            account_id = ""

        now = datetime.now(timezone.utc).isoformat()
        writes: list[tuple[str, str, dict[str, Any]]] = []
        changes = 0
        projections = 0
        for delivery, reaction_state, candidate in observed:
            fingerprint = str(delivery["operator_review_fingerprint"])
            state_pk = "publication_review_state_item:" + fingerprint
            payload = {
                "review_state_version": REVIEW_STATE_VERSION,
                "operator_review_fingerprint": fingerprint,
                "operator_review_payload_version": str(delivery["operator_review_payload_version"]),
                "publication_draft_fingerprint": str(delivery.get("operator_review_draft_fingerprint") or ""),
                "operator_review_media_manifest_json": str(delivery.get("operator_review_media_manifest_json") or "{}"),
                "delivery_key": str(delivery.get("delivery_key") or ""),
                "message_id": str(delivery.get("message_id") or ""),
                "chat_id": str(delivery.get("chat_id") or ""),
                "post_url": canonical_post_url(delivery),
                **reaction_state,
                "observation_complete": True,
                "observed_at": now,
                "updated_at": now,
            }
            previous = _read_pk(pool, ydb, table, state_pk)
            changed = reaction_revision_changed(previous, payload)
            payload["observation_hash"] = observation_hash(payload)
            if changed:
                changes += 1
                writes.append((state_pk, "publication_review_state_item", payload))
                event_id = reaction_event_id(
                    str(previous.get("observation_hash") or ""),
                    payload["observation_hash"],
                )
                event = {
                    **payload,
                    "review_event_version": REVIEW_EVENT_VERSION,
                    "event_id": event_id,
                    "previous_observation_hash": str(previous.get("observation_hash") or ""),
                    "created_at": now,
                }
                writes.append((
                    f"publication_review_event_item:{fingerprint}:{event_id}",
                    "publication_review_event_item",
                    event,
                ))

            # An old delivery remains historical. Only the exact current text +
            # ordered media revision may project a decision onto the candidate.
            if candidate is not None and fingerprint == publication_operator_review_fingerprint(candidate):
                candidate_pk = str(candidate.get("_ydb_pk") or delivery.get("publication_candidate_pk") or "")
                if candidate_pk:
                    projected = {key: value for key, value in candidate.items() if not key.startswith("_")}
                    projected.update({
                        "operator_review_fingerprint": fingerprint,
                        "operator_review_state_version": REVIEW_STATE_VERSION,
                        "operator_review_decision": payload["operator_review_decision"],
                        "operator_review_rewrite_status": payload["operator_review_rewrite_status"],
                        "operator_review_positive": payload["operator_review_positive"],
                        "operator_review_negative": payload["operator_review_negative"],
                        "operator_review_rewrite_requested": payload["operator_review_rewrite_requested"],
                        "operator_review_reactions_json": json.dumps(
                            payload["reactions_by_reviewer"], ensure_ascii=False, sort_keys=True
                        ),
                        "operator_review_observation_hash": payload["observation_hash"],
                        "operator_review_observed_at": now,
                        "updated_at": now,
                    })
                    projection_changed = any(
                        str(candidate.get(field)) != str(projected.get(field))
                        for field in (
                            "operator_review_fingerprint",
                            "operator_review_decision",
                            "operator_review_rewrite_status",
                            "operator_review_observation_hash",
                        )
                    )
                    if projection_changed:
                        projections += 1
                        writes.append((candidate_pk, "publication_candidate_item", projected))

        if args.execute:
            for pk, kind, payload in writes:
                _upsert(pool, ydb, table, pk, kind, payload)
        return {
            "ok": True,
            "stage": "operator_reaction_sync",
            "executed": bool(args.execute),
            "transport": TRANSPORT,
            "resolved_chat_id": resolved_chat_id,
            "delivery_account_id": account_id,
            "deliveries_observed_complete": len(observed),
            "reaction_revisions_changed": changes,
            "candidate_projections_changed": projections,
            "planned_ydb_rows": len(writes),
            "written_ydb_rows": len(writes) if args.execute else 0,
        }
    finally:
        driver.stop(timeout=5)


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync Region Talk operator reactions")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--execute", action="store_true", help="Persist after every page was observed")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--scan-limit", type=int, default=5000)
    parser.add_argument("--page-limit", type=int, default=100)
    parser.add_argument("--chat", default="")
    parser.add_argument("--expected-chat-id", default="")
    parser.add_argument("--reviewer-ids", default="")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    load_env(args.env_file)
    args.transport = TRANSPORT
    args.chat = args.chat or os.getenv("REGION_TALK_NOTIFY_CHAT") or DEFAULT_NOTIFY_CHAT
    args.expected_chat_id = (
        args.expected_chat_id or os.getenv("REGION_TALK_NOTIFY_CHAT_ID") or DEFAULT_NOTIFY_CHAT_ID
    )
    args.reviewer_ids = (
        args.reviewer_ids or os.getenv("REGION_TALK_OPERATOR_REVIEWER_IDS") or ""
    )
    try:
        result = asyncio.run(synchronize(args))
    except Exception as exc:
        result = {
            "ok": False,
            "stage": "operator_reaction_sync",
            "transport": TRANSPORT,
            "error": f"{type(exc).__name__}: {str(exc)[:800]}",
        }
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
