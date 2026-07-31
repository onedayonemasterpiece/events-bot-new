#!/usr/bin/env python3
"""Backfill grounded copy for legacy Region Talk social candidates.

Only role-scoped discovery sessions may fetch Telegram source text.  Existing
Gemini acceptance and delivery fields are monotonic: this worker updates draft
and draft-backfill fields only, never replaces the original publication
verdict.  Provider calls use the shared Supabase limiter plus a durable YDB
request/budget ledger.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import random
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "kaggle" / "RegionTalkCandidateReport") not in sys.path:
    sys.path.insert(0, str(ROOT / "kaggle" / "RegionTalkCandidateReport"))

import region_talk_candidate_report as rt  # type: ignore  # noqa: E402
from region_talk_llm_runtime import DurableGeminiBudget  # noqa: E402
from scripts import region_talk_goal_notify as notify  # noqa: E402
from scripts.region_talk_vk_media_prefetch import local_vk_posts, parse_vk_post  # noqa: E402


DRAFT_BACKFILL_VERSION = "region_talk_publication_draft_backfill_v1"
DRAFT_FIELDS = (
    "publication_draft_status",
    "publication_draft_title",
    "publication_draft_source_attribution",
    "publication_draft_telegram_text",
    "publication_draft_vk_text",
    "publication_draft_fact_points_json",
    "publication_draft_prompt_version",
)
TERMINAL_BACKFILL_STATUSES = {
    "ready",
    "llm_not_accepted",
    "needs_grounding_review",
    "source_text_unavailable",
    "unsupported_surface",
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_time(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def telegram_post_ref(value: str) -> tuple[str, int] | None:
    normalized = notify.canonical_post_url({"post_url": value})
    match = re.fullmatch(r"https://t\.me/([^/]+)/([0-9]+)", normalized, re.I)
    if not match or match.group(1).lower() in {"c", "joinchat"}:
        return None
    return match.group(1), int(match.group(2))


def social_post_surface(value: str) -> str:
    if telegram_post_ref(value) is not None:
        return "telegram"
    if parse_vk_post(value) is not None:
        return "vk"
    return ""


def backfill_is_actionable(
    row: dict[str, Any],
    *,
    now: datetime | None = None,
    surface: str = "all",
) -> bool:
    if not notify.is_confirmed_publication(row) or notify.is_publication_draft_ready(row):
        return False
    row_surface = social_post_surface(str(row.get("post_url") or ""))
    if not row_surface or surface not in {"all", row_surface}:
        return False
    status = str(row.get("publication_draft_backfill_status") or "").strip().lower()
    if status in TERMINAL_BACKFILL_STATUSES:
        return False
    retry_at = parse_time(row.get("publication_draft_backfill_next_attempt_after"))
    return retry_at is None or retry_at <= (now or utc_now())


def select_rows(
    rows: list[dict[str, Any]],
    *,
    limit: int,
    now: datetime | None = None,
    surface: str = "all",
) -> list[dict[str, Any]]:
    selected = [
        row for row in rows if backfill_is_actionable(row, now=now, surface=surface)
    ]
    selected.sort(key=lambda row: (
        str(row.get("sent_to_chat") or "").lower() == "true",
        int(row.get("publication_rank") or 999999),
        -float(row.get("publication_score") or row.get("publication_pre_score") or 0),
        notify.canonical_post_url(row),
    ))
    return selected[: max(0, int(limit))]


def draft_request_fingerprint(row: dict[str, Any], text: str, *, model: str) -> str:
    payload = {
        "version": DRAFT_BACKFILL_VERSION,
        "post_url": notify.canonical_post_url(row),
        "text_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        "original_llm_decision": row.get("llm_decision") or row.get("publication_llm_decision"),
        "prompt_version": rt.REGION_TALK_FINAL_VERIFIER_PROMPT_VERSION,
        "model": model,
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def upsert_publication_row(pool: Any, ydb: Any, table: str, row: dict[str, Any], updates: dict[str, Any]) -> None:
    now_iso = utc_now().isoformat()
    pk = str(row.get("_ydb_pk") or "")
    if not pk:
        raise RuntimeError("publication row has no durable YDB primary key")
    payload = {key: value for key, value in row.items() if not str(key).startswith("_")}
    payload.update(updates)
    payload["updated_at"] = now_iso
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
            {
                "$pk": pk,
                "$kind": "publication_candidate_item",
                "$payload_json": json.dumps(payload, ensure_ascii=False),
                "$updated_at": now_iso,
            },
            commit_tx=True,
        )

    pool.retry_operation_sync(op)


def build_client(transport: str) -> Any:
    auth_env = notify.TELETHON_TRANSPORT_AUTH_ENVS.get(transport)
    if not auth_env:
        raise RuntimeError(f"unsupported Region Talk Telethon transport: {transport}")
    bundle = notify.decode_discovery_bundle(str(os.getenv(auth_env) or ""))
    api_id = str(os.getenv("TELEGRAM_API_ID") or os.getenv("TG_API_ID") or "").strip()
    api_hash = str(os.getenv("TELEGRAM_API_HASH") or os.getenv("TG_API_HASH") or "").strip()
    if not api_id or not api_hash:
        raise RuntimeError("TELEGRAM_API_ID/API_HASH (or TG_ aliases) are required")
    try:
        from telethon import TelegramClient  # type: ignore
        from telethon.sessions import StringSession  # type: ignore
    except Exception as exc:
        raise RuntimeError("Telethon is required for Region Talk draft backfill") from exc
    return TelegramClient(
        StringSession(str(bundle["session"])),
        int(api_id),
        api_hash,
        request_retries=0,
        connection_retries=0,
        retry_delay=0,
        auto_reconnect=False,
        flood_sleep_threshold=0,
        raise_last_call_error=True,
        receive_updates=False,
        sequential_updates=True,
        device_model=str(bundle.get("device_model") or "Region Talk draft backfill"),
        system_version=str(bundle.get("system_version") or "Linux"),
        app_version=str(bundle.get("app_version") or "1.0"),
        lang_code=str(bundle.get("lang_code") or "ru"),
        system_lang_code=str(bundle.get("system_lang_code") or "ru"),
    )


async def fetch_exact_text(client: Any, row: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    ref = telegram_post_ref(str(row.get("post_url") or ""))
    if ref is None:
        raise RuntimeError("unsupported Telegram post URL")
    handle, message_id = ref
    message = await client.get_messages(handle, ids=message_id)
    if message is None or int(getattr(message, "id", 0) or 0) != message_id:
        raise RuntimeError("exact Telegram message is unavailable")
    text = str(getattr(message, "message", None) or getattr(message, "text", None) or "").strip()
    if not text:
        raise RuntimeError("exact Telegram message has no text")
    date = getattr(message, "date", None)
    return text, {
        "handle": handle,
        "post_id": str(message_id),
        "post_date": date.isoformat() if date is not None else str(row.get("post_date") or ""),
    }


def fetch_vk_text(row: dict[str, Any], posts: dict[str, dict[str, Any]], error: str) -> tuple[str, dict[str, Any]]:
    ref = parse_vk_post(str(row.get("post_url") or ""))
    if ref is None:
        raise RuntimeError("unsupported VK post URL")
    owner_id, post_id = ref
    post = posts.get(f"{owner_id}_{post_id}") or {}
    if not post:
        raise RuntimeError(error or "exact VK post is unavailable")
    if int(post.get("owner_id") or 0) != owner_id or int(post.get("id") or 0) != post_id:
        raise RuntimeError("VK API returned a different post")
    text = str(post.get("text") or "").strip()
    if not text:
        raise RuntimeError("exact VK post has no text")
    timestamp = int(post.get("date") or 0)
    return text, {
        "platform": "vk",
        "post_id": str(post_id),
        "post_date": (
            datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
            if timestamp
            else str(row.get("post_date") or "")
        ),
    }


def retry_updates(row: dict[str, Any], *, transport: str, reason: str) -> dict[str, Any]:
    attempts = int(row.get("publication_draft_backfill_attempt_count") or 0) + 1
    terminal = attempts >= 3
    return {
        "publication_draft_backfill_status": "source_text_unavailable" if terminal else "retry_due",
        "publication_draft_backfill_reason": reason[:500],
        "publication_draft_backfill_transport": transport,
        "publication_draft_backfill_attempt_count": attempts,
        "publication_draft_backfill_last_attempt_at": utc_now().isoformat(),
        "publication_draft_backfill_next_attempt_after": "" if terminal else (utc_now() + timedelta(hours=24)).isoformat(),
        "publication_draft_backfill_version": DRAFT_BACKFILL_VERSION,
    }


async def collect_source_texts(
    rows: list[dict[str, Any]],
    *,
    transport: str,
    delay_min: float,
    delay_max: float,
) -> tuple[dict[str, tuple[str, dict[str, Any], str]], dict[str, str]]:
    """Fetch exact social text before releasing the Telegram session lease."""

    fetched: dict[str, tuple[str, dict[str, Any], str]] = {}
    errors: dict[str, str] = {}
    telegram_rows = [row for row in rows if social_post_surface(str(row.get("post_url") or "")) == "telegram"]
    if telegram_rows:
        with notify.discovery_session_lease(transport):
            client = build_client(transport)
            await client.connect()
            try:
                if not await client.is_user_authorized():
                    raise RuntimeError(
                        f"{notify.TELETHON_TRANSPORT_AUTH_ENVS[transport]} is not authorized"
                    )
                for index, row in enumerate(telegram_rows):
                    url = notify.canonical_post_url(row)
                    try:
                        text, fields = await fetch_exact_text(client, row)
                        fetched[url] = (text, fields, transport)
                    except Exception as exc:
                        errors[url] = f"{type(exc).__name__}: {str(exc)[:400]}"
                    if index + 1 < len(telegram_rows):
                        await asyncio.sleep(random.uniform(delay_min, delay_max))
            finally:
                await client.disconnect()

    vk_rows = [row for row in rows if social_post_surface(str(row.get("post_url") or "")) == "vk"]
    if vk_rows:
        post_ids = []
        for row in vk_rows:
            ref = parse_vk_post(str(row.get("post_url") or ""))
            if ref:
                post_ids.append(f"{ref[0]}_{ref[1]}")
        posts, vk_error = local_vk_posts(post_ids)
        for row in vk_rows:
            url = notify.canonical_post_url(row)
            try:
                text, fields = fetch_vk_text(row, posts, vk_error)
                fetched[url] = (text, fields, "vk_api")
            except Exception as exc:
                errors[url] = f"{type(exc).__name__}: {str(exc)[:400]}"
    return fetched, errors


def build_draft_updates(
    row: dict[str, Any],
    *,
    text: str,
    fetched: dict[str, Any],
    source_transport: str,
    model: str,
    default_env: str,
    budget: DurableGeminiBudget,
) -> tuple[dict[str, Any], bool]:
    fingerprint = draft_request_fingerprint(row, text, model=model)
    reservation = budget.reserve(fingerprint)
    reservation_status = str(reservation.get("status") or "")
    if reservation_status == "replay":
        verdict = dict(reservation.get("result") or {})
        provider_called = False
    elif reservation_status in {"busy", "exhausted"}:
        verdict = {
            "llm_gate_status": "rate_limited",
            "llm_reason": "durable_budget_" + reservation_status,
        }
        provider_called = False
    else:
        post = {
            **row,
            **fetched,
            "text": text,
            "full_text": text,
            "text_excerpt": re.sub(r"\s+", " ", text)[:500],
        }
        evidence = {
            "stage": "publication_draft_backfill",
            "overall_media_score": row.get("overall_media_score"),
            "postcardness_score": row.get("postcardness_score"),
            "image_queue_status": row.get("image_queue_status"),
            "vector_gate_status": row.get("vector_gate_status"),
            "source_geo_class": row.get("source_class_guess") or row.get("source_geo_class"),
            "source_topic_class": row.get("source_topic_class") or "travel/general",
            "publication_text_story_score": row.get("candidate_score"),
            "media_kind": row.get("media_kind"),
            "media_review_mode": row.get("media_review_mode"),
            "manual_media_review_required": row.get("manual_media_review_required"),
        }
        verdict = rt.call_region_talk_semantic_llm(
            post,
            evidence,
            model=model,
            default_env_var_name=default_env,
        )
        budget.complete(fingerprint, verdict)
        provider_called = True

    gate = str(verdict.get("llm_gate_status") or "").lower()
    decision = str(verdict.get("llm_decision") or "").lower()
    draft_ready = str(verdict.get("publication_draft_status") or "") == "ready_for_operator_review"
    base_updates = {
        "publication_draft_backfill_transport": source_transport,
        "publication_draft_backfill_attempt_count": int(row.get("publication_draft_backfill_attempt_count") or 0) + 1,
        "publication_draft_backfill_last_attempt_at": utc_now().isoformat(),
        "publication_draft_backfill_text_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        "publication_draft_backfill_request_fingerprint": fingerprint,
        "publication_draft_backfill_version": DRAFT_BACKFILL_VERSION,
        "publication_draft_backfill_provider_called": str(provider_called).lower(),
        "publication_draft_backfill_llm_gate_status": gate,
        "publication_draft_backfill_llm_decision": decision,
    }
    if gate == "ok" and decision == "accept" and draft_ready:
        return ({
            **base_updates,
            **{field: verdict.get(field) for field in DRAFT_FIELDS},
            "publication_draft_backfill_status": "ready",
            "publication_draft_backfill_reason": str(verdict.get("llm_reason") or "")[:500],
            "publication_draft_backfill_next_attempt_after": "",
        }, provider_called)
    if gate == "ok":
        return ({
            **base_updates,
            "publication_draft_backfill_status": (
                "needs_grounding_review" if decision == "accept" else "llm_not_accepted"
            ),
            "publication_draft_backfill_reason": str(verdict.get("llm_reason") or "")[:500],
            "publication_draft_backfill_next_attempt_after": "",
        }, provider_called)
    return ({
        **base_updates,
        "publication_draft_backfill_status": "retry_due",
        "publication_draft_backfill_reason": str(verdict.get("llm_reason") or gate)[:500],
        "publication_draft_backfill_next_attempt_after": (utc_now() + timedelta(hours=1)).isoformat(),
    }, provider_called)


async def execute(args: argparse.Namespace) -> dict[str, Any]:
    ydb = driver = pool = table = None
    selected: list[dict[str, Any]] = []
    try:
        ydb, driver, pool, table, rows = notify.read_publication_rows(int(args.scan_limit))
        selected = select_rows(rows, limit=int(args.limit), surface=str(args.surface))
        if args.dry_run or not selected:
            return {
                "ok": True,
                "stage": "publication_draft_backfill",
                "dry_run": bool(args.dry_run),
                "selected": [notify.canonical_post_url(row) for row in selected],
                "selected_total": len(selected),
                "ready_total": 0,
                "failed_total": 0,
                "transport": str(args.transport),
            }

        model = str(args.model or os.getenv("REGION_TALK_LLM_MODEL") or "gemini-3.1-flash-lite")
        default_env = str(
            args.default_env_var_name
            or os.getenv("REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME")
            or "GOOGLE_API_KEY3"
        )
        budget = DurableGeminiBudget(
            pool,
            ydb,
            table,
            budget_id=str(args.llm_budget_id),
            budget_max=int(args.llm_budget_max),
            owner_prefix="region-talk-draft-backfill",
        )
        results: list[dict[str, Any]] = []
        fetched_by_url, fetch_errors = await collect_source_texts(
            selected,
            transport=str(args.transport),
            delay_min=float(args.delay_min),
            delay_max=float(args.delay_max),
        )
        for index, row in enumerate(selected):
            url = notify.canonical_post_url(row)
            fetched_item = fetched_by_url.get(url)
            if fetched_item is None:
                source_transport = "vk_api" if social_post_surface(url) == "vk" else str(args.transport)
                updates = retry_updates(
                    row,
                    transport=source_transport,
                    reason=fetch_errors.get(url) or "exact source text unavailable",
                )
                upsert_publication_row(pool, ydb, table, row, updates)
                results.append({"post_url": url, "status": updates["publication_draft_backfill_status"]})
                continue
            text, fetched, source_transport = fetched_item
            updates, provider_called = build_draft_updates(
                row,
                text=text,
                fetched=fetched,
                source_transport=source_transport,
                model=model,
                default_env=default_env,
                budget=budget,
            )
            upsert_publication_row(pool, ydb, table, row, updates)
            results.append({
                "post_url": url,
                "status": updates["publication_draft_backfill_status"],
                "provider_called": provider_called,
            })
            if index + 1 < len(selected):
                await asyncio.sleep(random.uniform(float(args.delay_min), float(args.delay_max)))
        return {
            "ok": True,
            "stage": "publication_draft_backfill",
            "dry_run": False,
            "selected_total": len(selected),
            "ready_total": sum(1 for item in results if item["status"] == "ready"),
            "failed_total": sum(1 for item in results if item["status"] != "ready"),
            "transport": str(args.transport),
            "surface": str(args.surface),
            "llm_budget_id": str(args.llm_budget_id),
            "results": results,
        }
    finally:
        if driver is not None:
            driver.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill grounded Region Talk social drafts")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--transport", choices=tuple(notify.TELETHON_TRANSPORT_AUTH_ENVS), default=None)
    parser.add_argument("--surface", choices=("all", "telegram", "vk"), default="all")
    parser.add_argument("--limit", type=int, default=2)
    parser.add_argument("--scan-limit", type=int, default=5000)
    parser.add_argument("--model", default="")
    parser.add_argument("--default-env-var-name", default="")
    parser.add_argument("--llm-budget-id", default="")
    parser.add_argument("--llm-budget-max", type=int, default=20)
    parser.add_argument("--delay-min", type=float, default=2.0)
    parser.add_argument("--delay-max", type=float, default=5.0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    notify.load_env(args.env_file)
    args.transport = args.transport or os.getenv("REGION_TALK_DRAFT_BACKFILL_TRANSPORT") or "telethon_discovery2"
    if args.transport not in notify.TELETHON_TRANSPORT_AUTH_ENVS:
        raise RuntimeError(f"unsupported REGION_TALK_DRAFT_BACKFILL_TRANSPORT: {args.transport}")
    args.limit = max(0, min(10, int(args.limit)))
    args.llm_budget_id = args.llm_budget_id or os.getenv("REGION_TALK_DRAFT_BACKFILL_BUDGET_ID") or utc_now().strftime("region-talk-draft-backfill-%Y%m%d")
    payload = asyncio.run(execute(args))
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
