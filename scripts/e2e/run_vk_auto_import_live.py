#!/usr/bin/env python3
"""Run a bounded production VK auto-import through the real Telegram UI.

Uses only the local E2E human session and always targets @events_love39_bot by
default. It never reads the local bot token to discover a target.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from telethon import TelegramClient
from telethon.sessions import StringSession


def _load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _config() -> tuple[int, str, str, dict[str, Any]]:
    raw_bundle = (os.getenv("TELEGRAM_AUTH_BUNDLE_E2E") or "").strip()
    bundle: dict[str, Any] = {}
    if raw_bundle:
        padding = "=" * (-len(raw_bundle) % 4)
        bundle = json.loads(base64.urlsafe_b64decode(raw_bundle + padding).decode("utf-8"))
    session = str(bundle.get("session") or os.getenv("TELEGRAM_SESSION") or "").strip()
    api_id = int(
        bundle.get("api_id")
        or os.getenv("TELEGRAM_API_ID")
        or os.getenv("TG_API_ID")
        or 0
    )
    api_hash = str(
        bundle.get("api_hash")
        or os.getenv("TELEGRAM_API_HASH")
        or os.getenv("TG_API_HASH")
        or ""
    ).strip()
    if not (session and api_id and api_hash):
        raise SystemExit("missing TELEGRAM_AUTH_BUNDLE_E2E (or local E2E session/API vars)")
    return api_id, api_hash, session, bundle


def _message_payload(message: Any) -> dict[str, Any]:
    return {
        "id": int(message.id),
        "date": message.date.astimezone(timezone.utc).isoformat() if message.date else None,
        "edit_date": message.edit_date.astimezone(timezone.utc).isoformat() if message.edit_date else None,
        "out": bool(message.out),
        "text": str(message.raw_text or ""),
    }


async def _main(args: argparse.Namespace) -> int:
    api_id, api_hash, session, bundle = _config()
    client = TelegramClient(
        StringSession(session),
        api_id,
        api_hash,
        device_model=str(bundle.get("device_model") or "Codex E2E"),
        system_version=str(bundle.get("system_version") or "Linux"),
        app_version=str(bundle.get("app_version") or "1.0"),
        lang_code=str(bundle.get("lang_code") or "ru"),
        system_lang_code=str(bundle.get("system_lang_code") or "ru"),
    )
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise RuntimeError("TELEGRAM_AUTH_BUNDLE_E2E is not authorized")
        me = await client.get_me()
        entity = await client.get_entity(args.bot)
        evidence: dict[str, Any] = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "bot": args.bot,
            "human_user_id": int(me.id),
            "runs_requested": args.runs,
            "limit": args.limit,
            "runs": [],
        }
        for index in range(args.runs):
            command = f"/vk_auto_import --limit={args.limit}"
            sent = await client.send_message(entity, command)
            deadline = time.monotonic() + args.timeout_seconds
            seen: dict[int, dict[str, Any]] = {int(sent.id): _message_payload(sent)}
            terminal = False
            while time.monotonic() < deadline:
                messages = await client.get_messages(entity, limit=80, min_id=int(sent.id) - 1)
                for message in messages:
                    seen[int(message.id)] = _message_payload(message)
                    if not message.out and "🏁 VK auto import завершён" in str(message.raw_text or ""):
                        terminal = True
                if terminal:
                    # One extra poll captures final edits to per-row progress.
                    await asyncio.sleep(3)
                    messages = await client.get_messages(entity, limit=80, min_id=int(sent.id) - 1)
                    for message in messages:
                        seen[int(message.id)] = _message_payload(message)
                    break
                await asyncio.sleep(args.poll_seconds)
            run_payload = {
                "index": index + 1,
                "command_message_id": int(sent.id),
                "command": command,
                "terminal_seen": terminal,
                "messages": [seen[key] for key in sorted(seen)],
            }
            evidence["runs"].append(run_payload)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            if not terminal:
                return 2
        evidence["finished_at"] = datetime.now(timezone.utc).isoformat()
        args.output.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return 0
    finally:
        await client.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bot", default="events_love39_bot")
    parser.add_argument("--runs", type=int, choices=range(1, 4), default=1)
    parser.add_argument("--limit", type=int, choices=range(1, 4), default=1)
    parser.add_argument("--timeout-seconds", type=float, default=2100.0)
    parser.add_argument("--poll-seconds", type=float, default=5.0)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/codex/INC-2026-07-11-event-vector-sidecar-sync-stalled/vk-live-e2e.json"),
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    args = parser.parse_args()
    _load_env(args.env_file)
    raise SystemExit(asyncio.run(_main(args)))
