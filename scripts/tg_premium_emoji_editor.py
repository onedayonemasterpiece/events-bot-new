#!/usr/bin/env python3
"""Edit Telegram daily free markers to the configured premium emoji label."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tg_premium_emojis import (  # noqa: E402
    edit_latest_daily_announcement,
    edit_message_daily_free_labels,
    load_telethon_config_from_env,
    parse_document_ids,
    raise_if_session_busy,
    telethon_client_from_config,
)


def _load_dotenv_if_present(path: str | None) -> None:
    if not path:
        for candidate in (Path.cwd() / ".env", ROOT / ".env"):
            if candidate.exists():
                path = str(candidate)
                break
    if not path:
        return
    env_path = Path(path)
    if not env_path.exists():
        raise SystemExit(f"dotenv file not found: {path}")
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        os.environ[key] = value


def _result_to_dict(result: Any) -> dict[str, Any]:
    return {
        "chat": result.chat,
        "message_id": result.message_id,
        "edited": result.edited,
        "replacements": result.replacements,
        "error": result.error,
        "before_has_free_marker": "🟡 Бесплатно" in result.before_text or "🚩 🟡" in result.before_text,
        "after_has_free_marker": "🟡 Бесплатно" in result.after_text or "🚩 🟡" in result.after_text,
    }


async def _main_async(args: argparse.Namespace) -> None:
    os.environ.setdefault("TG_PREMIUM_EMOJI_ALLOW_E2E_FALLBACK", "1" if args.allow_e2e_fallback else "0")
    cfg = load_telethon_config_from_env()
    await raise_if_session_busy(cfg.auth_scope)
    parse_document_ids()  # fail fast on malformed ids
    async with telethon_client_from_config(cfg) as client:
        if args.latest:
            results = await edit_latest_daily_announcement(
                client,
                args.chat,
                dry_run=args.dry_run,
            )
        else:
            results = [
                await edit_message_daily_free_labels(
                    client,
                    args.chat[0],
                    int(args.message_id),
                    dry_run=args.dry_run,
                )
            ]
    print(json.dumps([_result_to_dict(item) for item in results], ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dotenv", help="Path to .env; defaults to ./.env or repo .env")
    parser.add_argument("--chat", action="append", default=[], help="Target chat username/id; repeatable with --latest")
    parser.add_argument("--message-id", type=int, help="Message id to edit when not using --latest")
    parser.add_argument("--latest", action="store_true", help="Edit latest #ежедневныйанонс in each --chat")
    parser.add_argument("--dry-run", action="store_true", help="Compute replacements without editing")
    parser.add_argument(
        "--allow-e2e-fallback",
        action="store_true",
        help="Allow TELEGRAM_AUTH_BUNDLE_E2E/TELEGRAM_SESSION fallback for local manual edits",
    )
    args = parser.parse_args()
    _load_dotenv_if_present(args.dotenv)
    if not args.chat:
        raise SystemExit("--chat is required")
    if not args.latest and (len(args.chat) != 1 or not args.message_id):
        raise SystemExit("without --latest pass exactly one --chat and --message-id")
    asyncio.run(_main_async(args))


if __name__ == "__main__":
    main()
