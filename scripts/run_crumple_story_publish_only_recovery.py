from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

from aiogram import Bot

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from admin_chat import resolve_superadmin_chat_id
from db import Database, close_known_databases
from models import VideoAnnounceSession
from video_announce.kaggle_client import KaggleClient
from video_announce.poller import run_story_publish_only_recovery

logger = logging.getLogger("crumple_story_publish_only_recovery")


def _load_env() -> None:
    if load_dotenv is not None:
        load_dotenv(".env", override=False)


async def _run(args: argparse.Namespace) -> int:
    _load_env()
    db = Database(os.getenv("DB_PATH", "/data/db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        sess = await session.get(VideoAnnounceSession, args.session_id)
    if sess is None:
        logger.error("VideoAnnounceSession #%s not found", args.session_id)
        return 1

    notify_chat_id = args.notify_chat_id or await resolve_superadmin_chat_id(db)
    if not notify_chat_id:
        logger.error("Could not resolve notify chat id")
        return 1

    bot_token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    if not bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    bot = Bot(bot_token)
    try:
        ok = await run_story_publish_only_recovery(
            db,
            KaggleClient(),
            sess,
            bot=bot,
            notify_chat_id=int(notify_chat_id),
            download_dir=Path(args.download_dir) if args.download_dir else None,
        )
        return 0 if ok else 2
    finally:
        await bot.session.close()
        await close_known_databases()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Republish failed CrumpleVideo VK targets from an existing Kaggle output without rerendering."
    )
    parser.add_argument("session_id", type=int)
    parser.add_argument("--notify-chat-id", type=int, default=None)
    parser.add_argument("--download-dir", default=None)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
