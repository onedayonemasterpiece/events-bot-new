from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from aiogram import Bot
from sqlalchemy import func, select

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dependency in some runtimes
    load_dotenv = None

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from admin_chat import resolve_superadmin_chat_id
from db import Database, close_known_databases
from models import VideoAnnounceSession, VideoAnnounceSessionStatus
from scheduling import _run_scheduled_popular_review


logger = logging.getLogger("run_cherryflash_live")

TERMINAL_STATUSES = {
    VideoAnnounceSessionStatus.DONE,
    VideoAnnounceSessionStatus.FAILED,
    VideoAnnounceSessionStatus.PUBLISHED_TEST,
    VideoAnnounceSessionStatus.PUBLISHED_MAIN,
}


def _load_env() -> None:
    if load_dotenv is not None:
        load_dotenv(".env", override=False)


def _validate_args(args: argparse.Namespace) -> None:
    if not getattr(args, "wait", True):
        raise RuntimeError(
            "CherryFlash live runner does not support --no-wait: "
            "render launch continues in a background asyncio task and the "
            "runner must keep the event loop alive until terminal session status."
        )


async def _max_popular_review_session_id(db: Database) -> int:
    async with db.get_session() as session:
        result = await session.execute(
            select(func.max(VideoAnnounceSession.id)).where(
                VideoAnnounceSession.profile_key == "popular_review"
            )
        )
        value = result.scalar_one()
        return int(value or 0)


async def _load_session(db: Database, session_id: int) -> VideoAnnounceSession | None:
    async with db.get_session() as session:
        return await session.get(VideoAnnounceSession, session_id)


async def _latest_rendering_session(db: Database) -> VideoAnnounceSession | None:
    async with db.get_session() as session:
        result = await session.execute(
            select(VideoAnnounceSession)
            .where(VideoAnnounceSession.status == VideoAnnounceSessionStatus.RENDERING)
            .order_by(VideoAnnounceSession.id.desc())
        )
        return result.scalars().first()


async def _force_reset_rendering_session(db: Database) -> int | None:
    async with db.get_session() as session:
        result = await session.execute(
            select(VideoAnnounceSession)
            .where(VideoAnnounceSession.status == VideoAnnounceSessionStatus.RENDERING)
            .order_by(VideoAnnounceSession.id.desc())
        )
        stuck = result.scalars().first()
        if stuck is None:
            return None
        stuck.status = VideoAnnounceSessionStatus.FAILED
        stuck.finished_at = datetime.now(timezone.utc)
        stuck.error = "manual runner force reset"
        session.add(stuck)
        await session.commit()
        return int(stuck.id)


async def _wait_for_terminal_status(
    db: Database,
    *,
    session_id: int,
    timeout_minutes: int,
    poll_seconds: int,
) -> VideoAnnounceSession | None:
    deadline = datetime.now(timezone.utc) + timedelta(minutes=max(1, timeout_minutes))
    last_snapshot: tuple[str, str, str] | None = None
    while datetime.now(timezone.utc) < deadline:
        current = await _load_session(db, session_id)
        if current is None:
            logger.error("CherryFlash session #%s disappeared from DB", session_id)
            return None

        snapshot = (
            str(current.status),
            str(current.kaggle_dataset or ""),
            str(current.video_url or ""),
        )
        if snapshot != last_snapshot:
            logger.info(
                "CherryFlash session #%s status=%s dataset=%s video=%s error=%s",
                session_id,
                current.status,
                current.kaggle_dataset or "-",
                current.video_url or "-",
                current.error or "-",
            )
            last_snapshot = snapshot

        if current.status in TERMINAL_STATUSES:
            return current

        await asyncio.sleep(max(5, poll_seconds))

    logger.error(
        "Timed out waiting for CherryFlash session #%s after %s minutes",
        session_id,
        timeout_minutes,
    )
    return await _load_session(db, session_id)


async def _run(args: argparse.Namespace) -> int:
    _validate_args(args)
    _load_env()
    db_path = os.getenv("DB_PATH", "/data/db.sqlite")
    bot_token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    if not bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

    db = Database(db_path)
    await db.init()
    admin_chat_id = await resolve_superadmin_chat_id(db)
    if not admin_chat_id:
        raise RuntimeError("Could not resolve superadmin chat id from DB/env")

    bot = Bot(bot_token)
    before_id = await _max_popular_review_session_id(db)
    logger.info(
        "Starting CherryFlash live run from DB=%s admin_chat_id=%s previous_max_session_id=%s",
        db.path,
        admin_chat_id,
        before_id,
    )

    if args.force_reset_rendering:
        reset_id = await _force_reset_rendering_session(db)
        if reset_id is not None:
            logger.warning("Force-reset stale rendering CherryFlash session #%s", reset_id)
        else:
            logger.info("No active RENDERING session needed reset")
    else:
        current_rendering = await _latest_rendering_session(db)
        if current_rendering is not None:
            logger.warning(
                "An active rendering session already exists: #%s (%s). "
                "Use --force-reset-rendering if it is stale.",
                current_rendering.id,
                current_rendering.profile_key,
            )

    try:
        await _run_scheduled_popular_review(db, bot, startup_catchup=False)
        after_id = await _max_popular_review_session_id(db)
        if after_id <= before_id:
            logger.error("CherryFlash did not create a new popular_review session")
            return 1

        logger.info("CherryFlash created session #%s", after_id)
        if not args.wait:
            return 0

        final_session = await _wait_for_terminal_status(
            db,
            session_id=after_id,
            timeout_minutes=args.timeout_minutes,
            poll_seconds=args.poll_seconds,
        )
        if final_session is None:
            return 1
        if final_session.status == VideoAnnounceSessionStatus.FAILED:
            logger.error(
                "CherryFlash session #%s failed: %s",
                after_id,
                final_session.error or "-",
            )
            return 1
        logger.info(
            "CherryFlash session #%s finished with status=%s video=%s",
            after_id,
            final_session.status,
            final_session.video_url or "-",
        )
        return 0
    finally:
        try:
            await bot.session.close()
        except Exception:
            logger.warning("Failed to close bot session", exc_info=True)
        await close_known_databases()


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run CherryFlash through the production-like scheduled scenario."
    )
    parser.add_argument(
        "--no-wait",
        dest="wait",
        action="store_false",
        help="Start CherryFlash and exit after the new session is created.",
    )
    parser.add_argument(
        "--timeout-minutes",
        type=int,
        default=240,
        help="How long to keep the event loop alive while waiting for final status.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=30,
        help="DB poll interval while waiting for terminal session status.",
    )
    parser.add_argument(
        "--force-reset-rendering",
        action="store_true",
        help="Reset the latest RENDERING video session to FAILED before starting CherryFlash.",
    )
    parser.set_defaults(wait=True)
    return parser


def main() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
