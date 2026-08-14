from __future__ import annotations

import asyncio
import json
import logging
import os
import time as _time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import partial
from html import escape
from typing import Any, Awaitable, Callable, Dict, List, Mapping, Optional, Set
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from apscheduler.events import (
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
    EVENT_JOB_SUBMITTED,
)
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from admin_chat import resolve_superadmin_chat_id
from db import optimize, vacuum, wal_checkpoint_truncate
from heavy_ops import current_heavy_meta, describe_heavy_meta, heavy_operation
from ops_run import finish_ops_run, start_ops_run
from runtime import get_running_main


async def _notify_smart_update_retry_accepts(
    db,
    bot,
    accepted: list[tuple[str, int]],
) -> bool:
    """Report durable retry outcomes that otherwise have no interactive caller.

    Smart Update persists the result before this helper is called.  Notification
    failure therefore stays an observability failure and must not make the
    accepted candidate retry (or create a duplicate event).
    """

    if bot is None or not accepted:
        return False
    target_chat_id = await resolve_superadmin_chat_id(db)
    if not target_chat_id:
        logging.warning(
            "smart_update_retry_worker accepted_report_skipped reason=no_superadmin count=%s",
            len(accepted),
        )
        return False

    # A batch can contain several source candidates merged into the same event.
    # Keep one line per durable event/outcome while retaining truthful counters.
    unique = list(dict.fromkeys((str(outcome), int(event_id)) for outcome, event_id in accepted))
    event_ids = sorted({event_id for _, event_id in unique})
    placeholders = ",".join("?" for _ in event_ids)
    rows: dict[int, tuple[str, str | None]] = {}
    try:
        async with db.raw_conn() as conn:
            cursor = await conn.execute(
                f'SELECT id, title, telegraph_url FROM "event" WHERE id IN ({placeholders})',
                tuple(event_ids),
            )
            for row in await cursor.fetchall():
                rows[int(row[0])] = (str(row[1] or "Событие"), row[2])
            await cursor.close()

        created = sum(1 for outcome, _ in unique if outcome == "CREATED")
        merged = sum(1 for outcome, _ in unique if outcome == "MERGED")
        lines = ["✅ <b>Smart Update: фоновый повтор завершён</b>"]
        if created:
            lines.append(f"Создано событий: <b>{created}</b>")
        if merged:
            lines.append(f"Обновлено событий: <b>{merged}</b>")
        for outcome, event_id in unique[:12]:
            title, telegraph_url = rows.get(event_id, ("Событие", None))
            label = "создано" if outcome == "CREATED" else "обновлено"
            safe_title = escape(title[:180])
            if isinstance(telegraph_url, str) and telegraph_url.startswith("https://telegra.ph/"):
                rendered_title = f'<a href="{escape(telegraph_url, quote=True)}">{safe_title}</a>'
            else:
                rendered_title = safe_title
            lines.append(f"• #{event_id} — {rendered_title} ({label})")
        if len(unique) > 12:
            lines.append(f"…и ещё {len(unique) - 12}")
        await bot.send_message(
            int(target_chat_id),
            "\n".join(lines),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception:
        logging.exception(
            "smart_update_retry_worker accepted_report_failed count=%s",
            len(unique),
        )
        return False

    logging.info(
        "smart_update_retry_worker accepted_report_sent created=%s merged=%s events=%s",
        created,
        merged,
        len(unique),
    )
    return True


@dataclass
class Job:
    key: str
    func: Callable[[Any], Awaitable[None]]
    payload: List[Any] = field(default_factory=list)
    depends_on: Set[str] = field(default_factory=set)
    dirty: bool = False
    track: bool = True


MONTHS_NOM = [
    "",
    "январь",
    "февраль",
    "март",
    "апрель",
    "май",
    "июнь",
    "июль",
    "август",
    "сентябрь",
    "октябрь",
    "ноябрь",
    "декабрь",
]

MONTHS_GEN = [
    "",
    "января",
    "февраля",
    "марта",
    "апреля",
    "мая",
    "июня",
    "июля",
    "августа",
    "сентября",
    "октября",
    "ноября",
    "декабря",
]


async def _run_scheduled_guide_excursions(
    db,
    bot,
    *,
    mode: str,
) -> Any:
    from guide_excursions.service import (
        clear_guide_monitor_recovery_job,
        guide_digest_vk_enabled,
        publish_guide_digest,
        publish_latest_guide_digest_to_vk,
        run_guide_monitor,
    )

    target_chat_id = await resolve_superadmin_chat_id(db)
    result = await run_guide_monitor(
        db,
        bot,
        chat_id=target_chat_id,
        operator_id=None,
        trigger="scheduled",
        mode=mode,
        send_progress=bool(target_chat_id),
    )
    auto_publish = _env_enabled("ENABLE_GUIDE_DIGEST_SCHEDULED", default=False)
    # The visual one-card digest has its own morning scheduler.  Keep the old
    # after-scan hook behind an explicit legacy flag to avoid double-publishing
    # the same visual digest in the evening full monitor slot.
    visual_auto_publish = _env_enabled("ENABLE_GUIDE_VISUAL_DIGEST_AFTER_SCAN", default=False)
    warnings = [str(item) for item in (getattr(result, "warnings", None) or []) if str(item).strip()]
    if mode != "full" or result.errors or bot is None or not (auto_publish or visual_auto_publish):
        return
    publish_result = {"published": False, "reason": "disabled"}
    if auto_publish:
        try:
            publish_result = await publish_guide_digest(
                db,
                bot,
                family="new_occurrences",
                chat_id=target_chat_id,
            )
        except Exception as exc:
            logging.exception("SCHED scheduled guide digest publish failed")
            if target_chat_id:
                try:
                    await bot.send_message(
                        int(target_chat_id),
                        (
                            "❌ Scheduled guide digest publish stopped\n"
                            f"reason={str(exc) or type(exc).__name__}"
                        ),
                        disable_web_page_preview=True,
                    )
                except Exception:
                    logging.exception("SCHED failed to notify admin about scheduled guide digest publish failure")
            return
    vk_result = None
    if publish_result.get("published") and guide_digest_vk_enabled():
        try:
            vk_result = await publish_latest_guide_digest_to_vk(
                db,
                bot,
                family="new_occurrences",
                issue_id=int(publish_result.get("issue_id") or 0),
            )
        except Exception as exc:
            logging.exception("SCHED scheduled guide VK digest publish failed")
            vk_result = {"published": False, "reason": str(exc) or type(exc).__name__}
    visual_vk_result = None
    if visual_auto_publish:
        try:
            from guide_excursions.visual_digest import publish_visual_digest_to_vk

            immediate = _env_enabled("GUIDE_VISUAL_DIGEST_VK_IMMEDIATE", default=False)
            visual_vk_result = await publish_visual_digest_to_vk(
                db,
                bot,
                max_cards=1,
                publish_date=0 if immediate else None,
            )
        except Exception as exc:
            logging.exception("SCHED scheduled guide visual VK digest publish failed")
            visual_vk_result = {"published": False, "reason": str(exc) or type(exc).__name__}
    recovery_kernel_ref = str(getattr(result, "recovery_kernel_ref", "") or "").strip()
    if recovery_kernel_ref and getattr(result, "import_completed", False):
        try:
            await clear_guide_monitor_recovery_job(recovery_kernel_ref)
        except Exception:
            logging.warning(
                "SCHED scheduled guide failed to clear recovery job kernel=%s",
                recovery_kernel_ref,
                exc_info=True,
            )
    if target_chat_id and publish_result.get("published"):
        try:
            await bot.send_message(
                int(target_chat_id),
                (
                    "📣 Scheduled guide digest published\n"
                    f"issue_id={publish_result.get('issue_id')}\n"
                    f"target={publish_result.get('target_chat') or '—'}"
                    + (
                        "\n"
                        f"vk={vk_result.get('url') or 'skipped'}"
                        if vk_result
                        else ""
                    )
                    + (
                        "\n"
                        f"visual_vk={visual_vk_result.get('url') or visual_vk_result.get('reason') or 'skipped'}"
                        if visual_vk_result
                        else ""
                    )
                    + (
                        "\n"
                        f"warnings={len(warnings)}\n"
                        f"/guide_report {getattr(result, 'ops_run_id', None)}"
                        if warnings and getattr(result, "ops_run_id", None)
                        else ""
                    )
                ),
                disable_web_page_preview=True,
            )
        except Exception:
            logging.exception("SCHED failed to notify admin about scheduled guide digest publish")
    elif target_chat_id and publish_result.get("reason") == "no_items":
        try:
            await bot.send_message(
                int(target_chat_id),
                (
                    "ℹ️ Scheduled guide digest: новых экскурсионных находок нет\n"
                    f"issue_id={publish_result.get('issue_id')}"
                    + (
                        "\n"
                        f"visual_vk={visual_vk_result.get('url') or visual_vk_result.get('reason') or 'skipped'}"
                        if visual_vk_result
                        else ""
                    )
                    + (
                        "\n"
                        f"warnings={len(warnings)}\n"
                        f"/guide_report {getattr(result, 'ops_run_id', None)}"
                        if warnings and getattr(result, "ops_run_id", None)
                        else ""
                    )
                ),
                disable_web_page_preview=True,
            )
        except Exception:
            logging.exception("SCHED failed to notify admin about empty scheduled guide digest")
    elif target_chat_id and visual_vk_result:
        try:
            await bot.send_message(
                int(target_chat_id),
                (
                    "📣 Scheduled visual guide digest\n"
                    f"visual_vk={visual_vk_result.get('url') or visual_vk_result.get('reason') or 'skipped'}"
                    + (
                        "\n"
                        f"warnings={len(warnings)}\n"
                        f"/guide_report {getattr(result, 'ops_run_id', None)}"
                        if warnings and getattr(result, "ops_run_id", None)
                        else ""
                    )
                ),
                disable_web_page_preview=True,
            )
        except Exception:
            logging.exception("SCHED failed to notify admin about scheduled guide visual digest")


async def _run_scheduled_guide_visual_digest(
    db,
    bot,
    *,
    run_id: str | None = None,
    trigger: str = "scheduled",
) -> dict[str, Any]:
    from guide_excursions.visual_digest import publish_visual_digest_daily

    details: dict[str, Any] = {
        "scheduler_run_id": run_id,
        "max_cards": 1,
        "vk_delay_seconds": int(os.getenv("GUIDE_VISUAL_DIGEST_VK_DELAY_SECONDS", "600") or 600),
        "story_delay_seconds": int(os.getenv("GUIDE_VISUAL_DIGEST_VK_STORY_DELAY_SECONDS", "900") or 900),
    }
    ops_run_id = await start_ops_run(
        db,
        kind="guide_visual_digest",
        trigger=trigger,
        operator_id=0,
        details=details,
    )
    target_chat_id = await resolve_superadmin_chat_id(db)
    try:
        result = await publish_visual_digest_daily(
            db,
            bot,
            max_cards=1,
            vk_delay_seconds=details["vk_delay_seconds"],
        )
        details["result"] = result
        status = "success" if result.get("published") else "empty" if result.get("reason") == "no_items" else "failed"
        await finish_ops_run(db, run_id=ops_run_id, status=status, details=details)
        if target_chat_id and bot is not None:
            try:
                vk_result = dict(result.get("vk") or {}) if isinstance(result.get("vk"), Mapping) else {}
                tg_result = dict(result.get("telegram") or {}) if isinstance(result.get("telegram"), Mapping) else {}
                await bot.send_message(
                    int(target_chat_id),
                    (
                        "📣 Visual guide digest\n"
                        f"status={status}\n"
                        f"issue_id={result.get('issue_id')}\n"
                        f"tg={','.join(tg_result.get('target_chats') or []) if tg_result else 'skipped'}\n"
                        f"vk={vk_result.get('url') or vk_result.get('reason') or 'skipped'}"
                    ),
                    disable_web_page_preview=True,
                )
            except Exception:
                logging.exception("SCHED failed to notify admin about visual guide digest")
        return result
    except Exception as exc:
        details["error"] = str(exc) or type(exc).__name__
        await finish_ops_run(db, run_id=ops_run_id, status="failed", details=details)
        if target_chat_id and bot is not None:
            try:
                await bot.send_message(
                    int(target_chat_id),
                    "❌ Visual guide digest failed\n" f"reason={details['error']}",
                    disable_web_page_preview=True,
                )
            except Exception:
                logging.exception("SCHED failed to notify admin about visual guide digest failure")
        raise


async def _run_scheduled_guide_visual_digest_stories_due(
    db,
    bot,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    del run_id
    from guide_excursions.visual_digest import publish_due_visual_digest_vk_stories

    result = await publish_due_visual_digest_vk_stories(db, bot)
    logging.info("SCHED guide_visual_digest_stories_due result=%s", result)
    return result


async def _run_poll_to_forward_debug_tick(db, bot, run_id: str | None = None) -> None:
    from poll_to_forward import run_debug_tick

    result = await run_debug_tick(db, bot)
    logging.info("SCHED poll_to_forward_debug run_id=%s result=%s", run_id, result)


async def _run_poll_to_forward_prod_create_tick(db, bot, run_id: str | None = None) -> None:
    from poll_to_forward import create_prod_poll_if_due

    result = await create_prod_poll_if_due(db, bot)
    logging.info("SCHED poll_to_forward_prod_create run_id=%s result=%s", run_id, result)


async def _run_poll_to_forward_prod_resolve_tick(db, bot, run_id: str | None = None) -> None:
    from poll_to_forward import resolve_due_prod_polls

    result = await resolve_due_prod_polls(db, bot)
    logging.info("SCHED poll_to_forward_prod_resolve run_id=%s result=%s", run_id, result)


async def _run_scheduled_video_tomorrow_test(
    db,
    bot,
    *,
    profile_key: str,
) -> None:
    await _run_scheduled_video_tomorrow(
        db,
        bot,
        profile_key=profile_key,
        test_mode=True,
    )


async def _run_scheduled_video_tomorrow(
    db,
    bot,
    *,
    profile_key: str,
    test_mode: bool = False,
    startup_catchup: bool = False,
) -> None:
    from video_announce.scenario import (
        DEFAULT_SELECTED_MAX,
        TOMORROW_TEST_MIN_POSTERS,
        VideoAnnounceScenario,
    )

    normalized_profile_key = (profile_key or "default").strip() or "default"
    ops_details: dict[str, Any] = {
        "profile_key": normalized_profile_key,
        "test_mode": bool(test_mode),
        "startup_catchup": bool(startup_catchup),
    }
    ops_run_id = await start_ops_run(
        db,
        kind="video_tomorrow",
        trigger="scheduled",
        operator_id=0,
        details=ops_details,
    )
    target_chat_id = await resolve_superadmin_chat_id(db)
    if not target_chat_id or bot is None:
        logging.warning(
            "SCHED skipping video_tomorrow: missing target_chat_id=%s or bot=%s",
            target_chat_id,
            bot is not None,
        )
        ops_details["skip_reason"] = "missing_target_chat_or_bot"
        await finish_ops_run(
            db,
            run_id=ops_run_id,
            status="skipped",
            details=ops_details,
        )
        return

    try:
        scenario = VideoAnnounceScenario(
            db,
            bot,
            chat_id=int(target_chat_id),
            user_id=int(target_chat_id),
        )
        session_id = await scenario.run_tomorrow_pipeline(
            profile_key=normalized_profile_key,
            selected_max=TOMORROW_TEST_MIN_POSTERS if test_mode else DEFAULT_SELECTED_MAX,
            test_mode=test_mode,
            wait_for_handoff=True,
            trigger="startup_catchup" if startup_catchup else "scheduled",
        )
        if session_id is None:
            skip_reason = str(getattr(scenario, "last_tomorrow_skip_reason", "") or "")
            if skip_reason in {"video_lanes_busy", "render_in_progress"}:
                ops_details["skip_reason"] = skip_reason
                await finish_ops_run(
                    db,
                    run_id=ops_run_id,
                    status="skipped",
                    details=ops_details,
                )
                return
            reason = "video_tomorrow did not create a session"
            if skip_reason:
                reason = f"{reason} (skip_reason={skip_reason})"
                ops_details["skip_reason"] = skip_reason
            ops_details["error"] = reason
            raise RuntimeError(reason)
        ops_details["session_id"] = int(session_id)
        launch_state = await _video_session_launch_state(db, int(session_id))
        ops_details.update(launch_state)
        if not _video_session_has_remote_handoff(launch_state):
            reason = (
                "video_tomorrow did not reach confirmed Kaggle handoff: "
                f"status={launch_state.get('session_status') or '-'} "
                f"dataset={launch_state.get('kaggle_dataset') or '-'} "
                f"kernel={launch_state.get('kaggle_kernel_ref') or '-'}"
            )
            ops_details["error"] = reason
            raise RuntimeError(reason)
    except Exception as exc:
        ops_details["error"] = str(exc) or type(exc).__name__
        await finish_ops_run(
            db,
            run_id=ops_run_id,
            status="failed",
            details=ops_details,
        )
        raise

    await finish_ops_run(
        db,
        run_id=ops_run_id,
        status="success",
        details=ops_details,
    )


async def _run_scheduled_popular_review(
    db,
    bot,
    *,
    startup_catchup: bool = False,
) -> None:
    from video_announce.scenario import VideoAnnounceScenario

    ops_details: dict[str, Any] = {
        "profile_key": "popular_review",
        "startup_catchup": bool(startup_catchup),
    }
    ops_run_id = await start_ops_run(
        db,
        kind="video_popular_review",
        trigger="scheduled",
        operator_id=0,
        details=ops_details,
    )
    target_chat_id = await resolve_superadmin_chat_id(db)
    if not target_chat_id or bot is None:
        logging.warning(
            "SCHED skipping video_popular_review: missing target_chat_id=%s or bot=%s",
            target_chat_id,
            bot is not None,
        )
        ops_details["skip_reason"] = "missing_target_chat_or_bot"
        await finish_ops_run(
            db,
            run_id=ops_run_id,
            status="skipped",
            details=ops_details,
        )
        return

    try:
        scenario = VideoAnnounceScenario(
            db,
            bot,
            chat_id=int(target_chat_id),
            user_id=int(target_chat_id),
        )
        session_id = await scenario.run_popular_review_pipeline(wait_for_handoff=True)
        if session_id is None:
            reason = "CherryFlash did not create a popular_review session"
            ops_details["error"] = reason
            raise RuntimeError(reason)
        ops_details["session_id"] = int(session_id)
        launch_state = await _video_session_launch_state(db, int(session_id))
        ops_details.update(launch_state)
        if not _video_session_has_remote_handoff(launch_state):
            reason = (
                "CherryFlash did not reach confirmed Kaggle handoff: "
                f"status={launch_state.get('session_status') or '-'} "
                f"dataset={launch_state.get('kaggle_dataset') or '-'} "
                f"kernel={launch_state.get('kaggle_kernel_ref') or '-'}"
            )
            ops_details["error"] = reason
            raise RuntimeError(reason)
    except Exception as exc:
        ops_details["error"] = str(exc) or type(exc).__name__
        await finish_ops_run(
            db,
            run_id=ops_run_id,
            status="failed",
            details=ops_details,
        )
        raise

    await finish_ops_run(
        db,
        run_id=ops_run_id,
        status="success",
        details=ops_details,
    )


async def _run_scheduled_partner_track(
    db,
    bot,
    partner_track_id: str,
    *,
    startup_catchup: bool = False,
) -> None:
    """Scheduled launch for a single CherryFlash partner track.

    Mirrors `_run_scheduled_popular_review` but parameterised on the
    partner track. Runs through the same `VideoAnnounceScenario` pipeline
    so partner anti-repeat (`profile_key=popular_review_<track>`) and the
    parallel-safety guard (`has_rendering()`) apply automatically.
    """
    from video_announce.scenario import VideoAnnounceScenario
    from video_announce.partner_tracks import get_partner_track

    partner_track = get_partner_track(partner_track_id)
    if partner_track is None:
        logging.warning(
            "SCHED partner_track unknown track_id=%s — skipping", partner_track_id
        )
        return

    ops_details: dict[str, Any] = {
        "profile_key": partner_track.profile_key,
        "partner_track_id": partner_track.track_id,
        "startup_catchup": bool(startup_catchup),
    }
    ops_run_id = await start_ops_run(
        db,
        kind=f"video_partner_{partner_track.callback_action}",
        trigger="scheduled",
        operator_id=0,
        details=ops_details,
    )
    target_chat_id = await resolve_superadmin_chat_id(db)
    if not target_chat_id or bot is None:
        logging.warning(
            "SCHED skipping video_partner_%s: missing target_chat_id=%s or bot=%s",
            partner_track.callback_action,
            target_chat_id,
            bot is not None,
        )
        ops_details["skip_reason"] = "missing_target_chat_or_bot"
        await finish_ops_run(
            db,
            run_id=ops_run_id,
            status="skipped",
            details=ops_details,
        )
        return

    try:
        scenario = VideoAnnounceScenario(
            db,
            bot,
            chat_id=int(target_chat_id),
            user_id=int(target_chat_id),
        )
        session_id = await scenario.run_partner_track_pipeline(
            partner_track,
            wait_for_handoff=True,
            trigger="startup_catchup" if startup_catchup else "scheduled",
        )
        if session_id is None:
            skip_reason = str(
                getattr(scenario, "last_partner_track_skip_reason", "") or ""
            )
            if (
                partner_track.track_id == "partner_region_east_001"
                and skip_reason == "missing_business_target"
            ):
                ops_details["skip_reason"] = skip_reason
                await finish_ops_run(
                    db,
                    run_id=ops_run_id,
                    status="skipped",
                    details=ops_details,
                )
                return
            if skip_reason in {"video_lanes_busy", "render_in_progress"}:
                ops_details["skip_reason"] = skip_reason
                await finish_ops_run(
                    db,
                    run_id=ops_run_id,
                    status="skipped",
                    details=ops_details,
                )
                return
            reason = (
                f"partner track {partner_track.track_id} did not create a session"
            )
            if skip_reason:
                reason = f"{reason} (skip_reason={skip_reason})"
                ops_details["skip_reason"] = skip_reason
            ops_details["error"] = reason
            raise RuntimeError(reason)
        ops_details["session_id"] = int(session_id)
        launch_state = await _video_session_launch_state(db, int(session_id))
        ops_details.update(launch_state)
        if not _video_session_has_remote_handoff(launch_state):
            reason = (
                f"partner track {partner_track.track_id} did not reach confirmed "
                f"Kaggle handoff: status={launch_state.get('session_status') or '-'} "
                f"dataset={launch_state.get('kaggle_dataset') or '-'} "
                f"kernel={launch_state.get('kaggle_kernel_ref') or '-'}"
            )
            ops_details["error"] = reason
            raise RuntimeError(reason)
    except Exception as exc:
        ops_details["error"] = str(exc) or type(exc).__name__
        await finish_ops_run(
            db,
            run_id=ops_run_id,
            status="failed",
            details=ops_details,
        )
        raise

    await finish_ops_run(
        db,
        run_id=ops_run_id,
        status="success",
        details=ops_details,
    )


async def _run_scheduled_kenigsberg_story(
    db,
    bot,
    *,
    startup_catchup: bool = False,
) -> None:
    from handlers.kenigsberg_stories_cmd import launch_scheduled_kenigsberg_story

    ops_details: dict[str, Any] = {
        "profile_key": "kenigsberg_story",
        "target": "https://t.me/mostvkenig",
        "startup_catchup": bool(startup_catchup),
    }
    ops_run_id = await start_ops_run(
        db,
        kind="kenigsberg_story",
        trigger="scheduled",
        operator_id=0,
        details=ops_details,
    )
    target_chat_id = await resolve_superadmin_chat_id(db)
    if not target_chat_id or bot is None:
        logging.warning(
            "SCHED skipping kenigsberg_story: missing target_chat_id=%s or bot=%s",
            target_chat_id,
            bot is not None,
        )
        ops_details["skip_reason"] = "missing_target_chat_or_bot"
        await finish_ops_run(
            db,
            run_id=ops_run_id,
            status="skipped",
            details=ops_details,
        )
        return

    try:
        session_id = await launch_scheduled_kenigsberg_story(
            db,
            bot,
            notify_chat_id=int(target_chat_id),
            trigger="startup_catchup" if startup_catchup else "scheduled",
        )
        if session_id is None:
            ops_details["skip_reason"] = "active_or_not_started"
            await finish_ops_run(
                db,
                run_id=ops_run_id,
                status="skipped",
                details=ops_details,
            )
            return
        ops_details["session_id"] = int(session_id)
        launch_state = await _video_session_launch_state(db, int(session_id))
        ops_details.update(launch_state)
        if not _video_session_has_remote_handoff(launch_state):
            reason = (
                "Kenigsberg did not reach confirmed Kaggle handoff: "
                f"status={launch_state.get('session_status') or '-'} "
                f"dataset={launch_state.get('kaggle_dataset') or '-'} "
                f"kernel={launch_state.get('kaggle_kernel_ref') or '-'}"
            )
            ops_details["error"] = reason
            raise RuntimeError(reason)
    except Exception as exc:
        ops_details["error"] = str(exc) or type(exc).__name__
        await finish_ops_run(
            db,
            run_id=ops_run_id,
            status="failed",
            details=ops_details,
        )
        raise

    await finish_ops_run(
        db,
        run_id=ops_run_id,
        status="success",
        details=ops_details,
    )


def _cron_from_local(
    time_raw: str,
    tz_name: str,
    *,
    default_hour: str,
    default_minute: str,
    label: str,
) -> tuple[str, str]:
    hour = default_hour
    minute = default_minute
    try:
        if time_raw:
            hh, mm = map(int, time_raw.split(":"))
            tz = ZoneInfo(tz_name)
            local_dt = datetime.now(tz).replace(hour=hh, minute=mm, second=0, microsecond=0)
            utc_dt = local_dt.astimezone(timezone.utc)
            hour = str(utc_dt.hour)
            minute = str(utc_dt.minute)
    except Exception:
        logging.warning(
            "invalid %s time=%s tz=%s; using %s:%s UTC",
            label,
            time_raw,
            tz_name,
            default_hour,
            default_minute,
        )
    return hour, minute


def _safe_zoneinfo(tz_name: str, *, label: str) -> timezone | ZoneInfo:
    try:
        return ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        logging.warning("invalid %s timezone=%s; using UTC", label, tz_name)
        return timezone.utc


def _env_enabled(key: str, *, default: bool = False) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(key: str, default: int) -> int:
    try:
        return int((os.getenv(key) or str(default)).strip())
    except (TypeError, ValueError):
        return int(default)


def _first_env(*keys: str, default: str | None = None) -> str | None:
    for key in keys:
        raw = os.getenv(key)
        if raw is not None and raw.strip():
            return raw.strip()
    return default


def _parse_hhmm(
    time_raw: str,
    *,
    default_hour: int,
    default_minute: int,
    label: str,
) -> tuple[int, int]:
    try:
        if time_raw:
            hh, mm = map(int, time_raw.split(":"))
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return hh, mm
    except Exception:
        pass
    logging.warning(
        "invalid %s time=%s; using %02d:%02d local",
        label,
        time_raw,
        default_hour,
        default_minute,
    )
    return default_hour, default_minute


def _video_tomorrow_schedule_settings() -> tuple[bool, str, str, str, bool]:
    production_enabled = _env_enabled("ENABLE_V_TOMORROW_SCHEDULED", default=False)
    legacy_enabled = _env_enabled("ENABLE_V_TEST_TOMORROW_SCHEDULED", default=False)
    enabled = production_enabled or legacy_enabled
    if production_enabled:
        video_tz_name = _first_env(
            "V_TOMORROW_TZ",
            default="Europe/Kaliningrad",
        ) or "Europe/Kaliningrad"
        video_time_raw = _first_env(
            "V_TOMORROW_TIME_LOCAL",
            default="16:45",
        ) or "16:45"
        video_profile_key = _first_env(
            "V_TOMORROW_PROFILE",
            default="default",
        ) or "default"
    else:
        video_tz_name = _first_env(
            "V_TEST_TOMORROW_TZ",
            default="Europe/Kaliningrad",
        ) or "Europe/Kaliningrad"
        video_time_raw = _first_env(
            "V_TEST_TOMORROW_TIME_LOCAL",
            default="16:45",
        ) or "16:45"
        video_profile_key = _first_env(
            "V_TEST_TOMORROW_PROFILE",
            default="default",
        ) or "default"
    video_test_mode = _env_enabled("V_TOMORROW_TEST_MODE", default=False)
    return enabled, video_tz_name, video_time_raw, video_profile_key, video_test_mode


def _popular_review_schedule_settings() -> tuple[bool, str, str]:
    enabled = _env_enabled("ENABLE_V_POPULAR_REVIEW_SCHEDULED", default=False)
    tz_name = _first_env(
        "V_POPULAR_REVIEW_TZ",
        default="Europe/Kaliningrad",
    ) or "Europe/Kaliningrad"
    time_raw = _first_env(
        "V_POPULAR_REVIEW_TIME_LOCAL",
        default="10:15",
    ) or "10:15"
    return enabled, tz_name, time_raw


def _popular_review_watchdog_grace_seconds() -> int:
    raw = (os.getenv("V_POPULAR_REVIEW_WATCHDOG_GRACE_SECONDS") or "").strip()
    try:
        value = int(raw) if raw else 900
    except ValueError:
        value = 900
    return max(60, value)


def _guide_monitoring_schedule_settings() -> tuple[bool, str, str]:
    enabled = _env_enabled("ENABLE_GUIDE_EXCURSIONS_SCHEDULED", default=False)
    tz_name = (os.getenv("GUIDE_EXCURSIONS_TZ") or "Europe/Kaliningrad").strip() or "Europe/Kaliningrad"
    full_time_raw = (os.getenv("GUIDE_EXCURSIONS_FULL_TIME_LOCAL") or "20:10").strip() or "20:10"
    return enabled, tz_name, full_time_raw


def _guide_monitoring_misfire_grace_seconds() -> int:
    raw = (os.getenv("GUIDE_MONITORING_MISFIRE_GRACE_SECONDS") or "").strip()
    try:
        value = int(raw) if raw else 1800
    except ValueError:
        value = 1800
    return max(30, value)


def _critical_sched_watchdog_grace_seconds() -> int:
    raw = (os.getenv("CRITICAL_SCHED_WATCHDOG_GRACE_SECONDS") or "").strip()
    try:
        value = int(raw) if raw else 300
    except ValueError:
        value = 300
    return max(60, value)


def _critical_sched_interval_seconds() -> int:
    raw = (os.getenv("CRITICAL_SCHED_WATCHDOG_INTERVAL_SECONDS") or "").strip()
    try:
        value = int(raw) if raw else 60
    except ValueError:
        value = 60
    return max(30, value)


def _tg_monitoring_misfire_grace_seconds() -> int:
    raw = (os.getenv("TG_MONITORING_MISFIRE_GRACE_SECONDS") or "").strip()
    try:
        value = int(raw) if raw else 1800
    except ValueError:
        value = 1800
    return max(30, value)


def _tg_monitoring_catchup_lookback_seconds() -> int:
    raw = (os.getenv("TG_MONITORING_CATCHUP_LOOKBACK_SECONDS") or "").strip()
    try:
        value = int(raw) if raw else 86400
    except ValueError:
        value = 86400
    return max(3600, value)


def _vk_auto_import_misfire_grace_seconds() -> int:
    raw = (os.getenv("VK_AUTO_IMPORT_MISFIRE_GRACE_SECONDS") or "").strip()
    try:
        value = int(raw) if raw else 1800
    except ValueError:
        value = 1800
    return max(30, value)


def _vk_auto_import_catchup_lookback_seconds() -> int:
    raw = (os.getenv("VK_AUTO_IMPORT_CATCHUP_LOOKBACK_SECONDS") or "").strip()
    try:
        value = int(raw) if raw else 86400
    except ValueError:
        value = 86400
    return max(3600, value)


def _guide_monitoring_remote_busy_retry_seconds() -> int:
    raw = (os.getenv("GUIDE_MONITORING_REMOTE_BUSY_RETRY_SECONDS") or "").strip()
    try:
        value = int(raw) if raw else 3600
    except ValueError:
        value = 3600
    return max(300, value)


def _tg_monitoring_remote_busy_retry_seconds() -> int:
    raw = (os.getenv("TG_MONITORING_REMOTE_BUSY_RETRY_SECONDS") or "").strip()
    try:
        value = int(raw) if raw else 300
    except ValueError:
        value = 300
    return max(300, value)


def _tg_monitoring_terminal_retry_seconds() -> int:
    raw = (os.getenv("TG_MONITORING_TERMINAL_RETRY_SECONDS") or "").strip()
    try:
        value = int(raw) if raw else 3600
    except ValueError:
        value = 3600
    return max(300, value)


_critical_catchup_inflight: set[str] = set()
_critical_catchup_completed: set[str] = set()
_critical_catchup_deferred_until: dict[str, datetime] = {}


def video_tomorrow_watchdog_enabled() -> bool:
    enabled, _, _, _, _ = _video_tomorrow_schedule_settings()
    return enabled


def _video_tomorrow_misfire_grace_seconds() -> int:
    raw = (os.getenv("V_TOMORROW_MISFIRE_GRACE_SECONDS") or "").strip()
    try:
        value = int(raw) if raw else 600
    except ValueError:
        value = 600
    return max(30, value)


def _video_tomorrow_watchdog_grace_seconds() -> int:
    raw = (os.getenv("V_TOMORROW_WATCHDOG_GRACE_SECONDS") or "").strip()
    try:
        value = int(raw) if raw else 720
    except ValueError:
        value = 720
    return max(60, value)


def _utc_sql_text(dt: datetime) -> str:
    value = dt
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _video_session_has_remote_handoff(state: dict[str, Any]) -> bool:
    kernel_ref = str(state.get("kaggle_kernel_ref") or "").strip()
    dataset = str(state.get("kaggle_dataset") or "").strip()
    status = str(state.get("session_status") or "").strip()
    if status == "FAILED":
        return False
    if dataset and kernel_ref and not kernel_ref.startswith("local:"):
        return True
    return status in {"DONE", "PUBLISHED_TEST", "PUBLISHED_MAIN"} and not kernel_ref.startswith("local:")


async def _video_session_launch_state(db: Any, session_id: int) -> dict[str, Any]:
    if db is None or not hasattr(db, "raw_conn"):
        return {}
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT status, kaggle_dataset, kaggle_kernel_ref
            FROM videoannounce_session
            WHERE id = ?
            LIMIT 1
            """,
            (int(session_id),),
        )
        row = await cur.fetchone()
    if row is None:
        return {}
    status, dataset, kernel_ref = row
    return {
        "session_status": status,
        "kaggle_dataset": dataset,
        "kaggle_kernel_ref": kernel_ref,
    }


async def _video_tomorrow_dispatch_exists_today(
    db: Any,
    *,
    day_start_utc: datetime,
    day_end_utc: datetime,
) -> bool:
    if db is None or not hasattr(db, "raw_conn"):
        return False
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT 1
            FROM ops_run
            WHERE kind = 'video_tomorrow'
              AND trigger = 'scheduled'
              AND status IN ('running', 'success')
              AND started_at >= ?
              AND started_at < ?
            LIMIT 1
            """,
            (_utc_sql_text(day_start_utc), _utc_sql_text(day_end_utc)),
        )
        row = await cur.fetchone()
    return bool(row)


async def _guide_full_dispatch_exists_today(
    db: Any,
    *,
    day_start_utc: datetime,
    day_end_utc: datetime,
) -> bool:
    if db is None or not hasattr(db, "raw_conn"):
        return False
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT status, details_json
            FROM ops_run
            WHERE kind = 'guide_monitoring'
              AND trigger IN ('scheduled', 'recovery_import')
              AND started_at >= ?
              AND started_at < ?
            ORDER BY id DESC
            """,
            (_utc_sql_text(day_start_utc), _utc_sql_text(day_end_utc)),
        )
        rows = await cur.fetchall()
    for status, details_raw in rows:
        details: dict[str, Any] = {}
        if isinstance(details_raw, str) and details_raw.strip():
            try:
                parsed = json.loads(details_raw)
            except Exception:
                parsed = {}
            if isinstance(parsed, dict):
                details = parsed
        elif isinstance(details_raw, dict):
            details = details_raw
        if str(details.get("mode") or "").strip() != "full":
            continue
        if str(status or "").strip() in {"running", "success", "partial"}:
            return True
    return False


async def _ops_run_delivery_exists(
    db: Any,
    *,
    kind: str,
    day_start_utc: datetime,
    day_end_utc: datetime,
    triggers: set[str],
) -> bool:
    if db is None or not hasattr(db, "raw_conn"):
        return False
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT status, trigger
            FROM ops_run
            WHERE kind = ?
              AND started_at >= ?
              AND started_at < ?
            ORDER BY id DESC
            """,
            (kind, _utc_sql_text(day_start_utc), _utc_sql_text(day_end_utc)),
        )
        rows = await cur.fetchall()
    ok_statuses = {"running", "success", "partial", "empty"}
    for status, trigger in rows:
        if str(trigger or "").strip() not in triggers:
            continue
        if str(status or "").strip() in ok_statuses:
            return True
    return False


# A successful operator-triggered full Telegram run after the missed slot is a
# real compensating delivery. Count it so the watchdog cannot launch a second
# S22 Kaggle run as soon as the manual run clears its recovery registry row.
_TG_MONITORING_DELIVERY_TRIGGERS = {"scheduled", "recovery_import", "manual"}


def _last_local_slot(
    *,
    now_utc: datetime,
    tz_name: str,
    time_raw: str,
    default_hour: int,
    default_minute: int,
    label: str,
) -> tuple[datetime, datetime, datetime]:
    tz = _safe_zoneinfo(tz_name, label=label.replace("_TIME_LOCAL", "_TZ"))
    hour_local, minute_local = _parse_hhmm(
        time_raw,
        default_hour=default_hour,
        default_minute=default_minute,
        label=label,
    )
    now_local = now_utc.astimezone(tz)
    scheduled_local = now_local.replace(
        hour=hour_local,
        minute=minute_local,
        second=0,
        microsecond=0,
    )
    if now_local < scheduled_local:
        scheduled_local -= timedelta(days=1)
    return now_local, scheduled_local, scheduled_local.astimezone(timezone.utc)


def _slot_day_window_utc(scheduled_local: datetime) -> tuple[datetime, datetime]:
    day_start_local = scheduled_local.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_local = day_start_local + timedelta(days=1)
    return day_start_local.astimezone(timezone.utc), day_end_local.astimezone(timezone.utc)


def _details_have_remote_telegram_busy(details: dict[str, Any]) -> bool:
    def _walk(value: Any) -> bool:
        if isinstance(value, str):
            return "remote_telegram_session_busy" in value
        if isinstance(value, dict):
            return any(_walk(item) for item in value.values())
        if isinstance(value, (list, tuple, set)):
            return any(_walk(item) for item in value)
        return False

    return _walk(details)


def _parse_ops_run_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        raw = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            try:
                parsed = datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


async def _latest_guide_full_remote_busy_skip_started_at(
    db: Any,
    *,
    day_start_utc: datetime,
    day_end_utc: datetime,
) -> datetime | None:
    if db is None or not hasattr(db, "raw_conn"):
        return None
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT status, details_json, started_at
            FROM ops_run
            WHERE kind = 'guide_monitoring'
              AND trigger = 'scheduled'
              AND started_at >= ?
              AND started_at < ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (_utc_sql_text(day_start_utc), _utc_sql_text(day_end_utc)),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    status, details_raw, started_at_raw = row
    if str(status or "").strip() != "skipped":
        return None
    details: dict[str, Any] = {}
    if isinstance(details_raw, str) and details_raw.strip():
        try:
            parsed = json.loads(details_raw)
        except Exception:
            parsed = {}
        if isinstance(parsed, dict):
            details = parsed
    elif isinstance(details_raw, dict):
        details = details_raw
    if str(details.get("mode") or "").strip() != "full":
        return None
    if not _details_have_remote_telegram_busy(details):
        return None
    return _parse_ops_run_datetime(started_at_raw)


async def _latest_guide_full_retry_hold_started_at(
    db: Any,
    *,
    day_start_utc: datetime,
    day_end_utc: datetime,
) -> datetime | None:
    if db is None or not hasattr(db, "raw_conn"):
        return None
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT status, details_json, started_at
            FROM ops_run
            WHERE kind = 'guide_monitoring'
              AND trigger = 'scheduled'
              AND started_at >= ?
              AND started_at < ?
            ORDER BY id DESC
            LIMIT 20
            """,
            (_utc_sql_text(day_start_utc), _utc_sql_text(day_end_utc)),
        )
        rows = await cur.fetchall()
    for status, details_raw, started_at_raw in rows:
        details: dict[str, Any] = {}
        if isinstance(details_raw, str) and details_raw.strip():
            try:
                parsed = json.loads(details_raw)
            except Exception:
                parsed = {}
            if isinstance(parsed, dict):
                details = parsed
        elif isinstance(details_raw, dict):
            details = details_raw
        if str(details.get("mode") or "").strip() != "full":
            continue
        normalized_status = str(status or "").strip()
        if normalized_status in {"running", "success", "partial"}:
            return None
        if normalized_status == "skipped":
            if _details_have_remote_telegram_busy(details):
                return _parse_ops_run_datetime(started_at_raw)
            continue
        if normalized_status in {"error", "crashed"}:
            return _parse_ops_run_datetime(started_at_raw)
    return None


async def _latest_tg_monitoring_remote_busy_skip_started_at(
    db: Any,
    *,
    day_start_utc: datetime,
    day_end_utc: datetime,
) -> datetime | None:
    if db is None or not hasattr(db, "raw_conn"):
        return None
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT status, details_json, started_at
            FROM ops_run
            WHERE kind = 'tg_monitoring'
              AND trigger = 'scheduled'
              AND started_at >= ?
              AND started_at < ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (_utc_sql_text(day_start_utc), _utc_sql_text(day_end_utc)),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    status, details_raw, started_at_raw = row
    if str(status or "").strip() != "skipped":
        return None
    details: dict[str, Any] = {}
    if isinstance(details_raw, str) and details_raw.strip():
        try:
            parsed = json.loads(details_raw)
        except Exception:
            parsed = {}
        if isinstance(parsed, dict):
            details = parsed
    elif isinstance(details_raw, dict):
        details = details_raw
    if not _details_have_remote_telegram_busy(details):
        return None
    return _parse_ops_run_datetime(started_at_raw)


async def _latest_tg_monitoring_retry_hold_started_at(
    db: Any,
    *,
    day_start_utc: datetime,
    day_end_utc: datetime,
) -> datetime | None:
    if db is None or not hasattr(db, "raw_conn"):
        return None
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT status, details_json, started_at, finished_at
            FROM ops_run
            WHERE kind = 'tg_monitoring'
              AND trigger = 'scheduled'
              AND started_at >= ?
              AND started_at < ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (_utc_sql_text(day_start_utc), _utc_sql_text(day_end_utc)),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    status, details_raw, started_at_raw, finished_at_raw = row
    normalized_status = str(status or "").strip()
    if normalized_status in {"running", "success", "partial", "empty"}:
        return None
    details: dict[str, Any] = {}
    if isinstance(details_raw, str) and details_raw.strip():
        try:
            parsed = json.loads(details_raw)
        except Exception:
            parsed = {}
        if isinstance(parsed, dict):
            details = parsed
    elif isinstance(details_raw, dict):
        details = details_raw
    if normalized_status == "skipped" and not _details_have_remote_telegram_busy(details):
        return None
    if normalized_status in {"error", "crashed", "skipped"}:
        return _parse_ops_run_datetime(finished_at_raw or started_at_raw)
    return None


async def _kaggle_launch_intent_exists(
    job_type: str,
    *,
    db: Any | None = None,
    bot: Any | None = None,
) -> bool:
    try:
        from kaggle_registry import list_jobs, list_launch_intents, reconcile_launch_intents

        await reconcile_launch_intents(job_type)
        jobs = await list_jobs(job_type)
        intents = await list_launch_intents(job_type)
        reconciled_jobs = [
            job
            for job in jobs
            if isinstance(job.get("meta"), dict)
            and job["meta"].get("intent_reconciliation")
        ]
        if reconciled_jobs and db is not None and job_type == "guide_monitoring":
            from guide_excursions.service import resume_guide_monitor_jobs

            await resume_guide_monitor_jobs(db, bot)
            jobs = await list_jobs(job_type)
            reconciled_jobs = [
                job
                for job in jobs
                if isinstance(job.get("meta"), dict)
                and job["meta"].get("intent_reconciliation")
            ]
    except Exception:
        logging.warning(
            "SCHED critical watchdog could not inspect %s launch intents",
            job_type,
            exc_info=True,
        )
        return True
    return bool(intents or reconciled_jobs)


async def _tg_monitoring_recovery_job_exists(
    *,
    db: Any | None = None,
    bot: Any | None = None,
) -> bool:
    try:
        from kaggle_registry import list_jobs, list_launch_intents, reconcile_launch_intents

        await reconcile_launch_intents("tg_monitoring")
        jobs = await list_jobs("tg_monitoring")
        intents = await list_launch_intents("tg_monitoring")
        if jobs and db is not None:
            from source_parsing.telegram.service import resume_telegram_monitor_jobs

            await resume_telegram_monitor_jobs(db, bot)
            jobs = await list_jobs("tg_monitoring")
    except Exception:
        logging.warning("SCHED critical watchdog could not inspect tg_monitoring registry", exc_info=True)
        return True
    return bool(jobs or intents)




def _video_session_status_closes_scheduled_slot(
    status: str | None,
    *,
    video_url: str | None = None,
    terminal_ledger: bool = False,
) -> bool:
    normalized = str(status or "").strip()
    if normalized in _VIDEO_TOMORROW_EXISTING_SESSION_STATUSES:
        return True
    if str(video_url or "").strip():
        return True
    if terminal_ledger:
        return True
    return False


async def _video_sessions_with_terminal_kaggle_ledger(
    db: Any,
    session_ids: list[int],
) -> set[int]:
    if db is None or not hasattr(db, "raw_conn") or not session_ids:
        return set()
    placeholders = ",".join("?" for _ in session_ids)
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            f"""
            SELECT DISTINCT session_id
            FROM kaggle_run_ledger
            WHERE session_id IN ({placeholders})
              AND lower(status) IN ('done', 'partial')
              AND terminal_at IS NOT NULL
            """,
            tuple(int(sid) for sid in session_ids),
        )
        rows = await cur.fetchall()
    return {int(row[0]) for row in rows if row[0] is not None}


def _video_live_ledger_grace_minutes() -> int:
    raw = (os.getenv("VIDEO_KAGGLE_REMOTE_ALIVE_GRACE_MINUTES") or "").strip()
    try:
        value = int(raw) if raw else 15
    except ValueError:
        value = 15
    return max(1, value)


def _parse_utcish_datetime(value: Any) -> datetime | None:
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


async def _video_sessions_with_fresh_kaggle_heartbeat(
    db: Any,
    session_ids: list[int],
    *,
    now: datetime | None = None,
) -> set[int]:
    if db is None or not hasattr(db, "raw_conn") or not session_ids:
        return set()
    placeholders = ",".join("?" for _ in session_ids)
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            f"""
            SELECT session_id, status, last_heartbeat_at, updated_at, terminal_at
            FROM kaggle_run_ledger
            WHERE session_id IN ({placeholders})
              AND run_id LIKE 'videoannounce:%'
            """,
            tuple(int(sid) for sid in session_ids),
        )
        rows = await cur.fetchall()
    now_utc = now or datetime.now(timezone.utc)
    max_age = _video_live_ledger_grace_minutes() * 60
    live: set[int] = set()
    for session_id, status_raw, heartbeat_raw, updated_raw, terminal_raw in rows:
        status = str(status_raw or "").strip().casefold()
        if status in {"failed", "error", "cancelled", "canceled", "complete", "done"}:
            continue
        if terminal_raw:
            continue
        heartbeat_at = _parse_utcish_datetime(heartbeat_raw) or _parse_utcish_datetime(updated_raw)
        if heartbeat_at is None:
            continue
        age_seconds = (now_utc - heartbeat_at).total_seconds()
        if 0 <= age_seconds <= max(60, max_age):
            try:
                live.add(int(session_id))
            except (TypeError, ValueError):
                continue
    return live

async def _video_tomorrow_session_exists_today(
    db: Any,
    *,
    day_start_utc: datetime,
    day_end_utc: datetime,
    profile_key: str,
    target_date: str,
) -> bool:
    if db is None or not hasattr(db, "raw_conn"):
        return False
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT id, status, profile_key, selection_params, video_url
            FROM videoannounce_session
            WHERE created_at >= ?
              AND created_at < ?
            ORDER BY id DESC
            """,
            (_utc_sql_text(day_start_utc), _utc_sql_text(day_end_utc)),
        )
        rows = await cur.fetchall()
    session_ids = [int(row[0]) for row in rows if row and row[0] is not None]
    terminal_ledger_sessions = await _video_sessions_with_terminal_kaggle_ledger(
        db,
        session_ids,
    )
    live_ledger_sessions = await _video_sessions_with_fresh_kaggle_heartbeat(
        db,
        session_ids,
    )
    for session_id, status, row_profile_key, selection_params_raw, video_url in rows:
        if str(row_profile_key or "").strip() != profile_key:
            continue
        params: dict[str, Any] = {}
        if isinstance(selection_params_raw, str) and selection_params_raw.strip():
            try:
                parsed = json.loads(selection_params_raw)
            except Exception:
                parsed = {}
            if isinstance(parsed, dict):
                params = parsed
        elif isinstance(selection_params_raw, dict):
            params = selection_params_raw
        if str(params.get("target_date") or "").strip() != target_date:
            continue
        if int(session_id) in live_ledger_sessions:
            return True
        if _video_session_status_closes_scheduled_slot(
            status,
            video_url=video_url,
            terminal_ledger=int(session_id) in terminal_ledger_sessions,
        ):
            return True
    return False


async def _video_tomorrow_failed_session_count_today(
    db: Any,
    *,
    day_start_utc: datetime,
    day_end_utc: datetime,
    profile_key: str,
    target_date: str,
) -> int:
    if db is None or not hasattr(db, "raw_conn"):
        return 0
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT status, profile_key, selection_params
            FROM videoannounce_session
            WHERE created_at >= ?
              AND created_at < ?
            ORDER BY id DESC
            """,
            (_utc_sql_text(day_start_utc), _utc_sql_text(day_end_utc)),
        )
        rows = await cur.fetchall()
    count = 0
    for status, row_profile_key, selection_params_raw in rows:
        if str(row_profile_key or "").strip() != profile_key:
            continue
        if str(status or "").strip() != "FAILED":
            continue
        params: dict[str, Any] = {}
        if isinstance(selection_params_raw, str) and selection_params_raw.strip():
            try:
                parsed = json.loads(selection_params_raw)
            except Exception:
                parsed = {}
            if isinstance(parsed, dict):
                params = parsed
        elif isinstance(selection_params_raw, dict):
            params = selection_params_raw
        if str(params.get("target_date") or "").strip() == target_date:
            count += 1
    return count


async def _popular_review_session_exists_today(
    db: Any,
    *,
    day_start_utc: datetime,
    day_end_utc: datetime,
    target_date: str,
) -> bool:
    if db is None or not hasattr(db, "raw_conn"):
        return False
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT id, status, kaggle_dataset, kaggle_kernel_ref, selection_params, video_url
            FROM videoannounce_session
            WHERE profile_key = 'popular_review'
              AND created_at >= ?
              AND created_at < ?
            ORDER BY id DESC
            """,
            (_utc_sql_text(day_start_utc), _utc_sql_text(day_end_utc)),
        )
        rows = await cur.fetchall()
    session_ids = [int(row[0]) for row in rows if row and row[0] is not None]
    terminal_ledger_sessions = await _video_sessions_with_terminal_kaggle_ledger(
        db,
        session_ids,
    )
    live_ledger_sessions = await _video_sessions_with_fresh_kaggle_heartbeat(
        db,
        session_ids,
    )
    for session_id, status, dataset, kernel_ref, selection_params_raw, video_url in rows:
        params: dict[str, Any] = {}
        if isinstance(selection_params_raw, str) and selection_params_raw.strip():
            try:
                parsed = json.loads(selection_params_raw)
            except Exception:
                parsed = {}
            if isinstance(parsed, dict):
                params = parsed
        elif isinstance(selection_params_raw, dict):
            params = selection_params_raw
        if str(params.get("target_date") or "").strip() != target_date:
            continue
        if int(session_id) in live_ledger_sessions:
            return True
        if _video_session_status_closes_scheduled_slot(
            status,
            video_url=video_url,
            terminal_ledger=int(session_id) in terminal_ledger_sessions,
        ):
            return True
        if _video_session_has_remote_handoff(
            {
                "session_status": status,
                "kaggle_dataset": dataset,
                "kaggle_kernel_ref": kernel_ref,
            }
        ):
            return True
    return False


POPULAR_REVIEW_FAILED_SESSION_RETRY_CAP = 2


async def _popular_review_failed_session_count_today(
    db: Any,
    *,
    day_start_utc: datetime,
    day_end_utc: datetime,
    target_date: str,
) -> int:
    """Count persisted failures for today's scheduled CherryFlash slot.

    Keep the retry budget in SQLite so a runtime restart cannot reset it and
    turn a deterministic render or dependency failure into a ten-minute retry
    storm.
    """
    if db is None or not hasattr(db, "raw_conn"):
        return 0
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT status, selection_params
            FROM videoannounce_session
            WHERE profile_key = 'popular_review'
              AND created_at >= ?
              AND created_at < ?
            ORDER BY id DESC
            """,
            (_utc_sql_text(day_start_utc), _utc_sql_text(day_end_utc)),
        )
        rows = await cur.fetchall()
    count = 0
    for status, selection_params_raw in rows:
        if str(status or "").strip() != "FAILED":
            continue
        params: dict[str, Any] = {}
        if isinstance(selection_params_raw, str) and selection_params_raw.strip():
            try:
                parsed = json.loads(selection_params_raw)
            except Exception:
                parsed = {}
            if isinstance(parsed, dict):
                params = parsed
        elif isinstance(selection_params_raw, dict):
            params = selection_params_raw
        if str(params.get("target_date") or "").strip() == target_date:
            count += 1
    return count


async def _partner_track_session_exists_today(
    db: Any,
    *,
    day_start_utc: datetime,
    day_end_utc: datetime,
    target_date: str,
    profile_key: str,
) -> bool:
    """Return True if a partner-track session for `target_date` already reached
    confirmed Kaggle handoff (so the watchdog must not re-launch it)."""
    if db is None or not hasattr(db, "raw_conn"):
        return False
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT id, status, kaggle_dataset, kaggle_kernel_ref, selection_params, video_url
            FROM videoannounce_session
            WHERE profile_key = ?
              AND created_at >= ?
              AND created_at < ?
            ORDER BY id DESC
            """,
            (
                profile_key,
                _utc_sql_text(day_start_utc),
                _utc_sql_text(day_end_utc),
            ),
        )
        rows = await cur.fetchall()
    session_ids = [int(row[0]) for row in rows if row and row[0] is not None]
    terminal_ledger_sessions = await _video_sessions_with_terminal_kaggle_ledger(
        db,
        session_ids,
    )
    live_ledger_sessions = await _video_sessions_with_fresh_kaggle_heartbeat(
        db,
        session_ids,
    )
    for session_id, status, dataset, kernel_ref, selection_params_raw, video_url in rows:
        params: dict[str, Any] = {}
        if isinstance(selection_params_raw, str) and selection_params_raw.strip():
            try:
                parsed = json.loads(selection_params_raw)
            except Exception:
                parsed = {}
            if isinstance(parsed, dict):
                params = parsed
        elif isinstance(selection_params_raw, dict):
            params = selection_params_raw
        if str(params.get("target_date") or "").strip() != target_date:
            continue
        if int(session_id) in live_ledger_sessions:
            return True
        if _video_session_status_closes_scheduled_slot(
            status,
            video_url=video_url,
            terminal_ledger=int(session_id) in terminal_ledger_sessions,
        ):
            return True
        if _video_session_has_remote_handoff(
            {
                "session_status": status,
                "kaggle_dataset": dataset,
                "kaggle_kernel_ref": kernel_ref,
            }
        ):
            return True
    return False


async def _partner_track_skip_attempts_today(
    db: Any,
    *,
    day_start_utc: datetime,
    day_end_utc: datetime,
    kind: str,
    partner_track_id: str,
    skip_reason: str,
) -> int:
    if db is None or not hasattr(db, "raw_conn"):
        return 0
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT details_json
            FROM ops_run
            WHERE kind = ?
              AND started_at >= ?
              AND started_at < ?
              AND status IN ('skipped', 'failed')
            ORDER BY id DESC
            """,
            (
                kind,
                _utc_sql_text(day_start_utc),
                _utc_sql_text(day_end_utc),
            ),
        )
        rows = await cur.fetchall()
    count = 0
    for (details_raw,) in rows:
        details: dict[str, Any] = {}
        if isinstance(details_raw, str) and details_raw.strip():
            try:
                parsed = json.loads(details_raw)
            except Exception:
                parsed = {}
            if isinstance(parsed, dict):
                details = parsed
        elif isinstance(details_raw, dict):
            details = details_raw
        if str(details.get("partner_track_id") or "") != partner_track_id:
            continue
        item_skip_reason = str(details.get("skip_reason") or "")
        item_error = str(details.get("error") or "")
        if item_skip_reason != skip_reason:
            # Compatibility with runs created before skip_reason persistence:
            # they failed as generic no-session attempts after the same missing
            # Business target preflight.
            if "did not create a session" not in item_error:
                continue
        if details.get("session_id"):
            continue
        count += 1
    return count


async def _kenigsberg_story_session_exists_today(
    db: Any,
    *,
    day_start_utc: datetime,
    day_end_utc: datetime,
) -> bool:
    if db is None or not hasattr(db, "raw_conn"):
        return False
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT id, status, kaggle_dataset, kaggle_kernel_ref, selection_params, video_url
            FROM videoannounce_session
            WHERE profile_key = 'kenigsberg_story'
              AND created_at >= ?
              AND created_at < ?
            ORDER BY id DESC
            """,
            (_utc_sql_text(day_start_utc), _utc_sql_text(day_end_utc)),
        )
        rows = await cur.fetchall()
    terminal_ledger_sessions = await _video_sessions_with_terminal_kaggle_ledger(
        db,
        [int(row[0]) for row in rows if row and row[0] is not None],
    )
    for session_id, status, dataset, kernel_ref, selection_params_raw, video_url in rows:
        params: dict[str, Any] = {}
        if isinstance(selection_params_raw, str) and selection_params_raw.strip():
            try:
                parsed = json.loads(selection_params_raw)
            except Exception:
                parsed = {}
            if isinstance(parsed, dict):
                params = parsed
        elif isinstance(selection_params_raw, dict):
            params = selection_params_raw
        if str(params.get("trigger") or "").strip() not in {"scheduled", "startup_catchup"}:
            continue
        if not bool(params.get("story_publish_requested")):
            continue
        if _video_session_status_closes_scheduled_slot(
            status,
            video_url=video_url,
            terminal_ledger=int(session_id) in terminal_ledger_sessions,
        ):
            return True
        if _video_session_has_remote_handoff(
            {
                "session_status": status,
                "kaggle_dataset": dataset,
                "kaggle_kernel_ref": kernel_ref,
            }
        ):
            return True
    return False


async def _kenigsberg_story_failed_session_count_today(
    db: Any,
    *,
    day_start_utc: datetime,
    day_end_utc: datetime,
) -> int:
    if db is None or not hasattr(db, "raw_conn"):
        return 0
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT status, selection_params
            FROM videoannounce_session
            WHERE profile_key = 'kenigsberg_story'
              AND created_at >= ?
              AND created_at < ?
            ORDER BY id DESC
            """,
            (_utc_sql_text(day_start_utc), _utc_sql_text(day_end_utc)),
        )
        rows = await cur.fetchall()
    count = 0
    for status, selection_params_raw in rows:
        if str(status or "").strip() != "FAILED":
            continue
        params: dict[str, Any] = {}
        if isinstance(selection_params_raw, str) and selection_params_raw.strip():
            try:
                parsed = json.loads(selection_params_raw)
            except Exception:
                parsed = {}
            if isinstance(parsed, dict):
                params = parsed
        elif isinstance(selection_params_raw, dict):
            params = selection_params_raw
        if str(params.get("trigger") or "").strip() not in {"scheduled", "startup_catchup"}:
            continue
        if not bool(params.get("story_publish_requested")):
            continue
        count += 1
    return count


async def _maybe_catch_up_video_tomorrow_on_startup(db: Any, bot: Any) -> bool:
    enabled, video_tz_name, video_time_raw, profile_key, video_test_mode = _video_tomorrow_schedule_settings()
    if not enabled:
        return False
    video_tz = _safe_zoneinfo(video_tz_name, label="V_TOMORROW_TZ")
    hour_local, minute_local = _parse_hhmm(
        video_time_raw,
        default_hour=16,
        default_minute=45,
        label="V_TOMORROW_TIME_LOCAL",
    )
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(video_tz)
    scheduled_local = now_local.replace(
        hour=hour_local,
        minute=minute_local,
        second=0,
        microsecond=0,
    )
    if now_local <= scheduled_local + timedelta(seconds=30):
        return False

    day_start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_local = day_start_local + timedelta(days=1)
    day_start_utc = day_start_local.astimezone(timezone.utc)
    day_end_utc = day_end_local.astimezone(timezone.utc)
    target_date = (now_local + timedelta(days=1)).date().isoformat()
    normalized_profile_key = (profile_key or "default").strip() or "default"
    failed_session_count = await _video_tomorrow_failed_session_count_today(
        db,
        day_start_utc=day_start_utc,
        day_end_utc=day_end_utc,
        profile_key=normalized_profile_key,
        target_date=target_date,
    )
    if failed_session_count >= 2:
        logging.info(
            "SCHED startup catchup skip video_tomorrow: failed session retry cap reached count=%s",
            failed_session_count,
        )
        return False
    if await _video_tomorrow_dispatch_exists_today(
        db,
        day_start_utc=day_start_utc,
        day_end_utc=day_end_utc,
    ) and failed_session_count == 0:
        logging.info(
            "SCHED startup catchup skip video_tomorrow: scheduled dispatch already recorded today"
        )
        return False

    if await _video_tomorrow_session_exists_today(
        db,
        day_start_utc=day_start_utc,
        day_end_utc=day_end_utc,
        profile_key=normalized_profile_key,
        target_date=target_date,
    ):
        logging.info(
            "SCHED startup catchup skip video_tomorrow: matching session already exists today"
        )
        return False

    logging.warning(
        "SCHED startup catchup dispatching missed video_tomorrow slot scheduled_local=%s now_local=%s",
        scheduled_local.isoformat(),
        now_local.isoformat(),
    )
    await _run_scheduled_video_tomorrow(
        db,
        bot,
        profile_key=normalized_profile_key,
        test_mode=video_test_mode,
        startup_catchup=True,
    )
    return True


async def _maybe_catch_up_popular_review_on_startup(db: Any, bot: Any) -> bool:
    enabled, tz_name, time_raw = _popular_review_schedule_settings()
    if not enabled:
        return False
    review_tz = _safe_zoneinfo(tz_name, label="V_POPULAR_REVIEW_TZ")
    hour_local, minute_local = _parse_hhmm(
        time_raw,
        default_hour=10,
        default_minute=15,
        label="V_POPULAR_REVIEW_TIME_LOCAL",
    )
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(review_tz)
    scheduled_local = now_local.replace(
        hour=hour_local,
        minute=minute_local,
        second=0,
        microsecond=0,
    )
    if now_local <= scheduled_local + timedelta(seconds=30):
        return False

    day_start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_local = day_start_local + timedelta(days=1)
    target_date = now_local.date().isoformat()
    if await _popular_review_session_exists_today(
        db,
        day_start_utc=day_start_local.astimezone(timezone.utc),
        day_end_utc=day_end_local.astimezone(timezone.utc),
        target_date=target_date,
    ):
        logging.info(
            "SCHED startup catchup skip video_popular_review: confirmed Kaggle handoff already exists today"
        )
        return False

    failed_session_count = await _popular_review_failed_session_count_today(
        db,
        day_start_utc=day_start_local.astimezone(timezone.utc),
        day_end_utc=day_end_local.astimezone(timezone.utc),
        target_date=target_date,
    )
    if failed_session_count >= POPULAR_REVIEW_FAILED_SESSION_RETRY_CAP:
        logging.error(
            "SCHED startup catchup video_popular_review retry cap reached "
            "target_date=%s failed_sessions=%s cap=%s",
            target_date,
            failed_session_count,
            POPULAR_REVIEW_FAILED_SESSION_RETRY_CAP,
        )
        return False

    logging.warning(
        "SCHED startup catchup dispatching missed video_popular_review slot scheduled_local=%s now_local=%s",
        scheduled_local.isoformat(),
        now_local.isoformat(),
    )
    await _run_scheduled_popular_review(db, bot, startup_catchup=True)
    return True


async def _maybe_catch_up_kenigsberg_story_on_startup(db: Any, bot: Any) -> bool:
    tz_name = "Europe/Kaliningrad"
    time_raw = "19:30"
    weekly_weekday = 4  # Friday; keep the existing 19:30 local slot, but not daily.
    tz = _safe_zoneinfo(tz_name, label="KENIGSBERG_STORY_TZ")
    hour_local, minute_local = _parse_hhmm(
        time_raw,
        default_hour=19,
        default_minute=30,
        label="KENIGSBERG_STORY_TIME_LOCAL",
    )
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(tz)
    if now_local.weekday() != weekly_weekday:
        return False
    scheduled_local = now_local.replace(
        hour=hour_local,
        minute=minute_local,
        second=0,
        microsecond=0,
    )
    if now_local <= scheduled_local + timedelta(seconds=30):
        return False

    day_start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_local = day_start_local + timedelta(days=1)
    day_start_utc = day_start_local.astimezone(timezone.utc)
    day_end_utc = day_end_local.astimezone(timezone.utc)
    failed_session_count = await _kenigsberg_story_failed_session_count_today(
        db,
        day_start_utc=day_start_utc,
        day_end_utc=day_end_utc,
    )
    if failed_session_count >= 2:
        logging.info(
            "SCHED startup catchup skip kenigsberg_story: failed session retry cap reached count=%s",
            failed_session_count,
        )
        return False

    if await _kenigsberg_story_session_exists_today(
        db,
        day_start_utc=day_start_utc,
        day_end_utc=day_end_utc,
    ):
        logging.info(
            "SCHED startup catchup skip kenigsberg_story: confirmed scheduled story handoff already exists today"
        )
        return False

    logging.warning(
        "SCHED startup catchup dispatching missed kenigsberg_story slot scheduled_local=%s now_local=%s",
        scheduled_local.isoformat(),
        now_local.isoformat(),
    )
    await _run_scheduled_kenigsberg_story(db, bot, startup_catchup=True)
    return True


async def maybe_dispatch_video_tomorrow_watchdog(db: Any, bot: Any) -> bool:
    enabled, video_tz_name, video_time_raw, profile_key, video_test_mode = (
        _video_tomorrow_schedule_settings()
    )
    if not enabled:
        return False
    video_tz = _safe_zoneinfo(video_tz_name, label="V_TOMORROW_TZ")
    hour_local, minute_local = _parse_hhmm(
        video_time_raw,
        default_hour=16,
        default_minute=45,
        label="V_TOMORROW_TIME_LOCAL",
    )
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(video_tz)
    scheduled_local = now_local.replace(
        hour=hour_local,
        minute=minute_local,
        second=0,
        microsecond=0,
    )
    watchdog_delay_sec = max(
        _video_tomorrow_watchdog_grace_seconds(),
        _video_tomorrow_misfire_grace_seconds() + 120,
    )
    if now_local <= scheduled_local + timedelta(seconds=watchdog_delay_sec):
        return False

    day_start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_local = day_start_local + timedelta(days=1)
    day_start_utc = day_start_local.astimezone(timezone.utc)
    day_end_utc = day_end_local.astimezone(timezone.utc)
    target_date = (now_local + timedelta(days=1)).date().isoformat()
    normalized_profile_key = (profile_key or "default").strip() or "default"
    failed_session_count = await _video_tomorrow_failed_session_count_today(
        db,
        day_start_utc=day_start_utc,
        day_end_utc=day_end_utc,
        profile_key=normalized_profile_key,
        target_date=target_date,
    )
    if failed_session_count >= 2:
        return False
    if await _video_tomorrow_dispatch_exists_today(
        db,
        day_start_utc=day_start_utc,
        day_end_utc=day_end_utc,
    ) and failed_session_count == 0:
        return False

    if await _video_tomorrow_session_exists_today(
        db,
        day_start_utc=day_start_utc,
        day_end_utc=day_end_utc,
        profile_key=normalized_profile_key,
        target_date=target_date,
    ):
        return False

    logging.error(
        "SCHED watchdog dispatching missing live video_tomorrow slot scheduled_local=%s now_local=%s",
        scheduled_local.isoformat(),
        now_local.isoformat(),
    )
    await _run_scheduled_video_tomorrow(
        db,
        bot,
        profile_key=normalized_profile_key,
        test_mode=video_test_mode,
        startup_catchup=False,
    )
    return True


async def _maybe_dispatch_guide_critical_watchdog(db: Any, bot: Any) -> int:
    enabled, tz_name, full_time_raw = _guide_monitoring_schedule_settings()
    if not enabled:
        return 0
    now_utc = datetime.now(timezone.utc)
    now_local, scheduled_local, scheduled_utc = _last_local_slot(
        now_utc=now_utc,
        tz_name=tz_name,
        time_raw=full_time_raw,
        default_hour=20,
        default_minute=10,
        label="GUIDE_EXCURSIONS_FULL_TIME_LOCAL",
    )
    watchdog_delay_sec = max(
        _critical_sched_watchdog_grace_seconds(),
        _guide_monitoring_misfire_grace_seconds() + 120,
    )
    if now_utc <= scheduled_utc + timedelta(seconds=watchdog_delay_sec):
        return 0

    day_start_utc, day_end_utc = _slot_day_window_utc(scheduled_local)
    # A missed evening slot can still be retried after local midnight.  Keep
    # those catch-up attempts in the persisted evidence window; otherwise the
    # watchdog cannot see its own remote-session-busy skip and dispatches on
    # every watchdog tick until the competing S22 run finishes.
    evidence_end_utc = max(day_end_utc, now_utc + timedelta(seconds=1))
    if await _guide_full_dispatch_exists_today(
        db,
        day_start_utc=day_start_utc,
        day_end_utc=evidence_end_utc,
    ):
        return 0

    catchup_key = f"guide_excursions_full:{now_local.date().isoformat()}"
    if catchup_key in _critical_catchup_completed:
        return 0
    if catchup_key in _critical_catchup_inflight:
        return 0
    retry_seconds = _guide_monitoring_remote_busy_retry_seconds()
    deferred_until = _critical_catchup_deferred_until.get(catchup_key)
    if deferred_until is not None:
        if now_utc < deferred_until:
            return 0
        _critical_catchup_deferred_until.pop(catchup_key, None)
    latest_remote_busy_started = await _latest_guide_full_remote_busy_skip_started_at(
        db,
        day_start_utc=day_start_utc,
        day_end_utc=evidence_end_utc,
    )
    if latest_remote_busy_started is not None:
        db_deferred_until = latest_remote_busy_started + timedelta(seconds=retry_seconds)
        if now_utc < db_deferred_until:
            _critical_catchup_deferred_until[catchup_key] = db_deferred_until
            return 0
    latest_retry_hold_started = await _latest_guide_full_retry_hold_started_at(
        db,
        day_start_utc=day_start_utc,
        day_end_utc=evidence_end_utc,
    )
    if latest_retry_hold_started is not None:
        db_deferred_until = latest_retry_hold_started + timedelta(seconds=retry_seconds)
        if now_utc < db_deferred_until:
            _critical_catchup_deferred_until[catchup_key] = db_deferred_until
            logging.warning(
                "SCHED critical watchdog deferring guide_excursions_full retry "
                "after recent terminal run scheduled_local=%s now_local=%s retry_seconds=%s",
                scheduled_local.isoformat(),
                now_local.isoformat(),
                retry_seconds,
            )
            return 0
    if await _kaggle_launch_intent_exists(
        "guide_monitoring", db=db, bot=bot
    ):
        _critical_catchup_deferred_until[catchup_key] = now_utc + timedelta(
            seconds=retry_seconds
        )
        logging.warning(
            "SCHED critical watchdog deferring guide_excursions_full while a "
            "durable pre-push launch intent exists"
        )
        return 0

    _critical_catchup_inflight.add(catchup_key)
    try:
        logging.error(
            "SCHED critical watchdog dispatching missing guide_excursions_full slot "
            "scheduled_local=%s now_local=%s",
            scheduled_local.isoformat(),
            now_local.isoformat(),
        )
        async with heavy_operation(
            kind="guide_monitoring",
            trigger="scheduled",
            mode="wait",
            operator_id=0,
        ):
            await _run_scheduled_guide_excursions(db, bot, mode="full")
        if await _guide_full_dispatch_exists_today(
            db,
            day_start_utc=day_start_utc,
            day_end_utc=evidence_end_utc,
        ):
            _critical_catchup_completed.add(catchup_key)
            _critical_catchup_deferred_until.pop(catchup_key, None)
        else:
            latest_remote_busy_started = await _latest_guide_full_remote_busy_skip_started_at(
                db,
                day_start_utc=day_start_utc,
                day_end_utc=evidence_end_utc,
            )
            if latest_remote_busy_started is None:
                latest_retry_hold_started = await _latest_guide_full_retry_hold_started_at(
                    db,
                    day_start_utc=day_start_utc,
                    day_end_utc=evidence_end_utc,
                )
                if latest_retry_hold_started is None:
                    return 1
                _critical_catchup_deferred_until[catchup_key] = (
                    latest_retry_hold_started + timedelta(seconds=retry_seconds)
                )
                logging.warning(
                    "SCHED critical watchdog deferring guide_excursions_full retry "
                    "after terminal run scheduled_local=%s now_local=%s retry_seconds=%s",
                    scheduled_local.isoformat(),
                    now_local.isoformat(),
                    retry_seconds,
                )
            else:
                _critical_catchup_deferred_until[catchup_key] = (
                    latest_remote_busy_started + timedelta(seconds=retry_seconds)
                )
                logging.warning(
                    "SCHED critical watchdog deferring guide_excursions_full retry "
                    "after remote session busy scheduled_local=%s now_local=%s "
                    "retry_seconds=%s",
                    scheduled_local.isoformat(),
                    now_local.isoformat(),
                    retry_seconds,
                )
        return 1
    finally:
        _critical_catchup_inflight.discard(catchup_key)


async def _maybe_dispatch_tg_monitoring_watchdog(db: Any, bot: Any) -> int:
    is_prod = os.getenv("DEV_MODE") != "1" and os.getenv("PYTEST_CURRENT_TEST") is None
    if not _env_enabled("ENABLE_TG_MONITORING", default=is_prod):
        return 0
    now_utc = datetime.now(timezone.utc)
    now_local, scheduled_local, scheduled_utc = _last_local_slot(
        now_utc=now_utc,
        tz_name=os.getenv("TG_MONITORING_TZ", "Europe/Kaliningrad"),
        time_raw=os.getenv("TG_MONITORING_TIME_LOCAL", "23:40").strip(),
        default_hour=23,
        default_minute=40,
        label="TG_MONITORING_TIME_LOCAL",
    )
    watchdog_delay_sec = max(
        _critical_sched_watchdog_grace_seconds(),
        _tg_monitoring_misfire_grace_seconds() + 120,
    )
    if now_utc <= scheduled_utc + timedelta(seconds=watchdog_delay_sec):
        return 0
    if now_utc - scheduled_utc > timedelta(seconds=_tg_monitoring_catchup_lookback_seconds()):
        return 0

    window_start = scheduled_utc - timedelta(minutes=5)
    if await _ops_run_delivery_exists(
        db,
        kind="tg_monitoring",
        day_start_utc=window_start,
        day_end_utc=now_utc + timedelta(seconds=1),
        triggers=_TG_MONITORING_DELIVERY_TRIGGERS,
    ):
        return 0

    catchup_key = f"tg_monitoring:{scheduled_local.isoformat()}"
    if catchup_key in _critical_catchup_completed:
        return 0
    if catchup_key in _critical_catchup_inflight:
        return 0
    retry_seconds = _tg_monitoring_remote_busy_retry_seconds()
    deferred_until = _critical_catchup_deferred_until.get(catchup_key)
    if deferred_until is not None:
        if now_utc < deferred_until:
            return 0
        _critical_catchup_deferred_until.pop(catchup_key, None)
    latest_remote_busy_started = await _latest_tg_monitoring_remote_busy_skip_started_at(
        db,
        day_start_utc=window_start,
        day_end_utc=now_utc + timedelta(seconds=1),
    )
    if latest_remote_busy_started is not None:
        db_deferred_until = latest_remote_busy_started + timedelta(seconds=retry_seconds)
        if now_utc < db_deferred_until:
            _critical_catchup_deferred_until[catchup_key] = db_deferred_until
            return 0
    latest_retry_hold_started = await _latest_tg_monitoring_retry_hold_started_at(
        db,
        day_start_utc=window_start,
        day_end_utc=now_utc + timedelta(seconds=1),
    )
    terminal_retry_seconds = _tg_monitoring_terminal_retry_seconds()
    if latest_retry_hold_started is not None:
        db_deferred_until = latest_retry_hold_started + timedelta(
            seconds=terminal_retry_seconds
        )
        if now_utc < db_deferred_until:
            _critical_catchup_deferred_until[catchup_key] = db_deferred_until
            logging.warning(
                "SCHED critical watchdog deferring tg_monitoring catch-up after "
                "recent terminal run scheduled_local=%s now_local=%s retry_seconds=%s",
                scheduled_local.isoformat(),
                now_local.isoformat(),
                terminal_retry_seconds,
            )
            return 0
    if await _tg_monitoring_recovery_job_exists(db=db, bot=bot):
        _critical_catchup_deferred_until[catchup_key] = now_utc + timedelta(seconds=retry_seconds)
        logging.warning(
            "SCHED critical watchdog deferring tg_monitoring catch-up while recovery registry exists "
            "scheduled_local=%s now_local=%s retry_seconds=%s",
            scheduled_local.isoformat(),
            now_local.isoformat(),
            retry_seconds,
        )
        return 0

    from source_parsing.telegram.service import telegram_monitor_scheduler

    _critical_catchup_inflight.add(catchup_key)
    try:
        catchup_run_id = f"catchup-tg-monitoring-{uuid4().hex}"
        logging.error(
            "SCHED critical watchdog dispatching missing tg_monitoring slot "
            "scheduled_local=%s now_local=%s run_id=%s",
            scheduled_local.isoformat(),
            now_local.isoformat(),
            catchup_run_id,
        )
        async with heavy_operation(
            kind="tg_monitoring",
            trigger="scheduled",
            mode="wait",
            run_id=catchup_run_id,
            operator_id=0,
        ):
            await telegram_monitor_scheduler(db, bot, run_id=catchup_run_id)
        if await _ops_run_delivery_exists(
            db,
            kind="tg_monitoring",
            day_start_utc=window_start,
            day_end_utc=datetime.now(timezone.utc) + timedelta(seconds=1),
            triggers=_TG_MONITORING_DELIVERY_TRIGGERS,
        ):
            _critical_catchup_completed.add(catchup_key)
            _critical_catchup_deferred_until.pop(catchup_key, None)
        else:
            latest_remote_busy_started = await _latest_tg_monitoring_remote_busy_skip_started_at(
                db,
                day_start_utc=window_start,
                day_end_utc=datetime.now(timezone.utc) + timedelta(seconds=1),
            )
            if latest_remote_busy_started is not None:
                _critical_catchup_deferred_until[catchup_key] = (
                    latest_remote_busy_started + timedelta(seconds=retry_seconds)
                )
                logging.warning(
                    "SCHED critical watchdog deferring tg_monitoring catch-up after remote session busy "
                    "scheduled_local=%s now_local=%s retry_seconds=%s",
                    scheduled_local.isoformat(),
                    now_local.isoformat(),
                    retry_seconds,
                )
            else:
                latest_retry_hold_started = (
                    await _latest_tg_monitoring_retry_hold_started_at(
                        db,
                        day_start_utc=window_start,
                        day_end_utc=datetime.now(timezone.utc)
                        + timedelta(seconds=1),
                    )
                )
                if latest_retry_hold_started is not None:
                    _critical_catchup_deferred_until[catchup_key] = (
                        latest_retry_hold_started
                        + timedelta(seconds=terminal_retry_seconds)
                    )
                    logging.warning(
                        "SCHED critical watchdog deferring tg_monitoring catch-up "
                        "after terminal run scheduled_local=%s now_local=%s "
                        "retry_seconds=%s",
                        scheduled_local.isoformat(),
                        now_local.isoformat(),
                        terminal_retry_seconds,
                    )
        return 1
    finally:
        _critical_catchup_inflight.discard(catchup_key)


def _last_vk_auto_import_slot(now_utc: datetime) -> tuple[datetime, datetime, datetime] | None:
    times_raw = os.getenv(
        "VK_AUTO_IMPORT_TIMES_LOCAL", "06:15,10:15,12:00,15:30,18:30"
    ).strip()
    tz_name = os.getenv("VK_AUTO_IMPORT_TZ", "Europe/Kaliningrad").strip()
    candidates: list[tuple[datetime, datetime, datetime]] = []
    for raw in times_raw.split(","):
        time_raw = raw.strip()
        if not time_raw:
            continue
        candidates.append(
            _last_local_slot(
                now_utc=now_utc,
                tz_name=tz_name,
                time_raw=time_raw,
                default_hour=6,
                default_minute=30,
                label="VK_AUTO_IMPORT_TIMES_LOCAL",
            )
        )
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[2])


async def _maybe_dispatch_vk_auto_import_watchdog(db: Any, bot: Any) -> int:
    if not _env_enabled("ENABLE_VK_AUTO_IMPORT", default=False):
        return 0
    now_utc = datetime.now(timezone.utc)
    slot = _last_vk_auto_import_slot(now_utc)
    if slot is None:
        return 0
    now_local, scheduled_local, scheduled_utc = slot
    watchdog_delay_sec = max(
        _critical_sched_watchdog_grace_seconds(),
        _vk_auto_import_misfire_grace_seconds() + 120,
    )
    if now_utc <= scheduled_utc + timedelta(seconds=watchdog_delay_sec):
        return 0
    if now_utc - scheduled_utc > timedelta(seconds=_vk_auto_import_catchup_lookback_seconds()):
        return 0

    window_start = scheduled_utc - timedelta(minutes=5)
    if await _ops_run_delivery_exists(
        db,
        kind="vk_auto_import",
        day_start_utc=window_start,
        day_end_utc=now_utc + timedelta(seconds=1),
        triggers={"scheduled"},
    ):
        return 0

    catchup_key = f"vk_auto_import:{scheduled_local.isoformat()}"
    if catchup_key in _critical_catchup_completed:
        return 0
    if catchup_key in _critical_catchup_inflight:
        return 0

    from vk_auto_queue import vk_auto_import_scheduler

    _critical_catchup_inflight.add(catchup_key)
    try:
        catchup_run_id = f"catchup-vk-auto-import-{uuid4().hex}"
        logging.error(
            "SCHED critical watchdog dispatching missing vk_auto_import slot "
            "scheduled_local=%s now_local=%s run_id=%s",
            scheduled_local.isoformat(),
            now_local.isoformat(),
            catchup_run_id,
        )
        await vk_auto_import_scheduler(db, bot, run_id=catchup_run_id)
        if await _ops_run_delivery_exists(
            db,
            kind="vk_auto_import",
            day_start_utc=window_start,
            day_end_utc=datetime.now(timezone.utc) + timedelta(seconds=1),
            triggers={"scheduled"},
        ):
            _critical_catchup_completed.add(catchup_key)
        return 1
    finally:
        _critical_catchup_inflight.discard(catchup_key)


async def maybe_dispatch_critical_scheduler_watchdog(db: Any, bot: Any) -> int:
    dispatched = 0
    dispatched += await _maybe_dispatch_tg_monitoring_watchdog(db, bot)
    dispatched += await _maybe_dispatch_guide_critical_watchdog(db, bot)
    dispatched += await _maybe_dispatch_vk_auto_import_watchdog(db, bot)
    return dispatched


# Partner-track scheduling: defaults are intentionally hard-coded — the user
# explicitly asked NOT to gate this behind feature flags so the schedule cannot
# silently regress to "off". Per-track times can still be moved via env override
# without redeploy (V_PARTNER_TRACK_<NAME>_TIME_LOCAL).
PARTNER_TRACK_TZ = "Europe/Kaliningrad"
PARTNER_TRACK_DEFAULT_TIMES: dict[str, str] = {
    "partner_eco_nature_001": "12:30",
    # КОНБ launches 7 min after the eco/nature slot per operator brief
    # (round-3 prod launch, 2026-05-17): «через 7 минут после запуска
    # эко-природы».
    "partner_konb_library_001": "12:37",
    "partner_region_east_001": "18:30",
}
# Hard deadline (local time): after this the watchdog stops retrying for today
# and notifies the admin chat. Picked at 22:00 to leave a full hour after the
# nominal east slot for CPU-fallback renders, while not bleeding into the next
# day's slot.
PARTNER_TRACK_RETRY_DEADLINE_LOCAL = "22:00"
PARTNER_TRACK_WATCHDOG_GRACE_SECONDS = 60
# One scheduled attempt plus one recovery attempt is enough to distinguish a
# transient handoff failure from a deterministic render/configuration failure.
# Without this persisted cap the ten-minute watchdog cadence can create a
# same-day Kaggle retry storm until the 22:00 deadline.
PARTNER_TRACK_FAILED_SESSION_RETRY_CAP = 2


def _partner_track_time_local(partner_track_id: str) -> str:
    env_key = {
        "partner_eco_nature_001": "V_PARTNER_TRACK_ECO_TIME_LOCAL",
        "partner_konb_library_001": "V_PARTNER_TRACK_KONB_TIME_LOCAL",
        "partner_region_east_001": "V_PARTNER_TRACK_EAST_TIME_LOCAL",
    }.get(partner_track_id)
    if env_key:
        raw = (os.getenv(env_key) or "").strip()
        if raw:
            return raw
    return PARTNER_TRACK_DEFAULT_TIMES.get(partner_track_id, "12:00")


async def _partner_track_failed_session_count_today(
    db: Any,
    *,
    day_start_utc: datetime,
    day_end_utc: datetime,
    target_date: str,
    profile_key: str,
) -> int:
    """Count persisted scheduled failures for one partner slot.

    The count deliberately comes from ``videoannounce_session`` rather than
    in-memory watchdog state so deploys/restarts cannot reset the retry budget.
    """
    if db is None or not hasattr(db, "raw_conn"):
        return 0
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT status, selection_params
            FROM videoannounce_session
            WHERE profile_key = ?
              AND created_at >= ?
              AND created_at < ?
            ORDER BY id DESC
            """,
            (
                profile_key,
                _utc_sql_text(day_start_utc),
                _utc_sql_text(day_end_utc),
            ),
        )
        rows = await cur.fetchall()
    count = 0
    for status, selection_params_raw in rows:
        if str(status or "").strip() != "FAILED":
            continue
        params: dict[str, Any] = {}
        if isinstance(selection_params_raw, str) and selection_params_raw.strip():
            try:
                parsed = json.loads(selection_params_raw)
            except Exception:
                parsed = {}
            if isinstance(parsed, dict):
                params = parsed
        elif isinstance(selection_params_raw, dict):
            params = selection_params_raw
        if str(params.get("target_date") or "").strip() != target_date:
            continue
        if str(params.get("trigger") or "").strip() not in {
            "scheduled",
            "startup_catchup",
        }:
            continue
        count += 1
    return count


async def maybe_dispatch_partner_track_watchdog(
    db: Any, bot: Any, partner_track_id: str
) -> bool:
    """Re-dispatch a missed partner-track slot when render lane frees up.

    Returns True iff a launch was actually attempted. Stops retrying for the
    day after ``PARTNER_TRACK_RETRY_DEADLINE_LOCAL`` to avoid posting at
    unreasonable hours. Anti-repeat by ``profile_key`` keeps re-runs idempotent.
    """
    try:
        from video_announce.partner_tracks import get_partner_track
    except Exception:
        logging.exception(
            "SCHED partner_track watchdog: import failed track=%s", partner_track_id
        )
        return False
    partner_track = get_partner_track(partner_track_id)
    if partner_track is None:
        return False

    tz = _safe_zoneinfo(PARTNER_TRACK_TZ, label="PARTNER_TRACK_TZ")
    time_raw = _partner_track_time_local(partner_track_id)
    hour_local, minute_local = _parse_hhmm(
        time_raw,
        default_hour=12,
        default_minute=0,
        label=f"V_PARTNER_TRACK_{partner_track.callback_action.upper()}_TIME_LOCAL",
    )
    deadline_hour, deadline_minute = _parse_hhmm(
        PARTNER_TRACK_RETRY_DEADLINE_LOCAL,
        default_hour=22,
        default_minute=0,
        label="PARTNER_TRACK_RETRY_DEADLINE_LOCAL",
    )
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(tz)
    scheduled_local = now_local.replace(
        hour=hour_local, minute=minute_local, second=0, microsecond=0
    )
    deadline_local = now_local.replace(
        hour=deadline_hour, minute=deadline_minute, second=0, microsecond=0
    )
    if now_local <= scheduled_local + timedelta(
        seconds=PARTNER_TRACK_WATCHDOG_GRACE_SECONDS
    ):
        return False  # cron slot has not fired yet (or grace not elapsed)
    if now_local > deadline_local:
        return False  # missed slot for today; do not retry overnight

    day_start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_local = day_start_local + timedelta(days=1)
    target_date = now_local.date().isoformat()
    if await _partner_track_session_exists_today(
        db,
        day_start_utc=day_start_local.astimezone(timezone.utc),
        day_end_utc=day_end_local.astimezone(timezone.utc),
        target_date=target_date,
        profile_key=partner_track.profile_key,
    ):
        return False

    failed_session_count = await _partner_track_failed_session_count_today(
        db,
        day_start_utc=day_start_local.astimezone(timezone.utc),
        day_end_utc=day_end_local.astimezone(timezone.utc),
        target_date=target_date,
        profile_key=partner_track.profile_key,
    )
    if failed_session_count >= PARTNER_TRACK_FAILED_SESSION_RETRY_CAP:
        logging.error(
            "SCHED partner_track watchdog retry cap reached track=%s "
            "target_date=%s failed_sessions=%s cap=%s",
            partner_track.track_id,
            target_date,
            failed_session_count,
            PARTNER_TRACK_FAILED_SESSION_RETRY_CAP,
        )
        return False

    if partner_track.track_id == "partner_region_east_001":
        missing_business_attempts = await _partner_track_skip_attempts_today(
            db,
            day_start_utc=day_start_local.astimezone(timezone.utc),
            day_end_utc=day_end_local.astimezone(timezone.utc),
            kind=f"video_partner_{partner_track.callback_action}",
            partner_track_id=partner_track.track_id,
            skip_reason="missing_business_target",
        )
        if missing_business_attempts >= 2:
            logging.info(
                "SCHED partner_track watchdog deferring until tomorrow track=%s "
                "reason=missing_business_target attempts=%s",
                partner_track.track_id,
                missing_business_attempts,
            )
            return False

    logging.error(
        "SCHED partner_track watchdog dispatching missing slot track=%s "
        "scheduled_local=%s now_local=%s",
        partner_track.track_id,
        scheduled_local.isoformat(),
        now_local.isoformat(),
    )
    await _run_scheduled_partner_track(
        db, bot, partner_track.track_id, startup_catchup=False
    )
    return True


async def maybe_dispatch_popular_review_watchdog(db: Any, bot: Any) -> bool:
    enabled, tz_name, time_raw = _popular_review_schedule_settings()
    if not enabled:
        return False
    review_tz = _safe_zoneinfo(tz_name, label="V_POPULAR_REVIEW_TZ")
    hour_local, minute_local = _parse_hhmm(
        time_raw,
        default_hour=10,
        default_minute=15,
        label="V_POPULAR_REVIEW_TIME_LOCAL",
    )
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(review_tz)
    scheduled_local = now_local.replace(
        hour=hour_local,
        minute=minute_local,
        second=0,
        microsecond=0,
    )
    if now_local <= scheduled_local + timedelta(seconds=_popular_review_watchdog_grace_seconds()):
        return False

    day_start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_local = day_start_local + timedelta(days=1)
    target_date = now_local.date().isoformat()
    if await _popular_review_session_exists_today(
        db,
        day_start_utc=day_start_local.astimezone(timezone.utc),
        day_end_utc=day_end_local.astimezone(timezone.utc),
        target_date=target_date,
    ):
        return False

    failed_session_count = await _popular_review_failed_session_count_today(
        db,
        day_start_utc=day_start_local.astimezone(timezone.utc),
        day_end_utc=day_end_local.astimezone(timezone.utc),
        target_date=target_date,
    )
    if failed_session_count >= POPULAR_REVIEW_FAILED_SESSION_RETRY_CAP:
        logging.error(
            "SCHED video_popular_review watchdog retry cap reached "
            "target_date=%s failed_sessions=%s cap=%s",
            target_date,
            failed_session_count,
            POPULAR_REVIEW_FAILED_SESSION_RETRY_CAP,
        )
        return False

    logging.error(
        "SCHED watchdog dispatching missing live video_popular_review slot scheduled_local=%s now_local=%s",
        scheduled_local.isoformat(),
        now_local.isoformat(),
    )
    await _run_scheduled_popular_review(db, bot, startup_catchup=False)
    return True


def runtime_health_status() -> dict[str, Any]:
    enabled, _, _, _, _ = _video_tomorrow_schedule_settings()
    popular_review_enabled, _, _ = _popular_review_schedule_settings()
    guide_enabled, _, _ = _guide_monitoring_schedule_settings()
    guide_visual_enabled = _env_enabled("ENABLE_GUIDE_VISUAL_DIGEST_SCHEDULED", default=False)
    promo_vk_enabled = _env_enabled("ENABLE_PROMO_VK_SCHEDULER", default=True)
    is_prod = os.getenv("DEV_MODE") != "1" and os.getenv("PYTEST_CURRENT_TEST") is None
    tg_monitoring_enabled = _env_enabled("ENABLE_TG_MONITORING", default=is_prod)
    vk_auto_import_enabled = _env_enabled("ENABLE_VK_AUTO_IMPORT", default=False)
    email_worker_enabled = _env_enabled("ENABLE_EMAIL_OUTBOX_WORKER", default=False)
    email_monitor_enabled = _env_enabled("ENABLE_EMAIL_OUTBOX_MONITOR", default=False)
    region_talk_enabled = _env_enabled("ENABLE_REGION_TALK_SCHEDULED", default=False)
    critical_watchdog_enabled = (
        tg_monitoring_enabled or guide_enabled or vk_auto_import_enabled
    )
    scheduler = _scheduler
    payload: dict[str, Any] = {
        "scheduler": "missing" if scheduler is None else "unknown",
        "critical_scheduler_watchdog": "disabled",
        "tg_monitoring": "disabled",
        "vk_auto_import": "disabled",
        "email_outbox_worker": "disabled",
        "email_outbox_monitor": "disabled",
        "region_talk": "disabled",
        "region_talk_watchdog": "disabled",
        "video_tomorrow": "disabled",
        "video_popular_review": "disabled",
        "video_popular_review_watchdog": "disabled",
        "promo_vk": "disabled",
        "guide_excursions_light": "disabled",
        "guide_excursions_full": "disabled",
        "guide_visual_digest": "disabled",
        "guide_visual_digest_vk_story_due": "disabled",
        "kenigsberg_story_daily": "disabled" if os.getenv("DEV_MODE") == "1" else "unknown",
    }
    if scheduler is None:
        if enabled:
            payload["video_tomorrow"] = "missing_scheduler"
        if popular_review_enabled:
            payload["video_popular_review"] = "missing_scheduler"
            payload["video_popular_review_watchdog"] = "missing_scheduler"
        if promo_vk_enabled:
            payload["promo_vk"] = "missing_scheduler"
        if critical_watchdog_enabled:
            payload["critical_scheduler_watchdog"] = "missing_scheduler"
        if tg_monitoring_enabled:
            payload["tg_monitoring"] = "missing_scheduler"
        if vk_auto_import_enabled:
            payload["vk_auto_import"] = "missing_scheduler"
        if email_worker_enabled:
            payload["email_outbox_worker"] = "missing_scheduler"
        if email_monitor_enabled:
            payload["email_outbox_monitor"] = "missing_scheduler"
        if region_talk_enabled:
            payload["region_talk"] = "missing_scheduler"
            payload["region_talk_watchdog"] = "missing_scheduler"
        if guide_enabled:
            payload["guide_excursions_light"] = "missing_scheduler"
            payload["guide_excursions_full"] = "missing_scheduler"
        if guide_visual_enabled:
            payload["guide_visual_digest"] = "missing_scheduler"
            if _env_enabled("ENABLE_GUIDE_VISUAL_DIGEST_VK_STORIES", default=False):
                payload["guide_visual_digest_vk_story_due"] = "missing_scheduler"
        return payload

    try:
        running = bool(getattr(scheduler, "running"))
    except Exception:
        running = False
    payload["scheduler"] = "ok" if running else "stopped"

    def _set_job_health(key: str, job_id: str) -> None:
        try:
            job = scheduler.get_job(job_id)
        except Exception:
            payload[key] = "lookup_error"
            return
        next_run = _job_next_run(job) if job else None
        payload[key] = "ok" if next_run is not None else "missing"
        if next_run is not None:
            payload[f"{key}_next_run"] = (
                next_run.isoformat() if hasattr(next_run, "isoformat") else str(next_run)
            )

    if popular_review_enabled:
        _set_job_health("video_popular_review", "video_popular_review")
        _set_job_health("video_popular_review_watchdog", "video_popular_review_watchdog")

    if promo_vk_enabled:
        _set_job_health("promo_vk", "promo_vk")

    if critical_watchdog_enabled:
        _set_job_health("critical_scheduler_watchdog", "critical_scheduler_watchdog")

    if tg_monitoring_enabled:
        _set_job_health("tg_monitoring", "tg_monitoring")

    if email_worker_enabled:
        _set_job_health("email_outbox_worker", "email_outbox_worker")

    if email_monitor_enabled:
        _set_job_health("email_outbox_monitor", "email_outbox_monitor")

    if region_talk_enabled:
        try:
            jobs = list(scheduler.get_jobs()) if hasattr(scheduler, "get_jobs") else []
        except Exception:
            jobs = []
            payload["region_talk"] = "lookup_error"
        if jobs or payload.get("region_talk") != "lookup_error":
            region_jobs = [
                job for job in jobs
                if str(getattr(job, "id", "")).removeprefix("region_talk_").isdigit()
            ]
            if not region_jobs:
                idx = 0
                while True:
                    try:
                        job = scheduler.get_job(f"region_talk_{idx}")
                    except Exception:
                        job = None
                    if job is None:
                        break
                    region_jobs.append(job)
                    idx += 1
            region_next = [_job_next_run(job) for job in region_jobs]
            region_next = [value for value in region_next if value is not None]
            payload["region_talk"] = "ok" if region_next else "missing"
            if region_next:
                first_next = min(region_next)
                payload["region_talk_next_run"] = (
                    first_next.isoformat() if hasattr(first_next, "isoformat") else str(first_next)
                )
        _set_job_health("region_talk_watchdog", "region_talk_watchdog")

    if vk_auto_import_enabled:
        try:
            jobs = list(scheduler.get_jobs()) if hasattr(scheduler, "get_jobs") else []
        except Exception:
            jobs = []
            payload["vk_auto_import"] = "lookup_error"
        if jobs or payload.get("vk_auto_import") != "lookup_error":
            vk_jobs = [
                job for job in jobs if str(getattr(job, "id", "")).startswith("vk_auto_import_")
            ]
            if not vk_jobs:
                idx = 0
                while True:
                    try:
                        job = scheduler.get_job(f"vk_auto_import_{idx}")
                    except Exception:
                        job = None
                    if job is None:
                        break
                    vk_jobs.append(job)
                    idx += 1
            vk_next = [_job_next_run(job) for job in vk_jobs]
            vk_next = [value for value in vk_next if value is not None]
            payload["vk_auto_import"] = "ok" if vk_next else "missing"
            if vk_next:
                first_next = min(vk_next)
                payload["vk_auto_import_next_run"] = (
                    first_next.isoformat() if hasattr(first_next, "isoformat") else str(first_next)
                )

    if guide_enabled:
        try:
            full_job = scheduler.get_job("guide_excursions_full")
        except Exception:
            payload["guide_excursions_full"] = "lookup_error"
        else:
            next_run = _job_next_run(full_job) if full_job else None
            payload["guide_excursions_full"] = "ok" if next_run is not None else "missing"
            if next_run is not None:
                payload["guide_excursions_full_next_run"] = (
                    next_run.isoformat() if hasattr(next_run, "isoformat") else str(next_run)
                )
        try:
            jobs = list(scheduler.get_jobs()) if hasattr(scheduler, "get_jobs") else []
        except Exception:
            jobs = []
            payload["guide_excursions_light"] = "lookup_error"
        if jobs or payload.get("guide_excursions_light") != "lookup_error":
            light_jobs = [
                job for job in jobs if str(getattr(job, "id", "")).startswith("guide_excursions_light_")
            ]
            if not light_jobs:
                # Fallback for lightweight test scheduler doubles that do not expose get_jobs().
                idx = 0
                while True:
                    try:
                        job = scheduler.get_job(f"guide_excursions_light_{idx}")
                    except Exception:
                        job = None
                    if job is None:
                        break
                    light_jobs.append(job)
                    idx += 1
            light_next = [_job_next_run(job) for job in light_jobs]
            light_next = [value for value in light_next if value is not None]
            payload["guide_excursions_light"] = "ok" if light_next else "missing"
            if light_next:
                first_next = min(light_next)
                payload["guide_excursions_light_next_run"] = (
                    first_next.isoformat() if hasattr(first_next, "isoformat") else str(first_next)
                )

    if guide_visual_enabled:
        _set_job_health("guide_visual_digest", "guide_visual_digest")
        if _env_enabled("ENABLE_GUIDE_VISUAL_DIGEST_VK_STORIES", default=False):
            _set_job_health("guide_visual_digest_vk_story_due", "guide_visual_digest_vk_story_due")

    if os.getenv("DEV_MODE") != "1":
        try:
            kenigsberg_job = scheduler.get_job("kenigsberg_story_daily")
        except Exception:
            payload["kenigsberg_story_daily"] = "lookup_error"
        else:
            next_run = _job_next_run(kenigsberg_job) if kenigsberg_job else None
            payload["kenigsberg_story_daily"] = "ok" if next_run is not None else "missing"
            if next_run is not None:
                payload["kenigsberg_story_daily_next_run"] = (
                    next_run.isoformat() if hasattr(next_run, "isoformat") else str(next_run)
                )

    if not enabled:
        return payload

    try:
        job = scheduler.get_job("video_tomorrow")
    except Exception:
        payload["video_tomorrow"] = "lookup_error"
        return payload
    if job is None:
        payload["video_tomorrow"] = "missing"
        return payload
    next_run = _job_next_run(job)
    payload["video_tomorrow"] = "ok" if next_run is not None else "missing_next_run"
    if next_run is not None:
        payload["video_tomorrow_next_run"] = (
            next_run.isoformat() if hasattr(next_run, "isoformat") else str(next_run)
        )
    return payload


async def _video_tomorrow_force_marker_exists(db: Any, *, force_token: str) -> bool:
    if db is None or not hasattr(db, "raw_conn"):
        return False
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT status, details_json
            FROM ops_run
            WHERE kind = 'video_tomorrow_force'
            ORDER BY id DESC
            LIMIT 50
            """
        )
        rows = await cur.fetchall()
    for status, details_raw in rows:
        if str(status or "").strip() not in {"running", "success"}:
            continue
        details: dict[str, Any] = {}
        if isinstance(details_raw, str) and details_raw.strip():
            try:
                parsed = json.loads(details_raw)
            except Exception:
                parsed = {}
            if isinstance(parsed, dict):
                details = parsed
        if str(details.get("force_token") or "").strip() == force_token:
            return True
    return False


async def _force_reset_rendering_video_sessions(
    db: Any,
    *,
    reason: str,
) -> list[int]:
    if db is None or not hasattr(db, "get_session"):
        return []
    from sqlalchemy import select

    from models import VideoAnnounceSession, VideoAnnounceSessionStatus

    reset_ids: list[int] = []
    now_utc = datetime.now(timezone.utc)
    async with db.get_session() as session:
        res = await session.execute(
            select(VideoAnnounceSession).where(
                VideoAnnounceSession.status == VideoAnnounceSessionStatus.RENDERING
            )
        )
        rows = res.scalars().all()
        for obj in rows:
            obj.status = VideoAnnounceSessionStatus.FAILED
            obj.finished_at = now_utc
            obj.error = reason
            if obj.id is not None:
                reset_ids.append(int(obj.id))
        if reset_ids:
            await session.commit()
    return reset_ids


async def _maybe_force_video_tomorrow_on_startup(db: Any, bot: Any) -> bool:
    force_token = str(os.getenv("V_TOMORROW_FORCE_RUN_TOKEN") or "").strip()
    force_local_date = str(os.getenv("V_TOMORROW_FORCE_RUN_LOCAL_DATE") or "").strip()
    if not force_token or not force_local_date:
        return False

    enabled, video_tz_name, _video_time_raw, profile_key, video_test_mode = _video_tomorrow_schedule_settings()
    if not enabled:
        logging.warning(
            "SCHED startup force video_tomorrow skipped: schedule disabled for token=%s",
            force_token,
        )
        return False

    video_tz = _safe_zoneinfo(video_tz_name, label="V_TOMORROW_TZ")
    now_local = datetime.now(timezone.utc).astimezone(video_tz)
    if now_local.date().isoformat() != force_local_date:
        return False

    if await _video_tomorrow_force_marker_exists(db, force_token=force_token):
        logging.info(
            "SCHED startup force video_tomorrow skip: token already consumed token=%s",
            force_token,
        )
        return False

    reset_rendering = _env_enabled("V_TOMORROW_FORCE_RESET_RENDERING", default=False)
    normalized_profile_key = (profile_key or "default").strip() or "default"
    details: dict[str, Any] = {
        "force_token": force_token,
        "force_local_date": force_local_date,
        "profile_key": normalized_profile_key,
        "test_mode": bool(video_test_mode),
        "reset_rendering": bool(reset_rendering),
    }
    run_id = await start_ops_run(
        db,
        kind="video_tomorrow_force",
        trigger="startup",
        operator_id=0,
        details=details,
    )
    try:
        if reset_rendering:
            details["reset_session_ids"] = await _force_reset_rendering_video_sessions(
                db,
                reason=f"startup forced scheduled rerun token={force_token}",
            )
        logging.warning(
            "SCHED startup force dispatching video_tomorrow token=%s local_date=%s reset_session_ids=%s",
            force_token,
            force_local_date,
            details.get("reset_session_ids") or [],
        )
        await _run_scheduled_video_tomorrow(
            db,
            bot,
            profile_key=normalized_profile_key,
            test_mode=video_test_mode,
            startup_catchup=False,
        )
    except Exception as exc:
        details["error"] = str(exc) or type(exc).__name__
        await finish_ops_run(
            db,
            run_id=run_id,
            status="failed",
            details=details,
        )
        logging.exception(
            "SCHED startup force video_tomorrow failed token=%s",
            force_token,
        )
        return False

    await finish_ops_run(
        db,
        run_id=run_id,
        status="success",
        details=details,
    )
    return True


async def _run_video_tomorrow_startup_checks(db: Any, bot: Any) -> None:
    forced = await _maybe_force_video_tomorrow_on_startup(db, bot)
    if not forced:
        await _maybe_catch_up_video_tomorrow_on_startup(db, bot)
    await _maybe_catch_up_popular_review_on_startup(db, bot)
    await _maybe_catch_up_kenigsberg_story_on_startup(db, bot)


async def _enqueue_static_site_calendar_refresh(db: Any, *, trigger: str) -> bool:
    """Enqueue the zoned date effect independently from Smart Update."""

    if not _env_enabled("ENABLE_STATIC_SITE_KAGGLE_BUILDER", default=False):
        return False
    main_module = get_running_main()
    enqueue = getattr(main_module, "enqueue_static_site_build_request", None) if main_module else None
    if enqueue is None:
        logging.warning("SCHED static site calendar refresh skipped: main enqueue unavailable")
        return False
    await enqueue(
        db,
        reason="Europe/Kaliningrad local date rollover",
        event_ids=(),
        correlation_id=f"static-site-{trigger}-{datetime.now(timezone.utc).date().isoformat()}",
        delay_seconds=0,
        trigger=trigger,
    )
    logging.info("SCHED static site calendar refresh enqueued trigger=%s", trigger)
    return True


async def _run_startup_catchups(db: Any, bot: Any) -> None:
    await _run_video_tomorrow_startup_checks(db, bot)
    try:
        await _enqueue_static_site_calendar_refresh(db, trigger="startup_catchup")
    except Exception:
        logging.exception("SCHED static site startup catch-up failed")


_region_talk_catchup_inflight: set[str] = set()


def _last_region_talk_slot(now_utc: datetime) -> tuple[datetime, datetime, datetime] | None:
    times_raw = os.getenv(
        "REGION_TALK_TIMES_LOCAL", "06:20,09:50,13:50,17:50,21:50"
    ).strip()
    tz_name = os.getenv("REGION_TALK_TZ", "Europe/Kaliningrad").strip()
    candidates: list[tuple[datetime, datetime, datetime]] = []
    for raw in times_raw.split(","):
        value = raw.strip()
        if not value:
            continue
        try:
            hour, minute = map(int, value.split(":"))
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError
        except ValueError:
            continue
        candidates.append(_last_local_slot(
            now_utc=now_utc,
            tz_name=tz_name,
            time_raw=value,
            default_hour=hour,
            default_minute=minute,
            label="REGION_TALK_TIMES_LOCAL",
        ))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[2])


async def _region_talk_slot_runs(
    db: Any,
    *,
    window_start_utc: datetime,
    window_end_utc: datetime,
) -> list[tuple[str, str]]:
    if db is None or not hasattr(db, "raw_conn"):
        return []
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT status, trigger
            FROM ops_run
            WHERE kind = 'region_talk'
              AND started_at >= ?
              AND started_at < ?
            ORDER BY id DESC
            """,
            (_utc_sql_text(window_start_utc), _utc_sql_text(window_end_utc)),
        )
        rows = await cur.fetchall()
    return [(str(status or "").strip(), str(trigger or "").strip()) for status, trigger in rows]


async def maybe_dispatch_region_talk_watchdog(
    db: Any,
    bot: Any,
    *,
    now_utc: datetime | None = None,
) -> bool:
    """Resume the latest Region Talk slot after a deploy/process interruption."""

    if not _env_enabled("ENABLE_REGION_TALK_SCHEDULED", default=False):
        return False
    current = now_utc or datetime.now(timezone.utc)
    slot = _last_region_talk_slot(current)
    if slot is None:
        return False
    now_local, scheduled_local, scheduled_utc = slot
    try:
        grace_seconds = max(60, min(1800, int(
            (os.getenv("REGION_TALK_WATCHDOG_GRACE_SECONDS") or "300").strip()
        )))
        lookback_seconds = max(900, min(14400, int(
            (os.getenv("REGION_TALK_CATCHUP_LOOKBACK_SECONDS") or "10800").strip()
        )))
        max_attempts = max(1, min(10, int(
            (os.getenv("REGION_TALK_CATCHUP_MAX_ATTEMPTS") or "6").strip()
        )))
    except ValueError:
        grace_seconds, lookback_seconds, max_attempts = 300, 10800, 6
    if current <= scheduled_utc + timedelta(seconds=grace_seconds):
        return False
    if current - scheduled_utc > timedelta(seconds=lookback_seconds):
        return False

    window_start = scheduled_utc - timedelta(minutes=5)
    rows = await _region_talk_slot_runs(
        db,
        window_start_utc=window_start,
        window_end_utc=current + timedelta(seconds=1),
    )
    relevant_triggers = {"scheduled", "startup_catchup", "watchdog_catchup"}
    relevant = [(status, trigger) for status, trigger in rows if trigger in relevant_triggers]
    if any(status in {"running", "success"} for status, _trigger in relevant):
        return False
    if len(relevant) >= max_attempts:
        logging.error(
            "SCHED Region Talk catch-up retry cap reached scheduled_local=%s attempts=%s",
            scheduled_local.isoformat(),
            len(relevant),
        )
        return False

    catchup_key = scheduled_local.isoformat()
    if catchup_key in _region_talk_catchup_inflight:
        return False
    from scripts.region_talk_scheduled_runner import run_region_talk_scheduled

    _region_talk_catchup_inflight.add(catchup_key)
    try:
        run_id = f"watchdog-catchup-{scheduled_utc.strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex}"
        logging.error(
            "SCHED Region Talk watchdog resuming interrupted slot scheduled_local=%s now_local=%s run_id=%s",
            scheduled_local.isoformat(),
            now_local.isoformat(),
            run_id,
        )
        result = await run_region_talk_scheduled(
            db,
            bot,
            scheduler_run_id=run_id,
            ops_trigger="watchdog_catchup",
        )
        return bool(result.get("ok") or result.get("status") == "skipped")
    finally:
        _region_talk_catchup_inflight.discard(catchup_key)


class BatchProgress:
    """Track progress for a batch of event tasks."""

    def __init__(self, total_events: int) -> None:
        self.total_events = total_events
        self.events_done = 0
        self.status: Dict[str, str] = {}

    def register_job(self, key: str) -> None:
        self.status.setdefault(key, "pending")

    def finish_job(self, key: str, status: str = "done") -> None:
        if key in self.status:
            self.status[key] = status

    def event_completed(self) -> None:
        self.events_done += 1

    # Formatting -----------------------------------------------------------------

    def _format_range(self, start: datetime, end: datetime) -> str:
        if start.month == end.month and start.year == end.year:
            name = MONTHS_GEN[start.month]
            return f"{start.day}\u2013{end.day} {name} {start.year}"
        if start.year == end.year:
            s = f"{start.day} {MONTHS_GEN[start.month]}"
            e = f"{end.day} {MONTHS_GEN[end.month]} {start.year}"
        else:
            s = f"{start.day} {MONTHS_GEN[start.month]} {start.year}"
            e = f"{end.day} {MONTHS_GEN[end.month]} {end.year}"
        return f"{s}\u2013{e}"

    def _label(self, key: str) -> str:
        kind, _, ident = key.partition(":")
        if kind == "festival_pages":
            return "Festival"
        if kind == "month_pages":
            _, month = ident.split("-")
            name = MONTHS_NOM[int(month)].capitalize()
            return f"Month: {name}"
        if kind == "week_pages":
            year, week = ident.split("-")
            start = datetime.fromisocalendar(int(year), int(week), 1)
            end = start + timedelta(days=6)
            return f"Week: {self._format_range(start, end)}"
        if kind == "weekend_pages":
            start = datetime.strptime(ident, "%Y-%m-%d")
            end = start + timedelta(days=1)
            return f"Weekend: {self._format_range(start, end)}"
        if kind == "vk_week_post":
            year, week = ident.split("-")
            start = datetime.fromisocalendar(int(year), int(week), 1)
            end = start + timedelta(days=6)
            return f"VK week: {self._format_range(start, end)}"
        if kind == "vk_weekend_post":
            start = datetime.strptime(ident, "%Y-%m-%d")
            end = start + timedelta(days=1)
            return f"VK weekend: {self._format_range(start, end)}"
        return key

    def snapshot_text(self) -> str:
        icon = {
            "pending": "⏳",
            "running": "🔄",
            "deferred": "⏸",
            "captcha": "🧩⏸",
            "captcha_expired": "⚠️",
            "done": "✅",
            "error": "❌",
            "skipped_nochange": "⏭",
        }
        lines = [
            f"Events (Telegraph): {self.events_done}/{self.total_events}"
        ]
        order = {
            "festival_pages": 0,
            "month_pages": 1,
            "week_pages": 2,
            "weekend_pages": 3,
            "vk_week_post": 4,
            "vk_weekend_post": 5,
        }
        for key in sorted(
            self.status.keys(), key=lambda k: (order.get(k.split(":")[0], 99), k)
        ):
            lines.append(f"{icon[self.status[key]]} {self._label(key)}")
        return "\n".join(lines)

    def report(self) -> Dict[str, Any]:
        return {"events": (self.events_done, self.total_events), **self.status}


class CoalescingScheduler:
    def __init__(
        self,
        progress: Optional[BatchProgress] = None,
        debounce_seconds: float = 0.0,
        on_captcha: Optional[Callable[["CoalescingScheduler", str], None]] = None,
    ) -> None:
        self.jobs: Dict[str, Job] = {}
        self.progress = progress
        self.order: List[str] = []
        self.debounce_seconds = debounce_seconds
        self._remaining: Set[str] | None = None
        self.on_captcha = on_captcha

    def add_job(
        self,
        key: str,
        func: Callable[[Any], Awaitable[None]],
        payload: Optional[Any] = None,
        depends_on: Optional[List[str]] = None,
        track: bool = True,
        coalesce: bool = True,
    ) -> None:
        if key in self.jobs:
            job = self.jobs[key]
            if payload is not None and coalesce:
                if isinstance(job.payload, list):
                    if isinstance(payload, list):
                        job.payload.extend(payload)
                    else:
                        job.payload.append(payload)
                else:
                    job.payload = [job.payload, payload]
            job.dirty = True
            if depends_on:
                job.depends_on.update(depends_on)
            return
        job = Job(
            key=key,
            func=func,
            payload=
            []
            if payload is None
            else (
                [payload]
                if coalesce and not isinstance(payload, list)
                else payload
            ),
            depends_on=set(depends_on or []),
            track=track,
        )
        self.jobs[key] = job
        if track and self.progress:
            self.progress.register_job(key)

    async def run(self) -> None:
        if self.debounce_seconds > 0 and self._remaining is None:
            await asyncio.sleep(self.debounce_seconds)
        remaining = self._remaining if self._remaining is not None else set(self.jobs.keys())
        self._remaining = remaining
        completed: Set[str] = set(self.jobs.keys()) - remaining
        while remaining:
            progress_made = False
            for key in list(remaining):
                job = self.jobs[key]
                if job.depends_on - completed:
                    continue
                progress_made = True
                try:
                    if self.progress:
                        self.progress.finish_job(key, "running")
                    await job.func(job.payload)
                except Exception as e:
                    if getattr(e, "code", None) == 14:
                        if self.progress:
                            self.progress.finish_job(key, "captcha")
                        self._remaining = remaining
                        if self.on_captcha:
                            self.on_captcha(self, key)
                        return
                    if self.progress:
                        self.progress.finish_job(key, "error")
                    self._remaining = remaining
                    raise
                if self.progress:
                    self.progress.finish_job(key, "done")
                if job.track:
                    self.order.append(key)
                completed.add(key)
                remaining.remove(key)
            if not progress_made:
                raise RuntimeError("Circular dependency detected")
        self._remaining = None

    @property
    def remaining_jobs(self) -> Set[str]:
        return self._remaining or set()

# Utilities for tests -------------------------------------------------------------------

async def _dummy_job(payload: Any, progress: Optional[BatchProgress] = None) -> None:
    await asyncio.sleep(0)
    if progress and isinstance(payload, dict) and payload.get("event"):
        progress.event_completed()


def schedule_event_batch(
    scheduler: CoalescingScheduler,
    festival_id: int,
    dates: List[str],
) -> None:
    """Schedule tasks for a batch of events belonging to one festival."""

    festival_key = f"festival_pages:{festival_id}"
    scheduler.add_job(festival_key, _dummy_job)

    for idx, date_str in enumerate(dates, 1):
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        week = dt.isocalendar().week
        month_key = f"month_pages:{dt:%Y-%m}"
        week_key = f"week_pages:{dt.year}-{week:02d}"
        vk_week_key = f"vk_week_post:{dt.year}-{week:02d}"
        weekend_key = None
        vk_weekend_key = None
        if dt.weekday() >= 5:
            wstart = dt - timedelta(days=dt.weekday() - 5)
            weekend_key = f"weekend_pages:{wstart:%Y-%m-%d}"
            vk_weekend_key = f"vk_weekend_post:{wstart:%Y-%m-%d}"

        scheduler.add_job(
            f"telegraph:{idx}",
            lambda payload, p=scheduler.progress: _dummy_job(payload, p),
            payload={"event": idx},
            track=False,
            coalesce=False,
        )
        scheduler.add_job(
            month_key,
            _dummy_job,
            payload=idx,
            depends_on=[festival_key],
        )
        scheduler.add_job(
            week_key,
            _dummy_job,
            payload=idx,
            depends_on=[festival_key],
        )
        if weekend_key:
            scheduler.add_job(
                weekend_key,
                _dummy_job,
                payload=idx,
                depends_on=[festival_key],
            )
            wk_dep = [month_key, week_key, weekend_key]
        else:
            wk_dep = [month_key, week_key]
        scheduler.add_job(
            vk_week_key,
            _dummy_job,
            payload=idx,
            depends_on=wk_dep,
        )
        if vk_weekend_key and weekend_key:
            scheduler.add_job(
                vk_weekend_key,
                _dummy_job,
                payload=idx,
                depends_on=wk_dep,
            )


# ---------------------------------------------------------------------------
# APScheduler wrapper used by the main application

_scheduler: AsyncIOScheduler | None = None
_run_meta: dict[str, tuple[str, float]] = {}
_heavy_job_lock = asyncio.Lock()
_VIDEO_TOMORROW_EXISTING_SESSION_STATUSES: set[str] = {
    "SELECTED",
    "RENDERING",
    "DONE",
    "PUBLISH_BLOCKED",
    "PUBLISHED_TEST",
    "PUBLISHED_MAIN",
}

# Jobs that can take minutes/hours (Kaggle/LLM/rendering) and should not overlap in prod.
# `/3di` is intentionally excluded from the shared guard: the preview pipeline already
# serializes itself with its own internal lock and may run alongside unrelated heavy jobs.
_HEAVY_JOB_IDS: set[str] = {
    "tg_monitoring",
    "vk_auto_import",
    "guide_excursions_light",
    "guide_excursions_full",
    "guide_visual_digest",
    "source_parsing",
    "source_parsing_day",
    "festival_queue",
    "nightly_page_sync",
    "telegraph_cache_sanitize",
    "vk_post_prune",
    "event_vector_sync",
}

_OPS_RUN_KIND_BY_JOB_ID: dict[str, str] = {
    "3di_scheduler": "3di",
    "exhibition_duplicate_audit": "exhibition_duplicate_audit",
    "guide_excursions_light": "guide_monitoring",
    "guide_excursions_full": "guide_monitoring",
    "guide_visual_digest": "guide_visual_digest",
    "source_parsing": "parse",
    "source_parsing_day": "parse",
    "region_talk": "region_talk",
}


def _ops_run_kind_for_job(job_id: str) -> str:
    return _OPS_RUN_KIND_BY_JOB_ID.get(job_id, job_id)


async def _record_scheduler_skip(
    db_obj: Any,
    *,
    job_id: str,
    run_id: str | None,
    reason: str,
    blocked_by: Any | None = None,
) -> None:
    if db_obj is None or not hasattr(db_obj, "raw_conn"):
        return
    details: dict[str, Any] = {
        "run_id": run_id,
        "skip_reason": str(reason or "").strip() or "unknown",
        "scheduler_job_id": str(job_id or "").strip() or "scheduler_job",
    }
    blocked_kind = str(getattr(blocked_by, "kind", "") or "").strip()
    blocked_trigger = str(getattr(blocked_by, "trigger", "") or "").strip()
    if blocked_kind:
        details["blocked_by_kind"] = blocked_kind
    if blocked_trigger:
        details["blocked_by_trigger"] = blocked_trigger
    try:
        ops_run_id = await start_ops_run(
            db_obj,
            kind=_ops_run_kind_for_job(job_id),
            trigger="scheduled",
            operator_id=0,
            details=details,
        )
        await finish_ops_run(
            db_obj,
            run_id=ops_run_id,
            status="skipped",
            details=details,
        )
    except Exception:
        logging.warning("SCHED failed to record skipped ops_run job_id=%s", job_id, exc_info=True)


def _job_next_run(job):
    return getattr(job, "next_run_time", None) or getattr(job, "next_run_at", None)


def _job_wrapper(job_id: str, func, *, notify_skip: Callable[[str, str], None] | None = None):
    async def _run(*args, **kwargs):
        serialize_heavy = (os.getenv("SCHED_SERIALIZE_HEAVY_JOBS") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        is_heavy = job_id in _HEAVY_JOB_IDS
        guard_mode_raw = (os.getenv("SCHED_HEAVY_GUARD_MODE") or "").strip().lower()
        if guard_mode_raw in {"0", "off", "false", "no", "disable", "disabled"}:
            guard_mode = "off"
        elif guard_mode_raw in {"wait", "block", "serialize"}:
            guard_mode = "wait"
        elif guard_mode_raw in {"skip", "try", "nonblocking", "non-blocking"}:
            guard_mode = "skip"
        else:
            # Backwards-compatible default: old "serialize" mode implies waiting.
            guard_mode = "wait" if serialize_heavy else "skip"
        timeout_raw = (os.getenv("SCHED_HEAVY_TRY_TIMEOUT_SEC") or "0.2").strip()
        try:
            guard_timeout = max(0.0, float(timeout_raw))
        except ValueError:
            guard_timeout = 0.2
        run_id, start = _run_meta.get(job_id, (uuid4().hex, _time.perf_counter()))
        done = asyncio.Event()

        async def heartbeat():
            while not done.is_set():
                await asyncio.sleep(10)
                took_ms = (_time.perf_counter() - start) * 1000
                logging.info(
                    "job_heartbeat job_id=%s run_id=%s took_ms=%.0f",
                    job_id,
                    run_id,
                    took_ms,
                )

        async def _execute():
            hb_task = asyncio.create_task(heartbeat())
            try:
                return await func(*args, run_id=run_id, **kwargs)
            finally:
                done.set()
                hb_task.cancel()

        async def _run_guarded():
            if not is_heavy or guard_mode == "off":
                return await _execute()

            if guard_mode == "skip":
                async with heavy_operation(
                    kind=job_id,
                    trigger="scheduled",
                    mode="try",
                    timeout_sec=guard_timeout,
                    run_id=run_id,
                    operator_id=0,
                ) as acquired:
                    if not acquired:
                        meta = current_heavy_meta()
                        meta_txt = describe_heavy_meta(meta)
                        await _record_scheduler_skip(
                            args[0] if args else None,
                            job_id=job_id,
                            run_id=run_id,
                            reason="heavy_busy",
                            blocked_by=meta,
                        )
                        logging.info(
                            "job_skip_heavy_busy job_id=%s run_id=%s current=%s",
                            job_id,
                            run_id,
                            meta_txt,
                        )
                        if notify_skip:
                            notify_skip(job_id, f"идёт другая тяжёлая операция: {meta_txt}")
                        return None
                    return await _execute()

            async with heavy_operation(
                kind=job_id,
                trigger="scheduled",
                mode="wait",
                run_id=run_id,
                operator_id=0,
            ):
                return await _execute()

        if serialize_heavy and is_heavy:
            if _heavy_job_lock.locked():
                logging.info("job_wait_heavy_lock job_id=%s run_id=%s", job_id, run_id)
            async with _heavy_job_lock:
                return await _run_guarded()
        return await _run_guarded()

    return _run


def _on_event(event):
    if not hasattr(event, "job_id"):
        logging.debug(
            "scheduler event %s (no job_id), ignored", getattr(event, "code", None)
        )
        return
    job_id = event.job_id
    name_map = {
        EVENT_JOB_SUBMITTED: "JOB_SUBMITTED",
        EVENT_JOB_EXECUTED: "JOB_EXECUTED",
        EVENT_JOB_ERROR: "JOB_ERROR",
        EVENT_JOB_MISSED: "JOB_MISSED",
    }
    event_name = name_map.get(event.code, str(event.code))
    run_id = None
    start = None
    if event.code == EVENT_JOB_SUBMITTED:
        run_id = uuid4().hex
        start = _time.perf_counter()
        _run_meta[job_id] = (run_id, start)
    else:
        run_id, start = _run_meta.get(job_id, (uuid4().hex, None))
    took_ms = None
    if event.code in (EVENT_JOB_EXECUTED, EVENT_JOB_ERROR) and start is not None:
        took_ms = (_time.perf_counter() - start) * 1000
        _run_meta.pop(job_id, None)
    if event.code == EVENT_JOB_MISSED:
        _run_meta.pop(job_id, None)
        run_id = uuid4().hex
    next_run = None
    if _scheduler:
        job = _scheduler.get_job(job_id)
        next_run = _job_next_run(job) if job else None
    tb_excerpt = None
    tb = getattr(event, "traceback", None)
    if tb:
        tb_excerpt = " | ".join(tb.strip().splitlines()[-3:])
    logging.info(
        "%s job_id=%s run_id=%s next_run=%s took_ms=%s traceback_excerpt=%s",
        event_name,
        job_id,
        run_id,
        next_run,
        f"{took_ms:.0f}" if took_ms is not None else "0",
        tb_excerpt,
    )


def startup(
    db,
    bot,
    *,
    vk_scheduler=None,
    vk_poll_scheduler=None,
    vk_crawl_cron=None,
    cleanup_scheduler=None,
    partner_notification_scheduler=None,
    nightly_page_sync=None,
    rebuild_fest_nav_if_changed=None,
) -> AsyncIOScheduler:
    global _scheduler
    if _scheduler is None:
        executor = AsyncIOExecutor()
        _scheduler = AsyncIOScheduler(executors={"default": executor}, timezone="UTC")
        _scheduler.configure(
            job_defaults={
                "max_instances": 1,
                "coalesce": True,
                "misfire_grace_time": 30,
            }
        )

    is_prod = os.getenv("DEV_MODE") != "1" and os.getenv("PYTEST_CURRENT_TEST") is None

    main_module = None

    async def _notify_admin_skip_async(job_name: str, reason: str) -> None:
        chat_id = await resolve_superadmin_chat_id(db)
        if not chat_id:
            return
        if bot is None or not hasattr(bot, "send_message"):
            return
        text = f"⚠️ SCHED: пропуск {job_name}. Причина: {reason}"
        try:
            await bot.send_message(chat_id, text)
        except Exception:
            logging.exception("SCHED failed to notify admin chat")

    def _notify_admin_skip(job_name: str, reason: str) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logging.warning("SCHED failed to notify admin: no running event loop")
            return
        except Exception:
            logging.exception("SCHED failed to notify admin chat")
            return
        try:
            loop.create_task(_notify_admin_skip_async(job_name, reason))
        except Exception:
            logging.exception("SCHED failed to notify admin chat")

    def resolve(name: str, value):
        nonlocal main_module
        if value is not None:
            return value
        if main_module is None:
            main_module = get_running_main()
        if main_module is None:
            raise RuntimeError(
                f"{name} not provided and main module is not loaded"
            )
        try:
            return getattr(main_module, name)
        except AttributeError as exc:  # pragma: no cover - defensive
            raise RuntimeError(
                f"running main module does not define {name!r}"
            ) from exc

    vk_scheduler = resolve("vk_scheduler", vk_scheduler)
    vk_poll_scheduler = resolve("vk_poll_scheduler", vk_poll_scheduler)
    vk_crawl_cron = resolve("vk_crawl_cron", vk_crawl_cron)
    cleanup_scheduler = resolve("cleanup_scheduler", cleanup_scheduler)
    partner_notification_scheduler = resolve(
        "partner_notification_scheduler", partner_notification_scheduler
    )
    rebuild_fest_nav_if_changed = resolve(
        "rebuild_fest_nav_if_changed", rebuild_fest_nav_if_changed
    )
    nightly_page_sync = (
        resolve("nightly_page_sync", nightly_page_sync)
        if os.getenv("ENABLE_NIGHTLY_PAGE_SYNC") == "1"
        else nightly_page_sync
    )

    def _register_job(job_id: str, *args, **kwargs):
        try:
            job = _scheduler.add_job(*args, **kwargs)
        except Exception:
            logging.exception("SCHED failed to register job id=%s", job_id)
            return None
        logging.info(
            "SCHED registered job id=%s next_run=%s", job.id, _job_next_run(job)
        )
        return job

    if _env_enabled("SMART_UPDATE_RETRY_WORKER_ENABLED", default=True):
        try:
            smart_update_retry_interval = max(
                15,
                min(
                    3600,
                    int(os.getenv("SMART_UPDATE_RETRY_INTERVAL_SECONDS", "60") or "60"),
                ),
            )
        except ValueError:
            smart_update_retry_interval = 60
        try:
            smart_update_retry_batch = max(
                1,
                min(
                    100,
                    int(os.getenv("SMART_UPDATE_RETRY_BATCH_SIZE", "25") or "25"),
                ),
            )
        except ValueError:
            smart_update_retry_batch = 25

        async def smart_update_retry_scheduler(
            db_obj,
            bot_obj,
            *,
            run_id: str | None = None,
        ) -> None:
            del run_id
            from smart_event_update import retry_due_smart_update_candidates
            from smart_update_state import smart_update_funnel_counts

            accepted: list[tuple[str, int]] = []

            async def _capture_accepted(_candidate, result) -> None:
                if not result.is_accepted:
                    return
                if result.event_id is not None:
                    accepted.append((result.outcome.value, int(result.event_id)))

            counters = await retry_due_smart_update_candidates(
                db_obj,
                limit=smart_update_retry_batch,
                on_accepted=_capture_accepted,
            )
            await _notify_smart_update_retry_accepts(db_obj, bot_obj, accepted)
            funnel = await smart_update_funnel_counts(db_obj)
            logging.info(
                "smart_update_retry_worker counters=%s funnel=%s",
                counters,
                funnel,
            )

        _register_job(
            "smart_update_retry_worker",
            _job_wrapper("smart_update_retry_worker", smart_update_retry_scheduler),
            "interval",
            id="smart_update_retry_worker",
            seconds=smart_update_retry_interval,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=smart_update_retry_interval,
        )
    else:
        logging.info(
            "SCHED skipping smart_update_retry_worker "
            "(SMART_UPDATE_RETRY_WORKER_ENABLED!=1)"
        )

    if _env_enabled("VK_CRAWL_CONTINUATION_WORKER_ENABLED", default=True):
        def _bounded_int(name: str, default: int, low: int, high: int) -> int:
            try:
                return max(low, min(high, int(os.getenv(name, str(default)) or default)))
            except ValueError:
                return default

        continuation_interval = _bounded_int(
            "VK_CRAWL_CONTINUATION_INTERVAL_SECONDS", 60, 15, 3600
        )
        continuation_batch = _bounded_int(
            "VK_CRAWL_CONTINUATION_BATCH_SIZE", 2, 1, 25
        )
        continuation_pages = _bounded_int(
            "VK_CRAWL_CONTINUATION_PAGES_PER_JOB", 3, 1, 25
        )
        continuation_lease = _bounded_int(
            "VK_CRAWL_CONTINUATION_LEASE_SECONDS", 300, 30, 3600
        )

        async def durable_vk_crawl_continuation_scheduler(
            db_obj,
            bot_obj,
            *,
            run_id: str | None = None,
        ) -> None:
            from vk_intake import vk_crawl_continuation_scheduler

            counters = await vk_crawl_continuation_scheduler(
                db_obj,
                bot_obj,
                run_id=run_id,
                max_jobs=continuation_batch,
                max_pages_per_job=continuation_pages,
                lease_seconds=continuation_lease,
            )
            logging.info("vk_crawl_continuation_worker counters=%s", counters)

        _register_job(
            "vk_crawl_continuation_worker",
            _job_wrapper(
                "vk_crawl_continuation_worker",
                durable_vk_crawl_continuation_scheduler,
            ),
            "interval",
            id="vk_crawl_continuation_worker",
            seconds=continuation_interval,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=continuation_interval,
        )
    else:
        logging.info(
            "SCHED skipping vk_crawl_continuation_worker "
            "(VK_CRAWL_CONTINUATION_WORKER_ENABLED!=1)"
        )

    enable_core_schedulers = _env_enabled("ENABLE_CORE_SCHEDULERS", default=True)
    if enable_core_schedulers:
        _register_job(
            "vk_scheduler",
            _job_wrapper("vk_scheduler", vk_scheduler, notify_skip=_notify_admin_skip),
            "cron",
            id="vk_scheduler",
            minute="1,16,31,46",
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
        )
        _register_job(
            "vk_poll_scheduler",
            _job_wrapper(
                "vk_poll_scheduler", vk_poll_scheduler, notify_skip=_notify_admin_skip
            ),
            "cron",
            id="vk_poll_scheduler",
            minute="2,17,32,47",
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
        )
        _register_job(
            "cleanup_scheduler",
            _job_wrapper("cleanup_scheduler", cleanup_scheduler, notify_skip=_notify_admin_skip),
            "cron",
            id="cleanup_scheduler",
            hour="2",
            minute="7",
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
        )
        _register_job(
            "partner_notification_scheduler",
            _job_wrapper(
                "partner_notification_scheduler",
                partner_notification_scheduler,
                notify_skip=_notify_admin_skip,
            ),
            "cron",
            id="partner_notification_scheduler",
            minute="5",
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
        )
        _register_job(
            "fest_nav_rebuild",
            _job_wrapper(
                "fest_nav_rebuild",
                rebuild_fest_nav_if_changed,
                notify_skip=_notify_admin_skip,
            ),
            "cron",
            id="fest_nav_rebuild",
            hour="3",
            minute="0",
            args=[db],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
        )
        if _env_enabled("ENABLE_VK_DYNAMIC_COVER_SCHEDULER", default=True):
            from vk_dynamic_cover import dynamic_cover_expiry_scheduler

            _register_job(
                "vk_dynamic_cover_expiry",
                _job_wrapper(
                    "vk_dynamic_cover_expiry",
                    dynamic_cover_expiry_scheduler,
                    notify_skip=_notify_admin_skip,
                ),
                "cron",
                id="vk_dynamic_cover_expiry",
                minute="11",
                args=[db, bot],
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=30,
            )

        times_raw = os.getenv(
            "VK_CRAWL_TIMES_LOCAL", "05:15,09:15,13:15,17:15,21:15,22:45"
        )
        tz_name = os.getenv("VK_CRAWL_TZ", "Europe/Kaliningrad")
        tz = _safe_zoneinfo(tz_name, label="VK_CRAWL_TZ")
        for idx, t in enumerate(times_raw.split(",")):
            t = t.strip()
            if not t:
                continue
            try:
                hh, mm = map(int, t.split(":"))
            except ValueError:
                logging.warning("invalid VK_CRAWL_TIMES_LOCAL entry: %s", t)
                continue
            now_local = datetime.now(tz).replace(hour=hh, minute=mm, second=0, microsecond=0)
            now_utc = now_local.astimezone(timezone.utc)
            _register_job(
                f"vk_crawl_cron_{idx}",
                _job_wrapper(
                    "vk_crawl_cron", vk_crawl_cron, notify_skip=_notify_admin_skip
                ),
                "cron",
                id=f"vk_crawl_cron_{idx}",
                hour=str(now_utc.hour),
                minute=str(now_utc.minute),
                args=[db, bot],
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=30,
            )
    else:
        logging.info("SCHED skipping core schedulers (ENABLE_CORE_SCHEDULERS!=1)")

    if _env_enabled("ENABLE_REGION_TALK_SCHEDULED", default=False):
        from scripts.region_talk_scheduled_runner import run_region_talk_scheduled

        async def region_talk_scheduler(
            db_obj,
            bot_obj,
            *,
            run_id: str | None = None,
        ) -> None:
            result = await run_region_talk_scheduled(
                db_obj,
                bot_obj,
                scheduler_run_id=run_id,
            )
            if not result.get("ok") and result.get("status") != "skipped":
                logging.error("region_talk scheduled cycle failed result=%s", result)

        times_raw = os.getenv(
            "REGION_TALK_TIMES_LOCAL", "06:20,09:50,13:50,17:50,21:50"
        )
        tz_name = os.getenv("REGION_TALK_TZ", "Europe/Kaliningrad")
        tz = _safe_zoneinfo(tz_name, label="REGION_TALK_TZ")
        try:
            misfire_grace = max(
                60,
                min(
                    7200,
                    int((os.getenv("REGION_TALK_MISFIRE_GRACE_SECONDS") or "1800").strip()),
                ),
            )
        except ValueError:
            misfire_grace = 1800
        registered_times: set[tuple[int, int]] = set()
        for idx, value in enumerate(times_raw.split(",")):
            value = value.strip()
            if not value:
                continue
            try:
                hour, minute = map(int, value.split(":"))
                if not (0 <= hour <= 23 and 0 <= minute <= 59):
                    raise ValueError
            except ValueError:
                logging.warning("invalid REGION_TALK_TIMES_LOCAL entry: %s", value)
                continue
            if (hour, minute) in registered_times:
                logging.warning("duplicate REGION_TALK_TIMES_LOCAL entry ignored: %s", value)
                continue
            registered_times.add((hour, minute))
            local_run = datetime.now(tz).replace(
                hour=hour,
                minute=minute,
                second=0,
                microsecond=0,
            )
            utc_run = local_run.astimezone(timezone.utc)
            physical_job_id = f"region_talk_{idx}"
            _register_job(
                physical_job_id,
                _job_wrapper(
                    "region_talk",
                    region_talk_scheduler,
                    notify_skip=_notify_admin_skip,
                ),
                "cron",
                id=physical_job_id,
                hour=str(utc_run.hour),
                minute=str(utc_run.minute),
                args=[db, bot],
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=misfire_grace,
            )
        if not registered_times:
            logging.error("SCHED Region Talk enabled but no valid REGION_TALK_TIMES_LOCAL entries")
        try:
            watchdog_seconds = max(60, min(900, int(
                (os.getenv("REGION_TALK_WATCHDOG_INTERVAL_SECONDS") or "300").strip()
            )))
        except ValueError:
            watchdog_seconds = 300

        async def region_talk_watchdog(db_obj, bot_obj, *, run_id: str | None = None) -> None:
            del run_id
            await maybe_dispatch_region_talk_watchdog(db_obj, bot_obj)

        _register_job(
            "region_talk_watchdog",
            _job_wrapper(
                "region_talk_watchdog",
                region_talk_watchdog,
                notify_skip=_notify_admin_skip,
            ),
            "interval",
            id="region_talk_watchdog",
            seconds=watchdog_seconds,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60,
        )
    else:
        logging.info("SCHED skipping Region Talk autonomy (ENABLE_REGION_TALK_SCHEDULED!=1)")

    if _env_enabled("ENABLE_EMAIL_OUTBOX_WORKER", default=False):
        from email_control.scheduler import run_email_outbox_worker

        email_worker_interval = max(
            30,
            min(300, int((os.getenv("EMAIL_OUTBOX_WORKER_INTERVAL_SECONDS") or "60").strip() or "60")),
        )
        _register_job(
            "email_outbox_worker",
            _job_wrapper(
                "email_outbox_worker",
                run_email_outbox_worker,
                notify_skip=_notify_admin_skip,
            ),
            "interval",
            id="email_outbox_worker",
            seconds=email_worker_interval,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=email_worker_interval,
        )
    else:
        logging.info("SCHED skipping email_outbox_worker (ENABLE_EMAIL_OUTBOX_WORKER!=1)")

    if _env_enabled("ENABLE_EMAIL_OUTBOX_MONITOR", default=False):
        from email_control.scheduler import EmailMonitorConfig, run_email_outbox_monitor

        email_monitor_interval = EmailMonitorConfig.from_env().interval_seconds
        _register_job(
            "email_outbox_monitor",
            _job_wrapper(
                "email_outbox_monitor",
                run_email_outbox_monitor,
                notify_skip=_notify_admin_skip,
            ),
            "interval",
            id="email_outbox_monitor",
            seconds=email_monitor_interval,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=email_monitor_interval,
        )
    else:
        logging.info("SCHED skipping email_outbox_monitor (ENABLE_EMAIL_OUTBOX_MONITOR!=1)")

    # Source parsing from theatres (before daily announcement at 08:00)
    enable_source_parsing = _env_enabled("ENABLE_SOURCE_PARSING", default=is_prod)
    if enable_source_parsing:
        from source_parsing.commands import source_parsing_scheduler
        parsing_time_raw = os.getenv("SOURCE_PARSING_TIME_LOCAL", "04:30").strip()
        parsing_tz_name = os.getenv("SOURCE_PARSING_TZ", "Europe/Kaliningrad")
        parsing_hour, parsing_minute = _cron_from_local(
            parsing_time_raw,
            parsing_tz_name,
            default_hour="4",
            default_minute="30",
            label="SOURCE_PARSING_TIME_LOCAL",
        )
        _register_job(
            "source_parsing",
            _job_wrapper("source_parsing", source_parsing_scheduler, notify_skip=_notify_admin_skip),
            "cron",
            id="source_parsing",
            hour=parsing_hour,
            minute=parsing_minute,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
        )
    else:
        logging.info("SCHED skipping source_parsing (ENABLE_SOURCE_PARSING!=1)")
        _notify_admin_skip("source_parsing", "ENABLE_SOURCE_PARSING!=1")

    enable_source_parsing_day = _env_enabled("ENABLE_SOURCE_PARSING_DAY", default=is_prod)
    if enable_source_parsing_day:
        from source_parsing.commands import source_parsing_scheduler_if_changed
        day_time_raw = os.getenv("SOURCE_PARSING_DAY_TIME_LOCAL", "14:15").strip()
        day_tz_name = os.getenv("SOURCE_PARSING_DAY_TZ", "Europe/Kaliningrad")
        day_hour, day_minute = _cron_from_local(
            day_time_raw,
            day_tz_name,
            default_hour="12",
            default_minute="15",
            label="SOURCE_PARSING_DAY_TIME_LOCAL",
        )
        _register_job(
            "source_parsing_day",
            _job_wrapper("source_parsing_day", source_parsing_scheduler_if_changed, notify_skip=_notify_admin_skip),
            "cron",
            id="source_parsing_day",
            hour=day_hour,
            minute=day_minute,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
        )
    else:
        logging.info("SCHED skipping source_parsing_day (ENABLE_SOURCE_PARSING_DAY!=1)")
        _notify_admin_skip("source_parsing_day", "ENABLE_SOURCE_PARSING_DAY!=1")

    enable_tg_monitoring = _env_enabled("ENABLE_TG_MONITORING", default=is_prod)
    if enable_tg_monitoring:
        from source_parsing.telegram.service import telegram_monitor_scheduler
        tg_time_raw = os.getenv("TG_MONITORING_TIME_LOCAL", "23:40").strip()
        tg_tz_name = os.getenv("TG_MONITORING_TZ", "Europe/Kaliningrad")
        tg_hour, tg_minute = _cron_from_local(
            tg_time_raw,
            tg_tz_name,
            default_hour="23",
            default_minute="40",
            label="TG_MONITORING_TIME_LOCAL",
        )
        _register_job(
            "tg_monitoring",
            _job_wrapper("tg_monitoring", telegram_monitor_scheduler, notify_skip=_notify_admin_skip),
            "cron",
            id="tg_monitoring",
            hour=tg_hour,
            minute=tg_minute,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=_tg_monitoring_misfire_grace_seconds(),
        )
        from source_parsing.telegram.on_demand import (
            dispatch_due_on_demand_monitoring,
            is_on_demand_enabled,
            scheduler_poll_seconds,
        )

        if is_on_demand_enabled():
            _register_job(
                "tg_monitoring_on_demand",
                _job_wrapper(
                    "tg_monitoring_on_demand",
                    dispatch_due_on_demand_monitoring,
                    notify_skip=_notify_admin_skip,
                ),
                "interval",
                id="tg_monitoring_on_demand",
                seconds=scheduler_poll_seconds(),
                args=[db, bot],
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=scheduler_poll_seconds(),
            )
        else:
            logging.info("SCHED skipping tg_monitoring_on_demand (ENABLE_TG_MONITORING_ON_DEMAND!=1)")
            _notify_admin_skip("tg_monitoring_on_demand", "ENABLE_TG_MONITORING_ON_DEMAND!=1")
    else:
        logging.info("SCHED skipping tg_monitoring (ENABLE_TG_MONITORING!=1)")
        _notify_admin_skip("tg_monitoring", "ENABLE_TG_MONITORING!=1")

    enable_vk_auto_import = _env_enabled("ENABLE_VK_AUTO_IMPORT", default=False)
    if enable_vk_auto_import:
        from vk_auto_queue import vk_auto_import_scheduler

        vk_auto_times = os.getenv(
            "VK_AUTO_IMPORT_TIMES_LOCAL", "06:15,10:15,12:00,15:30,18:30"
        ).strip()
        vk_auto_tz = os.getenv("VK_AUTO_IMPORT_TZ", "Europe/Kaliningrad").strip()
        for idx, t in enumerate(vk_auto_times.split(",")):
            t = t.strip()
            if not t:
                continue
            hour, minute = _cron_from_local(
                t,
                vk_auto_tz,
                default_hour="6",
                default_minute="30",
                label="VK_AUTO_IMPORT_TIMES_LOCAL",
            )
            _register_job(
                f"vk_auto_import_{idx}",
                _job_wrapper("vk_auto_import", vk_auto_import_scheduler, notify_skip=_notify_admin_skip),
                "cron",
                id=f"vk_auto_import_{idx}",
                hour=hour,
                minute=minute,
                args=[db, bot],
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=_vk_auto_import_misfire_grace_seconds(),
            )
    else:
        logging.info("SCHED skipping vk_auto_import (ENABLE_VK_AUTO_IMPORT!=1)")
        _notify_admin_skip("vk_auto_import", "ENABLE_VK_AUTO_IMPORT!=1")

    if (
        enable_tg_monitoring
        or enable_vk_auto_import
        or _guide_monitoring_schedule_settings()[0]
    ):
        async def critical_scheduler_watchdog_scheduler(
            db_obj,
            bot_obj,
            *,
            run_id: str | None = None,
        ) -> None:
            await maybe_dispatch_critical_scheduler_watchdog(db_obj, bot_obj)

        _register_job(
            "critical_scheduler_watchdog",
            _job_wrapper(
                "critical_scheduler_watchdog",
                critical_scheduler_watchdog_scheduler,
                notify_skip=_notify_admin_skip,
            ),
            "interval",
            id="critical_scheduler_watchdog",
            seconds=_critical_sched_interval_seconds(),
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60,
        )
    else:
        logging.info("SCHED skipping critical_scheduler_watchdog (no critical jobs enabled)")

    # Auto-delete past-event VK posts in the klgdevents community. Runs twice a
    # day at quiet local times and shares the heavy-ops gate, so it never
    # competes with VK-writing/Kaggle heavy jobs (skips if one is busy).
    enable_vk_post_prune = _env_enabled("ENABLE_VK_POST_PRUNE", default=is_prod)
    if enable_vk_post_prune:
        vk_post_prune_scheduler = resolve("vk_post_prune_scheduler", None)
        prune_times = os.getenv("VK_POST_PRUNE_TIMES_LOCAL", "02:30,14:30").strip()
        prune_tz = os.getenv("VK_POST_PRUNE_TZ", "Europe/Kaliningrad").strip()
        for idx, t in enumerate(prune_times.split(",")):
            t = t.strip()
            if not t:
                continue
            hour, minute = _cron_from_local(
                t,
                prune_tz,
                default_hour="2",
                default_minute="30",
                label="VK_POST_PRUNE_TIMES_LOCAL",
            )
            _register_job(
                f"vk_post_prune_{idx}",
                _job_wrapper(
                    "vk_post_prune",
                    vk_post_prune_scheduler,
                    notify_skip=_notify_admin_skip,
                ),
                "cron",
                id=f"vk_post_prune_{idx}",
                hour=hour,
                minute=minute,
                args=[db, bot],
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=30,
            )
    else:
        logging.info("SCHED skipping vk_post_prune (ENABLE_VK_POST_PRUNE!=1)")

    enable_festival_queue = _env_enabled("ENABLE_FESTIVAL_QUEUE", default=False)
    if enable_festival_queue:
        from festival_queue import process_festival_queue

        async def festival_queue_scheduler(
            db_obj,
            bot_obj,
            *,
            run_id: str | None = None,
        ) -> None:
            limit_raw = (os.getenv("FESTIVAL_QUEUE_LIMIT") or "").strip()
            limit: int | None = None
            if limit_raw:
                try:
                    parsed_limit = int(limit_raw)
                    if parsed_limit > 0:
                        limit = parsed_limit
                except ValueError:
                    logging.warning("invalid FESTIVAL_QUEUE_LIMIT=%r; using no limit", limit_raw)
            admin_chat_id = await resolve_superadmin_chat_id(db_obj)
            web_research_service = None
            from festival_queue import is_festival_web_research_enabled
            if is_festival_web_research_enabled():
                from festival_web_research.runtime import build_festival_web_research_service
                web_research_service = build_festival_web_research_service(db_obj)
            report = await process_festival_queue(
                db_obj,
                bot=bot_obj,
                chat_id=admin_chat_id,
                limit=limit,
                trigger="scheduled",
                operator_id=0,
                run_id=run_id,
                web_research_service=web_research_service,
            )
            logging.info(
                "festival_queue_scheduler processed=%s success=%s failed=%s skipped=%s",
                report.processed,
                report.success,
                report.failed,
                report.skipped,
            )

        fest_queue_times = os.getenv("FESTIVAL_QUEUE_TIMES_LOCAL", "03:30,16:30").strip()
        fest_queue_tz = os.getenv("FESTIVAL_QUEUE_TZ", "Europe/Kaliningrad").strip()
        for idx, t in enumerate(fest_queue_times.split(",")):
            t = t.strip()
            if not t:
                continue
            hour, minute = _cron_from_local(
                t,
                fest_queue_tz,
                default_hour="3",
                default_minute="30",
                label="FESTIVAL_QUEUE_TIMES_LOCAL",
            )
            _register_job(
                f"festival_queue_{idx}",
                _job_wrapper("festival_queue", festival_queue_scheduler, notify_skip=_notify_admin_skip),
                "cron",
                id=f"festival_queue_{idx}",
                hour=hour,
                minute=minute,
                args=[db, bot],
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=30,
            )
    else:
        logging.info("SCHED skipping festival_queue (ENABLE_FESTIVAL_QUEUE!=1)")
        _notify_admin_skip("festival_queue", "ENABLE_FESTIVAL_QUEUE!=1")

    enable_ticket_sites_queue = _env_enabled("ENABLE_TICKET_SITES_QUEUE", default=False)
    if enable_ticket_sites_queue:
        from ticket_sites_queue import process_ticket_sites_queue

        async def ticket_sites_queue_scheduler(
            db_obj,
            bot_obj,
            *,
            run_id: str | None = None,
        ) -> None:
            limit_raw = (os.getenv("TICKET_SITES_QUEUE_LIMIT") or "").strip()
            limit: int | None = None
            if limit_raw:
                try:
                    parsed_limit = int(limit_raw)
                    if parsed_limit > 0:
                        limit = parsed_limit
                except ValueError:
                    logging.warning("invalid TICKET_SITES_QUEUE_LIMIT=%r; using default", limit_raw)
            admin_chat_id = await resolve_superadmin_chat_id(db_obj)
            report = await process_ticket_sites_queue(
                db_obj,
                bot=bot_obj,
                chat_id=admin_chat_id,
                limit=limit,
                trigger="scheduled",
                operator_id=0,
                run_id=run_id,
            )
            logging.info(
                "ticket_sites_queue_scheduler processed=%s success=%s failed=%s skipped=%s",
                report.processed,
                report.success,
                report.failed,
                report.skipped,
            )

        t_time_raw = os.getenv("TICKET_SITES_QUEUE_TIME_LOCAL", "11:20").strip()
        t_tz_name = os.getenv("TICKET_SITES_QUEUE_TZ", "Europe/Kaliningrad").strip()
        t_hour, t_minute = _cron_from_local(
            t_time_raw,
            t_tz_name,
            default_hour="9",
            default_minute="20",
            label="TICKET_SITES_QUEUE_TIME_LOCAL",
        )
        _register_job(
            "ticket_sites_queue",
            _job_wrapper("ticket_sites_queue", ticket_sites_queue_scheduler, notify_skip=_notify_admin_skip),
            "cron",
            id="ticket_sites_queue",
            hour=t_hour,
            minute=t_minute,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
        )
    else:
        logging.info("SCHED skipping ticket_sites_queue (ENABLE_TICKET_SITES_QUEUE!=1)")
        _notify_admin_skip("ticket_sites_queue", "ENABLE_TICKET_SITES_QUEUE!=1")

    enable_3di = _env_enabled("ENABLE_3DI_SCHEDULED", default=is_prod)
    if enable_3di:
        from preview_3d.handlers import run_3di_new_only_scheduler

        async def preview_3di_scheduler(
            db_obj,
            bot_obj,
            *,
            run_id: str | None = None,
        ) -> None:
            run_chat_id = await resolve_superadmin_chat_id(db_obj)
            await run_3di_new_only_scheduler(
                db_obj,
                bot_obj,
                chat_id=run_chat_id,
                run_id=run_id,
            )

        three_di_times = os.getenv("THREEDI_TIMES_LOCAL", "07:15,15:15,17:15")
        three_di_tz = os.getenv("THREEDI_TZ", "Europe/Kaliningrad")
        for idx, t in enumerate(three_di_times.split(",")):
            t = t.strip()
            if not t:
                continue
            hour, minute = _cron_from_local(
                t,
                three_di_tz,
                default_hour="7",
                default_minute="15",
                label="THREEDI_TIMES_LOCAL",
            )
            _register_job(
                f"3di_scheduler_{idx}",
                _job_wrapper("3di_scheduler", preview_3di_scheduler, notify_skip=_notify_admin_skip),
                "cron",
                id=f"3di_scheduler_{idx}",
                hour=hour,
                minute=minute,
                args=[db, bot],
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=30,
            )
    else:
        logging.info("SCHED skipping 3di_scheduler (ENABLE_3DI_SCHEDULED!=1)")
        _notify_admin_skip("3di_scheduler", "ENABLE_3DI_SCHEDULED!=1")

    enable_guide_excursions = _env_enabled("ENABLE_GUIDE_EXCURSIONS_SCHEDULED", default=False)
    if enable_guide_excursions:
        async def guide_excursions_scheduler(
            db_obj,
            bot_obj,
            *,
            mode: str,
            run_id: str | None = None,
        ) -> None:
            await _run_scheduled_guide_excursions(
                db_obj,
                bot_obj,
                mode=mode,
            )

        guide_tz_name = os.getenv("GUIDE_EXCURSIONS_TZ", "Europe/Kaliningrad").strip()
        light_times = (os.getenv("GUIDE_EXCURSIONS_LIGHT_TIMES_LOCAL", "09:05,13:20") or "").split(",")
        for idx, value in enumerate(light_times):
            raw_time = value.strip()
            if not raw_time:
                continue
            hour, minute = _cron_from_local(
                raw_time,
                guide_tz_name,
                default_hour="9",
                default_minute="5",
                label="GUIDE_EXCURSIONS_LIGHT_TIMES_LOCAL",
            )
            _register_job(
                f"guide_excursions_light_{idx}",
                _job_wrapper("guide_excursions_light", guide_excursions_scheduler, notify_skip=_notify_admin_skip),
                "cron",
                id=f"guide_excursions_light_{idx}",
                hour=hour,
                minute=minute,
                args=[db, bot],
                kwargs={"mode": "light"},
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=_guide_monitoring_misfire_grace_seconds(),
            )

        full_time_raw = os.getenv("GUIDE_EXCURSIONS_FULL_TIME_LOCAL", "20:10").strip()
        full_hour, full_minute = _cron_from_local(
            full_time_raw,
            guide_tz_name,
            default_hour="20",
            default_minute="10",
            label="GUIDE_EXCURSIONS_FULL_TIME_LOCAL",
        )
        _register_job(
            "guide_excursions_full",
            _job_wrapper("guide_excursions_full", guide_excursions_scheduler, notify_skip=_notify_admin_skip),
            "cron",
            id="guide_excursions_full",
            hour=full_hour,
            minute=full_minute,
            args=[db, bot],
            kwargs={"mode": "full"},
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=_guide_monitoring_misfire_grace_seconds(),
        )
    else:
        logging.info("SCHED skipping guide_excursions (ENABLE_GUIDE_EXCURSIONS_SCHEDULED!=1)")
        _notify_admin_skip("guide_excursions", "ENABLE_GUIDE_EXCURSIONS_SCHEDULED!=1")

    enable_guide_visual_digest = _env_enabled("ENABLE_GUIDE_VISUAL_DIGEST_SCHEDULED", default=False)
    if enable_guide_visual_digest:
        async def guide_visual_digest_scheduler(
            db_obj,
            bot_obj,
            *,
            run_id: str | None = None,
        ) -> None:
            await _run_scheduled_guide_visual_digest(db_obj, bot_obj, run_id=run_id)

        guide_visual_tz_name = (
            os.getenv("GUIDE_VISUAL_DIGEST_TZ")
            or os.getenv("GUIDE_EXCURSIONS_TZ")
            or "Europe/Kaliningrad"
        ).strip()
        guide_visual_time_raw = (os.getenv("GUIDE_VISUAL_DIGEST_TIME_LOCAL") or "10:30").strip() or "10:30"
        visual_hour, visual_minute = _cron_from_local(
            guide_visual_time_raw,
            guide_visual_tz_name,
            default_hour="10",
            default_minute="30",
            label="GUIDE_VISUAL_DIGEST_TIME_LOCAL",
        )
        _register_job(
            "guide_visual_digest",
            _job_wrapper("guide_visual_digest", guide_visual_digest_scheduler, notify_skip=_notify_admin_skip),
            "cron",
            id="guide_visual_digest",
            hour=visual_hour,
            minute=visual_minute,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=_guide_monitoring_misfire_grace_seconds(),
        )
        if _env_enabled("ENABLE_GUIDE_VISUAL_DIGEST_VK_STORIES", default=False):
            _register_job(
                "guide_visual_digest_vk_story_due",
                _job_wrapper("guide_visual_digest_vk_story_due", _run_scheduled_guide_visual_digest_stories_due, notify_skip=_notify_admin_skip),
                "interval",
                id="guide_visual_digest_vk_story_due",
                minutes=5,
                args=[db, bot],
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=300,
            )
    else:
        logging.info("SCHED skipping guide_visual_digest (ENABLE_GUIDE_VISUAL_DIGEST_SCHEDULED!=1)")
        _notify_admin_skip("guide_visual_digest", "ENABLE_GUIDE_VISUAL_DIGEST_SCHEDULED!=1")

    enable_video_tomorrow, video_tz_name, video_time_raw, video_profile_key, video_test_mode = (
        _video_tomorrow_schedule_settings()
    )
    if enable_video_tomorrow:
        async def video_tomorrow_scheduler(
            db_obj,
            bot_obj,
            *,
            profile_key: str,
            test_mode: bool,
            run_id: str | None = None,
        ) -> None:
            await _run_scheduled_video_tomorrow(
                db_obj,
                bot_obj,
                profile_key=profile_key,
                test_mode=test_mode,
            )

        video_hour, video_minute = _cron_from_local(
            video_time_raw,
            video_tz_name,
            default_hour="14",
            default_minute="45",
            label="V_TOMORROW_TIME_LOCAL",
        )
        _register_job(
            "video_tomorrow",
            _job_wrapper("video_tomorrow", video_tomorrow_scheduler, notify_skip=_notify_admin_skip),
            "cron",
            id="video_tomorrow",
            hour=video_hour,
            minute=video_minute,
            args=[db, bot],
            kwargs={"profile_key": video_profile_key, "test_mode": video_test_mode},
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=_video_tomorrow_misfire_grace_seconds(),
        )
    else:
        logging.info(
            "SCHED skipping video_tomorrow (ENABLE_V_TOMORROW_SCHEDULED!=1 and ENABLE_V_TEST_TOMORROW_SCHEDULED!=1)"
        )
        _notify_admin_skip(
            "video_tomorrow",
            "ENABLE_V_TOMORROW_SCHEDULED!=1 and ENABLE_V_TEST_TOMORROW_SCHEDULED!=1",
        )

    enable_popular_review, popular_review_tz_name, popular_review_time_raw = (
        _popular_review_schedule_settings()
    )
    if enable_popular_review:
        async def popular_review_scheduler(
            db_obj,
            bot_obj,
            *,
            run_id: str | None = None,
        ) -> None:
            await _run_scheduled_popular_review(
                db_obj,
                bot_obj,
            )

        async def popular_review_watchdog_scheduler(
            db_obj,
            bot_obj,
            *,
            run_id: str | None = None,
        ) -> None:
            await maybe_dispatch_popular_review_watchdog(db_obj, bot_obj)

        popular_review_hour, popular_review_minute = _cron_from_local(
            popular_review_time_raw,
            popular_review_tz_name,
            default_hour="7",
            default_minute="15",
            label="V_POPULAR_REVIEW_TIME_LOCAL",
        )
        _register_job(
            "video_popular_review",
            _job_wrapper("video_popular_review", popular_review_scheduler, notify_skip=_notify_admin_skip),
            "cron",
            id="video_popular_review",
            hour=popular_review_hour,
            minute=popular_review_minute,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=600,
        )
        _register_job(
            "video_popular_review_watchdog",
            _job_wrapper(
                "video_popular_review_watchdog",
                popular_review_watchdog_scheduler,
                notify_skip=_notify_admin_skip,
            ),
            "interval",
            id="video_popular_review_watchdog",
            minutes=10,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60,
        )
    else:
        logging.info("SCHED skipping video_popular_review (ENABLE_V_POPULAR_REVIEW_SCHEDULED!=1)")
        _notify_admin_skip(
            "video_popular_review",
            "ENABLE_V_POPULAR_REVIEW_SCHEDULED!=1",
        )

    if _env_enabled("ENABLE_PROMO_VK_SCHEDULER", default=True):
        async def promo_vk_scheduler(
            db_obj,
            bot_obj,
            *,
            run_id: str | None = None,
        ) -> None:
            from promo import run_promo_vk_activities

            results = await run_promo_vk_activities(db_obj, bot_obj)
            if results:
                logging.info(
                    "SCHED promo_vk results=%s",
                    [
                        {
                            "campaign_id": item.campaign_id,
                            "activity_id": item.activity_id,
                            "surface": item.surface,
                            "event_id": item.event_id,
                            "status": item.status,
                            "target_url": item.target_url,
                            "reason": item.reason,
                        }
                        for item in results
                    ],
                )

        try:
            promo_vk_interval = max(
                5,
                int(os.getenv("PROMO_VK_INTERVAL_MINUTES", "30")),
            )
        except ValueError:
            promo_vk_interval = 30
        _register_job(
            "promo_vk",
            _job_wrapper("promo_vk", promo_vk_scheduler, notify_skip=_notify_admin_skip),
            "interval",
            id="promo_vk",
            minutes=promo_vk_interval,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )
    else:
        logging.info("SCHED skipping promo_vk (ENABLE_PROMO_VK_SCHEDULER!=1)")
        _notify_admin_skip("promo_vk", "ENABLE_PROMO_VK_SCHEDULER!=1")

    if _env_enabled("ENABLE_POLL_TO_FORWARD_DEBUG", default=False):
        _register_job(
            "poll_to_forward_debug",
            _job_wrapper(
                "poll_to_forward_debug",
                _run_poll_to_forward_debug_tick,
                notify_skip=_notify_admin_skip,
            ),
            "cron",
            id="poll_to_forward_debug",
            minute="0,30",
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=120,
        )
    else:
        logging.info("SCHED skipping poll_to_forward_debug (ENABLE_POLL_TO_FORWARD_DEBUG!=1)")

    if _env_enabled("ENABLE_POLL_TO_FORWARD_PROD", default=False):
        poll_tz = os.getenv("POLL_TO_FORWARD_TZ", "Europe/Kaliningrad")
        create_hour, create_minute = _cron_from_local(
            os.getenv("POLL_TO_FORWARD_PROD_POLL_TIME_LOCAL", "16:00"),
            poll_tz,
            default_hour="14",
            default_minute="00",
            label="POLL_TO_FORWARD_PROD_POLL_TIME_LOCAL",
        )
        result_hour, result_minute = _cron_from_local(
            os.getenv("POLL_TO_FORWARD_PROD_RESULT_TIME_LOCAL", "19:55"),
            poll_tz,
            default_hour="17",
            default_minute="55",
            label="POLL_TO_FORWARD_PROD_RESULT_TIME_LOCAL",
        )
        _register_job(
            "poll_to_forward_prod_create",
            _job_wrapper(
                "poll_to_forward_prod_create",
                _run_poll_to_forward_prod_create_tick,
                notify_skip=_notify_admin_skip,
            ),
            "cron",
            id="poll_to_forward_prod_create",
            hour=create_hour,
            minute=create_minute,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=900,
        )
        _register_job(
            "poll_to_forward_prod_resolve",
            _job_wrapper(
                "poll_to_forward_prod_resolve",
                _run_poll_to_forward_prod_resolve_tick,
                notify_skip=_notify_admin_skip,
            ),
            "cron",
            id="poll_to_forward_prod_resolve",
            hour=result_hour,
            minute=result_minute,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=1200,
        )
    else:
        logging.info("SCHED skipping poll_to_forward_prod (ENABLE_POLL_TO_FORWARD_PROD!=1)")

    # CherryFlash partner tracks. Intentionally always on:
    # the user asked NOT to gate this behind a feature flag. Times still
    # overridable via env without redeploy.
    try:
        from video_announce.partner_tracks import PARTNER_TRACKS as _PARTNER_TRACKS

        partner_track_schedule_items = [
            (track.track_id, track.callback_action) for track in _PARTNER_TRACKS
        ]
    except Exception:
        logging.exception(
            "SCHED failed to import partner tracks; using fallback partner schedule"
        )
        partner_track_schedule_items = [
            ("partner_eco_nature_001", "eco"),
            ("partner_konb_library_001", "konb"),
            ("partner_region_east_001", "east"),
        ]
    for partner_track_id, callback_action in partner_track_schedule_items:
        track_time_raw = _partner_track_time_local(partner_track_id)
        track_hour, track_minute = _cron_from_local(
            track_time_raw,
            PARTNER_TRACK_TZ,
            default_hour="18" if callback_action == "east" else "12",
            default_minute="30",
            label=f"V_PARTNER_TRACK_{callback_action.upper()}_TIME_LOCAL",
        )
        cron_job_id = f"video_partner_track_{callback_action}"

        def _make_partner_cron(track_id: str):
            async def _runner(
                db_obj,
                bot_obj,
                *,
                run_id: str | None = None,
            ) -> None:
                await _run_scheduled_partner_track(db_obj, bot_obj, track_id)

            return _runner

        def _make_partner_watchdog(track_id: str):
            async def _watchdog(
                db_obj,
                bot_obj,
                *,
                run_id: str | None = None,
            ) -> None:
                await maybe_dispatch_partner_track_watchdog(
                    db_obj, bot_obj, track_id
                )

            return _watchdog

        _register_job(
            cron_job_id,
            _job_wrapper(
                cron_job_id,
                _make_partner_cron(partner_track_id),
                notify_skip=_notify_admin_skip,
            ),
            "cron",
            id=cron_job_id,
            hour=track_hour,
            minute=track_minute,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=600,
        )
        watchdog_job_id = f"{cron_job_id}_watchdog"
        _register_job(
            watchdog_job_id,
            _job_wrapper(
                watchdog_job_id,
                _make_partner_watchdog(partner_track_id),
                notify_skip=_notify_admin_skip,
            ),
            "interval",
            id=watchdog_job_id,
            minutes=10,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60,
        )

    if is_prod:
        async def kenigsberg_story_scheduler(
            db_obj,
            bot_obj,
            *,
            run_id: str | None = None,
        ) -> None:
            await _run_scheduled_kenigsberg_story(
                db_obj,
                bot_obj,
            )

        kenigsberg_hour, kenigsberg_minute = _cron_from_local(
            "19:30",
            "Europe/Kaliningrad",
            default_hour="17",
            default_minute="30",
            label="KENIGSBERG_STORY_TIME_LOCAL",
        )
        _register_job(
            "kenigsberg_story_daily",
            _job_wrapper("kenigsberg_story_daily", kenigsberg_story_scheduler, notify_skip=_notify_admin_skip),
            "cron",
            id="kenigsberg_story_daily",
            day_of_week="fri",
            hour=kenigsberg_hour,
            minute=kenigsberg_minute,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=1800,
        )
    else:
        logging.info("SCHED skipping kenigsberg_story_daily outside production")

    enable_exhibition_duplicate_audit = _env_enabled("ENABLE_EXHIBITION_DUPLICATE_AUDIT", default=False)
    if enable_exhibition_duplicate_audit:
        from exhibition_duplicate_audit import run_exhibition_duplicate_audit_scheduler

        audit_time_raw = os.getenv("EXHIBITION_DUPLICATE_AUDIT_TIME_LOCAL", "07:45").strip()
        audit_tz_name = os.getenv("EXHIBITION_DUPLICATE_AUDIT_TZ", "Europe/Kaliningrad").strip()
        audit_hour, audit_minute = _cron_from_local(
            audit_time_raw,
            audit_tz_name,
            default_hour="5",
            default_minute="45",
            label="EXHIBITION_DUPLICATE_AUDIT_TIME_LOCAL",
        )
        _register_job(
            "exhibition_duplicate_audit",
            _job_wrapper(
                "exhibition_duplicate_audit",
                run_exhibition_duplicate_audit_scheduler,
                notify_skip=_notify_admin_skip,
            ),
            "cron",
            id="exhibition_duplicate_audit",
            hour=audit_hour,
            minute=audit_minute,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=1800,
        )
    else:
        logging.info("SCHED skipping exhibition_duplicate_audit (ENABLE_EXHIBITION_DUPLICATE_AUDIT!=1)")

    if _env_enabled("ENABLE_EVENT_VECTOR_SYNC", default=False):
        from event_vector_sync import run_event_vector_sync

        async def event_vector_sync_scheduler(
            db_obj,
            bot_obj,
            *,
            run_id: str | None = None,
        ) -> None:
            await run_event_vector_sync(
                db_obj,
                trigger="scheduled",
                scheduler_run_id=run_id,
            )

        vector_interval = max(
            15,
            int((os.getenv("EVENT_VECTOR_SYNC_INTERVAL_MINUTES") or "180").strip() or "180"),
        )
        _register_job(
            "event_vector_sync",
            _job_wrapper(
                "event_vector_sync",
                event_vector_sync_scheduler,
                notify_skip=_notify_admin_skip,
            ),
            "interval",
            id="event_vector_sync",
            minutes=vector_interval,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=1800,
        )
    else:
        logging.info("SCHED skipping event_vector_sync (ENABLE_EVENT_VECTOR_SYNC!=1)")

    enable_general_stats = _env_enabled("ENABLE_GENERAL_STATS", default=False)
    if enable_general_stats:
        from general_stats import general_stats_scheduler

        general_stats_time_raw = os.getenv("GENERAL_STATS_TIME_LOCAL", "07:30").strip()
        general_stats_tz_name = os.getenv("GENERAL_STATS_TZ", "Europe/Kaliningrad").strip()
        general_stats_hour, general_stats_minute = _cron_from_local(
            general_stats_time_raw,
            general_stats_tz_name,
            default_hour="5",
            default_minute="30",
            label="GENERAL_STATS_TIME_LOCAL",
        )
        _register_job(
            "general_stats",
            _job_wrapper("general_stats", general_stats_scheduler, notify_skip=_notify_admin_skip),
            "cron",
            id="general_stats",
            hour=general_stats_hour,
            minute=general_stats_minute,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
        )
    else:
        logging.info("SCHED skipping general_stats (ENABLE_GENERAL_STATS!=1)")
        _notify_admin_skip("general_stats", "ENABLE_GENERAL_STATS!=1")

    enable_telegraph_cache = _env_enabled("ENABLE_TELEGRAPH_CACHE_SANITIZER", default=False)
    if enable_telegraph_cache:
        from telegraph_cache_sanitizer import run_telegraph_cache_sanitizer

        async def telegraph_cache_sanitize_scheduler(
            db_obj,
            bot_obj,
            *,
            run_id: str | None = None,
        ) -> None:
            admin_chat_id = await resolve_superadmin_chat_id(db_obj)
            res = await run_telegraph_cache_sanitizer(
                db_obj,
                bot=bot_obj,
                chat_id=admin_chat_id,
                operator_id=0,
                trigger="scheduled",
                run_id=run_id,
            )
            imported = res.get("imported") or {}
            regen = res.get("regen") or {}
            logging.info(
                "telegraph_cache_sanitize_scheduler total=%s ok=%s fail=%s regen=%s",
                imported.get("total"),
                imported.get("ok"),
                imported.get("fail"),
                regen,
            )
            if admin_chat_id and bot_obj is not None:
                try:
                    text = (
                        "🧼 Telegraph cache sanitizer (scheduled): готово\n"
                        f"ok={imported.get('ok', 0)} fail={imported.get('fail', 0)} total={imported.get('total', 0)}\n"
                        + (
                            (
                                "regen: "
                                + ", ".join(
                                    f"{k}={int(v)}"
                                    for k, v in regen.items()
                                    if int(v or 0) > 0
                                )
                            )
                            if regen
                            else ""
                        )
                    ).strip()
                    await bot_obj.send_message(admin_chat_id, text, disable_web_page_preview=True)
                except Exception:
                    logging.warning("telegraph_cache_sanitize_scheduler notify failed", exc_info=True)

        cache_time_raw = os.getenv("TELEGRAPH_CACHE_TIME_LOCAL", "01:10").strip()
        cache_tz_name = os.getenv("TELEGRAPH_CACHE_TZ", "Europe/Kaliningrad").strip() or "Europe/Kaliningrad"
        cache_hour, cache_minute = _cron_from_local(
            cache_time_raw,
            cache_tz_name,
            default_hour="23",
            default_minute="10",
            label="TELEGRAPH_CACHE_TIME_LOCAL",
        )
        _register_job(
            "telegraph_cache_sanitize",
            _job_wrapper(
                "telegraph_cache_sanitize",
                telegraph_cache_sanitize_scheduler,
                notify_skip=_notify_admin_skip,
            ),
            "cron",
            id="telegraph_cache_sanitize",
            hour=cache_hour,
            minute=cache_minute,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
        )
    else:
        logging.info("SCHED skipping telegraph_cache_sanitize (ENABLE_TELEGRAPH_CACHE_SANITIZER!=1)")
        _notify_admin_skip("telegraph_cache_sanitize", "ENABLE_TELEGRAPH_CACHE_SANITIZER!=1")

    enable_kaggle_recovery = _env_enabled("ENABLE_KAGGLE_RECOVERY", default=is_prod)
    if enable_kaggle_recovery:
        from kaggle_recovery import kaggle_recovery_scheduler
        interval_raw = os.getenv("KAGGLE_RECOVERY_INTERVAL_MINUTES", "5").strip()
        try:
            interval_min = max(1, int(interval_raw))
        except ValueError:
            interval_min = 5
        _register_job(
            "kaggle_recovery",
            _job_wrapper("kaggle_recovery", kaggle_recovery_scheduler, notify_skip=_notify_admin_skip),
            "interval",
            id="kaggle_recovery",
            minutes=interval_min,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
        )
    else:
        logging.info("SCHED skipping kaggle_recovery (ENABLE_KAGGLE_RECOVERY!=1)")
        _notify_admin_skip("kaggle_recovery", "ENABLE_KAGGLE_RECOVERY!=1")

    if os.getenv("ENABLE_NIGHTLY_PAGE_SYNC") == "1":
        _register_job(
            "nightly_page_sync",
            _job_wrapper("nightly_page_sync", nightly_page_sync, notify_skip=_notify_admin_skip),
            "cron",
            id="nightly_page_sync",
            hour="2",
            minute="30",
            args=[db],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
        )
    else:
        logging.info("SCHED skipping nightly_page_sync (ENABLE_NIGHTLY_PAGE_SYNC!=1)")

    # Pinned button update at 18:00 Kaliningrad time (UTC+2 = 16:00 UTC)
    from handlers.pinned_button import pinned_button_scheduler
    
    pinned_tz = _safe_zoneinfo("Europe/Kaliningrad", label="PINNED_BUTTON_TZ")
    pinned_local = datetime.now(pinned_tz).replace(hour=18, minute=0, second=0, microsecond=0)
    pinned_utc = pinned_local.astimezone(timezone.utc)
    _register_job(
        "pinned_button_scheduler",
        _job_wrapper("pinned_button_scheduler", pinned_button_scheduler, notify_skip=_notify_admin_skip),
        "cron",
        id="pinned_button_scheduler",
        hour=str(pinned_utc.hour),
        minute=str(pinned_utc.minute),
        args=[db, bot],
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=30,
    )

    async def _run_maintenance(job, name: str, timeout: float, run_id: str | None = None) -> None:
        start = _time.perf_counter()
        try:
            await asyncio.wait_for(job(), timeout=timeout)
            dur = (_time.perf_counter() - start) * 1000
            logging.info("db_maintenance %s done in %.0f ms", name, dur)
        except asyncio.TimeoutError:
            logging.warning(
                "db_maintenance %s timed out after %.1f s", name, timeout
            )
        except Exception:
            logging.warning("db_maintenance %s failed", name, exc_info=True)

    if db is not None:
        try:
            from source_parsing.post_metrics import cleanup_post_metrics
        except Exception:
            cleanup_post_metrics = None  # type: ignore[assignment]
        try:
            from social_metrics_kaggle import run_social_metrics_kaggle_batch
        except Exception:
            run_social_metrics_kaggle_batch = None  # type: ignore[assignment]

        _register_job(
            "db_optimize",
            _job_wrapper("db_optimize", _run_maintenance, notify_skip=_notify_admin_skip),
            "interval",
            id="db_optimize",
            hours=1,
            args=[partial(optimize, db.engine), "PRAGMA optimize", 10.0],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
        )
        _register_job(
            "db_wal_checkpoint",
            _job_wrapper("db_wal_checkpoint", _run_maintenance, notify_skip=_notify_admin_skip),
            "interval",
            id="db_wal_checkpoint",
            hours=1,
            args=[partial(wal_checkpoint_truncate, db.engine), "PRAGMA wal_checkpoint(TRUNCATE)", 30.0],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
        )
        _register_job(
            "db_vacuum",
            _job_wrapper("db_vacuum", _run_maintenance, notify_skip=_notify_admin_skip),
            "interval",
            id="db_vacuum",
            hours=12,
            args=[partial(vacuum, db.engine), "VACUUM", 120.0],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
        )
        if cleanup_post_metrics is not None:
            _register_job(
                "post_metrics_cleanup",
                _job_wrapper("post_metrics_cleanup", _run_maintenance, notify_skip=_notify_admin_skip),
                "interval",
                id="post_metrics_cleanup",
                hours=24,
                args=[partial(cleanup_post_metrics, db), "post_metrics_cleanup", 20.0],
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=30,
            )
        if (
            run_social_metrics_kaggle_batch is not None
            and _env_enabled("ENABLE_SOCIAL_METRICS_KAGGLE", default=False)
        ):
            _register_job(
                "social_metrics_batch",
                _job_wrapper(
                    "social_metrics_batch",
                    _run_maintenance,
                    notify_skip=_notify_admin_skip,
                ),
                "interval",
                id="social_metrics_batch",
                minutes=max(5, _env_int("SOCIAL_METRICS_BATCH_INTERVAL_MINUTES", 30)),
                args=[
                    partial(run_social_metrics_kaggle_batch, db),
                    "social_metrics_kaggle",
                    float(max(900, _env_int("SOCIAL_METRICS_KAGGLE_TIMEOUT_SECONDS", 1800) + 300)),
                ],
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=120,
            )

    if _env_enabled("ENABLE_STATIC_SITE_KAGGLE_BUILDER", default=False):
        static_midnight_hour, static_midnight_minute = _cron_from_local(
            "00:00",
            "Europe/Kaliningrad",
            default_hour="0",
            default_minute="0",
            label="STATIC_SITE_LOCAL_MIDNIGHT",
        )

        async def static_site_calendar_rollover(
            db_obj,
            _bot_obj,
            *,
            run_id: str | None = None,
        ) -> None:
            await _enqueue_static_site_calendar_refresh(db_obj, trigger="calendar_rollover")

        _register_job(
            "static_site_calendar_rollover",
            _job_wrapper(
                "static_site_calendar_rollover",
                static_site_calendar_rollover,
                notify_skip=_notify_admin_skip,
            ),
            "cron",
            id="static_site_calendar_rollover",
            hour=static_midnight_hour,
            minute=static_midnight_minute,
            args=[db, bot],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
        )

    _scheduler.add_listener(
        _on_event,
        EVENT_JOB_SUBMITTED | EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED,
    )
    _scheduler.start()
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        logging.warning("SCHED failed to schedule startup catchup for video_tomorrow: no running event loop")
    except Exception:
        logging.exception("SCHED failed to schedule startup catchup for video_tomorrow")
    else:
        loop.create_task(_run_startup_catchups(db, bot))
    return _scheduler


def cleanup() -> None:
    if _scheduler:
        try:
            _scheduler.shutdown(wait=False)
        except Exception:
            logging.exception("scheduler shutdown failed")
