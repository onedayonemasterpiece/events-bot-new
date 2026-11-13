"""Calendar documents on the managed Afisha wall, using existing VK and ICS transport."""
from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import unicodedata
from datetime import datetime, timedelta, timezone

from sqlmodel import select
from models import Event, Setting

COMMENT_TEXTS = (
    "Добавьте событие в календарь, чтобы не забыть 📅 Нажмите на файл два раза — откроется добавление в календарь.",
    "Чтобы не пропустить событие, добавьте его в календарь 📅 Нажмите на файл два раза — откроется добавление в календарь.",
    "Собираетесь пойти? Добавьте событие в календарь 📅 Нажмите на файл два раза — откроется добавление в календарь.",
    "Нажмите на файл два раза, чтобы добавить событие в календарь 📅 Поставьте ❤️ этому комментарию, если добавили.",
    "Чтобы не пропустить событие, нажмите на файл два раза — откроется добавление в календарь 📅 Если добавили — поставьте ❤️ этому комментарию.",
)


async def calendar_bytes(db, events):
    import main
    main._load_icalendar()
    calendar = main.Calendar()
    calendar.add("VERSION", "2.0")
    calendar.add("PRODID", "-//events-bot//RU")
    calendar.add("METHOD", "PUBLISH")
    for event in events:
        start = main.parse_iso_date(event.date.split("..", 1)[0])
        if not start:
            raise ValueError("Calendar requires an exact event date")
        timed = bool(main.parse_time_range(event.time)) and not event.time_is_default
        view = event.model_copy(update={"date": start.isoformat(), "time": event.time if timed else "00:00"})
        item = main.Calendar.from_ical(await main.build_ics_content(db, view)).walk("VEVENT")[0]
        item["UID"] = f"event-{event.id}@kenigevents.ru"
        # Stable stamp keeps an unchanged calendar unchanged across job retries.
        item.pop("DTSTAMP", None)
        stamp = event.added_at
        item.add("DTSTAMP", stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc))
        if not timed:
            end_raw = event.end_date or event.date.split("..")[-1]
            end = main.parse_iso_date(end_raw) or start
            item.pop("DTSTART", None)
            item.pop("DTEND", None)
            item.add("DTSTART", start)
            item.add("DTEND", max(start, end) + timedelta(days=1))
        calendar.add_component(item)
    return calendar.to_ical()


async def schedule_calendar_comment(db, event_id, post_url, publish_date):
    """One existing outbox job per new postponed announcement, never wall history."""
    import main
    group = str(main.VK_AFISHA_GROUP_ID or "").lstrip("-")
    ids = main._vk_owner_and_post_id(post_url)
    if not ids or not group or int(ids[0]) != -int(group):
        return
    delay = random.Random(post_url).randint(300, 900)
    await main.enqueue_job(
        db, event_id, main.JobTask.vk_calendar_comment,
        {"post_url": post_url, "publish_date": publish_date},
        coalesce_key=f"vk_calendar_comment:{ids[0]}:{ids[1]}",
        next_run_at=datetime.fromtimestamp(publish_date + delay, timezone.utc),
    )


async def ensure_calendar_comment(db, event, post_url):
    import aiohttp
    import main
    ids = main._vk_owner_and_post_id(str(post_url or ""))
    group = str(main.VK_AFISHA_GROUP_ID or "").lstrip("-")
    if not ids or not group or int(ids[0]) != -int(group):
        return False
    owner, post = map(int, ids)
    async with main.get_ics_semaphore():
        async with db.get_session() as session:
            events = list((await session.execute(select(Event).where(Event.source_vk_post_url == post_url).order_by(Event.id))).scalars().all())
        events = [e for e in events if not e.silent and e.lifecycle_status == "active" and e.identity_status != "merged"]
        if not events:
            return False
        filename = "".join(c for c in events[0].title if unicodedata.category(c) not in {"So", "Sk", "Cf"} and c not in "\ufe0e\ufe0f\u20e3")
        filename = re.sub(r'[\x00-\x1f<>:"/\\|?*]', " ", filename)
        filename = (" ".join(filename.split())[:120].rstrip(" .") or "Событие") + ".ics"
        body = await calendar_bytes(db, events)
        key = f"vk_calendar_comment:{owner}:{post}"
        async with db.get_session() as session:
            row = await session.get(Setting, key)
            state = json.loads(row.value) if row else {}
        async def persist():
            async with db.get_session() as session:
                row = await session.get(Setting, key)
                if row:
                    row.value = json.dumps(state, ensure_ascii=False)
                else:
                    row = Setting(key=key, value=json.dumps(state, ensure_ascii=False))
                session.add(row)
                await session.commit()

        if state.get("comment_id"):
            return False
        token = main._vk_user_token()
        if not token:
            raise RuntimeError("VK calendar document requires the configured user actor")

        async def api(method, **params):
            await asyncio.sleep(4)
            result = await main._vk_api(method, params, db, main.get_bot(), token=token, token_kind="user")
            if "response" not in result:
                raise RuntimeError(f"VK calendar method failed: {method}")
            return result["response"]

        # Only a retry with a saved document needs to recover a lost comment response.
        if state.get("document"):
            offset = 0
            while True:
                comments = await api("wall.getComments", owner_id=owner, post_id=post, count=100, offset=offset)
                document_id = "_".join(state["document"].split("_", 2)[:2])
                found = next((c for c in comments.get("items", []) if c.get("from_id") in {owner, int(state['document'].split('_', 1)[0][3:])} and any(
                    a.get("type") == "doc" and f"doc{a['doc']['owner_id']}_{a['doc']['id']}" == document_id
                    for a in c.get("attachments", []))), None)
                if found:
                    state["comment_id"] = found["id"]
                    await persist()
                    return False
                offset += len(comments.get("items", []))
                if not comments.get("items") or offset >= comments.get("count", 0):
                    break
        state.setdefault("text", random.choice(COMMENT_TEXTS))
        if not state.get("document"):
            upload = await api("docs.getWallUploadServer")
            form = aiohttp.FormData()
            form.add_field("file", body, filename=filename, content_type="text/calendar")
            await asyncio.sleep(4)
            async with main.get_http_session().post(upload["upload_url"], data=form) as result:
                result.raise_for_status()
                uploaded = await result.json(content_type=None)
            if not uploaded.get("file"):
                raise RuntimeError("VK calendar upload did not return a file")
            saved = await api("docs.save", file=uploaded["file"], title=filename)
            document = saved[0] if isinstance(saved, list) else saved.get("doc", saved)
            if document.get("ext") != "ics":
                raise RuntimeError("VK did not retain the calendar extension")
            state["document"] = f"doc{document['owner_id']}_{document['id']}" + (f"_{document['access_key']}" if document.get("access_key") else "")
            await persist()  # Reuse the uploaded document if comment delivery is retried.
        params = dict(owner_id=owner, message=state["text"], attachments=state["document"])
        result = await api("wall.createComment", post_id=post, guid=f"calendar:{owner}:{post}", **params)
        state["comment_id"] = result["comment_id"]
        await persist()
        logging.info("vk.calendar_comment delivered event_id=%s post_id=%s comment_id=%s", event.id, post, state["comment_id"])
        return True
