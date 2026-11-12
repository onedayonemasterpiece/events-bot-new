"""Calendar documents on the managed Afisha wall, using existing VK and ICS transport."""
from __future__ import annotations

import hashlib
import json
import logging
import random
from datetime import datetime, timedelta, timezone

from sqlmodel import select
from models import Event, Setting

COMMENT_TEXTS = (
    "Добавьте событие в календарь, чтобы не забыть 📅 Файл прикреплён.",
    "Чтобы не пропустить событие, добавьте его в календарь 📅 Файл во вложении.",
    "Собираетесь пойти? Добавьте событие в календарь 📅 Откройте прикреплённый файл.",
    "Добавьте событие в календарь 📅 Поставьте ❤️ этому комментарию, если добавили.",
    "Чтобы не пропустить событие, сохраните его в календарь 📅 Если добавили — поставьте ❤️ этому комментарию.",
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
        body = await calendar_bytes(db, events)
        fingerprint = hashlib.sha256(body).hexdigest()
        key = f"vk_calendar_comment:{owner}:{post}"
        async with db.get_session() as session:
            row = await session.get(Setting, key)
            state = json.loads(row.value) if row else {}
        if state.get("hash") == fingerprint and state.get("comment_id") and state.get("text") in COMMENT_TEXTS:
            return True
        token = main._vk_user_token()
        if not token:
            raise RuntimeError("VK calendar document requires the configured user actor")

        async def api(method, **params):
            result = await main._vk_api(method, params, token=token, token_kind="user")
            if "response" not in result:
                raise RuntimeError(f"VK calendar method failed: {method}")
            return result["response"]

        async def persist():
            async with db.get_session() as session:
                row = await session.get(Setting, key)
                if row:
                    row.value = json.dumps(state, ensure_ascii=False)
                else:
                    row = Setting(key=key, value=json.dumps(state, ensure_ascii=False))
                session.add(row)
                await session.commit()

        response = await api("wall.getById", posts=f"{owner}_{post}")
        posts = response.get("items", []) if isinstance(response, dict) else response
        if not posts or posts[0].get("post_type", "post") != "post" or posts[0].get("date", 0) > datetime.now(timezone.utc).timestamp():
            return False  # Existing live-ID scan calls us again when the item is public.
        # The owner requested today's backfill, not older wall history.
        if posts[0].get("date", 0) < datetime(2026, 11, 12, tzinfo=main.LOCAL_TZ).timestamp():
            return False
        # Recover an acknowledged-but-unrecorded comment, and adopt the owner's pilot.
        if not state.get("comment_id"):
            offset = 0
            while True:
                comments = await api("wall.getComments", owner_id=owner, post_id=post, count=100, offset=offset)
                found = next((c for c in comments.get("items", []) if c.get("from_id") == owner and any(a.get("type") == "doc" and a.get("doc", {}).get("ext") == "ics" for a in c.get("attachments", []))), None)
                if found:
                    state["comment_id"] = found["id"]
                    break
                offset += len(comments.get("items", []))
                if not comments.get("items") or offset >= comments.get("count", 0):
                    break
        state.setdefault("text", random.choice(COMMENT_TEXTS))
        if state["text"] not in COMMENT_TEXTS:
            state["text"] = random.choice(COMMENT_TEXTS)
        if state.get("document_hash") != fingerprint:
            upload = await api("docs.getWallUploadServer")
            form = aiohttp.FormData()
            form.add_field("file", body, filename=f"event-{event.id}.ics", content_type="text/calendar")
            async with main.get_http_session().post(upload["upload_url"], data=form) as result:
                result.raise_for_status()
                uploaded = await result.json(content_type=None)
            if not uploaded.get("file"):
                raise RuntimeError("VK calendar upload did not return a file")
            saved = await api("docs.save", file=uploaded["file"], title=f"event-{event.id}.ics")
            document = saved[0] if isinstance(saved, list) else saved.get("doc", saved)
            if document.get("ext") != "ics":
                raise RuntimeError("VK did not retain the calendar extension")
            state["document"] = f"doc{document['owner_id']}_{document['id']}" + (f"_{document['access_key']}" if document.get("access_key") else "")
            state["document_hash"] = fingerprint
            await persist()  # Reuse the uploaded document if comment delivery is retried.
        params = dict(owner_id=owner, message=state["text"], attachments=state["document"])
        if state.get("comment_id"):
            await api("wall.editComment", comment_id=state["comment_id"], **params)
        else:
            result = await api("wall.createComment", post_id=post, from_group=abs(owner), guid=f"calendar:{owner}:{post}", **params)
            state["comment_id"] = result["comment_id"]
        state["hash"] = fingerprint
        await persist()
        logging.info("vk.calendar_comment delivered event_id=%s post_id=%s comment_id=%s", event.id, post, state["comment_id"])
        return True
