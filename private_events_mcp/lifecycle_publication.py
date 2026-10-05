"""Existing JobOutbox/provider adapters for managed lifecycle reconciliation.

Uncertain sends pause the exact outbox intent. Never issue another notice until
an operator reconciles that intent; a worker crash is not permission to resend.
"""

from datetime import datetime, timezone
import json
from sqlalchemy import text
from models import Event, JobOutbox, JobStatus
from static_site_release import event_public_revision
from .event_commands import one
from .event_publication_receipts import _public_url


def public_change_text(event, before, after):
    status = after.get("lifecycle_status")
    if status == "cancelled":
        return "ОТМЕНЕНО. " + event.title
    if status == "postponed":
        return "ПЕРЕНЕСЕНО. " + event.title + ". Новая дата уточняется."
    labels = {
        "date": "Дата",
        "end_date": "Окончание",
        "time": "Время",
        "location_name": "Площадка",
        "location_address": "Адрес",
        "city": "Город",
    }
    changes = [
        f"{label}: {before.get(field) or '—'} → {after.get(field) or '—'}"
        for field, label in labels.items()
        if before.get(field) != after.get(field)
    ]
    return "ПЕРЕНОС. " + event.title + "\n" + "\n".join(changes)


async def run_lifecycle_publication(database, job, bot):
    import main

    payload = job.payload or {}
    notice = payload.get("publication_kind") == "lifecycle_notice"
    surface = "telegram" if job.task.value == "tg_event_publish" else "vk"
    target = "notice:" + payload["operation_ref"] if notice else "managed"
    async with database.get_session() as session:
        row = await one(
            session,
            "SELECT * FROM event_change_log WHERE operation_ref=:ref",
            ref=payload["operation_ref"],
        )
        event = await session.get(Event, job.event_id)
        if (
            event is None
            or not row
            or event_public_revision(event) != job.target_event_revision
        ):
            return False
        old = await one(
            session,
            "SELECT * FROM event_publication WHERE event_id=:id AND platform=:surface AND target=:target",
            id=event.id,
            surface=surface,
            target=target,
        )
        if (
            old
            and old["applied_event_revision"] == job.target_event_revision
            and old["status"] == "published"
        ):
            return True
        current = await session.get(JobOutbox, job.id)
        if (current.payload or {}).get("provider_attempted"):
            current.status = JobStatus.paused
            current.terminal_reason = "outcome_unknown"
            session.add(current)
            await session.commit()
            raise RuntimeError("LIFECYCLE_OUTCOME_UNKNOWN")
        current.payload = {**payload, "provider_attempted": True}
        session.add(current)
        await session.commit()
    before = json.loads(row["before_json"])
    after = json.loads(row["after_json"])
    message = public_change_text(event, before, after)
    public_notice = json.loads(row["request_json"])["request"].get("public_notice")
    if public_notice:
        message += "\n" + public_notice
    stable_url = _public_url(event.telegraph_url, "telegraph")
    if stable_url:
        message += "\n" + stable_url
    view = Event(**event.model_dump())
    view.title = message.split("\n")[0]
    if view.lifecycle_status != "active":
        view.date = ""
        view.end_date = None
        view.time = ""
        view.ics_url = None
    view.description = message
    if notice:
        view.tg_event_post_id = None
        view.tg_event_post_url = None
        view.tg_event_source_hash = None
        view.source_vk_post_url = None
        view.vk_source_hash = None
    try:
        if surface == "telegram":
            if not notice and not event.tg_event_post_id:
                return False
            url, post_id, mode, source_hash = await main.publish_tg_event_announcement(
                view, message, database, bot
            )
        else:
            if not notice and not await main._event_has_existing_managed_vk_post(event):
                return False
            url = await main.sync_vk_source_post(
                view, message, database, bot, append_text=False
            )
        url = _public_url(url, surface)
        if not url:
            raise RuntimeError("LIFECYCLE_RECEIPT_UNAVAILABLE")
        async with database.get_session() as session:
            if not notice:
                current_event = await session.get(Event, event.id)
                if current_event is not None:
                    if surface == "telegram":
                        current_event.tg_event_post_url = url
                        current_event.tg_event_post_id = post_id
                        current_event.tg_event_post_mode = mode
                        current_event.tg_event_source_hash = source_hash
                    else:
                        current_event.source_vk_post_url = url
                    session.add(current_event)
            now = datetime.now(timezone.utc).isoformat()
            await session.execute(
                text(
                    "INSERT INTO event_publication(event_id,platform,target,live_url,status,first_published_at,last_published_at,applied_event_revision,provider_operation_ref) VALUES(:id,:surface,:target,:url,'published',:first,:now,:revision,:ref) ON CONFLICT(event_id,platform,target) DO UPDATE SET live_url=excluded.live_url,status='published',last_published_at=excluded.last_published_at,applied_event_revision=excluded.applied_event_revision,provider_operation_ref=excluded.provider_operation_ref"
                ),
                dict(
                    id=event.id,
                    surface=surface,
                    target=target,
                    url=url,
                    first=now if notice else None,
                    now=now,
                    revision=job.target_event_revision,
                    ref="job:" + str(job.id),
                ),
            )
            await session.commit()
        return True
    except BaseException:
        async with database.get_session() as session:
            current = await session.get(JobOutbox, job.id)
            current.status = JobStatus.paused
            current.terminal_reason = "outcome_unknown"
            session.add(current)
            await session.commit()
        raise
