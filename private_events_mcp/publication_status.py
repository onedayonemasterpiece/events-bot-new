"""Revision-bound outbox evidence. A URL and a completed job alone are not receipts."""

from contextvars import ContextVar
from contextlib import contextmanager
from datetime import datetime, timezone
from sqlalchemy import text
from models import Event
from static_site_release import event_public_revision
from .actors import ActorContext
from .event_commands import one, fail
from .event_publication_receipts import _public_url
from .tool_catalog import ToolSpec

revision_binding = ContextVar("event_operation_revision_binding", default=None)
SURFACES = {
    "telegram": ("tg_event_publish", "tg_event_post_url"),
    "vk": ("vk_sync", "source_vk_post_url"),
    "telegraph": ("telegraph_build", "telegraph_url"),
    "calendar": ("ics_publish", "ics_url"),
    "static_site": ("static_site_build", None),
}
BOUND_TASKS = {
    "tg_event_publish",
    "vk_sync",
    "telegraph_build",
    "ics_publish",
    "tg_ics_post",
}


@contextmanager
def bind_event_operation(event, candidate):
    context = getattr(candidate, "event_operation_context", None)
    token = (
        revision_binding.set((event_public_revision(event), context["operation_ref"]))
        if context
        else None
    )
    try:
        yield
    finally:
        if token is not None:
            revision_binding.reset(token)


async def publication_job_is_current(database, job):
    if not job.target_event_revision:
        return True
    async with database.get_session() as session:
        event = await session.get(Event, job.event_id)
        return bool(event and event_public_revision(event) == job.target_event_revision)


async def record_worker_receipt(database, job, *, changed, url, had_publication=False):
    if not job.target_event_revision or not changed or not url:
        return
    surface = next(
        (s for s, (task, _) in SURFACES.items() if task == job.task.value), None
    )
    public_url = _public_url(url, surface) if surface else None
    if not public_url:
        return
    async with database.get_session() as session:
        event = await session.get(Event, job.event_id)
        if event is None or event_public_revision(event) != job.target_event_revision:
            return
        now = datetime.now(timezone.utc).isoformat()
        await session.execute(
            text(
                "INSERT INTO event_publication(event_id,platform,target,stored_url,live_url,status,first_published_at,last_published_at,applied_event_revision,provider_operation_ref) VALUES(:id,:surface,'managed',:url,:url,'published',:first,:now,:revision,:ref) ON CONFLICT(event_id,platform,target) DO UPDATE SET live_url=excluded.live_url,status='published',last_published_at=excluded.last_published_at,applied_event_revision=excluded.applied_event_revision,provider_operation_ref=excluded.provider_operation_ref"
            ),
            dict(
                id=event.id,
                surface=surface,
                url=public_url,
                first=None if had_publication else now,
                now=now,
                revision=job.target_event_revision,
                ref="job:" + str(job.id),
            ),
        )
        await session.commit()


class PublicationReadService:
    def __init__(self, database, policy):
        self.database, self.policy = database, policy

    async def read(self, actor, event_id):
        async with self.database.get_session() as session:
            await self.policy.check_session(
                session, actor, "publication_read", event_id=event_id
            )
            event = await session.get(Event, event_id)
            if event is None:
                fail("NOT_FOUND")
            current = event_public_revision(event)
            result = []
            for surface, (task, field) in SURFACES.items():
                job = await one(
                    session,
                    "SELECT * FROM joboutbox WHERE event_id=:id AND task=:task AND (coalesce_key IS NULL OR coalesce_key NOT LIKE 'notice:%') ORDER BY id DESC LIMIT 1",
                    id=event_id,
                    task=task,
                )
                receipt = await one(
                    session,
                    "SELECT * FROM event_publication WHERE event_id=:id AND platform=:surface AND target='managed'",
                    id=event_id,
                    surface=surface,
                )
                transport = "not_planned"
                state = "unknown"
                applied = receipt["applied_event_revision"] if receipt else None
                if receipt and receipt["status"] == "published":
                    transport = "published"
                if applied:
                    state = "up_to_date" if applied == current else "stale"
                if job:
                    status = job["status"]
                    if status in {"pending", "running", "error", "paused"}:
                        transport = {
                            "pending": "queued",
                            "running": "running",
                            "error": "retry_wait",
                            "paused": "paused",
                        }[status]
                        if job["target_event_revision"] == current:
                            state = "update_queued"
                    if job["terminal_reason"] == "failed":
                        transport = "failed"
                    if job["terminal_reason"] == "outcome_unknown":
                        transport = "outcome_unknown"
                    if job["terminal_reason"] == "superseded":
                        state = "superseded"
                    elif status == "done" and not receipt:
                        transport = "outcome_unknown"
                raw_url = (
                    receipt["live_url"]
                    if receipt
                    else getattr(event, field, None)
                    if field
                    else None
                )
                result.append(
                    {
                        "surface": surface,
                        "transport": transport,
                        "revision": state,
                        "public_url": _public_url(raw_url, surface),
                        "applied_revision": applied,
                        "target_revision": job["target_event_revision"]
                        if job
                        else None,
                        "first_published_at": receipt["first_published_at"]
                        if receipt
                        else None,
                        "last_published_at": receipt["last_published_at"]
                        if receipt
                        else None,
                        "job_ref": "job:" + str(job["id"]) if job else None,
                        "operation_ref": job["event_operation_ref"] if job else None,
                        "attempts": job["attempts"] if job else 0,
                        "next_retry": job["next_run_at"]
                        if job and job["status"] == "error"
                        else None,
                        "error": "PUBLICATION_RETRY_REQUIRED"
                        if job and job["last_error"]
                        else None,
                    }
                )
            notices = (
                (
                    await session.execute(
                        text(
                            "SELECT platform,live_url,status,first_published_at,applied_event_revision,provider_operation_ref FROM event_publication WHERE event_id=:id AND target LIKE 'notice:%' ORDER BY id DESC LIMIT 16"
                        ),
                        {"id": event_id},
                    )
                )
                .mappings()
                .all()
            )
            public_notices = [
                {**dict(row), "live_url": _public_url(row["live_url"], row["platform"])}
                for row in notices
            ]
            return {
                "event_id": event_id,
                "current_revision": current,
                "notices": public_notices,
                "surfaces": result,
                "last_checked": datetime.now(timezone.utc).isoformat(),
                "live_verified": False,
            }

    def tools(self, partner=False):
        async def read(args, context):
            event_id = args.get("event_id")
            if type(event_id) is not int or event_id < 1:
                fail("INVALID_ARGUMENTS")
            return await self.read(ActorContext.from_mcp(context), event_id)

        return (
            ToolSpec(
                name="event_publication_status",
                title="Current event publication status",
                description="Read an exact authorized event after create/change acceptance. Transport and revision are separate. Only durable provider receipts prove publication; URL alone does not. unknown/outcome_unknown require readback, never blind retry.",
                input_schema={
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {"event_id": {"type": "integer", "minimum": 1}},
                    "required": ["event_id"],
                },
                output_schema={"type": "object"},
                scopes=frozenset(
                    {"partner:publications:read" if partner else "operations:read"}
                ),
                handler=read,
                cacheable=False,
                publicly_discoverable=False,
            ),
        )
