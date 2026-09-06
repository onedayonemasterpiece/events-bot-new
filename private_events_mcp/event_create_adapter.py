from __future__ import annotations

from collections import Counter
from typing import Any

from sqlalchemy import select

from .event_create import EventCreateRequest


class MainEventCreateExecutor:
    """Thin adapter over the existing parser + Smart Update ingestion path."""

    def __init__(self, database: Any, *, resolve_media=None) -> None:
        self.database = database
        self.resolve_media = resolve_media

    async def create(self, request: EventCreateRequest) -> dict[str, Any]:
        # Lazy imports avoid pulling the bot/provider stack when the feature is off.
        import main
        from models import Event, JobOutbox, SmartUpdateCandidateState
        from smart_event_update import canonicalize_identity_url

        media = None
        if request.media:
            from .tool_catalog import ToolExecutionError
            if self.resolve_media is None:
                return {"status": "rejected", "error_code": "EVENT_ASSETS_DISABLED", "event_ids": [], "jobs": []}
            try:
                media = await self.resolve_media(request)
            except ToolExecutionError as exc:
                # Proven pre-parser failure, not an ambiguous post-write outcome.
                return {"status": "rejected", "error_code": exc.error_code, "event_ids": [], "jobs": []}
        try:
            result = await main.add_events_from_text(
                self.database,
                request.raw_text,
                request.source_url,
                html_text=None,
                media=media,
                poster_media=None,
                force_festival=False,
                raise_exc=True,
                creator_id=None,
                display_source=False,
                source_channel=None,
                source_type_override="manual",
                source_url_override=request.source_locator,
                bot=None,
                defer_external_projections=True,
                require_single_event=True,
                allow_festival_queue=False,
                allow_lifecycle_actions=False,
            )
        except main.MultiEventSourceRequiresSeparateRequests:
            return {
                "status": "rejected",
                "error_code": "MULTI_EVENT_SOURCE_REQUIRES_SEPARATE_REQUESTS",
                "event_ids": [],
                "jobs": [],
            }
        except main.FestivalSourceRequiresDedicatedIntake:
            return {
                "status": "rejected",
                "error_code": "FESTIVAL_SOURCE_REQUIRES_DEDICATED_INTAKE",
                "event_ids": [],
                "jobs": [],
            }
        except main.EventSourceRequiresExactlyOneEvent:
            return {
                "status": "rejected",
                "error_code": "EVENT_SOURCE_REQUIRES_EXACTLY_ONE_EVENT",
                "event_ids": [],
                "jobs": [],
            }
        except main.LifecycleSourceRequiresDedicatedChange:
            return {
                "status": "rejected",
                "error_code": "LIFECYCLE_SOURCE_REQUIRES_EVENT_CHANGE",
                "event_ids": [],
                "jobs": [],
            }

        events: list[dict[str, Any]] = []
        event_ids: list[int] = []
        for saved, added, _lines, status in result:
            if not isinstance(saved, Event) or not saved.id:
                continue
            event_id = int(saved.id)
            if event_id in event_ids:
                continue
            event_ids.append(event_id)
            events.append(
                {
                    "event_id": event_id,
                    "result": "created" if added else "merged_or_replay",
                    "status": status,
                    "title": saved.title,
                    "date": saved.date,
                    "time": saved.time,
                    "location_name": saved.location_name,
                }
            )

        canonical_locator = canonicalize_identity_url(request.source_locator)
        async with self.database.get_session() as session:
            jobs = []
            if event_ids:
                rows = (
                    await session.execute(
                        select(JobOutbox)
                        .where(JobOutbox.event_id.in_(event_ids))
                        .order_by(JobOutbox.id.asc())
                    )
                ).scalars().all()
                jobs = [
                    {
                        "job_id": int(job.id),
                        "event_id": int(job.event_id),
                        "task": getattr(job.task, "value", str(job.task)),
                        "status": getattr(job.status, "value", str(job.status)),
                    }
                    for job in rows
                    if job.id is not None
                ]
            candidate_rows = (
                await session.execute(
                    select(SmartUpdateCandidateState)
                    .where(
                        SmartUpdateCandidateState.canonical_source_url
                        == canonical_locator
                    )
                    .order_by(SmartUpdateCandidateState.id.asc())
                )
            ).scalars().all()

        candidate_receipts = [
            {
                "candidate_key": row.candidate_key,
                "outcome": row.current_outcome,
                "accepted_event_id": row.accepted_event_id,
                "reason": row.reason,
            }
            for row in candidate_rows
        ]
        status_counts = Counter(str(job["status"]) for job in jobs)
        decision = getattr(result, "source_decision", None)
        disposition = getattr(getattr(decision, "disposition", None), "value", None)
        retry_reason = getattr(getattr(decision, "retry_reason", None), "value", None)

        if event_ids:
            return {
                "status": "accepted",
                "event_ids": event_ids,
                "events": events,
                "jobs": jobs,
                "jobs_scope": "current event JobOutbox rows after accepted Smart Update",
                "job_status_counts": dict(sorted(status_counts.items())),
                "candidate_receipts": candidate_receipts,
                "source_disposition": disposition,
            }
        if bool(getattr(decision, "is_retry", False)):
            return {
                "status": "failed",
                "error_code": "SOURCE_PARSE_RETRY_REQUIRED",
                "event_ids": [],
                "jobs": [],
                "source_disposition": disposition,
                "retry_reason": retry_reason,
                "candidate_receipts": candidate_receipts,
            }
        if disposition in {"CONFIRMED_NO_EVENT", "LIFECYCLE_ONLY"}:
            return {
                "status": "rejected",
                "error_code": "NO_CREATABLE_EVENT",
                "event_ids": [],
                "jobs": [],
                "source_disposition": disposition,
                "candidate_receipts": candidate_receipts,
            }
        return {
            "status": "rejected",
            "error_code": "EVENT_REQUIRED_FIELDS_MISSING",
            "event_ids": [],
            "jobs": [],
            "source_disposition": disposition,
            "candidate_receipts": candidate_receipts,
        }
