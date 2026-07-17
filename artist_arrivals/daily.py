from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from db import Database
from models import ArtistDigestIssue, JobTask
from ops_run import finish_ops_run, start_ops_run

from .publisher import publish_artist_arrival_issue
from .service import (
    build_artist_arrival_issue,
    ensure_artist_arrivals_promo_campaign,
    ensure_curated_artist_data,
    prune_artist_arrival_shadow_issues,
)

logger = logging.getLogger(__name__)


async def run_artist_arrivals_daily(
    db: Database,
    bot=None,
    *,
    trigger: str = "scheduled",
) -> ArtistDigestIssue:
    """Build the single frozen daily issue used by social and static surfaces.

    Public delivery remains activity/flag controlled. The daily builder always
    records a shadow/threshold result so operators can review precision without
    producing empty posts.
    """

    run_id = await start_ops_run(db, kind="artist_arrivals_daily", trigger=trigger)
    previous_hash = None
    async with db.get_session() as session:
        previous_hash = (
            await session.execute(
                select(ArtistDigestIssue.manifest_hash)
                .order_by(ArtistDigestIssue.created_at.desc())
                .limit(1)
            )
        ).scalars().first()
    try:
        seed_stats = await ensure_curated_artist_data(db)
        campaign_stats = await ensure_artist_arrivals_promo_campaign(db)
        issue = await build_artist_arrival_issue(db)
        delivery = await publish_artist_arrival_issue(db, issue, bot)
        retention = await prune_artist_arrival_shadow_issues(
            db,
            keep_issue_id=issue.id,
        )
        changed = issue.manifest_hash != previous_hash
        if changed and issue.items_json and os.getenv("ENABLE_STATIC_SITE_KAGGLE_BUILDER", "").strip().lower() in {"1", "true", "yes", "on"}:
            event_ids = [int(x) for item in issue.items_json for x in item.get("event_ids", [])]
            if event_ids:
                from main import enqueue_job

                await enqueue_job(
                    db,
                    event_ids[0],
                    JobTask.static_site_build,
                    payload={"reason": "artist_arrivals", "manifest_hash": issue.manifest_hash},
                    coalesce_key="static_site_build:prod",
                    next_run_at=datetime.now(timezone.utc) + timedelta(minutes=3),
                )
        await finish_ops_run(
            db,
            run_id=run_id,
            status="success" if issue.items_json else "empty",
            metrics={
                "unique_artists": issue.unique_artist_count,
                "unique_projects": issue.unique_project_count,
                "meets_threshold": issue.meets_threshold,
                "manifest_changed": changed,
            },
            details={
                "issue_id": issue.id,
                "manifest_hash": issue.manifest_hash,
                "seed": seed_stats,
                "campaign": campaign_stats,
                "delivery": {
                    "mode": delivery.mode,
                    "ready": delivery.ready,
                    "blockers": delivery.blockers,
                    "targets": delivery.targets,
                },
                "retention": retention,
            },
        )
        return issue
    except Exception as exc:
        await finish_ops_run(
            db,
            run_id=run_id,
            status="failed",
            details={"error": str(exc) or type(exc).__name__},
        )
        raise
