from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sqlalchemy import select

from models import AcqDiscoveryRun, AcqOpportunity, AcqReviewFeedback, AcqSurface
from .config import AcqConfig, load_config
from .importer import ImportResult, import_discovery_result
from .report import publish_telegraph_report
from .review import publish_review_cards

_SAMPLE_FIXTURE = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "acq_discovery_result.sample.json"


async def run_acq_discovery_shadow(db, bot: Any | None = None, *, payload: dict[str, Any] | None = None, config: AcqConfig | None = None) -> ImportResult:
    cfg = config or load_config()
    if payload is None:
        candidate_paths = [cfg.discovery_results_path, cfg.fixture_path]
        for raw_path in candidate_paths:
            if not raw_path:
                continue
            path = Path(raw_path)
            if path.exists():
                payload = json.loads(path.read_text(encoding="utf-8"))
                break
        if payload is None and cfg.use_sample_fixture and _SAMPLE_FIXTURE.exists():
            payload = json.loads(_SAMPLE_FIXTURE.read_text(encoding="utf-8"))
        if payload is None:
            payload = {"run_id": "empty", "surfaces": [], "opportunities": []}
    result = await import_discovery_result(db, payload)
    telegraph_url = None
    if result.opportunities:
        try:
            telegraph_url = await publish_telegraph_report(result.run, result.surfaces, result.opportunities)
        except Exception:
            telegraph_url = None
        posted = 0
        if bot is not None and cfg.review_chat_id:
            posted = await publish_review_cards(db, bot, result.opportunities, config=cfg)
        async with db.get_session() as session:
            run = await session.get(AcqDiscoveryRun, result.run.id)
            if run is not None:
                stats = dict(run.stats_json or {})
                stats["review_cards_posted"] = posted
                run.stats_json = stats
                run.telegraph_url = telegraph_url
                session.add(run)
                await session.commit()
                await session.refresh(run)
                result.run = run
    return result


async def queue_counts(db) -> dict[str, int]:
    async with db.get_session() as session:
        rows = (await session.execute(select(AcqOpportunity.status))).all()
    counts: dict[str, int] = {}
    for (status,) in rows:
        counts[str(status or "pending")] = counts.get(str(status or "pending"), 0) + 1
    return counts


async def surface_counts(db) -> dict[str, int]:
    async with db.get_session() as session:
        rows = (await session.execute(select(AcqSurface.status))).all()
    counts: dict[str, int] = {}
    for (status,) in rows:
        counts[str(status or "candidate")] = counts.get(str(status or "candidate"), 0) + 1
    return counts


async def latest_report_url(db) -> str | None:
    async with db.get_session() as session:
        run = (await session.execute(
            select(AcqDiscoveryRun).where(AcqDiscoveryRun.telegraph_url.is_not(None)).order_by(AcqDiscoveryRun.id.desc()).limit(1)
        )).scalar_one_or_none()
    return run.telegraph_url if run else None


async def export_feedback_jsonl(db) -> str:
    async with db.get_session() as session:
        rows = (await session.execute(select(AcqReviewFeedback).order_by(AcqReviewFeedback.created_at.asc()))).scalars().all()
    lines = []
    for fb in rows:
        lines.append(json.dumps({
            "id": fb.id,
            "opportunity_id": fb.opportunity_id,
            "surface_id": fb.surface_id,
            "reviewer_id": fb.reviewer_id,
            "action": fb.action,
            "note": fb.note,
            "created_at": fb.created_at.isoformat() if fb.created_at else None,
        }, ensure_ascii=False))
    return "\n".join(lines) + ("\n" if lines else "")


async def list_surfaces(db, *, status: str | None = None, limit: int = 10) -> list[AcqSurface]:
    async with db.get_session() as session:
        stmt = select(AcqSurface).order_by(AcqSurface.updated_at.desc(), AcqSurface.id.desc()).limit(max(1, min(int(limit), 25)))
        if status:
            stmt = select(AcqSurface).where(AcqSurface.status == status).order_by(AcqSurface.updated_at.desc(), AcqSurface.id.desc()).limit(max(1, min(int(limit), 25)))
        return list((await session.execute(stmt)).scalars().all())


def normalize_surface_seed(url: str) -> dict[str, str]:
    raw = str(url or "").strip()
    if not raw:
        raise ValueError("surface url is required")
    lowered = raw.lower()
    platform = "vk" if "vk.com/" in lowered else "tg"
    if platform == "tg" and raw.startswith("@"):
        raw = "https://t.me/" + raw.lstrip("@")
    if platform == "tg" and "t.me/" not in raw.lower():
        raise ValueError("Telegram seed must be @username or https://t.me/... URL")
    handle = raw.rstrip("/").split("/")[-1].split("?")[0].lstrip("@")
    if not handle:
        raise ValueError("could not extract surface handle")
    return {
        "platform": platform,
        "surface_type": "community" if platform == "vk" else "unknown_public",
        "url": raw,
        "handle": handle,
        "external_id": f"{platform}:{handle}",
    }


async def add_surface_seed(db, url: str, *, reviewer_id: int | None = None, note: str | None = None) -> AcqSurface:
    seed = normalize_surface_seed(url)
    async with db.get_session() as session:
        existing = (await session.execute(
            select(AcqSurface).where(
                AcqSurface.platform == seed["platform"],
                AcqSurface.external_id == seed["external_id"],
            )
        )).scalar_one_or_none()
        if existing is None:
            surface = AcqSurface(
                platform=seed["platform"],
                surface_type=seed["surface_type"],
                url=seed["url"],
                handle=seed["handle"],
                external_id=seed["external_id"],
                status="candidate",
                source="manual",
                review_note=note,
            )
        else:
            surface = existing
            surface.url = seed["url"]
            surface.handle = seed["handle"]
            surface.source = surface.source or "manual"
            surface.review_note = note or surface.review_note
        if reviewer_id:
            surface.reviewed_by = reviewer_id
        session.add(surface)
        await session.commit()
        await session.refresh(surface)
        return surface
