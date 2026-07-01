from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from models import AcqDiscoveryRun, AcqLinkTarget, AcqOpportunity, AcqSurface
from .cooldowns import next_surface_scan_after, opportunity_expires_at
from .link_targets import VALID_LINK_TARGET_KINDS
from .scoring import conservative_reach_low
from .sticker import sticker_fit_from_observation
from .surface_filters import is_tg_bot_or_service_surface


def _dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        out = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return out if out.tzinfo else out.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _jsonable(value: Any, default: Any) -> Any:
    if value is None:
        return default
    try:
        json.dumps(value, ensure_ascii=False)
        return value
    except Exception:
        return default


def _dedupe_key(platform: str, context_url: str, snippet: str | None = None) -> str:
    raw = f"{platform}|{context_url}|{(snippet or '')[:120]}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _surface_was_scanned(item: dict[str, Any]) -> bool:
    """Return True only for surfaces the runtime actually touched this run.

    Runtime payloads intentionally include many seed/frontier links so the map can
    show the backlog. Those seed-only rows must not be marked as scanned, or the
    next Kaggle run will keep starting from the same head of the seed list instead
    of walking the unscanned frontier.
    """
    reach = item.get("reach") if isinstance(item, dict) else None
    basis = str((reach or {}).get("basis") or "").strip().lower()
    return bool(basis and basis != "seed_only")


@dataclass
class ImportResult:
    run: AcqDiscoveryRun
    surfaces: list[AcqSurface]
    opportunities: list[AcqOpportunity]
    skipped_duplicate_contexts: int = 0


async def import_discovery_result(db, payload: dict[str, Any]) -> ImportResult:
    generated_at = _dt(payload.get("generated_at")) or datetime.now(timezone.utc)
    surfaces_in = list(payload.get("surfaces") or [])
    opps_in = list(payload.get("opportunities") or [])
    async with db.get_session() as session:
        run = AcqDiscoveryRun(
            started_at=generated_at,
            status="running",
            source_config_json={"run_id": payload.get("run_id"), "source": "import"},
            stats_json={"surfaces_in": len(surfaces_in), "opportunities_in": len(opps_in)},
        )
        session.add(run)
        await session.commit()
        await session.refresh(run)

        surfaces_by_external: dict[str, AcqSurface] = {}
        surfaces: list[AcqSurface] = []
        for item in surfaces_in:
            platform = str(item.get("platform") or "").strip().lower() or "tg"
            external_id = str(item.get("external_id") or item.get("url") or "").strip() or None
            incoming_status = str(item.get("status") or "candidate").strip() or "candidate"
            if platform == "tg" and is_tg_bot_or_service_surface(url=item.get("url"), handle=item.get("handle"), external_id=external_id):
                incoming_status = "rejected_bot_or_service"
                item = dict(item)
                item["status"] = incoming_status
                risk_in = dict(item.get("risk") or {})
                risk_in.setdefault("level", "rejected")
                risk_in.setdefault("reason", "telegram_bot_or_service")
                item["risk"] = risk_in
            existing = None
            if external_id:
                existing = (await session.execute(
                    select(AcqSurface).where(AcqSurface.platform == platform, AcqSurface.external_id == external_id)
                )).scalar_one_or_none()
            reach = _jsonable(item.get("reach"), {})
            risk = _jsonable(item.get("risk"), {})
            scanned_this_run = _surface_was_scanned(item)
            if existing is None:
                surface = AcqSurface(
                    platform=platform,
                    surface_type=str(item.get("surface_type") or "unknown"),
                    url=str(item.get("url") or ""),
                    title=item.get("title"),
                    handle=item.get("handle"),
                    external_id=external_id,
                    status=incoming_status,
                    source=str(item.get("source") or "discovered"),
                    topic_hint=item.get("topic_hint"),
                    topic_cluster=item.get("topic_cluster"),
                    reach_json=reach,
                    risk_json=risk,
                    last_scan_at=generated_at if scanned_this_run else None,
                    next_scan_after=next_surface_scan_after(incoming_status, now=generated_at) if scanned_this_run else None,
                )
                session.add(surface)
                await session.flush()
            else:
                surface = existing
                if incoming_status.startswith("rejected"):
                    surface.status = incoming_status
                surface.title = item.get("title") or surface.title
                surface.url = str(item.get("url") or surface.url or "")
                surface.handle = item.get("handle") or surface.handle
                surface.topic_hint = item.get("topic_hint") or surface.topic_hint
                surface.topic_cluster = item.get("topic_cluster") or surface.topic_cluster
                surface.reach_json = reach or surface.reach_json
                surface.risk_json = risk or surface.risk_json
                if scanned_this_run:
                    surface.last_scan_at = generated_at
                    surface.next_scan_after = next_surface_scan_after(surface.status, now=generated_at)
                session.add(surface)
            await session.flush()
            surfaces.append(surface)
            if external_id:
                surfaces_by_external[external_id] = surface

        opportunities: list[AcqOpportunity] = []
        skipped = 0
        for item in opps_in:
            platform = str(item.get("platform") or "").strip().lower() or "tg"
            context_url = str(item.get("context_url") or "").strip()
            if not context_url:
                continue
            dedupe = str(item.get("dedupe_key") or "").strip() or _dedupe_key(platform, context_url, item.get("context_text_snippet"))
            existing_opp = (await session.execute(select(AcqOpportunity).where(AcqOpportunity.dedupe_key == dedupe))).scalar_one_or_none()
            if existing_opp is not None:
                skipped += 1
                continue
            surface = surfaces_by_external.get(str(item.get("surface_external_id") or "").strip())
            link = item.get("link_target") or {}
            fallback = item.get("fallback_link_target") or {}
            kind = str(link.get("kind") or "none").strip() or "none"
            if kind not in VALID_LINK_TARGET_KINDS:
                kind = "other"
            reach = item.get("reach") or {}
            context_dt = _dt(item.get("context_created_at"))
            if not reach.get("low"):
                reach = conservative_reach_low(platform=platform, surface_type=getattr(surface, "surface_type", None), context_created_at=context_dt)
            scores = item.get("scores") or {}
            sticker_obs = _jsonable(item.get("sticker_observation"), {})
            sticker_fit = str(sticker_obs.get("fit") or "").strip() or sticker_fit_from_observation(sticker_obs)
            event_ids = _jsonable(item.get("event_ids"), [])
            candidate_events = _jsonable(item.get("candidate_events"), [])
            opp = AcqOpportunity(
                run_id=run.id,
                platform=platform,
                surface_id=surface.id if surface else None,
                context_url=context_url,
                context_external_id=item.get("context_external_id"),
                context_created_at=context_dt,
                context_text_snippet=item.get("context_text_snippet"),
                evidence_json=_jsonable(item.get("evidence"), {}),
                matched_intent=item.get("matched_intent"),
                topic_cluster=item.get("topic_cluster"),
                event_ids_json=event_ids,
                candidate_events_json=candidate_events,
                link_target_kind=kind,
                link_target_url=link.get("url"),
                link_target_label=link.get("label"),
                link_target_reason=link.get("reason"),
                fallback_link_target_url=fallback.get("url"),
                reach_low=int(reach.get("low") or 0),
                reach_confidence=str(reach.get("confidence") or "low"),
                relevance_score=float(scores.get("relevance") or 0.0),
                safety_risk=str(scores.get("safety_risk") or "low"),
                spam_risk=str(scores.get("spam_risk") or "low"),
                sticker_fit=sticker_fit if sticker_fit in {"no", "weak", "possible", "strong"} else "no",
                sticker_observation_json=sticker_obs,
                expires_at=opportunity_expires_at(context_dt, now=generated_at),
                dedupe_key=dedupe,
            )
            session.add(opp)
            await session.flush()
            if opp.link_target_url:
                session.add(AcqLinkTarget(
                    kind=opp.link_target_kind,
                    url=opp.link_target_url,
                    label=opp.link_target_label or opp.link_target_kind,
                    topic_cluster=opp.topic_cluster,
                    event_id=(event_ids[0] if len(event_ids) == 1 and isinstance(event_ids[0], int) else None),
                    active=True,
                ))
            opportunities.append(opp)
        ydb_export = None
        try:
            from .ydb_stats import export_discovery_payload_to_ydb
            ydb_export = await export_discovery_payload_to_ydb(payload, run_db_id=run.id)
        except Exception:
            ydb_export = None
        run.status = "done"
        run.finished_at = datetime.now(timezone.utc)
        run.stats_json = {"surfaces": len(surfaces), "opportunities": len(opportunities), "skipped_duplicate_contexts": skipped, "ydb_export": ydb_export}
        session.add(run)
        await session.commit()
        await session.refresh(run)
        for s in surfaces:
            await session.refresh(s)
        for o in opportunities:
            await session.refresh(o)
        return ImportResult(run=run, surfaces=surfaces, opportunities=opportunities, skipped_duplicate_contexts=skipped)
