#!/usr/bin/env python3
"""INC-2026-05-29 repair: fix events damaged by the genai-repr leak and by the
location-as-title placeholder fallback, then re-render the public surfaces.

Two independent defects, repaired in one pass (run AFTER the fix is deployed):

  (A) description = stringified google-genai `GenerateContentResponse` dump.
      -> regenerate a fact-first description via the (now-fixed) pipeline; if
         regeneration is empty, CLEAR the dump (description -> None) so no public
         surface can read it.

  (B) title = generic "<event_type> — <venue>" placeholder.
      -> recover a grounded real title via the new native-schema title-recovery
         stage; keep the placeholder only if nothing grounded is found.

Affected events are discovered dynamically (no hardcoded list), then for each
changed event the standard rebuild jobs are enqueued (Telegraph + VK sync +
pages) which the running bot drains.

Dry-run by default; pass --apply to mutate. Run on the prod machine so it shares
`/data/db.sqlite` and the JobOutbox the live bot drains:

  cd /app && PYTHONPATH=/app python -u scripts/incident/regen_genai_leak_descriptions.py [--apply] [--ids 1,2]
"""
from __future__ import annotations

import argparse
import asyncio
import datetime
import logging
from types import SimpleNamespace

from sqlalchemy import select

from db import Database
from models import Event, EventPoster, EventSource, EventSourceFact
from markup import looks_like_genai_response_dump

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("repair_inc_20260529")


async def _canonical_facts(session, su, eid: int) -> list[str]:
    rows = (
        await session.execute(
            select(EventSourceFact.fact)
            .join(EventSource, EventSourceFact.source_id == EventSource.id)
            .where(
                EventSourceFact.event_id == int(eid),
                EventSourceFact.status.in_(("added", "duplicate")),
            )
            .order_by(EventSourceFact.created_at.asc(), EventSourceFact.id.asc())
        )
    ).all()
    facts = [str(r[0]).strip() for r in (rows or []) if (r and str(r[0] or "").strip())]
    return su._dedupe_source_facts(facts)[:120]


async def _regen_description(session, su, event) -> str | None:
    """Mirror of handle_rebuild_event_command(--regen-desc) regen chain."""
    if getattr(su, "SMART_UPDATE_LLM_DISABLED", False):
        return None
    canonical_facts = await _canonical_facts(session, su, event.id)
    anchors = [
        getattr(event, "date", None) or "",
        getattr(event, "time", None) or "",
        getattr(event, "city", None) or "",
        getattr(event, "location_name", None) or "",
        getattr(event, "location_address", None) or "",
    ]
    facts_text_clean = su._facts_text_clean_from_facts(canonical_facts, max_items=36, anchors=anchors)
    if not facts_text_clean:
        return None
    try:
        ff_desc = await su._llm_fact_first_description_md(
            title=getattr(event, "title", None),
            event_type=getattr(event, "event_type", None),
            facts_text_clean=facts_text_clean,
            anchors=anchors,
            label=f"inc20260529:{event.id}",
        )
    except Exception:
        log.exception("event %s: regen LLM call failed", event.id)
        ff_desc = None
    if not ff_desc:
        return None
    cleaned = su._dedupe_description(ff_desc) or ff_desc
    cleaned = su._normalize_plaintext_paragraphs(cleaned) or cleaned
    cleaned = su._promote_review_bullets_to_blockquotes(cleaned) or cleaned
    cleaned = su._normalize_blockquote_markers(cleaned) or cleaned
    cleaned = su._sanitize_description_output(cleaned, source_text=getattr(event, "source_text", None) or "") or cleaned
    cleaned = su._ensure_minimal_description_headings(cleaned) or cleaned
    cleaned = su._clip(cleaned, su.SMART_UPDATE_DESCRIPTION_MAX_CHARS)
    cleaned = (cleaned or "").strip()
    if not cleaned or looks_like_genai_response_dump(cleaned):
        return None
    return cleaned


async def _recover_title(session, su, event) -> str | None:
    """Recover a grounded title for a generic '<type> — <venue>' placeholder."""
    posters = (
        await session.execute(select(EventPoster).where(EventPoster.event_id == int(event.id)))
    ).scalars().all()
    cand = SimpleNamespace(
        source_text=getattr(event, "source_text", None) or "",
        raw_excerpt=None,
        location_name=getattr(event, "location_name", None) or "",
        event_type=getattr(event, "event_type", None) or "",
        city=getattr(event, "city", None) or "",
        source_type="repair",
        source_url=f"event:{event.id}",
        posters=[SimpleNamespace(ocr_title=p.ocr_title, ocr_text=p.ocr_text) for p in posters],
    )
    facts = await _canonical_facts(session, su, event.id)
    try:
        return await su._llm_recover_event_title(
            cand, normalized_event_type=getattr(event, "event_type", None), facts=facts
        )
    except Exception:
        log.exception("event %s: title recovery failed", event.id)
        return None


def _is_placeholder_title(su, event) -> bool:
    return su._is_generic_title_event_type_venue(
        getattr(event, "title", None),
        event_type=getattr(event, "event_type", None),
        location_name=getattr(event, "location_name", None),
        city=getattr(event, "city", None),
    )


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="/data/db.sqlite")
    ap.add_argument("--ids", default="", help="explicit comma-separated ids (default: dynamic scan)")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--mode", choices=["both", "desc", "title"], default="both")
    args = ap.parse_args()

    db = Database(args.db)
    await db.init()
    import main as main_mod
    import smart_event_update as su

    today = datetime.date.today().isoformat()
    async with db.get_session() as session:
        if args.ids.strip():
            ids = [int(x) for x in args.ids.split(",") if x.strip()]
            rows = []
            for eid in ids:
                e = await session.get(Event, eid)
                if e:
                    rows.append(e)
        else:
            rows = (
                await session.execute(select(Event).where(Event.date >= today))
            ).scalars().all()

        targets: list[tuple[int, bool, bool]] = []  # (id, fix_desc, fix_title)
        for e in rows:
            fix_desc = args.mode in ("both", "desc") and looks_like_genai_response_dump(
                getattr(e, "description", None) or ""
            )
            fix_title = args.mode in ("both", "title") and _is_placeholder_title(su, e)
            if fix_desc or fix_title:
                targets.append((e.id, fix_desc, fix_title))

    log.info("scan: %d events need repair (apply=%s)", len(targets), args.apply)

    summary: list[str] = []
    for eid, fix_desc, fix_title in targets:
        changed = False
        notes = []
        async with db.get_session() as session:
            event = await session.get(Event, eid)
            if not event:
                continue
            if fix_desc:
                new_desc = await _regen_description(session, su, event)
                notes.append(f"desc={'regen' if new_desc else 'cleared'}")
                if args.apply:
                    event.description = new_desc  # None clears the dump
                changed = True
            if fix_title:
                new_title = await _recover_title(session, su, event)
                if new_title:
                    notes.append(f"title={new_title!r}")
                    if args.apply:
                        event.title = new_title
                    changed = True
                else:
                    notes.append("title=kept(no_recovery)")
            if args.apply and changed:
                await session.commit()
        summary.append(f"{eid}: {', '.join(notes)}")
        if args.apply and changed:
            async with db.get_session() as session:
                event = await session.get(Event, eid)
            if event:
                try:
                    res = await main_mod.schedule_event_update_tasks(db, event)
                    log.info("event %s: enqueued %s", eid, {k.value: v for k, v in res.items()})
                except Exception:
                    log.exception("event %s: schedule_event_update_tasks failed", eid)

    log.info("=== summary (apply=%s) ===", args.apply)
    for line in summary:
        log.info("  %s", line)


if __name__ == "__main__":
    asyncio.run(main())
