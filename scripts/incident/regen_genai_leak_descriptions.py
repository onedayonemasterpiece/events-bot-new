#!/usr/bin/env python3
"""INC-2026-05-29 cleanup: repair events whose `description` is a stringified
google-genai `GenerateContentResponse` dump.

For each affected event:
  1. detect the SDK-repr dump (markup.looks_like_genai_response_dump);
  2. regenerate a fact-first description via the (now-fixed) Smart Update
     pipeline from canonical EventSourceFact rows (same chain as
     `/rebuild_event <id> --regen-desc`);
  3. if regeneration fails/empty, CLEAR the dump (description -> None) so no
     public surface can read it;
  4. enqueue the standard rebuild jobs (Telegraph + VK sync + pages) which the
     running bot drains, re-rendering the public surfaces.

Dry-run by default; pass --apply to mutate. Run on the prod machine so it shares
`/data/db.sqlite` and the JobOutbox the live bot drains.

Usage:
  python -m scripts.incident.regen_genai_leak_descriptions [--db /data/db.sqlite] \
      [--ids 5419,5398,...] [--apply]
"""
from __future__ import annotations

import argparse
import asyncio
import logging

from sqlalchemy import select

from db import Database
from models import Event, EventSource, EventSourceFact
from markup import looks_like_genai_response_dump

# Events found leaking the SDK repr in the 2026-05-29 production scan.
DEFAULT_IDS = [
    3979, 4006, 4775, 4776, 4899, 4957, 5081, 5083,
    5229, 5351, 5398, 5410, 5411, 5419, 5424, 5429,
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("regen_genai_leak")


async def _regen_description(session, su, event) -> str | None:
    """Mirror of handle_rebuild_event_command(--regen-desc) regen chain."""
    if getattr(su, "SMART_UPDATE_LLM_DISABLED", False):
        log.warning("event %s: SMART_UPDATE_LLM_DISABLED — cannot regenerate", event.id)
        return None
    rows = (
        await session.execute(
            select(EventSourceFact.fact)
            .join(EventSource, EventSourceFact.source_id == EventSource.id)
            .where(
                EventSourceFact.event_id == int(event.id),
                EventSourceFact.status.in_(("added", "duplicate")),
            )
            .order_by(EventSourceFact.created_at.asc(), EventSourceFact.id.asc())
        )
    ).all()
    canonical_facts = [str(r[0]).strip() for r in (rows or []) if (r and str(r[0] or "").strip())]
    canonical_facts = su._dedupe_source_facts(canonical_facts)[:120]
    anchors = [
        getattr(event, "date", None) or "",
        getattr(event, "time", None) or "",
        getattr(event, "city", None) or "",
        getattr(event, "location_name", None) or "",
        getattr(event, "location_address", None) or "",
    ]
    facts_text_clean = su._facts_text_clean_from_facts(canonical_facts, max_items=36, anchors=anchors)
    if not facts_text_clean:
        log.warning("event %s: no canonical facts for fact-first regen", event.id)
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


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="/data/db.sqlite")
    ap.add_argument("--ids", default="")
    ap.add_argument("--apply", action="store_true", help="commit changes + enqueue rebuilds")
    args = ap.parse_args()

    ids = [int(x) for x in args.ids.split(",") if x.strip()] if args.ids.strip() else list(DEFAULT_IDS)

    db = Database(args.db)
    await db.init()
    import main as main_mod
    import smart_event_update as su

    summary: list[str] = []
    for eid in ids:
        async with db.get_session() as session:
            event = await session.get(Event, eid)
            if not event:
                summary.append(f"{eid}: MISSING")
                continue
            desc = (getattr(event, "description", None) or "").strip()
            is_dump = looks_like_genai_response_dump(desc)
            if not is_dump:
                summary.append(f"{eid}: clean (skip)")
                continue
            new_desc = await _regen_description(session, su, event)
            action = "regen" if new_desc else "cleared(no_regen)"
            if args.apply:
                event.description = new_desc  # None clears the dump
                await session.commit()
            summary.append(f"{eid}: dump -> {action} (len={len(new_desc or '')})")
        if args.apply:
            # Re-render Telegraph + re-sync VK + pages; running bot drains the queue.
            async with db.get_session() as session:
                event = await session.get(Event, eid)
            if event:
                try:
                    res = await main_mod.schedule_event_update_tasks(db, event)
                    log.info("event %s: enqueued %s", eid, {k.value: v for k, v in res.items()})
                except Exception:
                    log.exception("event %s: schedule_event_update_tasks failed", eid)

    log.info("apply=%s", args.apply)
    for line in summary:
        log.info("  %s", line)


if __name__ == "__main__":
    asyncio.run(main())
