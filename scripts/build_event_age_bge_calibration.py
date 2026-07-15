#!/usr/bin/env python3
"""Build a private, leakage-masked BGE calibration corpus from declared ages."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import Database  # noqa: E402
from event_age_bge_service import CORPUS_VERSION, build_event_age_bge_input  # noqa: E402
from models import Event, EventPoster, EventSource  # noqa: E402


CLASSES = {"0+", "6+", "12+", "16+", "18+"}
AGE_TOKEN = re.compile(
    r"(?iu)(?<!\d)(?:0|6|12|16|18)\s*\+|"
    r"\b(?:старше|от)\s+(?:0|6|12|16|18)\s*(?:лет|года?)?\b|"
    r"\b(?:возраст(?:ное)?\s+ограничение|ценз)\s*[:—-]?\s*(?:0|6|12|16|18)\b"
)


def mask_declared_age(text: str) -> str:
    return AGE_TOKEN.sub("[AGE_MARK_REMOVED]", text)


def input_hash(event_id: int, text: str) -> str:
    payload = f"{CORPUS_VERSION}:calibration-mask-v1:{event_id}:{text}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


async def amain() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()
    db = Database(str(args.db))
    await db.init()
    inputs: list[dict] = []
    labels: list[dict] = []
    try:
        async with db.get_session() as session:
            result = await session.execute(
                select(Event)
                .where(Event.age_restriction.in_(sorted(CLASSES)))
                .order_by(Event.id.asc())
            )
            events = list(result.scalars().all())
            if args.limit > 0:
                events = events[: args.limit]
            for event in events:
                event_id = int(event.id or 0)
                sources = list(
                    (
                        await session.execute(
                            select(EventSource).where(EventSource.event_id == event_id)
                        )
                    ).scalars().all()
                )
                posters = list(
                    (
                        await session.execute(
                            select(EventPoster).where(EventPoster.event_id == event_id)
                        )
                    ).scalars().all()
                )
                corpus = build_event_age_bge_input(event, sources=sources, posters=posters)
                masked = mask_declared_age(corpus.text)
                row_hash = input_hash(event_id, masked)
                host = ""
                for source in sources:
                    host = urlparse(str(source.source_url or "")).hostname or ""
                    if host:
                        break
                title_group = " ".join(str(event.title or "").casefold().split())[:100]
                group_id = f"{host}:{title_group}" if host else f"event:{event_id}"
                inputs.append(
                    {
                        "event_id": event_id,
                        "input_hash": row_hash,
                        "text": masked,
                        "declared_age": str(event.age_restriction),
                        "ocr_coverage": corpus.ocr_coverage,
                        "poster_ocr_count": corpus.poster_ocr_count,
                        "corpus_version": f"{CORPUS_VERSION}:calibration-mask-v1",
                    }
                )
                labels.append(
                    {
                        "event_id": event_id,
                        "input_hash": row_hash,
                        "label": str(event.age_restriction),
                        "label_origin": "official_source_declared",
                        "group_id": group_id,
                    }
                )
    finally:
        await db.close()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "event_age_bge_input.jsonl").write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in inputs), encoding="utf-8"
    )
    (args.output_dir / "event_age_bge_labels.jsonl").write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in labels), encoding="utf-8"
    )
    summary = {
        "schema_version": "event-age-bge-calibration-corpus-v1",
        "rows": len(inputs),
        "labels": {label: sum(row["label"] == label for row in labels) for label in sorted(CLASSES)},
        "age_tokens_masked": True,
        "human_approval_required": False,
    }
    (args.output_dir / "event_age_bge_calibration_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(amain()))
