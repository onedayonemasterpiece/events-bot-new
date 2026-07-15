"""Build and import bounded CPU-BGE event age assessment batches.

The service never changes a declared restriction.  It treats cached, approved
poster OCR as first-class input and records an explicit OCR coverage state so a
missing OCR result cannot be silently confused with an image-free event.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from pathlib import Path

from sqlalchemy import or_, select

from db import Database
from event_age_rating import AgeRatingDecision, apply_age_decision
from models import Event, EventPoster, EventSource


CORPUS_VERSION = "event-age-bge-corpus-v2"
PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_PROTOTYPE_BANK = PROJECT_ROOT / "kaggle" / "EventAgeBgeAssessment" / "event_age_bge_prototypes.json"
ALLOWED_OCR_ROLES = {
    None,
    "event_identity_poster",
    "program_or_schedule",
    "attendee_information",
}


@dataclass(frozen=True, slots=True)
class EventAgeBgeInput:
    event_id: int
    input_hash: str
    text: str
    ocr_coverage: str
    poster_ocr_count: int

    def as_json(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "input_hash": self.input_hash,
            "text": self.text,
            "declared_age": None,
            "ocr_coverage": self.ocr_coverage,
            "poster_ocr_count": self.poster_ocr_count,
            "corpus_version": CORPUS_VERSION,
        }


def _clean(value: Any, *, limit: int) -> str:
    return " ".join(str(value or "").split()).strip()[:limit]


def _stable_hash(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def current_assessment_policy_version() -> str:
    """Hash every locally knowable artifact that can change an assessment."""

    prototype_path = Path(
        os.getenv("EVENT_AGE_BGE_PROTOTYPE_BANK_PATH") or DEFAULT_PROTOTYPE_BANK
    )
    prototype_sha = (
        hashlib.sha256(prototype_path.read_bytes()).hexdigest()
        if prototype_path.exists()
        else "missing"
    )
    payload = {
        "contract": os.getenv("EVENT_AGE_BGE_POLICY_VERSION") or "event-age-bge-policy-v2",
        "model_revision": (os.getenv("EVENT_AGE_BGE_MODEL_REVISION") or "").strip(),
        "prototype_sha256": prototype_sha,
        "classifier_sha256": (
            os.getenv("EVENT_AGE_BGE_CLASSIFIER_SHA256") or "retrieval-only"
        ).strip(),
        "corpus_version": CORPUS_VERSION,
    }
    return "event-age-bge-policy-v2:" + _stable_hash(payload)[:20]


def build_event_age_bge_input(
    event: Event,
    *,
    sources: list[EventSource],
    posters: list[EventPoster],
    now: datetime | None = None,
    ocr_wait: timedelta = timedelta(hours=2),
) -> EventAgeBgeInput:
    """Build a section-aware corpus with event-scoped OCR ahead of long prose."""

    now = now or datetime.now(timezone.utc)
    approved = [
        poster
        for poster in posters
        if str(poster.review_status or "") == "approved"
        and getattr(poster, "media_role", None) in ALLOWED_OCR_ROLES
    ]
    poster_blocks: list[dict[str, Any]] = []
    missing_ocr = 0
    for poster in sorted(approved, key=lambda item: (item.display_order, item.id or 0)):
        title = _clean(poster.ocr_title, limit=800)
        text = _clean(poster.ocr_text, limit=3500)
        if not title and not text:
            missing_ocr += 1
            continue
        poster_blocks.append(
            {
                "poster_hash": str(poster.poster_hash or "")[:96],
                "role": getattr(poster, "media_role", None),
                "ocr_title": title,
                "ocr_text": text,
            }
        )
    if not approved:
        ocr_coverage = "not_applicable"
    elif missing_ocr == 0:
        ocr_coverage = "complete"
    else:
        added_at = getattr(event, "added_at", None)
        if added_at is not None and added_at.tzinfo is None:
            added_at = added_at.replace(tzinfo=timezone.utc)
        within_wait = bool(added_at and now < added_at + ocr_wait)
        ocr_coverage = "pending" if within_wait else "terminal_unavailable"

    source_blocks = [
        {
            "source_type": _clean(source.source_type, limit=80),
            "source_url": _clean(source.source_url, limit=400),
            "text": _clean(source.source_text, limit=7000),
        }
        for source in sorted(sources, key=lambda item: item.id or 0)
        if _clean(source.source_text, limit=7000)
    ]
    # OCR is deliberately before descriptions/sources so max-length truncation
    # cannot systematically discard a rating printed on the poster.
    sections = [
        f"[EVENT]\nTITLE: {_clean(event.title, limit=700)}\nTYPE: {_clean(event.event_type, limit=120)}",
        *[
            "[EVENT_POSTER_OCR]\n"
            f"TITLE: {block['ocr_title']}\nTEXT: {block['ocr_text']}"
            for block in poster_blocks
        ],
        f"[DESCRIPTION]\n{_clean(event.description, limit=9000)}",
        f"[SEARCH_DIGEST]\n{_clean(event.search_digest, limit=2500)}",
        *[
            "[EVENT_SOURCE]\n"
            f"TYPE: {block['source_type']}\nURL: {block['source_url']}\nTEXT: {block['text']}"
            for block in source_blocks
        ],
    ]
    text = "\n\n".join(section for section in sections if section.rstrip("\n ").split("\n")[-1])
    hash_payload = {
        "corpus_version": CORPUS_VERSION,
        "event_id": int(event.id or 0),
        "title": _clean(event.title, limit=700),
        "event_type": _clean(event.event_type, limit=120),
        "description": _clean(event.description, limit=9000),
        "search_digest": _clean(event.search_digest, limit=2500),
        "sources": source_blocks,
        "posters": poster_blocks,
        "ocr_coverage": ocr_coverage,
    }
    return EventAgeBgeInput(
        event_id=int(event.id or 0),
        input_hash=_stable_hash(hash_payload),
        text=text,
        ocr_coverage=ocr_coverage,
        poster_ocr_count=len(poster_blocks),
    )


async def collect_event_age_bge_inputs(
    db: Database,
    *,
    limit: int = 64,
    now: datetime | None = None,
) -> tuple[list[EventAgeBgeInput], dict[str, int]]:
    """Select only active, non-declared, missing/stale assessment rows."""

    now = now or datetime.now(timezone.utc)
    today = now.date().isoformat()
    policy_version = current_assessment_policy_version()
    stats = {"selected": 0, "current": 0, "ocr_pending": 0, "conflict": 0}
    async with db.get_session() as session:
        result = await session.execute(
            select(Event)
            .where(
                Event.lifecycle_status == "active",
                Event.silent.is_(False),
                Event.merged_into_event_id.is_(None),
                or_(Event.date >= today, Event.end_date >= today),
                Event.age_restriction.is_(None),
                or_(Event.age_restriction_status.is_(None), Event.age_restriction_status != "conflict"),
            )
            .order_by(Event.age_assessment_updated_at.asc(), Event.id.asc())
            .limit(max(limit * 4, limit))
        )
        events = list(result.scalars().all())
        selected: list[EventAgeBgeInput] = []
        for event in events:
            sources = list(
                (
                    await session.execute(
                        select(EventSource).where(EventSource.event_id == int(event.id or 0))
                    )
                ).scalars().all()
            )
            posters = list(
                (
                    await session.execute(
                        select(EventPoster).where(EventPoster.event_id == int(event.id or 0))
                    )
                ).scalars().all()
            )
            row = build_event_age_bge_input(event, sources=sources, posters=posters, now=now)
            if row.ocr_coverage == "pending":
                stats["ocr_pending"] += 1
                if event.age_assessment_status != "ocr_pending":
                    event.age_assessment_status = "ocr_pending"
                    event.age_assessment_input_hash = row.input_hash
                    event.age_assessment_updated_at = now
                    session.add(event)
                continue
            if (
                event.age_assessment_input_hash == row.input_hash
                and event.age_assessment_decision_version == policy_version
                and event.age_assessment_status
                in {"assessed", "insufficient_evidence", "ocr_unavailable"}
            ):
                stats["current"] += 1
                continue
            event.age_assessment_status = "pending"
            event.age_assessment_input_hash = row.input_hash
            event.age_assessment_updated_at = now
            session.add(event)
            selected.append(row)
            if len(selected) >= limit:
                break
        await session.commit()
    stats["selected"] = len(selected)
    return selected, stats


async def apply_event_age_bge_report(
    db: Database,
    report: dict[str, Any],
    *,
    now: datetime | None = None,
) -> dict[str, int]:
    """Import a hash-bound report idempotently and never overwrite declared."""

    now = now or datetime.now(timezone.utc)
    counts = {"applied": 0, "terminal_unrateable": 0, "stale": 0, "declared": 0, "invalid": 0}
    if report.get("schema_version") != "event-age-bge-shadow-v1":
        raise ValueError("unsupported event age BGE report")
    configured_revision = (os.getenv("EVENT_AGE_BGE_MODEL_REVISION") or "").strip()
    if configured_revision and report.get("model_revision") != configured_revision:
        raise ValueError("event age BGE report uses another model revision")
    if report.get("encoder_contract") != "bge_m3_cpu_dense_retrieval_v1":
        raise ValueError("event age BGE report uses another encoder contract")
    policy_version = current_assessment_policy_version()
    if report.get("assessment_policy_version") != policy_version:
        raise ValueError("event age BGE report uses another assessment policy")
    expected_classifier = (os.getenv("EVENT_AGE_BGE_CLASSIFIER_SHA256") or "").strip()
    if expected_classifier and report.get("classifier_sha256") != expected_classifier:
        raise ValueError("event age BGE report uses another classifier")
    run_id = str(report.get("run_id") or "") or None
    engine = ":".join(
        str(value or "")
        for value in (
            "bge-m3",
            report.get("model_revision"),
            report.get("classifier_sha256") or "retrieval-only",
        )
    )
    for item in report.get("results") or []:
        if not isinstance(item, dict) or not isinstance(item.get("event_id"), int):
            counts["invalid"] += 1
            continue
        event_id = int(item["event_id"])
        async with db.get_session() as session:
            event = await session.get(Event, event_id)
            if event is None:
                counts["invalid"] += 1
                continue
            if event.age_restriction:
                counts["declared"] += 1
                continue
            sources = list(
                (await session.execute(select(EventSource).where(EventSource.event_id == event_id))).scalars().all()
            )
            posters = list(
                (await session.execute(select(EventPoster).where(EventPoster.event_id == event_id))).scalars().all()
            )
            current = build_event_age_bge_input(event, sources=sources, posters=posters, now=now)
            if item.get("input_hash") != current.input_hash:
                counts["stale"] += 1
                continue
            status = str(item.get("status") or "")
            if status == "assessed" and item.get("age_assessment") in {"0+", "6+", "12+", "16+", "18+"}:
                if (
                    report.get("evaluation_approval_status") != "approved"
                    or not report.get("classifier_sha256")
                    or item.get("model_revision") != report.get("model_revision")
                    or item.get("encoder_contract") != report.get("encoder_contract")
                    or item.get("classifier_sha256") != report.get("classifier_sha256")
                ):
                    counts["invalid"] += 1
                    continue
                decision = AgeRatingDecision(
                    status="assessed",
                    value=str(item["age_assessment"]),
                    provenance="bge_assessed",
                    confidence=float(item.get("confidence") or 0.0),
                    evidence={
                        "kind": "bge_calibrated_assessment",
                        "retrieval": item.get("retrieval") or {},
                        "ocr_coverage": current.ocr_coverage,
                    },
                    decision_version=policy_version,
                    input_hash=current.input_hash,
                    assessment_engine=engine,
                    run_id=run_id,
                )
                if apply_age_decision(event, decision, now=now):
                    session.add(event)
                    await session.commit()
                    counts["applied"] += 1
            elif status == "insufficient_evidence":
                decision = AgeRatingDecision(
                    status="insufficient_evidence",
                    input_hash=current.input_hash,
                    decision_version=policy_version,
                    assessment_engine=engine,
                    run_id=run_id,
                )
                apply_age_decision(event, decision, now=now)
                # The report is bound to the *new* corpus.  A prior numeric
                # assessment for an older hash must not survive an abstention
                # and masquerade as current data.
                event.age_assessment = None
                event.age_assessment_provenance = None
                event.age_assessment_confidence = None
                event.age_assessment_evidence = {}
                event.age_assessment_decision_version = decision.decision_version
                event.age_assessment_input_hash = current.input_hash
                event.age_assessment_status = (
                    "ocr_unavailable"
                    if current.ocr_coverage == "terminal_unavailable"
                    else "insufficient_evidence"
                )
                event.age_assessment_engine = engine
                event.age_assessment_run_id = run_id
                event.age_assessment_updated_at = now
                event.age_restriction_status = "insufficient_evidence"
                session.add(event)
                await session.commit()
                counts["terminal_unrateable"] += 1
            else:
                counts["invalid"] += 1
    return counts


def _dataset_slug(value: str, *, limit: int = 60) -> str:
    safe = "".join(ch if ch.isalnum() else "-" for ch in value.casefold())
    safe = "-".join(part for part in safe.split("-") if part)
    return safe[:limit].strip("-")


async def run_event_age_bge_batch(db: Database) -> dict[str, Any]:
    """Create one private missing-only dataset, run Kaggle, and import it."""

    limit = max(1, int((os.getenv("EVENT_AGE_BGE_BATCH_LIMIT") or "64").strip() or "64"))
    rows, selection = await collect_event_age_bge_inputs(db, limit=limit)
    if not rows:
        return {"status": "empty", "selection": selection, "import": {}}
    revision = (os.getenv("EVENT_AGE_BGE_MODEL_REVISION") or "").strip()
    if not revision:
        raise RuntimeError("EVENT_AGE_BGE_MODEL_REVISION is required")
    username = (os.getenv("KAGGLE_USERNAME") or "").strip()
    if not username:
        raise RuntimeError("KAGGLE_USERNAME is required for the private input dataset")
    prototype_path = Path(
        os.getenv("EVENT_AGE_BGE_PROTOTYPE_BANK_PATH") or DEFAULT_PROTOTYPE_BANK
    )
    if not prototype_path.exists():
        raise FileNotFoundError(prototype_path)
    run_id = "event-age-bge-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    slug_name = _dataset_slug(run_id)
    dataset_ref = f"{username}/{slug_name}"
    from video_announce.kaggle_client import KaggleClient
    from source_parsing.kaggle_runner import run_kaggle_kernel

    client = KaggleClient()
    with tempfile.TemporaryDirectory(prefix="event-age-bge-input-") as tmp:
        root = Path(tmp)
        (root / "event_age_bge_input.jsonl").write_text(
            "".join(json.dumps(row.as_json(), ensure_ascii=False) + "\n" for row in rows),
            encoding="utf-8",
        )
        (root / "event_age_bge_run.json").write_text(
            json.dumps(
                {
                    "run_id": run_id,
                    "model_revision": revision,
                    "batch_limit": limit,
                    "batch_size": max(1, int(os.getenv("EVENT_AGE_BGE_BATCH_SIZE") or "4")),
                    "max_length": max(512, int(os.getenv("EVENT_AGE_BGE_MAX_LENGTH") or "768")),
                    "top_k": max(1, int(os.getenv("EVENT_AGE_BGE_TOP_K") or "8")),
                    "max_runtime_seconds": max(
                        300, int(os.getenv("EVENT_AGE_BGE_MAX_RUNTIME_SECONDS") or "2400")
                    ),
                    "runtime_guard_seconds": max(
                        30, int(os.getenv("EVENT_AGE_BGE_RUNTIME_GUARD_SECONDS") or "120")
                    ),
                    "catalog_hash": _stable_hash(
                        [(row.event_id, row.input_hash) for row in rows]
                    ),
                    "assessment_policy_version": current_assessment_policy_version(),
                    "expected_classifier_sha256": (
                        os.getenv("EVENT_AGE_BGE_CLASSIFIER_SHA256") or ""
                    ).strip(),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (root / "event_age_bge_prototypes.json").write_bytes(prototype_path.read_bytes())
        (root / "dataset-metadata.json").write_text(
            json.dumps(
                {
                    "title": f"Event age BGE input {run_id}",
                    "id": dataset_ref,
                    "licenses": [{"name": "CC0-1.0"}],
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        await asyncio.to_thread(client.create_dataset, root, public=False)
    artifact_ref = (os.getenv("EVENT_AGE_BGE_ARTIFACT_DATASET") or "").strip()
    dataset_sources = [dataset_ref, *([artifact_ref] if artifact_ref else [])]
    status, files, duration = await run_kaggle_kernel(
        "EventAgeBgeAssessment",
        timeout_minutes=max(10, int(os.getenv("EVENT_AGE_BGE_KAGGLE_TIMEOUT_MINUTES") or "55")),
        poll_interval=max(10, int(os.getenv("EVENT_AGE_BGE_KAGGLE_POLL_SECONDS") or "30")),
        run_config={"run_id": run_id, "cpu_only": True},
        dataset_sources=dataset_sources,
        db=db,
        registry_job_type="event_age_bge",
        ledger_kind="event_age_bge_assessment",
        resource_leases=["kaggle_kernel:event_age_bge"],
        output_namespace=run_id,
        registry_meta={
            "run_id": run_id,
            "input_dataset_ref": dataset_ref,
            "event_count": len(rows),
        },
    )
    if status != "complete":
        raise RuntimeError(f"event age BGE Kaggle status={status} duration={duration:.1f}s")
    result_paths = [Path(path) for path in files if Path(path).name == "event_age_bge_result.json"]
    if len(result_paths) != 1:
        raise RuntimeError("event_age_bge_result.json missing or ambiguous")
    report = json.loads(result_paths[0].read_text(encoding="utf-8"))
    imported = await apply_event_age_bge_report(db, report)
    try:
        await asyncio.to_thread(client.delete_dataset, dataset_ref, no_confirm=True)
    except Exception:
        # Dataset cleanup is best effort; it must not turn an imported batch
        # into a retry that could launch another kernel.
        pass
    return {
        "status": str(report.get("status") or "complete"),
        "run_id": run_id,
        "duration_seconds": duration,
        "selection": selection,
        "import": imported,
        "event_count": len(rows),
    }
