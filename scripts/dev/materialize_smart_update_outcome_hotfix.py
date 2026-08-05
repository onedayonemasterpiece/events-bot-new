#!/usr/bin/env python3
"""Materialize the focused Smart Update caller-boundary hotfix.

This one-shot script exists only because the GitHub connector cannot apply a
unified patch to very large files. It performs exact, assertion-guarded source
edits, then deletes itself and its workflow before the resulting commit.
"""

from __future__ import annotations

from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[2]


def replace_once(text: str, old: str, new: str, *, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected one anchor, found {count}")
    return text.replace(old, new, 1)


def patch_smart_event_update() -> None:
    path = ROOT / "smart_event_update.py"
    text = path.read_text(encoding="utf-8")

    text = replace_once(
        text,
        "from dataclasses import dataclass, field\n",
        "from dataclasses import dataclass, field\nfrom enum import Enum\n",
        label="enum import",
    )

    result_block = '''@dataclass(slots=True)
class SmartUpdateResult:
    status: str
    event_id: int | None = None
    created: bool = False
    merged: bool = False
    added_posters: int = 0
    added_sources: bool = False
    added_facts: list[str] = field(default_factory=list)
    skipped_conflicts: list[str] = field(default_factory=list)
    reason: str | None = None
    queue_notes: list[str] = field(default_factory=list)
'''
    outcome_contract = result_block + '''    # Evidence-only identity selected before a fail-closed result. Callers must
    # never use this field to authorize domain or publication side effects.
    matched_event_id: int | None = None


class SmartUpdateOutcomeKind(str, Enum):
    """Caller-facing outcome class; every unknown status fails closed."""

    ACCEPTED_CHANGED = "accepted_changed"
    ACCEPTED_NO_CHANGE = "accepted_no_change"
    NOT_ACCEPTED = "not_accepted"


_SMART_UPDATE_ACCEPTED_CHANGED = frozenset({"created", "merged"})
_SMART_UPDATE_ACCEPTED_NO_CHANGE = frozenset(
    {
        "skipped_nochange",
        "skipped_same_source_url",
        "noop_exact_source_replay",
    }
)


def classify_smart_update_status(status: str | None) -> SmartUpdateOutcomeKind:
    value = str(status or "").strip().lower()
    if value in _SMART_UPDATE_ACCEPTED_CHANGED:
        return SmartUpdateOutcomeKind.ACCEPTED_CHANGED
    if value in _SMART_UPDATE_ACCEPTED_NO_CHANGE:
        return SmartUpdateOutcomeKind.ACCEPTED_NO_CHANGE
    return SmartUpdateOutcomeKind.NOT_ACCEPTED


def smart_update_result_allows_caller_side_effects(result: Any) -> bool:
    return (
        classify_smart_update_status(getattr(result, "status", None))
        is not SmartUpdateOutcomeKind.NOT_ACCEPTED
    )


def _caller_safe_smart_update_result(result: SmartUpdateResult) -> SmartUpdateResult:
    """Remove the capability legacy callers interpreted as accepted identity."""

    if smart_update_result_allows_caller_side_effects(result):
        return result
    if result.event_id is not None:
        if result.matched_event_id is None:
            result.matched_event_id = int(result.event_id)
        logger.warning(
            "smart_update.outcome_boundary status=%s reason=%s matched_event_id=%s caller_event_id=masked",
            result.status,
            result.reason,
            result.matched_event_id,
        )
    result.event_id = None
    result.created = False
    result.merged = False
    return result
'''
    text = replace_once(
        text,
        result_block,
        outcome_contract,
        label="SmartUpdateResult outcome contract",
    )

    public_pattern = re.compile(
        r"async def smart_event_update\(\n.*?\n\n\ndef _candidate_source_role\(",
        re.DOTALL,
    )
    public_replacement = '''async def smart_event_update(
    db: Database,
    candidate: EventCandidate,
    *,
    check_source_url: bool = True,
    schedule_tasks: bool = True,
    schedule_kwargs: dict[str, Any] | None = None,
) -> SmartUpdateResult:
    async with _SMART_UPDATE_LOCK:
        try:
            result = await _smart_event_update_impl(
                db,
                candidate,
                check_source_url=check_source_url,
                schedule_tasks=schedule_tasks,
                schedule_kwargs=schedule_kwargs,
            )
        except SourceBindingConflict as exc:
            result = SmartUpdateResult(
                status="review_required",
                event_id=exc.existing_event_id,
                reason="source_binding_conflict",
            )
        except IntegrityError as exc:
            # The partial unique indexes are the authoritative cross-process
            # source ownership guard. Translate only their race failure; every
            # unrelated integrity error remains visible to the caller.
            message = str(getattr(exc, "orig", exc) or "").casefold()
            if "event_source.canonical_source_url" not in message:
                raise
            canonical = canonicalize_identity_url(candidate.source_url)
            owner_id: int | None = None
            if canonical:
                async with db.raw_conn() as conn:
                    cursor = await conn.execute(
                        "SELECT event_id FROM event_source WHERE canonical_source_url=? "
                        "AND source_role='identity_bearing' ORDER BY id LIMIT 1",
                        (canonical,),
                    )
                    row = await cursor.fetchone()
                    await cursor.close()
                    owner_id = int(row[0]) if row and row[0] is not None else None
            result = SmartUpdateResult(
                status="review_required",
                event_id=owner_id,
                reason="source_binding_conflict",
            )
    return _caller_safe_smart_update_result(result)


def _candidate_source_role('''
    text, count = public_pattern.subn(public_replacement, text, count=1)
    if count != 1:
        raise RuntimeError(f"public smart_event_update: expected one function, found {count}")

    legacy_pattern = re.compile(
        r"async def _source_identity_binding_conflict\(\n.*?\n\n\nasync def _attached_collection_source\(",
        re.DOTALL,
    )
    legacy_replacement = '''async def _source_identity_binding_conflict(
    session: Any,
    *,
    event_id: int,
    canonical_source_url: str,
    source_role: str,
) -> int | None:
    if source_role != "identity_bearing":
        return None

    row = (
        await session.execute(
            select(EventSource.event_id)
            .where(
                EventSource.canonical_source_url == canonical_source_url,
                EventSource.source_role == "identity_bearing",
                EventSource.event_id != int(event_id),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    if row is not None:
        return int(row)

    # Legacy rows are deliberately not mass-classified. An unknown owner on a
    # different Event is evidence of ambiguity, so fail closed instead of
    # silently taking the canonical source for the new candidate.
    canonical = canonicalize_identity_url(
        canonical_source_url,
        preserve_ticket_fragment=True,
    )
    if not canonical:
        return None
    try:
        parts = urlsplit(canonical)
    except (TypeError, ValueError):
        parts = None
    token: str | None = None
    if parts is not None:
        fragment = str(parts.fragment or "").strip().lstrip("/")
        path_bits = [item for item in str(parts.path or "").split("/") if item]
        if fragment and len(fragment) >= 8:
            token = fragment
        elif len(path_bits) >= 2:
            token = "/".join(path_bits[-2:])
        elif path_bits:
            token = path_bits[-1]

    predicates = [
        EventSource.canonical_source_url == canonical,
        EventSource.source_url == canonical,
    ]
    if token:
        escaped = (
            token.replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
        )
        predicates.append(EventSource.source_url.like(f"%{escaped}%", escape="\\"))

    legacy_rows = (
        await session.execute(
            select(
                EventSource.event_id,
                EventSource.source_url,
                EventSource.canonical_source_url,
            )
            .where(
                EventSource.event_id != int(event_id),
                or_(EventSource.source_role.is_(None), EventSource.source_role == ""),
                or_(*predicates),
            )
            .limit(100)
        )
    ).all()
    for owner_id, raw_url, stored_canonical in legacy_rows:
        candidate_canonical = canonicalize_identity_url(
            stored_canonical or raw_url,
            preserve_ticket_fragment=True,
        )
        if candidate_canonical == canonical:
            logger.warning(
                "smart_update.legacy_source_owner_review owner_event_id=%s requested_event_id=%s",
                owner_id,
                event_id,
            )
            return int(owner_id)
    return None


async def _attached_collection_source('''
    text, count = legacy_pattern.subn(legacy_replacement, text, count=1)
    if count != 1:
        raise RuntimeError(f"legacy source conflict: expected one function, found {count}")

    path.write_text(text, encoding="utf-8")


def patch_smart_update_identity() -> None:
    path = ROOT / "smart_update_identity.py"
    text = path.read_text(encoding="utf-8")

    helper_marker = "def _coerce_merge_action(value: Any) -> MergeIdentityAction:\n"
    helper = '''_TICKET_OCCURRENCE_ROUTE_RE = re.compile(
    r"(?i)(?:^|/)buy/event/(?P<event_id>[^/]+)/"
    r"(?P<date>\\d{4}-\\d{2}-\\d{2})/"
    r"(?P<time>\\d{1,2}[:.]\\d{2}(?::\\d{2})?)(?:/|$)"
)


def specific_ticket_occurrence_identity(
    value: str | None,
) -> tuple[str, str, str, str] | None:
    """Extract only an explicit vendor occurrence identity.

    Generic ticket landing pages intentionally remain an LLM-first semantic
    decision and return ``None`` here.
    """

    canonical = canonicalize_identity_url(value, preserve_ticket_fragment=True)
    if not canonical:
        return None
    try:
        parts = urlsplit(canonical)
    except (TypeError, ValueError):
        return None
    routes = [
        str(parts.fragment or "").strip().lstrip("/"),
        str(parts.path or "").strip().lstrip("/"),
    ]
    match = next(
        (
            found
            for route in routes
            if (found := _TICKET_OCCURRENCE_ROUTE_RE.search(route))
        ),
        None,
    )
    if match is None:
        return None
    raw_time = match.group("time").replace(".", ":")
    normalized_time = ":".join(raw_time.split(":")[:2])
    return (
        str(parts.hostname or "").casefold(),
        match.group("event_id").casefold(),
        match.group("date"),
        normalized_time,
    )


'''
    text = replace_once(
        text,
        helper_marker,
        helper + helper_marker,
        label="specific ticket helper",
    )

    start = text.index("def build_merge_identity_gate_verdict(")
    end = text.index("\ndef merge_identity_gate_fail_safe_verdict", start)
    block = text[start:end]
    return_marker = "\n    return MergeIdentityGateVerdict(\n"
    position = block.rfind(return_marker)
    if position < 0:
        raise RuntimeError("merge verdict final return not found")
    hard_rail = '''
    # Semantic identity remains LLM-first. This rail handles only an explicit
    # impossibility: two different vendor occurrence identities cannot be one
    # public Event even if a model incorrectly says SAME_EVENT.
    candidate_ticket_identity = specific_ticket_occurrence_identity(
        candidate_subject.ticket_link
    )
    existing_ticket_identity = specific_ticket_occurrence_identity(
        existing_subject.ticket_link
    )
    if (
        candidate_ticket_identity is not None
        and existing_ticket_identity is not None
        and candidate_ticket_identity != existing_ticket_identity
    ):
        conflict = (
            "specific_ticket_occurrence_conflict:"
            f"{candidate_ticket_identity[0]}:{candidate_ticket_identity[1]}:"
            f"{candidate_ticket_identity[2]}:{candidate_ticket_identity[3]}"
            "!="
            f"{existing_ticket_identity[0]}:{existing_ticket_identity[1]}:"
            f"{existing_ticket_identity[2]}:{existing_ticket_identity[3]}"
        )
        action = MergeIdentityAction.REVIEW_REQUIRED
        relation = MergeIdentityRelation.UNSAFE_TO_MERGE
        reason_code = "specific_ticket_occurrence_conflict"
        deterministic = True
        conflicts = tuple(dict.fromkeys((*conflicts, conflict)))
        reasons = tuple(
            dict.fromkeys(
                (
                    *reasons,
                    "different explicit ticket occurrence identities require review",
                )
            )
        )
'''
    block = block[:position] + hard_rail + block[position:]
    text = text[:start] + block + text[end:]
    path.write_text(text, encoding="utf-8")


def main() -> None:
    patch_smart_event_update()
    patch_smart_update_identity()

    # The final commit must contain only product code, tests and incident docs.
    for relative in (
        "scripts/dev/materialize_smart_update_outcome_hotfix.py",
        ".github/workflows/materialize-smart-update-outcome-hotfix.yml",
    ):
        target = ROOT / relative
        if target.exists():
            target.unlink()


if __name__ == "__main__":
    main()
