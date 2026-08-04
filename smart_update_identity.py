"""Identity gate helpers for Smart Update create/merge-path safety.

The helpers in this module are intentionally small and dependency-light so they can
be unit-tested without the Smart Update DB/LLM stack.  The create-path gate never
decides to merge; it can only allow create or veto create when another identity
signal is strong enough that creating a new row would be unsafe.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields, is_dataclass
from enum import Enum
import hashlib
import json
import re
import unicodedata
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from telegram_sources import canonicalize_tg_url


class IdentityGateMode(str, Enum):
    OFF = "off"
    SHADOW = "shadow"
    ENFORCE = "enforce"


class IdentityGateAction(str, Enum):
    ALLOW_CREATE = "allow_create"
    VETO_CREATE = "veto_create"


class MergeIdentityAction(str, Enum):
    """Decision shape for the merge-path identity gate.

    ``SKIP_MERGE_SIDE_EFFECTS`` is the safe action for suspected identity glue:
    Smart Update must not mutate the matched row, sources, posters, or scheduled
    jobs.  ``ALLOW_SAFE_METADATA_ONLY`` is reserved for future narrow cases where
    telemetry could be persisted without semantic row changes; current
    orchestration treats it as an allow.
    """

    ALLOW_MERGE = "allow_merge"
    ALLOW_SAFE_METADATA_ONLY = "allow_safe_metadata_only"
    SKIP_MERGE_SIDE_EFFECTS = "skip_merge_side_effects"
    REVIEW_REQUIRED = "review_required"


class MergeIdentityRelation(str, Enum):
    SAME_EVENT = "same_event"
    SOURCE_UPDATE = "source_update"
    RELATED_BUT_DISTINCT = "related_but_distinct"
    FESTIVAL_CONTEXT_SIBLING = "festival_context_sibling"
    UNSAFE_TO_MERGE = "unsafe_to_merge"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class IdentitySourceFlags:
    """Normalized source/type flags carried into the identity policy."""

    source_type: str = ""
    source_kind: str = "unknown"
    is_parser: bool = False
    is_telegram: bool = False
    is_vk: bool = False
    is_bot: bool = False


@dataclass(frozen=True, slots=True)
class IdentitySubject:
    """Minimal, serializable view of a candidate/existing event for identity checks."""

    role: str
    event_id: int | None = None
    title: str | None = None
    date: str | None = None
    end_date: str | None = None
    time: str | None = None
    time_is_default: bool = False
    location_name: str | None = None
    location_address: str | None = None
    city: str | None = None
    event_type: str | None = None
    ticket_link: str | None = None
    source_url: str | None = None
    source_role: str = "identity_bearing"
    source_flags: IdentitySourceFlags = field(default_factory=IdentitySourceFlags)
    poster_hashes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class IdentityVectorEvidence:
    """Optional vector-lane evidence.  Safe to omit until vector RPC/schema lands."""

    available: bool = False
    nearest_event_id: int | None = None
    score: float | None = None
    reason: str | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class IdentityGateVerdict:
    """Structured create-path identity verdict.

    ``action`` is intentionally create-oriented: this gate never instructs Smart
    Update to merge, because a false positive identity hit must fail safe by
    stopping automatic create rather than auto-merging unrelated rows.
    """

    mode: IdentityGateMode
    action: IdentityGateAction
    reason_code: str
    reasons: tuple[str, ...] = ()
    candidate: IdentitySubject | None = None
    matched_event_id: int | None = None
    confidence: float = 0.0
    deterministic: bool = False
    vector: IdentityVectorEvidence | None = None
    fail_safe: bool = False

    @property
    def should_veto_create(self) -> bool:
        return self.mode is IdentityGateMode.ENFORCE and self.action is IdentityGateAction.VETO_CREATE

    @property
    def would_veto_create(self) -> bool:
        return self.action is IdentityGateAction.VETO_CREATE


@dataclass(frozen=True, slots=True)
class MergeIdentityGateVerdict:
    """Structured merge-path identity verdict.

    The verdict does not try to repair matching itself.  It gates side effects
    after a candidate has been matched to an existing row and before that row is
    mutated.  In shadow mode it records what would have happened; only enforce
    mode blocks.
    """

    mode: IdentityGateMode
    action: MergeIdentityAction
    relation: MergeIdentityRelation
    reason_code: str
    reasons: tuple[str, ...] = ()
    candidate: IdentitySubject | None = None
    existing_event_id: int | None = None
    confidence: float = 0.0
    blocking_conflicts: tuple[str, ...] = ()
    allowed_fields: tuple[str, ...] = ()
    deterministic: bool = False
    llm: Mapping[str, Any] | None = None
    fail_safe: bool = False

    @property
    def should_skip_side_effects(self) -> bool:
        return self.mode is IdentityGateMode.ENFORCE and self.action in {
            MergeIdentityAction.SKIP_MERGE_SIDE_EFFECTS,
            MergeIdentityAction.REVIEW_REQUIRED,
        }

    @property
    def would_skip_side_effects(self) -> bool:
        return self.action in {
            MergeIdentityAction.SKIP_MERGE_SIDE_EFFECTS,
            MergeIdentityAction.REVIEW_REQUIRED,
        }


_TOKEN_RE = re.compile(r"[\wа-яё]+", re.IGNORECASE)
_EVENT_WORDS = {
    "афиша",
    "билет",
    "билеты",
    "встреча",
    "выставка",
    "концерт",
    "лекция",
    "мастер",
    "класс",
    "мероприятие",
    "музей",
    "показ",
    "программа",
    "событие",
    "спектакль",
    "фестиваль",
    "экскурсия",
}

_TRACKING_QUERY_KEYS = {
    "_openstat",
    "fbclid",
    "from",
    "gclid",
    "igshid",
    "mc_cid",
    "mc_eid",
    "ref",
    "source",
    "yclid",
}
_TG_NON_IDENTITY_QUERY_KEYS = {"single"}
_VK_PUBLIC_HOSTS = {"vk.com", "m.vk.com", "vk.ru", "m.vk.ru"}
_VK_WALL_TOKEN_RE = re.compile(r"^wall-?\d+_\d+$", re.IGNORECASE)


def parse_identity_gate_mode(raw: str | None, *, default: IdentityGateMode = IdentityGateMode.OFF) -> IdentityGateMode:
    value = (raw or "").strip().lower()
    if not value:
        return default
    aliases = {"0": "off", "false": "off", "no": "off", "1": "enforce", "true": "enforce", "yes": "enforce"}
    value = aliases.get(value, value)
    try:
        return IdentityGateMode(value)
    except ValueError:
        return default


def source_flags_for(source_type: str | None) -> IdentitySourceFlags:
    raw = str(source_type or "").strip()
    lower = raw.lower()
    if lower.startswith("parser:"):
        kind = "parser"
    elif lower in {"tg", "telegram"} or lower.startswith("telegram"):
        kind = "telegram"
    elif lower == "vk" or lower.startswith("vk"):
        kind = "vk"
    elif lower == "bot" or lower.startswith("bot"):
        kind = "bot"
    else:
        kind = lower or "unknown"
    return IdentitySourceFlags(
        source_type=raw,
        source_kind=kind,
        is_parser=kind == "parser",
        is_telegram=kind == "telegram",
        is_vk=kind == "vk",
        is_bot=kind == "bot",
    )


def _get(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _source_url_for(obj: Any, role: str) -> str | None:
    if _normalize_source_role(_get(obj, "source_role")) == "context_only":
        return None
    if role == "event":
        return (
            _get(obj, "source_url")
            or _get(obj, "source_post_url")
            or _get(obj, "source_vk_post_url")
        )
    return _get(obj, "source_url") or _get(obj, "source_post_url") or _get(obj, "source_vk_post_url")


def _normalize_source_role(value: Any) -> str:
    role = str(value or "identity_bearing").strip().lower()
    return "context_only" if role == "context_only" else "identity_bearing"


def _poster_hashes_for(obj: Any) -> tuple[str, ...]:
    posters = _get(obj, "posters") or []
    hashes: list[str] = []
    for poster in posters:
        for attr in ("sha256", "poster_hash", "phash", "dhash"):
            value = _get(poster, attr)
            if value:
                hashes.append(str(value))
                break
    direct = _get(obj, "poster_scope_hashes") or []
    for value in direct:
        if value:
            hashes.append(str(value))
    # Preserve deterministic ordering while removing duplicates.
    return tuple(dict.fromkeys(hashes))


def identity_subject_from(obj: Any, *, role: str) -> IdentitySubject:
    return IdentitySubject(
        role=role,
        event_id=_coerce_int(_get(obj, "id") or _get(obj, "event_id")),
        title=_clean_str(_get(obj, "title")),
        date=_clean_str(_get(obj, "date")),
        end_date=_clean_str(_get(obj, "end_date")),
        time=_clean_str(_get(obj, "time")),
        time_is_default=bool(_get(obj, "time_is_default", False)),
        location_name=_clean_str(_get(obj, "location_name")),
        location_address=_clean_str(_get(obj, "location_address")),
        city=_clean_str(_get(obj, "city")),
        event_type=_clean_str(_get(obj, "event_type")),
        ticket_link=_clean_str(_get(obj, "ticket_link")),
        source_url=_clean_str(_source_url_for(obj, role)),
        source_role=_normalize_source_role(_get(obj, "source_role")),
        source_flags=source_flags_for(_get(obj, "source_type")),
        poster_hashes=_poster_hashes_for(obj),
    )


def deterministic_identity_veto(
    candidate: IdentitySubject,
    existing: Sequence[IdentitySubject],
    *,
    vector_evidence: IdentityVectorEvidence | None = None,
) -> IdentityGateVerdict:
    """Return a structured allow/veto verdict using narrow deterministic evidence.

    The helper is deliberately conservative.  It vetoes create only when an
    existing event has a strong same-identity signal; it never recommends merge.
    """

    for ev in existing:
        if _single_occurrence_inside_recurring_series(candidate, ev):
            # Let the LLM/create path materialize the exact occurrence instead of
            # vetoing create against the broader recurring season row.
            continue

        same_day = _date_overlaps(candidate, ev)
        same_time = _same_known_time(candidate, ev)
        title_related = _titles_related(candidate.title, ev.title)
        location_related = _locations_related(candidate, ev)

        if (
            candidate.source_url
            and ev.source_url
            and candidate.source_url == ev.source_url
            and same_day
            and (same_time or candidate.time_is_default or ev.time_is_default or not candidate.time or not ev.time)
            and (title_related or location_related)
        ):
            return _veto("deterministic_same_source_identity", candidate, ev, 0.98, "same source URL and compatible anchors")

        if (
            candidate.ticket_link
            and ev.ticket_link
            and _normalize_url(candidate.ticket_link) == _normalize_url(ev.ticket_link)
            and same_day
            and same_time
            and (title_related or location_related)
        ):
            return _veto("deterministic_same_ticket_slot", candidate, ev, 0.92, "same ticket URL, date/time slot, and related title/location")

        if (
            candidate.poster_hashes
            and ev.poster_hashes
            and set(candidate.poster_hashes).intersection(ev.poster_hashes)
            and same_day
            and (title_related or location_related)
        ):
            return _veto("deterministic_same_poster_identity", candidate, ev, 0.9, "same poster hash and compatible date/title/location")

    if vector_evidence and vector_evidence.error:
        return IdentityGateVerdict(
            mode=IdentityGateMode.ENFORCE,
            action=IdentityGateAction.VETO_CREATE,
            reason_code="vector_identity_error",
            reasons=(str(vector_evidence.error),),
            candidate=candidate,
            matched_event_id=vector_evidence.nearest_event_id,
            confidence=0.0,
            deterministic=False,
            vector=vector_evidence,
            fail_safe=True,
        )
    vector_match = _vector_supported_identity_match(candidate, existing, vector_evidence)
    if vector_match is not None:
        ev, reason = vector_match
        return IdentityGateVerdict(
            mode=IdentityGateMode.ENFORCE,
            action=IdentityGateAction.VETO_CREATE,
            reason_code="vector_nearest_identity",
            reasons=(reason,),
            candidate=candidate,
            matched_event_id=ev.event_id,
            confidence=float(vector_evidence.score or 0.0),
            deterministic=False,
            vector=vector_evidence,
        )

    return IdentityGateVerdict(
        mode=IdentityGateMode.ENFORCE,
        action=IdentityGateAction.ALLOW_CREATE,
        reason_code="no_identity_veto",
        reasons=("no deterministic/vector identity veto",),
        candidate=candidate,
        vector=vector_evidence,
    )


def build_identity_gate_verdict(
    candidate: Any,
    existing_events: Iterable[Any],
    *,
    mode: IdentityGateMode | str = IdentityGateMode.OFF,
    vector_evidence: IdentityVectorEvidence | Mapping[str, Any] | None = None,
) -> IdentityGateVerdict:
    resolved_mode = mode if isinstance(mode, IdentityGateMode) else parse_identity_gate_mode(str(mode))
    subject = identity_subject_from(candidate, role="candidate")
    vector = _coerce_vector_evidence(vector_evidence)
    if resolved_mode is IdentityGateMode.OFF:
        return IdentityGateVerdict(
            mode=resolved_mode,
            action=IdentityGateAction.ALLOW_CREATE,
            reason_code="identity_gate_off",
            reasons=("SMART_UPDATE_IDENTITY_GATE=off",),
            candidate=subject,
            vector=vector,
        )

    existing = [identity_subject_from(ev, role="event") for ev in existing_events]
    verdict = deterministic_identity_veto(subject, existing, vector_evidence=vector)
    return IdentityGateVerdict(
        mode=resolved_mode,
        action=verdict.action,
        reason_code=verdict.reason_code,
        reasons=verdict.reasons,
        candidate=subject,
        matched_event_id=verdict.matched_event_id,
        confidence=verdict.confidence,
        deterministic=verdict.deterministic,
        vector=verdict.vector,
        fail_safe=verdict.fail_safe,
    )


def identity_gate_fail_safe_verdict(
    *,
    mode: IdentityGateMode | str,
    candidate: Any | None = None,
    reason: str = "identity gate error",
) -> IdentityGateVerdict:
    resolved_mode = mode if isinstance(mode, IdentityGateMode) else parse_identity_gate_mode(str(mode))
    subject = identity_subject_from(candidate, role="candidate") if candidate is not None else None
    return IdentityGateVerdict(
        mode=resolved_mode,
        action=IdentityGateAction.VETO_CREATE,
        reason_code="identity_gate_error",
        reasons=(reason,),
        candidate=subject,
        confidence=0.0,
        fail_safe=True,
    )


def build_merge_identity_gate_verdict(
    candidate: Any,
    existing_event: Any,
    *,
    mode: IdentityGateMode | str = IdentityGateMode.OFF,
    llm_data: Mapping[str, Any] | None = None,
    blocking_conflicts: Sequence[str] | None = None,
) -> MergeIdentityGateVerdict:
    """Build a merge-path identity verdict from LLM output plus narrow rails.

    This is intentionally LLM-first: ``llm_data`` carries the semantic identity
    classification.  Deterministic rails only fail-closed for structural
    contradictions that are strong enough to be unsafe regardless of wording
    (for example: a single-slot lecture row being about to absorb a long-running
    exhibition source).
    """

    resolved_mode = mode if isinstance(mode, IdentityGateMode) else parse_identity_gate_mode(str(mode))
    candidate_subject = identity_subject_from(candidate, role="candidate")
    existing_subject = identity_subject_from(existing_event, role="event")
    if resolved_mode is IdentityGateMode.OFF:
        return MergeIdentityGateVerdict(
            mode=resolved_mode,
            action=MergeIdentityAction.ALLOW_MERGE,
            relation=MergeIdentityRelation.UNKNOWN,
            reason_code="merge_identity_gate_off",
            reasons=("SMART_UPDATE_MERGE_IDENTITY_GATE=off",),
            candidate=candidate_subject,
            existing_event_id=existing_subject.event_id,
        )

    llm = dict(llm_data or {})
    llm_contract_valid = bool(
        llm
        and str(llm.get("action") or "").strip().lower()
        in {item.value for item in MergeIdentityAction}
        and str(llm.get("relation") or "").strip().lower()
        in {item.value for item in MergeIdentityRelation}
    )
    action = _coerce_merge_action(llm.get("action"))
    relation = _coerce_merge_relation(llm.get("relation"))
    confidence = _coerce_float(llm.get("confidence")) or 0.0
    reason_code = _clean_str(llm.get("reason_code")) or "merge_identity_llm_unavailable"
    raw_reasons = llm.get("reasons")
    reason_items = raw_reasons if isinstance(raw_reasons, Sequence) and not isinstance(raw_reasons, (str, bytes)) else []
    reasons = tuple(s for s in (_clean_str(llm.get("reason")), *[_clean_str(v) for v in reason_items]) if s)
    raw_llm_conflicts = llm.get("blocking_conflicts")
    llm_conflict_items = (
        raw_llm_conflicts
        if isinstance(raw_llm_conflicts, Sequence) and not isinstance(raw_llm_conflicts, (str, bytes))
        else []
    )
    conflicts = tuple(
        dict.fromkeys(
            [
                str(v).strip()
                for v in [
                    *(blocking_conflicts or []),
                    *llm_conflict_items,
                ]
                if str(v or "").strip()
            ]
        )
    )
    raw_allowed_fields = llm.get("allowed_fields")
    allowed_field_items = (
        raw_allowed_fields
        if isinstance(raw_allowed_fields, Sequence) and not isinstance(raw_allowed_fields, (str, bytes))
        else []
    )
    allowed_fields = tuple(
        dict.fromkeys(str(v).strip() for v in allowed_field_items if str(v or "").strip())
    )

    if not llm_contract_valid:
        action = MergeIdentityAction.REVIEW_REQUIRED
        relation = MergeIdentityRelation.UNKNOWN
        reason_code = "merge_identity_llm_unavailable"
        reasons = reasons or ("merge identity decision is unavailable or invalid",)
    elif candidate_subject.source_role == "context_only" and relation in {
        MergeIdentityRelation.SAME_EVENT,
        MergeIdentityRelation.SOURCE_UPDATE,
    }:
        # Context sources may be attached to a caller-selected event for provenance,
        # but their text/link must never assert event identity or authorize a merge.
        action = MergeIdentityAction.REVIEW_REQUIRED
        relation = MergeIdentityRelation.UNKNOWN
        reason_code = "context_only_cannot_assert_identity"
        reasons = tuple(dict.fromkeys((*reasons, "context-only source cannot assert SAME_EVENT")))
    elif relation in {
        MergeIdentityRelation.RELATED_BUT_DISTINCT,
        MergeIdentityRelation.FESTIVAL_CONTEXT_SIBLING,
        MergeIdentityRelation.UNSAFE_TO_MERGE,
    }:
        action = MergeIdentityAction.SKIP_MERGE_SIDE_EFFECTS
    elif action in {MergeIdentityAction.SKIP_MERGE_SIDE_EFFECTS, MergeIdentityAction.REVIEW_REQUIRED}:
        relation = relation if relation is not MergeIdentityRelation.UNKNOWN else MergeIdentityRelation.UNSAFE_TO_MERGE
    elif relation in {MergeIdentityRelation.SAME_EVENT, MergeIdentityRelation.SOURCE_UPDATE}:
        action = MergeIdentityAction.ALLOW_MERGE
    else:
        action = MergeIdentityAction.REVIEW_REQUIRED
        reason_code = "merge_identity_uncertain"
        reasons = tuple(dict.fromkeys((*reasons, "identity relation is not affirmative")))

    if conflicts:
        # The contract is fail-closed: the LLM cannot authorize mutation while
        # either deterministic plumbing or its own result reports a blocker.
        if action not in {
            MergeIdentityAction.SKIP_MERGE_SIDE_EFFECTS,
            MergeIdentityAction.REVIEW_REQUIRED,
        }:
            action = MergeIdentityAction.REVIEW_REQUIRED
            relation = MergeIdentityRelation.UNSAFE_TO_MERGE
            reason_code = "merge_identity_blocking_conflict"
            reasons = tuple(dict.fromkeys((*reasons, "blocking identity conflict requires review")))

    deterministic_code = _deterministic_merge_identity_veto_reason(candidate_subject, existing_subject)
    deterministic = False
    has_strong_shared_anchor = _strong_shared_anchor(candidate_subject, existing_subject)
    force_structural_veto = deterministic_code == "single_occurrence_vs_recurring_series" or (
        deterministic_code == "same_place_date_unrelated_type_time_conflict"
        and not has_strong_shared_anchor
    )
    if deterministic_code and (force_structural_veto or not has_strong_shared_anchor):
        # A high-confidence LLM can still allow genuinely same long-running events,
        # but not when it already says uncertainty/distinctness or has low confidence.
        if action in {MergeIdentityAction.SKIP_MERGE_SIDE_EFFECTS, MergeIdentityAction.REVIEW_REQUIRED}:
            conflicts = tuple(dict.fromkeys((*conflicts, deterministic_code)))
        elif force_structural_veto or relation not in {MergeIdentityRelation.SAME_EVENT, MergeIdentityRelation.SOURCE_UPDATE} or confidence < 0.9:
            action = MergeIdentityAction.SKIP_MERGE_SIDE_EFFECTS
            relation = MergeIdentityRelation.UNSAFE_TO_MERGE
            reason_code = deterministic_code
            deterministic = True
            conflicts = tuple(dict.fromkeys((*conflicts, deterministic_code)))
            reasons = tuple(dict.fromkeys((*reasons, "structural identity conflict between matched event and candidate")))

    return MergeIdentityGateVerdict(
        mode=resolved_mode,
        action=action,
        relation=relation,
        reason_code=reason_code,
        reasons=reasons or (reason_code,),
        candidate=candidate_subject,
        existing_event_id=existing_subject.event_id,
        confidence=confidence,
        blocking_conflicts=conflicts,
        allowed_fields=allowed_fields,
        deterministic=deterministic,
        llm=llm or None,
    )


def merge_identity_gate_fail_safe_verdict(
    *,
    mode: IdentityGateMode | str,
    candidate: Any | None = None,
    existing_event: Any | None = None,
    reason: str = "merge identity gate error",
) -> MergeIdentityGateVerdict:
    resolved_mode = mode if isinstance(mode, IdentityGateMode) else parse_identity_gate_mode(str(mode))
    subject = identity_subject_from(candidate, role="candidate") if candidate is not None else None
    existing_subject = identity_subject_from(existing_event, role="event") if existing_event is not None else None
    return MergeIdentityGateVerdict(
        mode=resolved_mode,
        action=MergeIdentityAction.SKIP_MERGE_SIDE_EFFECTS,
        relation=MergeIdentityRelation.UNSAFE_TO_MERGE,
        reason_code="merge_identity_gate_error",
        reasons=(reason,),
        candidate=subject,
        existing_event_id=existing_subject.event_id if existing_subject else None,
        confidence=0.0,
        fail_safe=True,
    )


def _veto(code: str, candidate: IdentitySubject, ev: IdentitySubject, confidence: float, reason: str) -> IdentityGateVerdict:
    return IdentityGateVerdict(
        mode=IdentityGateMode.ENFORCE,
        action=IdentityGateAction.VETO_CREATE,
        reason_code=code,
        reasons=(reason,),
        candidate=candidate,
        matched_event_id=ev.event_id,
        confidence=confidence,
        deterministic=True,
    )


def _vector_supported_identity_match(
    candidate: IdentitySubject,
    existing: Sequence[IdentitySubject],
    vector_evidence: IdentityVectorEvidence | None,
) -> tuple[IdentitySubject, str] | None:
    if (
        not vector_evidence
        or not vector_evidence.available
        or vector_evidence.nearest_event_id is None
        or vector_evidence.score is None
    ):
        return None
    ev = next((item for item in existing if item.event_id == vector_evidence.nearest_event_id), None)
    if ev is None:
        return None
    if not _date_overlaps(candidate, ev):
        return None
    if _is_long_running(candidate) or _is_long_running(ev):
        if vector_evidence.score < 0.86:
            return None
        if _locations_related(candidate, ev) or _titles_related(candidate.title, ev.title) or _strong_shared_anchor(candidate, ev):
            return ev, vector_evidence.reason or "high-confidence vector identity for overlapping long-running event"
        return None
    if vector_evidence.score < 0.94:
        return None
    if _same_known_time(candidate, ev) and (_titles_related(candidate.title, ev.title) or _locations_related(candidate, ev) or _strong_shared_anchor(candidate, ev)):
        return ev, vector_evidence.reason or "high-confidence vector identity for same dated slot"
    return None


def _strong_shared_anchor(left: IdentitySubject, right: IdentitySubject) -> bool:
    if left.source_url and right.source_url and left.source_url == right.source_url:
        return True
    if left.ticket_link and right.ticket_link and _normalize_url(left.ticket_link) == _normalize_url(right.ticket_link):
        return True
    return bool(left.poster_hashes and right.poster_hashes and set(left.poster_hashes).intersection(right.poster_hashes))


def _is_exhibition_like(subject: IdentitySubject) -> bool:
    event_type = _normalize_text(subject.event_type)
    return any(word in event_type for word in ("выстав", "экспозиц", "ярмарк", "exhibition", "fair"))


def _is_long_running(subject: IdentitySubject) -> bool:
    if _is_exhibition_like(subject):
        return True
    return bool(subject.end_date and subject.date and subject.end_date != subject.date)


def _single_occurrence_inside_recurring_series(
    candidate: IdentitySubject,
    existing: IdentitySubject,
) -> bool:
    """Return true for an exact one-day occurrence matched to a broader series row.

    A fresh source like "10 июля 20:00" should not mutate a public row whose
    anchors mean "1 мая — 30 сентября, every Friday".  This guardrail does not
    decide broad semantic identity; it only prevents deterministic source/ticket/
    poster anchors from treating the occurrence as the same public card as the
    whole season.
    """

    if _is_exhibition_like(candidate) or _is_exhibition_like(existing):
        return False
    if not (candidate.date and existing.date and existing.end_date):
        return False
    if candidate.end_date and candidate.end_date != candidate.date:
        return False
    if existing.end_date == existing.date:
        return False
    if not (existing.date < candidate.date <= existing.end_date):
        return False
    if not (_titles_related(candidate.title, existing.title) or _locations_related(candidate, existing)):
        return False
    return bool(
        _same_known_time(candidate, existing)
        or (
            candidate.ticket_link
            and existing.ticket_link
            and _normalize_url(candidate.ticket_link) == _normalize_url(existing.ticket_link)
        )
        or (
            candidate.source_url
            and existing.source_url
            and candidate.source_url == existing.source_url
        )
        or (
            candidate.poster_hashes
            and existing.poster_hashes
            and set(candidate.poster_hashes).intersection(existing.poster_hashes)
        )
    )


def _deterministic_merge_identity_veto_reason(
    candidate: IdentitySubject,
    existing: IdentitySubject,
) -> str | None:
    if _single_occurrence_inside_recurring_series(candidate, existing):
        return "single_occurrence_vs_recurring_series"

    candidate_long = _is_long_running(candidate)
    existing_long = _is_long_running(existing)
    title_related = _titles_related(candidate.title, existing.title)
    location_related = _locations_related(candidate, existing)
    same_time = _same_known_time(candidate, existing)
    type_conflict = _event_types_conflict(candidate.event_type, existing.event_type)
    explicit_time_conflict = _known_time_conflict(candidate, existing)

    if (
        candidate_long != existing_long
        and type_conflict
        and not title_related
        and not same_time
        and _date_overlaps(candidate, existing)
    ):
        return "single_slot_vs_long_running_type_conflict"

    if (
        type_conflict
        and not title_related
        and explicit_time_conflict
        and location_related
        and _date_overlaps(candidate, existing)
    ):
        return "same_place_date_unrelated_type_time_conflict"

    return None


def _event_types_conflict(left: str | None, right: str | None) -> bool:
    left_type = _coarse_event_type(left)
    right_type = _coarse_event_type(right)
    return bool(left_type and right_type and left_type != right_type)


def _coarse_event_type(value: str | None) -> str | None:
    text = _normalize_text(value)
    if not text:
        return None
    groups = {
        "exhibition": ("выстав", "экспозиц", "ярмарк", "exhibition", "fair"),
        "lecture": ("лекц", "встреч", "разговор", "дискус", "бесед", "lecture", "talk"),
        "concert": ("концерт", "музык", "music", "concert"),
        "performance": ("спектак", "театр", "показ", "performance", "show"),
        "tour": ("экскурс", "прогул", "tour"),
        "workshop": ("мастер", "воркш", "workshop"),
        "festival": ("фестив", "festival"),
    }
    for name, needles in groups.items():
        if any(needle in text for needle in needles):
            return name
    return None


def _coerce_merge_action(value: Any) -> MergeIdentityAction:
    try:
        return MergeIdentityAction(str(value or "").strip().lower())
    except Exception:
        return MergeIdentityAction.ALLOW_MERGE


def _coerce_merge_relation(value: Any) -> MergeIdentityRelation:
    try:
        return MergeIdentityRelation(str(value or "").strip().lower())
    except Exception:
        return MergeIdentityRelation.UNKNOWN


def _coerce_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except Exception:
        return None


def _clean_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_vector_evidence(value: IdentityVectorEvidence | Mapping[str, Any] | None) -> IdentityVectorEvidence | None:
    if value is None or isinstance(value, IdentityVectorEvidence):
        return value
    return IdentityVectorEvidence(
        available=bool(value.get("available", True)),
        nearest_event_id=_coerce_int(value.get("nearest_event_id") or value.get("event_id")),
        score=_coerce_float(value.get("score")),
        reason=_clean_str(value.get("reason")),
        error=_clean_str(value.get("error")),
    )


def _coerce_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except Exception:
        return None


def _normalize_text(value: str | None) -> str:
    return (value or "").replace("ё", "е").lower().strip()


def _tokens(value: str | None) -> set[str]:
    return {t for t in _TOKEN_RE.findall(_normalize_text(value)) if len(t) > 2 and t not in _EVENT_WORDS}


def _titles_related(left: str | None, right: str | None) -> bool:
    lt = _tokens(left)
    rt = _tokens(right)
    if not lt or not rt:
        return False
    overlap = lt & rt
    if not overlap:
        return False
    return len(overlap) >= min(2, min(len(lt), len(rt))) or len(overlap) / max(len(lt), len(rt)) >= 0.5


def _locations_related(left: IdentitySubject, right: IdentitySubject) -> bool:
    left_bits = _normalize_text(" ".join(x for x in [left.location_name, left.location_address, left.city] if x))
    right_bits = _normalize_text(" ".join(x for x in [right.location_name, right.location_address, right.city] if x))
    if not left_bits or not right_bits:
        return False
    return left_bits == right_bits or left_bits in right_bits or right_bits in left_bits


def _date_overlaps(left: IdentitySubject, right: IdentitySubject) -> bool:
    if not left.date or not right.date:
        return False
    left_end = left.end_date or left.date
    right_end = right.end_date or right.date
    return left.date <= right_end and right.date <= left_end


def _same_known_time(left: IdentitySubject, right: IdentitySubject) -> bool:
    if left.time_is_default or right.time_is_default:
        return False
    lt = _time_key(left.time)
    rt = _time_key(right.time)
    return bool(lt and rt and lt == rt)


def _known_time_conflict(left: IdentitySubject, right: IdentitySubject) -> bool:
    """Return true only for two explicit, valid and different start times."""

    if left.time_is_default or right.time_is_default:
        return False
    lt = _time_key(left.time)
    rt = _time_key(right.time)
    return bool(lt and rt and lt != rt)


def _time_key(value: str | None) -> str | None:
    raw = (value or "").strip()
    if not raw or raw == "00:00":
        return None
    match = re.search(r"(\d{1,2})[:.](\d{2})", raw)
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2))
    if hour > 23 or minute > 59:
        return None
    return f"{hour:02d}:{minute:02d}"


def _normalize_url(value: str | None) -> str:
    return canonicalize_identity_url(value, preserve_ticket_fragment=True) or ""


def canonicalize_identity_url(
    value: str | None,
    *,
    preserve_ticket_fragment: bool = False,
) -> str | None:
    """Return a stable public URL identity without erasing ticket semantics.

    Telegram preview/host variants and VK mobile/query variants collapse to one
    public identity. Tracking parameters never participate. Tretyakov's ``#buy``
    and ``#/buy`` fragments are application routes, not analytics fragments, so
    ticket canonicalization retains them.
    """

    raw = str(value or "").strip().strip("<>\"'")
    if not raw:
        return None
    canonical_tg = canonicalize_tg_url(raw)
    if canonical_tg:
        parts = urlsplit(canonical_tg)
        path_parts = [part for part in parts.path.split("/") if part]
        if len(path_parts) == 3 and path_parts[0].casefold() == "s":
            path_parts = path_parts[1:]
        if path_parts:
            path_parts[0] = path_parts[0].casefold()
        path = "/" + "/".join(path_parts) if path_parts else "/"
        query = _canonical_query(parts.query, ignored=_TG_NON_IDENTITY_QUERY_KEYS)
        return urlunsplit(("https", "t.me", path.rstrip("/") or "/", query, ""))

    if "://" not in raw and re.match(r"(?i)^(?:www\.|m\.)?(?:vk\.com|vk\.ru)/", raw):
        raw = f"https://{raw}"
    try:
        parts = urlsplit(raw)
        host = (parts.hostname or "").casefold().removeprefix("www.")
    except (TypeError, ValueError):
        return raw.rstrip("/") or None
    if not host:
        return raw.rstrip("/") or None

    query_pairs = parse_qsl(parts.query, keep_blank_values=True)
    if host in _VK_PUBLIC_HOSTS:
        wall_token = (parts.path or "").strip("/")
        if not _VK_WALL_TOKEN_RE.fullmatch(wall_token):
            wall_token = next(
                (
                    str(v).strip()
                    for k, v in query_pairs
                    if str(k).casefold() == "w" and _VK_WALL_TOKEN_RE.fullmatch(str(v).strip())
                ),
                "",
            )
        if wall_token:
            return f"https://vk.com/{wall_token.casefold()}"
        host = "vk.com"

    path = re.sub(r"/{2,}", "/", parts.path or "/")
    path = path.rstrip("/") or "/"
    query = _canonical_query(parts.query)
    fragment = ""
    if host.endswith("tretyakovgallery.ru"):
        raw_fragment = str(parts.fragment or "").strip()
        route = re.sub(r"/{2,}", "/", raw_fragment.lstrip("/"))
        if route.casefold() == "buy" or re.fullmatch(
            r"(?i)buy/event/[^/]+/[^/]+/[^/]+", route
        ):
            # A Tretyakov SPA fragment is the direct ticket/slot identity, not
            # an analytics fragment. #buy and #/buy converge to #/buy.
            fragment = "/" + route
    return urlunsplit(("https", host, path, query, fragment))


def _canonical_query(raw_query: str, *, ignored: set[str] | None = None) -> str:
    ignored_keys = {str(item).casefold() for item in (ignored or set())}
    pairs: list[tuple[str, str]] = []
    for key, value in parse_qsl(raw_query or "", keep_blank_values=True):
        folded = str(key).casefold()
        if folded.startswith("utm_") or folded in _TRACKING_QUERY_KEYS or folded in ignored_keys:
            continue
        pairs.append((str(key), str(value)))
    return urlencode(sorted(pairs), doseq=True)


def input_packet_fingerprint(value: Any) -> str:
    """SHA-256 of stable source-packet inputs before Smart Update mutates them.

    Provider metrics, token counts, generated prose/semantic decisions and the
    fingerprint field itself are intentionally excluded. Poster identity uses
    stable byte hashes (with a hashed URL fallback), never OCR/provider output.
    """

    if hasattr(value, "source_type") and hasattr(value, "source_text"):
        stable_scalar_fields = (
            "title",
            "date",
            "time",
            "time_is_default",
            "end_date",
            "end_date_is_inferred",
            "festival",
            "festival_context",
            "festival_full",
            "festival_source",
            "festival_series",
            "location_name",
            "location_address",
            "city",
            "ticket_price_min",
            "ticket_price_max",
            "ticket_status",
            "age_restriction",
            "age_restriction_is_structured",
            "event_type",
            "is_free",
            "pushkin_card",
            "source_chat_username",
            "source_chat_id",
            "source_message_id",
            "creator_id",
            "trust_level",
        )
        posters = []
        for poster in list(getattr(value, "posters", None) or []):
            digest = (
                getattr(poster, "raw_sha256", None)
                or getattr(poster, "sha256", None)
                or getattr(poster, "phash", None)
            )
            if digest:
                posters.append(str(digest).strip().lower())
                continue
            fallback_url = (
                getattr(poster, "supabase_url", None)
                or getattr(poster, "catbox_url", None)
            )
            canonical_fallback = canonicalize_identity_url(fallback_url)
            if canonical_fallback:
                posters.append("url_sha256:" + hashlib.sha256(canonical_fallback.encode("utf-8")).hexdigest())
        payload = {
            "canonical_source_url": canonicalize_identity_url(getattr(value, "source_url", None)),
            "source_type": str(getattr(value, "source_type", "") or "").strip().lower(),
            "source_role": _normalize_source_role(getattr(value, "source_role", None)),
            "source_text": _normalized_packet_text(getattr(value, "source_text", None)),
            "raw_excerpt": _normalized_packet_text(getattr(value, "raw_excerpt", None)),
            "ticket_link": canonicalize_identity_url(
                getattr(value, "ticket_link", None), preserve_ticket_fragment=True
            ),
            "festival_dedup_links": sorted(
                filter(None, (canonicalize_identity_url(item) for item in (getattr(value, "festival_dedup_links", None) or [])))
            ),
            "poster_hashes": sorted(set(posters)),
            "poster_scope_hashes": sorted(
                str(item).strip().lower()
                for item in (getattr(value, "poster_scope_hashes", None) or [])
                if str(item or "").strip()
            ),
            "links_payload": _fingerprint_value(getattr(value, "links_payload", None)),
            "organizer_names": sorted(str(item).strip() for item in (getattr(value, "organizer_names", None) or []) if str(item or "").strip()),
            "structured": {
                name: _fingerprint_value(getattr(value, name, None))
                for name in stable_scalar_fields
            },
        }
    else:
        payload = _fingerprint_value(value)
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _normalized_packet_text(value: Any) -> str | None:
    if value is None:
        return None
    text = unicodedata.normalize("NFC", str(value)).replace("\r\n", "\n").replace("\r", "\n")
    text = "\n".join(re.sub(r"[ \t]+", " ", line).strip() for line in text.split("\n"))
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text or None


def _fingerprint_value(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return {
            item.name: _fingerprint_value(getattr(value, item.name))
            for item in fields(value)
        }
    if isinstance(value, Mapping):
        return {
            str(key): _fingerprint_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_fingerprint_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        normalized = [_fingerprint_value(item) for item in value]
        return sorted(normalized, key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True))
    if isinstance(value, bytes):
        return {"__bytes_sha256__": hashlib.sha256(value).hexdigest()}
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)
