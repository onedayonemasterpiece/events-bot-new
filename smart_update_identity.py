"""Identity gate helpers for Smart Update create-path safety.

The helpers in this module are intentionally small and dependency-light so they can
be unit-tested without the Smart Update DB/LLM stack.  They never decide to merge;
they can only allow create or veto create when another identity signal is strong
enough that creating a new row would be unsafe.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import re
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


class IdentityGateMode(str, Enum):
    OFF = "off"
    SHADOW = "shadow"
    ENFORCE = "enforce"


class IdentityGateAction(str, Enum):
    ALLOW_CREATE = "allow_create"
    VETO_CREATE = "veto_create"


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
    ticket_link: str | None = None
    source_url: str | None = None
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
    if role == "event":
        return (
            _get(obj, "source_url")
            or _get(obj, "source_post_url")
            or _get(obj, "source_vk_post_url")
        )
    return _get(obj, "source_url") or _get(obj, "source_post_url") or _get(obj, "source_vk_post_url")


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
        ticket_link=_clean_str(_get(obj, "ticket_link")),
        source_url=_clean_str(_source_url_for(obj, role)),
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

    if vector_evidence and vector_evidence.available and vector_evidence.error:
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
    if (
        vector_evidence
        and vector_evidence.available
        and vector_evidence.nearest_event_id is not None
        and vector_evidence.score is not None
        and vector_evidence.score >= 0.94
    ):
        return IdentityGateVerdict(
            mode=IdentityGateMode.ENFORCE,
            action=IdentityGateAction.VETO_CREATE,
            reason_code="vector_nearest_identity",
            reasons=(vector_evidence.reason or "high-confidence vector identity evidence",),
            candidate=candidate,
            matched_event_id=vector_evidence.nearest_event_id,
            confidence=float(vector_evidence.score),
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
    raw = (value or "").strip()
    if not raw:
        return ""
    try:
        parts = urlsplit(raw)
    except Exception:
        return raw.rstrip("/")
    scheme = (parts.scheme or "https").lower()
    netloc = parts.netloc.lower().removeprefix("www.")
    path = (parts.path or "/").rstrip("/") or "/"
    query = urlencode(sorted(parse_qsl(parts.query, keep_blank_values=True)))
    return urlunsplit((scheme, netloc, path, query, ""))
