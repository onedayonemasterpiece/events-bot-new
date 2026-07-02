"""Event identity candidate documents and Supabase vector recall helpers.

This module is intentionally not wired into Smart Update yet.  It only provides
server-side/backend primitives that can be called by future Smart Update lanes
with an injected service-role Supabase client.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, field
from queue import Queue
from threading import Thread
from typing import Any, Mapping, Sequence

IDENTITY_CANDIDATE_DOC_KIND = "identity_candidate_v1"
EVENT_IDENTITY_RPC_NAME = "event_identity_candidates_by_embedding_v1"
_DEFAULT_DOC_MAX_CHARS = 1800
_DEFAULT_FIELD_MAX_CHARS = 420
_DEFAULT_SOURCE_TEXT_MAX_CHARS = 700


@dataclass(frozen=True)
class IdentityCandidateDocument:
    """Compact text document prepared for embedding an incoming candidate."""

    kind: str
    text: str
    sha256: str
    truncated: bool
    provenance_labels: tuple[str, ...]
    char_count: int


@dataclass(frozen=True)
class EventIdentityRecallConfig:
    """Tunable recall configuration for service-role backend callers."""

    top_k: int = 8
    min_similarity: float = 0.75
    timeout_seconds: float = 2.5
    embedding_doc_kind: str = IDENTITY_CANDIDATE_DOC_KIND
    max_top_k: int = 50

    def normalized_top_k(self, override: int | None = None) -> int:
        raw = self.top_k if override is None else override
        try:
            value = int(raw)
        except Exception:
            value = self.top_k
        return max(1, min(int(self.max_top_k or 50), value))

    def normalized_timeout(self) -> float:
        try:
            value = float(self.timeout_seconds)
        except Exception:
            value = 2.5
        return max(0.05, value)

    def normalized_min_similarity(self) -> float:
        try:
            value = float(self.min_similarity)
        except Exception:
            value = 0.75
        if math.isnan(value):
            return 0.75
        return max(-1.0, min(1.0, value))


@dataclass(frozen=True)
class EventIdentityCandidateEvidence:
    """One row returned by the event identity vector recall RPC."""

    event_id: int | None = None
    document_id: int | None = None
    embedding_id: int | None = None
    similarity: float | None = None
    distance: float | None = None
    embedding_doc_kind: str | None = None
    candidate_doc_kind: str | None = None
    title: str | None = None
    event_date: str | None = None
    event_time: str | None = None
    end_date: str | None = None
    city: str | None = None
    event_type: str | None = None
    location_name: str | None = None
    location_address: str | None = None
    ticket_link: str | None = None
    source_url: str | None = None
    source_type: str | None = None
    telegraph_url: str | None = None
    tg_event_post_url: str | None = None
    source_vk_post_url: str | None = None
    document_hash: str | None = None
    document_text: str | None = None
    evidence: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EventIdentityRecallResult:
    """Safe result wrapper: callers can treat failures as empty recall."""

    ok: bool
    candidates: tuple[EventIdentityCandidateEvidence, ...] = ()
    rpc_name: str = EVENT_IDENTITY_RPC_NAME
    error_type: str | None = None
    error_message: str | None = None
    timed_out: bool = False


def _read_attr(source: Any, name: str, default: Any = None) -> Any:
    if isinstance(source, Mapping):
        return source.get(name, default)
    return getattr(source, name, default)


def _as_clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        parts = [_as_clean_text(v) for v in value]
        return "; ".join(part for part in parts if part)
    text = str(value).replace("\x00", " ")
    return " ".join(text.split())


def _clip_text(text: str, max_chars: int) -> tuple[str, bool]:
    text = _as_clean_text(text)
    if max_chars <= 0 or len(text) <= max_chars:
        return text, False
    suffix = "…[truncated]"
    keep = max(0, max_chars - len(suffix))
    return f"{text[:keep].rstrip()}{suffix}", True


def _append_line(
    lines: list[str],
    labels: list[str],
    label: str,
    value: Any,
    *,
    max_chars: int = _DEFAULT_FIELD_MAX_CHARS,
) -> bool:
    text, truncated = _clip_text(_as_clean_text(value), max_chars)
    if not text:
        return False
    lines.append(f"[{label}] {text}")
    labels.append(label)
    return truncated


def _poster_values(candidate: Any, attr: str, *, max_items: int = 4) -> list[str]:
    out: list[str] = []
    for poster in list(_read_attr(candidate, "posters", []) or [])[:max_items]:
        value = _read_attr(poster, attr)
        text = _as_clean_text(value)
        if text:
            out.append(text)
    return out


def build_identity_candidate_document(
    candidate: Any,
    *,
    kind: str = IDENTITY_CANDIDATE_DOC_KIND,
    max_chars: int = _DEFAULT_DOC_MAX_CHARS,
    field_max_chars: int = _DEFAULT_FIELD_MAX_CHARS,
    source_text_max_chars: int = _DEFAULT_SOURCE_TEXT_MAX_CHARS,
) -> IdentityCandidateDocument:
    """Build a compact, provenance-labelled identity document for embeddings.

    ``candidate`` may be a ``smart_event_update.EventCandidate``, a dataclass, or
    a plain mapping.  The output text is deliberately compact and stable: each
    line carries a provenance label so downstream evidence can explain which
    candidate field contributed to identity matching.
    """

    lines: list[str] = [f"[doc.kind] {kind}"]
    labels: list[str] = ["doc.kind"]
    truncated = False

    for label, name in (
        ("candidate.title", "title"),
        ("candidate.date", "date"),
        ("candidate.time", "time"),
        ("candidate.end_date", "end_date"),
        ("candidate.location_name", "location_name"),
        ("candidate.location_address", "location_address"),
        ("candidate.city", "city"),
        ("candidate.event_type", "event_type"),
        ("candidate.festival", "festival"),
        ("candidate.ticket_status", "ticket_status"),
        ("candidate.ticket_link", "ticket_link"),
        ("candidate.source_type", "source_type"),
        ("candidate.source_url", "source_url"),
        ("candidate.search_digest", "search_digest"),
        ("candidate.raw_excerpt", "raw_excerpt"),
    ):
        truncated |= _append_line(
            lines,
            labels,
            label,
            _read_attr(candidate, name),
            max_chars=field_max_chars,
        )

    # Source text is identity-useful, but also the biggest field; give it its own
    # tighter budget to keep embedding costs predictable.
    truncated |= _append_line(
        lines,
        labels,
        "candidate.source_text",
        _read_attr(candidate, "source_text"),
        max_chars=source_text_max_chars,
    )

    poster_titles = _poster_values(candidate, "ocr_title")
    if poster_titles:
        truncated |= _append_line(
            lines,
            labels,
            "candidate.posters.ocr_title",
            poster_titles,
            max_chars=field_max_chars,
        )
    poster_texts = _poster_values(candidate, "ocr_text", max_items=2)
    if poster_texts:
        truncated |= _append_line(
            lines,
            labels,
            "candidate.posters.ocr_text",
            poster_texts,
            max_chars=field_max_chars,
        )
    poster_hashes = _poster_values(candidate, "phash") or list(
        _read_attr(candidate, "poster_scope_hashes", []) or []
    )[:4]
    if poster_hashes:
        truncated |= _append_line(
            lines,
            labels,
            "candidate.posters.phash",
            poster_hashes,
            max_chars=field_max_chars,
        )

    text = "\n".join(lines).strip()
    clipped_text, text_truncated = _clip_text(text, max_chars)
    truncated = truncated or text_truncated
    digest = hashlib.sha256(clipped_text.encode("utf-8")).hexdigest()
    return IdentityCandidateDocument(
        kind=kind,
        text=clipped_text,
        sha256=digest,
        truncated=truncated,
        provenance_labels=tuple(dict.fromkeys(labels)),
        char_count=len(clipped_text),
    )


def _coerce_embedding(values: Sequence[float] | Sequence[int]) -> list[float]:
    out: list[float] = []
    for value in values or []:
        out.append(float(value))
    if not out:
        raise ValueError("embedding must contain at least one numeric value")
    return out


def _rpc_data(result: Any) -> list[Mapping[str, Any]]:
    data = getattr(result, "data", result)
    if data is None:
        return []
    if isinstance(data, Mapping):
        return [data]
    if isinstance(data, list):
        return [row for row in data if isinstance(row, Mapping)]
    return []


def _candidate_from_row(row: Mapping[str, Any]) -> EventIdentityCandidateEvidence:
    def as_int(name: str) -> int | None:
        value = row.get(name)
        try:
            return int(value) if value is not None else None
        except Exception:
            return None

    def as_float(name: str) -> float | None:
        value = row.get(name)
        try:
            return float(value) if value is not None else None
        except Exception:
            return None

    evidence = row.get("evidence")
    if not isinstance(evidence, Mapping):
        evidence = {}
    return EventIdentityCandidateEvidence(
        event_id=as_int("event_id"),
        document_id=as_int("document_id"),
        embedding_id=as_int("embedding_id"),
        similarity=as_float("similarity"),
        distance=as_float("distance"),
        embedding_doc_kind=_as_clean_text(row.get("embedding_doc_kind")) or None,
        candidate_doc_kind=_as_clean_text(row.get("candidate_doc_kind")) or None,
        title=_as_clean_text(row.get("title")) or None,
        event_date=_as_clean_text(row.get("event_date")) or None,
        event_time=_as_clean_text(row.get("event_time")) or None,
        end_date=_as_clean_text(row.get("end_date")) or None,
        city=_as_clean_text(row.get("city")) or None,
        event_type=_as_clean_text(row.get("event_type")) or None,
        location_name=_as_clean_text(row.get("location_name")) or None,
        location_address=_as_clean_text(row.get("location_address")) or None,
        ticket_link=_as_clean_text(row.get("ticket_link")) or None,
        source_url=_as_clean_text(row.get("source_url")) or None,
        source_type=_as_clean_text(row.get("source_type")) or None,
        telegraph_url=_as_clean_text(row.get("telegraph_url")) or None,
        tg_event_post_url=_as_clean_text(row.get("tg_event_post_url")) or None,
        source_vk_post_url=_as_clean_text(row.get("source_vk_post_url")) or None,
        document_hash=_as_clean_text(row.get("document_hash")) or None,
        document_text=_as_clean_text(row.get("document_text")) or None,
        evidence=dict(evidence),
    )


def recall_identity_candidates_by_embedding(
    supabase_client: Any,
    embedding: Sequence[float] | Sequence[int],
    *,
    city: str | None = None,
    event_type: str | None = None,
    config: EventIdentityRecallConfig | None = None,
    top_k: int | None = None,
    min_similarity: float | None = None,
) -> EventIdentityRecallResult:
    """Call service-role Supabase RPC and return safe vector recall results.

    No credentials are read here: callers must inject a backend/service-role
    Supabase client.  Any RPC, network, schema, or timeout failure returns
    ``ok=False`` with an empty candidate tuple so Smart Update can fail closed.
    """

    cfg = config or EventIdentityRecallConfig()
    try:
        vector = _coerce_embedding(embedding)
    except Exception as exc:
        return EventIdentityRecallResult(
            ok=False,
            error_type=type(exc).__name__,
            error_message=str(exc)[:300],
        )

    if supabase_client is None:
        return EventIdentityRecallResult(
            ok=False,
            error_type="MissingSupabaseClient",
            error_message="service-role Supabase client is required",
        )

    try:
        effective_min = (
            cfg.normalized_min_similarity()
            if min_similarity is None
            else max(-1.0, min(1.0, float(min_similarity)))
        )
    except Exception:
        effective_min = cfg.normalized_min_similarity()

    payload = {
        "p_embedding": vector,
        "p_embedding_doc_kind": cfg.embedding_doc_kind or IDENTITY_CANDIDATE_DOC_KIND,
        "p_city": _as_clean_text(city) or None,
        "p_event_type": _as_clean_text(event_type) or None,
        "p_limit": cfg.normalized_top_k(top_k),
        "p_min_similarity": effective_min,
    }

    result_queue: Queue[tuple[str, Any]] = Queue(maxsize=1)

    def _call_rpc() -> None:
        try:
            value = supabase_client.rpc(EVENT_IDENTITY_RPC_NAME, payload).execute()
        except Exception as exc:  # pragma: no cover - exercised via result path
            result_queue.put(("error", exc))
            return
        result_queue.put(("ok", value))

    worker = Thread(target=_call_rpc, name="event-identity-rpc", daemon=True)
    worker.start()
    worker.join(timeout=cfg.normalized_timeout())
    if worker.is_alive():
        return EventIdentityRecallResult(
            ok=False,
            timed_out=True,
            error_type="TimeoutError",
            error_message=f"{EVENT_IDENTITY_RPC_NAME} exceeded {cfg.normalized_timeout():.2f}s",
        )

    try:
        status, result = result_queue.get_nowait()
    except Exception as exc:
        return EventIdentityRecallResult(
            ok=False,
            error_type=type(exc).__name__,
            error_message=str(exc)[:300],
        )

    if status == "error":
        exc = result
        return EventIdentityRecallResult(
            ok=False,
            error_type=type(exc).__name__,
            error_message=str(exc)[:300],
        )

    try:
        rows = _rpc_data(result)
        candidates = tuple(_candidate_from_row(row) for row in rows)
        return EventIdentityRecallResult(ok=True, candidates=candidates)
    except Exception as exc:
        return EventIdentityRecallResult(
            ok=False,
            error_type=type(exc).__name__,
            error_message=str(exc)[:300],
        )
