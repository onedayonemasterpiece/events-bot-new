"""Event identity candidate documents and Supabase vector recall helpers.

Smart Update uses these backend primitives for vector-first identity recall.
Vectors retrieve candidates only; LLM/source-grounded gates make the final
match/create decision.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import urllib.request
from dataclasses import dataclass, field
from queue import Queue
from threading import Thread
from typing import Any, Mapping, Sequence

IDENTITY_CANDIDATE_DOC_KIND = "identity_candidate_v1"
EVENT_RELATED_DOC_KIND = "related_v1"
EVENT_SEARCH_DOC_KIND = "search_v3"
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
    embedding_model: str = "gemini-embedding-2"
    embedding_dim: int = 768


@dataclass(frozen=True)
class EventIdentityRecallConfig:
    """Tunable recall configuration for service-role backend callers."""

    top_k: int = 8
    min_similarity: float = 0.75
    timeout_seconds: float = 2.5
    # Existing event embeddings are stored under document kinds such as
    # ``related_v1`` and ``search_v3``.  The incoming candidate document remains
    # ``identity_candidate_v1``; this value selects the *target* embedding kind
    # to search against.
    embedding_doc_kind: str = EVENT_RELATED_DOC_KIND
    max_top_k: int = 50
    embedding_model: str = "gemini-embedding-2"
    embedding_dim: int = 768

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
class EventIdentityEmbeddingResult:
    """Safe wrapper around ephemeral candidate embedding generation."""

    ok: bool
    embedding: tuple[float, ...] = ()
    model: str = "gemini-embedding-2"
    dim: int = 768
    error_type: str | None = None
    error_message: str | None = None


class SupabaseRestRpcClient:
    """Tiny service-role REST RPC client compatible with ``recall_*`` helpers."""

    def __init__(self, url: str, service_key: str, *, timeout_seconds: float = 2.5) -> None:
        self.url = (url or "").rstrip("/")
        self.service_key = service_key or ""
        self.timeout_seconds = max(0.05, float(timeout_seconds or 2.5))

    def rpc(self, name: str, payload: Mapping[str, Any]) -> "_SupabaseRpcRequest":
        return _SupabaseRpcRequest(self, name, payload)


class _SupabaseRpcRequest:
    def __init__(self, client: SupabaseRestRpcClient, name: str, payload: Mapping[str, Any]) -> None:
        self.client = client
        self.name = name
        self.payload = dict(payload)

    def execute(self) -> Any:
        if not self.client.url or not self.client.service_key:
            raise RuntimeError("Supabase URL and service-role key are required")
        endpoint = f"{self.client.url}/rest/v1/rpc/{self.name}"
        body = json.dumps(self.payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            endpoint,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "apikey": self.client.service_key,
                "Authorization": f"Bearer {self.client.service_key}",
            },
        )
        with urllib.request.urlopen(request, timeout=self.client.timeout_seconds) as response:
            data = json.loads(response.read().decode("utf-8") or "[]")
        return data


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
    embedding_model: str = "gemini-embedding-2",
    embedding_dim: int = 768,
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

    # Keep the incoming vector in the same semantic space as persisted
    # ``related_v1``. Raw source/OCR, URLs, ticket logistics and image hashes are
    # valuable adjudication evidence, but embedding them here makes the query
    # document asymmetric and lets sponsor/venue noise swamp event identity.
    for label, name in (
        ("candidate.title", "title"),
        ("candidate.event_type", "event_type"),
        ("candidate.search_digest", "search_digest"),
        ("candidate.short_description", "short_description"),
        ("candidate.description", "description"),
        ("candidate.location_name", "location_name"),
        ("candidate.city", "city"),
        ("candidate.festival", "festival"),
    ):
        truncated |= _append_line(
            lines,
            labels,
            label,
            _read_attr(candidate, name),
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
        embedding_model=embedding_model,
        embedding_dim=int(embedding_dim),
    )


def embed_identity_document_with_gemini(
    document: IdentityCandidateDocument | str,
    *,
    google_ai_client: Any | None = None,
    api_key: str | None = None,
    model: str = "gemini-embedding-2",
    dim: int = 768,
    timeout_seconds: float = 10.0,
) -> EventIdentityEmbeddingResult:
    """Generate an ephemeral Gemini embedding for a candidate identity document.

    The candidate embedding is not stored.  Failures are returned as data so
    Smart Update can log/review instead of silently bypassing the identity gate.

    ``google_ai_client`` must be an initialized :class:`GoogleAIClient`.  Raw
    API-key transport was intentionally removed: every provider send, including
    SDK retries, must pass through the client's shared reserve/mark/finalize
    accounting.  ``api_key`` remains only as a fail-closed compatibility
    argument so older callers receive a useful error instead of silently making
    an unaccounted request.
    """

    text = document.text if isinstance(document, IdentityCandidateDocument) else _as_clean_text(document)
    try:
        if api_key:
            raise RuntimeError(
                "direct identity embedding without GoogleAIClient limiter is disabled; "
                "raw Google API keys are not accepted"
            )
        if google_ai_client is None:
            raise RuntimeError("shared-limiter GoogleAIClient is required for identity embedding")
        embed_async = getattr(google_ai_client, "embed_content_async", None)
        if not callable(embed_async):
            raise TypeError("google_ai_client must provide embed_content_async")
        if not text:
            raise RuntimeError("identity document text is empty")

        async def _embed() -> tuple[float, ...]:
            values, _usage = await embed_async(
                model=model,
                text=text,
                output_dimensionality=int(dim),
            )
            return tuple(float(value) for value in values)

        result_queue: Queue[tuple[str, Any]] = Queue(maxsize=1)

        def _run_embed() -> None:
            try:
                result_queue.put(("ok", asyncio.run(_embed())))
            except BaseException as exc:  # preserve the safe result contract
                result_queue.put(("error", exc))

        worker = Thread(target=_run_embed, name="event-identity-embedding", daemon=True)
        worker.start()
        effective_timeout = max(0.1, float(timeout_seconds or 10.0))
        worker.join(timeout=effective_timeout)
        if worker.is_alive():
            raise TimeoutError(
                f"GoogleAIClient identity embedding exceeded {effective_timeout:.1f}s"
            )
        status, values = result_queue.get_nowait()
        if status == "error":
            raise values
        if len(values) != int(dim):
            got = len(values)
            raise RuntimeError(f"Gemini embedding returned unexpected dimension: {got}")
        return EventIdentityEmbeddingResult(
            ok=True,
            embedding=values,
            model=model,
            dim=int(dim),
        )
    except Exception as exc:
        return EventIdentityEmbeddingResult(
            ok=False,
            model=model,
            dim=int(dim),
            error_type=type(exc).__name__,
            error_message=str(exc)[:300],
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


def recall_identity_candidates_across_doc_kinds(
    supabase_client: Any,
    embedding: Sequence[float] | Sequence[int],
    *,
    city: str | None = None,
    event_type: str | None = None,
    doc_kinds: Sequence[str] = (EVENT_RELATED_DOC_KIND, EVENT_SEARCH_DOC_KIND),
    config: EventIdentityRecallConfig | None = None,
    top_k: int | None = None,
    min_similarity: float | None = None,
) -> EventIdentityRecallResult:
    """Search existing event vectors across related/search document kinds.

    Results are unioned by event id and the strongest similarity is retained.
    ``search_v3`` is recall evidence only; deterministic/LLM guard layers decide
    whether it can veto create.
    """

    cfg = config or EventIdentityRecallConfig()
    merged: dict[int, EventIdentityCandidateEvidence] = {}
    errors: list[str] = []
    any_ok = False
    for kind in tuple(dict.fromkeys(k for k in doc_kinds if k)) or (EVENT_RELATED_DOC_KIND,):
        kind_cfg = EventIdentityRecallConfig(
            top_k=cfg.top_k,
            min_similarity=cfg.min_similarity,
            timeout_seconds=cfg.timeout_seconds,
            embedding_doc_kind=str(kind),
            max_top_k=cfg.max_top_k,
            embedding_model=cfg.embedding_model,
            embedding_dim=cfg.embedding_dim,
        )
        result = recall_identity_candidates_by_embedding(
            supabase_client,
            embedding,
            city=city,
            event_type=event_type,
            config=kind_cfg,
            top_k=top_k,
            min_similarity=min_similarity,
        )
        if not result.ok:
            errors.append(f"{kind}:{result.error_type or 'error'}")
            continue
        any_ok = True
        for cand in result.candidates:
            if cand.event_id is None:
                continue
            old = merged.get(cand.event_id)
            old_score = old.similarity if old and old.similarity is not None else -999.0
            new_score = cand.similarity if cand.similarity is not None else -999.0
            if old is None or new_score > old_score:
                merged[cand.event_id] = cand
    if not any_ok and errors:
        return EventIdentityRecallResult(
            ok=False,
            error_type=";".join(errors)[:120],
            error_message="all identity vector recall doc-kind searches failed",
        )
    ordered = tuple(
        sorted(merged.values(), key=lambda c: (c.similarity if c.similarity is not None else -999.0), reverse=True)
    )
    return EventIdentityRecallResult(ok=True, candidates=ordered)
