from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import parse_qs, urlsplit

from .config import PrivateEventsMCPConfig
from .incident_index import IncidentDocument, IncidentIndex
from .readonly_sqlite import (
    DatabaseUnavailableError,
    QueryBudgetExceeded,
    ReadOnlySQLite,
    ReadOnlySQLiteError,
)


_WORD_RE = re.compile(r"[0-9A-Za-zА-Яа-яЁё_-]+", re.UNICODE)

# Runtime payloads are JSON-shaped but may contain provider credentials or
# personal operator identifiers nested several levels deep.  Redaction must be
# recursive: filtering only top-level SQLite column names is not sufficient.
_SENSITIVE_KEY_PARTS = (
    "password",
    "passwd",
    "secret",
    "client_secret",
    "access_token",
    "refresh_token",
    "operator_token",
    "signing_key",
    "private_key",
    "api_key",
    "apikey",
    "authorization",
    "bearer",
)
_NON_SECRET_TOKEN_COUNTER_KEYS = frozenset(
    {
        "total_tokens",
        "prompt_tokens",
        "completion_tokens",
        "input_tokens",
        "output_tokens",
        "cached_tokens",
        "reasoning_tokens",
    }
)
_PERSONAL_IDENTIFIER_KEYS = frozenset(
    {
        "operator_id",
        "operator_email",
        "operator_username",
        "operator_name",
        "reviewer_id",
        "reviewer_email",
        "reviewer_username",
        "reviewer_name",
        "user_id",
        "creator_id",
        "chat_id",
        "source_chat_id",
        "telegram_user_id",
        "telegram_id",
        "decided_by",
    }
)
_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)\b(access[_-]?token|refresh[_-]?token|client[_-]?secret|"
    r"telegram[_-]?bot[_-]?token|bot[_-]?token|operator[_-]?token|"
    r"operator[_-]?(?:id|email|username|name)|"
    r"reviewer[_-]?(?:id|email|username|name)|token|"
    r"signing[_-]?key|private[_-]?key|api[_-]?key|"
    r"password|authorization)\b(\s*[:=]\s*)([^\s,;&]+)"
)
_BEARER_RE = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{12,}")
_VK_TOKEN_RE = re.compile(r"\bvk1\.a\.[A-Za-z0-9_-]{12,}")
_OPENAI_KEY_RE = re.compile(r"\bsk-[A-Za-z0-9_-]{16,}")
_TELEGRAM_BOT_TOKEN_RE = re.compile(r"(?<!\d)\d{5,16}:[A-Za-z0-9_-]{20,100}\b")


def _normalise_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.casefold()).strip("_")


def _is_sensitive_key(value: str) -> bool:
    normalized = _normalise_key(value)
    if normalized in _NON_SECRET_TOKEN_COUNTER_KEYS:
        return False
    if normalized == "token" or normalized.endswith("_token"):
        return True
    if normalized in _PERSONAL_IDENTIFIER_KEYS or normalized.endswith(
        ("_operator_id", "_operator_email", "_operator_username", "_operator_name")
    ):
        return True
    return any(part in normalized for part in _SENSITIVE_KEY_PARTS)


def _redact_text(value: str) -> str:
    value = value.replace("\x00", "")
    # Consume complete bearer credentials before the generic assignment rule;
    # otherwise ``Authorization: Bearer <token>`` can leave the token tail behind.
    value = _BEARER_RE.sub("Bearer <redacted>", value)
    value = _SECRET_ASSIGNMENT_RE.sub(
        lambda match: f"{match.group(1)}{match.group(2)}<redacted>", value
    )
    value = _VK_TOKEN_RE.sub("vk1.a.<redacted>", value)
    value = _OPENAI_KEY_RE.sub("sk-<redacted>", value)
    value = _TELEGRAM_BOT_TOKEN_RE.sub("<telegram-bot-token-redacted>", value)
    return value


_URL_COLUMNS = (
    "telegraph_url",
    "tg_event_post_url",
    "source_post_url",
    "source_vk_post_url",
    "vk_repost_url",
    "ics_url",
)
_EVENT_COLUMNS = (
    "id",
    "title",
    "description",
    "short_description",
    "festival",
    "date",
    "end_date",
    "end_date_is_inferred",
    "time",
    "time_is_default",
    "location_name",
    "location_address",
    "city",
    "ticket_price_min",
    "ticket_price_max",
    "ticket_link",
    "ticket_status",
    "ticket_trust_level",
    "event_type",
    "emoji",
    "is_free",
    "pushkin_card",
    "silent",
    "lifecycle_status",
    "source_text",
    "source_texts",
    "search_digest",
    "topics",
    "topics_manual",
    "telegraph_url",
    "telegraph_path",
    "source_post_url",
    "source_vk_post_url",
    "vk_repost_url",
    "tg_event_post_url",
    "ics_url",
    "photo_urls",
    "photo_count",
    "preview_3d_url",
    "linked_event_ids",
    "added_at",
    "content_hash",
)
_SOURCE_COLUMNS = (
    "id",
    "event_id",
    "source_type",
    "source_url",
    "canonical_source_url",
    "source_role",
    "source_fingerprint",
    "source_chat_username",
    "source_chat_id",
    "source_message_id",
    "source_text",
    "imported_at",
    "trust_level",
)
_PUBLICATION_COLUMNS = (
    "id",
    "event_id",
    "platform",
    "target",
    "stored_url",
    "live_url",
    "stored_post_id",
    "live_post_id",
    "match_method",
    "match_confidence",
    "status",
    "resolved_at",
)
_IDENTITY_DECISION_COLUMNS = (
    "id",
    "event_id",
    "candidate_event_id",
    "source_id",
    "source_type",
    "source_url",
    "decision",
    "decision_reason",
    "confidence",
    "decided_by",
    "decision_payload",
    "created_at",
)
_VK_INBOX_COLUMNS = (
    "id",
    "group_id",
    "owner_type",
    "post_id",
    "date",
    "text",
    "matched_kw",
    "has_date",
    "event_ts_hint",
    "status",
    "imported_event_id",
    "review_batch",
    "attempts",
    "created_at",
)
_VK_INBOX_IMPORT_COLUMNS = ("inbox_id", "event_id", "created_at")
_FACT_COLUMNS = (
    "id",
    "event_id",
    "source_id",
    "fact_type",
    "fact_key",
    "fact_value",
    "key",
    "value",
    "confidence",
    "status",
    "provenance",
    "provenance_json",
    "created_at",
    "updated_at",
)
_JOB_COLUMNS = (
    "id",
    "event_id",
    "task",
    "status",
    "attempts",
    "last_error",
    "last_result",
    "coalesce_key",
    "depends_on",
    "updated_at",
    "next_run_at",
    "payload",
    "error_class",
)
_RUN_COLUMNS = (
    "id",
    "run_id",
    "kind",
    "operation",
    "name",
    "status",
    "event_id",
    "subject_id",
    "started_at",
    "finished_at",
    "created_at",
    "updated_at",
    "error",
    "error_json",
    "result",
    "result_json",
    "details",
    "details_json",
    "meta_json",
    "payload_json",
    "parent_run_id",
    "correlation_id",
    "error_class",
)
_REVIEW_COLUMNS = (
    "id",
    "event_id",
    "status",
    "reason",
    "review_reason",
    "review_type",
    "source_url",
    "source_type",
    "evidence_json",
    "details_json",
    "decision_json",
    "created_at",
    "updated_at",
    "resolved_at",
)
_POSTER_COLUMNS = (
    "id",
    "event_id",
    "catbox_url",
    "supabase_url",
    "poster_hash",
    "phash",
    "ocr_title",
    "ocr_text",
    "updated_at",
)


class RepositoryError(RuntimeError):
    pass


class NotFoundError(RepositoryError):
    pass


class InvalidArgumentsError(RepositoryError):
    pass


@dataclass(frozen=True, slots=True)
class SocialPostIdentity:
    platform: str
    canonical_url: str
    provider_key: str
    vk_owner_id: int | None = None
    vk_post_id: int | None = None


_VK_POST_RE = re.compile(r"^wall(-?[1-9][0-9]*)_([1-9][0-9]*)$")
_TG_PUBLIC_POST_RE = re.compile(r"^([A-Za-z0-9_]{5,})/([1-9][0-9]*)$")
_TG_PRIVATE_POST_RE = re.compile(r"^c/([1-9][0-9]*)/([1-9][0-9]*)$")


def canonicalize_social_post_url(value: str) -> SocialPostIdentity:
    """Parse one exact VK/TG post URL without resolving or fetching it.

    Only provider-owned hosts and unambiguous post forms are accepted.  The
    resulting identity is safe to use solely in fixed, parameterized equality
    predicates; it is never interpreted as SQL or followed as a URL.
    """

    if not isinstance(value, str):
        raise InvalidArgumentsError("post_url must be a string")
    raw = value.strip()
    if not raw or len(raw) > 1000:
        raise InvalidArgumentsError("post_url is invalid")
    try:
        parsed = urlsplit(raw)
        port = parsed.port
    except ValueError as exc:
        raise InvalidArgumentsError("post_url is invalid") from exc
    if parsed.scheme.casefold() not in {"http", "https"}:
        raise InvalidArgumentsError("post_url must use http or https")
    if parsed.username or parsed.password or port is not None or parsed.fragment:
        raise InvalidArgumentsError("post_url contains forbidden URL components")
    host = (parsed.hostname or "").casefold().rstrip(".")
    path = re.sub(r"/+", "/", parsed.path or "/").strip("/")

    if host in {"vk.com", "www.vk.com", "m.vk.com"}:
        query = parse_qs(parsed.query, keep_blank_values=False, strict_parsing=False)
        candidates: list[str] = []
        if path:
            candidates.append(path)
        for item in query.get("w", ()):  # VK feed/community links encode wall id here.
            candidates.append(item.strip("/"))
        matches = []
        for candidate in candidates:
            match = _VK_POST_RE.fullmatch(candidate)
            if match:
                identity = (int(match.group(1)), int(match.group(2)))
                if identity not in matches:
                    matches.append(identity)
        if len(matches) != 1:
            raise InvalidArgumentsError("post_url must identify exactly one VK wall post")
        owner_id, post_id = matches[0]
        key = f"wall{owner_id}_{post_id}"
        return SocialPostIdentity(
            platform="vk",
            canonical_url=f"https://vk.com/{key}",
            provider_key=key,
            vk_owner_id=owner_id,
            vk_post_id=post_id,
        )

    if host in {"t.me", "telegram.me", "www.telegram.me"}:
        if parsed.query and any(key not in {"single"} for key in parse_qs(parsed.query)):
            raise InvalidArgumentsError("post_url contains unsupported Telegram query fields")
        if path.startswith("s/"):
            path = path[2:]
        public_match = _TG_PUBLIC_POST_RE.fullmatch(path)
        if public_match:
            username = public_match.group(1)
            message_id = int(public_match.group(2))
            key = f"{username.casefold()}/{message_id}"
            return SocialPostIdentity(
                platform="telegram",
                canonical_url=f"https://t.me/{username}/{message_id}",
                provider_key=key,
            )
        private_match = _TG_PRIVATE_POST_RE.fullmatch(path)
        if private_match:
            chat_id = int(private_match.group(1))
            message_id = int(private_match.group(2))
            key = f"c/{chat_id}/{message_id}"
            return SocialPostIdentity(
                platform="telegram",
                canonical_url=f"https://t.me/{key}",
                provider_key=key,
            )
        raise InvalidArgumentsError("post_url must identify exactly one Telegram post")

    raise InvalidArgumentsError("post_url host is not an allowed social provider")


@dataclass(frozen=True, slots=True)
class SearchHit:
    document_id: str
    title: str
    url: str
    kind: str
    snippet: str
    metadata: Mapping[str, Any]
    score: float = 0.0

    def as_search_result(self) -> dict[str, str]:
        return {"id": self.document_id, "title": self.title, "url": self.url}


@dataclass(frozen=True, slots=True)
class FetchedDocument:
    document_id: str
    title: str
    text: str
    url: str
    metadata: Mapping[str, Any]

    def as_fetch_result(self) -> dict[str, Any]:
        return {
            "id": self.document_id,
            "title": self.title,
            "text": self.text,
            "url": self.url,
            "metadata": dict(self.metadata),
        }


def _clip(value: Any, limit: int = 4000) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, bytes):
        return {
            "bytes": len(value),
            "sha256": hashlib.sha256(value).hexdigest(),
        }
    if isinstance(value, str):
        value = _redact_text(value)
        return value if len(value) <= limit else value[:limit] + "…"
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for raw_key, nested in list(value.items())[:100]:
            key = str(raw_key)[:120]
            result[key] = "<redacted>" if _is_sensitive_key(key) else _clip(nested, limit)
        return result
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_clip(item, limit) for item in list(value)[:100]]
    return _clip(str(value), limit)


def redact_and_clip_untrusted(value: Any, *, limit: int = 4000) -> Any:
    """Apply the MCP output-boundary redaction to provider-derived data."""

    return _clip(value, limit)


def _decode_jsonish(value: Any, *, text_limit: int = 4000) -> Any:
    if not isinstance(value, str):
        return _clip(value, text_limit)
    stripped = value.strip()
    if stripped and stripped[0] in "[{":
        try:
            return _clip(json.loads(stripped), text_limit)
        except (json.JSONDecodeError, TypeError):
            pass
    return _clip(value, text_limit)


def _normalise_text(value: str) -> str:
    return " ".join((value or "").replace("\x00", " ").split())


def _snippet(value: str, query: str, *, limit: int = 280) -> str:
    text = _normalise_text(value)
    if len(text) <= limit:
        return text
    tokens = [item.casefold() for item in _WORD_RE.findall(query or "") if len(item) >= 2]
    lowered = text.casefold()
    offsets = [lowered.find(token) for token in tokens]
    offsets = [offset for offset in offsets if offset >= 0]
    center = min(offsets) if offsets else 0
    start = max(0, center - limit // 3)
    end = min(len(text), start + limit)
    prefix = "…" if start else ""
    suffix = "…" if end < len(text) else ""
    return prefix + text[start:end] + suffix


def _parse_date(value: str | None, *, name: str) -> str | None:
    if value is None or value == "":
        return None
    try:
        return date.fromisoformat(value).isoformat()
    except (TypeError, ValueError) as exc:
        raise InvalidArgumentsError(f"{name} must be YYYY-MM-DD") from exc


def _parse_timestamp(value: str | None, *, name: str) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise InvalidArgumentsError(f"{name} must be an ISO-8601 timestamp")
    raw = value.strip()
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidArgumentsError(f"{name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise InvalidArgumentsError(f"{name} must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _bounded_limit(value: Any, *, default: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(1, min(parsed, maximum))


class EventsEvidenceRepository:
    """Read-only projection over the event database and incident records."""

    def __init__(self, config: PrivateEventsMCPConfig) -> None:
        self.config = config
        self.db = ReadOnlySQLite(
            config.database_path,
            query_timeout_ms=config.query_timeout_ms,
            busy_timeout_ms=min(250, config.query_timeout_ms),
            max_rows=config.max_rows,
        )
        repository_ref = self._read_repository_ref()
        self.incidents = IncidentIndex(
            config.repository_root,
            repository_slug=config.repository_slug,
            repository_ref=repository_ref,
            cache_ttl_seconds=config.incident_index_ttl_seconds,
            scan_byte_limit=config.incident_scan_bytes,
            max_document_chars=config.max_document_chars,
        )

    def _read_repository_ref(self) -> str:
        try:
            value = Path(self.config.repository_sha_file).read_text(encoding="utf-8").strip()
        except OSError:
            return "main"
        if re.fullmatch(r"[0-9a-f]{40}", value):
            return value
        return "main"

    async def _columns(self, table: str, candidates: Iterable[str]) -> tuple[str, ...]:
        return await self.db.existing_columns(table, candidates)

    async def _safe_rows_by_id(
        self,
        table: str,
        *,
        id_column: str,
        id_value: Any,
        candidates: Sequence[str],
        order_by: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        snapshot = await self.db.schema()
        if not snapshot.has_table(table) or id_column not in snapshot.columns(table):
            return []
        columns = tuple(item for item in candidates if item in snapshot.columns(table))
        if not columns:
            return []
        select_sql = ", ".join(self.db.quote_identifier(item) for item in columns)
        sql = (
            f"SELECT {select_sql} FROM {self.db.quote_identifier(table)} "
            f"WHERE {self.db.quote_identifier(id_column)}=?"
        )
        if order_by and order_by in snapshot.columns(table):
            sql += f" ORDER BY {self.db.quote_identifier(order_by)} DESC"
        sql += " LIMIT ?"
        row_limit = min(limit or self.config.max_rows, self.config.max_rows)
        rows = await self.db.query(sql, (id_value, row_limit), max_rows=row_limit)
        return [self._normalise_row(row) for row in rows]

    async def _identity_decisions_for_event(self, event_id: int) -> list[dict[str, Any]]:
        snapshot = await self.db.schema()
        columns = snapshot.columns("event_identity_decision_log")
        selected = tuple(item for item in _IDENTITY_DECISION_COLUMNS if item in columns)
        predicates = [
            f"{self.db.quote_identifier(column)}=?"
            for column in ("event_id", "candidate_event_id")
            if column in columns
        ]
        if not selected or not predicates:
            return []
        select_sql = ", ".join(self.db.quote_identifier(item) for item in selected)
        params: list[Any] = [event_id] * len(predicates)
        order = "created_at" if "created_at" in columns else "id"
        sql = (
            f"SELECT {select_sql} FROM {self.db.quote_identifier('event_identity_decision_log')} "
            f"WHERE ({' OR '.join(predicates)}) ORDER BY {self.db.quote_identifier(order)} DESC LIMIT ?"
        )
        params.append(self.config.max_rows)
        rows = await self.db.query(sql, tuple(params), max_rows=self.config.max_rows)
        return [self._normalise_row(row) for row in rows]

    async def _vk_inbox_evidence_for_event(self, event_id: int) -> list[dict[str, Any]]:
        snapshot = await self.db.schema()
        inbox_columns = snapshot.columns("vk_inbox")
        selected = tuple(item for item in _VK_INBOX_COLUMNS if item in inbox_columns)
        if "id" not in selected:
            return []
        inbox_ids: set[int] = set()
        direct_rows: list[dict[str, Any]] = []
        if "imported_event_id" in inbox_columns:
            direct_rows = await self._safe_rows_by_id(
                "vk_inbox",
                id_column="imported_event_id",
                id_value=event_id,
                candidates=_VK_INBOX_COLUMNS,
                order_by="created_at",
            )
            inbox_ids.update(int(row["id"]) for row in direct_rows if row.get("id") is not None)
        mapping_rows = await self._safe_rows_by_id(
            "vk_inbox_import_event",
            id_column="event_id",
            id_value=event_id,
            candidates=_VK_INBOX_IMPORT_COLUMNS,
            order_by="created_at",
        )
        inbox_ids.update(
            int(row["inbox_id"]) for row in mapping_rows if row.get("inbox_id") is not None
        )
        result = {int(row["id"]): row for row in direct_rows if row.get("id") is not None}
        for inbox_id in sorted(inbox_ids):
            if inbox_id in result:
                continue
            rows = await self._safe_rows_by_id(
                "vk_inbox",
                id_column="id",
                id_value=inbox_id,
                candidates=_VK_INBOX_COLUMNS,
                limit=1,
            )
            if rows:
                result[inbox_id] = rows[0]
        return list(result.values())[: self.config.max_rows]

    @staticmethod
    def _normalise_row(row: Mapping[str, Any], *, text_limit: int = 8000) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in row.items():
            if _is_sensitive_key(key):
                result[key] = "<redacted>"
                continue
            result[key] = _decode_jsonish(value, text_limit=text_limit)
        return result

    @staticmethod
    def _event_url(row: Mapping[str, Any]) -> str:
        for column in _URL_COLUMNS:
            value = row.get(column)
            if isinstance(value, str) and value.startswith(("https://", "http://")):
                return value
        path = row.get("telegraph_path")
        if isinstance(path, str) and path.strip():
            return f"https://telegra.ph/{path.lstrip('/')}"
        return ""

    @staticmethod
    def _canonical_evidence_url(value: Any) -> str:
        if not isinstance(value, str) or not value.startswith(("https://", "http://")):
            return ""
        try:
            return canonicalize_social_post_url(value).canonical_url
        except InvalidArgumentsError:
            return _redact_text(value.strip())[:1000]

    def _build_evidence_links(
        self,
        event: Mapping[str, Any],
        sources: Sequence[Mapping[str, Any]],
        publications: Sequence[Mapping[str, Any]],
        vk_inbox_rows: Sequence[Mapping[str, Any]],
    ) -> dict[str, list[dict[str, Any]]]:
        buckets: dict[str, list[dict[str, Any]]] = {
            "original_sources": [],
            "context_sources": [],
            "unclassified_sources": [],
            "managed_publications": [],
            "inbox_import_sources": [],
            "derived_pages": [],
        }

        def add(
            bucket: str,
            raw_url: Any,
            *,
            relation: str,
            table: str,
            row_id: Any,
            column: str,
            source_role: Any = None,
        ) -> None:
            canonical = self._canonical_evidence_url(raw_url)
            if not canonical:
                return
            provenance = {"table": table, "row_id": row_id, "column": column}
            existing = next(
                (
                    item
                    for item in buckets[bucket]
                    if str(item.get("canonical_url") or "").casefold() == canonical.casefold()
                ),
                None,
            )
            if existing is not None:
                if provenance not in existing["provenance"]:
                    existing["provenance"].append(provenance)
                return
            entry = {
                "url": _redact_text(str(raw_url))[:1000],
                "canonical_url": canonical,
                "relation": relation,
                "provenance": [provenance],
                "contains_untrusted_external_content": True,
            }
            if source_role:
                entry["source_role"] = str(source_role)
            buckets[bucket].append(entry)

        for source in sources:
            relation = self._relation_for_source_role(source.get("source_role"))
            bucket = {
                "original_identity_source": "original_sources",
                "context_source": "context_sources",
            }.get(relation, "unclassified_sources")
            add(
                bucket,
                source.get("canonical_source_url") or source.get("source_url"),
                relation=relation,
                table="event_source",
                row_id=source.get("id"),
                column="canonical_source_url" if source.get("canonical_source_url") else "source_url",
                source_role=source.get("source_role") or "unclassified",
            )

        for publication in publications:
            for column in ("live_url", "stored_url"):
                add(
                    "managed_publications",
                    publication.get(column),
                    relation="managed_publication",
                    table="event_publication",
                    row_id=publication.get("id"),
                    column=column,
                )

        for column, bucket, relation in (
            ("tg_event_post_url", "managed_publications", "managed_publication"),
            ("vk_repost_url", "managed_publications", "managed_publication"),
            ("telegraph_url", "derived_pages", "derived_page"),
            ("ics_url", "derived_pages", "derived_page"),
        ):
            add(
                bucket,
                event.get(column),
                relation=relation,
                table="event",
                row_id=event.get("id"),
                column=column,
            )

        for column in ("source_post_url", "source_vk_post_url"):
            raw_url = event.get(column)
            canonical = self._canonical_evidence_url(raw_url)
            destination = "unclassified_sources"
            relation = "legacy_source"
            for bucket, known_relation in (
                ("managed_publications", "managed_publication"),
                ("original_sources", "original_identity_source"),
                ("context_sources", "context_source"),
            ):
                if any(
                    str(item.get("canonical_url") or "").casefold() == canonical.casefold()
                    for item in buckets[bucket]
                ):
                    destination, relation = bucket, known_relation
                    break
            add(
                destination,
                raw_url,
                relation=relation,
                table="event",
                row_id=event.get("id"),
                column=column,
            )

        for row in vk_inbox_rows:
            try:
                group_id = int(row.get("group_id"))
                post_id = int(row.get("post_id"))
            except (TypeError, ValueError):
                continue
            owner_type = str(row.get("owner_type") or "group").casefold()
            if owner_type in {"user", "person", "profile"}:
                owner_id = abs(group_id)
            else:
                owner_id = -abs(group_id)
            add(
                "inbox_import_sources",
                f"https://vk.com/wall{owner_id}_{post_id}",
                relation="inbox_import",
                table="vk_inbox",
                row_id=row.get("id"),
                column="group_id,post_id",
            )
        return buckets

    @staticmethod
    def _post_url_candidates(raw_url: str, identity: SocialPostIdentity) -> tuple[str, ...]:
        values = {
            raw_url.strip(),
            identity.canonical_url,
            identity.canonical_url + "/",
        }
        if identity.canonical_url.startswith("https://"):
            values.add("http://" + identity.canonical_url.removeprefix("https://"))
        if identity.platform == "telegram" and not identity.provider_key.startswith("c/"):
            values.add(f"https://t.me/s/{identity.provider_key}")
            values.add(f"https://telegram.me/{identity.provider_key}")
        return tuple(sorted(item for item in values if item))

    async def _rows_with_exact_url(
        self,
        table: str,
        *,
        url_columns: Sequence[str],
        selected_columns: Sequence[str],
        url_candidates: Sequence[str],
    ) -> list[dict[str, Any]]:
        snapshot = await self.db.schema()
        columns = snapshot.columns(table)
        available_urls = tuple(item for item in url_columns if item in columns)
        selected = tuple(item for item in selected_columns if item in columns)
        if not available_urls or not selected or not url_candidates:
            return []
        predicates: list[str] = []
        params: list[Any] = []
        for column in available_urls:
            predicates.append(
                "(" + " OR ".join(
                    f"{self.db.quote_identifier(column)} = ? COLLATE NOCASE"
                    for _ in url_candidates
                ) + ")"
            )
            params.extend(url_candidates)
        where = "(" + " OR ".join(predicates) + ")"
        count_rows = await self.db.query(
            f"SELECT COUNT(*) AS count FROM {self.db.quote_identifier(table)} WHERE {where}",
            tuple(params),
            max_rows=1,
        )
        count = int(count_rows[0]["count"]) if count_rows else 0
        if count > self.config.max_rows:
            raise QueryBudgetExceeded("post_url_match_budget_exceeded")
        select_sql = ", ".join(self.db.quote_identifier(item) for item in selected)
        rows = await self.db.query(
            f"SELECT {select_sql} FROM {self.db.quote_identifier(table)} "
            f"WHERE {where} LIMIT ?",
            (*params, self.config.max_rows),
            max_rows=self.config.max_rows,
        )
        return [self._normalise_row(row, text_limit=3000) for row in rows]

    @staticmethod
    def _relation_for_source_role(value: Any) -> str:
        role = str(value or "").casefold()
        if role == "identity_bearing":
            return "original_identity_source"
        if role == "context_only":
            return "context_source"
        return "unclassified_source"

    async def _post_url_event_relations(
        self, raw_url: str
    ) -> tuple[SocialPostIdentity, dict[int, list[dict[str, Any]]]]:
        identity = canonicalize_social_post_url(raw_url)
        url_candidates = self._post_url_candidates(raw_url, identity)
        relations: dict[int, list[dict[str, Any]]] = {}

        def add(event_id: Any, relation: str, table: str, row: Mapping[str, Any], column: str) -> None:
            try:
                parsed_event_id = int(event_id)
            except (TypeError, ValueError):
                return
            if parsed_event_id <= 0:
                return
            item = {
                "relation": relation,
                "table": table,
                "row_id": row.get("id") or row.get("inbox_id"),
                "column": column,
                "match_method": "canonical_url_exact",
            }
            if item not in relations.setdefault(parsed_event_id, []):
                relations[parsed_event_id].append(item)

        source_rows = await self._rows_with_exact_url(
            "event_source",
            url_columns=("canonical_source_url", "source_url"),
            selected_columns=("id", "event_id", "source_role", "canonical_source_url", "source_url"),
            url_candidates=url_candidates,
        )
        candidate_keys = {item.casefold() for item in url_candidates}
        for row in source_rows:
            matched_column = (
                "canonical_source_url"
                if str(row.get("canonical_source_url") or "").casefold() in candidate_keys
                else "source_url"
            )
            add(
                row.get("event_id"),
                self._relation_for_source_role(row.get("source_role")),
                "event_source",
                row,
                matched_column,
            )

        publication_rows = await self._rows_with_exact_url(
            "event_publication",
            url_columns=("live_url", "stored_url"),
            selected_columns=("id", "event_id", "live_url", "stored_url", "match_method"),
            url_candidates=url_candidates,
        )
        for row in publication_rows:
            matched_column = (
                "live_url"
                if str(row.get("live_url") or "").casefold() in candidate_keys
                else "stored_url"
            )
            add(row.get("event_id"), "managed_publication", "event_publication", row, matched_column)

        event_rows = await self._rows_with_exact_url(
            "event",
            url_columns=("tg_event_post_url", "vk_repost_url", "source_post_url", "source_vk_post_url"),
            selected_columns=("id", "tg_event_post_url", "vk_repost_url", "source_post_url", "source_vk_post_url"),
            url_candidates=url_candidates,
        )
        for row in event_rows:
            for column in ("tg_event_post_url", "vk_repost_url", "source_post_url", "source_vk_post_url"):
                if str(row.get(column) or "").casefold() not in candidate_keys:
                    continue
                relation = "managed_publication" if column in {"tg_event_post_url", "vk_repost_url"} else "legacy_source"
                # A legacy field can contain a managed projection.  Existing
                # publication/source rows, when available, are the authority.
                existing = relations.get(int(row["id"]), [])
                if any(item["relation"] == "managed_publication" for item in existing):
                    relation = "managed_publication"
                elif any(item["relation"] in {"original_identity_source", "context_source"} for item in existing):
                    relation = next(
                        item["relation"] for item in existing
                        if item["relation"] in {"original_identity_source", "context_source"}
                    )
                add(row.get("id"), relation, "event", row, column)

        if identity.platform == "vk":
            snapshot = await self.db.schema()
            inbox_columns = snapshot.columns("vk_inbox")
            if {"id", "group_id", "post_id"}.issubset(inbox_columns):
                owner_candidates = [identity.vk_owner_id]
                if identity.vk_owner_id is not None and identity.vk_owner_id < 0:
                    owner_candidates.append(abs(identity.vk_owner_id))
                owner_candidates = list(dict.fromkeys(owner_candidates))
                owner_predicate = " OR ".join(
                    f"{self.db.quote_identifier('group_id')}=?" for _ in owner_candidates
                )
                selected = tuple(item for item in _VK_INBOX_COLUMNS if item in inbox_columns)
                select_sql = ", ".join(self.db.quote_identifier(item) for item in selected)
                inbox_count_rows = await self.db.query(
                    f"SELECT COUNT(*) AS count FROM {self.db.quote_identifier('vk_inbox')} "
                    f"WHERE ({owner_predicate}) AND {self.db.quote_identifier('post_id')}=?",
                    (*owner_candidates, identity.vk_post_id),
                    max_rows=1,
                )
                if inbox_count_rows and int(inbox_count_rows[0]["count"]) > self.config.max_rows:
                    raise QueryBudgetExceeded("post_url_inbox_budget_exceeded")
                inbox_rows = await self.db.query(
                    f"SELECT {select_sql} FROM {self.db.quote_identifier('vk_inbox')} "
                    f"WHERE ({owner_predicate}) AND {self.db.quote_identifier('post_id')}=? LIMIT ?",
                    (*owner_candidates, identity.vk_post_id, self.config.max_rows),
                    max_rows=self.config.max_rows,
                )
                for raw_row in inbox_rows:
                    row = self._normalise_row(raw_row, text_limit=3000)
                    add(row.get("imported_event_id"), "inbox_import", "vk_inbox", row, "group_id,post_id")
                    mapping_columns = snapshot.columns("vk_inbox_import_event")
                    if {"inbox_id", "event_id"}.issubset(mapping_columns):
                        mapping_count_rows = await self.db.query(
                            f"SELECT COUNT(*) AS count FROM "
                            f"{self.db.quote_identifier('vk_inbox_import_event')} "
                            f"WHERE {self.db.quote_identifier('inbox_id')}=?",
                            (row.get("id"),),
                            max_rows=1,
                        )
                        if (
                            mapping_count_rows
                            and int(mapping_count_rows[0]["count"]) > self.config.max_rows
                        ):
                            raise QueryBudgetExceeded("post_url_event_budget_exceeded")
                        mapping_rows = await self._safe_rows_by_id(
                            "vk_inbox_import_event",
                            id_column="inbox_id",
                            id_value=row.get("id"),
                            candidates=_VK_INBOX_IMPORT_COLUMNS,
                        )
                        for mapping in mapping_rows:
                            add(mapping.get("event_id"), "inbox_import", "vk_inbox_import_event", mapping, "inbox_id")

        if len(relations) > self.config.max_rows:
            raise QueryBudgetExceeded("post_url_event_budget_exceeded")
        return identity, relations

    async def _search_events_by_post_url(self, post_url: str) -> list[SearchHit]:
        identity, relations = await self._post_url_event_relations(post_url)
        if not relations:
            return []
        snapshot = await self.db.schema()
        columns = snapshot.columns("event")
        selected = tuple(item for item in _EVENT_COLUMNS if item in columns)
        if "id" not in selected or "title" not in selected:
            return []
        event_ids = sorted(relations)
        placeholders = ",".join("?" for _ in event_ids)
        selected_sql = ", ".join(self.db.quote_identifier(item) for item in selected)
        rows = await self.db.query(
            f"SELECT {selected_sql} FROM {self.db.quote_identifier('event')} "
            f"WHERE {self.db.quote_identifier('id')} IN ({placeholders}) "
            f"ORDER BY {self.db.quote_identifier('id')} ASC LIMIT ?",
            (*event_ids, self.config.max_rows),
            max_rows=self.config.max_rows,
        )
        ambiguity = len(relations) > 1
        hits: list[SearchHit] = []
        for raw in rows:
            row = self._normalise_row(raw, text_limit=3000)
            event_id = int(row["id"])
            title = str(row.get("title") or f"Event {event_id}")
            evidence_relations = relations[event_id]
            metadata = {
                "post_url": identity.canonical_url,
                "post_platform": identity.platform,
                "match_method": "canonical_url_exact",
                "relations": evidence_relations,
                "ambiguous_multi_event": ambiguity,
                "match_count": len(relations),
                "date": row.get("date"),
                "time": row.get("time"),
                "city": row.get("city"),
                "location_name": row.get("location_name"),
                "contains_untrusted_external_content": True,
            }
            hits.append(
                SearchHit(
                    document_id=f"event:{event_id}",
                    title=title[:500],
                    url=self._event_url(row),
                    kind="event",
                    snippet="Exact stored evidence match for the supplied social post URL.",
                    metadata=metadata,
                    score=200.0,
                )
            )
        return hits

    async def search_events(
        self,
        *,
        query: str = "",
        date_from: str | None = None,
        date_to: str | None = None,
        city: str | None = None,
        event_type: str | None = None,
        lifecycle_status: str | None = None,
        include_past: bool = True,
        limit: int = 10,
        post_url: str | None = None,
    ) -> list[SearchHit]:
        limit = _bounded_limit(limit, default=10, maximum=self.config.max_rows)
        if post_url:
            incompatible = any(
                (
                    _normalise_text(query),
                    date_from,
                    date_to,
                    city,
                    event_type,
                    lifecycle_status,
                )
            ) or not include_past
            if incompatible:
                raise InvalidArgumentsError("post_url cannot be combined with event search filters")
            # Exact URL lookup intentionally ignores caller limit so every match
            # inside the repository-wide row budget is returned.
            return await self._search_events_by_post_url(post_url)
        date_from = _parse_date(date_from, name="date_from")
        date_to = _parse_date(date_to, name="date_to")
        if date_from and date_to and date_from > date_to:
            raise InvalidArgumentsError("date_from must not exceed date_to")
        snapshot = await self.db.schema()
        columns = snapshot.columns("event")
        if not columns:
            return []
        selected = tuple(item for item in _EVENT_COLUMNS if item in columns)
        if "id" not in selected or "title" not in selected:
            return []
        text_columns = tuple(
            item
            for item in (
                "title",
                "description",
                "short_description",
                "source_text",
                "search_digest",
                "festival",
                "location_name",
                "location_address",
                "city",
                "event_type",
            )
            if item in columns
        )
        where: list[str] = []
        params: list[Any] = []
        clean_query = _normalise_text(query)[:500]
        if clean_query and text_columns:
            pattern = f"%{clean_query}%"
            where.append(
                "(" + " OR ".join(
                    f"COALESCE({self.db.quote_identifier(column)}, '') LIKE ? COLLATE NOCASE"
                    for column in text_columns
                ) + ")"
            )
            params.extend([pattern] * len(text_columns))
        today = datetime.now(timezone.utc).date().isoformat()
        if not include_past and "date" in columns:
            where.append(f"{self.db.quote_identifier('date')} >= ?")
            params.append(today)
        if date_from and "date" in columns:
            where.append(f"COALESCE({self.db.quote_identifier('end_date') if 'end_date' in columns else self.db.quote_identifier('date')}, {self.db.quote_identifier('date')}) >= ?")
            params.append(date_from)
        if date_to and "date" in columns:
            where.append(f"{self.db.quote_identifier('date')} <= ?")
            params.append(date_to)
        for column, value in (
            ("city", city),
            ("event_type", event_type),
            ("lifecycle_status", lifecycle_status),
        ):
            clean = _normalise_text(value or "")[:200]
            if clean and column in columns:
                where.append(f"COALESCE({self.db.quote_identifier(column)}, '') = ? COLLATE NOCASE")
                params.append(clean)
        select_sql = ", ".join(self.db.quote_identifier(item) for item in selected)
        sql = f"SELECT {select_sql} FROM {self.db.quote_identifier('event')}"
        if where:
            sql += " WHERE " + " AND ".join(where)
        if "date" in columns:
            sql += (
                f" ORDER BY CASE WHEN {self.db.quote_identifier('date')} >= ? THEN 0 ELSE 1 END, "
                f"{self.db.quote_identifier('date')} ASC, {self.db.quote_identifier('id')} DESC"
            )
            params.append(today)
        else:
            sql += f" ORDER BY {self.db.quote_identifier('id')} DESC"
        sql += " LIMIT ?"
        params.append(limit)
        rows = await self.db.query(sql, tuple(params), max_rows=limit)
        hits: list[SearchHit] = []
        for raw in rows:
            row = self._normalise_row(raw, text_limit=3000)
            event_id = row.get("id")
            title = str(row.get("title") or f"Event {event_id}")
            body = " ".join(
                str(row.get(item) or "")
                for item in ("short_description", "description", "search_digest", "source_text")
            )
            metadata = {
                key: row.get(key)
                for key in (
                    "date",
                    "end_date",
                    "time",
                    "city",
                    "location_name",
                    "event_type",
                    "festival",
                    "lifecycle_status",
                )
                if key in row
            }
            metadata["contains_untrusted_external_content"] = True
            hits.append(
                SearchHit(
                    document_id=f"event:{event_id}",
                    title=title[:500],
                    url=self._event_url(row),
                    kind="event",
                    snippet=_snippet(body, clean_query),
                    metadata=metadata,
                    score=100.0,
                )
            )
        return hits

    async def get_event(self, event_id: int) -> FetchedDocument:
        try:
            event_id = int(event_id)
        except (TypeError, ValueError) as exc:
            raise InvalidArgumentsError("event_id must be an integer") from exc
        if event_id <= 0:
            raise InvalidArgumentsError("event_id must be positive")
        rows = await self._safe_rows_by_id(
            "event",
            id_column="id",
            id_value=event_id,
            candidates=_EVENT_COLUMNS,
            limit=1,
        )
        if not rows:
            raise NotFoundError("event_not_found")
        event = rows[0]
        sources, facts, jobs, posters, reviews, publications, identity_decisions, vk_inbox_rows = await asyncio.gather(
            self._safe_rows_by_id(
                "event_source",
                id_column="event_id",
                id_value=event_id,
                candidates=_SOURCE_COLUMNS,
                order_by="imported_at",
            ),
            self._safe_rows_by_id(
                "event_source_fact",
                id_column="event_id",
                id_value=event_id,
                candidates=_FACT_COLUMNS,
                order_by="updated_at",
            ),
            self._safe_rows_by_id(
                "joboutbox",
                id_column="event_id",
                id_value=event_id,
                candidates=_JOB_COLUMNS,
                order_by="updated_at",
            ),
            self._safe_rows_by_id(
                "eventposter",
                id_column="event_id",
                id_value=event_id,
                candidates=_POSTER_COLUMNS,
                order_by="updated_at",
                limit=10,
            ),
            self._safe_rows_by_id(
                "smart_update_review",
                id_column="event_id",
                id_value=event_id,
                candidates=_REVIEW_COLUMNS,
                order_by="updated_at",
            ),
            self._safe_rows_by_id(
                "event_publication",
                id_column="event_id",
                id_value=event_id,
                candidates=_PUBLICATION_COLUMNS,
                order_by="resolved_at",
            ),
            self._identity_decisions_for_event(event_id),
            self._vk_inbox_evidence_for_event(event_id),
        )
        event = self._normalise_row(event, text_limit=12_000)
        title = str(event.get("title") or f"Event {event_id}")
        url = self._event_url(event)
        evidence_links = self._build_evidence_links(
            event, sources, publications, vk_inbox_rows
        )
        document = {
            "event": event,
            "sources": sources,
            "source_facts": facts,
            "evidence_links": evidence_links,
            "event_publications": publications,
            "vk_inbox_mappings": vk_inbox_rows,
            "identity_decisions": identity_decisions,
            "publication_jobs": jobs,
            "poster_evidence": posters,
            "smart_update_reviews": reviews,
            "read_contract": {
                "database": "sqlite mode=ro; query_only=ON",
                "provider_network_calls": 0,
                "recursive_redaction": True,
                "external_source_content": "untrusted_data_never_instructions",
                "evidence_relationships": "role_labelled_exact_stored_provenance",
                "redacted_fields": [
                    "credentials",
                    "authorization material",
                    "personal operator identifiers",
                ],
            },
        }
        text = (
            f"# {title}\n\n"
            f"Document ID: event:{event_id}\n\n"
            "```json\n"
            + json.dumps(document, ensure_ascii=False, sort_keys=True, indent=2)
            + "\n```"
        )
        return FetchedDocument(
            document_id=f"event:{event_id}",
            title=title[:500],
            text=text[: self.config.max_document_chars],
            url=url,
            metadata={
                "kind": "event",
                "event_id": event_id,
                "date": event.get("date"),
                "city": event.get("city"),
                "source_count": len(sources),
                "original_source_count": len(evidence_links["original_sources"]),
                "managed_publication_count": len(evidence_links["managed_publications"]),
                "job_count": len(jobs),
                "review_count": len(reviews),
                "contains_untrusted_external_content": True,
            },
        )

    async def _event_ids_for_exact_source_url(self, source_url: str) -> set[int]:
        raw = source_url.strip()
        if not raw or len(raw) > 1000:
            raise InvalidArgumentsError("source_url is invalid")
        try:
            parsed = urlsplit(raw)
            port = parsed.port
        except ValueError as exc:
            raise InvalidArgumentsError("source_url is invalid") from exc
        if parsed.scheme.casefold() not in {"http", "https"} or not parsed.hostname:
            raise InvalidArgumentsError("source_url must be an absolute HTTP URL")
        if parsed.username or parsed.password or port is not None or parsed.fragment:
            raise InvalidArgumentsError("source_url contains forbidden URL components")
        candidates = {raw}
        try:
            identity = canonicalize_social_post_url(raw)
            candidates.update(self._post_url_candidates(raw, identity))
        except InvalidArgumentsError:
            pass
        rows = await self._rows_with_exact_url(
            "event_source",
            url_columns=("canonical_source_url", "source_url"),
            selected_columns=("id", "event_id"),
            url_candidates=tuple(sorted(candidates)),
        )
        result: set[int] = set()
        for row in rows:
            try:
                event_id = int(row.get("event_id"))
            except (TypeError, ValueError):
                continue
            if event_id > 0:
                result.add(event_id)
        return result

    async def search_incidents(
        self,
        query: str = "",
        *,
        event_id: int | None = None,
        source_url: str | None = None,
        post_url: str | None = None,
        run_id: str | None = None,
        job_id: str | None = None,
        error_class: str | None = None,
        time_from: str | None = None,
        time_to: str | None = None,
        limit: int = 10,
    ) -> list[SearchHit]:
        limit = _bounded_limit(limit, default=10, maximum=self.config.max_rows)
        clean_query = _normalise_text(query)[:1000]
        time_from = _parse_timestamp(time_from, name="time_from")
        time_to = _parse_timestamp(time_to, name="time_to")
        if time_from and time_to and time_from > time_to:
            raise InvalidArgumentsError("time_from must not exceed time_to")
        exact_filters = any(
            value not in (None, "")
            for value in (
                event_id, source_url, post_url, run_id, job_id,
                error_class, time_from, time_to,
            )
        )
        if not clean_query and not exact_filters:
            raise InvalidArgumentsError(
                "query or at least one structured incident filter is required"
            )

        event_ids: set[int] | None = None
        if event_id is not None:
            try:
                parsed_event_id = int(event_id)
            except (TypeError, ValueError) as exc:
                raise InvalidArgumentsError("event_id must be an integer") from exc
            if parsed_event_id <= 0:
                raise InvalidArgumentsError("event_id must be positive")
            event_ids = {parsed_event_id}
        if source_url:
            source_ids = await self._event_ids_for_exact_source_url(source_url)
            event_ids = source_ids if event_ids is None else event_ids.intersection(source_ids)
        if post_url:
            _, relations = await self._post_url_event_relations(post_url)
            post_ids = set(relations)
            event_ids = post_ids if event_ids is None else event_ids.intersection(post_ids)

        documents: list[IncidentDocument] = []
        if clean_query and not exact_filters:
            documents = await self.incidents.search(
                clean_query, limit=max(1, min(limit, 15))
            )
        runtime = await self._search_runtime_evidence(
            clean_query,
            event_ids=event_ids,
            run_id=run_id,
            job_id=job_id,
            error_class=error_class,
            time_from=time_from,
            time_to=time_to,
            failed_only=not exact_filters,
            limit=limit,
        )
        hits: list[SearchHit] = []
        for document in documents:
            safe_title = _redact_text(document.title)
            safe_text = _redact_text(document.text)
            safe_metadata = _clip(document.metadata)
            hits.append(
                SearchHit(
                    document_id=document.document_id,
                    title=safe_title,
                    url=document.url,
                    kind="incident_report",
                    snippet=_snippet(safe_text, clean_query),
                    metadata={
                        "path": document.relative_path,
                        "fingerprint": document.fingerprint,
                        **(safe_metadata if isinstance(safe_metadata, Mapping) else {}),
                    },
                    score=150.0,
                )
            )
        hits.extend(runtime)
        hits.sort(key=lambda item: item.score, reverse=True)
        return hits[:limit]

    @staticmethod
    def _row_error_classes(value: Any) -> set[str]:
        result: set[str] = set()

        def visit(item: Any) -> None:
            if isinstance(item, Mapping):
                for key, nested in item.items():
                    if _normalise_key(str(key)) in {
                        "error_class", "exception_class", "type"
                    }:
                        if isinstance(nested, str) and nested.strip():
                            result.add(nested.strip().casefold())
                    visit(nested)
            elif isinstance(item, (list, tuple)):
                for nested in item:
                    visit(nested)
            elif isinstance(item, str):
                for match in re.findall(
                    r"\b[A-Za-z_][A-Za-z0-9_.]*(?:Error|Exception)\b", item
                ):
                    result.add(match.casefold())

        visit(value)
        return result

    async def _search_runtime_evidence(
        self,
        query: str,
        *,
        event_ids: set[int] | None = None,
        run_id: str | None = None,
        job_id: str | None = None,
        error_class: str | None = None,
        time_from: str | None = None,
        time_to: str | None = None,
        failed_only: bool = False,
        limit: int,
    ) -> list[SearchHit]:
        snapshot = await self.db.schema()
        hits: list[SearchHit] = []
        clean = _normalise_text(query)[:400]
        for table, prefix, candidates in (
            ("ops_run", "run", _RUN_COLUMNS),
            ("joboutbox", "job", _JOB_COLUMNS),
        ):
            columns = snapshot.columns(table)
            if not columns:
                continue
            requested_record_id = run_id if prefix == "run" else job_id
            if prefix == "run" and job_id and not run_id:
                continue
            if prefix == "job" and run_id and not job_id:
                continue
            identity_column = (
                "id" if "id" in columns else
                ("run_id" if "run_id" in columns else "")
            )
            if not identity_column:
                continue
            selected = tuple(item for item in candidates if item in columns)
            if not selected:
                continue
            error_columns = tuple(
                item
                for item in (
                    "error", "error_json", "last_error", "result", "result_json",
                    "last_result", "details", "details_json", "operation", "kind",
                    "name", "task", "status",
                )
                if item in columns
            )
            where: list[str] = []
            params: list[Any] = []
            if requested_record_id:
                record_text = str(requested_record_id)
                if not re.fullmatch(r"[0-9A-Za-z_.:-]{1,160}", record_text):
                    raise InvalidArgumentsError(f"{prefix}_id is invalid")
                id_predicates = [f"{self.db.quote_identifier(identity_column)}=?"]
                params.append(record_text)
                if identity_column != "run_id" and "run_id" in columns:
                    id_predicates.append(f"{self.db.quote_identifier('run_id')}=?")
                    params.append(record_text)
                where.append("(" + " OR ".join(id_predicates) + ")")
            if event_ids is not None:
                if not event_ids:
                    continue
                event_columns = (
                    ["event_id"] if "event_id" in columns else
                    (["subject_id"] if "subject_id" in columns else [])
                )
                if not event_columns:
                    continue
                event_predicates: list[str] = []
                ordered_ids = sorted(event_ids)
                for column in event_columns:
                    placeholders = ",".join("?" for _ in ordered_ids)
                    event_predicates.append(
                        f"{self.db.quote_identifier(column)} IN ({placeholders})"
                    )
                    params.extend(ordered_ids)
                where.append("(" + " OR ".join(event_predicates) + ")")
            if failed_only and "status" in columns:
                where.append(
                    f"LOWER(COALESCE({self.db.quote_identifier('status')}, '')) IN "
                    "('error','failed','failure','review_required','blocked','paused')"
                )
            order = next(
                (
                    item for item in (
                        "updated_at", "finished_at", "created_at", "started_at", "id"
                    ) if item in columns
                ),
                identity_column,
            )
            if time_from:
                where.append(f"{self.db.quote_identifier(order)} >= ?")
                params.append(time_from)
            if time_to:
                where.append(f"{self.db.quote_identifier(order)} <= ?")
                params.append(time_to)
            native_error_class_filter = False
            if error_class and "error_class" in columns:
                where.append(
                    f"{self.db.quote_identifier('error_class')} = ? COLLATE NOCASE"
                )
                params.append(error_class.strip())
                native_error_class_filter = True
            elif error_class and not where and not clean:
                # Schemas without a dedicated error_class column get a bounded
                # recent-row scan followed by exact decoded-class comparison.
                where.append("1=1")
            if clean and error_columns:
                pattern = f"%{clean}%"
                where.append(
                    "(" + " OR ".join(
                        f"COALESCE({self.db.quote_identifier(column)}, '') "
                        "LIKE ? COLLATE NOCASE"
                        for column in error_columns
                    ) + ")"
                )
                params.extend([pattern] * len(error_columns))
            elif not where and error_columns:
                nonempty_error_columns = tuple(
                    column for column in error_columns if "error" in column
                )
                if nonempty_error_columns:
                    where.append(
                        "(" + " OR ".join(
                            f"NULLIF(TRIM(COALESCE({self.db.quote_identifier(column)}, '')), '') "
                            "IS NOT NULL"
                            for column in nonempty_error_columns
                        ) + ")"
                    )
            if not where:
                continue
            selected_sql = ", ".join(
                self.db.quote_identifier(item) for item in selected
            )
            sql = (
                f"SELECT {selected_sql} FROM {self.db.quote_identifier(table)} WHERE "
                + " AND ".join(where)
                + f" ORDER BY {self.db.quote_identifier(order)} DESC LIMIT ?"
            )
            params.append(min(limit, self.config.max_rows))
            rows = await self.db.query(sql, tuple(params), max_rows=limit)
            for raw in rows:
                row = self._normalise_row(raw, text_limit=3000)
                if error_class and not native_error_class_filter:
                    if error_class.strip().casefold() not in self._row_error_classes(row):
                        continue
                record_id = row.get(identity_column)
                label = (
                    row.get("operation") or row.get("kind") or row.get("name")
                    or row.get("task") or table
                )
                status = row.get("status") or "runtime evidence"
                title = f"{label}: {status} ({prefix} {record_id})"
                body = json.dumps(row, ensure_ascii=False, sort_keys=True)
                hits.append(
                    SearchHit(
                        document_id=f"{prefix}:{record_id}",
                        title=title[:500],
                        url=f"{self.config.resource}#{prefix}:{record_id}",
                        kind=f"runtime_{prefix}",
                        snippet=_snippet(body, clean),
                        metadata={
                            "table": table,
                            "status": status,
                            "event_id": row.get("event_id"),
                            "query_contract": "bounded_parameterized_database_evidence",
                            "runtime_file_mirror": "not_integrated_use_fixed_mirror_adapter",
                            "error_class_filter": (
                                "native_exact" if native_error_class_filter else
                                "bounded_decoded_exact" if error_class else "not_requested"
                            ),
                        },
                        score=90.0 if not failed_only else 80.0,
                    )
                )
        return hits[:limit]

    async def _search_runtime_failures(
        self, query: str, *, limit: int
    ) -> list[SearchHit]:
        return await self._search_runtime_evidence(
            query, failed_only=True, limit=limit
        )

    async def get_incident(self, document_id: str) -> FetchedDocument:
        document = await self.incidents.get(document_id)
        if document is not None:
            safe_metadata = _clip(document.metadata)
            return FetchedDocument(
                document_id=document.document_id,
                title=_redact_text(document.title),
                text=_redact_text(document.text),
                url=document.url,
                metadata={
                    "kind": "incident_report",
                    "path": document.relative_path,
                    "sha256": document.fingerprint,
                    **(safe_metadata if isinstance(safe_metadata, Mapping) else {}),
                },
            )
        if document_id.startswith("run:"):
            return await self._get_runtime_record("ops_run", "run", document_id[4:])
        if document_id.startswith("job:"):
            return await self._get_runtime_record("joboutbox", "job", document_id[4:])
        raise NotFoundError("incident_not_found")

    async def _get_runtime_record(self, table: str, prefix: str, record_id: str) -> FetchedDocument:
        if not re.fullmatch(r"[0-9A-Za-z_.:-]{1,160}", record_id):
            raise InvalidArgumentsError("runtime record id is invalid")
        snapshot = await self.db.schema()
        columns = snapshot.columns(table)
        if not columns:
            raise NotFoundError("runtime_record_not_found")
        id_column = "id" if "id" in columns else ("run_id" if "run_id" in columns else "")
        if not id_column:
            raise NotFoundError("runtime_record_not_found")
        candidates = _RUN_COLUMNS if table == "ops_run" else _JOB_COLUMNS
        rows = await self._safe_rows_by_id(
            table,
            id_column=id_column,
            id_value=record_id,
            candidates=candidates,
            limit=1,
        )
        if not rows and id_column == "id" and "run_id" in columns:
            rows = await self._safe_rows_by_id(
                table,
                id_column="run_id",
                id_value=record_id,
                candidates=candidates,
                limit=1,
            )
        if not rows:
            raise NotFoundError("runtime_record_not_found")
        row = rows[0]
        title = str(row.get("operation") or row.get("kind") or row.get("task") or f"{prefix} {record_id}")
        text = (
            f"# {title}\n\n"
            f"Document ID: {prefix}:{record_id}\n\n"
            "```json\n"
            + json.dumps(row, ensure_ascii=False, sort_keys=True, indent=2)
            + "\n```"
        )
        return FetchedDocument(
            document_id=f"{prefix}:{record_id}",
            title=title,
            text=text[: self.config.max_document_chars],
            url=f"{self.config.resource}#{prefix}:{record_id}",
            metadata={"kind": f"runtime_{prefix}", "table": table, "status": row.get("status")},
        )

    async def global_search(
        self,
        query: str,
        *,
        kinds: Sequence[str] | None = None,
        limit: int = 10,
    ) -> list[SearchHit]:
        limit = _bounded_limit(limit, default=10, maximum=self.config.max_rows)
        selected = {item.casefold() for item in (kinds or ("events", "incidents", "operations"))}
        tasks: list[asyncio.Future | asyncio.Task | Any] = []
        if "events" in selected:
            tasks.append(self.search_events(query=query, limit=limit))
        if "incidents" in selected or "operations" in selected:
            tasks.append(self.search_incidents(query, limit=limit))
        if not tasks:
            raise InvalidArgumentsError("kinds must contain events, incidents, or operations")
        groups = await asyncio.gather(*tasks)
        merged: list[SearchHit] = []
        seen: set[str] = set()
        for group in groups:
            for hit in group:
                if hit.document_id in seen:
                    continue
                seen.add(hit.document_id)
                merged.append(hit)
        merged.sort(key=lambda item: item.score, reverse=True)
        return merged[:limit]

    async def fetch(self, document_id: str) -> FetchedDocument:
        if not isinstance(document_id, str) or len(document_id) > 220:
            raise InvalidArgumentsError("document id is invalid")
        if document_id.startswith("event:"):
            raw_event_id = document_id.split(":", 1)[1]
            if not raw_event_id.isdigit():
                raise InvalidArgumentsError("event document id is invalid")
            return await self.get_event(int(raw_event_id))
        return await self.get_incident(document_id)

    async def operations_snapshot(self) -> dict[str, Any]:
        snapshot = await self.db.schema()
        today = datetime.now(timezone.utc).date().isoformat()

        async def _count(table: str, where: str = "", params: Sequence[Any] = ()) -> int | None:
            if not snapshot.has_table(table):
                return None
            sql = f"SELECT COUNT(*) AS count FROM {self.db.quote_identifier(table)}"
            if where:
                sql += " WHERE " + where
            rows = await self.db.query(sql, params, max_rows=1)
            return int(rows[0]["count"]) if rows else 0

        counts: dict[str, Any] = {
            "events_total": await _count("event"),
            "events_upcoming": await _count(
                "event",
                f"{self.db.quote_identifier('date')} >= ?" if "date" in snapshot.columns("event") else "",
                (today,) if "date" in snapshot.columns("event") else (),
            ),
            "incident_documents": len(await self.incidents.documents()),
        }
        for table, key in (
            ("event_source", "event_sources"),
            ("event_source_fact", "source_facts"),
            ("smart_update_review", "smart_update_reviews"),
            ("telegram_scanned_message", "telegram_scanned_messages"),
            ("vk_inbox", "vk_inbox_rows"),
            ("ops_run", "ops_runs"),
            ("joboutbox", "jobs"),
        ):
            count = await _count(table)
            if count is not None:
                counts[key] = count

        status_counts: dict[str, Any] = {}
        for table, key in (
            ("joboutbox", "joboutbox"),
            ("ops_run", "ops_run"),
            ("vk_inbox", "vk_inbox"),
            ("telegram_monitoring_on_demand_queue", "telegram_on_demand"),
            ("smart_update_review", "smart_update_review"),
        ):
            columns = snapshot.columns(table)
            if "status" not in columns:
                continue
            rows = await self.db.query(
                f"SELECT COALESCE({self.db.quote_identifier('status')}, '<null>') AS status, "
                f"COUNT(*) AS count FROM {self.db.quote_identifier(table)} "
                f"GROUP BY {self.db.quote_identifier('status')} ORDER BY count DESC LIMIT ?",
                (self.config.max_rows,),
                max_rows=self.config.max_rows,
            )
            status_counts[key] = {str(row["status"]): int(row["count"]) for row in rows}

        recent_failures = [
            {
                "id": hit.document_id,
                "title": hit.title,
                "snippet": hit.snippet,
                "metadata": dict(hit.metadata),
            }
            for hit in await self._search_runtime_failures("", limit=10)
        ]
        try:
            quick_check = await self.db.quick_check()
        except (ReadOnlySQLiteError, DatabaseUnavailableError, QueryBudgetExceeded) as exc:
            quick_check = f"unavailable:{type(exc).__name__}"
        try:
            database_bytes = os.path.getsize(self.config.database_path)
        except OSError:
            database_bytes = None
        return {
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "database": {
                "path_basename": Path(self.config.database_path).name,
                "bytes": database_bytes,
                "quick_check": quick_check,
                "mode": "read_only",
                "query_timeout_ms": self.config.query_timeout_ms,
            },
            "counts": counts,
            "status_counts": status_counts,
            "recent_failures": recent_failures,
            "repository": {
                "slug": self.config.repository_slug,
                "ref": self._read_repository_ref(),
            },
            "network": {"provider_calls": 0, "media_transferred": False},
        }
