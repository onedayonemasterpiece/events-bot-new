from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from importlib import import_module
from typing import Any, Awaitable, Callable, Iterable

from sqlalchemy import func
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from sqlmodel import select

from db import Database
from models import OcrUsage as OcrUsageModel, PosterOcrCache
import vision_test.ocr
from vision_test.ocr import OcrResult, OcrUsage, run_ocr

DAILY_TOKEN_LIMIT = 10_000_000


logger = logging.getLogger(__name__)


def _google_fallback_enabled() -> bool:
    raw = os.getenv("POSTER_OCR_GOOGLE_FALLBACK_ENABLED", "1") or "1"
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _image_mime(data: bytes) -> str:
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        return "image/webp"
    return "image/jpeg"


def _parse_ocr_json(raw: str) -> tuple[str, str]:
    value = (raw or "").strip()
    if value.startswith("```"):
        lines = value.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        value = "\n".join(lines).strip()
    payload = json.loads(value)
    if not isinstance(payload, dict):
        raise ValueError("Google OCR fallback returned a non-object")
    text = payload.get("poster_ocr_text")
    title = payload.get("ocr_title")
    if not isinstance(text, str) or not isinstance(title, str):
        raise ValueError("Google OCR fallback omitted string OCR fields")
    return text.strip(), title.strip()


async def _run_google_media_evidence(
    data: bytes,
    *,
    detail: str,
    mime_type: str,
) -> OcrResult:
    """Use one separately limited multimodal provider for visual evidence.

    The fallback stays inside the same source-row invocation.  It is not a
    background retry and therefore cannot strand an event-bearing poster in a
    queue that nobody drains.
    """

    from google_ai import GoogleAIClient, SecretsProvider
    from google_ai.limiter_supabase import build_google_ai_limiter_supabase_client
    from main import get_supabase_client

    model = (
        os.getenv("POSTER_OCR_GOOGLE_FALLBACK_MODEL")
        or "gemini-3.1-flash-lite"
    ).strip()
    key_pool = [
        item.strip()
        for item in (
            os.getenv("POSTER_OCR_GOOGLE_FALLBACK_KEY_ENVS")
            or "GOOGLE_API_KEY5,GOOGLE_API_KEY6"
        ).split(",")
        if item.strip()
    ]
    client = GoogleAIClient(
        supabase_client=build_google_ai_limiter_supabase_client(
            fallback_factory=get_supabase_client
        ),
        secrets_provider=SecretsProvider(),
        consumer="poster_ocr_fallback",
        account_name="poster-ocr-fallback",
        default_env_var_name=(key_pool[0] if key_pool else "GOOGLE_API_KEY5"),
        reserve_key_envs=key_pool,
        reserve_overflow_key_envs=[],
    )
    client.allow_reserve_fallback = False
    client.allow_local_limiter_fallback = False
    client.allow_local_limiter_on_reserve_error = False
    client.fallback_models = []
    client.max_retries = 1
    client.hard_single_provider_attempt = True
    client.provider_timeout_seconds = float(
        os.getenv("POSTER_OCR_GOOGLE_FALLBACK_TIMEOUT_SEC", "45") or "45"
    )
    schema = {
        "type": "object",
        "properties": {
            "poster_ocr_text": {"type": "string"},
            "ocr_title": {"type": "string"},
        },
        "required": ["poster_ocr_text", "ocr_title"],
        "additionalProperties": False,
    }
    media_instruction = (
        "Распознай весь видимый текст."
        if mime_type.startswith("image/")
        else (
            "Проанализируй видео целиком: распознай видимый и произнесённый текст, "
            "даты, время, место и явно показанные сведения о событии."
        )
    )
    raw, usage = await client.generate_content_async(
        model=model,
        prompt=[
            {
                "inline_data": {
                    "mime_type": mime_type,
                    "data": data,
                }
            },
            (
                f"{media_instruction} Верни только JSON: "
                '{"poster_ocr_text":"...","ocr_title":"..."}. '
                "ocr_title — крупнейший смысловой заголовок; если его нет, "
                f"верни пустую строку. detail={detail}."
            ),
        ],
        generation_config={
            "temperature": 0,
            "response_mime_type": "application/json",
            "response_json_schema": schema,
        },
        max_output_tokens=2048,
        allow_model_fallback=False,
        max_provider_attempts=1,
    )
    text, title = _parse_ocr_json(raw)
    return OcrResult(
        text=text,
        title=title,
        usage=OcrUsage(
            prompt_tokens=int(usage.input_tokens or 0),
            completion_tokens=int(usage.output_tokens or 0),
            total_tokens=int(usage.total_tokens or 0),
        ),
        request_id=usage.provider_response_id or usage.provider_request_id,
        provider_model=model,
    )


async def _run_google_ocr_fallback(data: bytes, *, detail: str) -> OcrResult:
    """Use Google after the bounded primary image OCR path is unavailable."""

    return await _run_google_media_evidence(
        data,
        detail=detail,
        mime_type=_image_mime(data),
    )


async def recognize_video_evidence(data: bytes, *, detail: str = "video") -> OcrResult:
    """Extract event-bearing evidence from one short inline MP4, once."""

    return await _run_google_media_evidence(
        data,
        detail=detail,
        mime_type="video/mp4",
    )


class PosterOcrLimitExceededError(RuntimeError):
    """Raised when the daily OCR token limit has been exhausted."""

    def __init__(
        self,
        message: str,
        *,
        spent_tokens: int,
        remaining: int,
        results: Iterable[PosterOcrCache] | None = None,
    ) -> None:
        super().__init__(message)
        self.spent_tokens = spent_tokens
        self.remaining = remaining
        self.results = list(results) if results is not None else []


def _today_key() -> str:
    return datetime.now(timezone.utc).date().isoformat()


_HTTP_CONFIGURED = False
_USAGE_LOGGER_CONFIGURED = False
_LOG_TOKEN_USAGE: Callable[..., Awaitable[None]] | None = None
_BOT_CODE: str | None = None


def _ensure_http() -> None:
    global _HTTP_CONFIGURED
    if _HTTP_CONFIGURED:
        return
    main_mod = import_module("main")
    session = main_mod.get_http_session()
    semaphore = main_mod.HTTP_SEMAPHORE
    vision_test.ocr.configure_http(session=session, semaphore=semaphore)
    _HTTP_CONFIGURED = True


def _ensure_usage_logger() -> None:
    global _USAGE_LOGGER_CONFIGURED, _LOG_TOKEN_USAGE, _BOT_CODE
    if _USAGE_LOGGER_CONFIGURED:
        return
    main_mod = import_module("main")
    _LOG_TOKEN_USAGE = getattr(main_mod, "log_token_usage")
    _BOT_CODE = getattr(main_mod, "BOT_CODE", None)
    _USAGE_LOGGER_CONFIGURED = True


def _ensure_bytes(item: Any) -> bytes:
    if isinstance(item, (bytes, bytearray, memoryview)):
        return bytes(item)
    if isinstance(item, tuple) and item:
        return bytes(item[0])
    if isinstance(item, dict) and "data" in item:
        return bytes(item["data"])
    data = getattr(item, "data", None)
    if data is None:
        raise TypeError("poster OCR item must provide image bytes")
    return bytes(data)


async def recognize_posters(
    db: Database,
    items: Iterable[Any],
    detail: str = "auto",
    *,
    count_usage: bool = True,
    log_context: dict[str, Any] | None = None,
) -> tuple[list[PosterOcrCache], int, int]:
    log_extra = dict(log_context) if log_context else None
    payloads: list[tuple[bytes, str]] = []
    for item in items:
        data = _ensure_bytes(item)
        digest = hashlib.sha256(data).hexdigest()
        payloads.append((data, digest))

    _ensure_http()

    model = os.getenv("POSTER_OCR_MODEL", "gpt-4o-mini")
    total_items = len(payloads)
    logger.info(
        "poster_ocr.start items=%d detail=%s model=%s count_usage=%s",
        total_items,
        detail,
        model,
        count_usage,
        extra=log_extra,
    )

    if not payloads:
        async with db.get_session() as session:
            today = _today_key()
            usage_row = await session.get(OcrUsageModel, today)
        remaining = DAILY_TOKEN_LIMIT - (usage_row.spent_tokens if usage_row else 0)
        remaining = max(0, remaining)
        logger.info(
            "poster_ocr.stats cache_hits=%d new_entries=%d blocked_uncached=%d spent_tokens=%d charged_tokens=%d total_new_tokens=%d remaining=%d",
            0,
            0,
            0,
            0,
            0,
            0,
            remaining,
            extra=log_extra,
        )
        return [], 0, remaining

    async with db.get_session() as session:
        hashes = [digest for _, digest in payloads]
        cache_map: dict[tuple[str, str, str], PosterOcrCache] = {}
        if hashes:
            result = await session.execute(
                select(PosterOcrCache).where(
                    PosterOcrCache.hash.in_(hashes),
                    PosterOcrCache.detail == detail,
                    PosterOcrCache.model == model,
                )
            )
            for row in result.scalars():
                cache_map[(row.hash, row.detail, row.model)] = row

        results: list[PosterOcrCache] = []
        result_keys: list[tuple[str, str, str]] = []
        total_new_tokens = 0
        cache_hits = 0
        blocked_uncached_count = 0
        entries_to_upsert: list[dict[str, Any]] = []
        today = _today_key()
        usage_row = await session.get(OcrUsageModel, today)
        spent_before = usage_row.spent_tokens if usage_row else 0
        limit_remaining = DAILY_TOKEN_LIMIT - spent_before
        block_new_requests = count_usage and limit_remaining <= 0
        encountered_uncached_after_limit = False
        failed_uncached_count = 0

        for data, digest in payloads:
            cache_key = (digest, detail, model)
            cached = cache_map.get(cache_key)
            if cached:
                results.append(cached)
                result_keys.append(cache_key)
                cache_hits += 1
                continue

            primary_error: Exception | None = None
            if block_new_requests:
                primary_error = PosterOcrLimitExceededError(
                    "primary poster OCR daily token limit exhausted",
                    spent_tokens=spent_before,
                    remaining=0,
                )
                if not _google_fallback_enabled():
                    encountered_uncached_after_limit = True
                    blocked_uncached_count += 1
                    continue
            try:
                if primary_error is not None:
                    raise primary_error
                ocr_result = await run_ocr(data, model=model, detail=detail)
            except Exception as primary_exc:
                if _google_fallback_enabled():
                    try:
                        ocr_result = await _run_google_ocr_fallback(data, detail=detail)
                        logger.warning(
                            "poster_ocr.fallback_recovered hash=%s primary_model=%s fallback_model=%s",
                            digest,
                            model,
                            ocr_result.provider_model,
                            extra=log_extra,
                        )
                    except Exception as fallback_exc:
                        # Preserve both provider failures in the durable log;
                        # the caller will keep the evidence manifest incomplete
                        # rather than silently declaring a no-event.
                        failed_uncached_count += 1
                        if block_new_requests:
                            encountered_uncached_after_limit = True
                            blocked_uncached_count += 1
                        logger.error(
                            "poster_ocr.image_failed hash=%s primary_error=%s fallback_error=%s",
                            digest,
                            primary_exc,
                            fallback_exc,
                            extra=log_extra,
                            exc_info=True,
                        )
                        continue
                else:
                    failed_uncached_count += 1
                    logger.error(
                        "poster_ocr.image_failed hash=%s error=%s",
                        digest,
                        primary_exc,
                        extra=log_extra,
                        exc_info=True,
                    )
                    continue
            if ocr_result is None:  # pragma: no cover - defensive
                # One persistently unreadable/unavailable image must not erase
                # successful OCR evidence from its siblings.  Keep processing
                # the bounded gallery; the caller derives an incomplete
                # manifest from the missing result count and cannot falsely
                # terminalize the carrier as a proved no-event.
                failed_uncached_count += 1
                logger.error(
                    "poster_ocr.image_failed hash=%s error=%s",
                    digest,
                    "provider returned no result",
                    extra=log_extra,
                )
                continue
            logger.info(
                "poster_ocr.llm_result hash=%s ocr_title=%r",
                digest,
                (ocr_result.title or "")[:120],
                extra=log_extra,
            )
            usage = ocr_result.usage
            entry = cache_map.get(cache_key)
            if entry is None:
                entry = await session.get(PosterOcrCache, cache_key)
            token_counts = {
                "prompt_tokens": int(getattr(usage, "prompt_tokens", 0) or 0),
                "completion_tokens": int(getattr(usage, "completion_tokens", 0) or 0),
                "total_tokens": int(getattr(usage, "total_tokens", 0) or 0),
            }
            if count_usage:
                _ensure_usage_logger()
                if _LOG_TOKEN_USAGE is not None and _BOT_CODE is not None:
                    usage_payload = dict(token_counts)
                    meta = {
                        "source": "poster_ocr",
                        "detail": detail,
                        "hash": digest,
                        "bytes": len(data),
                    }
                    try:
                        await _LOG_TOKEN_USAGE(
                            _BOT_CODE,
                            ocr_result.provider_model or model,
                            usage_payload,
                            endpoint=(
                                "google_ai.generate_content"
                                if ocr_result.provider_model
                                else "chat.completions"
                            ),
                            request_id=ocr_result.request_id,
                            meta=meta,
                        )
                    except Exception:  # pragma: no cover - defensive logging
                        logger.exception("poster_ocr.log_token_usage_failed")
            created_at = datetime.now(timezone.utc)
            if entry is None:
                entry = PosterOcrCache(
                    hash=digest,
                    detail=detail,
                    model=model,
                    text=ocr_result.text,
                    title=ocr_result.title,
                    created_at=created_at,
                    **token_counts,
                )
            else:
                entry = PosterOcrCache(
                    hash=entry.hash,
                    detail=entry.detail,
                    model=entry.model,
                    text=ocr_result.text,
                    prompt_tokens=token_counts["prompt_tokens"],
                    completion_tokens=token_counts["completion_tokens"],
                    total_tokens=token_counts["total_tokens"],
                    created_at=created_at,
                )
            entries_to_upsert.append(
                {
                    "hash": entry.hash,
                    "detail": entry.detail,
                    "model": entry.model,
                    "text": entry.text,
                    "title": entry.title,
                    "prompt_tokens": entry.prompt_tokens,
                    "completion_tokens": entry.completion_tokens,
                    "total_tokens": entry.total_tokens,
                    "created_at": entry.created_at,
                }
            )
            cache_map[cache_key] = entry
            results.append(entry)
            result_keys.append(cache_key)
            if count_usage:
                total_new_tokens += entry.total_tokens
                limit_remaining = DAILY_TOKEN_LIMIT - (spent_before + total_new_tokens)
                if limit_remaining <= 0:
                    block_new_requests = True

        spent_after = spent_before
        charged_amount = 0
        if entries_to_upsert:
            if count_usage and total_new_tokens:
                allowed_remaining = max(0, DAILY_TOKEN_LIMIT - spent_before)
                charged_amount = min(total_new_tokens, allowed_remaining)
                if charged_amount:
                    usage_table = OcrUsageModel.__table__
                    usage_insert = sqlite_insert(usage_table).values(
                        date=today, spent_tokens=charged_amount
                    )
                    usage_stmt = usage_insert.on_conflict_do_update(
                        index_elements=[usage_table.c.date],
                        set_={
                            "spent_tokens": func.min(
                                DAILY_TOKEN_LIMIT,
                                usage_table.c.spent_tokens
                                + usage_insert.excluded.spent_tokens,
                            )
                        },
                    )
                    await session.execute(usage_stmt)
            cache_table = PosterOcrCache.__table__
            insert_stmt = sqlite_insert(cache_table).values(entries_to_upsert)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=[
                    cache_table.c.hash,
                    cache_table.c.detail,
                    cache_table.c.model,
                ],
                set_={
                    "text": insert_stmt.excluded.text,
                    "title": insert_stmt.excluded.title,
                    "prompt_tokens": insert_stmt.excluded.prompt_tokens,
                    "completion_tokens": insert_stmt.excluded.completion_tokens,
                    "total_tokens": insert_stmt.excluded.total_tokens,
                    "created_at": insert_stmt.excluded.created_at,
                },
            )
            await session.execute(upsert_stmt)
            await session.commit()
            usage_row = await session.get(OcrUsageModel, today)
            if usage_row is not None:
                await session.refresh(usage_row)
                spent_after = usage_row.spent_tokens
        else:
            spent_after = usage_row.spent_tokens if usage_row else spent_after

        hydrated_results: list[PosterOcrCache] = []
        for idx, cache_key in enumerate(result_keys):
            fresh = await session.get(PosterOcrCache, cache_key)
            if fresh is None:
                cached_entry = cache_map.get(cache_key) or results[idx]
            else:
                cached_entry = fresh
            hydrated_results.append(PosterOcrCache(**cached_entry.model_dump()))
        results = hydrated_results

        remaining = DAILY_TOKEN_LIMIT - spent_after
        remaining = max(0, remaining)
        if count_usage:
            spent_tokens = charged_amount
        else:
            spent_tokens = 0

        logger.info(
            "poster_ocr.stats cache_hits=%d new_entries=%d blocked_uncached=%d failed_uncached=%d spent_tokens=%d charged_tokens=%d total_new_tokens=%d remaining=%d",
            cache_hits,
            len(entries_to_upsert),
            blocked_uncached_count,
            failed_uncached_count,
            spent_tokens,
            charged_amount,
            total_new_tokens,
            remaining,
            extra=log_extra,
        )
        if count_usage and encountered_uncached_after_limit:
            logger.warning(
                "poster_ocr.limit_exceeded blocked_uncached=%d remaining=%d",
                blocked_uncached_count,
                remaining,
                extra=log_extra,
            )
            raise PosterOcrLimitExceededError(
                "poster OCR daily token limit exhausted",
                spent_tokens=spent_tokens,
                remaining=remaining,
                results=results,
            )
        return results, spent_tokens, remaining
