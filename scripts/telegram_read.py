#!/usr/bin/env python3
"""Read a tightly bounded set of Telegram messages with a role-scoped Telethon session.

The script is deliberately read-only:
- it never marks messages as read;
- it never sends typing indicators or reactions;
- it never downloads media;
- it never prints authentication material.

It accepts either a JSON request file or direct CLI arguments and writes one JSON result
file suitable for a short-lived GitHub Actions artifact.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import random
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urlparse

SCHEMA_VERSION = 1
DEFAULT_AUTH_BUNDLE_ENV = "TELEGRAM_AUTH_BUNDLE_GH_ACTIONS"
MAX_TARGETS = 10
MAX_MESSAGES_PER_TARGET = 50
MAX_TOTAL_MESSAGES = 100

PUBLIC_HOSTS = {"t.me", "www.t.me", "telegram.me", "www.telegram.me"}
PUBLIC_USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{5,32}$")
EXACT_PUBLIC_LINK_RE = re.compile(
    r"^https?://(?:www\.)?(?:t|telegram)\.me/(?:s/)?"
    r"(?P<username>[A-Za-z0-9_]{5,32})/(?P<message_id>[1-9][0-9]*)"
    r"(?:[/?#].*)?$",
    flags=re.IGNORECASE,
)
EXACT_PRIVATE_LINK_RE = re.compile(
    r"^https?://(?:www\.)?(?:t|telegram)\.me/c/"
    r"(?P<internal_id>[1-9][0-9]*)/(?P<message_id>[1-9][0-9]*)"
    r"(?:[/?#].*)?$",
    flags=re.IGNORECASE,
)


class TelegramReadError(RuntimeError):
    """Expected, user-facing failure that does not include secrets."""


@dataclass(frozen=True, slots=True)
class AuthConfig:
    api_id: int
    api_hash: str
    session: str
    device_model: str | None = None
    system_version: str | None = None
    app_version: str | None = None
    lang_code: str | None = None
    system_lang_code: str | None = None


@dataclass(frozen=True, slots=True)
class Request:
    mode: str
    targets: tuple[str, ...]
    limit: int

    @property
    def requested_message_cap(self) -> int:
        return min(MAX_TOTAL_MESSAGES, self.limit * len(self.targets))


def _first_nonempty(mapping: Mapping[str, Any], *names: str) -> str:
    for name in names:
        value = mapping.get(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _first_env(*names: str) -> str:
    for name in names:
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return ""


def _decode_auth_bundle(raw: str) -> dict[str, Any]:
    """Decode a raw JSON or URL-safe base64 JSON auth bundle."""
    value = raw.strip()
    if not value:
        raise TelegramReadError("the configured Telegram auth bundle is empty")

    candidates: list[str] = [value]
    try:
        padding = "=" * (-len(value) % 4)
        candidates.append(base64.urlsafe_b64decode(value + padding).decode("utf-8"))
    except (ValueError, UnicodeDecodeError, base64.binascii.Error):
        pass

    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except (TypeError, ValueError):
            continue
        if isinstance(parsed, dict):
            return parsed

    raise TelegramReadError("the configured Telegram auth bundle is not valid JSON/base64 JSON")


def load_auth_config(bundle_env: str = DEFAULT_AUTH_BUNDLE_ENV) -> AuthConfig:
    """Load only the explicitly named role-scoped auth bundle."""
    raw = os.getenv(bundle_env, "")
    bundle = _decode_auth_bundle(raw)

    api_id_raw = _first_nonempty(bundle, "api_id", "apiId") or _first_env(
        "TELEGRAM_API_ID", "TG_API_ID"
    )
    api_hash = _first_nonempty(bundle, "api_hash", "apiHash") or _first_env(
        "TELEGRAM_API_HASH", "TG_API_HASH"
    )
    session = _first_nonempty(
        bundle,
        "session",
        "session_string",
        "string_session",
        "stringSession",
    )

    try:
        api_id = int(api_id_raw)
    except (TypeError, ValueError) as exc:
        raise TelegramReadError(
            "Telegram API ID is missing or invalid in the bundle/environment"
        ) from exc

    if api_id <= 0:
        raise TelegramReadError("Telegram API ID must be positive")
    if not api_hash:
        raise TelegramReadError("Telegram API hash is missing in the bundle/environment")
    if not session:
        raise TelegramReadError("Telethon StringSession is missing in the auth bundle")

    return AuthConfig(
        api_id=api_id,
        api_hash=api_hash,
        session=session,
        device_model=_first_nonempty(bundle, "device_model", "deviceModel") or None,
        system_version=_first_nonempty(bundle, "system_version", "systemVersion") or None,
        app_version=_first_nonempty(bundle, "app_version", "appVersion") or None,
        lang_code=_first_nonempty(bundle, "lang_code", "langCode") or None,
        system_lang_code=_first_nonempty(
            bundle, "system_lang_code", "systemLangCode"
        )
        or None,
    )


def parse_seconds_range(name: str, default: tuple[float, float]) -> tuple[float, float]:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        left, right = (float(part.strip()) for part in raw.split(",", 1))
    except (TypeError, ValueError) as exc:
        raise TelegramReadError(
            f"{name} must contain two comma-separated numbers"
        ) from exc
    return max(0.0, min(left, right)), max(0.0, max(left, right))


def parse_positive_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise TelegramReadError(f"{name} must be an integer") from exc
    if value < 0:
        raise TelegramReadError(f"{name} must not be negative")
    return value


class HumanPacer:
    """The repository's canonical bounded, random pacing for Telegram reads."""

    def __init__(self) -> None:
        self._random = random.SystemRandom()
        self.startup_range = parse_seconds_range(
            "TELEGRAM_READ_STARTUP_DELAY_SECONDS", (4.0, 12.0)
        )
        self.request_range = parse_seconds_range(
            "TELEGRAM_READ_BETWEEN_REQUESTS_SECONDS", (2.0, 5.0)
        )
        self.target_range = parse_seconds_range(
            "TELEGRAM_READ_BETWEEN_TARGETS_SECONDS", (5.0, 15.0)
        )
        self._request_count = 0

    async def _sleep(self, bounds: tuple[float, float]) -> float:
        lower, upper = bounds
        delay = self._random.uniform(lower, upper) if upper > lower else lower
        if delay > 0:
            await asyncio.sleep(delay)
        return delay

    async def before_connect(self) -> float:
        return await self._sleep(self.startup_range)

    async def before_request(self) -> float:
        delay = 0.0
        if self._request_count:
            delay = await self._sleep(self.request_range)
        self._request_count += 1
        return delay

    async def between_targets(self) -> float:
        return await self._sleep(self.target_range)


def _coerce_targets(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        raw_targets = [line.strip() for line in value.replace(",", "\n").splitlines()]
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        raw_targets = [str(item).strip() for item in value]
    else:
        raise TelegramReadError("targets must be a string or an array of strings")

    targets = tuple(item for item in raw_targets if item)
    if not targets:
        raise TelegramReadError("at least one Telegram target is required")
    if len(targets) > MAX_TARGETS:
        raise TelegramReadError(f"no more than {MAX_TARGETS} targets are allowed")
    return targets


def parse_request(payload: Mapping[str, Any]) -> Request:
    mode = str(payload.get("mode") or "latest").strip().lower()
    if mode not in {"latest", "messages"}:
        raise TelegramReadError("mode must be either 'latest' or 'messages'")

    targets = _coerce_targets(payload.get("targets"))
    try:
        limit = int(payload.get("limit", 2))
    except (TypeError, ValueError) as exc:
        raise TelegramReadError("limit must be an integer") from exc
    if limit < 1 or limit > MAX_MESSAGES_PER_TARGET:
        raise TelegramReadError(
            f"limit must be between 1 and {MAX_MESSAGES_PER_TARGET}"
        )

    if mode == "messages":
        effective_total = len(targets)
    else:
        effective_total = len(targets) * limit
    if effective_total > MAX_TOTAL_MESSAGES:
        raise TelegramReadError(
            f"the request may return at most {MAX_TOTAL_MESSAGES} messages"
        )

    return Request(mode=mode, targets=targets, limit=limit)


def load_request(path: str | None, mode: str | None, targets: Sequence[str], limit: int) -> Request:
    if path:
        try:
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise TelegramReadError(f"request file not found: {path}") from exc
        except (OSError, ValueError) as exc:
            raise TelegramReadError(f"request file is not valid JSON: {path}") from exc
        if not isinstance(payload, dict):
            raise TelegramReadError("request JSON root must be an object")
        return parse_request(payload)

    return parse_request({"mode": mode or "latest", "targets": list(targets), "limit": limit})


def normalize_latest_target(raw: str) -> str | int:
    value = raw.strip()
    if not value:
        raise TelegramReadError("empty Telegram target")

    if re.fullmatch(r"-?\d+", value):
        return int(value)
    if value.startswith("@"):
        username = value[1:]
        if not PUBLIC_USERNAME_RE.fullmatch(username):
            raise TelegramReadError(f"invalid Telegram username: {value}")
        return username
    if PUBLIC_USERNAME_RE.fullmatch(value):
        return value

    parsed = urlparse(value if "://" in value else f"https://{value}")
    if parsed.scheme not in {"http", "https"} or parsed.netloc.lower() not in PUBLIC_HOSTS:
        raise TelegramReadError(f"unsupported Telegram target: {value}")

    parts = [part for part in parsed.path.split("/") if part]
    if parts and parts[0].lower() == "s":
        parts = parts[1:]
    if len(parts) == 1 and PUBLIC_USERNAME_RE.fullmatch(parts[0]):
        return parts[0]
    if len(parts) == 2 and parts[0].lower() == "c" and parts[1].isdigit():
        return int(f"-100{parts[1]}")

    raise TelegramReadError(
        f"latest mode expects a channel/chat URL or username, not a post/invite link: {value}"
    )


def parse_exact_message_link(raw: str) -> tuple[str | int, int, str]:
    value = raw.strip()
    public = EXACT_PUBLIC_LINK_RE.fullmatch(value)
    if public:
        username = public.group("username")
        message_id = int(public.group("message_id"))
        return username, message_id, f"https://t.me/{username}/{message_id}"

    private = EXACT_PRIVATE_LINK_RE.fullmatch(value)
    if private:
        internal_id = private.group("internal_id")
        message_id = int(private.group("message_id"))
        return int(f"-100{internal_id}"), message_id, f"https://t.me/c/{internal_id}/{message_id}"

    raise TelegramReadError(f"invalid exact Telegram message link: {value}")


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(value)


def _reaction_key(reaction: Any) -> str:
    emoticon = getattr(reaction, "emoticon", None)
    if emoticon:
        return str(emoticon)
    document_id = getattr(reaction, "document_id", None)
    if document_id:
        return f"custom:{document_id}"
    return reaction.__class__.__name__


def _serialize_reactions(message: Any) -> dict[str, int] | None:
    container = getattr(message, "reactions", None)
    results = getattr(container, "results", None)
    if not results:
        return None
    output: dict[str, int] = {}
    for item in results:
        count = getattr(item, "count", None)
        if isinstance(count, int):
            output[_reaction_key(getattr(item, "reaction", None))] = count
    return output or None


def _serialize_message(message: Any, entity: Any, fallback_target: str) -> dict[str, Any]:
    message_id = int(getattr(message, "id", 0) or 0)
    username = getattr(entity, "username", None)
    entity_id = getattr(entity, "id", None)
    if username and message_id:
        link = f"https://t.me/{username}/{message_id}"
    elif entity_id and message_id:
        link = f"https://t.me/c/{entity_id}/{message_id}"
    else:
        link = fallback_target

    media = getattr(message, "media", None)
    media_type = media.__class__.__name__ if media is not None else None
    replies = getattr(message, "replies", None)
    reply_count = getattr(replies, "replies", None)

    return {
        "id": message_id,
        "link": link,
        "date": _iso(getattr(message, "date", None)),
        "edit_date": _iso(getattr(message, "edit_date", None)),
        "text": str(getattr(message, "message", None) or ""),
        "media_type": media_type,
        "views": getattr(message, "views", None),
        "forwards": getattr(message, "forwards", None),
        "replies": reply_count if isinstance(reply_count, int) else None,
        "reactions": _serialize_reactions(message),
    }


def _client_kwargs(auth: AuthConfig, flood_sleep_threshold: int) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "receive_updates": False,
        "auto_reconnect": False,
        "flood_sleep_threshold": flood_sleep_threshold,
    }
    for key in (
        "device_model",
        "system_version",
        "app_version",
        "lang_code",
        "system_lang_code",
    ):
        value = getattr(auth, key)
        if value:
            kwargs[key] = value
    return kwargs


async def _resolve_entity(client: Any, pacer: HumanPacer, peer: str | int) -> Any:
    await pacer.before_request()
    return await client.get_entity(peer)


async def _get_messages(client: Any, pacer: HumanPacer, entity: Any, **kwargs: Any) -> Any:
    await pacer.before_request()
    return await client.get_messages(entity, **kwargs)


async def collect(request: Request, auth: AuthConfig) -> dict[str, Any]:
    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
    except ImportError as exc:
        raise TelegramReadError("Telethon is not installed") from exc

    flood_sleep_threshold = parse_positive_int("TELEGRAM_READ_FLOOD_SLEEP_SECONDS", 60)
    pacer = HumanPacer()
    client = TelegramClient(
        StringSession(auth.session),
        auth.api_id,
        auth.api_hash,
        **_client_kwargs(auth, flood_sleep_threshold),
    )

    results: list[dict[str, Any]] = []
    await pacer.before_connect()
    await client.connect()
    try:
        await pacer.before_request()
        if not await client.is_user_authorized():
            raise TelegramReadError("the configured Telethon session is not authorized")

        if request.mode == "latest":
            for index, raw_target in enumerate(request.targets):
                if index:
                    await pacer.between_targets()
                try:
                    peer = normalize_latest_target(raw_target)
                    entity = await _resolve_entity(client, pacer, peer)
                    messages = await _get_messages(
                        client, pacer, entity, limit=request.limit
                    )
                    serialized = [
                        _serialize_message(item, entity, raw_target)
                        for item in messages
                        if item is not None
                    ]
                    results.append(
                        {
                            "target": raw_target,
                            "resolved": {
                                "id": getattr(entity, "id", None),
                                "username": getattr(entity, "username", None),
                                "title": getattr(entity, "title", None),
                            },
                            "messages": serialized,
                            "error": None,
                        }
                    )
                except TelegramReadError as exc:
                    results.append(
                        {"target": raw_target, "resolved": None, "messages": [], "error": str(exc)}
                    )
                except Exception as exc:
                    results.append(
                        {
                            "target": raw_target,
                            "resolved": None,
                            "messages": [],
                            "error": f"{exc.__class__.__name__}",
                        }
                    )
        else:
            grouped: dict[str | int, list[tuple[str, int, str]]] = {}
            for raw_target in request.targets:
                try:
                    peer, message_id, canonical = parse_exact_message_link(raw_target)
                    grouped.setdefault(peer, []).append((raw_target, message_id, canonical))
                except TelegramReadError as exc:
                    results.append(
                        {"target": raw_target, "resolved": None, "messages": [], "error": str(exc)}
                    )

            for index, (peer, entries) in enumerate(grouped.items()):
                if index:
                    await pacer.between_targets()
                try:
                    entity = await _resolve_entity(client, pacer, peer)
                    ids = [message_id for _, message_id, _ in entries]
                    fetched = await _get_messages(client, pacer, entity, ids=ids)
                    if not isinstance(fetched, Iterable) or isinstance(fetched, (str, bytes)):
                        fetched = [fetched]
                    by_id = {
                        int(getattr(item, "id", 0) or 0): item
                        for item in fetched
                        if item is not None
                    }
                    for raw_target, message_id, canonical in entries:
                        message = by_id.get(message_id)
                        results.append(
                            {
                                "target": raw_target,
                                "resolved": {
                                    "id": getattr(entity, "id", None),
                                    "username": getattr(entity, "username", None),
                                    "title": getattr(entity, "title", None),
                                },
                                "messages": [
                                    _serialize_message(message, entity, canonical)
                                ]
                                if message is not None
                                else [],
                                "error": None if message is not None else "message_not_found",
                            }
                        )
                except Exception as exc:
                    for raw_target, _, _ in entries:
                        results.append(
                            {
                                "target": raw_target,
                                "resolved": None,
                                "messages": [],
                                "error": f"{exc.__class__.__name__}",
                            }
                        )
    finally:
        await client.disconnect()

    message_count = sum(len(item["messages"]) for item in results)
    error_count = sum(1 for item in results if item["error"])
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "request": {
            "mode": request.mode,
            "targets": list(request.targets),
            "limit": request.limit,
        },
        "summary": {
            "target_count": len(request.targets),
            "message_count": message_count,
            "error_count": error_count,
        },
        "results": results,
    }


def write_json(path: str, payload: Mapping[str, Any]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(output)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", help="JSON request file")
    parser.add_argument("--mode", choices=("latest", "messages"), default="latest")
    parser.add_argument(
        "--target",
        action="append",
        default=[],
        help="Telegram channel/chat target or exact post link; repeat as needed",
    )
    parser.add_argument("--limit", type=int, default=2)
    parser.add_argument(
        "--auth-bundle-env",
        default=DEFAULT_AUTH_BUNDLE_ENV,
        help="name of the role-scoped environment variable containing auth JSON/base64 JSON",
    )
    parser.add_argument("--output", required=True, help="JSON output path")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        request = load_request(args.request, args.mode, args.target, args.limit)
        auth = load_auth_config(args.auth_bundle_env)
        payload = asyncio.run(collect(request, auth))
        write_json(args.output, payload)
        if payload["summary"]["message_count"] == 0:
            print("Telegram read completed without messages", file=sys.stderr)
            return 2
        print(
            "Telegram read completed: "
            f"{payload['summary']['message_count']} message(s), "
            f"{payload['summary']['error_count']} target error(s)"
        )
        return 0
    except TelegramReadError as exc:
        error_payload = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "summary": {"target_count": 0, "message_count": 0, "error_count": 1},
            "error": str(exc),
        }
        try:
            write_json(args.output, error_payload)
        except OSError:
            pass
        print(f"Telegram read failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
