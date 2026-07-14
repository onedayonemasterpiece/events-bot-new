from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any
from pathlib import Path
from functools import lru_cache
from urllib.parse import urlsplit, urlunsplit

import yaml


TRUST_LEVELS = ("low", "medium", "high")
_TRUST_PRIORITY = {"low": 0, "medium": 1, "high": 2}


@dataclass(frozen=True, slots=True)
class TelegramSourceSpec:
    username: str
    trust_level: str
    default_location: str | None = None
    festival_series: str | None = None
    filters: dict[str, Any] | None = None
    notes: str | None = None


_USERNAME_RE = re.compile(r"^[a-z0-9_]{4,64}$")
TELEGRAM_PUBLIC_HOSTS = frozenset({"t.me", "telegram.me"})


def canonicalize_tg_url(value: str | None) -> str | None:
    """Accept Telegram's public host aliases and return one stable ``t.me`` URL.

    ``telegram.me`` is an official public-link alias, not a different source identity.
    Input boundaries should accept either host, while persisted and generated links remain
    canonical ``https://t.me/...`` URLs so Smart Update/source deduplication cannot split one
    post into two identities.
    """
    raw = str(value or "").strip().strip("<>\"'")
    if not raw:
        return None
    if raw.lower().startswith("tg://resolve?domain="):
        username = raw.split("=", 1)[1].split("&", 1)[0].strip().lstrip("@").lower()
        return f"https://t.me/{username}" if _USERNAME_RE.fullmatch(username) else None
    if "://" not in raw:
        if not re.match(r"(?i)^(?:www\.)?(?:t\.me|telegram\.me)/", raw):
            return None
        raw = f"https://{raw}"
    try:
        parsed = urlsplit(raw)
    except Exception:
        return None
    try:
        host = (parsed.hostname or "").casefold().removeprefix("www.")
        has_forbidden_authority = bool(parsed.username or parsed.password or parsed.port)
    except ValueError:
        return None
    if host not in TELEGRAM_PUBLIC_HOSTS:
        return None
    if has_forbidden_authority:
        return None
    path = parsed.path or "/"
    return urlunsplit(("https", "t.me", path, parsed.query, parsed.fragment))


def parse_tg_post_url(value: str | None) -> tuple[str, int] | None:
    """Parse a public Telegram post URL on either host alias.

    ``/s/<username>/<id>`` preview links are accepted and normalized to the same public
    post identity. Private ``/c/...`` links deliberately have no public username and are
    therefore outside this helper.
    """
    canonical = canonicalize_tg_url(value)
    if not canonical:
        return None
    parts = [part for part in urlsplit(canonical).path.split("/") if part]
    if len(parts) == 3 and parts[0].casefold() == "s":
        parts = parts[1:]
    if len(parts) != 2 or not parts[1].isdigit():
        return None
    username = parts[0].strip().lstrip("@").casefold()
    message_id = int(parts[1])
    if not _USERNAME_RE.fullmatch(username) or message_id <= 0:
        return None
    return username, message_id


def normalize_tg_username(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    canonical = canonicalize_tg_url(raw)
    if canonical:
        raw = urlsplit(canonical).path.lstrip("/")
        if raw.casefold().startswith("s/"):
            raw = raw[2:]
    raw = raw.lstrip("@").strip()
    raw = re.sub(r"^https?://", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"^tg://resolve\?domain=", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"^(?:www\.)?(?:t\.me|telegram\.me)/", "", raw, flags=re.IGNORECASE)
    raw = raw.split("?", 1)[0].split("#", 1)[0]
    raw = raw.split("/", 1)[0]
    raw = raw.strip().lstrip("@").strip().lower()
    if not raw:
        return ""
    if not _USERNAME_RE.match(raw):
        return ""
    return raw


def trust_priority(value: str | None) -> int:
    key = (value or "").strip().lower()
    return _TRUST_PRIORITY.get(key, 0)


def _load_canonical_sources() -> list[TelegramSourceSpec]:
    root = Path(__file__).resolve().parent
    path = root / "docs" / "features" / "telegram-monitoring" / "sources.yml"
    if not path.exists():
        raise FileNotFoundError(f"Canonical telegram sources file not found: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict) or int(payload.get("version") or 0) != 1:
        raise ValueError("Invalid sources.yml: expected version: 1")
    raw_sources = payload.get("sources") or []
    if not isinstance(raw_sources, list):
        raise ValueError("Invalid sources.yml: sources must be a list")
    out: list[TelegramSourceSpec] = []
    for item in raw_sources:
        if not isinstance(item, dict):
            continue
        username = normalize_tg_username(item.get("username"))
        trust = str(item.get("trust_level") or "").strip().lower()
        if not username or trust not in TRUST_LEVELS:
            continue
        default_location = str(item.get("default_location") or "").strip() or None
        festival_series = str(item.get("festival_series") or "").strip() or None
        filters = item.get("filters")
        if filters is not None and not isinstance(filters, dict):
            filters = None
        notes = str(item.get("notes") or "").strip() or None
        out.append(
            TelegramSourceSpec(
                username=username,
                trust_level=trust,
                default_location=default_location,
                festival_series=festival_series,
                filters=dict(filters) if isinstance(filters, dict) else None,
                notes=notes,
            )
        )
    # Keep deterministic order for pagination and reproducibility.
    out.sort(key=lambda s: s.username)
    return out


@lru_cache(maxsize=1)
def canonical_tg_sources() -> tuple[TelegramSourceSpec, ...]:
    return tuple(_load_canonical_sources())
