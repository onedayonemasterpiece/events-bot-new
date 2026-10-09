"""Bounded local cache for the canonical shared VK mentions registry.

The registry is sourced from a single authoritative remote URL and cached locally
with TTL-based refresh. Only verified entries are used for VK publication mentions.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Optional

import httpx

_LOG = logging.getLogger(__name__)

VK_MENTIONS_REGISTRY_URL = os.getenv(
    "VK_MENTIONS_REGISTRY_URL",
    "https://raw.githubusercontent.com/onedayonemasterpiece/idea-hub/chatgpt/vk-mentions-20261009/registry/vk-mentions/registry-v1.json",
)
VK_MENTIONS_CACHE_PATH = Path(
    os.getenv(
        "VK_MENTIONS_CACHE_PATH",
        "/data/vk_mentions_registry_cache.json",
    )
)
VK_MENTIONS_TTL_SECONDS = int(os.getenv("VK_MENTIONS_TTL_SECONDS", "3600"))
VK_MENTIONS_MAX_ENTRIES = int(os.getenv("VK_MENTIONS_MAX_ENTRIES", "500"))

_ID_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_VK_CLUB_RE = re.compile(r"^club(\d+)$", re.IGNORECASE)
_VK_ID_RE = re.compile(r"^id(\d+)$", re.IGNORECASE)
_VK_PUBLIC_RE = re.compile(r"^public(\d+)$", re.IGNORECASE)
_VK_MENTION_RE = re.compile(r"^\[(club|id|public)(\d+)\|([^\]]+)\]$", re.IGNORECASE)

_VERIFIED_STATUSES = {"verified", "candidate"}
_ENTRY_TYPES = {"community", "user"}


class VKMentionsRegistryError(ValueError):
    """Raised when registry data violates the contract."""


@dataclass(frozen=True)
class VKMentionEntry:
    """A single verified VK mention entry."""

    id: str
    type: Literal["community", "user"]
    vk_id: str
    label: str
    status: Literal["verified", "candidate"]
    aliases: tuple[str, ...] = field(default_factory=tuple)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def mention_markup(self) -> str:
        """Return VK raw markup for this entry."""
        prefix = "club" if self.type == "community" else "id"
        return f"[{prefix}{self.vk_id}|{self.label}]"

    def matches_query(self, query: str) -> bool:
        """Check if query matches this entry (case-insensitive partial match)."""
        q = query.casefold()
        if q in self.label.casefold():
            return True
        if q == self.id.casefold():
            return True
        if q == f"{('club' if self.type == 'community' else 'id')}{self.vk_id}".casefold():
            return True
        for alias in self.aliases:
            if q in alias.casefold():
                return True
        return False


@dataclass
class VKMentionsRegistry:
    """Local cache of the VK mentions registry with TTL refresh."""

    entries: dict[str, VKMentionEntry] = field(default_factory=dict)
    verified_entries: dict[str, VKMentionEntry] = field(default_factory=dict)
    last_refresh: float = 0.0
    etag: str = ""
    content_hash: str = ""
    _refresh_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def __post_init__(self) -> None:
        self._rebuild_indices()

    def _rebuild_indices(self) -> None:
        self.verified_entries = {
            e.id: e for e in self.entries.values() if e.status == "verified"
        }

    def get_verified(self, entry_id: str) -> Optional[VKMentionEntry]:
        """Get a verified entry by ID."""
        return self.verified_entries.get(entry_id)

    def get_verified_by_vk_id(self, vk_id: str, type_: Literal["community", "user"]) -> Optional[VKMentionEntry]:
        """Get a verified entry by VK numeric ID and type."""
        for entry in self.verified_entries.values():
            if entry.vk_id == vk_id and entry.type == type_:
                return entry
        return None

    def search_verified(self, query: str, limit: int = 3) -> list[VKMentionEntry]:
        """Search verified entries by label, ID, or aliases (case-insensitive partial match)."""
        query = query.strip().casefold()
        if not query:
            return []
        results = []
        for entry in self.verified_entries.values():
            if entry.matches_query(query):
                results.append(entry)
                if len(results) >= limit:
                    break
        return results

    def all_verified(self) -> list[VKMentionEntry]:
        """Return all verified entries."""
        return list(self.verified_entries.values())

    def is_stale(self) -> bool:
        return time.time() - self.last_refresh > VK_MENTIONS_TTL_SECONDS

    def to_json(self) -> dict[str, Any]:
        return {
            "schemaVersion": "vk_mentions_registry_v1",
            "registryVersion": 1,
            "refreshedAt": datetime.fromtimestamp(self.last_refresh, tz=timezone.utc).isoformat(),
            "etag": self.etag,
            "contentHash": self.content_hash,
            "entries": [
                {
                    "id": e.id,
                    "type": e.type,
                    "vkId": e.vk_id,
                    "label": e.label,
                    "status": e.status,
                    "aliases": list(e.aliases),
                    "metadata": dict(e.metadata),
                }
                for e in self.entries.values()
            ],
        }

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> "VKMentionsRegistry":
        if data.get("schemaVersion") != "vk_mentions_registry_v1":
            raise VKMentionsRegistryError("Invalid schema version")
        entries = {}
        seen_ids = set()
        seen_vk_ids = set()

        for idx, item in enumerate(data.get("entries", [])):
            if not isinstance(item, dict):
                raise VKMentionsRegistryError(f"entries[{idx}] must be an object")

            entry_id = item.get("id")
            if not isinstance(entry_id, str) or not _ID_RE.fullmatch(entry_id):
                raise VKMentionsRegistryError(f"entries[{idx}].id must be stable kebab-case ID")
            if entry_id in seen_ids:
                raise VKMentionsRegistryError(f"Duplicate entry id: {entry_id}")
            seen_ids.add(entry_id)

            entry_type = item.get("type")
            if entry_type not in _ENTRY_TYPES:
                raise VKMentionsRegistryError(f"entries[{idx}].type must be one of {_ENTRY_TYPES}")

            vk_id = item.get("vkId")
            if not isinstance(vk_id, str) or not vk_id.isdigit():
                raise VKMentionsRegistryError(f"entries[{idx}].vkId must be numeric string")
            vk_key = f"{entry_type}:{vk_id}"
            if vk_key in seen_vk_ids:
                raise VKMentionsRegistryError(f"Duplicate vkId for type {entry_type}: {vk_id}")
            seen_vk_ids.add(vk_key)

            label = item.get("label")
            if not isinstance(label, str) or not label.strip():
                raise VKMentionsRegistryError(f"entries[{idx}].label must be non-empty string")

            status = item.get("status", "candidate")
            if status not in _VERIFIED_STATUSES:
                raise VKMentionsRegistryError(f"entries[{idx}].status must be one of {_VERIFIED_STATUSES}")

            aliases = tuple(item.get("aliases", []))
            if not isinstance(aliases, (list, tuple)) or not all(isinstance(a, str) for a in aliases):
                raise VKMentionsRegistryError(f"entries[{idx}].aliases must be list of strings")

            metadata = item.get("metadata", {})
            if not isinstance(metadata, dict):
                raise VKMentionsRegistryError(f"entries[{idx}].metadata must be an object")

            entry = VKMentionEntry(
                id=entry_id,
                type=entry_type,
                vk_id=vk_id,
                label=label,
                status=status,
                aliases=aliases,
                metadata=metadata,
            )
            entries[entry_id] = entry

        registry = cls(entries=entries)
        registry.last_refresh = data.get("refreshedAt", 0.0)
        registry.etag = data.get("etag", "")
        registry.content_hash = data.get("contentHash", "")
        return registry


def _parse_remote_registry(data: Mapping[str, Any]) -> VKMentionsRegistry:
    """Parse and validate the remote registry JSON."""
    if not isinstance(data, dict):
        raise VKMentionsRegistryError("Registry root must be an object")
    if data.get("schemaVersion") != "vk_mentions_registry_v1":
        raise VKMentionsRegistryError(f"Expected schemaVersion 'vk_mentions_registry_v1', got {data.get('schemaVersion')!r}")

    entries = {}
    seen_ids = set()
    seen_vk_ids = set()

    for idx, item in enumerate(data.get("entries", [])):
        if not isinstance(item, dict):
            raise VKMentionsRegistryError(f"entries[{idx}] must be an object")

        entry_id = item.get("id")
        if not isinstance(entry_id, str) or not _ID_RE.fullmatch(entry_id):
            raise VKMentionsRegistryError(f"entries[{idx}].id must be stable kebab-case ID")
        if entry_id in seen_ids:
            raise VKMentionsRegistryError(f"Duplicate entry id: {entry_id}")
        seen_ids.add(entry_id)

        entry_type = item.get("type")
        if entry_type not in _ENTRY_TYPES:
            raise VKMentionsRegistryError(f"entries[{idx}].type must be one of {_ENTRY_TYPES}")

        vk_id = item.get("vkId")
        if not isinstance(vk_id, str) or not vk_id.isdigit():
            raise VKMentionsRegistryError(f"entries[{idx}].vkId must be numeric string")
        vk_key = f"{entry_type}:{vk_id}"
        if vk_key in seen_vk_ids:
            raise VKMentionsRegistryError(f"Duplicate vkId for type {entry_type}: {vk_id}")
        seen_vk_ids.add(vk_key)

        label = item.get("label")
        if not isinstance(label, str) or not label.strip():
            raise VKMentionsRegistryError(f"entries[{idx}].label must be non-empty string")

        status = item.get("status", "candidate")
        if status not in _VERIFIED_STATUSES:
            raise VKMentionsRegistryError(f"entries[{idx}].status must be one of {_VERIFIED_STATUSES}")

        aliases = tuple(item.get("aliases", []))
        if not isinstance(aliases, (list, tuple)) or not all(isinstance(a, str) for a in aliases):
            raise VKMentionsRegistryError(f"entries[{idx}].aliases must be list of strings")

        metadata = item.get("metadata", {})
        if not isinstance(metadata, dict):
            raise VKMentionsRegistryError(f"entries[{idx}].metadata must be an object")

        entry = VKMentionEntry(
            id=entry_id,
            type=entry_type,
            vk_id=vk_id,
            label=label,
            status=status,
            aliases=aliases,
            metadata=metadata,
        )
        entries[entry_id] = entry

    registry = VKMentionsRegistry(entries=entries)
    return registry


async def _fetch_remote_registry(client: httpx.AsyncClient, etag: str = "") -> tuple[Optional[dict], str]:
    """Fetch the remote registry with ETag support."""
    headers = {}
    if etag:
        headers["If-None-Match"] = etag
    try:
        resp = await client.get(VK_MENTIONS_REGISTRY_URL, headers=headers, timeout=30.0)
        if resp.status_code == 304:
            return None, etag
        resp.raise_for_status()
        new_etag = resp.headers.get("ETag", "")
        return resp.json(), new_etag
    except httpx.HTTPError as exc:
        _LOG.warning("Failed to fetch VK mentions registry: %s", exc)
        raise


async def _persist_cache(registry: VKMentionsRegistry) -> None:
    """Persist registry to local cache file atomically."""
    VK_MENTIONS_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = VK_MENTIONS_CACHE_PATH.with_suffix(".tmp")
    try:
        tmp_path.write_text(json.dumps(registry.to_json(), ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(VK_MENTIONS_CACHE_PATH)
    except OSError as exc:
        _LOG.error("Failed to write VK mentions cache: %s", exc)
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise


def _load_cache() -> Optional[VKMentionsRegistry]:
    """Load registry from local cache file."""
    if not VK_MENTIONS_CACHE_PATH.exists():
        return None
    try:
        data = json.loads(VK_MENTIONS_CACHE_PATH.read_text(encoding="utf-8"))
        return VKMentionsRegistry.from_json(data)
    except (OSError, json.JSONDecodeError, VKMentionsRegistryError) as exc:
        _LOG.warning("Failed to load VK mentions cache: %s", exc)
        return None


async def refresh_registry(registry: VKMentionsRegistry, force: bool = False) -> bool:
    """Refresh the registry from remote source. Returns True if updated."""
    async with registry._refresh_lock:
        if not force and not registry.is_stale():
            return False

        async with httpx.AsyncClient() as client:
            try:
                remote_data, new_etag = await _fetch_remote_registry(client, registry.etag)
                if remote_data is None:
                    _LOG.info("VK mentions registry not modified (ETag match)")
                    registry.last_refresh = time.time()
                    return False

                new_registry = _parse_remote_registry(remote_data)
                new_registry.etag = new_etag
                new_registry.last_refresh = time.time()
                new_registry.content_hash = hashlib.sha256(
                    json.dumps(remote_data, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest()

                if new_registry.content_hash == registry.content_hash:
                    _LOG.info("VK mentions registry content unchanged")
                    registry.last_refresh = time.time()
                    return False

                await _persist_cache(new_registry)
                registry.entries = new_registry.entries
                registry.verified_entries = new_registry.verified_entries
                registry.last_refresh = new_registry.last_refresh
                registry.etag = new_registry.etag
                registry.content_hash = new_registry.content_hash
                _LOG.info("VK mentions registry refreshed: %d entries (%d verified)", len(registry.entries), len(registry.verified_entries))
                return True
            except Exception as exc:
                _LOG.error("VK mentions registry refresh failed: %s", exc)
                raise


async def load_or_create_registry() -> VKMentionsRegistry:
    """Load registry from cache or create empty and fetch."""
    cached = _load_cache()
    if cached:
        _LOG.info("Loaded VK mentions registry from cache: %d entries (%d verified)", len(cached.entries), len(cached.verified_entries))
        await refresh_registry(cached)
        return cached

    registry = VKMentionsRegistry()
    await refresh_registry(registry, force=True)
    return registry


async def start_registry_refresh_scheduler(registry: VKMentionsRegistry, interval: float = VK_MENTIONS_TTL_SECONDS) -> asyncio.Task:
    """Start background task to periodically refresh the registry."""

    async def _scheduler() -> None:
        while True:
            await asyncio.sleep(interval)
            try:
                await refresh_registry(registry)
            except asyncio.CancelledError:
                raise
            except Exception:
                _LOG.exception("Scheduled VK mentions registry refresh failed")

    return asyncio.create_task(_scheduler())


def parse_vk_mention_markup(text: str) -> Optional[tuple[str, str, str]]:
    """Parse VK mention markup [club123|Label] or [id456|Label].
    Returns (prefix, vk_id, label) or None.
    """
    match = _VK_MENTION_RE.match(text.strip())
    if match:
        return match.group(1).lower(), match.group(2), match.group(3)
    return None


def build_vk_mention_markup(vk_id: str, label: str, type_: Literal["community", "user"] = "community") -> str:
    """Build VK raw mention markup."""
    prefix = "club" if type_ == "community" else "id"
    return f"[{prefix}{vk_id}|{label}]"


def escape_vk_mention_text(text: str) -> str:
    """Escape text for safe inclusion in VK mention markup."""
    return text.replace("|", "\\|").replace("[", "\\[").replace("]", "\\]")


__all__ = [
    "VKMentionEntry",
    "VKMentionsRegistry",
    "VKMentionsRegistryError",
    "VK_MENTIONS_REGISTRY_URL",
    "VK_MENTIONS_CACHE_PATH",
    "VK_MENTIONS_TTL_SECONDS",
    "load_or_create_registry",
    "refresh_registry",
    "start_registry_refresh_scheduler",
    "parse_vk_mention_markup",
    "build_vk_mention_markup",
    "escape_vk_mention_text",
]