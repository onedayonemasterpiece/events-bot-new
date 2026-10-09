"""Read-only MCP tools for VK mentions registry search and validation.

These tools allow LLMs to lookup verified VK mentions for persons/guides/venues
from Event360, guide_names/organizer_names, and add mentions where appropriate.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, Optional

from guide_excursions.parser import collapse_ws
from vk_mentions_registry import (
    VKMentionEntry,
    VKMentionsRegistry,
    VKMentionsRegistryError,
    build_vk_mention_markup,
    escape_vk_mention_text,
    load_or_create_registry,
    parse_vk_mention_markup,
    refresh_registry,
)

_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class VKMentionSearchResult:
    """Result of a VK mention search."""

    entries: list[VKMentionEntry]
    query: str
    limited: bool


@dataclass(frozen=True)
class VKMentionValidationResult:
    """Result of validating a VK mention candidate."""

    valid: bool
    entry: Optional[VKMentionEntry]
    reason: str
    markup: Optional[str] = None


class VKMentionsMCPTools:
    """Read-only MCP tools for VK mentions registry."""

    def __init__(self, registry: Optional[VKMentionsRegistry] = None):
        self._registry = registry
        self._init_lock = asyncio.Lock()

    async def _ensure_registry(self) -> VKMentionsRegistry:
        if self._registry is None:
            async with self._init_lock:
                if self._registry is None:
                    self._registry = await load_or_create_registry()
        return self._registry

    async def search_mentions(
        self,
        query: str,
        *,
        limit: int = 3,
        only_verified: bool = True,
    ) -> VKMentionSearchResult:
        """Search for VK mentions by label, ID, or alias.

        Args:
            query: Search query (label, VK ID, or alias)
            limit: Maximum results to return (default 3, max 10)
            only_verified: Only return verified entries (default True)

        Returns:
            VKMentionSearchResult with matching entries
        """
        registry = await self._ensure_registry()
        limit = max(1, min(limit, 10))

        if only_verified:
            entries = registry.search_verified(query, limit=limit)
        else:
            entries = []
            query_lower = query.casefold()
            for entry in registry.entries.values():
                if entry.matches_query(query):
                    entries.append(entry)
                    if len(entries) >= limit:
                        break

        return VKMentionSearchResult(
            entries=entries,
            query=query,
            limited=len(entries) >= limit,
        )

    async def validate_mention(
        self,
        vk_id: str,
        type_: Literal["community", "user"],
        label: Optional[str] = None,
    ) -> VKMentionValidationResult:
        """Validate a VK mention candidate and return canonical markup.

        Args:
            vk_id: Numeric VK ID (e.g., "241261191")
            type_: "community" for groups/pages, "user" for profiles
            label: Optional expected label for verification

        Returns:
            VKMentionValidationResult with validation status and markup
        """
        registry = await self._ensure_registry()

        if not vk_id.isdigit():
            return VKMentionValidationResult(
                valid=False,
                entry=None,
                reason=f"Invalid VK ID: must be numeric, got {vk_id!r}",
            )

        entry = registry.get_verified_by_vk_id(vk_id, type_)
        if not entry:
            return VKMentionValidationResult(
                valid=False,
                entry=None,
                reason=f"No verified {type_} entry found for VK ID {vk_id}",
            )

        if label and label.casefold() != entry.label.casefold():
            return VKMentionValidationResult(
                valid=False,
                entry=entry,
                reason=f"Label mismatch: expected {label!r}, registry has {entry.label!r}",
                markup=entry.mention_markup(),
            )

        markup = build_vk_mention_markup(entry.vk_id, entry.label, entry.type)
        return VKMentionValidationResult(
            valid=True,
            entry=entry,
            reason="Verified entry found",
            markup=markup,
        )

    async def validate_mention_markup(self, markup: str) -> VKMentionValidationResult:
        """Validate an existing VK mention markup string.

        Args:
            markup: VK mention markup like "[club241261191|Полюбить Калининград]"

        Returns:
            VKMentionValidationResult with validation status
        """
        parsed = parse_vk_mention_markup(markup)
        if not parsed:
            return VKMentionValidationResult(
                valid=False,
                entry=None,
                reason=f"Invalid VK mention markup format: {markup!r}",
            )

        prefix, vk_id, label = parsed
        type_ = "community" if prefix in ("club", "public") else "user"
        return await self.validate_mention(vk_id, type_, label)

    async def get_mention_markup(
        self,
        vk_id: str,
        type_: Literal["community", "user"],
    ) -> Optional[str]:
        """Get the canonical VK mention markup for a verified entry.

        Args:
            vk_id: Numeric VK ID
            type_: "community" or "user"

        Returns:
            Markup string like "[club241261191|Label]" or None if not verified
        """
        registry = await self._ensure_registry()
        entry = registry.get_verified_by_vk_id(vk_id, type_)
        if entry:
            return entry.mention_markup()
        return None

    async def list_verified_mentions(self, limit: int = 50) -> list[VKMentionEntry]:
        """List all verified VK mentions (for debugging/admin)."""
        registry = await self._ensure_registry()
        entries = registry.all_verified()
        return entries[:max(1, min(limit, 100))]

    async def refresh_registry(self, force: bool = False) -> bool:
        """Force refresh the registry from remote source."""
        registry = await self._ensure_registry()
        return await refresh_registry(registry, force=force)


async def build_mentions_for_text(
    text: str,
    guide_names: Sequence[str] = (),
    organizer_names: Sequence[str] = (),
    venue_names: Sequence[str] = (),
    *,
    max_mentions: int = 3,
    exclude_self_vk_id: Optional[str] = None,
    exclude_self_type: Literal["community", "user"] = "community",
    registry: Optional[VKMentionsRegistry] = None,
) -> tuple[str, list[VKMentionEntry]]:
    """Build VK mention markups for relevant entities in text.

    This is the main LLM-facing function that takes event/guide text and
    candidate names, searches the registry for verified matches, and returns
    the text with up to max_mentions appended as VK raw markup.

    Args:
        text: The source text (event description, guide digest, etc.)
        guide_names: Names of guides from guide_names field
        organizer_names: Names of organizers from organizer_names field
        venue_names: Names of venues from location/venue fields
        max_mentions: Maximum mentions to add (default 3)
        exclude_self_vk_id: VK ID to exclude (e.g., author's own VK ID)
        exclude_self_type: Type of the excluded ID
        registry: Optional pre-loaded registry

    Returns:
        Tuple of (text_with_mentions, list_of_used_entries)
    """
    if registry is None:
        registry = await load_or_create_registry()

    all_candidates = []
    for name in (*guide_names, *organizer_names, *venue_names):
        name = name.strip()
        if name:
            all_candidates.append(name)

    seen_entries = set()
    used_entries = []
    mention_markups = []

    for candidate in all_candidates:
        if len(used_entries) >= max_mentions:
            break

        results = registry.search_verified(candidate, limit=1)
        for entry in results:
            if entry.id in seen_entries:
                continue
            if exclude_self_vk_id and entry.vk_id == exclude_self_vk_id and entry.type == exclude_self_type:
                continue

            if _is_relevant_to_text(entry, text):
                mention_markups.append(entry.mention_markup())
                used_entries.append(entry)
                seen_entries.add(entry.id)
                break

    if mention_markups:
        separator = " "
        if text and not text.rstrip().endswith("\n"):
            separator = "\n"
        text = text.rstrip() + separator + separator.join(mention_markups)

    return text, used_entries


def _is_relevant_to_text(entry: VKMentionEntry, text: str) -> bool:
    """Check if a mention entry is relevant to the given text."""
    text_lower = text.casefold()
    label_lower = entry.label.casefold()
    if label_lower in text_lower:
        return True
    for alias in entry.aliases:
        if alias.casefold() in text_lower:
            return True
    return False


async def build_guide_vk_digest_text_with_mentions(
    base_text: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    max_mentions: int = 3,
    registry: Optional[VKMentionsRegistry] = None,
) -> str:
    """Enhance guide VK digest text with verified VK mentions.

    Scans all rows for guide_names, organizer_names, and venue/city fields,
    searches the registry for verified matches, and appends up to max_mentions
    VK mention markups to the end of the text.

    Args:
        base_text: The base VK digest text from build_guide_vk_digest_text
        rows: The digest rows containing guide/organizer/venue info
        max_mentions: Maximum mentions to append (default 3)
        registry: Optional pre-loaded registry

    Returns:
        Enhanced text with VK mention markups appended
    """
    if registry is None:
        registry = await load_or_create_registry()

    all_guide_names = []
    all_organizer_names = []
    all_venue_names = []

    for row in rows:
        guide_names = row.get("guide_names") or []
        if isinstance(guide_names, list):
            all_guide_names.extend(collapse_ws(n) for n in guide_names if collapse_ws(n))

        organizer_names = row.get("organizer_names") or []
        if isinstance(organizer_names, list):
            all_organizer_names.extend(collapse_ws(n) for n in organizer_names if collapse_ws(n))

        city = row.get("city")
        if city:
            all_venue_names.append(collapse_ws(city))

        meeting_point = row.get("meeting_point") or row.get("meeting_point_line")
        if meeting_point:
            all_venue_names.append(collapse_ws(meeting_point))

    seen_entries = set()
    mention_markups = []

    for candidate in (*all_guide_names, *all_organizer_names, *all_venue_names):
        if len(mention_markups) >= max_mentions:
            break

        results = registry.search_verified(candidate, limit=1)
        for entry in results:
            if entry.id in seen_entries:
                continue
            if _is_relevant_to_text(entry, base_text):
                mention_markups.append(entry.mention_markup())
                seen_entries.add(entry.id)
                break

    if mention_markups:
        separator = "\n"
        if base_text and not base_text.rstrip().endswith("\n"):
            separator = "\n"
        base_text = base_text.rstrip() + separator + separator.join(mention_markups)

    return base_text


__all__ = [
    "VKMentionSearchResult",
    "VKMentionValidationResult",
    "VKMentionsMCPTools",
    "build_mentions_for_text",
    "build_guide_vk_digest_text_with_mentions",
    "build_vk_mention_markup",
    "escape_vk_mention_text",
    "parse_vk_mention_markup",
]