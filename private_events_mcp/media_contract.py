"""Provider-neutral contract for authenticated ChatGPT file ingestion.

The core runtime deliberately does not download caller supplied URLs.  A
production binding must inject an :class:`AssetIngestor` which authenticates
the ChatGPT file object, copies and verifies bounded bytes, and returns only a
server-owned opaque storage reference plus measured metadata.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class ChatGPTFile:
    """The exact file object supplied for an ``openai/fileParams`` field."""

    download_url: str
    file_id: str
    mime_type: str | None = None
    file_name: str | None = None


@dataclass(frozen=True, slots=True)
class VerifiedAsset:
    """A byte-verified, principal-owned asset safe to hand to an adapter.

    ``storage_ref`` is an opaque server-minted reference, never a URL or a
    caller-provided file identifier.  ``owner_binding`` is an irreversible
    binding supplied by the runtime and repeated by the ingestor so a result
    cannot be replayed across OAuth principals/resources.
    """

    storage_ref: str
    owner_binding: str
    content_digest: str
    mime_type: str
    byte_length: int
    expires_at: int
    width: int | None = None
    height: int | None = None


class AssetIngestor(Protocol):
    """Authenticated outbound/storage boundary injected into the core."""

    async def ingest(
        self,
        file: ChatGPTFile,
        *,
        owner_binding: str,
        max_bytes: int,
        expires_at: int,
    ) -> VerifiedAsset: ...


__all__ = ["AssetIngestor", "ChatGPTFile", "VerifiedAsset"]
