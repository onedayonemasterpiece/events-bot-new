"""Narrow integration contracts for VK media materialization and multipart upload.

The workspace adapter never treats a storage reference as a path or URL.  The
application injects a principal-bound asset reader and a multipart transport.
Only the fixed upload purposes below are expressible; callers cannot supply a VK
method name or multipart field name.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol


class VKUploadPurpose(str, Enum):
    WALL_PHOTO = "wall_photo"
    STORY_PHOTO = "story_photo"
    STORY_VIDEO = "story_video"


@dataclass(frozen=True, slots=True)
class VKAssetMaterialization:
    """Principal-bound bytes returned by the server-owned media store."""

    storage_ref: str
    owner_binding: str
    content_digest: str
    mime_type: str
    byte_length: int
    content: bytes


class VKVerifiedAssetReader(Protocol):
    """Resolve an opaque storage reference without interpreting it as a path."""

    async def open_verified(
        self, storage_ref: str, owner_binding: str
    ) -> VKAssetMaterialization: ...


@dataclass(frozen=True, slots=True)
class VKMultipartUploadResult:
    """Normalized result of one fixed-purpose VK multipart request."""

    server: int | None = None
    photo: str | None = None
    upload_hash: str | None = None
    story_upload_result: str | None = None


class VKMultipartTransport(Protocol):
    """Perform multipart I/O only for a server-selected, fixed VK purpose.

    ``upload_url`` is obtained by the adapter from the corresponding fixed VK
    upload-server API method.  It is never accepted from an MCP request or an
    asset descriptor.  Implementations must not follow redirects outside the
    validated VK upload host.
    """

    async def upload(
        self,
        *,
        purpose: VKUploadPurpose,
        upload_url: str,
        content: bytes,
        filename: str,
        mime_type: str,
        timeout_seconds: float,
    ) -> VKMultipartUploadResult: ...


@dataclass(frozen=True, slots=True)
class VKStoryMediaMaterialization:
    """Bounded bytes fetched from a validated VK CDN story-media reference."""

    mime_type: str
    content_digest: str
    byte_length: int
    content: bytes


class VKStoryMediaReader(Protocol):
    """Closed VK-CDN reader used by ``VKWorkspaceAdapter.read_asset`` only."""

    async def fetch_vk_cdn(
        self,
        *,
        source_url: str,
        owner_binding: str,
        max_bytes: int,
        timeout_seconds: float,
    ) -> VKStoryMediaMaterialization: ...


__all__ = [
    "VKAssetMaterialization",
    "VKMultipartTransport",
    "VKMultipartUploadResult",
    "VKStoryMediaMaterialization",
    "VKStoryMediaReader",
    "VKUploadPurpose",
    "VKVerifiedAssetReader",
]
