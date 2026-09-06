"""Event-only image staging over the existing durable secure media store.

No provider staging, public uploads, event writes, or in-memory reference map.
The host owns policy and must retain the store directory and binding key.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import re
import time
from collections.abc import Awaitable, Callable
from typing import Any

from .media_contract import AssetIngestor, ChatGPTFile, VerifiedAsset
from .tool_catalog import ToolCallContext, ToolExecutionError

_REF = re.compile(r"^ing_[A-Za-z0-9_-]{24,160}$")
_DIGEST = re.compile(r"^sha256:[a-f0-9]{64}$")
_IMAGE_TYPES = frozenset({"image/jpeg", "image/png", "image/webp"})


def _error(code: str = "EVENT_ASSET_INVALID") -> ToolExecutionError:
    return ToolExecutionError(code, "Event image is unavailable or not authorized.", retry_safe=False)


class EventAssetService:
    def __init__(
        self, *, ingestor: AssetIngestor, binding_key: str,
        authorize: Callable[[ToolCallContext, str], Awaitable[bool]],
        max_bytes: int = 10 * 1024 * 1024, ttl_seconds: int = 3600,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if not callable(authorize) or not isinstance(binding_key, str) or len(binding_key) < 32:
            raise ValueError("current authorization and a stable binding key are required")
        if (isinstance(max_bytes, bool) or not isinstance(max_bytes, int)
                or not 1 <= max_bytes <= 30 * 1024 * 1024):
            raise ValueError("max_bytes must be between 1 and 30 MiB")
        if (isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, int)
                or not 1 <= ttl_seconds <= 86400):
            raise ValueError("ttl_seconds must be between 1 and 86400")
        self.ingestor = ingestor
        self._key = binding_key.encode()
        self._authorize_callback = authorize
        self.max_bytes = max_bytes
        self.ttl_seconds = ttl_seconds
        self.clock = clock

    async def _authorize(self, context: ToolCallContext, action: str) -> str:
        identity = context.identity
        if not context.resource or context.resource != identity.audience:
            raise _error("EVENT_ASSET_ACCESS_DENIED")
        try:
            allowed = await self._authorize_callback(context, action)
        except Exception:
            raise _error("EVENT_ASSET_ACCESS_DENIED") from None
        if allowed is not True:
            raise _error("EVENT_ASSET_ACCESS_DENIED")
        return self._binding(identity.subject, identity.client_id, context.resource)

    def _binding(self, subject: str, client_id: str, audience: str) -> str:
        if any(not isinstance(value, str) or not 1 <= len(value) <= 2048
               for value in (subject, client_id, audience)):
            raise _error("EVENT_ASSET_ACCESS_DENIED")
        # Shared by HTTP and durable execution; no synthetic OAuth identity.
        payload = json.dumps(["event-image-v1", client_id, subject, audience],
                             ensure_ascii=False, separators=(",", ":"))
        return hmac.new(self._key, payload.encode(), hashlib.sha256).hexdigest()

    def _validate(self, asset: VerifiedAsset, binding: str) -> VerifiedAsset:
        if (not isinstance(asset, VerifiedAsset)
                or not _REF.fullmatch(asset.storage_ref)
                or not hmac.compare_digest(asset.owner_binding, binding)
                or not _DIGEST.fullmatch(asset.content_digest)
                or asset.role != "image" or asset.mime_type not in _IMAGE_TYPES
                or isinstance(asset.byte_length, bool) or not isinstance(asset.byte_length, int)
                or not 1 <= asset.byte_length <= self.max_bytes
                or not isinstance(asset.expires_at, int) or isinstance(asset.expires_at, bool)
                or asset.expires_at <= int(self.clock())
                or any(isinstance(v, bool) or not isinstance(v, int) or v <= 0
                       for v in (asset.width, asset.height))):
            raise _error()
        return asset

    @staticmethod
    def _public(asset: VerifiedAsset) -> dict[str, Any]:
        return {"asset_ref": asset.storage_ref, "content_digest": asset.content_digest,
                "mime_type": asset.mime_type, "byte_length": asset.byte_length,
                "width": asset.width, "height": asset.height,
                "expires_at": asset.expires_at, "role": "image"}

    async def stage(self, file: ChatGPTFile, context: ToolCallContext) -> dict[str, Any]:
        binding = await self._authorize(context, "stage")
        if not isinstance(file, ChatGPTFile):
            raise _error()
        expires_at = int(self.clock()) + self.ttl_seconds
        try:
            asset = await self.ingestor.ingest(
                file, owner_binding=binding, max_bytes=self.max_bytes,
                expires_at=expires_at, role="event_image",
            )
            self._validate(asset, binding)
            if asset.expires_at > expires_at:
                raise _error()
        except Exception:
            raise _error() from None
        await self._authorize(context, "stage")
        return self._public(asset)

    async def _resolve(
        self, asset_ref: str, context: ToolCallContext, *, action: str,
        expected_digest: str | None = None,
    ) -> VerifiedAsset:
        binding = await self._authorize(context, action)
        asset = await self._reverify_bound(asset_ref, binding, expected_digest=expected_digest)
        await self._authorize(context, action)
        return asset

    async def _reverify_bound(
        self, asset_ref: str, binding: str, *, expected_digest: str | None,
    ) -> VerifiedAsset:
        if not isinstance(asset_ref, str) or not _REF.fullmatch(asset_ref):
            raise _error()
        try:
            asset = await asyncio.to_thread(
                self.ingestor.reverify, asset_ref, owner_binding=binding,
                max_bytes=self.max_bytes, role="image",
            )
            self._validate(asset, binding)
            if asset.storage_ref != asset_ref:
                raise _error()
            if expected_digest is not None and not hmac.compare_digest(
                asset.content_digest, expected_digest
            ):
                raise _error("EVENT_ASSET_DIGEST_MISMATCH")
        except ToolExecutionError:
            raise
        except Exception:
            raise _error() from None
        return asset

    async def read(self, asset_ref: str, context: ToolCallContext) -> dict[str, Any]:
        return self._public(await self._resolve(asset_ref, context, action="read"))

    async def reverify(
        self, asset_ref: str, context: ToolCallContext, *, expected_digest: str,
    ) -> VerifiedAsset:
        """Internal mutation-boundary result; never serialize owner_binding."""
        if not isinstance(expected_digest, str) or not _DIGEST.fullmatch(expected_digest):
            raise _error("EVENT_ASSET_DIGEST_MISMATCH")
        return await self._resolve(asset_ref, context, action="use", expected_digest=expected_digest)


    async def read_durable(
        self, asset_ref: str, *, expected_digest: str, actor_subject: str,
        actor_client_id: str, actor_audience: str,
        authorize: Callable[[], Awaitable[bool]],
    ) -> tuple[bytes, str]:
        """Read exact staged bytes for a stored actor without fabricating tokens.

        The callback must check current durable-operation policy. No original
        HTTP token is needed; expiry of the staged image is never extended.
        """
        if not callable(authorize):
            raise ValueError("current durable-operation authorization is required")

        async def check() -> None:
            try:
                permitted = await authorize()
            except Exception:
                raise _error("EVENT_ASSET_ACCESS_DENIED") from None
            if permitted is not True:
                raise _error("EVENT_ASSET_ACCESS_DENIED")

        await check()
        binding = self._binding(actor_subject, actor_client_id, actor_audience)
        if not isinstance(expected_digest, str) or not _DIGEST.fullmatch(expected_digest):
            raise _error("EVENT_ASSET_DIGEST_MISMATCH")
        asset = await self._reverify_bound(asset_ref, binding, expected_digest=expected_digest)
        await check()
        opener = getattr(self.ingestor, "open_verified", None)
        if not callable(opener):
            raise _error()

        def read_bytes() -> bytes:
            with opener(asset_ref, binding) as (stream, metadata):
                self._validate(metadata, binding)
                if self._public(metadata) != self._public(asset):
                    raise _error()
                content = stream.read(self.max_bytes + 1)
                if (not isinstance(content, bytes) or len(content) != asset.byte_length
                        or len(content) > self.max_bytes):
                    raise _error()
                # Also hash the actual returned bytes, closing the gap between
                # store pre-read verification and a changed file during reading.
                digest = "sha256:" + hashlib.sha256(content).hexdigest()
                if not hmac.compare_digest(digest, expected_digest):
                    raise _error("EVENT_ASSET_DIGEST_MISMATCH")
                return content

        await check()
        try:
            content = await asyncio.to_thread(read_bytes)
        except ToolExecutionError:
            raise
        except Exception:
            raise _error() from None
        await check()
        self._validate(asset, binding)  # expiry may have crossed during I/O
        extension = {"image/jpeg": "jpg", "image/png": "png", "image/webp": "webp"}[asset.mime_type]
        return content, "event-image-" + expected_digest[7:23] + "." + extension
