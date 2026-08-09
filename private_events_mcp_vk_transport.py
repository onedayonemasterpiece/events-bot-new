"""Production-safe VK multipart and story-media transports for Private MCP."""
from __future__ import annotations

import asyncio
import hashlib
import io
import ipaddress
import json
import socket
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urlsplit

from private_events_mcp_vk_upload import (
    VKMultipartUploadResult,
    VKStoryMediaMaterialization,
    VKUploadPurpose,
)

_VK_SUFFIXES = (
    ".userapi.com",
    ".vk.com",
    ".vk.me",
    ".vkuser.net",
    ".vkuseraudio.net",
    ".vkvideo.ru",
)
_IMAGE_MIMES = {"JPEG": "image/jpeg", "PNG": "image/png", "WEBP": "image/webp"}


class VKMediaTransportError(RuntimeError):
    """Sanitized provider media transport failure."""


def _validated_url(value: str) -> tuple[str, int]:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 4096
        or "\\" in value
        or any(ch.isspace() or ord(ch) < 0x20 or ord(ch) == 0x7F for ch in value)
    ):
        raise VKMediaTransportError("VK media URL is invalid")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise VKMediaTransportError("VK media URL is invalid") from exc
    host = (parsed.hostname or "").casefold()
    if (
        parsed.scheme != "https"
        or not host
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or port not in {None, 443}
        or not any(host == suffix[1:] or host.endswith(suffix) for suffix in _VK_SUFFIXES)
    ):
        raise VKMediaTransportError("VK media URL is invalid")
    return host, 443


async def _public_addresses(host: str, port: int) -> tuple[str, ...]:
    try:
        records = await asyncio.get_running_loop().getaddrinfo(
            host, port, type=socket.SOCK_STREAM
        )
    except OSError as exc:
        raise VKMediaTransportError("VK media DNS failed") from exc
    values: list[str] = []
    for record in records:
        try:
            ip = ipaddress.ip_address(record[4][0].split("%", 1)[0])
        except (ValueError, IndexError, TypeError) as exc:
            raise VKMediaTransportError("VK media DNS failed") from exc
        if not ip.is_global or any(
            (
                ip.is_private,
                ip.is_loopback,
                ip.is_link_local,
                ip.is_multicast,
                ip.is_reserved,
                ip.is_unspecified,
            )
        ):
            raise VKMediaTransportError("VK media DNS is not public")
        canonical = str(ip)
        if canonical not in values:
            values.append(canonical)
        if len(values) > 16:
            raise VKMediaTransportError("VK media DNS returned too many addresses")
    if not values:
        raise VKMediaTransportError("VK media DNS failed")
    return tuple(values)


class _PinnedResolver:
    def __init__(self, host: str, addresses: Sequence[str]) -> None:
        self.host = host
        self.addresses = tuple(addresses)

    async def resolve(
        self, host: str, port: int = 0, family: int = socket.AF_UNSPEC
    ) -> list[dict[str, Any]]:
        if host != self.host:
            raise OSError("unexpected DNS lookup")
        return [
            {
                "hostname": host,
                "host": address,
                "port": port,
                "family": socket.AF_INET6
                if ipaddress.ip_address(address).version == 6
                else socket.AF_INET,
                "proto": 0,
                "flags": socket.AI_NUMERICHOST,
            }
            for address in self.addresses
        ]

    async def close(self) -> None:
        return None


async def _session_for(url: str, timeout_seconds: float) -> tuple[Any, Any]:
    import aiohttp

    host, port = _validated_url(url)
    addresses = await _public_addresses(host, port)
    connector = aiohttp.TCPConnector(
        resolver=_PinnedResolver(host, addresses),
        use_dns_cache=False,
        ttl_dns_cache=0,
        limit=1,
    )
    session = aiohttp.ClientSession(
        connector=connector,
        cookie_jar=aiohttp.DummyCookieJar(),
        auto_decompress=False,
        trust_env=False,
        timeout=aiohttp.ClientTimeout(total=timeout_seconds),
        headers={"User-Agent": "events-bot-private-vk-media/1"},
    )
    return session, connector


class SecureVKMultipartTransport:
    """Fixed-purpose, redirect-free VK multipart uploader."""

    def __init__(self, *, max_response_bytes: int = 64 * 1024) -> None:
        self.max_response_bytes = max(1024, min(int(max_response_bytes), 256 * 1024))

    async def upload(
        self,
        *,
        purpose: VKUploadPurpose,
        upload_url: str,
        content: bytes,
        filename: str,
        mime_type: str,
        timeout_seconds: float,
    ) -> VKMultipartUploadResult:
        import aiohttp

        if not isinstance(purpose, VKUploadPurpose):
            raise VKMediaTransportError("VK upload purpose is invalid")
        if type(content) is not bytes or not 1 <= len(content) <= 64 * 1024 * 1024:
            raise VKMediaTransportError("VK upload content is invalid")
        if mime_type not in {"image/jpeg", "image/png", "image/webp", "video/mp4"}:
            raise VKMediaTransportError("VK upload MIME is invalid")
        field = "photo" if purpose is VKUploadPurpose.WALL_PHOTO else "file"
        session, _connector = await _session_for(upload_url, timeout_seconds)
        try:
            form = aiohttp.FormData()
            form.add_field(
                field,
                content,
                filename="asset." + filename.rsplit(".", 1)[-1].casefold(),
                content_type=mime_type,
            )
            async with session.post(upload_url, data=form, allow_redirects=False) as response:
                if response.status != 200 or 300 <= response.status < 400:
                    raise VKMediaTransportError("VK upload failed")
                announced = response.headers.get("Content-Length")
                if announced is not None:
                    try:
                        if int(announced) > self.max_response_bytes:
                            raise VKMediaTransportError("VK upload response is too large")
                    except ValueError as exc:
                        raise VKMediaTransportError("VK upload response is invalid") from exc
                raw = await response.content.read(self.max_response_bytes + 1)
                if len(raw) > self.max_response_bytes:
                    raise VKMediaTransportError("VK upload response is too large")
        finally:
            await session.close()
        try:
            payload = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise VKMediaTransportError("VK upload response is invalid") from exc
        if not isinstance(payload, Mapping):
            raise VKMediaTransportError("VK upload response is invalid")
        nested = payload.get("response")
        data: Mapping[str, Any] = nested if isinstance(nested, Mapping) else payload
        if purpose is VKUploadPurpose.WALL_PHOTO:
            server = data.get("server")
            photo = data.get("photo")
            upload_hash = data.get("hash")
            if type(server) is not int or not isinstance(photo, str) or not isinstance(upload_hash, str):
                raise VKMediaTransportError("VK upload response is invalid")
            return VKMultipartUploadResult(
                server=server, photo=photo[:65536], upload_hash=upload_hash[:8192]
            )
        upload_result = data.get("upload_result")
        if not isinstance(upload_result, str) or not 8 <= len(upload_result) <= 8192:
            raise VKMediaTransportError("VK upload response is invalid")
        return VKMultipartUploadResult(story_upload_result=upload_result)


class SecureVKStoryMediaReader:
    """Read a bounded VK story image without forwarding credentials or redirects."""

    async def fetch_vk_cdn(
        self,
        *,
        source_url: str,
        owner_binding: str,
        max_bytes: int,
        timeout_seconds: float,
    ) -> VKStoryMediaMaterialization:
        if not isinstance(owner_binding, str) or len(owner_binding) != 64:
            raise VKMediaTransportError("VK media owner binding is invalid")
        if type(max_bytes) is not int or not 1 <= max_bytes <= 30 * 1024 * 1024:
            raise VKMediaTransportError("VK media bound is invalid")
        session, _connector = await _session_for(source_url, timeout_seconds)
        try:
            async with session.get(source_url, allow_redirects=False) as response:
                if response.status != 200 or 300 <= response.status < 400:
                    raise VKMediaTransportError("VK media read failed")
                chunks: list[bytes] = []
                total = 0
                async for chunk in response.content.iter_chunked(64 * 1024):
                    total += len(chunk)
                    if total > max_bytes:
                        raise VKMediaTransportError("VK media exceeds byte limit")
                    chunks.append(bytes(chunk))
                content = b"".join(chunks)
        finally:
            await session.close()
        try:
            from PIL import Image

            with Image.open(io.BytesIO(content)) as image:
                fmt = (image.format or "").upper()
                image.verify()
        except Exception as exc:
            raise VKMediaTransportError("VK story media is not a supported image") from exc
        mime = _IMAGE_MIMES.get(fmt)
        if mime is None:
            raise VKMediaTransportError("VK story media is not a supported image")
        return VKStoryMediaMaterialization(
            mime_type=mime,
            content_digest="sha256:" + hashlib.sha256(content).hexdigest(),
            byte_length=len(content),
            content=content,
        )


__all__ = [
    "SecureVKMultipartTransport",
    "SecureVKStoryMediaReader",
    "VKMediaTransportError",
]
