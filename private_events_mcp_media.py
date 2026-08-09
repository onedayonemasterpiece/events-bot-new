"""Hardened, owner-bound media ingress for the private-events MCP service.

The store deliberately accepts only images which Pillow can fully verify.  In
particular, merely having an image-ish extension or Content-Type is never
enough.  Video is rejected until the application has a real bounded container
and codec validator.

The module has no dependency on the MCP implementation except for a lazy
import of ``private_events_mcp.media_contract.VerifiedAsset``.  This keeps it
usable by the integration branch while still structurally implementing its
``AssetIngestor`` protocol.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import inspect
import ipaddress
import os
import re
import secrets
import socket
import sqlite3
import stat
import threading
import time
from collections.abc import AsyncIterator, Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any,
    BinaryIO,
    ClassVar,
    Protocol,
)
from urllib.parse import parse_qsl, urlsplit


class MediaStoreError(RuntimeError):
    """Base class for deliberately non-detailed media-store failures."""

    error_code = "WORKSPACE_NOT_BOUND"
    audit_reason_code = "workspace_not_bound"


class MediaIngressRejected(MediaStoreError):
    """The remote input did not satisfy the ingress security contract."""

    _MESSAGE_CODES: ClassVar[dict[str, str]] = {
        "invalid download URL": "FILE_REF_UNRESOLVED",
        "numeric download hosts are forbidden": "FILE_REF_UNRESOLVED",
        "download host is not allowlisted": "FILE_HOST_NOT_ALLOWED",
        "Azure blob URL requires a current read-only SAS": (
            "FILE_SOURCE_NOT_AUTHORIZED"
        ),
        "DNS resolution failed": "FILE_FETCH_FAILED",
        "DNS returned an invalid answer": "FILE_FETCH_FAILED",
        "DNS returned a non-public address": "FILE_FETCH_FAILED",
        "DNS returned too many answers": "FILE_FETCH_FAILED",
        "DNS returned no usable answers": "FILE_FETCH_FAILED",
        "invalid file identity": "FILE_REF_UNRESOLVED",
        "unsupported media role": "MIME_NOT_ALLOWED",
        "invalid byte limit": "WORKSPACE_NOT_BOUND",
        "invalid expiry": "WORKSPACE_NOT_BOUND",
        "expiry must be in the next 24 hours": "FILE_EXPIRED",
        "media type is not permitted for this role": "MIME_NOT_ALLOWED",
        "claimed media type does not match content": "MIME_NOT_ALLOWED",
        "media download timed out": "FILE_FETCH_FAILED",
        "redirect responses are forbidden": "FILE_FETCH_FAILED",
        "media download failed": "FILE_FETCH_FAILED",
        "invalid Content-Length": "FILE_FETCH_FAILED",
        "media exceeds byte limit": "FILE_TOO_LARGE",
        "invalid media response chunk": "FILE_FETCH_FAILED",
        "empty media response": "FILE_FETCH_FAILED",
        "HTTP response is not streamable": "FILE_FETCH_FAILED",
        "unsupported media format": "MIME_NOT_ALLOWED",
        "invalid image dimensions": "MIME_NOT_ALLOWED",
        "image dimensions exceed limits": "FILE_TOO_LARGE",
        "image validation failed": "MIME_NOT_ALLOWED",
        "media store capacity exceeded": "WORKSPACE_NOT_BOUND",
    }

    def __init__(
        self,
        message: str,
        *,
        error_code: str | None = None,
        audit_detail: str | None = None,
    ) -> None:
        super().__init__(message)
        selected = error_code or self._MESSAGE_CODES.get(message, "FILE_FETCH_FAILED")
        if not re.fullmatch(r"[A-Z][A-Z0-9_]{2,63}", selected):
            selected = "FILE_FETCH_FAILED"
        self.error_code = selected
        detail = (
            audit_detail
            if isinstance(audit_detail, str)
            and re.fullmatch(r"[a-z0-9]{6,32}", audit_detail)
            else None
        )
        self.audit_reason_code = selected.lower() + (
            "_" + detail if detail is not None else ""
        )


class MediaNotFound(MediaStoreError):
    """The opaque reference is unknown."""

    error_code = "FILE_REF_UNRESOLVED"
    audit_reason_code = "file_ref_unresolved"


class MediaOwnershipError(MediaStoreError):
    """The reference is not bound to the requesting principal."""

    error_code = "FILE_PRINCIPAL_MISMATCH"
    audit_reason_code = "file_principal_mismatch"


class MediaExpired(MediaStoreError):
    """The asset is no longer usable."""

    error_code = "FILE_EXPIRED"
    audit_reason_code = "file_expired"


class MediaIntegrityError(MediaStoreError):
    """The stored bytes or filesystem object failed integrity checks."""

    error_code = "FILE_INTEGRITY_FAILED"
    audit_reason_code = "file_integrity_failed"


def _host_audit_fingerprint(host: str) -> str:
    """Fingerprint a dynamic host and its stable DNS suffixes without disclosure."""

    labels = host.split(".")
    values = [host]
    values.append(".".join(labels[-2:]) if len(labels) >= 2 else host)
    values.append(".".join(labels[-3:]) if len(labels) >= 3 else host)
    return "".join(
        hashlib.sha256(value.encode("ascii")).hexdigest()[:8] for value in values
    )


@dataclass(frozen=True, slots=True)
class StoredMediaAsset:
    """Internal verified metadata.

    Both the opaque capability and the filesystem path are excluded from repr
    so exception/debug rendering cannot turn them into accidental disclosures.
    The path is never returned by the public contract adapter.
    """

    storage_ref: str = field(repr=False)
    _path: Path = field(repr=False)
    sha256: str
    detected_mime: str
    bytes: int
    width: int | None
    height: int | None
    duration: float | None
    created_at: int
    expires_at: int

    @property
    def content_digest(self) -> str:
        return self.sha256

    @property
    def mime_type(self) -> str:
        return self.detected_mime

    @property
    def byte_length(self) -> int:
        return self.bytes


@dataclass(frozen=True, slots=True)
class _FallbackVerifiedAsset:
    """Standalone fallback matching the integration contract exactly."""

    storage_ref: str = field(repr=False)
    owner_binding: str = field(repr=False)
    content_digest: str
    mime_type: str
    byte_length: int
    expires_at: int
    width: int | None = None
    height: int | None = None


class _Resolver(Protocol):
    def __call__(self, host: str, port: int) -> Any: ...


class _Fetcher(Protocol):
    def __call__(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        allowed_ips: Sequence[str],
        timeout_seconds: float,
    ) -> Any: ...


_REF_RE = re.compile(r"^ing_[A-Za-z0-9_-]{24,160}$")
_FILE_RE = re.compile(r"^[0-9a-f]{64}\.asset$")
_OWNER_BINDING_RE = re.compile(r"^[0-9a-f]{64}$")
_NUMERIC_HOST_RE = re.compile(
    r"^(?:0x[0-9a-f]+|[0-9]+|[0-9.]+|(?:0[0-7]+\.){1,3}0[0-7]+)$", re.IGNORECASE
)
_FORMAT_MIME = {"JPEG": "image/jpeg", "PNG": "image/png", "WEBP": "image/webp"}
_IMAGE_MIMES = frozenset(_FORMAT_MIME.values())
_GENERIC_MIMES = frozenset({"", "application/octet-stream", "binary/octet-stream"})
_ROLE_MATRIX = {
    "story_image": _IMAGE_MIMES,
    "event_image": _IMAGE_MIMES,
    "story_media": _IMAGE_MIMES,
}


def _normalise_host(host: str) -> str:
    if not host or any(ord(char) <= 32 or ord(char) == 127 for char in host):
        raise ValueError("invalid host")
    host = host.removesuffix(".")
    try:
        normalised = host.encode("idna").decode("ascii").lower()
    except UnicodeError as exc:
        raise ValueError("invalid host") from exc
    if not normalised or len(normalised) > 253:
        raise ValueError("invalid host")
    return normalised


def _mime_without_parameters(value: str | None) -> str:
    return (value or "").split(";", 1)[0].strip().lower()


class _PinnedAiohttpResolver:
    """aiohttp resolver which can only return the addresses already checked."""

    def __init__(self, host: str, addresses: Sequence[str]) -> None:
        self._host = host
        self._addresses = tuple(addresses)

    async def resolve(
        self, host: str, port: int = 0, family: int = socket.AF_UNSPEC
    ) -> list[dict[str, Any]]:
        if host != self._host:
            raise OSError("unexpected DNS lookup")
        answers: list[dict[str, Any]] = []
        for address in self._addresses:
            ip = ipaddress.ip_address(address)
            answers.append(
                {
                    "hostname": host,
                    "host": address,
                    "port": port,
                    "family": socket.AF_INET6 if ip.version == 6 else socket.AF_INET,
                    "proto": 0,
                    "flags": socket.AI_NUMERICHOST,
                }
            )
        return answers

    async def close(self) -> None:
        return None


class SecureMediaAssetStore:
    """Secure image-only implementation of the AssetIngestor contract."""

    def __init__(
        self,
        root: str | os.PathLike[str] | None = None,
        *,
        root_dir: str | os.PathLike[str] | None = None,
        allowed_hosts: Sequence[str],
        resolver: _Resolver | None = None,
        http_fetch: _Fetcher | None = None,
        max_asset_bytes: int = 30 * 1024 * 1024,
        max_store_bytes: int = 128 * 1024 * 1024,
        ttl_seconds: int = 60 * 60,
        timeout_seconds: float = 20.0,
        max_width: int = 8_192,
        max_height: int = 8_192,
        max_pixels: int = 40_000_000,
        clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if (root is None) == (root_dir is None):
            raise ValueError("provide exactly one of root or root_dir")
        if not allowed_hosts:
            raise ValueError("allowed_hosts must not be empty")
        hosts: set[str] = set()
        wildcard_suffixes: set[str] = set()
        try:
            for configured_host in allowed_hosts:
                if configured_host.startswith("*."):
                    wildcard_suffixes.add(_normalise_host(configured_host[2:]))
                else:
                    hosts.add(_normalise_host(configured_host))
        except (AttributeError, ValueError) as exc:
            raise ValueError("allowed_hosts contains an invalid hostname") from exc
        if not hosts and not wildcard_suffixes:
            raise ValueError("allowed_hosts must not be empty")
        if any(_NUMERIC_HOST_RE.fullmatch(host) for host in hosts | wildcard_suffixes):
            raise ValueError("numeric hosts are not supported")
        if (
            min(max_asset_bytes, max_store_bytes) <= 0
            or max_asset_bytes > max_store_bytes
        ):
            raise ValueError("invalid media byte limits")
        if timeout_seconds <= 0:
            raise ValueError("byte and time limits must be positive")
        if ttl_seconds <= 0 or ttl_seconds > 86_400:
            raise ValueError("ttl_seconds must be between 1 and 86400")
        if min(max_width, max_height, max_pixels) <= 0:
            raise ValueError("image dimension limits must be positive")

        selected_root = root if root is not None else root_dir
        assert selected_root is not None
        self._root = Path(selected_root)
        self._allowed_hosts = frozenset(hosts)
        self._allowed_wildcard_suffixes = frozenset(wildcard_suffixes)
        self._resolver = resolver or self._default_resolver
        self._http_fetch = http_fetch or self._default_http_fetch
        self._max_asset_bytes = int(max_asset_bytes)
        self._max_store_bytes = int(max_store_bytes)
        self._download_timeout = float(timeout_seconds)
        self._max_ttl = int(ttl_seconds)
        self._max_width = int(max_width)
        self._max_height = int(max_height)
        self._max_pixels = int(max_pixels)
        self._clock = clock
        self._monotonic = monotonic
        self._manifest_lock = threading.RLock()
        self._initialise_storage()

    def _initialise_storage(self) -> None:
        if self._root.exists() and self._root.is_symlink():
            raise MediaIntegrityError("storage root must not be a symlink")
        self._root.mkdir(mode=0o700, parents=True, exist_ok=True)
        root_stat = os.lstat(self._root)
        if not stat.S_ISDIR(root_stat.st_mode) or stat.S_ISLNK(root_stat.st_mode):
            raise MediaIntegrityError("invalid storage root")
        os.chmod(self._root, 0o700)

        self._key_path = self._root / ".owner_hmac.key"
        self._manifest_path = self._root / ".manifest.sqlite3"
        self._owner_key = self._load_or_create_secret(self._key_path)
        self._create_regular_exclusive(self._manifest_path, 0o600)
        self._validate_control_file(self._manifest_path, 0o600)
        with self._db() as db:
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS media_assets (
                    storage_ref TEXT PRIMARY KEY,
                    filename TEXT NOT NULL UNIQUE,
                    owner_mac TEXT NOT NULL,
                    file_id_mac TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    detected_mime TEXT NOT NULL,
                    byte_length INTEGER NOT NULL,
                    width INTEGER,
                    height INTEGER,
                    duration REAL,
                    created_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL
                )
                """
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS media_assets_expiry ON media_assets(expires_at)"
            )

    @staticmethod
    def _create_regular_exclusive(path: Path, mode: int) -> bool:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(path, flags, mode)
        except FileExistsError:
            return False
        try:
            os.fchmod(fd, mode)
            os.fsync(fd)
        finally:
            os.close(fd)
        return True

    def _load_or_create_secret(self, path: Path) -> bytes:
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(path, flags)
        except FileNotFoundError:
            temporary = self._root / (".key-init-" + secrets.token_hex(32))
            create_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            if hasattr(os, "O_NOFOLLOW"):
                create_flags |= os.O_NOFOLLOW
            temporary_fd = os.open(temporary, create_flags, 0o600)
            try:
                key = secrets.token_bytes(32)
                os.write(temporary_fd, key)
                os.fchmod(temporary_fd, 0o600)
                os.fsync(temporary_fd)
            finally:
                os.close(temporary_fd)
            try:
                # Publish only a complete key and never replace an existing
                # one.  Concurrent initialisers safely converge on the winner.
                os.link(temporary, path, follow_symlinks=False)
            except FileExistsError:
                pass
            finally:
                temporary.unlink()
            directory_fd = os.open(
                self._root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            )
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            fd = os.open(path, flags)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise MediaIntegrityError("invalid key file")
            os.fchmod(fd, 0o600)
            key = os.read(fd, 33)
            if len(key) != 32:
                raise MediaIntegrityError("invalid key file")
            return key
        finally:
            os.close(fd)

    @staticmethod
    def _validate_control_file(path: Path, mode: int) -> None:
        info = os.lstat(path)
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_ISLNK(info.st_mode)
            or info.st_nlink != 1
        ):
            raise MediaIntegrityError("invalid storage metadata file")
        os.chmod(path, mode)

    @contextlib.contextmanager
    def _db(self) -> Iterator[sqlite3.Connection]:
        self._validate_control_file(self._manifest_path, 0o600)
        db = sqlite3.connect(self._manifest_path, timeout=10)
        try:
            db.execute("PRAGMA journal_mode=DELETE")
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA secure_delete=ON")
            db.execute("PRAGMA temp_store=MEMORY")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    async def _default_resolver(host: str, port: int) -> list[str]:
        loop = asyncio.get_running_loop()
        records = await loop.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        return [record[4][0] for record in records]

    @contextlib.asynccontextmanager
    async def _default_http_fetch(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        allowed_ips: Sequence[str],
        timeout_seconds: float,
    ) -> AsyncIterator[Any]:
        # Imported lazily so test fakes and offline store inspection do not need
        # to initialise an HTTP stack.
        import aiohttp

        host = _normalise_host(urlsplit(url).hostname or "")
        connector = aiohttp.TCPConnector(
            resolver=_PinnedAiohttpResolver(host, allowed_ips),
            use_dns_cache=False,
            ttl_dns_cache=0,
            limit=1,
        )
        timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        async with (
            aiohttp.ClientSession(
                connector=connector,
                cookie_jar=aiohttp.DummyCookieJar(),
                auto_decompress=False,
                trust_env=False,
                timeout=timeout,
            ) as session,
            session.get(url, headers=dict(headers), allow_redirects=False) as response,
        ):
            yield response

    def _validate_url(self, url: str) -> tuple[str, int]:
        if (
            not isinstance(url, str)
            or not url
            or any(ord(char) <= 32 or ord(char) == 127 for char in url)
        ):
            raise MediaIngressRejected("invalid download URL")
        if "\\" in url:
            raise MediaIngressRejected("invalid download URL")
        try:
            parsed = urlsplit(url)
            port = parsed.port
            host = _normalise_host(parsed.hostname or "")
        except (ValueError, UnicodeError) as exc:
            raise MediaIngressRejected("invalid download URL") from exc
        if (
            parsed.scheme.lower() != "https"
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise MediaIngressRejected("invalid download URL")
        if (
            parsed.fragment
            or parsed.query
            and any(ord(c) <= 32 or ord(c) == 127 for c in parsed.query)
        ):
            raise MediaIngressRejected("invalid download URL")
        if port not in (None, 443):
            raise MediaIngressRejected("invalid download URL")
        # IP literals and legacy integer/octal/hex IPv4 spellings are rejected,
        # even when an operator accidentally adds one to the hostname allowlist.
        try:
            ipaddress.ip_address(host)
        except ValueError:
            pass
        else:
            raise MediaIngressRejected("numeric download hosts are forbidden")
        if _NUMERIC_HOST_RE.fullmatch(host):
            raise MediaIngressRejected("numeric download hosts are forbidden")
        wildcard_match = any(
            host.endswith("." + suffix) for suffix in self._allowed_wildcard_suffixes
        )
        if host not in self._allowed_hosts and not wildcard_match:
            raise MediaIngressRejected(
                "download host is not allowlisted",
                audit_detail=_host_audit_fingerprint(host),
            )
        if host.endswith(".blob.core.windows.net"):
            self._validate_azure_blob_read_sas(parsed, host)
        return host, 443

    def _validate_azure_blob_read_sas(self, parsed: Any, host: str) -> None:
        """Accept rotating ChatGPT Azure hosts only as signed read-only blobs.

        ChatGPT does not publish a stable storage-account hostname for
        ``openai/fileParams`` downloads.  Operators may therefore allow the
        Azure Blob suffix, but the wildcard is deliberately narrower than an
        ordinary host wildcard: one canonical storage-account label, one blob
        path, and a current blob-scoped read-only SAS are all mandatory.
        """

        account = host.removesuffix(".blob.core.windows.net")
        rejected = MediaIngressRejected(
            "Azure blob URL requires a current read-only SAS"
        )
        if not re.fullmatch(r"[a-z0-9]{3,24}", account):
            raise rejected
        path_segments = [segment for segment in parsed.path.split("/") if segment]
        if len(path_segments) < 2 or any(
            segment in {".", ".."} for segment in path_segments
        ):
            raise rejected
        if not parsed.query or len(parsed.query) > 8_192:
            raise rejected
        try:
            pairs = parse_qsl(
                parsed.query,
                keep_blank_values=True,
                strict_parsing=True,
                max_num_fields=32,
            )
        except ValueError as exc:
            raise rejected from exc
        query: dict[str, str] = {}
        for key, value in pairs:
            if key in query or not key or len(key) > 32 or len(value) > 2_048:
                raise rejected
            query[key] = value
        if (
            query.get("sp") != "r"
            or query.get("sr") != "b"
            or not query.get("sv")
            or not query.get("sig")
            or not query.get("se")
            or query.get("spr", "https") != "https"
        ):
            raise rejected
        try:
            expiry = datetime.fromisoformat(query["se"].replace("Z", "+00:00"))
            if expiry.tzinfo is None:
                raise ValueError("SAS expiry lacks timezone")
            expiry_epoch = expiry.astimezone(timezone.utc).timestamp()
        except (OverflowError, ValueError) as exc:
            raise rejected from exc
        if expiry_epoch <= self._clock():
            raise rejected

    async def _resolve_public(self, host: str, port: int) -> tuple[str, ...]:
        try:
            result = self._resolver(host, port)
            if inspect.isawaitable(result):
                result = await result
        except Exception as exc:
            raise MediaIngressRejected("DNS resolution failed") from exc
        addresses: list[str] = []
        for answer in result or ():
            if isinstance(answer, str):
                raw = answer
            elif (
                isinstance(answer, tuple)
                and len(answer) >= 5
                and isinstance(answer[4], tuple)
            ):
                raw = answer[4][0]
            elif isinstance(answer, Mapping) and isinstance(answer.get("host"), str):
                raw = answer["host"]
            else:
                raise MediaIngressRejected("DNS returned an invalid answer")
            try:
                ip = ipaddress.ip_address(raw.split("%", 1)[0])
            except ValueError as exc:
                raise MediaIngressRejected("DNS returned an invalid answer") from exc
            if (
                ip.is_loopback
                or ip.is_private
                or ip.is_link_local
                or ip.is_multicast
                or ip.is_reserved
                or ip.is_unspecified
                or not ip.is_global
            ):
                raise MediaIngressRejected("DNS returned a non-public address")
            canonical = str(ip)
            if canonical not in addresses:
                addresses.append(canonical)
            if len(addresses) > 16:
                raise MediaIngressRejected("DNS returned too many answers")
        if not addresses:
            raise MediaIngressRejected("DNS returned no usable answers")
        return tuple(addresses)

    def _owner_mac(self, owner_binding: str) -> str:
        if not isinstance(owner_binding, str) or not _OWNER_BINDING_RE.fullmatch(
            owner_binding
        ):
            raise MediaOwnershipError("invalid owner binding")
        return hmac.new(
            self._owner_key, b"owner\0" + owner_binding.encode("utf-8"), hashlib.sha256
        ).hexdigest()

    def _file_id_mac(self, file_id: str) -> str:
        if not isinstance(file_id, str) or not file_id or len(file_id) > 4096:
            raise MediaIngressRejected("invalid file identity")
        return hmac.new(
            self._owner_key, b"file\0" + file_id.encode("utf-8"), hashlib.sha256
        ).hexdigest()

    async def ingest(
        self,
        file: Any,
        *,
        owner_binding: str,
        max_bytes: int,
        expires_at: int,
        role: str = "story_media",
    ) -> Any:
        """Download, validate, and persist one owner-bound media asset.

        ``file`` structurally follows ``ChatGPTFile``.  The return value is the
        exact integration ``VerifiedAsset`` when that module is present.
        """

        try:
            download_url = file.download_url
            file_id = file.file_id
            declared_mime = file.mime_type
        except AttributeError as exc:
            raise TypeError("file must implement the ChatGPTFile contract") from exc
        if role not in _ROLE_MATRIX:
            raise MediaIngressRejected("unsupported media role")
        if (
            not isinstance(max_bytes, int)
            or isinstance(max_bytes, bool)
            or max_bytes <= 0
        ):
            raise MediaIngressRejected("invalid byte limit")
        byte_limit = min(max_bytes, self._max_asset_bytes)
        now = int(self._clock())
        if not isinstance(expires_at, int) or isinstance(expires_at, bool):
            raise MediaIngressRejected("invalid expiry")
        if expires_at <= now or expires_at > now + self._max_ttl:
            raise MediaIngressRejected("expiry must be in the next 24 hours")
        owner_mac = self._owner_mac(owner_binding)
        file_id_mac = self._file_id_mac(file_id)
        host, port = self._validate_url(download_url)
        allowed_ips = await self._resolve_public(host, port)

        temp_path: Path | None = None
        final_path: Path | None = None
        try:
            temp_path, digest, length, http_mime = await asyncio.wait_for(
                self._download_to_exclusive_temp(download_url, allowed_ips, byte_limit),
                timeout=self._download_timeout,
            )
            detected_mime, width, height = self._verify_image(temp_path)
            if detected_mime not in _ROLE_MATRIX[role]:
                raise MediaIngressRejected("media type is not permitted for this role")
            for claimed in (declared_mime, http_mime):
                normalised = _mime_without_parameters(claimed)
                if normalised not in _GENERIC_MIMES and normalised != detected_mime:
                    raise MediaIngressRejected(
                        "claimed media type does not match content"
                    )

            storage_ref = "ing_" + secrets.token_urlsafe(32)
            filename = secrets.token_hex(32) + ".asset"
            final_path = self._root / filename
            self._publish_temp(temp_path, final_path)
            temp_path = None
            with self._manifest_lock, self._db() as db:
                db.execute("BEGIN IMMEDIATE")
                retained = int(
                    db.execute(
                        "SELECT COALESCE(SUM(byte_length), 0) FROM media_assets"
                    ).fetchone()[0]
                )
                if retained + length > self._max_store_bytes:
                    raise MediaIngressRejected("media store capacity exceeded")
                db.execute(
                    """INSERT INTO media_assets
                       (storage_ref, filename, owner_mac, file_id_mac, sha256,
                        detected_mime, byte_length, width, height, duration,
                        created_at, expires_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)""",
                    (
                        storage_ref,
                        filename,
                        owner_mac,
                        file_id_mac,
                        digest,
                        detected_mime,
                        length,
                        width,
                        height,
                        now,
                        expires_at,
                    ),
                )
            asset = StoredMediaAsset(
                storage_ref=storage_ref,
                _path=final_path,
                sha256=digest,
                detected_mime=detected_mime,
                bytes=length,
                width=width,
                height=height,
                duration=None,
                created_at=now,
                expires_at=expires_at,
            )
            return self._contract_asset(asset, owner_binding)
        except asyncio.TimeoutError as exc:
            raise MediaIngressRejected("media download timed out") from exc
        finally:
            if temp_path is not None:
                with contextlib.suppress(FileNotFoundError):
                    temp_path.unlink()
            # A published file without a manifest row must not become an
            # untracked retention leak.
            if final_path is not None:
                with self._db() as db:
                    present = db.execute(
                        "SELECT 1 FROM media_assets WHERE filename = ?",
                        (final_path.name,),
                    ).fetchone()
                if present is None:
                    with contextlib.suppress(FileNotFoundError):
                        final_path.unlink()

    async def _download_to_exclusive_temp(
        self, url: str, allowed_ips: Sequence[str], byte_limit: int
    ) -> tuple[Path, str, int, str | None]:
        temp_path = self._root / (".ingress-" + secrets.token_hex(32))
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(temp_path, flags, 0o600)
        digest = hashlib.sha256()
        total = 0
        started = self._monotonic()
        headers = {
            "Accept": "image/jpeg,image/png,image/webp",
            "User-Agent": "events-bot-private-media-ingress/1",
        }
        try:
            fetch_context = self._http_fetch(
                url,
                headers=headers,
                allowed_ips=tuple(allowed_ips),
                timeout_seconds=self._download_timeout,
            )
            if inspect.isawaitable(fetch_context):
                fetch_context = await fetch_context
            async with self._as_response_context(fetch_context) as response:
                status = int(
                    getattr(response, "status", getattr(response, "status_code", 0))
                )
                if 300 <= status < 400:
                    raise MediaIngressRejected("redirect responses are forbidden")
                if status != 200:
                    raise MediaIngressRejected("media download failed")
                response_headers = getattr(response, "headers", {}) or {}
                content_length = self._header(response_headers, "content-length")
                if content_length:
                    try:
                        announced = int(content_length)
                    except ValueError as exc:
                        raise MediaIngressRejected("invalid Content-Length") from exc
                    if announced < 0 or announced > byte_limit:
                        raise MediaIngressRejected("media exceeds byte limit")
                async for chunk in self._iter_response_chunks(response):
                    if self._monotonic() - started > self._download_timeout:
                        raise MediaIngressRejected("media download timed out")
                    if not isinstance(chunk, (bytes, bytearray, memoryview)):
                        raise MediaIngressRejected("invalid media response chunk")
                    if not chunk:
                        continue
                    total += len(chunk)
                    if total > byte_limit:
                        raise MediaIngressRejected("media exceeds byte limit")
                    view = memoryview(chunk)
                    while view:
                        written = os.write(fd, view)
                        if written <= 0:
                            raise MediaIntegrityError("short media write")
                        view = view[written:]
                    digest.update(chunk)
                if total == 0:
                    raise MediaIngressRejected("empty media response")
                os.fsync(fd)
                os.fchmod(fd, 0o600)
                return (
                    temp_path,
                    digest.hexdigest(),
                    total,
                    self._header(response_headers, "content-type"),
                )
        except BaseException:
            os.close(fd)
            fd = -1
            with contextlib.suppress(FileNotFoundError):
                temp_path.unlink()
            raise
        finally:
            if fd >= 0:
                os.close(fd)

    @staticmethod
    @contextlib.asynccontextmanager
    async def _as_response_context(value: Any) -> AsyncIterator[Any]:
        if hasattr(value, "__aenter__"):
            async with value as response:
                yield response
            return
        try:
            yield value
        finally:
            closer = getattr(value, "aclose", None)
            if closer is not None:
                result = closer()
                if inspect.isawaitable(result):
                    await result
            else:
                release = getattr(value, "release", None)
                if release is not None:
                    release()

    @staticmethod
    def _header(headers: Mapping[str, Any], key: str) -> str | None:
        for name, value in headers.items():
            if str(name).lower() == key:
                return str(value)
        return None

    @staticmethod
    async def _iter_response_chunks(response: Any) -> AsyncIterator[bytes]:
        if hasattr(response, "aiter_bytes"):
            async for chunk in response.aiter_bytes(64 * 1024):
                yield chunk
            return
        content = getattr(response, "content", None)
        if content is not None and hasattr(content, "iter_chunked"):
            async for chunk in content.iter_chunked(64 * 1024):
                yield chunk
            return
        if hasattr(response, "iter_chunked"):
            async for chunk in response.iter_chunked(64 * 1024):
                yield chunk
            return
        raise MediaIngressRejected("HTTP response is not streamable")

    def _verify_image(self, path: Path) -> tuple[str, int, int]:
        try:
            from PIL import Image, UnidentifiedImageError
        except ImportError as exc:  # pragma: no cover - deployment dependency guard
            raise MediaIntegrityError(
                "Pillow is required for media validation"
            ) from exc
        try:
            with Image.open(path) as image:
                image_format = (image.format or "").upper()
                width, height = image.size
                if image_format not in _FORMAT_MIME:
                    raise MediaIngressRejected("unsupported media format")
                if width <= 0 or height <= 0:
                    raise MediaIngressRejected("invalid image dimensions")
                if (
                    width > self._max_width
                    or height > self._max_height
                    or width * height > self._max_pixels
                ):
                    raise MediaIngressRejected("image dimensions exceed limits")
                image.verify()
        except MediaIngressRejected:
            raise
        except (UnidentifiedImageError, OSError, SyntaxError, ValueError) as exc:
            raise MediaIngressRejected("image validation failed") from exc
        except Exception as exc:
            # Pillow plugins can raise parser-specific exceptions, including
            # DecompressionBombError.  Attacker-controlled parser failures are
            # rejected rather than surfaced as retryable operational errors.
            raise MediaIngressRejected("image validation failed") from exc
        return _FORMAT_MIME[image_format], width, height

    def _publish_temp(self, temp_path: Path, final_path: Path) -> None:
        info = os.lstat(temp_path)
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_ISLNK(info.st_mode)
            or info.st_nlink != 1
        ):
            raise MediaIntegrityError("invalid ingress temporary file")
        fd = os.open(temp_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            os.fchmod(fd, 0o400)
            os.fsync(fd)
        finally:
            os.close(fd)
        # link(2) is atomic and, unlike replace(), refuses to overwrite a
        # pre-existing destination.  The root is private and on one filesystem.
        os.link(temp_path, final_path, follow_symlinks=False)
        temp_path.unlink()
        directory_fd = os.open(self._root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    @staticmethod
    def _contract_asset(asset: StoredMediaAsset, owner_binding: str) -> Any:
        try:
            from private_events_mcp.media_contract import VerifiedAsset
        except ImportError:
            VerifiedAsset = _FallbackVerifiedAsset
        return VerifiedAsset(
            storage_ref=asset.storage_ref,
            owner_binding=owner_binding,
            content_digest="sha256:" + asset.sha256,
            mime_type=asset.detected_mime,
            byte_length=asset.bytes,
            expires_at=asset.expires_at,
            width=asset.width,
            height=asset.height,
        )

    def _row_for(self, storage_ref: str) -> sqlite3.Row:
        if not isinstance(storage_ref, str) or not _REF_RE.fullmatch(storage_ref):
            raise MediaNotFound("unknown media asset")
        with self._db() as db:
            db.row_factory = sqlite3.Row
            row = db.execute(
                "SELECT * FROM media_assets WHERE storage_ref = ?", (storage_ref,)
            ).fetchone()
        if row is None:
            raise MediaNotFound("unknown media asset")
        return row

    def _asset_from_row(self, row: sqlite3.Row, owner_binding: str) -> StoredMediaAsset:
        if not hmac.compare_digest(
            str(row["owner_mac"]), self._owner_mac(owner_binding)
        ):
            raise MediaOwnershipError("media asset belongs to another owner")
        if int(row["expires_at"]) <= int(self._clock()):
            raise MediaExpired("media asset has expired")
        filename = str(row["filename"])
        if not _FILE_RE.fullmatch(filename):
            raise MediaIntegrityError("invalid media manifest")
        return StoredMediaAsset(
            storage_ref=str(row["storage_ref"]),
            _path=self._root / filename,
            sha256=str(row["sha256"]),
            detected_mime=str(row["detected_mime"]),
            bytes=int(row["byte_length"]),
            width=int(row["width"]) if row["width"] is not None else None,
            height=int(row["height"]) if row["height"] is not None else None,
            duration=float(row["duration"]) if row["duration"] is not None else None,
            created_at=int(row["created_at"]),
            expires_at=int(row["expires_at"]),
        )

    def _open_and_rehash(self, asset: StoredMediaAsset) -> BinaryIO:
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(asset._path, flags)
        except (FileNotFoundError, OSError) as exc:
            raise MediaIntegrityError("stored media object is unavailable") from exc
        stream = os.fdopen(fd, "rb", closefd=True)
        try:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise MediaIntegrityError("stored media object is not a regular file")
            digest = hashlib.sha256()
            total = 0
            for chunk in iter(lambda: stream.read(64 * 1024), b""):
                total += len(chunk)
                if total > self._max_asset_bytes:
                    raise MediaIntegrityError("stored media object exceeds limits")
                digest.update(chunk)
            if total != asset.bytes or not hmac.compare_digest(
                digest.hexdigest(), asset.sha256
            ):
                raise MediaIntegrityError(
                    "stored media object failed digest verification"
                )
            stream.seek(0)
            return stream
        except BaseException:
            stream.close()
            raise

    def verify(self, storage_ref: str, owner_binding: str) -> Any:
        """Re-hash an asset and return contract metadata, never its path."""

        asset = self._asset_from_row(self._row_for(storage_ref), owner_binding)
        stream = self._open_and_rehash(asset)
        stream.close()
        return self._contract_asset(asset, owner_binding)

    @contextlib.contextmanager
    def open_verified(
        self, storage_ref: str, owner_binding: str
    ) -> Iterator[tuple[BinaryIO, Any]]:
        """Yield a re-hashed file handle plus metadata without exposing a path."""

        asset = self._asset_from_row(self._row_for(storage_ref), owner_binding)
        stream = self._open_and_rehash(asset)
        try:
            yield stream, self._contract_asset(asset, owner_binding)
        finally:
            stream.close()

    def cleanup_expired(self, *, now: int | None = None) -> int:
        """Delete expired manifest rows and their files; return row count."""

        cutoff = int(self._clock()) if now is None else int(now)
        with self._db() as db:
            rows = db.execute(
                "SELECT storage_ref, filename FROM media_assets WHERE expires_at <= ?",
                (cutoff,),
            ).fetchall()
            db.executemany(
                "DELETE FROM media_assets WHERE storage_ref = ?",
                ((row[0],) for row in rows),
            )
        for _, filename_value in rows:
            filename = str(filename_value)
            if not _FILE_RE.fullmatch(filename):
                continue
            path = self._root / filename
            try:
                info = os.lstat(path)
            except FileNotFoundError:
                continue
            # unlink removes a malicious symlink itself and never follows it.
            if stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
                with contextlib.suppress(FileNotFoundError):
                    path.unlink()
        return len(rows)

    # A concise alias makes scheduler integration less error-prone.
    cleanup = cleanup_expired


__all__ = [
    "MediaExpired",
    "MediaIngressRejected",
    "MediaIntegrityError",
    "MediaNotFound",
    "MediaOwnershipError",
    "MediaStoreError",
    "SecureMediaAssetStore",
    "StoredMediaAsset",
]
