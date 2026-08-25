from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import ipaddress
import os
import re
import secrets
import socket
import sqlite3
import stat
import tempfile
import threading
import time
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlsplit


class AudioAssetError(RuntimeError):
    error_code = "AUDIO_ASSET_FAILED"


class AudioAssetRejected(AudioAssetError):
    error_code = "AUDIO_FILE_REJECTED"

    def __init__(self, error_code: str, message: str) -> None:
        super().__init__(message)
        self.error_code = error_code


class AudioAssetNotFound(AudioAssetError):
    error_code = "AUDIO_REF_UNRESOLVED"


class AudioAssetOwnershipError(AudioAssetError):
    error_code = "AUDIO_PRINCIPAL_MISMATCH"


class AudioAssetExpired(AudioAssetError):
    error_code = "AUDIO_FILE_EXPIRED"


class AudioAssetIntegrityError(AudioAssetError):
    error_code = "AUDIO_FILE_INTEGRITY_FAILED"


@dataclass(frozen=True, slots=True)
class AudioFileParam:
    download_url: str
    file_id: str
    mime_type: str | None = None
    file_name: str | None = None


@dataclass(frozen=True, slots=True)
class VerifiedAudioAsset:
    storage_ref: str = field(repr=False)
    content_digest: str
    mime_type: str
    byte_length: int
    expires_at: int
    display_name: str
    suffix: str
    _path: Path = field(repr=False)


_REF_RE = re.compile(r"^aud_[A-Za-z0-9_-]{24,160}$")
_OWNER_RE = re.compile(r"^[0-9a-f]{64}$")
_NUMERIC_HOST_RE = re.compile(
    r"^(?:0x[0-9a-f]+|[0-9]+|[0-9.]+|(?:0[0-7]+\.){1,3}0[0-7]+)$",
    re.IGNORECASE,
)
_GENERIC_MIMES = frozenset({"", "application/octet-stream", "binary/octet-stream"})
_ALLOWED_DECLARED_MIMES = frozenset(
    {
        "audio/aac",
        "audio/flac",
        "audio/m4a",
        "audio/mp3",
        "audio/mp4",
        "audio/mpeg",
        "audio/ogg",
        "audio/opus",
        "audio/wav",
        "audio/wave",
        "audio/webm",
        "audio/x-m4a",
        "audio/x-wav",
        "video/mp4",  # common m4a transport hint; ffprobe still requires audio
        "video/webm",
    }
)
_MIME_SUFFIX = {
    "audio/aac": ".aac",
    "audio/flac": ".flac",
    "audio/mp4": ".m4a",
    "audio/mpeg": ".mp3",
    "audio/ogg": ".ogg",
    "audio/wav": ".wav",
    "audio/webm": ".webm",
}


def _normalise_mime(value: str | None) -> str:
    return (value or "").split(";", 1)[0].strip().casefold()


def _normalise_host(value: str) -> str:
    if not value or any(ord(char) <= 32 or ord(char) == 127 for char in value):
        raise ValueError("invalid hostname")
    host = value.removesuffix(".")
    try:
        host = host.encode("idna").decode("ascii").casefold()
    except UnicodeError as exc:
        raise ValueError("invalid hostname") from exc
    if not host or len(host) > 253:
        raise ValueError("invalid hostname")
    return host


def _safe_display_name(value: str | None, *, suffix: str) -> str:
    raw = Path(str(value or "recording")).name
    stem = Path(raw).stem
    stem = re.sub(r"[^0-9A-Za-zА-Яа-яЁё._ -]+", "_", stem).strip(" ._")
    stem = re.sub(r"\s+", " ", stem)[:100].strip()
    return (stem or "recording") + suffix



def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise OSError("short write")
        view = view[written:]


def detect_audio_mime(header: bytes) -> str | None:
    """Classify common audio containers without trusting the file name."""

    if header.startswith(b"OggS"):
        return "audio/ogg"
    if header.startswith(b"fLaC"):
        return "audio/flac"
    if header.startswith(b"RIFF") and header[8:12] == b"WAVE":
        return "audio/wav"
    if header.startswith(b"ID3"):
        return "audio/mpeg"
    if len(header) >= 2 and header[0] == 0xFF and (header[1] & 0xE0) == 0xE0:
        # Covers MPEG audio and AAC ADTS. Use the layer bits to distinguish the
        # common forms; ffprobe remains the authoritative stream validator.
        return "audio/aac" if (header[1] & 0xF6) == 0xF0 else "audio/mpeg"
    if len(header) >= 12 and header[4:8] == b"ftyp":
        return "audio/mp4"
    if header.startswith(b"\x1aE\xdf\xa3"):
        return "audio/webm"
    return None


class _PinnedResolver:
    def __init__(self, host: str, addresses: Sequence[str]) -> None:
        self._host = host
        self._addresses = tuple(addresses)

    async def resolve(
        self, host: str, port: int = 0, family: int = socket.AF_UNSPEC
    ) -> list[dict[str, Any]]:
        if host != self._host:
            raise OSError("unexpected DNS lookup")
        return [
            {
                "hostname": host,
                "host": address,
                "port": port,
                "family": (
                    socket.AF_INET6
                    if ipaddress.ip_address(address).version == 6
                    else socket.AF_INET
                ),
                "proto": 0,
                "flags": socket.AI_NUMERICHOST,
            }
            for address in self._addresses
        ]

    async def close(self) -> None:
        return None


class AudioAssetStore:
    """Owner-bound immutable audio ingress for ChatGPT fileParams."""

    def __init__(
        self,
        root: str | os.PathLike[str],
        *,
        allowed_hosts: Sequence[str],
        max_asset_bytes: int,
        max_store_bytes: int,
        ttl_seconds: int,
        timeout_seconds: int,
        resolver: Callable[[str, int], Any] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.root = Path(root)
        if not self.root.is_absolute():
            raise ValueError("audio asset root must be absolute")
        if not allowed_hosts:
            raise ValueError("audio asset hosts cannot be empty")
        exact: set[str] = set()
        wildcards: set[str] = set()
        for raw in allowed_hosts:
            value = str(raw).strip()
            if value.startswith("*."):
                wildcards.add(_normalise_host(value[2:]))
            else:
                exact.add(_normalise_host(value))
        if any(_NUMERIC_HOST_RE.fullmatch(host) for host in exact | wildcards):
            raise ValueError("numeric hosts are forbidden")
        if max_asset_bytes <= 0 or max_store_bytes < max_asset_bytes:
            raise ValueError("invalid audio byte limits")
        if not 60 <= ttl_seconds <= 7 * 24 * 3600:
            raise ValueError("invalid audio asset TTL")
        if not 1 <= timeout_seconds <= 600:
            raise ValueError("invalid audio download timeout")
        self._exact_hosts = frozenset(exact)
        self._wildcard_suffixes = frozenset(wildcards)
        self._max_asset_bytes = int(max_asset_bytes)
        self._max_store_bytes = int(max_store_bytes)
        self._ttl_seconds = int(ttl_seconds)
        self._timeout_seconds = int(timeout_seconds)
        self._resolver = resolver or self._default_resolver
        self._clock = clock
        self._lock = threading.RLock()
        self._initialise()

    def _initialise(self) -> None:
        if self.root.exists() and self.root.is_symlink():
            raise AudioAssetIntegrityError("audio asset root cannot be a symlink")
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.root, 0o700)
        self._key_path = self.root / ".owner-hmac.key"
        self._db_path = self.root / ".assets.sqlite3"
        self._key = self._load_or_create_key()
        with self._db() as db:
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS audio_assets (
                    storage_ref TEXT PRIMARY KEY,
                    filename TEXT NOT NULL UNIQUE,
                    owner_mac TEXT NOT NULL,
                    file_id_mac TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    mime_type TEXT NOT NULL,
                    byte_length INTEGER NOT NULL,
                    display_name TEXT NOT NULL,
                    suffix TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL
                )
                """
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS audio_assets_expiry ON audio_assets(expires_at)"
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS audio_assets_owner_file "
                "ON audio_assets(owner_mac, file_id_mac)"
            )
        self.cleanup_expired()

    def _load_or_create_key(self) -> bytes:
        if not self._key_path.exists():
            fd = os.open(self._key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                os.write(fd, secrets.token_bytes(32))
                os.fsync(fd)
            finally:
                os.close(fd)
        info = os.lstat(self._key_path)
        if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
            raise AudioAssetIntegrityError("invalid owner key")
        os.chmod(self._key_path, 0o600)
        key = self._key_path.read_bytes()
        if len(key) != 32:
            raise AudioAssetIntegrityError("invalid owner key length")
        return key

    @contextlib.contextmanager
    def _db(self):
        db = sqlite3.connect(self._db_path, timeout=20)
        try:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA secure_delete=ON")
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

    def _owner_mac(self, owner_binding: str) -> str:
        if not _OWNER_RE.fullmatch(str(owner_binding or "")):
            raise AudioAssetOwnershipError("invalid owner binding")
        return hmac.new(
            self._key,
            b"owner\0" + owner_binding.encode("ascii"),
            hashlib.sha256,
        ).hexdigest()

    def _file_id_mac(self, file_id: str) -> str:
        if not isinstance(file_id, str) or not file_id or len(file_id) > 4096:
            raise AudioAssetRejected("AUDIO_REF_UNRESOLVED", "invalid file identity")
        return hmac.new(
            self._key,
            b"file\0" + file_id.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _validate_url(self, url: str) -> tuple[str, int]:
        if not isinstance(url, str) or not url or "\\" in url:
            raise AudioAssetRejected("AUDIO_REF_UNRESOLVED", "invalid download URL")
        if any(ord(char) <= 32 or ord(char) == 127 for char in url):
            raise AudioAssetRejected("AUDIO_REF_UNRESOLVED", "invalid download URL")
        try:
            parsed = urlsplit(url)
            host = _normalise_host(parsed.hostname or "")
            port = parsed.port
        except ValueError as exc:
            raise AudioAssetRejected("AUDIO_REF_UNRESOLVED", "invalid download URL") from exc
        if (
            parsed.scheme.casefold() != "https"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
            or port not in (None, 443)
        ):
            raise AudioAssetRejected("AUDIO_REF_UNRESOLVED", "invalid download URL")
        try:
            ipaddress.ip_address(host)
        except ValueError:
            pass
        else:
            raise AudioAssetRejected("AUDIO_HOST_NOT_ALLOWED", "numeric host forbidden")
        if _NUMERIC_HOST_RE.fullmatch(host):
            raise AudioAssetRejected("AUDIO_HOST_NOT_ALLOWED", "numeric host forbidden")
        wildcard = any(host.endswith("." + suffix) for suffix in self._wildcard_suffixes)
        if host not in self._exact_hosts and not wildcard:
            raise AudioAssetRejected("AUDIO_HOST_NOT_ALLOWED", "download host not allowlisted")
        if host.endswith(".blob.core.windows.net"):
            self._validate_azure_sas(parsed)
        return host, 443

    def _validate_azure_sas(self, parsed: Any) -> None:
        try:
            pairs = parse_qsl(
                parsed.query,
                keep_blank_values=True,
                strict_parsing=True,
                max_num_fields=32,
            )
        except ValueError as exc:
            raise AudioAssetRejected("AUDIO_SOURCE_NOT_AUTHORIZED", "invalid blob SAS") from exc
        query: dict[str, str] = {}
        for key, value in pairs:
            if key in query:
                raise AudioAssetRejected("AUDIO_SOURCE_NOT_AUTHORIZED", "duplicate blob SAS field")
            query[key] = value
        if query.get("sp") != "r" or query.get("sr") != "b" or not query.get("sig"):
            raise AudioAssetRejected("AUDIO_SOURCE_NOT_AUTHORIZED", "blob SAS must be read-only")
        raw_expiry = query.get("se")
        if not raw_expiry:
            raise AudioAssetRejected("AUDIO_SOURCE_NOT_AUTHORIZED", "blob SAS expiry missing")
        try:
            expiry = datetime.fromisoformat(raw_expiry.replace("Z", "+00:00"))
            if (
                expiry.tzinfo is None
                or expiry.astimezone(timezone.utc).timestamp() <= self._clock()
            ):
                raise ValueError("expired")
        except ValueError as exc:
            raise AudioAssetRejected("AUDIO_SOURCE_NOT_AUTHORIZED", "blob SAS expired") from exc

    async def _resolve_public(self, host: str, port: int) -> tuple[str, ...]:
        try:
            result = self._resolver(host, port)
            if hasattr(result, "__await__"):
                result = await result
        except Exception as exc:
            raise AudioAssetRejected("AUDIO_FETCH_FAILED", "DNS resolution failed") from exc
        addresses: list[str] = []
        for raw in result or ():
            if not isinstance(raw, str):
                if isinstance(raw, tuple) and len(raw) >= 5:
                    raw = raw[4][0]
                elif isinstance(raw, Mapping):
                    raw = raw.get("host")
            try:
                ip = ipaddress.ip_address(str(raw).split("%", 1)[0])
            except ValueError as exc:
                raise AudioAssetRejected("AUDIO_FETCH_FAILED", "invalid DNS answer") from exc
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
                raise AudioAssetRejected("AUDIO_FETCH_FAILED", "non-public DNS answer")
            canonical = str(ip)
            if canonical not in addresses:
                addresses.append(canonical)
            if len(addresses) > 16:
                raise AudioAssetRejected("AUDIO_FETCH_FAILED", "too many DNS answers")
        if not addresses:
            raise AudioAssetRejected("AUDIO_FETCH_FAILED", "no DNS answers")
        return tuple(addresses)

    @contextlib.asynccontextmanager
    async def _http_get(
        self, url: str, *, host: str, allowed_ips: Sequence[str]
    ) -> AsyncIterator[Any]:
        import aiohttp

        connector = aiohttp.TCPConnector(
            resolver=_PinnedResolver(host, allowed_ips),
            use_dns_cache=False,
            ttl_dns_cache=0,
            limit=1,
        )
        timeout = aiohttp.ClientTimeout(total=self._timeout_seconds)
        async with (
            aiohttp.ClientSession(
                connector=connector,
                cookie_jar=aiohttp.DummyCookieJar(),
                auto_decompress=False,
                trust_env=False,
                timeout=timeout,
            ) as session,
            session.get(
                url,
                allow_redirects=False,
                headers={
                    "Accept-Encoding": "identity",
                    "User-Agent": "events-bot-audio-ingress/1",
                },
            ) as response,
        ):
            yield response

    def _capacity_bytes(self, db: sqlite3.Connection) -> int:
        row = db.execute("SELECT COALESCE(SUM(byte_length), 0) FROM audio_assets").fetchone()
        return int(row[0] or 0)

    async def ingest(
        self,
        file: AudioFileParam,
        *,
        owner_binding: str,
    ) -> VerifiedAudioAsset:
        owner_mac = self._owner_mac(owner_binding)
        file_id_mac = self._file_id_mac(file.file_id)
        now = int(self._clock())
        with self._lock, self._db() as db:
            row = db.execute(
                """
                SELECT storage_ref FROM audio_assets
                WHERE owner_mac=? AND file_id_mac=? AND expires_at>?
                ORDER BY created_at DESC LIMIT 1
                """,
                (owner_mac, file_id_mac, now),
            ).fetchone()
        if row:
            return self.reverify(str(row[0]), owner_binding=owner_binding)

        declared = _normalise_mime(file.mime_type)
        if declared not in _GENERIC_MIMES and declared not in _ALLOWED_DECLARED_MIMES:
            raise AudioAssetRejected("AUDIO_MIME_NOT_ALLOWED", "declared MIME is not audio")
        host, port = self._validate_url(file.download_url)
        addresses = await self._resolve_public(host, port)
        temp_fd, temp_name = tempfile.mkstemp(prefix=".ingress-", dir=self.root)
        temp_path = Path(temp_name)
        digest = hashlib.sha256()
        length = 0
        header = bytearray()
        http_mime = ""
        try:
            os.fchmod(temp_fd, 0o600)
            async with self._http_get(
                file.download_url, host=host, allowed_ips=addresses
            ) as response:
                if response.status in {301, 302, 303, 307, 308}:
                    raise AudioAssetRejected("AUDIO_FETCH_FAILED", "redirects are forbidden")
                if response.status != 200:
                    raise AudioAssetRejected("AUDIO_FETCH_FAILED", "audio download failed")
                http_mime = _normalise_mime(response.headers.get("Content-Type"))
                content_length = response.headers.get("Content-Length")
                if content_length:
                    try:
                        announced = int(content_length)
                    except ValueError as exc:
                        raise AudioAssetRejected(
                            "AUDIO_FETCH_FAILED", "invalid Content-Length"
                        ) from exc
                    if announced <= 0 or announced > self._max_asset_bytes:
                        raise AudioAssetRejected("AUDIO_FILE_TOO_LARGE", "audio exceeds byte limit")
                while True:
                    chunk = await response.content.read(256 * 1024)
                    if not chunk:
                        break
                    length += len(chunk)
                    if length > self._max_asset_bytes:
                        raise AudioAssetRejected("AUDIO_FILE_TOO_LARGE", "audio exceeds byte limit")
                    if len(header) < 65_536:
                        header.extend(chunk[: 65_536 - len(header)])
                    digest.update(chunk)
                    _write_all(temp_fd, chunk)
            os.fsync(temp_fd)
        except asyncio.TimeoutError as exc:
            temp_path.unlink(missing_ok=True)
            raise AudioAssetRejected("AUDIO_FETCH_FAILED", "audio download timed out") from exc
        except BaseException:
            temp_path.unlink(missing_ok=True)
            raise
        finally:
            os.close(temp_fd)
        if length <= 0:
            temp_path.unlink(missing_ok=True)
            raise AudioAssetRejected("AUDIO_FILE_INVALID", "audio is empty")
        detected = detect_audio_mime(bytes(header))
        if detected is None:
            temp_path.unlink(missing_ok=True)
            raise AudioAssetRejected("AUDIO_FILE_INVALID", "unsupported audio container")
        for claimed in (declared, http_mime):
            if claimed in _GENERIC_MIMES:
                continue
            # m4a is commonly transported under several equivalent hints.
            equivalent_mp4 = detected == "audio/mp4" and claimed in {
                "audio/m4a",
                "audio/mp4",
                "audio/x-m4a",
                "video/mp4",
            }
            equivalent_mpeg = detected == "audio/mpeg" and claimed in {
                "audio/mp3",
                "audio/mpeg",
            }
            equivalent_wav = detected == "audio/wav" and claimed in {
                "audio/wav",
                "audio/wave",
                "audio/x-wav",
            }
            equivalent_ogg = detected == "audio/ogg" and claimed in {
                "audio/ogg",
                "audio/opus",
            }
            equivalent_webm = detected == "audio/webm" and claimed in {
                "audio/webm",
                "video/webm",
            }
            if not any(
                (
                    equivalent_mp4,
                    equivalent_mpeg,
                    equivalent_wav,
                    equivalent_ogg,
                    equivalent_webm,
                    claimed == detected,
                )
            ):
                temp_path.unlink(missing_ok=True)
                raise AudioAssetRejected("AUDIO_MIME_MISMATCH", "audio MIME does not match bytes")

        suffix = _MIME_SUFFIX[detected]
        display_name = _safe_display_name(file.file_name, suffix=suffix)
        storage_ref = "aud_" + secrets.token_urlsafe(32)
        final_name = secrets.token_hex(32) + ".asset"
        final_path = self.root / final_name
        expires_at = now + self._ttl_seconds
        with self._lock, self._db() as db:
            self._cleanup_expired_locked(db, now=now)
            if self._capacity_bytes(db) + length > self._max_store_bytes:
                temp_path.unlink(missing_ok=True)
                raise AudioAssetRejected("AUDIO_STORE_FULL", "audio store capacity exceeded")
            os.replace(temp_path, final_path)
            os.chmod(final_path, 0o400)
            db.execute(
                """
                INSERT INTO audio_assets(
                    storage_ref, filename, owner_mac, file_id_mac, sha256,
                    mime_type, byte_length, display_name, suffix, created_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    storage_ref,
                    final_name,
                    owner_mac,
                    file_id_mac,
                    digest.hexdigest(),
                    detected,
                    length,
                    display_name,
                    suffix,
                    now,
                    expires_at,
                ),
            )
        return VerifiedAudioAsset(
            storage_ref=storage_ref,
            content_digest=digest.hexdigest(),
            mime_type=detected,
            byte_length=length,
            expires_at=expires_at,
            display_name=display_name,
            suffix=suffix,
            _path=final_path,
        )

    def ingest_provider_media(
        self,
        content: bytes | bytearray | memoryview | Any,
        *,
        owner_binding: str,
        provider_fingerprint: str,
        mime_type: str | None = None,
        display_name: str | None = None,
    ) -> VerifiedAudioAsset:
        """Ingest trusted provider bytes without fabricating a URL or fileParams.

        ``provider_fingerprint`` is an authenticated, non-reversible identity
        produced inside the Telegram adapter.  It is owner-bound in this store
        and used only for cache lookup; native provider identifiers are never
        persisted in request JSON or returned to callers.
        """

        if not isinstance(provider_fingerprint, str) or re.fullmatch(
            r"[0-9a-f]{64}", provider_fingerprint
        ) is None:
            raise AudioAssetRejected("AUDIO_REF_UNRESOLVED", "invalid provider media identity")
        owner_mac = self._owner_mac(owner_binding)
        file_id_mac = self._file_id_mac("provider:" + provider_fingerprint)
        now = int(self._clock())
        with self._lock, self._db() as db:
            row = db.execute(
                """SELECT storage_ref FROM audio_assets
                   WHERE owner_mac=? AND file_id_mac=? AND expires_at>?
                   ORDER BY created_at DESC LIMIT 1""",
                (owner_mac, file_id_mac, now),
            ).fetchone()
        if row:
            return self.reverify(str(row[0]), owner_binding=owner_binding)

        declared = _normalise_mime(mime_type)
        if declared not in _GENERIC_MIMES and declared not in _ALLOWED_DECLARED_MIMES:
            raise AudioAssetRejected("AUDIO_MIME_NOT_ALLOWED", "declared MIME is not audio")
        if isinstance(content, (bytes, bytearray, memoryview)):
            chunks: Any = (bytes(content),)
        elif callable(getattr(content, "read", None)):
            chunks = iter(lambda: content.read(256 * 1024), b"")
        else:
            raise AudioAssetRejected("AUDIO_FILE_INVALID", "provider media is not a byte stream")

        temp_fd, temp_name = tempfile.mkstemp(prefix=".provider-ingress-", dir=self.root)
        temp_path = Path(temp_name)
        digest = hashlib.sha256()
        header = bytearray()
        length = 0
        try:
            os.fchmod(temp_fd, 0o600)
            for chunk in chunks:
                if not isinstance(chunk, (bytes, bytearray, memoryview)):
                    raise AudioAssetRejected("AUDIO_FILE_INVALID", "provider stream returned invalid bytes")
                data = bytes(chunk)
                if not data:
                    continue
                length += len(data)
                if length > self._max_asset_bytes:
                    raise AudioAssetRejected("AUDIO_FILE_TOO_LARGE", "audio exceeds byte limit")
                if len(header) < 65_536:
                    header.extend(data[: 65_536 - len(header)])
                digest.update(data)
                _write_all(temp_fd, data)
            os.fsync(temp_fd)
        except BaseException:
            temp_path.unlink(missing_ok=True)
            raise
        finally:
            os.close(temp_fd)
        if length <= 0:
            temp_path.unlink(missing_ok=True)
            raise AudioAssetRejected("AUDIO_FILE_INVALID", "audio is empty")
        detected = detect_audio_mime(bytes(header))
        compatible = {
            "audio/mp4": {"audio/m4a", "audio/mp4", "audio/x-m4a", "video/mp4"},
            "audio/mpeg": {"audio/mp3", "audio/mpeg"},
            "audio/wav": {"audio/wav", "audio/wave", "audio/x-wav"},
            "audio/ogg": {"audio/ogg", "audio/opus"},
            "audio/webm": {"audio/webm", "video/webm"},
        }
        if detected is None:
            temp_path.unlink(missing_ok=True)
            raise AudioAssetRejected("AUDIO_FILE_INVALID", "unsupported audio container")
        if declared not in _GENERIC_MIMES and declared not in compatible.get(
            detected, {detected}
        ):
            temp_path.unlink(missing_ok=True)
            raise AudioAssetRejected("AUDIO_MIME_MISMATCH", "audio MIME does not match bytes")

        suffix = _MIME_SUFFIX[detected]
        safe_name = _safe_display_name(display_name, suffix=suffix)
        storage_ref = "aud_" + secrets.token_urlsafe(32)
        final_name = secrets.token_hex(32) + ".asset"
        final_path = self.root / final_name
        expires_at = now + self._ttl_seconds
        with self._lock, self._db() as db:
            self._cleanup_expired_locked(db, now=now)
            if self._capacity_bytes(db) + length > self._max_store_bytes:
                temp_path.unlink(missing_ok=True)
                raise AudioAssetRejected("AUDIO_STORE_FULL", "audio store capacity exceeded")
            os.replace(temp_path, final_path)
            os.chmod(final_path, 0o400)
            db.execute(
                """INSERT INTO audio_assets(
                   storage_ref,filename,owner_mac,file_id_mac,sha256,mime_type,
                   byte_length,display_name,suffix,created_at,expires_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    storage_ref, final_name, owner_mac, file_id_mac,
                    digest.hexdigest(), detected, length, safe_name, suffix,
                    now, expires_at,
                ),
            )
        return VerifiedAudioAsset(
            storage_ref=storage_ref,
            content_digest=digest.hexdigest(),
            mime_type=detected,
            byte_length=length,
            expires_at=expires_at,
            display_name=safe_name,
            suffix=suffix,
            _path=final_path,
        )

    def _load_row(self, storage_ref: str) -> tuple[Any, ...]:
        if not _REF_RE.fullmatch(str(storage_ref or "")):
            raise AudioAssetNotFound("unknown audio reference")
        with self._lock, self._db() as db:
            row = db.execute(
                """
                SELECT filename, owner_mac, sha256, mime_type, byte_length,
                       display_name, suffix, expires_at
                FROM audio_assets WHERE storage_ref=?
                """,
                (storage_ref,),
            ).fetchone()
        if row is None:
            raise AudioAssetNotFound("unknown audio reference")
        return row

    def reverify(self, storage_ref: str, *, owner_binding: str) -> VerifiedAudioAsset:
        row = self._load_row(storage_ref)
        (
            filename,
            owner_mac,
            expected_digest,
            mime,
            byte_length,
            display_name,
            suffix,
            expires_at,
        ) = row
        if not hmac.compare_digest(str(owner_mac), self._owner_mac(owner_binding)):
            raise AudioAssetOwnershipError("audio reference belongs to another principal")
        if int(expires_at) <= int(self._clock()):
            self.delete(storage_ref, owner_binding=owner_binding)
            raise AudioAssetExpired("audio asset expired")
        path = self.root / str(filename)
        try:
            info = os.lstat(path)
        except FileNotFoundError as exc:
            raise AudioAssetIntegrityError("stored audio missing") from exc
        if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode) or info.st_nlink != 1:
            raise AudioAssetIntegrityError("stored audio is not an immutable regular file")
        if info.st_size != int(byte_length):
            raise AudioAssetIntegrityError("stored audio size changed")
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        if not hmac.compare_digest(digest.hexdigest(), str(expected_digest)):
            raise AudioAssetIntegrityError("stored audio digest changed")
        return VerifiedAudioAsset(
            storage_ref=storage_ref,
            content_digest=str(expected_digest),
            mime_type=str(mime),
            byte_length=int(byte_length),
            expires_at=int(expires_at),
            display_name=str(display_name),
            suffix=str(suffix),
            _path=path,
        )

    def copy_verified_to(
        self,
        storage_ref: str,
        *,
        owner_binding: str,
        destination: str | Path,
    ) -> VerifiedAudioAsset:
        asset = self.reverify(storage_ref, owner_binding=owner_binding)
        target = Path(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(target, flags, 0o600)
        try:
            with asset._path.open("rb") as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    _write_all(fd, chunk)
            os.fsync(fd)
        finally:
            os.close(fd)
        return asset

    def delete(self, storage_ref: str, *, owner_binding: str) -> None:
        row = self._load_row(storage_ref)
        filename, owner_mac = row[0], row[1]
        if not hmac.compare_digest(str(owner_mac), self._owner_mac(owner_binding)):
            raise AudioAssetOwnershipError("audio reference belongs to another principal")
        with self._lock, self._db() as db:
            db.execute("DELETE FROM audio_assets WHERE storage_ref=?", (storage_ref,))
        (self.root / str(filename)).unlink(missing_ok=True)

    def _cleanup_expired_locked(self, db: sqlite3.Connection, *, now: int) -> int:
        rows = db.execute(
            "SELECT storage_ref, filename FROM audio_assets WHERE expires_at<=?",
            (now,),
        ).fetchall()
        for _storage_ref, filename in rows:
            (self.root / str(filename)).unlink(missing_ok=True)
        db.execute("DELETE FROM audio_assets WHERE expires_at<=?", (now,))
        return len(rows)

    def cleanup_expired(self) -> int:
        now = int(self._clock())
        with self._lock, self._db() as db:
            return self._cleanup_expired_locked(db, now=now)
