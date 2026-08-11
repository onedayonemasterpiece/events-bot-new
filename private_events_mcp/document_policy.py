"""Fail-closed structural policy for document assets.

The policy is deliberately separate from image validation.  It never executes
or extracts a document.  ZIP-derived formats are classified from a bounded
central-directory inventory and text is decoded in full under the caller's
byte limit.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import struct
import unicodedata
import zipfile
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path
from typing import BinaryIO, Final

MIB: Final = 1024 * 1024
DEFAULT_MAX_DOCUMENT_BYTES: Final = 48 * MIB
HARD_MAX_DOCUMENT_BYTES: Final = 64 * MIB
MAX_SAFE_FILENAME_BYTES: Final = 180
MAX_ZIP_ENTRIES: Final = 50_000
MAX_ZIP_UNCOMPRESSED_BYTES: Final = 4 * 1024 * MIB
MAX_ZIP_EXPANSION_RATIO: Final = 2_000

APK_MIME: Final = "application/vnd.android.package-archive"
PDF_MIME: Final = "application/pdf"
ZIP_MIME: Final = "application/zip"
JSON_MIME: Final = "application/json"
TEXT_MIME: Final = "text/plain"
CSV_MIME: Final = "text/csv"
MARKDOWN_MIME: Final = "text/markdown"
DOCX_MIME: Final = (
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
)
XLSX_MIME: Final = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
PPTX_MIME: Final = (
    "application/vnd.openxmlformats-officedocument.presentationml.presentation"
)
OCTET_STREAM_MIME: Final = "application/octet-stream"

DEFAULT_ALLOWED_MIME_TYPES: Final = frozenset(
    {
        APK_MIME,
        PDF_MIME,
        ZIP_MIME,
        JSON_MIME,
        TEXT_MIME,
        CSV_MIME,
        MARKDOWN_MIME,
        DOCX_MIME,
        XLSX_MIME,
        PPTX_MIME,
    }
)

_MIME_TO_EXTENSION: Final = {
    APK_MIME: ".apk",
    PDF_MIME: ".pdf",
    ZIP_MIME: ".zip",
    JSON_MIME: ".json",
    TEXT_MIME: ".txt",
    CSV_MIME: ".csv",
    MARKDOWN_MIME: ".md",
    DOCX_MIME: ".docx",
    XLSX_MIME: ".xlsx",
    PPTX_MIME: ".pptx",
}
_EXTENSION_TO_TEXT_MIME: Final = {
    ".txt": TEXT_MIME,
    ".log": TEXT_MIME,
    ".csv": CSV_MIME,
    ".md": MARKDOWN_MIME,
    ".markdown": MARKDOWN_MIME,
    ".json": JSON_MIME,
}
_CONTAINER_EXTENSIONS: Final = {
    ".apk": APK_MIME,
    ".docx": DOCX_MIME,
    ".xlsx": XLSX_MIME,
    ".pptx": PPTX_MIME,
}
_BIDI_CONTROL_RE: Final = re.compile("[\u061c\u200e\u200f\u202a-\u202e\u2066-\u2069]")
_WINDOWS_RESERVED: Final = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{number}" for number in range(1, 10)),
    *(f"LPT{number}" for number in range(1, 10)),
}
_PDF_HEADER_RE: Final = re.compile(br"%PDF-1\.[0-9]")
_SAFE_ZIP_METHODS: Final = frozenset({zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED})


class DocumentPolicyError(ValueError):
    """Fail-closed validation error carrying a stable public reason code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class ValidatedDocument:
    safe_file_name: str
    mime_type: str
    byte_length: int
    content_digest: str
    classification: str
    declared_mime_matches: bool | None


@dataclass(frozen=True, slots=True)
class _ZipInventory:
    names: frozenset[str]
    sizes: dict[str, int]


def _normalise_mime(value: str | None) -> str | None:
    if value is None:
        return None
    normalised = value.split(";", 1)[0].strip().casefold()
    return normalised or None


def _truncate_utf8_preserving_extension(value: str, maximum: int) -> str:
    if len(value.encode("utf-8")) <= maximum:
        return value
    suffix = Path(value).suffix
    suffix_size = len(suffix.encode("utf-8"))
    stem = value[: -len(suffix)] if suffix else value
    budget = maximum - suffix_size
    if budget < 1:
        suffix = ""
        budget = maximum
    while stem and len(stem.encode("utf-8")) > budget:
        stem = stem[:-1]
    return f"{stem.rstrip(' .')}{suffix}" or "document.bin"


def sanitize_document_filename(
    file_name: str | None,
    *,
    detected_mime: str,
) -> str:
    """Return a path-free, display-only name bounded to 180 UTF-8 bytes."""

    expected_extension = _MIME_TO_EXTENSION.get(detected_mime, ".bin")
    raw = file_name if isinstance(file_name, str) else ""
    # Both separator styles are treated as paths regardless of host platform.
    basename_parts = re.split(r"[/\\]+", raw)
    raw = next((part for part in reversed(basename_parts) if part), "")
    normalized = unicodedata.normalize("NFKC", raw)
    normalized = _BIDI_CONTROL_RE.sub("", normalized)
    normalized = "".join(
        " " if character.isspace() else character
        for character in normalized
        if unicodedata.category(character) not in {"Cc", "Cf", "Cs"}
    )
    normalized = re.sub(r"\s+", " ", normalized).strip(" .")
    normalized = re.sub(r"\.{2,}", ".", normalized)
    normalized = re.sub(r"[^\w .()+,@#%&'\-\[\]]", "_", normalized, flags=re.UNICODE)
    normalized = re.sub(r"_+", "_", normalized).strip(" ._")
    if not normalized:
        normalized = f"document{expected_extension}"

    stem = Path(normalized).stem or "document"
    if stem.upper() in _WINDOWS_RESERVED:
        normalized = f"_{normalized}"

    if Path(normalized).suffix.casefold() != expected_extension:
        stem = Path(normalized).stem.rstrip(" ._") or "document"
        normalized = f"{stem}{expected_extension}"

    normalized = _truncate_utf8_preserving_extension(
        normalized, MAX_SAFE_FILENAME_BYTES
    )
    if (
        not normalized
        or normalized in {".", ".."}
        or len(normalized.encode("utf-8")) > MAX_SAFE_FILENAME_BYTES
    ):
        raise DocumentPolicyError("FILE_NAME_INVALID", "document filename is invalid")
    return normalized


def _zip_name_key(name: str) -> tuple[str, str]:
    if not isinstance(name, str) or not name:
        raise DocumentPolicyError("FILE_TYPE_INVALID", "ZIP entry name is invalid")
    if "\x00" in name:
        raise DocumentPolicyError("FILE_TYPE_INVALID", "ZIP entry name is invalid")
    normalised = unicodedata.normalize("NFKC", name.replace("\\", "/"))
    if (
        normalised.startswith("/")
        or re.match(r"^[A-Za-z]:", normalised)
        or any(part in {"", ".", ".."} for part in normalised.rstrip("/").split("/"))
        or any(unicodedata.category(char) in {"Cc", "Cs"} for char in normalised)
    ):
        raise DocumentPolicyError("FILE_TYPE_INVALID", "ZIP contains unsafe entry names")
    canonical = normalised.rstrip("/")
    return canonical, canonical.casefold()


def _validate_zip_inventory(stream: BinaryIO) -> _ZipInventory:
    try:
        stream.seek(0)
        with zipfile.ZipFile(stream) as archive:
            infos = archive.infolist()
            if not infos or len(infos) > MAX_ZIP_ENTRIES:
                raise DocumentPolicyError(
                    "FILE_TYPE_INVALID", "ZIP entry count is invalid"
                )
            exact_names: set[str] = set()
            folded_names: set[str] = set()
            sizes: dict[str, int] = {}
            occupied_ranges: list[tuple[int, int]] = []
            total_uncompressed = 0
            total_compressed = 0
            for info in infos:
                canonical, folded = _zip_name_key(info.filename)
                if canonical in exact_names or folded in folded_names:
                    raise DocumentPolicyError(
                        "FILE_TYPE_INVALID", "ZIP contains colliding entry names"
                    )
                exact_names.add(canonical)
                folded_names.add(folded)
                if info.flag_bits & 0x1:
                    raise DocumentPolicyError(
                        "FILE_TYPE_INVALID", "encrypted ZIP containers are not accepted"
                    )
                if info.compress_type not in _SAFE_ZIP_METHODS:
                    raise DocumentPolicyError(
                        "FILE_TYPE_INVALID", "ZIP compression method is not accepted"
                    )
                file_size = int(info.file_size)
                compressed_size = int(info.compress_size)
                if file_size < 0 or compressed_size < 0:
                    raise DocumentPolicyError(
                        "FILE_TYPE_INVALID", "ZIP entry sizes are invalid"
                    )
                if compressed_size == 0 and file_size > 0:
                    raise DocumentPolicyError(
                        "FILE_TYPE_INVALID", "ZIP expansion ratio exceeds policy"
                    )
                if (
                    compressed_size
                    and file_size > compressed_size * MAX_ZIP_EXPANSION_RATIO
                ):
                    raise DocumentPolicyError(
                        "FILE_TYPE_INVALID", "ZIP expansion ratio exceeds policy"
                    )
                total_uncompressed += file_size
                total_compressed += compressed_size
                if total_uncompressed > MAX_ZIP_UNCOMPRESSED_BYTES:
                    raise DocumentPolicyError(
                        "FILE_TYPE_INVALID", "ZIP declared size exceeds policy"
                    )
                sizes[folded] = file_size
                # Cross-check each central-directory record against its local
                # header without inflating the entry.  This catches truncated,
                # overlapping and header-confusion containers while retaining
                # an inventory-only policy.
                header_offset = int(info.header_offset)
                if header_offset < 0 or header_offset + 30 > archive.start_dir:
                    raise DocumentPolicyError(
                        "FILE_TYPE_INVALID", "ZIP local header is invalid"
                    )
                stream.seek(header_offset)
                local_header = stream.read(30)
                if len(local_header) != 30:
                    raise DocumentPolicyError(
                        "FILE_TYPE_INVALID", "ZIP local header is truncated"
                    )
                (
                    signature,
                    _extract_version,
                    local_flags,
                    local_method,
                    _mtime,
                    _mdate,
                    local_crc,
                    local_compressed,
                    local_uncompressed,
                    name_length,
                    extra_length,
                ) = struct.unpack("<4s5H3L2H", local_header)
                if (
                    signature != b"PK\x03\x04"
                    or local_flags != info.flag_bits
                    or local_method != info.compress_type
                ):
                    raise DocumentPolicyError(
                        "FILE_TYPE_INVALID", "ZIP headers are inconsistent"
                    )
                local_name_bytes = stream.read(name_length)
                if len(local_name_bytes) != name_length:
                    raise DocumentPolicyError(
                        "FILE_TYPE_INVALID", "ZIP local filename is truncated"
                    )
                try:
                    local_name = local_name_bytes.decode(
                        "utf-8" if local_flags & 0x800 else "cp437"
                    )
                except UnicodeDecodeError as exc:
                    raise DocumentPolicyError(
                        "FILE_TYPE_INVALID", "ZIP local filename is invalid"
                    ) from exc
                if local_name != info.filename:
                    raise DocumentPolicyError(
                        "FILE_TYPE_INVALID", "ZIP headers are inconsistent"
                    )
                data_offset = header_offset + 30 + name_length + extra_length
                data_end = data_offset + compressed_size
                if data_offset > archive.start_dir or data_end > archive.start_dir:
                    raise DocumentPolicyError(
                        "FILE_TYPE_INVALID", "ZIP entry data is truncated"
                    )
                if not local_flags & 0x08 and (
                    (local_crc != info.CRC)
                    or local_compressed not in {compressed_size, 0xFFFFFFFF}
                    or local_uncompressed not in {file_size, 0xFFFFFFFF}
                ):
                    raise DocumentPolicyError(
                        "FILE_TYPE_INVALID", "ZIP headers are inconsistent"
                    )
                occupied_ranges.append((header_offset, data_end))
            occupied_ranges.sort()
            if any(
                current_start < previous_end
                for (_, previous_end), (current_start, _) in pairwise(
                    occupied_ranges
                )
            ):
                raise DocumentPolicyError(
                    "FILE_TYPE_INVALID", "ZIP entries overlap"
                )
            if (
                total_compressed
                and total_uncompressed
                > total_compressed * MAX_ZIP_EXPANSION_RATIO
            ):
                raise DocumentPolicyError(
                    "FILE_TYPE_INVALID", "ZIP expansion ratio exceeds policy"
                )
            return _ZipInventory(frozenset(folded_names), sizes)
    except DocumentPolicyError:
        raise
    except (OSError, EOFError, RuntimeError, zipfile.BadZipFile, zipfile.LargeZipFile) as exc:
        raise DocumentPolicyError("FILE_TYPE_INVALID", "ZIP container is invalid") from exc


def _nonempty(inventory: _ZipInventory, name: str) -> bool:
    return inventory.sizes.get(name.casefold(), 0) > 0


def _classify_zip(stream: BinaryIO, file_name: str | None) -> tuple[str, str]:
    inventory = _validate_zip_inventory(stream)
    names = inventory.names
    suffix = Path(file_name or "").suffix.casefold()

    has_manifest = _nonempty(inventory, "AndroidManifest.xml")
    has_android_payload = (
        _nonempty(inventory, "classes.dex")
        or _nonempty(inventory, "resources.arsc")
        or any(name.startswith("lib/") and size > 0 for name, size in inventory.sizes.items())
    )
    if has_manifest and has_android_payload:
        return APK_MIME, "android_apk"

    office_markers = {
        DOCX_MIME: (
            "word/document.xml",
            any(name.startswith("word/") for name in names),
        ),
        XLSX_MIME: (
            "xl/workbook.xml",
            any(name.startswith("xl/") for name in names),
        ),
        PPTX_MIME: (
            "ppt/presentation.xml",
            any(name.startswith("ppt/") for name in names),
        ),
    }
    families = [
        mime
        for mime, (main_part, family_present) in office_markers.items()
        if family_present and _nonempty(inventory, main_part)
    ]
    has_ooxml_base = (
        _nonempty(inventory, "[Content_Types].xml")
        and "_rels/.rels" in names
    )
    if has_ooxml_base and len(families) == 1:
        mime = families[0]
        return mime, {
            DOCX_MIME: "office_docx",
            XLSX_MIME: "office_xlsx",
            PPTX_MIME: "office_pptx",
        }[mime]
    if families or "[content_types].xml" in names:
        raise DocumentPolicyError(
            "FILE_TYPE_INVALID", "Office container structure is invalid"
        )

    expected = _CONTAINER_EXTENSIONS.get(suffix)
    if expected is not None:
        raise DocumentPolicyError(
            "FILE_TYPE_MISMATCH",
            "document filename does not match its container structure",
        )
    return ZIP_MIME, "zip"


def _classify_text(
    payload: bytes, file_name: str | None, declared_mime: str | None
) -> tuple[str, str] | None:
    if b"\x00" in payload:
        return None
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError:
        return None
    if any(
        unicodedata.category(character) in {"Cc", "Cs"}
        and character not in {"\t", "\n", "\r"}
        for character in text
    ):
        return None
    suffix = Path(file_name or "").suffix.casefold()
    candidate = _EXTENSION_TO_TEXT_MIME.get(suffix)
    declared = _normalise_mime(declared_mime)
    if candidate is None and declared in {JSON_MIME, CSV_MIME, MARKDOWN_MIME, TEXT_MIME}:
        candidate = declared
    candidate = candidate or TEXT_MIME
    if candidate == JSON_MIME:
        try:
            json.loads(text)
        except (json.JSONDecodeError, RecursionError) as exc:
            raise DocumentPolicyError(
                "FILE_TYPE_MISMATCH", "JSON document is malformed"
            ) from exc
        return JSON_MIME, "json"
    if candidate == CSV_MIME:
        return CSV_MIME, "csv_text"
    if candidate == MARKDOWN_MIME:
        return MARKDOWN_MIME, "markdown_text"
    return TEXT_MIME, "utf8_text"


def _detect_document_type(
    stream: BinaryIO,
    *,
    file_name: str | None,
    declared_mime: str | None,
    byte_length: int,
) -> tuple[str, str]:
    stream.seek(0)
    prefix = stream.read(min(byte_length, 16))
    if _PDF_HEADER_RE.match(prefix):
        stream.seek(0)
        payload = stream.read(byte_length + 1)
        if len(payload) != byte_length or not re.search(br"%%EOF\s*\Z", payload[-1024:]):
            raise DocumentPolicyError("FILE_TYPE_INVALID", "PDF document is truncated")
        return PDF_MIME, "pdf"
    if prefix.startswith((b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")):
        return _classify_zip(stream, file_name)
    stream.seek(0)
    payload = stream.read(byte_length + 1)
    if len(payload) != byte_length:
        raise DocumentPolicyError("FILE_INTEGRITY_FAILED", "document changed while read")
    text_classification = _classify_text(payload, file_name, declared_mime)
    if text_classification is not None:
        return text_classification
    raise DocumentPolicyError(
        "FILE_TYPE_NOT_ALLOWED",
        "document bytes do not match an allowed file type",
    )


def validate_document_stream(
    stream: BinaryIO,
    *,
    file_name: str | None,
    declared_mime: str | None,
    max_bytes: int = DEFAULT_MAX_DOCUMENT_BYTES,
    allowed_mime_types: frozenset[str] = DEFAULT_ALLOWED_MIME_TYPES,
) -> ValidatedDocument:
    """Validate an already-open regular file, restoring its position to zero."""

    if not 1 <= max_bytes <= HARD_MAX_DOCUMENT_BYTES:
        raise ValueError("max_bytes must be within the document hard limit")
    try:
        before = os.fstat(stream.fileno())
    except (AttributeError, OSError, ValueError) as exc:
        raise DocumentPolicyError(
            "FILE_INTEGRITY_FAILED", "asset must be an open regular file"
        ) from exc
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
        raise DocumentPolicyError(
            "FILE_INTEGRITY_FAILED", "asset must be a single-link regular file"
        )
    size = int(before.st_size)
    if size <= 0:
        raise DocumentPolicyError("FILE_TYPE_INVALID", "empty documents are not accepted")
    if size > max_bytes:
        raise DocumentPolicyError("FILE_TOO_LARGE", "document exceeds configured size limit")

    detected_mime, classification = _detect_document_type(
        stream,
        file_name=file_name,
        declared_mime=declared_mime,
        byte_length=size,
    )
    if detected_mime not in allowed_mime_types:
        raise DocumentPolicyError(
            "FILE_TYPE_NOT_ALLOWED", "detected document type is not allowed"
        )

    declared = _normalise_mime(declared_mime)
    declared_matches: bool | None = None
    if declared:
        declared_matches = declared in {detected_mime, OCTET_STREAM_MIME}
        if not declared_matches:
            raise DocumentPolicyError(
                "FILE_TYPE_MISMATCH",
                "declared MIME type does not match detected bytes",
            )

    stream.seek(0)
    digest = hashlib.sha256()
    total = 0
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        total += len(chunk)
        if total > max_bytes:
            raise DocumentPolicyError("FILE_TOO_LARGE", "document exceeds configured size limit")
        digest.update(chunk)
    after = os.fstat(stream.fileno())
    if (
        total != size
        or after.st_size != before.st_size
        or after.st_mtime_ns != before.st_mtime_ns
        or after.st_ctime_ns != before.st_ctime_ns
        or after.st_ino != before.st_ino
        or after.st_dev != before.st_dev
    ):
        raise DocumentPolicyError("FILE_INTEGRITY_FAILED", "document changed while read")
    stream.seek(0)
    return ValidatedDocument(
        safe_file_name=sanitize_document_filename(
            file_name, detected_mime=detected_mime
        ),
        mime_type=detected_mime,
        byte_length=size,
        content_digest=f"sha256:{digest.hexdigest()}",
        classification=classification,
        declared_mime_matches=declared_matches,
    )


def validate_document_file(
    path: str | os.PathLike[str],
    *,
    file_name: str | None,
    declared_mime: str | None,
    max_bytes: int = DEFAULT_MAX_DOCUMENT_BYTES,
    allowed_mime_types: frozenset[str] = DEFAULT_ALLOWED_MIME_TYPES,
) -> ValidatedDocument:
    """Open without following links and validate bounded bytes without execution."""

    candidate = Path(path)
    try:
        info = os.lstat(candidate)
    except OSError as exc:
        raise DocumentPolicyError("FILE_INTEGRITY_FAILED", "asset is unavailable") from exc
    if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
        raise DocumentPolicyError("FILE_INTEGRITY_FAILED", "asset must be a regular file")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(candidate, flags)
    except OSError as exc:
        raise DocumentPolicyError("FILE_INTEGRITY_FAILED", "asset is unavailable") from exc
    with os.fdopen(fd, "rb", closefd=True) as stream:
        return validate_document_stream(
            stream,
            file_name=file_name,
            declared_mime=declared_mime,
            max_bytes=max_bytes,
            allowed_mime_types=allowed_mime_types,
        )


def detect_document_type(path: Path, *, file_name: str | None) -> tuple[str, str]:
    """Compatibility classifier; callers needing metadata should use validate."""

    result = validate_document_file(
        path,
        file_name=file_name,
        declared_mime=None,
    )
    return result.mime_type, result.classification


__all__ = [
    "APK_MIME",
    "CSV_MIME",
    "DEFAULT_ALLOWED_MIME_TYPES",
    "DEFAULT_MAX_DOCUMENT_BYTES",
    "DOCX_MIME",
    "HARD_MAX_DOCUMENT_BYTES",
    "JSON_MIME",
    "MARKDOWN_MIME",
    "PDF_MIME",
    "PPTX_MIME",
    "TEXT_MIME",
    "XLSX_MIME",
    "ZIP_MIME",
    "DocumentPolicyError",
    "ValidatedDocument",
    "detect_document_type",
    "sanitize_document_filename",
    "validate_document_file",
    "validate_document_stream",
]
