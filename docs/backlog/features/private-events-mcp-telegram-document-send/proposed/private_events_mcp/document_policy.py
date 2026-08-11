"""Conservative document validation policy for private Events MCP.

This module is an integration-ready prototype, not a complete patch.  It is
intentionally independent from the current image validator so document support
cannot weaken existing JPEG/PNG/WebP checks.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import unicodedata
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Final


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
_BIDI_CONTROL_RE: Final = re.compile(
    "[\u061c\u200e\u200f\u202a-\u202e\u2066-\u2069]"
)
_WINDOWS_RESERVED: Final = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{number}" for number in range(1, 10)),
    *(f"LPT{number}" for number in range(1, 10)),
}


class DocumentPolicyError(ValueError):
    """Fail-closed validation error with a stable public reason code."""

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


def _truncate_utf8_preserving_extension(value: str, maximum: int) -> str:
    encoded = value.encode("utf-8")
    if len(encoded) <= maximum:
        return value
    suffix = Path(value).suffix
    suffix_bytes = suffix.encode("utf-8")
    stem = value[: -len(suffix)] if suffix else value
    budget = maximum - len(suffix_bytes)
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
    """Return a transport-safe display name; never return a filesystem path."""

    expected_extension = _MIME_TO_EXTENSION.get(detected_mime, ".bin")
    raw = file_name or f"document{expected_extension}"
    normalized = unicodedata.normalize("NFKC", raw)
    normalized = _BIDI_CONTROL_RE.sub("", normalized)
    normalized = "".join(
        " " if character.isspace() else character
        for character in normalized
        if unicodedata.category(character) not in {"Cc", "Cs"}
    )
    normalized = normalized.replace("/", "_").replace("\\", "_")
    normalized = re.sub(r"\s+", " ", normalized).strip(" .")
    normalized = re.sub(r"\.{2,}", ".", normalized)
    normalized = re.sub(r"[^\w .()+,@#%&'\-\[\]]", "_", normalized, flags=re.UNICODE)
    normalized = re.sub(r"_+", "_", normalized).strip(" ._")
    if not normalized:
        normalized = f"document{expected_extension}"

    stem = Path(normalized).stem or "document"
    if stem.upper() in _WINDOWS_RESERVED:
        normalized = f"_{normalized}"

    actual_extension = Path(normalized).suffix.casefold()
    if detected_mime in _MIME_TO_EXTENSION and actual_extension != expected_extension:
        normalized = f"{Path(normalized).stem.rstrip(' ._') or 'document'}{expected_extension}"

    normalized = _truncate_utf8_preserving_extension(
        normalized,
        MAX_SAFE_FILENAME_BYTES,
    )
    if normalized in {".", ".."} or not normalized:
        raise DocumentPolicyError("FILE_NAME_INVALID", "document filename is invalid")
    return normalized


def _validate_zip_inventory(path: Path) -> tuple[set[str], int]:
    try:
        with zipfile.ZipFile(path) as archive:
            infos = archive.infolist()
    except (OSError, zipfile.BadZipFile, zipfile.LargeZipFile) as exc:
        raise DocumentPolicyError("FILE_TYPE_INVALID", "ZIP container is invalid") from exc

    if not infos or len(infos) > MAX_ZIP_ENTRIES:
        raise DocumentPolicyError("FILE_TYPE_INVALID", "ZIP entry count is invalid")

    names: set[str] = set()
    total_uncompressed = 0
    total_compressed = 0
    for info in infos:
        name = info.filename.replace("\\", "/")
        if (
            name.startswith("/")
            or re.match(r"^[A-Za-z]:/", name)
            or any(part == ".." for part in name.split("/"))
            or "\x00" in name
        ):
            raise DocumentPolicyError(
                "FILE_TYPE_INVALID",
                "ZIP contains unsafe entry names",
            )
        if info.flag_bits & 0x1:
            raise DocumentPolicyError(
                "FILE_TYPE_INVALID",
                "encrypted ZIP containers are not accepted",
            )
        names.add(name)
        total_uncompressed += max(0, int(info.file_size))
        total_compressed += max(0, int(info.compress_size))
        if total_uncompressed > MAX_ZIP_UNCOMPRESSED_BYTES:
            raise DocumentPolicyError(
                "FILE_TYPE_INVALID",
                "ZIP declared size exceeds policy",
            )

    if total_compressed and total_uncompressed > total_compressed * MAX_ZIP_EXPANSION_RATIO:
        raise DocumentPolicyError(
            "FILE_TYPE_INVALID",
            "ZIP expansion ratio exceeds policy",
        )
    return names, total_uncompressed


def _classify_zip(path: Path, file_name: str | None) -> tuple[str, str]:
    names, _ = _validate_zip_inventory(path)
    folded = {name.casefold() for name in names}
    suffix = Path(file_name or "").suffix.casefold()

    has_manifest = "androidmanifest.xml" in folded
    has_android_payload = (
        "classes.dex" in folded
        or "resources.arsc" in folded
        or any(name.startswith("lib/") for name in folded)
    )
    if has_manifest and has_android_payload:
        return APK_MIME, "android_apk"
    if suffix == ".apk":
        raise DocumentPolicyError(
            "FILE_TYPE_MISMATCH",
            "file is named as APK but does not have an APK structure",
        )

    if "[content_types].xml" in folded:
        if any(name.startswith("word/") for name in folded):
            return DOCX_MIME, "office_docx"
        if any(name.startswith("xl/") for name in folded):
            return XLSX_MIME, "office_xlsx"
        if any(name.startswith("ppt/") for name in folded):
            return PPTX_MIME, "office_pptx"
    return ZIP_MIME, "zip"


def _classify_text(sample: bytes, file_name: str | None) -> tuple[str, str] | None:
    if b"\x00" in sample:
        return None
    try:
        text = sample.decode("utf-8")
    except UnicodeDecodeError:
        return None

    suffix = Path(file_name or "").suffix.casefold()
    candidate = _EXTENSION_TO_TEXT_MIME.get(suffix, TEXT_MIME)
    if candidate == JSON_MIME:
        try:
            json.loads(text)
        except json.JSONDecodeError as exc:
            raise DocumentPolicyError(
                "FILE_TYPE_MISMATCH",
                "file is named as JSON but JSON parsing failed",
            ) from exc
        return JSON_MIME, "json"
    if candidate == CSV_MIME:
        return CSV_MIME, "csv_text"
    if candidate == MARKDOWN_MIME:
        return MARKDOWN_MIME, "markdown_text"
    return TEXT_MIME, "utf8_text"


def detect_document_type(path: Path, *, file_name: str | None) -> tuple[str, str]:
    with path.open("rb") as stream:
        prefix = stream.read(1024 * 1024)

    if prefix.startswith(b"%PDF-"):
        return PDF_MIME, "pdf"
    if prefix.startswith((b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")):
        return _classify_zip(path, file_name)

    text_classification = _classify_text(prefix, file_name)
    if text_classification is not None:
        return text_classification

    raise DocumentPolicyError(
        "FILE_TYPE_NOT_ALLOWED",
        "document bytes do not match an allowed file type",
    )


def validate_document_file(
    path: str | os.PathLike[str],
    *,
    file_name: str | None,
    declared_mime: str | None,
    max_bytes: int = DEFAULT_MAX_DOCUMENT_BYTES,
    allowed_mime_types: frozenset[str] = DEFAULT_ALLOWED_MIME_TYPES,
) -> ValidatedDocument:
    """Validate immutable file bytes without executing or extracting them."""

    candidate = Path(path)
    if candidate.is_symlink() or not candidate.is_file():
        raise DocumentPolicyError("FILE_INTEGRITY_FAILED", "asset must be a regular file")
    if not 1 <= max_bytes <= HARD_MAX_DOCUMENT_BYTES:
        raise ValueError("max_bytes must be within the document hard limit")

    size = candidate.stat().st_size
    if size <= 0:
        raise DocumentPolicyError("FILE_TYPE_INVALID", "empty documents are not accepted")
    if size > max_bytes:
        raise DocumentPolicyError("FILE_TOO_LARGE", "document exceeds configured size limit")

    detected_mime, classification = detect_document_type(
        candidate,
        file_name=file_name,
    )
    if detected_mime not in allowed_mime_types:
        raise DocumentPolicyError(
            "FILE_TYPE_NOT_ALLOWED",
            "detected document type is not allowed",
        )

    declared = declared_mime.casefold().strip() if declared_mime else None
    declared_matches: bool | None = None
    if declared:
        declared_matches = declared in {detected_mime, OCTET_STREAM_MIME}
        if not declared_matches:
            raise DocumentPolicyError(
                "FILE_TYPE_MISMATCH",
                "declared MIME type does not match detected bytes",
            )

    digest = hashlib.sha256()
    with candidate.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)

    safe_name = sanitize_document_filename(
        file_name,
        detected_mime=detected_mime,
    )
    return ValidatedDocument(
        safe_file_name=safe_name,
        mime_type=detected_mime,
        byte_length=size,
        content_digest=f"sha256:{digest.hexdigest()}",
        classification=classification,
        declared_mime_matches=declared_matches,
    )


__all__ = [
    "APK_MIME",
    "DEFAULT_ALLOWED_MIME_TYPES",
    "DEFAULT_MAX_DOCUMENT_BYTES",
    "DocumentPolicyError",
    "HARD_MAX_DOCUMENT_BYTES",
    "ValidatedDocument",
    "detect_document_type",
    "sanitize_document_filename",
    "validate_document_file",
]
