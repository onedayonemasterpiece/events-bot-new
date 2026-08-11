from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from private_events_mcp.document_policy import (
    APK_MIME,
    DOCX_MIME,
    PDF_MIME,
    TEXT_MIME,
    DocumentPolicyError,
    sanitize_document_filename,
    validate_document_file,
)


def _zip(path: Path, entries: dict[str, bytes]) -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in entries.items():
            archive.writestr(name, payload)


def test_sanitizes_traversal_bidi_and_forces_detected_extension() -> None:
    name = sanitize_document_filename(
        "../bad\u202egpj\\tailscale.exe",
        detected_mime=APK_MIME,
    )
    assert "/" not in name
    assert "\\" not in name
    assert "\u202e" not in name
    assert name.endswith(".apk")


def test_validates_pdf_and_recomputes_digest(tmp_path: Path) -> None:
    path = tmp_path / "input"
    path.write_bytes(b"%PDF-1.7\nminimal\n%%EOF")
    result = validate_document_file(
        path,
        file_name="guide.pdf",
        declared_mime=PDF_MIME,
    )
    assert result.mime_type == PDF_MIME
    assert result.safe_file_name == "guide.pdf"
    assert result.content_digest.startswith("sha256:")
    assert len(result.content_digest) == 71


def test_validates_apk_structure_without_extracting(tmp_path: Path) -> None:
    path = tmp_path / "input.zip"
    _zip(
        path,
        {
            "AndroidManifest.xml": b"binary-manifest",
            "classes.dex": b"dex\n035\x00",
            "META-INF/MANIFEST.MF": b"Manifest-Version: 1.0\n",
        },
    )
    result = validate_document_file(
        path,
        file_name="tailscale-universal.apk",
        declared_mime="application/octet-stream",
    )
    assert result.mime_type == APK_MIME
    assert result.classification == "android_apk"
    assert result.safe_file_name == "tailscale-universal.apk"


def test_rejects_zip_masquerading_as_apk(tmp_path: Path) -> None:
    path = tmp_path / "fake.apk"
    _zip(path, {"notes.txt": b"not an apk"})
    with pytest.raises(DocumentPolicyError) as caught:
        validate_document_file(
            path,
            file_name="fake.apk",
            declared_mime=APK_MIME,
        )
    assert caught.value.code == "FILE_TYPE_MISMATCH"


def test_detects_docx_from_container(tmp_path: Path) -> None:
    path = tmp_path / "doc.zip"
    _zip(
        path,
        {
            "[Content_Types].xml": b"<Types/>",
            "word/document.xml": b"<document/>",
        },
    )
    result = validate_document_file(
        path,
        file_name="report.docx",
        declared_mime=DOCX_MIME,
    )
    assert result.mime_type == DOCX_MIME
    assert result.safe_file_name == "report.docx"


def test_accepts_utf8_text(tmp_path: Path) -> None:
    path = tmp_path / "note"
    path.write_text("hello\n", encoding="utf-8")
    result = validate_document_file(
        path,
        file_name="note.txt",
        declared_mime=TEXT_MIME,
    )
    assert result.mime_type == TEXT_MIME
    assert result.classification == "utf8_text"


def test_rejects_declared_mime_mismatch(tmp_path: Path) -> None:
    path = tmp_path / "guide.pdf"
    path.write_bytes(b"%PDF-1.7\n%%EOF")
    with pytest.raises(DocumentPolicyError) as caught:
        validate_document_file(
            path,
            file_name="guide.pdf",
            declared_mime=TEXT_MIME,
        )
    assert caught.value.code == "FILE_TYPE_MISMATCH"


def test_rejects_oversize_document(tmp_path: Path) -> None:
    path = tmp_path / "large.txt"
    path.write_bytes(b"a" * 10)
    with pytest.raises(DocumentPolicyError) as caught:
        validate_document_file(
            path,
            file_name="large.txt",
            declared_mime=TEXT_MIME,
            max_bytes=5,
        )
    assert caught.value.code == "FILE_TOO_LARGE"


def test_rejects_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target.txt"
    target.write_text("hello", encoding="utf-8")
    link = tmp_path / "link.txt"
    link.symlink_to(target)
    with pytest.raises(DocumentPolicyError) as caught:
        validate_document_file(
            link,
            file_name="link.txt",
            declared_mime=TEXT_MIME,
        )
    assert caught.value.code == "FILE_INTEGRITY_FAILED"


def test_rejects_unsafe_zip_entry(tmp_path: Path) -> None:
    path = tmp_path / "unsafe.zip"
    _zip(path, {"../escape.txt": b"no"})
    with pytest.raises(DocumentPolicyError) as caught:
        validate_document_file(
            path,
            file_name="unsafe.zip",
            declared_mime="application/zip",
        )
    assert caught.value.code == "FILE_TYPE_INVALID"
