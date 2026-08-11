from __future__ import annotations

import os
import zipfile
from pathlib import Path

import pytest

import private_events_mcp.document_policy as policy
from private_events_mcp.document_policy import (
    APK_MIME,
    CSV_MIME,
    DOCX_MIME,
    JSON_MIME,
    MARKDOWN_MIME,
    PDF_MIME,
    PPTX_MIME,
    TEXT_MIME,
    XLSX_MIME,
    ZIP_MIME,
    DocumentPolicyError,
    sanitize_document_filename,
    validate_document_file,
)


def _zip(path: Path, entries: list[tuple[str, bytes]] | dict[str, bytes]) -> None:
    values = entries.items() if isinstance(entries, dict) else entries
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in values:
            archive.writestr(name, payload)


def _assert_code(path: Path, code: str, **kwargs: object) -> None:
    with pytest.raises(DocumentPolicyError) as caught:
        validate_document_file(
            path,
            file_name=kwargs.get("file_name", path.name),
            declared_mime=kwargs.get("declared_mime"),
            max_bytes=kwargs.get("max_bytes", policy.DEFAULT_MAX_DOCUMENT_BYTES),
        )
    assert caught.value.code == code


def _office(path: Path, family: str) -> None:
    main_parts = {
        "word": "word/document.xml",
        "xl": "xl/workbook.xml",
        "ppt": "ppt/presentation.xml",
    }
    _zip(
        path,
        {
            "[Content_Types].xml": b"<Types/>",
            "_rels/.rels": b"<Relationships/>",
            main_parts[family]: b"<main/>",
        },
    )


def test_filename_is_basename_nfkc_safe_extension_and_utf8_bounded() -> None:
    name = sanitize_document_filename(
        "../ignored/CON\\\u202ereport." + "я" * 200 + ".exe",
        detected_mime=APK_MIME,
    )
    assert "/" not in name and "\\" not in name and "\u202e" not in name
    assert name.endswith(".apk")
    assert len(name.encode("utf-8")) <= policy.MAX_SAFE_FILENAME_BYTES
    assert sanitize_document_filename("NUL.txt", detected_mime=TEXT_MIME).startswith(
        "_NUL"
    )


def test_pdf_is_structurally_bounded_and_digest_is_recomputed(tmp_path: Path) -> None:
    path = tmp_path / "opaque"
    payload = b"%PDF-1.7\n1 0 obj\n<<>>\nendobj\n%%EOF\n"
    path.write_bytes(payload)
    result = validate_document_file(
        path, file_name="guide.exe", declared_mime="application/octet-stream"
    )
    assert result.mime_type == PDF_MIME
    assert result.safe_file_name == "guide.pdf"
    assert result.byte_length == len(payload)
    assert result.content_digest.startswith("sha256:")
    assert len(result.content_digest) == 71


def test_pdf_without_eof_is_rejected_as_truncated(tmp_path: Path) -> None:
    path = tmp_path / "bad.pdf"
    path.write_bytes(b"%PDF-1.7\ntruncated")
    _assert_code(path, "FILE_TYPE_INVALID", declared_mime=PDF_MIME)


@pytest.mark.parametrize(
    ("suffix", "payload", "mime", "classification"),
    [
        ("txt", "plain text\n", TEXT_MIME, "utf8_text"),
        ("md", "# heading\n", MARKDOWN_MIME, "markdown_text"),
        ("csv", "a,b\n1,2\n", CSV_MIME, "csv_text"),
        ("json", '{"ok": true}\n', JSON_MIME, "json"),
    ],
)
def test_full_utf8_text_family_is_accepted(
    tmp_path: Path, suffix: str, payload: str, mime: str, classification: str
) -> None:
    path = tmp_path / f"input.{suffix}"
    path.write_text(payload, encoding="utf-8")
    result = validate_document_file(
        path, file_name=path.name, declared_mime=mime
    )
    assert (result.mime_type, result.classification) == (mime, classification)


def test_utf8_validation_covers_the_entire_bounded_file(tmp_path: Path) -> None:
    path = tmp_path / "late-invalid.txt"
    path.write_bytes(b"a" * (1024 * 1024 + 5) + b"\xff")
    _assert_code(path, "FILE_TYPE_NOT_ALLOWED", declared_mime=TEXT_MIME)


def test_malformed_json_and_declared_mime_mismatch_fail_closed(tmp_path: Path) -> None:
    malformed = tmp_path / "bad.json"
    malformed.write_text('{"unfinished":', encoding="utf-8")
    _assert_code(malformed, "FILE_TYPE_MISMATCH", declared_mime=JSON_MIME)
    text = tmp_path / "note.txt"
    text.write_text("hello", encoding="utf-8")
    _assert_code(text, "FILE_TYPE_MISMATCH", declared_mime=PDF_MIME)


def test_apk_requires_android_manifest_and_nonempty_payload(tmp_path: Path) -> None:
    valid = tmp_path / "valid.zip"
    _zip(
        valid,
        {
            "AndroidManifest.xml": b"binary manifest",
            "classes.dex": b"dex\n035\x00",
            "META-INF/MANIFEST.MF": b"Manifest-Version: 1.0\n",
        },
    )
    result = validate_document_file(
        valid, file_name="release.apk", declared_mime=APK_MIME
    )
    assert (result.mime_type, result.classification) == (APK_MIME, "android_apk")

    fake = tmp_path / "fake.apk"
    _zip(fake, {"notes.txt": b"ordinary ZIP"})
    _assert_code(fake, "FILE_TYPE_MISMATCH", declared_mime=APK_MIME)

    empty_payload = tmp_path / "empty-payload.apk"
    _zip(
        empty_payload,
        {"AndroidManifest.xml": b"manifest", "classes.dex": b""},
    )
    _assert_code(empty_payload, "FILE_TYPE_MISMATCH", declared_mime=APK_MIME)


@pytest.mark.parametrize(
    ("family", "suffix", "mime", "classification"),
    [
        ("word", "docx", DOCX_MIME, "office_docx"),
        ("xl", "xlsx", XLSX_MIME, "office_xlsx"),
        ("ppt", "pptx", PPTX_MIME, "office_pptx"),
    ],
)
def test_office_families_require_base_and_exact_main_part(
    tmp_path: Path,
    family: str,
    suffix: str,
    mime: str,
    classification: str,
) -> None:
    path = tmp_path / f"office.{suffix}"
    _office(path, family)
    result = validate_document_file(path, file_name=path.name, declared_mime=mime)
    assert (result.mime_type, result.classification) == (mime, classification)


def test_malformed_or_ambiguous_office_container_is_rejected(tmp_path: Path) -> None:
    malformed = tmp_path / "bad.docx"
    _zip(
        malformed,
        {
            "[Content_Types].xml": b"<Types/>",
            "word/document.xml": b"<document/>",
        },
    )
    _assert_code(malformed, "FILE_TYPE_INVALID", declared_mime=DOCX_MIME)

    ambiguous = tmp_path / "ambiguous.docx"
    _zip(
        ambiguous,
        {
            "[Content_Types].xml": b"<Types/>",
            "_rels/.rels": b"<rels/>",
            "word/document.xml": b"<document/>",
            "xl/workbook.xml": b"<workbook/>",
        },
    )
    _assert_code(ambiguous, "FILE_TYPE_INVALID", declared_mime=DOCX_MIME)


def test_ordinary_safe_zip_is_accepted_without_extracting(tmp_path: Path) -> None:
    path = tmp_path / "bundle.zip"
    _zip(path, {"folder/readme.txt": b"hello"})
    result = validate_document_file(path, file_name=path.name, declared_mime=ZIP_MIME)
    assert (result.mime_type, result.classification) == (ZIP_MIME, "zip")
    assert not (tmp_path / "folder").exists()


@pytest.mark.parametrize(
    "entries",
    [
        [("../escape.txt", b"x")],
        [("/absolute.txt", b"x")],
        [("C:\\escape.txt", b"x")],
        [("a//b.txt", b"x")],
        [("same.txt", b"a"), ("same.txt", b"b")],
        [("Case.txt", b"a"), ("case.TXT", b"b")],
        [("K.txt", b"a"), ("\u212a.txt", b"b")],
    ],
)
def test_zip_traversal_duplicates_and_casefold_collisions_are_rejected(
    tmp_path: Path, entries: list[tuple[str, bytes]]
) -> None:
    path = tmp_path / "unsafe.zip"
    with pytest.warns(UserWarning) if len({name for name, _ in entries}) < len(entries) else _nullcontext():
        _zip(path, entries)
    _assert_code(path, "FILE_TYPE_INVALID", declared_mime=ZIP_MIME)


class _nullcontext:
    def __enter__(self) -> None:
        return None

    def __exit__(self, *_args: object) -> None:
        return None


def test_encrypted_flag_is_rejected_without_decryption_attempt(tmp_path: Path) -> None:
    path = tmp_path / "encrypted.zip"
    _zip(path, {"safe.txt": b"payload"})
    payload = bytearray(path.read_bytes())
    local = payload.index(b"PK\x03\x04")
    central = payload.index(b"PK\x01\x02")
    payload[local + 6 : local + 8] = (1).to_bytes(2, "little")
    payload[central + 8 : central + 10] = (1).to_bytes(2, "little")
    path.write_bytes(payload)
    _assert_code(path, "FILE_TYPE_INVALID", declared_mime=ZIP_MIME)


def test_inconsistent_local_and_central_zip_names_are_rejected(tmp_path: Path) -> None:
    path = tmp_path / "confused.zip"
    _zip(path, {"safe.txt": b"payload"})
    payload = bytearray(path.read_bytes())
    local = payload.index(b"PK\x03\x04")
    payload[local + 30 : local + 38] = b"evil.txt"
    path.write_bytes(payload)
    _assert_code(path, "FILE_TYPE_INVALID", declared_mime=ZIP_MIME)


def test_archive_entry_count_size_and_ratio_limits_are_mutation_guards(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "bounded.zip"
    _zip(path, {"one.txt": b"a" * 100, "two.txt": b"b" * 100})
    monkeypatch.setattr(policy, "MAX_ZIP_ENTRIES", 1)
    _assert_code(path, "FILE_TYPE_INVALID", declared_mime=ZIP_MIME)
    monkeypatch.setattr(policy, "MAX_ZIP_ENTRIES", 10)
    monkeypatch.setattr(policy, "MAX_ZIP_UNCOMPRESSED_BYTES", 50)
    _assert_code(path, "FILE_TYPE_INVALID", declared_mime=ZIP_MIME)
    monkeypatch.setattr(policy, "MAX_ZIP_UNCOMPRESSED_BYTES", 1_000)
    monkeypatch.setattr(policy, "MAX_ZIP_EXPANSION_RATIO", 1)
    _assert_code(path, "FILE_TYPE_INVALID", declared_mime=ZIP_MIME)


def test_malformed_empty_oversize_nonregular_and_hardlink_are_rejected(
    tmp_path: Path,
) -> None:
    malformed = tmp_path / "malformed.zip"
    malformed.write_bytes(b"PK\x03\x04truncated")
    _assert_code(malformed, "FILE_TYPE_INVALID", declared_mime=ZIP_MIME)
    empty = tmp_path / "empty.txt"
    empty.touch()
    _assert_code(empty, "FILE_TYPE_INVALID", declared_mime=TEXT_MIME)
    large = tmp_path / "large.txt"
    large.write_bytes(b"abcdef")
    _assert_code(large, "FILE_TOO_LARGE", declared_mime=TEXT_MIME, max_bytes=5)
    _assert_code(tmp_path, "FILE_INTEGRITY_FAILED", file_name="directory.txt")
    link = tmp_path / "link.txt"
    link.symlink_to(large)
    _assert_code(link, "FILE_INTEGRITY_FAILED", declared_mime=TEXT_MIME)
    hardlink = tmp_path / "hardlink.txt"
    os.link(large, hardlink)
    _assert_code(hardlink, "FILE_INTEGRITY_FAILED", declared_mime=TEXT_MIME)


def test_opaque_binary_is_not_accepted_as_octet_stream(tmp_path: Path) -> None:
    for name, payload in (
        ("binary.exe", b"MZ\x00\x01\x02"),
        ("binary.elf", b"\x7fELF\x02\x01\x01"),
    ):
        path = tmp_path / name
        path.write_bytes(payload)
        _assert_code(
            path,
            "FILE_TYPE_NOT_ALLOWED",
            declared_mime="application/octet-stream",
        )
