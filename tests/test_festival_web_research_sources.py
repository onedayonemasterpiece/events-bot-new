from __future__ import annotations

import io
import os
import tarfile
import zipfile
from pathlib import Path

import pytest

from festival_web_research.artifacts import (
    ArtifactSafetyError, build_artifact_manifest, safe_extract_tar, safe_extract_zip,
)
from festival_web_research.sources import (
    UnsafeSourceURL, canonicalize_public_url, validate_resolved_addresses,
)


def test_url_canonicalization_is_stable_and_strips_fragment() -> None:
    assert canonicalize_public_url("HTTPS://ExAmPle.COM:443/a%20b?z=2&a=1#fragment") == "https://example.com/a%20b?a=1&z=2"
    assert canonicalize_public_url("http://example.com") == "http://example.com/"


@pytest.mark.parametrize("url", [
    "file:///etc/passwd", "http://user:pw@example.com/", "http://127.0.0.1/",
    "http://10.0.0.1/", "http://[::1]/", "http://localhost/",
    "https://example.com\\@127.0.0.1/", "javascript:alert(1)",
])
def test_unsafe_urls_are_rejected(url: str) -> None:
    with pytest.raises(UnsafeSourceURL):
        canonicalize_public_url(url)


def test_dns_validation_rejects_empty_private_and_mixed_answers() -> None:
    with pytest.raises(UnsafeSourceURL):
        validate_resolved_addresses("example.org", [])
    with pytest.raises(UnsafeSourceURL):
        validate_resolved_addresses("example.org", ["93.184.216.34", "127.0.0.1"])
    assert validate_resolved_addresses("example.org", ["93.184.216.34"]) == ("93.184.216.34",)
    with pytest.raises(UnsafeSourceURL, match="no resolver"):
        canonicalize_public_url("https://example.org", require_dns=True)
    assert canonicalize_public_url("https://example.org", resolver=lambda host, port: ["93.184.216.34"], require_dns=True)


def test_zip_extraction_and_manifest_are_bounded(tmp_path: Path) -> None:
    archive = tmp_path / "ok.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr("state.json", b"{}")
        bundle.writestr("claims/S1.jsonl", b'{"claim":1}\n')
    output = tmp_path / "out"
    paths = safe_extract_zip(archive, output, max_files=2, max_total_bytes=100)
    assert sorted(path.relative_to(output).as_posix() for path in paths) == ["claims/S1.jsonl", "state.json"]
    manifest = build_artifact_manifest(output)
    assert manifest.total_bytes == 14
    assert [entry.relative_path for entry in manifest.entries] == ["claims/S1.jsonl", "state.json"]


def test_zip_traversal_symlink_and_size_are_rejected(tmp_path: Path) -> None:
    traversal = tmp_path / "bad.zip"
    with zipfile.ZipFile(traversal, "w") as bundle:
        bundle.writestr("../escape", "x")
    with pytest.raises(ArtifactSafetyError, match="unsafe archive path"):
        safe_extract_zip(traversal, tmp_path / "x")
    bomb = tmp_path / "big.zip"
    with zipfile.ZipFile(bomb, "w") as bundle:
        bundle.writestr("big", "x" * 20)
    with pytest.raises(ArtifactSafetyError, match="total-size"):
        safe_extract_zip(bomb, tmp_path / "b", max_total_bytes=10)
    symlink = tmp_path / "link.zip"
    info = zipfile.ZipInfo("link")
    info.create_system = 3
    info.external_attr = (0o120777 << 16)
    with zipfile.ZipFile(symlink, "w") as bundle:
        bundle.writestr(info, "target")
    with pytest.raises(ArtifactSafetyError, match="symlink"):
        safe_extract_zip(symlink, tmp_path / "s")


def test_tar_links_and_traversal_are_rejected(tmp_path: Path) -> None:
    archive = tmp_path / "bad.tar"
    with tarfile.open(archive, "w") as bundle:
        info = tarfile.TarInfo("safe")
        info.type = tarfile.SYMTYPE
        info.linkname = "/etc/passwd"
        bundle.addfile(info)
    with pytest.raises(ArtifactSafetyError, match="links/devices"):
        safe_extract_tar(archive, tmp_path / "out")
    traversal = tmp_path / "traversal.tar"
    with tarfile.open(traversal, "w") as bundle:
        info = tarfile.TarInfo("../escape")
        info.size = 1
        bundle.addfile(info, io.BytesIO(b"x"))
    with pytest.raises(ArtifactSafetyError, match="unsafe archive path"):
        safe_extract_tar(traversal, tmp_path / "out2")


def test_manifest_rejects_symlink(tmp_path: Path) -> None:
    root = tmp_path / "tree"
    root.mkdir()
    (root / "file").write_text("ok")
    try:
        os.symlink(root / "file", root / "link")
    except OSError:
        pytest.skip("symlinks unavailable")
    with pytest.raises(ArtifactSafetyError, match="symlink"):
        build_artifact_manifest(root)

@pytest.mark.parametrize("url", ["http://metadata.google.internal/", "http://service.internal/", "http://printer.local/"])
def test_internal_hostname_suffixes_are_rejected(url: str) -> None:
    with pytest.raises(UnsafeSourceURL):
        canonicalize_public_url(url)
