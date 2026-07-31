"""Bounded artifact manifests and traversal-safe archive extraction."""
from __future__ import annotations

import hashlib
import stat
import tarfile
import zipfile
from pathlib import Path, PurePosixPath
from typing import BinaryIO

from pydantic import Field

from .contracts import ClosedModel


class ArtifactSafetyError(ValueError):
    pass


class ArtifactEntry(ClosedModel):
    relative_path: str = Field(min_length=1, max_length=1024)
    byte_count: int = Field(ge=0, le=64 * 1024 * 1024)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class ArtifactManifest(ClosedModel):
    entries: list[ArtifactEntry] = Field(max_length=512)
    total_bytes: int = Field(ge=0, le=256 * 1024 * 1024)


def _safe_member_path(name: str) -> PurePosixPath:
    if not name or "\x00" in name or "\\" in name:
        raise ArtifactSafetyError("empty/NUL/backslash archive path")
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts or any(part in {"", "."} for part in path.parts):
        raise ArtifactSafetyError(f"unsafe archive path: {name!r}")
    if len(name) > 1024:
        raise ArtifactSafetyError("archive path too long")
    return path


def _copy_bounded(source: BinaryIO, destination: Path, *, expected_size: int, max_member_bytes: int) -> int:
    if expected_size < 0 or expected_size > max_member_bytes:
        raise ArtifactSafetyError("archive member exceeds size limit")
    written = 0
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with destination.open("xb") as output:
            while True:
                chunk = source.read(min(1024 * 1024, max_member_bytes - written + 1))
                if not chunk:
                    break
                written += len(chunk)
                if written > max_member_bytes or written > expected_size:
                    raise ArtifactSafetyError("archive member expanded beyond declared/allowed size")
                output.write(chunk)
        if written != expected_size:
            raise ArtifactSafetyError("archive member size mismatch")
    except Exception:
        destination.unlink(missing_ok=True)
        raise
    return written


def safe_extract_zip(archive: str | Path, destination: str | Path, *, max_files: int = 256, max_total_bytes: int = 128 * 1024 * 1024, max_member_bytes: int = 32 * 1024 * 1024) -> list[Path]:
    root = Path(destination)
    root.mkdir(parents=True, exist_ok=True)
    extracted: list[Path] = []
    total = 0
    with zipfile.ZipFile(archive) as bundle:
        infos = bundle.infolist()
        files = [info for info in infos if not info.is_dir()]
        if len(files) > max_files:
            raise ArtifactSafetyError("archive file-count limit exceeded")
        seen: set[PurePosixPath] = set()
        for info in infos:
            member = _safe_member_path(info.filename.rstrip("/") if info.is_dir() else info.filename)
            if member in seen:
                raise ArtifactSafetyError("duplicate archive member")
            seen.add(member)
            mode = (info.external_attr >> 16) & 0xFFFF
            if stat.S_ISLNK(mode):
                raise ArtifactSafetyError("symlink archive member forbidden")
            target = root.joinpath(*member.parts)
            if info.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            total += info.file_size
            if total > max_total_bytes:
                raise ArtifactSafetyError("archive total-size limit exceeded")
            with bundle.open(info, "r") as source:
                _copy_bounded(source, target, expected_size=info.file_size, max_member_bytes=max_member_bytes)
            extracted.append(target)
    return extracted


def safe_extract_tar(archive: str | Path, destination: str | Path, *, max_files: int = 256, max_total_bytes: int = 128 * 1024 * 1024, max_member_bytes: int = 32 * 1024 * 1024) -> list[Path]:
    root = Path(destination)
    root.mkdir(parents=True, exist_ok=True)
    extracted: list[Path] = []
    total = 0
    with tarfile.open(archive, mode="r:*") as bundle:
        members = bundle.getmembers()
        files = [member for member in members if member.isfile()]
        if len(files) > max_files:
            raise ArtifactSafetyError("archive file-count limit exceeded")
        seen: set[PurePosixPath] = set()
        for info in members:
            member = _safe_member_path(info.name.rstrip("/") if info.isdir() else info.name)
            if member in seen:
                raise ArtifactSafetyError("duplicate archive member")
            seen.add(member)
            if info.issym() or info.islnk() or info.isdev() or info.isfifo():
                raise ArtifactSafetyError("links/devices/fifos are forbidden")
            target = root.joinpath(*member.parts)
            if info.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            if not info.isfile():
                raise ArtifactSafetyError("unsupported tar member type")
            total += info.size
            if total > max_total_bytes:
                raise ArtifactSafetyError("archive total-size limit exceeded")
            source = bundle.extractfile(info)
            if source is None:
                raise ArtifactSafetyError("unable to read tar member")
            with source:
                _copy_bounded(source, target, expected_size=info.size, max_member_bytes=max_member_bytes)
            extracted.append(target)
    return extracted


def build_artifact_manifest(root: str | Path, *, max_files: int = 512, max_total_bytes: int = 256 * 1024 * 1024, max_member_bytes: int = 64 * 1024 * 1024) -> ArtifactManifest:
    base = Path(root).resolve(strict=True)
    entries: list[ArtifactEntry] = []
    total = 0
    for path in sorted(base.rglob("*")):
        if path.is_symlink():
            raise ArtifactSafetyError(f"symlink forbidden in artifact tree: {path}")
        if path.is_dir():
            continue
        if not path.is_file():
            raise ArtifactSafetyError(f"non-regular artifact: {path}")
        relative = path.relative_to(base).as_posix()
        _safe_member_path(relative)
        size = path.stat().st_size
        if size > max_member_bytes:
            raise ArtifactSafetyError("artifact member exceeds size limit")
        total += size
        if total > max_total_bytes:
            raise ArtifactSafetyError("artifact tree exceeds total-size limit")
        if len(entries) >= max_files:
            raise ArtifactSafetyError("artifact tree exceeds file-count limit")
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        entries.append(ArtifactEntry(relative_path=relative, byte_count=size, sha256=digest.hexdigest()))
    return ArtifactManifest(entries=entries, total_bytes=total)
