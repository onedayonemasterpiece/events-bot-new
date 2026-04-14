from __future__ import annotations

from pathlib import Path
import re


EXPECTED_RENDER_SCRIPT = Path("scripts/render_cherryflash_full.py")


def _dataset_priority(name: str) -> int:
    lowered = name.strip().casefold()
    if lowered.startswith("cherryflash-session-"):
        return 0
    if lowered.startswith("cherryflash-runtime"):
        return 1
    if lowered.startswith("cherryflash"):
        return 2
    return 9


def _dataset_timestamp(name: str) -> int:
    match = re.search(r"(\d+)$", name.strip())
    if not match:
        return 0
    try:
        return int(match.group(1))
    except Exception:
        return 0


def _bundle_sort_key(bundle_root: Path, *, input_root: Path) -> tuple[int, int, float]:
    try:
        top_name = bundle_root.relative_to(input_root).parts[0]
    except Exception:
        top_name = bundle_root.name
    try:
        mtime = bundle_root.stat().st_mtime
    except Exception:
        mtime = 0.0
    return (
        _dataset_priority(top_name),
        -_dataset_timestamp(top_name),
        -mtime,
    )


def find_scripts_root(base: Path) -> Path | None:
    direct = base / EXPECTED_RENDER_SCRIPT
    if direct.exists():
        return base
    matches = sorted(base.rglob(str(EXPECTED_RENDER_SCRIPT)))
    if not matches:
        return None
    matches.sort(key=lambda path: (len(path.parts), path.as_posix()))
    return matches[0].parents[1]


def find_mounted_bundle(input_root: Path) -> Path | None:
    if not input_root.exists():
        return None
    candidates: list[Path] = []
    for top_level in sorted((p for p in input_root.iterdir() if p.is_dir()), key=lambda p: p.name):
        resolved = find_scripts_root(top_level)
        if resolved is not None:
            candidates.append(resolved)
    if not candidates:
        return None
    candidates.sort(key=lambda path: _bundle_sort_key(path, input_root=input_root))
    return candidates[0]


def pick_bundle_archive(input_root: Path, archive_name: str) -> Path | None:
    if not input_root.exists():
        return None
    archives: list[tuple[tuple[int, int, float], Path]] = []
    for top_level in sorted((p for p in input_root.iterdir() if p.is_dir()), key=lambda p: p.name):
        for archive in sorted(top_level.rglob(archive_name)):
            archives.append((_bundle_sort_key(top_level, input_root=input_root), archive))
    if not archives:
        return None
    archives.sort(key=lambda item: item[0])
    return archives[0][1]
