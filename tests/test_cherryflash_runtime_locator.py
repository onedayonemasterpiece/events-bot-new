from __future__ import annotations

import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "kaggle" / "CherryFlash" / "runtime_locator.py"
SPEC = importlib.util.spec_from_file_location("cherryflash_runtime_locator", MODULE_PATH)
assert SPEC and SPEC.loader
runtime_locator = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runtime_locator)

find_mounted_bundle = runtime_locator.find_mounted_bundle
pick_bundle_archive = runtime_locator.pick_bundle_archive


def _write_bundle(root: Path) -> None:
    script_path = root / "scripts" / "render_cherryflash_full.py"
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text("print('ok')\n", encoding="utf-8")


def test_find_mounted_bundle_prefers_latest_cherryflash_session(tmp_path: Path) -> None:
    older = tmp_path / "cherryflash-session-142-1776020451"
    newer = tmp_path / "cherryflash-session-144-1776027988"
    _write_bundle(older)
    _write_bundle(newer)

    resolved = find_mounted_bundle(tmp_path)

    assert resolved == newer


def test_find_mounted_bundle_prefers_session_over_generic_bundle(tmp_path: Path) -> None:
    generic = tmp_path / "cherryflash-runtime"
    session = tmp_path / "cherryflash-session-145-1776030000"
    _write_bundle(generic)
    _write_bundle(session)

    resolved = find_mounted_bundle(tmp_path)

    assert resolved == session


def test_pick_bundle_archive_prefers_latest_session_archive(tmp_path: Path) -> None:
    older = tmp_path / "cherryflash-session-142-1776020451"
    newer = tmp_path / "cherryflash-session-144-1776027988"
    older.mkdir(parents=True, exist_ok=True)
    newer.mkdir(parents=True, exist_ok=True)
    (older / "cherryflash.zip").write_bytes(b"older")
    (newer / "cherryflash.zip").write_bytes(b"newer")

    resolved = pick_bundle_archive(tmp_path, "cherryflash.zip")

    assert resolved == newer / "cherryflash.zip"
