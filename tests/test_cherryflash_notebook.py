from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = ROOT / "kaggle" / "CherryFlash" / "cherryflash.ipynb"


def _cell_source(index: int) -> str:
    notebook = json.loads(NOTEBOOK_PATH.read_text(encoding="utf-8"))
    cell = notebook["cells"][index]
    assert cell["cell_type"] == "code"
    raw_source = cell.get("source", "")
    return "".join(raw_source) if isinstance(raw_source, list) else str(raw_source)


def test_cherryflash_notebook_installs_shared_story_helper_dependencies():
    source = _cell_source(0)

    assert "NOTEBOOK_VERSION = 'v17-story-media-safe'" in source
    assert "'opencv-python'" in source
    assert "'requests'" in source
    assert "'telethon'" in source
    assert "'cryptography'" in source


def test_cherryflash_notebook_hardens_apt_bootstrap_against_flaky_cran_repo():
    source = _cell_source(0)

    assert "def disable_flaky_apt_repos()" in source
    assert "cloud.r-project.org/bin/linux/ubuntu/jammy-cran40" in source
    assert "candidate.rename(disabled)" in source
    assert "def apt_update_resilient()" in source
    assert "'Acquire::Retries=3'" in source
    assert "'Acquire::By-Hash=force'" in source
    assert "apt_update_resilient()" in source


def test_cherryflash_notebook_uses_common_story_publish_helper():
    source = _cell_source(1)

    assert "helper_path = source_folder / 'kaggle_common' / 'story_publish.py'" in source
    assert "from story_publish import preflight_story_publish_from_kaggle, publish_story_from_kaggle" in source
    assert "thread = threading.Thread(target=_runner)" in source
    assert "asyncio.get_running_loop()" in source
    assert "story_publish_requested = bool((selection_manifest or {}).get('story_publish_enabled'))" in source
    assert "CherryFlash story publish requested but shared story helper is unavailable in Kaggle bundle" in source
    assert "CherryFlash story publish requested but story_publish.json was not mounted into Kaggle input" in source


def test_bootstrap_installs_and_imports_pillow_in_blender_interpreter(monkeypatch, tmp_path):
    import subprocess

    bundle = tmp_path / "blender"
    blender = bundle / "blender"
    python = bundle / "4.5/python/bin/python3.11"
    python.parent.mkdir(parents=True)
    blender.touch()
    python.touch()
    source = _cell_source(0).replace("/opt/blender-4.5.4-linux-x64", str(bundle))
    source = source.replace("/etc/apt/sources.list.d", str(tmp_path / "no-apt-sources"))
    installed = set()
    checked = []

    def fake_run(cmd, **kwargs):
        if cmd[0] == str(python) and cmd[1:4] == ["-m", "pip", "install"]:
            installed.update(cmd[4:])
        if cmd[0] == str(blender) and "--python-expr" in cmd:
            if "pillow" not in installed:
                raise subprocess.CalledProcessError(1, cmd, stderr="No module named PIL")
            checked.append(True)
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(subprocess, "run", fake_run)
    exec(compile(source, str(NOTEBOOK_PATH), "exec"), {})
    assert "pillow" in installed
    assert checked == [True]
