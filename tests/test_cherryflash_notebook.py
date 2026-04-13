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

    assert "NOTEBOOK_VERSION = 'v11-crumple-shared-runtime-deps'" in source
    assert "'opencv-python'" in source
    assert "'requests'" in source
    assert "'telethon'" in source
    assert "'cryptography'" in source


def test_cherryflash_notebook_uses_common_story_publish_helper():
    source = _cell_source(1)

    assert "helper_path = source_folder / 'kaggle_common' / 'story_publish.py'" in source
    assert "from story_publish import preflight_story_publish_from_kaggle, publish_story_from_kaggle" in source
