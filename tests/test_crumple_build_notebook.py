from __future__ import annotations

import ast
import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CRUMPLE_DIR = ROOT / "kaggle" / "CrumpleVideo"
NOTEBOOK_PATH = CRUMPLE_DIR / "crumple_video.ipynb"
POSTER_MODULE_PATH = CRUMPLE_DIR / "poster_overlay.py"
STORY_MODULE_PATH = CRUMPLE_DIR / "story_publish.py"
GESTURE_MODULE_PATH = CRUMPLE_DIR / "story_gesture_overlay.py"
BUILD_NOTEBOOK_PATH = CRUMPLE_DIR / "build_notebook.py"


def _load_build_notebook_module():
    spec = importlib.util.spec_from_file_location("crumple_build_notebook", BUILD_NOTEBOOK_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _pipeline_source() -> str:
    notebook = json.loads(NOTEBOOK_PATH.read_text(encoding="utf-8"))
    for cell in notebook.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        raw_source = cell.get("source", "")
        source = "".join(raw_source) if isinstance(raw_source, list) else str(raw_source)
        if "def _ensure_poster_overlay_module():" in source:
            return source
    raise AssertionError("CrumpleVideo pipeline cell not found")


def _embedded_module_source(source: str, *, anchor: str) -> str:
    anchor_index = source.index(anchor)
    start = source.index("        code = ", anchor_index) + len("        code = ")
    end = source.index("\n        target.write_text(code, encoding='utf-8')", start)
    return ast.literal_eval(source[start:end])


def test_replace_embedded_module_does_not_leave_double_quote():
    module = _load_build_notebook_module()
    source = (
        "def _ensure_story_publish_module():\n"
        "    target = Path('/kaggle/working/story_publish.py')\n"
        "    try:\n"
        "        code = 'old helper'\n"
        "        target.write_text(code, encoding='utf-8')\n"
        "    except Exception:\n"
        "        pass\n"
    )

    actual = module._replace_embedded_module(
        source,
        anchor="def _ensure_story_publish_module():",
        module_source="print(\"story ok\")\nprint('still ok')",
    )

    assert "code = 'old helper'" not in actual
    assert "print(\"story ok\")" in actual
    assert "''\n        target.write_text" not in actual
    ast.parse(actual)


def test_embedded_helpers_match_repo_sources():
    source = _pipeline_source()

    embedded_poster = _embedded_module_source(
        source,
        anchor="def _ensure_poster_overlay_module():",
    )
    embedded_story = _embedded_module_source(
        source,
        anchor="def _ensure_story_publish_module():",
    )
    embedded_gesture = _embedded_module_source(
        source,
        anchor="def _ensure_story_gesture_overlay_module():",
    )

    assert embedded_poster == POSTER_MODULE_PATH.read_text(encoding="utf-8").rstrip("\n")
    assert embedded_story == STORY_MODULE_PATH.read_text(encoding="utf-8").rstrip("\n")
    assert embedded_gesture == GESTURE_MODULE_PATH.read_text(encoding="utf-8").rstrip("\n")
    assert "from story_gesture_overlay import GESTURE_STEP_COUNT, apply_story_gesture_frame" in source
    assert (
        "from story_publish import load_story_publish_runtime, preflight_story_publish_from_kaggle, publish_story_from_kaggle"
        in source
    )
    assert "pending_gesture_step" in source
    assert "KAGGLE_SECRETS_READY = False" in source
    assert "def _load_telegram_auth(log):" in source
    assert "Telegram auth source: story runtime" in source
    assert "def _safe_telegram_cache_path(url: str, idx: int, posters_dir: Path) -> Path:" in source
    assert "telegram_cache = _prepare_telegram_cache(urls_to_cache, posters_dir)" in source
    assert "telegram_cache = {url: path for url, path in url_map.items() if path.exists()}" not in source
    assert 'local_path = posters_dir / f"tg_{idx}_{fname}"' not in source
