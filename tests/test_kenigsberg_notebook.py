import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = ROOT / "kaggle" / "KoenigsbergStories" / "koenigsberg_stories.ipynb"


def test_kenigsberg_notebook_code_cells_compile() -> None:
    notebook = json.loads(NOTEBOOK_PATH.read_text(encoding="utf-8"))

    for index, cell in enumerate(notebook["cells"]):
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source") or [])
        assert "\\n" not in source
        compile(source, f"{NOTEBOOK_PATH}:cell-{index}", "exec")


def test_kenigsberg_notebook_uses_common_story_publish_helper() -> None:
    notebook = json.loads(NOTEBOOK_PATH.read_text(encoding="utf-8"))
    source = "\n".join(
        "".join(cell.get("source") or [])
        for cell in notebook["cells"]
        if cell.get("cell_type") == "code"
    )

    assert "helper_path = source_folder / 'kaggle_common' / 'story_publish.py'" in source
    assert "from story_publish import preflight_story_publish_from_kaggle, publish_story_from_kaggle" in source
    assert "Kenigsberg story publish requested but story_publish.json was not mounted into Kaggle input" in source
    assert "publish_story_from_kaggle(final_video_path=final_mp4" in source
