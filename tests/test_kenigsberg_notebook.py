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
