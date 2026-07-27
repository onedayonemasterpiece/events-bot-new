import json
from pathlib import Path


def _source() -> str:
    notebook = json.loads(
        Path("kaggle/ParseTheatres/parse_theatres.ipynb").read_text(encoding="utf-8")
    )
    return "".join(
        "".join(cell.get("source", []))
        for cell in notebook.get("cells", [])
        if cell.get("cell_type") == "code"
    )


def test_theatre_notebook_retries_page_content_during_navigation() -> None:
    source = _source()

    assert "async def stable_page_content(page, attempts=5):" in source
    assert "await page.wait_for_load_state('domcontentloaded', timeout=15000)" in source
    assert "await page.wait_for_timeout(1000)" in source
    # Every parser DOM read is guarded; the only direct content() call lives
    # inside the retry helper itself.
    assert source.count("await page.content()") == 1
    assert source.count("await stable_page_content(page)") == 5
