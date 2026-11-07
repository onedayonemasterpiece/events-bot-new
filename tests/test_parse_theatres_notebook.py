import json
import ast
import re
from pathlib import Path

import pytest
from bs4 import BeautifulSoup


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


@pytest.mark.asyncio
@pytest.mark.parametrize("body_mode", ["full", "list_only", "empty", "absent"])
async def test_muz_details_preserves_product_prose_and_metadata_fallback(body_mode):
    """The real DOM boundary must not substitute a preview or sibling content."""
    prose = "Описание постановки с подробной программой. " * 65
    warning = "В спектакле используется сценический дым."
    product = (
        '<div class="my_left_content"><div class="mycol">'
        '<h3></h3><div class="spek_bor">Премьера: 2023</div>'
        f'<p>{prose if body_mode == "full" else " "}</p>'
        f'<p><span>{warning if body_mode == "full" else ""}</span></p>'
        f'<ul><li>{"Первый пункт программы" if body_mode in ["full", "list_only"] else ""}</li>'
        f'<li>{"Второй пункт программы" if body_mode in ["full", "list_only"] else ""}</li></ul>'
        '<h3>Ближайшие спектакли</h3><div class="row"><p>Другой сеанс</p></div>'
        '</div><div class="mycol"><p>Посторонний отзыв</p></div></div>'
        if body_mode != "absent" else ""
    )
    html = '<meta property="og:description" content="Краткий анонс">' + product
    html += '<aside class="mycol"><p>Описание соседнего спектакля</p></aside>'

    class Page:
        async def goto(self, *args, **kwargs):
            pass

        async def close(self):
            pass

    class Context:
        async def new_page(self):
            return Page()

    async def content(page):
        return html

    node = next(n for n in ast.parse(_source()).body if isinstance(n, ast.AsyncFunctionDef)
                and n.name == "scrape_muz_details")
    namespace = {"BeautifulSoup": BeautifulSoup, "re": re,
                 "stable_page_content": content, "BASE_URL_MUZ": "https://muzteatr39.ru"}
    exec(compile(ast.Module(body=[node], type_ignores=[]), "notebook", "exec"), namespace)
    url = "https://muzteatr39.ru/spektakli/muzykalnye/bremenskie-muzykanty/"
    result = await namespace["scrape_muz_details"](Context(), [url])
    description = result[url]["description"]
    if body_mode == "full":
        assert prose.strip() in description
        assert warning in description
        assert "Краткий анонс" not in description
    elif body_mode == "list_only":
        assert "Краткий анонс" not in description
    else:
        assert description == "Краткий анонс"
    if body_mode in ["full", "list_only"]:
        assert "Первый пункт программы\nВторой пункт программы" in description
    assert not any(text in description for text in
                   ["Другой сеанс", "Посторонний отзыв", "соседнего спектакля", "2023"])
