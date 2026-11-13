"""Exercise real collector HTML parsing without browser installation or HTTP."""
import ast
from datetime import datetime
import json
import logging
from pathlib import Path
import re

from bs4 import BeautifulSoup
import pytest


def parse(html):
    source = Path("kaggle/ParseQtickets/parse_qtickets.py").read_text()
    nodes = [n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef)
             and n.name in {"normalize_location", "parse_event_detail"}]
    scope = dict(json=json, re=re, datetime=datetime, logger=logging.getLogger(__name__),
                 get_soup=lambda _: BeautifulSoup(html, "html.parser"))
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "<qtickets-collector>", "exec"), scope)
    return scope["parse_event_detail"]("https://kaliningrad.qtickets.events/273677-impuls")


def test_real_impuls_hero_uses_declared_two_x_and_preserves_structured_facts():
    html = Path("tests/fixtures/qtickets_impuls_poster.html").read_text()
    event = parse(html)
    soup = BeautifulSoup(html, "html.parser")
    structured = json.loads(soup.script.string)
    assert event["photos"] == [
        "https://cdn.qtickets.tech/thumbs/633791_0_634_0_0_restricted_90_96f3a79f.webp"
    ]
    assert event["title"] == structured["name"]
    assert event["date_raw"] == structured["startDate"]
    assert event["location"] == structured["location"]["name"]


@pytest.mark.parametrize("hero", ["", '<img srcset="broken 2q">',
                                   '<img srcset="https://example.com/poster.webp 1x">',
                                   '<img srcset="https://example.com/logo.webp 10x">'])
def test_existing_image_fallback_survives_missing_invalid_or_unrelated_srcset(hero):
    html = '<script type="application/ld+json">' + json.dumps({
        "@type": "Event", "name": "Event", "startDate": "2026-11-29T19:00:00+02:00",
        "image": ["https://example.com/original.jpg"],
    }) + '</script>'
    html += f'<div class="{"other" if "logo" in hero else "poster-container"}">{hero}</div>'
    assert parse(html)["photos"] == ["https://example.com/original.jpg"]
