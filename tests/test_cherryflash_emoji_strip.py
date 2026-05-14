from __future__ import annotations

import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


pytest.importorskip("moviepy")

from scripts.render_cherryflash_full import _strip_leading_emoji  # noqa: E402


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("🌊 Концерт", "Концерт"),
        ("✨Премьера", "Премьера"),
        ("🎸  Рок-фест", "Рок-фест"),
        ("🇷🇺 День города", "День города"),
        ("Без эмодзи", "Без эмодзи"),
        ("", ""),
        ("    ", ""),
        ("🍃🌿 Природа", "Природа"),
    ],
)
def test_strip_leading_emoji_cases(raw, expected):
    assert _strip_leading_emoji(raw) == expected


def test_strip_leading_emoji_keeps_internal_emoji():
    # Only the leading emoji is stripped; inline emoji stays untouched.
    assert _strip_leading_emoji("Концерт 🎸 в парке") == "Концерт 🎸 в парке"
