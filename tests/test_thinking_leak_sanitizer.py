"""Regression tests for ``_strip_thinking_leak`` (INC-2026-05-08).

Production event 4711 («Любовь по-итальянски», Telegraph
``Spektakl-Lyubov-po-italyanski-05-08``) shipped with a public
``event.description`` that contained both an English draft and a Russian
self-corrected version concatenated together, with the LLM's own
internal monologue (``Wait, let me rewrite the EC05 part ...``,
``Corrected description_md: ...``) visible on the rendered Telegraph
page.
"""

from __future__ import annotations

from smart_event_update import _strip_thinking_leak


def test_strips_self_correction_marker_keeps_only_final() -> None:
    txt = (
        "Постановка obtains special prize of the jury "
        "of the international annual theatrical festival \"Amur Autumn\".\n\n"
        "Wait, let me rewrite the EC05 part to be natural Russian.\n\n"
        "Corrected description_md: Комедия «Любовь по-итальянски».\n\n"
        "Постановка стала обладателем специального приза."
    )
    out = _strip_thinking_leak(txt)
    assert "obtains special prize" not in out
    assert "Wait, let me rewrite" not in out
    assert "Corrected description_md" not in out
    assert "Комедия «Любовь по-итальянски»." in out
    assert "Постановка стала обладателем" in out


def test_strips_standalone_wait_let_me_lines() -> None:
    txt = (
        "Концерт камерной музыки.\n\n"
        "Wait, let me rephrase that.\n\n"
        "Произведения Баха и Вивальди."
    )
    out = _strip_thinking_leak(txt)
    assert "Wait, let me rephrase" not in out
    assert "Концерт камерной музыки." in out
    assert "Произведения Баха и Вивальди." in out


def test_strips_actually_let_me_rewrite() -> None:
    txt = (
        "Initial draft text.\n\n"
        "Actually, let me rewrite this section.\n\n"
        "Final correct text."
    )
    out = _strip_thinking_leak(txt)
    assert "Actually, let me rewrite" not in out


def test_strips_note_to_self() -> None:
    txt = (
        "Спектакль о любви.\n\n"
        "Note to self: should add the venue name.\n\n"
        "Билеты в кассе."
    )
    out = _strip_thinking_leak(txt)
    assert "Note to self" not in out
    assert "Спектакль о любви." in out
    assert "Билеты в кассе." in out


def test_clean_text_unchanged() -> None:
    txt = "Концерт камерной музыки. Исполнители — известные музыканты."
    assert _strip_thinking_leak(txt) == txt


def test_empty_input() -> None:
    assert _strip_thinking_leak("") == ""
    assert _strip_thinking_leak(None) is None


def test_takes_last_correction_marker_when_multiple() -> None:
    txt = (
        "Draft 1.\n\n"
        "Corrected version: Draft 2.\n\n"
        "Wait, let me try again.\n\n"
        "Corrected description_md: Final answer here."
    )
    out = _strip_thinking_leak(txt)
    assert "Final answer here." in out
    assert "Draft 1" not in out
    assert "Draft 2" not in out
    assert "Corrected" not in out
