"""Regression tests for ``fallback_one_sentence`` (INC-2026-05-08).

Production event 3983 (`Эдит Пиаф`) shipped with
``short_description='> Спектакль-байопик «Эдит Пиаф.'`` because the
fallback (a) did not strip the leading ``>`` markdown blockquote and
(b) split sentences on the period that sits inside the title proper
noun. Both classes are covered here.
"""

from __future__ import annotations

from digest_helper import fallback_one_sentence


def test_strips_leading_blockquote_marker() -> None:
    out = fallback_one_sentence(
        "> Спектакль-байопик о судьбе певицы.",
        max_words=16,
    )
    assert out is not None
    assert not out.startswith(">")


def test_does_not_split_inside_russian_guillemets_period() -> None:
    out = fallback_one_sentence(
        "Спектакль-байопик «Эдит Пиаф. На Балу удачи» рассказывает о судьбе. "
        "Постановка охватывает жизненный путь.",
        max_words=16,
    )
    # Must NOT cut on the period inside «Эдит Пиаф. На Балу удачи»; must cut
    # on the period after «о судьбе.».
    assert out is not None
    assert "«Эдит Пиаф. На Балу удачи»" in out
    assert "Постановка охватывает" not in out


def test_handles_combined_blockquote_and_titled_period() -> None:
    out = fallback_one_sentence(
        "> Спектакль-байопик «Эдит Пиаф. На Балу удачи» рассказывает о судьбе.",
        max_words=16,
    )
    assert out is not None
    assert not out.startswith(">")
    assert "«Эдит Пиаф. На Балу удачи»" in out


def test_question_marks_inside_guillemets_preserved() -> None:
    out = fallback_one_sentence(
        "Лекция «Кто? Что? Где?» о криминальных загадках. Проводит профессор.",
        max_words=16,
    )
    assert out is not None
    assert "«Кто? Что? Где?»" in out
    assert "Проводит" not in out


def test_period_at_end_of_title_only() -> None:
    out = fallback_one_sentence(
        "Спектакль «Конец.» рассказывает о неизбежном. И финал.",
        max_words=16,
    )
    assert out is not None
    assert "«Конец.»" in out
    assert "финал" not in out


def test_plain_text_no_quotes_unchanged_behavior() -> None:
    out = fallback_one_sentence(
        "Концерт камерной музыки. Произведения Баха и Вивальди.",
        max_words=16,
    )
    assert out == "Концерт камерной музыки."


def test_empty_input() -> None:
    assert fallback_one_sentence("", max_words=16) is None
    assert fallback_one_sentence(None, max_words=16) is None
