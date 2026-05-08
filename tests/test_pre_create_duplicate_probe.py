"""Regression tests for ``_pre_create_duplicate_probe`` (INC-2026-05-08).

After the deterministic anchor + LLM matcher chain says "no match", the
probe is the last safety net before ``INSERT event``. It catches the dup
classes that produced events 4584/4585 (Барн), 2819/4038 (Ансамбль),
4173/4200 (Окна Победы) where the existing matchers happened to look the
other way.
"""

from __future__ import annotations

from dataclasses import dataclass

from smart_event_update import _pre_create_duplicate_probe


@dataclass
class _Cand:
    title: str
    date: str
    end_date: str | None = None
    time: str | None = None
    time_is_default: bool = False
    location_name: str | None = None
    location_address: str | None = None
    city: str | None = None
    ticket_link: str | None = None
    source_type: str = "vk"


@dataclass
class _Event:
    id: int
    title: str
    date: str
    end_date: str | None = None
    time: str | None = None
    time_is_default: bool = False
    location_name: str | None = None
    location_address: str | None = None
    city: str | None = None
    ticket_link: str | None = None


def test_branch1_ticket_link_parity_with_related_title() -> None:
    cand = _Cand(
        title="Концерт «Портрет девушки-бойца»",
        date="2026-05-08",
        time="19:00",
        location_name="Научная библиотека",
        ticket_link="https://vmuzey.com/event/portret-devushek-boycov",
    )
    existing = _Event(
        id=4445,
        title="Концерт «Портрет девушки-бойца»",
        date="2026-05-08",
        time="19:00",
        location_name="Научная библиотека",
        ticket_link="https://vmuzey.com/event/portret-devushek-boycov",
    )
    assert _pre_create_duplicate_probe(cand, [existing]) is existing


def test_branch1_ticket_link_parity_blocks_unrelated_titles() -> None:
    # Same season-subscription URL must NOT drag two unrelated programs together.
    cand = _Cand(
        title="Карнавальная ночь — балет в одно отделение",
        date="2026-05-08",
        time="19:00",
        location_name="Янтарь холл",
        ticket_link="https://янтарьхолл.рф/season/",
    )
    existing = _Event(
        id=2819,
        title="Ансамбль песни и пляски Балтийского флота",
        date="2026-05-08",
        time="19:00",
        location_name="Янтарь холл",
        ticket_link="https://янтарьхолл.рф/season/",
    )
    assert _pre_create_duplicate_probe(cand, [existing]) is None


def test_branch2_location_time_related_title_matches_repost() -> None:
    # Cross-source repost where one had ticket URL and the other did not.
    cand = _Cand(
        title="Окна Победы",
        date="2026-05-08",
        time="17:00",
        location_name="Научная библиотека, Мира 9, Калининград",
    )
    existing = _Event(
        id=4173,
        title="Окна Победы",
        date="2026-05-08",
        time="17:00",
        location_name="Научная библиотека, Мира 9, Калининград",
        ticket_link=None,
    )
    assert _pre_create_duplicate_probe(cand, [existing]) is existing


def test_branch2_blocks_when_time_anchor_is_missing_on_either_side() -> None:
    # Without a time anchor on either side we cannot trust the location-only match.
    cand = _Cand(
        title="Окна Победы",
        date="2026-05-08",
        location_name="Научная библиотека",
    )
    existing = _Event(
        id=4173,
        title="Окна Победы",
        date="2026-05-08",
        time="17:00",
        location_name="Научная библиотека",
    )
    assert _pre_create_duplicate_probe(cand, [existing]) is None


def test_branch2_blocks_unrelated_titles_at_same_anchor() -> None:
    # Two genuinely different events at the same venue/time/date (rare but real).
    cand = _Cand(
        title="Лекция о теории относительности",
        date="2026-05-08",
        time="19:00",
        location_name="Научная библиотека",
    )
    existing = _Event(
        id=4445,
        title="Концерт «Портрет девушки-бойца»",
        date="2026-05-08",
        time="19:00",
        location_name="Научная библиотека",
    )
    assert _pre_create_duplicate_probe(cand, [existing]) is None


def test_skip_for_parser_canonical_sources() -> None:
    cand = _Cand(
        title="Концерт",
        date="2026-05-08",
        time="19:00",
        location_name="Янтарь холл",
        ticket_link="https://янтарьхолл.рф/x",
        source_type="parser:muzteatr",
    )
    existing = _Event(
        id=2819,
        title="Концерт",
        date="2026-05-08",
        time="19:00",
        location_name="Янтарь холл",
        ticket_link="https://янтарьхолл.рф/x",
    )
    assert _pre_create_duplicate_probe(cand, [existing]) is None


def test_skip_for_empty_shortlist_or_meaningless_title() -> None:
    cand = _Cand(title="Концерт", date="2026-05-08", time="19:00", location_name="X")
    assert _pre_create_duplicate_probe(cand, []) is None
    cand2 = _Cand(title="", date="2026-05-08", time="19:00", location_name="X")
    existing = _Event(id=1, title="Y", date="2026-05-08", time="19:00", location_name="X")
    assert _pre_create_duplicate_probe(cand2, [existing]) is None
