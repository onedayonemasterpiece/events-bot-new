from __future__ import annotations

from video_announce.cherryflash_text import event_count_label


def test_event_count_label_uses_russian_plural_forms() -> None:
    assert event_count_label(1) == "1 событие"
    assert event_count_label(2) == "2 события"
    assert event_count_label(4) == "4 события"
    assert event_count_label(5) == "5 событий"
    assert event_count_label(21) == "21 событие"
