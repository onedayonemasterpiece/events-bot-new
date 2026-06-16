"""Regression test for telegraph tel:-as-contact-line rendering.

Telegraph silently strips ``tel:`` from ``<a href>`` (only ``http``,
``https``, ``mailto`` survive its allowlist). When ``ticket_link``
arrives as ``tel:+79114743004`` the public infoblock used to render
``<a href="tel:...">Билеты</a>`` which Telegraph collapsed to a plain
``Билеты`` word with no clickable phone (see
INC-2026-05-07-vk-auto-import-merge-regression-gemma4 follow-up).
``format_tel_link_for_display`` is the helper that lifts the digits
out of ``tel:`` URIs so callers can show the phone as plain text
instead.
"""

from markup import format_tel_link_for_display


def test_format_russian_eleven_digit_with_eight_prefix() -> None:
    assert format_tel_link_for_display("tel:89673569479") == "+7 (967) 356-94-79"


def test_format_russian_eleven_digit_with_plus_seven_prefix() -> None:
    assert format_tel_link_for_display("tel:+79114743004") == "+7 (911) 474-30-04"


def test_format_kaliningrad_landline_with_4012_area_code() -> None:
    assert format_tel_link_for_display("tel:+74012463635") == "+7 (4012) 46-36-35"


def test_format_russian_ten_digit_without_country_code() -> None:
    assert format_tel_link_for_display("tel:9114743004") == "+7 (911) 474-30-04"


def test_format_strips_separators_and_parens() -> None:
    assert format_tel_link_for_display("tel:+7 (911) 474-30-04") == "+7 (911) 474-30-04"


def test_format_returns_empty_for_non_tel_scheme() -> None:
    assert format_tel_link_for_display("https://kassir.ru/x") == ""
    assert format_tel_link_for_display("") == ""
    assert format_tel_link_for_display(None) == ""


def test_format_returns_empty_for_tel_without_digits() -> None:
    assert format_tel_link_for_display("tel:") == ""
    assert format_tel_link_for_display("tel:abcdef") == ""


def test_format_falls_back_for_non_russian_length() -> None:
    # 13-digit international number stays as-is with leading +.
    assert format_tel_link_for_display("tel:441234567890") == "+441234567890"
