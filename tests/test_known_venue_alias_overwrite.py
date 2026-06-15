"""Regression test for INC-2026-05-08: curated venue alias must always win.

A `Янтарь холл` event whose post mentions the box-office point
`ТРЦ "Европа" 2 этаж` arrives with `city='Калининград'` and a confused
`location_address`. Without this fix the previous `addr_conflicts_with_name_match`
guard refused to overwrite either field, leaving 14 production events
with the wrong address/city even though `docs/reference/location-aliases.md`
already had the right alias.
"""

from __future__ import annotations

import main


def test_yantar_holl_alias_overwrites_kaliningrad_city_and_trc_evropa_address() -> None:
    payload = {
        "location_name": "Янтарь-холл",
        "location_address": "ТРЦ «Европа» 2 этаж",
        "city": "Калининград",
    }
    main._normalise_event_location_from_reference(payload)
    assert payload == {
        "location_name": "Янтарь холл",
        "location_address": "Ленина 11",
        "city": "Светлогорск",
    }


def test_yantar_holl_alias_overwrites_zal_yantarnyy_compound_name() -> None:
    payload = {
        "location_name": 'Зал "Янтарный", Театр эстрады "Янтарь-холл"',
        "location_address": "ТРЦ «Европа» 2 этаж",
        "city": "Калининград",
    }
    main._normalise_event_location_from_reference(payload)
    assert payload["location_name"] == "Янтарь холл"
    assert payload["location_address"] == "Ленина 11"
    assert payload["city"] == "Светлогорск"


def test_yantar_holl_alias_overwrites_teatr_estrady_form() -> None:
    payload = {
        "location_name": 'Театр эстрады "Янтарь-холл"',
        "location_address": "Калининград ТРЦ «Европа» 2 этаж",
        "city": "Калининград",
    }
    main._normalise_event_location_from_reference(payload)
    assert payload["location_name"] == "Янтарь холл"
    assert payload["location_address"] == "Ленина 11"
    assert payload["city"] == "Светлогорск"


def test_unknown_venue_left_alone() -> None:
    payload = {
        "location_name": "Какой-то неизвестный зал",
        "location_address": "Ленинский 999",
        "city": "Калининград",
    }
    main._normalise_event_location_from_reference(payload)
    assert payload == {
        "location_name": "Какой-то неизвестный зал",
        "location_address": "Ленинский 999",
        "city": "Калининград",
    }


def test_known_venue_without_alias_still_normalises_when_address_empty() -> None:
    payload = {"location_name": "Янтарь холл", "location_address": None, "city": None}
    main._normalise_event_location_from_reference(payload)
    assert payload["location_name"] == "Янтарь холл"
    assert payload["location_address"] == "Ленина 11"
    assert payload["city"] == "Светлогорск"


def test_canonical_venue_with_correct_address_unchanged() -> None:
    payload = {
        "location_name": "Сигнал",
        "location_address": "Леонова 22",
        "city": "Калининград",
    }
    main._normalise_event_location_from_reference(payload)
    assert payload == {
        "location_name": "Сигнал",
        "location_address": "Леонова 22",
        "city": "Калининград",
    }


def test_inc_2026_06_15_location_audit_aliases() -> None:
    cases = [
        (
            {"location_name": "Ростехarena", "location_address": None, "city": "Калининград"},
            ("Ростех Арена", "Солнечный бульвар 25", "Калининград"),
        ),
        (
            {"location_name": 'Клуб "СКЛАД" (Warehouse Club)', "location_address": None, "city": "Калининград"},
            ("СКЛАD", "Ялтинская 20П", "Калининград"),
        ),
        (
            {"location_name": "Дом с Горгульей", "location_address": None, "city": "Калининград"},
            ("Дом Горгульи", "Комсомольская 24", "Калининград"),
        ),
        (
            {"location_name": "Калининградский областной драматический театр", "location_address": "пр.Мира 4", "city": "Калининград"},
            ("Драматический театр", "Мира 4", "Калининград"),
        ),
        (
            {"location_name": "Лекторий центра Мой бизнес", "location_address": None, "city": "Калининград"},
            ("Лекторий Центра «Мой бизнес»", "Уральская 18, 4 этаж", "Калининград"),
        ),
    ]
    for payload, expected in cases:
        main._normalise_event_location_from_reference(payload)
        assert (
            payload["location_name"],
            payload["location_address"],
            payload["city"],
        ) == expected
