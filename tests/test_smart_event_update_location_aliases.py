from __future__ import annotations

import smart_event_update as su
from location_reference import (
    canonicalize_known_place_name,
    find_known_venue_in_text,
    match_known_venue,
    match_known_venue_by_address,
    normalise_event_location_from_reference,
)


def test_gate_locations_do_not_collapse_into_one_bucket() -> None:
    assert su._normalize_location("Закхаймские ворота") == "закхаймские ворота"
    assert su._normalize_location("Арт-пространство Ворота") == "закхаймские ворота"
    assert su._normalize_location("Арт-пространство Ворота, Литовский Вал 61") == "закхаймские ворота"

    assert su._normalize_location("Фридландские ворота") == "фридландские ворота"
    assert su._normalize_location("Фридландские ворота, Дзержинского 30, Калининград") == "фридландские ворота"

    assert su._normalize_location("Железнодорожные ворота") == "железнодорожные ворота"
    assert (
        su._normalize_location("Железнодорожные ворота, Гвардейский проспект 51А, Калининград")
        == "железнодорожные ворота"
    )


def test_solenaya_vorona_railway_street_is_not_railway_gates() -> None:
    """Regression for INC-2026-06-29: street name != Kaliningrad Railway Gates."""

    assert (
        su._canonicalize_location_fields(
            location_name="Театральная гостиная Солёная ворона",
            location_address="Железнодорожная 1",
            city="Зеленоградск",
        )
        == ("Театральная гостиная Солёная ворона", "Железнодорожная 1", "Зеленоградск")
    )
    assert (
        su._normalize_location(
            "Театральная гостиная Солёная ворона, Железнодорожная 1, Зеленоградск"
        )
        != "железнодорожные ворота"
    )


def test_konb_room_location_uses_source_building_not_room() -> None:
    """Regression for INC-2026-06-29: KОНБ room names are not public venues."""

    assert (
        su._canonicalize_location_fields(
            location_name="ЧИТАЛЬНЫЙ ЗАЛ",
            location_address="Мира 9",
            city="Калининград",
            source_url="https://vk.com/wall-30777579_15489",
        )
        == ("Научная библиотека", "Мира 9", "Калининград")
    )
    assert (
        su._canonicalize_location_fields(
            location_name="4 ЭТАЖ ЛЕКЦИОННЫЙ ЗАЛ",
            location_address="Мира 9",
            city="Калининград",
            source_url="https://vk.com/wall-30777579_15501",
        )
        == ("Научная библиотека", "Мира 9", "Калининград")
    )
    assert (
        su._canonicalize_location_fields(
            location_name="читальный зал, 2 этаж",
            location_address="Мира 9",
            city="Калининград",
            source_url="https://vk.com/wall-30777579_15514",
        )
        == ("Научная библиотека", "Мира 9", "Калининград")
    )


def test_konb_room_location_negative_controls() -> None:
    """Do not classify generic room names without the source grounding."""

    assert (
        su._canonicalize_location_fields(
            location_name="ЧИТАЛЬНЫЙ ЗАЛ",
            location_address="Мира 9",
            city="Калининград",
            source_url="https://vk.com/wall-123_456",
        )
        == ("ЧИТАЛЬНЫЙ ЗАЛ", "Мира 9", "Калининград")
    )
    assert (
        su._canonicalize_location_fields(
            location_name="Дом китобоя",
            location_address="Мира 9",
            city="Калининград",
            source_url="https://vk.com/wall-30777579_15489",
        )
        == ("Дом китобоя", "Мира 9", "Калининград")
    )


def test_generic_vorota_is_not_forced_into_zakheim_bucket() -> None:
    assert su._normalize_location("Ворота") == "ворота"


def test_generic_city_park_not_fuzzy_bound_via_location_reference() -> None:
    """Regression for INC-2026-07-02 / INC-2026-06-26.

    Smart Update uses location_reference.py directly.  A generic municipal park
    in Pionersky must remain source-grounded and must not bind to the known
    Зеленоградск culture-center venue through the shared token ``городской``.
    """

    payload = {
        "location_name": "Городской парк",
        "location_address": None,
        "city": "Пионерский",
    }
    normalise_event_location_from_reference(payload)
    assert payload == {
        "location_name": "Городской парк",
        "location_address": None,
        "city": "Пионерский",
    }
    assert match_known_venue("Городской парк", city="Пионерский") is None
    assert (
        su._canonicalize_location_fields(
            location_name="Городской парк",
            location_address=None,
            city="Пионерский",
            source_url="https://vk.com/wall-169817694_32270",
        )
        == ("Городской парк", None, "Пионерский")
    )


def test_new_incident_location_aliases_resolve_to_canonical_venues() -> None:
    camember = match_known_venue('сырный магазин "Камамбер"', city="Зеленоградск")
    assert camember is not None
    assert camember.name == "Сырный магазин Камамбер"
    assert camember.address == "Потемкина 20Б"

    les = match_known_venue("бар ЛЕС", city="Светлогорск")
    assert les is not None
    assert les.name == "Бар ЛЕС"

    gusev = match_known_venue("Станция Гусев", city="Гусев")
    assert gusev is not None
    assert gusev.name == "Железнодорожный вокзал Гусев"

    fort = match_known_venue("Форт №11 «Дёнхофф»")
    assert fort is not None
    assert fort.name == "Форт №11 Дёнхофф"
    assert fort.city == "Калининград"

    cultural_place_payload = {
        "location_name": "Культурное место на Острове Канта",
        "location_address": "",
        "city": "Калининград",
    }
    normalise_event_location_from_reference(cultural_place_payload)
    assert cultural_place_payload == {
        "location_name": "Культурное место",
        "location_address": "Остров Канта",
        "city": "Калининград",
    }


def test_place_alias_canonicalizes_yantarnoe_to_yantarny() -> None:
    assert canonicalize_known_place_name("Янтарное") == "Янтарный"
    assert canonicalize_known_place_name("Янтарный") == "Янтарный"
    payload = {
        "location_name": "Янтарное",
        "location_address": None,
        "city": "Янтарное",
    }
    normalise_event_location_from_reference(payload)
    assert payload["location_name"] == "Янтарный"
    assert payload["city"] == "Янтарный"

    already_canonical = {
        "location_name": "Янтарный",
        "location_address": None,
        "city": "Янтарный",
    }
    normalise_event_location_from_reference(already_canonical)
    assert already_canonical["location_name"] == "Янтарный"
    assert already_canonical["city"] == "Янтарный"


def test_inc_2026_05_09_location_aliases_resolve_to_canonical_venues() -> None:
    yantarny = find_known_venue_in_text('перед дворцом спорта "Янтарный"')
    assert yantarny is not None
    assert yantarny.name == "Дворец спорта «Янтарный»"
    assert yantarny.address == "Согласия 39"

    signal_payload = {
        "location_name": "Арт-пространство Сигнал",
        "location_address": "Космонавта Леонова 22",
        "city": "Калининград",
    }
    normalise_event_location_from_reference(signal_payload)
    assert signal_payload == {
        "location_name": "Сигнал",
        "location_address": "Леонова 22",
        "city": "Калининград",
    }

    library_payload = {
        "location_name": "библиотека",
        "location_address": "Мира 9",
        "city": "Калининград",
    }
    normalise_event_location_from_reference(library_payload)
    assert library_payload == {
        "location_name": "Научная библиотека",
        "location_address": "Мира 9",
        "city": "Калининград",
    }

    dreadnought = match_known_venue('бар "Дредноут"', city="Калининград")
    assert dreadnought is not None
    assert dreadnought.name == "Бар Дредноут"
    assert dreadnought.address == "Генделя 5"

    mysig = match_known_venue("Шоурум Mysig", city="Калининград")
    assert mysig is not None
    assert mysig.name == "Шоурум Mysig"
    assert mysig.address == "Судостроительная 6/1"

    icae = match_known_venue("ИЦАЭ Калининграда", city="Калининград")
    assert icae is not None
    assert icae.name == "ИЦАЭ (в КГТУ)"


def test_inc_2026_07_27_house_number_prefix_does_not_bind_icae() -> None:
    """Советский 1 and Советский 12 are different attendee-facing addresses."""

    assert (
        match_known_venue_by_address(
            "Советский проспект, 12",
            city="Калининград",
        )
        is None
    )
    assert (
        match_known_venue(
            "Советский пр-т 12, 8 этаж, студия 809",
            city="Калининград",
        )
        is None
    )

    payload = {
        "location_name": "студия 809",
        "location_address": "Советский проспект, 12",
        "city": "Калининград",
    }
    normalise_event_location_from_reference(payload)
    assert payload == {
        "location_name": "студия 809",
        "location_address": "Советский проспект, 12",
        "city": "Калининград",
    }
    assert su._canonicalize_location_fields(**payload) == (
        "студия 809",
        "Советский проспект, 12",
        "Калининград",
    )


def test_inc_2026_07_27_address_suffix_tolerance_keeps_valid_icae_control() -> None:
    """The safety fix must retain a real known-address match with room detail."""

    venue = match_known_venue_by_address(
        "Советский 1, 2 этаж",
        city="Калининград",
    )
    assert venue is not None
    assert venue.name == "ИЦАЭ (в КГТУ)"
    assert venue.address == "Советский 1"


def test_inc_2026_07_27_smart_address_match_requires_full_house_number() -> None:
    assert su._address_matches("Советский 1", "Советский 12") is False
    assert su._address_matches("Советский 1", "Советский 1, 2 этаж") is True


def test_russian_art_center_reference_resolves_oktyabrskaya_10() -> None:
    venue = match_known_venue("Русский центр искусств", city="Калининград")
    assert venue is not None
    assert venue.name == "Русский центр искусства"
    assert venue.address == "Октябрьская 10"

    payload = {
        "location_name": "РЦИ",
        "location_address": "",
        "city": "Калининград",
    }
    normalise_event_location_from_reference(payload)
    assert payload == {
        "location_name": "Русский центр искусства",
        "location_address": "Октябрьская 10",
        "city": "Калининград",
    }


def test_kldscope_reference_normalizes_official_channel_wording() -> None:
    payload = {
        "location_name": "Уютное пространство «КЛДскоп»",
        "location_address": "ул. Земельная, д. 12",
        "city": "Калининград",
    }

    normalise_event_location_from_reference(payload)

    assert payload == {
        "location_name": "КЛДскоп",
        "location_address": "Земельная 12, 1 этаж, кабинет 3",
        "city": "Калининград",
    }

    from_text = find_known_venue_in_text(
        "19 июля, ул. Земельная, 12, каб. 3, пространство КЛДскоп",
        city="Калининград",
    )
    assert from_text is not None
    assert from_text.name == "КЛДскоп"
    assert from_text.address == "Земельная 12, 1 этаж, кабинет 3"

    declined = match_known_venue("КЛДскопе", city="Калининград")
    assert declined is not None
    assert declined.name == "КЛДскоп"
