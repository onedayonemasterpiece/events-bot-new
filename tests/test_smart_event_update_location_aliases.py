from __future__ import annotations

import smart_event_update as su
from location_reference import (
    canonicalize_known_place_name,
    find_known_venue_in_text,
    match_known_venue,
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


def test_generic_vorota_is_not_forced_into_zakheim_bucket() -> None:
    assert su._normalize_location("Ворота") == "ворота"


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
