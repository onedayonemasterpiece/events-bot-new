from __future__ import annotations

import smart_event_update as su
from location_reference import match_known_venue


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
