from __future__ import annotations

import smart_event_update as su


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
