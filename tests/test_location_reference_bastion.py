"""Regression test for INC-2026-04-29-bar-bastion-city-jazz-location.

Bar Bastion lives at Судостроительная 6/1 (the Ponart cluster), not at
City Jazz on Mira. An addressless «в Бастионе» mention from VK group
``bar_bastion`` (149955604) must normalise to Bar Bastion via the
canonical reference layer.
"""

from __future__ import annotations

import main
from location_reference import (
    match_known_venue,
    normalise_event_location_from_reference,
)


def test_bastion_reference_maps_to_current_ponart_address() -> None:
    venue = match_known_venue("Бастион", city="Калининград")
    assert venue is not None
    assert venue.name == "Бар Бастион"
    assert venue.address == "Судостроительная 6/1"

    payload = {
        "location_name": "Бастион",
        "location_address": None,
        "city": "Калининград",
    }
    normalise_event_location_from_reference(payload)
    assert payload == {
        "location_name": "Бар Бастион",
        "location_address": "Судостроительная 6/1",
        "city": "Калининград",
    }


def test_event_parse_reference_normalizes_bastion_without_alias_file() -> None:
    payload = {
        "location_name": "Бастион",
        "location_address": None,
        "city": "Калининград",
    }

    main._normalise_event_location_from_reference(payload)

    assert payload["location_name"] == "Бар Бастион"
    assert payload["location_address"] == "Судостроительная 6/1"
