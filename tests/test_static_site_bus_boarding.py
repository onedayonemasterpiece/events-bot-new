import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BUS_DATA_PATH = ROOT / "site" / "src" / "data" / "busTransportSchedules.json"
BUS_HELPER_PATH = ROOT / "site" / "src" / "lib" / "busBoarding.ts"


def _resolve_boarding(terminal_departure: str, terminal_ride_minutes: int, preferred: dict) -> dict:
    script = f"""
import {{ resolveBusBoarding }} from {json.dumps(BUS_HELPER_PATH.as_uri())};
const result = resolveBusBoarding({{
  terminalDeparture: {json.dumps(terminal_departure)},
  terminalStop: 'Автовокзал Калининград',
  terminalRideEstimateMinutes: {terminal_ride_minutes},
  preferredBoarding: {json.dumps(preferred, ensure_ascii=False)},
}});
process.stdout.write(JSON.stringify(result));
"""
    completed = subprocess.run(
        ["node", "--experimental-strip-types", "--input-type=module", "-e", script],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def _clock(minutes: int) -> str:
    return f"{minutes // 60 % 24:02d}:{minutes % 60:02d}"


def test_romanovo_preferred_north_boarding_preserves_raw_and_arrival_times() -> None:
    data = json.loads(BUS_DATA_PATH.read_text(encoding="utf-8"))
    route = next(item for item in data["routes"] if item["id"] == "romanovo-holmogorye")
    route_119 = next(item for item in route["outbound_groups"] if item["routes"] == "119")
    preferred = route["preferred_boarding"]

    assert route["origin_stop"] == "Автовокзал Калининград"
    assert route_119["departure_stop"] == "Автовокзал Калининград"
    assert preferred["stop_name"] == "Северный вокзал"
    assert preferred["offset_from_terminal_minutes"] == 15
    assert preferred["time_is_estimated"] is True
    assert "16:30" in route_119["departures"]
    assert "17:55" in route_119["departures"]

    expected = [
        ("16:30", "16:45", "17:35", "18:28"),
        ("17:55", "18:10", "19:00", "19:53"),
    ]
    for terminal, north, romanovo, venue in expected:
        result = _resolve_boarding(terminal, route_119["ride_estimate_minutes"], preferred)
        assert result["terminalDeparture"] == terminal
        assert result["boardingDeparture"] == north
        assert result["boardingStop"] == "Северный вокзал"
        assert result["boardingTimeEstimated"] is True
        assert result["remainingRideMinutes"] == route_119["ride_estimate_minutes"] - 15
        assert _clock(result["destinationArrivalMinutes"]) == romanovo
        assert _clock(result["destinationArrivalMinutes"] + 53) == venue
        assert (
            result["terminalDepartureMinutes"] + route_119["ride_estimate_minutes"]
            == result["boardingDepartureMinutes"] + result["remainingRideMinutes"]
        )
