from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from transport_refresh.ics import build_event_ics, build_transport_ics
from transport_refresh.provider_job import run_provider_job
from transport_refresh.schema import ManifestValidationError, SCHEMA_VERSION, validate_provider_manifest
from transport_refresh.selection import is_transport_eligible_event, select_event_transport
from transport_refresh.store import TransportManifestStore

NOW = datetime(2026, 7, 17, 8, tzinfo=timezone.utc)


def manifest(provider: str, *, fetched_at: datetime = NOW, suffix: str = "a") -> dict:
    mode = "rail" if provider == "kppk" else "bus"
    trip = "6717" if provider == "kppk" else "118"
    dep = "2026-07-18T12:00:00+02:00"
    arr = "2026-07-18T13:00:00+02:00"
    if suffix == "changed":
        arr = "2026-07-18T13:05:00+02:00"
    return {
        "schema_version": SCHEMA_VERSION,
        "provider": provider,
        "snapshot_id": f"{provider}-{suffix}-{fetched_at.isoformat()}",
        "fetched_at": fetched_at.isoformat(),
        "timezone": "Europe/Kaliningrad",
        "source": {
            "url": "https://www.kppk39.ru/raspisanie/" if provider == "kppk" else "https://avl39.ru/routes/reg/kaliningrad/",
            "fetched_at": fetched_at.isoformat(),
            "document_sha256": "a" * 64,
        },
        "validity": {"status": "valid", "valid_from": "2026-07-17", "valid_until": "2026-07-31"},
        "services": [{
            "service_id": f"{provider}-{trip}-20260718-{suffix}",
            "mode": mode,
            "trip_id": trip,
            "route": {"id": trip, "name": f"Калининград — Светлогорск {trip}"},
            "stops": [
                {"id": "kaliningrad", "name": "Калининград-Северный", "lat": 54.7209, "lon": 20.5002},
                {"id": "svetlogorsk", "name": "Светлогорск-2", "lat": 54.9434, "lon": 20.1513},
            ],
            "service_date": "2026-07-18",
            "departure": {"stop_id": "kaliningrad", "time": dep},
            "arrival": {"stop_id": "svetlogorsk", "time": arr},
            "direction": "outbound",
            "binding": {
                "event_ids": [6510], "city": "Светлогорск",
                "venue": {"name": "Янтарь-холл", "lat": 54.9410, "lon": 20.1530},
                "venue_aliases": ["Янтарь холл"],
            },
            "source": {"url": "https://www.kppk39.ru/raspisanie/" if provider == "kppk" else "https://avl39.ru/routes/reg/kaliningrad/"},
        }],
    }


def return_service(provider_manifest: dict) -> dict:
    service = copy.deepcopy(provider_manifest["services"][0])
    service["service_id"] += "-return"
    service["direction"] = "return"
    service["departure"] = {"stop_id": "svetlogorsk", "time": "2026-07-18T18:30:00+02:00"}
    service["arrival"] = {"stop_id": "kaliningrad", "time": "2026-07-18T19:30:00+02:00"}
    return service


def event(**overrides) -> dict:
    value = {
        "id": 6510, "title": "Концерт", "city": "Светлогорск", "venue_name": "Янтарь холл",
        "venue_lat": 54.941, "venue_lon": 20.153,
        "start_datetime": "2026-07-18T15:00:00+02:00", "end_datetime": "2026-07-18T18:00:00+02:00",
    }
    value.update(overrides)
    return value


def test_common_schema_requires_exact_date_timezone_named_stops_and_venue() -> None:
    valid = validate_provider_manifest(manifest("kppk"), expected_provider="kppk", now=NOW)
    assert valid["content_hash"]
    invalid = manifest("kppk")
    invalid["services"][0]["service_date"] = "2026-07-19"
    invalid["services"][0]["binding"]["venue"]["name"] = ""
    with pytest.raises(ManifestValidationError) as caught:
        validate_provider_manifest(invalid, expected_provider="kppk", now=NOW)
    assert "services[0].departure:wrong_service_date" in caught.value.reasons
    assert "services[0].binding.venue.name:required" in caught.value.reasons


def test_provider_specific_mode_empty_and_stale_are_rejected() -> None:
    invalid = manifest("bus")
    invalid["services"][0]["mode"] = "rail"
    with pytest.raises(ManifestValidationError, match="provider_mismatch"):
        validate_provider_manifest(invalid, now=NOW)
    empty = manifest("bus")
    empty["services"] = []
    with pytest.raises(ManifestValidationError, match="services:empty"):
        validate_provider_manifest(empty, now=NOW)
    with pytest.raises(ManifestValidationError, match="source_stale"):
        validate_provider_manifest(manifest("bus", fetched_at=NOW - timedelta(days=4)), now=NOW)


def test_provider_job_accepts_controlled_adapter_payload_and_emits_bounded_outputs(tmp_path: Path) -> None:
    source = tmp_path / "source.json"
    candidate = manifest("kppk")
    source.write_text(__import__("json").dumps({
        "validity": candidate["validity"], "timezone": candidate["timezone"], "services": candidate["services"],
    }, ensure_ascii=False), encoding="utf-8")
    output = tmp_path / "output"
    result = run_provider_job({
        "provider": "kppk", "source_payload_path": str(source),
        "source_url": "https://www.kppk39.ru/raspisanie/",
    }, output_dir=output, now=NOW)
    assert result["service_count"] == 1
    assert sorted(item.name for item in output.iterdir()) == [
        "transport-kppk-manifest.json", "transport_provider_result.json",
    ]


def test_fan_in_is_deterministic_changed_once_unchanged_zero(tmp_path: Path) -> None:
    queued: list[tuple[str, dict]] = []
    store = TransportManifestStore(tmp_path)
    first = store.publish("kppk", manifest("kppk"), now=NOW, enqueue=lambda key, payload: queued.append((key, payload)))
    assert first["status"] == "provider_accepted_waiting_for_fan_in"
    assert queued == []
    second = store.publish("bus", manifest("bus"), now=NOW, enqueue=lambda key, payload: queued.append((key, payload)))
    assert second["status"] == "published_changed"
    assert [item[0] for item in queued] == ["static_site_build:prod"]
    combined_hash = second["combined_hash"]

    refreshed = manifest("bus", fetched_at=NOW + timedelta(hours=1))
    refreshed["snapshot_id"] = "bus-refreshed-same-services"
    third = store.publish("bus", refreshed, now=NOW + timedelta(hours=1), enqueue=lambda key, payload: queued.append((key, payload)))
    assert third["status"] == "published_unchanged"
    assert third["combined_hash"] == combined_hash
    assert len(queued) == 1

    changed = store.publish("bus", manifest("bus", fetched_at=NOW + timedelta(hours=2), suffix="changed"), now=NOW + timedelta(hours=2), enqueue=lambda key, payload: queued.append((key, payload)))
    assert changed["status"] == "published_changed"
    assert changed["combined_hash"] != combined_hash
    assert len(queued) == 2
    assert len(list((tmp_path / "combined" / "manifests").glob("*.json"))) == 3


def test_timeout_and_invalid_partial_failure_preserve_last_good(tmp_path: Path) -> None:
    store = TransportManifestStore(tmp_path)
    store.publish("kppk", manifest("kppk"), now=NOW)
    complete = store.publish("bus", manifest("bus"), now=NOW)
    current_before = store.combined_pointer()
    provider_before = store.provider_pointer("bus")
    timed_out = store.publish("bus", None, now=NOW + timedelta(hours=1), failure_reason="timeout")
    assert timed_out["status"] == "published_unchanged"
    assert timed_out["reasons"] == ["provider:timeout"]
    assert timed_out["freshness"]["bus"] == {"status": "last_good", "reasons": ["provider:timeout"]}
    assert store.provider_pointer("bus") == provider_before
    assert store.combined_pointer()["content_hash"] == current_before["content_hash"] == complete["combined_hash"]

    invalid = manifest("kppk", fetched_at=NOW + timedelta(hours=1))
    invalid["services"] = []
    rejected = store.publish("kppk", invalid, now=NOW + timedelta(hours=1))
    assert rejected["provider_accepted"] is False
    assert store.provider_pointer("kppk")["content_hash"]


def test_stale_last_good_blocks_publication_until_both_providers_recover(tmp_path: Path) -> None:
    store = TransportManifestStore(tmp_path, max_age_hours=72)
    store.publish("kppk", manifest("kppk"), now=NOW)
    store.publish("bus", manifest("bus"), now=NOW)
    stale_time = NOW + timedelta(days=4)
    blocked = store.publish("kppk", manifest("kppk", fetched_at=stale_time, suffix="changed"), now=stale_time)
    assert blocked["published"] is False
    assert blocked["freshness"]["bus"]["status"] == "stale"
    recovered = store.publish("bus", manifest("bus", fetched_at=stale_time), now=stale_time)
    assert recovered["published"] is True
    assert recovered["freshness"]["kppk"]["status"] == "fresh"


def test_selection_is_exact_date_before_start_return_after_end_and_no_kaliningrad() -> None:
    kppk = manifest("kppk")
    kppk["services"].append(return_service(kppk))
    normalized = validate_provider_manifest(kppk, now=NOW)
    combined = {"services": [dict(item, provider="kppk") for item in normalized["services"]]}
    selected = select_event_transport(event(), combined)
    assert [item["direction"] for item in selected["outbound"]] == ["outbound"]
    assert [item["direction"] for item in selected["return"]] == ["return"]
    assert select_event_transport(event(city="Калининград"), combined) == {"outbound": [], "return": []}
    assert not is_transport_eligible_event(event(venue_lat=None))
    assert select_event_transport(event(start_datetime="2026-07-19T15:00:00+02:00"), combined) == {"outbound": [], "return": []}
    assert select_event_transport(event(end_datetime=None), combined)["return"] == []


def test_event_and_transport_ics_are_distinct_and_transport_has_alarm() -> None:
    candidate = manifest("kppk")
    service = dict(candidate["services"][0], provider="kppk")
    event_ics = build_event_ics(event(), public_url="https://kenigevents.ru/sobytiya/concert")
    trip_ics = build_transport_ics(event(), service, public_url="https://kenigevents.ru/sobytiya/concert")
    assert "UID:event-6510-2026-07-18@kenigevents.ru" in event_ics
    assert "BEGIN:VALARM" not in event_ics
    assert "UID:transport-" in trip_ics
    assert "TRIGGER:-PT30M" in trip_ics
    assert "DTSTART:20260718T100000Z" in trip_ics
    assert "DTEND:20260718T110000Z" in trip_ics


def test_kaggle_provider_kernels_use_existing_status_heartbeat_and_lease_contract() -> None:
    kernel = Path("transport_refresh/kernel.py").read_text(encoding="utf-8")
    assert "kernel_started" in kernel
    assert "start_alive" in kernel
    assert "acquire_resource" in kernel
    assert "report_written" in kernel
    runner = Path("scripts/run_transport_schedule_kaggle.py").read_text(encoding="utf-8")
    assert "transport_schedule:{args.provider}:refresh" in runner
    assert "TransportKppkRefresh" in runner and "TransportBusRefresh" in runner
