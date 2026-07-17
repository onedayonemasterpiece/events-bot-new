from __future__ import annotations

import copy
import json
import sys
import types
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
    stale_document = manifest("bus")
    stale_document["source"]["fetched_at"] = (NOW - timedelta(days=4)).isoformat()
    with pytest.raises(ManifestValidationError, match="source_document_stale"):
        validate_provider_manifest(stale_document, now=NOW)


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


def test_kaggle_runtime_package_manifest_is_allowlisted_and_hash_complete(tmp_path: Path) -> None:
    from scripts.run_transport_schedule_kaggle import TRANSPORT_PACKAGE_FILES, _copy_transport_package

    names = _copy_transport_package(tmp_path)
    manifest_value = json.loads((tmp_path / names[0]).read_text(encoding="utf-8"))
    assert manifest_value["schema_version"] == "kenigevents.transport_runtime_package.v1"
    assert set(manifest_value["files"]) == {f"transport_refresh/{name}" for name in TRANSPORT_PACKAGE_FILES}
    for relative, expected_hash in manifest_value["files"].items():
        import hashlib
        assert hashlib.sha256((tmp_path / relative).read_bytes()).hexdigest() == expected_hash


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
    assert store.combined_manifest()["content_hash"] == changed["combined_hash"]


def test_enqueue_failure_keeps_durable_pending_intent_and_unchanged_retry_enqueues(tmp_path: Path) -> None:
    store = TransportManifestStore(tmp_path)
    store.publish("kppk", manifest("kppk"), now=NOW)

    def fail_enqueue(_key: str, _payload: dict) -> None:
        raise OSError("sqlite unavailable")

    failed = store.publish("bus", manifest("bus"), now=NOW, enqueue=fail_enqueue)
    assert failed["status"] == "published_changed_rebuild_pending"
    assert failed["rebuild_pending"] is True
    assert store.combined_pointer()["content_hash"] == failed["combined_hash"]
    assert store.rebuild_state()["status"] == "pending"

    queued: list[tuple[str, dict]] = []
    same = manifest("bus", fetched_at=NOW + timedelta(hours=1))
    same["snapshot_id"] = "bus-retry-same-services"
    retried = store.publish(
        "bus",
        same,
        now=NOW + timedelta(hours=1),
        enqueue=lambda key, payload: queued.append((key, payload)),
    )
    assert retried["status"] == "published_unchanged"
    assert retried["rebuild_enqueued"] is True
    assert retried["rebuild_pending"] is False
    assert [key for key, _payload in queued] == ["static_site_build:prod"]
    assert store.rebuild_state()["acknowledged_hash"] == failed["combined_hash"]


def test_current_pointer_fails_closed_for_tampered_or_escaping_manifest(tmp_path: Path) -> None:
    store = TransportManifestStore(tmp_path)
    store.publish("kppk", manifest("kppk"), now=NOW)
    store.publish("bus", manifest("bus"), now=NOW)
    combined_path = tmp_path / store.combined_pointer()["manifest_path"]
    original = combined_path.read_text(encoding="utf-8")
    combined_path.write_text(original.replace("2026-07-18", "2026-07-19", 1), encoding="utf-8")
    assert store.combined_manifest() is None

    pointer_path = tmp_path / "combined" / "current.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["manifest_path"] = "../outside.json"
    pointer_path.write_text(json.dumps(pointer), encoding="utf-8")
    assert store.combined_manifest() is None


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
    assert store.provider_status("bus")["attempt_status"] == "failed"
    assert store.provider_status("bus")["serving_status"] == "last_good"
    assert store.provider_status("bus")["reasons"] == ["provider:timeout"]
    assert store.fan_in_status()["status"] == "complete"

    invalid = manifest("kppk", fetched_at=NOW + timedelta(hours=1))
    invalid["services"] = []
    rejected = store.publish("kppk", invalid, now=NOW + timedelta(hours=1))
    assert rejected["provider_accepted"] is False
    assert store.provider_pointer("kppk")["content_hash"]
    assert store.provider_status("kppk")["attempt_status"] == "partial"
    assert len(list((tmp_path / "providers" / "kppk" / "attempts").glob("*.json"))) == 2


def test_initial_invalid_provider_records_partial_health_without_pointer(tmp_path: Path) -> None:
    store = TransportManifestStore(tmp_path)
    invalid = manifest("bus")
    invalid["services"][0]["mode"] = "rail"
    report = store.publish("bus", invalid, now=NOW)
    assert report["published"] is False
    assert report["attempt_status"] == "invalid"
    assert store.provider_pointer("bus") is None
    assert store.provider_status("bus")["serving_status"] == "missing"
    assert store.provider_status("bus")["fan_in_status"] == "partial"
    assert store.fan_in_status()["provider_freshness"]["kppk"]["status"] == "missing"


def test_stale_last_good_blocks_publication_until_both_providers_recover(tmp_path: Path) -> None:
    store = TransportManifestStore(tmp_path, max_age_hours=72)
    store.publish("kppk", manifest("kppk"), now=NOW)
    store.publish("bus", manifest("bus"), now=NOW)
    stale_time = NOW + timedelta(days=4)
    blocked = store.publish("kppk", manifest("kppk", fetched_at=stale_time, suffix="changed"), now=stale_time)
    assert blocked["published"] is False
    assert blocked["freshness"]["bus"]["status"] == "stale"
    assert store.fan_in_status()["status"] == "partial"
    assert store.fan_in_status()["provider_freshness"]["bus"]["status"] == "stale"
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
    assert "delete_dataset" in runner and "keep_input_datasets" in runner
    assert "transport_refresh_package_manifest.json" in runner
    assert 'metadata["id"] = kernel_ref' in runner
    for provider in ("TransportKppkRefresh", "TransportBusRefresh"):
        metadata = json.loads(Path("kaggle", provider, "kernel-metadata.json").read_text(encoding="utf-8"))
        assert metadata["events_bot_disable_status_instrumentation"] is True
        wrapper = next(Path("kaggle", provider).glob("transport_*_refresh.py")).read_text(encoding="utf-8")
        assert "EXPECTED_FILES" in wrapper
        assert "hashlib.sha256(path.read_bytes()).hexdigest()" in wrapper
        assert "path.relative_to(root)" in wrapper
    activation = Path("scheduling.py").read_text(encoding="utf-8") + Path("fly.toml").read_text(encoding="utf-8")
    assert "TransportKppkRefresh" not in activation
    assert "TransportBusRefresh" not in activation
    assert "ENABLE_TRANSPORT_REFRESH" not in activation


def test_kernel_emits_status_heartbeat_report_and_releases_lease(tmp_path: Path, monkeypatch) -> None:
    import transport_refresh.kernel as kernel

    source = tmp_path / "source.json"
    candidate = manifest("kppk")
    source.write_text(json.dumps({
        "validity": candidate["validity"],
        "timezone": candidate["timezone"],
        "services": candidate["services"],
    }), encoding="utf-8")
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    (input_dir / "transport_refresh_config.json").write_text(json.dumps({
        "provider": "kppk",
        "source_url": "https://www.kppk39.ru/raspisanie/",
        "source_payload_filename": "source.json",
    }), encoding="utf-8")
    (input_dir / "source.json").write_bytes(source.read_bytes())

    class FakeStatus:
        enabled = True
        config = {"resource_leases": ["transport_schedule:kppk:refresh"]}

        def __init__(self):
            self.events = []
            self.acquired = []
            self.released = []
            self.alive = False

        def event(self, event, **kwargs):
            self.events.append((event, kwargs))

        def acquire_resource(self, key, **_kwargs):
            self.acquired.append(key)
            return True

        def release_resource(self, key):
            self.released.append(key)

        def start_alive(self, **_kwargs):
            self.alive = True

        def stop_alive(self):
            self.alive = False

    status = FakeStatus()
    monkeypatch.setitem(sys.modules, "kaggle_status_client", types.SimpleNamespace(load_status_client=lambda **_kwargs: status))
    monkeypatch.setattr(kernel, "INPUT", input_dir)
    monkeypatch.setattr(kernel, "WORKING", output_dir)
    monkeypatch.setattr(kernel, "STATUS", None)
    monkeypatch.setattr(kernel, "RESOURCES", [])
    assert kernel.kernel_main("kppk") == 0
    assert [name for name, _kwargs in status.events] == [
        "kernel_started", "preflight_ok", "alive", "alive", "report_written",
    ]
    assert status.acquired == status.released == ["transport_schedule:kppk:refresh"]
    assert status.alive is False


@pytest.mark.asyncio
async def test_multiple_transport_hash_updates_merge_into_one_pending_static_build(tmp_path: Path) -> None:
    import main
    from models import JobOutbox, JobStatus, JobTask

    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    results = []
    for content_hash in ("hash-1", "hash-2", "hash-2"):
        results.append(await main.enqueue_job(
            db, 0, JobTask.static_site_build,
            payload={"transport_content_hash": content_hash},
            coalesce_key="static_site_build:prod", requeue_done=True,
        ))
    assert results == ["new", "merged-rearmed", "merged-rearmed"]
    async with db.get_session() as session:
        rows = (await session.execute(
            main.select(JobOutbox).where(JobOutbox.coalesce_key == "static_site_build:prod")
        )).scalars().all()
    assert len(rows) == 1
    assert rows[0].status == JobStatus.pending
    assert rows[0].payload == {"transport_content_hash": "hash-2"}


@pytest.mark.asyncio
async def test_running_static_build_gets_exactly_one_coalesced_followup(tmp_path: Path) -> None:
    import main
    from models import JobOutbox, JobStatus, JobTask

    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        session.add(JobOutbox(
            event_id=0,
            task=JobTask.static_site_build,
            status=JobStatus.running,
            coalesce_key="static_site_build:prod",
            payload={"transport_content_hash": "old"},
            updated_at=main.datetime.now(main.timezone.utc),
        ))
        await session.commit()

    first = await main.enqueue_job(
        db, 0, JobTask.static_site_build,
        payload={"transport_content_hash": "new-1"},
        coalesce_key="static_site_build:prod", requeue_done=True,
    )
    second = await main.enqueue_job(
        db, 0, JobTask.static_site_build,
        payload={"transport_content_hash": "new-2"},
        coalesce_key="static_site_build:prod", requeue_done=True,
    )
    assert first == "merged"
    assert second == "merged-rearmed"
    async with db.get_session() as session:
        rows = (await session.execute(
            main.select(JobOutbox)
            .where(JobOutbox.coalesce_key == "static_site_build:prod")
            .order_by(JobOutbox.id)
        )).scalars().all()
    assert len(rows) == 2
    assert [row.status for row in rows] == [JobStatus.running, JobStatus.pending]
    assert rows[1].payload == {"transport_content_hash": "new-2"}
