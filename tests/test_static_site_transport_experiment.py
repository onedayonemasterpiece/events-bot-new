import hashlib
import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE = ROOT / "site" / "src" / "lib" / "transportExperiment.ts"
COMPONENT = ROOT / "site" / "src" / "components" / "KaupTransportSchedule.astro"
HOST = ROOT / "site" / "src" / "components" / "transport" / "TransportTimetableExperiment.astro"
CLIENT = ROOT / "site" / "src" / "lib" / "transportExperimentClient.ts"


def _node(script: str) -> dict:
    completed = subprocess.run(
        ["node", "--experimental-strip-types", "--input-type=module", "-e", script],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def test_tr_exp_01_sha256_assignment_is_stable_and_matches_reference() -> None:
    subject = "11111111-1111-4111-8111-111111111111"
    expected_u32 = int.from_bytes(
        hashlib.sha256(f"transport_timetable_layout|1|{subject}".encode()).digest()[:4], "big"
    )
    expected_bucket = expected_u32 * 10_000 // 2**32
    script = f"""
import {{ assignTransportExperimentVariant }} from {json.dumps(MODULE.as_uri())};
const one = await assignTransportExperimentVariant({json.dumps(subject)});
const two = await assignTransportExperimentVariant({json.dumps(subject)});
process.stdout.write(JSON.stringify({{one, two}}));
"""
    result = _node(script)
    assert expected_bucket == 2892
    assert result["one"] == result["two"] == {
        "bucket": expected_bucket,
        "variant": "departure_board_v1",
    }


def test_tr_exp_02_bucket_contract_has_no_gaps_or_overlap() -> None:
    script = f"""
import {{ transportVariantForBucket }} from {json.dumps(MODULE.as_uri())};
const counts = {{}};
for (let bucket = 0; bucket < 10000; bucket += 1) {{
  const variant = transportVariantForBucket(bucket);
  if (!variant) throw new Error(`missing bucket ${{bucket}}`);
  counts[variant] = (counts[variant] || 0) + 1;
}}
process.stdout.write(JSON.stringify({{counts, below: transportVariantForBucket(-1), above: transportVariantForBucket(10000)}}));
"""
    result = _node(script)
    assert result == {
        "counts": {
            "departure_board_v1": 3333,
            "route_strips_v1": 3333,
            "next_departure_queue_v1": 3334,
        },
        "below": None,
        "above": None,
    }


def test_tr_exp_03_and_07_mode_and_shared_time_eligibility_fail_closed() -> None:
    script = f"""
import {{ normalizeTransportExperimentMode, transportExperimentEligible }} from {json.dumps(MODULE.as_uri())};
const now = Date.parse('2026-07-25T14:00:00+02:00');
process.stdout.write(JSON.stringify({{
  modes: ['off','qa','focus_group','live','unexpected'].map(normalizeTransportExperimentMode),
  eligible: transportExperimentEligible(['2026-07-25T16:45:00+02:00','2026-07-25T18:10:00+02:00'], now),
  oneTrip: transportExperimentEligible(['2026-07-25T16:45:00+02:00'], now),
  allPast: transportExperimentEligible(['2026-07-25T12:00:00+02:00','2026-07-25T13:00:00+02:00'], now),
  malformed: transportExperimentEligible(['bad','2026-07-25T18:10:00+02:00'], now),
}}));
"""
    result = _node(script)
    assert result["modes"] == ["off", "qa", "focus_group", "live", "off"]
    assert result["eligible"] is True
    assert result["oneTrip"] is False
    assert result["allPast"] is False
    assert result["malformed"] is False


def test_tr_exp_04_05_08_09_static_contract_preserves_actions_and_qa_trust_boundary() -> None:
    component = COMPONENT.read_text(encoding="utf-8")
    host = HOST.read_text(encoding="utf-8")
    client = CLIENT.read_text(encoding="utf-8")
    for action in (
        "official_transfer_booking_click",
        "bus_origin_map_click",
        "walk_route_click",
        "car_route_click",
    ):
        assert f'data-transport-action="{action}"' in component
    for variant in (
        "DepartureBoardTimetable",
        "RouteStripsTimetable",
        "NextDepartureQueueTimetable",
    ):
        assert variant in host
    assert "mode === 'off'" in host
    assert "entry?.intersectionRatio >= 0.5" in client
    assert "}, 1000);" in client
    assert "state.qaOverride" in client
    assert "mode !== 'focus_group' && mode !== 'live'" in client
    assert "keepalive: true" in client
    assert "event.preventDefault" not in client


def test_tr_exp_06_treatments_have_many_trip_disclosure_and_same_trip_markers() -> None:
    treatment_dir = ROOT / "site" / "src" / "components" / "transport"
    for name in (
        "DepartureBoardTimetable.astro",
        "RouteStripsTimetable.astro",
        "NextDepartureQueueTimetable.astro",
    ):
        source = (treatment_dir / name).read_text(encoding="utf-8")
        assert "data-transport-trip-id" in source
        assert "data-departure-at" in source
        assert "schedule_expand" in source
    assert "slice(0, 5)" in (treatment_dir / "DepartureBoardTimetable.astro").read_text(encoding="utf-8")
    assert "slice(0, 5)" in (treatment_dir / "RouteStripsTimetable.astro").read_text(encoding="utf-8")


def test_tr_exp_11_balanced_sample_passes_and_biased_sample_blocks() -> None:
    script = f"""
import {{ evaluateTransportSampleRatio }} from {json.dumps(MODULE.as_uri())};
process.stdout.write(JSON.stringify({{
  low: evaluateTransportSampleRatio([10, 10, 10]),
  balanced: evaluateTransportSampleRatio([3333, 3333, 3334]),
  biased: evaluateTransportSampleRatio([7000, 1500, 1500]),
}}));
"""
    result = _node(script)
    assert result["low"]["diagnosticOnly"] is True
    assert result["low"]["blocker"] is False
    assert result["balanced"]["blocker"] is False
    assert result["balanced"]["pValue"] > 0.99
    assert result["biased"]["blocker"] is True
    assert result["biased"]["pValue"] < 0.001


def test_tr_exp_12_manifest_record_defaults_root_off_and_hashes_definition() -> None:
    canonical = (
        "transport_timetable_layout|1|sha256-u32be-bucket-10000-v1|"
        "departure_board_v1:0-3332|route_strips_v1:3333-6665|"
        "next_departure_queue_v1:6666-9999"
    )
    expected_hash = "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()
    script = f"""
import {{ transportExperimentManifestRecord }} from {json.dumps(MODULE.as_uri())};
process.stdout.write(JSON.stringify({{
  root: transportExperimentManifestRecord(undefined),
  secret: transportExperimentManifestRecord('focus_group'),
}}));
"""
    result = _node(script)
    assert result["root"]["mode"] == "off"
    assert result["secret"]["mode"] == "focus_group"
    assert result["root"]["config_hash"] == result["secret"]["config_hash"] == expected_hash
    assert result["root"]["assignment_unit"] == "browser_subject"
