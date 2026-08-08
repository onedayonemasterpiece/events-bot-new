import json
from pathlib import Path
import subprocess

import pytest


REPO = Path(__file__).resolve().parents[1]
MODULE = REPO / "scripts/current_ui_resource_graph/v1/transport.mjs"
FIXTURE = REPO / "tests/fixtures/current-ui-decoder-v1/transport/expected-summary.json"


def _node(source: str):
    completed = subprocess.run(
        ["node", "--input-type=module", "-e", source],
        cwd=REPO,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def _module_import(names: str) -> str:
    return f"import {{ {names} }} from {json.dumps(MODULE.as_uri())};\n"


def test_transport_lane_has_bounded_reviewed_counts_and_exact_pinned_facts():
    expected = json.loads(FIXTURE.read_text(encoding="utf-8"))
    observed = _node(
        _module_import("buildTransportDecoderLane")
        + """
const lane = buildTransportDecoderLane();
const count = (rows, field) => Object.fromEntries([...new Set(rows.map((row) => row[field]))].sort().map((value) => [value, rows.filter((row) => row[field] === value).length]));
console.log(JSON.stringify({
  schema_version: lane.schema_version,
  state_record_count: lane.state_records.length,
  specimen_plan_count: lane.specimen_plan.length,
  state_counts: count(lane.state_records, 'family'),
  specimen_counts: count(lane.specimen_plan, 'specimen_family'),
  qa_specimen_count: lane.specimen_plan.filter((row) => row.state_axes.experiment_mode === 'qa').length,
  source_only_experiment_count: lane.state_records.filter((row) => row.reachability === 'source-only').length,
  real_route_ids: lane.real_route_representatives.map((row) => row.route_id).filter(Number.isInteger),
  pinned: {
    corpus_event_count: lane.pinned_observations.corpus_event_count,
    rail_suggestions: lane.pinned_observations.rail.suggestions,
    rail_explicit: lane.pinned_observations.rail.end_basis.explicit,
    rail_forecast: lane.pinned_observations.rail.end_basis.forecast,
    rail_schedule_cutoff: lane.pinned_observations.rail.end_basis.schedule_cutoff,
    bus_suggestions: lane.pinned_observations.bus.suggestions,
    kaup_suggestions: lane.pinned_observations.kaup.suggestions,
  },
}));
"""
    )
    assert observed == expected


def test_rail_closed_axes_accept_representatives_and_reject_impossible_combinations():
    result = _node(
        _module_import("validateRailState")
        + """
const base = { outbound_present:true, return_present:true, event_end_present:true, estimated_end:false, next_day_return:false, warning:false };
const cases = [
  ['explicit-valid', { ...base, event_end_basis:'explicit' }],
  ['forecast-valid', { ...base, event_end_basis:'forecast', estimated_end:true }],
  ['cutoff-valid', { ...base, event_end_basis:'schedule_cutoff', return_present:false, event_end_present:false }],
  ['cutoff-return-invalid', { ...base, event_end_basis:'schedule_cutoff' }],
  ['forecast-next-day-invalid', { ...base, event_end_basis:'forecast', estimated_end:true, next_day_return:true }],
  ['explicit-estimated-invalid', { ...base, event_end_basis:'explicit', estimated_end:true }],
  ['warning-with-return-invalid', { ...base, event_end_basis:'explicit', warning:true }],
];
console.log(JSON.stringify(Object.fromEntries(cases.map(([name, axes]) => {
  try { validateRailState(axes); return [name, 'accepted']; } catch (error) { return [name, error.message]; }
}))));
"""
    )
    assert result["explicit-valid"] == "accepted"
    assert result["forecast-valid"] == "accepted"
    assert result["cutoff-valid"] == "accepted"
    for name in (
        "cutoff-return-invalid",
        "forecast-next-day-invalid",
        "explicit-estimated-invalid",
        "warning-with-return-invalid",
    ):
        assert result[name] != "accepted"


def test_closed_axis_and_invalid_combination_catalog_is_explicit_and_not_merged():
    result = _node(
        _module_import("TRANSPORT_AXIS_DEFINITIONS, TRANSPORT_INVALID_COMBINATIONS")
        + "console.log(JSON.stringify({axes:TRANSPORT_AXIS_DEFINITIONS, invalid:TRANSPORT_INVALID_COMBINATIONS}));"
    )
    assert result["axes"]["rail"]["event_end_basis"] == ["explicit", "forecast", "schedule_cutoff"]
    assert result["axes"]["kaup"]["experiment_mode"] == ["off", "qa", "focus_group", "live"]
    assert result["axes"]["kaup"]["treatment"] == [
        "departure_board_v1", "route_strips_v1", "next_departure_queue_v1"
    ]
    assert len(result["invalid"]) == 13
    assert {row["family"] for row in result["invalid"]} == {
        "transport.rail", "transport.bus", "transport.kaup"
    }
    assert all(row["decision"] == "NOT_MERGED" for row in result["invalid"])


def test_kaup_off_and_qa_treatments_are_separated_fail_closed():
    result = _node(
        _module_import("buildTransportStateRecords, validateKaupState")
        + """
const rows = buildTransportStateRecords();
const qa = rows.filter((row) => row.axes.experiment_mode === 'qa');
const off = rows.filter((row) => row.axes.experiment_mode === 'off');
let invalidOff = 'accepted';
try { validateKaupState({ compact:false, outbound_present:true, departure_estimated:false, tight:false, public_return_available:false, transfer_details_open:false, experiment_host_present:false, initial_hidden:false, experiment_mode:'off', treatment:'route_strips_v1' }); }
catch (error) { invalidOff = error.message; }
console.log(JSON.stringify({
  qa: qa.map((row) => ({ treatment:row.axes.treatment, reachability:row.reachability, proof_label:row.proof_label, implementation_reachability:row.implementation_reachability, initial_hidden:row.axes.initial_hidden })),
  offTreatments: [...new Set(off.map((row) => row.axes.treatment))],
  invalidOff,
}));
"""
    )
    assert result["offTreatments"] == ["departure_board_v1"]
    assert result["invalidOff"] != "accepted"
    assert {row["treatment"] for row in result["qa"]} == {
        "departure_board_v1",
        "route_strips_v1",
        "next_departure_queue_v1",
    }
    assert all(row["reachability"] == "controlled-specimen-only" for row in result["qa"])
    assert all(row["proof_label"] == "controlled-candidate-qa-never-production-observed" for row in result["qa"])
    assert all(row["implementation_reachability"] == "experiment-off" for row in result["qa"])
    assert {row["treatment"]: row["initial_hidden"] for row in result["qa"]} == {
        "departure_board_v1": False,
        "route_strips_v1": True,
        "next_departure_queue_v1": True,
    }


def test_source_only_experiment_modes_never_enter_specimen_plan():
    result = _node(
        _module_import("buildTransportStateRecords, buildTransportSpecimenPlan")
        + """
const states = buildTransportStateRecords();
const plan = buildTransportSpecimenPlan();
console.log(JSON.stringify({
  sourceModes:[...new Set(states.filter((row) => row.reachability === 'source-only').map((row) => row.axes.experiment_mode))].sort(),
  planModes:[...new Set(plan.map((row) => row.state_axes.experiment_mode).filter(Boolean))].sort(),
  productionObserved:states.filter((row) => row.reachability === 'production-observed').length,
}));
"""
    )
    assert result == {
        "sourceModes": ["focus_group", "live"],
        "planModes": ["off", "qa"],
        "productionObserved": 0,
    }


def test_kaup_specimens_link_shell_wrapper_and_exact_treatment_sources():
    result = _node(
        _module_import("buildTransportSpecimenPlan")
        + """
const rows=buildTransportSpecimenPlan().filter((row) => row.state_axes.experiment_mode === 'qa');
console.log(JSON.stringify(rows.map((row) => ({
  treatment:row.state_axes.treatment,
  logical_path:row.logical_path,
  implementation_logical_path:row.implementation_logical_path,
  consumer_source_paths:row.consumer_source_paths,
  same_component_id:row.component_id === row.implementation_component_id,
}))));
"""
    )
    assert all(row["logical_path"] == "src/components/KaupTransportSchedule.astro" for row in result)
    expected = {
        "departure_board_v1": "src/components/transport/DepartureBoardTimetable.astro",
        "route_strips_v1": "src/components/transport/RouteStripsTimetable.astro",
        "next_departure_queue_v1": "src/components/transport/NextDepartureQueueTimetable.astro",
    }
    assert {row["treatment"]: row["implementation_logical_path"] for row in result} == expected
    assert all("src/components/transport/TransportTimetableExperiment.astro" in row["consumer_source_paths"] for row in result)
    assert all(row["same_component_id"] is False for row in result)


def test_exact_marker_allowlist_and_sensitive_marker_rejection():
    result = _node(
        _module_import("assertTransportEvidenceRecord")
        + """
const base = { family:'transport.kaup', markers:['data-kaup-transport','data-transport-treatment'], proof_label:'controlled-candidate-qa-never-production-observed', reachability:'controlled-specimen-only', decision:'NOT_MERGED', normalization_allowed:false };
const cases = {
  safe: base,
  secretMarker: { ...base, markers:['data-supabase-key'] },
  unknownMarker: { ...base, markers:['data-made-up-marker'] },
  unsafeValue: { ...base, note:'https://private.example.invalid/review' },
  fullHtml: { ...base, html:'<aside data-kaup-transport>private corpus</aside>' },
  promotedQa: { ...base, reachability:'production-observed' },
  broadFamily: { ...base, family:'family.transport' },
};
console.log(JSON.stringify(Object.fromEntries(Object.entries(cases).map(([name, record]) => {
  try { assertTransportEvidenceRecord(record); return [name, 'accepted']; } catch (error) { return [name, error.message]; }
}))));
"""
    )
    assert result["safe"] == "accepted"
    for name in ("secretMarker", "unknownMarker", "unsafeValue", "fullHtml", "promotedQa", "broadFamily"):
        assert result[name] != "accepted"


def test_breakpoints_are_source_derived_boundary_pairs_and_capture_is_component_scoped():
    result = _node(
        _module_import("TRANSPORT_BREAKPOINT_CONTEXTS, transportCaptureRequirements")
        + """
const capture = transportCaptureRequirements();
console.log(JSON.stringify({ breakpoints:TRANSPORT_BREAKPOINT_CONTEXTS, capture }));
"""
    )
    pairs = {(row["family"], row["kind"], row["below"], row["at"]) for row in result["breakpoints"]}
    assert ("rail", "container", 539, 540) in pairs
    assert ("bus", "container", 699, 700) in pairs
    assert ("bus", "viewport-media", 720, 721) in pairs
    assert ("kaup", "container", 360, 361) in pairs
    assert ("kaup", "container", 390, 391) in pairs
    assert ("kaup", "container", 560, 561) in pairs
    assert ("consumer", "viewport", 1023, 1024) in pairs
    assert result["capture"]["full_html_retained"] is False
    assert result["capture"]["endpoint_or_key_attributes_retained"] is False
    assert result["capture"]["selectors"]["rail"] == "[data-event-transport-schedule]"
    assert "element_screenshot" in result["capture"]["evidence_fields"]
    assert "container" in result["capture"]["evidence_fields"]


def test_source_paths_and_markers_match_the_as_is_components():
    lane = _node(
        _module_import("buildTransportDecoderLane")
        + "const lane=buildTransportDecoderLane(); console.log(JSON.stringify({source_paths:lane.source_paths, allowlist:lane.exact_marker_allowlist}));"
    )
    for family in ("rail", "bus", "kaup"):
        source = (REPO / "site" / lane["source_paths"][family]).read_text(encoding="utf-8")
        for marker in lane["allowlist"][family]:
            # Some Kaup markers belong to its imported timetable children; the
            # whole reviewed source set, rather than only the shell, is authoritative.
            corpus = source
            if family == "kaup":
                corpus += "\n".join(
                    (REPO / "site" / path).read_text(encoding="utf-8")
                    for key, path in lane["source_paths"].items()
                    if key not in {"rail", "bus", "kaup"}
                )
            assert marker in corpus, f"{marker} missing from reviewed {family} source set"


def test_builder_output_is_deterministic_and_all_decisions_remain_not_merged():
    result = _node(
        _module_import("buildTransportDecoderLane")
        + """
const first=JSON.stringify(buildTransportDecoderLane());
const second=JSON.stringify(buildTransportDecoderLane());
const lane=JSON.parse(first);
console.log(JSON.stringify({
  equal:first === second,
  stateDecisions:[...new Set(lane.state_records.map((row) => row.decision))],
  planDecisions:[...new Set(lane.specimen_plan.map((row) => row.decision))],
  normalizing:lane.state_records.filter((row) => row.normalization_allowed !== false).length + lane.specimen_plan.filter((row) => row.normalization_allowed !== false).length,
  families:lane.families,
  rejected:lane.rejected_legacy_families,
}));
"""
    )
    assert result == {
        "equal": True,
        "stateDecisions": ["NOT_MERGED"],
        "planDecisions": ["NOT_MERGED"],
        "normalizing": 0,
        "families": ["transport.rail", "transport.bus", "transport.kaup"],
        "rejected": ["family.transport"],
    }
