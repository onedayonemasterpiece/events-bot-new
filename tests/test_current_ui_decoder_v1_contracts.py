import json
import subprocess
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
CONTRACTS = REPO / "scripts/current_ui_resource_graph/v1/contracts.mjs"
CAPSULES = REPO / "scripts/current_ui_resource_graph/v1/capsules.mjs"


def _node(source: str):
    completed = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        cwd=REPO,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


def _imports(names: str, module: Path = CONTRACTS) -> str:
    return f'import {{ {names} }} from {json.dumps(module.as_uri())};\n'


def test_candidate_suite_is_deterministic_rich_pinned_and_not_normative():
    value = _node(
        _imports("buildCandidateContracts, stableSerializeCandidateContracts")
        + """
const a=buildCandidateContracts(); const b=buildCandidateContracts();
console.log(JSON.stringify({
  count:a.length, ids:a.map((row)=>row.id), deterministic:JSON.stringify(a)===JSON.stringify(b),
  serializedEqual:stableSerializeCandidateContracts(a)===stableSerializeCandidateContracts(b),
  records:a.map((row)=>({
    id:row.id, sha:row.detached_contract_sha256, relationship:row.relationship_kind,
    confidence:row.confidence, version:row.candidate_contract.version,
    fields:Object.keys(row.candidate_contract), sourceShas:[...new Set(row.source_bindings.map((item)=>item.source_sha))],
    decision:row.decision, recommendation:row.recommendation, normalization:row.normalization_allowed,
    normative:row.normative_status, tokens:row.candidate_contract.token_refs,
  }))
}));
"""
    )
    assert value["count"] == 9
    assert value["deterministic"] is True
    assert value["serializedEqual"] is True
    assert set(value["ids"]) == {
        "candidate.event-detail-presentation", "candidate.button-cta-fragmented",
        "candidate.event-media", "candidate.transport-rail", "candidate.transport-bus",
        "candidate.transport-kaup", "candidate.event-token-medallions",
        "candidate.artifacts-focus-egg", "candidate.artifacts-amber",
    }
    required = {
        "version", "semantic_role", "anatomy", "props", "slots", "variant_axes",
        "state_axes", "valid_combinations", "invalid_combinations",
        "nested_component_refs", "token_refs", "responsive_contract", "media_contract",
        "accessibility_contract", "fixture_classes",
    }
    for row in value["records"]:
        assert len(row["sha"]) == 64
        assert row["version"].startswith("0.")
        assert required <= set(row["fields"])
        assert row["sourceShas"] == ["ef7aa62e45c60f7a12da6160f490719c0721ec03"]
        assert row["decision"] == "NOT_MERGED"
        assert row["recommendation"] == "unresolved"
        assert row["normalization"] is False
        assert row["normative"] == "candidate-as-is-not-accepted"
        assert row["tokens"] == []


def test_button_event_media_and_event_presentation_as_is_contracts_preserve_conflicts():
    value = _node(
        _imports("buildCandidateContracts")
        + """
const byId=Object.fromEntries(buildCandidateContracts().map((row)=>[row.id,row]));
console.log(JSON.stringify({
  button:byId['candidate.button-cta-fragmented'],
  event:byId['candidate.event-detail-presentation'],
  media:byId['candidate.event-media'],
}));
"""
    )
    button = value["button"]
    assert button["relationship_kind"] == "unresolved"
    assert button["candidate_contract"]["props"]["variant"]["union"] == [
        "primary", "secondary", "quiet", "inverse", "danger"
    ]
    assert button["candidate_contract"]["props"]["state"]["union"] == [
        "default", "hover", "focus", "pressed", "loading", "disabled"
    ]
    assert "fragmented implementations are recorded, not merged" in button["normalization_gaps"]

    event = value["event"]
    assert event["candidate_contract"]["variant_axes"]["desktop_family"] == ["editorial", "split"]
    assert "editorial => action_layout=stacked" in event["candidate_contract"]["valid_combinations"]
    assert "split => action_layout=inline" in event["candidate_contract"]["valid_combinations"]
    assert event["reachability"] == "production-reachable-record-binding-pending"

    media = value["media"]
    formats = media["candidate_contract"]["variant_axes"]["resource_format"]
    assert "editorial-large-poster-companion" in formats
    assert "editorial-small-companion-previews" in formats
    assert "split-small-photo-rail" in formats
    assert "treating every media item as an equal-size preview" in media["candidate_contract"]["invalid_combinations"]


def test_specialized_lane_contracts_preserve_source_only_and_independent_systems():
    value = _node(
        _imports("buildCandidateContracts")
        + """
const byId=Object.fromEntries(buildCandidateContracts().map((row)=>[row.id,row]));
console.log(JSON.stringify({
  kaup:byId['candidate.transport-kaup'], med:byId['candidate.event-token-medallions'],
  focus:byId['candidate.artifacts-focus-egg'], amber:byId['candidate.artifacts-amber'],
}));
"""
    )
    kaup = value["kaup"]
    assert kaup["reachability"] == "production-baseline-plus-experiment-off-source-implementations"
    assert kaup["candidate_contract"]["variant_axes"]["timetable_treatment"] == [
        "departure_board_v1", "route_strips_v1", "next_departure_queue_v1"
    ]
    assert kaup["unresolved_alternatives"] == [
        "baseline and experiment implementations remain NOT_MERGED"
    ]

    med = value["med"]
    assert med["candidate_contract"]["props"]["layout"]["default"] == "inline"
    assert med["candidate_contract"]["props"]["layout"]["union"] == ["inline", "desktop-slots"]
    assert med["candidate_contract"]["variant_axes"]["kind"] == [
        "organizer", "source", "program", "pushkin", "badge", "pill"
    ]

    assert value["focus"]["reachability"] == "lab-only"
    assert value["amber"]["reachability"] == "source-only"
    assert value["focus"]["unresolved_alternatives"] == ["independent-not-a-variant"]
    assert value["amber"]["unresolved_alternatives"] == ["independent-not-a-variant"]
    assert "must not be promoted to production-observed" in value["amber"]["promotion_blockers"]


def test_existing_event_presentation_records_are_consumed_without_changing_decision():
    value = _node(
        _imports("buildCandidateContracts")
        + """
const rows=[
 {id:'event-format.desktop.editorial-landscape',status:'observed',decision:'NOT_MERGED',source_component_ids:['source.a'],runtime_route_count:2},
 {id:'event-format.desktop.split-portrait-poster',status:'source_only',decision:'NOT_MERGED',source_component_ids:['source.a'],runtime_route_count:0},
];
const event=buildCandidateContracts({eventPresentationRecords:rows}).find((row)=>row.id==='candidate.event-detail-presentation');
console.log(JSON.stringify(event));
"""
    )
    assert value["reachability"] == "production-reachable-records-supplied"
    evidence = {row["id"]: row for row in value["evidence"]}
    assert evidence["existing.event-format.desktop.editorial-landscape"]["confidence"] == "observed"
    assert evidence["existing.event-format.desktop.split-portrait-poster"]["confidence"] == "deterministic"
    assert value["decision"] == "NOT_MERGED"
    assert value["human_review_status"] == "pending"


def test_detached_contract_hash_and_closed_enums_fail_closed():
    result = _node(
        _imports("buildCandidateContracts, assertCandidateContract")
        + """
const original=buildCandidateContracts()[0];
const cases={
  good:original,
  tampered:{...original,reachability:'invented'},
  badRelationship:{...original,relationship_kind:'merged'},
  badConfidence:{...original,confidence:'certain'},
  accepted:{...original,normative_status:'accepted'},
  tokenized:{...original,candidate_contract:{...original.candidate_contract,token_refs:['color.brand']}},
};
console.log(JSON.stringify(Object.fromEntries(Object.entries(cases).map(([name,row])=>{
  try { assertCandidateContract(row); return [name,'accepted']; } catch(error) { return [name,error.message]; }
}))));
"""
    )
    assert result["good"] == "accepted"
    for key in ("tampered", "badRelationship", "badConfidence", "accepted", "tokenized"):
        assert result[key] != "accepted"


def test_six_capsules_have_canonical_directory_files_and_no_fake_review_or_capture():
    value = _node(
        _imports("buildReconciliationCapsules, CAPSULE_DIRECTORIES, CAPSULE_FILES", CAPSULES)
        + """
const rows=buildReconciliationCapsules();
console.log(JSON.stringify({directories:CAPSULE_DIRECTORIES,canonicalFiles:CAPSULE_FILES,rows}));
"""
    )
    assert [row["directory"] for row in value["rows"]] == value["directories"]
    assert len(value["rows"]) == 6
    for capsule in value["rows"]:
        assert set(capsule["files"]) == set(value["canonicalFiles"])
        overview = capsule["files"]["capsule.json"]
        screenshots = capsule["files"]["screenshot-refs.json"]
        reviewer = capsule["files"]["reviewer-conclusion.json"]
        specimen = capsule["files"]["specimen-ref.json"]
        page = capsule["files"]["real-page-ref.json"]
        assert overview["review_status"] == "pending"
        assert overview["evidence_status"] == "planned-not-captured"
        assert screenshots == {
            "human_visual_review": "pending",
            "references": [],
            "status": "no-capture-attached",
        }
        assert reviewer["status"] == "pending"
        assert reviewer["conclusion"] is None
        assert reviewer["reviewer"] is None
        assert specimen["observations"] == []
        assert page["production_observed_by_capsule"] is False
        assert capsule["decision"] == "NOT_MERGED"
        assert capsule["normalization_allowed"] is False
        source_record = capsule["files"]["source-facts.json"]
        assert source_record["facts"]
        assert isinstance(source_record["inference"], list)
        assert isinstance(source_record["open_questions"], list)
        assert source_record["decision"]


def test_capsule_contract_refs_are_integral_and_hash_review_claims_fail_closed():
    result = _node(
        _imports("buildReconciliationCapsules, assertReconciliationCapsules", CAPSULES)
        + """
const original=buildReconciliationCapsules();
const dangling=structuredClone(original); dangling[0].files['candidate-contract-ref.json'].contract_ids=['candidate.missing'];
const reviewed=structuredClone(original); reviewed[0].files['reviewer-conclusion.json'].status='complete';
const capture=structuredClone(original); capture[0].files['screenshot-refs.json'].references=['fake.png'];
const tampered=structuredClone(original); tampered[0].title='changed';
const cases={good:original,dangling,reviewed,capture,tampered};
console.log(JSON.stringify(Object.fromEntries(Object.entries(cases).map(([name,rows])=>{
  try { assertReconciliationCapsules(rows); return [name,'accepted']; } catch(error) { return [name,error.message]; }
}))));
"""
    )
    assert result["good"] == "accepted"
    for key in ("dangling", "reviewed", "capture", "tampered"):
        assert result[key] != "accepted"


def test_consolidated_bundle_uses_only_allowed_conclusions_and_never_promotes_style_counts():
    value = _node(
        _imports(
            "buildDecoderReconciliationBundle, RECONCILIATION_CONCLUSIONS, assertMismatchRecords",
            CAPSULES,
        )
        + """
const bundle=buildDecoderReconciliationBundle();
let rawStyle='accepted';
try { assertMismatchRecords([{id:'mismatch.style-809',kind:'style-divergence',conclusion:'unresolved mapping',decision:'NOT_MERGED',normalization_allowed:false}]); }
catch(error) { rawStyle=error.message; }
console.log(JSON.stringify({
  contractCount:bundle.candidate_contracts.length,capsuleCount:bundle.capsules.length,
  observationCount:bundle.specimen_observations.length, conclusions:[...new Set(bundle.mismatches.map((row)=>row.conclusion))].sort(),
  allowed:RECONCILIATION_CONCLUSIONS, mismatchIds:bundle.mismatches.map((row)=>row.id),
  constraints:bundle.constraints, unresolved:bundle.unresolved, rawStyle,
}));
"""
    )
    assert value["contractCount"] == 9
    assert value["capsuleCount"] == 6
    assert value["observationCount"] == 0
    assert set(value["conclusions"]) <= set(value["allowed"])
    assert all("809" not in item for item in value["mismatchIds"])
    assert value["rawStyle"] != "accepted"
    assert value["constraints"]["capture_claimed"] is False
    assert value["constraints"]["human_review_claimed"] is False
    assert all(value["constraints"][key] is False for key in (
        "merge", "split", "normalization", "tokenization", "penpot_mutation", "astro_css_mutation"
    ))
    unresolved = {item["id"]: item for item in value["unresolved"]}
    assert unresolved["unresolved.contracts.human-capsule-review"]["blocks_handoff"] is True
    assert unresolved["unresolved.contracts.transport-experiment-source-only"]["blocks_handoff"] is False


def test_bundle_serialization_is_deterministic_and_has_unique_plan_ids():
    value = _node(
        _imports("buildDecoderReconciliationBundle, stableSerializeDecoderReconciliationBundle", CAPSULES)
        + """
const a=buildDecoderReconciliationBundle(); const b=buildDecoderReconciliationBundle();
console.log(JSON.stringify({
  equal:JSON.stringify(a)===JSON.stringify(b), serialized:stableSerializeDecoderReconciliationBundle(a)===stableSerializeDecoderReconciliationBundle(b),
  planCount:a.specimen_plan.length,uniquePlans:new Set(a.specimen_plan.map((row)=>row.id)).size,
  planStatuses:[...new Set(a.specimen_plan.map((row)=>row.observation_status||row.status))].sort(),
}));
"""
    )
    assert value["equal"] is True
    assert value["serialized"] is True
    assert value["planCount"] == value["uniquePlans"]
    assert value["planCount"] >= 50
    assert set(value["planStatuses"]) <= {"not-captured", "planned-not-captured"}
