import json
from pathlib import Path
import subprocess


REPO = Path(__file__).resolve().parents[1]
MODULE = REPO / "scripts/current_ui_resource_graph/v1/specimens/index.mjs"
FIXTURE = REPO / "tests/fixtures/current-ui-decoder-v1/specimens/expected-registry.json"


def _node(source: str):
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        cwd=REPO,
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _imports(names: str) -> str:
    return f'import {{ {names} }} from {json.dumps(MODULE.as_uri())};\n'


def test_registry_is_bounded_pairwise_deterministic_and_covers_six_capsules():
    expected = json.loads(FIXTURE.read_text(encoding="utf-8"))
    observed = _node(
        _imports("buildSpecimenRegistry, assertSpecimenRegistry, stableHash")
        + """
const a=buildSpecimenRegistry(); const b=buildSpecimenRegistry(); assertSpecimenRegistry(a);
console.log(JSON.stringify({
 controlled_count:a.controlled_specimens.length,
 real_route_count:a.real_route_verifications.length,
 source_model_only_count:a.source_model_only_cases.length,
 capsules:[...new Set([...a.controlled_specimens,...a.real_route_verifications].flatMap((row)=>row.capsule_ids))].sort(),
 renderers:[...new Set(a.controlled_specimens.map((row)=>row.renderer))].sort(),
 deterministic:stableHash(a)===stableHash(b), cartesian_forbidden:a.policy.cartesian_product_forbidden,
}));
"""
    )
    assert {key: observed[key] for key in expected} == expected
    assert observed["deterministic"] is True
    assert observed["cartesian_forbidden"] is True


def test_every_controlled_specimen_has_explicit_trace_and_never_claims_production():
    observed = _node(
        _imports("buildSpecimenRegistry")
        + """
const r=buildSpecimenRegistry();
console.log(JSON.stringify(r.controlled_specimens.map((row)=>({
 id:row.id, route_kind:row.route_kind, claim_scope:row.claim_scope, trace_kind:row.trace_kind,
 equivalence:row.state_equivalence, production:row.production_state_claimed,
 sources:row.source_paths.length, root:Boolean(row.root_selector), parts:row.evidence_parts,
}))));
"""
    )
    assert len(observed) == 19
    for row in observed:
        assert row["route_kind"] == "controlled-specimen"
        assert row["claim_scope"] == "controlled-candidate-source-render-only"
        assert row["trace_kind"] == "source-to-controlled-specimen"
        assert row["equivalence"]
        assert row["production"] is False
        assert row["sources"] > 0 and row["root"] is True
        assert {"element-screenshot", "aria", "computed-style", "geometry", "css-vars", "pseudo", "focus", "open-hidden", "media"} <= set(row["parts"])


def test_real_preview_events_are_used_for_medallions_rail_and_kaup_without_projection_tokens():
    observed = _node(
        _imports("buildSpecimenRegistry, loadPreviewEventCatalog, resolvePreviewEventFixture")
        + f"""
const r=buildSpecimenRegistry(); const catalog=loadPreviewEventCatalog({json.dumps(str(REPO / 'site'))});
const selected=r.controlled_specimens.filter((row)=>['medallions','rail','kaup'].includes(row.renderer)).map((row)=>{{
 const resolved=resolvePreviewEventFixture(catalog,row.fixture_ref,row.fixture_delta);
 return {{id:row.id,event_id:resolved.event.id,source_prod_id:resolved.event.source_prod_id,delta:resolved.trace.delta_fields,props:row.props}};
}});
console.log(JSON.stringify(selected));
"""
    )
    assert observed
    assert all(row["event_id"] == row["source_prod_id"] for row in observed)
    medallions = [row for row in observed if row["id"].startswith("medallions-")]
    assert {row["event_id"] for row in medallions} == {2601, 5336, 6856, 6994}
    assert all(set(row["props"]) == {"layout", "allowTopSlot"} for row in medallions)
    assert all("tokens" not in row["props"] for row in medallions)
    assert [row for row in observed if row["delta"]] == [next(row for row in observed if row["id"] == "rail-forecast-controlled-delta")]


def test_unreachable_bus_state_is_source_model_only_and_not_materialized():
    observed = _node(
        _imports("buildSpecimenRegistry")
        + """
const r=buildSpecimenRegistry(); const model=r.source_model_only_cases[0];
console.log(JSON.stringify({model, controlled:r.controlled_specimens.filter((row)=>row.renderer==='bus')}));
"""
    )
    assert observed["model"]["id"] == "bus-no-outbound-groups"
    assert observed["model"]["reachability"] == "source-model-only"
    assert observed["model"]["production_state_claimed"] is False
    assert observed["controlled"] == []


def test_materializer_generates_only_temp_wrappers_and_copies_candidate_src(tmp_path):
    harness = tmp_path / "harness"
    modules = tmp_path / "node_modules"
    modules.mkdir()
    observed = _node(
        _imports("buildSpecimenRegistry, materializeSpecimenHarness")
        + f"""
import {{ readFileSync, lstatSync }} from 'node:fs';
const registry=buildSpecimenRegistry();
const receipt=materializeSpecimenHarness({{candidateSite:{json.dumps(str(REPO / 'site'))},harnessRoot:{json.dumps(str(harness))},nodeModules:{json.dumps(str(modules))},registry}});
const med=readFileSync({json.dumps(str(harness / 'src/pages/specimens/medallions-desktop-slots-no-top.astro'))},'utf8');
console.log(JSON.stringify({{receipt, sourceIsSymlink:lstatSync({json.dumps(str(harness / 'upstream'))}).isSymbolicLink(), med}}));
"""
    )
    assert observed["receipt"]["generated_wrapper_count"] == 19
    assert observed["receipt"]["source_copy_mode"] == "exact-src-reflink-or-copy"
    assert observed["receipt"]["source_symlinked"] is False
    assert observed["receipt"]["production_source_mutated"] is False
    assert observed["sourceIsSymlink"] is False
    assert "EventTokenMedallions.astro" in observed["med"]
    assert 'layout="desktop-slots"' in observed["med"]
    assert "allowTopSlot={false}" in observed["med"]
    assert "tokens=" not in observed["med"]
    assert all(not Path(row["path"]).is_absolute() for row in observed["receipt"]["pages"])


def test_validators_reject_dangling_refs_fake_claims_sensitive_packets_and_bad_deltas():
    observed = _node(
        _imports("buildSpecimenRegistry, assertSpecimenRegistry, assertEvidencePacket, validateTraceIntegrity")
        + """
const base=buildSpecimenRegistry();
const cases={
 fakeRegistry:()=>assertSpecimenRegistry({...base,controlled_specimens:base.controlled_specimens.map((row,index)=>index?row:{...row,production_state_claimed:true})}),
 dangling:()=>validateTraceIntegrity(base,[{specimen_id:'missing'}],[]),
 badDelta:()=>assertSpecimenRegistry({...base,controlled_specimens:base.controlled_specimens.map((row)=>row.id==='rail-explicit-real-event'?{...row,fixture_delta:{title:'fake'}}:row)}),
 sensitive:()=>assertEvidencePacket({schema_version:base.schema_version,evidence_status:'captured-not-reviewed',production_state_claimed:false,proof_label:'controlled-specimen-browser-element',screenshot:{sha256:'a'.repeat(64),dhash:'b'.repeat(16)},dom:{full_html_retained:false},network:{raw_urls_retained:false},note:'https://private.invalid'}),
};
console.log(JSON.stringify(Object.fromEntries(Object.entries(cases).map(([name,fn])=>{try{fn();return [name,'accepted']}catch(error){return [name,error.message]}}))));
"""
    )
    assert all(value != "accepted" for value in observed.values())


def test_materializer_rejects_candidate_ancestor_as_destructive_harness_root():
    observed = _node(
        _imports("materializeSpecimenHarness")
        + f"""
let result='accepted';
try {{ materializeSpecimenHarness({{candidateSite:{json.dumps(str(REPO / 'site'))},harnessRoot:{json.dumps(str(REPO))}}}); }}
catch(error) {{ result=error.message; }}
console.log(JSON.stringify(result));
"""
    )
    assert observed != "accepted"
    assert "specific disposable path" in observed


def test_png_perceptual_hash_is_deterministic_and_pixel_sensitive():
    observed = _node(
        _imports("pngDifferenceHash, decodePngRgb")
        + """
import { deflateSync } from 'node:zlib';
function png(width,height,pixel){
 const signature=Buffer.from([137,80,78,71,13,10,26,10]);
 const chunk=(type,data)=>{const head=Buffer.alloc(8);head.writeUInt32BE(data.length);head.write(type,4,'ascii');return Buffer.concat([head,data,Buffer.alloc(4)])};
 const ihdr=Buffer.alloc(13);ihdr.writeUInt32BE(width,0);ihdr.writeUInt32BE(height,4);ihdr[8]=8;ihdr[9]=6;
 const raw=Buffer.concat(Array.from({length:height},(_,y)=>Buffer.concat([Buffer.from([0]),Buffer.concat(Array.from({length:width},(_,x)=>Buffer.from(pixel(x,y))))])));
 return Buffer.concat([signature,chunk('IHDR',ihdr),chunk('IDAT',deflateSync(raw)),chunk('IEND',Buffer.alloc(0))]);
}
const a=png(9,8,(x)=>x<4?[255,255,255,255]:[0,0,0,255]); const b=png(9,8,(x)=>x<5?[255,255,255,255]:[0,0,0,255]);
console.log(JSON.stringify({a:pngDifferenceHash(a),again:pngDifferenceHash(a),b:pngDifferenceHash(b),decoded:decodePngRgb(a)} ,(key,value)=>key==='pixels'?undefined:value));
"""
    )
    assert observed["a"] == observed["again"]
    assert len(observed["a"]) == 16
    assert observed["a"] != observed["b"]
    assert observed["decoded"] == {"width": 9, "height": 8, "channels": 4}


def test_capture_contract_does_not_claim_any_capture_during_unit_tests():
    observed = _node(
        _imports("buildSpecimenRegistry")
        + """
const r=buildSpecimenRegistry();
console.log(JSON.stringify({statuses:[...r.controlled_specimens,...r.real_route_verifications].map((row)=>row.evidence_status||'planned'), reviewed:[...r.controlled_specimens,...r.real_route_verifications].some((row)=>row.review_status==='reviewed')}));
"""
    )
    assert set(observed["statuses"]) == {"planned"}
    assert observed["reviewed"] is False
