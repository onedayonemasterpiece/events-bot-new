import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PINNED = 'ef7aa62e45c60f7a12da6160f490719c0721ec03'
CANDIDATE = Path('/home/dev/.codex/worktrees/events-bot-new/exact-candidate-ef7-packet/site')


def node(script, *args, check=True):
    return subprocess.run(
        ['node', '--input-type=module', '-e', script, *map(str, args)],
        cwd=ROOT, text=True, capture_output=True, check=check,
    )


def exact_matrix(tmp_path):
    if not CANDIDATE.exists():
        return None
    output = tmp_path / 'source-pass'
    subprocess.run([
        'node', 'scripts/current_ui_resource_graph/behavioral-decode.mjs',
        '--source-root', str(CANDIDATE),
        '--base-snapshot-root', 'tests/fixtures/current-ui-behavioral-mini/base',
        '--output', str(output), '--source-sha', PINNED,
    ], cwd=ROOT, text=True, capture_output=True, check=True)
    return output / 'breakpoint-and-container-matrix.jsonl'


def test_source_parser_records_axes_combined_queries_and_nonnumeric():
    script = r"""
      import {parseSourceAtRules} from './scripts/current_ui_resource_graph/v1/behavioral/breakpoint-source.mjs';
      const content=`<style>
        @media (min-width: 760px) and (max-height: 720px) { .x, .y:hover { width:1px; overflow:hidden } }
        @media (prefers-reduced-motion:no-preference) { .x { animation:none } }
        @container event-rail (max-width:539px) { .z { grid-template-columns:1fr } }
      </style>`;
      const rows=parseSourceAtRules({path:'src/example.astro',content});
      console.log(JSON.stringify(rows.map(x=>({kind:x.kind,query:x.query,condition_query:x.condition_query,name:x.container_name,features:x.features.map(f=>[f.name,f.axis,f.threshold_px]),selectors:x.affected_selectors,declarations:x.affected_declarations.map(d=>d.property),fingerprint:x.rule_fingerprint}))));
    """
    payload = json.loads(node(script).stdout)
    assert payload[0]['features'] == [['min-width', 'width', 760], ['max-height', 'height', 720]]
    assert payload[1]['features'] == [['prefers-reduced-motion', 'reduced-motion', None]]
    assert payload[2]['query'] == 'event-rail (max-width:539px)'
    assert payload[2]['condition_query'] == '(max-width:539px)'
    assert payload[2]['name'] == 'event-rail'
    assert all(len(row['fingerprint']) == 64 for row in payload)


def test_exact_matrix_enrichment_preserves_293_ids_and_32_paths(tmp_path):
    matrix = exact_matrix(tmp_path)
    if matrix is None:
        return
    script = r"""
      import {loadAndEnrichBreakpointMatrix} from './scripts/current_ui_resource_graph/v1/behavioral/breakpoint-source.mjs';
      import {buildBreakpointProbePlans} from './scripts/current_ui_resource_graph/v1/behavioral/breakpoint-plan.mjs';
      const rows=loadAndEnrichBreakpointMatrix({matrixPath:process.argv[1],sourceRoot:process.argv[2],sourceSha:process.argv[3]});
      const plans=buildBreakpointProbePlans(rows);const second=buildBreakpointProbePlans(rows);
      console.log(JSON.stringify({count:rows.length,ids:new Set(rows.map(x=>x.id)).size,paths:new Set(rows.map(x=>x.path)).size,media:rows.filter(x=>x.kind==='media').length,container:rows.filter(x=>x.kind==='container').length,numeric:rows.filter(x=>x.threshold_px!==null).length,nonnumeric:rows.filter(x=>x.threshold_px===null).length,noRules:rows.filter(x=>!x.source_rules.length).length,allMapped:plans.every(x=>x.consumers.length),hashes:rows.every(x=>x.rule_fingerprint.length===64&&x.at_rule_fingerprint.length===64),deterministic:JSON.stringify(plans)===JSON.stringify(second)}));
    """
    payload = json.loads(node(script, matrix, CANDIDATE, PINNED).stdout)
    assert payload == {'count': 293, 'ids': 293, 'paths': 32, 'media': 272,
                       'container': 21, 'numeric': 273, 'nonnumeric': 20,
                       'noRules': 0, 'allMapped': True, 'hashes': True,
                       'deterministic': True}


def test_combined_width_height_and_named_container_plans_are_exact():
    script = r"""
      import {planProbeEnvironment} from './scripts/current_ui_resource_graph/v1/behavioral/breakpoint-plan.mjs';
      const width={kind:'media',probe_px:1023,axis:'viewport-width',container_name:null,target_feature:{name:'min-width',axis:'width',comparison:'min',threshold_px:1024},condition_features:[{name:'min-width',axis:'width',comparison:'min',threshold_px:1024},{name:'max-height',axis:'height',comparison:'max',threshold_px:720}]};
      const height={...width,probe_px:721,axis:'viewport-height',target_feature:width.condition_features[1]};
      const container={kind:'container',probe_px:539,axis:'container-width',container_name:'event-rail',target_feature:{name:'max-width',axis:'width',comparison:'max',threshold_px:539},condition_features:[{name:'max-width',axis:'width',comparison:'max',threshold_px:539}]};
      console.log(JSON.stringify([planProbeEnvironment(width),planProbeEnvironment(height),planProbeEnvironment(container)]));
    """
    width, height, container = json.loads(node(script).stdout)
    assert width['viewport'] == {'width': 1023, 'height': 656}
    assert width['expected_branch'] is False
    assert height['viewport'] == {'width': 1088, 'height': 721}
    assert height['expected_branch'] is False
    assert container['container'] == {'name': 'event-rail', 'width': 539, 'height': None, 'box': 'content-box'}
    assert container['expected_branch'] is True


def test_loading_state_selector_gets_deterministic_image_request_control():
    script = r"""
      import {planProbeEnvironment} from './scripts/current_ui_resource_graph/v1/behavioral/breakpoint-plan.mjs';
      const base={kind:'media',probe_px:null,axis:'source-query',container_name:null,target_feature:null,condition_features:[{name:'prefers-reduced-motion',axis:'reduced-motion',threshold_px:null,value:'no-preference'}]};
      const stable=planProbeEnvironment({...base,affected_selectors:['.card']});
      const loading=planProbeEnvironment({...base,affected_selectors:['.frame[data-image-state="loading"] .skeleton']});
      console.log(JSON.stringify({stable:stable.resource_control,loading:loading.resource_control}));
    """
    payload = json.loads(node(script).stdout)
    assert payload == {
        'stable': {'image_requests': 'normal', 'reason': None},
        'loading': {'image_requests': 'held-during-observation',
                    'reason': 'exact-affected-selector-requires-loading-state'},
    }


def test_marker_only_pass_and_nonterminal_status_are_rejected():
    base = {
        'schema_version': 'current_ui_breakpoint_container_probe_v1_1',
        'id': 'breakpoint.0123456789abcdef', 'source': {'sha': PINNED, 'path': 'src/x.astro',
        'line': 1, 'at_rule_ordinal': 0, 'rule_fingerprint': '0' * 64},
        'component': 'X', 'consumer': 'X', 'route': '/', 'environment': {},
        'expected_branch': True, 'actual_branch': True,
        'runtime_marker': {'kind': 'synthetic-only'}, 'root_bbox': {},
        'affected_target_bbox': {}, 'computed_styles': [], 'overflow_clipping': {},
        'visibility': {}, 'selector_cascade_evidence': {'exact_compiled_cssom_rule': False,
        'real_exact_source_consumer': True, 'affected_selector_resolved_count': 1,
        'cascade_reconciliation': 'winning'}, 'terminal_status': 'PASS',
        'terminal_reason': 'bad marker only', 'raster': {'selected': False},
        'production_state_claimed': False, 'normalization_allowed': False, 'decision': 'NOT_MERGED',
    }
    script = r"""
      import {assertBreakpointProbeRecord} from './scripts/current_ui_resource_graph/v1/behavioral/probe-validate.mjs';
      const row=JSON.parse(process.argv[1]);try{assertBreakpointProbeRecord(row);console.log('accepted')}catch(e){console.log(e.message)}
      row.terminal_status='planned';try{assertBreakpointProbeRecord(row);console.log('accepted')}catch(e){console.log(e.message)}
    """
    lines = node(script, json.dumps(base)).stdout.splitlines()
    assert 'PASS without exact compiled CSSOM rule' in lines[0]
    assert 'not terminal' in lines[1]


def test_explicit_unreachable_secret_scan_raster_bound_and_determinism():
    script = r"""
      import {createUnreachableProbeRecord,selectBreakpointRasterReason} from './scripts/current_ui_resource_graph/v1/behavioral/breakpoint-runtime.mjs';
      import {stableProbeJson,assertBreakpointProbeRecord} from './scripts/current_ui_resource_graph/v1/behavioral/probe-validate.mjs';
      const plan={id:'breakpoint.0123456789abcdef',source_sha:'ef7aa62e45c60f7a12da6160f490719c0721ec03',path:'src/pages/zakrytaya-afisha/index.astro',source_sha256:'1'.repeat(64),line:10,source_offset:100,at_rule_ordinal:0,at_rule_kind_ordinal:0,at_rule_fingerprint:'2'.repeat(64),rule_fingerprint:'3'.repeat(64),kind:'media',query:'(max-width:620px)',container_name:null,affected_selectors:['.focus-secret'],affected_declarations:[],contract_id:'secret',threshold_px:620,probe_px:619,probe:'threshold-minus-one',axis:'viewport-width',condition_features:[],environment:{viewport:{width:619,height:900},expected_branch:true},consumers:[]};
      const row=createUnreachableProbeRecord(plan,null,'NO_EXACT_CONSUMER_MAPPING','exact route/root absent');
      console.log(stableProbeJson({same:stableProbeJson(row)===stableProbeJson(row),terminal:row.terminal_status,code:row.unreachable.reason_code,raster:selectBreakpointRasterReason(row),mismatchRaster:selectBreakpointRasterReason({terminal_status:'MISMATCH'})}));
      row.terminal_reason='api_key=secret-value';try{assertBreakpointProbeRecord(row)}catch(e){console.log(e.message)}
    """
    lines = node(script).stdout.splitlines()
    payload = json.loads(lines[0])
    assert payload == {'code': 'NO_EXACT_CONSUMER_MAPPING', 'mismatchRaster': 'mismatch-terminal',
                       'raster': None, 'same': True, 'terminal': 'UNREACHABLE_WITH_REASON'}
    assert 'Sensitive probe evidence' in lines[1]


def test_semantic_digest_ignores_raster_bytes_sha_and_aggregate_counts(tmp_path):
    script = r"""
      import {writeBreakpointProbeEvidence} from './scripts/current_ui_resource_graph/v1/behavioral/breakpoint-runtime.mjs';
      import {deterministicProbeHash,stableProbeJson} from './scripts/current_ui_resource_graph/v1/behavioral/probe-validate.mjs';
      const root=process.argv[1];const row={schema_version:'current_ui_breakpoint_container_probe_v1_1',id:'breakpoint.0123456789abcdef',source:{sha:'ef7aa62e45c60f7a12da6160f490719c0721ec03',path:'src/x.astro',line:1,at_rule_ordinal:0,rule_fingerprint:'0'.repeat(64)},component:'X',consumer:'X',route:'/',environment:{},expected_branch:true,actual_branch:true,runtime_marker:{kind:'native'},root_bbox:{},affected_target_bbox:{},computed_styles:[],overflow_clipping:{},visibility:{},selector_cascade_evidence:{exact_compiled_cssom_rule:true,compiled_rule_fingerprints:['1'.repeat(64)],real_exact_source_consumer:true,affected_selector_resolved_count:1,cascade_reconciliation:'winning'},terminal_status:'MISMATCH',terminal_reason:'visual mismatch',raster:{selected:true,selection_reason:'mismatch-terminal',path:'r/a.png',bytes:99,sha256:'2'.repeat(64),dhash:'3'.repeat(16),clip:{x:0,y:0,width:1,height:1}},production_state_claimed:false,normalization_allowed:false,decision:'NOT_MERGED'};
      const other={...row,selector_cascade_evidence:{...row.selector_cascade_evidence,affected_selector_resolved_count:472},raster:{...row.raster,bytes:101,sha256:'4'.repeat(64)}};
      const receipt=writeBreakpointProbeEvidence({outputDir:root,records:[row],requireFullClosure:false});console.log(JSON.stringify({receipt,sameSemantic:deterministicProbeHash([row])===deterministicProbeHash([other]),sameExact:stableProbeJson(row)===stableProbeJson(other)}));
    """
    payload = json.loads(node(script, tmp_path).stdout)
    terminal = json.loads((tmp_path / 'breakpoint-probe-terminal.jsonl').read_text())
    index = json.loads((tmp_path / 'breakpoint-probe-raster-index.json').read_text())
    assert terminal['raster']['dhash'] == '3' * 16
    assert terminal['raster']['sha256'] == '2' * 64 and terminal['raster']['bytes'] == 99
    assert index['entries'][0]['sha256'] == '2' * 64 and index['entries'][0]['bytes'] == 99
    assert payload['sameSemantic'] is True and payload['sameExact'] is False
    assert payload['receipt']['deterministic_scope'].startswith('semantic-terminal')


def test_rail_contract_accepts_nonblocking_home_end_drag_and_link_gap():
    script = r"""
      import {assertRailKeyboardPacket} from './scripts/current_ui_resource_graph/v1/behavioral/probe-validate.mjs';
      const focus={focus_visible:true};const raster=(name)=>({selected:true,path:`rail/${name}.png`,bytes:10,sha256:'0'.repeat(64),dhash:'0'.repeat(16),full_resolution:true});const row={schema_version:'current_ui_rail_keyboard_packet_v1_1',source_sha:'ef7aa62e45c60f7a12da6160f490719c0721ec03',viewport:{width:390,height:844},role_contract:{tag:'div',role:null,tabindex:0,composite:false},focus_acquisition:{programmatic_focus_used:false,keys_used:['Tab','Shift+Tab']},tab_sequence:{rail:focus,like:focus},like_keyboard:{space:{toggled:true},enter:{toggled:true}},arrow_keys:{start:{observed:true,at_start:true},middle:{observed:true},end:{observed:true,at_end:true}},home_end:{required:false,blocks_ready:false,classification:'observed-enhancement-nonblocking'},drag_only_not_interested:{blocks_ready:false,classification:'evidence-complete-conformance-gap',visible_sequential_focusable_equivalents:[]},link_sequential_reachability:{observed:false,blocks_ready:false},rasters:[raster('rail'),raster('like')],blocks_ready:false,terminal_status:'PASS'};
      console.log(assertRailKeyboardPacket(row));
    """
    assert node(script).stdout.strip() == 'true'
