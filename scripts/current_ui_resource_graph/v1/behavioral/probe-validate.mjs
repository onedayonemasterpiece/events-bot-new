import { createHash } from 'node:crypto';

export const BREAKPOINT_PROBE_SCHEMA='current_ui_breakpoint_container_probe_v1_1';
export const RAIL_KEYBOARD_SCHEMA='current_ui_rail_keyboard_packet_v1_1';
export const PROBE_TERMINAL_STATUSES=Object.freeze(['PASS','MISMATCH','UNREACHABLE_WITH_REASON']);
const TERMINAL=new Set(PROBE_TERMINAL_STATUSES);
const SENSITIVE=/(?:authorization|bearer\s|password|client[_-]?secret|access[_-]?token|api[_-]?key|sb_(?:secret|publishable)|https?:\/\/[^/\s]*:[^@\s]*@)/iu;
const sha=(value)=>createHash('sha256').update(value).digest('hex');

function safeTree(value,path='$'){
  if(typeof value==='string'&&SENSITIVE.test(value))throw new Error(`Sensitive probe evidence at ${path}`);
  if(!value||typeof value!=='object')return;
  for(const [key,child] of Object.entries(value)){
    if(/^(?:innerhtml|outerhtml|full_html|authorization|access_token|raw_url|url)$/iu.test(key))throw new Error(`Unsafe probe field ${path}.${key}`);
    safeTree(child,`${path}.${key}`);
  }
}
export function stableProbeJson(value){
  const stable=(item)=>Array.isArray(item)?item.map(stable):item&&typeof item==='object'?Object.fromEntries(Object.keys(item).sort().map((key)=>[key,stable(item[key])])):item;
  return JSON.stringify(stable(value));
}
export function semanticBreakpointProbeProjection(row){
  const projected=JSON.parse(stableProbeJson(row));
  if(projected.raster){delete projected.raster.bytes;delete projected.raster.sha256;}
  if(projected.selector_cascade_evidence){const count=projected.selector_cascade_evidence.affected_selector_resolved_count;projected.selector_cascade_evidence.affected_selector_resolved=Boolean(count>0);delete projected.selector_cascade_evidence.affected_selector_resolved_count;}
  for(const attempt of projected.consumer_attempts||[]){const count=attempt.affected_selector_resolved_count;attempt.affected_selector_resolved=Boolean(count>0);delete attempt.affected_selector_resolved_count;}
  return projected;
}
export function deterministicProbeHash(rows){return sha(rows.slice().sort((a,b)=>a.id.localeCompare(b.id)).map((row)=>stableProbeJson(semanticBreakpointProbeProjection(row))).join('\n'));}

export function assertBreakpointProbeRecord(row){
  if(row.schema_version!==BREAKPOINT_PROBE_SCHEMA||!/^breakpoint\.[a-f0-9]{16}$/u.test(row.id||''))throw new Error('Invalid breakpoint probe identity');
  if(!TERMINAL.has(row.terminal_status))throw new Error(`Probe is not terminal: ${row.id}`);
  if(!row.terminal_reason||!row.source?.sha||!row.source?.path||!Number.isInteger(row.source?.line)||!Number.isInteger(row.source?.at_rule_ordinal)||!/^[a-f0-9]{64}$/u.test(row.source?.rule_fingerprint||''))throw new Error(`Probe source proof missing: ${row.id}`);
  if(!row.component||!row.consumer||!row.route||!row.environment||typeof row.expected_branch!=='boolean')throw new Error(`Probe consumer/environment missing: ${row.id}`);
  if(row.terminal_status==='UNREACHABLE_WITH_REASON'){
    if(row.actual_branch!==null||!row.unreachable?.reason_code||!row.unreachable?.detail)throw new Error(`Unreachable reason missing: ${row.id}`);
  }else{
    if(typeof row.actual_branch!=='boolean'||!row.runtime_marker||!row.root_bbox||!row.affected_target_bbox||!Array.isArray(row.computed_styles)||!row.overflow_clipping||!row.visibility||!row.selector_cascade_evidence)throw new Error(`Runtime facts missing: ${row.id}`);
  }
  if(row.terminal_status==='PASS'){
    const evidence=row.selector_cascade_evidence;
    if(evidence?.exact_compiled_cssom_rule!==true)throw new Error(`PASS without exact compiled CSSOM rule: ${row.id}`);
    if(!Array.isArray(evidence?.compiled_rule_fingerprints)||!evidence.compiled_rule_fingerprints.length||evidence.compiled_rule_fingerprints.some((value)=>!/^[a-f0-9]{64}$/u.test(value)))throw new Error(`PASS without compiled CSSOM fingerprint: ${row.id}`);
    if(evidence?.real_exact_source_consumer!==true)throw new Error(`PASS without real exact-source consumer: ${row.id}`);
    if(!(evidence?.affected_selector_resolved_count>0))throw new Error(`PASS without affected selector resolution: ${row.id}`);
    if(!['winning','attributed-consumer-override'].includes(evidence?.cascade_reconciliation))throw new Error(`PASS without cascade reconciliation: ${row.id}`);
    if(row.runtime_marker.kind==='synthetic-only')throw new Error(`PASS from a synthetic marker alone: ${row.id}`);
    if(row.actual_branch!==row.expected_branch)throw new Error(`PASS branch mismatch: ${row.id}`);
  }
  if(row.raster?.selected===true&&(!row.raster.path||!row.raster.selection_reason||(!/^[a-f0-9]{64}$/u.test(row.raster.sha256||'')&&!/^[a-f0-9]{16}$/u.test(row.raster.dhash||''))))throw new Error(`Selected raster is not indexed: ${row.id}`);
  if(row.raster?.selected!==true&&row.raster?.path)throw new Error(`Unselected probe exposes raster path: ${row.id}`);
  if(row.production_state_claimed!==false||row.normalization_allowed!==false||row.decision!=='NOT_MERGED')throw new Error(`Probe STOP boundary missing: ${row.id}`);
  safeTree(row);return true;
}

export function assertBreakpointProbeClosure(rows,{expectedIds=null,expectedCount=293,expectedPaths=32}={}){
  if(!Array.isArray(rows)||rows.length!==expectedCount)throw new Error(`Breakpoint terminal coverage mismatch: ${rows?.length||0}/${expectedCount}`);
  const ids=rows.map((row)=>row.id);if(new Set(ids).size!==ids.length)throw new Error('Breakpoint terminal IDs duplicated');
  for(const row of rows)assertBreakpointProbeRecord(row);
  if(expectedIds){const expected=[...expectedIds].sort();const actual=[...ids].sort();if(stableProbeJson(expected)!==stableProbeJson(actual))throw new Error('Breakpoint terminal ID set drift');}
  if(new Set(rows.map((row)=>row.source.path)).size!==expectedPaths)throw new Error(`Breakpoint source path coverage mismatch: ${new Set(rows.map((row)=>row.source.path)).size}/${expectedPaths}`);
  return {terminal:rows.length,pass:rows.filter((row)=>row.terminal_status==='PASS').length,mismatch:rows.filter((row)=>row.terminal_status==='MISMATCH').length,unreachable:rows.filter((row)=>row.terminal_status==='UNREACHABLE_WITH_REASON').length,rasters:rows.filter((row)=>row.raster?.selected).length,blocks_ready:false,deterministic_sha256:deterministicProbeHash(rows)};
}

export function assertRailKeyboardPacket(row){
  if(row.schema_version!==RAIL_KEYBOARD_SCHEMA||row.source_sha!=='ef7aa62e45c60f7a12da6160f490719c0721ec03')throw new Error('Rail keyboard identity mismatch');
  if(row.viewport?.width!==390||row.viewport?.height!==844||row.role_contract?.tag!=='div'||row.role_contract?.role!==null||row.role_contract?.tabindex!==0||row.role_contract?.composite!==false)throw new Error('Rail must remain an ordinary focusable div, not a composite widget');
  if(row.focus_acquisition?.programmatic_focus_used!==false||row.focus_acquisition?.keys_used?.some((key)=>!['Tab','Shift+Tab'].includes(key)))throw new Error('Rail focus acquisition must use only Tab/Shift+Tab');
  if(row.tab_sequence?.rail?.focus_visible!==true||row.tab_sequence?.like?.focus_visible!==true)throw new Error('Rail/like focus-visible evidence missing');
  if(!row.like_keyboard?.space?.toggled||!row.like_keyboard?.enter?.toggled)throw new Error('Space/Enter like toggle evidence missing');
  for(const position of ['start','middle','end'])if(!row.arrow_keys?.[position]?.observed)throw new Error(`Rail arrow evidence missing: ${position}`);
  if(row.arrow_keys.start.at_start!==true||row.arrow_keys.end.at_end!==true)throw new Error('Rail boundary booleans missing');
  if(row.home_end?.required!==false||row.home_end?.blocks_ready!==false||row.home_end?.classification!=='observed-enhancement-nonblocking')throw new Error('Home/End must be non-required nonblocking evidence');
  if(row.drag_only_not_interested?.blocks_ready!==false||row.drag_only_not_interested?.classification!=='evidence-complete-conformance-gap'||!Array.isArray(row.drag_only_not_interested?.visible_sequential_focusable_equivalents))throw new Error('Drag-only conformance gap is not fully evidenced');
  if(row.link_sequential_reachability?.blocks_ready!==false)throw new Error('Rail link reachability finding must be nonblocking once evidenced');
  if(!Array.isArray(row.rasters)||row.rasters.length<2||row.rasters.some((item)=>item.selected!==true||!item.path||!item.bytes||!/^[a-f0-9]{64}$/u.test(item.sha256||'')||!/^[a-f0-9]{16}$/u.test(item.dhash||'')||item.full_resolution!==true))throw new Error('Rail focused full-resolution raster index incomplete');
  if(row.blocks_ready!==false||row.terminal_status!=='PASS')throw new Error('Complete rail packet must be terminal nonblocking');
  safeTree(row);return true;
}
