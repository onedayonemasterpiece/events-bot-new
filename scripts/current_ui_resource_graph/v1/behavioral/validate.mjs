import { createHash } from 'node:crypto';
import { ALL_BREAKPOINT_PROBE_IDS, BEHAVIOR_PACKET_SCHEMA, PINNED_SOURCE_SHA } from './registry.mjs';
import { DYNAMIC_REGIONS } from '../behavioral-requirements.mjs';

const SAFE_ID=/^behavior-packet\.[a-z0-9][a-z0-9-]{2,100}$/u;
const SENSITIVE=/(?:authorization|bearer\s|password|client[_-]?secret|access[_-]?token|api[_-]?key|sb_(?:secret|publishable)|\/_review\/|https?:\/\/[^/\s]*:[^@\s]*@)/iu;
export const stableHash=(value)=>{
  const stable=(item)=>Array.isArray(item)?item.map(stable):item&&typeof item==='object'?Object.fromEntries(Object.keys(item).sort().map((key)=>[key,stable(item[key])])):item;
  return createHash('sha256').update(JSON.stringify(stable(value))).digest('hex');
};
function safeTree(value,path='$'){
  if(typeof value==='string'&&SENSITIVE.test(value)) throw new Error(`Sensitive behavioral evidence at ${path}`);
  if(!value||typeof value!=='object') return;
  for(const [key,child] of Object.entries(value)){
    if(/^(?:innerhtml|outerhtml|full_html|authorization|access_token)$/iu.test(key)) throw new Error(`Unsafe behavioral field ${path}.${key}`);
    safeTree(child,`${path}.${key}`);
  }
}
export function assertBehaviorPacketRegistry(registry){
  if(registry.schema_version!==BEHAVIOR_PACKET_SCHEMA||registry.source_sha!==PINNED_SOURCE_SHA) throw new Error('Behavior registry identity mismatch');
  if(!Array.isArray(registry.plans)||registry.plans.length!==67) throw new Error('Behavior registry must contain the corrected 67-packet matrix');
  const ids=new Set();
  for(const row of registry.plans){
    if(!SAFE_ID.test(row.id)||ids.has(row.id)) throw new Error(`Invalid or duplicate packet id: ${row.id}`); ids.add(row.id);
    if(row.source_sha!==PINNED_SOURCE_SHA||row.production_state_claimed!==false||row.decision!=='NOT_MERGED'||row.normalization_allowed!==false) throw new Error(`Behavior STOP boundary missing: ${row.id}`);
    if(!['candidate-runtime','controlled-exact-component-runtime','exact-blocker'].includes(row.reachability)) throw new Error(`Behavior reachability missing: ${row.id}`);
    if(!Array.isArray(row.dynamic_region_ids||[])||!Array.isArray(row.breakpoint_probe_ids)||!Array.isArray(row.coverage_refs)||!row.coverage_refs.length) throw new Error(`Behavior coverage refs missing: ${row.id}`);
    if(row.execution_status==='explicit-blocker'){
      if(row.steps.length||!row.blocker_reason||row.review_status!=='not-applicable-no-raster') throw new Error(`Invalid explicit blocker: ${row.id}`);
    }else{
      if(!row.route?.startsWith('/')||!row.root_selector||!row.steps?.length||!row.viewport?.width||!row.viewport?.height) throw new Error(`Incomplete executable packet: ${row.id}`);
      for(const step of row.steps){
        if(!step.expect?.root_observed) throw new Error(`Behavior phase has no root assertion: ${row.id}/${step.phase}`);
        if(step.actions.length&&!step.expect.semantic_delta) throw new Error(`Behavior action phase has no semantic-delta assertion: ${row.id}/${step.phase}`);
        for(const action of step.actions){
          if(action.optional) throw new Error(`Optional no-op action forbidden: ${row.id}/${step.phase}`);
          if(!['required-element','required-active-element','required-viewport','not-applicable-controlled-runtime'].includes(action.target_requirement)) throw new Error(`Action target contract missing: ${row.id}/${step.phase}/${action.kind}`);
          if(action.target_requirement==='required-element'&&!action.selector&&!['focus','focus-scroll-target','scroll-element'].includes(action.kind)) throw new Error(`Required action selector missing: ${row.id}/${step.phase}/${action.kind}`);
        }
      }
      if(row.family==='media'&&!row.media_provenance) throw new Error(`Media OCR/photo provenance missing: ${row.id}`);
    }
  }
  for(const ratio of ['4:5','5:4']) if(!registry.plans.some((row)=>row.ratios?.includes(ratio)&&row.execution_status!=='explicit-blocker')) throw new Error(`Missing executable ${ratio} packet`);
  for(const treatment of ['departure_board_v1','route_strips_v1','next_departure_queue_v1']){
    const rows=registry.plans.filter((row)=>row.id.includes(`transport-${treatment.replaceAll('_','-')}`));
    if(rows.length!==2) throw new Error(`Incomplete transport packet pair: ${treatment}`);
    const disclosure=rows.find((row)=>row.id.endsWith('-disclosure'));const open=disclosure?.steps.find((row)=>row.phase==='compact-open');
    if(disclosure?.fixture_provenance?.minimum_options<4||open?.actions?.some((action)=>action.optional)||!open?.expect?.details_open) throw new Error(`Transport disclosure is not truthfully actionable: ${treatment}`);
  }
  const rail=registry.plans.find((row)=>row.id==='behavior-packet.rail-keyboard-home-end');
  if(rail?.execution_status!=='explicit-blocker'||rail?.blocks_ready!==true||rail?.runtime_probe?.observed_scroll_left!==0) throw new Error('Rail keyboard End/Home exact blocker missing');
  const stickyWeekend=registry.plans.find((row)=>row.id==='behavior-packet.sticky-weekend-nav');
  if(!stickyWeekend?.visible_root_required||stickyWeekend.steps.some((row)=>row.expect?.root_geometry!=='nonzero')) throw new Error('Sticky weekend visible nonzero geometry contract missing');
  const home=registry.plans.find((row)=>row.id==='behavior-packet.home-static-to-local-rerank');
  if(home?.screenshot_stabilization?.images!=='eager-complete-bounded'||home?.screenshot_stabilization?.timeout_ms!==30000) throw new Error('Home lazy image stabilization contract missing');
  const dynamicCovered=new Set(registry.plans.flatMap((row)=>row.dynamic_region_ids||[]));
  for(const region of DYNAMIC_REGIONS)if(!dynamicCovered.has(region.id))throw new Error(`Dynamic region lacks runtime packet or exact blocker: ${region.id}`);
  const breakpointCovered=new Set(registry.plans.flatMap((row)=>row.breakpoint_probe_ids||[]));
  for(const id of ALL_BREAKPOINT_PROBE_IDS)if(!breakpointCovered.has(id))throw new Error(`Breakpoint/container probe lacks packet or exact blocker: ${id}`);
  if(ALL_BREAKPOINT_PROBE_IDS.length!==293||breakpointCovered.size!==293)throw new Error('Breakpoint/container reconciliation must cover exact 293-row matrix');
  const breakpointGap=registry.plans.find((row)=>row.id==='behavior-packet.breakpoint-container-runtime-coverage-gap');
  if(breakpointGap?.blocks_ready!==true)throw new Error('Unobserved breakpoint/container matrix must block READY');
  safeTree(registry); return true;
}
export function assertBehaviorObservation(row){
  if(row.schema_version!==BEHAVIOR_PACKET_SCHEMA||!SAFE_ID.test(row.plan_id)||row.source_sha!==PINNED_SOURCE_SHA) throw new Error('Behavior observation identity mismatch');
  if(row.production_state_claimed!==false||row.normalization_allowed!==false||row.evidence_status!=='captured-not-reviewed') throw new Error(`Behavior observation boundary missing: ${row.id}`);
  if(!row.screenshot||!/^[a-f0-9]{64}$/u.test(row.screenshot.sha256||'')||!/^[a-f0-9]{16}$/u.test(row.screenshot.dhash||'')) throw new Error(`Behavior raster hash missing: ${row.id}`);
  if(row.dom?.full_html_retained!==false||row.network?.raw_urls_retained!==false) throw new Error(`Unbounded behavioral observation: ${row.id}`);
  if(!['candidate-runtime','controlled-exact-component-runtime'].includes(row.reachability)||!Array.isArray(row.dynamic_region_ids)||!Array.isArray(row.breakpoint_probe_ids)||!Array.isArray(row.coverage_refs)) throw new Error(`Behavior observation reachability/coverage missing: ${row.id}`);
  if(row.transition?.assertions_passed!==true||!['ready','timed-out-continued'].includes(row.font_settle?.status)) throw new Error(`Behavior observation semantic/timeout receipt missing: ${row.id}`);
  if(!Array.isArray(row.action_receipts)||row.action_receipts.some((item)=>item.result!=='applied'||!item.target?.status)) throw new Error(`Behavior observation action receipt invalid: ${row.id}`);
  safeTree(row); return true;
}
export function assertBehaviorCaptureComplete(registry,observations,blockers){
  assertBehaviorPacketRegistry(registry);
  const planMap=new Map(registry.plans.map((row)=>[row.id,row]));const byPlan=new Map(); for(const row of observations){assertBehaviorObservation(row);const plan=planMap.get(row.plan_id);if(!plan||row.reachability!==plan.reachability||stableHash(row.dynamic_region_ids)!==stableHash(plan.dynamic_region_ids||[])||stableHash(row.breakpoint_probe_ids)!==stableHash(plan.breakpoint_probe_ids||[]))throw new Error(`Behavior observation coverage drift: ${row.id}`);if(plan.screenshot_stabilization?.images==='eager-complete-bounded'&&(row.image_settle?.status!=='settled'||row.image_settle?.complete_count!==row.image_settle?.image_count))throw new Error(`Behavior image stabilization receipt missing: ${row.id}`);if(!byPlan.has(row.plan_id))byPlan.set(row.plan_id,new Set());byPlan.get(row.plan_id).add(row.phase);}
  const blockerIds=new Set((blockers||[]).map((row)=>row.plan_id));
  for(const plan of registry.plans){
    if(plan.execution_status==='explicit-blocker'){if(!blockerIds.has(plan.id)) throw new Error(`Missing exact blocker receipt: ${plan.id}`);continue;}
    const phases=byPlan.get(plan.id)||new Set();for(const phase of plan.steps.filter((row)=>row.capture).map((row)=>row.phase))if(!phases.has(phase))throw new Error(`Missing captured phase: ${plan.id}/${phase}`);
  }
  return true;
}
