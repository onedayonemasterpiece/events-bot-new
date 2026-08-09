import { createHash } from 'node:crypto';
import { BEHAVIOR_PACKET_SCHEMA, PINNED_SOURCE_SHA } from './registry.mjs';

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
  if(!Array.isArray(registry.plans)||registry.plans.length!==50) throw new Error('Behavior registry must contain the reviewed 50-packet matrix');
  const ids=new Set();
  for(const row of registry.plans){
    if(!SAFE_ID.test(row.id)||ids.has(row.id)) throw new Error(`Invalid or duplicate packet id: ${row.id}`); ids.add(row.id);
    if(row.source_sha!==PINNED_SOURCE_SHA||row.production_state_claimed!==false||row.decision!=='NOT_MERGED'||row.normalization_allowed!==false) throw new Error(`Behavior STOP boundary missing: ${row.id}`);
    if(row.execution_status==='explicit-blocker'){
      if(row.steps.length||!row.blocker_reason||row.review_status!=='not-applicable-no-raster') throw new Error(`Invalid explicit blocker: ${row.id}`);
    }else if(!row.route?.startsWith('/')||!row.root_selector||!row.steps?.length||!row.viewport?.width||!row.viewport?.height) throw new Error(`Incomplete executable packet: ${row.id}`);
  }
  for(const ratio of ['4:5','5:4']) if(!registry.plans.some((row)=>row.ratios?.includes(ratio)&&row.execution_status!=='explicit-blocker')) throw new Error(`Missing executable ${ratio} packet`);
  for(const treatment of ['departure_board_v1','route_strips_v1','next_departure_queue_v1']){
    const rows=registry.plans.filter((row)=>row.id.includes(`transport-${treatment.replaceAll('_','-')}`));
    if(rows.length!==2) throw new Error(`Incomplete transport packet pair: ${treatment}`);
  }
  safeTree(registry); return true;
}
export function assertBehaviorObservation(row){
  if(row.schema_version!==BEHAVIOR_PACKET_SCHEMA||!SAFE_ID.test(row.plan_id)||row.source_sha!==PINNED_SOURCE_SHA) throw new Error('Behavior observation identity mismatch');
  if(row.production_state_claimed!==false||row.normalization_allowed!==false||row.evidence_status!=='captured-not-reviewed') throw new Error(`Behavior observation boundary missing: ${row.id}`);
  if(!row.screenshot||!/^[a-f0-9]{64}$/u.test(row.screenshot.sha256||'')||!/^[a-f0-9]{16}$/u.test(row.screenshot.dhash||'')) throw new Error(`Behavior raster hash missing: ${row.id}`);
  if(row.dom?.full_html_retained!==false||row.network?.raw_urls_retained!==false) throw new Error(`Unbounded behavioral observation: ${row.id}`);
  safeTree(row); return true;
}
export function assertBehaviorCaptureComplete(registry,observations,blockers){
  assertBehaviorPacketRegistry(registry);
  const byPlan=new Map(); for(const row of observations){assertBehaviorObservation(row); if(!byPlan.has(row.plan_id))byPlan.set(row.plan_id,new Set());byPlan.get(row.plan_id).add(row.phase);}
  const blockerIds=new Set((blockers||[]).map((row)=>row.plan_id));
  for(const plan of registry.plans){
    if(plan.execution_status==='explicit-blocker'){if(!blockerIds.has(plan.id)) throw new Error(`Missing exact blocker receipt: ${plan.id}`);continue;}
    const phases=byPlan.get(plan.id)||new Set();for(const phase of plan.steps.filter((row)=>row.capture).map((row)=>row.phase))if(!phases.has(phase))throw new Error(`Missing captured phase: ${plan.id}/${phase}`);
  }
  return true;
}
