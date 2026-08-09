import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { pngDifferenceHash, startSpecimenServer } from '../specimens/capture.mjs';
import { PINNED_SOURCE_SHA } from './registry.mjs';
import { BREAKPOINT_PROBE_SCHEMA, assertBreakpointProbeClosure, assertBreakpointProbeRecord, stableProbeJson } from './probe-validate.mjs';

const sha=(value)=>createHash('sha256').update(value).digest('hex');
const wait=(ms)=>new Promise((done)=>setTimeout(done,ms));
const round=(value)=>Math.round(Number(value)*1000)/1000;
const safeReason=(error)=>String(error?.message||error||'unknown').replace(/https?:\/\/\S+/gu,'[redacted-url]').slice(0,500);
export const selectBreakpointRasterReason=(record)=>record.terminal_status==='MISMATCH'?'mismatch-terminal':record.selector_cascade_evidence?.cascade_reconciliation==='ambiguous'?'ambiguous-cascade':null;

// The exact source loads optional remote fonts. Playwright 1.61 otherwise
// waits for document.fonts.ready inside screenshot() after our probe facts are
// already terminal, which made the first Actions raster time out. Capture the
// actually rendered fallback font instead, matching the bounded behavioral
// packet capture policy in ./capture.mjs.
process.env.PW_TEST_SCREENSHOT_NO_FONTS_READY='1';

async function installDeterministicRuntime(context){
  await context.addInitScript(({fixedNow})=>{
    const NativeDate=Date;class FixedDate extends NativeDate{constructor(...args){super(...(args.length?args:[fixedNow]));}static now(){return fixedNow;}}Object.setPrototypeOf(FixedDate,NativeDate);globalThis.Date=FixedDate;
    let state=0x6d2b79f5;Math.random=()=>{state=(Math.imul(state^state>>>15,1|state)+Math.imul(state^state>>>7,61|state)^state)>>>0;return((state^state>>>14)>>>0)/4294967296;};
  },{fixedNow:Date.parse('2026-08-08T12:48:42.000Z')});
}

function sourceProof(plan){return {sha:plan.source_sha,path:plan.path,content_sha256:plan.source_sha256,line:plan.line,offset:plan.source_offset,at_rule_ordinal:plan.at_rule_ordinal,at_rule_kind_ordinal:plan.at_rule_kind_ordinal,at_rule_fingerprint:plan.at_rule_fingerprint,rule_fingerprint:plan.rule_fingerprint,kind:plan.kind,query:plan.query,container_name:plan.container_name||null,affected_selectors:plan.affected_selectors.slice(0,40),affected_declarations:plan.affected_declarations.slice(0,80)};}
function baseRecord(plan,consumer){return {schema_version:BREAKPOINT_PROBE_SCHEMA,id:plan.id,source:sourceProof(plan),component:consumer?.component||plan.contract_id,consumer:consumer?.consumer||consumer?.component||plan.contract_id,consumer_set:(plan.consumers||[]).map((item)=>({component:item.component,consumer:item.consumer||item.component,route:item.route,root_selector:item.root_selector})),route:consumer?.route||'source-route-unavailable',root_selector:consumer?.root_selector||null,threshold_px:plan.threshold_px??null,probe_px:plan.probe_px??null,probe:plan.probe,axis:plan.axis,condition_features:plan.condition_features,environment:plan.environment,expected_branch:Boolean(plan.environment.expected_branch),actual_branch:null,runtime_marker:null,root_bbox:null,affected_target_bbox:null,computed_styles:[],overflow_clipping:null,visibility:null,selector_cascade_evidence:null,raster:{selected:false,selection_reason:'bounded-numeric-runtime-facts-sufficient'},production_state_claimed:false,normalization_allowed:false,decision:'NOT_MERGED'};}
export function createUnreachableProbeRecord(plan,consumer,code,detail,attempts=[]){const row={...baseRecord(plan,consumer),terminal_status:'UNREACHABLE_WITH_REASON',terminal_reason:`${code}: ${detail}`,unreachable:{reason_code:code,detail},consumer_attempts:attempts,raster:{selected:false,selection_reason:'unreachable-has-no-runtime-raster'}};assertBreakpointProbeRecord(row);return row;}

async function setContainerTarget(root,environment){
  if(!environment.container?.width&&!environment.container?.height)return {applied:false};
  return root.evaluate((node,target)=>{
    const before=getComputedStyle(node);const original={style:node.getAttribute('style'),boxSizing:before.boxSizing,containerName:before.containerName,containerType:before.containerType};
    node.style.boxSizing='content-box';
    if(target.width!==null){node.style.width=`${target.width}px`;node.style.minWidth=`${target.width}px`;node.style.maxWidth=`${target.width}px`;}
    if(target.height!==null){node.style.height=`${target.height}px`;node.style.minHeight=`${target.height}px`;node.style.maxHeight=`${target.height}px`;}
    const after=getComputedStyle(node);const contentWidth=node.clientWidth-parseFloat(after.paddingLeft||'0')-parseFloat(after.paddingRight||'0');const contentHeight=node.clientHeight-parseFloat(after.paddingTop||'0')-parseFloat(after.paddingBottom||'0');
    return {applied:true,instrumentation:'test-only-inline-content-box-sizing',requested:{width:target.width,height:target.height,name:target.name},original,actual:{content_width:contentWidth,content_height:contentHeight,container_name:after.containerName,container_type:after.containerType}};
  },environment.container);
}

async function collectCssomEvidence(page,root,plan){
  const handle=await root.elementHandle();
  const result=await page.evaluate(({root,probe})=>{
    const normalize=(value)=>String(value||'').replace(/\/\*[\s\S]*?\*\//gu,' ').replace(/\s+/gu,' ').trim().replace(/\s*([:;,(){}])\s*/gu,'$1').toLowerCase();
    const box=(node)=>{if(!node)return null;const r=node.getBoundingClientRect();return {x:Math.round(r.x*1000)/1000,y:Math.round(r.y*1000)/1000,width:Math.round(r.width*1000)/1000,height:Math.round(r.height*1000)/1000,top:Math.round(r.top*1000)/1000,right:Math.round(r.right*1000)/1000,bottom:Math.round(r.bottom*1000)/1000,left:Math.round(r.left*1000)/1000};};
    const visible=(node)=>{const r=node.getBoundingClientRect(),s=getComputedStyle(node);return !node.hidden&&s.display!=='none'&&s.visibility!=='hidden'&&Number(s.opacity)!==0&&r.width>0&&r.height>0;};
    const cleanSelector=(selector)=>selector.replace(/::[\w-]+(?:\([^)]*\))?/gu,'').replace(/:(?:hover|active|focus|focus-visible|focus-within|visited)(?![\w-])/gu,'');
    const targetsFor=(selector)=>{try{const cleaned=cleanSelector(selector);const found=[...document.querySelectorAll(cleaned)];return found.filter((node)=>root===document.body||node===root||root.contains(node)).slice(0,8);}catch{return [];}};
    const sourceDeclarations=new Map();for(const item of probe.affected_declarations||[]){if(!sourceDeclarations.has(item.property))sourceDeclarations.set(item.property,[]);sourceDeclarations.get(item.property).push({value:normalize(item.value.replace(/\s*!important\s*$/iu,'')),selector:item.selector});}
    const selectorTokens=(selector)=>new Set(String(selector||'').match(/[.#][a-z_][\w-]*|\[data-[\w-]+/giu)||[]);const selectorRelated=(compiledSelector,sourceSelector)=>{const left=selectorTokens(compiledSelector),right=selectorTokens(sourceSelector);if(!right.size)return true;return [...right].some((token)=>left.has(token));};
    const shorthandPrefixes={transition:'transition-',animation:'animation-',padding:'padding-',margin:'margin-',border:'border-',background:'background-',font:'font-',flex:'flex-',grid:'grid-',outline:'outline-',columns:'column-',inset:'inset-'};const propertyRelated=(compiledProperty,sourceProperty)=>compiledProperty===sourceProperty||Boolean(shorthandPrefixes[sourceProperty]&&compiledProperty.startsWith(shorthandPrefixes[sourceProperty]));
    const compiled=[];const allMatched=[];let order=0;
    const visit=(rules,groups=[])=>{for(const rule of [...(rules||[])]){order+=1;const ctor=rule.constructor?.name||'';const condition=rule.conditionText||null;const name=rule.name||null;const groupKind=/CSSMediaRule/u.test(ctor)?'media':/CSSContainerRule/u.test(ctor)?'container':null;const nextGroups=groupKind?[...groups,{kind:groupKind,condition,name}]:groups;
      if(rule.selectorText&&rule.style){const properties=[...rule.style].map((property)=>({property,value:rule.style.getPropertyValue(property).trim(),priority:rule.style.getPropertyPriority(property)||null}));const targetNodes=targetsFor(rule.selectorText);if(targetNodes.length){allMatched.push({selector:rule.selectorText,properties,order,groups:nextGroups,targetNodes});}
        const exactGroup=nextGroups.findLast((group)=>group.kind===probe.kind&&normalize(group.condition)===normalize(probe.kind==='container'?probe.query:probe.condition_query));
        // CSSOM canonicalizes shorthands (notably transition and animation),
        // so exact-source matching binds condition + property + selector token;
        // the original and compiled values are both retained for cascade proof.
        const overlap=properties.filter((item)=>[...sourceDeclarations.entries()].some(([sourceProperty,sources])=>propertyRelated(item.property,sourceProperty)&&sources.some((source)=>selectorRelated(rule.selectorText,source.selector))));
        if(exactGroup&&overlap.length)compiled.push({selector:rule.selectorText,properties:overlap,all_properties:properties.slice(0,24),order,condition:exactGroup.condition,container_name:exactGroup.name||null,targetNodes});
      }
      if(rule.cssRules)visit(rule.cssRules,nextGroups);
    }};
    for(const sheet of [...document.styleSheets]){try{visit(sheet.cssRules,[]);}catch{}}
    const resolved=[];for(const item of compiled){for(const node of item.targetNodes){const style=getComputedStyle(node);resolved.push({selector:item.selector,order:item.order,target:node,properties:item.properties.map((decl)=>({property:decl.property,declared:decl.value,computed:style.getPropertyValue(decl.property).trim(),priority:decl.priority}))});}}
    const marker=document.createElement('i');marker.hidden=true;marker.dataset.currentUiProbeMarker=probe.id;root.append(marker);const markerStyle=document.createElement('style');const markerSelector=`[data-current-ui-probe-marker="${CSS.escape(probe.id)}"]`;markerStyle.textContent=probe.kind==='container'?`@container ${probe.container_name||''} ${probe.condition_query}{${markerSelector}{--current-ui-probe-branch:active}}`:`@media ${probe.condition_query}{${markerSelector}{--current-ui-probe-branch:active}}`;document.head.append(markerStyle);const markerValue=getComputedStyle(marker).getPropertyValue('--current-ui-probe-branch').trim();
    const actualBranch=probe.kind==='media'?matchMedia(probe.condition_query).matches:markerValue==='active';
    const rootStyle=getComputedStyle(root);const first=resolved[0]?.target||null;const targetStyle=first?getComputedStyle(first):null;
    const computed=resolved.flatMap((item)=>item.properties.map((decl)=>({selector:item.selector,property:decl.property,declared:decl.declared,computed:decl.computed,priority:decl.priority}))).slice(0,24);
    const winning=computed.some((item)=>normalize(item.declared)===normalize(item.computed));
    // A cascade candidate must match the exact affected target. Treating a
    // rule that merely matched some other descendant as an override made this
    // evidence depend on unrelated asynchronous image-state changes.
    const overrideCandidates=[];if(first){for(const rule of allMatched.filter((item)=>item.targetNodes.includes(first))){for(const decl of rule.properties){if(sourceDeclarations.has(decl.property)&&!compiled.some((item)=>item.selector===rule.selector&&item.order===rule.order))overrideCandidates.push({selector:rule.selector,property:decl.property,value:decl.value,priority:decl.priority,order:rule.order});}}}
    const reconciliation=winning?'winning':!actualBranch&&resolved.length?'attributed-consumer-override':overrideCandidates.length?'attributed-consumer-override':'unreconciled';
    const rr=box(root),tr=box(first);const viewport={left:0,top:0,right:innerWidth,bottom:innerHeight};const intersection=tr?Math.max(0,Math.min(tr.right,viewport.right)-Math.max(tr.left,viewport.left))*Math.max(0,Math.min(tr.bottom,viewport.bottom)-Math.max(tr.top,viewport.top)):0;
    const resolvedCount=new Set(resolved.map((item)=>item.target)).size;
    const result={actual_branch:actualBranch,runtime_marker:{kind:probe.kind==='media'?'native-match-media-plus-exact-cssom':'synthetic-container-condition-plus-exact-cssom-and-cascade',condition:probe.condition_query,value:markerValue||null,match_media:probe.kind==='media'?actualBranch:null,synthetic_supporting_only:probe.kind==='container'},root_bbox:rr||{unavailable:true,reason:'root-had-no-client-rect'},affected_target_bbox:tr||{unavailable:true,reason:'no-affected-selector-resolved'},computed_styles:computed,overflow_clipping:{root:{overflow_x:rootStyle.overflowX,overflow_y:rootStyle.overflowY,scroll_width:root.scrollWidth,scroll_height:root.scrollHeight,client_width:root.clientWidth,client_height:root.clientHeight,clips_x:['hidden','clip'].includes(rootStyle.overflowX),clips_y:['hidden','clip'].includes(rootStyle.overflowY)},target:targetStyle&&tr?{overflow_x:targetStyle.overflowX,overflow_y:targetStyle.overflowY,clipped_by_viewport:tr.left<0||tr.top<0||tr.right>innerWidth||tr.bottom>innerHeight}:{unavailable:true,reason:'no-affected-selector-resolved'}},visibility:{root:visible(root),affected_target:first?visible(first):false,affected_target_focusable:first?first.matches('a[href],button,input,select,textarea,[tabindex]:not([tabindex="-1"])'):false,affected_target_focused:first?document.activeElement===first:false,affected_target_focus_visible:first?first.matches(':focus-visible'):false,intersection_area:intersection},selector_cascade_evidence:{exact_compiled_cssom_rule:compiled.length>0,real_exact_source_consumer:true,compiled_rule_count:compiled.length,compiled_condition:probe.kind==='container'?probe.query:probe.condition_query,compiled_selectors:[...new Set(compiled.map((item)=>item.selector))].slice(0,24),compiled_rule_signatures:compiled.slice(0,24).map((item)=>JSON.stringify([item.condition,item.container_name,item.selector,item.properties.map((decl)=>[decl.property,decl.value,decl.priority])])),affected_selector_resolved_count:resolvedCount,affected_selector_resolved:resolvedCount>0,cascade_reconciliation:reconciliation,override_candidates:overrideCandidates.slice(-12),source_selector_count:(probe.affected_selectors||[]).length,source_declaration_count:(probe.affected_declarations||[]).length}};
    markerStyle.remove();marker.remove();return result;
  },{root:handle,probe:{id:plan.id,kind:plan.kind,query:plan.query,condition_query:plan.condition_query,container_name:plan.container_name,affected_selectors:plan.affected_selectors,affected_declarations:plan.affected_declarations}});result.selector_cascade_evidence.compiled_rule_fingerprints=result.selector_cascade_evidence.compiled_rule_signatures.map((value)=>sha(value));delete result.selector_cascade_evidence.compiled_rule_signatures;return result;
}

async function attemptConsumer({browser,baseUrl,plan,consumer}){
  const context=await browser.newContext({viewport:plan.environment.viewport,reducedMotion:plan.environment.media.reduced_motion,hasTouch:plan.environment.media.has_touch,deviceScaleFactor:1,timezoneId:'Europe/Kaliningrad'});await installDeterministicRuntime(context);
  if(plan.environment.resource_control?.image_requests==='held-during-observation')await context.route('**/*',(route)=>route.request().resourceType()==='image'?undefined:route.continue());
  const page=await context.newPage();page.setDefaultTimeout(8000);
  let keepOpen=false;
  try{
    const response=await page.goto(`${baseUrl}${consumer.route}`,{waitUntil:'domcontentloaded',timeout:20000});if(!response||response.status()>=400)return {kind:'unreachable',code:'ROUTE_HTTP_ERROR',detail:`route returned ${response?.status()??'no-response'}`,consumer};
    // External media/fonts are intentionally not required for numeric CSSOM
    // evidence. DOMContentLoaded plus an exact root wait is deterministic and
    // avoids adding a fixed network-idle timeout to each of 293 probes.
    await page.waitForTimeout(50);const roots=page.locator(consumer.root_selector);if(await roots.count()<1)return {kind:'unreachable',code:'ROOT_SELECTOR_MISSING',detail:`exact route has no ${consumer.root_selector}`,consumer};
    const root=roots.filter({visible:true}).first();const selected=await root.count()?root:roots.first();await selected.waitFor({state:'attached',timeout:5000});
    if(plan.environment.resource_control?.image_requests==='held-during-observation')await selected.locator('[data-image-state="loading"]').first().waitFor({state:'attached',timeout:5000});
    const container_receipt=plan.kind==='container'?await setContainerTarget(selected,plan.environment):{applied:false};await page.evaluate(()=>new Promise((done)=>requestAnimationFrame(()=>requestAnimationFrame(done))));
    const facts=await collectCssomEvidence(page,selected,plan);const mismatch=[];if(facts.actual_branch!==plan.environment.expected_branch)mismatch.push('runtime branch differs from planned source condition');if(!facts.selector_cascade_evidence.exact_compiled_cssom_rule)mismatch.push('exact compiled CSSOM rule not reconciled');if(facts.selector_cascade_evidence.affected_selector_resolved_count<1)mismatch.push('no affected compiled selector resolved inside exact consumer');if(!['winning','attributed-consumer-override'].includes(facts.selector_cascade_evidence.cascade_reconciliation))mismatch.push('cascade is unreconciled');
    keepOpen=true;return {kind:'runtime',consumer,container_receipt,facts,terminal_status:mismatch.length?'MISMATCH':'PASS',reasons:mismatch,page,context,root:selected};
  }catch(error){return {kind:'unreachable',code:'RUNTIME_NAVIGATION_OR_EVALUATION_ERROR',detail:safeReason(error),consumer};}
  finally{if(!keepOpen)await context.close();}
}
async function closeAttempt(attempt){if(attempt?.kind==='runtime')await attempt.context.close();}

async function captureBoundedRaster(attempt,outputDir,id,reason){
  const box=attempt.facts.affected_target_bbox?.width?attempt.facts.affected_target_bbox:attempt.facts.root_bbox;if(!box?.width||!box?.height)return {selected:false,selection_reason:'selected-condition-had-no-nonzero-box'};
  const viewport=attempt.page.viewportSize();const clip={x:Math.max(0,box.x),y:Math.max(0,box.y),width:Math.min(800,viewport.width-Math.max(0,box.x),box.width),height:Math.min(600,viewport.height-Math.max(0,box.y),box.height)};if(clip.width<1||clip.height<1)return {selected:false,selection_reason:'selected-condition-outside-viewport'};
  const bytes=await attempt.page.screenshot({type:'png',animations:'disabled',caret:'hide',scale:'css',clip});const name=`${id}.png`;const path=join(resolve(outputDir),'breakpoint-probe-rasters',name);mkdirSync(dirname(path),{recursive:true});writeFileSync(path,bytes);return {selected:true,selection_reason:reason,path:`breakpoint-probe-rasters/${name}`,bytes:bytes.length,sha256:sha(bytes),dhash:pngDifferenceHash(bytes),clip};
}

function recordFromAttempt(plan,attempt,consumerAttempts){
  const coverage={policy:plan.consumer_coverage_policy,required_count:plan.consumers.length,attempted_count:consumerAttempts.length,root_resolved_count:consumerAttempts.filter((item)=>item.root_resolved).length,complete:plan.consumer_coverage_policy!=='all-mapped-consumers-root-required'||(consumerAttempts.length===plan.consumers.length&&consumerAttempts.every((item)=>item.root_resolved))};const coverageMismatch=attempt.terminal_status==='PASS'&&!coverage.complete;const reasons=coverageMismatch?['not every required exact consumer root resolved']:attempt.reasons;const terminalStatus=coverageMismatch?'MISMATCH':attempt.terminal_status;const reason=terminalStatus==='PASS'?'expected branch and exact compiled source cascade reconciled':reasons.join('; ');
  const row={...baseRecord(plan,attempt.consumer),...attempt.facts,container_receipt:attempt.container_receipt,consumer_set_coverage:coverage,terminal_status:terminalStatus,terminal_reason:reason,unreachable:null,consumer_attempts:consumerAttempts,raster:{selected:false,selection_reason:'bounded-numeric-runtime-facts-sufficient'}};return row;
}

export async function executeBreakpointProbePlans({browser,baseUrl,plans,outputDir=null,maxRasters=24,onlyIds=null}){
  const selected=plans.filter((row)=>!onlyIds||onlyIds.includes(row.id)).sort((a,b)=>a.id.localeCompare(b.id));const records=[];const rasterSignatures=new Map();let rasterCount=0;
  for(let index=0;index<selected.length;index+=1){const plan=selected[index];process.stderr.write(`[breakpoint-probe] ${index+1}/${selected.length} ${plan.id}\n`);const attempts=[];let best=null;
    if(!plan.consumers.length){records.push(createUnreachableProbeRecord(plan,null,'NO_EXACT_CONSUMER_MAPPING',`no exact route/root mapping exists for ${plan.path}`));continue;}
    for(const consumer of plan.consumers){const attempt=await attemptConsumer({browser,baseUrl,plan,consumer});attempts.push(attempt.kind==='runtime'?{consumer:attempt.consumer,terminal_status:attempt.terminal_status,reasons:attempt.reasons,actual_branch:attempt.facts.actual_branch,root_resolved:true,affected_selector_resolved_count:attempt.facts.selector_cascade_evidence.affected_selector_resolved_count}:{consumer:attempt.consumer,terminal_status:'UNREACHABLE_WITH_REASON',reason_code:attempt.code,detail:attempt.detail,root_resolved:false});if(attempt.kind==='runtime'&&(!best||attempt.terminal_status==='PASS')){if(best)await closeAttempt(best);best=attempt;if(attempt.terminal_status==='PASS'&&plan.path!=='src/components/DesktopEventPage.astro')break;}else await closeAttempt(attempt);}
    if(!best){const first=attempts[0];records.push(createUnreachableProbeRecord(plan,plan.consumers[0],first?.reason_code||'ALL_CONSUMERS_UNREACHABLE',first?.detail||'all exact consumer attempts were unreachable',attempts));continue;}
    const row=recordFromAttempt(plan,best,attempts);const selection=selectBreakpointRasterReason(row);const signature=selection&&`${selection}\0${plan.path}\0${plan.query}\0${row.selector_cascade_evidence?.cascade_reconciliation}`;
    if(selection&&!outputDir)row.raster={selected:false,selection_reason:'eligible-but-output-directory-not-supplied'};
    else if(selection&&rasterSignatures.has(signature))row.raster={selected:false,selection_reason:'duplicate-visual-signature-covered-by-index',covered_by_probe_id:rasterSignatures.get(signature)};
    else if(selection&&rasterCount>=maxRasters)row.raster={selected:false,selection_reason:'bounded-raster-limit-reached'};
    else if(selection){row.raster=await captureBoundedRaster(best,outputDir,row.id,selection);if(row.raster.selected){rasterCount+=1;rasterSignatures.set(signature,row.id);}}
    assertBreakpointProbeRecord(row);records.push(row);await closeAttempt(best);
  }
  return records;
}

export function writeBreakpointProbeEvidence({outputDir,records,expectedIds=null,requireFullClosure=true}){
  const root=resolve(outputDir);mkdirSync(root,{recursive:true});const ordered=records.slice().sort((a,b)=>a.id.localeCompare(b.id));for(const row of ordered)assertBreakpointProbeRecord(row);const counts=requireFullClosure?assertBreakpointProbeClosure(ordered,{expectedIds}):{terminal:ordered.length,pass:ordered.filter((row)=>row.terminal_status==='PASS').length,mismatch:ordered.filter((row)=>row.terminal_status==='MISMATCH').length,unreachable:ordered.filter((row)=>row.terminal_status==='UNREACHABLE_WITH_REASON').length,rasters:ordered.filter((row)=>row.raster?.selected).length,blocks_ready:false};const body=`${ordered.map(stableProbeJson).join('\n')}\n`;writeFileSync(join(root,'breakpoint-probe-terminal.jsonl'),body);const rasterIndex=ordered.filter((row)=>row.raster?.selected).map((row)=>({probe_id:row.id,...row.raster}));writeFileSync(join(root,'breakpoint-probe-raster-index.json'),`${JSON.stringify({schema_version:BREAKPOINT_PROBE_SCHEMA,count:rasterIndex.length,max_policy:'bounded-unique-mismatch-or-ambiguous',entries:rasterIndex},null,2)}\n`);const receipt={schema_version:BREAKPOINT_PROBE_SCHEMA,status:'TERMINAL_EVIDENCE_COMPLETE',source_sha:PINNED_SOURCE_SHA,counts,records_sha256:sha(body),deterministic_scope:'semantic-terminal-excludes-raster-bytes-sha-and-aggregate-selector-counts',terminal_enum:['PASS','MISMATCH','UNREACHABLE_WITH_REASON'],planned_or_unconfirmed:0,blocks_ready:false,production_ui_mutated:false,normalization_allowed:false,decision:'NOT_MERGED'};writeFileSync(join(root,'breakpoint-probe-receipt.json'),`${JSON.stringify(receipt,null,2)}\n`);return receipt;
}

export async function executeBreakpointProbesWithExactPlaywright({nodeModules,dist,plans,outputDir,maxRasters=24,onlyIds=null,requireFullClosure=true}){
  const entry=join(resolve(nodeModules),'playwright/index.mjs');if(!existsSync(entry))throw new Error('Exact Playwright entrypoint missing');const {chromium}=await import(pathToFileURL(entry).href);const server=await startSpecimenServer({dist});const browser=await chromium.launch({headless:true});
  try{const records=await executeBreakpointProbePlans({browser,baseUrl:server.baseUrl,plans,outputDir,maxRasters,onlyIds});const receipt=writeBreakpointProbeEvidence({outputDir,records,expectedIds:requireFullClosure?plans.map((row)=>row.id):null,requireFullClosure});return {records,receipt};}finally{await browser.close();await server.close();}
}

export function verifyImmutablePriorSupplement({supplementRoot,expectedManifestSha256}){
  const path=join(resolve(supplementRoot),'manifest.json');if(!existsSync(path))throw new Error('Prior reviewed supplement manifest missing');const bytes=readFileSync(path);const actual=sha(bytes);if(actual!==expectedManifestSha256)throw new Error(`Prior supplement manifest hash mismatch: ${actual}`);const manifest=JSON.parse(bytes);if(manifest.source_sha!==PINNED_SOURCE_SHA||manifest.human_visual_review?.completed!==true)throw new Error('Prior supplement is not the pinned reviewed immutable base');return {path:'manifest.json',sha256:actual,source_sha:manifest.source_sha,supplement_id:manifest.supplement_id,reviewed_raster_count:manifest.human_visual_review.reviewed_raster_count,immutable:true};
}
