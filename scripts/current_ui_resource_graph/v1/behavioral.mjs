import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { basename, extname, join, relative, resolve } from 'node:path';
import { execFileSync } from 'node:child_process';
import { buildDynamicRegionMatrix, buildMediaPolicyMatrix, buildRequirementsProvenance } from './behavioral-requirements.mjs';
import { buildCuratedBehavioralHistoryRows, buildTransportExperimentRows } from './behavioral-experiments.mjs';

export const BEHAVIOR_SCHEMA = 'current_ui_behavioral_decoder_v1_1';
export const BEHAVIOR_STATUS = 'READY_FOR_PROJECT_NORMALIZATION_SYNTHESIS';
const PINNED_BEHAVIOR_SOURCE_SHA = 'ef7aa62e45c60f7a12da6160f490719c0721ec03';
export const REQUIRED_BEHAVIOR_FILES = Object.freeze([
  'behavior-contracts.jsonl','geometry-constraints.jsonl','media-behavior.jsonl','loading-recovery-states.jsonl',
  'interaction-state-machines.jsonl','positioning-sticky-fixed.jsonl','shelves-and-rails.jsonl','overlays-disclosures-selection.jsonl',
  'experiment-registry.jsonl','historical-variant-evidence.jsonl','breakpoint-and-container-matrix.jsonl','behavior-specimen-plan.jsonl',
  'behavior-specimen-observations.jsonl','behavior-page-verification.jsonl','unresolved.jsonl','audit-report.md','manifest.json','receipt.json',
  'requirements-provenance-ledger.jsonl','media-policy-matrix.jsonl','dynamic-region-loading-matrix.jsonl',
  'action-packet-index.jsonl','visual-review-ledger.jsonl','artifact-receipt.json',
  'artifact-index.json','independent-audit.json',
]);
const sha = (value) => createHash('sha256').update(value).digest('hex');
const stable = (value) => Array.isArray(value) ? value.map(stable) : value && typeof value === 'object' ? Object.fromEntries(Object.keys(value).sort().map((key) => [key, stable(value[key])])) : value;
const json = (value, pretty = false) => `${JSON.stringify(stable(value), null, pretty ? 2 : 0)}\n`;
const lineOf = (text, offset) => text.slice(0, offset).split('\n').length;
const sourceId = (path, content) => `behavior-source.${sha(`${path}\0${sha(content)}`).slice(0, 16)}`;
const words = (value) => value.replace(/[^a-z0-9]+/giu, '-').replace(/^-|-$/gu, '').toLowerCase();

const CATALOG = Object.freeze([
  ['event-hero','src/components/EventHero.astro','media','hero;photo;poster;fallback;crop;loading'],
  ['event-media-rail','src/components/EventMediaRail.astro','rail','gallery;preview;overflow;media'],
  ['event-card','src/components/EventCard.astro','media','card;media;fallback;loading'],
  ['desktop-event-page','src/components/DesktopEventPage.astro','media','event-detail;desktop;poster;photo;ocr;gallery;rail'],
  ['event-layout-media','src/layouts/EventLayout.astro','media','shared-layout;4:5;5:4;hero;card;skeleton'],
  ['listing-event-card','src/components/listings/ListingEventCard.astro','media','card;listing;srcset;crop;loading'],
  ['mobile-listing-rail-row','src/components/listings/MobileListingRailRow.astro','rail','mobile;rail;media;scroll'],
  ['mobile-listing-rail-surface','src/components/listings/MobileListingRailSurface.astro','rail','mobile;rail;calendar;sheet;scroll'],
  ['listing-discovery-rail','src/components/listings/ListingDiscoveryRail.astro','rail','discovery;rail;overflow'],
  ['listing-time-nav','src/components/listings/ListingTimeNav.astro','overlay','time;popover;selection;sticky'],
  ['listing-personal-filter','src/components/ListingPersonalFilter.astro','selection','filter;version;all;personal;floating'],
  ['reference4-mobile-menu','src/components/Reference4MobileMenu.astro','overlay','mobile;menu;dialog;focus'],
  ['mobile-bottom-nav','src/components/MobileBottomNav.astro','overlay','mobile;fixed;navigation'],
  ['mobile-search-bottom-nav','src/components/MobileSearchBottomNav.astro','overlay','mobile;dead-unreachable'],
  ['desktop-event-action-panel','src/components/DesktopEventActionPanel.astro','cta','desktop;event;action;horizontal;portrait'],
  ['event-cta-panel','src/components/EventCtaPanel.astro','cta','event;cta;loading;action'],
  ['design-system-button','src/components/design-system/Button.astro','cta','button;interactive;disabled'],
  ['calendar-link','src/components/CalendarLink.astro','cta','calendar;link;action'],
  ['transport-rail','src/components/EventTransportSchedule.astro','transport','rail;train;schedule;experiment'],
  ['transport-bus','src/components/EventBusTransportSchedule.astro','transport','bus;schedule;map'],
  ['transport-kaup','src/components/KaupTransportSchedule.astro','transport','kaup;schedule;experiment'],
  ['transport-experiment','src/components/transport/TransportTimetableExperiment.astro','experiment','transport;experiment;treatment'],
  ['medallions','src/components/EventTokenMedallions.astro','media','medallion;overflow;identity;media'],
  ['amber-artifact-rail','src/components/listings/AmberRailArtifact.astro','rail','artifact;rail;collect;motion'],
  ['artifact-collection','src/components/artifacts/ArtifactCollection.astro','overlay','artifact;collection;dialog;storage'],
  ['focus-egg-artifact','src/components/FocusEggArtifact.astro','selection','focus;egg;eligible;found'],
  ['focus-egg-saved-list','src/components/FocusEggSavedListDemo.astro','selection','focus;egg;saved;interaction'],
  ['authorized-event-search','src/components/AuthorizedEventSearch.astro','dynamic','search;skeleton;loading;result;empty;error;retry'],
  ['favorites-surface','src/components/FavoritesSurface.astro','dynamic','favorites;skeleton;loading;static-fallback;cloud'],
  ['personalization-runtime','src/components/personalization/PersonalizationRuntime.astro','dynamic','personalization;rerank;stale;static-fallback'],
  ['personal-feed-slot','src/components/PersonalFeedSlot.astro','dynamic','continuation;recommendations;empty;error'],
  ['weather-date-context','src/components/WeatherDateContext.astro','dynamic','weather;loading;unavailable'],
  ['exhibitions-personal-surface','src/components/ExhibitionsPersonalSurface.astro','dynamic','gallery;skeleton;loading;error'],
]);
const REACHABILITY = Object.freeze({
  'mobile-search-bottom-nav': 'dead-unreachable',
  'transport-experiment': 'experiment-off',
  'focus-egg-artifact': 'lab-only', 'focus-egg-saved-list': 'lab-only',
  'artifact-collection': 'controlled-specimen-only', 'amber-artifact-rail': 'controlled-specimen-only',
});
const INTERACTION_STATES = Object.freeze({
  overlay: ['closed','opening','open','inner-plane','scrollable','closing','focus-entry','focus-return','escape','tab','shift-tab','resize','short-viewport','reduced-motion'],
  rail: ['empty','one','many','overflow','edge-start','edge-end','touch-scroll','keyboard-alternative','focus-scroll','scroll-restoration','clipped'],
  selection: ['hidden','visible','all','personal','pointer','keyboard','focus','disabled'],
  cta: ['idle','hover','focus','pressed','disabled','unavailable','success'],
  transport: ['baseline','outbound','return','cutoff','qa-treatment','experiment-off'],
  media: ['intrinsic-known','rendered','lazy','eager','placeholder','missing','error-fallback'],
  dynamic: ['initial-html','idle','loading','partial','stale','refreshing','empty','error','retry','offline','unavailable','success'],
});

function files(root) {
  const output = []; const visit = (dir) => {
    for (const entry of readdirSync(dir, { withFileTypes: true }).sort((a,b) => a.name.localeCompare(b.name))) {
      const path = join(dir, entry.name); if (entry.isDirectory()) visit(path); else if (/\.(?:astro|css|ts|js|mjs)$/u.test(entry.name)) output.push(path);
    }
  }; visit(root); return output;
}
function selectorsWithDeclarations(path, text) {
  const rows = []; const re = /([^{}@][^{}]{0,320})\{([^{}]{1,6000})\}/gu;
  for (const match of text.matchAll(re)) {
    const selector = match[1].trim().replace(/\s+/gu, ' ').slice(0, 320); const body = match[2];
    for (const decl of body.matchAll(/([\w-]+)\s*:\s*([^;{}]{1,240})/gu)) rows.push({ path, selector, property: decl[1].toLowerCase(), value: decl[2].trim(), line: lineOf(text, match.index + decl.index) });
  }
  return rows;
}
function sourceFor(root, relativePath) {
  const path = join(root, relativePath); if (!existsSync(path)) return null; const content = readFileSync(path, 'utf8'); return { path: relativePath, content, id: sourceId(relativePath, content) };
}
function mediaRows(source, contract) {
  const rows = []; const declaration = selectorsWithDeclarations(source.path, source.content);
  for (const match of source.content.matchAll(/<(?:img|source|picture)\b[^>]*>/giu)) {
    const tag = match[0]; const attributes = Object.fromEntries([...tag.matchAll(/([:\w-]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+)))?/gu)].slice(1).map((m) => [m[1].toLowerCase(), m[2] ?? m[3] ?? m[4] ?? true]));
    if (!('src' in attributes || 'srcset' in attributes || tag.startsWith('<picture'))) continue;
    rows.push({ id:`media.${sha(`${source.id}\0${match.index}`).slice(0,16)}`, contract_id:contract.id, source_id:source.id, path:source.path, line:lineOf(source.content,match.index), element:tag.startsWith('<source')?'source':tag.startsWith('<picture')?'picture':'img', intrinsic_dimensions:{width:attributes.width||null,height:attributes.height||null}, responsive:{srcset:Boolean(attributes.srcset),sizes:attributes.sizes||null}, loading:attributes.loading||'browser-default', decoding:attributes.decoding||'browser-default', fetchpriority:attributes.fetchpriority||'browser-default', fallback:{alt:attributes.alt??null,source_marker:/fallback|placeholder|error|missing/iu.test(tag)?'source-marked':'not-explicit-in-element'}, reachability:contract.reachability, decision:'NOT_MERGED' });
  }
  for (const decl of declaration.filter((row) => ['object-fit','object-position','aspect-ratio','background','background-image','contain-intrinsic-size'].includes(row.property))) rows.push({ id:`media-css.${sha(`${source.id}\0${decl.line}\0${decl.property}\0${decl.value}`).slice(0,16)}`, contract_id:contract.id, source_id:source.id, path:source.path, line:decl.line, selector:decl.selector, css_property:decl.property, css_value:decl.value, intrinsic_dimensions:'runtime-measurement-required', rendered_dimensions:'runtime-measurement-required', crop_focal_strategy:decl.property==='object-fit'||decl.property==='object-position'?'source-css-observed':'not-applicable', reachability:contract.reachability, decision:'NOT_MERGED' });
  return rows;
}
function geometryRows(source, contract) {
  const accepted = new Set(['width','min-width','max-width','height','min-height','max-height','aspect-ratio','grid-template-columns','grid-column','gap','padding','overflow','overflow-x','overflow-y','flex','flex-basis','line-clamp','-webkit-line-clamp','white-space','word-break','overflow-wrap','position','top','bottom','left','right','z-index']);
  return selectorsWithDeclarations(source.path, source.content).filter((row)=>accepted.has(row.property)).map((row)=>({id:`geometry.${sha(`${source.id}\0${row.line}\0${row.property}\0${row.value}`).slice(0,16)}`,contract_id:contract.id,source_id:source.id,path:source.path,line:row.line,selector:row.selector,property:row.property,value:row.value,constraint_kind:/width/u.test(row.property)?'width':/height|aspect/u.test(row.property)?'height-or-ratio':/grid|flex/u.test(row.property)?'layout':'flow',consumer_dependency:'source-selector-context-only',reachability:contract.reachability,decision:'NOT_MERGED'}));
}
function breakpoints(source, contract) {
  const rows=[]; const re=/@(media|container)\s*([^\{]{1,280})\{/giu;
  for (const match of source.content.matchAll(re)) {
    const query=match[2].trim(); const values=[...query.matchAll(/(?:min|max)-(?:width|height)\s*:\s*(\d+(?:\.\d+)?)px/giu)].map((m)=>Number(m[1]));
    if (!values.length) rows.push({id:`breakpoint.${sha(`${source.id}\0${match.index}`).slice(0,16)}`,contract_id:contract.id,source_id:source.id,path:source.path,line:lineOf(source.content,match.index),kind:match[1].toLowerCase(),query,threshold_px:null,probe:'source-query-nonnumeric',decision:'NOT_MERGED'});
    for (const threshold of values) for (const delta of [-1,0,1]) rows.push({id:`breakpoint.${sha(`${source.id}\0${match.index}\0${threshold}\0${delta}`).slice(0,16)}`,contract_id:contract.id,source_id:source.id,path:source.path,line:lineOf(source.content,match.index),kind:match[1].toLowerCase(),query,threshold_px:threshold,probe_px:threshold+delta,probe:delta===0?'threshold':delta<0?'threshold-minus-one':'threshold-plus-one',decision:'NOT_MERGED'});
  } return rows;
}
function stickyRows(source, contract) {
  return selectorsWithDeclarations(source.path,source.content).filter((row)=>row.property==='position'&&/\b(?:sticky|fixed)\b/iu.test(row.value)).map((row)=>({id:`position.${sha(`${source.id}\0${row.line}\0${row.selector}`).slice(0,16)}`,contract_id:contract.id,source_id:source.id,path:source.path,line:row.line,selector:row.selector,position:row.value.trim(),state_machine:['static','approaching','pinned','collision','leaving-container','unpinned'],anchor:'runtime-measurement-required',scroll_ancestor:'runtime-measurement-required',offsets:'source-selector-reconciliation-required',z_index:'source-selector-reconciliation-required',safe_area:/safe-area/iu.test(source.content)?'source-observed':'not-observed',decision:'NOT_MERGED'}));
}
function recoveryRows(source, contract) {
  const found=[]; const text=source.content;
  for (const state of ['idle','loading','skeleton','spinner','optimistic','partial','stale','refreshing','empty','error','retry','offline','unavailable','success']) {
    const hits=[...text.matchAll(new RegExp(`\\b${state}\\b`,'giu'))];
    if(hits.length) found.push({id:`recovery.${sha(`${source.id}\0${state}`).slice(0,16)}`,contract_id:contract.id,source_id:source.id,path:source.path,state,applicability:'source-observed',source_lines:hits.slice(0,16).map((match)=>lineOf(text,match.index)),runtime_status:'not-captured',decision:'NOT_MERGED'});
  }
  if(!found.length) found.push({id:`recovery.${sha(`${source.id}\0not-applicable`).slice(0,16)}`,contract_id:contract.id,source_id:source.id,path:source.path,state:'not-applicable',applicability:'explicit-not-applicable',reason:'No bounded loading/recovery marker appears in the pinned component source; runtime capture must not infer one.',runtime_status:'not-captured',decision:'NOT_MERGED'});
  return found;
}
function interactionRows(source, contract) {
  const states=INTERACTION_STATES[contract.kind]||['idle']; const attrs=[...source.content.matchAll(/\b(data-[\w-]+|aria-(?:expanded|controls|selected|pressed)|hidden|open|disabled)\b/giu)].map((m)=>m[1].toLowerCase());
  return [{id:`interaction.${sha(source.id).slice(0,16)}`,contract_id:contract.id,source_id:source.id,path:source.path,kind:contract.kind,states,markers:[...new Set(attrs)].sort(),transitions:contract.kind==='overlay'?[['closed','opening'],['opening','open'],['open','closing'],['closing','closed'],['open','escape'],['open','focus-return']]:contract.kind==='rail'?[['edge-start','touch-scroll'],['touch-scroll','edge-end'],['focus-scroll','edge-end']]:[],runtime_status:'planned-not-captured',reachability:contract.reachability,decision:'NOT_MERGED'}];
}
function railRows(source, contract) { if(contract.kind!=='rail') return []; return [{id:`rail.${sha(source.id).slice(0,16)}`,contract_id:contract.id,source_id:source.id,path:source.path,states:INTERACTION_STATES.rail,overflow_contract:/overflow|scroll/iu.test(source.content)?'source-observed':'unresolved',snap_contract:/scroll-snap/iu.test(source.content)?'source-observed':'not-observed',keyboard_alternative:/keydown|key(?:down|up)/iu.test(source.content)?'source-observed':'unresolved',runtime_status:'planned-not-captured',reachability:contract.reachability,decision:'NOT_MERGED'}]; }
function overlayRows(source, contract) { if(!['overlay','selection'].includes(contract.kind)) return []; return [{id:`overlay.${sha(source.id).slice(0,16)}`,contract_id:contract.id,source_id:source.id,path:source.path,kind:contract.kind,states:INTERACTION_STATES[contract.kind],focus_contract:/focus|activeElement/iu.test(source.content)?'source-observed':'unresolved',escape_contract:/Escape/iu.test(source.content)?'source-observed':'unresolved',responsive_contract:/@media|matchMedia/iu.test(source.content)?'source-observed':'not-observed',runtime_status:'planned-not-captured',decision:'NOT_MERGED'}]; }
function history(repoRoot, path, contract) {
  try { const output=execFileSync('git',['-C',repoRoot,'log',PINNED_BEHAVIOR_SOURCE_SHA,'--follow','--format=%H%x1f%cs%x1f%s','--',path],{encoding:'utf8',maxBuffer:4*1024*1024}); return output.trim().split('\n').filter(Boolean).slice(0,80).map((line)=>{const [commit,date,subject]=line.split('\x1f');return {id:`history.${sha(`${contract.id}\0${commit}`).slice(0,16)}`,contract_id:contract.id,commit,date,subject,path,classification:'pinned-ancestry-commit-unclassified',semantic_status:'requires-curated-reconciliation',acceptance_claimed:false,history_scope:`ancestors-of-${PINNED_BEHAVIOR_SOURCE_SHA}`,evidence_kind:'git-commit-subject-bounded',decision:'NOT_MERGED'};}); } catch { return [{id:`history.${sha(`${contract.id}\0unavailable`).slice(0,16)}`,contract_id:contract.id,path,classification:'historical-unresolved',evidence_kind:'git-history-unavailable',history_scope:`ancestors-of-${PINNED_BEHAVIOR_SOURCE_SHA}`,decision:'NOT_MERGED'}]; }
}
function specimenPlans(contracts, breakpoints) {
  return contracts.filter((contract)=>contract.capture_required!==false).flatMap((contract)=>{ const points=breakpoints.filter((row)=>row.contract_id===contract.id).slice(0,18);
    const state=INTERACTION_STATES[contract.kind]||['idle']; const pairs=contract.id==='behavior.transport-experiment'
      ? [['baseline','departure_board_v1'],['baseline','route_strips_v1'],['baseline','next_departure_queue_v1']]
      : state.length>1?[[state[0],state[1]]]:[[state[0],state[0]]];
    return pairs.map(([initial,final],index)=>({id:`behavior-plan.${contract.id}.${index+1}`,contract_id:contract.id,initial_state:initial,final_state:final,
      treatment: contract.id==='behavior.transport-experiment'?final:null,
      actions:contract.kind==='overlay'?['activate','assert-focus','escape','assert-focus-return']:contract.kind==='rail'?['scroll-to-end','keyboard-focus-visible-item']:contract.kind==='selection'?['activate','keyboard-toggle','assert-selected']:contract.kind==='experiment'?['set-qa-treatment','wait-for-rendered-variant','assert-treatment-marker']:['settle','focus','activate'],
      viewport:{width:contract.kind==='media'?1728:390,height:844},breakpoint_probes:points.map((row)=>row.probe_px),reduced_motion:true,reachability:contract.reachability,capture_status:'planned-not-captured',required_packet:['initial-screenshot','final-screenshot','action-sequence','dom-summary','computed-styles','geometry','scroll','focus','accessibility','network-media','viewport-container','reduced-motion','source-sha','route-consumer','reachability'],decision:'NOT_MERGED'}));
  });
}
function auditMarkdown({baseSnapshot, records, status, reasons}) { return `# Current UI Behavioral Decoder & Experiment Archaeology v1.1\n\n- Base immutable Decoder v1 manifest SHA-256: \`${baseSnapshot.manifest_sha256}\`\n- Base snapshot path: \`${baseSnapshot.path}\`\n- Supplement status: **${status}**\n- Behavior contracts: ${records.behaviorContracts.length}\n- Geometry records: ${records.geometry.length}\n- Media records: ${records.media.length}\n- Interaction machines: ${records.interaction.length}\n- Positioning records: ${records.positioning.length}\n- Experiment treatments: ${records.experiments.length}\n- Pinned-ancestry commit-subject records: ${records.history.filter((row)=>row.evidence_kind==='git-commit-subject-bounded').length}\n- Curated semantic history records: ${records.history.filter((row)=>row.evidence_kind==='curated-semantic-history').length}\n- Planned behavior specimens: ${records.plans.length}\n- Captured behavior specimens: ${records.observations.length}\n\n## Boundary\n\nThis is append-only AS-IS evidence. It does not change the immutable v1 snapshot, production Astro/CSS/JS, tokens, Penpot, component identities, experiment winners, or normalization decisions. Every inferred or source-only fact remains explicitly scoped. Generic Git history is deterministically bounded to ancestors of \`${PINNED_BEHAVIOR_SOURCE_SHA}\`; commit subjects are not assigned semantic acceptance. Explicit PR/run/branch records form a curated supplement and do not claim exhaustive coverage of every mutable remote ref, tag, release or artifact.\n\n## Status basis\n\n${reasons.map((r)=>`- ${r}`).join('\n')}\n`; }
function writes(root, records) { const paths={ 'behavior-contracts.jsonl':records.behaviorContracts,'geometry-constraints.jsonl':records.geometry,'media-behavior.jsonl':records.media,'loading-recovery-states.jsonl':records.recovery,'interaction-state-machines.jsonl':records.interaction,'positioning-sticky-fixed.jsonl':records.positioning,'shelves-and-rails.jsonl':records.rails,'overlays-disclosures-selection.jsonl':records.overlays,'experiment-registry.jsonl':records.experiments,'historical-variant-evidence.jsonl':records.history,'breakpoint-and-container-matrix.jsonl':records.breakpoints,'behavior-specimen-plan.jsonl':records.plans,'behavior-specimen-observations.jsonl':records.observations,'behavior-page-verification.jsonl':records.pageVerification,'requirements-provenance-ledger.jsonl':records.requirementsProvenance,'media-policy-matrix.jsonl':records.mediaPolicy,'dynamic-region-loading-matrix.jsonl':records.dynamicRegions,'action-packet-index.jsonl':records.actionPacketIndex,'visual-review-ledger.jsonl':records.visualReviewLedger,'unresolved.jsonl':records.unresolved}; const output={}; for(const [name,rows] of Object.entries(paths)){ const content=rows.sort((a,b)=>a.id.localeCompare(b.id)).map((row)=>json(row)).join('');writeFileSync(join(root,name),content);output[name]={bytes:Buffer.byteLength(content),sha256:sha(content),records:rows.length};} return output; }
export function buildBehavioralSupplement({sourceRoot, baseSnapshotRoot, outputRoot, sourceSha, requestedStatus = null}) {
  const baseManifestPath=join(resolve(baseSnapshotRoot),'manifest.json'); if(!existsSync(baseManifestPath)) throw new Error('Immutable Decoder v1 manifest is required'); const baseManifestBytes=readFileSync(baseManifestPath); const baseManifest=JSON.parse(baseManifestBytes); const expected=PINNED_BEHAVIOR_SOURCE_SHA; if(baseManifest.identity_planes?.latest_checked_kaggle_candidate?.source_sha!==expected) throw new Error('Base snapshot does not have the required exact candidate pin');
  const root=resolve(outputRoot); if(existsSync(root)&&readdirSync(root).length) throw new Error('Behavioral supplement output must be a new empty append-only directory'); mkdirSync(root,{recursive:true});
  const contracts=[]; const all=[]; for(const [id,path,kind,tags] of CATALOG){const source=sourceFor(sourceRoot,path); if(!source){all.push({id:`unresolved.missing-${id}`,kind:'missing-pinned-source',contract_id:`behavior.${id}`,blocks_ready:true,decision:'NOT_MERGED'});continue;} const reachability=REACHABILITY[id]||'production-reachable-not-observed';contracts.push({id:`behavior.${id}`,logical_path:path,source_id:source.id,source_sha256:sha(source.content),source_sha:sourceSha,kind,tags:tags.split(';'),reachability,behavioral_disposition:reachability==='dead-unreachable'?'dead-unreachable':reachability==='experiment-off'?'experiment-off':reachability,capture_required:true,decision:'NOT_MERGED',normalization_allowed:false});}
  const knownPaths=new Set(contracts.map((item)=>item.logical_path));
  for(const path of files(join(sourceRoot,'src')).filter((file)=>file.endsWith('.astro'))){const rel=relative(sourceRoot,path);if(knownPaths.has(rel))continue;const content=readFileSync(path,'utf8');if(!/<(?:img|picture|source)\b|(?:object-fit|object-position|aspect-ratio)\s*:/iu.test(content))continue;contracts.push({id:`behavior.media-consumer-${sha(rel).slice(0,12)}`,logical_path:rel,source_id:sourceId(rel,content),source_sha256:sha(content),source_sha:sourceSha,kind:'media',tags:['auto-discovered-media-consumer'],reachability:'requires-source-runtime-reconciliation',behavioral_disposition:'source-inventory-runtime-reconciliation-required',capture_required:false,decision:'NOT_MERGED',normalization_allowed:false});}
  const byContract=new Map(contracts.map((item)=>[item.logical_path,item])); const records={behaviorContracts:contracts,geometry:[],media:[],recovery:[],interaction:[],positioning:[],rails:[],overlays:[],experiments:buildTransportExperimentRows({sourceRoot}),history:[],breakpoints:[],plans:[],observations:[],pageVerification:[],requirementsProvenance:[],mediaPolicy:[],dynamicRegions:[],actionPacketIndex:[],visualReviewLedger:[],unresolved:all};
  for(const contract of contracts){
    const source=sourceFor(sourceRoot,contract.logical_path);
    records.geometry.push(...geometryRows(source,contract));
    records.media.push(...mediaRows(source,contract));
    records.recovery.push(...recoveryRows(source,contract));
    records.interaction.push(...interactionRows(source,contract));
    records.positioning.push(...stickyRows(source,contract));
    records.rails.push(...railRows(source,contract));
    records.overlays.push(...overlayRows(source,contract));
    records.breakpoints.push(...breakpoints(source,contract));
    if(contract.capture_required!==false) records.history.push(...history(sourceRoot,contract.logical_path,contract));
    else records.history.push({
      id:`history.${sha(`${contract.id}\0source-current`).slice(0,16)}`,
      contract_id:contract.id,
      path:contract.logical_path,
      classification:'implemented-current',
      evidence_kind:'pinned-source-content-sha',
      source_sha256:contract.source_sha256,
      decision:'NOT_MERGED',
    });
  }
  records.history.push(...buildCuratedBehavioralHistoryRows());
  // Include global CSS behavior as plane-bound source evidence, without transforming it into a component definition.
  for(const path of files(join(sourceRoot,'src')).filter((file)=>extname(file)==='.css'||file.endsWith('.astro'))){const rel=relative(sourceRoot,path);const text=readFileSync(path,'utf8');for(const decl of selectorsWithDeclarations(rel,text).filter((row)=>row.property==='position'&&/\b(?:sticky|fixed)\b/iu.test(row.value))){records.positioning.push({id:`position-global.${sha(`${rel}\0${decl.line}\0${decl.selector}`).slice(0,16)}`,contract_id:null,source_id:sourceId(rel,text),path:rel,line:decl.line,selector:decl.selector,position:decl.value,scope:'global-or-page-source',state_machine:['static','approaching','pinned','collision','leaving-container','unpinned'],decision:'NOT_MERGED'});}}
  records.requirementsProvenance=buildRequirementsProvenance({sourceRoot});
  records.mediaPolicy=buildMediaPolicyMatrix({media:records.media,provenance:records.requirementsProvenance});
  records.dynamicRegions=buildDynamicRegionMatrix({sourceRoot,provenance:records.requirementsProvenance});
  records.plans=specimenPlans(contracts,records.breakpoints); for(const contract of contracts.filter((item)=>item.id==='behavior.desktop-event-action-panel')) records.unresolved.push({id:'unresolved.cta-horizontal-image-alternative-provenance',kind:'cta-provenance',contract_id:contract.id,observed_fact:'Curated commit/PR/run records and pinned source identify Split inline versus Editorial stacked CTA anatomy; exact runtime capture remains a separate evidence plane.',required_evidence:['exact-runtime'],status:'semantic-provenance-established-runtime-capture-pending',blocks_ready:false,decision:'NOT_MERGED'});
  records.unresolved.push({id:'unresolved.listing-personal-filter-version-contract',kind:'requirements-source-conflict',contract_id:'behavior.listing-personal-filter',observed_fact:'The design-system document names v2 as candidate while the source catalog identifies a v3 shell and date/weekend consumers still pass behavior version 2 with floating placement.',status:'conflict',blocks_ready:false,decision:'NOT_MERGED'});
  records.unresolved.push(
    {id:'unresolved.behavioral-packets-pending',kind:'capture-and-review-gate',observed_fact:'The source pass contains plans only. Exact-source action packets, raster hashes, and file-level full-resolution visual reviews are not attached yet.',blocks_ready:true,decision:'NOT_MERGED'},
    {id:'unresolved.media-2x3-not-a-token',kind:'media-requirement',observed_fact:'2:3 is observed as intrinsic source orientation but no normative universal 2:3 frame rule exists.',blocks_ready:false,decision:'NOT_MERGED'},
    {id:'unresolved.media-consumer-local-crop-policies',kind:'media-policy-conflict',observed_fact:'Hero, related cards, listing cards and mobile rails use different current crop gates; no single global crop contract is proven.',blocks_ready:false,decision:'NOT_MERGED'},
    {id:'unresolved.personal-feed-no-skeleton',kind:'dynamic-region-disposition',observed_fact:'PersonalFeedSlot has a real runtime wait but no resolved-card-shaped skeleton, aria-busy state, or reserved result geometry.',blocks_ready:false,decision:'NOT_MERGED'},
    {id:'unresolved.dynamic-offline-states',kind:'dynamic-region-disposition',observed_fact:'Search, PersonalFeed, discovery, Favorites and Weather have no first-class offline state.',blocks_ready:false,decision:'NOT_MERGED'},
    {id:'unresolved.dynamic-sticky-failure-cache',kind:'dynamic-region-disposition',observed_fact:'Discovery and Favorites cache failed data loads for the page lifetime and expose no reliable retry path.',blocks_ready:false,decision:'NOT_MERGED'},
    {id:'unresolved.search-vector-preview-dead',kind:'dead-unreachable-state',observed_fact:'Search provisional vector preview insertion is defined but not invoked; it is not a runtime-observed visual state.',blocks_ready:false,decision:'NOT_MERGED'},
    {id:'unresolved.skeleton-geometry-not-exact',kind:'loading-geometry',observed_fact:'Search and Favorites skeletons resemble resolved cards but do not prove exact resolved height or zero CLS.',blocks_ready:false,decision:'NOT_MERGED'},
    {id:'unresolved.personal-feed-hint-write-only',kind:'source-document-conflict',observed_fact:'The personal-feed compact hint is written but no pinned source path reads it for cross-navigation restoration.',blocks_ready:false,decision:'NOT_MERGED'},
    {id:'unresolved.favorites-sync-error-not-surfaced',kind:'dynamic-recovery-gap',observed_fact:'FavoritesSurface does not subscribe to saved-event-state-error, so durable sync failures are not surfaced immediately.',blocks_ready:false,decision:'NOT_MERGED'},
    {id:'unresolved.dynamic-doc-runtime-drift',kind:'source-document-conflict',observed_fact:'Current /vystavki/ use and mobile personal-feed visibility differ from the existing canonical prose.',blocks_ready:false,decision:'NOT_MERGED'},
  );
  for(const exp of records.experiments.filter((item)=>item.winner_decision_receipt==='absent')) records.unresolved.push({id:`unresolved.${exp.id}`,kind:'experiment-winner-receipt',experiment_id:exp.experiment_id,treatment:exp.treatment,blocks_ready:false,reason:'No decision receipt was supplied by the immutable decoder input; treatment remains unresolved and unaccepted.',decision:'NOT_MERGED'});
  const requestedReady=requestedStatus===BEHAVIOR_STATUS; const allCaptured=records.plans.length>0&&records.plans.every((item)=>item.capture_status==='captured-and-reviewed'); const blockers=records.unresolved.filter((item)=>item.blocks_ready); const status=requestedReady&&allCaptured&&!blockers.length?BEHAVIOR_STATUS:'EVIDENCE_COLLECTION_INCOMPLETE'; const reasons=status===BEHAVIOR_STATUS?['All planned interaction packets are captured and human-reviewed.','No blocking unresolved record remains.']:['Exact-source extraction, ef7 ancestry and curated semantic history are bounded and recorded, but do not claim exhaustive coverage of every mutable remote ref/tag/release/artifact; behavior specimens remain planned-not-captured until an exact capture and human-review ledger is attached.','CTA semantic ancestry identifies Split inline and Editorial stacked anatomy; transport has no winner receipt, so no treatment is accepted.'];
  const output=writes(root,records);const baseSnapshot={path:resolve(baseSnapshotRoot),manifest_sha256:sha(baseManifestBytes),snapshot_id:baseManifest.snapshot_id};const report=auditMarkdown({baseSnapshot,records,status,reasons});writeFileSync(join(root,'audit-report.md'),report);output['audit-report.md']={bytes:Buffer.byteLength(report),sha256:sha(report),records:null}; const artifactReceipt={schema_version:BEHAVIOR_SCHEMA,status:'local-source-pass',actions:{repository:process.env.GITHUB_REPOSITORY||null,run_id:process.env.GITHUB_RUN_ID||null,run_attempt:process.env.GITHUB_RUN_ATTEMPT||null,artifact_id:null,artifact_digest:null,run_url:process.env.GITHUB_SERVER_URL&&process.env.GITHUB_REPOSITORY&&process.env.GITHUB_RUN_ID?`${process.env.GITHUB_SERVER_URL}/${process.env.GITHUB_REPOSITORY}/actions/runs/${process.env.GITHUB_RUN_ID}`:null},source_sha:sourceSha,base_snapshot_manifest_sha256:baseSnapshot.manifest_sha256};writeFileSync(join(root,'artifact-receipt.json'),json(artifactReceipt,true));output['artifact-receipt.json']={bytes:readFileSync(join(root,'artifact-receipt.json')).length,sha256:sha(readFileSync(join(root,'artifact-receipt.json'))),records:null};const artifactIndex={schema_version:BEHAVIOR_SCHEMA,status:'source-pass-no-heavy-evidence',base_snapshot:baseSnapshot,entries:[]};writeFileSync(join(root,'artifact-index.json'),json(artifactIndex,true));output['artifact-index.json']={bytes:readFileSync(join(root,'artifact-index.json')).length,sha256:sha(readFileSync(join(root,'artifact-index.json'))),records:null};const independentAudit={schema_version:BEHAVIOR_SCHEMA,status:'pending',reason:'Capture, full-resolution review and independent audit have not run.'};writeFileSync(join(root,'independent-audit.json'),json(independentAudit,true));output['independent-audit.json']={bytes:readFileSync(join(root,'independent-audit.json')).length,sha256:sha(readFileSync(join(root,'independent-audit.json'))),records:null}; const manifest={schema_version:BEHAVIOR_SCHEMA,supplement_version:'1.1',status,base_snapshot:baseSnapshot,source_sha:sourceSha,immutable_v1_modified:false,constraints:{append_only:true,component_merge:false,component_split:false,component_deletion:false,normalization:false,tokens:false,production_astro_css_js:false,penpot:false,experiment_winner_decision:false},outputs:output,counts:Object.fromEntries(Object.entries(records).map(([key,value])=>[key,value.length])),human_visual_review:{required:true,completed:false,raster_count:0,perceptual_hash_is_not_review:true}};writeFileSync(join(root,'manifest.json'),json(manifest,true));const receipt={schema_version:BEHAVIOR_SCHEMA,status:status===BEHAVIOR_STATUS?'complete':'partial',final_status:status,manifest_sha256:sha(readFileSync(join(root,'manifest.json'))),blockers:blockers.map((item)=>item.id)};writeFileSync(join(root,'receipt.json'),json(receipt,true));assertBehavioralSupplement(root,{allowIncomplete:true});return {root,manifest,receipt,records};
}
function readJsonl(base,name){
  return readFileSync(join(base,name),'utf8').trim().split('\n').filter(Boolean).map(JSON.parse);
}

export function assertBehavioralSupplement(root,{allowIncomplete=false}={}) {
  const base=resolve(root);
  for(const name of REQUIRED_BEHAVIOR_FILES) if(!existsSync(join(base,name))) throw new Error(`Required behavioral output missing: ${name}`);
  const manifest=JSON.parse(readFileSync(join(base,'manifest.json'),'utf8'));
  if(manifest.schema_version!==BEHAVIOR_SCHEMA||manifest.supplement_version!=='1.1') throw new Error('Invalid behavioral supplement schema');
  if(![BEHAVIOR_STATUS,'EVIDENCE_COLLECTION_INCOMPLETE'].includes(manifest.status)) throw new Error(`Invalid behavioral supplement status: ${manifest.status}`);
  if(manifest.constraints?.append_only!==true||manifest.constraints?.normalization!==false||manifest.constraints?.production_astro_css_js!==false||manifest.constraints?.penpot!==false||manifest.constraints?.experiment_winner_decision!==false) throw new Error('Behavioral STOP invariant missing');
  const outputNames=Object.keys(manifest.outputs||{});
  for(const name of REQUIRED_BEHAVIOR_FILES.filter((item)=>!['manifest.json','receipt.json'].includes(item))) if(!outputNames.includes(name)) throw new Error(`Required behavioral output is not manifest-bound: ${name}`);
  for(const [name,entry] of Object.entries(manifest.outputs||{})){
    if(name.startsWith('/')||name.split('/').includes('..')) throw new Error(`Unsafe behavioral manifest path: ${name}`);
    const content=readFileSync(join(base,name));
    if(content.length!==entry.bytes||sha(content)!==entry.sha256)throw new Error(`Behavioral manifest hash mismatch: ${name}`);
  }
  const plans=readJsonl(base,'behavior-specimen-plan.jsonl');
  const observations=readJsonl(base,'behavior-specimen-observations.jsonl');
  const reviews=readJsonl(base,'visual-review-ledger.jsonl');
  const actionPackets=readJsonl(base,'action-packet-index.jsonl');
  const unresolved=readJsonl(base,'unresolved.jsonl');
  const unique=(rows,label)=>{const ids=rows.map((row)=>row.id);if(ids.some((id)=>typeof id!=='string'||!id)||new Set(ids).size!==ids.length)throw new Error(`Behavioral ${label} IDs are missing or duplicated`);};
  unique(plans,'plan');unique(observations,'observation');unique(reviews,'review');unique(actionPackets,'action-packet');
  const plansById=new Map(plans.map((row)=>[row.id,row]));const observationsById=new Map(observations.map((row)=>[row.id,row]));
  for(const observation of observations){const plan=plansById.get(observation.plan_id);if(!plan||observation.family!==plan.family||observation.source_sha!==manifest.source_sha)throw new Error(`Behavioral observation binding mismatch: ${observation.id}`);if(!plan.expected_capture_phases?.includes(observation.phase))throw new Error(`Behavioral observation phase is not planned: ${observation.id}`);}
  for(const plan of plans){const actual=(plan.observation_ids||[]).slice().sort();const bound=observations.filter((row)=>row.plan_id===plan.id).map((row)=>row.id).sort();if(JSON.stringify(actual)!==JSON.stringify(bound))throw new Error(`Behavioral plan observation references mismatch: ${plan.id}`);}
  for(const packet of actionPackets){const plan=plansById.get(packet.plan_id);if(!plan||JSON.stringify((packet.observation_ids||[]).slice().sort())!==JSON.stringify((plan.observation_ids||[]).slice().sort()))throw new Error(`Behavioral action packet binding mismatch: ${packet.id}`);}
  const artifactIndex=JSON.parse(readFileSync(join(base,'artifact-index.json'),'utf8'));const artifactEntries=artifactIndex.entries||[];const artifactPaths=artifactEntries.map((row)=>row.path);if(new Set(artifactPaths).size!==artifactPaths.length)throw new Error('Behavioral artifact index paths are duplicated');
  for(const entry of artifactEntries){if(!entry.path||entry.path.startsWith('/')||entry.path.split('/').includes('..')||!/^[a-f0-9]{64}$/u.test(entry.sha256||'')||!Number.isInteger(entry.bytes)||entry.bytes<0)throw new Error(`Invalid behavioral artifact entry: ${entry.path||'unknown'}`);if(entry.storage==='compact-supplement'){const content=readFileSync(join(base,entry.path));if(content.length!==entry.bytes||sha(content)!==entry.sha256)throw new Error(`Behavioral compact artifact hash mismatch: ${entry.path}`);}}
  const heavyEntries=artifactEntries.filter((row)=>row.storage==='actions-and-permanent-heavy-artifact');
  if(heavyEntries.length){if(heavyEntries.length!==observations.length||new Set(heavyEntries.map((row)=>row.observation_id)).size!==observations.length)throw new Error('Behavioral heavy artifact index coverage mismatch');for(const entry of heavyEntries){const observation=observationsById.get(entry.observation_id);if(!observation||entry.path!==observation.screenshot?.path||entry.sha256!==observation.screenshot?.sha256||entry.bytes!==observation.screenshot?.bytes)throw new Error(`Behavioral heavy artifact binding mismatch: ${entry.path}`);}}
  if(observations.some((row)=>row.capture_status==='captured'&&!row.review_status))throw new Error('Captured behavioral observation lacks review status');
  const receipt=JSON.parse(readFileSync(join(base,'receipt.json'),'utf8'));if(receipt.manifest_sha256!==sha(readFileSync(join(base,'manifest.json'))))throw new Error('Behavioral receipt manifest hash mismatch');
  if(manifest.status===BEHAVIOR_STATUS){
    if(manifest.supplement_id!=='behavioral-supplement-v1.1-snapshot-20260808T124842-4786ac53bc'||!plans.length||!observations.length||plans.some((row)=>!['captured-and-reviewed','explicit-blocker'].includes(row.capture_status))) throw new Error('Ready supplement has incorrect identity or uncaptured plans');
    if(manifest.counts?.behavior_packet_plans!==plans.length||manifest.counts?.observations!==observations.length||manifest.counts?.rasters!==observations.length||manifest.counts?.explicit_blockers!==plans.filter((row)=>row.capture_status==='explicit-blocker').length) throw new Error('Ready supplement manifest counts do not match bound evidence');
    if(receipt.counts?.plans!==plans.length||receipt.counts?.observations!==observations.length||receipt.counts?.rasters!==observations.length||receipt.counts?.explicit_blockers!==plans.filter((row)=>row.capture_status==='explicit-blocker').length) throw new Error('Ready supplement receipt counts do not match bound evidence');
    if(unresolved.some((row)=>row.blocks_ready===true)) throw new Error('Ready supplement has blocking unresolved records');
    const rasterReviews=reviews.filter((row)=>row.media_type==='image/png'||row.media_type==='image/jpeg');
    if(rasterReviews.length!==observations.length||rasterReviews.some((row)=>row.review_status!=='reviewed-full-resolution'||row.full_resolution_opened!==true)) throw new Error('Ready supplement lacks file-level full-resolution raster review');
    if(new Set(rasterReviews.map((row)=>row.observation_id)).size!==observations.length)throw new Error('Ready supplement review ledger is not one-to-one with observations');
    if(actionPackets.length!==plans.length||new Set(actionPackets.map((row)=>row.plan_id)).size!==plans.length)throw new Error('Ready supplement action-packet index coverage mismatch');
    const reviewedEvidence=observations.filter((row)=>row.review_status==='reviewed-full-resolution');
    for(const ratio of ['4:5','5:4']) if(!reviewedEvidence.some((row)=>row.ratios?.includes?.(ratio))) throw new Error(`Ready supplement lacks reviewed ${ratio} runtime visual evidence`);
    const artifactReceipt=JSON.parse(readFileSync(join(base,'artifact-receipt.json'),'utf8'));
    if(artifactReceipt.status!=='complete'||artifactReceipt.independent_audit?.status!=='PASS'||artifactReceipt.secret_scan?.status!=='PASS')throw new Error('Ready supplement durable provenance/audit is incomplete');
    if(receipt.status!=='complete'||receipt.final_status!==BEHAVIOR_STATUS||receipt.manifest_sha256!==sha(readFileSync(join(base,'manifest.json'))))throw new Error('Ready supplement receipt chain is invalid');
  }
  if(!allowIncomplete&&manifest.status!==BEHAVIOR_STATUS)throw new Error('Behavioral supplement is not ready for project normalization synthesis');
  return {status:'valid',plans:plans.length,observations:observations.length,reviews:reviews.length};
}
