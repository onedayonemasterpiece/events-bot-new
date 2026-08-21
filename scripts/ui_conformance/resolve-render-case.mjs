#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, resolve, join } from 'node:path';
import { formatOccurrencePresentation, resolveOccurrenceFamily } from '../../site/src/lib/eventOccurrences.ts';
import { resolveMobileEventCardMedia, resolveRelatedCardMediaTreatment } from '../../site/src/lib/relatedCardLayout.mjs';

function parse(argv) { const out={}; for(let i=0;i<argv.length;i+=1){const key=argv[i]; if(!key.startsWith('--'))throw new Error(`Unexpected argument: ${key}`); const value=argv[i+1]; if(!value||value.startsWith('--'))throw new Error(`${key} requires a value`); out[key.slice(2)]=value;i+=1;} return out; }
const args=parse(process.argv.slice(2));
for(const key of ['case','corpus-root','output']) if(!args[key]) throw new Error(`--${key} is required`);
const stable=(value)=>createHash('sha256').update(`${JSON.stringify(sortValue(value))}\n`).digest('hex');
function sortValue(value){if(Array.isArray(value))return value.map(sortValue);if(value&&typeof value==='object')return Object.fromEntries(Object.keys(value).sort().map(k=>[k,sortValue(value[k])]));return value;}
const read=(path)=>JSON.parse(readFileSync(resolve(path),'utf8'));
const row=read(args.case); const corpusRoot=resolve(args['corpus-root']); const corpus=read(join(corpusRoot,'corpus.json')); const assetsManifest=read(join(corpusRoot,'assets-manifest.json'));
const fixtureMeta=corpus.fixtures.find((item)=>item.fixture_id===row.fixture_id); if(!fixtureMeta)throw new Error(`Fixture is not in exact corpus: ${row.fixture_id}`);
const wrapper=read(join(corpusRoot,fixtureMeta.payload_path)); const event=wrapper.preview_event;
// The extraction adapter computes canonical payload hashes with Python's JSON
// number serialization.  Preserve that exact cross-repository digest instead
// of silently replacing it with JavaScript's different 1.0 serialization.
if(wrapper.preview_event_sha256!==fixtureMeta.preview_event_sha256) throw new Error(`Frozen PreviewEvent hash binding mismatch: ${row.fixture_id}`);
if(row.fixture_sha256!==fixtureMeta.preview_event_sha256) throw new Error(`Case fixture hash mismatch: ${row.fixture_id}`);
const allEvents=corpus.fixtures.map((item)=>read(join(corpusRoot,item.payload_path)).preview_event);
const occurrence=formatOccurrencePresentation(resolveOccurrenceFamily(event,allEvents,{currentDate:corpus.reference_clock.current_date}),corpus.reference_clock.current_date);
const months=['января','февраля','марта','апреля','мая','июня','июля','августа','сентября','октября','ноября','декабря'];
const dateText=(value,includeYear=false)=>{const [y,m,d]=value.split('-').map(Number);return `${d} ${months[m-1]}${includeYear?` ${y}`:''}`;};
const displayDate=event.end_date&&event.end_date!==event.start_date?`${dateText(event.start_date,event.start_date.slice(0,4)!==corpus.reference_clock.current_date.slice(0,4))} — до ${dateText(event.end_date,event.end_date.slice(0,4)!==corpus.reference_clock.current_date.slice(0,4))}`:dateText(event.start_date,event.start_date.slice(0,4)!==corpus.reference_clock.current_date.slice(0,4));
const status=[event.ticket?.status,event.ticket?.label,event.status_label,event.ticket?.note].filter(Boolean).join(' ').toLowerCase();
const sold=/sold|unavailable|not[_\s-]?available|нет\s+бил|законч|распрод/u.test(status);
let admission=sold?'Билеты закончились':event.ticket?.is_free?(/регистрац|registration|зарегистр/u.test(status)?'Бесплатно · регистрация':/запис|phone|телефон|коммент/u.test(status)?'Бесплатно · по записи':'Бесплатно · вход свободный'):event.ticket?.price_label||(/донат|пожертв/u.test(status)?'За донат':event.ticket?.kind==='phone'?'Запись по телефону':event.ticket?.kind==='ticket'?'Билеты':/билет/u.test(status)?'Билеты':event.status_label||event.ticket?.label||'Условия уточняются');
const primary=(event.image_assets||[])[0]||null; const viewport=row.viewport_id.startsWith('mobile')?'mobile':'desktop';
const mediaDecision=primary?(viewport==='mobile'?resolveMobileEventCardMedia(event):resolveRelatedCardMediaTreatment(event,4/5)):null;
const assetRows=assetsManifest.assets.filter((item)=>item.fixture_id===row.fixture_id); const primaryAsset=assetRows.find((item)=>item.role==='primary')||null;
const canCalendar=!event.end_date||event.end_date===event.start_date;
const frameWidth=Math.max(1,row.container_width-2);
const frameHeight=primary?Number((frameWidth*(Number(primary.height)||1)/(Number(primary.width)||1)).toFixed(6)):null;
const resolved={
  schema_version:'resolved-render-case.v1', case_id:row.case_id, component_id:row.component_id,
  contract_version:row.contract_version, contract_sha256:row.contract_sha256, authority_mode:row.authority_mode,
  conformance_profile:row.conformance_profile, state_key:row.state_key, viewport_id:row.viewport_id,
  container_geometry:{viewport_width:row.viewport_width,viewport_height:row.viewport_height,container_width:row.container_width,device_scale_factor:row.device_scale_factor},
  event_fixture_id:row.fixture_id, event_payload_sha256:fixtureMeta.preview_event_sha256,
  event_fixture_path:fixtureMeta.payload_path, corpus_id:corpus.corpus_id, corpus_sha256:corpus.corpus_sha256,
  source_database_snapshot_fingerprint:fixtureMeta.source_database_snapshot_fingerprint,
  reference_clock:corpus.reference_clock,
  asset_refs:assetRows.map((item)=>item.asset_id), asset_manifest_sha256:assetsManifest.assets_manifest_sha256,
  resolved_content:{title:event.title,date:displayDate,time:event.display_time||null,place:[event.city,event.venue_name].filter(Boolean).join(' · ')||null,admission,counts:{likes:Number(event.likes_count||0),shares:Number(event.shares_count||0)},labels:{event_type:String(event.event_type||'').trim().toLowerCase()||null,occurrence:occurrence.compactLabel,not_interested:'Не интересно',calendar:canCalendar?'В календарь':null,share:'Поделиться'}},
  resolved_media:primary?{asset_id:primaryAsset?.asset_id||null,frame_role:'event-card-primary',frame_geometry:{outer_width:row.container_width,content_width:frameWidth,content_height:frameHeight,border_width:1,intrinsic_width:primary.width,intrinsic_height:primary.height},fit:mediaDecision?.fit||'contain',object_position:mediaDecision?.objectPosition||'50% 50%',crop_window:null,protected_regions:{ocr_boxes:primary.ocr_boxes||[],face_boxes:primary.face_boxes||[],valuable_region:primary.valuable_region||null},media_treatment:mediaDecision?.mediaTreatment||null,crop_reason:mediaDecision?.cropReason||null}:null,
  resolved_visibility:{event_type:Boolean(event.event_type),place:Boolean(event.city||event.venue_name),admission:Boolean(admission),calendar:canCalendar,share:true,like:true,not_interested:true,media:Boolean(primary)},
  resolved_nested_components:[
    {component_id:'event.media-frame',role:'media'}, {component_id:'event.action.not-interested',role:'negative-action'},
    ...(canCalendar?[{component_id:'event.action.calendar',role:'calendar-action'}]:[]),
    {component_id:'event.action.share',role:'share-action'}, {component_id:'event.action.like',role:'like-action'},
    ...(event.event_type?[{component_id:'event.meta.event-type',role:'event-type'}]:[]), {component_id:'event.meta.admission',role:'admission'}
  ],
  resolved_props:{variant:'split-actions',desktopRelatedCrop:viewport==='desktop',mobileFlowMedia:viewport==='mobile',presentation:'dark-shell'},
  expected_candidate_deltas:row.expected_candidate_deltas||[]
};
resolved.resolved_render_case_sha256=stable(resolved);
mkdirSync(dirname(resolve(args.output)),{recursive:true});writeFileSync(resolve(args.output),`${JSON.stringify(resolved,null,2)}\n`);
process.stdout.write(`${JSON.stringify({output:resolve(args.output),resolved_render_case_sha256:resolved.resolved_render_case_sha256,event_fixture_id:resolved.event_fixture_id,event_payload_sha256:resolved.event_payload_sha256},null,2)}\n`);
