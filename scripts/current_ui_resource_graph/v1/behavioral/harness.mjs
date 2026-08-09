import { constants, cpSync, existsSync, mkdirSync, readFileSync, readdirSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { basename, join, parse, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import { PINNED_SOURCE_SHA, buildBehaviorPacketRegistry } from './registry.mjs';
import { assertBehaviorPacketRegistry, stableHash } from './validate.mjs';

function assertDisposable(root){
  const target=resolve(root);if(target===parse(target).root||target.length<16||!basename(target).includes('behavior')) throw new Error('Behavior harness root must be an explicitly disposable behavior path');
  return target;
}
function reflinkCopy(source,target){
  const result=spawnSync('cp',['-a','--reflink=auto',source,target],{encoding:'utf8'});
  if(result.status!==0) cpSync(source,target,{recursive:true,mode:constants.COPYFILE_FICLONE});
}
export function copySiteWithoutNodeModules(source,target){
  mkdirSync(target,{recursive:true});
  for(const entry of readdirSync(source,{withFileTypes:true}).sort((a,b)=>a.name.localeCompare(b.name))){
    if(entry.name==='node_modules')continue;
    reflinkCopy(join(source,entry.name),join(target,entry.name));
  }
}
function cleanEnvironment(extra={}){
  return {
    ...process.env,
    STATIC_SITE_REPO_SHA:PINNED_SOURCE_SHA,
    PUBLIC_STATIC_SITE_CURRENT_DATE:'2026-08-08',
    PUBLIC_STATIC_SITE_REFERENCE_ISO:'2026-08-08T12:48:42.000Z',
    PUBLIC_PERSONALIZATION_SUPABASE_URL:'https://example.supabase.co',
    PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY:'decoder-fixture-public-value',
    PUBLIC_PERSONALIZATION_SUPABASE_RELAY_URL:'',
    PUBLIC_AUTHORIZED_SEARCH_TRANSPORT:'json',
    PUBLIC_YANDEX_AUTH_PROVIDER:'custom:yandex',
    PUBLIC_TRANSPORT_TIMETABLE_EXPERIMENT_MODE:'off',
    PUBLIC_PRELAUNCH_MODE:'0',
    ...extra,
  };
}
const timeNavPage=`---
import ListingTimeNav from '../../components/listings/ListingTimeNav.astro';
import EventLayout from '../../layouts/EventLayout.astro';
const exact=['10:00','11:00','12:00','13:00','14:00','15:00'].map((label,index)=>({key:label.replace(':',''),label,count:index+1,target:'slot-'+index}));
const items=[{key:'day',label:'Днём',count:21,href:'#slot-0',exact}];
---
<EventLayout title="Behavior time nav" description="Controlled exact-source disclosure" canonicalUrl="https://kenigevents.ru/behavior-specimens/time-nav/" structuredData={[]}>
  <main id="main" style="margin:64px auto;width:min(100% - 24px,720px)"><ListingTimeNav items={items} force /></main>
</EventLayout>
`;
const transportPage=`---
import eventsData from '../../data/preview-events.json';
import { getKaupTransportSuggestion } from '../../lib/eventKaupTransport';
import DepartureBoard from '../../components/transport/DepartureBoardTimetable.astro';
import RouteStrips from '../../components/transport/RouteStripsTimetable.astro';
import NextDeparture from '../../components/transport/NextDepartureQueueTimetable.astro';
const events=Array.isArray(eventsData)?eventsData:(eventsData.events||[]);const event=events.find((item)=>item.id===5374);if(!event)throw new Error('Pinned event 5374 missing');
const suggestion=getKaupTransportSuggestion(event);if(!suggestion)throw new Error('Pinned Kaup suggestion missing');
const props={route:suggestion.busRoute,arrivalStop:suggestion.busArrivalStop,originName:suggestion.busOriginName,originMapUrl:suggestion.busOriginMapUrl,walkRouteUrl:suggestion.stopToVenueDirectionsUrl,publicReturnAvailable:suggestion.publicReturnAvailable,options:suggestion.outbound};
---
<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Behavior transport</title></head><body><main data-behavior-transport-fixture data-event-id="5374"><DepartureBoard {...props} hidden={true}/><RouteStrips {...props}/><NextDeparture {...props}/></main><script>const treatment=window.__BEHAVIOR_TREATMENT__||'departure_board_v1';document.querySelectorAll('[data-transport-treatment]').forEach((node)=>{node.hidden=node.dataset.transportTreatment!==treatment});document.querySelector('[data-behavior-transport-fixture]')?.setAttribute('data-selected-treatment',treatment);</script></body></html>
<style is:global>html{font-family:system-ui;background:#f4f1e8;color:#17201f}body{margin:0}main{container:kaup-transport / inline-size;width:min(calc(100% - 24px),var(--behavior-container,391px));margin:24px auto;padding:16px;border-radius:20px;background:#fff;box-sizing:border-box}</style>
`;
const mediaRailPage=`---
import eventsData from '../../data/preview-events.json';
import EventMediaRail from '../../components/EventMediaRail.astro';
const events=Array.isArray(eventsData)?eventsData:(eventsData.events||[]);const event=events.find((item)=>item.id===2781);if(!event)throw new Error('Pinned event 2781 missing');
---
<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Behavior media rail</title></head><body><main data-behavior-media-fixture><section class="primary" aria-label="Крупная афиша"><img src={event.image_url} alt="" /></section><EventMediaRail assets={event.image_assets||[]} galleryId="behavior-media" eventTitle={event.title} maxVisible={3}/></main></body></html>
<style is:global>html{font-family:system-ui;background:#ece8e1}body{margin:0}main{width:min(calc(100% - 32px),760px);margin:24px auto}.primary{display:grid;place-items:center;min-height:420px;margin-bottom:16px;background:#ddd5ca}.primary img{max-width:100%;max-height:520px;object-fit:contain}</style>
`;

export function materializeBehaviorHarness({candidateRepo,harnessRoot,nodeModules,registry=buildBehaviorPacketRegistry()}){
  assertBehaviorPacketRegistry(registry);const source=resolve(candidateRepo);const root=assertDisposable(harnessRoot);const site=join(root,'site');
  if(!existsSync(join(source,'site/src/data/preview-events.json'))) throw new Error('Exact candidate repository missing');
  const head=spawnSync('git',['rev-parse','HEAD'],{cwd:source,encoding:'utf8'}).stdout.trim();if(head!==PINNED_SOURCE_SHA)throw new Error('Behavior harness source SHA mismatch');
  const clean=spawnSync('git',['status','--porcelain'],{cwd:source,encoding:'utf8'}).stdout.trim();if(clean)throw new Error('Exact candidate worktree is dirty');
  rmSync(root,{recursive:true,force:true});mkdirSync(root,{recursive:true});copySiteWithoutNodeModules(join(source,'site'),site);
  symlinkSync(join(source,'docs'),join(root,'docs'),'dir');
  const modules=resolve(nodeModules);if(!existsSync(join(modules,'astro/bin/astro.mjs'))||!existsSync(join(modules,'playwright/index.mjs')))throw new Error('Pinned-compatible Astro/Playwright node_modules missing');
  symlinkSync(modules,join(site,'node_modules'),'dir');mkdirSync(join(site,'src/pages/behavior-specimens'),{recursive:true});
  writeFileSync(join(site,'src/pages/behavior-specimens/time-nav.astro'),timeNavPage);
  writeFileSync(join(site,'src/pages/behavior-specimens/transport.astro'),transportPage);
  writeFileSync(join(site,'src/pages/behavior-specimens/media-rail.astro'),mediaRailPage);
  const generated=['time-nav.astro','transport.astro','media-rail.astro'].map((name)=>({path:`site/src/pages/behavior-specimens/${name}`,sha256:stableHash(readFileSync(join(site,'src/pages/behavior-specimens',name),'utf8'))}));
  const receipt={schema_version:registry.schema_version,status:'materialized-not-built',source_sha:PINNED_SOURCE_SHA,source_worktree_clean_before:true,source_copy_mode:'reflink-or-copy',production_source_mutated:false,generated_test_only_pages:generated,plan_count:registry.plans.length,normalization_allowed:false};
  writeFileSync(join(root,'behavior-harness-receipt.json'),`${JSON.stringify(receipt,null,2)}\n`);return {...receipt,root,site};
}
export function buildBehaviorHarness({harnessRoot,env={}}){
  const root=assertDisposable(harnessRoot);const site=join(root,'site');const astro=join(site,'node_modules/astro/bin/astro.mjs');
  const result=spawnSync(process.execPath,[astro,'build'],{cwd:site,env:cleanEnvironment(env),encoding:'utf8',maxBuffer:16*1024*1024});
  const receipt={ok:result.status===0,status:result.status,stdout_tail:result.stdout.slice(-6000),stderr_tail:result.stderr.slice(-6000),dist:join(site,'dist')};
  writeFileSync(join(root,'behavior-build-receipt.json'),`${JSON.stringify(receipt,null,2)}\n`);return receipt;
}
