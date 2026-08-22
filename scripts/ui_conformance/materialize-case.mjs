#!/usr/bin/env node
import { constants, cpSync, existsSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { dirname, join, parse, relative, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import { renderSpecimenPage } from '../current_ui_resource_graph/v1/specimens/materialize.mjs';
import { assertEventAssetsLocalized, resolveEventCardArchetypeContext, rewriteEventAssets } from './archetype-specimen.mjs';
import { assertImmutableCheckout, assertImmutableSha } from './immutable-checkout.mjs';

function parseArgs(argv) { const out={}; for(let i=0;i<argv.length;i+=1){const key=argv[i];if(!key.startsWith('--'))throw new Error(`Unexpected argument: ${key}`);const value=argv[i+1];if(!value||value.startsWith('--'))throw new Error(`${key} requires a value`);out[key.slice(2)]=value;i+=1;}return out; }
const sha=(value)=>createHash('sha256').update(value).digest('hex');
const read=(path)=>JSON.parse(readFileSync(resolve(path),'utf8'));
const args=parseArgs(process.argv.slice(2));
for(const key of ['resolved','astro-source-site','astro-source-sha','tooling-root','tooling-sha','harness','corpus-root','font-manifest']) if(!args[key]) throw new Error(`--${key} is required`);
const data=read(args.resolved); const site=resolve(args['astro-source-site']); const root=resolve(args.harness); const corpusRoot=resolve(args['corpus-root']);
const corpus=read(join(corpusRoot,'corpus.json')); const assets=read(join(corpusRoot,'assets-manifest.json')); const fontManifest=read(args['font-manifest']);
if(data.corpus_id!==corpus.corpus_id||data.corpus_sha256!==corpus.corpus_sha256) throw new Error('Resolved case/corpus identity mismatch');
if(data.asset_manifest_sha256!==assets.assets_manifest_sha256) throw new Error('Resolved case/asset manifest mismatch');
const fixture=corpus.fixtures.find((row)=>row.fixture_id===data.event_fixture_id); if(!fixture)throw new Error('Resolved fixture is absent from corpus');
const wrapper=read(join(corpusRoot,fixture.payload_path)); if(wrapper.preview_event_sha256!==data.event_payload_sha256)throw new Error('Frozen event payload hash binding mismatch');
const archetype=resolveEventCardArchetypeContext(data);
const fixtureIds=archetype?.input_fixture_ids||[fixture.fixture_id];
const fixtureRows=fixtureIds.map((fixtureId)=>{
  const metadata=corpus.fixtures.find((row)=>row.fixture_id===fixtureId);
  if(!metadata)throw new Error(`Archetype fixture is absent from corpus: ${fixtureId}`);
  const payload=read(join(corpusRoot,metadata.payload_path));
  if(payload.preview_event_sha256!==metadata.preview_event_sha256)throw new Error(`Frozen sibling payload hash binding mismatch: ${fixtureId}`);
  return {metadata,payload};
});
const astroSource=assertImmutableCheckout({root:resolve(site,'..'),expectedSha:args['astro-source-sha'],label:'Astro source checkout'});
const tooling=assertImmutableCheckout({root:args['tooling-root'],expectedSha:args['tooling-sha'],label:'Conformance tooling checkout'});
assertImmutableSha(fontManifest.astro_repository_sha,'Font manifest Astro repository SHA');
if(astroSource.sha!==fontManifest.astro_repository_sha) throw new Error(`Astro source/font manifest mismatch: ${astroSource.sha}`);
const rel=relative(root,site); if(root===parse(root).root||root.length<12||rel==='')throw new Error('Harness root must be a specific disposable path outside candidate source');
rmSync(root,{recursive:true,force:true}); mkdirSync(join(root,'src/pages/specimens'),{recursive:true});
copyTree(join(site,'src'),join(root,'upstream')); copyTree(join(site,'public'),join(root,'public'));
const modules=resolve(args['node-modules']||join(site,'node_modules')); if(!existsSync(modules))throw new Error('Exact candidate node_modules is missing'); symlinkSync(modules,join(root,'node_modules'),'dir');
const localAssets=new Map(); const verifiedAssets=[]; mkdirSync(join(root,'public/__ui-assets'),{recursive:true});
for(const asset of assets.assets.filter((row)=>fixtureIds.includes(row.fixture_id))){
  const target=join(root,'public/__ui-assets',`${asset.sha256}.${extension(asset.mime_type)}`); let bytes;
  if(existsSync(target))bytes=readFileSync(target);
  else if(asset.storage_mode==='git-content-addressed-bundle') bytes=readFileSync(join(corpusRoot,asset.bundle_relpath));
  else { const response=await fetch(asset.resolved_url); if(!response.ok)throw new Error(`Asset fetch failed ${response.status}: ${asset.resolved_url}`); bytes=Buffer.from(await response.arrayBuffer()); }
  if(sha(bytes)!==asset.sha256||bytes.length!==asset.byte_length)throw new Error(`BLOCKED_ASSET_MISMATCH: ${asset.asset_id}`);
  const localUrl=`/__ui-assets/${asset.sha256}.${extension(asset.mime_type)}`;
  writeFileSync(target,bytes); localAssets.set(asset.source_url,localUrl); localAssets.set(asset.resolved_url,localUrl);
  // Thumbnail CDN bytes are not independent corpus assets. Bind their URLs to
  // the verified full-resolution bytes for the same content hash so a copied
  // production component can never escape the immutable fixture boundary.
  const fixturePayload=fixtureRows.find(({metadata})=>metadata.fixture_id===asset.fixture_id)?.payload.preview_event;
  for(const eventAsset of fixturePayload?.image_assets||[]){
    if(eventAsset.current_pixel_sha256!==asset.sha256&&eventAsset.src!==asset.source_url&&eventAsset.src!==asset.resolved_url)continue;
    for(const derivative of eventAsset.thumbnail_sources||[]){if(derivative.src)localAssets.set(derivative.src,localUrl);if(derivative.url)localAssets.set(derivative.url,localUrl);}
  }
  verifiedAssets.push({asset_id:asset.asset_id,expected_sha256:asset.sha256,actual_sha256:sha(bytes),byte_length:bytes.length,storage_mode:asset.storage_mode});
}
const events=fixtureRows.map(({payload})=>assertEventAssetsLocalized(rewriteEventAssets(structuredClone(payload.preview_event),localAssets)));
const event=events.find((item)=>item.id===fixture.event_id); if(!event)throw new Error('Selected EventCard is absent from materialized archetype events');
mkdirSync(join(root,'public/__ui-fonts'),{recursive:true}); const fontSource=resolve(dirname(resolve(args['font-manifest'])),fontManifest.files[0].path); const fontBytes=readFileSync(fontSource);
if(sha(fontBytes)!==fontManifest.files[0].sha256)throw new Error('BLOCKED_FONT_ENV: font bytes mismatch'); writeFileSync(join(root,'public/__ui-fonts',fontManifest.files[0].filename),fontBytes);
writeFileSync(join(root,'package.json'),`${JSON.stringify({name:'ui-conformance-specimen',private:true,type:'module',scripts:{build:'astro build'}},null,2)}\n`);
writeFileSync(join(root,'astro.config.mjs'),"import { defineConfig } from 'astro/config';\nexport default defineConfig({ output:'static', trailingSlash:'always', vite:{ server:{ fs:{ strict:false } } } });\n");
const row=archetype?{
  id:data.case_id,renderer:'optimized-event-card-grid',
  source_paths:['src/components/OptimizedEventCardGrid.astro','src/components/EventCard.astro','src/lib/relatedCardLayout.mjs','src/layouts/EventLayout.astro'],
  props:{limit:archetype.limit,rowSize:archetype.row_size,mediaTreatment:archetype.media_treatment,surface:'event_detail_related'},
  root_selector:archetype.parent_selector,container:{width:archetype.container_width,height:'auto'},viewport:{width:archetype.viewport.width,height:archetype.viewport.height},
}:{id:data.case_id,renderer:'event-card',source_paths:['src/components/EventCard.astro','src/layouts/EventLayout.astro'],props:data.resolved_props,root_selector:'[data-event-card]',container:{width:data.container_geometry.container_width,height:'auto'},viewport:{width:data.container_geometry.viewport_width,height:data.container_geometry.viewport_height}};
const trace={fixture_catalog:corpus.corpus_id,fixture_id:fixture.fixture_id,event_id:fixture.event_id,preview_event_sha256:fixture.preview_event_sha256,resolved_render_case_sha256:data.resolved_render_case_sha256,assets_verified:true,archetype};
const fontCss=`@font-face{font-family:Inter;src:url('/__ui-fonts/${fontManifest.files[0].filename}') format('woff2');font-style:normal;font-weight:100 900;font-display:block}`;
const content=renderSpecimenPage(row,archetype?{events,selectedEventId:event.id,trace}:{event,trace}).replace('<style is:global>',`<style is:global>${fontCss}`);
const target=join(root,'src/pages/specimens',`${data.case_id}.astro`); writeFileSync(target,content);
const astro=join(root,'node_modules/astro/bin/astro.mjs'); const build=spawnSync(process.execPath,[astro,'build'],{cwd:root,encoding:'utf8',env:{...process.env,TZ:corpus.reference_clock.timezone,LANG:'ru_RU.UTF-8',PUBLIC_STATIC_SITE_CURRENT_DATE:corpus.reference_clock.current_date,PUBLIC_STATIC_SITE_REFERENCE_ISO:corpus.reference_clock.reference_iso,PUBLIC_TRANSPORT_TIMETABLE_EXPERIMENT_MODE:'off'}});
const receipt={schema_version:'ui_conformance_specimen_materialization_v2',case_id:data.case_id,resolved_render_case_sha256:data.resolved_render_case_sha256,contract_sha256:data.contract_sha256,corpus_id:corpus.corpus_id,corpus_sha256:corpus.corpus_sha256,event_fixture_id:fixture.fixture_id,event_payload_sha256:fixture.preview_event_sha256,event_fixture_bindings:fixtureRows.map(({metadata})=>({fixture_id:metadata.fixture_id,event_id:metadata.event_id,preview_event_sha256:metadata.preview_event_sha256})),asset_manifest_sha256:assets.assets_manifest_sha256,verified_assets:verifiedAssets,fixture_assets_localized:true,fixture_network_bound:false,font_manifest_sha256:fontManifest.font_manifest_sha256,astro_source_repository_sha:astroSource.sha,conformance_tooling_repository_sha:tooling.sha,frozen_clock:corpus.reference_clock,production_source_mutated:false,source_copy_mode:'exact-src-reflink-or-copy',route:`/specimens/${data.case_id}/`,root_selector:row.root_selector,selected_card_selector:archetype?.selected_card_selector||row.root_selector,archetype_context:archetype,ranking_supplied:false,production_route_placement_claimed:false,build_ok:build.status===0,stdout_tail:build.stdout.slice(-3000),stderr_tail:build.stderr.slice(-3000)};
writeFileSync(join(root,'specimen-materialization-receipt.json'),`${JSON.stringify(receipt,null,2)}\n`);process.stdout.write(`${JSON.stringify(receipt,null,2)}\n`);if(build.status!==0)process.exitCode=1;

function extension(mime){return mime==='image/png'?'png':mime==='image/jpeg'?'jpg':'webp';}
function copyTree(source,target){const result=spawnSync('cp',['-a','--reflink=auto',source,target],{encoding:'utf8'});if(result.status!==0)cpSync(source,target,{recursive:true,mode:constants.COPYFILE_FICLONE});}
