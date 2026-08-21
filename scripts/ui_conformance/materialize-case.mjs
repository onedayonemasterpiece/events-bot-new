#!/usr/bin/env node
import { constants, cpSync, existsSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { dirname, join, parse, relative, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import { renderSpecimenPage } from '../current_ui_resource_graph/v1/specimens/materialize.mjs';

function parseArgs(argv) { const out={}; for(let i=0;i<argv.length;i+=1){const key=argv[i];if(!key.startsWith('--'))throw new Error(`Unexpected argument: ${key}`);const value=argv[i+1];if(!value||value.startsWith('--'))throw new Error(`${key} requires a value`);out[key.slice(2)]=value;i+=1;}return out; }
const sha=(value)=>createHash('sha256').update(value).digest('hex');
const read=(path)=>JSON.parse(readFileSync(resolve(path),'utf8'));
const args=parseArgs(process.argv.slice(2));
for(const key of ['resolved','site','harness','corpus-root','font-manifest']) if(!args[key]) throw new Error(`--${key} is required`);
const data=read(args.resolved); const site=resolve(args.site); const root=resolve(args.harness); const corpusRoot=resolve(args['corpus-root']);
const corpus=read(join(corpusRoot,'corpus.json')); const assets=read(join(corpusRoot,'assets-manifest.json')); const fontManifest=read(args['font-manifest']);
if(data.corpus_id!==corpus.corpus_id||data.corpus_sha256!==corpus.corpus_sha256) throw new Error('Resolved case/corpus identity mismatch');
if(data.asset_manifest_sha256!==assets.assets_manifest_sha256) throw new Error('Resolved case/asset manifest mismatch');
const fixture=corpus.fixtures.find((row)=>row.fixture_id===data.event_fixture_id); if(!fixture)throw new Error('Resolved fixture is absent from corpus');
const wrapper=read(join(corpusRoot,fixture.payload_path)); if(wrapper.preview_event_sha256!==data.event_payload_sha256)throw new Error('Frozen event payload hash binding mismatch');
const exactHead=spawnSync('git',['rev-parse','HEAD'],{cwd:resolve(site,'..'),encoding:'utf8'}).stdout.trim();
if(exactHead!==fontManifest.astro_repository_sha) throw new Error(`Astro candidate SHA mismatch: ${exactHead}`);
const rel=relative(root,site); if(root===parse(root).root||root.length<12||rel==='')throw new Error('Harness root must be a specific disposable path outside candidate source');
rmSync(root,{recursive:true,force:true}); mkdirSync(join(root,'src/pages/specimens'),{recursive:true});
copyTree(join(site,'src'),join(root,'upstream')); copyTree(join(site,'public'),join(root,'public'));
const modules=resolve(args['node-modules']||join(site,'node_modules')); if(!existsSync(modules))throw new Error('Exact candidate node_modules is missing'); symlinkSync(modules,join(root,'node_modules'),'dir');
const localAssets=new Map(); const verifiedAssets=[]; mkdirSync(join(root,'public/__ui-assets'),{recursive:true});
for(const asset of assets.assets.filter((row)=>row.fixture_id===fixture.fixture_id)){
  const target=join(root,'public/__ui-assets',`${asset.sha256}.${extension(asset.mime_type)}`); let bytes;
  if(asset.storage_mode==='git-content-addressed-bundle') bytes=readFileSync(join(corpusRoot,asset.bundle_relpath));
  else { const response=await fetch(asset.resolved_url); if(!response.ok)throw new Error(`Asset fetch failed ${response.status}: ${asset.resolved_url}`); bytes=Buffer.from(await response.arrayBuffer()); }
  if(sha(bytes)!==asset.sha256||bytes.length!==asset.byte_length)throw new Error(`BLOCKED_ASSET_MISMATCH: ${asset.asset_id}`);
  writeFileSync(target,bytes); localAssets.set(asset.source_url,`/__ui-assets/${asset.sha256}.${extension(asset.mime_type)}`); localAssets.set(asset.resolved_url,`/__ui-assets/${asset.sha256}.${extension(asset.mime_type)}`);
  verifiedAssets.push({asset_id:asset.asset_id,expected_sha256:asset.sha256,actual_sha256:sha(bytes),byte_length:bytes.length,storage_mode:asset.storage_mode});
}
const event=structuredClone(wrapper.preview_event); rewriteEventAssets(event,localAssets);
mkdirSync(join(root,'public/__ui-fonts'),{recursive:true}); const fontSource=resolve(dirname(resolve(args['font-manifest'])),fontManifest.files[0].path); const fontBytes=readFileSync(fontSource);
if(sha(fontBytes)!==fontManifest.files[0].sha256)throw new Error('BLOCKED_FONT_ENV: font bytes mismatch'); writeFileSync(join(root,'public/__ui-fonts',fontManifest.files[0].filename),fontBytes);
writeFileSync(join(root,'package.json'),`${JSON.stringify({name:'ui-conformance-specimen',private:true,type:'module',scripts:{build:'astro build'}},null,2)}\n`);
writeFileSync(join(root,'astro.config.mjs'),"import { defineConfig } from 'astro/config';\nexport default defineConfig({ output:'static', trailingSlash:'always', vite:{ server:{ fs:{ strict:false } } } });\n");
const row={id:data.case_id,renderer:'event-card',source_paths:['src/components/EventCard.astro','src/layouts/EventLayout.astro'],props:data.resolved_props,root_selector:'[data-event-card]',container:{width:data.container_geometry.container_width,height:'auto'},viewport:{width:data.container_geometry.viewport_width,height:data.container_geometry.viewport_height}};
const trace={fixture_catalog:corpus.corpus_id,fixture_id:fixture.fixture_id,event_id:fixture.event_id,preview_event_sha256:fixture.preview_event_sha256,resolved_render_case_sha256:data.resolved_render_case_sha256,assets_verified:true};
const fontCss=`@font-face{font-family:Inter;src:url('/__ui-fonts/${fontManifest.files[0].filename}') format('woff2');font-style:normal;font-weight:100 900;font-display:block}`;
const content=renderSpecimenPage(row,{event,trace}).replace('<style is:global>',`<style is:global>${fontCss}`);
const target=join(root,'src/pages/specimens',`${data.case_id}.astro`); writeFileSync(target,content);
const astro=join(root,'node_modules/astro/bin/astro.mjs'); const build=spawnSync(process.execPath,[astro,'build'],{cwd:root,encoding:'utf8',env:{...process.env,TZ:corpus.reference_clock.timezone,LANG:'ru_RU.UTF-8',PUBLIC_STATIC_SITE_CURRENT_DATE:corpus.reference_clock.current_date,PUBLIC_STATIC_SITE_REFERENCE_ISO:corpus.reference_clock.reference_iso,PUBLIC_TRANSPORT_TIMETABLE_EXPERIMENT_MODE:'off'}});
const receipt={schema_version:'ui_conformance_specimen_materialization_v1',case_id:data.case_id,resolved_render_case_sha256:data.resolved_render_case_sha256,corpus_id:corpus.corpus_id,event_fixture_id:fixture.fixture_id,event_payload_sha256:fixture.preview_event_sha256,asset_manifest_sha256:assets.assets_manifest_sha256,verified_assets:verifiedAssets,font_manifest_sha256:fontManifest.font_manifest_sha256,astro_repository_sha:exactHead,frozen_clock:corpus.reference_clock,production_source_mutated:false,source_copy_mode:'exact-src-reflink-or-copy',route:`/specimens/${data.case_id}/`,root_selector:row.root_selector,build_ok:build.status===0,stdout_tail:build.stdout.slice(-3000),stderr_tail:build.stderr.slice(-3000)};
writeFileSync(join(root,'specimen-materialization-receipt.json'),`${JSON.stringify(receipt,null,2)}\n`);process.stdout.write(`${JSON.stringify(receipt,null,2)}\n`);if(build.status!==0)process.exitCode=1;

function extension(mime){return mime==='image/png'?'png':mime==='image/jpeg'?'jpg':'webp';}
function copyTree(source,target){const result=spawnSync('cp',['-a','--reflink=auto',source,target],{encoding:'utf8'});if(result.status!==0)cpSync(source,target,{recursive:true,mode:constants.COPYFILE_FICLONE});}
function rewriteEventAssets(event,map){for(const asset of event.image_assets||[]){const value=map.get(asset.url)||map.get(asset.source_url);if(value){asset.url=value;if(asset.source_url)asset.source_url=value;}}if(event.image_url&&map.has(event.image_url))event.image_url=map.get(event.image_url);if(event.image_source_url&&map.has(event.image_source_url))event.image_source_url=map.get(event.image_source_url);}
