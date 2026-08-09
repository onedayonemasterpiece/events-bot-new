import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { buildBehaviorPacketRegistry } from './registry.mjs';
import { assertBehaviorCaptureComplete, assertBehaviorObservation, assertBehaviorPacketRegistry } from './validate.mjs';
import { captureStableLocatorPng, captureStablePagePng, collectBoundedElementFacts, loadPinnedPlaywrightImageComparator, safeCapturedValue, startSpecimenServer } from '../specimens/capture.mjs';

const sha=(value)=>createHash('sha256').update(value).digest('hex');
const wait=(ms)=>new Promise((done)=>setTimeout(done,ms));
function profile({mature=false,liked_event_ids=[],hidden_event_ids=[]}={}){return {consent_ok:true,profile_version:'anon-profile-v1',feature_schema_version:'event-detail-related-v1',taxonomy_version:'event-taxonomy-v1',anon_id:'11111111-1111-4111-8111-111111111111',session_id:'22222222-2222-4222-8222-222222222222',positive_tags:mature?{music:3,art:2}:{},negative_interest_tags:{},liked_event_ids,not_interested_event_ids:[],hidden_event_ids,share_counts:{}};}
function telemetry(page){const value={console_counts:{},console_hashes:[],resource_counts:{},status_counts:{},failed_count:0};page.on('console',(m)=>{value.console_counts[m.type()]=(value.console_counts[m.type()]||0)+1;value.console_hashes.push(sha(m.text()));});page.on('request',(r)=>{const type=r.resourceType();value.resource_counts[type]=(value.resource_counts[type]||0)+1;});page.on('response',(r)=>{const status=String(r.status());value.status_counts[status]=(value.status_counts[status]||0)+1;});page.on('requestfailed',()=>{value.failed_count+=1;});return value;}
export async function installSearchRuntime(page){
  await page.addInitScript(()=>{
    const encode=(value)=>btoa(JSON.stringify(value)).replace(/=+$/u,'').replace(/\+/gu,'-').replace(/\//gu,'_');const now=Math.floor(Date.now()/1000);
    const user={id:'11111111-2222-4333-8444-555555555555',aud:'authenticated',role:'authenticated',email:'decoder@example.invalid',app_metadata:{provider:'custom:yandex',providers:['custom:yandex']},user_metadata:{},created_at:'2026-08-08T00:00:00.000Z'};
    const session={access_token:`${encode({alg:'none',typ:'JWT'})}.${encode({aud:'authenticated',exp:now+3600,iat:now,sub:user.id,email:user.email,role:'authenticated'})}.c2lnbmF0dXJl`,refresh_token:'decoder-refresh',expires_in:3600,expires_at:now+3600,token_type:'bearer',user};
    const json=(value,status=200)=>new Response(JSON.stringify(value),{status,headers:{'content-type':'application/json; charset=utf-8'}});
    const nativeFetch=window.fetch.bind(window);let pending=[];
    window.__behaviorReleaseSearch=(result)=>{const current=pending.shift();if(!current)return false;const display={id:7003,event_id:7003,title:'Хоровое многоголосие',href:'/sobytiya/horovoe-mnogogolosie/',event_type:'концерт',display_date:'31 июля',display_time:'19:00',display_date_time:'31 июля · 19:00',city:'Калининград',place:'Калининград · Филармония',status_label:'По билетам',likes_count:0,shares_count:0};
      if(result==='error')current.resolve(json({error:'provider_unavailable'},503));else current.resolve(json({schema_version:'event-search-results-v1',surface:'authorized_event_search',algorithm_id:'behavior-decoder',request_id:'behavior-request',served_list_id:'behavior-served',served_list_hash:'behavior-hash',items:result==='empty'?[]:[{id:7003,event_id:7003,title:display.title,semantic_score:.93,display}],fallback_items:[],has_more:false,next_offset:result==='empty'?0:1,quota:{day_remaining:7}}));return true;};
    window.fetch=(input,init={})=>{const url=String(input instanceof Request?input.url:input);if(url.includes('/auth/v1/health'))return Promise.resolve(json({version:'test'}));if(url.endsWith('/functions/v1/transport-probe')){let nonce='';try{nonce=JSON.parse(String(init?.body||'{}')).nonce||'';}catch{}return Promise.resolve(json({schema:1,nonce}));}if(url.includes('/auth/v1/token?grant_type=pkce'))return Promise.resolve(json(session));if(url.endsWith('/auth/v1/user'))return Promise.resolve(json(user));if(url.includes('/auth/v1/logout'))return Promise.resolve(new Response(null,{status:204}));if(url.endsWith('/rest/v1/rpc/get_event_search_quota_v1'))return Promise.resolve(json([{day_remaining:8,month_remaining:30}]));if(url.endsWith('/functions/v1/event-search'))return new Promise((resolvePending)=>pending.push({resolve:resolvePending}));return nativeFetch(input,init);};
    localStorage.setItem('sb-example-auth-token-code-verifier',JSON.stringify('decoder-code-verifier'));
  });
}
async function installBeforeNavigation(page,context,plan){
  for(const action of plan.before_navigation||[]){
    if(action.kind==='seed-profile') await context.addInitScript((value)=>localStorage.setItem('ke_personalization_profile',JSON.stringify(value)),profile(action));
    else if(action.kind==='set-listing-mode') await context.addInitScript((value)=>localStorage.setItem('ke_listing_mode_v1',value),action.mode);
    else if(action.kind==='select-treatment') await context.addInitScript((value)=>{window.__BEHAVIOR_TREATMENT__=value;document.documentElement.style.setProperty('--behavior-container',`${innerWidth}px`);},action.treatment);
  }
  if(plan.runtime_profile?.startsWith('search-')) await installSearchRuntime(page);
}
function deferredRoute(page,pattern){let release=null;const ready=new Promise((done)=>{release=done;});page.route(pattern,async(route)=>{const result=await ready;if(result==='error')await route.abort('failed');else await route.continue();});return (result)=>release(result);}
async function installNetwork(page,plan){
  const controls={};
  if(plan.runtime_profile?.startsWith('favorites-')||plan.runtime_profile?.startsWith('personal-feed-')) controls.releaseData=deferredRoute(page,'**/data/personal-feed.json*');
  if(plan.network_profile==='deferred-images'){
    let release=null;const ready=new Promise((done)=>{release=done;});controls.releaseImages=release;
    await page.route('https://static.kenigevents.ru/**',async(route)=>{const result=await ready;if(result==='error')await route.abort('failed');else await route.continue();});
  }
  return controls;
}
async function target(root,page,action){if(action.scope==='page')return page.locator(action.selector);if(!action.selector)return root;return root.locator(action.selector);}
async function applyAction({page,root,action,controls}){
  if(action.kind==='click'){const loc=await target(root,page,action);if(action.optional&&await loc.count()===0)return;await loc.first().click();}
  else if(action.kind==='focus')await root.focus();
  else if(action.kind==='press')await page.keyboard.press(action.key);
  else if(action.kind==='scroll-element'){const loc=action.selector?await target(root,page,action):root;await loc.evaluate((node,edge)=>{node.scrollLeft=edge==='middle'?(node.scrollWidth-node.clientWidth)/2:edge==='end'?node.scrollWidth-node.clientWidth:0;node.scrollTop=edge==='end'?node.scrollHeight-node.clientHeight:0;},action.edge);}
  else if(action.kind==='scroll-window')await page.evaluate((value)=>scrollTo(0,value.edge==='end'?document.documentElement.scrollHeight:value.y||0),action);
  else if(action.kind==='scroll-to-selector'){const loc=page.locator(action.selector);if(action.optional&&await loc.count()===0)return;await loc.first().evaluate((node,offset)=>scrollTo(0,node.getBoundingClientRect().top+scrollY+offset),action.offset||0);}
  else if(action.kind==='mock-clipboard')await page.evaluate((result)=>{Object.defineProperty(navigator,'clipboard',{configurable:true,value:{writeText:()=>result==='success'?Promise.resolve():Promise.reject(new Error('controlled clipboard failure'))}});document.execCommand=()=>false;},action.result);
  else if(action.kind==='search-submit'){await page.locator('[data-search-input]').fill(action.query);await page.locator('[data-search-form]').evaluate((node)=>node.requestSubmit());}
  else if(action.kind==='release-search')await page.evaluate((result)=>window.__behaviorReleaseSearch?.(result),action.result);
  else if(action.kind==='release-favorites'||action.kind==='release-personal-feed')controls.releaseData?.(action.result);
  else if(action.kind==='release-images')controls.releaseImages?.(action.result);
  else throw new Error(`Unsupported behavioral action: ${action.kind}`);
  await page.evaluate(()=>new Promise((done)=>requestAnimationFrame(()=>requestAnimationFrame(done))));
}
async function waitForPhase(page,plan,phase){
  const checks={
    loading:(runtimeProfile)=>runtimeProfile.startsWith('search-')
      ? document.querySelector('[data-search-skeletons]')?.hidden===false
      : runtimeProfile.startsWith('favorites-')
        ? document.querySelector('[data-favorites-surface]')?.dataset.favoritesState==='loading'
        : true,
    result:(runtimeProfile)=>runtimeProfile.startsWith('search-')
      ? document.querySelector('[data-search-results]')?.hidden===false
      : runtimeProfile.startsWith('favorites-')
        ? document.querySelector('[data-favorites-surface]')?.dataset.favoritesState==='ready'
        : true,
    empty:(runtimeProfile)=>runtimeProfile.startsWith('search-')
      ? document.querySelector('[data-authorized-search]')?.dataset.searchState==='empty'
      : runtimeProfile.startsWith('favorites-')
        ? document.querySelector('[data-favorites-empty]')?.hidden===false
        : true,
    error:(runtimeProfile)=>runtimeProfile.startsWith('search-')
      ? document.querySelector('[data-authorized-search]')?.dataset.searchState==='error'||document.querySelector('[data-search-status]')?.getAttribute('role')==='alert'
      : runtimeProfile.startsWith('favorites-')
        ? document.querySelector('[data-favorites-surface]')?.dataset.favoritesState==='error'
        : true,
    'retry-loading':()=>document.querySelector('[data-search-skeletons]')?.hidden===false,
    'phone-copied':()=>/скопирован|скопировано|готово/iu.test(document.querySelector('[data-desktop-phone-status]')?.textContent||''),
    'phone-copy-error':()=>/не удалось|ошиб/iu.test(document.querySelector('[data-desktop-phone-status]')?.textContent||''),
    personal:(runtimeProfile)=>!runtimeProfile.startsWith('personal-feed-')||document.querySelector('[data-personal-feed-section]')?.dataset.personalFeedMode==='personal',
    'popular-fallback':(runtimeProfile)=>!runtimeProfile.startsWith('personal-feed-')||document.querySelector('[data-personal-feed-section]')?.dataset.personalFeedMode==='popular_fallback',
  };
  if(checks[phase]){try{await page.waitForFunction(checks[phase],plan.runtime_profile||'',{timeout:12000});}catch(error){const state=await page.evaluate(()=>({search:document.querySelector('[data-authorized-search]')?.dataset.searchState,status:document.querySelector('[data-search-status]')?.textContent,role:document.querySelector('[data-search-status]')?.getAttribute('role'),resultsHidden:document.querySelector('[data-search-results]')?.hidden,favorites:document.querySelector('[data-favorites-surface]')?.dataset.favoritesState,personal:document.querySelector('[data-personal-feed-section]')?.dataset.personalFeedMode}));throw new Error(`Behavior phase did not settle: ${plan.id}/${phase}: ${error.message}; state=${JSON.stringify(state)}`);}}
  if(plan.id.includes('favorites-loading-')&&phase!=='loading'){
    const expected=phase==='result'||phase==='empty'?'ready':phase;try{await page.waitForFunction((value)=>document.querySelector('[data-favorites-surface]')?.dataset.favoritesState===value,expected,{timeout:12000});}catch(error){throw new Error(`Favorites phase did not settle: ${phase}`);}
  }
  await wait(50);
}
async function stateFacts(page,root){
  const basic=await page.evaluate(()=>({window_scroll:{x:scrollX,y:scrollY},document:{scroll_width:document.documentElement.scrollWidth,scroll_height:document.documentElement.scrollHeight,client_width:document.documentElement.clientWidth,client_height:document.documentElement.clientHeight},visual_viewport:visualViewport?{width:visualViewport.width,height:visualViewport.height,offset_left:visualViewport.offsetLeft,offset_top:visualViewport.offsetTop,scale:visualViewport.scale}:null,focus_owner:document.activeElement?{tag:document.activeElement.tagName.toLowerCase(),role:document.activeElement.getAttribute('role'),data_names:[...document.activeElement.attributes].map((item)=>item.name).filter((name)=>name.startsWith('data-')).slice(0,20),aria:Object.fromEntries([...document.activeElement.attributes].filter((item)=>item.name.startsWith('aria-')).slice(0,20).map((item)=>[item.name,item.value])),text_length:(document.activeElement.textContent||'').length}:null}));
  const scroll=await root.evaluate((node)=>({scroll_left:node.scrollLeft,scroll_top:node.scrollTop,client_width:node.clientWidth,client_height:node.clientHeight,scroll_width:node.scrollWidth,scroll_height:node.scrollHeight}));return {...basic,root_scroll:scroll};
}
async function capturePhase({page,root,plan,phase,outputDir,imageComparator,telemetryState,index}){
  await page.evaluate(()=>document.fonts?.ready);const name=`${plan.id.slice('behavior-packet.'.length)}-${String(index).padStart(2,'0')}-${phase}.png`;const path=join(outputDir,'behavior-rasters',name);
  const screenshot=plan.capture_scope==='page'?await captureStablePagePng({page,path,imageComparator,label:`Behavior ${plan.id}/${phase}`}):await captureStableLocatorPng({locator:root,path,imageComparator,label:`Behavior ${plan.id}/${phase}`});
  const facts=await collectBoundedElementFacts(root,[]);let aria;try{aria=safeCapturedValue(await root.ariaSnapshot({timeout:3000}),6000);}catch(error){aria={unavailable:true,error_class:error.constructor?.name||'Error'};}
  const state=await stateFacts(page,root);const row={schema_version:plan.schema_version,id:`behavior-observation.${sha(`${plan.id}\0${phase}`).slice(0,18)}`,plan_id:plan.id,family:plan.family,phase,sequence_index:index,source_sha:plan.source_sha,evidence_plane:plan.evidence_plane,route_hash:sha(plan.route),viewport:plan.viewport,container:{planned_width:plan.container_width||null,actual_width:facts.geometry.width,actual_height:facts.geometry.height},reduced_motion:plan.reduced_motion,ratios:plan.ratios||[],actions_since_prior_state:[],state,dom:{tag:facts.tag,classes:facts.classes,attributes:facts.attributes,redacted_attribute_names:facts.redacted_attribute_names,child_count:facts.child_count,text_length:facts.text_length,text_sha256:facts.text_sha256,full_html_retained:false},accessibility:{aria_snapshot:aria,...facts.state},computed:facts.computed,geometry:facts.geometry,css_variables:facts.css_variables,pseudo:facts.pseudo,media:facts.media,media_queries:facts.media_queries,cascade:facts.cascade,loaded_fonts:facts.loaded_fonts,screenshot:{path:`behavior-rasters/${name}`,...screenshot},network:{counts_by_resource_type:telemetryState.resource_counts,response_status_counts:telemetryState.status_counts,failed_count:telemetryState.failed_count,raw_urls_retained:false},console:{counts:telemetryState.console_counts,message_hashes:telemetryState.console_hashes.slice(0,20),message_text_retained:false},evidence_status:'captured-not-reviewed',review_status:'pending-human-full-resolution-review',production_state_claimed:false,normalization_allowed:false,decision:'NOT_MERGED'};assertBehaviorObservation(row);return row;
}
export async function captureBehaviorPackets({browser,baseUrl,outputDir,nodeModules,registry=buildBehaviorPacketRegistry(),onlyIds=null}){
  assertBehaviorPacketRegistry(registry);mkdirSync(join(outputDir,'behavior-rasters'),{recursive:true});const observations=[];const blockers=[];const imageComparator=loadPinnedPlaywrightImageComparator(nodeModules,'image/png');
  const selected=registry.plans.filter((row)=>!onlyIds||onlyIds.includes(row.id));
  for(const plan of selected){
    if(plan.execution_status==='explicit-blocker'){blockers.push({schema_version:plan.schema_version,id:`behavior-blocker.${sha(plan.id).slice(0,18)}`,plan_id:plan.id,family:plan.family,reason:plan.blocker_reason,source_path:plan.source_path||null,status:'explicit-blocker',production_state_claimed:false,decision:'NOT_MERGED'});continue;}
    const context=await browser.newContext({viewport:plan.viewport,reducedMotion:plan.reduced_motion?'reduce':'no-preference',deviceScaleFactor:plan.device_scale_factor,timezoneId:'Europe/Kaliningrad'});
    await context.addInitScript(({fixedNow})=>{const NativeDate=Date;class FixedDate extends NativeDate{constructor(...args){super(...(args.length?args:[fixedNow]));}static now(){return fixedNow;}}Object.setPrototypeOf(FixedDate,NativeDate);globalThis.Date=FixedDate;let state=0x6d2b79f5;Math.random=()=>{state=(Math.imul(state^state>>>15,1|state)+Math.imul(state^state>>>7,61|state)^state)>>>0;return((state^state>>>14)>>>0)/4294967296;};},{fixedNow:Date.parse('2026-08-08T12:48:42.000Z')});
    const page=await context.newPage();const telemetryState=telemetry(page);await installBeforeNavigation(page,context,plan);const controls=await installNetwork(page,plan);
    try{
      const searchQuery=plan.runtime_profile?.startsWith('search-')?'?code=behavior-code':'';await page.goto(`${baseUrl}${plan.route}${searchQuery}`,{waitUntil:'domcontentloaded',timeout:30000});await page.waitForLoadState('networkidle',{timeout:3000}).catch(()=>{});await page.addStyleTag({content:'*,*::before,*::after{animation:none!important;transition:none!important;caret-color:transparent!important;scroll-behavior:auto!important}'});
      const candidates=page.locator(plan.root_selector);const root=(plan.capture_scope==='locator'?candidates.filter({visible:true}):candidates).first();await root.waitFor({state:plan.capture_scope==='locator'?'visible':'attached',timeout:15000});
      for(let index=0;index<plan.steps.length;index+=1){const phase=plan.steps[index];for(const action of phase.actions)await applyAction({page,root,action,controls});await waitForPhase(page,plan,phase.phase);if(phase.capture){const row=await capturePhase({page,root,plan,phase:phase.phase,outputDir,imageComparator,telemetryState,index});row.actions_since_prior_state=phase.actions;observations.push(row);}}
    }finally{await context.close();}
  }
  writeFileSync(join(outputDir,'behavior-specimen-observations.jsonl'),`${observations.map((row)=>JSON.stringify(row)).join('\n')}\n`);writeFileSync(join(outputDir,'behavior-capture-blockers.jsonl'),`${blockers.map((row)=>JSON.stringify(row)).join('\n')}\n`);
  if(!onlyIds)assertBehaviorCaptureComplete(registry,observations,blockers);return {observations,blockers};
}
export async function captureBehaviorWithExactPlaywright({nodeModules,dist,outputDir,registry=buildBehaviorPacketRegistry(),onlyIds=null}){
  const entry=join(resolve(nodeModules),'playwright/index.mjs');if(!existsSync(entry))throw new Error('Exact Playwright entrypoint missing');const {chromium}=await import(pathToFileURL(entry).href);const server=await startSpecimenServer({dist});const browser=await chromium.launch({headless:true});
  try{return await captureBehaviorPackets({browser,baseUrl:server.baseUrl,outputDir,nodeModules,registry,onlyIds});}finally{await browser.close();await server.close();}
}
