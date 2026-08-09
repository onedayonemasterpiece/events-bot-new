import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { buildBehaviorPacketRegistry } from './registry.mjs';
import { assertBehaviorCaptureComplete, assertBehaviorObservation, assertBehaviorPacketRegistry } from './validate.mjs';
import { captureStableLocatorPng, collectBoundedElementFacts, loadPinnedPlaywrightImageComparator, pngDifferenceHash, safeCapturedValue, startSpecimenServer } from '../specimens/capture.mjs';
import { capturePlaywrightStablePair } from '../evidence.mjs';

const sha=(value)=>createHash('sha256').update(value).digest('hex');
const wait=(ms)=>new Promise((done)=>setTimeout(done,ms));
const FONT_SETTLE_TIMEOUT_MS=4000;
const SCREENSHOT_TIMEOUT_MS=30000;
const CONTROLLED_ROUTE_TIMEOUT_MS=20000;
// Playwright otherwise re-waits without a bound inside screenshot() even
// after our explicit document.fonts.ready guard has timed out.  The packet
// records that timeout and captures the actually rendered fallback instead.
process.env.PW_TEST_SCREENSHOT_NO_FONTS_READY='1';
async function bounded(label,promise,timeoutMs){let timer;try{return await Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error(`${label} exceeded ${timeoutMs}ms`)),timeoutMs);})]);}finally{clearTimeout(timer);}}
async function settleFontsBounded(page){try{await bounded('font settle',page.evaluate(()=>document.fonts?.ready),FONT_SETTLE_TIMEOUT_MS);return {status:'ready',timeout_ms:FONT_SETTLE_TIMEOUT_MS};}catch{return {status:'timed-out-continued',timeout_ms:FONT_SETTLE_TIMEOUT_MS};}}
async function captureStablePagePngBounded({page,path,imageComparator,label}){
  const viewport=page.viewportSize();if(!viewport?.width||!viewport?.height)throw new Error(`${label} has no fixed viewport`);
  await page.evaluate(()=>new Promise((done)=>requestAnimationFrame(()=>requestAnimationFrame(done))));
  const options={type:'png',animations:'disabled',caret:'hide',scale:'css',fullPage:false,timeout:12000};
  const pair=await bounded(`${label} stable page screenshot`,capturePlaywrightStablePair({capture:()=>page.screenshot(options),comparator:imageComparator,label}),SCREENSHOT_TIMEOUT_MS);
  mkdirSync(dirname(resolve(path)),{recursive:true});writeFileSync(path,pair.accepted);
  return {bytes:pair.accepted.length,sha256:sha(pair.accepted),dhash:pngDifferenceHash(pair.accepted),first_sha256:sha(pair.first),first_dhash:pngDifferenceHash(pair.first),width:viewport.width,height:viewport.height,exact_stable:pair.first.equals(pair.accepted),perceptually_stable:true,stability_attempts:pair.attempts,comparator:pair.comparator};
}
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
function deferredRoute(page,pattern,{resourceType=null,fulfill=null}={}){
  let release=null;let settled=false;const ready=new Promise((done)=>{release=(result)=>{if(settled)return false;settled=true;done(result);return true;};});
  page.route(pattern,async(route)=>{
    if(resourceType&&route.request().resourceType()!==resourceType){await route.continue();return;}
    const result=await Promise.race([ready,wait(CONTROLLED_ROUTE_TIMEOUT_MS).then(()=>'controlled-timeout')]);
    if(result==='success'&&fulfill){await fulfill(route);return;}
    if(result==='error'||result==='controlled-timeout'){await route.abort('failed');return;}
    await route.continue();
  });
  return release;
}
const weatherSnapshot=JSON.stringify({schema:'weather-calendar-v1',snapshot_id:'weather-behavior-20260808',generated_at:'2026-08-08T12:00:00.000Z',valid_until:'2026-08-09T12:00:00.000Z',timezone:'Europe/Kaliningrad',provider:{name:'Open-Meteo',attribution_url:'https://open-meteo.com/'},location_revision:'behavior-v1',days:[{date:'2026-08-08',kaliningrad:{status:'fresh',temperature_day_min_c:17,temperature_day_max_c:23,weather_code:2,wind_day_max_m_s:5.2,source_updated_at:'2026-08-08T12:00:00.000Z'},coast:{status:'degraded',temperature_day_min_c:16,temperature_day_max_c:21,weather_code:3,wind_day_max_m_s:7.1,sea_surface_temperature_c:18.4,show_water_temperature:true,wave_height_day_max_m:0.8,source_updated_at:'2026-08-08T12:00:00.000Z'}}],errors:[]});
const weatherSnapshotSha=sha(weatherSnapshot);
async function fulfillWeather(route){const pathname=new URL(route.request().url()).pathname;if(pathname.endsWith('/current.json')){const pointer={schema:'weather-calendar-pointer-v1',snapshot_id:'weather-behavior-20260808',snapshot_url:`/data/weather/v1/snapshots/${weatherSnapshotSha}.json`,sha256:weatherSnapshotSha,updated_at:'2026-08-08T12:00:00.000Z'};await route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(pointer)});}else await route.fulfill({status:200,contentType:'application/json',body:weatherSnapshot});}
async function installNetwork(page,plan){
  const controls={route_timeout_ms:CONTROLLED_ROUTE_TIMEOUT_MS};
  if(plan.runtime_profile?.startsWith('favorites-')||plan.runtime_profile?.startsWith('personal-feed-')) controls.releaseData=deferredRoute(page,'**/data/personal-feed.json*');
  if(plan.network_profile?.startsWith('discovery-')) controls.releaseDiscovery=deferredRoute(page,'**/data/discovery/*.json*');
  if(plan.network_profile?.startsWith('weather-')) controls.releaseWeather=deferredRoute(page,'**/data/weather/v1/**',{fulfill:fulfillWeather});
  if(plan.network_profile?.startsWith('gallery-')) controls.releaseGalleryImage=deferredRoute(page,'https://static.kenigevents.ru/**',{resourceType:'image'});
  if(plan.network_profile==='deferred-images'){
    // Only image requests are held. The cancelled Actions run proved that
    // holding every static CDN request also held fonts and made fonts.ready
    // unbounded during the first loading-state screenshot.
    controls.releaseImages=deferredRoute(page,'https://static.kenigevents.ru/**',{resourceType:'image'});
  }
  return controls;
}
async function target(root,page,action){if(action.scope==='page')return page.locator(action.selector);if(!action.selector)return root;return root.locator(action.selector);}
async function assertActionTarget(page,root,action){
  if(action.target_requirement==='not-applicable-controlled-runtime'||action.target_requirement==='required-viewport')return {requirement:action.target_requirement,status:'not-applicable'};
  if(action.target_requirement==='required-active-element'){
    const active=await page.locator(':focus').count();if(active<1)throw new Error(`Behavior action requires an active element: ${action.kind}/${action.key||''}`);
    return {requirement:action.target_requirement,status:'resolved',count:active};
  }
  const loc=await target(root,page,action);const count=await loc.count();if(count<1)throw new Error(`Behavior action target missing: ${action.kind}/${action.selector||':root'}`);
  if(action.kind==='click'){const visibleCount=await loc.filter({visible:true}).count();if(visibleCount<1)throw new Error(`Behavior click target is not visible: ${action.selector||':root'}`);return {requirement:action.target_requirement,status:'resolved-visible',count,visible_count:visibleCount};}
  return {requirement:action.target_requirement,status:'resolved',count};
}
async function applyAction({page,root,action,controls}){
  const targetReceipt=await assertActionTarget(page,root,action);
  if(action.kind==='click'){const loc=await target(root,page,action);await loc.filter({visible:true}).first().click();}
  else if(action.kind==='focus')await root.focus();
  else if(action.kind==='focus-scroll-target'){await root.evaluate((node)=>{if(!node.hasAttribute('tabindex')){node.setAttribute('tabindex','0');node.setAttribute('data-behavior-focus-instrumented','true');}node.focus({preventScroll:true});});}
  else if(action.kind==='press')await page.keyboard.press(action.key);
  else if(action.kind==='scroll-element'){const loc=action.selector?await target(root,page,action):root;await loc.evaluate((node,edge)=>{node.scrollLeft=edge==='middle'?(node.scrollWidth-node.clientWidth)/2:edge==='end'?node.scrollWidth-node.clientWidth:0;node.scrollTop=edge==='end'?node.scrollHeight-node.clientHeight:0;},action.edge);}
  else if(action.kind==='scroll-window')await page.evaluate((value)=>scrollTo(0,value.edge==='end'?document.documentElement.scrollHeight:value.y||0),action);
  else if(action.kind==='scroll-to-selector'){const loc=page.locator(action.selector);await loc.first().evaluate((node,offset)=>scrollTo(0,node.getBoundingClientRect().top+scrollY+offset),action.offset||0);}
  else if(action.kind==='mock-clipboard')await page.evaluate((result)=>{Object.defineProperty(navigator,'clipboard',{configurable:true,value:{writeText:()=>result==='success'?Promise.resolve():Promise.reject(new Error('controlled clipboard failure'))}});document.execCommand=()=>false;},action.result);
  else if(action.kind==='search-submit'){await page.locator('[data-search-input]').fill(action.query);await page.locator('[data-search-form]').evaluate((node)=>node.requestSubmit());}
  else if(action.kind==='release-search')await page.evaluate((result)=>window.__behaviorReleaseSearch?.(result),action.result);
  else if(action.kind==='release-favorites'||action.kind==='release-personal-feed'){if(!controls.releaseData?.(action.result))throw new Error(`Controlled data release was unavailable: ${action.kind}`);}
  else if(action.kind==='release-images'){if(!controls.releaseImages?.(action.result))throw new Error('Controlled image release was unavailable');}
  else if(action.kind==='release-discovery'){if(!controls.releaseDiscovery?.(action.result))throw new Error('Controlled discovery release was unavailable');}
  else if(action.kind==='release-weather'){if(!controls.releaseWeather?.(action.result))throw new Error('Controlled weather release was unavailable');}
  else if(action.kind==='release-gallery-image'){if(!controls.releaseGalleryImage?.(action.result))throw new Error('Controlled gallery image release was unavailable');}
  else if(action.kind==='set-profile')await page.evaluate((value)=>{localStorage.setItem('ke_personalization_profile',JSON.stringify(value));dispatchEvent(new StorageEvent('storage',{key:'ke_personalization_profile',newValue:JSON.stringify(value)}));},profile(action));
  else if(action.kind==='resize-viewport')await page.setViewportSize({width:action.width,height:action.height});
  else throw new Error(`Unsupported behavioral action: ${action.kind}`);
  await page.evaluate(()=>new Promise((done)=>requestAnimationFrame(()=>requestAnimationFrame(done))));
  return {kind:action.kind,target:targetReceipt,result:'applied'};
}
async function waitForPhase(page,plan,phase){
  const checkedPhases=new Set(['loading','result','empty','error','retry-loading','phone-copied','phone-copy-error','personal','popular-fallback','reranked','appended','local-rerank','ready','hidden-unavailable','dialog-open-loading','dialog-open-loaded','dialog-open-error','loaded']);
  if(checkedPhases.has(phase)){try{await page.waitForFunction(({phase,runtimeProfile,networkProfile})=>{
    if(phase==='loading')return runtimeProfile.startsWith('search-')?document.querySelector('[data-search-skeletons]')?.hidden===false:runtimeProfile.startsWith('favorites-')?document.querySelector('[data-favorites-surface]')?.dataset.favoritesState==='loading':true;
    if(phase==='result')return runtimeProfile.startsWith('search-')?document.querySelector('[data-search-results]')?.hidden===false:runtimeProfile.startsWith('favorites-')?document.querySelector('[data-favorites-surface]')?.dataset.favoritesState==='ready':true;
    if(phase==='empty')return runtimeProfile.startsWith('search-')?document.querySelector('[data-authorized-search]')?.dataset.searchState==='empty':runtimeProfile.startsWith('favorites-')?document.querySelector('[data-favorites-empty]')?.hidden===false:true;
    if(phase==='error')return networkProfile==='deferred-images'?Boolean(document.querySelector('[data-listing-media-loading]')?.dataset.listingMediaReady==='true'||document.querySelector('.event-media')?.classList.contains('is-error')):runtimeProfile.startsWith('search-')?document.querySelector('[data-authorized-search]')?.dataset.searchState==='error'||document.querySelector('[data-search-status]')?.getAttribute('role')==='alert':runtimeProfile.startsWith('favorites-')?document.querySelector('[data-favorites-surface]')?.dataset.favoritesState==='error':true;
    if(phase==='retry-loading')return document.querySelector('[data-search-skeletons]')?.hidden===false;
    if(phase==='phone-copied')return /скопирован|скопировано|готово/iu.test(document.querySelector('[data-desktop-phone-status]')?.textContent||'');
    if(phase==='phone-copy-error')return /не удалось|ошиб/iu.test(document.querySelector('[data-desktop-phone-status]')?.textContent||'');
    if(phase==='personal')return !runtimeProfile.startsWith('personal-feed-')||document.querySelector('[data-personal-feed-section]')?.dataset.personalFeedMode==='personal';
    if(phase==='popular-fallback')return !runtimeProfile.startsWith('personal-feed-')||document.querySelector('[data-personal-feed-section]')?.dataset.personalFeedMode==='popular_fallback';
    if(phase==='reranked'||phase==='appended')return Boolean(document.querySelector('[data-discovery-feed]')?.dataset.effectiveAlgorithmId);
    if(phase==='local-rerank')return document.querySelector('[data-home-cold-start-feed]')?.dataset.homeFeedMode==='local_rerank';
    if(phase==='ready')return !networkProfile.startsWith('weather-')||document.querySelector('[data-weather-date-context]')?.dataset.weatherState==='ready';
    if(phase==='hidden-unavailable')return !networkProfile.startsWith('weather-')||document.querySelector('[data-weather-date-context]')?.hidden===true;
    if(phase==='dialog-open-loading')return document.querySelector('[data-gallery]')?.open===true&&document.querySelector('[data-gallery-media]')?.dataset.imageState==='loading';
    if(phase==='dialog-open-loaded')return document.querySelector('[data-gallery-media]')?.dataset.imageState==='loaded';
    if(phase==='dialog-open-error')return document.querySelector('[data-gallery-media]')?.dataset.imageState==='error';
    if(phase==='loaded')return networkProfile!=='deferred-images'||document.querySelector('[data-listing-media-loading]')?.dataset.listingMediaReady==='true';
    return true;
  },{phase,runtimeProfile:plan.runtime_profile||'',networkProfile:plan.network_profile||''},{timeout:12000});}catch(error){const state=await page.evaluate(()=>({search:document.querySelector('[data-authorized-search]')?.dataset.searchState,status:document.querySelector('[data-search-status]')?.textContent,role:document.querySelector('[data-search-status]')?.getAttribute('role'),resultsHidden:document.querySelector('[data-search-results]')?.hidden,favorites:document.querySelector('[data-favorites-surface]')?.dataset.favoritesState,personal:document.querySelector('[data-personal-feed-section]')?.dataset.personalFeedMode,weather:document.querySelector('[data-weather-date-context]')?.dataset.weatherState,gallery:document.querySelector('[data-gallery-media]')?.dataset.imageState}));throw new Error(`Behavior phase did not settle: ${plan.id}/${phase}: ${error.message}; state=${JSON.stringify(state)}`);}}
  if(plan.id.includes('favorites-loading-')&&phase!=='loading'){
    const expected=phase==='result'||phase==='empty'?'ready':phase;try{await page.waitForFunction((value)=>document.querySelector('[data-favorites-surface]')?.dataset.favoritesState===value,expected,{timeout:12000});}catch(error){throw new Error(`Favorites phase did not settle: ${phase}`);}
  }
  await wait(50);
}
async function stateFacts(page,root){
  const basic=await page.evaluate(()=>({window_scroll:{x:scrollX,y:scrollY},document:{scroll_width:document.documentElement.scrollWidth,scroll_height:document.documentElement.scrollHeight,client_width:document.documentElement.clientWidth,client_height:document.documentElement.clientHeight},visual_viewport:visualViewport?{width:visualViewport.width,height:visualViewport.height,offset_left:visualViewport.offsetLeft,offset_top:visualViewport.offsetTop,scale:visualViewport.scale}:null,focus_owner:document.activeElement?{tag:document.activeElement.tagName.toLowerCase(),role:document.activeElement.getAttribute('role'),data_names:[...document.activeElement.attributes].map((item)=>item.name).filter((name)=>name.startsWith('data-')).slice(0,20),aria:Object.fromEntries([...document.activeElement.attributes].filter((item)=>item.name.startsWith('aria-')).slice(0,20).map((item)=>[item.name,item.value])),text_length:(document.activeElement.textContent||'').length}:null}));
  const scroll=await root.evaluate((node)=>({scroll_left:node.scrollLeft,scroll_top:node.scrollTop,client_width:node.clientWidth,client_height:node.clientHeight,scroll_width:node.scrollWidth,scroll_height:node.scrollHeight}));return {...basic,root_scroll:scroll};
}
async function semanticSnapshot(page,root){
  return page.evaluate((node)=>{
    const rect=node.getBoundingClientRect();const style=getComputedStyle(node);const active=document.activeElement;
    const focusRect=active?.getBoundingClientRect?.();const focusStyle=active instanceof Element?getComputedStyle(active):null;
    const focusKey=active instanceof Element?JSON.stringify([active.tagName.toLowerCase(),active.id||'',active.getAttribute('role')||'',[...active.attributes].map((item)=>item.name).filter((name)=>name.startsWith('data-')).sort()]):null;
    const semanticAttrs=['aria-checked','aria-pressed','aria-expanded','aria-selected','aria-busy'];
    const descendant_states=[...node.querySelectorAll('[aria-checked],[aria-pressed],[aria-expanded],[aria-selected],[aria-busy],details,[hidden]')].slice(0,120).map((item)=>({tag:item.tagName.toLowerCase(),attrs:semanticAttrs.map((name)=>[name,item.getAttribute(name)]).filter(([,value])=>value!==null),open:item instanceof HTMLDetailsElement?item.open:null,hidden:item instanceof HTMLElement?item.hidden:null}));
    const signature=JSON.stringify({attrs:[...node.attributes].map((item)=>[item.name,item.value]).sort(),classes:[...node.classList].sort(),child_count:node.childElementCount,text_length:(node.textContent||'').length,html_length:node.innerHTML.length,details:[...node.querySelectorAll('details')].map((item)=>item.open),descendant_states,root_scroll:[node.scrollLeft,node.scrollTop,node.scrollWidth,node.scrollHeight],window_scroll:[scrollX,scrollY],viewport:[innerWidth,innerHeight],focus:focusKey});
    return {signature,root:{visible:!node.hidden&&style.display!=='none'&&style.visibility!=='hidden'&&rect.width>0&&rect.height>0,width:rect.width,height:rect.height,scroll_left:node.scrollLeft,scroll_top:node.scrollTop,scroll_width:node.scrollWidth,scroll_height:node.scrollHeight,client_width:node.clientWidth,client_height:node.clientHeight,child_count:node.childElementCount},window_scroll:{x:scrollX,y:scrollY},viewport:{width:innerWidth,height:innerHeight},focus:{key:focusKey,within_root:Boolean(active&&(active===node||node.contains(active))),visible:Boolean(focusRect&&focusStyle&&focusStyle.display!=='none'&&focusStyle.visibility!=='hidden'&&focusRect.width>0&&focusRect.height>0)}};
  },await root.elementHandle());
}
async function expectationLocator(page,root,expect,selector){return expect.selector_scope==='page'?page.locator(selector):root.locator(selector);}
async function assertPhaseExpectation({page,root,plan,step,before,after,phaseSnapshots}){
  const expect=step.expect||{};const fail=(reason)=>{throw new Error(`Behavior phase semantic mismatch: ${plan.id}/${step.phase}: ${reason}`);};
  if(expect.root_observed&&!after.root)fail('root not observed');
  if(expect.semantic_delta&&before.signature===after.signature)fail('no DOM/geometry/scroll/focus delta after declared actions');
  if(expect.root_geometry==='nonzero'&&(after.root.width<=0||after.root.height<=0))fail(`root geometry is ${after.root.width}x${after.root.height}`);
  if(expect.root_visibility==='visible'&&!after.root.visible)fail('root is not visible');
  if(expect.root_visibility==='hidden'&&after.root.visible)fail('root is unexpectedly visible');
  if(expect.viewport&&(after.viewport.width!==expect.viewport.width||after.viewport.height!==expect.viewport.height))fail(`viewport is ${after.viewport.width}x${after.viewport.height}`);
  if(expect.reduced_motion!==undefined){const reduced=await page.evaluate(()=>matchMedia('(prefers-reduced-motion: reduce)').matches);if(reduced!==expect.reduced_motion)fail(`reduced-motion is ${reduced}`);}
  if(expect.root_scrollable_x&&after.root.scroll_width<=after.root.client_width)fail('root is not horizontally scrollable');
  if(expect.root_scroll==='start'&&after.root.scroll_left>1)fail(`root scrollLeft is ${after.root.scroll_left}, expected start`);
  if(expect.root_scroll==='end'&&Math.abs((after.root.scroll_width-after.root.client_width)-after.root.scroll_left)>1)fail(`root scrollLeft is ${after.root.scroll_left}, expected end`);
  if(expect.root_scroll_changed&&Math.abs(after.root.scroll_left-before.root.scroll_left)<1)fail('root scroll position did not change');
  if(expect.window_scroll_changed&&Math.abs(after.window_scroll.y-before.window_scroll.y)<1)fail('window scroll position did not change');
  if(expect.root_height_changed&&Math.abs(after.root.height-before.root.height)<1)fail('root height did not change');
  if(expect.root_child_count_changed&&after.root.child_count===before.root.child_count)fail('root child count did not change');
  if(expect.focus){
    if(!after.focus.visible)fail('focus owner is not visibly rendered');
    if(expect.focus==='within-root-visible'&&!after.focus.within_root)fail('focus did not remain within root');
    if(expect.focus==='changed-visible'&&after.focus.key===before.focus.key)fail('focus owner did not change');
    if(expect.focus==='returned-visible'&&after.focus.key!==phaseSnapshots.get('open-focus-entry')?.focus.key)fail('focus did not return to entry owner');
  }
  if(expect.details_open!==undefined){const open=await root.locator('details').first().evaluate((node)=>node.open).catch(async()=>root.evaluate((node)=>Boolean(node.open)));if(open!==expect.details_open)fail(`details open is ${open}`);}
  if(expect.root_attribute){const value=await root.getAttribute(expect.root_attribute.name);if(expect.root_attribute.present&&value===null)fail(`root attribute ${expect.root_attribute.name} absent`);if('value'in expect.root_attribute&&value!==expect.root_attribute.value)fail(`root attribute ${expect.root_attribute.name} is ${value}`);}
  if(expect.selector_exists||expect.selector_visible||expect.selector_hidden||expect.selector_open||expect.selector_closed||expect.selector_attribute||expect.selector_count){
    if(expect.selector_exists){const loc=await expectationLocator(page,root,expect,expect.selector_exists);if(await loc.count()<1)fail(`selector absent: ${expect.selector_exists}`);}
    if(expect.selector_count){const loc=await expectationLocator(page,root,expect,expect.selector_count.selector);const count=await loc.count();if(count!==expect.selector_count.exact)fail(`selector count ${count}, expected ${expect.selector_count.exact}`);}
    if(expect.selector_visible){const loc=await expectationLocator(page,root,expect,expect.selector_visible);if(!(await loc.count()>0&&await loc.first().isVisible()))fail(`selector not visible: ${expect.selector_visible}`);}
    if(expect.selector_hidden){const loc=await expectationLocator(page,root,expect,expect.selector_hidden);if(await loc.count()>0&&await loc.first().isVisible())fail(`selector visible: ${expect.selector_hidden}`);}
    if(expect.selector_open){const loc=await expectationLocator(page,root,expect,expect.selector_open);if(!(await loc.count()>0&&await loc.first().evaluate((node)=>Boolean(node.open))))fail(`selector not open: ${expect.selector_open}`);}
    if(expect.selector_closed){const loc=await expectationLocator(page,root,expect,expect.selector_closed);if(await loc.count()>0&&await loc.first().evaluate((node)=>Boolean(node.open)))fail(`selector not closed: ${expect.selector_closed}`);}
    if(expect.selector_attribute){const loc=await expectationLocator(page,root,expect,expect.selector_attribute.selector);const value=await loc.first().getAttribute(expect.selector_attribute.name);if(value!==expect.selector_attribute.value)fail(`selector attribute ${expect.selector_attribute.name} is ${value}`);}
  }
  return {semantic_delta:before.signature!==after.signature,root_scroll_delta:after.root.scroll_left-before.root.scroll_left,window_scroll_delta:after.window_scroll.y-before.window_scroll.y,root_height_delta:after.root.height-before.root.height,focus_changed:after.focus.key!==before.focus.key,assertions_passed:true};
}
async function capturePhase({page,root,plan,phase,outputDir,imageComparator,telemetryState,index,transition,actionReceipts}){
  const font_settle=await settleFontsBounded(page);const name=`${plan.id.slice('behavior-packet.'.length)}-${String(index).padStart(2,'0')}-${phase}.png`;const path=join(outputDir,'behavior-rasters',name);
  const screenshot=plan.capture_scope==='page'?await captureStablePagePngBounded({page,path,imageComparator,label:`Behavior ${plan.id}/${phase}`}):await bounded(`Behavior ${plan.id}/${phase} stable locator screenshot`,captureStableLocatorPng({locator:root,path,imageComparator,label:`Behavior ${plan.id}/${phase}`}),SCREENSHOT_TIMEOUT_MS);
  const facts=await collectBoundedElementFacts(root,[]);let aria;try{aria=safeCapturedValue(await root.ariaSnapshot({timeout:3000}),6000);}catch(error){aria={unavailable:true,error_class:error.constructor?.name||'Error'};}
  const state=await stateFacts(page,root);const viewport=page.viewportSize();const row={schema_version:plan.schema_version,id:`behavior-observation.${sha(`${plan.id}\0${phase}`).slice(0,18)}`,plan_id:plan.id,family:plan.family,phase,sequence_index:index,source_sha:plan.source_sha,evidence_plane:plan.evidence_plane,reachability:plan.reachability,dynamic_region_ids:plan.dynamic_region_ids||[],breakpoint_probe_ids:plan.breakpoint_probe_ids||[],coverage_refs:plan.coverage_refs||[],route_hash:sha(plan.route),viewport,container:{planned_width:plan.container_width||null,actual_width:facts.geometry.width,actual_height:facts.geometry.height},reduced_motion:plan.reduced_motion,ratios:plan.ratios||[],media_provenance:plan.media_provenance||null,actions_since_prior_state:[],action_receipts:actionReceipts,transition,font_settle,state,dom:{tag:facts.tag,classes:facts.classes,attributes:facts.attributes,redacted_attribute_names:facts.redacted_attribute_names,child_count:facts.child_count,text_length:facts.text_length,text_sha256:facts.text_sha256,full_html_retained:false},accessibility:{aria_snapshot:aria,...facts.state},computed:facts.computed,geometry:facts.geometry,css_variables:facts.css_variables,pseudo:facts.pseudo,media:facts.media,media_queries:facts.media_queries,cascade:facts.cascade,loaded_fonts:facts.loaded_fonts,screenshot:{path:`behavior-rasters/${name}`,...screenshot},network:{counts_by_resource_type:telemetryState.resource_counts,response_status_counts:telemetryState.status_counts,failed_count:telemetryState.failed_count,raw_urls_retained:false},console:{counts:telemetryState.console_counts,message_hashes:telemetryState.console_hashes.slice(0,20),message_text_retained:false},evidence_status:'captured-not-reviewed',review_status:'pending-human-full-resolution-review',production_state_claimed:false,normalization_allowed:false,decision:'NOT_MERGED'};assertBehaviorObservation(row);return row;
}
export async function captureBehaviorPackets({browser,baseUrl,outputDir,nodeModules,registry=buildBehaviorPacketRegistry(),onlyIds=null}){
  assertBehaviorPacketRegistry(registry);mkdirSync(join(outputDir,'behavior-rasters'),{recursive:true});const observations=[];const blockers=[];const imageComparator=loadPinnedPlaywrightImageComparator(nodeModules,'image/png');
  const selected=registry.plans.filter((row)=>!onlyIds||onlyIds.includes(row.id));
  for(let planIndex=0;planIndex<selected.length;planIndex+=1){
    const plan=selected[planIndex];const planStarted=Date.now();process.stderr.write(`[behavior-capture] plan ${planIndex+1}/${selected.length} ${plan.id} start\n`);
    if(plan.execution_status==='explicit-blocker'){blockers.push({schema_version:plan.schema_version,id:`behavior-blocker.${sha(plan.id).slice(0,18)}`,plan_id:plan.id,family:plan.family,reason:plan.blocker_reason,source_path:plan.source_path||null,reachability:plan.reachability,dynamic_region_ids:plan.dynamic_region_ids||[],breakpoint_probe_ids:plan.breakpoint_probe_ids||[],coverage_refs:plan.coverage_refs||[],blocked_states:plan.blocked_states||[],blocks_ready:plan.blocks_ready===true,status:'explicit-blocker',production_state_claimed:false,decision:'NOT_MERGED'});process.stderr.write(`[behavior-capture] plan ${planIndex+1}/${selected.length} ${plan.id} complete blocker elapsed_ms=${Date.now()-planStarted}\n`);continue;}
    const context=await browser.newContext({viewport:plan.viewport,reducedMotion:plan.reduced_motion?'reduce':'no-preference',deviceScaleFactor:plan.device_scale_factor,timezoneId:'Europe/Kaliningrad'});
    await context.addInitScript(({fixedNow})=>{const NativeDate=Date;class FixedDate extends NativeDate{constructor(...args){super(...(args.length?args:[fixedNow]));}static now(){return fixedNow;}}Object.setPrototypeOf(FixedDate,NativeDate);globalThis.Date=FixedDate;let state=0x6d2b79f5;Math.random=()=>{state=(Math.imul(state^state>>>15,1|state)+Math.imul(state^state>>>7,61|state)^state)>>>0;return((state^state>>>14)>>>0)/4294967296;};},{fixedNow:Date.parse('2026-08-08T12:48:42.000Z')});
    const page=await context.newPage();const telemetryState=telemetry(page);await installBeforeNavigation(page,context,plan);const controls=await installNetwork(page,plan);
    try{
      page.setDefaultTimeout(15000);
      const searchQuery=plan.runtime_profile?.startsWith('search-')?'?code=behavior-code':'';await page.goto(`${baseUrl}${plan.route}${searchQuery}`,{waitUntil:'domcontentloaded',timeout:30000});await page.waitForLoadState('networkidle',{timeout:3000}).catch(()=>{});await page.addStyleTag({content:'*,*::before,*::after{animation:none!important;transition:none!important;caret-color:transparent!important;scroll-behavior:auto!important}'});
      const candidates=page.locator(plan.root_selector);const visibleCandidates=candidates.filter({visible:true});let root;
      if(await visibleCandidates.count())root=visibleCandidates.first();else{if(plan.visible_root_required)throw new Error(`Behavior plan has no visible root: ${plan.id}`);root=candidates.first();}
      await root.waitFor({state:(plan.capture_scope==='locator'&&!plan.allow_hidden_root)?'visible':'attached',timeout:15000});
      const phaseSnapshots=new Map();
      for(let index=0;index<plan.steps.length;index+=1){const phase=plan.steps[index];const before=await semanticSnapshot(page,root);const actionReceipts=[];for(const action of phase.actions)actionReceipts.push(await applyAction({page,root,action,controls}));await waitForPhase(page,plan,phase.phase);const after=await semanticSnapshot(page,root);const transition=await assertPhaseExpectation({page,root,plan,step:phase,before,after,phaseSnapshots});phaseSnapshots.set(phase.phase,after);if(phase.capture){const row=await capturePhase({page,root,plan,phase:phase.phase,outputDir,imageComparator,telemetryState,index,transition,actionReceipts});row.actions_since_prior_state=phase.actions;observations.push(row);}}
      process.stderr.write(`[behavior-capture] plan ${planIndex+1}/${selected.length} ${plan.id} complete elapsed_ms=${Date.now()-planStarted}\n`);
    }catch(error){process.stderr.write(`[behavior-capture] plan ${planIndex+1}/${selected.length} ${plan.id} failed error_class=${error?.constructor?.name||'Error'} elapsed_ms=${Date.now()-planStarted}\n`);throw error;}finally{await context.close();}
  }
  writeFileSync(join(outputDir,'behavior-specimen-observations.jsonl'),`${observations.map((row)=>JSON.stringify(row)).join('\n')}\n`);writeFileSync(join(outputDir,'behavior-capture-blockers.jsonl'),`${blockers.map((row)=>JSON.stringify(row)).join('\n')}\n`);
  if(!onlyIds)assertBehaviorCaptureComplete(registry,observations,blockers);return {observations,blockers};
}
export async function captureBehaviorWithExactPlaywright({nodeModules,dist,outputDir,registry=buildBehaviorPacketRegistry(),onlyIds=null}){
  const entry=join(resolve(nodeModules),'playwright/index.mjs');if(!existsSync(entry))throw new Error('Exact Playwright entrypoint missing');const {chromium}=await import(pathToFileURL(entry).href);const server=await startSpecimenServer({dist});const browser=await chromium.launch({headless:true});
  try{return await captureBehaviorPackets({browser,baseUrl:server.baseUrl,outputDir,nodeModules,registry,onlyIds});}finally{await browser.close();await server.close();}
}
