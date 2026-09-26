import assert from 'node:assert/strict';
import {chromium} from 'playwright';
import {mkdir,writeFile} from 'node:fs/promises';
const base=process.env.CHECK_BASE?.replace(/\/$/,''),out=process.env.CHECK_OUTPUT;
if(!base||!out)throw new Error('CHECK_BASE and CHECK_OUTPUT required');
await mkdir(out,{recursive:true});
const browser=await chromium.launch({executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH||undefined});
const results=[];
const rect=async(p,s)=>p.locator(s).first().evaluate(e=>{const r=e.getBoundingClientRect();return {x:r.x,y:r.y,width:r.width,height:r.height};});
try{
 for(const width of [320,390,430]){
  const ctx=await browser.newContext({viewport:{width,height:844},isMobile:true,hasTouch:true});
  const p=await ctx.newPage(),errors=[];p.on('pageerror',e=>errors.push(e.message));
  for(const route of ['','segodnya/','zavtra/','vystavki/','podborki/gastronomiya/','neobychnoe/','dlya-menya/']){
   assert.equal((await p.goto(base+'/'+route,{waitUntil:'domcontentloaded',timeout:90000})).status(),200);
   await p.waitForTimeout(1500);
   assert.ok(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),route+' overflow');
   assert.equal(await p.locator('[data-product-breadcrumbs]:visible,[data-product-parent-link]:visible').count(),0);
   if(!route){
    assert.equal(await p.getByRole('button',{name:'Пауза',exact:true}).count(),0);
    for(const root of await p.locator('[data-home-hero-talk]').all()){
     const media=root.locator('[data-home-hero-media]').first();if(!await media.count())continue;
     await media.waitFor({state:'attached'});await root.scrollIntoViewIfNeeded();await p.waitForTimeout(800);
     if(await media.isHidden())continue;
     const m=await media.evaluate(e=>{const r=e.getBoundingClientRect(),s=e.closest('[data-home-hero-scene]').getBoundingClientRect();return{top:r.top-s.top,w:r.width,h:r.height,rows:getComputedStyle(e).getPropertyValue('--mosaic-rows')};});
     assert.equal(m.top,0);assert.equal(m.rows.trim(),'8');assert.ok(m.h/m.w>.8&&m.h/m.w<1.4);
    }
   }
   if(route==='vystavki/')assert.equal(await p.locator('body').evaluate(e=>getComputedStyle(e).backgroundColor),'rgb(36, 33, 31)');
   if(['segodnya/','zavtra/'].includes(route)){
    const control=p.locator('[data-mobile-listing-rails] [data-listing-controls]');
    if(await control.count()){
     await p.waitForFunction(()=>document.body.dataset.fiMotion==='ready');
     await p.evaluate(()=>scrollTo(0,260));await p.waitForTimeout(800);
     assert.equal(await control.getAttribute('data-fi-docked'),'true');assert.equal(await control.getAttribute('data-fi-caption-phase'),'ready');
     const r=await rect(p,'[data-mobile-listing-rails] [data-listing-controls]');assert.ok(Math.abs(r.y-20)<2,JSON.stringify(r));
     await control.locator('[data-island-city-toggle]').click();await control.locator('[data-island-city-panel]').waitFor({state:'visible'});await p.keyboard.press('Escape');
     await p.evaluate(()=>scrollTo(0,0));await p.waitForTimeout(800);assert.equal(await control.getAttribute('data-fi-docked'),'false');
    }
   }
   if(route==='dlya-menya/'){
    const section=p.locator('[data-listing-context="for-me"][data-personal-feed-section]');
    await section.waitFor({state:'visible'});await p.waitForFunction(()=>document.querySelector('[data-listing-context="for-me"][data-personal-feed-section]')?.dataset.personalFeedReady==='true',{},{timeout:30000});
    assert.ok(await section.locator('[data-event-card]:visible').count()>=3);
    const like=section.locator('[data-feedback-action="like"]:visible').first();const id=await like.getAttribute('data-event-id');await like.click();
    await p.waitForFunction(id=>JSON.parse(localStorage.getItem('ke_personalization_profile')||'{}').liked_event_ids?.map(String).includes(id),id);
    assert.equal(await p.locator('[data-personalization-consent]:visible,[data-focus-consent]:visible').count(),0);
    await p.reload({waitUntil:'domcontentloaded'});await p.waitForTimeout(1500);
    assert.ok(await p.evaluate(id=>JSON.parse(localStorage.getItem('ke_personalization_profile')||'{}').liked_event_ids.map(String).includes(id),id));
    assert.ok(await p.locator('[data-event-card]:visible').count()>=3);
   }
   await p.evaluate(()=>scrollTo(0,0));await p.screenshot({path:`${out}/${width}-${route.replaceAll('/','')||'home'}.png`});
   results.push({width,route:route||'home',passed:true});
  }
  assert.deepEqual(errors,[]);await ctx.close();
 }
 // Compare genuine no-JS server layout with the enhanced page at the same width.
 const geometry=[];
 for(const js of [false,true]){
  const p=await browser.newPage({viewport:{width:390,height:844},javaScriptEnabled:js});await p.goto(base+'/zavtra/',{waitUntil:'load',timeout:90000});
  await p.waitForTimeout(50);
  if(js)await p.waitForFunction(()=>document.body.dataset.fiMotion==='ready');
  geometry.push({title:await rect(p,'[data-mobile-listing-rails] h1'),city:await rect(p,'[data-mobile-listing-rails] [data-listing-controls]')});await p.close();
 }
 for(const key of ['title','city'])for(const axis of ['x','y','width','height'])assert.ok(Math.abs(geometry[0][key][axis]-geometry[1][key][axis])<=1,`${key}.${axis}: ${JSON.stringify(geometry)}`);
 results.push({firstPaint:geometry,passed:true});
 await writeFile(out+'/checks.json',JSON.stringify({passed:true,layer:'L1 browser mobile viewport; not native Android/iOS acceptance',results},null,2));
}catch(e){await writeFile(out+'/checks.json',JSON.stringify({passed:false,error:e.stack,results},null,2));throw e;}finally{await browser.close();}
