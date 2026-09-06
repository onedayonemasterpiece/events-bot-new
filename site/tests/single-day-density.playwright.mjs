import { chromium } from 'playwright';
import assert from 'node:assert/strict';
import { mkdirSync, writeFileSync } from 'node:fs';
const base=process.env.CHECK_BASE,out=process.env.CHECK_OUTPUT;
if(!base||!out)throw new Error('CHECK_BASE/CHECK_OUTPUT required');
mkdirSync(out,{recursive:true});
const browser=await chromium.launch(),results=[];
const viewports=process.env.CHECK_WIDTHS ? process.env.CHECK_WIDTHS.split(',').map(Number).map(width=>({width,height:width>=430?844:720})) : [{width:320,height:720},{width:384,height:720},{width:430,height:844}];
for(const viewport of viewports){
 const page=await browser.newPage({viewport,hasTouch:true}),errors=[];
 page.on('pageerror',e=>errors.push(e.message));
 for(const route of ['/date-2026-07-23/','/segodnya/','/zavtra/']){
  await page.goto(base+route,{waitUntil:'domcontentloaded'});
  await page.waitForFunction(()=>document.body.dataset.fiMotion==='ready');
  await page.waitForTimeout(250);
  const state=await page.evaluate(()=>{
   const root=document.querySelector('[data-mobile-listing-rails]'),heading=root.querySelector('h1'),dock=document.querySelector('[data-date-dock-surface]'),rows=[...root.querySelectorAll('[data-mobile-listing-row]')].filter(n=>n.getClientRects().length),rect=rows[0].getBoundingClientRect(),edge=dock.getBoundingClientRect().y;
   return {singleDay:document.body.hasAttribute('data-fi-single-day'),firstY:rect.y,rowHeight:rect.height,visible:rows.filter(n=>{const r=n.getBoundingClientRect();return r.y>=0&&r.bottom<=edge}).length,total:rows.length,headingX:heading.getBoundingClientRect().x,feedVisible:!!root.querySelector('.feed-head').getClientRects().length,dockHeight:dock.getBoundingClientRect().height,railHeight:root.querySelector('.date-rail').getBoundingClientRect().height,overflow:document.documentElement.scrollWidth>innerWidth+1};
  });
  assert.equal(state.singleDay,true);assert.equal(state.headingX,12);assert.equal(state.feedVisible,false);assert.ok(state.firstY<=230,JSON.stringify(state));assert.equal(state.rowHeight,112);assert.ok(state.visible>=Math.min(viewport.height>=844?4:3,state.total),JSON.stringify(state));assert.equal(state.dockHeight,110);assert.equal(state.railHeight,64);assert.equal(state.overflow,false);
  await page.screenshot({path:`${out}/${viewport.width}-${route.split('/').filter(Boolean).join('-')}-entry.png`});
  await page.evaluate(()=>scrollTo(0,300));await page.waitForFunction(()=>{const n=document.querySelector('[data-mobile-listing-rails] [data-listing-controls]');return n?.dataset.fiDocked==='true'&&!n.hasAttribute('data-fi-moving');});
  const docked=await page.evaluate(()=>{const root=document.querySelector('[data-mobile-listing-rails]'),h=root.querySelector('h1').getBoundingClientRect(),city=root.querySelector('[data-listing-controls]'),r=city.getBoundingClientRect(),brand=document.querySelector('[data-mobile-discovery-menu]>summary').getBoundingClientRect();return {headingBottom:h.bottom,cityX:r.x,cityY:r.y,cityWidth:r.width,cityHeight:r.height,caption:city.querySelector("[data-island-city-toggle]").textContent,brandRight:brand.right,visibleCount:Number(city.dataset.fiVisibleCount),docked:city.dataset.fiDocked,titleSkinVisible:!!root.querySelector('.fi-mobile-title-skin')?.getClientRects().length,sectionVisible:!!root.querySelector('[data-floating-section-context]')?.getClientRects().length};});
  assert.ok(docked.headingBottom<0);assert.equal(docked.docked,'true');assert.ok(docked.cityX>=docked.brandRight+8-1);assert.ok(Math.abs(docked.cityY-20)<1,JSON.stringify({viewport,route,...docked}));assert.equal(docked.titleSkinVisible,false);assert.equal(docked.sectionVisible,false);assert.equal(docked.visibleCount,0);assert.equal(docked.caption,'Все города');assert.equal(docked.cityHeight,44);assert.ok(docked.cityWidth<=130);assert.ok(docked.cityX<=docked.brandRight+13);
  await page.screenshot({path:`${out}/${viewport.width}-${route.split('/').filter(Boolean).join('-')}-scroll.png`});
  // Use original city choices through the actual overflow popover.
  const toggle=page.locator('[data-mobile-listing-rails] [data-island-city-toggle]:visible');
  await toggle.click();const choice=page.locator('[data-island-city-panel]:popover-open [data-mobile-v23-city]:not([data-mobile-v23-city="all"]):visible').first();
  const city=await choice.getAttribute('data-mobile-v23-city');await choice.click();await page.waitForFunction(()=>{const n=document.querySelector('[data-mobile-listing-rails] [data-island-city-toggle]');return n?.textContent!=='Все города'&&!document.querySelector('[data-island-city-panel]:popover-open');});await page.waitForTimeout(200);
  // Filtering a short day can naturally clamp scroll back to its expanded row.
  const stillDocked=await page.locator('[data-mobile-listing-rails] [data-listing-controls]').getAttribute('data-fi-docked')==='true';
  if(stillDocked){const selectedCaption=await toggle.textContent();assert.ok(selectedCaption.includes(city),JSON.stringify({selectedCaption,city}));await toggle.click();assert.ok(await page.locator('[data-island-city-panel]:popover-open [data-mobile-v23-city="all"]:visible').count());await page.keyboard.press('Escape');}
  else assert.equal(await page.locator('[data-mobile-v23-city]').evaluateAll((ns,c)=>ns.find(n=>n.dataset.mobileV23City===c)?.getAttribute('aria-pressed'),city),'true');
  const filtered=await page.locator('[data-mobile-listing-row]:visible').evaluateAll(ns=>ns.map(n=>n.dataset.listingCity));assert.ok(filtered.length>0);assert.ok(filtered.every(x=>x===city));assert.equal(Number(await page.locator('[data-mobile-v23-result-count]').textContent()),filtered.length);
  results.push({viewport,route,...state,...docked,filter:true,errors:[...errors]});assert.deepEqual(errors,[]);
 }
 await page.close();
}
const page=await browser.newPage({viewport:{width:384,height:720}});
await page.goto(base+'/date-2027-04-23/',{waitUntil:'domcontentloaded'});await page.waitForFunction(()=>document.body.dataset.fiMotion==='ready');
assert.equal(await page.locator('body').getAttribute('data-fi-single-day'),'');assert.equal(await page.locator('[data-mobile-listing-rails] [data-mobile-v23-city-picker]').count(),0);assert.equal(await page.locator('[data-mobile-listing-rails] h1').evaluate(n=>getComputedStyle(n).position),'static');await page.screenshot({path:out+'/384-single-city-day.png'});results.push({singleCityStaticHeading:true});
await page.goto(base+'/vyhodnye/',{waitUntil:'domcontentloaded'});await page.waitForFunction(()=>document.body.dataset.fiMotion==='ready');assert.equal(await page.locator('body').getAttribute('data-fi-single-day'),null);await page.evaluate(()=>scrollTo(0,900));await page.waitForTimeout(900);assert.ok(await page.locator('[data-floating-section-context]:visible').count()>0);await page.screenshot({path:out+'/384-weekend-context.png'});results.push({weekendContextRetained:true});
await page.emulateMedia({reducedMotion:'reduce'});await page.goto(base+'/date-2026-07-23/',{waitUntil:'domcontentloaded'});await page.waitForFunction(()=>document.body.dataset.fiMotion==='ready');await page.evaluate(()=>scrollTo(0,300));await page.waitForTimeout(100);assert.ok((await page.locator('[data-mobile-listing-rails] h1').boundingBox()).y<0);results.push({reducedMotionStaticHeading:true});await browser.close();
writeFileSync(out+'/checks.json',JSON.stringify(results,null,2));console.log('PASS',JSON.stringify(results));
