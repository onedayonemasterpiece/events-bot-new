import {chromium} from 'playwright';
import assert from 'node:assert/strict';
import {specimens} from '../scripts/write-island-archetype-index.mjs';
import {mkdirSync,writeFileSync} from 'node:fs';
const out=process.env.CHECK_OUTPUT,base=process.env.CHECK_BASE;
if(!out||!base)throw new Error('CHECK_OUTPUT and CHECK_BASE required');mkdirSync(out,{recursive:true});const routes=specimens.map(x=>x[0]).filter(x=>!process.env.CHECK_ROUTES||JSON.parse(process.env.CHECK_ROUTES).includes(x));
const browser=await chromium.launch({headless:true}),results=[];
for(const width of (process.env.CHECK_WIDTHS||'384,1440').split(',').map(Number))for(const route of routes){
 const page=await browser.newPage({viewport:{width,height:844}}),errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.goto(base+route,{waitUntil:'domcontentloaded'});await page.waitForTimeout(900);
 for(const y of [0,350,900]){await page.evaluate(y=>scrollTo(0,y),y);await page.waitForTimeout(900);
 const state=await page.evaluate(()=>({ready:document.body.dataset.fiMotion,mode:document.body.hasAttribute('data-fi-mobile')?'city-mobile':document.body.hasAttribute('data-fi-content')?'content':'desktop',overflow:document.documentElement.scrollWidth>innerWidth+1,h1:[...document.querySelectorAll('h1')].filter(n=>n.getClientRects().length).map(n=>n.textContent.trim()),city:[...document.querySelectorAll('[data-listing-controls]')].filter(n=>n.getBoundingClientRect().width>0).map(n=>({docked:n.dataset.fiDocked,x:n.getBoundingClientRect().x,y:n.getBoundingClientRect().y,width:n.getBoundingClientRect().width})),context:document.querySelector('[data-floating-section-title]')?.textContent,titleColor:document.querySelector('body[data-fi-content-mobile] .fi-content-layer h1')?getComputedStyle(document.querySelector('.fi-content-layer h1')).color:null,originalTitleColor:document.querySelector('body[data-fi-content-mobile] .site-header__context')?.style.getPropertyValue('--fi-content-original-color')}));
 if(y===0&&state.titleColor)assert.equal(state.titleColor,state.originalTitleColor,'reparented heading must retain original readable color');
 results.push({width,route,y,...state,errors:[...errors]});
 await page.screenshot({path:`${out}/${width}-${route.split('/').filter(Boolean).join('_')}-${y}.png`});
 }
 const toggle=page.locator('[data-island-city-toggle]:visible').first();if(await toggle.count()){await toggle.click();await page.waitForTimeout(150);results.push({width,route,picker:await page.locator('[data-island-city-panel]:popover-open').count()});await page.keyboard.press('Escape');}
 if(width<721&&['/segodnya/','/vyhodnye/'].includes(route)){
  await toggle.click();const choice=page.locator('[data-island-city-panel]:popover-open [data-mobile-v23-city]:not([data-mobile-v23-city="all"])').first();
  const city=await choice.getAttribute('data-mobile-v23-city');await choice.click();await page.waitForTimeout(100);
  const visibleCities=await page.locator('[data-mobile-listing-row]:visible').evaluateAll(ns=>ns.map(n=>n.dataset.listingCity));assert.ok(visibleCities.length>0);assert.ok(visibleCities.every(x=>x===city),'native city filter must affect actual rows');
  results.push({width,route,filter:true,city,visibleRows:visibleCities.length});await page.keyboard.press('Escape');
 }
 await page.close();
}
await browser.close();writeFileSync(out+'/checks.json',JSON.stringify(results,null,2));const failed=results.filter(x=>x.errors?.length||x.overflow||('ready'in x&&!x.ready)||x.picker===0);assert.deepEqual(failed,[]);console.log(`PASS ${results.length} route/state/interaction checks`);
