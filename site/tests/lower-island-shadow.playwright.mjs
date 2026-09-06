import {chromium} from 'playwright';import assert from 'node:assert/strict';import {mkdirSync,writeFileSync} from 'node:fs';
const base=process.env.CHECK_BASE,old=process.env.CHECK_BEFORE,out=process.env.CHECK_OUTPUT;if(!base||!old||!out)throw Error('CHECK_BASE/CHECK_BEFORE/CHECK_OUTPUT required');mkdirSync(out,{recursive:true});
const routes=[['free','/podborki/besplatnye-sobytiya/'],['weekend','/vyhodnye/'],['date','/date-2026-07-23/'],['event','/sobytiya/golosyaschiy-kivin-2026-i-letniy-kubok-kvn-svetlogorsk-6941/']];
const cases=[...routes.map(r=>[384,...r]),[320,...routes[1]],[430,...routes[1]],[1440,...routes[0]]];const browser=await chromium.launch(),checks=[];
for(const [width,name,route] of cases){const results=[];
 for(const [version,url] of [['before',old],['after',base]]){
  const page=await browser.newPage({viewport:{width,height:900}}),errors=[];page.on('pageerror',e=>errors.push(e.message));await page.goto(url+route,{waitUntil:'domcontentloaded'});await page.waitForFunction(()=>document.body.dataset.fiMotion==='ready'||document.body.hasAttribute('data-event-dock'));await page.evaluate(()=>document.fonts.ready);
  if(width<721&&(name==='weekend'||name==='date'))await page.waitForSelector('[data-date-dock-surface]');
  if(name==='event'&&width<721)await page.waitForSelector('[data-event-dock-surface]');
  const state=await page.evaluate(()=>{const n=document.querySelector('[data-date-dock-surface],[data-event-dock-surface]')||document.querySelector('[data-mobile-bottom-nav]'),r=n.getBoundingClientRect(),nav=n.querySelector('[data-mobile-bottom-nav]'),upper=document.querySelector('.site-nav');return{box:{x:r.x,y:r.y,width:r.width,height:r.height},shadow:getComputedStyle(n).boxShadow,innerShadow:nav?getComputedStyle(nav).boxShadow:null,upperShadow:upper?getComputedStyle(upper).boxShadow:null,token:getComputedStyle(document.documentElement).getPropertyValue('--ke-island-lower-shadow'),overflow:document.documentElement.scrollWidth>innerWidth+1}});
  assert.equal(state.overflow,false);if(state.innerShadow!==null)assert.equal(state.innerShadow,'none');await page.screenshot({path:`${out}/${width}-${name}-${version}.png`});assert.deepEqual(errors,[]);results.push(state);await page.close();
 }
 const [before,after]=results;for(const k of Object.keys(before.box))assert.ok(Math.abs(before.box[k]-after.box[k])<1,JSON.stringify({width,name,k,before,after}));assert.notEqual(before.shadow,after.shadow);assert.equal(before.upperShadow,after.upperShadow);assert.match(after.token,/\.32/);checks.push({width,name,before,after,geometryPreserved:true,upperUnchanged:true});
}
await browser.close();writeFileSync(out+'/checks.json',JSON.stringify(checks,null,2));console.log('PASS lower shadow, same geometry and upper style',checks.length);
