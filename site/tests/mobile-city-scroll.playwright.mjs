import {chromium} from 'playwright';import assert from 'node:assert/strict';import {mkdirSync,writeFileSync} from 'node:fs';
const base=process.env.CHECK_BASE,out=process.env.CHECK_OUTPUT;if(!base||!out)throw new Error('CHECK_BASE/CHECK_OUTPUT required');mkdirSync(out,{recursive:true});
const browser=await chromium.launch(),checks=[];
for(const width of (process.env.CHECK_WIDTHS||'320,384,430').split(',').map(Number))for(const route of (process.env.CHECK_ROUTES?JSON.parse(process.env.CHECK_ROUTES):['/date-2026-07-23/','/vyhodnye/','/populyarnoe/'])){
 const page=await browser.newPage({viewport:{width,height:844},hasTouch:true}),errors=[];page.on('pageerror',e=>errors.push(e.message));await page.goto(base+route,{waitUntil:'domcontentloaded'});await page.waitForFunction(()=>document.body.dataset.fiMotion==='ready');
 const row=page.locator('.fi-mobile-city-origin .fi-city-visible'),controls=page.locator('.fi-mobile-city-origin [data-listing-controls]'),toggle=controls.locator('[data-island-city-toggle]');
 assert.equal(await controls.getAttribute('data-fi-docked'),'false');assert.equal(await toggle.isVisible(),false);assert.ok(await row.evaluate(n=>n.scrollWidth>n.clientWidth+10));assert.equal(await row.locator('.fi-city-item[hidden]').count(),0);
 const cdp=await page.context().newCDPSession(page),box=await row.boundingBox(),x=box.x+box.width*.8,y=box.y+box.height/2;
 await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[{x,y}]});for(let i=1;i<=8;i++){await cdp.send('Input.dispatchTouchEvent',{type:'touchMove',touchPoints:[{x:x-Math.min(210,box.width*.65)*i/8,y}]});await page.waitForTimeout(25);}await cdp.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});await page.waitForTimeout(400);
 const offset=await row.evaluate(n=>n.scrollLeft),maxOffset=await row.evaluate(n=>n.scrollWidth-n.clientWidth);assert.ok(offset>Math.min(30,maxOffset*.5),JSON.stringify({width,route,offset,maxOffset,check:'real native touch swipe'}));assert.ok(await page.evaluate(()=>scrollY)<2,'horizontal swipe must not scroll the page');
 await page.screenshot({path:`${out}/${width}-${route.split('/')[1]}-swiped.png`});
 async function motion(destination,reverse=false){
  await page.evaluate(({destination,reverse})=>{window.__cityFrames=[];const end=performance.now()+1100;
   function clipWidth(n){if(n.hidden)return 0;const r=n.getBoundingClientRect(),s=getComputedStyle(n).clipPath;if(s==='none')return r.width;const parts=s.slice(6,-1).split(/\s+/);const v=parts[1]||parts[0];return Math.max(0,r.width-(v.endsWith('%')?parseFloat(v)*r.width/100:parseFloat(v)));}
   function sample(){const row=document.querySelector('.fi-mobile-city-origin .fi-city-visible'),t=document.querySelector('.fi-mobile-city-origin [data-island-city-toggle]');window.__cityFrames.push({time:performance.now(),row:clipWidth(row),toggle:clipWidth(t),scroll:row.scrollLeft,moving:row.parentElement.hasAttribute("data-fi-moving"),overflow:getComputedStyle(row.parentElement).overflow});if(performance.now()<end)requestAnimationFrame(sample);}requestAnimationFrame(sample);scrollTo(0,destination);if(reverse)setTimeout(()=>scrollTo(0,0),160);
  },{destination,reverse});await page.waitForTimeout(1150);const frames=await page.evaluate(()=>window.__cityFrames);assert.ok(frames.length>=10);assert.deepEqual(frames.filter(f=>f.moving&&f.overflow!=='visible'),[],'moving skin must not be clipped by compact target bounds');assert.deepEqual(frames.filter(f=>f.row>1&&f.toggle>1),[],'row/caption masks must not expose overlapping text in any frame');return frames;
 }
 const down=await motion(300);assert.equal(await controls.getAttribute('data-fi-docked'),'true');const docked=await controls.boundingBox();assert.ok(Math.abs(width-docked.x-docked.width-12)<2);
 await toggle.click();assert.equal(await controls.locator('[data-island-city-panel]:popover-open').count(),1);await page.keyboard.press('Escape');
 const up=await motion(0);assert.equal(await controls.getAttribute('data-fi-docked'),'false');assert.equal(await toggle.isVisible(),false);assert.ok(Math.abs(await row.evaluate(n=>n.scrollLeft)-offset)<2,'native scroll position survives collapse and expansion');
 const reversal=await motion(300,true);assert.equal(await controls.getAttribute('data-fi-docked'),'false');
 // Click a real city from the native scroller. Playwright scrolls the same row
 // to the focused button, without substituting a filter call or mock dataset.
 const last=row.locator('.fi-city-item').last(),city=await last.getAttribute('data-fi-city-value');await last.click();await page.waitForTimeout(250);
 assert.equal(await last.getAttribute('aria-pressed'),'true');
 if(route!=='/populyarnoe/'){const visible=await page.locator('[data-mobile-listing-row]:visible').evaluateAll(ns=>ns.map(n=>n.dataset.listingCity));assert.ok(visible.length);assert.ok(visible.every(x=>x===city));}
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);assert.deepEqual(errors,[]);
 checks.push({width,route,touchScroll:offset,restored:true,originalFilter:true,frames:{down,up,reversal},errors});await page.close();
}
await browser.close();writeFileSync(out+'/checks.json',JSON.stringify(checks,null,2));console.log('PASS native city touch, filtering, restoration and frame masks:',checks.length);
