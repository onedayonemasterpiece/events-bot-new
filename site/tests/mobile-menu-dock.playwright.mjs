import {chromium} from 'playwright';
import assert from 'node:assert/strict';
import {mkdirSync,writeFileSync} from 'node:fs';
const base=process.env.CHECK_BASE,out=process.env.CHECK_OUTPUT;
if(!base||!out)throw Error('CHECK_BASE/CHECK_OUTPUT required');
mkdirSync(out,{recursive:true});
const browser=await chromium.launch(),checks=[];
for(const route of ['date-2026-07-23','vyhodnye']) for(const height of [844,600]){
 const p=await browser.newPage({viewport:{width:390,height},hasTouch:true}),errors=[];
 p.on('pageerror',e=>errors.push(e.message));await p.goto(`${base}/${route}/`);
 await p.waitForFunction(()=>document.body.hasAttribute('data-date-dock'));
 const menu=p.locator('[data-reference4-fullscreen]'),summary=menu.locator(':scope > summary'),panel=menu.locator('.mobile-discovery-menu__panel'),dock=p.locator('[data-date-dock-surface]');
 const current=await p.locator('.date-rail [aria-current="date"]').getAttribute('data-date');
 const cdp=await p.context().newCDPSession(p);
 async function swipe(x,y,dx,dy){await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[{x,y}]});for(let i=1;i<=8;i++){await cdp.send('Input.dispatchTouchEvent',{type:'touchMove',touchPoints:[{x:x+dx*i/8,y:y+dy*i/8}]});await p.waitForTimeout(25)}await cdp.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});}
 async function open(){await summary.click();await p.waitForTimeout(440);assert.equal(await menu.getAttribute('open'),'');assert.equal(await dock.evaluate(n=>getComputedStyle(n).visibility),'hidden');assert.equal(await dock.evaluate(n=>n.inert),true);const r=await dock.boundingBox();assert.equal(await p.evaluate(({x,y})=>Boolean(document.elementFromPoint(x,y)?.closest('[data-date-dock-surface]')),{x:r.x+20,y:r.y+20}),false);}
 async function restored(){await p.waitForTimeout(390);assert.equal(await menu.getAttribute('open'),null);assert.equal(await dock.evaluate(n=>getComputedStyle(n).visibility),'visible');assert.equal(await dock.evaluate(n=>n.inert),false);assert.ok(await summary.evaluate(n=>document.activeElement===n));assert.equal(await p.locator('.date-rail [aria-current="date"]').getAttribute('data-date'),current);}
 await open();await p.screenshot({path:`${out}/${route}-${height}-menu.png`});
 // A long menu must scroll natively rather than being dismissed.
 const scrollable=await panel.evaluate(n=>n.scrollHeight>n.clientHeight+2);
 if(scrollable){await swipe(205,height-150,0,-110);await p.waitForTimeout(350);assert.equal(await menu.getAttribute('open'),'');assert.ok(await panel.evaluate(n=>n.scrollTop>20),'native menu scrolling');}
 await panel.evaluate(n=>n.scrollTop=n.scrollHeight);await p.waitForTimeout(100);
 await swipe(205,height-130,0,-25);await p.waitForTimeout(100);assert.equal(await menu.getAttribute('open'),'','short swipe preserved');
 await swipe(205,height-130,100,-3);await p.waitForTimeout(100);assert.equal(await menu.getAttribute('open'),'','horizontal swipe preserved');
 await swipe(205,height-130,0,-105);
 assert.equal(await dock.evaluate(n=>getComputedStyle(n).visibility),'hidden','dock remains hidden during close animation');
 await restored();await p.screenshot({path:`${out}/${route}-${height}-restored.png`});
 await open();const brand=await menu.locator('.reference4-menu__brand').boundingBox();await swipe(brand.x+35,brand.y+brand.height-8,0,-80);await restored();
 await open();await menu.locator('[data-reference4-service-open]').click();
 assert.equal(await menu.locator('[data-reference4-service]').getAttribute('aria-hidden'),'false');
 await menu.locator('[data-reference4-service-back]').click();
 assert.equal(await menu.locator('[data-reference4-main]').getAttribute('aria-hidden'),'false');
 await menu.locator('[data-reference4-collections-open]').click();
 await p.keyboard.press('Escape');await restored();
 await open();assert.equal(await menu.locator('[data-reference4-main]').getAttribute('aria-hidden'),'false');
 await p.setViewportSize({width:1200,height:844});await p.waitForTimeout(450);
 assert.equal(await menu.getAttribute('open'),null);assert.equal(await p.locator('html').evaluate(n=>n.classList.contains('shell-menu-open')),false);
 await p.setViewportSize({width:390,height});await p.waitForTimeout(300);

 await open();await menu.locator('[data-reference4-close]').click();await restored();
 // Calendar still opens after menu ownership has been released.
 await p.locator('[data-calendar-open]').click();assert.equal(await dock.getAttribute('data-expanded'),'true');await p.keyboard.press('Escape');assert.equal(await dock.getAttribute('data-expanded'),'false');
 await open();await menu.locator('[data-reference4-main] a').filter({hasText:'Популярное'}).click();await p.waitForURL('**/populyarnoe/');
 assert.deepEqual(errors,[]);checks.push({route,height,scrollable,nativeUp:true,shortHorizontalPreserved:true,headerUp:true,occlusionThroughClose:true,restoreFocusDateCalendar:true,escapeClose:true,navigation:true,errors});await p.close();
}
// Native details fallback must occlude even before controller scripts run.
const staticPage=await browser.newPage({viewport:{width:390,height:844},javaScriptEnabled:false});
await staticPage.goto(`${base}/vyhodnye/`);await staticPage.locator('[data-reference4-fullscreen] > summary').click();
assert.equal(await staticPage.locator('[data-mobile-bottom-nav]').evaluate(n=>getComputedStyle(n).visibility),'hidden');await staticPage.close();
const reduced=await browser.newPage({viewport:{width:390,height:844},reducedMotion:'reduce'});
await reduced.goto(`${base}/date-2026-07-23/`);await reduced.waitForFunction(()=>document.body.hasAttribute('data-date-dock'));
await reduced.locator('[data-reference4-fullscreen] > summary').click();await reduced.waitForTimeout(50);
await reduced.keyboard.press('Escape');await reduced.waitForTimeout(50);
assert.equal(await reduced.locator('[data-date-dock-surface]').evaluate(n=>n.inert),false);await reduced.close();
await browser.close();writeFileSync(out+'/checks.json',JSON.stringify(checks,null,2));console.log('PASS menu/dock ownership + native swipe',checks.length);
