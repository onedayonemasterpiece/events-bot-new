// Real rendered preview and anonymous Auth UI, no mail/provider calls.
import test from 'node:test';import assert from 'node:assert/strict';import{mkdirSync}from'node:fs';import{chromium}from'playwright';
const base=process.env.VOICE_PREVIEW_BASE,out=process.env.VOICE_PREVIEW_EVIDENCE;
for(const width of[320,390,1440])test(`clean voice landing/auth/compact geometry ${width}`,{skip:!base},async()=>{
 const browser=await chromium.launch({executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH||undefined});
 try{const page=await browser.newPage({viewport:{width,height:900}});const errors=[],paid=[];page.on('pageerror',e=>errors.push(e.message));page.on('request',r=>{if(r.method()==='POST'&&/\/assistant\/|\/auth\/v1\/(otp|verify)/.test(r.url()))paid.push(r.url());});
 await page.goto(base+'/poisk/',{waitUntil:'networkidle'});await page.waitForFunction(()=>document.querySelector('[data-assistant]')?.dataset.assistantStartup==='signed_out');
 const mic=page.locator('[data-assistant-launcher]'),input=page.locator('[data-assistant-quick-input]');
 assert.equal(await page.locator('[data-search-form]').isVisible(),false);assert.equal(await input.isVisible(),true);assert.equal(Math.round((await mic.boundingBox()).width),128);assert.equal(await mic.evaluate(e=>getComputedStyle(e.closest('[data-assistant-dock]')).position),'relative');
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
 if(out){mkdirSync(out,{recursive:true});await page.screenshot({path:`${out}/${width}-landing.png`});}
 await mic.click();assert.equal(await page.locator('[data-assistant-sign-in]').isVisible(),true);assert.equal(await page.getByRole('button',{name:'Войти через Яндекс',exact:true}).filter({visible:true}).count(),1);assert.equal(await page.locator('.static-sign-in input[type=email]').isVisible(),true);
 if(out)await page.screenshot({path:`${out}/${width}-sign-in.png`});
 await page.getByRole('button',{name:'Закрыть вход',exact:true}).click();assert.equal(await mic.evaluate(e=>document.activeElement===e),true);
 // Explicit geometry fixture only; ASR-to-transition is checked by the live lane.
 await page.evaluate(()=>{document.querySelector('[data-assistant]').dataset.assistantPhase='conversation';});
 const r=await mic.boundingBox(),q=await input.boundingBox();assert.equal(Math.round(r.width),width<760?56:64);assert.ok(q.x+q.width<r.x);assert.ok(r.x+r.width<=width-12);
 if(width<760){const nav=await page.locator('[data-mobile-bottom-nav]').boundingBox();assert.ok(r.y+r.height<=nav.y-8);}
 if(out)await page.screenshot({path:`${out}/${width}-compact-layout-fixture.png`});
 assert.deepEqual(errors,[]);assert.deepEqual(paid,[]);
 }finally{await browser.close();}
});
