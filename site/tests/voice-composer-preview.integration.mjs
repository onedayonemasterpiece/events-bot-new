// Anonymous rendered preview only. Does not claim user login, physical mic or ASR.
import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdirSync} from 'node:fs';
import {chromium} from 'playwright';
const base=process.env.VOICE_PREVIEW_BASE;
const output=process.env.VOICE_PREVIEW_EVIDENCE;
for(const width of [1440,1280,390])test(`anonymous floating composer ${width}px`,{skip:!base},async()=>{
 const browser=await chromium.launch({executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH||undefined});
 try{
 const p=await browser.newPage({viewport:{width,height:900}});const errors=[],requests=[];
 p.on('pageerror',error=>errors.push(error.message));p.on('request',request=>{if(new URL(request.url()).pathname.includes('/assistant/'))requests.push(request.url());});
 await p.goto(base+'/poisk/',{waitUntil:'networkidle'});
 const launcher=p.locator('[data-assistant-launcher]'),dialog=p.locator('[data-assistant-composer]');
 assert.equal(await launcher.isVisible(),true);assert.equal(await launcher.isDisabled(),false);assert.equal(await dialog.isVisible(),false);
 assert.equal(await p.locator('[data-assistant-history]').isVisible(),false);
 assert.equal(await launcher.evaluate(e=>getComputedStyle(e.closest('[data-assistant-dock]')).position),'fixed');assert.notEqual(await p.locator('.authorized-search__head h1').evaluate(e=>getComputedStyle(e).position),'fixed');
 const rect=await launcher.boundingBox();assert.ok(rect.x>=0&&rect.x+rect.width<=width&&rect.y+rect.height<=900);
 if(width<760){const nav=await p.locator('[data-mobile-bottom-nav]').boundingBox();assert.ok(rect.y+rect.height<=nav.y-8);}
 assert.equal(await p.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);assert.equal(Math.round(rect.width),Math.round(rect.height));
 if(output){mkdirSync(output,{recursive:true});await p.screenshot({path:`${output}/${width}-page.png`});}
 await launcher.click();await p.waitForFunction(()=>document.querySelector('[data-assistant-auth]')?.textContent?.includes('Войдите'));
 assert.equal(await dialog.isVisible(),true);assert.equal(await p.locator('[data-assistant-login]').isVisible(),true);if(await p.locator('[data-assistant]').getAttribute('data-assistant-capture-only')==='true')assert.match(await p.locator('[data-assistant-preview-limit]').textContent(),/пока не подключены/);else{assert.equal(await p.locator('[data-assistant]').getAttribute('data-assistant-host'),'devcoveer');assert.equal(await p.locator('[data-assistant-preview-limit]').count(),0);}
 const modal=await dialog.boundingBox();assert.ok(modal.x>=0&&modal.x+modal.width<=width);assert.equal(await p.locator('dialog[data-assistant-composer]').count(),0);
 if(output)await p.screenshot({path:`${output}/${width}-composer.png`});
 await p.locator('[data-assistant-close]').click();assert.equal(await dialog.isVisible(),false);
 await launcher.focus();await p.keyboard.press('Enter');assert.equal(await dialog.isVisible(),true);assert.equal(await dialog.count(),1);
 await p.locator('[data-assistant-close]').click();await p.reload({waitUntil:'networkidle'});assert.equal(await dialog.isVisible(),false);assert.deepEqual(errors,[]);assert.deepEqual(requests,[]);
 await p.evaluate(()=>{document.querySelector('[data-assistant-dock]').dataset.captureState='recording';document.querySelector('[data-assistant-live]').textContent='Записываю · нажмите круг, чтобы остановить';document.querySelector('[data-assistant-details]').hidden=true;const timer=document.querySelector('[data-assistant-timer]');timer.hidden=false;timer.textContent='0:12';});
 assert.equal(await p.locator('.assistant__mic-halo').evaluate(e=>getComputedStyle(e).animationName),'assistant-mic-breathe');
 if(output)await p.screenshot({path:`${output}/${width}-recording-render-fixture.png`});
 await p.emulateMedia({reducedMotion:'reduce'});assert.equal(await p.locator('.assistant__mic-halo').evaluate(e=>getComputedStyle(e).animationName),'none');
 }finally{await browser.close();}
});
