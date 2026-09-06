// Real Chromium UI behavior; injected callbacks, NOT live Auth/microphone/ASR.
import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {build} from 'esbuild';
import {chromium} from 'playwright';
let server,browser,origin;
test.before(async()=>{
 const bundle=(await build({stdin:{contents:`import {mountComposerPresentation} from './src/lib/assistant/composerPresentation.ts';window.ui=mountComposerPresentation(document.querySelector('[data-assistant]'));window.activations=0;window.ui.bind(()=>{window.activations++;},async()=>{await window.stopPending;});`,resolveDir:new URL('../',import.meta.url).pathname,loader:'ts'},bundle:true,format:'esm',write:false})).outputFiles[0].text;
 const html=`<!doctype html><div data-assistant><button data-assistant-launcher aria-expanded="false">Голосом</button><dialog data-assistant-composer><button data-assistant-close>Закрыть</button><button data-assistant-record>Записать</button><p data-assistant-processing></p></dialog></div><script type="module" src="/bundle.js"></script>`;
 server=createServer((req,res)=>{res.setHeader('Content-Type',req.url==='/bundle.js'?'text/javascript':'text/html');res.end(req.url==='/bundle.js'?bundle:html);});await new Promise(r=>server.listen(0,'127.0.0.1',r));origin=`http://127.0.0.1:${server.address().port}`;
 browser=await chromium.launch({executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH||undefined});
});
test.after(async()=>{await browser?.close();await new Promise(r=>server?.close(r));});
test('floating entry opens the same composer; Escape waits for capture and returns focus',async()=>{
 const p=await browser.newPage();await p.goto(origin);await p.waitForFunction(()=>window.ui);
 assert.equal(await p.locator('dialog').isVisible(),false);
 await p.locator('[data-assistant-launcher]').click();assert.equal(await p.evaluate(()=>window.activations),1);assert.equal(await p.locator('dialog').isVisible(),true);
 await p.evaluate(()=>{window.stopPending=new Promise(resolve=>{window.finishStop=resolve;});});
 await p.keyboard.press('Escape');assert.equal(await p.locator('dialog').isVisible(),true);
 await p.evaluate(()=>window.finishStop());await p.waitForFunction(()=>!document.querySelector('dialog').open);
 assert.equal(await p.locator('[data-assistant-launcher]').getAttribute('aria-expanded'),'false');assert.equal(await p.locator('[data-assistant-launcher]').evaluate(e=>e===document.activeElement),true);
 await p.keyboard.press('Enter');assert.equal(await p.evaluate(()=>window.activations),2);await p.close();
});
test('failed mount keeps launcher responsive and displays explanation instead of silent disabled entry',async()=>{
 const p=await browser.newPage();await p.goto(origin);await p.waitForFunction(()=>window.ui);await p.evaluate(()=>window.ui.fail());
 await p.locator('[data-assistant-launcher]').click();assert.equal(await p.locator('dialog').isVisible(),true);assert.match(await p.locator('[data-assistant-processing]').textContent(),/не подключился/);assert.equal(await p.locator('[data-assistant-record]').isDisabled(),true);
 await p.locator('[data-assistant-close]').click();await p.waitForFunction(()=>!document.querySelector('dialog').open);await p.close();
});
