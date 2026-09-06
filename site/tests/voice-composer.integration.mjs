// Real Chromium UI behavior; injected callbacks, NOT live Auth/microphone/ASR.
import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {build} from 'esbuild';
import {chromium} from 'playwright';
let server,browser,origin;
test.before(async()=>{
 const bundle=(await build({stdin:{contents:`import {mountComposerPresentation} from './src/lib/assistant/composerPresentation.ts';window.ui=mountComposerPresentation(document.querySelector('[data-assistant]'));window.activations=0;window.ui.bind(()=>{window.activations++;},async()=>{await window.stopPending;});`,resolveDir:new URL('../',import.meta.url).pathname,loader:'ts'},bundle:true,format:'esm',write:false})).outputFiles[0].text;
 const html=`<!doctype html><div data-assistant><div data-assistant-dock><span data-assistant-timer hidden></span><span data-assistant-live></span><button data-assistant-details aria-expanded="false">Запрос и записи</button><button data-assistant-launcher aria-pressed="false">Микрофон</button></div><section data-assistant-composer hidden><button data-assistant-close>Закрыть</button><button data-assistant-record>Записать</button><p data-assistant-processing></p></section></div><script type="module" src="/bundle.js"></script>`;
 server=createServer((req,res)=>{res.setHeader('Content-Type',req.url==='/bundle.js'?'text/javascript':'text/html');res.end(req.url==='/bundle.js'?bundle:html);});await new Promise(r=>server.listen(0,'127.0.0.1',r));origin=`http://127.0.0.1:${server.address().port}`;
 browser=await chromium.launch({executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH||undefined});
});
test.after(async()=>{await browser?.close();await new Promise(r=>server?.close(r));});
test('mic starts without panel/focus theft; toggle keeps page usable; details are explicit',async()=>{
 const p=await browser.newPage();await p.goto(origin);await p.waitForFunction(()=>window.ui);
 const panel=p.locator('[data-assistant-composer]'),mic=p.locator('[data-assistant-launcher]');
 await mic.click();assert.equal(await p.evaluate(()=>window.activations),1);assert.equal(await panel.isVisible(),false);
 await p.evaluate(()=>window.ui.setCapture('recording','Записываю'));
 assert.equal(await mic.getAttribute('aria-pressed'),'true');assert.equal(await p.locator('[data-assistant-timer]').isVisible(),true);
 assert.equal(await p.locator('[data-assistant-details]').isVisible(),false);
 await p.keyboard.press('Enter');assert.equal(await p.evaluate(()=>window.activations),2);
 await p.evaluate(()=>window.ui.setCapture('saved','Запись сохранена'));
 assert.equal(await mic.getAttribute('aria-pressed'),'false');assert.equal(await panel.isVisible(),false);
 await p.locator('[data-assistant-details]').click();assert.equal(await panel.isVisible(),true);
 await p.locator('[data-assistant-close]').click();assert.equal(await panel.isVisible(),false);await p.close();
});
test('failed mount retains visible feedback through responsive mic',async()=>{
 const p=await browser.newPage();await p.goto(origin);await p.waitForFunction(()=>window.ui);await p.evaluate(()=>window.ui.fail());
 await p.locator('[data-assistant-launcher]').click();assert.match(await p.locator('[data-assistant-live]').textContent(),/не подключился/);
 assert.equal(await p.locator('[data-assistant-composer]').isVisible(),false);await p.close();
});
