// Native Chromium capture + IndexedDB; synthetic device, NOT an ASR/phone test.
import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { build } from 'esbuild';
import { chromium } from 'playwright';
const root = new URL('../', import.meta.url).pathname;
let server, browser, origin, bundle;
test.before(async () => {
  bundle = (await build({ stdin: { contents: `
    import { MicrophoneCapture } from './src/lib/assistant/microphoneCapture.ts';
    import { VoiceStore } from './src/lib/assistant/voiceStore.ts';
    window.modules = { MicrophoneCapture, VoiceStore };
    `, resolveDir: root, loader: 'ts' }, bundle: true, write: false, format: 'esm' })).outputFiles[0].text;
  const worklet = await readFile(new URL('../public/voice/pcm-capture-worklet.js', import.meta.url));
  server = createServer((req, res) => {
    if (req.url === '/bundle.js') { res.setHeader('Content-Type','text/javascript'); res.end(bundle); }
    else if (req.url === '/voice/pcm-capture-worklet.js') { res.setHeader('Content-Type','text/javascript'); res.end(worklet); }
    else { res.setHeader('Content-Type','text/html'); res.end('<!doctype html><button id="start">Запись</button><button id="stop">Стоп</button><script type="module" src="/bundle.js"></script>'); }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  origin = `http://127.0.0.1:${server.address().port}`;
  browser = await chromium.launch({ executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH || undefined,
    args: ['--use-fake-device-for-media-stream', '--no-sandbox'] });
});
test.after(async () => { await browser?.close(); await new Promise(resolve => server?.close(resolve)); });
async function setup(permissions = ['microphone']) {
  const context = await browser.newContext({ permissions }); const page = await context.newPage();
  await page.goto(origin); await page.waitForFunction(() => !!window.modules);
  return { context, page };
}
test('native microphone/worklet stop acknowledges all locally committed WAV parts; reload retains owner-scoped data', async () => {
  const {context, page} = await setup();
  await page.evaluate(async () => {
    const {VoiceStore, MicrophoneCapture} = window.modules;
    window.store = await VoiceStore.open(); await store.create('owner-a','recording-a');
    window.states = [];
    window.capture = new MicrophoneCapture({workletUrl:'/voice/pcm-capture-worklet.js',
      budget: {maxWireBytes:32768,envelopeBytes:2048,encoding:'base64'},
      onPart: part => store.putPart('owner-a','recording-a',part), onStatus: status => states.push(status)});
    document.querySelector('#start').onclick = () => { window.start = capture.start(); };
    document.querySelector('#stop').onclick = () => { window.stop = capture.stop().then(async receipt => {
      await store.finish('owner-a','recording-a',receipt); return receipt;
    }); };
  });
  await page.click('#start'); await page.waitForFunction(() => states.includes('recording'));
  await page.waitForTimeout(750); await page.click('#stop');
  const receipt = await page.evaluate(() => window.stop);
  assert.equal(receipt.complete, true); assert.ok(receipt.frames > 1000);
  assert.equal(receipt.frames, receipt.savedFrames); assert.ok(receipt.partCount > 1);
  assert.ok([16000,44100,48000].includes(receipt.sampleRate));
  await page.reload(); await page.waitForFunction(() => !!window.modules);
  const result = await page.evaluate(async () => {
    const store = await window.modules.VoiceStore.open();
    const own = await store.page('recordings','owner-a'); const other = await store.page('recordings','owner-b');
    const parts = await store.parts('owner-a','recording-a');
    const before = own[0].partCount; await store.putPart('owner-a','recording-a',parts[0]);
    let conflict = false;
    try { const bad = structuredClone(parts[0]); bad.bytes[45] ^= 1; await store.putPart('owner-a','recording-a',bad); }
    catch (e) { conflict = e.message === 'audio_payload_conflict'; }
    const after = (await store.page('recordings','owner-a'))[0].partCount;
    return { own, other, frames: parts.reduce((n,p)=>n+p.frameCount,0), indexes:parts.map(p=>p.index), before,after,conflict };
  });
  assert.equal(result.own[0].state,'saved'); assert.equal(result.other.length,0);
  assert.equal(result.frames,receipt.frames); assert.equal(result.before,result.after); assert.equal(result.conflict,true);
  assert.deepEqual(result.indexes,Array.from({length:receipt.partCount},(_,i)=>i));
  await context.close();
});
test('native permission denial has no fabricated transcript, part or successful receipt', async () => {
  const {context,page} = await setup([]);
  const cdp = await context.newCDPSession(page);
  const {targetInfo}=await cdp.send('Target.getTargetInfo');
  await cdp.send('Browser.setPermission', { permission:{name:'audioCapture'}, setting:'denied', origin, browserContextId:targetInfo.browserContextId });
  const result = await page.evaluate(async () => {
    let saved=0; const states=[];
    const capture = new window.modules.MicrophoneCapture({workletUrl:'/voice/pcm-capture-worklet.js',
      budget:{maxWireBytes:32768,envelopeBytes:2048,encoding:'base64'},onPart:async()=>{saved++;},onStatus:(state,reason)=>states.push({state,reason})});
    try { await capture.start(); } catch {}
    return {saved,states};
  });
  assert.equal(result.saved,0); assert.equal(result.states.at(-1).state,'error');
  assert.equal(result.states.at(-1).reason,'microphone_denied'); await context.close();
});
test('local answer history paginates without erasing earlier pages or another owner', async () => {
  const {context,page}=await setup();
  const result=await page.evaluate(async()=>{
    const store=await window.modules.VoiceStore.open();
    for(let i=0;i<55;i++) await store.saveAnswer('owner-a',`answer-${String(i).padStart(2,'0')}`,{items:[],title:`Answer ${i}`});
    const first=await store.page('answers','owner-a',undefined,20); const last=first.at(-1);
    const second=await store.page('answers','owner-a',[last.createdAt,last.id],20);
    return {first:first.map(r=>r.id),second:second.map(r=>r.id),other:await store.page('answers','owner-b')};
  });
  assert.equal(result.first.length,20); assert.equal(result.second.length,20);
  assert.equal(new Set([...result.first,...result.second]).size,40); assert.equal(result.other.length,0);
  await context.close();
});
