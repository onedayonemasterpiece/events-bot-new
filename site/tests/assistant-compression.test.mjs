import test from 'node:test';
import assert from 'node:assert/strict';
import { CompressedCapture } from '../src/lib/assistant/compressedCapture.ts';
class Recorder {
 static support = ['audio/webm;codecs=opus']; static instances = [];
 static isTypeSupported(type) { return this.support.includes(type); }
 constructor(stream, options) { this.mimeType = options.mimeType; this.options = options; this.state = 'inactive'; Recorder.instances.push(this); }
 start(interval) { this.interval = interval; this.state = 'recording'; }
 emit(value) { this.ondataavailable?.({data: new Blob([Uint8Array.of(value)], {type: this.mimeType})}); }
 stop() { this.state = 'inactive'; queueMicrotask(() => { this.emit(9); this.onstop?.(); }); }
}
test.beforeEach(() => { globalThis.MediaRecorder = Recorder; Recorder.support = ['audio/webm;codecs=opus']; Recorder.instances = []; });
test.afterEach(() => { delete globalThis.MediaRecorder; });
test('Opus is preferred to AAC; final event and durable transaction finish before complete receipt', async () => {
 Recorder.support.push('audio/mp4'); const writes = []; let release;
 const c = new CompressedCapture({}, async part => { if (part.index === 1) await new Promise(r => {release = r;}); writes.push(part); });
 const recorder = Recorder.instances[0]; assert.equal(recorder.options.audioBitsPerSecond, 32000); assert.equal(recorder.interval, 1000);
 recorder.emit(1); const stopped = c.stop(); assert.equal(c.stop(), stopped);
 while (!release) await new Promise(r => setTimeout(r, 0)); let done = false; stopped.then(() => {done = true;});
 assert.equal(done, false); release(); const receipt = await stopped;
 assert.deepEqual(receipt, {mimeType:'audio/webm;codecs=opus',partCount:2,bytes:2,complete:true});
 assert.deepEqual(writes.map(p => p.index), [0,1]); assert.deepEqual(writes.map(p=>p.bytes[0]), [1,9]);
});
test('Ogg Opus precedes AAC; AAC only on actual supported MP4 fallback', async () => {
 Recorder.support = ['audio/ogg;codecs=opus','audio/mp4'];
 assert.equal((await new CompressedCapture({}, async()=>{}).stop()).mimeType,'audio/ogg;codecs=opus');
 Recorder.support = ['audio/mp4'];
 assert.equal((await new CompressedCapture({}, async()=>{}).stop()).mimeType,'audio/mp4');
});
test('unsupported recorder and companion write failure expose explicit fallback', async () => {
 delete globalThis.MediaRecorder;
 assert.equal((await new CompressedCapture({},async()=>{}).stop()).reason,'media_recorder_unavailable');
 globalThis.MediaRecorder = Recorder;
 const c = new CompressedCapture({},async()=>{throw Error('quota');});
 Recorder.instances[0].emit(1); const receipt = await c.stop();
 assert.equal(receipt.complete,false); assert.equal(receipt.reason,'compressed_storage_or_data_failed');
});
test('unexpected recorder stop is not a successful complete recording; old callbacks cannot write new recording', async () => {
 const first = [], second = []; const c = new CompressedCapture({},async p=>first.push(p));
 const old = Recorder.instances[0], late = old.ondataavailable;
 old.state = 'inactive'; old.emit(1); old.onstop();
 const receipt = await c.stop(); assert.equal(receipt.complete,false); assert.equal(receipt.reason,'compressed_interrupted');
 const next = new CompressedCapture({},async p=>second.push(p));
 late({data:new Blob(['late'], {type:old.mimeType})}); await next.stop();
 assert.equal(first.length,1); assert.equal(second.length,1); assert.equal(second[0].index,0);
});
test('actual emitted MIME is retained rather than re-labelling audio with requested codec', async () => {
 const c = new CompressedCapture({},async()=>{});const recorder = Recorder.instances[0];
 recorder.mimeType = 'audio/webm;codecs=opus';recorder.emit(1);
 assert.equal((await c.stop()).mimeType,'audio/webm;codecs=opus');
});
test('encoder start rejection tries next supported codec without surfacing microphone failure', async () => {
 Recorder.support=['audio/webm;codecs=opus','audio/ogg;codecs=opus'];
 globalThis.MediaRecorder=class extends Recorder {start(interval){if(this.mimeType==='audio/webm;codecs=opus')throw Error('encoder unavailable');super.start(interval);}};
 const result=await new CompressedCapture({},async()=>{}).stop();assert.equal(result.complete,true);assert.equal(result.mimeType,'audio/ogg;codecs=opus');
});
test('lost native stop event times out to fallback, never to a complete compressed seal', async () => {
 const saved=[];const c = new CompressedCapture({},async p=>saved.push(p));const recorder=Recorder.instances[0];const late=recorder.ondataavailable;
 recorder.emit(1);recorder.stop=()=>{recorder.state='inactive';};
 const result=await c.stop();assert.equal(result.complete,false);assert.equal(result.reason,'compressed_stop_timeout');
 late({data:new Blob(['late'],{type:recorder.mimeType})});await Promise.resolve();assert.equal(saved.length,1);
});
