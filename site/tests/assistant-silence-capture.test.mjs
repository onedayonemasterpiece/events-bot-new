// Deterministic capture lifecycle model, not native microphone evidence.
import test from 'node:test';
import assert from 'node:assert/strict';
import { MicrophoneCapture } from '../src/lib/assistant/microphoneCapture.ts';
import { segmentPcm16 } from '../src/lib/assistant/audioSegments.ts';
const budget={maxWireBytes:1048576,envelopeBytes:8192,encoding:'base64'};
let node, recorder, track, restoring, onClose;
class FakeContext {
 constructor(){this.sampleRate=16000;this.state='running';this.audioWorklet={addModule:async()=>{}};}
 async resume(){} async close(){this.state='closed';onClose();} createGain(){return {gain:{},connect(){}};}
 createMediaStreamSource(){return {connect(){}};}
}
class FakeNode {
 constructor(){node=this;this.frames=0;this.tail=Float32Array.of(-0.012,0.004);this.port={close(){},postMessage:()=>queueMicrotask(()=>{
  this.emit(this.tail);this.port.onmessage({data:{type:'stopped',frames:this.frames,sampleRate:16000}});
 })};}
 emit(pcm){const firstFrame=this.frames;this.frames+=pcm.length;this.port.onmessage({data:{type:'pcm',pcm,firstFrame,sampleRate:16000}});}
 connect(){} disconnect(){}
}
class FakeRecorder {
 static isTypeSupported(mime){return mime==='audio/webm;codecs=opus';}
 constructor(_,options){recorder=this;this.mimeType=options.mimeType;this.state='inactive';}
 start(){this.state='recording';this.emit(1);}
 emit(value){this.ondataavailable?.({data:new Blob([Uint8Array.of(value)],{type:this.mimeType})});}
 stop(){this.state='inactive';queueMicrotask(()=>{assert.equal(track.stopped,false);this.emit(9);this.onstop?.();});}
}
test.beforeEach(()=>{
 onClose=()=>{};
 track={stopped:false,stop(){this.stopped=true;}};
 const stream={getTracks:()=>[track],getAudioTracks:()=>[track]};restoring=[];
 for(const [name,value] of Object.entries({isSecureContext:true,AudioContext:FakeContext,AudioWorkletNode:FakeNode,MediaRecorder:FakeRecorder,
  navigator:{mediaDevices:{getUserMedia:async()=>stream}},document:{hidden:false,addEventListener(){},removeEventListener(){}},window:{addEventListener(){},removeEventListener(){}}})){
  restoring.push([name,Object.getOwnPropertyDescriptor(globalThis,name)]);Object.defineProperty(globalThis,name,{configurable:true,writable:true,value});
 }
});
test.afterEach(()=>{for(const [name,descriptor] of restoring){if(descriptor)Object.defineProperty(globalThis,name,descriptor);else delete globalThis[name];}});
async function setup(extra={}){
 const pcm=[],compressed=[],states=[],receipts=[];const capture=new MicrophoneCapture({workletUrl:'/fixture',budget,onPart:async p=>pcm.push(p),onCompressedPart:async p=>compressed.push(p),onStatus:(...v)=>states.push(v),onStopped:r=>receipts.push(r),...extra});await capture.start();return {capture,pcm,compressed,states,receipts};
}
function utterance(){const pcm=new Float32Array(16000*2);pcm.fill(0.01,0,3200);return pcm;}
test('automatic silence drains ALL PCM and compressed tails before one complete receipt',async()=>{
 const s=await setup();const input=utterance();node.emit(input);assert.equal(s.capture.status,'stopping');
 const receipt=await s.capture.stop();assert.equal(receipt.reason,'silence');assert.equal(receipt.complete,true);assert.equal(receipt.captureComplete,true);
 assert.equal(receipt.frames,input.length+2);assert.equal(receipt.savedFrames,receipt.frames);assert.equal(s.receipts.length,1);
 const original=new Float32Array(input.length+2);original.set(input);original.set(node.tail,input.length);
 const expected=segmentPcm16(original,16000,budget)[0].bytes.subarray(44);
 const actual=Buffer.concat(s.pcm.map(p=>p.bytes.subarray(44)));assert.deepEqual(actual,Buffer.from(expected));
 assert.deepEqual(s.compressed.map(p=>p.bytes[0]),[1,9]);assert.equal(receipt.compressed.complete,true);assert.equal(track.stopped,true);
});
test('manual-only opt-out does not auto-stop',async()=>{
 const s=await setup({silenceDetection:false});node.emit(utterance());assert.equal(s.capture.status,'recording');assert.equal((await s.capture.stop()).reason,'user');
});
test('first silence does not auto-stop',async()=>{
 const s=await setup();node.emit(new Float32Array(16000*3));assert.equal(s.capture.status,'recording');assert.equal((await s.capture.stop()).complete,true);
});
test('background racing automatic flush cannot produce an accepted utterance',async()=>{
 const s=await setup();node.emit(utterance());const receipt=await s.capture.stop('background');
 assert.equal(receipt.reason,'background');assert.equal(receipt.captureComplete,false);assert.equal(receipt.complete,false);assert.equal(receipt.frames,receipt.savedFrames);
});
test('hidden PCM cannot become a silence receipt',async()=>{
 const s=await setup();document.hidden=true;node.emit(utterance());assert.equal((await s.capture.stop()).reason,'background');
});
test('visibility changing during final context close is still a partial background stop',async()=>{
 const s=await setup();node.emit(utterance());onClose=()=>{document.hidden=true;};
 const receipt=await s.capture.stop();assert.equal(receipt.reason,'background');assert.equal(receipt.complete,false);
});
test('next capture starts with fresh endpoint evidence',async()=>{
 const s=await setup();node.emit(utterance());await s.capture.stop();track.stopped=false;
 await s.capture.start();node.emit(new Float32Array(16000*3));assert.equal(s.capture.status,'recording');assert.equal((await s.capture.stop()).reason,'user');
});
test('silent-stop storage failure keeps capture completeness for local byte recovery',async()=>{
 let fail=true;const s=await setup({onPart:async()=>{if(fail)throw Error('fixture disk');}});node.emit(utterance());const before=await s.capture.stop();
 assert.equal(before.reason,'storage_failed');assert.equal(before.complete,false);assert.equal(before.captureComplete,true);assert.ok(s.capture.unsavedParts().length);
 fail=false;await s.capture.retryUnsaved();assert.equal(s.capture.receipt().complete,true);assert.equal(s.capture.receipt().frames,s.capture.receipt().savedFrames);
});
