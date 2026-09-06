import {test} from 'node:test';
import assert from 'node:assert/strict';
import {segmentPcm16, validateAudioParts, wireBytes, maxWavBytes} from '../src/lib/assistant/audioSegments.ts';
const budget={maxWireBytes:1024*1024,envelopeBytes:4096,encoding:'base64'};
for(const rate of [8000,16000,44100,48000,96000])test(`actual rate ${rate}; complete PCM across boundaries`,()=>{
 const x=Float32Array.from({length:rate*3+17},(_,i)=>Math.sin(i*.1)*.75);
 const b={...budget,maxWireBytes:8105};const parts=segmentPcm16(x,rate,b);validateAudioParts(parts,x.length,b);
 assert.equal(parts.reduce((n,p)=>n+p.frameCount,0),x.length);
 for(const p of parts){assert.ok(wireBytes(p,b)<=b.maxWireBytes);const v=new DataView(p.bytes.buffer);assert.equal(v.getUint32(24,true),rate);
  for(let i=0;i<p.frameCount;i++)assert.ok(Math.abs(v.getInt16(44+i*2,true)/32768-x[p.firstFrame+i])<.0001);
 }
});
for(const encoding of ['binary','base64'])test(`${encoding} fits exact envelope with no truncated last sample`,()=>{
 const b={...budget,encoding,maxWireBytes:4110,envelopeBytes:100};const x=new Float32Array(5001);x[5000]=.8;
 const parts=segmentPcm16(x,16000,b);validateAudioParts(parts,x.length,b);assert.equal(parts.at(-1).firstFrame+parts.at(-1).frameCount,5001);
});
for(const n of [NaN,Infinity,-Infinity])test(`reject nonfinite PCM ${n}`,()=>assert.throws(()=>segmentPcm16(new Float32Array([0,n]),16000,budget),/non_finite/));
test('reject silence trimming disguised as transport split',()=>{
 const x=new Float32Array(6000);x[123]=.2;x[5999]=-.1;const b={...budget,maxWireBytes:1000,envelopeBytes:100};
 const parts=segmentPcm16(x,16000,b);validateAudioParts(parts,6000,b);assert.equal(parts[0].firstFrame,0);
});
test('insufficient wire room does not loop',()=>assert.throws(()=>maxWavBytes({...budget,envelopeBytes:budget.maxWireBytes-10}),/too_small/));
test('tiny packets cannot allocate unlimited parts',()=>assert.throws(()=>segmentPcm16(new Float32Array(100000),16000,{maxWireBytes:64,envelopeBytes:0,encoding:'binary'}),/too_many_parts/));
test('empty capture is not a valid utterance',()=>assert.throws(()=>segmentPcm16(new Float32Array(),16000,budget),/sample_count/));
test('metadata must match actual WAV header',()=>{
 const p=segmentPcm16(new Float32Array(100),16000,budget);p[0].sampleRate=48000;assert.throws(()=>validateAudioParts(p,100,budget),/wav_header/);
});
test('overlap and missing parts are rejected',()=>{
 const b={maxWireBytes:300,envelopeBytes:30,encoding:'binary'},p=segmentPcm16(new Float32Array(1000),16000,b);
 const overlap=structuredClone(p);overlap[1].firstFrame--;assert.throws(()=>validateAudioParts(overlap,1000,b),/gap_or_overlap/);
 assert.throws(()=>validateAudioParts(p.slice(0,-1),1000,b),/incomplete/);
});
test('sliced Uint8Array headers use actual byteOffset',()=>{
 const p=segmentPcm16(new Float32Array(100),16000,budget);const wrap=new Uint8Array(p[0].bytes.length+7);wrap.set(p[0].bytes,7);p[0].bytes=wrap.subarray(7);validateAudioParts(p,100,budget);
});
test('tampered PCM codec is rejected before interpretation',()=>{
 const p=segmentPcm16(new Float32Array(100),16000,budget);p[0].bytes[20]=3;assert.throws(()=>validateAudioParts(p,100,budget),/wav_header/);
});
