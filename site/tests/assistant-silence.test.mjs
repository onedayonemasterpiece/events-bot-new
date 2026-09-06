import test from 'node:test';
import assert from 'node:assert/strict';
import { SilenceEndpointDetector, SILENCE_ENDPOINT_MS } from '../src/lib/assistant/silenceEndpoint.ts';
const samples = (ms, amplitude, rate = 16000) => new Float32Array(Math.round(rate * ms / 1000)).fill(amplitude);
test('initial silence and a short click never arm an automatic endpoint', () => {
 const d = new SilenceEndpointDetector();
 assert.equal(d.push(samples(10000, 0),16000),false);
 assert.equal(d.push(samples(40, 0.05),16000),false);
 assert.equal(d.push(samples(10000, 0),16000),false);
});
test('sound followed by exactly 1500 ms observed silence ends without mutating bytes', () => {
 const d = new SilenceEndpointDetector(); const speech=samples(200,0.01);const copy=speech.slice();
 assert.equal(d.push(speech,16000),false);assert.deepEqual(speech,copy);
 assert.equal(d.push(samples(SILENCE_ENDPOINT_MS-20,0),16000),false);
 assert.equal(d.push(samples(20,0),16000),true);
});
test('quiet words and noisy ambiguity reset the quiet interval instead of being trimmed', () => {
 const d = new SilenceEndpointDetector();d.push(samples(200,0.01),16000);
 assert.equal(d.push(samples(1400,0),16000),false);
 assert.equal(d.push(samples(100,0.002),16000),false);
 assert.equal(d.push(samples(1400,0),16000),false);
 assert.equal(d.push(samples(100,0),16000),true);
 const noisy = new SilenceEndpointDetector();noisy.push(samples(200,0.01),16000);
 assert.equal(noisy.push(samples(30000,0.002),16000),false);
});
test('unavailable analysis and sample-rate changes require fresh sound evidence', () => {
 for(const bad of [null,Float32Array.of(NaN),Float32Array.of(Infinity),new Float32Array()]){
  const d=new SilenceEndpointDetector();d.push(samples(200,0.01),16000);
  assert.equal(d.push(bad,16000),false);assert.equal(d.push(samples(5000,0),16000),false);
 }
 const d=new SilenceEndpointDetector();d.push(samples(200,0.01),16000);
 assert.equal(d.push(samples(5000,0,48000),48000),false);
 d.push(samples(200,0.01,48000),48000);d.reset();assert.equal(d.push(samples(5000,0,48000),48000),false);
});
test('endpoint evidence is independent of transport boundaries and sample rate', () => {
 for(const rate of [8000,16000,44100,48000,96000]){
  const d=new SilenceEndpointDetector();const pcm=new Float32Array(rate*2);
  pcm.set(samples(200,0.01,rate));let endpointAt=0;
  for(let offset=0;offset<pcm.length;offset+=137){if(d.push(pcm.subarray(offset,offset+137),rate)){endpointAt=Math.min(offset+137,pcm.length)/rate*1000;break;}}
  assert.ok(endpointAt>=1700 && endpointAt<1740,`${rate}: ${endpointAt}`);
 }
});
