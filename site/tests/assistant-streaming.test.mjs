import test from 'node:test';
import assert from 'node:assert/strict';
import { StreamingPcm16 } from '../src/lib/assistant/streamingAudio.ts';
import { validateAudioParts } from '../src/lib/assistant/audioSegments.ts';
const budget = { maxWireBytes: 4096, envelopeBytes: 128, encoding: 'base64' };
test('streaming framing preserves a long utterance beyond the batch sample cap', () => {
  const stream = new StreamingPcm16(16000, budget); let frames = 0, next = 0, index = 0;
  for (let i = 0; i < 1200; i++) {
    const pcm = new Float32Array(4096); pcm[0] = -0.0001;
    for (const part of stream.push(pcm)) {
      assert.equal(part.firstFrame, next); assert.equal(part.index, index++); next += part.frameCount;
      assert.ok(part.bytes.length <= 4096);
    }
    frames += pcm.length;
  }
  for (const part of stream.finish()) { assert.equal(part.firstFrame, next); next += part.frameCount; }
  assert.ok(frames > 4 * 1024 * 1024); assert.equal(next, frames); assert.equal(stream.frames, frames);
});
test('streaming and batch WAV validation agree on continuous parts', () => {
  const stream = new StreamingPcm16(44100, budget);
  const parts = [...stream.push(Float32Array.from({length: 10003}, (_, i) => Math.sin(i) / 10)), ...stream.finish()];
  validateAudioParts(parts, 10003, budget);
  assert.deepEqual(stream.finish(), []); assert.throws(() => stream.push(new Float32Array(1)), /capture_closed/);
});
test('gap and invalid PCM are rejected before any mutation', () => {
  const stream = new StreamingPcm16(48000, budget);
  assert.throws(() => stream.push(Float32Array.from([0, NaN])), /non_finite/);
  assert.equal(stream.frames, 0);
  assert.throws(() => stream.push(new Float32Array(1), 9), /gap_or_overlap/);
  assert.equal(stream.frames, 0);
});
test('one-second checkpoint cap is lossless and stays within the wire budget',()=>{
 const s=new StreamingPcm16(16000,{maxWireBytes:1048576,envelopeBytes:8192,encoding:'base64'},16000);
 const parts=s.push(new Float32Array(36000));assert.equal(parts.length,2);assert.deepEqual(parts.map(p=>[p.firstFrame,p.frameCount]),[[0,16000],[16000,16000]]);
 assert.equal(s.finish()[0].frameCount,4000);assert.equal(s.frames,36000);
 assert.throws(()=>new StreamingPcm16(16000,{maxWireBytes:1048576,envelopeBytes:8192,encoding:'base64'},0),/invalid_checkpoint_frames/);
});
