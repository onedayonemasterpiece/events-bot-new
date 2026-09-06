import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const source = fs.readFileSync(new URL('../public/voice/pcm-capture-worklet.js', import.meta.url), 'utf8');
function harness(rate = 48000) {
  const messages = []; let Processor;
  vm.runInNewContext(source, { Float32Array, sampleRate: rate,
    AudioWorkletProcessor: class { constructor() { this.port = { postMessage: m => messages.push(m) }; } },
    registerProcessor: (name, cls) => { assert.equal(name, 'kenig-voice-pcm'); Processor = cls; },
  });
  const p = new Processor();
  return { p, messages, stop: () => p.port.onmessage({ data: { type: 'stop' } }) };
}
test('capture retains quiet PCM, zeros and final short block at real rate', () => {
  const h = harness(44100);
  const pcm = Float32Array.from([0, -0.00001, 0.00001, -0.2, 0.7]);
  h.p.process([[pcm]]); h.stop();
  assert.deepEqual(Array.from(h.messages[0].pcm), Array.from(pcm));
  assert.equal(h.messages[0].sampleRate, 44100);
  assert.equal(h.messages[1].frames, 5);
  assert.equal(h.messages[1].type, 'stopped');
});
test('arbitrary render quanta retain every frame across block boundary', () => {
  const h = harness(); const expected = [];
  for (const n of [64, 512, 8192, 3]) {
    const pcm = Float32Array.from({ length: n }, (_, i) => (i % 10) / 10);
    expected.push(...pcm); h.p.process([[pcm]]);
  }
  h.stop(); let next = 0; const actual = [];
  for (const m of h.messages.filter(m => m.type === 'pcm')) {
    assert.equal(m.firstFrame, next); next += m.pcm.length; actual.push(...m.pcm);
  }
  assert.deepEqual(actual, expected); assert.equal(h.messages.at(-1).frames, expected.length);
});
test('stereo downmix and silent output do not double duration', () => {
  const h = harness(); const output = new Float32Array(3);
  h.p.process([[Float32Array.from([1, 0, -1]), Float32Array.from([-1, 0, 1])]], [[output]]);
  h.stop(); assert.deepEqual(Array.from(h.messages[0].pcm), [0, 0, 0]);
  assert.deepEqual(Array.from(output), [0, 0, 0]);
});
test('stop is idempotent and capture cannot append after acknowledgement', () => {
  const h = harness(); h.p.process([[new Float32Array(17)]]); h.stop(); h.stop();
  assert.equal(h.messages.length, 2); assert.equal(h.p.process([[new Float32Array(99)]]), false);
  assert.equal(h.messages.length, 2);
});
test('missing input does not manufacture recorded frames', () => {
  const h = harness(); h.p.process([]); h.p.process([[]]); h.stop();
  assert.equal(h.messages.length, 1); assert.equal(h.messages[0].frames, 0);
});
