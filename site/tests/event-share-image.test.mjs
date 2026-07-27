import assert from 'node:assert/strict';
import test from 'node:test';

import {
  EVENT_SHARE_IMAGE_HEIGHT,
  EVENT_SHARE_IMAGE_WIDTH,
  composeEventShareImage,
  drawEventShareImage,
  eventShareImagePayload,
  shouldComposeEventShareImage,
  wrapEventShareText,
} from '../src/lib/eventShareImage.mjs';

function fakeContext() {
  const calls = [];
  const context = {
    calls,
    fillStyle:'',
    font:'',
    beginPath() {},
    roundRect(...args) { calls.push(['roundRect', ...args]); },
    fill() { calls.push(['fill']); },
    fillRect(...args) { calls.push(['fillRect', ...args]); },
    drawImage(...args) { calls.push(['drawImage', ...args]); },
    fillText(...args) { calls.push(['fillText', ...args]); },
    measureText(text) { return { width:String(text).length * 20 }; },
    createLinearGradient() { return { addColorStop() {} }; },
  };
  return context;
}

test('share composer fails closed for OCR and unknown assets', () => {
  assert.equal(shouldComposeEventShareImage('visual_only'), true);
  assert.equal(shouldComposeEventShareImage('ocr_text'), false);
  assert.equal(shouldComposeEventShareImage('unknown'), false);
  assert.equal(shouldComposeEventShareImage('unexpected'), false);
});

test('share payload is normalized and bounded', () => {
  const payload = eventShareImagePayload({
    title:`  ${'x'.repeat(400)}  `,
    dateTime:'  27 июля, 18:00  ',
    place:'  Рыбная деревня  ',
    admission:'  Вход свободный  ',
  });
  assert.equal(payload.title.length, 280);
  assert.equal(payload.dateTime, '27 июля, 18:00');
  assert.equal(payload.place, 'Рыбная деревня');
  assert.equal(payload.admission, 'Вход свободный');
  assert.equal(Object.isFrozen(payload), true);
});

test('text wrapping truncates overflowing copy with an ellipsis', () => {
  const ctx = fakeContext();
  const lines = wrapEventShareText(ctx, 'один два три четыре пять шесть семь', 145, 2);
  assert.equal(lines.length, 2);
  assert.match(lines.at(-1), /…$/u);
});

test('drawing composes the photo, brand bitmap, title, date and place', () => {
  const ctx = fakeContext();
  const source = { width:800, height:1200, kind:'source' };
  const brand = { width:320, height:82, kind:'brand' };
  drawEventShareImage(ctx, source, {
    title:'Летний концерт',
    dateTime:'27 июля, 18:00',
    place:'Рыбная деревня',
    admission:'Вход свободный',
  }, brand);

  const imageCalls = ctx.calls.filter(([name]) => name === 'drawImage');
  assert.equal(imageCalls[0][1], source);
  assert.equal(imageCalls[1][1], brand);
  const copy = ctx.calls.filter(([name]) => name === 'fillText').map(([, text]) => text);
  assert.ok(copy.includes('Летний концерт'));
  assert.ok(copy.includes('27 июля, 18:00'));
  assert.ok(copy.includes('Рыбная деревня'));
  assert.ok(copy.includes('Вход свободный'));
});

test('compose emits a branded portrait PNG for visual-only media', async () => {
  const ctx = fakeContext();
  const outputBlob = new Blob(['png'], { type:'image/png' });
  const canvas = {
    width:0,
    height:0,
    getContext:() => ctx,
    toBlob:callback => callback(outputBlob),
  };
  const created = [];
  const createImageBitmapImpl = async blob => {
    const bitmap = { width:blob.type === 'image/svg+xml' ? 320 : 800, height:blob.type === 'image/svg+xml' ? 82 : 1200, close() { this.closed = true; } };
    created.push(bitmap);
    return bitmap;
  };
  const result = await composeEventShareImage({
    sourceBlob:new Blob(['photo'], { type:'image/jpeg' }),
    imageTextMode:'visual_only',
    title:'Летний концерт',
    dateTime:'27 июля, 18:00',
    place:'Рыбная деревня',
    admission:'Вход свободный',
    brandUrl:'/assets/logo.svg',
    documentLike:{ createElement:tag => tag === 'canvas' ? canvas : null },
    fetchImpl:async () => ({ ok:true, blob:async () => new Blob(['svg'], { type:'image/svg+xml' }) }),
    createImageBitmapImpl,
  });

  assert.equal(result, outputBlob);
  assert.equal(canvas.width, EVENT_SHARE_IMAGE_WIDTH);
  assert.equal(canvas.height, EVENT_SHARE_IMAGE_HEIGHT);
  assert.equal(created.length, 2);
  assert.ok(created.every(bitmap => bitmap.closed));
});

test('compose does not touch browser APIs for non-visual media', async () => {
  let touched = false;
  const result = await composeEventShareImage({
    imageTextMode:'ocr_text',
    documentLike:{ createElement() { touched = true; } },
  });
  assert.equal(result, null);
  assert.equal(touched, false);
});
