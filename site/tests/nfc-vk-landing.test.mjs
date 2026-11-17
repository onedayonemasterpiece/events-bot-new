import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { runInNewContext } from 'node:vm';

const html = readFileSync(new URL('../public/vk.html', import.meta.url), 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)?.[1];
const target = 'https://vk.ru/klgdevents';
const app = 'vk://vk.com/klgdevents';

function openOn(userAgent) {
  assert.ok(script, 'inline transition script must exist');
  const links = { 'open-app': { href: '' }, 'message': { textContent: '' } };
  const calls = [];
  const events = {};
  const timers = [];
  const document = {
    visibilityState: 'visible',
    getElementById: (id) => links[id],
    addEventListener: (type, handler) => { events[type] = handler; },
  };
  const window = {
    location: {
      assign: (url) => calls.push(['assign', url]),
      replace: (url) => calls.push(['replace', url]),
    },
    setTimeout: (handler, delay) => timers.push({ handler, delay }),
    addEventListener: (type, handler) => { events[type] = handler; },
  };
  runInNewContext(script, { document, window, navigator: { userAgent }, encodeURIComponent });
  return { links, calls, timers, events, document };
}

test('fixed, noindex NFC page always exposes both community destinations', () => {
  assert.match(html, /name="robots" content="noindex,nofollow,noarchive"/);
  assert.match(html, /id="open-app" href="vk:\/\/vk\.com\/klgdevents"/);
  assert.match(html, /id="open-web" href="https:\/\/vk\.ru\/klgdevents"/);
  assert.doesNotMatch(html, /location\.replace\(['"]https:\/\/vk\.ru\/['"]\)/);
});

test('Android: app path is preserved in explicit VK Intent; timeout falls back to community web page', () => {
  const r = openOn('Mozilla/5.0 (Linux; Android 15; Pixel) AppleWebKit/537.36 Chrome/127');
  assert.equal(r.links['open-app'].href,
    'intent://vk.com/klgdevents#Intent;scheme=vk;package=com.vkontakte.android;S.browser_fallback_url=https%3A%2F%2Fvk.ru%2Fklgdevents;end');
  assert.deepEqual(r.calls, [['assign', app]]);
  assert.equal(r.timers[0].delay, 2700);
  r.timers[0].handler();
  assert.deepEqual(r.calls[1], ['replace', target]);
});

test('iOS: app-scheme navigation with same community path and web fallback', () => {
  const r = openOn('Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X)');
  assert.equal(r.links['open-app'].href, app);
  assert.deepEqual(r.calls, [['assign', app]]);
  r.timers[0].handler();
  assert.deepEqual(r.calls[1], ['replace', target]);
});

test('successful app switch suppresses fallback, preventing an unrelated browser opening', () => {
  const r = openOn('Android');
  r.document.visibilityState = 'hidden';
  r.events.visibilitychange();
  r.document.visibilityState = 'visible';
  r.timers[0].handler();
  assert.deepEqual(r.calls, [['assign', app]]);
});

test('desktop does not attempt to open VK app or start a web redirect timer', () => {
  const r = openOn('Mozilla/5.0 (Windows NT 10.0; Win64; x64)');
  assert.equal(r.timers.length, 0);
  assert.deepEqual(r.calls, []);
});
