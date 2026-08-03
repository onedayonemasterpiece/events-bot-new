import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const component = readFileSync(path.join(siteRoot, 'src/components/personalization/PersonalizationRuntime.astro'), 'utf8');
const runtimeScript = component.match(/<script is:inline>([\s\S]*?)<\/script>/u)?.[1];
assert.ok(runtimeScript, 'extract the actual PersonalizationRuntime inline mount');
const executablePath = [
  process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE,
  '/home/dev/.cache/ms-playwright/chromium-1228/chrome-linux64/chrome',
  '/opt/ms-playwright/chromium-1223/chrome-linux64/chrome',
].find((candidate) => candidate && existsSync(candidate));
const browser = await chromium.launch({ headless:true, executablePath });
const context = await browser.newContext();
const page = await context.newPage();
const requests = [];
page.on('request', (request) => requests.push(request.url()));
await page.addInitScript(() => {
  localStorage.setItem('existing-profile', JSON.stringify({ liked:['11'], revision:3 }));
  window.__p13nHandlerAdds = 0;
  const originalAdd = EventTarget.prototype.addEventListener;
  EventTarget.prototype.addEventListener = function (...args) {
    window.__p13nHandlerAdds += 1;
    return originalAdd.apply(this, args);
  };
});
await page.route('https://p13n-off.test/', async (route) => {
  await route.fulfill({
    contentType:'text/html',
    body:`<!doctype html><html><body>
      <main id="visible-surface"><article id="card-a">Alpha</article><article id="card-b">Beta</article></main>
      <script>
        window.__p13nBefore = {
          storage:Object.fromEntries(Object.entries(localStorage).sort(([a],[b]) => a.localeCompare(b))),
          order:Array.from(document.querySelectorAll('#visible-surface>article'), (node) => node.id),
          text:document.querySelector('#visible-surface').innerText,
          handlers:window.__p13nHandlerAdds,
        };
      </script>
      <span hidden aria-hidden="true" data-p13n-runtime-marker="p13n-runtime-v1"
        data-p13n-mode="off" data-p13n-mode-diagnostic="p13n_mode.default"
        data-p13n-page-family="home" data-p13n-surface="static_only"
        data-p13n-policy="unknown-static" data-p13n-policy-version="collection-surfaces-v1"
        data-p13n-static-only-reason="wave0-home-static-baseline"></span>
      <script>${runtimeScript}</script>
    </body></html>`,
  });
});

try {
  await page.goto('https://p13n-off.test/', { waitUntil:'load' });
  await page.waitForTimeout(350);
  const after = await page.evaluate(() => ({
    storage:Object.fromEntries(Object.entries(localStorage).sort(([a],[b]) => a.localeCompare(b))),
    order:Array.from(document.querySelectorAll('#visible-surface>article'), (node) => node.id),
    text:document.querySelector('#visible-surface').innerText,
    handlers:window.__p13nHandlerAdds,
    before:window.__p13nBefore,
    markerCount:document.querySelectorAll('[data-p13n-runtime-marker="p13n-runtime-v1"]').length,
    markerMode:document.querySelector('[data-p13n-runtime-marker]')?.dataset.p13nMode,
    testApiPresent:Object.prototype.hasOwnProperty.call(window, '__KENIGEVENTS_P13N_TEST_V1__'),
  }));
  assert.equal(requests.length, 1, 'the document is the only request; the off runtime mounts with zero network');
  assert.equal(requests[0], 'https://p13n-off.test/');
  assert.deepEqual(after.storage, after.before.storage, 'off mount does not add/change localStorage keys, values or bytes');
  assert.deepEqual(after.order, after.before.order, 'off mount does not reorder visible cards');
  assert.equal(after.text, after.before.text, 'off mount does not change visible copy');
  assert.equal(after.handlers, after.before.handlers, 'off mount registers no duplicate event handlers');
  assert.equal(after.markerCount, 1);
  assert.equal(after.markerMode, 'off');
  assert.equal(after.testApiPresent, false, 'production/off mode omits the test API');
  process.stdout.write('personalization production/off browser characterization: PASS (network=0, storage_delta=0, reorder=0, handler_adds=0)\n');
} finally {
  await browser.close();
}
