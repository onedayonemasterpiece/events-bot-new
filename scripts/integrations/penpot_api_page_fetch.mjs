#!/usr/bin/env node

import process from 'node:process';
import { chromium } from '../../site/node_modules/playwright/index.mjs';

const baseUrl = (process.env.PENPOT_BASE_URL || 'https://design.penpot.app').replace(/\/$/, '');
const origin = new URL(baseUrl).origin;
const userAgent =
  process.env.PENPOT_HTTP_USER_AGENT ||
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36';

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({
  userAgent,
  locale: 'en-US',
  viewport: { width: 1365, height: 900 },
  colorScheme: 'light',
});
const page = await context.newPage();

async function waitForCloudflare() {
  await page.goto(`${origin}/`, { waitUntil: 'domcontentloaded', timeout: 45_000 });
  const deadline = Date.now() + 35_000;
  while (Date.now() < deadline) {
    const title = await page.title().catch(() => '');
    const bodyText = await page.locator('body').innerText({ timeout: 2_000 }).catch(() => '');
    const challenged =
      /just a moment|security verification|checking your browser|verify you are human/i.test(`${title}\n${bodyText}`);
    if (!challenged) return;
    await page.waitForTimeout(1_000);
  }
  throw new Error('Cloudflare challenge did not clear in the Chromium page context.');
}

await waitForCloudflare();

const nativeLog = console.log.bind(console);
const nativeError = console.error.bind(console);
let resolveCompletion;
const completion = new Promise((resolve) => {
  resolveCompletion = resolve;
});
let completionScheduled = false;

function scheduleCompletion() {
  if (completionScheduled) return;
  completionScheduled = true;
  queueMicrotask(() => resolveCompletion());
}

console.log = (...args) => {
  nativeLog(...args);
  const text = args.map(String).join(' ');
  if (text.includes('PENPOT_SMOKE_PASS') || text.includes('PENPOT_WORKSPACE_URL')) scheduleCompletion();
};
console.error = (...args) => {
  nativeError(...args);
  scheduleCompletion();
};

globalThis.fetch = async (input, init = {}) => {
  const request = input instanceof Request ? input : null;
  const url = request?.url || String(input);
  const method = init.method || request?.method || 'GET';
  const headers = Object.fromEntries(new Headers(init.headers || request?.headers || {}).entries());
  const body = init.body == null ? null : String(init.body);

  const result = await page.evaluate(
    async ({ requestUrl, requestMethod, requestHeaders, requestBody }) => {
      const response = await window.fetch(requestUrl, {
        method: requestMethod,
        headers: requestHeaders,
        body: requestBody,
        credentials: 'include',
        cache: 'no-store',
      });
      return {
        status: response.status,
        statusText: response.statusText,
        headers: Array.from(response.headers.entries()),
        body: await response.text(),
        url: response.url,
      };
    },
    {
      requestUrl: url,
      requestMethod: method,
      requestHeaders: headers,
      requestBody: body,
    },
  );

  return new Response(result.body, {
    status: result.status,
    statusText: result.statusText,
    headers: result.headers,
  });
};

try {
  await import('./penpot_api_smoke_test.mjs');
  await Promise.race([
    completion,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error('Penpot smoke adapter did not reach a terminal state within 120 seconds.')), 120_000),
    ),
  ]);
} finally {
  console.log = nativeLog;
  console.error = nativeError;
  await browser.close();
}

if (process.exitCode && process.exitCode !== 0) process.exit(process.exitCode);
