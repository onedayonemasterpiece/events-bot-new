#!/usr/bin/env node

import process from 'node:process';
import { chromium } from '../../site/node_modules/playwright/index.mjs';

const baseUrl = (process.env.PENPOT_BASE_URL || 'https://design.penpot.app').replace(/\/$/, '');
const origin = new URL(baseUrl).origin;
const userAgent =
  process.env.PENPOT_HTTP_USER_AGENT ||
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36';

async function obtainBrowserCookies() {
  const browser = await chromium.launch({ headless: true });
  try {
    const context = await browser.newContext({
      userAgent,
      locale: 'en-US',
      viewport: { width: 1365, height: 900 },
      colorScheme: 'light',
    });
    const page = await context.newPage();
    await page.goto(`${origin}/`, { waitUntil: 'domcontentloaded', timeout: 45_000 });

    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
      const title = await page.title().catch(() => '');
      const bodyText = await page.locator('body').innerText({ timeout: 2_000 }).catch(() => '');
      const challenged =
        /just a moment|security verification|checking your browser|verify you are human/i.test(`${title}\n${bodyText}`);
      if (!challenged) break;
      await page.waitForTimeout(1_000);
    }

    const title = await page.title().catch(() => '');
    const bodyText = await page.locator('body').innerText({ timeout: 2_000 }).catch(() => '');
    if (/just a moment|security verification|checking your browser|verify you are human/i.test(`${title}\n${bodyText}`)) {
      throw new Error('Cloudflare challenge did not clear in a real Chromium context on the GitHub-hosted runner.');
    }

    const cookies = await context.cookies(origin);
    return cookies.map(({ name, value }) => `${name}=${value}`).join('; ');
  } finally {
    await browser.close();
  }
}

const cookieHeader = await obtainBrowserCookies();
const nativeFetch = globalThis.fetch;
const browserHeaders = {
  Accept: 'application/json, text/plain, */*',
  'Accept-Language': 'en-US,en;q=0.9',
  Origin: origin,
  Referer: `${origin}/`,
  'Sec-Fetch-Dest': 'empty',
  'Sec-Fetch-Mode': 'cors',
  'Sec-Fetch-Site': 'same-origin',
  'User-Agent': userAgent,
};

if (cookieHeader) browserHeaders.Cookie = cookieHeader;

globalThis.fetch = async (input, init = {}) => {
  const headers = new Headers(init.headers || {});
  for (const [name, value] of Object.entries(browserHeaders)) {
    if (!headers.has(name)) headers.set(name, value);
  }
  return nativeFetch(input, { ...init, headers });
};

await import('./penpot_api_smoke_test.mjs');
