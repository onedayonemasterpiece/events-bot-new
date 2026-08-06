#!/usr/bin/env node

import process from 'node:process';

const nativeFetch = globalThis.fetch;
const baseUrl = process.env.PENPOT_BASE_URL || 'https://design.penpot.app';
const origin = new URL(baseUrl).origin;

const browserHeaders = {
  Accept: 'application/json, text/plain, */*',
  'Accept-Language': 'en-US,en;q=0.9',
  Origin: origin,
  Referer: `${origin}/`,
  'Sec-Fetch-Dest': 'empty',
  'Sec-Fetch-Mode': 'cors',
  'Sec-Fetch-Site': 'same-origin',
  'User-Agent':
    process.env.PENPOT_HTTP_USER_AGENT ||
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
};

globalThis.fetch = async (input, init = {}) => {
  const headers = new Headers(init.headers || {});
  for (const [name, value] of Object.entries(browserHeaders)) {
    if (!headers.has(name)) headers.set(name, value);
  }
  return nativeFetch(input, { ...init, headers });
};

await import('./penpot_api_smoke_test.mjs');
