#!/usr/bin/env node
'use strict';

/**
 * Controlled desktop acceptance smoke for the authorized Search page.
 *
 * Default mode is fully deterministic: Supabase PKCE/Auth and event-search are
 * intercepted in this Playwright browser context. The production application
 * is not patched and still has to perform its normal PKCE exchange, establish
 * a session, attach a bearer token, and render the Edge Function response.
 *
 * `--real-edge` is deliberately separate and opt-in. It creates a short-lived
 * Supabase Auth test user through the admin API, returns that legitimate
 * session only from the browser's mocked PKCE callback, and lets event-search
 * reach the real Edge Function. The user is removed in a finally block.
 *
 * Secrets are accepted only from environment variables and are never printed.
 */

const crypto = require('node:crypto');
const fs = require('node:fs');
const http = require('node:http');
const path = require('node:path');

const DEFAULT_SUPABASE_URL = 'https://example.supabase.co';
const DEFAULT_QUERY = 'урбанистика в четверг вечером по регистрации';
const DEFAULT_REAL_QUERIES = [
  'На природу с детьми',
  'искусство у моря',
  'в пятницу бесплатно',
];
const VIEWPORT = Object.freeze({ width: 1440, height: 1000 });

function usage() {
  return `Usage:
  NODE_PATH="$(npm root -g)" node scripts/smoke_authorized_search_desktop.cjs [options]

Options:
  --dist PATH              Built site dist or dist/preview-* directory.
  --supabase-url URL       Public Supabase project URL embedded in the build.
  --query TEXT             Deterministic mocked-flow query.
  --real-edge              Use real Supabase Auth token + real event-search.
  --real-query TEXT        Real Edge query (repeatable; defaults to incident set).
  --timeout-ms NUMBER      Per real Edge query timeout (default: 70000).
  --help                   Show this help.

Real mode environment:
  PERSONALIZATION_SUPABASE_URL
  PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY
  PERSONALIZATION_SUPABASE_SECRET_KEY

No Yandex browser session is read or required.`;
}

function parseArgs(argv) {
  const args = {
    dist: '',
    supabaseUrl: '',
    query: DEFAULT_QUERY,
    realEdge: false,
    realQueries: [],
    timeoutMs: 70_000,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    const next = () => {
      index += 1;
      if (index >= argv.length) throw new Error(`Missing value for ${token}`);
      return argv[index];
    };
    if (token === '--dist') args.dist = next();
    else if (token === '--supabase-url') args.supabaseUrl = next();
    else if (token === '--query') args.query = next();
    else if (token === '--real-edge') args.realEdge = true;
    else if (token === '--real-query') args.realQueries.push(next());
    else if (token === '--timeout-ms') args.timeoutMs = Number(next());
    else if (token === '--help' || token === '-h') args.help = true;
    else throw new Error(`Unknown option: ${token}`);
  }
  if (!Number.isFinite(args.timeoutMs) || args.timeoutMs < 1_000) {
    throw new Error('--timeout-ms must be at least 1000');
  }
  if (!args.realQueries.length) args.realQueries = [...DEFAULT_REAL_QUERIES];
  return args;
}

function loadPlaywright() {
  const usable = (candidate) => {
    const playwright = require(candidate);
    return fs.existsSync(playwright.chromium.executablePath()) ? playwright : null;
  };
  try {
    const playwright = usable('playwright');
    if (playwright) return playwright;
  } catch (firstError) {
    if (firstError.code !== 'MODULE_NOT_FOUND') {
      throw firstError;
    }
  }
  const candidates = [
    process.env.PLAYWRIGHT_MODULE || '',
    path.resolve(__dirname, '../site/node_modules/playwright'),
    '/home/dev/projects/events-bot-new/site/node_modules/playwright',
    '/home/dev/.codex/venvs/events-bot-new/lib/python3.11/site-packages/playwright/driver/package',
  ].filter(Boolean);
  for (const candidate of candidates) {
    try {
      const playwright = usable(candidate);
      if (playwright) return playwright;
    } catch (_) {
      // Try the next known shared install without changing project deps.
    }
  }
  throw new Error(
    'Playwright with an installed Chromium is unavailable. '
    + 'Set PLAYWRIGHT_MODULE to an existing shared Playwright package.',
  );
}

function latestPreviewDist(distRoot) {
  if (!fs.existsSync(distRoot)) {
    throw new Error(`No build directory found: ${distRoot}`);
  }
  const candidates = fs.readdirSync(distRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && entry.name.startsWith('preview-'))
    .map((entry) => path.join(distRoot, entry.name))
    .sort((left, right) => fs.statSync(right).mtimeMs - fs.statSync(left).mtimeMs);
  if (!candidates.length) throw new Error(`No preview-* build found under ${distRoot}`);
  return candidates[0];
}

function resolveDist(value) {
  const candidate = value
    ? path.resolve(value)
    : latestPreviewDist(path.resolve(__dirname, '../site/dist'));
  if (!fs.statSync(candidate).isDirectory()) throw new Error(`Dist is not a directory: ${candidate}`);
  const directSearch = path.join(candidate, 'poisk', 'index.html');
  if (!fs.existsSync(directSearch)) {
    throw new Error(`Search page is absent from build: ${directSearch}`);
  }
  return candidate;
}

function contentType(filePath) {
  const extension = path.extname(filePath).toLowerCase();
  return ({
    '.css': 'text/css; charset=utf-8',
    '.gif': 'image/gif',
    '.html': 'text/html; charset=utf-8',
    '.ico': 'image/x-icon',
    '.jpeg': 'image/jpeg',
    '.jpg': 'image/jpeg',
    '.js': 'text/javascript; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.mjs': 'text/javascript; charset=utf-8',
    '.png': 'image/png',
    '.svg': 'image/svg+xml',
    '.webp': 'image/webp',
  })[extension] || 'application/octet-stream';
}

async function serveDist(dist) {
  const isPreview = path.basename(dist).startsWith('preview-');
  const root = isPreview ? path.dirname(dist) : dist;
  const server = http.createServer((request, response) => {
    try {
      const urlPath = decodeURIComponent(new URL(request.url, 'http://127.0.0.1').pathname);
      const relative = urlPath.replace(/^\/+/u, '');
      let filePath = path.resolve(root, relative);
      if (!filePath.startsWith(`${path.resolve(root)}${path.sep}`) && filePath !== path.resolve(root)) {
        response.writeHead(403).end('forbidden');
        return;
      }
      if (fs.existsSync(filePath) && fs.statSync(filePath).isDirectory()) {
        filePath = path.join(filePath, 'index.html');
      }
      if (!fs.existsSync(filePath) || !fs.statSync(filePath).isFile()) {
        response.writeHead(404).end('not found');
        return;
      }
      response.writeHead(200, {
        'content-type': contentType(filePath),
        'cache-control': 'no-store',
      });
      fs.createReadStream(filePath).pipe(response);
    } catch (_) {
      response.writeHead(500).end('server error');
    }
  });
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  const previewPath = isPreview ? `/${path.basename(dist)}/poisk/` : '/poisk/';
  return {
    baseUrl: `http://127.0.0.1:${address.port}`,
    close: () => new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve())),
    previewPath,
  };
}

function encodeJwtPart(value) {
  return Buffer.from(JSON.stringify(value)).toString('base64url');
}

function fakeUser() {
  return {
    id: '11111111-2222-4333-8444-555555555555',
    aud: 'authenticated',
    role: 'authenticated',
    email: 'desktop-search-smoke@example.invalid',
    app_metadata: {
      provider: 'custom:yandex',
      providers: ['custom:yandex'],
    },
    user_metadata: {
      purpose: 'authorized_search_desktop_smoke',
      preferred_username: 'desktop-search-smoke',
    },
    created_at: '2026-07-23T00:00:00.000Z',
  };
}

function fakeSession() {
  const user = fakeUser();
  const now = Math.floor(Date.now() / 1000);
  const accessToken = [
    encodeJwtPart({ alg: 'none', typ: 'JWT' }),
    encodeJwtPart({
      aud: 'authenticated',
      exp: now + 3600,
      iat: now,
      sub: user.id,
      email: user.email,
      role: 'authenticated',
    }),
    'desktop-smoke-signature',
  ].join('.');
  return {
    access_token: accessToken,
    refresh_token: 'desktop-search-smoke-refresh-token',
    expires_in: 3600,
    expires_at: now + 3600,
    token_type: 'bearer',
    user,
  };
}

function fakeSearchResponse() {
  const item = {
    event_id: 6310,
    id: 6310,
    title: 'Архитектурно-урбанистическая студия',
    category: 'лекция',
    tags: ['урбанистика', 'город', 'регистрация'],
    base_similarity: 0.9255,
    semantic_score: 0.9255,
    display: {
      id: 6310,
      event_id: 6310,
      title: 'Архитектурно-урбанистическая студия',
      href: '/sobytiya/arhitekturno-urbanisticheskaya-studiya-zanyatie-3-formiruem-kontseptsii-i-kaliningrad-6310/',
      absolute_url: 'https://kenigevents.ru/sobytiya/arhitekturno-urbanisticheskaya-studiya-zanyatie-3-formiruem-kontseptsii-i-kaliningrad-6310/',
      event_type: 'лекция',
      image_url: 'https://static.kenigevents.ru/p/dh16/21/2111009450924c4948058765c7664636c636ccb3489bce9b46331e634e630c77.webp',
      image_alt: 'Фотография события',
      image_text_mode: 'visual_only',
      image_media_role: 'unknown_document',
      image_width: 800,
      image_height: 534,
      focal_y: 0.5,
      display_date: '23 июля',
      display_time: '18:30',
      display_date_time: '23 июля · 18:30',
      city: 'Калининград',
      venue_name: 'Музей',
      place: 'Калининград · Музей',
      status_label: 'Бесплатно · регистрация',
      likes_count: 7,
      shares_count: 2,
      calendar_href: '/sobytiya/arhitekturno-urbanisticheskaya-studiya-zanyatie-3-formiruem-kontseptsii-i-kaliningrad-6310/event.ics',
      calendar_eligible: true,
    },
  };
  return {
    schema_version: 'event-search-results-v1',
    surface: 'authorized_event_search',
    algorithm_id: 'pgvector_gemini_embedding_2_llm_verify_v1',
    request_id: '00000000-0000-4000-8000-000000000005',
    served_list_id: '00000000-0000-4000-8000-000000000006',
    served_list_hash: 'desktop-search-smoke-served-list-hash',
    query_hash: 'desktop-search-smoke-query-hash',
    query_facets: {
      weekday_iso: 4,
      weekday_ru: 'четверг',
      time_of_day: 'evening',
      admission: 'registration_required',
    },
    quota: {
      day_remaining: 4,
      month_remaining: 29,
      llm_day_remaining: 1,
      llm_month_remaining: 9,
    },
    items: [item],
    fallback_items: [],
    has_more: false,
    next_offset: 1,
    retrieved_count: 1,
    llm_verifier: { requested: true, used: true, status: 'ok' },
    timings_ms: { total_ms: 42 },
  };
}

function ndjsonResponse(payload) {
  const events = [
    {
      type: 'progress',
      request_id: payload.request_id,
      stage: 'accepted',
      progress: 2,
      label: 'Запрос принят',
    },
    {
      type: 'progress',
      request_id: payload.request_id,
      stage: 'vector_search',
      progress: 55,
      label: 'Ищу события',
    },
    {
      type: 'progress',
      request_id: payload.request_id,
      stage: 'finalize',
      progress: 96,
      label: 'Собираю результат',
    },
    {
      type: 'result',
      request_id: payload.request_id,
      progress: 100,
      label: 'Готово',
      data: payload,
    },
  ];
  return `${events.map((event) => JSON.stringify(event)).join('\n')}\n`;
}

function pageErrors(page) {
  const errors = [];
  // Keep diagnostics intentionally content-free: browser exception text can
  // include request details, while this smoke must never echo auth material.
  page.on('pageerror', () => errors.push('pageerror'));
  page.on('console', (message) => {
    if (message.type() === 'error') errors.push('console:error');
  });
  return errors;
}

async function routeStaticCdn(page, dist) {
  if (!path.basename(dist).startsWith('preview-')) return;
  const buildId = path.basename(dist);
  await page.route(`https://static.kenigevents.ru/${buildId}/**`, async (route) => {
    const marker = `/${buildId}/`;
    const requestPath = new URL(route.request().url()).pathname;
    const relative = decodeURIComponent(requestPath.split(marker, 2)[1] || '');
    const filePath = path.resolve(dist, relative);
    if (
      (!filePath.startsWith(`${path.resolve(dist)}${path.sep}`) && filePath !== path.resolve(dist))
      || !fs.existsSync(filePath)
      || !fs.statSync(filePath).isFile()
    ) {
      await route.fulfill({ status: 404, body: 'missing local preview asset' });
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: contentType(filePath),
      body: fs.readFileSync(filePath),
    });
  });
}

async function assertSignedOutTyping(page, query) {
  const root = page.locator('[data-authorized-search]').first();
  const input = page.locator('[data-search-input]').first();
  await root.waitFor({ state: 'visible' });
  await page.locator('[data-search-login]').first().waitFor({ state: 'visible' });
  await input.waitFor({ state: 'visible' });
  if (!(await input.isEditable())) throw new Error('Desktop Search input is not editable before auth');
  if (await root.evaluate((node) => node.classList.contains('is-authorized'))) {
    throw new Error('Fresh browser context unexpectedly started authorized');
  }
  const inputBox = await input.boundingBox();
  if (!inputBox || inputBox.width < 320) {
    throw new Error(`Desktop Search input is not a usable typing target: ${JSON.stringify(inputBox)}`);
  }
  await input.click();
  await page.keyboard.type(query, { delay: 2 });
  if ((await input.inputValue()).replace(/\s+/gu, ' ').trim() !== query) {
    throw new Error('Desktop keyboard typing did not preserve the requested query');
  }
}

async function submitThroughPkce(page, supabaseUrl, localReturnUrl) {
  await page.locator('[data-search-submit]').first().click();
  await page.waitForURL((url) => (
    url.origin === new URL(supabaseUrl).origin
    && url.pathname.endsWith('/auth/v1/authorize')
  ), { timeout: 5_000 });
  await page.goto(`${localReturnUrl}?code=desktop-search-smoke-code`, { waitUntil: 'networkidle' });
  await page.waitForFunction(
    () => document.querySelector('[data-authorized-search]')?.classList.contains('is-authorized'),
    null,
    { timeout: 10_000 },
  );
}

async function assertAuthorizedResult(page, expectedQuery, requestCalls, timeoutMs) {
  const results = page.locator('[data-search-results]').first();
  await results.waitFor({ state: 'visible', timeout: timeoutMs });
  const firstCard = results.locator('[data-event-card]').first();
  await firstCard.waitFor({ state: 'visible', timeout: 5_000 });
  await page.waitForFunction(
    () => document.querySelector('[data-search-submit]')?.getAttribute('aria-busy') === 'false',
    null,
    { timeout: 5_000 },
  );
  if (await page.locator('[data-search-login]').first().isVisible()) {
    throw new Error('Login control remained visible after authorized callback');
  }
  if (!(await page.locator('[data-search-user]').first().isVisible())) {
    throw new Error('Authorized account control is not visible');
  }
  if (!requestCalls.length) throw new Error('event-search was not called after the PKCE callback');
  const request = requestCalls.at(-1);
  if (request.body?.query !== expectedQuery) {
    throw new Error(`Authorized request lost the typed query: ${JSON.stringify(request.body)}`);
  }
  if (request.body?.use_llm_verifier !== true) {
    throw new Error('Authorized Search did not request the bounded LLM verifier');
  }
  if (!request.authorized) {
    throw new Error('Authorized Search request omitted the bearer token');
  }
  return {
    cardCount: await results.locator('[data-event-card]').count(),
    firstEventId: await firstCard.getAttribute('data-event-id'),
  };
}

async function runMocked(args, playwright, dist, server) {
  const supabaseUrl = (args.supabaseUrl || DEFAULT_SUPABASE_URL).replace(/\/+$/u, '');
  const session = fakeSession();
  const requests = [];
  const browser = await playwright.chromium.launch({ headless: true });
  try {
    const page = await browser.newPage({ viewport: VIEWPORT });
    const errors = pageErrors(page);
    await routeStaticCdn(page, dist);
    await page.route(`${supabaseUrl}/**`, async (route) => {
      const request = route.request();
      const url = request.url();
      if (url.includes('/auth/v1/authorize')) {
        await route.fulfill({
          status: 200,
          contentType: 'text/html; charset=utf-8',
          body: '<!doctype html><title>Mock OAuth boundary</title>',
        });
        return;
      }
      if (url.includes('/auth/v1/token?grant_type=pkce')) {
        await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(session) });
        return;
      }
      if (url.endsWith('/auth/v1/user')) {
        await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(session.user) });
        return;
      }
      if (
        url.endsWith('/rest/v1/rpc/get_event_search_quota_v1')
        || url.endsWith('/rest/v1/rpc/get_event_search_quota_v2')
      ) {
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify([{ day_remaining: 5, month_remaining: 30 }]),
        });
        return;
      }
      if (url.endsWith('/functions/v1/event-search')) {
        let body = {};
        try {
          body = JSON.parse(request.postData() || '{}');
        } catch (_) {
          throw new Error('event-search request body was not JSON');
        }
        requests.push({
          body,
          authorized: /^Bearer\s+\S+$/u.test(request.headers().authorization || ''),
        });
        await new Promise((resolve) => setTimeout(resolve, 250));
        const payload = fakeSearchResponse();
        if ((request.headers().accept || '').includes('application/x-ndjson')) {
          await route.fulfill({
            status: 200,
            contentType: 'application/x-ndjson; charset=utf-8',
            body: ndjsonResponse(payload),
          });
        } else {
          await route.fulfill({
            status: 200,
            contentType: 'application/json; charset=utf-8',
            body: JSON.stringify(payload),
          });
        }
        return;
      }
      await route.fulfill({ status: 404, body: 'unexpected mocked Supabase request' });
    });

    const searchUrl = `${server.baseUrl}${server.previewPath}`;
    await page.goto(searchUrl, { waitUntil: 'networkidle' });
    await assertSignedOutTyping(page, args.query);
    if (requests.length) throw new Error('Signed-out typing called event-search before authorization');
    await submitThroughPkce(page, supabaseUrl, searchUrl);
    const result = await assertAuthorizedResult(page, args.query, requests, 10_000);
    if (result.firstEventId !== '6310') {
      throw new Error(`Unexpected mocked result event: ${result.firstEventId}`);
    }
    const draft = await page.evaluate(() => localStorage.getItem('ke_authorized_search_draft_v1'));
    if (draft !== null) throw new Error('Pending typed query was not consumed after auth');
    if (await page.locator('[data-search-skeletons]').first().isVisible()) {
      throw new Error('Search skeleton remained visible after cards rendered');
    }
    if (errors.length) throw new Error(`Browser errors: ${errors.join(' | ')}`);
    return {
      mode: 'mocked-browser',
      viewport: `${VIEWPORT.width}x${VIEWPORT.height}`,
      typedQueryPreserved: true,
      authBoundary: 'mocked-pkce-session-browser-context-only',
      requestCalls: requests.length,
      cards: result.cardCount,
      firstEventId: result.firstEventId,
    };
  } finally {
    await browser.close();
  }
}

async function fetchJson(url, options, label) {
  const response = await fetch(url, options);
  if (!response.ok) throw new Error(`${label} failed with HTTP ${response.status}`);
  return response.json();
}

async function createTemporaryRealSession(args) {
  const supabaseUrl = (
    args.supabaseUrl
    || process.env.PERSONALIZATION_SUPABASE_URL
    || ''
  ).replace(/\/+$/u, '');
  const publishableKey = process.env.PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY || '';
  const secretKey = process.env.PERSONALIZATION_SUPABASE_SECRET_KEY || '';
  if (!supabaseUrl || !publishableKey || !secretKey) {
    throw new Error(
      '--real-edge requires PERSONALIZATION_SUPABASE_URL, '
      + 'PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY, and '
      + 'PERSONALIZATION_SUPABASE_SECRET_KEY',
    );
  }
  const suffix = `${Date.now()}-${crypto.randomBytes(5).toString('hex')}`;
  const email = `authorized-search-desktop-smoke-${suffix}@example.invalid`;
  const password = `SearchSmoke!${crypto.randomBytes(24).toString('base64url')}`;
  const adminHeaders = {
    apikey: secretKey,
    authorization: `Bearer ${secretKey}`,
    'content-type': 'application/json',
  };
  const user = await fetchJson(
    `${supabaseUrl}/auth/v1/admin/users`,
    {
      method: 'POST',
      headers: adminHeaders,
      body: JSON.stringify({
        email,
        password,
        email_confirm: true,
        app_metadata: { purpose: 'authorized_search_desktop_smoke' },
        user_metadata: { preferred_username: 'desktop-search-smoke' },
      }),
    },
    'temporary smoke-user creation',
  );
  try {
    const session = await fetchJson(
      `${supabaseUrl}/auth/v1/token?grant_type=password`,
      {
        method: 'POST',
        headers: { apikey: publishableKey, 'content-type': 'application/json' },
        body: JSON.stringify({ email, password }),
      },
      'temporary smoke-user sign-in',
    );
    return {
      session,
      supabaseUrl,
      cleanup: async () => {
        const response = await fetch(`${supabaseUrl}/auth/v1/admin/users/${encodeURIComponent(user.id)}`, {
          method: 'DELETE',
          headers: adminHeaders,
        });
        if (!response.ok && response.status !== 404) {
          throw new Error(`temporary smoke-user cleanup failed with HTTP ${response.status}`);
        }
      },
    };
  } catch (error) {
    await fetch(`${supabaseUrl}/auth/v1/admin/users/${encodeURIComponent(user.id)}`, {
      method: 'DELETE',
      headers: adminHeaders,
    }).catch(() => {});
    throw error;
  }
}

async function runRealEdge(args, playwright, dist, server) {
  const real = await createTemporaryRealSession(args);
  const requests = [];
  const timings = [];
  const browser = await playwright.chromium.launch({ headless: true });
  let cleanupError = null;
  try {
    const page = await browser.newPage({ viewport: VIEWPORT });
    const errors = pageErrors(page);
    await routeStaticCdn(page, dist);
    await page.route(`${real.supabaseUrl}/**`, async (route) => {
      const request = route.request();
      const url = request.url();
      if (url.includes('/auth/v1/authorize')) {
        await route.fulfill({
          status: 200,
          contentType: 'text/html; charset=utf-8',
          body: '<!doctype html><title>Controlled OAuth boundary</title>',
        });
        return;
      }
      if (url.includes('/auth/v1/token?grant_type=pkce')) {
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify(real.session),
        });
        return;
      }
      if (url.endsWith('/auth/v1/user')) {
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify(real.session.user),
        });
        return;
      }
      if (url.endsWith('/functions/v1/event-search')) {
        const body = JSON.parse(request.postData() || '{}');
        requests.push({
          body,
          authorized: /^Bearer\s+\S+$/u.test(request.headers().authorization || ''),
          startedAt: Date.now(),
        });
      }
      await route.continue();
    });

    const searchUrl = `${server.baseUrl}${server.previewPath}`;
    await page.goto(searchUrl, { waitUntil: 'networkidle' });
    const firstQuery = args.realQueries[0];
    await assertSignedOutTyping(page, firstQuery);
    await submitThroughPkce(page, real.supabaseUrl, searchUrl);
    await assertAuthorizedResult(page, firstQuery, requests, args.timeoutMs);
    timings.push({
      query: firstQuery,
      browserMs: Date.now() - requests.at(-1).startedAt,
      cards: await page.locator('[data-search-results] [data-event-card]').count(),
    });

    for (const query of args.realQueries.slice(1)) {
      const input = page.locator('[data-search-input]').first();
      await input.fill(query);
      const requestCount = requests.length;
      const startedAt = Date.now();
      await page.locator('[data-search-submit]').first().click();
      await page.waitForFunction(
        (count) => {
          const button = document.querySelector('[data-search-submit]');
          return count >= 0 && button?.getAttribute('aria-busy') === 'true';
        },
        requestCount,
        { timeout: 5_000 },
      );
      await page.waitForFunction(
        () => document.querySelector('[data-search-submit]')?.getAttribute('aria-busy') === 'false',
        null,
        { timeout: args.timeoutMs },
      );
      if (requests.length !== requestCount + 1 || requests.at(-1).body.query !== query) {
        throw new Error(`Real Edge request did not preserve query: ${query}`);
      }
      const cards = await page.locator('[data-search-results] [data-event-card]').count();
      if (cards < 1) throw new Error(`Real Edge query returned no rendered cards: ${query}`);
      timings.push({ query, browserMs: Date.now() - startedAt, cards });
    }
    if (errors.length) throw new Error(`Browser errors: ${errors.join(' | ')}`);
    return {
      mode: 'real-edge',
      viewport: `${VIEWPORT.width}x${VIEWPORT.height}`,
      authBoundary: 'mocked-pkce-callback-with-legitimate-supabase-test-session',
      realEdgeValidatedToken: true,
      queries: timings,
    };
  } finally {
    await browser.close();
    try {
      await real.cleanup();
    } catch (error) {
      cleanupError = error;
    }
    if (cleanupError) throw cleanupError;
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    process.stdout.write(`${usage()}\n`);
    return;
  }
  const playwright = loadPlaywright();
  const dist = resolveDist(args.dist);
  const server = await serveDist(dist);
  try {
    const result = args.realEdge
      ? await runRealEdge(args, playwright, dist, server)
      : await runMocked(args, playwright, dist, server);
    process.stdout.write(`${JSON.stringify({
      status: 'ok',
      target: path.basename(dist),
      ...result,
    })}\n`);
  } finally {
    await server.close();
  }
}

main().catch((error) => {
  process.stderr.write(`authorized_search_desktop_smoke=failed ${error.message}\n`);
  process.exitCode = 1;
});
