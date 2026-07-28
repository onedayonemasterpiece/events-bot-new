import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { existsSync, readFileSync } from 'node:fs';
import { extname, join, normalize, resolve, sep } from 'node:path';
import { test } from 'node:test';

import { chromium } from 'playwright';

const buildDir = process.env.SEARCH_RECOVERY_BUILD_DIR
  ? resolve(process.env.SEARCH_RECOVERY_BUILD_DIR)
  : resolve('dist');
const hasBuild = existsSync(join(buildDir, 'poisk', 'index.html'));
const builtSearchHtml = hasBuild ? readFileSync(join(buildDir, 'poisk', 'index.html'), 'utf8') : '';
const builtSupabaseUrl = builtSearchHtml.match(/\bdata-supabase-url="([^"]+)"/u)?.[1] || 'https://example.supabase.co';
const builtBasePath = builtSearchHtml.match(/\bsrc="(\/[^"]*?)\/_astro\//u)?.[1] || '';
const builtProjectRef = new URL(builtSupabaseUrl).hostname.split('.', 1)[0] || 'example';
const builtCodeVerifierKey = `sb-${builtProjectRef}-auth-token-code-verifier`;
const types = new Map([
  ['.css', 'text/css; charset=utf-8'],
  ['.html', 'text/html; charset=utf-8'],
  ['.js', 'text/javascript; charset=utf-8'],
  ['.json', 'application/json; charset=utf-8'],
  ['.svg', 'image/svg+xml'],
  ['.webp', 'image/webp'],
]);

function localFile(root, requestPath) {
  let pathname = decodeURIComponent(requestPath.split('?', 1)[0]);
  if (builtBasePath && pathname.startsWith(`${builtBasePath}/`)) {
    pathname = pathname.slice(builtBasePath.length);
  }
  const relative = pathname.replace(/^\/+/u, '');
  const candidate = normalize(join(root, relative));
  if (candidate !== root && !candidate.startsWith(`${root}${sep}`)) return null;
  return candidate.endsWith(sep) || !extname(candidate) ? join(candidate, 'index.html') : candidate;
}

async function withStaticServer(root, callback) {
  const server = createServer((request, response) => {
    const path = localFile(root, request.url || '/');
    if (!path || !existsSync(path)) {
      response.writeHead(404).end('not found');
      return;
    }
    response.writeHead(200, { 'content-type': types.get(extname(path)) || 'application/octet-stream' });
    response.end(readFileSync(path));
  });
  await new Promise((resolveListen) => server.listen(0, '127.0.0.1', resolveListen));
  try {
    await callback(`http://127.0.0.1:${server.address().port}`);
  } finally {
    await new Promise((resolveClose, reject) => server.close((error) => (error ? reject(error) : resolveClose())));
  }
}

test('authenticated Search recovers from missing headers and stalled streams', { skip: !hasBuild }, async () => {
  await withStaticServer(buildDir, async (origin) => {
    const browser = await chromium.launch({ headless: true });
    try {
      const page = await browser.newPage({ viewport: { width: 390, height: 844 } });
      await page.addInitScript(({ codeVerifierKey }) => {
        window.__KENIGEVENTS_SEARCH_TEST_TIMEOUTS__ = {
          fetchHeaders: 80,
          streamIdle: 100,
          overall: 600,
        };
        const encode = (value) => btoa(JSON.stringify(value)).replace(/=+$/u, '').replace(/\+/gu, '-').replace(/\//gu, '_');
        const now = Math.floor(Date.now() / 1000);
        const user = {
          id: '11111111-2222-4333-8444-555555555555',
          aud: 'authenticated',
          role: 'authenticated',
          email: 'alex@example.invalid',
          app_metadata: { provider: 'custom:yandex', providers: ['custom:yandex'] },
          user_metadata: {},
          created_at: '2026-07-24T00:00:00.000Z',
        };
        const session = {
          access_token: `${encode({ alg: 'none', typ: 'JWT' })}.${encode({
            aud: 'authenticated',
            exp: now + 3600,
            iat: now,
            sub: user.id,
            email: user.email,
            role: 'authenticated',
          })}.signature`,
          refresh_token: 'r11-search-refresh',
          expires_in: 3600,
          expires_at: now + 3600,
          token_type: 'bearer',
          user,
        };
        const result = {
          schema_version: 'event-search-results-v1',
          surface: 'authorized_event_search',
          algorithm_id: 'r11-mocked-recovery',
          request_id: 'r11-request',
          served_list_id: 'r11-served',
          served_list_hash: 'r11-hash',
          items: [{
            id: 7003,
            event_id: 7003,
            title: 'Хоровое многоголосие',
            semantic_score: 0.93,
            display: {
              id: 7003,
              event_id: 7003,
              title: 'Хоровое многоголосие',
              href: '/sobytiya/horovoe-mnogogolosie/',
              event_type: 'концерт',
              display_date: '31 июля',
              display_time: '19:00',
              display_date_time: '31 июля · 19:00',
              city: 'Калининград',
              place: 'Калининград · Филармония',
              status_label: 'По билетам',
              likes_count: 0,
              shares_count: 0,
            },
          }],
          fallback_items: [],
          has_more: false,
          next_offset: 1,
          quota: { day_remaining: 7 },
        };
        const json = (value, status = 200) => new Response(JSON.stringify(value), {
          status,
          headers: { 'content-type': 'application/json; charset=utf-8' },
        });
        window.__r11SearchCalls = [];
        window.__r11StreamCancelCount = 0;
        const nativeFetch = window.fetch.bind(window);
        window.fetch = (input, init = {}) => {
          const url = String(input instanceof Request ? input.url : input);
          if (url.includes('/auth/v1/token?grant_type=pkce')) return Promise.resolve(json(session));
          if (url.endsWith('/auth/v1/user')) return Promise.resolve(json(user));
          if (url.includes('/auth/v1/logout')) return Promise.resolve(new Response(null, { status: 204 }));
          if (url.endsWith('/rest/v1/rpc/get_event_search_quota_v1')) {
            return Promise.resolve(json([{ day_remaining: 8, month_remaining: 30 }]));
          }
          if (!url.endsWith('/functions/v1/event-search')) return nativeFetch(input, init);

          const body = JSON.parse(String(init.body || '{}'));
          const accept = new Headers(init.headers).get('accept') || '';
          window.__r11SearchCalls.push({ query: body.query, accept, body });
          if (
            body.query === 'заголовки не пришли'
            && window.__r11SearchCalls.filter((call) => call.query === body.query).length === 1
          ) {
            return new Promise(() => {});
          }
          if (accept.includes('application/json')) {
            if (body.query === 'ответ тоже замер') {
              return Promise.resolve(new Response(new ReadableStream({ start() {} }), {
                status: 200,
                headers: { 'content-type': 'application/json; charset=utf-8' },
              }));
            }
            if (body.query === 'полный сбой') {
              return Promise.resolve(json({ error: 'provider_unavailable' }, 503));
            }
            return Promise.resolve(json(result));
          }

          const encoder = new TextEncoder();
          return Promise.resolve(new Response(new ReadableStream({
            start(controller) {
              setTimeout(() => {
                controller.enqueue(encoder.encode(`${JSON.stringify({
                  type: 'progress',
                  stage: 'embedding',
                  progress: 28,
                  label: 'Ищу события…',
                })}\n`));
              }, 15);
            },
            cancel() {
              window.__r11StreamCancelCount += 1;
            },
          }), {
            status: 200,
            headers: { 'content-type': 'application/x-ndjson; charset=utf-8' },
          }));
        };
        localStorage.setItem(codeVerifierKey, JSON.stringify('r11-code-verifier'));
      }, { codeVerifierKey: builtCodeVerifierKey });

      await page.goto(`${origin}/poisk/?code=r11-code`, { waitUntil: 'domcontentloaded' });
      await page.waitForFunction(() => document.querySelector('[data-authorized-search]')?.classList.contains('is-authorized'));

      const input = page.locator('[data-search-input]');
      const form = page.locator('[data-search-form]');
      const button = page.locator('[data-search-submit]');
      const progress = page.locator('[data-search-progress]');
      const skeletons = page.locator('[data-search-skeletons]');
      const accountToggle = page.locator('[data-search-account-toggle]');

      assert.equal((await page.locator('[data-search-avatar-initial]').textContent())?.trim(), 'A');
      assert.match(await accountToggle.getAttribute('aria-label') || '', /Вошли как alex@example\.invalid/u);
      assert.equal(await accountToggle.getAttribute('title'), 'Вошли как alex@example.invalid');
      assert.equal(await page.locator('[data-search-avatar-img], [data-search-avatar-icon]').count(), 0);

      await input.fill('заголовки не пришли');
      await form.evaluate((node) => node.requestSubmit());
      await page.waitForSelector('[data-search-results] [data-event-card][data-event-id="7003"]');
      assert.equal(await input.isEditable(), true);
      assert.equal(await button.isEnabled(), true);
      assert.equal(await button.getAttribute('aria-busy'), 'false');
      assert.equal(await skeletons.isHidden(), true);
      const headerRescueCalls = await page.evaluate(() => (
        window.__r11SearchCalls.filter((call) => call.query === 'заголовки не пришли')
      ));
      assert.equal(headerRescueCalls.length, 2, 'a JSON header timeout receives exactly one fast JSON retry');
      assert.match(headerRescueCalls[0].accept, /application\/json/u);
      assert.match(headerRescueCalls[1].accept, /application\/json/u);
      assert.equal(headerRescueCalls[1].body.stream_rescue, true);
      assert.equal(headerRescueCalls[1].body.use_llm_verifier, false);

      assert.equal(await input.getAttribute('enterkeyhint'), 'search');
      await input.fill('ввод через ime');
      await input.dispatchEvent('keydown', { key: 'Enter', code: 'Enter', isComposing: true });
      await page.waitForTimeout(30);
      assert.equal(
        await page.evaluate(() => window.__r11SearchCalls.filter((call) => call.query === 'ввод через ime').length),
        0,
        'IME composition Enter must not submit an incomplete query',
      );
      await input.fill('поиск энтером');
      await input.press('Enter');
      await page.waitForFunction(() => (
        window.__r11SearchCalls.filter((call) => call.query === 'поиск энтером').length === 1
      ));
      assert.equal(await input.inputValue(), 'поиск энтером', 'Enter submits instead of inserting a newline');

      await page.locator('[data-authorized-search]').evaluate((node) => {
        node.dataset.searchTransport = 'ndjson';
      });
      const cancellationsBeforeIdleRescue = await page.evaluate(() => window.__r11StreamCancelCount);
      await input.fill('поток замер');
      await form.evaluate((node) => node.requestSubmit());
      await page.waitForSelector('[data-search-results] [data-event-card][data-event-id="7003"]');
      assert.equal(await input.isEditable(), true);
      assert.equal(await button.isEnabled(), true);
      assert.equal(await skeletons.isHidden(), true);
      const rescueCalls = await page.evaluate(() => (
        window.__r11SearchCalls.filter((call) => call.query === 'поток замер')
      ));
      assert.equal(rescueCalls.length, 2, 'one streaming request receives exactly one JSON rescue');
      assert.match(rescueCalls[0].accept, /application\/x-ndjson/u);
      assert.match(rescueCalls[1].accept, /application\/json/u);
      assert.equal(rescueCalls[1].body.stream_rescue, true);
      assert.equal(rescueCalls[1].body.use_llm_verifier, false);
      assert.equal(
        await page.evaluate(() => window.__r11StreamCancelCount),
        cancellationsBeforeIdleRescue + 1,
      );
      await page.locator('[data-authorized-search]').evaluate((node) => {
        node.dataset.searchTransport = 'json';
      });

      await input.fill('ответ тоже замер');
      await form.evaluate((node) => node.requestSubmit());
      await page.waitForFunction(() => /слишком много времени/u.test(document.querySelector('[data-search-status]')?.textContent || ''));
      assert.equal(await input.isEditable(), true);
      assert.equal(await button.isEnabled(), true);
      assert.equal(await button.getAttribute('aria-busy'), 'false');
      assert.equal(await skeletons.isHidden(), true);
      assert.equal(await progress.isHidden(), true);

      await input.fill('полный сбой');
      await form.evaluate((node) => node.requestSubmit());
      await page.waitForFunction(() => (
        window.__r11SearchCalls.filter((call) => call.query === 'полный сбой').length === 1
        && document.querySelector('[data-search-status]')?.getAttribute('role') === 'alert'
        && document.querySelector('[data-search-submit]')?.getAttribute('aria-busy') === 'false'
      ));
      assert.equal(await input.isEditable(), true);
      assert.equal(await button.isEnabled(), true);
      assert.equal(await button.getAttribute('aria-busy'), 'false');
      assert.equal(await skeletons.isHidden(), true);
      assert.equal(await progress.isHidden(), true);
      assert.equal(
        await page.evaluate(() => window.__r11SearchCalls.filter((call) => call.query === 'полный сбой').length),
        1,
        'an HTTP provider failure is surfaced without a redundant transport retry',
      );
    } finally {
      await browser.close();
    }
  });
});
