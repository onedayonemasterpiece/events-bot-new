import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { existsSync, mkdirSync, readFileSync } from 'node:fs';
import { basename, dirname, extname, join, normalize, resolve, sep } from 'node:path';
import { test } from 'node:test';
import { pathToFileURL } from 'node:url';

import { chromium } from 'playwright';

const previewDir = process.env.SEARCH_PROGRESS_PREVIEW_DIR
  ? resolve(process.env.SEARCH_PROGRESS_PREVIEW_DIR)
  : resolve('dist/preview-r9-search-local');
const hasPreview = existsSync(join(previewDir, 'poisk', 'index.html'));

const types = new Map([
  ['.css', 'text/css; charset=utf-8'],
  ['.html', 'text/html; charset=utf-8'],
  ['.js', 'text/javascript; charset=utf-8'],
  ['.json', 'application/json; charset=utf-8'],
  ['.svg', 'image/svg+xml'],
  ['.webp', 'image/webp'],
]);

function localFile(root, requestPath) {
  const relative = decodeURIComponent(requestPath.split('?', 1)[0]).replace(/^\/+/u, '');
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
  const address = server.address();
  try {
    await callback(`http://127.0.0.1:${address.port}`);
  } finally {
    await new Promise((resolveClose, reject) => server.close((error) => (error ? reject(error) : resolveClose())));
  }
}

test('390px Search CTA follows accepted progress lifecycle', { skip: !hasPreview }, async () => {
  const previewName = basename(previewDir);
  const distRoot = dirname(previewDir);
  await withStaticServer(distRoot, async (origin) => {
    const browser = await chromium.launch({ headless: true });
    try {
    const page = await browser.newPage({ viewport: { width: 390, height: 844 }, deviceScaleFactor: 2 });
    await page.addInitScript(() => {
      const encode = (value) => btoa(JSON.stringify(value)).replace(/=+$/u, '').replace(/\+/gu, '-').replace(/\//gu, '_');
      const now = Math.floor(Date.now() / 1000);
      const user = {
        id: '11111111-2222-4333-8444-555555555555',
        aud: 'authenticated',
        role: 'authenticated',
        email: 'r9-search@example.invalid',
        app_metadata: { provider: 'custom:yandex', providers: ['custom:yandex'] },
        user_metadata: { preferred_username: 'r9-search' },
        created_at: '2026-07-23T00:00:00.000Z',
      };
      const accessToken = `${encode({ alg: 'none', typ: 'JWT' })}.${encode({
        aud: 'authenticated',
        exp: now + 3600,
        iat: now,
        sub: user.id,
        email: user.email,
        role: 'authenticated',
      })}.signature`;
      const session = {
        access_token: accessToken,
        refresh_token: 'r9-search-refresh',
        expires_in: 3600,
        expires_at: now + 3600,
        token_type: 'bearer',
        user,
      };
      const result = {
        schema_version: 'event-search-results-v1',
        surface: 'authorized_event_search',
        algorithm_id: 'r9-playwright',
        request_id: 'r9-request',
        served_list_id: 'r9-served',
        served_list_hash: 'r9-hash',
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
      window.__r9SearchCalls = 0;
      window.__r9AbortCount = 0;
      window.__r9NativeFetch = window.fetch.bind(window);
      window.fetch = (input, init = {}) => {
        const url = String(input instanceof Request ? input.url : input);
        if (url.includes('/auth/v1/token?grant_type=pkce')) return Promise.resolve(json(session));
        if (url.endsWith('/auth/v1/user')) return Promise.resolve(json(user));
        if (url.includes('/auth/v1/logout')) return Promise.resolve(new Response(null, { status: 204 }));
        if (url.endsWith('/rest/v1/rpc/get_event_search_quota_v1')) {
          return Promise.resolve(json([{ day_remaining: 8, month_remaining: 30 }]));
        }
        if (!url.endsWith('/functions/v1/event-search')) return window.__r9NativeFetch(input, init);
        window.__r9SearchCalls += 1;
        const query = JSON.parse(String(init.body || '{}')).query || '';
        const encoder = new TextEncoder();
        const frames = query.includes('ошиб')
          ? [
              [80, { type: 'progress', stage: 'embedding', progress: 28, label: 'Ищу события…' }],
              [180, { type: 'error', status: 503, error: 'provider_unavailable', message: 'Временно недоступно' }],
            ]
          : query.includes('отмена')
            ? [
                [80, { type: 'progress', stage: 'embedding', progress: 28, label: 'Ищу события…' }],
                [2000, { type: 'result', progress: 100, label: 'Готово', data: result }],
              ]
            : [
                [400, { type: 'progress', stage: 'embedding', progress: 28, label: 'Ищу события…' }],
                [650, { type: 'progress', stage: 'vector_search', progress: 55, label: 'Варианты найдены…' }],
                [3000, { type: 'result', progress: 100, label: 'Готово', data: result }],
              ];
        const signal = init.signal;
        const stream = new ReadableStream({
          start(controller) {
            const timers = frames.map(([delay, frame], index) => setTimeout(() => {
              if (signal?.aborted) return;
              controller.enqueue(encoder.encode(`${JSON.stringify(frame)}\n`));
              if (index === frames.length - 1) controller.close();
            }, delay));
            signal?.addEventListener('abort', () => {
              window.__r9AbortCount += 1;
              timers.forEach(clearTimeout);
              controller.error(new DOMException('Aborted', 'AbortError'));
            }, { once: true });
          },
        });
        return Promise.resolve(new Response(stream, {
          status: 200,
          headers: { 'content-type': 'application/x-ndjson; charset=utf-8' },
        }));
      };
    });

    const cdnPrefix = `https://static.kenigevents.ru/${previewName}/`;
    await page.route(`${cdnPrefix}**`, async (route) => {
      const relative = route.request().url().slice(cdnPrefix.length).split('?', 1)[0];
      const path = localFile(previewDir, relative);
      if (!path || !existsSync(path)) {
        await route.fulfill({ status: 404, body: 'not found' });
        return;
      }
      await route.fulfill({
        status: 200,
        contentType: types.get(extname(path)) || 'application/octet-stream',
        body: readFileSync(path),
      });
    });

    const searchUrl = `${origin}/${previewName}/poisk/`;
    await page.goto(searchUrl, { waitUntil: 'domcontentloaded' });
    const configuredSupabaseUrl = await page.locator('[data-authorized-search]').getAttribute('data-supabase-url');
    const configuredProjectRef = configuredSupabaseUrl
      ? new URL(configuredSupabaseUrl).hostname.split('.', 1)[0]
      : 'search-r9';
    await page.evaluate((projectRef) => {
      localStorage.setItem(`sb-${projectRef}-auth-token-code-verifier`, JSON.stringify('r9-code-verifier'));
    }, configuredProjectRef);
    await page.goto(`${searchUrl}?code=r9-code`, { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => document.querySelector('[data-authorized-search]')?.classList.contains('is-authorized'));

    const button = page.locator('[data-search-submit]');
    const label = page.locator('[data-search-submit-label]');
    const progress = page.locator('[data-search-progress]');
    const input = page.locator('[data-search-input]');
    const form = page.locator('[data-search-form]');

    await input.fill('послушать хор');
    await form.evaluate((node) => {
      node.requestSubmit();
      node.requestSubmit();
    });
    await assert.doesNotReject(async () => {
      await button.waitFor({ state: 'visible' });
      assert.equal(await button.getAttribute('aria-busy'), 'true');
      assert.equal(await button.isDisabled(), true);
      assert.equal((await label.textContent())?.trim(), 'Ищу…');
      assert.equal(await progress.getAttribute('aria-valuenow'), null);
    });
    assert.equal(await page.evaluate(() => window.__r9SearchCalls), 1, 'loading guard prevents a duplicate request');

    await page.waitForFunction(() => document.querySelector('[data-search-progress]')?.getAttribute('aria-valuenow') === '55');
    await page.waitForFunction(() => {
      const node = document.querySelector('[data-search-submit]');
      if (!node) return false;
      const ratio = parseFloat(getComputedStyle(node, '::before').width) / node.getBoundingClientRect().width;
      return ratio > 0.53 && ratio < 0.57;
    });
    const visual = await button.evaluate((node) => {
      const rect = node.getBoundingClientRect();
      const before = getComputedStyle(node, '::before');
      return {
        width: rect.width,
        height: rect.height,
        radius: getComputedStyle(node).borderRadius,
        shell: getComputedStyle(node).backgroundColor,
        fill: before.backgroundColor,
        fillWidth: parseFloat(before.width),
      };
    });
    assert.ok(visual.width >= 350 && visual.width <= 366, JSON.stringify(visual));
    assert.equal(visual.height, 50);
    assert.equal(visual.radius, '8px');
    assert.equal(visual.shell, 'rgb(34, 26, 20)');
    assert.equal(visual.fill, 'rgb(152, 64, 31)');
    assert.ok(visual.fillWidth / visual.width > 0.53 && visual.fillWidth / visual.width < 0.57, JSON.stringify(visual));
    assert.match((await page.locator('[data-search-progress-label]').textContent()) || '', /Варианты|Ищу/u);

    const evidenceDir = process.env.SEARCH_PROGRESS_EVIDENCE_DIR;
    if (evidenceDir) {
      mkdirSync(evidenceDir, { recursive: true });
      await page.screenshot({ path: join(evidenceDir, 'r9-search-progress-55-390x844-dpr2.png'), fullPage: true });
    }

    await page.waitForFunction(() => document.querySelector('[data-search-progress]')?.getAttribute('aria-valuenow') === '100');
    assert.equal(await progress.getAttribute('aria-valuenow'), '100');
    assert.equal((await label.textContent())?.trim(), 'Готово');
    assert.equal(await button.getAttribute('aria-busy'), 'false');
    await page.waitForSelector('[data-search-results] [data-event-card][data-event-id="7003"]');
    await page.waitForFunction(() => document.querySelector('[data-search-progress]')?.hidden === true);
    assert.equal((await label.textContent())?.trim(), 'Искать');

    await input.fill('ошибка провайдера');
    await form.evaluate((node) => node.requestSubmit());
    await page.waitForFunction(() => document.querySelector('[data-search-status]')?.getAttribute('role') === 'alert');
    assert.equal(await button.getAttribute('aria-busy'), 'false');
    assert.equal((await label.textContent())?.trim(), 'Искать');
    assert.equal(await progress.isHidden(), true);

    await input.fill('отмена поиска');
    await form.evaluate((node) => node.requestSubmit());
    await page.waitForFunction(() => document.querySelector('[data-search-submit]')?.getAttribute('aria-busy') === 'true');
    await page.locator('[data-search-account-toggle]').click();
    await page.locator('[data-search-logout]').click();
    await page.waitForFunction(() => window.__r9AbortCount === 1);
    assert.equal(await button.getAttribute('aria-busy'), 'false');
    assert.equal((await label.textContent())?.trim(), 'Искать');
    assert.equal(await progress.isHidden(), true);
    assert.equal(await page.locator('[data-search-results] [data-event-card]').count(), 0);

    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth), true);
    } finally {
      await browser.close();
    }
  });
});

if (import.meta.url === pathToFileURL(process.argv[1] || '').href && !hasPreview) {
  process.stderr.write(`Skipped: build preview first or set SEARCH_PROGRESS_PREVIEW_DIR (looked for ${previewDir})\n`);
}
