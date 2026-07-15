import { createHash } from 'node:crypto';
import { expect, test, type Page } from '@playwright/test';

const CANONICAL_URL = 'https://kenigevents.ru/';
const PATH_PREFIX = `/${String(process.env.SERVICE_SHARE_PATH_PREFIX || '').replace(/^\/+|\/+$/gu, '')}`.replace(/^\/$/u, '');
const withPathPrefix = (path: string) => `${PATH_PREFIX}${path}`;
const webpBytes = Buffer.from('52494646080000005745425056503820', 'hex');
const pngBytes = Buffer.from('89504e470d0a1a0a0000000049454e44ae426082', 'hex');
const manifest = {
  schema_version: 'service-share-card-manifest-v1',
  asset_version: 'test-v1',
  visual_payload_hash: createHash('sha256').update('test-visual').digest('hex'),
  canonical_url: CANONICAL_URL,
  share_text: 'KenigEvents — события Калининграда и области\nНайдите своё событие быстрее',
  assets: {
    webp: { url: '/service-share/assets/test-v1.webp', mime_type: 'image/webp', byte_size: webpBytes.length, sha256: createHash('sha256').update(webpBytes).digest('hex') },
    png: { url: '/service-share/assets/test-v1.png', mime_type: 'image/png', byte_size: pngBytes.length, sha256: createHash('sha256').update(pngBytes).digest('hex') },
  },
};

type MockOptions = {
  canShare?: boolean;
  share?: 'resolve' | 'abort' | 'reject' | 'unavailable';
  clipboardWrite?: 'resolve' | 'reject' | 'pending';
  clipboardText?: 'resolve' | 'reject' | 'unavailable';
};

async function installRoutes(page: Page) {
  await page.route('**/service-share/current/manifest.json', (route) => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(manifest) }));
  await page.route('**/service-share/assets/test-v1.webp', (route) => route.fulfill({ status: 200, contentType: 'image/webp', body: webpBytes }));
  await page.route('**/service-share/assets/test-v1.png', (route) => route.fulfill({ status: 200, contentType: 'image/png', body: pngBytes }));
}

async function installBrowserMocks(page: Page, options: MockOptions = {}) {
  await page.addInitScript((settings) => {
    const state: any = {
      shareCalls: [],
      clipboardTextCalls: [],
      clipboardWrites: [],
      canShareCalls: [],
    };
    (window as any).__serviceShareMocks = state;
    Object.defineProperty(navigator, 'canShare', {
      configurable: true,
      value: (payload: any) => {
        state.canShareCalls.push({ files: (payload.files || []).map((file: File) => ({ name: file.name, type: file.type, size: file.size })) });
        return settings.canShare !== false;
      },
    });
    if (settings.share === 'unavailable') {
      Object.defineProperty(navigator, 'share', { configurable: true, value: undefined });
    } else {
      Object.defineProperty(navigator, 'share', {
        configurable: true,
        value: async (payload: any) => {
          state.shareCalls.push({
            text: payload.text,
            url: payload.url,
            files: (payload.files || []).map((file: File) => ({ name: file.name, type: file.type, size: file.size })),
          });
          if (settings.share === 'abort') throw new DOMException('Cancelled', 'AbortError');
          if (settings.share === 'reject') throw new DOMException('Denied', 'NotAllowedError');
        },
      });
    }
    class FakeClipboardItem {
      data: Record<string, Blob | Promise<Blob>>;
      types: string[];
      constructor(data: Record<string, Blob | Promise<Blob>>) {
        this.data = data;
        this.types = Object.keys(data);
      }
      static supports(type: string) { return ['text/plain', 'text/html', 'image/png'].includes(type); }
    }
    Object.defineProperty(window, 'ClipboardItem', { configurable: true, value: FakeClipboardItem });
    const clipboard: any = {
      write: async (items: FakeClipboardItem[]) => {
        if (settings.clipboardWrite === 'reject') throw new DOMException('Blocked', 'DataError');
        if (settings.clipboardWrite === 'pending') {
          await new Promise<void>((resolve) => { state.resolveClipboardWrite = resolve; });
        }
        const captured = [];
        for (const item of items) {
          const representations = [];
          for (const type of item.types) {
            const blob = await item.data[type];
            representations.push({
              type,
              blobType: blob.type,
              size: blob.size,
              text: type.startsWith('text/') ? await blob.text() : null,
              signature: type === 'image/png' ? [...new Uint8Array((await blob.arrayBuffer()).slice(0, 8))] : null,
            });
          }
          captured.push({ types: item.types, representations });
        }
        state.clipboardWrites.push(captured);
      },
    };
    if (settings.clipboardText !== 'unavailable') {
      clipboard.writeText = async (value: string) => {
        if (settings.clipboardText === 'reject') throw new DOMException('Denied', 'NotAllowedError');
        state.clipboardTextCalls.push(value);
      };
    }
    Object.defineProperty(navigator, 'clipboard', { configurable: true, value: clipboard });
  }, options);
}

async function open(page: Page, path: string, options: MockOptions = {}) {
  await installRoutes(page);
  await installBrowserMocks(page, options);
  await page.goto(withPathPrefix(path));
}

const footerRoot = (page: Page) => page.locator('.site-footer [data-service-share-root][data-service-share-surface="footer"]');

test.describe('F18 mobile footer transport', () => {
  test.use({ viewport: { width: 390, height: 844 } });

  test('shares one verified WebP File, shared copy and canonical service URL', async ({ page }) => {
    await open(page, '/sobytiya/pesni-sssr-svetlogorsk-5878/');
    const root = footerRoot(page);
    await expect(root).toHaveCount(1);
    await expect(page.locator('.site-header [data-service-share-root]')).toHaveCount(0);
    await expect(root).toHaveAttribute('data-service-share-canonical-url', CANONICAL_URL);
    await expect(root).toHaveAttribute('data-service-share-ready', 'file');
    await root.getByRole('button', { name: 'Поделиться афишей KenigEvents' }).focus();
    await page.keyboard.press('Enter');
    await expect.poll(() => page.evaluate(() => (window as any).__serviceShareMocks.shareCalls.length)).toBe(1);
    const call = await page.evaluate(() => (window as any).__serviceShareMocks.shareCalls[0]);
    expect(call.url).toBe(CANONICAL_URL);
    expect(call.text).toBe(manifest.share_text);
    expect(call.files).toEqual([{ name: 'kenigevents-service-test-v1.webp', type: 'image/webp', size: webpBytes.length }]);
    expect(JSON.stringify(call)).not.toContain('/sobytiya/');
    await expect(root.locator('[aria-live="polite"]')).toHaveText('Меню «Поделиться» закрыто');
  });

  test('file unsupported uses system share text+URL', async ({ page }) => {
    await open(page, '/segodnya/', { canShare: false });
    const root = footerRoot(page);
    await expect(root).toHaveAttribute('data-service-share-ready', 'file');
    await root.getByRole('button', { name: 'Поделиться афишей KenigEvents' }).click();
    await expect.poll(() => page.evaluate(() => (window as any).__serviceShareMocks.shareCalls.length)).toBe(1);
    const call = await page.evaluate(() => (window as any).__serviceShareMocks.shareCalls[0]);
    expect(call.files).toEqual([]);
    expect(call.url).toBe(CANONICAL_URL);
  });

  test('Web Share rejection falls back to clipboard text+URL', async ({ page }) => {
    await open(page, '/poisk/', { share: 'reject' });
    const root = footerRoot(page);
    await expect(root).toHaveAttribute('data-service-share-ready', 'file');
    await root.getByRole('button', { name: 'Поделиться афишей KenigEvents' }).click();
    await expect(root.locator('[aria-live="polite"]')).toHaveText('Скопированы текст и ссылка');
    const copied = await page.evaluate(() => (window as any).__serviceShareMocks.clipboardTextCalls[0]);
    expect(copied).toContain(CANONICAL_URL);
    expect(copied).not.toContain('/poisk/');
  });

  test('AbortError is cancellation and does not copy', async ({ page }) => {
    await open(page, '/__preview/', { share: 'abort' });
    const root = footerRoot(page);
    await expect(root).toHaveAttribute('data-service-share-ready', 'file');
    await root.getByRole('button', { name: 'Поделиться афишей KenigEvents' }).click();
    await expect(root.locator('[aria-live="polite"]')).toHaveText('Отменено');
    expect(await page.evaluate(() => (window as any).__serviceShareMocks.clipboardTextCalls.length)).toBe(0);
  });

  test('clipboard denial reveals an ordinary canonical link', async ({ page }) => {
    await open(page, '/__preview/', { share: 'unavailable', clipboardText: 'reject' });
    const root = footerRoot(page);
    await root.getByRole('button', { name: 'Поделиться афишей KenigEvents' }).click();
    await expect(root.locator('[data-service-share-fallback]')).toBeVisible();
    await expect(root.locator('[data-service-share-fallback]')).toHaveAttribute('href', CANONICAL_URL);
    await expect(root.locator('[aria-live="polite"]')).toContainText('Ссылка доступна ниже');
  });
});

test.describe('F18 desktop footer with explicit clipboard intents', () => {
  test.use({ viewport: { width: 1366, height: 768 } });

  test('renders a quiet utility row with two equal desktop actions', async ({ page }) => {
    await open(page, '/__preview/');
    const root = footerRoot(page);
    await expect(root).toHaveAttribute('data-service-share-desktop-mode', 'd0');
    await expect(root).toContainText('Поделиться афишей');
    await expect(root).not.toContainText('Понравилась афиша?');
    await expect(root.getByRole('button', { name: 'Скопировать карточку KenigEvents' })).toBeVisible();
    await expect(root.getByRole('button', { name: 'Скопировать текст и ссылку KenigEvents' })).toBeVisible();
    await expect(root.getByRole('button', { name: 'Поделиться афишей KenigEvents' })).toBeHidden();
    const linkIconMask = await root.locator('[data-service-share-intent="text"] [data-service-share-icon-default]').evaluate((element) => {
      const computed = getComputedStyle(element);
      return computed.maskImage || (computed as any).webkitMaskImage || '';
    });
    expect(linkIconMask).toContain('link-minimalistic-svgrepo.svg');
    const styles = await root.evaluate((element) => {
      const computed = getComputedStyle(element);
      return {
        backgroundImage: computed.backgroundImage,
        boxShadow: computed.boxShadow,
        borderTopWidth: computed.borderTopWidth,
        borderRadius: computed.borderRadius,
      };
    });
    expect(styles).toEqual({ backgroundImage: 'none', boxShadow: 'none', borderTopWidth: '0px', borderRadius: '0px' });
  });

  test('text action writes only plain text and canonical URL', async ({ page }) => {
    await open(page, '/__preview/');
    const root = footerRoot(page);
    const textButton = root.getByRole('button', { name: 'Скопировать текст и ссылку KenigEvents' });
    const heightBefore = (await root.boundingBox())?.height;
    await textButton.click();
    await expect(root.locator('[aria-live="polite"]')).toHaveText('Текст и ссылка скопированы');
    await expect(textButton).toHaveAttribute('data-service-share-state', 'success');
    await expect(textButton.locator('[data-service-share-icon-default]')).toBeHidden();
    await expect(textButton.locator('[data-service-share-icon-success]')).toBeVisible();
    expect(await textButton.evaluate((element) => getComputedStyle(element).color)).toBe('rgb(47, 116, 73)');
    expect((await root.boundingBox())?.height).toBe(heightBefore);
    await expect(root.locator('[aria-live="polite"]')).toHaveCSS('position', 'absolute');
    await expect(root.locator('[aria-live="polite"]')).toHaveCSS('width', '1px');
    await expect(root.locator('[aria-live="polite"]')).toHaveCSS('height', '1px');
    expect(await page.evaluate(() => (window as any).__serviceShareMocks.shareCalls.length)).toBe(0);
    expect(await page.evaluate(() => (window as any).__serviceShareMocks.clipboardTextCalls[0])).toContain(CANONICAL_URL);
    expect(await page.evaluate(() => (window as any).__serviceShareMocks.clipboardWrites.length)).toBe(0);
  });

  test('card action writes one ClipboardItem containing image/png only', async ({ page }) => {
    await open(page, '/lab/service-share/');
    const root = page.locator('[data-service-share-root][data-service-share-surface="lab"]');
    await expect(root).toHaveAttribute('data-service-share-ready', 'file');
    await root.getByRole('button', { name: 'Скопировать карточку KenigEvents' }).click();
    await expect(root.locator('[aria-live="polite"]')).toHaveText('Карточка скопирована в буфер');
    const writes = await page.evaluate(() => (window as any).__serviceShareMocks.clipboardWrites);
    expect(writes).toHaveLength(1);
    expect(writes[0]).toHaveLength(1);
    expect(writes[0][0].types).toEqual(['image/png']);
    expect(writes[0][0].representations).toHaveLength(1);
    expect(writes[0][0].representations[0].blobType).toBe('image/png');
    expect(writes[0][0].representations[0].signature).toEqual([137, 80, 78, 71, 13, 10, 26, 10]);
    expect(await page.evaluate(() => (window as any).__serviceShareMocks.clipboardTextCalls.length)).toBe(0);
    expect(await page.evaluate(() => (window as any).__serviceShareMocks.shareCalls.length)).toBe(0);
  });

  test('image failure stays an image error and never falls back to text', async ({ page }) => {
    await open(page, '/lab/service-share/', { clipboardWrite: 'reject' });
    const root = page.locator('[data-service-share-root][data-service-share-surface="lab"]');
    await expect(root).toHaveAttribute('data-service-share-ready', 'file');
    await root.getByRole('button', { name: 'Скопировать карточку KenigEvents' }).click();
    await expect(root.locator('[aria-live="polite"]')).toHaveText('Не удалось скопировать картинку');
    await expect(root.getByRole('button', { name: 'Скопировать карточку KenigEvents' })).toHaveAttribute('data-service-share-state', 'error');
    expect(await page.evaluate(() => (window as any).__serviceShareMocks.clipboardTextCalls)).toHaveLength(0);
    await expect(root.getByRole('button', { name: 'Скопировать текст и ссылку KenigEvents' })).toBeEnabled();
  });

  test('busy state is scoped to the invoked desktop button', async ({ page }) => {
    await open(page, '/__preview/', { clipboardWrite: 'pending' });
    const root = footerRoot(page);
    const imageButton = root.getByRole('button', { name: 'Скопировать карточку KenigEvents' });
    const textButton = root.getByRole('button', { name: 'Скопировать текст и ссылку KenigEvents' });
    const clickPromise = imageButton.click();
    await expect(imageButton).toHaveAttribute('aria-busy', 'true');
    expect(await imageButton.isDisabled()).toBe(true);
    expect(await textButton.isEnabled()).toBe(true);
    expect(await textButton.getAttribute('aria-busy')).toBe('false');
    await page.evaluate(() => (window as any).__serviceShareMocks.resolveClipboardWrite());
    await clickPromise;
    await expect(imageButton).toBeEnabled();
    await expect(imageButton).toHaveAttribute('aria-busy', 'false');
  });

  test('desktop controls wrap safely at 200% zoom', async ({ page }) => {
    await open(page, '/__preview/');
    await page.evaluate(() => { document.documentElement.style.zoom = '2'; });
    const root = footerRoot(page);
    await expect(root.getByRole('button', { name: 'Скопировать карточку KenigEvents' })).toBeVisible();
    await expect(root.getByRole('button', { name: 'Скопировать текст и ссылку KenigEvents' })).toBeVisible();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
  });

  test('lab exposes bounded capabilities, controlled paste targets and ledger clear', async ({ page }) => {
    await open(page, '/lab/service-share/');
    await expect(page.locator('[data-capability="secure-context"]')).toHaveText('yes');
    await expect(page.locator('[data-capability="navigator-share"]')).toHaveText('yes');
    await expect(page.locator('[data-capability="clipboard-item"]')).toHaveText('yes');
    await expect(page.locator('[data-paste-target="plain"]')).toBeVisible();
    await expect(page.locator('[data-paste-target="rich"]')).toBeVisible();
    await expect(page.locator('[data-paste-target="image"]')).toBeVisible();
    await footerRoot(page).getByRole('button', { name: 'Скопировать текст и ссылку KenigEvents' }).click();
    await expect(page.locator('[data-service-share-ledger] li')).not.toHaveCount(0);
    await page.locator('[data-clear-service-share-ledger]').click();
    await expect(page.locator('[data-service-share-ledger] li')).toHaveCount(0);
  });
});

test('common page families expose only the footer placement', async ({ page }) => {
  await installRoutes(page);
  await installBrowserMocks(page);
  for (const path of ['/__preview/', '/segodnya/', '/poisk/', '/sobytiya/pesni-sssr-svetlogorsk-5878/']) {
    await page.goto(withPathPrefix(path));
    await expect(footerRoot(page), path).toHaveCount(1);
    await expect(page.locator('.site-header [data-service-share-root]'), path).toHaveCount(0);
  }
});

test('no-JS footer keeps an ordinary selectable canonical link', async ({ browser }) => {
  const context = await browser.newContext({ javaScriptEnabled: false, viewport: { width: 390, height: 844 } });
  const page = await context.newPage();
  await page.goto(withPathPrefix('/__preview/'), { waitUntil: 'domcontentloaded', timeout: 15_000 });
  const link = page.locator('.site-footer noscript a[href="https://kenigevents.ru/"]');
  await expect(link).toBeVisible();
  await expect(link).toHaveText(CANONICAL_URL);
  await context.close();
});
