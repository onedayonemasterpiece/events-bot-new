import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('owner shell keeps the complete brand lockup and explicit Today through contextual scroll', async () => {
  const layout = await read('src/layouts/EventLayout.astro');
  const headerFilter = layout.slice(layout.indexOf('const visibleHeaderNavigation'), layout.indexOf('const drawerCurrent'));

  assert.match(layout, /\{ key: 'today', label: 'Сегодня', href: '\/segodnya\/' \}/u);
  assert.doesNotMatch(headerFilter, /item\.key !== 'today'/u);
  assert.match(headerFilter, /item\.key !== 'popular'/u);
  assert.match(layout, /\.site-header__brand-tag \{[\s\S]*flex: 0 0 240px;[\s\S]*width: 240px;[\s\S]*height: 88px;/u);
  assert.doesNotMatch(layout, /body\[data-floating-context="visible"\] \.site-header__brand-tag\s*\{\s*display\s*:\s*none/u);
  assert.match(layout, /\.site-nav \{[\s\S]*overflow-x: auto;[\s\S]*justify-content: flex-start;/u);
  assert.match(layout, /\.site-nav a \{[\s\S]*flex: 0 0 auto;/u);
  assert.match(layout, /data-floating-page-context[\s\S]*ariaLabel="Вернуться к заголовку страницы"/u);
});

test('mobile menu keeps its 120 by 84 leather tag and removes only the rejected all-collections entry', async () => {
  const menu = await read('src/components/Reference4MobileMenu.astro');

  assert.match(menu, /--shell-tag-w:120px; --shell-tag-h:84px/u);
  assert.doesNotMatch(menu, /data-floating-context="visible"[\s\S]{0,260}(?:height:48px|brand-tag__endorsement\s*\{\s*display:none)/u);
  assert.doesNotMatch(menu, /<span>Все подборки<\/span>/u);
  assert.match(menu, /<button class="reference4-menu__row reference4-menu__row--collections"/u);
  for (const destination of ['Афиша', 'Даты', 'Поиск', 'Для меня']) {
    assert.match(await read('src/components/MobileBottomNav.astro'), new RegExp(`label: '${destination}'`, 'u'));
  }
});

test('prelaunch scroll lock is isolated and footer keeps the four-destination dock clear without a blank tail', async () => {
  const [prelaunch, foundations, footer, nav] = await Promise.all([
    read('src/layouts/PrelaunchLayout.astro'),
    read('src/components/design-system/shell-foundations.css'),
    read('src/components/SiteFooter.astro'),
    read('src/components/MobileBottomNav.astro'),
  ]);

  assert.match(prelaunch, /<html lang="ru-RU" class="prelaunch-document">/u);
  assert.match(prelaunch, /body\[data-ds-family="PrelaunchLayout"\] \{[\s\S]*overflow: hidden;/u);
  assert.doesNotMatch(prelaunch, /\n      body \{[\s\S]*?overflow: hidden;/u);
  assert.doesNotMatch(foundations, /body\[data-primary-island="true"\] \{[\s\S]*padding-bottom:/u);
  assert.doesNotMatch(foundations, /mobile-bottom-nav \{ display: none; \}/u);
  assert.match(footer, /padding: var\(--ke-footer-padding-top\) 0 calc\(var\(--ke-footer-padding-bottom\) \+ var\(--ke-lower-surface-offset, 0px\) \+ 24px\);/u);
  for (const destination of ['Афиша', 'Даты', 'Поиск', 'Для меня']) assert.match(nav, new RegExp(`label: '${destination}'`, 'u'));
});
