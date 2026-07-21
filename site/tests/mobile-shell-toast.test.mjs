import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('EventLayout is the single mobile shell owner with canonical route mapping', async () => {
  const layout = await read('src/layouts/EventLayout.astro');
  const search = await read('src/pages/poisk/index.astro');
  const collection = await read('src/pages/podborki/[slug]/index.astro');
  assert.match(layout, /import MobileBottomNav/u);
  assert.match(layout, /import MobileToastRegion/u);
  assert.match(layout, /<\/header>\s*<MobileToastRegion\s*\/>/u, 'toast region must immediately follow the header');
  assert.match(layout, /resolvedMobileBottomMode === 'nav' && <MobileBottomNav current=\{mobileSection\} \/>/u);
  assert.match(layout, /\/(?:\\\/)?\(\?:poisk\|podborki\)/u);
  assert.match(layout, /headerCurrent === 'today' \|\| headerCurrent === 'tomorrow'/u);
  assert.match(layout, /resolvedMobileTopMode === 'immersive' \? 'cta'/u);
  assert.match(search, /mobileSection="search"/u);
  assert.match(collection, /mobileSection="search"/u);
  assert.doesNotMatch(search, /MobileSearchBottomNav|MobileBottomNav/u);
  assert.doesNotMatch(collection, /MobileSearchBottomNav|MobileBottomNav/u);
});

test('shared bottom nav has one prop-owned current item and no scroll or :has ownership', async () => {
  const nav = await read('src/components/MobileBottomNav.astro');
  const compatibility = await read('src/components/MobileSearchBottomNav.astro');
  const layout = await read('src/layouts/EventLayout.astro');
  assert.match(nav, /'afisha' \| 'dates' \| 'search' \| 'personal' \| null/u);
  assert.equal((nav.match(/aria-current=\{item\.key === current/gu) || []).length, 1);
  assert.match(nav, /item\.key === current \? 'page'/u);
  assert.doesNotMatch(nav, /body:has|backdrop-filter|scrollY|is-hidden/u);
  assert.match(nav, /--mobile-bottom-stack-h/u);
  assert.match(compatibility, /<MobileBottomNav current="search" \/>/u);
  for (const variable of ['--mobile-header-h', '--mobile-top-chrome-bottom', '--mobile-nav-h', '--mobile-bottom-stack-h']) {
    assert.match(layout, new RegExp(variable));
  }
  assert.match(layout, /body\[data-mobile-bottom-mode="nav"\] \{ padding-bottom: var\(--mobile-bottom-stack-h\); \}/u);
  assert.doesNotMatch(layout, /body:has\(\.mobile-(?:search-)?bottom-nav/u);
});

test('toast API owns bounded FIFO replacement, persistence and stale timers', async () => {
  const toast = await read('src/components/MobileToastRegion.astro');
  assert.match(toast, /KenigEventsToast/u);
  assert.match(toast, /kenigevents:toast/u);
  assert.match(toast, /const MAX_QUEUE = 4/u);
  assert.match(toast, /queue\.findIndex\(\(item\) => item\.dedupeKey === entry\.dedupeKey\)/u);
  assert.match(toast, /current\?\.dedupeKey === entry\.dedupeKey\) render\(entry\)/u);
  assert.match(toast, /if \(queue\.length > MAX_QUEUE\) queue\.shift\(\)/u);
  assert.match(toast, /type === 'error' \|\| Boolean\(normalizedAction\)/u);
  assert.match(toast, /duration:persistent \? null/u);
  assert.match(toast, /Number\(source\.duration\) \|\| 5000/u);
  assert.match(toast, /expectedGeneration !== generation/u);
  assert.match(toast, /pagehide[\s\S]*clearTimer\(\)[\s\S]*controller\.abort\(\)/u);
});

test('toast pauses actual time and countdown and honors safe/reduced-motion geometry', async () => {
  const toast = await read('src/components/MobileToastRegion.astro');
  assert.match(toast, /remaining = Math\.max\(0, remaining - \(performance\.now\(\) - startedAt\)\)/u);
  for (const reason of ['pointer', 'focus', 'touch', 'window', 'visibility', 'drawer']) {
    assert.match(toast, new RegExp(`(?:pause|resume)\\('${reason}'\\)`));
  }
  assert.match(toast, /top:calc\(var\(--mobile-top-chrome-bottom\) \+ 8px\)/u);
  assert.match(toast, /max-height:72px/u);
  assert.match(toast, /min-width:44px; min-height:44px/u);
  assert.match(toast, /transform-origin:left/u);
  assert.match(toast, /@keyframes mobile-toast-retreat \{ to \{ transform:scaleX\(0\); \} \}/u);
  assert.match(toast, /prefers-reduced-motion:reduce/u);
  assert.match(toast, /animation:none!important/u);
});

test('shared producers avoid duplicate keyboard and phone announcements', async () => {
  const keyboardUi = await read('src/components/KeyboardEventNavigationPrototype.astro');
  const keyboardRuntime = await read('src/lib/keyboardEventNavigation.mjs');
  const phone = await read('src/components/DesktopEventActionPanel.astro');
  const layout = await read('src/layouts/EventLayout.astro');
  assert.doesNotMatch(keyboardUi, /data-keyboard-action-toast|keyboard-action-toast/u);
  assert.match(keyboardUi, /data-keyboard-prototype-status role="status" aria-live="polite"/u);
  assert.match(keyboardRuntime, /dedupeKey: 'keyboard-action', announce: false/u);
  assert.match(phone, /dedupeKey:'phone-copy', announce:false/u);
  assert.match(phone, /copyEpochs\.get\(button\) !== epoch/u);
  assert.match(layout, /showMobileActionToast\('Ссылка скопирована'/u);
  assert.match(layout, /showMobileActionToast\('Не удалось скопировать ссылку'/u);
  assert.match(layout, /shareStatusEpochs\.get\(button\) !== epoch/u);
  assert.doesNotMatch(layout, /authorized-search[^\n]+KenigEventsToast|KenigEventsToast[^\n]+authorized-search/u);
});
