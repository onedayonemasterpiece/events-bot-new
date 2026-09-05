import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('EventLayout is the single mobile shell owner with canonical route mapping', async () => {
  const layout = await read('src/layouts/EventLayout.astro');
  const drawer = await read('src/components/Reference4MobileMenu.astro');
  const search = await read('src/pages/poisk/index.astro');
  const collection = await read('src/pages/podborki/[slug]/index.astro');
  assert.match(layout, /import MobileBottomNav/u);
  assert.match(layout, /import MobileToastRegion/u);
  assert.match(layout, /import Reference4MobileMenu/u);
  assert.match(layout, /<Reference4MobileMenu current=\{drawerCurrent\} discoveryBases=\{mobileDiscoveryBases\} badge=\{sharedHeaderBadge\} \/>/u);
  assert.match(layout, /<\/header>\s*<MobileToastRegion\s*\/>/u, 'toast region must immediately follow the header');
  assert.match(layout, /resolvedMobileBottomMode !== 'none' && <MobileBottomNav current=\{mobileSection\} \/>/u);
  assert.match(layout, /\/(?:\\\/)?\(\?:poisk\|podborki\)/u);
  assert.match(layout, /headerCurrent === 'today' \|\| headerCurrent === 'tomorrow'/u);
  assert.match(layout, /resolvedMobileTopMode === 'immersive' \? 'cta'/u);
  assert.match(search, /mobileSection="search"/u);
  assert.match(
    collection,
    /mobileSection=\{collection\.slug === 'besplatnye-sobytiya' \? 'home' : 'search'\}/u,
    'the Free collection stays in the Afisha shell instead of masquerading as Search',
  );
  assert.doesNotMatch(search, /MobileSearchBottomNav|MobileBottomNav/u);
  assert.doesNotMatch(collection, /MobileSearchBottomNav|MobileBottomNav/u);
  assert.match(drawer, /data-reference4-fullscreen/u);
  assert.match(drawer, /--shell-tag-w:120px; --shell-tag-h:84px/u);
  assert.match(drawer, /transform:translate3d\(0,-100%,0\)/u);
  assert.match(drawer, /height:100%!important/u);
  assert.match(drawer, /backdrop-filter:blur\(22px\) saturate\(\.96\) brightness\(\.84\)/u);
  assert.doesNotMatch(drawer, /\.reference4-menu__brand::before/u);
  assert.match(drawer, /\.reference4-menu__brand \{[\s\S]*filter:drop-shadow\(0 1px 1px rgba\(255,255,255,.28\)\)/u);
  assert.match(drawer, /html\.shell-menu-open \.mobile-bottom-nav/u);
  assert.match(drawer, /data-service-share-root/u);
  assert.match(drawer, /hydrateServiceShareActions/u);
  assert.match(drawer, /Бесплатно[\s\S]*Подборки[\s\S]*Выставки[\s\S]*Фестивали[\s\S]*Популярное[\s\S]*О сервисе[\s\S]*Поиск[\s\S]*Для меня/u);
  assert.match(drawer, /data-reference4-collections[\s\S]*Необычное[\s\S]*Бесплатно[\s\S]*Гастрономия[\s\S]*Клубы по интересам/u);
  assert.doesNotMatch(drawer, /data-reference4-collections[\s\S]{0,480}Все подборки/u);
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
  assert.match(toast, /const actionGeneration = generation;[\s\S]*dismiss\(actionGeneration\);[\s\S]*callback\?\.\(\)/u,
    'an action must dismiss only the entry it was rendered for');
  assert.match(toast, /data-app-lower-surface="notification"/u);
  assert.match(toast, /toast\.dataset\.appLowerLifecycle = entry\.duration === null \? 'persistent' : 'passive'/u);
  assert.match(toast, /syncLowerSurfaceState[\s\S]*is-modal-obscured[\s\S]*pause\('lower-surface-modal'\)[\s\S]*resume\('lower-surface-modal'\)/u);
  assert.match(toast, /pagehide[\s\S]*clearTimer\(\)[\s\S]*controller\.abort\(\)/u);
});

test('toast pauses actual time and countdown and honors safe/reduced-motion geometry', async () => {
  const toast = await read('src/components/MobileToastRegion.astro');
  assert.match(toast, /remaining = Math\.max\(0, remaining - \(performance\.now\(\) - startedAt\)\)/u);
  assert.match(toast, /if \(!pauseReasons\.delete\(reason\)\) return;/u, 'repeat resume events must not restart the timer');
  assert.match(toast, /if \(document\.hidden\) pause\('visibility'\);[\s\S]*syncLowerSurfaceState\(document\.body\.dataset\.lowerSurfaceState === 'modal'\)/u);
  assert.match(toast, /data-mobile-toast-time[\s\S]*Ещё \$\{Math\.max\(1, Math\.ceil\(remaining \/ 1000\)\)\} с/u);
  for (const reason of ['pointer', 'focus', 'touch', 'window', 'visibility', 'drawer']) {
    assert.match(toast, new RegExp(`(?:pause|resume)\\('${reason}'\\)`));
  }
  assert.match(toast, /bottom:calc\(var\(--ke-lower-surface-offset, 0px\) \+ max\(12px, env\(safe-area-inset-bottom\)\)\)/u);
  assert.doesNotMatch(toast, /max-height:72px|mobile-toast__message[^\n]*-webkit-line-clamp/u, 'lower notices must not clip long messages');
  assert.match(toast, /min-width:var\(--ke-toast-control-size\); min-height:var\(--ke-toast-control-size\)/u);
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
  assert.match(keyboardUi, /data-keyboard-prototype-status[\s\S]{0,80}role="status"[\s\S]{0,80}aria-live="polite"/u);
  assert.match(keyboardRuntime, /dedupeKey: 'keyboard-action', announce: false/u);
  assert.match(phone, /dedupeKey:'phone-copy', announce:false/u);
  assert.match(phone, /copyEpochs\.get\(button\) !== epoch/u);
  assert.match(layout, /showMobileActionToast\('Ссылка скопирована'/u);
  assert.match(layout, /showMobileActionToast\('Не удалось скопировать ссылку'/u);
  assert.match(layout, /shareStatusEpochs\.get\(button\) !== epoch/u);
  assert.doesNotMatch(layout, /window as any/u, 'inline EventLayout runtime must stay browser-valid JavaScript');
  assert.doesNotMatch(layout, /authorized-search[^\n]+KenigEventsToast|KenigEventsToast[^\n]+authorized-search/u);
});

class RuntimeTarget {
  #listeners = new Map();
  addEventListener(type, listener) { const listeners = this.#listeners.get(type) || []; listeners.push(listener); this.#listeners.set(type, listeners); }
  dispatchEvent(event) { event.currentTarget = this; for (const listener of this.#listeners.get(event.type) || []) listener(event); }
}
const element = () => {
  const target = new RuntimeTarget(); const classes = new Set();
  target.hidden = false; target.textContent = ''; target.dataset = {}; target.style = { setProperty() {} };
  target.classList = { add: (...values) => values.forEach((value) => classes.add(value)), remove: (...values) => values.forEach((value) => classes.delete(value)), toggle: (value, force) => { const enabled = force === undefined ? !classes.has(value) : force; if (enabled) classes.add(value); else classes.delete(value); return enabled; }, contains: (value) => classes.has(value) };
  target.contains = () => false; Object.defineProperty(target, 'offsetWidth', { get: () => 1 }); return target;
};
const toastRuntime = async ({ reducedMotion = false, hidden = false, modal = false } = {}) => {
  const { stripTypeScriptTypes } = await import('node:module'); const vm = await import('node:vm');
  const source = await read('src/components/MobileToastRegion.astro'); const script = source.match(/<script>([\s\S]*?)<\/script>/u)?.[1];
  assert.ok(script, 'toast component must contain its client runtime');
  let now = 0; let nextTimer = 1; const timers = new Map();
  const clock = { setTimeout(callback, delay) { const id = nextTimer++; timers.set(id, { callback, at: now + delay }); return id; }, clearTimeout(id) { timers.delete(id); }, advance(milliseconds) { const until = now + milliseconds; while (true) { const due = [...timers.entries()].filter(([, timer]) => timer.at <= until).sort(([, left], [, right]) => left.at - right.at)[0]; if (!due) break; const [id, timer] = due; timers.delete(id); now = timer.at; timer.callback(); } now = until; } };
  const region = element(); const toast = element(); const message = element(); const action = element(); const close = element(); const countdown = element(); const time = element(); const polite = element(); const assertive = element();
  const inside = new Map([['[data-mobile-toast]', toast], ['[data-mobile-toast-message]', message], ['[data-mobile-toast-action]', action], ['[data-mobile-toast-close]', close], ['[data-mobile-toast-countdown]', countdown], ['[data-mobile-toast-time]', time]]);
  region.querySelector = (selector) => inside.get(selector) || null;
  const document = new RuntimeTarget(); document.hidden = hidden; document.body = { dataset: { lowerSurfaceState: modal ? 'modal' : 'ready' } }; document.querySelector = (selector) => ({ '[data-mobile-toast-region]': region, '[data-mobile-toast-status]': polite, '[data-mobile-toast-alert]': assertive }[selector] || null);
  const window = new RuntimeTarget(); window.setTimeout = clock.setTimeout; window.clearTimeout = clock.clearTimeout;
  const context = { window, document, performance: { now: () => now }, matchMedia: () => ({ matches: reducedMotion }), requestAnimationFrame: (callback) => callback(), setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout, AbortController, console };
  vm.runInNewContext(stripTypeScriptTypes(script, { mode: 'transform' }), context);
  return { window, document, region, toast, countdown, time, clock };
};

test('toast runtime does not extend passive lifetime on repeated ready state and preserves it through a modal', async () => {
  const runtime = await toastRuntime(); runtime.window.KenigEventsToast.show({ message: 'Сохранено', duration: 1000 }); runtime.clock.advance(300);
  for (let index = 0; index < 4; index += 1) runtime.window.dispatchEvent({ type: 'kenigevents:lower-surface-state', detail: { modalOpen: false } });
  runtime.clock.advance(699); assert.equal(runtime.region.hidden, false, 'the original timer remains active before its deadline'); runtime.clock.advance(1); assert.equal(runtime.region.hidden, true, 'repeat ready events must not re-arm a passive toast');
  runtime.window.KenigEventsToast.show({ message: 'Сохранено снова', duration: 1000 }); runtime.clock.advance(300); runtime.window.dispatchEvent({ type: 'kenigevents:lower-surface-state', detail: { modalOpen: true } }); assert.equal(runtime.region.classList.contains('is-modal-obscured'), true);
  runtime.clock.advance(2000); assert.equal(runtime.region.hidden, false, 'modal pause retains the passive toast'); runtime.window.dispatchEvent({ type: 'kenigevents:lower-surface-state', detail: { modalOpen: false } }); runtime.clock.advance(699); assert.equal(runtime.region.hidden, false); runtime.clock.advance(1); assert.equal(runtime.region.hidden, true, 'resume consumes only the pre-modal remaining time');
});
test('toast runtime synchronizes hidden/modal startup and gives reduced-motion users truthful remaining time', async () => {
  const startup = await toastRuntime({ hidden: true, modal: true }); startup.window.KenigEventsToast.show({ message: 'Фоновое', duration: 1000 }); startup.clock.advance(2000); assert.equal(startup.region.hidden, false, 'initial hidden/modal state pauses before the first toast');
  startup.document.hidden = false; startup.document.dispatchEvent({ type: 'visibilitychange' }); startup.window.dispatchEvent({ type: 'kenigevents:lower-surface-state', detail: { modalOpen: false } }); startup.clock.advance(1000); assert.equal(startup.region.hidden, true);
  const reduced = await toastRuntime({ reducedMotion: true }); reduced.window.KenigEventsToast.show({ message: 'Без анимации', duration: 1500 }); assert.equal(reduced.time.hidden, false); assert.equal(reduced.time.textContent, 'Ещё 2 с'); reduced.clock.advance(1000); assert.equal(reduced.time.textContent, 'Ещё 1 с', 'the owner timer updates reduced-motion remaining time'); reduced.clock.advance(500); assert.equal(reduced.region.hidden, true);
});
