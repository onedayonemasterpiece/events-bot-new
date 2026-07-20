import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('production route mounts reviewed keyboard navigation only on secret candidates behind its flag', async () => {
  const route = await read('src/pages/sobytiya/[slug].astro');
  assert.match(route, /import KeyboardEventNavigation from '..\/..\/components\/KeyboardEventNavigation\.astro'/u);
  assert.match(route, /IS_SECRET_CANDIDATE\s*&&\s*String\(import\.meta\.env\.PUBLIC_KEYBOARD_EVENT_NAVIGATION_ENABLED \|\| '1'\) !== '0'/u);
  assert.match(route, /\{keyboardEventNavigationEnabled && <KeyboardEventNavigation \/>\}/u);
  assert.doesNotMatch(route, /KeyboardEventNavigationPrototype/u);
});

test('prototype-only lab route is not shipped by either production artifact profile', async () => {
  await assert.rejects(read('src/pages/lab/keyboard-event-navigation/index.astro'), /ENOENT/u);
  const productionCheck = await read('scripts/check-production.mjs');
  const secretCheck = await read('scripts/check-secret-candidate.mjs');
  assert.match(productionCheck, /preview\/fixture route leaked/u);
  assert.match(secretCheck, /QA route leaked/u);
  assert.doesNotMatch(secretCheck, /keyboard-event-navigation/u);
});

test('desktop similar cards use the same discovery controller and broad hydration survives a resize race', async () => {
  const desktop = await read('src/components/DesktopEventPage.astro');
  const layout = await read('src/layouts/EventLayout.astro');
  assert.match(desktop, /data-related-start[\s\S]*data-discovery-feed[\s\S]*data-discovery-src/u);
  assert.match(layout, /personalFeedHydrationInFlight = new WeakSet\(\)/u);
  assert.match(layout, /if \(personalFeedSectionCanHydrate\(section\)\) personalFeedReached\.add\(section\)/u);
  assert.doesNotMatch(layout, /personalFeedReached\.add\(section\);\s*\n\s*hydratePersonalFeedSlots/u);
});

test('prototype and production share the exact extracted V7 router', async () => {
  const prototype = await read('src/components/KeyboardEventNavigationPrototype.astro');
  const production = await read('src/components/KeyboardEventNavigation.astro');
  const router = await read('src/lib/keyboardEventNavigation.mjs');
  assert.match(prototype, /import \{ initKeyboardEventNavigation \} from '..\/lib\/keyboardEventNavigation\.mjs'/u);
  assert.match(production, /<KeyboardEventNavigationPrototype showQuickstart=\{false\} \/>/u);
  assert.match(router, /Source contract: d0027a53/u);
  assert.match(router, /export function initKeyboardEventNavigation\(options = \{\}\)/u);
  assert.match(router, /gallery_close_down/u);
  assert.match(router, /downGesture\?\.released && event\.timeStamp - downGesture\.at <= 430/u);
  assert.match(router, /event\.code === 'KeyL' && !event\.repeat && bodyRecoveryArmed/u);
  assert.match(router, /\['KeyL', 'KeyK', 'KeyS'\]\.includes\(event\.code\)/u);
  assert.match(router, /event\.isComposing/u);
  assert.doesNotMatch(router, /surface\.focus\(\{ preventScroll: true \}\);\s*\n\s*\}/u, 'module must not end with prototype autofocus');
});

test('router has reversible lifecycle and disarms lost-focus provenance on page lifecycle loss', async () => {
  const router = await read('src/lib/keyboardEventNavigation.mjs');
  assert.match(router, /new win\.AbortController\(\)/u);
  assert.match(router, /\{ \.\.\.init, signal \}/u);
  assert.match(router, /observers\.forEach\(\(observer\) => observer\.disconnect\(\)\)/u);
  assert.match(router, /abortController\.abort\(\)/u);
  assert.match(router, /timeouts\.forEach\(\(id\) => win\.clearTimeout\(id\)\)/u);
  assert.match(router, /frames\.forEach\(\(id\) => win\.cancelAnimationFrame\(id\)\)/u);
  assert.match(router, /listen\(win, 'blur',[\s\S]*bodyRecoveryArmed = false/u);
  assert.match(router, /listen\(doc, 'visibilitychange',[\s\S]*bodyRecoveryArmed = false/u);
  assert.match(router, /pendingConsentOwner = \{ owner, opener: action \}/u);
  assert.match(router, /captureVisibleConsent\(\);\s*\n\s*\}\)\.observe\(doc\.body/u,
    'production consent may appear asynchronously after the real feedback controller yields');
  assert.doesNotMatch(router, /pendingConsentOwner\?\.opener === action\) pendingConsentOwner = null;\s*\n\s*\}, 2000/u,
    'consent ownership must be transition-driven, not discarded by a fixed timeout');
  assert.match(router, /const attributeSnapshots = new Map\(\)/u);
  assert.match(router, /restoreManagedAttributes\(\)/u);
  assert.match(router, /snapshot\.kind !== 'card' \|\| snapshot\.zone !== 'related'/u);
  assert.match(router, /win\.cancelAnimationFrame\(relatedRestoreFrame\)[\s\S]*resolveLogicalOwner\(snapshot\)/u,
    'shared discovery reorders must restore the related card that owned focus');
  assert.match(router, /return \{ destroy, get active\(\)/u);
});

test('browser regression wrapper is engine-configurable and asserts no autofocus', async () => {
  const gate = await read('scripts/check-keyboard-event-navigation-playwright.sh');
  assert.match(gate, /STATIC_SITE_PLAYWRIGHT_BROWSER:-chromium/u);
  assert.match(gate, /open --browser="\$BROWSER"/u);
  assert.match(gate, /must not autofocus the current-event CTA surface/u);
  assert.match(gate, /key: "з"/u, 'Cyrillic physical KeyP remains covered');
  assert.match(gate, /key: "ы"/u, 'Cyrillic physical KeyS remains covered');
  assert.match(gate, /Gallery ArrowDown must close without scrolling/u);
});
