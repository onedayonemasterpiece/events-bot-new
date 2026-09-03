import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('production route mounts reviewed keyboard navigation for immutable artifacts behind its flag', async () => {
  const route = await read('src/pages/sobytiya/[slug].astro');
  assert.match(route, /import KeyboardEventNavigation from '..\/..\/components\/KeyboardEventNavigation\.astro'/u);
  assert.match(route, /IS_PRODUCTION_FAMILY[\s\S]*PREVIEW_BUILD_ID !== 'local'[\s\S]*PUBLIC_KEYBOARD_EVENT_NAVIGATION_FORCE[\s\S]*PUBLIC_KEYBOARD_EVENT_NAVIGATION_ENABLED/u);
  assert.match(route, /\{keyboardEventNavigationEnabled && <KeyboardEventNavigation \/>\}/u);
  assert.doesNotMatch(route, /KeyboardEventNavigationPrototype/u);
});

test('named noindex previews cannot silently omit the event keyboard router', async () => {
  const [route, component] = await Promise.all([
    read('src/pages/sobytiya/[slug].astro'),
    read('src/components/KeyboardEventNavigationPrototype.astro'),
  ]);
  assert.match(route, /PREVIEW_BUILD_ID !== 'local'/u);
  assert.match(component, /data-keyboard-event-navigation-mounted/u);
  assert.match(component, /window\.KenigEventsKeyboardNavigation = initKeyboardEventNavigation\(\)/u);
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
  const [desktop, optimizedGrid] = await Promise.all([
    read('src/components/DesktopEventPage.astro'),
    read('src/components/OptimizedEventCardGrid.astro'),
  ]);
  const layout = await read('src/layouts/EventLayout.astro');
  assert.match(desktop, /data-related-start[\s\S]*<OptimizedEventCardGrid/u);
  assert.match(optimizedGrid, /discoverySrc=\{discoverySrc\}/u);
  assert.match(optimizedGrid, /discoveryFeed=\{!responsiveMobile\}/u);
  assert.match(optimizedGrid, /import AdaptiveEventCardGrid from '\.\/AdaptiveEventCardGrid\.astro'/u);
  assert.doesNotMatch(optimizedGrid, /data-discovery-feed=\{!responsiveMobile/u);
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
  assert.match(router, /\['KeyL', 'KeyK', 'KeyS', 'Enter'\]\.includes\(event\.code\)[\s\S]*bodyRecoveryArmed && bodyTarget/u);
  assert.match(router, /\['KeyC', 'KeyP'\]\.includes\(event\.code\)[\s\S]*bodyTarget[\s\S]*coldBodyHeroEntryArmed \|\| bodyRecoveryArmed \|\| galleryHandoffArmed/u);
  assert.match(router, /if \(event\.code === 'KeyC'\) void copyDescription\(\{ keyboard:true \}\);[\s\S]*else void copyPoster\(\{ keyboard:true \}\)/u);
  assert.doesNotMatch(router, /event\.key === ['"][сз]['"]/u, 'layout independence must stay on physical code, not Cyrillic key aliases');
  assert.match(router, /let coldBodyHeroEntryArmed = true/u);
  assert.match(router, /inertCurrentEventPointer[\s\S]*bodyRecoveryArmed = true/u);
  assert.match(router, /\['KeyL', 'KeyK', 'KeyS'\]\.includes\(event\.code\)/u);
  assert.match(router, /event\.isComposing/u);
  assert.doesNotMatch(router, /surface\.focus\(\{ preventScroll: true \}\);\s*\n\s*\}/u, 'module must not end with prototype autofocus');
});

test('keyboard visual feedback delegates to the one layout toast while SR status remains local', async () => {
  const prototype = await read('src/components/KeyboardEventNavigationPrototype.astro');
  const router = await read('src/lib/keyboardEventNavigation.mjs');
  const layout = await read('src/layouts/EventLayout.astro');
  assert.match(prototype, /data-keyboard-prototype-status[\s\S]*role="status"[\s\S]*aria-live="polite"/u);
  assert.doesNotMatch(prototype, /data-keyboard-action-toast|keyboard-action-toast/u);
  assert.match(router, /KenigEventsToast\?\.show/u);
  assert.match(router, /kenigevents:toast/u);
  assert.match(router, /announce: false/u);
  assert.equal((layout.match(/<MobileToastRegion\s*\/>/gu) || []).length, 1);
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

test('dynamic continuation destination owns its K hint before keyboard focus moves', async () => {
  const router = await read('src/lib/keyboardEventNavigation.mjs');
  assert.match(router, /const enhanceManagedCard = \(card\) => \{[\s\S]*calendar\.append\(keycap\);[\s\S]*\};\s*\n\s*const enhanceManagedCards/u);
  assert.match(
    router,
    /const focusCard = \(card\) => \{[\s\S]*enhanceManagedCard\(card\);\s*\n\s*updateShortcutHintVisibility\(\);\s*\n\s*card\.focus\(\{ preventScroll: true \}\)/u,
    'ArrowDown may focus a freshly injected continuation card before its MutationObserver runs',
  );
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
