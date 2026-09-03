import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import {
  A0_CONSUMER_CLOSURE_PATHS,
  EXHIBITIONS_PRIVATE_THEME_ALIASES,
  EXHIBITIONS_REQUIRED_CENTRAL_BINDINGS,
  EXHIBITIONS_RUNTIME_VARIABLES,
  assertA0ConsumerPostconditions,
  transformA0Consumer,
} from '../scripts/apply-a0-current-successor-consumer-closure.mjs';

const read = (path) => readFile(new URL(`../../${path}`, import.meta.url), 'utf8');
const customProperties = (source, prefix) => [...new Set(
  [...source.matchAll(new RegExp(`${prefix}[a-z0-9-]+`, 'gu'))].map((match) => match[0]),
)].sort();

const expectedPaths = [
  'site/src/components/InterestClubCard.astro',
  'site/src/pages/kluby-po-interesam/[slug]/index.astro',
  'site/src/pages/festivali/index.astro',
  'site/src/components/ExhibitionsPersonalSurface.astro',
  'site/src/pages/fokus-gruppa/kollektsiya/index.astro',
  'site/src/pages/zakrytaya-afisha/index.astro',
];

test('A0 closure targets only actual consumers and no F0, FR0 or M0 canonical root', () => {
  assert.deepEqual(A0_CONSUMER_CLOSURE_PATHS, expectedPaths);
  const forbiddenCanonicalRoots = [
    'site/src/components/design-system/',
    'site/src/components/media-frame.css',
    'site/src/components/EventMediaRail.astro',
    'site/src/components/EventHero.astro',
    'site/src/components/EventCard.astro',
    'site/src/components/listings/ListingEventCard.astro',
    'site/src/components/AdaptiveEventCardGrid.astro',
  ];
  for (const path of A0_CONSUMER_CLOSURE_PATHS) {
    assert.ok(
      forbiddenCanonicalRoots.every((forbidden) => !path.startsWith(forbidden)),
      `${path} crosses the F0/FR0/M0 ownership boundary`,
    );
  }
});

for (const path of expectedPaths) {
  test(`${path} has an idempotent, postcondition-complete A0 transform`, async () => {
    const current = await read(path);
    const transformed = transformA0Consumer(path, current);
    assertA0ConsumerPostconditions(path, transformed);
    assert.equal(transformA0Consumer(path, transformed), transformed, `${path} transform is not idempotent`);
  });
}

test('canonical entrypoint exposes the exact F0 inspection vocabulary without implementing a second transform', async () => {
  const source = await read('site/scripts/apply-a0-current-successor-consumer-closure.mjs');
  assert.match(source, /a0-current-successor-consumer-closure-lib\.mjs/u);
  assert.match(source, /--ke-color-festival-guide-like-surface/u);
  assert.match(source, /--ke-color-festival-category-surface/u);
  for (const variable of EXHIBITIONS_RUNTIME_VARIABLES) assert.match(source, new RegExp(variable, 'u'));
  for (const token of [
    '--ke-exhibitions-signal-icon-size',
    '--ke-exhibitions-action-icon-size',
    '--ke-exhibitions-gallery-arrow-icon-size',
  ]) assert.match(source, new RegExp(token, 'u'));
  assert.match(source, /<SemanticIcon name="arrow-left" role="control" \/>/u);
  assert.match(source, /<SemanticIcon name="arrow-right" role="control" \/>/u);
  assert.doesNotMatch(source, /assert\.ok\(!source\.includes\('--ex-'\)\)/u);
  assert.doesNotMatch(
    source,
    /replaceAllIfPresent\(source, 'background: rgba\(165, 72, 33, 0\.1\);', 'background: var\(--ke-color-festival-guide-like-surface\);'\)/u,
  );
});

test('festival transform preserves taxonomy assets and separates equal-color semantic owners', async () => {
  const source = transformA0Consumer(
    'site/src/pages/festivali/index.astro',
    await read('site/src/pages/festivali/index.astro'),
  );
  assert.match(source, /festivalCategoryIcons/u);
  assert.match(source, /festival-categories/u);
  assert.match(source, /--festival-category-icon/u);
  assert.match(source, /<SemanticIcon name="link" role="control" \/>/u);
  assert.match(source, /<SemanticIcon name="calendar" role="control" \/>/u);
  assert.equal((source.match(/<SemanticIcon name="heart" role="control" \/>/gu) || []).length, 2);
  assert.doesNotMatch(source, /<Icon name="heart" \/>/u);
  assert.match(
    source,
    /\.festival-guide__icon--heart \{[\s\S]*?background: var\(--ke-color-festival-guide-like-surface\);[\s\S]*?\n  \}/u,
  );
  assert.match(
    source,
    /\.festival-month__categories li \{[\s\S]*?background: var\(--ke-color-festival-category-surface\);[\s\S]*?\n  \}/u,
  );
  assert.doesNotMatch(
    source,
    /\.festival-month__categories li \{[\s\S]*?background: var\(--ke-color-festival-guide-like-surface\);[\s\S]*?\n  \}/u,
  );
  assert.doesNotMatch(source, /\.festival-guide__icon :global\(svg\) \{ width: 0\.95rem; height: 0\.95rem/u);
  assert.doesNotMatch(source, /\.festival-card__like :global\(svg\) \{ width: 1\.12rem; height: 1\.12rem/u);
  assert.doesNotMatch(source, /width:\s*1\.8rem;\s*height:\s*1\.8rem/u);
});

test('exhibitions transform removes only private theme aliases and preserves the exact runtime layout variables', async () => {
  const source = transformA0Consumer(
    'site/src/components/ExhibitionsPersonalSurface.astro',
    await read('site/src/components/ExhibitionsPersonalSurface.astro'),
  );
  assert.deepEqual(customProperties(source, '--ex-'), [...EXHIBITIONS_RUNTIME_VARIABLES].sort());
  for (const alias of EXHIBITIONS_PRIVATE_THEME_ALIASES) {
    assert.doesNotMatch(source, new RegExp(`${alias}:`, 'u'));
    assert.doesNotMatch(source, new RegExp(`var\\(${alias}\\)`, 'u'));
  }
  for (const variable of EXHIBITIONS_RUNTIME_VARIABLES) assert.match(source, new RegExp(variable, 'u'));
  assert.match(source, /--ex-media-column:clamp/u);
  assert.match(source, /--ex-row-gap:clamp/u);
  assert.match(source, /--ex-surface-start:/u);
  assert.match(source, /--ex-rail-color:/u);
});

test('exhibitions transform consumes every strengthened F0 binding and central icon role', async () => {
  const source = transformA0Consumer(
    'site/src/components/ExhibitionsPersonalSurface.astro',
    await read('site/src/components/ExhibitionsPersonalSurface.astro'),
  );
  for (const token of EXHIBITIONS_REQUIRED_CENTRAL_BINDINGS) {
    assert.match(source, new RegExp(`var\\(${token}\\)`, 'u'), `missing ${token}`);
  }
  assert.match(source, /<SemanticIcon name="arrow-left" role="control" \/>/u);
  assert.match(source, /<SemanticIcon name="arrow-right" role="control" \/>/u);
  assert.doesNotMatch(source, /<span aria-hidden="true">←<\/span>/u);
  assert.doesNotMatch(source, /<span aria-hidden="true">→<\/span>/u);
  for (const selector of [
    '.ex-discussed svg',
    '.ex-signal svg',
    '.ex-action svg',
    '.ex-action--like svg',
    '.ex-action--reject svg',
  ]) {
    const escaped = selector.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&');
    const block = source.match(new RegExp(`${escaped} \\{([^}]*)\\}`, 'u'))?.[1] || '';
    assert.ok(block, `${selector} block missing`);
    assert.doesNotMatch(block, /(?:width|height):\s*(?:14|18|19|21)px/u);
  }
  assert.match(source, /\.ex-discussed svg \{[^}]*var\(--ke-exhibitions-signal-icon-size\)/u);
  assert.match(source, /\.ex-action svg \{[^}]*var\(--ke-exhibitions-action-icon-size\)/u);
  assert.match(source, /\.ex-gallery__stage button \{[^}]*var\(--ke-exhibitions-gallery-arrow-icon-size\)/u);
});

test('exhibitions transform preserves all accepted behavior hooks', async () => {
  const source = transformA0Consumer(
    'site/src/components/ExhibitionsPersonalSurface.astro',
    await read('site/src/components/ExhibitionsPersonalSurface.astro'),
  );
  for (const hook of [
    'data-mode-switch',
    'data-category-filter',
    'data-keyboard-help',
    'data-gallery',
    'data-gallery-prev',
    'data-gallery-next',
    'data-tail-toggle',
    'data-live-undo',
    'data-row-focus',
    'data-exhibition-row',
  ]) assert.match(source, new RegExp(hook, 'u'));
});

test('focus route transforms publish runtime state without changing participation semantics', async () => {
  const collection = transformA0Consumer(
    'site/src/pages/fokus-gruppa/kollektsiya/index.astro',
    await read('site/src/pages/fokus-gruppa/kollektsiya/index.astro'),
  );
  const hub = transformA0Consumer(
    'site/src/pages/zakrytaya-afisha/index.astro',
    await read('site/src/pages/zakrytaya-afisha/index.astro'),
  );
  assert.match(collection, /readFocusEggPrototypeState/u);
  assert.match(collection, /focus-egg-found/u);
  assert.match(collection, /root\.dataset\.dsState = `found-\$\{found\}-of-\$\{eligible\}`;/u);
  assert.match(hub, /readFocusParticipationMarker/u);
  assert.match(hub, /clearFocusParticipationMarker/u);
  assert.match(hub, /marker\?\.status === 'active' \? 'available' : 'locked'/u);
});
