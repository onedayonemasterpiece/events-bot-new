import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import {
  A0_CONSUMER_CLOSURE_PATHS,
  assertA0ConsumerPostconditions,
  transformA0Consumer,
} from '../scripts/apply-a0-current-successor-consumer-closure.mjs';

const read = (path) => readFile(new URL(`../../${path}`, import.meta.url), 'utf8');

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
    assert.equal(
      transformA0Consumer(path, transformed),
      transformed,
      `${path} transform is not idempotent`,
    );
  });
}

test('festival transform preserves category assets while centralizing action icons and targets', async () => {
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
  assert.doesNotMatch(source, /width:\s*1\.8rem;\s*height:\s*1\.8rem/u);
});

test('exhibitions transform changes only ownership labels and leaves behavior hooks intact', async () => {
  const source = transformA0Consumer(
    'site/src/components/ExhibitionsPersonalSurface.astro',
    await read('site/src/components/ExhibitionsPersonalSurface.astro'),
  );
  for (const hook of [
    'data-mode-switch',
    'data-category-filter',
    'data-keyboard-help',
    'data-gallery',
    'data-tail-toggle',
    'data-live-undo',
  ]) assert.match(source, new RegExp(hook, 'u'));
  assert.doesNotMatch(source, /--ex-[a-z]/u);
  assert.match(source, /var\(--ke-color-exhibitions-background\)/u);
  assert.match(source, /var\(--ke-exhibitions-motion-base\)/u);
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
  assert.match(hub, /readFocusParticipationMarker/u);
  assert.match(hub, /clearFocusParticipationMarker/u);
  assert.match(hub, /marker\?\.status === 'active' \? 'available' : 'locked'/u);
});
