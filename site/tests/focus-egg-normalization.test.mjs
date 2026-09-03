import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('focus egg components expose distinct family and runtime states', async () => {
  const [artifact, card, demo] = await Promise.all([
    read('src/components/FocusEggArtifact.astro'),
    read('src/components/FocusEggCollectionCard.astro'),
    read('src/components/FocusEggSavedListDemo.astro'),
  ]);

  assert.match(artifact, /data-ds-family="FocusEggArtifact"/u);
  assert.match(artifact, /data-ds-state=\{state\}/u);
  assert.match(artifact, /data-ke-foundation-consumer="focus-egg-artifact"/u);
  assert.match(card, /data-ds-family="FocusEggCollectionCard"/u);
  assert.match(card, /data-ds-state=\{state\}/u);
  assert.match(card, /data-ke-foundation-consumer="focus-egg-collection-card"/u);
  assert.match(demo, /data-ds-family="FocusEggSavedListDemo"/u);
  assert.match(demo, /data-ds-state="insufficient-items"/u);
  assert.match(demo, /data-ke-foundation-consumer="focus-egg-saved-list-demo"/u);
  assert.match(demo, /root\.dataset\.dsState = placement \? placement\.state : 'insufficient-items'/u);
  assert.match(demo, /root\.dataset\.dsState = 'found'/u);
  assert.match(demo, /artifact\.dataset\.dsState = placement\.state/u);
  assert.match(demo, /artifact\.dataset\.dsState = 'found'/u);
});

test('focus egg behavior remains deterministic and browser-local', async () => {
  const demo = await read('src/components/FocusEggSavedListDemo.astro');
  assert.match(demo, /getFgE12Placement/u);
  assert.match(demo, /visibleIds/u);
  assert.match(demo, /index === 2/u);
  assert.match(demo, /readFocusEggPrototypeState\(storage\)/u);
  assert.match(demo, /markFocusEggFound/u);
  assert.match(demo, /storeFocusEggPrototypeState/u);
  assert.match(demo, /focus-egg-found/u);
  assert.doesNotMatch(demo, /\bfetch\s*\(|\/rpc\/|supabase/iu);
});

test('focus egg roots consume shared surface, focus and touch roles without pretending artifact glyphs are action icons', async () => {
  const [artifact, card, demo] = await Promise.all([
    read('src/components/FocusEggArtifact.astro'),
    read('src/components/FocusEggCollectionCard.astro'),
    read('src/components/FocusEggSavedListDemo.astro'),
  ]);
  for (const source of [artifact, card, demo]) {
    assert.match(source, /product-contour-foundations\.css/u);
    assert.match(source, /--ke-color-border-default|--ke-color-background-page/u);
  }
  assert.match(artifact, /--ke-size-touch-target/u);
  assert.match(artifact, /--ke-color-focus-ring-accent/u);
  assert.match(card, /--ke-elevation-card/u);
  assert.match(demo, /--ke-color-status-info-background/u);
  assert.match(demo, /--ke-color-surface-brand-selected/u);
  assert.doesNotMatch([artifact, card, demo].join('\n'), /data-ke-icon-role/u,
    'decorative artifact-state glyphs are not canonical action-icon roles');
});
