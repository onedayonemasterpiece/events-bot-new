import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const source = await readFile(new URL('../src/components/InterestProfile.astro', import.meta.url), 'utf8');

test('interest profile exposes one foundation-bound family root and canonical lock identity', () => {
  assert.match(source, /import SemanticIcon from '\.\/design-system\/SemanticIcon\.astro'/u);
  assert.match(source, /import '\.\/design-system\/product-contour-foundations\.css'/u);
  assert.match(source, /data-ds-family="InterestProfile"/u);
  assert.match(source, /data-ds-version="1"/u);
  assert.match(source, /data-ds-variant="local-explicit-signals"/u);
  assert.match(source, /data-ds-state="consent"/u);
  assert.match(source, /data-ke-foundation-consumer="interest-profile"/u);
  assert.match(source, /<SemanticIcon name="lock" role="action" \/>/u);
  assert.doesNotMatch(source, /<svg\b/u, 'consumer must not own an inline lock glyph');
});

test('interest profile preserves explicit tri-state, evidence and digest behavior', () => {
  assert.match(source, /FOCUS_STORAGE_KEY/u);
  assert.match(source, /localStorage\.getItem\(storageKey\)/u);
  assert.match(source, /localStorage\.setItem\(storageKey, JSON\.stringify\(state\)\)/u);
  assert.match(source, /value="like" data-focus-stance-input/u);
  assert.match(source, /value="neutral" data-focus-stance-input checked/u);
  assert.match(source, /value="not-for-me" data-focus-stance-input/u);
  assert.match(source, /<meter min="0" max="100" value="50"/u);
  assert.match(source, /Отсутствие реакции не считается неприязнью/u);
  assert.match(source, /const explicitScore/u);
  assert.match(source, /const recommendationScore/u);
  assert.match(source, /explicitCount >= 3 && hasPositive && reactionCount >= 3/u);
  assert.match(source, /state\.digestOptIn = !state\.digestOptIn/u);
  assert.match(source, /localStorage\.removeItem\(storageKey\)/u);
});

test('interest profile diagnostics track consent and workspace states', () => {
  assert.match(source, /root\.dataset\.dsState = 'workspace'/u);
  assert.match(source, /root\.dataset\.dsState = 'consent'/u);
  assert.match(source, /consent\.hidden = true/u);
  assert.match(source, /workspace\.hidden = false/u);
  assert.match(source, /workspace\.hidden = true/u);
  assert.match(source, /consent\.hidden = false/u);
});

test('interest profile consumes the published personalization theme and continuity clusters', () => {
  for (const token of [
    '--ke-color-personalization-ink',
    '--ke-color-personalization-paper',
    '--ke-color-personalization-teal',
    '--ke-color-personalization-line',
    '--ke-color-personalization-workspace-start',
    '--ke-color-personalization-workspace-end',
    '--ke-elevation-personalization-panel',
    '--ke-personalization-consent-padding',
    '--ke-personalization-section-padding',
    '--ke-personalization-interest-column-title',
    '--ke-personalization-meter-height',
    '--ke-personalization-map-row-label-min',
    '--ke-personalization-digest-column-panel',
    '--ke-personalization-recommendation-gap',
    '--ke-personalization-consent-icon-size',
  ]) assert.match(source, new RegExp(token, 'u'));

  assert.doesNotMatch(source, /--focus-[a-z0-9-]+\s*:/iu, 'consumer must not recreate a local palette registry');
  assert.doesNotMatch(source, /--ke-personalization-receipt-gap/u, 'undefined token references are forbidden');
  assert.doesNotMatch(source, /#0f766e|#0b5f5a|#a54821|#fffdf8|#eee2d4|#f0ebe4|#f6f1e9|#d8cbbd|#f7eee6|#0f5f5a/u,
    'canonical visible palette values must remain in F0 foundations');
});
