import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');

test('EventCard activates its canonical control-size binding', async () => {
  const [card, styles] = await Promise.all([
    read('src/components/EventCard.astro'),
    read('src/components/event-card.css'),
  ]);

  assert.match(card, /import '\.\/event-card\.css';/u);
  assert.match(card, /data-ds-component="EventCard"/u);
  assert.match(card, /event-card__utility-row[\s\S]*feedback-button--negative/u);
  assert.match(
    styles,
    /\[data-ds-component="EventCard"\]\.event-card--split-actions\s+\.event-card__utility-row\s+\.feedback-button--negative\s*\{\s*min-height:\s*var\(--ke-control-min,\s*44px\);\s*\}/u,
  );
});

test('the V0 36px regression is overridden by the canonical EventCard selector', async () => {
  const [styles, layout, foundations] = await Promise.all([
    read('src/components/event-card.css'),
    read('src/layouts/EventLayout.astro'),
    read('src/styles/design-system.css'),
  ]);

  assert.match(foundations, /--ke-control-min:\s*44px/u);
  assert.match(styles, /min-height:\s*var\(--ke-control-min,\s*44px\)/u);

  const legacyCompactRule = /\.event-card--split-actions \.event-card__utility-row \.feedback-button--negative\s*\{[^}]*min-height:\s*36px[^}]*\}/u;
  if (legacyCompactRule.test(layout)) {
    assert.match(layout, legacyCompactRule, 'current A0 removal boundary is the route-local 36px override');
    assert.match(styles, /\[data-ds-component="EventCard"\]\.event-card--split-actions/u,
      'the canonical selector must remain more specific while the legacy rule exists');
  }
});

test('all EventCard action glyphs consume the central action icon role', async () => {
  const card = await read('src/components/EventCard.astro');

  for (const name of ['dislike', 'share', 'heart', 'calendar']) {
    assert.match(card, new RegExp(`<SemanticIcon name="${name}" role="action" \\/>`, 'u'));
  }
  assert.doesNotMatch(card, /<Icon\b/u);
});
