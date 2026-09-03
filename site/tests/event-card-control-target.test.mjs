import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');

test('EventCard activates its canonical 44px control-size binding', async () => {
  const [card, styles] = await Promise.all([
    read('src/components/EventCard.astro'),
    read('src/components/event-card.css'),
  ]);

  assert.match(card, /import '\.\/event-card\.css';/u);
  assert.match(card, /data-ds-component="EventCard"/u);
  assert.match(card, /data-ds-family="EventCard"/u);
  assert.match(card, /event-card__utility-row[\s\S]*feedback-button--negative/u);
  assert.match(
    styles,
    /\[data-ds-component="EventCard"\]\.event-card--split-actions\s+\.event-card__utility-row\s+\.feedback-button--negative\s*\{\s*min-height:\s*var\(--ke-control-min,\s*44px\);\s*\}/u,
  );
  assert.doesNotMatch(
    styles,
    /min-height:\s*(?:[0-3]?\d(?:\.\d+)?)px/u,
    'the M0-owned EventCard action selector must not contain a literal target below 40px',
  );
});

test('EventCard owns the action binding without reading route or foundation implementations', async () => {
  const source = await read('src/components/event-card.css');

  assert.match(source, /min-height:\s*var\(--ke-control-min,\s*44px\)/u);
  assert.doesNotMatch(source, /EventLayout|design-system\.css/u);
});

test('all EventCard action glyphs consume the central action icon role', async () => {
  const card = await read('src/components/EventCard.astro');

  for (const name of ['dislike', 'share', 'heart', 'calendar']) {
    assert.match(card, new RegExp(`<SemanticIcon name="${name}" role="action" \\/>`, 'u'));
  }
  assert.doesNotMatch(card, /<Icon\b/u);
});
