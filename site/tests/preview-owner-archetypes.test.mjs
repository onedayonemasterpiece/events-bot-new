import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (relativePath) => readFile(new URL(`../${relativePath}`, import.meta.url), 'utf8');

test('Preview hub exposes a materialized arbitrary DateListingSurface representative', async () => {
  const hub = await read('src/pages/[preview]/index.astro');
  const checker = await read('scripts/check-unified-prototype.mjs');

  assert.match(hub, /getStaticEventDateAvailability/u);
  assert.match(hub, /const currentDate = getCurrentDate\(\)/u);
  assert.match(hub, /const arbitraryDate =/u);
  assert.match(hub, /id: 'date', href: `\/date-\$\{arbitraryDate\}\//u);
  assert.match(hub, /representativeLink\('date'\)/u);

  assert.match(checker, /'date'/u);
  assert.match(checker, /link\.id === 'date'/u);
  assert.match(checker, /previewManifest\.currentDate/u);
  assert.match(checker, /data-ds-family="DateListingSurface"/u);
  assert.match(checker, /data-ds-variant="date"/u);
});
