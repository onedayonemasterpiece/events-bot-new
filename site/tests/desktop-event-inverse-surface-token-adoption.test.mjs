import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('desktop event inverse surfaces consume canonical background roles', async () => {
  const [desktopEvent, foundations, primitives] = await Promise.all([
    read('src/components/DesktopEventPage.astro'),
    read('src/components/design-system/foundations.css'),
    read('src/styles/design-system.css'),
  ]);

  assert.match(primitives, /--ke-color-surface-inverse:\s*#24211f/u);
  assert.match(primitives, /--ke-color-surface-inverse-raised:\s*#292521/u);
  assert.match(foundations, /--ke-color-background-inverse:\s*var\(--ke-color-surface-inverse\)/u);
  assert.match(foundations, /--ke-color-background-inverse-raised:\s*var\(--ke-color-surface-inverse-raised\)/u);
  assert.match(desktopEvent, /background:var\(--ke-color-background-inverse\)/u);
  assert.ok(
    (desktopEvent.match(/background:var\(--ke-color-background-inverse-raised\)/gu) || []).length >= 7,
    'all raised inverse desktop event surfaces consume the shared role',
  );
  assert.doesNotMatch(desktopEvent, /#(?:24211f|292521)\b/iu);
});
