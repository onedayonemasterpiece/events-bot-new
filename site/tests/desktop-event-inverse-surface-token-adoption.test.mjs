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


test('graphite related-card share action keeps the canonical inverse foreground', async () => {
  const desktopEvent = await read('src/components/DesktopEventPage.astro');
  assert.match(desktopEvent, /:global\(\[data-desktop-clean-event\] \.desktop-clean-related__grid \.event-card--split-actions \.event-card__feedback--under \.feedback-button--share\) \{ color:var\(--ke-color-text-inverse\); \}/u);
  assert.match(desktopEvent, /feedback-button--share:hover\),[\s\S]*?color:var\(--ke-color-text-inverse\);/u);
});

// AdaptiveEventCardGrid owns a different Astro scope; parent scoped selectors
// cannot reach the className passed to that component. Keep the unique root
// boundary inside :global as well, not just the descendant button.
test('inverse action rule crosses the child component scope at its owned root', async () => {
  const source = await read('src/components/DesktopEventPage.astro');
  assert.doesNotMatch(source, /\[data-desktop-clean-event\] \.desktop-clean-related__grid :global\(/u);
});
