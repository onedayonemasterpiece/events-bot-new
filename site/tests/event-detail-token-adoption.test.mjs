import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('desktop event detail consumes its established canonical ink and surface roles', async () => {
  const [desktop, eventDetailFoundations, surfaceFoundations, foundationBindings, baseDesignSystem, actionPanel] = await Promise.all([
    read('src/components/DesktopEventPage.astro'),
    read('src/components/design-system/event-detail-foundations.css'),
    read('src/components/design-system/surface-foundations.css'),
    read('src/components/design-system/foundations.css'),
    read('src/styles/design-system.css'),
    read('src/components/DesktopEventActionPanel.astro'),
  ]);

  assert.match(eventDetailFoundations, /--ke-color-event-detail-ink:\s*#241c17;/u);
  assert.match(eventDetailFoundations, /--ke-color-event-detail-surface:\s*var\(--ke-color-surface-warm\);/u);
  assert.match(surfaceFoundations, /--ke-color-surface-warm:\s*var\(--ke-color-background-header\);/u);
  assert.match(foundationBindings, /--ke-color-background-header:\s*var\(--ke-color-header\);/u);
  assert.match(baseDesignSystem, /--ke-color-header:\s*#fffaf2;/u);
  assert.match(eventDetailFoundations, /--ke-color-event-detail-action:\s*#b54d22;/u);

  assert.doesNotMatch(desktop, /--clean-(?:ink|paper|accent)\s*:\s*#/u);
  assert.doesNotMatch(desktop, /--clean-ink/u);
  assert.match(desktop, /--clean-paper:var\(--ke-color-event-detail-surface\)/u);
  assert.match(desktop, /--clean-accent:var\(--ke-color-event-detail-action\)/u);
  assert.match(desktop, /color:var\(--ke-color-event-detail-ink\)/u);
  assert.match(desktop, /background:var\(--ke-color-event-detail-surface\)/u);
  assert.match(actionPanel, /background: var\(--clean-accent, #b54d22\)/u,
    'removing the obsolete inherited alias retains the action panel’s exact fallback colour');
});
