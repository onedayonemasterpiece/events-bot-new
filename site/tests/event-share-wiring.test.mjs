import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = relative => readFile(new URL(`../${relative}`, import.meta.url), 'utf8');

test('every authored native share control exposes image semantics and the brand asset', async () => {
  const files = await Promise.all([
    'src/components/EventHero.astro',
    'src/components/DesktopEventActionPanel.astro',
    'src/components/EventCard.astro',
    'src/components/EventCtaPanel.astro',
  ].map(read));
  for (const source of files) {
    const controls = source.split('data-native-share').slice(1);
    assert.ok(controls.length > 0);
    for (const control of controls) {
      const buttonTail = control.split('</button>', 1)[0];
      assert.match(buttonTail, /data-share-image-text-mode=/u);
      assert.match(buttonTail, /data-share-brand-image=/u);
    }
  }
});

test('desktop share controls receive the semantics of their selected media', async () => {
  const page = await read('src/components/DesktopEventPage.astro');
  assert.equal((page.match(/<DesktopEventActionPanel /gu) || []).length, 3);
  assert.equal((page.match(/shareImageTextMode=\{selectedMediaMode\}/gu) || []).length, 3);
});
