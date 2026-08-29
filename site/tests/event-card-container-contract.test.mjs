import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { packRelatedCardRows } from '../src/lib/relatedCardLayout.mjs';

const optimizedGrid = await readFile(new URL('../src/components/OptimizedEventCardGrid.astro', import.meta.url), 'utf8');

test('the free collection uses the central EventCard row owner and preserves all exact cards', () => {
  const event = (id, width, height, imageTextMode, safeCrop = false) => ({
    id,
    image_url: `https://static.kenigevents.ru/test/${id}.webp`,
    image_text_mode: imageTextMode,
    safe_crop: safeCrop,
    image_assets: [{
      src: `https://static.kenigevents.ru/test/${id}.webp`, width, height,
      image_text_mode: imageTextMode,
      media_role: imageTextMode === 'visual_only' ? 'event_photo' : 'event_identity_poster',
      media_semantic_status: 'classified', safe_crop: safeCrop,
    }],
  });
  const packed = packRelatedCardRows([
    event(2182, 1280, 853, 'visual_only', true),
    event(6711, 1280, 960, 'visual_only', true),
    event(7609, 1254, 1254, 'ocr_text'),
  ], { limit:3, rowSize:3, mediaTreatment:'hybrid', preserveAll:true });

  assert.deepEqual(new Set(packed.map(({ item }) => item.id)), new Set([2182, 6711, 7609]));
  assert.equal(packed.length, 3);
  assert.match(optimizedGrid, /import EventCard from '\.\/EventCard\.astro'/u);
  assert.match(optimizedGrid, /preserveAll:\s*true/u);
  assert.match(optimizedGrid, /data-optimized-event-card-row/u);
  assert.match(optimizedGrid, /grid-template-columns:\s*repeat\(var\(--row-card-count\),\s*minmax\(0,\s*1fr\)\)/u);
  assert.doesNotMatch(optimizedGrid, /page-local|FreeCollectionEventCard/u);
});
