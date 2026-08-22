import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import test from 'node:test';
import { collapseLinkedSessions, expectedSurfaceOrder, extractSurfaceMarkers, isExhibitionLike, runPlacementVerification } from '../scripts/ui_conformance/verify-surface-placement.mjs';

test('L0 oracle follows semantic exhibition, chronological and linked-session rules', () => {
  const events = [
    { id: 3, slug: 'exhibition-3', event_type: 'выставка', topics: [], start_date: '2026-08-22', starts_at: null, other_date_ids: [] },
    { id: 2, slug: 'late-2', event_type: 'концерт', topics: [], start_date: '2026-08-22', starts_at: '2026-08-22T18:00:00+02:00', other_date_ids: [20] },
    { id: 1, slug: 'early-1', event_type: 'лекция', topics: [], start_date: '2026-08-22', starts_at: '2026-08-22T12:00:00+02:00', other_date_ids: [] },
    { id: 20, slug: 'linked-20', event_type: 'концерт', topics: [], start_date: '2026-08-22', starts_at: '2026-08-22T19:00:00+02:00', other_date_ids: [2] },
  ];
  assert.equal(isExhibitionLike(events[0]), true);
  assert.deepEqual(collapseLinkedSessions(events.slice(1)).map((event) => event.id), [1, 2]);
  assert.deepEqual(expectedSurfaceOrder(events, 'tomorrow', '/zavtra/', { current_date: '2026-08-21' }), [1, 2]);
});

test('L0 marker extractor reads component and state evidence from generated HTML', () => {
  const html = '<article data-ds-component="ListingEventCard" data-event-id="42" data-listing-density="regular" data-listing-media-treatment="cover"></article>';
  assert.deepEqual(extractSurfaceMarkers(html, 'tomorrow'), [{ id: 42, component: 'listing.event-card', state: 'density=regular;media=cover' }]);
});

test('real Astro surface placement matches Golden Corpus v1 when UI_REFERENCE_CORPUS_ROOT is supplied', { skip: !process.env.UI_REFERENCE_CORPUS_ROOT }, async () => {
  const harness = mkdtempSync(join(tmpdir(), 'events-l0-placement-'));
  try {
    const report = await runPlacementVerification({ corpusRoot: resolve(process.env.UI_REFERENCE_CORPUS_ROOT), site: resolve('site'), harness, nodeModules: process.env.UI_REFERENCE_NODE_MODULES });
    assert.equal(report.status, 'PASS', JSON.stringify(report, null, 2));
    assert.equal(report.production_source_mutated, false);
    assert.equal(report.summary.declared_gaps, 6);
  } finally { rmSync(harness, { recursive: true, force: true }); }
});
