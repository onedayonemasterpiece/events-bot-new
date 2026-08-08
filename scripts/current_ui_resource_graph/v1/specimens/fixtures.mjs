import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

export const FIXTURE_DELTA_ALLOWLIST = Object.freeze([
  'time_range_end', 'transport_end_basis', 'start_time', 'venue_name', 'city',
]);

export function loadPreviewEventCatalog(candidateSite) {
  const path = resolve(candidateSite, 'src/data/preview-events.json');
  const parsed = JSON.parse(readFileSync(path, 'utf8'));
  if (!Array.isArray(parsed.events) || !parsed.build?.generated_at) throw new Error('Invalid pinned PreviewEvent catalog');
  return parsed;
}

export function assertFixtureDelta(delta = {}) {
  if (!delta || Array.isArray(delta) || typeof delta !== 'object') throw new Error('Fixture delta must be an object');
  for (const key of Object.keys(delta)) {
    if (!FIXTURE_DELTA_ALLOWLIST.includes(key)) throw new Error(`Fixture delta field is not allowed: ${key}`);
  }
}

export function resolvePreviewEventFixture(catalog, fixtureRef, delta = {}) {
  if (!fixtureRef || fixtureRef.catalog !== 'preview-events' || !Number.isSafeInteger(fixtureRef.event_id)) {
    throw new Error('A concrete pinned PreviewEvent fixture_ref is required');
  }
  assertFixtureDelta(delta);
  const original = catalog.events.find((event) => event.id === fixtureRef.event_id);
  if (!original) throw new Error(`Pinned PreviewEvent is missing: ${fixtureRef.event_id}`);
  const event = structuredClone(original);
  Object.assign(event, structuredClone(delta));
  if (event.id !== original.id || event.slug !== original.slug || event.source_prod_id !== original.source_prod_id) {
    throw new Error('Controlled fixture delta changed event identity');
  }
  return {
    event,
    trace: {
      fixture_catalog: 'src/data/preview-events.json', event_id: original.id, source_prod_id: original.source_prod_id,
      slug_sha256: createHash('sha256').update(original.slug).digest('hex'), delta_fields: Object.keys(delta).sort(),
      exact_catalog_row_unchanged: Object.keys(delta).length === 0,
    },
  };
}
