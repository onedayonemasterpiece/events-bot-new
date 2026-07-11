import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const dataDir = resolve(new URL('../src/data', import.meta.url).pathname);
const readJson = (name) => JSON.parse(readFileSync(resolve(dataDir, name), 'utf8'));
const routes = readJson('busRouteDirectory.json');
const access = readJson('busVenueAccess.json');
const schedules = readJson('busTransportSchedules.json');

function indexUnique(rows, label) {
  const index = new Map();
  for (const row of rows) {
    if (!row?.id || index.has(row.id)) throw new Error(`${label} has a missing/duplicate id: ${row?.id}`);
    index.set(row.id, row);
  }
  return index;
}

const localities = indexUnique(routes.localities, 'localities');
const corridors = indexUnique(routes.route_corridors, 'route_corridors');
const stops = indexUnique(access.stops, 'stops');
const venues = indexUnique(access.venues, 'venues');

const activeLocalities = [...localities.values()].filter((row) => row.active_event_count > 0);
const activeVenues = [...venues.values()].filter((row) => row.active_event_count > 0);
if (activeLocalities.length !== 14 || activeVenues.length !== 21) throw new Error('Production-day inventory must contain 14 active localities and 21 logical venues');
if (activeLocalities.reduce((sum, row) => sum + row.active_event_count, 0) !== 30) throw new Error('Production-day locality counts must total 30 events');

for (const locality of localities.values()) {
  for (const id of locality.route_corridor_ids) if (!corridors.has(id)) throw new Error(`Locality ${locality.id} references unknown corridor ${id}`);
}
for (const corridor of corridors.values()) {
  for (const id of corridor.locality_ids) if (!localities.has(id)) throw new Error(`Corridor ${corridor.id} references unknown locality ${id}`);
}
for (const stop of stops.values()) {
  if (!localities.has(stop.locality_id)) throw new Error(`Stop ${stop.id} references unknown locality ${stop.locality_id}`);
  if (!Array.isArray(stop.coordinates) || stop.coordinates.length !== 2) throw new Error(`Stop ${stop.id} needs [lat, lon] coordinates`);
}
for (const venue of venues.values()) {
  if (!localities.has(venue.locality_id)) throw new Error(`Venue ${venue.id} references unknown locality ${venue.locality_id}`);
  for (const leg of venue.access || []) {
    if (!stops.has(leg.stop_id)) throw new Error(`Venue ${venue.id} references unknown stop ${leg.stop_id}`);
    if (leg.confidence === 'blocked' && (leg.distance_m != null || leg.walk_minutes != null)) throw new Error(`Blocked access ${venue.id}/${leg.stop_id} must not publish a numeric distance`);
    if (leg.distance_m != null && leg.distance_m < 0) throw new Error(`Negative distance for ${venue.id}/${leg.stop_id}`);
  }
}

for (const schedule of schedules.routes) {
  if (!localities.has(schedule.route_directory_locality_id)) throw new Error(`Schedule ${schedule.id} misses a directory locality`);
  if (!venues.has(schedule.venue_access_id)) throw new Error(`Schedule ${schedule.id} misses a venue access record`);
  for (const id of schedule.route_corridor_ids || []) if (!corridors.has(id)) throw new Error(`Schedule ${schedule.id} references unknown corridor ${id}`);
}

const ushakovo = localities.get('ushakovo');
if (ushakovo.route_numbers.includes('110') || !ushakovo.route_numbers.includes('117')) throw new Error('Brandenburg must use the Mamonovo corridor 117, never homonymous route 110');
if (venues.get('baltiysk-territoriya-ya').access[0].distance_m !== 2611) throw new Error('Territoriya Ya last mile must keep the reviewed 2.611 km route');
if (venues.get('medvedevka-settlement').access[0].confidence !== 'blocked') throw new Error('Medvedevka must stay blocked until the event venue point is known');
if (venues.get('romanovo-kaup').access[0].confidence !== 'low') throw new Error('Kaup must not be enabled before the pedestrian entrance is checked');

console.log(`Bus transport directory checks passed: ${localities.size} localities, ${venues.size} venues, ${stops.size} stops`);
