import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const dataDir = resolve(new URL('../src/data', import.meta.url).pathname);
const directory = JSON.parse(readFileSync(resolve(dataDir, 'railRouteDirectory.json'), 'utf8'));
const busDirectory = JSON.parse(readFileSync(resolve(dataDir, 'busRouteDirectory.json'), 'utf8'));

function indexUnique(rows, label, key = 'id') {
  const result = new Map();
  for (const row of rows) {
    const id = row?.[key];
    if (!id || result.has(id)) throw new Error(`${label} has a missing/duplicate ${key}: ${id}`);
    result.set(id, row);
  }
  return result;
}

function minutes(value) {
  if (!/^\d{2}:\d{2}$/.test(value)) throw new Error(`Invalid HH:MM value: ${value}`);
  const [hour, minute] = value.split(':').map(Number);
  if (hour > 23 || minute > 59) throw new Error(`Invalid HH:MM value: ${value}`);
  return hour * 60 + minute;
}

const sources = indexUnique(directory.direction_sources, 'direction_sources');
const stations = indexUnique(directory.stations, 'stations');
const routes = indexUnique(directory.routes, 'routes');
const policies = indexUnique(directory.locality_policies, 'locality_policies', 'locality_id');
const patterns = indexUnique(directory.reviewed_service_patterns, 'reviewed_service_patterns');
const busLocalities = indexUnique(busDirectory.localities, 'bus localities');
const supportingSources = indexUnique(directory.supporting_sources || [], 'supporting_sources');

if (directory.official_index_url !== 'https://www.kppk39.ru/raspisanie/') throw new Error('The carrier index must be the canonical КППК schedule page');
if (sources.size !== 13) throw new Error(`Expected all 13 carrier direction/product pages, got ${sources.size}`);

for (const source of sources.values()) {
  if (!source.page_url.startsWith('https://www.kppk39.ru/')) throw new Error(`Non-carrier source page in ${source.id}`);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(source.effective_from)) throw new Error(`Missing effective_from in ${source.id}`);
  if (!source.assets?.length) throw new Error(`Missing source image in ${source.id}`);
  for (const asset of source.assets) {
    if (!asset.url.startsWith('https://www.kppk39.ru/upload/medialibrary/')) throw new Error(`Non-carrier asset in ${source.id}`);
    if (!/^[a-f0-9]{64}$/.test(asset.sha256)) throw new Error(`Missing SHA-256 in ${source.id}`);
  }
  if ('valid_to' in source) throw new Error(`Do not fabricate direction valid_to in ${source.id}`);
}

for (const route of routes.values()) {
  for (const id of route.source_ids) if (!sources.has(id)) throw new Error(`Route ${route.id} references unknown source ${id}`);
  for (const id of [...route.origin_station_ids, ...route.destination_station_ids, ...(route.via_station_ids || [])]) {
    if (!stations.has(id)) throw new Error(`Route ${route.id} references unknown station ${id}`);
  }
}

for (const source of supportingSources.values()) {
  if (!source.url?.startsWith('https://') || !/^\d{4}-\d{2}-\d{2}$/.test(source.published_at)) throw new Error(`Invalid supporting source ${source.id}`);
}

for (const venue of directory.venue_access) {
  if (!stations.has(venue.station_id)) throw new Error(`Venue ${venue.id} references unknown station ${venue.station_id}`);
  if (venue.route_id && !routes.has(venue.route_id)) throw new Error(`Venue ${venue.id} references unknown route ${venue.route_id}`);
}

for (const policy of policies.values()) {
  for (const id of policy.route_ids) if (!routes.has(id)) throw new Error(`Policy ${policy.locality_id} references unknown route ${id}`);
  if (policy.bus_locality_id && !busLocalities.has(policy.bus_locality_id)) throw new Error(`Policy ${policy.locality_id} references unknown bus locality ${policy.bus_locality_id}`);
}

for (const pattern of patterns.values()) {
  if (!routes.has(pattern.route_id)) throw new Error(`Pattern ${pattern.id} references unknown route ${pattern.route_id}`);
  if (!pattern.calendar?.effective_from || pattern.calendar.valid_to !== null) throw new Error(`Pattern ${pattern.id} must be open-ended until superseded`);
  for (const trip of pattern.trips) {
    if (trip.departure && trip.arrival && minutes(trip.arrival) <= minutes(trip.departure)) throw new Error(`Non-monotonic endpoint times in ${pattern.id}/${trip.number}`);
    if (trip.stops) {
      let previous = -1;
      for (const stop of trip.stops) {
        if (!stations.has(stop.station_id)) throw new Error(`Trip ${trip.number} references unknown station ${stop.station_id}`);
        const time = stop.departure || stop.arrival;
        const current = minutes(time);
        if (current < previous) throw new Error(`Non-monotonic stop times in ${pattern.id}/${trip.number}`);
        previous = current;
      }
      for (const id of trip.skips_station_ids || []) if (!stations.has(id)) throw new Error(`Trip ${trip.number} skips unknown station ${id}`);
    }
  }
}

for (const locality of ['svetlogorsk', 'zelenogradsk', 'pionersky']) {
  if (policies.get(locality)?.mode_policy !== 'rail_primary') throw new Error(`${locality} must be rail-primary`);
}
if (!policies.get('baltiysk')?.mode_policy.startsWith('rail_primary_')) throw new Error('Baltiysk must be rail-primary while the multi-pair summer table is current');
for (const locality of ['gvardeysk', 'chernyakhovsk', 'gusev', 'bagrationovsk', 'zheleznodorozhny']) {
  if (!policies.get(locality)?.mode_policy.startsWith('rail_and_bus_parallel')) throw new Error(`${locality} must compare rail and bus`);
}

const baltiyskOutbound = patterns.get('baltiysk-outbound');
const baltiyskReturn = patterns.get('baltiysk-return');
if (baltiyskOutbound.trips.length !== 5 || baltiyskReturn.trips.length !== 6) throw new Error('Current Baltiysk base must retain five outbound and six return services');

const eastOutbound = patterns.get('east-outbound').trips;
for (const number of ['6582/6591', '6572']) {
  const trip = eastOutbound.find((row) => row.number === number);
  if (!trip?.skips_station_ids?.includes('gvardeysk') || !trip.skips_station_ids.includes('znamensk')) throw new Error(`${number} must not be inferred to stop at Gvardeysk/Znamensk`);
}

const ushakovo = policies.get('ushakovo');
if (ushakovo.direct_rail_station !== false || !ushakovo.mode_policy.includes('ladushkin')) throw new Error('Brandenburg must be a reviewed Ladushkin road transfer, never a direct rail stop');
const tyunin = directory.venue_access.find((row) => row.id === 'ferma-tyuniny');
if (tyunin?.station_id !== 'znamensk' || tyunin.walk_distance_m !== 1057 || tyunin.walk_minutes !== 14 || !tyunin.journey_rule.includes('mixed')) throw new Error('Ferma Tyuniny must preserve the reviewed station walk and mixed-mode rule');
const krasnolesye = patterns.get('krasnolesye-roundtrip');
if (krasnolesye.calendar.kind !== 'weekends_holidays' || krasnolesye.trips[0].departure !== '09:55' || krasnolesye.trips[1].departure !== '18:25') throw new Error('Krasnolesye must remain an exact-date weekend/holiday round trip');
const yantarny = directory.venue_access.find((row) => row.id === 'kaliningrad-ds-yantarny');
if (yantarny?.station_id !== 'elizavetinskaya' || yantarny.route_id !== 'elizavetinskaya-venue') throw new Error('DS Yantarny must use only the Elizavetinskaya venue-specific route');
if (yantarny.walk_distance_m !== 627 || yantarny.walk_minutes !== 9 || !yantarny.journey_rule.includes('never enable it for other Kaliningrad events')) throw new Error('DS Yantarny must preserve the reviewed walk and venue-only safety rule');
if (routes.get('elizavetinskaya-venue')?.priority_class !== 'venue_specific_optional') throw new Error('Elizavetinskaya must remain optional and venue-specific');
if (!directory.excluded_services.some((row) => row.id === 'victory-echelon')) throw new Error('The exhibition train exclusion is required');

console.log(`Rail transport directory checks passed: ${sources.size} official pages, ${routes.size} routes, ${policies.size} locality policies, ${patterns.size} service patterns`);
