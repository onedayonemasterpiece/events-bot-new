import data from '../data/busTransportSchedules.json';
import type { PreviewEvent } from './types';

export interface EventBusOutboundGroup {
  routes: string;
  departureStop: string;
  northStop: string;
  northOffsetMinutes: number;
  arrivalStop: string;
  rideEstimateLabel: string;
  walkEstimateMinutes: number;
  walkDistanceKm: number;
  walkMapUrl: string;
  departures: Array<{ terminal: string; northEstimated: string }>;
}

export interface EventBusReturnGroup {
  routes: string;
  departureStop: string;
  destinationStop: string;
  rideEstimateLabel: string;
  isEstimated: boolean;
  departures: string[];
}

export interface EventBusSuggestion {
  id: string;
  originStop: string;
  venueName: string;
  venueMapUrl: string;
  mapImageUrl: string;
  outboundGroups: EventBusOutboundGroup[];
  returnGroups: EventBusReturnGroup[];
  sourceName: string;
  sourceUrl: string;
  sourceEffectiveFrom: string;
}

function normalize(value: string | null | undefined): string {
  return String(value || '')
    .toLocaleLowerCase('ru-RU')
    .replace(/ё/gu, 'е')
    .replace(/[^а-яa-z0-9]+/gu, ' ')
    .trim();
}

function addMinutes(value: string, minutes: number): string {
  const [hours, mins] = value.split(':').map(Number);
  const total = (hours * 60 + mins + minutes) % (24 * 60);
  return `${String(Math.floor(total / 60)).padStart(2, '0')}:${String(total % 60).padStart(2, '0')}`;
}

export function getEventBusSuggestion(event: Pick<PreviewEvent, 'city' | 'venue_name' | 'start_time' | 'time_range_end'>): EventBusSuggestion | null {
  const city = normalize(event.city);
  const venue = normalize(event.venue_name);
  const route = data.routes.find((candidate) =>
    candidate.cities.some((item) => normalize(item) === city)
    && candidate.venues.some((item) => normalize(item) === venue)
    && candidate.event_start === event.start_time
    && candidate.event_end === event.time_range_end,
  );
  if (!route) return null;
  return {
    id: route.id,
    originStop: route.origin_stop,
    venueName: route.venue_name,
    venueMapUrl: route.venue_map_url,
    mapImageUrl: route.map_image_url,
    outboundGroups: route.outbound_groups.map((group) => ({
      routes: group.routes,
      departureStop: group.departure_stop,
      northStop: group.north_stop,
      northOffsetMinutes: group.north_offset_minutes,
      arrivalStop: group.arrival_stop,
      rideEstimateLabel: group.ride_estimate_label,
      walkEstimateMinutes: group.walk_estimate_minutes,
      walkDistanceKm: group.walk_distance_km,
      walkMapUrl: group.walk_map_url,
      departures: group.departures.map((terminal) => ({ terminal, northEstimated: addMinutes(terminal, group.north_offset_minutes) })),
    })),
    returnGroups: route.return_groups.map((group) => ({
      routes: group.routes,
      departureStop: group.departure_stop,
      destinationStop: group.destination_stop,
      rideEstimateLabel: group.ride_estimate_label,
      isEstimated: group.is_estimated,
      departures: group.departures,
    })),
    sourceName: data.source.name,
    sourceUrl: data.source.url,
    sourceEffectiveFrom: data.source.effective_from,
  };
}
