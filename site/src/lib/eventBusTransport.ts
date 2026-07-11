import data from '../data/busTransportSchedules.json';
import type { PreviewEvent } from './types';

export interface EventBusOption {
  route: string;
  alsoRoute: string | null;
  departure: string;
  arrivalStop: string;
  rideEstimateMinutes: number;
  walkEstimateMinutes: number;
  walkDistanceKm: number;
  estimatedVenueArrival: string;
  walkMapUrl: string;
  note: string;
}

export interface EventBusSuggestion {
  id: string;
  originStop: string;
  venueName: string;
  venueMapUrl: string;
  mapEmbedUrl: string;
  outbound: EventBusOption[];
  returnOption: {
    route: string;
    eventEnd: string;
    estimatedDeparture: string;
    arrivalKaliningrad: string;
    note: string;
  };
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
    mapEmbedUrl: route.map_embed_url,
    outbound: route.outbound.map((option) => ({
      route: option.route,
      alsoRoute: option.also_route,
      departure: option.departure,
      arrivalStop: option.arrival_stop,
      rideEstimateMinutes: option.ride_estimate_minutes,
      walkEstimateMinutes: option.walk_estimate_minutes,
      walkDistanceKm: option.walk_distance_km,
      estimatedVenueArrival: option.estimated_venue_arrival,
      walkMapUrl: option.walk_map_url,
      note: option.note,
    })),
    returnOption: {
      route: route.return.route,
      eventEnd: route.return.event_end,
      estimatedDeparture: route.return.estimated_departure,
      arrivalKaliningrad: route.return.arrival_kaliningrad,
      note: route.return.note,
    },
    sourceName: data.source.name,
    sourceUrl: data.source.url,
    sourceEffectiveFrom: data.source.effective_from,
  };
}
