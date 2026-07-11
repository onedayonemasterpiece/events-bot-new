import data from '../data/busTransportSchedules.json';
import type { PreviewEvent } from './types';

export interface EventBusOutboundGroup {
  routes: string;
  departureStop: string;
  arrivalStop: string;
  walkEstimateMinutes: number;
  walkDistanceKm: number;
  departures: string[];
}

export interface EventBusReturnGroup {
  routes: string;
  departureStop: string;
  destinationStop: string;
  isEstimated: boolean;
  departures: string[];
}

export interface EventBusSuggestion {
  id: string;
  originStop: string;
  venueName: string;
  venueMapUrl: string;
  mapSquareImageUrl: string;
  mapPortraitImageUrl: string;
  walkMapUrl: string;
  northStop: string;
  northTravelEstimateLabel: string;
  venueHoursLabel: string;
  sharedRideEstimateLabel: string;
  minimumVisitMinutes: number;
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

function timeToMinutes(value: string | null | undefined): number | null {
  const match = /^(\d{2}):(\d{2})$/u.exec(String(value || ''));
  if (!match) return null;
  return Number(match[1]) * 60 + Number(match[2]);
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
    mapSquareImageUrl: route.map_square_image_url,
    mapPortraitImageUrl: route.map_portrait_image_url,
    walkMapUrl: route.walk_map_url,
    northStop: route.north_stop,
    northTravelEstimateLabel: route.north_travel_estimate_label,
    venueHoursLabel: route.venue_hours_label,
    sharedRideEstimateLabel: route.shared_ride_estimate_label,
    minimumVisitMinutes: route.return_min_visit_minutes,
    outboundGroups: route.outbound_groups.map((group) => ({
      routes: group.routes,
      departureStop: group.departure_stop,
      arrivalStop: group.arrival_stop,
      walkEstimateMinutes: group.walk_estimate_minutes,
      walkDistanceKm: group.walk_distance_km,
      departures: group.departures.filter((departure) => {
        const eventStart = timeToMinutes(event.start_time);
        const departureMinutes = timeToMinutes(departure);
        if (eventStart === null || departureMinutes === null) return false;
        const venueArrival = departureMinutes + group.ride_estimate_minutes + group.walk_estimate_minutes;
        const arrivalLead = eventStart - venueArrival;
        return arrivalLead >= route.outbound_min_arrival_lead_minutes && arrivalLead <= route.outbound_max_arrival_lead_minutes;
      }),
    })).filter((group) => group.departures.length > 0),
    returnGroups: route.return_groups.map((group) => {
      const eventStart = timeToMinutes(event.start_time);
      const eventEnd = timeToMinutes(event.time_range_end);
      const earliest = eventStart === null ? null : eventStart + route.return_min_visit_minutes + group.walk_from_venue_minutes;
      const latest = eventEnd === null ? null : eventEnd + group.walk_from_venue_minutes + route.return_max_wait_minutes;
      return {
        routes: group.routes,
        departureStop: group.departure_stop,
        destinationStop: group.destination_stop,
        isEstimated: group.is_estimated,
        departures: group.departures.filter((departure) => {
          const minutes = timeToMinutes(departure);
          return minutes !== null && (earliest === null || minutes >= earliest) && (latest === null || minutes <= latest);
        }),
      };
    }).filter((group) => group.departures.length > 0),
    sourceName: data.source.name,
    sourceUrl: data.source.url,
    sourceEffectiveFrom: data.source.effective_from,
  };
}
