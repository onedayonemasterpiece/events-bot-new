import busData from '../data/busTransportSchedules.json';
import { busClockToMinutes, busMinutesToClock, resolveBusBoarding } from './busBoarding';
import type { PreviewEvent } from './types';

export interface KaupBusOption {
  tripId: string;
  terminalDeparture: string;
  departure: string;
  departureEstimated: boolean;
  departureAt: string;
  stopArrivalAt: string;
  venueArrivalAt: string;
  estimatedStopArrival: string;
  estimatedVenueArrival: string;
  arrivalLeadMinutes: number;
  tight: boolean;
}

export interface KaupTransportSuggestion {
  venueName: string;
  venueMapUrl: string;
  directionsMapUrl: string;
  stopToVenueDirectionsUrl: string;
  officialSourceUrl: string;
  transferBookingUrl: string;
  transferPriceLabel: string;
  transferBoardingPoints: string[];
  busRoute: string;
  busOriginName: string;
  busOriginAddress: string;
  busOriginMapUrl: string;
  busArrivalStop: string;
  walkDistanceLabel: string;
  walkEstimateLabel: string;
  outbound: KaupBusOption[];
  publicReturnAvailable: boolean;
  scheduleSourceUrl: string;
  scheduleEffectiveFrom: string;
  scheduleSnapshotHash: string;
}

function normalize(value: string | null | undefined): string {
  return String(value || '')
    .toLocaleLowerCase('ru-RU')
    .replace(/ё/gu, 'е')
    .replace(/[^а-яa-z0-9]+/gu, ' ')
    .trim();
}

function minutes(value: string | null | undefined): number | null {
  return busClockToMinutes(value);
}

function clock(value: number): string {
  return busMinutesToClock(value);
}

function serviceTimestamp(serviceDate: string, value: number): string | null {
  if (!/^\d{4}-\d{2}-\d{2}$/u.test(serviceDate)) return null;
  const time = clock(value);
  return `${serviceDate}T${time}:00+02:00`;
}

export function getKaupTransportSuggestion(event: Pick<PreviewEvent, 'venue_name' | 'start_date' | 'start_time' | 'time_range_end'>): KaupTransportSuggestion | null {
  const venue = normalize(event.venue_name);
  if (!['поселение викингов кауп', 'кауп', 'kaup'].includes(venue)) return null;
  const eventStart = minutes(event.start_time);
  if (eventStart === null) return null;
  const publicRoute = busData.routes.find((route) => route.id === 'romanovo-holmogorye');
  const route119 = publicRoute?.outbound_groups.find((group) => group.routes === '119');
  const preferredBoarding = publicRoute?.preferred_boarding;
  const rideMinutes = Number(route119?.ride_estimate_minutes || 65);
  // The reviewed Kaup venue access record is 4.306 km / about 53 minutes from
  // the public stop. It is presented as a caution, never as an accessible walk.
  const walkMinutes = 53;
  const outbound = (route119?.departures || [])
    .map((departure) => {
      const boarding = resolveBusBoarding({
        terminalDeparture:departure,
        terminalStop:route119?.departure_stop || publicRoute?.origin_stop || 'Автовокзал Калининград',
        terminalRideEstimateMinutes:rideMinutes,
        preferredBoarding,
      });
      if (!boarding) return null;
      const estimatedArrivalMinutes = boarding.destinationArrivalMinutes + walkMinutes;
      const arrivalLeadMinutes = eventStart - estimatedArrivalMinutes;
      const departureAt = serviceTimestamp(event.start_date, boarding.boardingDepartureMinutes);
      const stopArrivalAt = serviceTimestamp(event.start_date, boarding.destinationArrivalMinutes);
      const venueArrivalAt = serviceTimestamp(event.start_date, estimatedArrivalMinutes);
      if (!departureAt || !stopArrivalAt || !venueArrivalAt) return null;
      return {
        tripId:`bus-119-${event.start_date.replace(/-/gu, '')}-${departure.replace(':', '')}`,
        terminalDeparture:departure,
        departure:boarding.boardingDeparture,
        departureEstimated:boarding.boardingTimeEstimated,
        departureAt,
        stopArrivalAt,
        venueArrivalAt,
        estimatedStopArrival:clock(boarding.destinationArrivalMinutes),
        estimatedVenueArrival:clock(estimatedArrivalMinutes),
        arrivalLeadMinutes,
        tight:arrivalLeadMinutes < 20,
      };
    })
    .filter((option): option is KaupBusOption => Boolean(option && option.arrivalLeadMinutes >= 0 && option.arrivalLeadMinutes <= 120))
    .slice(-20);
  const assumedReturnReady = (minutes(event.time_range_end) ?? eventStart + 120) + walkMinutes;
  const publicReturnAvailable = Boolean(publicRoute?.return_groups
    .find((group) => group.routes === '119')
    ?.departures.some((departure) => (minutes(departure) ?? -1) >= assumedReturnReady));
  return {
    venueName:'Поселение викингов Кауп',
    venueMapUrl:'https://yandex.ru/maps/org/poseleniye_vikingov_kaup/1685907695/?ll=20.430595%2C55.036368&z=8',
    directionsMapUrl:'https://yandex.ru/maps/?rtext=54.710426%2C20.452214~54.8781221%2C20.2789453&rtt=auto',
    stopToVenueDirectionsUrl:'https://yandex.ru/maps/?rtext=54.8958609%2C20.2759337~54.8781221%2C20.2789453&rtt=pd',
    officialSourceUrl:'https://www.kaup39.ru/',
    transferBookingUrl:'https://radario.ru/customer/afisha/07491527ea271252d4cf919044700687b69e8774898812c?openAsLinkKey=076uuwhz5i',
    transferPriceLabel:'600 ₽ туда и обратно',
    transferBoardingPoints:[
      'Калининград · у Дома Советов',
      'Зеленоградск · Ленина, 10, у автовокзала',
      'Светлогорск-2 · Ленина, 33, у вокзала',
    ],
    busRoute:'119',
    busOriginName:preferredBoarding?.stop_name || route119?.departure_stop || 'Автовокзал Калининград',
    busOriginAddress:preferredBoarding?.locality_label || 'Калининград',
    busOriginMapUrl:preferredBoarding?.map_url || 'https://yandex.ru/maps/?text=%D0%90%D0%B2%D1%82%D0%BE%D0%B2%D0%BE%D0%BA%D0%B7%D0%B0%D0%BB%20%D0%9A%D0%B0%D0%BB%D0%B8%D0%BD%D0%B8%D0%BD%D0%B3%D1%80%D0%B0%D0%B4',
    busArrivalStop:'Романово',
    walkDistanceLabel:'около 4 км',
    walkEstimateLabel:'около 53 минут пешком',
    outbound,
    publicReturnAvailable,
    scheduleSourceUrl:busData.source.url,
    scheduleEffectiveFrom:busData.source.effective_from,
    scheduleSnapshotHash:`${busData.schema_version}:${busData.generated_at}:${busData.source.effective_from}`,
  };
}
