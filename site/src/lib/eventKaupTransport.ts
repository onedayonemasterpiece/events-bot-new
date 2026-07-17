import busData from '../data/busTransportSchedules.json';
import type { PreviewEvent } from './types';

export interface KaupBusOption {
  departure: string;
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
}

function normalize(value: string | null | undefined): string {
  return String(value || '')
    .toLocaleLowerCase('ru-RU')
    .replace(/ё/gu, 'е')
    .replace(/[^а-яa-z0-9]+/gu, ' ')
    .trim();
}

function minutes(value: string | null | undefined): number | null {
  const match = /^(\d{2}):(\d{2})$/u.exec(String(value || ''));
  return match ? Number(match[1]) * 60 + Number(match[2]) : null;
}

function clock(value: number): string {
  return `${String(Math.floor(value / 60) % 24).padStart(2, '0')}:${String(value % 60).padStart(2, '0')}`;
}

export function getKaupTransportSuggestion(event: Pick<PreviewEvent, 'venue_name' | 'start_time' | 'time_range_end'>): KaupTransportSuggestion | null {
  const venue = normalize(event.venue_name);
  if (!['поселение викингов кауп', 'кауп', 'kaup'].includes(venue)) return null;
  const eventStart = minutes(event.start_time);
  if (eventStart === null) return null;
  const publicRoute = busData.routes.find((route) => route.id === 'romanovo-holmogorye');
  const route119 = publicRoute?.outbound_groups.find((group) => group.routes === '119');
  const rideMinutes = Number(route119?.ride_estimate_minutes || 65);
  // The reviewed Kaup venue access record is 4.306 km / about 53 minutes from
  // the public stop. It is presented as a caution, never as an accessible walk.
  const walkMinutes = 53;
  const outbound = (route119?.departures || [])
    .map((departure) => {
      const departureMinutes = minutes(departure);
      if (departureMinutes === null) return null;
      const estimatedArrivalMinutes = departureMinutes + rideMinutes + walkMinutes;
      const arrivalLeadMinutes = eventStart - estimatedArrivalMinutes;
      return {
        departure,
        estimatedStopArrival:clock(departureMinutes + rideMinutes),
        estimatedVenueArrival:clock(estimatedArrivalMinutes),
        arrivalLeadMinutes,
        tight:arrivalLeadMinutes < 20,
      };
    })
    .filter((option): option is KaupBusOption => Boolean(option && option.arrivalLeadMinutes >= 0 && option.arrivalLeadMinutes <= 120))
    .slice(-2);
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
    busOriginName:'Калининградский автовокзал',
    busOriginAddress:'ул. Железнодорожная, 7',
    busOriginMapUrl:'https://yandex.ru/maps/?text=%D0%9A%D0%B0%D0%BB%D0%B8%D0%BD%D0%B8%D0%BD%D0%B3%D1%80%D0%B0%D0%B4%2C%20%D1%83%D0%BB.%20%D0%96%D0%B5%D0%BB%D0%B5%D0%B7%D0%BD%D0%BE%D0%B4%D0%BE%D1%80%D0%BE%D0%B6%D0%BD%D0%B0%D1%8F%2C%207',
    busArrivalStop:'Романово',
    walkDistanceLabel:'около 4 км',
    walkEstimateLabel:'около 53 минут пешком',
    outbound,
    publicReturnAvailable,
    scheduleSourceUrl:busData.source.url,
    scheduleEffectiveFrom:busData.source.effective_from,
  };
}
