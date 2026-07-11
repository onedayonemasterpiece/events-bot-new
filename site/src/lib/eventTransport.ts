import scheduleData from '../data/transportSchedules.json';
import type { PreviewEvent } from './types';

interface TransportTrainRecord {
  number: string;
  departure: string;
  arrival: string;
  duration_minutes: number;
  train_type: string;
  calendar: Record<string, string>;
}

interface TransportRouteRecord {
  city: string;
  origin_station: string;
  destination_station: string;
  outbound_live_url: string;
  return_live_url: string;
  outbound: TransportTrainRecord[];
  return: TransportTrainRecord[];
}

interface TransportScheduleData {
  generated_at: string;
  timezone: string;
  source: {
    schedule: string;
    retrieved_at: string;
    note: string;
  };
  selection: {
    arrival_before_event_min_minutes: number;
    arrival_before_event_max_minutes: number;
    max_return_wait_minutes: number;
    max_options_per_direction: number;
  };
  routes: TransportRouteRecord[];
}

export interface EventTrainOption {
  number: string;
  trainType: string;
  serviceDate: string;
  departure: string;
  arrival: string;
  durationMinutes: number;
  minutesBeforeEvent?: number;
  waitAfterEventMinutes?: number;
  nextDay: boolean;
  departureIso: string;
  arrivalIso: string;
}

export type EventTransportDirection = 'outbound' | 'return';
export type EventEndBasis = 'explicit' | 'event_type_default' | 'unknown';

export interface EventTransportSuggestion {
  city: string;
  originStation: string;
  destinationStation: string;
  outboundLiveUrl: string;
  returnLiveUrl: string;
  eventStart: string;
  eventEnd: string | null;
  eventEndBasis: EventEndBasis;
  eventEndDurationMinutes: number | null;
  eventEndTypeLabel: string | null;
  outbound: EventTrainOption[];
  returns: EventTrainOption[];
  arrivalWindow: { min: number; max: number };
  maxReturnWaitMinutes: number;
  scheduleSource: string;
  scheduleRetrievedAt: string;
  scheduleNote: string;
}

const schedules = scheduleData as TransportScheduleData;

const DEFAULT_EVENT_DURATION_MINUTES: Record<string, number> = {
  'выставка': 120,
  'концерт': 120,
  'concert': 120,
  'спектакль': 150,
  'театр': 150,
  'лекция': 90,
  'встреча': 90,
  'презентация': 90,
  'кинопоказ': 150,
  'movie': 150,
  'мастер класс': 120,
  'дегустация': 120,
  'therapy': 120,
  'экскурсия': 120,
  'virtual excursion': 120,
  'вечеринка': 240,
  'фестиваль': 360,
  'ярмарка': 360,
  'спорт': 180,
  'турнир': 180,
  'интенсив': 180,
};

function normalizeCity(value: string | null | undefined): string {
  return String(value || '')
    .toLocaleLowerCase('ru-RU')
    .replace(/ё/gu, 'е')
    .replace(/[^а-яa-z]+/gu, ' ')
    .trim();
}

function timeToMinutes(value: string | null | undefined): number | null {
  const match = /^(\d{1,2}):(\d{2})/u.exec(String(value || '').trim());
  if (!match) return null;
  const hours = Number(match[1]);
  const minutes = Number(match[2]);
  if (hours < 0 || hours > 23 || minutes < 0 || minutes > 59) return null;
  return hours * 60 + minutes;
}

function addIsoDays(value: string, days: number): string | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/u.exec(value);
  if (!match) return null;
  const date = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3]) + days));
  if (!Number.isFinite(date.getTime())) return null;
  return date.toISOString().slice(0, 10);
}

function normalizeEventType(value: string | null | undefined): string {
  return String(value || '')
    .toLocaleLowerCase('ru-RU')
    .replace(/ё/gu, 'е')
    .replace(/[^а-яa-z]+/gu, ' ')
    .trim();
}

function formatMinutes(value: number): string {
  const normalized = ((value % (24 * 60)) + 24 * 60) % (24 * 60);
  return `${String(Math.floor(normalized / 60)).padStart(2, '0')}:${String(normalized % 60).padStart(2, '0')}`;
}

function localIso(serviceDate: string, time: string, dayOffset = 0): string {
  const targetDate = addIsoDays(serviceDate, dayOffset) || serviceDate;
  return `${targetDate}T${time}:00+02:00`;
}

function serviceRunsOn(train: TransportTrainRecord, serviceDate: string): boolean {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/u.exec(serviceDate);
  if (!match) return false;
  const monthBits = train.calendar[`${match[1]}-${match[2]}`];
  const dayIndex = Number(match[3]) - 1;
  return Boolean(monthBits && dayIndex >= 0 && monthBits[dayIndex] === '1');
}

function toOption(
  train: TransportTrainRecord,
  serviceDate: string,
  nextDay: boolean,
  extra: Partial<Pick<EventTrainOption, 'minutesBeforeEvent' | 'waitAfterEventMinutes'>>,
): EventTrainOption {
  const departureMinutes = timeToMinutes(train.departure) || 0;
  const arrivalMinutes = timeToMinutes(train.arrival) || 0;
  return {
    number: train.number,
    trainType: train.train_type,
    serviceDate,
    departure: train.departure,
    arrival: train.arrival,
    durationMinutes: train.duration_minutes,
    nextDay,
    departureIso: localIso(serviceDate, train.departure),
    arrivalIso: localIso(serviceDate, train.arrival, arrivalMinutes < departureMinutes ? 1 : 0),
    ...extra,
  };
}

export function eventTransportTripKey(direction: EventTransportDirection, train: EventTrainOption): string {
  const number = train.number.toLocaleLowerCase('ru-RU').replace(/[^a-zа-я0-9]+/gu, '-').replace(/^-|-$/gu, '');
  return `${direction}-${train.serviceDate}-${number}`;
}

export function eventTransportCalendarPath(
  event: Pick<PreviewEvent, 'slug'>,
  direction: EventTransportDirection,
  train: EventTrainOption,
): string {
  return `/sobytiya/${event.slug}/transport/${eventTransportTripKey(direction, train)}.ics`;
}

function getRoute(city: string | null | undefined): TransportRouteRecord | null {
  const target = normalizeCity(city);
  if (!target) return null;
  return schedules.routes.find((route) => normalizeCity(route.city) === target) || null;
}

export function getEventTransportSuggestion(
  event: Pick<PreviewEvent, 'city' | 'start_date' | 'end_date' | 'start_time' | 'time_range_end' | 'event_type'>,
): EventTransportSuggestion | null {
  const route = getRoute(event.city);
  const eventStartMinutes = timeToMinutes(event.start_time);
  const isSingleDay = !event.end_date || event.end_date === event.start_date;
  if (!route || eventStartMinutes === null || !event.start_date || !isSingleDay) return null;

  const minArrivalLead = schedules.selection.arrival_before_event_min_minutes;
  const maxArrivalLead = schedules.selection.arrival_before_event_max_minutes;
  const optionLimit = schedules.selection.max_options_per_direction;
  const outbound = route.outbound
    .filter((train) => serviceRunsOn(train, event.start_date))
    .map((train) => ({ train, arrivalMinutes: timeToMinutes(train.arrival) }))
    .filter((item): item is { train: TransportTrainRecord; arrivalMinutes: number } => item.arrivalMinutes !== null)
    .map(({ train, arrivalMinutes }) => ({ train, lead: eventStartMinutes - arrivalMinutes }))
    .filter(({ lead }) => lead >= minArrivalLead && lead <= maxArrivalLead)
    .sort((left, right) => Math.abs(left.lead - 30) - Math.abs(right.lead - 30) || right.lead - left.lead)
    .slice(0, optionLimit)
    .map(({ train, lead }) => toOption(train, event.start_date, false, { minutesBeforeEvent: lead }));

  const eventEndRaw = timeToMinutes(event.time_range_end);
  const normalizedType = normalizeEventType(event.event_type);
  const defaultDuration = eventEndRaw === null ? DEFAULT_EVENT_DURATION_MINUTES[normalizedType] || null : null;
  const eventEndBasis: EventEndBasis = eventEndRaw !== null ? 'explicit' : defaultDuration ? 'event_type_default' : 'unknown';
  let eventEndMinutes = eventEndRaw !== null ? eventEndRaw : defaultDuration ? eventStartMinutes + defaultDuration : null;
  if (eventEndMinutes !== null && eventEndMinutes < eventStartMinutes) eventEndMinutes += 24 * 60;
  const nextDate = addIsoDays(event.start_date, 1);
  const returnCandidates: Array<{ train: TransportTrainRecord; serviceDate: string; departureMinutes: number; nextDay: boolean }> = [];
  if (eventEndMinutes !== null) {
    for (const train of route.return) {
      const sameDayDeparture = timeToMinutes(train.departure);
      if (sameDayDeparture !== null && serviceRunsOn(train, event.start_date)) {
        returnCandidates.push({ train, serviceDate: event.start_date, departureMinutes: sameDayDeparture, nextDay: false });
      }
      if (sameDayDeparture !== null && nextDate && serviceRunsOn(train, nextDate)) {
        returnCandidates.push({ train, serviceDate: nextDate, departureMinutes: sameDayDeparture + 24 * 60, nextDay: true });
      }
    }
  }
  const returns = eventEndMinutes === null
    ? []
    : returnCandidates
      .map((item) => ({ ...item, wait: item.departureMinutes - eventEndMinutes }))
      .filter(({ wait }) => wait >= 0 && wait <= schedules.selection.max_return_wait_minutes)
      .sort((left, right) => left.wait - right.wait || left.departureMinutes - right.departureMinutes)
      .slice(0, optionLimit)
      .map(({ train, serviceDate, nextDay: isNextDay, wait }) => toOption(train, serviceDate, isNextDay, { waitAfterEventMinutes: wait }));

  return {
    city: route.city,
    originStation: route.origin_station,
    destinationStation: route.destination_station,
    outboundLiveUrl: route.outbound_live_url,
    returnLiveUrl: route.return_live_url,
    eventStart: event.start_time || '',
    eventEnd: eventEndMinutes === null ? null : formatMinutes(eventEndMinutes),
    eventEndBasis,
    eventEndDurationMinutes: defaultDuration,
    eventEndTypeLabel: defaultDuration ? (event.event_type || null) : null,
    outbound,
    returns,
    arrivalWindow: { min: minArrivalLead, max: maxArrivalLead },
    maxReturnWaitMinutes: schedules.selection.max_return_wait_minutes,
    scheduleSource: schedules.source.schedule,
    scheduleRetrievedAt: schedules.source.retrieved_at,
    scheduleNote: schedules.source.note,
  };
}
