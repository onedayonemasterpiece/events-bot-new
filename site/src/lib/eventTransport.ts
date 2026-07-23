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
export type EventEndBasis = 'explicit' | 'forecast' | 'schedule_cutoff';

export interface EventTransportCalendarEntry {
  direction: EventTransportDirection;
  train: EventTrainOption;
}

export interface EventTransportSuggestion {
  city: string;
  originStation: string;
  destinationStation: string;
  outboundLiveUrl: string;
  returnLiveUrl: string;
  eventStart: string;
  eventEnd: string | null;
  eventEndBasis: EventEndBasis;
  returnAccessMinutes: number;
  returnAccessLabel: string;
  returnReadyTime: string | null;
  eventTypeGenitive: string;
  outbound: EventTrainOption[];
  returns: EventTrainOption[];
  lastSameDayReturn: EventTrainOption | null;
  firstNightReturn: EventTrainOption | null;
  firstNextDayReturn: EventTrainOption | null;
  returnCalendarCovered: boolean;
  arrivalWindow: { min: number; max: number };
  maxReturnWaitMinutes: number;
  scheduleSource: string;
  scheduleRetrievedAt: string;
  scheduleNote: string;
}

const schedules = scheduleData as TransportScheduleData;

const EVENT_TYPE_GENITIVE: Record<string, string> = {
  'выставка': 'выставки',
  'концерт': 'концерта',
  'concert': 'концерта',
  'спектакль': 'спектакля',
  'театр': 'спектакля',
  'лекция': 'лекции',
  'встреча': 'встречи',
  'презентация': 'презентации',
  'кинопоказ': 'кинопоказа',
  'movie': 'кинопоказа',
  'мастер класс': 'мастер-класса',
  'дегустация': 'дегустации',
  'therapy': 'занятия',
  'экскурсия': 'экскурсии',
  'virtual excursion': 'экскурсии',
  'вечеринка': 'вечеринки',
  'фестиваль': 'фестиваля',
  'ярмарка': 'ярмарки',
  'спорт': 'спортивного события',
  'турнир': 'турнира',
  'интенсив': 'интенсива',
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

function clockFromMinutes(value: number): string {
  const normalized = ((value % (24 * 60)) + (24 * 60)) % (24 * 60);
  return `${String(Math.floor(normalized / 60)).padStart(2, '0')}:${String(normalized % 60).padStart(2, '0')}`;
}

function returnAccessProfile(event: Pick<PreviewEvent, 'venue_name' | 'city'>): { minutes: number; label: string } {
  const venue = normalizeCity(event.venue_name);
  if (venue.includes('янтарь холл') || venue.includes('yantar hall')) {
    return {
      minutes:30,
      label:'Закладываем 30 минут: выход из зала, около 15 минут пешком до Светлогорска-2 и запас на посадку.',
    };
  }
  return {
    minutes:25,
    label:'Закладываем 25 минут на выход с площадки, дорогу до станции и посадку.',
  };
}

function localIso(serviceDate: string, time: string, dayOffset = 0): string {
  const targetDate = addIsoDays(serviceDate, dayOffset) || serviceDate;
  return `${targetDate}T${time}:00+02:00`;
}

function calendarCovers(trains: TransportTrainRecord[], serviceDate: string | null): boolean {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/u.exec(String(serviceDate || ''));
  if (!match) return false;
  const month = `${match[1]}-${match[2]}`;
  const dayIndex = Number(match[3]) - 1;
  return trains.some((train) => {
    const bits = train.calendar[month];
    return Boolean(bits && dayIndex >= 0 && dayIndex < bits.length);
  });
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

function stationFileSlug(value: string): string {
  const station = normalizeCity(value);
  const known: Array<[string, string]> = [
    ['елизаветинская', 'elizavetinskaya'],
    ['светлогорск', 'svetlogorsk'],
    ['зеленоградск', 'zelenogradsk'],
    ['пионерский', 'pionersky'],
    ['балтийск', 'baltiysk'],
    ['багратионовск', 'bagrationovsk'],
    ['железнодорожный', 'zheleznodorozhny'],
    ['краснолесье', 'krasnolesye'],
    ['мамоново', 'mamonovo'],
    ['ладушкин', 'ladushkin'],
    ['черняховск', 'chernyakhovsk'],
    ['чернышевское', 'chernyshevskoe'],
    ['гусев', 'gusev'],
    ['нестеров', 'nesterov'],
    ['знаменск', 'znamensk'],
    ['гвардейск', 'gvardeysk'],
    ['полесск', 'polessk'],
    ['советск', 'sovetsk'],
    ['неман', 'neman'],
    ['калининград', 'kaliningrad'],
  ];
  return known.find(([needle]) => station.includes(needle))?.[1] || 'train';
}

export function eventTransportTripKey(
  suggestion: Pick<EventTransportSuggestion, 'originStation' | 'destinationStation'>,
  direction: EventTransportDirection,
  train: EventTrainOption,
  boardingQualifier?: string,
): string {
  const target = direction === 'outbound' ? suggestion.destinationStation : suggestion.originStation;
  const targetSlug = stationFileSlug(target);
  const qualifier = String(boardingQualifier || '').toLocaleLowerCase('en-US').replace(/[^a-z0-9]+/gu, '-').replace(/^-|-$/gu, '');
  const number = train.number.toLocaleLowerCase('en-US').replace(/[^a-z0-9]+/gu, '-').replace(/^-|-$/gu, '') || 'service';
  const date = train.serviceDate.replace(/-/gu, '');
  return ['rzd', targetSlug, qualifier, date, number].filter(Boolean).join('-');
}

export function eventTransportCalendarPath(
  event: Pick<PreviewEvent, 'slug'>,
  suggestion: Pick<EventTransportSuggestion, 'originStation' | 'destinationStation'>,
  direction: EventTransportDirection,
  train: EventTrainOption,
  boardingQualifier?: string,
): string {
  return `/sobytiya/${event.slug}/transport/${eventTransportTripKey(suggestion, direction, train, boardingQualifier)}.ics`;
}

export function eventTransportCalendarEntries(suggestion: EventTransportSuggestion): EventTransportCalendarEntry[] {
  const entries: EventTransportCalendarEntry[] = [
    ...suggestion.outbound.map((train) => ({ direction: 'outbound' as const, train })),
    ...(suggestion.eventEndBasis === 'schedule_cutoff'
      ? [suggestion.lastSameDayReturn]
        .filter((train): train is EventTrainOption => Boolean(train))
        .map((train) => ({ direction: 'return' as const, train }))
      : suggestion.returns.map((train) => ({ direction: 'return' as const, train }))),
  ];
  const unique = new Map(entries.map((entry) => [eventTransportTripKey(suggestion, entry.direction, entry.train), entry]));
  if (unique.size > 6) throw new Error(`Transport ICS hard budget exceeded: ${unique.size} files`);
  return [...unique.values()];
}

function getRoute(city: string | null | undefined): TransportRouteRecord | null {
  const target = normalizeCity(city);
  if (!target) return null;
  return schedules.routes.find((route) => normalizeCity(route.city) === target) || null;
}

export function getEventTransportSuggestion(
  event: Pick<PreviewEvent, 'id' | 'city' | 'venue_name' | 'start_date' | 'end_date' | 'start_time' | 'time_range_end' | 'event_type' | 'transport_end_basis'>,
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

  const normalizedType = normalizeEventType(event.event_type);
  const eventEndRaw = timeToMinutes(event.time_range_end);
  const eventEndBasis: EventEndBasis = eventEndRaw !== null
    ? event.transport_end_basis === 'forecast' ? 'forecast' : 'explicit'
    : 'schedule_cutoff';
  let eventEndMinutes = eventEndRaw;
  if (eventEndMinutes !== null && eventEndMinutes < eventStartMinutes) eventEndMinutes += 24 * 60;
  const accessProfile = returnAccessProfile(event);
  const returnReadyMinutes = eventEndMinutes === null ? null : eventEndMinutes + accessProfile.minutes;
  const earliestReturnMinutes = returnReadyMinutes ?? Number.POSITIVE_INFINITY;
  const nextDate = addIsoDays(event.start_date, 1);
  const sameDayCovered = calendarCovers(route.return, event.start_date);
  const nextDayCovered = calendarCovers(route.return, nextDate);
  const returnCalendarCovered = eventEndBasis === 'explicit'
    ? sameDayCovered && nextDayCovered
    : sameDayCovered;
  const sameDayRunning = route.return
    .filter((train) => serviceRunsOn(train, event.start_date))
    .map((train) => ({ train, departureMinutes: timeToMinutes(train.departure) }))
    .filter((item): item is { train: TransportTrainRecord; departureMinutes: number } => item.departureMinutes !== null)
    .sort((left, right) => left.departureMinutes - right.departureMinutes);
  const nextDayRunning = nextDate ? route.return
    .filter((train) => serviceRunsOn(train, nextDate))
    .map((train) => ({ train, departureMinutes: timeToMinutes(train.departure) }))
    .filter((item): item is { train: TransportTrainRecord; departureMinutes: number } => item.departureMinutes !== null)
    .sort((left, right) => left.departureMinutes - right.departureMinutes) : [];
  const lastSameDayRecord = sameDayRunning.at(-1) || null;
  const firstNightRecord = nextDayRunning.find((item) => item.departureMinutes < 3 * 60) || null;
  const firstNextDayRecord = nextDayRunning[0] || null;
  const lastSameDayReturn = lastSameDayRecord ? toOption(lastSameDayRecord.train, event.start_date, false, {}) : null;
  const firstNightReturn = eventEndBasis === 'explicit' && firstNightRecord && nextDate
    ? toOption(firstNightRecord.train, nextDate, true, {})
    : null;
  const firstNextDayReturn = eventEndBasis === 'explicit' && firstNextDayRecord && nextDate
    ? toOption(firstNextDayRecord.train, nextDate, true, {})
    : null;
  const returnCandidates: Array<{ train: TransportTrainRecord; serviceDate: string; departureMinutes: number; nextDay: boolean }> = [];
  if (eventEndMinutes !== null) {
    for (const train of route.return) {
      const sameDayDeparture = timeToMinutes(train.departure);
      if (sameDayDeparture !== null && serviceRunsOn(train, event.start_date)) {
        returnCandidates.push({ train, serviceDate: event.start_date, departureMinutes: sameDayDeparture, nextDay: false });
      }
      if (eventEndBasis === 'explicit' && sameDayDeparture !== null && nextDate && serviceRunsOn(train, nextDate)) {
        returnCandidates.push({ train, serviceDate: nextDate, departureMinutes: sameDayDeparture + 24 * 60, nextDay: true });
      }
    }
  }
  const returns = eventEndMinutes === null
    ? []
    : returnCandidates
      .map((item) => ({ ...item, wait: item.departureMinutes - eventEndMinutes }))
      .filter(({ departureMinutes, wait }) => departureMinutes >= earliestReturnMinutes && wait <= schedules.selection.max_return_wait_minutes)
      .sort((left, right) => left.wait - right.wait || left.departureMinutes - right.departureMinutes)
      .slice(0, optionLimit)
      .map(({ train, serviceDate, nextDay: isNextDay, wait }) => toOption(train, serviceDate, isNextDay, { waitAfterEventMinutes: wait }));

  if (outbound.length === 0 && returns.length === 0 && !(eventEndBasis === 'schedule_cutoff' && lastSameDayReturn)) return null;

  return {
    city: route.city,
    originStation: route.origin_station,
    destinationStation: route.destination_station,
    outboundLiveUrl: route.outbound_live_url,
    returnLiveUrl: route.return_live_url,
    eventStart: event.start_time || '',
    eventEnd: event.time_range_end || null,
    eventEndBasis,
    returnAccessMinutes: accessProfile.minutes,
    returnAccessLabel: accessProfile.label,
    returnReadyTime: returnReadyMinutes === null ? null : clockFromMinutes(returnReadyMinutes),
    eventTypeGenitive: EVENT_TYPE_GENITIVE[normalizedType] || 'события',
    outbound,
    returns,
    lastSameDayReturn,
    firstNightReturn,
    firstNextDayReturn,
    returnCalendarCovered,
    arrivalWindow: { min: minArrivalLead, max: maxArrivalLead },
    maxReturnWaitMinutes: schedules.selection.max_return_wait_minutes,
    scheduleSource: schedules.source.schedule,
    scheduleRetrievedAt: schedules.source.retrieved_at,
    scheduleNote: schedules.source.note,
  };
}
