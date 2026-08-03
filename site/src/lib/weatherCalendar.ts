export const WEATHER_CALENDAR_SCHEMA = 'weather-calendar-v1' as const;
export const WEATHER_CALENDAR_POINTER_SCHEMA = 'weather-calendar-pointer-v1' as const;
export const WEATHER_CALENDAR_TIMEZONE = 'Europe/Kaliningrad' as const;
export const WEATHER_CALENDAR_PROVIDER = 'Open-Meteo' as const;
export const WEATHER_CALENDAR_ATTRIBUTION_URL = 'https://open-meteo.com/' as const;
export const WEATHER_CALENDAR_HORIZON_DAYS = 7;
export const WEATHER_CALENDAR_WATER_THRESHOLD_C = 16.0;
export const WEATHER_POINTER_MAX_BYTES = 4 * 1024;
export const WEATHER_SNAPSHOT_MAX_BYTES = 64 * 1024;

export type WeatherLocationStatus = 'fresh' | 'degraded';
export type WeatherRouteKind = 'today' | 'tomorrow' | 'date' | 'weekend';
export type WeatherIconName =
  | 'clear'
  | 'cloud'
  | 'showers'
  | 'fog'
  | 'rain'
  | 'heavy-rain'
  | 'snow'
  | 'thunderstorm';

export interface WeatherProvider {
  name: typeof WEATHER_CALENDAR_PROVIDER;
  attribution_url: typeof WEATHER_CALENDAR_ATTRIBUTION_URL;
}

export interface WeatherAirLocation {
  status: WeatherLocationStatus;
  temperature_day_min_c?: number;
  temperature_day_max_c?: number;
  weather_code?: number;
  wind_day_max_m_s?: number;
  source_updated_at: string;
}

export interface WeatherCoastLocation extends WeatherAirLocation {
  sea_surface_temperature_c?: number;
  wave_height_day_max_m?: number;
  show_water_temperature?: boolean;
}

export interface WeatherCalendarDay {
  date: string;
  kaliningrad?: WeatherAirLocation;
  coast?: WeatherCoastLocation;
}

export interface WeatherCalendarError {
  scope: string;
  code: string;
  message?: string;
}

export interface WeatherCalendarSnapshot {
  schema: typeof WEATHER_CALENDAR_SCHEMA;
  snapshot_id: string;
  generated_at: string;
  valid_until: string;
  timezone: typeof WEATHER_CALENDAR_TIMEZONE;
  provider: WeatherProvider;
  location_revision: string;
  days: WeatherCalendarDay[];
  errors: WeatherCalendarError[];
}

export interface WeatherCalendarPointer {
  schema: typeof WEATHER_CALENDAR_POINTER_SCHEMA;
  snapshot_id: string;
  snapshot_url: string;
  sha256: string;
  updated_at: string;
}

export interface VisibleWeatherDay {
  date: string;
  quality: WeatherLocationStatus;
  kaliningrad?: WeatherAirLocation;
  coast?: WeatherCoastLocation;
}

const SNAPSHOT_KEYS = ['days', 'errors', 'generated_at', 'location_revision', 'provider', 'schema', 'snapshot_id', 'timezone', 'valid_until'] as const;
const POINTER_KEYS = ['schema', 'sha256', 'snapshot_id', 'snapshot_url', 'updated_at'] as const;
const PROVIDER_KEYS = ['attribution_url', 'name'] as const;
const DAY_KEYS = ['coast', 'date', 'kaliningrad'] as const;
const AIR_KEYS = ['source_updated_at', 'status', 'temperature_day_max_c', 'temperature_day_min_c', 'weather_code', 'wind_day_max_m_s'] as const;
const COAST_KEYS = [...AIR_KEYS, 'sea_surface_temperature_c', 'show_water_temperature', 'wave_height_day_max_m'] as const;
const ERROR_KEYS = ['code', 'message', 'scope'] as const;
const WMO_CODES = new Set([0, 1, 2, 3, 45, 48, 51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 71, 73, 75, 77, 80, 81, 82, 85, 86, 95, 96, 99]);

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : null;
}

function exactKeys(value: Record<string, unknown>, allowed: readonly string[]): boolean {
  const set = new Set(allowed);
  return Object.keys(value).every((key) => set.has(key));
}

function validIsoInstant(value: unknown): value is string {
  return typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T/u.test(value) && Number.isFinite(Date.parse(value));
}

function validDate(value: unknown): value is string {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/u.test(value)) return false;
  const [year, month, day] = value.split('-').map(Number);
  const parsed = new Date(Date.UTC(year, month - 1, day));
  return parsed.getUTCFullYear() === year && parsed.getUTCMonth() === month - 1 && parsed.getUTCDate() === day;
}

function boundedNumber(value: unknown, min: number, max: number): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value >= min && value <= max;
}

function optionalBoundedNumber(value: unknown, min: number, max: number): boolean {
  return value === undefined || boundedNumber(value, min, max);
}

function parseLocation(value: unknown, coast: boolean): WeatherAirLocation | WeatherCoastLocation | null {
  const source = record(value);
  const keys = coast ? COAST_KEYS : AIR_KEYS;
  if (!source || !exactKeys(source, keys)) return null;
  if (source.status !== 'fresh' && source.status !== 'degraded') return null;
  if (!validIsoInstant(source.source_updated_at)) return null;
  if (!optionalBoundedNumber(source.temperature_day_min_c, -80, 60)) return null;
  if (!optionalBoundedNumber(source.temperature_day_max_c, -80, 60)) return null;
  const hasMin = source.temperature_day_min_c !== undefined;
  const hasMax = source.temperature_day_max_c !== undefined;
  if (hasMin !== hasMax) return null;
  if (hasMin && Number(source.temperature_day_min_c) > Number(source.temperature_day_max_c)) return null;
  if (source.weather_code !== undefined && (!Number.isInteger(source.weather_code) || !WMO_CODES.has(Number(source.weather_code)))) return null;
  if (!optionalBoundedNumber(source.wind_day_max_m_s, 0, 120)) return null;

  if (!coast) {
    if (!hasMin && source.weather_code === undefined) return null;
    return source as unknown as WeatherAirLocation;
  }

  if (!optionalBoundedNumber(source.sea_surface_temperature_c, -5, 40)) return null;
  if (!optionalBoundedNumber(source.wave_height_day_max_m, 0, 30)) return null;
  if (source.show_water_temperature !== undefined && typeof source.show_water_temperature !== 'boolean') return null;
  const hasSea = source.sea_surface_temperature_c !== undefined;
  if (hasSea !== (source.show_water_temperature !== undefined)) return null;
  if (hasSea && source.show_water_temperature !== shouldShowWaterTemperature(Number(source.sea_surface_temperature_c))) return null;
  if (!hasMin && source.weather_code === undefined && !hasSea) return null;
  return source as unknown as WeatherCoastLocation;
}

export function parseWeatherCalendarSnapshot(value: unknown): WeatherCalendarSnapshot | null {
  const source = record(value);
  if (!source || !exactKeys(source, SNAPSHOT_KEYS)) return null;
  if (source.schema !== WEATHER_CALENDAR_SCHEMA) return null;
  if (typeof source.snapshot_id !== 'string' || !/^weather-[A-Za-z0-9._-]{8,120}$/u.test(source.snapshot_id)) return null;
  if (!validIsoInstant(source.generated_at) || !validIsoInstant(source.valid_until)) return null;
  if (Date.parse(source.valid_until) <= Date.parse(source.generated_at)) return null;
  if (source.timezone !== WEATHER_CALENDAR_TIMEZONE) return null;
  if (typeof source.location_revision !== 'string' || !/^[A-Za-z0-9._:-]{8,160}$/u.test(source.location_revision)) return null;
  const provider = record(source.provider);
  if (!provider || !exactKeys(provider, PROVIDER_KEYS) || provider.name !== WEATHER_CALENDAR_PROVIDER || provider.attribution_url !== WEATHER_CALENDAR_ATTRIBUTION_URL) return null;
  if (!Array.isArray(source.days) || source.days.length < 1 || source.days.length > WEATHER_CALENDAR_HORIZON_DAYS) return null;
  const days: WeatherCalendarDay[] = [];
  const seenDates = new Set<string>();
  for (const item of source.days) {
    const day = record(item);
    if (!day || !exactKeys(day, DAY_KEYS) || !validDate(day.date) || seenDates.has(day.date)) return null;
    const kaliningrad = day.kaliningrad === undefined ? undefined : parseLocation(day.kaliningrad, false) as WeatherAirLocation | null;
    const coast = day.coast === undefined ? undefined : parseLocation(day.coast, true) as WeatherCoastLocation | null;
    if (kaliningrad === null || coast === null || (!kaliningrad && !coast)) return null;
    seenDates.add(day.date);
    days.push({ date: day.date, ...(kaliningrad ? { kaliningrad } : {}), ...(coast ? { coast } : {}) });
  }
  for (let index = 1; index < days.length; index += 1) if (days[index - 1].date >= days[index].date) return null;
  if (!Array.isArray(source.errors) || source.errors.length > 32) return null;
  const errors: WeatherCalendarError[] = [];
  for (const item of source.errors) {
    const error = record(item);
    if (!error || !exactKeys(error, ERROR_KEYS)) return null;
    if (typeof error.scope !== 'string' || error.scope.length < 1 || error.scope.length > 80) return null;
    if (typeof error.code !== 'string' || !/^[A-Z0-9_:-]{2,80}$/u.test(error.code)) return null;
    if (error.message !== undefined && (typeof error.message !== 'string' || error.message.length > 240)) return null;
    errors.push(error as unknown as WeatherCalendarError);
  }
  return {
    schema: WEATHER_CALENDAR_SCHEMA,
    snapshot_id: source.snapshot_id,
    generated_at: source.generated_at,
    valid_until: source.valid_until,
    timezone: WEATHER_CALENDAR_TIMEZONE,
    provider: { name: WEATHER_CALENDAR_PROVIDER, attribution_url: WEATHER_CALENDAR_ATTRIBUTION_URL },
    location_revision: source.location_revision,
    days,
    errors,
  };
}

export function parseWeatherCalendarPointer(value: unknown): WeatherCalendarPointer | null {
  const source = record(value);
  if (!source || !exactKeys(source, POINTER_KEYS)) return null;
  if (source.schema !== WEATHER_CALENDAR_POINTER_SCHEMA) return null;
  if (typeof source.snapshot_id !== 'string' || !/^weather-[A-Za-z0-9._-]{8,120}$/u.test(source.snapshot_id)) return null;
  if (typeof source.sha256 !== 'string' || !/^[0-9a-f]{64}$/u.test(source.sha256)) return null;
  if (typeof source.snapshot_url !== 'string' || !/^\/[^?#]*data\/weather\/v1\/snapshots\/[0-9a-f]{64}\.json$/u.test(source.snapshot_url)) return null;
  if (!source.snapshot_url.endsWith(`/${source.sha256}.json`)) return null;
  if (!validIsoInstant(source.updated_at)) return null;
  return source as unknown as WeatherCalendarPointer;
}

export function roundHalfUpOneDecimal(value: number): number {
  return Math.round((value + Number.EPSILON) * 10) / 10;
}

export function shouldShowWaterTemperature(value: number): boolean {
  return Number.isFinite(value) && roundHalfUpOneDecimal(value) > WEATHER_CALENDAR_WATER_THRESHOLD_C;
}

export function kaliningradDate(now: Date): string {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: WEATHER_CALENDAR_TIMEZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(now);
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${values.year}-${values.month}-${values.day}`;
}

export function calendarDayDistance(fromDate: string, toDate: string): number | null {
  if (!validDate(fromDate) || !validDate(toDate)) return null;
  const toUtc = Date.UTC(Number(toDate.slice(0, 4)), Number(toDate.slice(5, 7)) - 1, Number(toDate.slice(8, 10)));
  const fromUtc = Date.UTC(Number(fromDate.slice(0, 4)), Number(fromDate.slice(5, 7)) - 1, Number(fromDate.slice(8, 10)));
  return Math.round((toUtc - fromUtc) / 86_400_000);
}

function freshLocation<T extends WeatherAirLocation>(location: T | undefined, targetDistance: number, nowMs: number): T | undefined {
  if (!location) return undefined;
  const sourceMs = Date.parse(location.source_updated_at);
  const maximumAgeMs = (targetDistance === 0 ? 3 : 6) * 60 * 60 * 1000;
  if (!Number.isFinite(sourceMs) || sourceMs > nowMs + 5 * 60 * 1000 || nowMs - sourceMs > maximumAgeMs) return undefined;
  return location;
}

export function selectVisibleWeatherDay(
  snapshot: WeatherCalendarSnapshot,
  targetDate: string,
  routeKind: WeatherRouteKind,
  now: Date = new Date(),
): VisibleWeatherDay | null {
  const nowMs = now.getTime();
  if (!Number.isFinite(nowMs) || Date.parse(snapshot.generated_at) > nowMs + 5 * 60 * 1000 || Date.parse(snapshot.valid_until) <= nowMs) return null;
  const today = kaliningradDate(now);
  const distance = calendarDayDistance(today, targetDate);
  if (distance === null || distance < 0 || distance >= WEATHER_CALENDAR_HORIZON_DAYS) return null;
  if (routeKind === 'today' && distance !== 0) return null;
  if (routeKind === 'tomorrow' && distance !== 1) return null;
  const day = snapshot.days.find((candidate) => candidate.date === targetDate);
  if (!day) return null;
  const kaliningrad = freshLocation(day.kaliningrad, distance, nowMs);
  const coast = freshLocation(day.coast, distance, nowMs);
  if (!kaliningrad && !coast) return null;
  return {
    date: targetDate,
    quality: kaliningrad?.status === 'degraded' || coast?.status === 'degraded' ? 'degraded' : 'fresh',
    ...(kaliningrad ? { kaliningrad } : {}),
    ...(coast ? { coast } : {}),
  };
}

export function weatherCondition(code: number | undefined): { label: string; icon: WeatherIconName } | null {
  if (code === undefined) return null;
  if (code === 0) return { label: 'Ясно', icon: 'clear' };
  if (code >= 1 && code <= 3) return { label: code === 3 ? 'Облачно' : 'Переменная облачность', icon: 'cloud' };
  if (code === 45 || code === 48) return { label: 'Туман', icon: 'fog' };
  if ([51, 53, 55, 56, 57].includes(code)) return { label: 'Морось', icon: 'rain' };
  if ([61, 63, 66, 67].includes(code)) return { label: 'Дождь', icon: 'rain' };
  if ([65, 80, 81, 82].includes(code)) return { label: 'Ливень', icon: code >= 80 ? 'showers' : 'heavy-rain' };
  if ([71, 73, 75, 77, 85, 86].includes(code)) return { label: 'Снег', icon: 'snow' };
  if ([95, 96, 99].includes(code)) return { label: 'Гроза', icon: 'thunderstorm' };
  return null;
}

export function formatAirRange(location: WeatherAirLocation): string | null {
  if (location.temperature_day_min_c === undefined || location.temperature_day_max_c === undefined) return null;
  const signed = (value: number) => `${Math.round(value) > 0 ? '+' : ''}${Math.round(value)}`;
  return `${signed(location.temperature_day_min_c)}…${signed(location.temperature_day_max_c)}°`;
}

export function formatWaterTemperature(location: WeatherCoastLocation): string | null {
  if (location.sea_surface_temperature_c === undefined || location.show_water_temperature !== true) return null;
  const value = roundHalfUpOneDecimal(location.sea_surface_temperature_c);
  if (value <= WEATHER_CALENDAR_WATER_THRESHOLD_C) return null;
  return `${value > 0 ? '+' : ''}${value.toFixed(1).replace('.', ',')}°`;
}
