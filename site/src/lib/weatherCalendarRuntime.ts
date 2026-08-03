import {
  WEATHER_CALENDAR_ATTRIBUTION_URL,
  WEATHER_CALENDAR_PROVIDER,
  WEATHER_POINTER_MAX_BYTES,
  WEATHER_SNAPSHOT_MAX_BYTES,
  formatAirRange,
  formatWaterTemperature,
  parseWeatherCalendarPointer,
  parseWeatherCalendarSnapshot,
  selectVisibleWeatherDay,
  weatherCondition,
  type VisibleWeatherDay,
  type WeatherAirLocation,
  type WeatherCalendarSnapshot,
  type WeatherCoastLocation,
  type WeatherRouteKind,
} from './weatherCalendar.ts';

export type WeatherCalendarLoadFailure =
  | 'pointer_url_invalid'
  | 'pointer_unavailable'
  | 'pointer_too_large'
  | 'pointer_invalid'
  | 'snapshot_url_invalid'
  | 'snapshot_unavailable'
  | 'snapshot_too_large'
  | 'snapshot_integrity_mismatch'
  | 'snapshot_invalid'
  | 'snapshot_id_mismatch';

export type WeatherCalendarLoadResult =
  | { ok: true; snapshot: WeatherCalendarSnapshot }
  | { ok: false; reason: WeatherCalendarLoadFailure };

interface LoadOptions {
  pointerUrl: string;
  pageUrl?: string;
  fetchImpl?: typeof fetch;
  cryptoImpl?: Crypto;
}

function safeJson(text: string): unknown {
  try { return JSON.parse(text); } catch { return null; }
}

function byteLength(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

async function sha256(value: string, cryptoImpl: Crypto): Promise<string> {
  const digest = await cryptoImpl.subtle.digest('SHA-256', new TextEncoder().encode(value));
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

function currentPointerUrl(value: string, pageUrl: string): URL | null {
  try {
    const page = new URL(pageUrl);
    const url = new URL(value, page);
    if (url.origin !== page.origin || url.search || url.hash || !/\/data\/weather\/v1\/current\.json$/u.test(url.pathname)) return null;
    return url;
  } catch {
    return null;
  }
}

function immutableSnapshotUrl(value: string, pageUrl: string): URL | null {
  try {
    const page = new URL(pageUrl);
    const url = new URL(value, page);
    if (url.origin !== page.origin || url.search || url.hash || !/\/data\/weather\/v1\/snapshots\/[0-9a-f]{64}\.json$/u.test(url.pathname)) return null;
    return url;
  } catch {
    return null;
  }
}

export async function loadWeatherCalendarSnapshot(options: LoadOptions): Promise<WeatherCalendarLoadResult> {
  const pageUrl = options.pageUrl || (typeof location !== 'undefined' ? location.href : 'https://kenigevents.ru/');
  const pointerUrl = currentPointerUrl(options.pointerUrl, pageUrl);
  if (!pointerUrl) return { ok: false, reason: 'pointer_url_invalid' };
  const fetchImpl = options.fetchImpl || fetch;
  const cryptoImpl = options.cryptoImpl || globalThis.crypto;

  let pointerResponse: Response;
  try {
    pointerResponse = await fetchImpl(pointerUrl, { cache: 'no-store', credentials: 'same-origin', headers: { accept: 'application/json' } });
  } catch {
    return { ok: false, reason: 'pointer_unavailable' };
  }
  if (!pointerResponse.ok) return { ok: false, reason: 'pointer_unavailable' };
  const pointerText = await pointerResponse.text();
  if (byteLength(pointerText) > WEATHER_POINTER_MAX_BYTES) return { ok: false, reason: 'pointer_too_large' };
  const pointer = parseWeatherCalendarPointer(safeJson(pointerText));
  if (!pointer) return { ok: false, reason: 'pointer_invalid' };

  const snapshotUrl = immutableSnapshotUrl(pointer.snapshot_url, pageUrl);
  if (!snapshotUrl) return { ok: false, reason: 'snapshot_url_invalid' };
  let snapshotResponse: Response;
  try {
    snapshotResponse = await fetchImpl(snapshotUrl, { cache: 'force-cache', credentials: 'same-origin', headers: { accept: 'application/json' } });
  } catch {
    return { ok: false, reason: 'snapshot_unavailable' };
  }
  if (!snapshotResponse.ok) return { ok: false, reason: 'snapshot_unavailable' };
  const snapshotText = await snapshotResponse.text();
  if (byteLength(snapshotText) > WEATHER_SNAPSHOT_MAX_BYTES) return { ok: false, reason: 'snapshot_too_large' };
  if (!cryptoImpl?.subtle || await sha256(snapshotText, cryptoImpl) !== pointer.sha256) return { ok: false, reason: 'snapshot_integrity_mismatch' };
  const snapshot = parseWeatherCalendarSnapshot(safeJson(snapshotText));
  if (!snapshot) return { ok: false, reason: 'snapshot_invalid' };
  if (snapshot.snapshot_id !== pointer.snapshot_id) return { ok: false, reason: 'snapshot_id_mismatch' };
  return { ok: true, snapshot };
}

function element<K extends keyof HTMLElementTagNameMap>(documentRef: Document, name: K, className?: string): HTMLElementTagNameMap[K] {
  const node = documentRef.createElement(name);
  if (className) node.className = className;
  return node;
}

function icon(documentRef: Document, base: string, name: string): HTMLElement {
  const node = element(documentRef, 'span', 'weather-date-context__icon');
  node.setAttribute('aria-hidden', 'true');
  node.style.setProperty('--weather-icon', `url("${base}/${name}.svg")`);
  return node;
}

function locationRow(
  documentRef: Document,
  iconBase: string,
  name: string,
  location: WeatherAirLocation | WeatherCoastLocation,
  coast: boolean,
): HTMLElement | null {
  const condition = weatherCondition(location.weather_code);
  const air = formatAirRange(location);
  const water = coast ? formatWaterTemperature(location as WeatherCoastLocation) : null;
  if (!condition && !air && !water) return null;
  const row = element(documentRef, 'div', 'weather-date-context__location');
  row.append(icon(documentRef, iconBase, condition?.icon || (water ? 'water-temperature' : 'cloud')));
  const copy = element(documentRef, 'span', 'weather-date-context__location-copy');
  const label = element(documentRef, 'strong');
  label.textContent = name;
  copy.append(label);
  if (condition) {
    const conditionNode = element(documentRef, 'span', 'weather-date-context__condition');
    conditionNode.textContent = condition.label;
    copy.append(conditionNode);
  }
  row.append(copy);
  if (air) {
    const temperature = element(documentRef, 'b', 'weather-date-context__temperature');
    temperature.textContent = air;
    temperature.setAttribute('aria-label', `температура воздуха ${air.replace('…', ' до ')}`);
    row.append(temperature);
  }
  if (water) {
    const waterNode = element(documentRef, 'span', 'weather-date-context__water');
    waterNode.append(icon(documentRef, iconBase, 'water-temperature'));
    const text = element(documentRef, 'span');
    text.textContent = `вода ${water}`;
    text.setAttribute('aria-label', `температура поверхности воды ${water}`);
    waterNode.append(text);
    row.append(waterNode);
  }
  return row;
}

function updatedAt(day: VisibleWeatherDay): string | null {
  const timestamps = [day.kaliningrad?.source_updated_at, day.coast?.source_updated_at]
    .filter((value): value is string => Boolean(value))
    .map(Date.parse)
    .filter(Number.isFinite);
  if (!timestamps.length) return null;
  return new Intl.DateTimeFormat('ru-RU', {
    timeZone: 'Europe/Kaliningrad',
    hour: '2-digit',
    minute: '2-digit',
  }).format(new Date(Math.min(...timestamps)));
}

export function renderWeatherDateContext(mount: HTMLElement, day: VisibleWeatherDay): void {
  const documentRef = mount.ownerDocument;
  const iconBase = mount.dataset.weatherIconBase || '/assets/weather';
  const body = element(documentRef, 'div', 'weather-date-context__body');
  const city = day.kaliningrad ? locationRow(documentRef, iconBase, 'Калининград', day.kaliningrad, false) : null;
  const coast = day.coast ? locationRow(documentRef, iconBase, 'Побережье', day.coast, true) : null;
  if (city) body.append(city);
  if (coast) body.append(coast);
  if (!body.childElementCount) {
    mount.hidden = true;
    mount.dataset.weatherState = 'empty';
    return;
  }
  const meta = element(documentRef, 'div', 'weather-date-context__meta');
  const forecastLabel = mount.dataset.weatherDateLabel || day.date;
  const time = updatedAt(day);
  const label = element(documentRef, 'span');
  label.textContent = `Прогноз на ${forecastLabel}${time ? ` · обновлён ${time}` : ''}`;
  meta.append(label);
  const attribution = element(documentRef, 'a');
  attribution.href = WEATHER_CALENDAR_ATTRIBUTION_URL;
  attribution.rel = 'external noopener';
  attribution.textContent = WEATHER_CALENDAR_PROVIDER;
  attribution.setAttribute(
    'aria-label',
    'Источник Open-Meteo, данные агрегированы и округлены, лицензия CC BY 4.0',
  );
  attribution.title = 'Данные агрегированы и округлены · CC BY 4.0';
  meta.append(attribution);
  mount.replaceChildren(body, meta);
  mount.dataset.weatherState = 'ready';
  mount.dataset.weatherQuality = day.quality;
  mount.removeAttribute('aria-busy');
  mount.hidden = false;
}

function hideMount(mount: HTMLElement, state: string): void {
  mount.hidden = true;
  mount.dataset.weatherState = state;
  mount.removeAttribute('aria-busy');
}

export async function hydrateWeatherDateContexts(
  documentRef: Document = document,
  now: Date = new Date(),
  load = loadWeatherCalendarSnapshot,
): Promise<void> {
  const mounts = [...documentRef.querySelectorAll<HTMLElement>('[data-weather-date-context][data-weather-enabled="true"]')];
  const eligible = mounts.filter((mount) => {
    const routeKind = mount.dataset.weatherRouteKind as WeatherRouteKind;
    const targetDate = mount.dataset.weatherDate || '';
    // A minimal local snapshot lets the canonical selector own all date guards
    // without issuing a request for obviously ineligible mounts.
    const distanceSnapshot: WeatherCalendarSnapshot = {
      schema: 'weather-calendar-v1', snapshot_id: 'weather-runtime-date-guard', generated_at: now.toISOString(),
      valid_until: new Date(now.getTime() + 60_000).toISOString(), timezone: 'Europe/Kaliningrad',
      provider: { name: 'Open-Meteo', attribution_url: 'https://open-meteo.com/' }, location_revision: 'runtime-date-guard',
      days: [{ date: targetDate, kaliningrad: { status: 'fresh', weather_code: 0, source_updated_at: now.toISOString() } }], errors: [],
    };
    if (!selectVisibleWeatherDay(distanceSnapshot, targetDate, routeKind, now)) {
      hideMount(mount, 'date-ineligible');
      return false;
    }
    return true;
  });
  if (!eligible.length) return;

  const groups = new Map<string, HTMLElement[]>();
  for (const mount of eligible) {
    const pointer = mount.dataset.weatherPointer || '';
    groups.set(pointer, [...(groups.get(pointer) || []), mount]);
  }
  await Promise.all([...groups].map(async ([pointerUrl, group]) => {
    const result = await load({ pointerUrl, pageUrl: documentRef.location?.href });
    if (!result.ok) {
      group.forEach((mount) => hideMount(mount, result.reason));
      return;
    }
    for (const mount of group) {
      const routeKind = mount.dataset.weatherRouteKind as WeatherRouteKind;
      const day = selectVisibleWeatherDay(result.snapshot, mount.dataset.weatherDate || '', routeKind, now);
      if (!day) hideMount(mount, 'weather-unavailable');
      else renderWeatherDateContext(mount, day);
    }
  }));
}
