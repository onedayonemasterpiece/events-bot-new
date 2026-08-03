export type ConnectivityProbeState =
  | 'ok'
  | 'http_error'
  | 'timeout'
  | 'network_error'
  | 'not_configured';

export type ConnectivityTransportRoute = 'direct' | 'relay';
export type ConnectivityAvailability = 'available' | 'partial' | 'unavailable' | 'not_configured';
export type ConnectivityDiagnosticSeverity = 'ok' | 'degraded' | 'blocked';
export type ConnectivityDiagnosticCode =
  | 'CORE_AVAILABLE_BOTH'
  | 'CORE_AVAILABLE_YDB_DEGRADED'
  | 'CORE_AVAILABLE_DIRECT_YANDEX_DEGRADED'
  | 'CORE_AVAILABLE_RELAY_DIRECT_DEGRADED'
  | 'CORE_AVAILABLE_ROUTE_DEGRADED'
  | 'CORE_AVAILABLE_DIAGNOSTIC_INCONSISTENT'
  | 'CORE_PARTIALLY_AVAILABLE'
  | 'CORE_UNAVAILABLE'
  | 'DEVICE_OFFLINE'
  | 'CONFIGURATION_INCOMPLETE';

export interface ConnectivityAttempt {
  state: ConnectivityProbeState;
  status: number | null;
  elapsedMs: number;
  bytes: number | null;
  route?: ConnectivityTransportRoute | null;
}

export interface ConnectivityProbeResult {
  id: string;
  label: string;
  state: ConnectivityProbeState;
  status: number | null;
  attempts: ConnectivityAttempt[];
  minMs: number | null;
  medianMs: number | null;
  maxMs: number | null;
  route?: ConnectivityTransportRoute | null;
}

interface ProbeTarget {
  id: string;
  label: string;
  url: string;
  headers?: Record<string, string>;
  mode?: RequestMode;
  acceptOpaque?: boolean;
  routeHint?: ConnectivityTransportRoute;
}

interface RunOptions {
  attempts?: number;
  timeoutMs?: number;
  fetchImpl?: typeof fetch;
  now?: () => number;
}

export interface ConnectivityDiagnosis {
  code: ConnectivityDiagnosticCode;
  severity: ConnectivityDiagnosticSeverity;
  headline: string;
  detail: string;
  routeSummary: string;
  guidance: string;
  core: ConnectivityAvailability;
  direct: ConnectivityAvailability;
  relay: ConnectivityAvailability;
  ydb: ConnectivityAvailability;
  yandexFromDevice: ConnectivityAvailability;
  authRoute: ConnectivityTransportRoute | null;
  dataRoute: ConnectivityTransportRoute | null;
  canContinue: boolean;
  confirmedActionsNeedRepeat: false;
}

interface DiagnoseOptions {
  online?: boolean;
  authRoute?: ConnectivityTransportRoute | null;
  dataRoute?: ConnectivityTransportRoute | null;
}

interface CompactReceiptDetails {
  probeId: string;
  checkedAt: string;
  diagnosis: ConnectivityDiagnosis;
  mode: 'APP' | 'WEB';
  effectiveType?: string;
  serviceWorkerActive?: boolean;
  online?: boolean;
}

const rounded = (value: number): number => Math.round(value * 10) / 10;

const normalizeRoute = (value: string | null | undefined): ConnectivityTransportRoute | null => (
  value === 'direct' || value === 'relay' ? value : null
);

export const summarizeConnectivityAttempts = (
  id: string,
  label: string,
  attempts: ConnectivityAttempt[],
): ConnectivityProbeResult => {
  const timings = attempts
    .filter((item) => item.state !== 'not_configured')
    .map((item) => item.elapsedMs)
    .sort((left, right) => left - right);
  const middle = Math.floor(timings.length / 2);
  const median = timings.length === 0
    ? null
    : timings.length % 2 === 1
      ? timings[middle]
      : (timings[middle - 1] + timings[middle]) / 2;
  const failed = attempts.find((item) => item.state !== 'ok');
  const last = attempts.at(-1);
  const route = [...attempts]
    .reverse()
    .find((item) => item.state === 'ok' && item.route)?.route
    ?? [...attempts].reverse().find((item) => item.route)?.route
    ?? null;
  return {
    id,
    label,
    state: failed?.state || last?.state || 'not_configured',
    status: failed?.status ?? last?.status ?? null,
    attempts,
    minMs: timings.length ? rounded(timings[0]) : null,
    medianMs: median === null ? null : rounded(median),
    maxMs: timings.length ? rounded(timings.at(-1) as number) : null,
    route,
  };
};

const runAttempt = async (
  target: ProbeTarget,
  timeoutMs: number,
  fetchImpl: typeof fetch,
  now: () => number,
): Promise<ConnectivityAttempt> => {
  if (!target.url) {
    return {
      state: 'not_configured',
      status: null,
      elapsedMs: 0,
      bytes: null,
      route: target.routeHint || null,
    };
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort('connectivity_probe_timeout'), timeoutMs);
  const startedAt = now();
  try {
    const response = await fetchImpl(target.url, {
      method: 'GET',
      headers: target.headers,
      mode: target.mode,
      cache: 'no-store',
      credentials: 'omit',
      signal: controller.signal,
    });
    const route = normalizeRoute(response.headers?.get?.('x-ke-transport-route')) || target.routeHint || null;
    const bytes = (await response.arrayBuffer()).byteLength;
    const acceptedOpaque = target.acceptOpaque === true && response.type === 'opaque';
    return {
      state: response.ok || acceptedOpaque ? 'ok' : 'http_error',
      status: acceptedOpaque ? null : response.status,
      elapsedMs: rounded(now() - startedAt),
      bytes,
      route,
    };
  } catch (error) {
    const aborted = controller.signal.aborted
      || (error instanceof DOMException && error.name === 'AbortError');
    return {
      state: aborted ? 'timeout' : 'network_error',
      status: null,
      elapsedMs: rounded(now() - startedAt),
      bytes: null,
      route: target.routeHint || null,
    };
  } finally {
    clearTimeout(timer);
  }
};

export const runConnectivityProbe = async (
  target: ProbeTarget,
  options: RunOptions = {},
): Promise<ConnectivityProbeResult> => {
  const count = Math.min(5, Math.max(1, options.attempts ?? 1));
  const timeoutMs = Math.min(25_000, Math.max(1_000, options.timeoutMs ?? 20_000));
  const fetchImpl = options.fetchImpl ?? fetch;
  const now = options.now ?? (() => performance.now());
  const attempts: ConnectivityAttempt[] = [];
  for (let index = 0; index < count; index += 1) {
    attempts.push(await runAttempt(target, timeoutMs, fetchImpl, now));
  }
  return summarizeConnectivityAttempts(target.id, target.label, attempts);
};

const availabilityForPair = (
  first: ConnectivityProbeResult | undefined,
  second: ConnectivityProbeResult | undefined,
): ConnectivityAvailability => {
  const states = [first?.state || 'not_configured', second?.state || 'not_configured'];
  if (states.every((state) => state === 'not_configured')) return 'not_configured';
  const successes = states.filter((state) => state === 'ok').length;
  if (successes === 2) return 'available';
  if (successes === 1) return 'partial';
  return 'unavailable';
};

const availabilityForSingle = (result: ConnectivityProbeResult | undefined): ConnectivityAvailability => {
  if (!result || result.state === 'not_configured') return 'not_configured';
  return result.state === 'ok' ? 'available' : 'unavailable';
};

const routeLabel = (route: ConnectivityTransportRoute | null): string => {
  if (route === 'direct') return 'прямой маршрут';
  if (route === 'relay') return 'резервный маршрут через Yandex';
  return 'маршрут не определён';
};

const routeSummary = (
  authRoute: ConnectivityTransportRoute | null,
  dataRoute: ConnectivityTransportRoute | null,
): string => {
  if (authRoute && authRoute === dataRoute) {
    return `Для входа и данных выбран ${routeLabel(authRoute)}.`;
  }
  if (authRoute || dataRoute) {
    return `Для входа: ${routeLabel(authRoute)}. Для данных: ${routeLabel(dataRoute)}.`;
  }
  return 'Рабочий маршрут не выбран.';
};

const yandexAvailability = (
  relay: ConnectivityAvailability,
  ydb: ConnectivityAvailability,
): ConnectivityAvailability => {
  if (relay === 'not_configured' && ydb === 'not_configured') return 'not_configured';
  if (relay === 'available' && ydb === 'available') return 'available';
  if ((relay === 'unavailable' || relay === 'not_configured')
    && (ydb === 'unavailable' || ydb === 'not_configured')) return 'unavailable';
  return 'partial';
};

export const diagnoseConnectivity = (
  results: ConnectivityProbeResult[],
  options: DiagnoseOptions = {},
): ConnectivityDiagnosis => {
  const byId = new Map(results.map((result) => [result.id, result]));
  const direct = availabilityForPair(byId.get('direct-auth'), byId.get('direct-data'));
  const relay = availabilityForPair(byId.get('relay-auth'), byId.get('relay-data'));
  const core = availabilityForPair(byId.get('framework-auth'), byId.get('framework-data'));
  const ydb = availabilityForSingle(byId.get('ydb-control'));
  const yandexFromDevice = yandexAvailability(relay, ydb);
  const authRoute = byId.get('framework-auth')?.route || options.authRoute || null;
  const dataRoute = byId.get('framework-data')?.route || options.dataRoute || null;
  const selectedRouteSummary = routeSummary(authRoute, dataRoute);

  let code: ConnectivityDiagnosticCode;
  if (options.online === false) {
    code = 'DEVICE_OFFLINE';
  } else if (core === 'not_configured' || direct === 'not_configured') {
    code = 'CONFIGURATION_INCOMPLETE';
  } else if (core === 'available') {
    if (direct === 'available' && relay === 'available') {
      code = ydb === 'available' ? 'CORE_AVAILABLE_BOTH' : 'CORE_AVAILABLE_YDB_DEGRADED';
    } else if (direct === 'available' && relay !== 'available') {
      code = 'CORE_AVAILABLE_DIRECT_YANDEX_DEGRADED';
    } else if (relay === 'available' && direct !== 'available') {
      code = 'CORE_AVAILABLE_RELAY_DIRECT_DEGRADED';
    } else if (direct === 'partial' || relay === 'partial') {
      code = 'CORE_AVAILABLE_ROUTE_DEGRADED';
    } else {
      code = 'CORE_AVAILABLE_DIAGNOSTIC_INCONSISTENT';
    }
  } else if (core === 'partial') {
    code = 'CORE_PARTIALLY_AVAILABLE';
  } else {
    code = 'CORE_UNAVAILABLE';
  }

  const commonPositiveGuidance = 'Можно продолжать пользоваться сайтом. Если конкретное действие уже подтверждено сайтом, повторять его не нужно. Эта проверка не подтверждает доставку отдельного отзыва.';
  const commonFailureGuidance = 'Не нажимайте отправку многократно. Скопируйте строку результата, попробуйте ещё раз позднее или в другой сети и сообщите команде фокус-группы.';

  let severity: ConnectivityDiagnosticSeverity = 'degraded';
  let headline = '';
  let detail = '';
  let guidance = commonPositiveGuidance;
  let canContinue = core === 'available';

  switch (code) {
    case 'CORE_AVAILABLE_BOTH':
      severity = 'ok';
      headline = 'Основные функции и оба маршрута доступны.';
      detail = 'Прямой Supabase, резервный маршрут через Yandex и служебный канал YDB ответили с этого устройства.';
      break;
    case 'CORE_AVAILABLE_YDB_DEGRADED':
      headline = 'Основные функции доступны.';
      detail = 'Прямой и резервный маршруты работают, но служебный канал YDB сейчас не ответил. Это не подтверждает потерю входа или сохранённых действий.';
      break;
    case 'CORE_AVAILABLE_DIRECT_YANDEX_DEGRADED':
      headline = 'Основные функции доступны напрямую.';
      detail = ydb === 'available'
        ? 'Прямой Supabase отвечает, а резервный маршрут через Yandex с этого устройства сейчас не отвечает. Служебный канал YDB доступен, поэтому это не общий отказ всех сервисов Yandex.'
        : 'Прямой Supabase отвечает. Резервный маршрут через Yandex и служебный канал YDB с этого устройства сейчас не отвечают. Это не является доказательством глобального сбоя Yandex Cloud.';
      break;
    case 'CORE_AVAILABLE_RELAY_DIRECT_DEGRADED':
      headline = 'Основные функции доступны через резервный маршрут.';
      detail = 'Прямой Supabase с этого устройства не ответил, поэтому вход и данные обслуживаются через Yandex relay.';
      break;
    case 'CORE_AVAILABLE_ROUTE_DEGRADED':
      headline = 'Основные функции доступны, один из маршрутов работает частично.';
      detail = 'У входа и данных различается доступность отдельных путей. Сайт использует маршрут, который подтвердил конкретную операцию.';
      break;
    case 'CORE_AVAILABLE_DIAGNOSTIC_INCONSISTENT':
      headline = 'Основные функции доступны, но контрольные проверки расходятся.';
      detail = 'Рабочие запросы завершились, а отдельные прямые проверки не подтвердили маршрут. Нужна строка результата для точной диагностики.';
      break;
    case 'CORE_PARTIALLY_AVAILABLE':
      severity = 'blocked';
      headline = 'Часть основных функций не подтвердилась.';
      detail = 'Вход или доступ к данным не завершил проверку. Не считайте незавершённое действие отправленным без отдельного подтверждения на его экране.';
      guidance = commonFailureGuidance;
      canContinue = false;
      break;
    case 'CORE_UNAVAILABLE':
      severity = 'blocked';
      headline = 'Основное соединение не подтвердилось.';
      detail = 'Ни прямой, ни устойчивый путь не подтвердил одновременно вход и данные с этого устройства.';
      guidance = commonFailureGuidance;
      canContinue = false;
      break;
    case 'DEVICE_OFFLINE':
      severity = 'blocked';
      headline = 'Устройство сейчас без подключения к сети.';
      detail = 'Браузер сообщает offline-состояние. Проверка маршрутов не может быть достоверной до восстановления сети.';
      guidance = 'Включите сеть, затем повторите проверку. Уже подтверждённые сайтом действия повторять не нужно.';
      canContinue = false;
      break;
    case 'CONFIGURATION_INCOMPLETE':
      severity = 'blocked';
      headline = 'Диагностическая страница настроена не полностью.';
      detail = 'Не хватает публичной конфигурации одного из обязательных маршрутов. Это дефект сборки, а не устройства участника.';
      guidance = 'Скопируйте строку результата и сообщите команде. Не пытайтесь исправлять настройки устройства.';
      canContinue = false;
      break;
  }

  return {
    code,
    severity,
    headline,
    detail,
    routeSummary: selectedRouteSummary,
    guidance,
    core,
    direct,
    relay,
    ydb,
    yandexFromDevice,
    authRoute,
    dataRoute,
    canContinue,
    confirmedActionsNeedRepeat: false,
  };
};

const compactProbe = (result: ConnectivityProbeResult | undefined): string => {
  if (!result) return 'NC';
  const state = {
    ok: 'OK',
    http_error: `HTTP${result.status || 0}`,
    timeout: 'TO',
    network_error: 'NET',
    not_configured: 'NC',
  }[result.state];
  const timing = result.medianMs === null ? '' : `/${Math.round(result.medianMs)}`;
  const route = result.route === 'direct' ? '@D' : result.route === 'relay' ? '@R' : '';
  return `${state}${timing}${route}`;
};

export const makeCompactConnectivityReceipt = (
  results: ConnectivityProbeResult[],
  details: CompactReceiptDetails,
): string => {
  const byId = new Map(results.map((result) => [result.id, result]));
  const quality = String(details.effectiveType || 'unknown').toUpperCase().replace(/[^A-Z0-9-]/gu, '').slice(0, 16) || 'UNKNOWN';
  const routeCode = (route: ConnectivityTransportRoute | null) => route === 'direct' ? 'D' : route === 'relay' ? 'R' : 'N';
  return [
    'KE5',
    `ID=${details.probeId}`,
    `AT=${details.checkedAt}`,
    `CODE=${details.diagnosis.code}`,
    `DA=${compactProbe(byId.get('direct-auth'))}`,
    `DD=${compactProbe(byId.get('direct-data'))}`,
    `RA=${compactProbe(byId.get('relay-auth'))}`,
    `RD=${compactProbe(byId.get('relay-data'))}`,
    `FA=${compactProbe(byId.get('framework-auth'))}`,
    `FD=${compactProbe(byId.get('framework-data'))}`,
    `YC=${compactProbe(byId.get('ydb-control'))}`,
    `PATHA=${routeCode(details.diagnosis.authRoute)}`,
    `PATHD=${routeCode(details.diagnosis.dataRoute)}`,
    `MODE=${details.mode}`,
    `QUALITY=${quality}`,
    `ONLINE=${details.online === false ? 0 : 1}`,
    `PWA=${details.serviceWorkerActive ? 1 : 0}`,
  ].join(' ');
};

export const makeConnectivityReceipt = (
  results: ConnectivityProbeResult[],
  details: {
    origin: string;
    online: boolean;
    effectiveType?: string;
    standalone?: boolean;
    checkedAt?: string;
  },
) => ({
  schema: 'kenigevents.focus_connectivity.v1',
  checked_at: details.checkedAt || new Date().toISOString(),
  origin: details.origin,
  online: details.online,
  effective_type: String(details.effectiveType || 'unknown').slice(0, 16),
  standalone: details.standalone === true,
  probes: results.map((result) => ({
    id: result.id,
    state: result.state,
    status: result.status,
    route: result.route || null,
    min_ms: result.minMs,
    median_ms: result.medianMs,
    max_ms: result.maxMs,
    attempts: result.attempts.map((attempt) => ({
      state: attempt.state,
      status: attempt.status,
      elapsed_ms: attempt.elapsedMs,
      bytes: attempt.bytes,
      route: attempt.route || null,
    })),
  })),
});
