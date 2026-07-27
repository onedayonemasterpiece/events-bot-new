export interface MobileDiscoveryBases {
  calendarBase?: string;
  searchBase?: string;
}

function cleanBase(value: string | undefined) {
  return String(value || '').replace(/\/+$/u, '');
}

function append(base: string, path: string) {
  return `${base}${path.startsWith('/') ? path : `/${path}`}`;
}

const CALENDAR_ROUTES = /^\/(?:segodnya|zavtra|vyhodnye(?:-[^/]+)?|populyarnoe|dlya-menya)(?:\/|$)/u;
const SEARCH_ROUTES = /^\/(?:poisk|podborki)(?:\/|$)/u;

/** Resolve every mobile discovery entry through the same preview composition. */
export function mobileDiscoveryHref(path: string, bases: MobileDiscoveryBases = {}, localBase = '') {
  const calendarBase = cleanBase(bases.calendarBase);
  const searchBase = cleanBase(bases.searchBase);
  if (SEARCH_ROUTES.test(path) && searchBase) return append(searchBase, path);
  if ((path === '/' || CALENDAR_ROUTES.test(path)) && calendarBase) return append(calendarBase, path);
  const base = cleanBase(localBase);
  return append(base, path);
}
