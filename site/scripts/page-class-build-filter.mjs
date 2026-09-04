import { readFileSync } from 'node:fs';

const PAGE_CLASS_CONTRACT = JSON.parse(readFileSync(
  new URL('./static-site-page-classes.v1.json', import.meta.url),
  'utf8',
));

if (
  PAGE_CLASS_CONTRACT?.schema_version !== 'kenigevents_static_site_page_classes_v1'
  || !PAGE_CLASS_CONTRACT.classes
  || typeof PAGE_CLASS_CONTRACT.classes !== 'object'
  || Array.isArray(PAGE_CLASS_CONTRACT.classes)
  || !Array.isArray(PAGE_CLASS_CONTRACT.always_preview_components)
) {
  throw new Error('Invalid static-site page-class contract');
}

const PAGE_CLASS_COMPONENTS = Object.freeze(PAGE_CLASS_CONTRACT.classes);
const ALWAYS_PREVIEW_COMPONENTS = Object.freeze(PAGE_CLASS_CONTRACT.always_preview_components);

export const STATIC_SITE_PAGE_CLASSES = Object.freeze(Object.keys(PAGE_CLASS_COMPONENTS));
export const FOCUSED_PREVIEW_SUPPORT_ROUTES = Object.freeze(['/robots.txt']);

function normalizedComponent(value) {
  return String(value || '').replaceAll('\\', '/').replace(/^\.\//u, '');
}

function componentMatches(component, patterns) {
  return patterns.some((pattern) => (
    pattern.endsWith('/') ? component.startsWith(pattern) : component === pattern
  ));
}

export function normalizeStaticSitePageClasses(value) {
  const raw = Array.isArray(value) ? value : String(value || '').split(',');
  const selected = [...new Set(raw.flatMap((item) => String(item || '').split(','))
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean))];
  if (!selected.length || selected.includes('all')) {
    if (selected.length > 1) throw new Error('STATIC_SITE_PAGE_CLASSES cannot combine all with named classes');
    return ['all'];
  }
  const unknown = selected.filter((item) => !STATIC_SITE_PAGE_CLASSES.includes(item));
  if (unknown.length) {
    throw new Error(`Unknown STATIC_SITE_PAGE_CLASSES: ${unknown.join(', ')}; expected ${STATIC_SITE_PAGE_CLASSES.join(', ')}`);
  }
  return selected;
}

export function pageClassForComponent(value) {
  const component = normalizedComponent(value);
  if (componentMatches(component, ALWAYS_PREVIEW_COMPONENTS)) return 'shell';
  for (const [pageClass, patterns] of Object.entries(PAGE_CLASS_COMPONENTS)) {
    if (componentMatches(component, patterns)) return pageClass;
  }
  return null;
}

export function normalizeFocusedRoute(value) {
  let route = String(value || '').trim();
  if (!route) throw new Error('Focused route cannot be empty');
  if (!route.startsWith('/')) route = `/${route}`;
  route = route.replace(/\/+/gu, '/');
  if (route.includes('..') || route.includes('?') || route.includes('#')) {
    throw new Error(`Focused route must be a clean pathname: ${value}`);
  }
  if (!/\/[^/]+\.[a-z0-9]+$/iu.test(route) && !route.endsWith('/')) route += '/';
  return route;
}

export function normalizeStaticSiteFocusedRoutes(value) {
  if (value === null || value === undefined || value === '') return [];
  let raw = value;
  if (typeof value === 'string' && value.trim().startsWith('[')) {
    try {
      raw = JSON.parse(value);
    } catch (error) {
      throw new Error(`Invalid STATIC_SITE_FOCUSED_ROUTES JSON: ${error.message}`);
    }
  }
  const values = Array.isArray(raw) ? raw : String(raw).split(',');
  return [...new Set(values.map(normalizeFocusedRoute))];
}

function entryPathname(entry) {
  let pathname = String(entry?.pathname || entry?.route?.pathname || '');
  if (!pathname) return null;
  if (!pathname.startsWith('/')) pathname = `/${pathname}`;
  const buildId = String(process.env.PREVIEW_BUILD_ID || '').trim();
  const base = buildId ? `/${buildId}` : '';
  if (base && (pathname === base || pathname.startsWith(`${base}/`))) {
    pathname = pathname.slice(base.length) || '/';
  }
  return normalizeFocusedRoute(pathname);
}

export function filterPrerenderPaths(paths, pageClasses, focusedRoutes = []) {
  const allowed = new Set(['shell', ...pageClasses]);
  const focused = new Set(normalizeStaticSiteFocusedRoutes(focusedRoutes));
  const support = new Set(FOCUSED_PREVIEW_SUPPORT_ROUTES);
  const classScoped = paths.filter((entry) => (
    allowed.has(pageClassForComponent(entry.route?.component))
    || support.has(entryPathname(entry))
  ));
  if (!focused.size) return classScoped;
  const kept = classScoped.filter((entry) => {
    const pathname = entryPathname(entry);
    return pathname && focused.has(pathname);
  });
  if (!kept.some(({ route }) => pageClassForComponent(route?.component) === 'shell')) {
    throw new Error('Focused preview lost its mandatory __preview/ shell');
  }
  return kept;
}

export function staticSitePageClassFilterIntegration(
  value,
  focusedRoutes = process.env.STATIC_SITE_FOCUSED_ROUTES || '',
) {
  const pageClasses = normalizeStaticSitePageClasses(value);
  const focused = normalizeStaticSiteFocusedRoutes(focusedRoutes);
  if (pageClasses[0] === 'all' && !focused.length) return null;
  if (pageClasses[0] === 'all' && focused.length) {
    throw new Error('STATIC_SITE_FOCUSED_ROUTES requires one or more named page classes');
  }
  return {
    name: focused.length
      ? `kenigevents-static-focused-route:${pageClasses.join(',')}`
      : 'kenigevents-static-page-class-filter',
    hooks: {
      'astro:build:start': ({ setPrerenderer }) => {
        setPrerenderer((defaultPrerenderer) => ({
          ...defaultPrerenderer,
          name: focused.length
            ? `kenigevents-focused:${pageClasses.join(',')}`
            : `kenigevents-page-classes:${pageClasses.join(',')}`,
          async getStaticPaths() {
            const paths = await defaultPrerenderer.getStaticPaths();
            const kept = filterPrerenderPaths(paths, pageClasses, focused);
            if (!kept.some(({ route }) => pageClassForComponent(route?.component) === 'shell')) {
              throw new Error('Page-class preview lost its mandatory __preview/ shell');
            }
            if (focused.length) {
              const generated = new Set(kept.map(entryPathname).filter(Boolean));
              const missing = focused.filter((route) => !generated.has(route));
              if (missing.length) {
                throw new Error(`Focused preview did not materialize requested routes: ${missing.join(', ')}`);
              }
            }
            return kept;
          },
        }));
      },
    },
  };
}
