import { readFileSync } from 'node:fs';

const PAGE_CLASS_CONTRACT = JSON.parse(readFileSync(
  new URL('./static-site-page-classes.v1.json', import.meta.url),
  'utf8',
));

function validPatterns(patterns, { templated = false } = {}) {
  return Array.isArray(patterns) && patterns.length > 0 && patterns.every((pattern) => (
    pattern
    && typeof pattern === 'object'
    && !Array.isArray(pattern)
    && ['exact', 'prefix'].includes(pattern.kind)
    && ['page', 'dependency'].includes(pattern.role)
    && typeof pattern.required === 'boolean'
    && (
      (typeof pattern.value === 'string' && pattern.value.startsWith('/'))
      || (templated && typeof pattern.template === 'string' && pattern.template.startsWith('/'))
    )
  ));
}

const routeFamilies = PAGE_CLASS_CONTRACT?.focused_route_families;
const eventDetail = PAGE_CLASS_CONTRACT?.focused_event_detail;
if (
  PAGE_CLASS_CONTRACT?.schema_version !== 'kenigevents_static_site_page_classes_v1'
  || !PAGE_CLASS_CONTRACT.classes
  || typeof PAGE_CLASS_CONTRACT.classes !== 'object'
  || Array.isArray(PAGE_CLASS_CONTRACT.classes)
  || !Array.isArray(PAGE_CLASS_CONTRACT.always_preview_components)
  || !routeFamilies
  || typeof routeFamilies !== 'object'
  || Array.isArray(routeFamilies)
  || !Object.values(routeFamilies).every((entry) => (
    entry
    && typeof entry === 'object'
    && !Array.isArray(entry)
    && Array.isArray(entry.page_classes)
    && entry.page_classes.length > 0
    && entry.page_classes.every((item) => typeof item === 'string' && item.length > 0)
    && validPatterns(entry.patterns)
  ))
  || !eventDetail
  || typeof eventDetail !== 'object'
  || !Array.isArray(eventDetail.page_classes)
  || eventDetail.page_classes.length === 0
  || !eventDetail.page_classes.every((item) => typeof item === 'string' && item.length > 0)
  || !validPatterns(eventDetail.patterns, { templated: true })
) {
  throw new Error('Invalid static-site page-class contract');
}

const PAGE_CLASS_COMPONENTS = Object.freeze(PAGE_CLASS_CONTRACT.classes);
const ALWAYS_PREVIEW_COMPONENTS = Object.freeze(PAGE_CLASS_CONTRACT.always_preview_components);
const FOCUSED_ROUTE_FAMILIES = Object.freeze(routeFamilies);
const FOCUSED_EVENT_DETAIL = Object.freeze(eventDetail);

export const STATIC_SITE_PAGE_CLASSES = Object.freeze(Object.keys(PAGE_CLASS_COMPONENTS));
export const STATIC_SITE_FOCUSED_ROUTE_FAMILIES = Object.freeze(Object.keys(FOCUSED_ROUTE_FAMILIES));

for (const [name, entry] of [
  ...Object.entries(FOCUSED_ROUTE_FAMILIES),
  ['event-detail', FOCUSED_EVENT_DETAIL],
]) {
  const unknown = entry.page_classes.filter((item) => !STATIC_SITE_PAGE_CLASSES.includes(item));
  if (unknown.length) {
    throw new Error(`Focused selector ${name} references unknown page classes: ${unknown.join(', ')}`);
  }
}

function normalizedComponent(value) {
  return String(value || '').replaceAll('\\', '/').replace(/^\.\//u, '');
}

function componentMatches(component, patterns) {
  return patterns.some((pattern) => (
    pattern.endsWith('/') ? component.startsWith(pattern) : component === pattern
  ));
}

function normalizedPathname(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const withLeadingSlash = raw.startsWith('/') ? raw : `/${raw}`;
  const collapsed = withLeadingSlash.replace(/\/{2,}/gu, '/');
  return collapsed === '/' ? '/' : collapsed.replace(/\/+$/u, '');
}

function patternMatches(pathname, pattern) {
  const rawPattern = String(pattern.value || pattern.template || '');
  const route = normalizedPathname(pathname);
  const expected = normalizedPathname(rawPattern);
  if (!route || !expected) return false;
  if (pattern.kind === 'exact') return route === expected;
  if (rawPattern.endsWith('/')) {
    return route === expected || route.startsWith(`${expected}/`);
  }
  return route.startsWith(expected);
}

function renderedEventPattern(pattern, event) {
  if (!pattern.template) return pattern;
  return {
    ...pattern,
    value: pattern.template
      .replaceAll('{id}', String(event.id))
      .replaceAll('{slug}', event.slug),
  };
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

export function normalizeStaticSiteRouteFamilies(value) {
  const raw = Array.isArray(value) ? value : String(value || '').split(',');
  const selected = [...new Set(raw.flatMap((item) => String(item || '').split(','))
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean))];
  const unknown = selected.filter((item) => !STATIC_SITE_FOCUSED_ROUTE_FAMILIES.includes(item));
  if (unknown.length) {
    throw new Error(
      `Unknown STATIC_SITE_FOCUSED_ROUTE_FAMILIES: ${unknown.join(', ')}; expected ${STATIC_SITE_FOCUSED_ROUTE_FAMILIES.join(', ')}`,
    );
  }
  return selected;
}

export function normalizeStaticSiteFocusedEvent(value) {
  if (value == null || value === '') return null;
  let source = value;
  if (typeof value === 'string') {
    try {
      source = JSON.parse(value);
    } catch (error) {
      throw new Error('STATIC_SITE_FOCUSED_EVENT must be valid JSON', { cause: error });
    }
  }
  if (!source || typeof source !== 'object' || Array.isArray(source)) {
    throw new Error('STATIC_SITE_FOCUSED_EVENT must be an object');
  }
  const id = Number(source.id);
  const slug = String(source.slug || '').trim().toLowerCase();
  if (!Number.isSafeInteger(id) || id <= 0) {
    throw new Error('STATIC_SITE_FOCUSED_EVENT.id must be a positive integer');
  }
  if (!/^[a-z0-9][a-z0-9-]{0,190}$/u.test(slug)) {
    throw new Error('STATIC_SITE_FOCUSED_EVENT.slug must be a canonical event slug');
  }
  return Object.freeze({ id, slug });
}

export function pageClassesForFocusedSelection(routeFamilyValue, eventValue = null) {
  const routeFamilyNames = normalizeStaticSiteRouteFamilies(routeFamilyValue);
  const event = normalizeStaticSiteFocusedEvent(eventValue);
  const pageClasses = [];
  const add = (pageClass) => {
    if (!pageClasses.includes(pageClass)) pageClasses.push(pageClass);
  };
  for (const name of routeFamilyNames) {
    FOCUSED_ROUTE_FAMILIES[name].page_classes.forEach(add);
  }
  if (event) FOCUSED_EVENT_DETAIL.page_classes.forEach(add);
  return pageClasses;
}

export function pageClassForComponent(value) {
  const component = normalizedComponent(value);
  if (componentMatches(component, ALWAYS_PREVIEW_COMPONENTS)) return 'shell';
  for (const [pageClass, patterns] of Object.entries(PAGE_CLASS_COMPONENTS)) {
    if (componentMatches(component, patterns)) return pageClass;
  }
  return null;
}

function focusedPatternGroups(routeFamilyNames, event) {
  const groups = routeFamilyNames.map((name) => ({
    id: `route-family:${name}`,
    patterns: FOCUSED_ROUTE_FAMILIES[name].patterns,
  }));
  if (event) {
    groups.push({
      id: `event-detail:${event.id}`,
      patterns: FOCUSED_EVENT_DETAIL.patterns.map((pattern) => renderedEventPattern(pattern, event)),
    });
  }
  return groups;
}

export function validateFocusedMaterializedRoutes(materializedRouteValues, options = {}) {
  const routeFamilyNames = normalizeStaticSiteRouteFamilies(options.routeFamilies || []);
  const event = normalizeStaticSiteFocusedEvent(options.event || null);
  const groups = focusedPatternGroups(routeFamilyNames, event);
  if (!groups.length) throw new Error('Focused route validation requires a route family or event detail');
  const routes = [...new Set((materializedRouteValues || [])
    .map((value) => normalizedPathname(value))
    .filter(Boolean))];
  const shellRoutes = new Set(['/__preview', '/robots.txt']);
  const unexpected = routes.filter((pathname) => (
    !shellRoutes.has(pathname)
    && !groups.some((group) => group.patterns.some((pattern) => patternMatches(pathname, pattern)))
  ));
  if (unexpected.length) {
    throw new Error(`Focused route selector materialized unselected routes: ${unexpected.join(', ')}`);
  }
  for (const group of groups) {
    for (const pattern of group.patterns.filter((item) => item.required)) {
      if (!routes.some((pathname) => patternMatches(pathname, pattern))) {
        throw new Error(
          `Focused route selector did not materialize required ${group.id} ${pattern.role}: ${pattern.value}`,
        );
      }
    }
  }
  const pagePatterns = groups.flatMap((group) => group.patterns.filter((pattern) => pattern.role === 'page'));
  if (!routes.some((pathname) => pagePatterns.some((pattern) => patternMatches(pathname, pattern)))) {
    throw new Error('Focused route selector materialized no user-facing page');
  }
  return Object.freeze({
    routeFamilies: Object.freeze([...routeFamilyNames]),
    event,
    pageClasses: Object.freeze(pageClassesForFocusedSelection(routeFamilyNames, event)),
  });
}

export function staticSitePageClassFilterIntegration(value, options = {}) {
  const pageClasses = normalizeStaticSitePageClasses(value);
  const routeFamilyValue = Object.hasOwn(options, 'routeFamilies')
    ? options.routeFamilies
    : process.env.STATIC_SITE_FOCUSED_ROUTE_FAMILIES || '';
  const eventValue = Object.hasOwn(options, 'event')
    ? options.event
    : process.env.STATIC_SITE_FOCUSED_EVENT || null;
  const routeFamilyNames = normalizeStaticSiteRouteFamilies(routeFamilyValue);
  const event = normalizeStaticSiteFocusedEvent(eventValue);
  const focused = routeFamilyNames.length > 0 || event !== null;
  if (pageClasses[0] === 'all') {
    if (focused) throw new Error('Focused route selection requires named STATIC_SITE_PAGE_CLASSES');
    return null;
  }

  if (focused) {
    const requiredPageClasses = pageClassesForFocusedSelection(routeFamilyNames, event);
    const missing = requiredPageClasses.filter((item) => !pageClasses.includes(item));
    if (missing.length) {
      throw new Error(`Focused route selection is missing required page classes: ${missing.join(', ')}`);
    }
  }

  const allowed = new Set(['shell', ...pageClasses]);
  const groups = focusedPatternGroups(routeFamilyNames, event);
  return {
    name: focused
      ? `kenigevents-focused-routes:${[...routeFamilyNames, event ? `event-${event.id}` : null].filter(Boolean).join(',')}`
      : `kenigevents-page-classes:${pageClasses.join(',')}`,
    hooks: {
      'astro:build:start': ({ setPrerenderer }) => {
        setPrerenderer((defaultPrerenderer) => ({
          ...defaultPrerenderer,
          name: focused
            ? `kenigevents-focused-routes:${[...routeFamilyNames, event ? `event-${event.id}` : null].filter(Boolean).join(',')}`
            : `kenigevents-page-classes:${pageClasses.join(',')}`,
          async getStaticPaths() {
            const paths = await defaultPrerenderer.getStaticPaths();
            const candidates = paths.filter(({ route }) => allowed.has(pageClassForComponent(route.component)));
            const kept = focused
              ? candidates.filter(({ pathname, route }) => (
                  pageClassForComponent(route.component) === 'shell'
                  || groups.some((group) => group.patterns.some((pattern) => patternMatches(pathname, pattern)))
                ))
              : candidates;
            if (!kept.some(({ route }) => pageClassForComponent(route.component) === 'shell')) {
              throw new Error('Page-class preview lost its mandatory __preview/ shell');
            }
            if (focused) {
              validateFocusedMaterializedRoutes(kept.map(({ pathname }) => pathname), {
                routeFamilies: routeFamilyNames,
                event,
              });
            }
            return kept;
          },
        }));
      },
    },
  };
}
