const PAGE_CLASS_COMPONENTS = Object.freeze({
  event: [
    'src/pages/sobytiya/',
    'src/pages/data/discovery/',
  ],
  date: [
    'src/pages/index.astro',
    'src/pages/segodnya/',
    'src/pages/zavtra/',
    'src/pages/date-[date].astro',
    'src/pages/data/event-dates.json.ts',
  ],
  weekend: [
    'src/pages/vyhodnye/',
  ],
  collection: [
    'src/pages/artefakty/',
    'src/pages/festivali/',
    'src/pages/kluby-po-interesam/',
    'src/pages/neobychnoe/',
    'src/pages/podborki/',
    'src/pages/populyarnoe/',
    'src/pages/vystavki/',
  ],
  personal: [
    'src/pages/data/personal-feed.json.ts',
    'src/pages/dlya-menya/',
    'src/pages/izbrannoe/',
    'src/pages/manifest.webmanifest.ts',
    'src/pages/poisk/',
    'src/pages/pwa-sw.js.ts',
    'src/pages/zakrytaya-afisha/',
  ],
  focus: [
    'src/pages/fokus-gruppa/',
  ],
  partner: [
    'src/pages/partners/',
    'src/pages/partnerstvo/',
  ],
  lab: [
    'src/pages/lab/',
  ],
});

const ALWAYS_PREVIEW_COMPONENTS = Object.freeze([
  'src/pages/[preview]/index.astro',
  'src/pages/robots.txt.ts',
]);

export const STATIC_SITE_PAGE_CLASSES = Object.freeze(Object.keys(PAGE_CLASS_COMPONENTS));

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

export function staticSitePageClassFilterIntegration(value) {
  const pageClasses = normalizeStaticSitePageClasses(value);
  if (pageClasses[0] === 'all') return null;
  const allowed = new Set(['shell', ...pageClasses]);
  return {
    name: 'kenigevents-static-page-class-filter',
    hooks: {
      'astro:build:start': ({ setPrerenderer }) => {
        setPrerenderer((defaultPrerenderer) => ({
          ...defaultPrerenderer,
          name: `kenigevents-page-classes:${pageClasses.join(',')}`,
          async getStaticPaths() {
            const paths = await defaultPrerenderer.getStaticPaths();
            const kept = paths.filter(({ route }) => allowed.has(pageClassForComponent(route.component)));
            if (!kept.some(({ route }) => pageClassForComponent(route.component) === 'shell')) {
              throw new Error('Page-class preview lost its mandatory __preview/ shell');
            }
            return kept;
          },
        }));
      },
    },
  };
}
