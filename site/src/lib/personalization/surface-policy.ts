import {
  P13N_SURFACE_REGISTRY_VERSION,
  type PersonalizationPolicyIdV1,
  type PersonalizationSurfaceIdV1,
  type SurfacePolicyV1,
} from './contract.ts';

function policy(value: Omit<SurfacePolicyV1, 'registryVersion' | 'networkOnPageView' | 'source'>): SurfacePolicyV1 {
  return Object.freeze({
    ...value,
    registryVersion: P13N_SURFACE_REGISTRY_VERSION,
    networkOnPageView: false,
    source: 'personalization-to-be.md',
  });
}

export const PERSONALIZATION_SURFACE_POLICIES_V1: Readonly<Record<PersonalizationPolicyIdV1, SurfacePolicyV1>> = Object.freeze({
  'unknown-static': policy({ id: 'unknown-static', rankingMode: 'identity', reorderScope: 'none', exactHide: 'compatible-local-only', signalCollection: 'none', fallback: 'static' }),
  'calendar-exact-only': policy({ id: 'calendar-exact-only', rankingMode: 'chronological', reorderScope: 'none', exactHide: 'global', signalCollection: 'explicit-actions-only', fallback: 'static-chronological' }),
  'calendar-personal-tail': policy({ id: 'calendar-personal-tail', rankingMode: 'profile', reorderScope: 'invisible-tail', exactHide: 'global', signalCollection: 'explicit-actions-only', fallback: 'hidden-or-static-popular-tail' }),
  'thematic-weak': policy({ id: 'thematic-weak', rankingMode: 'baseline-plus-profile-tiebreak', reorderScope: 'invisible-tail', exactHide: 'global', signalCollection: 'explicit-actions-only', fallback: 'static-editorial' }),
  'popular-tiebreak': policy({ id: 'popular-tiebreak', rankingMode: 'popularity-first-profile-tiebreak', reorderScope: 'invisible-tail', exactHide: 'global', signalCollection: 'explicit-actions-only', fallback: 'static-popularity' }),
  'search-query-first': policy({ id: 'search-query-first', rankingMode: 'query-first-profile-tiebreak', reorderScope: 'unseen-results-only', exactHide: 'global', signalCollection: 'explicit-actions-only', fallback: 'query-order' }),
  'related-anchor-first': policy({ id: 'related-anchor-first', rankingMode: 'anchor-first-profile-tiebreak', reorderScope: 'invisible-tail', exactHide: 'global', signalCollection: 'explicit-actions-only', fallback: 'static-related' }),
  'for-me-strong': policy({ id: 'for-me-strong', rankingMode: 'profile', reorderScope: 'whole-unseen-list', exactHide: 'global', signalCollection: 'explicit-actions-only', fallback: 'onboarding-or-general' }),
  'free-weak': policy({ id: 'free-weak', rankingMode: 'eligibility-first-profile-tiebreak', reorderScope: 'invisible-tail', exactHide: 'global', signalCollection: 'explicit-actions-only', fallback: 'static-free' }),
  'children-weak': policy({ id: 'children-weak', rankingMode: 'eligibility-first-profile-tiebreak', reorderScope: 'invisible-tail', exactHide: 'global', signalCollection: 'explicit-actions-only', fallback: 'static-children' }),
});

const POLICY_BY_SURFACE: Readonly<Record<PersonalizationSurfaceIdV1, PersonalizationPolicyIdV1>> = Object.freeze({
  unknown: 'unknown-static',
  static_only: 'unknown-static',
  calendar_primary: 'calendar-exact-only',
  today_primary: 'calendar-exact-only',
  tomorrow_primary: 'calendar-exact-only',
  weekend_primary: 'calendar-exact-only',
  calendar_personal_tail: 'calendar-personal-tail',
  thematic_collection: 'thematic-weak',
  popular_primary: 'popular-tiebreak',
  search_results: 'search-query-first',
  event_detail_related: 'related-anchor-first',
  for_me: 'for-me-strong',
  free_primary: 'free-weak',
  children_primary: 'children-weak',
});

const SURFACES = new Set<PersonalizationSurfaceIdV1>(Object.keys(POLICY_BY_SURFACE) as PersonalizationSurfaceIdV1[]);

export interface RouteSurfaceResolutionV1 {
  pageFamily: string;
  surfaceId: PersonalizationSurfaceIdV1;
  policy: SurfacePolicyV1;
  staticOnlyReason: string | null;
  diagnostic: 'p13n_surface.registered' | 'p13n_surface.static_only' | 'p13n_surface.unknown_static';
}

export function normalizePersonalizationRoutePath(pathname: unknown): string {
  const raw = String(pathname || '/').split(/[?#]/u, 1)[0] || '/';
  const normalized = `/${raw.replace(/^\/+|\/+$/gu, '')}/`.replace(/\/{2,}/gu, '/');
  return normalized.replace(/^\/preview-[A-Za-z0-9._-]+(?=\/)/u, '') || '/';
}

export function isPersonalizationSurfaceIdV1(value: unknown): value is PersonalizationSurfaceIdV1 {
  return typeof value === 'string' && SURFACES.has(value as PersonalizationSurfaceIdV1);
}

export function resolveSurfacePolicyV1(surface: unknown): SurfacePolicyV1 {
  const id = isPersonalizationSurfaceIdV1(surface) ? surface : 'unknown';
  return PERSONALIZATION_SURFACE_POLICIES_V1[POLICY_BY_SURFACE[id]];
}

export function resolveRouteSurfaceV1(pathname: unknown): RouteSurfaceResolutionV1 {
  const path = normalizePersonalizationRoutePath(pathname);
  const registered = (pageFamily: string, surfaceId: PersonalizationSurfaceIdV1): RouteSurfaceResolutionV1 => ({
    pageFamily,
    surfaceId,
    policy: resolveSurfacePolicyV1(surfaceId),
    staticOnlyReason: null,
    diagnostic: 'p13n_surface.registered',
  });
  if (path === '/segodnya/') return registered('today', 'today_primary');
  if (path === '/zavtra/') return registered('tomorrow', 'tomorrow_primary');
  if (path.startsWith('/vyhodnye/') || path.startsWith('/kalendar/') || path.startsWith('/date-')) {
    return registered('calendar', path.startsWith('/vyhodnye/') ? 'weekend_primary' : 'calendar_primary');
  }
  if (path === '/populyarnoe/') return registered('popular', 'popular_primary');
  if (path === '/poisk/') return registered('search', 'search_results');
  if (path.startsWith('/sobytiya/')) return registered('event-detail', 'event_detail_related');
  if (path === '/dlya-menya/') return registered('for-me', 'for_me');
  if (path.startsWith('/besplatno/')) return registered('free', 'free_primary');
  if (path.startsWith('/detyam/') || path.startsWith('/s-detmi/')) return registered('children', 'children_primary');
  if (['/vystavki/', '/festivali/', '/neobychnoe/', '/artefakty/'].includes(path)
      || path.startsWith('/kluby-po-interesam/') || path.startsWith('/podborki/')) {
    return registered('thematic-collection', 'thematic_collection');
  }
  const staticReasons: Array<[boolean, string, string]> = [
    [path === '/', 'home', 'wave0-home-static-baseline'],
    [path === '/__preview/', 'preview-index', 'isolated-preview-index'],
    [path === '/partnerstvo/', 'partnership', 'non-recommendation-content'],
    [path === '/partners/', 'partners', 'non-recommendation-content'],
    [path === '/izbrannoe/', 'favorites', 'wave0-favorites-policy-not-defined'],
    [path.startsWith('/fokus-gruppa/'), 'focus-group', 'isolated-test-cohort-surface'],
    [path === '/zakrytaya-afisha/', 'closed-listing', 'restricted-static-listing'],
    [path === '/404/', 'not-found', 'error-document'],
  ];
  const staticMatch = staticReasons.find(([matches]) => matches);
  if (staticMatch) {
    return {
      pageFamily: staticMatch[1],
      surfaceId: 'static_only',
      policy: resolveSurfacePolicyV1('static_only'),
      staticOnlyReason: staticMatch[2],
      diagnostic: 'p13n_surface.static_only',
    };
  }
  return {
    pageFamily: 'unknown',
    surfaceId: 'unknown',
    policy: resolveSurfacePolicyV1('unknown'),
    staticOnlyReason: null,
    diagnostic: 'p13n_surface.unknown_static',
  };
}
