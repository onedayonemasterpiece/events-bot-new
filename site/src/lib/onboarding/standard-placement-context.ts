export const STANDARD_ONBOARDING_CONTEXT_VERSION = 'standard-onboarding-context-v1' as const;

export type StandardOnboardingRouteContext =
  | 'home'
  | 'listing'
  | 'event_detail'
  | 'search'
  | 'personal'
  | 'information';

export type StandardOnboardingPlacementSlot = 'page_end';

export interface StandardOnboardingPlacementContext {
  version: typeof STANDARD_ONBOARDING_CONTEXT_VERSION;
  routeContext: StandardOnboardingRouteContext;
  placementSlot: StandardOnboardingPlacementSlot;
  runtimeMode: 'inert';
  artifactProgram: 'disabled';
  clubProgram: 'disabled';
  raffleProgram: 'disabled';
}

export function normalizeStandardOnboardingPath(pathname: unknown): string {
  const raw = String(pathname || '/').split(/[?#]/u, 1)[0] || '/';
  const normalized = `/${raw.replace(/^\/+|\/+$/gu, '')}/`.replace(/\/{2,}/gu, '/');
  return normalized.replace(/^\/preview-[A-Za-z0-9._-]+(?=\/)/u, '') || '/';
}

export function resolveStandardOnboardingPlacementContext(
  pathname: unknown,
): StandardOnboardingPlacementContext {
  const path = normalizeStandardOnboardingPath(pathname);
  let routeContext: StandardOnboardingRouteContext = 'information';
  if (path === '/' || path === '/__preview/') routeContext = 'home';
  else if (path.startsWith('/sobytiya/')) routeContext = 'event_detail';
  else if (path === '/poisk/') routeContext = 'search';
  else if (path === '/dlya-menya/' || path === '/izbrannoe/') routeContext = 'personal';
  else if (
    path.startsWith('/date-')
    || path.startsWith('/vyhodnye/')
    || path.startsWith('/podborki/')
    || [
      '/segodnya/', '/zavtra/', '/vystavki/', '/festivali/', '/populyarnoe/',
      '/neobychnoe/', '/artefakty/', '/kluby-po-interesam/',
    ].includes(path)
  ) routeContext = 'listing';
  return Object.freeze({
    version:STANDARD_ONBOARDING_CONTEXT_VERSION,
    routeContext,
    placementSlot:'page_end',
    runtimeMode:'inert',
    artifactProgram:'disabled',
    clubProgram:'disabled',
    raffleProgram:'disabled',
  });
}
