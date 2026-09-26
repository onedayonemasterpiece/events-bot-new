/** EventLayout is the sole shell owner. Policies select participants before mount. */
export const SHELL_COMPOSITION_VERSION = 'shell-composition-v2' as const;
export function shellCompositionForRoute(routePath: string) {
  const normalized = routePath.replace(/\/+$/u, '') || '/';
  const home = normalized === '/';
  const eventDetail = /^\/sobytiya\/[^/]+$/u.test(normalized);
  const today = normalized === '/segodnya';
  return Object.freeze({
    version: SHELL_COMPOSITION_VERSION,
    id: home ? 'home-navigation-only' : eventDetail ? 'event-navigation-only' : 'contextual',
    topParticipants: !home && !eventDetail,
    globalNavigation: true,
    brandInFlow: false,
    lowerNavigation: home ? 'afisha' : 'route',
    desktopSectionContext: today ? 'none' : 'section',
  } as const);
}

/** A keyboard/modal suspends the shared dock, not a route-specific z-index layer. */
export function lowerNavigationState(modalOpen: boolean, keyboardOffset: number, editableFocus: boolean) {
  return modalOpen ? 'modal' : editableFocus && keyboardOffset >= 120 ? 'keyboard' : 'ready';
}
