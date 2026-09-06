/** EventLayout is the sole shell owner. Policies select participants before mount. */
export const SHELL_COMPOSITION_VERSION = 'shell-composition-v1' as const;
export function shellCompositionForRoute(routePath: string) {
  const home = routePath.replace(/\/+$/u, '') === '';
  return Object.freeze({
    version: SHELL_COMPOSITION_VERSION,
    id: home ? 'home-navigation-only' : 'contextual',
    topParticipants: !home,
    globalNavigation: true,
    brandInFlow: home,
    lowerNavigation: home ? 'afisha' : 'route',
  } as const);
}

/** A keyboard/modal suspends the shared dock, not a route-specific z-index layer. */
export function lowerNavigationState(modalOpen: boolean, keyboardOffset: number, editableFocus: boolean) {
  return modalOpen ? 'modal' : editableFocus && keyboardOffset >= 120 ? 'keyboard' : 'ready';
}
