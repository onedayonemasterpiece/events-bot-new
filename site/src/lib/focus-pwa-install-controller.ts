import {
  createPwaInstallController,
  isStandaloneDisplay,
} from './pwa-install-controller.js';

interface FocusPwaInstallRoot extends HTMLElement {
  dataset: DOMStringMap;
}

interface FocusPwaInstallOptions {
  windowRef: Window;
  navigatorRef: Navigator;
  root: FocusPwaInstallRoot;
  button: HTMLButtonElement;
  status?: HTMLElement | null;
  guidance?: HTMLElement | null;
}

function isAppleMobile(navigatorRef: Navigator): boolean {
  const navigatorWithUaData = navigatorRef as Navigator & {
    userAgentData?: { platform?: string };
  };
  const platform = String(navigatorWithUaData.userAgentData?.platform || navigatorRef.platform || '');
  const userAgent = String(navigatorRef.userAgent || '');
  return /iPhone|iPad|iPod/iu.test(`${platform} ${userAgent}`)
    || (platform === 'MacIntel' && Number(navigatorRef.maxTouchPoints || 0) > 1);
}

export function createFocusPwaInstallController({
  windowRef,
  navigatorRef,
  root,
  button,
  status,
  guidance,
}: FocusPwaInstallOptions) {
  const controller = createPwaInstallController({
    windowRef,
    navigatorRef,
    // Reuse the incident-tested, one-shot presentation mode. This is a
    // synthetic controller flag, not a claim that installation is available.
    locationRef: { search: '?install=presentation' },
    root,
    button,
    status,
    guidance,
  });

  const standalone = isStandaloneDisplay(windowRef, navigatorRef);
  const appleMobile = isAppleMobile(navigatorRef);
  if (standalone) {
    if (status) {
      status.textContent = '«Анонсы» уже открыты как приложение.';
    }
  } else if (appleMobile) {
    if (status) {
      status.textContent = 'На iPhone и iPad установка открывается через «Поделиться» → «На экран Домой».';
    }
  } else if (!/android/iu.test(String(navigatorRef.userAgent || ''))) {
    if (status) {
      status.textContent = 'Можно продолжить в этой вкладке.';
    }
  }

  const onInstalled = () => {
    root.dataset.focusPwaInstalled = 'true';
    if (status) {
      status.textContent = 'Готово. Откройте «Анонсы» с главного экрана.';
    }
    if (typeof root.dispatchEvent === 'function' && typeof CustomEvent === 'function') {
      root.dispatchEvent(new CustomEvent('focuspwainstalled', { bubbles: true }));
    }
  };
  windowRef.addEventListener('appinstalled', onInstalled);

  return {
    destroy() {
      windowRef.removeEventListener('appinstalled', onInstalled);
      controller?.destroy();
    },
    get ready() {
      return Boolean(controller?.ready);
    },
  };
}

export function hydrateFocusPwaInstallActions({
  documentRef = document,
  windowRef = window,
  navigatorRef = navigator,
} = {}) {
  documentRef.querySelectorAll<FocusPwaInstallRoot>('[data-focus-pwa-install]').forEach((root) => {
    if (root.dataset.focusPwaBound === 'true') return;
    const button = root.querySelector<HTMLButtonElement>('[data-pwa-install-button]');
    if (!button) return;
    root.dataset.focusPwaBound = 'true';
    createFocusPwaInstallController({
      windowRef,
      navigatorRef,
      root,
      button,
      status: root.querySelector<HTMLElement>('[data-pwa-install-status]'),
      guidance: root.querySelector<HTMLElement>('[data-pwa-install-guidance]'),
    });
  });
}
