import { isStandaloneDisplay } from './pwa-install-controller.js';

interface FocusPwaInstallRoot extends HTMLElement {
  dataset: DOMStringMap;
}

interface FocusPwaInstallOptions {
  windowRef: Window;
  navigatorRef: Navigator;
  root: FocusPwaInstallRoot;
  button: HTMLButtonElement;
  openButton?: HTMLAnchorElement | null;
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
  openButton,
  status,
  guidance,
}: FocusPwaInstallOptions) {
  const standalone = isStandaloneDisplay(windowRef, navigatorRef);
  const appleMobile = isAppleMobile(navigatorRef);
  const android = /android/iu.test(String(navigatorRef.userAgent || ''));
  let installPrompt: {
    preventDefault(): void;
    prompt(): Promise<{ outcome?: string } | void>;
  } | null = null;
  let prompting = false;

  const showFallback = () => {
    root.hidden = false;
    root.dataset.pwaInstallReady = 'false';
    button.hidden = true;
    button.disabled = false;
    if (openButton) openButton.hidden = standalone;
    if (guidance) guidance.hidden = true;
  };

  const showInstall = () => {
    root.hidden = false;
    root.dataset.pwaInstallReady = 'true';
    button.hidden = false;
    button.disabled = false;
    if (openButton) openButton.hidden = true;
    if (guidance) guidance.hidden = true;
  };

  showFallback();
  if (standalone) {
    if (status) {
      status.textContent = '«Анонсы» уже открыты как приложение.';
    }
  } else if (appleMobile) {
    if (status) {
      status.textContent = 'Если приложение не открылось, продолжите на сайте. На iPhone установка доступна через «Поделиться» → «На экран Домой».';
    }
  } else if (!android) {
    if (status) {
      status.textContent = 'Если приложение не открылось, продолжите на сайте.';
    }
  }

  const onBeforeInstallPrompt = (event: Event) => {
    if (!android || standalone) return;
    const promptEvent = event as Event & {
      prompt(): Promise<{ outcome?: string } | void>;
    };
    promptEvent.preventDefault();
    installPrompt = promptEvent;
    if (status) status.textContent = 'Приложение можно установить на этот телефон.';
    showInstall();
  };

  const onInstallClick = async (event: Event) => {
    event.preventDefault();
    if (!installPrompt || prompting) return;
    const promptEvent = installPrompt;
    installPrompt = null;
    prompting = true;
    button.disabled = true;
    root.dataset.pwaInstallReady = 'false';
    try {
      const result = await promptEvent.prompt();
      if (result?.outcome === 'accepted') {
        if (status) status.textContent = 'Установка подтверждена. Теперь можно открыть «Анонсы».';
      } else if (status) {
        status.textContent = 'Установка не завершена. Можно открыть уже установленное приложение или продолжить на сайте.';
      }
    } catch {
      if (status) {
        status.textContent = 'Системное окно не открылось. Можно открыть уже установленное приложение или продолжить на сайте.';
      }
    } finally {
      prompting = false;
      showFallback();
    }
  };

  const onInstalled = () => {
    root.dataset.focusPwaInstalled = 'true';
    showFallback();
    if (status) {
      status.textContent = 'Готово. Нажмите «Открыть “Анонсы”» или запустите их с главного экрана.';
    }
    if (typeof root.dispatchEvent === 'function' && typeof CustomEvent === 'function') {
      root.dispatchEvent(new CustomEvent('focuspwainstalled', { bubbles: true }));
    }
  };
  windowRef.addEventListener('beforeinstallprompt', onBeforeInstallPrompt);
  windowRef.addEventListener('appinstalled', onInstalled);
  button.addEventListener('click', onInstallClick);

  return {
    destroy() {
      windowRef.removeEventListener('beforeinstallprompt', onBeforeInstallPrompt);
      windowRef.removeEventListener('appinstalled', onInstalled);
      button.removeEventListener('click', onInstallClick);
      installPrompt = null;
      prompting = false;
    },
    get ready() {
      return Boolean(installPrompt);
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
      openButton: root.querySelector<HTMLAnchorElement>('[data-pwa-open-app]'),
      status: root.querySelector<HTMLElement>('[data-pwa-install-status]'),
      guidance: root.querySelector<HTMLElement>('[data-pwa-install-guidance]'),
    });
  });
}
