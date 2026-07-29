import { isStandaloneDisplay } from './pwa-install-controller.js';

interface FocusPwaInstallRoot extends HTMLElement {
  dataset: DOMStringMap;
}

interface FocusPwaInstallOptions {
  windowRef: Window;
  navigatorRef: Navigator & {
    getInstalledRelatedApps?: () => Promise<Array<{ platform?: string; url?: string }>>;
  };
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
  let destroyed = false;
  let installCheckTimer = 0;
  let installCheckStartedAt = 0;
  const originalButtonText = button.textContent || 'Установить «Анонсы»';
  const installSettleDelayMs = 4_000;
  const installDetectionTimeoutMs = 30_000;

  const showFallback = () => {
    root.hidden = false;
    root.dataset.pwaInstallReady = 'false';
    button.hidden = true;
    button.disabled = false;
    button.dataset.installing = 'false';
    button.textContent = originalButtonText;
    if (openButton) openButton.hidden = standalone;
    if (guidance) guidance.hidden = true;
  };

  const showInstall = () => {
    root.hidden = false;
    root.dataset.pwaInstallReady = 'true';
    button.hidden = false;
    button.disabled = false;
    button.dataset.installing = 'false';
    button.textContent = originalButtonText;
    if (openButton) openButton.hidden = true;
    if (guidance) guidance.hidden = true;
  };

  const showInstalling = () => {
    root.hidden = false;
    root.dataset.pwaInstallReady = 'false';
    root.dataset.focusPwaInstalling = 'true';
    button.hidden = false;
    button.disabled = true;
    button.dataset.installing = 'true';
    button.textContent = 'Устанавливаем…';
    if (openButton) openButton.hidden = true;
    if (guidance) guidance.hidden = true;
    if (status) status.textContent = 'Устанавливаем «Анонсы». Подождите немного.';
  };

  const showInstalled = () => {
    root.dataset.focusPwaInstalling = 'false';
    root.dataset.focusPwaInstalled = 'true';
    showFallback();
    if (status) {
      status.textContent = 'Готово. Откройте «Анонсы» — продолжение появится в приложении.';
    }
    if (typeof root.dispatchEvent === 'function' && typeof CustomEvent === 'function') {
      root.dispatchEvent(new CustomEvent('focuspwainstalled', { bubbles: true }));
    }
  };

  const scheduleInstallCheck = (delayMs: number) => {
    if (destroyed) return;
    if (installCheckTimer) windowRef.clearTimeout(installCheckTimer);
    installCheckTimer = windowRef.setTimeout(() => {
      installCheckTimer = 0;
      void checkInstallReady();
    }, delayMs);
  };

  const checkInstallReady = async () => {
    if (destroyed || root.dataset.focusPwaInstalled === 'true') return;
    const elapsed = Date.now() - installCheckStartedAt;
    try {
      const relatedApps = await navigatorRef.getInstalledRelatedApps?.();
      if (relatedApps && relatedApps.length > 0) {
        showInstalled();
        return;
      }
    } catch {
      // Continue with the bounded wait below.
    }
    if (elapsed < installDetectionTimeoutMs) {
      scheduleInstallCheck(1_000);
      return;
    }
    showInstalled();
  };

  const waitForInstallReady = () => {
    showInstalling();
    if (!installCheckStartedAt) installCheckStartedAt = Date.now();
    if (typeof navigatorRef.getInstalledRelatedApps === 'function') {
      scheduleInstallCheck(350);
      return;
    }
    if (installCheckTimer) windowRef.clearTimeout(installCheckTimer);
    installCheckTimer = windowRef.setTimeout(() => {
      installCheckTimer = 0;
      if (!destroyed) showInstalled();
    }, installSettleDelayMs);
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
        waitForInstallReady();
      } else if (status) {
        status.textContent = 'Установка не завершена. Можно открыть уже установленное приложение или продолжить на сайте.';
        showFallback();
      }
    } catch {
      if (status) {
        status.textContent = 'Системное окно не открылось. Можно открыть уже установленное приложение или продолжить на сайте.';
      }
      showFallback();
    } finally {
      prompting = false;
    }
  };

  const onInstalled = () => {
    waitForInstallReady();
  };

  const onOpenClick = (event: Event) => {
    if (!android || standalone || !openButton) return;
    event.preventDefault();
    const target = new URL(openButton.href, windowRef.location.href);
    const fallback = target.toString();
    const intentPath = `${target.host}${target.pathname}${target.search}`;
    const intent = `intent://${intentPath}#Intent;scheme=${target.protocol.replace(':', '')};action=android.intent.action.VIEW;category=android.intent.category.BROWSABLE;S.browser_fallback_url=${encodeURIComponent(fallback)};end`;
    if (status) status.textContent = 'Открываем «Анонсы». Если приложение не откроется, запустите его с главного экрана.';
    windowRef.location.href = intent;
  };

  windowRef.addEventListener('beforeinstallprompt', onBeforeInstallPrompt);
  windowRef.addEventListener('appinstalled', onInstalled);
  button.addEventListener('click', onInstallClick);
  openButton?.addEventListener('click', onOpenClick);

  if (!standalone && typeof navigatorRef.getInstalledRelatedApps === 'function') {
    navigatorRef.getInstalledRelatedApps().then((apps) => {
      if (destroyed || apps.length === 0) return;
      root.dataset.focusPwaInstalled = 'true';
      if (status) status.textContent = '«Анонсы» уже установлены. Нажмите «Открыть».';
    }).catch(() => {});
  }

  return {
    destroy() {
      destroyed = true;
      windowRef.removeEventListener('beforeinstallprompt', onBeforeInstallPrompt);
      windowRef.removeEventListener('appinstalled', onInstalled);
      button.removeEventListener('click', onInstallClick);
      openButton?.removeEventListener('click', onOpenClick);
      if (installCheckTimer) windowRef.clearTimeout(installCheckTimer);
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
