export function isAndroidPlatform(navigatorRef) {
  const userAgentDataPlatform = String(navigatorRef?.userAgentData?.platform || '');
  const userAgent = String(navigatorRef?.userAgent || '');
  return /^android$/iu.test(userAgentDataPlatform) || /android/iu.test(userAgent);
}

export function isStandaloneDisplay(windowRef, navigatorRef) {
  return Boolean(
    windowRef?.matchMedia?.('(display-mode: standalone)')?.matches
    || navigatorRef?.standalone === true
  );
}

export function isPresentationInstall(locationRef) {
  return new URLSearchParams(String(locationRef?.search || '')).get('install') === 'presentation';
}

export function createPwaInstallController({
  windowRef,
  navigatorRef,
  locationRef = windowRef?.location,
  root,
  button,
  status,
  guidance,
}) {
  if (!windowRef || !navigatorRef || !root || !button) return null;

  let installPrompt = null;
  let prompting = false;
  const android = isAndroidPlatform(navigatorRef);
  const standalone = isStandaloneDisplay(windowRef, navigatorRef);
  const presentation = isPresentationInstall(locationRef);

  if (presentation) root.dataset.pwaInstallPresentation = 'true';

  const hide = () => {
    root.hidden = true;
    button.hidden = true;
    button.disabled = false;
    root.dataset.pwaInstallReady = 'false';
  };
  const showPresentationWaiting = () => {
    root.hidden = false;
    button.hidden = true;
    button.disabled = false;
    root.dataset.pwaInstallReady = 'false';
    if (guidance) guidance.hidden = false;
  };
  const reveal = () => {
    root.hidden = false;
    button.hidden = false;
    button.disabled = false;
    root.dataset.pwaInstallReady = 'true';
  };
  const clear = () => {
    installPrompt = null;
    prompting = false;
    hide();
  };

  const onBeforeInstallPrompt = (event) => {
    if (!android || standalone) return;
    event.preventDefault();
    installPrompt = event;
    if (status) status.textContent = 'Установка готова. Нажмите кнопку ниже.';
    reveal();
  };

  const onClick = async (event) => {
    event.preventDefault();
    if (!installPrompt || prompting) return;

    // A BeforeInstallPromptEvent is one-shot. Clear and hide atomically before
    // awaiting the system dialog so a double tap can never call prompt twice.
    const promptEvent = installPrompt;
    installPrompt = null;
    prompting = true;
    root.hidden = true;
    button.hidden = true;
    button.disabled = true;
    root.dataset.pwaInstallReady = 'false';

    try {
      const result = await promptEvent.prompt();
      if (presentation) {
        showPresentationWaiting();
        if (status) {
          status.textContent = result?.outcome === 'accepted'
            ? 'Установка подтверждена.'
            : 'Установка не завершена. Можно выбрать «Добавить на главный экран» в меню Chrome.';
        }
      }
    } catch {
      if (presentation) {
        showPresentationWaiting();
        if (status) status.textContent = 'Не удалось открыть системное окно. Используйте меню Chrome: «Добавить на главный экран».';
      }
    } finally {
      prompting = false;
      button.disabled = false;
      if (!presentation && status) status.textContent = 'Системное окно установки закрыто.';
    }
  };

  const onAppInstalled = () => {
    if (status) status.textContent = 'Приложение установлено.';
    if (presentation) {
      installPrompt = null;
      prompting = false;
      showPresentationWaiting();
      if (guidance) guidance.hidden = true;
    } else {
      clear();
    }
  };

  if (presentation) {
    showPresentationWaiting();
    if (standalone) {
      if (status) status.textContent = 'Приложение уже установлено.';
      if (guidance) guidance.hidden = true;
    } else if (!android) {
      if (status) status.textContent = 'Для установки откройте эту ссылку на Android-телефоне в Chrome.';
    } else if (status) {
      status.textContent = 'Подготавливаем установку. Если кнопка не появилась, откройте страницу в Chrome.';
    }
  } else {
    hide();
  }
  windowRef.addEventListener('beforeinstallprompt', onBeforeInstallPrompt);
  windowRef.addEventListener('appinstalled', onAppInstalled);
  button.addEventListener('click', onClick);

  return {
    destroy() {
      windowRef.removeEventListener('beforeinstallprompt', onBeforeInstallPrompt);
      windowRef.removeEventListener('appinstalled', onAppInstalled);
      button.removeEventListener('click', onClick);
      clear();
    },
    get ready() {
      return Boolean(installPrompt);
    },
  };
}

export function hydratePwaInstallActions({
  documentRef = document,
  windowRef = window,
  navigatorRef = navigator,
} = {}) {
  documentRef.querySelectorAll('[data-pwa-install-root]').forEach((root) => {
    if (root.dataset.pwaInstallBound === 'true') return;
    root.dataset.pwaInstallBound = 'true';
    createPwaInstallController({
      windowRef,
      navigatorRef,
      locationRef:windowRef.location,
      root,
      button:root.querySelector('[data-pwa-install-button]'),
      status:root.querySelector('[data-pwa-install-status]'),
      guidance:root.querySelector('[data-pwa-install-guidance]'),
    });
  });
}
