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
  texts = {},
}) {
  if (!windowRef || !navigatorRef || !root || !button) return null;

  let installPrompt = null;
  let prompting = false;
  let installPoll = null;
  const android = isAndroidPlatform(navigatorRef);
  const standalone = isStandaloneDisplay(windowRef, navigatorRef);
  const presentation = isPresentationInstall(locationRef);
  const installId = String(root.dataset.pwaInstallId || 'default').replace(/[^a-z0-9/_-]/giu, '').slice(0, 160) || 'default';
  const installMarkerKey = `ke:pwa:installed:${installId}`;
  const copy = {
    installedButton: 'Открыть Анонсы',
    standaloneStatus: 'Приложение открыто.',
    installedStatus: 'Если значок ещё не появился, подождите до минуты.',
    installingButton: 'Устанавливается…',
    installingStatus: 'Установка началась. Она завершится в течение минуты.',
    readyStatus: 'Установка готова. Нажмите кнопку ниже.',
    cancelledStatus: 'Установка не завершена. Обновите страницу, когда захотите повторить.',
    promptErrorStatus: 'Не удалось открыть системное окно. Откройте страницу в Chrome и обновите её.',
    dialogClosedStatus: 'Системное окно установки закрыто.',
    appInstalledStatus: 'Приложение установлено.',
    nonAndroidStatus: 'Для установки откройте эту ссылку на Android-телефоне в Chrome.',
    waitingStatus: 'Подготавливаем установку. Если кнопка не появилась, откройте страницу в Chrome.',
    ...texts,
  };
  const storage = (() => {
    try { return windowRef.localStorage || null; } catch { return null; }
  })();
  const readInstalledMarker = () => {
    try { return storage?.getItem(installMarkerKey) === '1'; } catch { return false; }
  };
  const writeInstalledMarker = (value) => {
    try {
      if (value) storage?.setItem(installMarkerKey, '1');
      else storage?.removeItem(installMarkerKey);
    } catch { /* storage is optional */ }
  };
  let installed = standalone || readInstalledMarker();

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
  const stopInstallPoll = () => {
    if (installPoll !== null && typeof windowRef.clearInterval === 'function') {
      windowRef.clearInterval(installPoll);
    }
    installPoll = null;
  };
  const showInstalled = () => {
    stopInstallPoll();
    installed = true;
    writeInstalledMarker(true);
    root.hidden = false;
    button.hidden = false;
    button.disabled = false;
    button.textContent = copy.installedButton;
    root.dataset.pwaInstallReady = 'installed';
    if (status) status.textContent = standalone ? copy.standaloneStatus : copy.installedStatus;
    if (guidance) guidance.hidden = true;
  };
  const showInstalling = () => {
    root.hidden = false;
    button.hidden = false;
    button.disabled = true;
    button.textContent = copy.installingButton;
    root.dataset.pwaInstallReady = 'installing';
    if (status) status.textContent = copy.installingStatus;
    if (guidance) guidance.hidden = true;
    if (installPoll === null && typeof windowRef.setInterval === 'function') {
      installPoll = windowRef.setInterval(() => {
        if (isStandaloneDisplay(windowRef, navigatorRef)) showInstalled();
      }, 750);
    }
  };
  const reveal = () => {
    root.hidden = false;
    button.hidden = false;
    button.disabled = false;
    root.dataset.pwaInstallReady = 'true';
  };
  const clear = () => {
    stopInstallPoll();
    installPrompt = null;
    prompting = false;
    hide();
  };

  const onBeforeInstallPrompt = (event) => {
    if (!android || standalone) return;
    event.preventDefault();
    installed = false;
    writeInstalledMarker(false);
    if (windowRef.__kenigEventsPwaInstallPrompt === event) {
      windowRef.__kenigEventsPwaInstallPrompt = null;
    }
    installPrompt = event;
    if (status) status.textContent = copy.readyStatus;
    reveal();
  };

  const onClick = async (event) => {
    event.preventDefault();
    if (installed) {
      const openHref = String(root.dataset.pwaOpenHref || '');
      if (openHref && typeof locationRef?.assign === 'function') locationRef.assign(openHref);
      else if (openHref && locationRef) locationRef.href = openHref;
      return;
    }
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
        if (result?.outcome === 'accepted') showInstalling();
        else {
          showPresentationWaiting();
          if (status) status.textContent = copy.cancelledStatus;
        }
      }
    } catch {
      if (presentation) {
        showPresentationWaiting();
        if (status) status.textContent = copy.promptErrorStatus;
      }
    } finally {
      prompting = false;
      if (root.dataset.pwaInstallReady !== 'installing') button.disabled = false;
      if (!presentation && status) status.textContent = copy.dialogClosedStatus;
    }
  };

  const onAppInstalled = () => {
    if (presentation) {
      installPrompt = null;
      prompting = false;
      showInstalled();
    } else {
      if (status) status.textContent = copy.appInstalledStatus;
      writeInstalledMarker(true);
      clear();
    }
  };

  if (presentation) {
    if (installed) {
      showInstalled();
    } else if (!android) {
      showPresentationWaiting();
      if (status) status.textContent = copy.nonAndroidStatus;
    } else {
      showPresentationWaiting();
      if (status) status.textContent = copy.waitingStatus;
    }
  } else {
    hide();
  }
  windowRef.addEventListener('beforeinstallprompt', onBeforeInstallPrompt);
  windowRef.addEventListener('appinstalled', onAppInstalled);
  button.addEventListener('click', onClick);
  const capturedPrompt = windowRef.__kenigEventsPwaInstallPrompt;
  if (capturedPrompt) onBeforeInstallPrompt(capturedPrompt);

  return {
    destroy() {
      stopInstallPoll();
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
