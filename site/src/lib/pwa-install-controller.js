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

export function createPwaInstallController({
  windowRef,
  navigatorRef,
  root,
  button,
  status,
}) {
  if (!windowRef || !navigatorRef || !root || !button) return null;

  let installPrompt = null;
  let prompting = false;

  const hide = () => {
    root.hidden = true;
    button.hidden = true;
    button.disabled = false;
    root.dataset.pwaInstallReady = 'false';
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
    if (!isAndroidPlatform(navigatorRef) || isStandaloneDisplay(windowRef, navigatorRef)) return;
    event.preventDefault();
    installPrompt = event;
    if (status) status.textContent = 'Приложение можно установить.';
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
      await promptEvent.prompt();
    } finally {
      prompting = false;
      button.disabled = false;
      if (status) status.textContent = 'Системное окно установки закрыто.';
    }
  };

  const onAppInstalled = () => {
    if (status) status.textContent = 'Приложение установлено.';
    clear();
  };

  hide();
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
      root,
      button:root.querySelector('[data-pwa-install-button]'),
      status:root.querySelector('[data-pwa-install-status]'),
    });
  });
}
