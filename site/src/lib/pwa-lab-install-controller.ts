import {
  createPwaInstallController,
  isStandaloneDisplay,
} from './pwa-install-controller.js';

interface LabPwaInstallRoot extends HTMLElement {
  dataset: DOMStringMap;
}

interface LabPwaInstallOptions {
  windowRef: Window;
  navigatorRef: Navigator;
  root: LabPwaInstallRoot;
  button: HTMLButtonElement;
  status?: HTMLElement | null;
  guidance?: HTMLElement | null;
  standaloneStatus?: HTMLElement | null;
  installDetail?: HTMLElement | null;
}

const AFTER_ACCEPTED = 'Закройте вкладку и откройте PWA Lab через новую иконку.';

export function createLabPwaInstallController({
  windowRef,
  navigatorRef,
  root,
  button,
  status,
  guidance,
  standaloneStatus,
  installDetail,
}: LabPwaInstallOptions) {
  const standalone = isStandaloneDisplay(windowRef, navigatorRef);
  const controller = createPwaInstallController({
    windowRef,
    navigatorRef,
    // Reuse the focus-group controller's controlled presentation mode without
    // changing the visible lab URL or relying on a query parameter.
    locationRef: {
      search: '?install=presentation',
      assign(href: string) { windowRef.location.assign(href); },
      set href(href: string) { windowRef.location.href = href; },
    },
    root,
    button,
    status,
    guidance,
    onInstallResult(result) {
      if (!installDetail) return;
      if (result.phase === 'ready') {
        const platforms = result.platforms?.length ? result.platforms.join(', ') : 'не указана';
        installDetail.textContent = `Системный install event: готов; platform: ${platforms}.`;
      } else if (result.phase === 'prompt-result') {
        installDetail.textContent = `Системный prompt: ${result.outcome}; platform: ${result.platform || 'не указана'}. Это ещё не подтверждает WebAPK.`;
      } else if (result.phase === 'appinstalled') {
        installDetail.textContent = 'Событие appinstalled: получено; WebAPK подтверждается только launcher/Settings и standalone-запуском.';
      } else if (result.phase === 'prompt-error') {
        installDetail.textContent = `Ошибка системного prompt: ${result.message || 'без сообщения'}.`;
      }
    },
    texts: {
      installedButton: 'Приложение установлено',
      standaloneStatus: 'PWA Lab запущена с главного экрана',
      installedStatus: AFTER_ACCEPTED,
      installingStatus: AFTER_ACCEPTED,
      readyStatus: 'Установка PWA Lab готова. Нажмите кнопку ниже.',
      nonAndroidStatus: 'Автоматическая установка не поддерживается в этом браузере. Используйте Chrome на Android.',
      waitingStatus: 'Ожидаем готовности браузера к установке PWA Lab.',
      promptErrorStatus: 'Не удалось открыть системное окно. В Chrome Custom Tab откройте меню ⋮ → «Открыть в Chrome» и обновите страницу.',
    },
  });

  if (standalone) {
    root.dataset.pwaLabStandalone = 'true';
    button.textContent = 'Приложение установлено';
    button.disabled = true;
    if (status) status.textContent = 'PWA Lab запущена с главного экрана';
    if (standaloneStatus) {
      standaloneStatus.hidden = false;
      standaloneStatus.textContent = 'display-mode: standalone';
    }
    if (installDetail) installDetail.textContent = 'Запуск standalone подтверждён браузером.';
  }

  return controller;
}

export function hydrateLabPwaInstallAction({
  documentRef = document,
  windowRef = window,
  navigatorRef = navigator,
} = {}) {
  const root = documentRef.querySelector<LabPwaInstallRoot>('[data-pwa-lab-install]');
  if (!root || root.dataset.pwaLabBound === 'true') return null;
  const button = root.querySelector<HTMLButtonElement>('[data-pwa-install-button]');
  if (!button) return null;
  root.dataset.pwaLabBound = 'true';
  return createLabPwaInstallController({
    windowRef,
    navigatorRef,
    root,
    button,
    status: root.querySelector<HTMLElement>('[data-pwa-install-status]'),
    guidance: root.querySelector<HTMLElement>('[data-pwa-install-guidance]'),
    standaloneStatus: root.querySelector<HTMLElement>('[data-pwa-install-standalone]'),
    installDetail: root.querySelector<HTMLElement>('[data-pwa-install-detail]'),
  });
}
