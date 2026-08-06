const PROMISE = 'Оставьте e-mail — 1 сентября пришлём ссылку на сервис. Для подписавшихся будет приятный сюрприз.';
const CONSENT = 'Согласен получать письма о запуске и новостях сервиса. Отписаться можно в любой момент.';

function calibratePrelaunchCopy(): void {
  const form = document.querySelector<HTMLFormElement>('[data-prelaunch-form]');
  if (!form) return;

  const apply = (): boolean => {
    const promise = form.querySelector<HTMLElement>('.prelaunch-form__promise');
    const consent = form.querySelector<HTMLElement>('.prelaunch-form__consent span');
    if (promise) promise.textContent = PROMISE;
    if (consent) consent.textContent = CONSENT;
    if (promise && consent) {
      form.dataset.copyCalibrated = 'true';
      return true;
    }
    return false;
  };

  if (apply()) return;

  const observer = new MutationObserver(() => {
    if (!apply()) return;
    observer.disconnect();
  });
  observer.observe(form, { childList: true, subtree: true });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', calibratePrelaunchCopy, { once: true });
} else {
  calibratePrelaunchCopy();
}
