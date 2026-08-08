import { getResilientDataClient } from '../lib/resilientDataClient';
import { normalizePrelaunchEmail } from '../lib/prelaunchEmail';
import { parseSupabaseTransportError } from '../lib/resilientSupabaseTransport';

const STORAGE_KEY = 'ke_prelaunch_notification_v1';
const CONSENT_POLICY = 'prelaunch-updates-2026-v1';
const EMAIL_PATTERN = "[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+";

type FormState = 'idle' | 'submitting' | 'error' | 'success' | 'registered';

function initializePrelaunchForm(): void {
  const form = document.querySelector<HTMLFormElement>('[data-prelaunch-form]');
  if (!form || form.dataset.experienceBound === 'true') return;
  form.dataset.experienceBound = 'true';
  form.dataset.emailGuardReady = 'true';
  form.dataset.consentPolicy = CONSENT_POLICY;

  const row = form.querySelector<HTMLElement>('.prelaunch-form__row');
  const promise = form.querySelector<HTMLElement>('.prelaunch-form__promise');
  const consent = form.querySelector<HTMLElement>('.prelaunch-form__consent');
  const status = form.querySelector<HTMLElement>('[data-prelaunch-status]');
  const complete = form.querySelector<HTMLElement>('[data-prelaunch-complete]');
  const completeTitle = form.querySelector<HTMLElement>('[data-prelaunch-complete-title]');
  const completeBody = form.querySelector<HTMLElement>('[data-prelaunch-complete-body]');
  const reset = form.querySelector<HTMLButtonElement>('[data-prelaunch-reset]');
  const submit = form.querySelector<HTMLButtonElement>('[data-prelaunch-submit]');
  const submitLabel = form.querySelector<HTMLElement>('[data-prelaunch-submit-label]');
  const email = form.elements.namedItem('email') as HTMLInputElement | null;
  const consentInput = form.elements.namedItem('consent') as HTMLInputElement | null;
  const website = form.elements.namedItem('website') as HTMLInputElement | null;
  if (!email || !consentInput || !submit || !submitLabel) return;
  let requestInFlight = false;

  email.pattern = EMAIL_PATTERN;
  email.autocapitalize = 'none';
  email.spellcheck = false;

  function announce(kind: 'idle' | 'success' | 'error', message: string): void {
    if (!status) return;
    status.hidden = false;
    status.dataset.kind = kind;
    status.textContent = message;
  }

  function setSubmitting(active: boolean): void {
    submit.disabled = active;
    submit.setAttribute('aria-busy', String(active));
    submitLabel.textContent = active ? 'Сохраняем…' : 'Напомнить о запуске';
  }

  function setState(
    state: FormState,
    options: { title?: string; body?: string; focus?: boolean } = {},
  ): void {
    form.dataset.experienceState = state;
    const completeState = state === 'success' || state === 'registered';
    if (row) row.hidden = completeState;
    if (promise) promise.hidden = completeState;
    if (consent) consent.hidden = completeState;
    if (complete) complete.hidden = !completeState;
    if (status) status.hidden = completeState;

    if (completeState) {
      if (completeTitle) completeTitle.textContent = options.title || 'Вы записаны';
      if (completeBody) {
        completeBody.textContent = options.body
          || '1 сентября пришлём ссылку на сервис. Дальше будем писать только о важных обновлениях и полезных подборках; отписаться можно из любого письма.';
      }
      if (options.focus) complete?.focus({ preventScroll: true });
    }
  }

  function readRegistrationHint(): boolean {
    try {
      return window.localStorage.getItem(STORAGE_KEY) === 'registered';
    } catch {
      return false;
    }
  }

  if (readRegistrationHint()) {
    setState('registered', {
      title: 'Вы уже записаны',
      body: '1 сентября пришлём ссылку на сервис. Для подписавшихся будет приятный сюрприз, а дальше — только важные обновления и полезные подборки.',
    });
  } else {
    setState('idle');
  }

  email.addEventListener('input', () => {
    email.setCustomValidity('');
    if (form.dataset.experienceState === 'error') {
      form.dataset.emailValidation = 'pending';
      announce('idle', '');
      setState('idle');
    }
  });

  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    // `disabled` blocks ordinary pointer/keyboard activation.  Keep a runtime
    // lock as well so a scripted or repeated SubmitEvent cannot start a second
    // idempotent transport while the first request is unresolved.
    if (requestInFlight) return;
    announce('idle', '');

    const emailResult = normalizePrelaunchEmail(email.value);
    if (!emailResult.ok) {
      form.dataset.emailValidation = emailResult.reason;
      email.setCustomValidity('Проверьте адрес электронной почты.');
      setState('error');
      announce('error', 'Проверьте адрес электронной почты.');
      email.focus({ preventScroll: true });
      return;
    }

    const normalizedEmail = emailResult.email;
    email.value = normalizedEmail;
    email.setCustomValidity('');
    form.dataset.emailValidation = 'accepted';

    if (!consentInput.checked) {
      setState('error');
      announce('error', 'Подтвердите согласие на письма о запуске и важных обновлениях.');
      consentInput.focus({ preventScroll: true });
      return;
    }

    const directUrl = String(import.meta.env.PUBLIC_PERSONALIZATION_SUPABASE_URL || '').replace(/\/+$/u, '');
    const relayUrl = String(import.meta.env.PUBLIC_PERSONALIZATION_SUPABASE_RELAY_URL || '').replace(/\/+$/u, '');
    const publishableKey = String(import.meta.env.PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY || '');
    if (!directUrl || !publishableKey) {
      setState('error');
      announce('error', 'Форма временно недоступна. Попробуйте ещё раз немного позже.');
      return;
    }

    requestInFlight = true;
    setState('submitting');
    setSubmitting(true);
    try {
      const client = getResilientDataClient({
        directUrl,
        relayUrl,
        publishableKey,
        routeCacheNamespace: 'prelaunch-notification-v2',
      });
      const response = await client.request(
        `${directUrl}/rest/v1/rpc/register_prelaunch_notification_v1`,
        {
          method: 'POST',
          cache: 'no-store',
          credentials: 'omit',
          headers: {
            apikey: publishableKey,
            Authorization: `Bearer ${publishableKey}`,
            Accept: 'application/json',
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            p_email: normalizedEmail,
            p_source: 'prelaunch_home',
            p_consent_version: CONSENT_POLICY,
            p_website: String(website?.value || '').trim(),
          }),
        },
      );
      const payload = await response.json().catch(() => null) as {
        accepted?: boolean;
        status?: string;
      } | null;
      if (!response.ok) throw new Error(`prelaunch_signup_http_${response.status}`);
      if (!payload || payload.accepted !== true) {
        throw new Error(payload?.status === 'daily_capacity_reached'
          ? 'prelaunch_signup_capacity'
          : 'prelaunch_signup_rejected');
      }
      if (payload.status !== 'registered' && payload.status !== 'already_registered') {
        throw new Error('prelaunch_signup_unexpected_status');
      }

      form.reset();
      try {
        window.localStorage.setItem(STORAGE_KEY, 'registered');
      } catch {
        // Durable registration is server-side; localStorage is only a UX hint.
      }
      const alreadyRegistered = payload.status === 'already_registered';
      setState(alreadyRegistered ? 'registered' : 'success', alreadyRegistered
        ? {
            title: 'Вы уже записаны',
            body: '1 сентября пришлём ссылку на сервис. Для подписавшихся будет приятный сюрприз, а дальше — только важные обновления и полезные подборки.',
            focus: true,
          }
        : {
            title: 'Готово, вы записаны',
            body: '1 сентября пришлём ссылку на сервис. Для подписавшихся мы приготовили приятный сюрприз; затем будем писать только о важных обновлениях и полезных подборках.',
            focus: true,
          });
    } catch (error) {
      setState('error');
      const transport = parseSupabaseTransportError(error);
      if (transport?.code === 'ambiguous') {
        announce('error', 'Связь прервалась. Повторите отправку — второй записи не появится.');
      } else if (String((error as Error)?.message || '').includes('capacity')) {
        announce('error', 'Сейчас слишком много запросов. Попробуйте ещё раз позже.');
      } else {
        announce('error', 'Не удалось сохранить email. Проверьте соединение и повторите отправку.');
      }
    } finally {
      requestInFlight = false;
      setSubmitting(false);
    }
  });

  reset?.addEventListener('click', () => {
    try {
      window.localStorage.removeItem(STORAGE_KEY);
    } catch {
      // UX hint only.
    }
    form.reset();
    announce('idle', '');
    setState('idle');
    email.focus({ preventScroll: true });
  });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initializePrelaunchForm, { once: true });
} else {
  initializePrelaunchForm();
}
