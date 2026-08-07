import { getResilientDataClient } from '../lib/resilientDataClient';
import { normalizePrelaunchEmail } from '../lib/prelaunchEmail';
import { parseSupabaseTransportError } from '../lib/resilientSupabaseTransport';

const STORAGE_KEY = 'ke_prelaunch_notification_v1';
const WINDOW_COUNT = 8;
const CONSENT_POLICY = 'prelaunch-updates-2026-v1';
const CONSENT_COPY = 'Согласен получать письма о запуске, важных обновлениях и полезных подборках сервиса. Отписаться можно в любой момент.';
const PROMISE_COPY = 'Оставьте e-mail — и 1 сентября мы пришлём ссылку на сервис. Иногда будем отправлять важные обновления и полезные подборки; для подписавшихся мы приготовили приятный сюрприз.';

type ExperienceState = 'idle' | 'submitting' | 'error' | 'success' | 'registered';

type TileMetric = {
  tile: HTMLElement;
  index: number;
  row: number;
  column: number;
  centerX: number;
  centerY: number;
  width: number;
  height: number;
};

function splitGridTracks(value: string): string[] {
  const tracks: string[] = [];
  let depth = 0;
  let start = 0;
  for (let index = 0; index < value.length; index += 1) {
    const character = value[index];
    if (character === '(') depth += 1;
    if (character === ')') depth = Math.max(0, depth - 1);
    if (character === ' ' && depth === 0) {
      const track = value.slice(start, index).trim();
      if (track) tracks.push(track);
      start = index + 1;
    }
  }
  const tail = value.slice(start).trim();
  if (tail) tracks.push(tail);
  return tracks;
}

function initializePrelaunchExperience(): void {
  const root = document.querySelector<HTMLElement>('[data-prelaunch-page]');
  if (!root || root.dataset.experienceReady === 'true') return;
  root.dataset.experienceReady = 'true';

  const mosaic = root.querySelector<HTMLElement>('[data-prelaunch-mosaic]');
  const projection = root.querySelector<HTMLImageElement>('.prelaunch__projection');
  const tiles = Array.from(root.querySelectorAll<HTMLElement>('[data-prelaunch-tile]'));
  let resizeFrame = 0;

  function artworkElement(): HTMLElement | null {
    return root?.querySelector<HTMLElement>('.prelaunch__artwork') || projection;
  }

  function calibrateTiles(): void {
    if (!mosaic || tiles.length === 0) return;
    const computedColumns = splitGridTracks(getComputedStyle(mosaic).gridTemplateColumns).length;
    const columns = Math.max(1, computedColumns || 9);
    const rows = Math.ceil(tiles.length / columns);
    const sourceColumn = columns + .35;
    const sourceRow = -.75;

    const metrics: TileMetric[] = tiles.map((tile, index) => {
      const row = Math.floor(index / columns);
      const column = index % columns;
      const deltaX = sourceColumn - column;
      const deltaY = (row - sourceRow) * .84;
      const sourceDistance = Math.hypot(deltaX, deltaY);
      const rect = tile.getBoundingClientRect();

      let edge = 'ambient';
      if (sourceDistance < 2.45) edge = 'hot';
      else if (sourceDistance < 4.2) edge = 'warm';
      else if (sourceDistance < 5.8) edge = 'soft';

      tile.dataset.edge = edge;
      tile.dataset.row = String(row);
      tile.dataset.column = String(column);
      tile.dataset.window = 'false';
      tile.dataset.accent = 'false';
      return {
        tile,
        index,
        row,
        column,
        centerX: rect.left + rect.width / 2,
        centerY: rect.top + rect.height / 2,
        width: rect.width,
        height: rect.height,
      };
    });

    const artwork = artworkElement();
    const artworkRect = artwork?.getBoundingClientRect();
    const targetX = artworkRect && artworkRect.width > 0
      ? artworkRect.left + artworkRect.width * .5
      : window.innerWidth * .54;
    const targetY = artworkRect && artworkRect.height > 0
      ? artworkRect.top + artworkRect.height * .62
      : window.innerHeight * .65;

    metrics
      .filter(({ tile }) => tile.dataset.state === 'revealed')
      .sort((left, right) => {
        const leftDistance = Math.hypot(
          (left.centerX - targetX) / Math.max(1, left.width),
          (left.centerY - targetY) / Math.max(1, left.height),
        );
        const rightDistance = Math.hypot(
          (right.centerX - targetX) / Math.max(1, right.width),
          (right.centerY - targetY) / Math.max(1, right.height),
        );
        return leftDistance - rightDistance || left.index - right.index;
      })
      .slice(0, WINDOW_COUNT)
      .forEach(({ tile }) => {
        tile.dataset.window = 'true';
      });

    const accentCoordinates = [
      { row: 1, column: columns - 1 },
      { row: 2, column: Math.max(0, columns - 2) },
      { row: 3, column: columns - 1 },
    ];
    for (const coordinate of accentCoordinates) {
      const match = metrics.find(
        ({ row, column }) => row === coordinate.row && column === coordinate.column,
      );
      if (match) match.tile.dataset.accent = 'true';
    }

    root.style.setProperty('--prelaunch-grid-columns', String(columns));
    root.style.setProperty('--prelaunch-grid-rows', String(rows));
    root.dataset.artworkRevealAligned = artworkRect && artworkRect.width > 0 ? 'true' : 'fallback';
  }

  function scheduleCalibration(): void {
    if (resizeFrame) cancelAnimationFrame(resizeFrame);
    resizeFrame = requestAnimationFrame(() => {
      resizeFrame = 0;
      calibrateTiles();
    });
  }

  calibrateTiles();
  window.addEventListener('prelaunch-artwork-ready', scheduleCalibration, { passive: true });

  if (mosaic && 'ResizeObserver' in window) {
    const observer = new ResizeObserver(scheduleCalibration);
    observer.observe(mosaic);
    const artwork = artworkElement();
    if (artwork) observer.observe(artwork);
  } else {
    window.addEventListener('resize', scheduleCalibration, { passive: true });
  }

  const tileStateObserver = new MutationObserver(scheduleCalibration);
  for (const tile of tiles) {
    tileStateObserver.observe(tile, {
      attributes: true,
      attributeFilter: ['data-state'],
    });
  }

  const form = root.querySelector<HTMLFormElement>('[data-prelaunch-form]');
  if (!form || form.dataset.experienceBound === 'true') return;
  form.dataset.experienceBound = 'true';
  form.dataset.consentPolicy = CONSENT_POLICY;

  const row = form.querySelector<HTMLElement>('.prelaunch-form__row');
  const consent = form.querySelector<HTMLElement>('.prelaunch-form__consent');
  const consentCopy = consent?.querySelector<HTMLElement>('span');
  const status = form.querySelector<HTMLElement>('[data-prelaunch-status]');
  const submit = form.querySelector<HTMLButtonElement>('[data-prelaunch-submit]');
  const submitLabel = form.querySelector<HTMLElement>('[data-prelaunch-submit-label]');
  const email = form.elements.namedItem('email') as HTMLInputElement | null;
  const consentInput = form.elements.namedItem('consent') as HTMLInputElement | null;
  const website = form.elements.namedItem('website') as HTMLInputElement | null;

  if (consentCopy) consentCopy.textContent = CONSENT_COPY;

  const promise = document.createElement('p');
  promise.className = 'prelaunch-form__promise';
  promise.id = 'prelaunch-promise';
  promise.textContent = PROMISE_COPY;
  row?.insertAdjacentElement('afterend', promise);

  if (email) {
    const describedBy = new Set(
      String(email.getAttribute('aria-describedby') || '')
        .split(/\s+/u)
        .filter(Boolean),
    );
    describedBy.add(promise.id);
    email.setAttribute('aria-describedby', [...describedBy].join(' '));
  }

  const complete = document.createElement('div');
  complete.className = 'prelaunch-form__complete';
  complete.hidden = true;
  complete.tabIndex = -1;
  complete.setAttribute('role', 'status');
  complete.setAttribute('aria-live', 'polite');

  const completeMark = document.createElement('span');
  completeMark.className = 'prelaunch-form__complete-mark';
  completeMark.setAttribute('aria-hidden', 'true');

  const completeCopy = document.createElement('p');
  completeCopy.className = 'prelaunch-form__complete-copy';
  const completeTitle = document.createElement('strong');
  const completeBody = document.createElement('span');
  completeCopy.append(completeTitle, completeBody);

  const reset = document.createElement('button');
  reset.className = 'prelaunch-form__reset';
  reset.type = 'button';
  reset.textContent = 'Другой e-mail';

  complete.append(completeMark, completeCopy, reset);
  form.append(complete);

  function announce(kind: 'idle' | 'success' | 'error', message: string): void {
    if (!status) return;
    status.hidden = false;
    status.dataset.kind = kind;
    status.textContent = message;
  }

  function setSubmitting(active: boolean): void {
    if (!submit || !submitLabel) return;
    submit.disabled = active;
    submit.setAttribute('aria-busy', String(active));
    submitLabel.textContent = active ? 'Сохраняем…' : 'Напомнить о запуске';
  }

  function setState(
    state: ExperienceState,
    options: { title?: string; body?: string; focus?: boolean } = {},
  ): void {
    form.dataset.experienceState = state;
    const completeState = state === 'success' || state === 'registered';

    if (row) row.hidden = completeState;
    if (consent) consent.hidden = completeState;
    promise.hidden = completeState;
    complete.hidden = !completeState;
    if (status) status.hidden = completeState;

    if (completeState) {
      completeTitle.textContent = options.title || 'Вы записаны';
      completeBody.textContent = options.body || '1 сентября пришлём ссылку на сервис. Дальше будем писать только о важных обновлениях и полезных подборках; отписаться можно из любого письма.';
      if (options.focus) complete.focus({ preventScroll: true });
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

  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    event.stopImmediatePropagation();
    announce('idle', '');

    const emailResult = normalizePrelaunchEmail(email?.value ?? '');
    if (!emailResult.ok) {
      form.dataset.emailValidation = emailResult.reason;
      form.dataset.experienceState = 'error';
      email?.setCustomValidity('Проверьте адрес электронной почты.');
      announce('error', 'Проверьте адрес электронной почты.');
      email?.focus();
      return;
    }
    const normalizedEmail = emailResult.email;
    if (email) {
      email.value = normalizedEmail;
      email.setCustomValidity('');
    }
    form.dataset.emailValidation = 'accepted';

    if (!consentInput?.checked) {
      form.dataset.experienceState = 'error';
      announce('error', 'Подтвердите согласие на письма о запуске и важных обновлениях.');
      consentInput?.focus();
      return;
    }

    const directUrl = String(import.meta.env.PUBLIC_PERSONALIZATION_SUPABASE_URL || '').replace(/\/+$/u, '');
    const relayUrl = String(import.meta.env.PUBLIC_PERSONALIZATION_SUPABASE_RELAY_URL || '').replace(/\/+$/u, '');
    const publishableKey = String(import.meta.env.PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY || '');
    if (!directUrl || !publishableKey) {
      form.dataset.experienceState = 'error';
      announce('error', 'Форма временно недоступна. Попробуйте ещё раз немного позже.');
      return;
    }

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
      const payload = await response.json().catch(() => null) as { accepted?: boolean; status?: string } | null;
      if (!response.ok) throw new Error(`prelaunch_signup_http_${response.status}`);
      if (!payload || payload.accepted !== true) {
        throw new Error(payload?.status === 'daily_capacity_reached'
          ? 'prelaunch_signup_capacity'
          : 'prelaunch_signup_rejected');
      }

      form.reset();
      try {
        window.localStorage.setItem(STORAGE_KEY, 'registered');
      } catch {
        // Registration is durable in Supabase; localStorage is only a UX hint.
      }
      setState('success', {
        title: 'Готово, вы записаны',
        body: '1 сентября пришлём ссылку на сервис. Для подписавшихся мы приготовили приятный сюрприз; затем будем писать только о важных обновлениях и полезных подборках.',
        focus: true,
      });
    } catch (error) {
      form.dataset.experienceState = 'error';
      const transport = parseSupabaseTransportError(error);
      if (transport?.code === 'ambiguous') {
        announce('error', 'Связь прервалась. Повторите отправку — второй записи не появится.');
      } else if (String((error as Error)?.message || '').includes('capacity')) {
        announce('error', 'Сейчас слишком много запросов. Попробуйте ещё раз позже.');
      } else {
        announce('error', 'Не удалось сохранить email. Проверьте соединение и повторите отправку.');
      }
    } finally {
      setSubmitting(false);
    }
  }, { capture: true });

  reset.addEventListener('click', () => {
    try {
      window.localStorage.removeItem(STORAGE_KEY);
    } catch {
      // UX hint only.
    }
    form.reset();
    announce('idle', '');
    setState('idle');
    email?.focus({ preventScroll: true });
  });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initializePrelaunchExperience, { once: true });
} else {
  initializePrelaunchExperience();
}
