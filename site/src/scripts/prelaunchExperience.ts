const STORAGE_KEY = 'ke_prelaunch_notification_v1';
const WINDOW_COUNT = 8;

type ExperienceState = 'idle' | 'submitting' | 'error' | 'success' | 'registered';

type TileMetric = {
  tile: HTMLElement;
  index: number;
  row: number;
  column: number;
  sourceDistance: number;
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
  const tiles = Array.from(root.querySelectorAll<HTMLElement>('[data-prelaunch-tile]'));
  let resizeFrame = 0;

  function calibrateTiles(): void {
    if (!mosaic || tiles.length === 0) return;
    const computedColumns = splitGridTracks(getComputedStyle(mosaic).gridTemplateColumns).length;
    const columns = Math.max(1, computedColumns || 9);
    const rows = Math.ceil(tiles.length / columns);
    const sourceColumn = columns + 0.35;
    const sourceRow = -0.75;

    const metrics: TileMetric[] = tiles.map((tile, index) => {
      const row = Math.floor(index / columns);
      const column = index % columns;
      const deltaX = sourceColumn - column;
      const deltaY = (row - sourceRow) * 0.84;
      const sourceDistance = Math.hypot(deltaX, deltaY);

      let edge = 'ambient';
      if (sourceDistance < 2.45) edge = 'hot';
      else if (sourceDistance < 4.2) edge = 'warm';
      else if (sourceDistance < 5.8) edge = 'soft';

      tile.dataset.edge = edge;
      tile.dataset.row = String(row);
      tile.dataset.column = String(column);
      tile.dataset.window = 'false';
      tile.dataset.accent = 'false';
      return { tile, index, row, column, sourceDistance };
    });

    // Keep one coherent reveal cluster around the branded wordmark. The map is
    // recomputed from the real column count, so desktop and mobile share the
    // same rule instead of maintaining separate hand-authored scenes.
    const targetColumn = (columns - 1) * 0.48;
    const targetRow = (rows - 1) * 0.61;
    metrics
      .filter(({ tile }) => tile.dataset.state === 'revealed')
      .sort((left, right) => {
        const leftDistance = Math.hypot(
          (left.column - targetColumn) * 1.05,
          left.row - targetRow,
        );
        const rightDistance = Math.hypot(
          (right.column - targetColumn) * 1.05,
          right.row - targetRow,
        );
        return leftDistance - rightDistance || left.index - right.index;
      })
      .slice(0, WINDOW_COUNT)
      .forEach(({ tile }) => {
        tile.dataset.window = 'true';
      });

    // Three deterministic edge accents sit along the incoming top-right ray.
    // They strengthen borders only; no pane receives a local radial spotlight.
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
  }

  function scheduleCalibration(): void {
    if (resizeFrame) cancelAnimationFrame(resizeFrame);
    resizeFrame = requestAnimationFrame(() => {
      resizeFrame = 0;
      calibrateTiles();
    });
  }

  calibrateTiles();
  if (mosaic && 'ResizeObserver' in window) {
    const observer = new ResizeObserver(scheduleCalibration);
    observer.observe(mosaic);
  } else {
    window.addEventListener('resize', scheduleCalibration, { passive: true });
  }

  // The underlying hero-talk motion mutates only data-state. Recompute the
  // coherent reveal cluster after each sparse transition without observing the
  // enhancement attributes themselves, avoiding a mutation loop.
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

  const row = form.querySelector<HTMLElement>('.prelaunch-form__row');
  const consent = form.querySelector<HTMLElement>('.prelaunch-form__consent');
  const status = form.querySelector<HTMLElement>('[data-prelaunch-status]');
  const email = form.elements.namedItem('email') as HTMLInputElement | null;

  const promise = document.createElement('p');
  promise.className = 'prelaunch-form__promise';
  promise.id = 'prelaunch-promise';
  promise.textContent = 'Оставьте e-mail — и 1 сентября мы пришлём письмо о запуске. Для подписавшихся мы приготовили отдельный приятный сюрприз.';
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
      completeBody.textContent = options.body || '1 сентября пришлём письмо о запуске. И не забудем про обещанный приятный сюрприз.';
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
      body: '1 сентября пришлём письмо о запуске. Для подписавшихся будет отдельный приятный сюрприз.',
    });
  } else {
    setState('idle');
  }

  form.addEventListener('submit', () => {
    if (form.dataset.experienceState !== 'registered') setState('submitting');
  });

  if (status) {
    const reflectStatus = (): void => {
      const kind = status.dataset.kind;
      const message = status.textContent?.trim() || '';
      if (kind === 'success' && message) {
        setState('success', {
          title: 'Готово, вы записаны',
          body: '1 сентября пришлём письмо со ссылкой на сервис. Для подписавшихся мы приготовили отдельный приятный сюрприз.',
          focus: true,
        });
      } else if (kind === 'error' && message) {
        form.dataset.experienceState = 'error';
        status.hidden = false;
      } else if (!message && !readRegistrationHint()) {
        setState('idle');
      }
    };

    const statusObserver = new MutationObserver(reflectStatus);
    statusObserver.observe(status, {
      childList: true,
      characterData: true,
      subtree: true,
      attributes: true,
      attributeFilter: ['data-kind'],
    });
  }

  reset.addEventListener('click', () => {
    try {
      window.localStorage.removeItem(STORAGE_KEY);
    } catch {
      // The registration is still idempotent in Supabase; this is only a UX hint.
    }
    form.reset();
    if (status) {
      status.textContent = '';
      status.dataset.kind = 'idle';
      status.hidden = false;
    }
    setState('idle');
    email?.focus({ preventScroll: true });
  });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initializePrelaunchExperience, { once: true });
} else {
  initializePrelaunchExperience();
}
