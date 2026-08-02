const DB_NAME = 'kenigevents-pwa-capabilities-lab';
const DB_VERSION = 1;
const CARD_STORE = 'cards';
const PERIODIC_TAG = 'pwa-capabilities-lab-refresh';

export function resolvePwaLabRuntimeConfig(config, locationHref = globalThis.location?.href) {
  if (!config?.labUrl || !locationHref) throw new Error('Base-aware labUrl не задан.');
  const labUrl = new URL(config.labUrl, locationHref);
  if (!labUrl.pathname.endsWith('/')) labUrl.pathname = `${labUrl.pathname}/`;
  const workerUrl = new URL('./sw.js', labUrl);
  const scopeUrl = new URL('./', labUrl);
  const imageUrls = (config.imageUrls || []).map((value) => new URL(value, locationHref).href);
  if (!imageUrls.length) throw new Error('Base-aware imageUrls не заданы.');
  return {
    labUrl,
    workerUrl,
    scopeUrl,
    imageUrls,
    cacheName: config.cacheName || `kenigevents-pwa-capabilities-lab-${encodeURIComponent(scopeUrl.pathname)}`,
    vapidPublicKey: config.vapidPublicKey || '',
  };
}

export function createDemoCards(now = new Date(), imageUrls = []) {
  return Array.from({ length: 30 }, (_, index) => ({
    id: `pwa-lab-${String(index + 1).padStart(2, '0')}`,
    version: 1,
    title: `Демо-событие ${index + 1}`,
    startsAt: new Date(now.getTime() + (index + 1) * 3_600_000).toISOString(),
    place: index % 2 ? 'Остров Канта' : 'Музейный квартал',
    imageUrl: imageUrls.length ? imageUrls[index % imageUrls.length] : null,
    generatedAt: now.toISOString(),
    profileKey: 'pwa-lab-demo',
  }));
}

export function escapeIcs(value) {
  return String(value)
    .replace(/\\/gu, '\\\\')
    .replace(/\r?\n/gu, '\\n')
    .replace(/,/gu, '\\,')
    .replace(/;/gu, '\\;');
}

function foldIcsLine(line) {
  const encoder = new TextEncoder();
  const chunks = [];
  let chunk = '';
  let limit = 75;
  for (const character of line) {
    if (encoder.encode(chunk + character).byteLength > limit && chunk) {
      chunks.push(chunk);
      chunk = character;
      limit = 74;
    } else {
      chunk += character;
    }
  }
  chunks.push(chunk);
  return chunks.join('\r\n ');
}

function utcStamp(value) {
  return value.toISOString().replace(/[-:]/gu, '').replace(/\.\d{3}Z$/u, 'Z');
}

export function createIcs(event, uid = 'pwa-capabilities-lab@kenigevents.ru', now = new Date()) {
  const lines = [
    'BEGIN:VCALENDAR',
    'VERSION:2.0',
    'PRODID:-//KenigEvents//PWA Capabilities Lab//RU',
    'CALSCALE:GREGORIAN',
    'METHOD:PUBLISH',
    'BEGIN:VEVENT',
    `UID:${escapeIcs(uid)}`,
    `DTSTAMP:${utcStamp(now)}`,
    `DTSTART:${utcStamp(event.start)}`,
    `DTEND:${utcStamp(event.end)}`,
    `SUMMARY:${escapeIcs(event.title)}`,
    `DESCRIPTION:${escapeIcs(event.description)}`,
    `LOCATION:${escapeIcs(event.location)}`,
    `URL:${escapeIcs(event.url)}`,
    'END:VEVENT',
    'END:VCALENDAR',
  ];
  return `${lines.map(foldIcsLine).join('\r\n')}\r\n`;
}

export function createGoogleCalendarUrl(event) {
  const url = new URL('https://calendar.google.com/calendar/render');
  url.search = new URLSearchParams({
    action: 'TEMPLATE',
    text: event.title,
    dates: `${utcStamp(event.start)}/${utcStamp(event.end)}`,
    details: `${event.description}\n\n${event.url}`,
    location: event.location,
    ctz: event.timezone,
  }).toString();
  return url.toString();
}

export function canShareFiles(navigatorLike, files) {
  if (!navigatorLike?.share || !navigatorLike?.canShare) return false;
  try { return Boolean(navigatorLike.canShare({ files })); }
  catch { return false; }
}

function zonedLocalToDate(value, timeZone) {
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})$/u.exec(value);
  if (!match) throw new Error('Укажите дату и время.');
  const [, year, month, day, hour, minute] = match.map(Number);
  const intendedUtc = Date.UTC(year, month - 1, day, hour, minute);
  let guess = intendedUtc;
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23',
  });
  for (let attempt = 0; attempt < 2; attempt += 1) {
    const values = Object.fromEntries(formatter.formatToParts(new Date(guess)).map((part) => [part.type, part.value]));
    const representedUtc = Date.UTC(Number(values.year), Number(values.month) - 1, Number(values.day), Number(values.hour), Number(values.minute), Number(values.second));
    guess += intendedUtc - representedUtc;
  }
  return new Date(guess);
}

function formatForInput(date, timeZone) {
  const parts = Object.fromEntries(new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hourCycle: 'h23',
  }).formatToParts(date).map((part) => [part.type, part.value]));
  return `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}`;
}

function waitForActivation(registration) {
  const worker = registration.active || registration.waiting || registration.installing;
  if (!worker || worker.state === 'activated') return Promise.resolve(registration);
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('Service worker не активировался вовремя.')), 10_000);
    worker.addEventListener('statechange', () => {
      if (worker.state === 'activated') {
        clearTimeout(timeout);
        resolve(registration);
      }
    });
  });
}

function openDb() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(CARD_STORE)) db.createObjectStore(CARD_STORE, { keyPath: 'id' });
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error || new Error('IndexedDB недоступна.'));
  });
}

function transact(mode, callback) {
  return openDb().then((db) => new Promise((resolve, reject) => {
    const transaction = db.transaction(CARD_STORE, mode);
    const store = transaction.objectStore(CARD_STORE);
    let result;
    try { result = callback(store); } catch (error) { db.close(); reject(error); return; }
    transaction.oncomplete = () => { db.close(); resolve(result); };
    transaction.onerror = () => { db.close(); reject(transaction.error || new Error('Ошибка IndexedDB.')); };
  }));
}

function requestResult(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error || new Error('Ошибка IndexedDB.'));
  });
}

function base64UrlToUint8Array(value) {
  const padding = '='.repeat((4 - value.length % 4) % 4);
  const binary = atob((value + padding).replace(/-/gu, '+').replace(/_/gu, '/'));
  return Uint8Array.from(binary, (character) => character.charCodeAt(0));
}

export function initPwaCapabilitiesLab(config = {}) {
  const {
    labUrl,
    workerUrl,
    scopeUrl,
    imageUrls,
    cacheName,
    vapidPublicKey,
  } = resolvePwaLabRuntimeConfig(config, window.location.href);
  const byId = (id) => document.getElementById(id);
  const logElement = byId('result-log');
  const cardsElement = byId('offline-cards');
  const offlineStatus = byId('offline-status');
  const notificationStatus = byId('notification-status');
  const pushOutput = byId('push-subscription-output');
  const android = /Android/iu.test(navigator.userAgent);
  const nativeBridge = window.KenigEventsNative?.calendar?.addEvent;

  function log(method, status, message) {
    const time = new Date().toLocaleTimeString('ru-RU');
    const item = document.createElement('li');
    item.dataset.status = status;
    item.textContent = `${time} · ${method} · ${status}: ${message}`;
    logElement.prepend(item);
  }

  function setStatus(element, message, status = 'info') {
    element.textContent = message;
    element.dataset.status = status;
  }

  function guard(action, handler) {
    const button = byId(action);
    button.addEventListener('click', async () => {
      button.disabled = true;
      try { await handler(button); }
      catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        log(action, 'error', message);
      } finally { button.disabled = false; }
    });
  }

  function eventFromForm() {
    const form = byId('lab-event-form');
    const data = new FormData(form);
    const timezone = String(data.get('timezone') || 'Europe/Kaliningrad');
    const url = new URL(String(data.get('url')), window.location.href).toString();
    const start = zonedLocalToDate(String(data.get('start')), timezone);
    const end = zonedLocalToDate(String(data.get('end')), timezone);
    if (end <= start) throw new Error('Окончание должно быть позже начала.');
    return {
      title: String(data.get('title')).trim(),
      start, end, timezone,
      location: String(data.get('location')).trim(),
      description: String(data.get('description')).trim(),
      url,
    };
  }

  function stableUid() {
    const key = 'pwa-capabilities-lab-uid';
    let uid = localStorage.getItem(key);
    if (!uid) {
      uid = `${crypto.randomUUID ? crypto.randomUUID() : `${Date.now()}-${Math.random().toString(16).slice(2)}`}@kenigevents.ru`;
      localStorage.setItem(key, uid);
    }
    return uid;
  }

  function icsFile() {
    const event = eventFromForm();
    return { event, file: new File([createIcs(event, stableUid())], 'kenigevents-pwa-lab.ics', { type: 'text/calendar' }) };
  }

  function downloadFile(file) {
    const href = URL.createObjectURL(file);
    const link = Object.assign(document.createElement('a'), { href, download: file.name });
    link.click();
    setTimeout(() => URL.revokeObjectURL(href), 1_000);
  }

  async function registerLabWorker() {
    if (!window.isSecureContext) throw new Error('Нужен HTTPS или localhost.');
    if (!('serviceWorker' in navigator)) throw new Error('Service Worker не поддерживается.');
    if (workerUrl.origin !== window.location.origin) throw new Error('Lab worker должен быть same-origin.');
    const registration = await navigator.serviceWorker.register(workerUrl.href);
    await waitForActivation(registration);
    updateDiagnostics();
    return registration;
  }

  async function copyPlain(text) {
    if (navigator.clipboard?.writeText) return navigator.clipboard.writeText(text);
    const area = document.createElement('textarea');
    area.value = text;
    area.style.position = 'fixed';
    area.style.opacity = '0';
    document.body.append(area);
    area.select();
    const copied = document.execCommand('copy');
    area.remove();
    if (!copied) throw new Error('Clipboard недоступен; выделите текст вручную.');
  }

  function postText(event) {
    return `${event.title}\n${event.description}\n${event.location}\n${event.url}`;
  }

  function renderCards(cards) {
    cardsElement.replaceChildren(...cards.map((card) => {
      const article = document.createElement('article');
      article.className = 'demo-card';
      article.innerHTML = `<img alt="" src="${card.imageUrl}"><div><strong></strong><small></small></div>`;
      article.querySelector('strong').textContent = card.title;
      article.querySelector('small').textContent = `${card.place} · ${new Date(card.startsAt).toLocaleString('ru-RU')}`;
      return article;
    }));
    cardsElement.hidden = cards.length === 0;
  }

  async function readCards(announce = true) {
    if (!('indexedDB' in window)) throw new Error('IndexedDB не поддерживается.');
    const db = await openDb();
    const transaction = db.transaction(CARD_STORE, 'readonly');
    const request = transaction.objectStore(CARD_STORE).getAll();
    const cards = await requestResult(request);
    db.close();
    cards.sort((a, b) => a.id.localeCompare(b.id));
    renderCards(cards);
    const size = new Blob([JSON.stringify(cards)]).size;
    const updated = cards[0]?.generatedAt ? new Date(cards[0].generatedAt).toLocaleString('ru-RU') : '—';
    setStatus(offlineStatus, `Карточек: ${cards.length}; примерно ${size} байт; обновлено: ${updated}.`, 'success');
    if (announce) log('IndexedDB read', 'success', `прочитано ${cards.length} карточек`);
    return cards;
  }

  async function updateDiagnostics() {
    let registration = null;
    if ('serviceWorker' in navigator) {
      const candidate = await navigator.serviceWorker.getRegistration(labUrl.href).catch(() => null);
      const isLabWorker = candidate
        && new URL(candidate.scope).href === scopeUrl.href
        && new URL(candidate.active?.scriptURL || '', window.location.href).href === workerUrl.href;
      registration = isLabWorker ? candidate : null;
    }
    let fileShare = false;
    try {
      const probe = new File(['probe'], 'probe.txt', { type: 'text/plain' });
      fileShare = canShareFiles(navigator, [probe]);
    } catch { fileShare = false; }
    let icsFileShare = false;
    try {
      const probe = new File(['BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n'], 'probe.ics', { type: 'text/calendar' });
      icsFileShare = canShareFiles(navigator, [probe]);
    } catch { icsFileShare = false; }
    const values = {
      secure: window.isSecureContext,
      android,
      standalone: window.matchMedia('(display-mode: standalone)').matches,
      share: Boolean(navigator.share),
      canShare: Boolean(navigator.canShare),
      fileShare,
      icsFileShare,
      serviceWorker: 'serviceWorker' in navigator,
      workerScope: registration?.scope || 'не зарегистрирован',
      workerControlling: Boolean(
        navigator.serviceWorker?.controller
        && new URL(navigator.serviceWorker.controller.scriptURL).href === workerUrl.href
      ),
      notifications: 'Notification' in window ? Notification.permission : 'нет API',
      push: 'PushManager' in window,
      indexedDb: 'indexedDB' in window,
      cacheStorage: 'caches' in window,
      periodicSync: Boolean(registration && 'periodicSync' in registration),
      clipboard: Boolean(navigator.clipboard),
      clipboardItem: 'ClipboardItem' in window,
      nativeBridge: Boolean(nativeBridge),
    };
    for (const [key, value] of Object.entries(values)) {
      const cell = document.querySelector(`[data-diagnostic="${key}"]`);
      if (cell) cell.textContent = typeof value === 'boolean' ? (value ? 'да' : 'нет') : String(value);
    }
    byId('android-intent').disabled = !android;
    byId('android-intent').title = android ? 'Экспериментальный browser intent' : 'Доступно только на Android';
    byId('native-insert').disabled = !nativeBridge;
    byId('native-insert').textContent = nativeBridge ? 'Native ACTION_INSERT' : 'Native bridge не обнаружен';
    byId('push-subscribe').hidden = !vapidPublicKey;
    setStatus(notificationStatus, `Notification.permission: ${'Notification' in window ? Notification.permission : 'API отсутствует'}.`, 'info');
  }

  const timezone = 'Europe/Kaliningrad';
  const now = new Date();
  byId('event-start').value = formatForInput(new Date(now.getTime() + 2 * 3_600_000), timezone);
  byId('event-end').value = formatForInput(new Date(now.getTime() + 3.5 * 3_600_000), timezone);
  byId('event-url').value = labUrl.href;

  guard('google-calendar', async () => {
    const url = createGoogleCalendarUrl(eventFromForm());
    const opened = window.open(url, '_blank');
    if (!opened) throw new Error('Popup заблокирован; разрешите открытие новой вкладки.');
    opened.opener = null;
    setStatus(byId('calendar-status'), 'Google Calendar: заполненная web-форма открыта; сохранение подтверждает пользователь.', 'success');
    log('Google Calendar', 'success', 'открыта заполненная форма; сохранение подтверждает пользователь');
  });

  guard('download-ics', async () => {
    const { file } = icsFile();
    downloadFile(file);
    setStatus(byId('calendar-status'), 'ICS скачан. Откройте файл из «Загрузок» и выберите календарь вручную.', 'success');
    log('ICS download', 'success', 'файл передан браузеру для скачивания');
  });

  guard('share-ics', async () => {
    const { event, file } = icsFile();
    if (!canShareFiles(navigator, [file])) {
      downloadFile(file);
      setStatus(byId('calendar-status'), 'ICS Web Share недоступен: браузер не разрешает .ics/text/calendar. Файл скачан; откройте его из «Загрузок».', 'fallback');
      log('ICS Web Share', 'fallback', '.ics/text/calendar не разрешён этим браузером — файл скачан вместо Share');
      return;
    }
    try {
      await navigator.share({ title: event.title, text: 'Событие для календаря', files: [file] });
      setStatus(byId('calendar-status'), 'Share sheet завершён, но это не доказывает импорт в календарь: результат подтверждается в target-приложении.', 'success');
      log('ICS Web Share', 'success', 'системное меню Share завершило операцию; импорт календарём не подтверждён');
    } catch (error) {
      if (error?.name === 'AbortError') {
        setStatus(byId('calendar-status'), 'ICS Share отменён пользователем.', 'info');
        log('ICS Web Share', 'cancel', 'пользователь закрыл меню');
      }
      else throw error;
    }
  });

  guard('android-intent', async () => {
    const event = eventFromForm();
    const fallback = createGoogleCalendarUrl(event);
    const intent = `intent://com.android.calendar/events#Intent;scheme=content;action=android.intent.action.INSERT;type=vnd.android.cursor.dir/event;S.title=${encodeURIComponent(event.title)};S.eventLocation=${encodeURIComponent(event.location)};l.beginTime=${event.start.getTime()};l.endTime=${event.end.getTime()};S.browser_fallback_url=${encodeURIComponent(fallback)};end`;
    setStatus(byId('calendar-status'), 'Android intent передан Chrome. Системный календарь откроется только при наличии BROWSABLE Activity; иначе используйте Google Calendar или скачанный ICS.', 'fallback');
    log('Android intent', 'experiment', 'передан браузеру; факт открытия календаря нужно отметить вручную');
    window.location.href = intent;
  });

  guard('native-insert', async () => {
    if (!nativeBridge) throw new Error('Native bridge не обнаружен.');
    const event = eventFromForm();
    await nativeBridge({ ...event, start: event.start.toISOString(), end: event.end.toISOString() });
    log('Native ACTION_INSERT', 'success', 'payload принят обнаруженным bridge');
  });

  guard('local-notification', async () => {
    if (!('Notification' in window)) throw new Error('Notifications API не поддерживается.');
    const permission = Notification.permission === 'default' ? await Notification.requestPermission() : Notification.permission;
    await updateDiagnostics();
    if (permission === 'denied') {
      setStatus(notificationStatus, 'Разрешение запрещено. Повторный prompt не появится: измените настройку сайта/Android.', 'fallback');
      log('Local notification', 'denied', 'это пользовательское состояние, не техническая ошибка');
      return;
    }
    if (permission !== 'granted') {
      log('Local notification', 'cancel', 'разрешение не выдано');
      return;
    }
    const registration = await registerLabWorker();
    await registration.showNotification('PWA Lab: локальное уведомление', {
      body: 'Показано service worker без remote push.', tag: 'pwa-lab-local',
      icon: imageUrls[0], data: { url: labUrl.href, kind: 'local' },
    });
    setStatus(notificationStatus, 'Локальное уведомление передано service worker.', 'success');
    log('Local notification', 'success', 'showNotification выполнен');
  });

  guard('simulate-push', async () => {
    if (!('Notification' in window) || Notification.permission !== 'granted') throw new Error('Сначала выдайте разрешение на уведомления.');
    const registration = await registerLabWorker();
    const worker = registration.active || registration.waiting;
    if (!worker) throw new Error('Активный lab worker не найден.');
    worker.postMessage({ type: 'SIMULATE_PUSH', payload: { title: 'PWA Lab: simulation', body: 'Это simulated payload, не настоящий remote push.' } });
    log('Simulated push payload', 'simulation', 'payload отправлен через postMessage, remote push не выполнялся');
  });

  guard('push-subscribe', async () => {
    if (!vapidPublicKey) throw new Error('PUBLIC_PWA_LAB_VAPID_PUBLIC_KEY не задан.');
    if (!('PushManager' in window)) throw new Error('PushManager не поддерживается.');
    if (!('Notification' in window) || Notification.permission !== 'granted') throw new Error('Сначала выдайте notification permission.');
    const registration = await registerLabWorker();
    const subscription = await registration.pushManager.getSubscription()
      || await registration.pushManager.subscribe({ userVisibleOnly: true, applicationServerKey: base64UrlToUint8Array(vapidPublicKey) });
    const json = subscription.toJSON();
    const endpoint = new URL(json.endpoint);
    const sanitized = {
      endpointOrigin: endpoint.origin,
      endpointPath: '/[redacted]',
      expirationTime: json.expirationTime,
      keys: { p256dh: json.keys?.p256dh ? '[present]' : null, auth: json.keys?.auth ? '[present]' : null },
      note: 'Backend sender/VAPID private key не входят в лабораторию.',
    };
    pushOutput.value = JSON.stringify(sanitized, null, 2);
    await copyPlain(pushOutput.value);
    log('PushManager.subscribe', 'success', 'создана подписка; в UI и clipboard помещена sanitized версия');
  });

  guard('clear-worker', async () => {
    if ('serviceWorker' in navigator) {
      const registrations = await navigator.serviceWorker.getRegistrations();
      const labRegistrations = registrations.filter((item) => (
        new URL(item.scope).href === scopeUrl.href
        || new URL(item.active?.scriptURL || '', window.location.href).href === workerUrl.href
      ));
      await Promise.all(labRegistrations.map((item) => item.unregister()));
      log('Lab worker', 'success', `удалено регистраций: ${labRegistrations.length}; root worker не затронут`);
    }
    await updateDiagnostics();
  });

  guard('save-cards', async () => {
    const cards = createDemoCards(new Date(), imageUrls);
    await transact('readwrite', (store) => cards.forEach((card) => store.put(card)));
    await readCards(false);
    log('IndexedDB save', 'success', 'сохранено ровно 30 demo-card DTO');
  });

  guard('read-cards', () => readCards(true));

  guard('warm-images', async () => {
    if (!('caches' in window)) throw new Error('Cache Storage не поддерживается.');
    const cache = await caches.open(cacheName);
    const results = await Promise.allSettled(imageUrls.map((url) => cache.add(new Request(url, { cache: 'reload' }))));
    const count = results.filter((result) => result.status === 'fulfilled').length;
    if (!count) throw new Error('Не удалось закэшировать ни одного изображения.');
    log('Cache Storage', count === imageUrls.length ? 'success' : 'fallback', `прогрето ${count}/${imageUrls.length} same-origin изображений`);
  });

  guard('periodic-sync', async () => {
    const registration = await registerLabWorker();
    if (!('periodicSync' in registration)) throw new Error('Periodic Background Sync не поддерживается; обновляйте при открытии/online.');
    await registration.periodicSync.register(PERIODIC_TAG, { minInterval: 24 * 60 * 60 * 1_000 });
    log('Periodic sync', 'best-effort', 'зарегистрирован без гарантии времени запуска');
  });

  guard('clear-offline', async () => {
    if ('indexedDB' in window) await new Promise((resolve, reject) => {
      const request = indexedDB.deleteDatabase(DB_NAME);
      request.onsuccess = resolve; request.onerror = () => reject(request.error); request.onblocked = resolve;
    });
    if ('caches' in window) await caches.delete(cacheName);
    renderCards([]);
    setStatus(offlineStatus, 'Lab IndexedDB и cache очищены.', 'success');
    log('Offline clear', 'success', 'lab data удалены; production cache не затронут');
  });

  guard('share-plain', async () => {
    const event = eventFromForm();
    if (!navigator.share) {
      await copyPlain(postText(event));
      log('Plain Web Share', 'fallback', 'Web Share нет — plain post скопирован');
      return;
    }
    try { await navigator.share({ title: event.title, text: `${event.description}\n${event.location}`, url: event.url }); log('Plain Web Share', 'success', 'share sheet завершил операцию'); }
    catch (error) { if (error?.name === 'AbortError') log('Plain Web Share', 'cancel', 'пользователь закрыл меню'); else throw error; }
  });

  guard('share-image', async () => {
    const event = eventFromForm();
    const response = await fetch(imageUrls[1] || imageUrls[0]);
    if (!response.ok) throw new Error(`Не удалось получить demo image (${response.status}).`);
    const file = new File([await response.blob()], 'kenigevents-demo.png', { type: 'image/png' });
    if (!navigator.share || !navigator.canShare?.({ files: [file] })) {
      await copyPlain(postText(event));
      log('File Web Share', 'fallback', 'file share нет — plain post скопирован');
      return;
    }
    try { await navigator.share({ title: event.title, text: event.description, url: event.url, files: [file] }); log('File Web Share', 'success', 'изображение и plain fields переданы target'); }
    catch (error) { if (error?.name === 'AbortError') log('File Web Share', 'cancel', 'пользователь закрыл меню'); else throw error; }
  });

  guard('copy-rich', async () => {
    const event = eventFromForm();
    const plain = postText(event);
    const html = `<p><strong>${event.title.replace(/[<>&"]/gu, '')}</strong></p><p>${event.description.replace(/[<>&"]/gu, '')}</p><p>${event.location.replace(/[<>&"]/gu, '')}</p><p><a href="${event.url}">${event.url}</a></p>`;
    if (!navigator.clipboard?.write || !('ClipboardItem' in window)) {
      await copyPlain(plain);
      log('Rich Clipboard', 'fallback', 'rich clipboard нет — скопирован plain text');
      return;
    }
    await navigator.clipboard.write([new ClipboardItem({ 'text/html': new Blob([html], { type: 'text/html' }), 'text/plain': new Blob([plain], { type: 'text/plain' }) })]);
    log('Rich Clipboard', 'success', 'скопированы text/html и text/plain; формат выбирает target');
  });

  guard('copy-plain', async () => { await copyPlain(postText(eventFromForm())); log('Plain Clipboard', 'success', 'plain fallback скопирован'); });

  guard('share-html-file', async () => {
    const event = eventFromForm();
    const file = new File([`<h1>${event.title}</h1><p>${event.description}</p><p><a href="${event.url}">${event.url}</a></p>`], 'kenigevents-post.html', { type: 'text/html' });
    if (!navigator.share || !navigator.canShare?.({ files: [file] })) throw new Error('HTML file share не поддерживается; используйте Clipboard.');
    await navigator.share({ title: event.title, text: 'HTML attachment (не body сообщения)', files: [file] });
    log('HTML file share', 'success', 'HTML передан как attachment, не как форматированный body');
  });

  window.addEventListener('error', (event) => log('window', 'error', event.message || 'Необработанная ошибка'));
  window.addEventListener('unhandledrejection', (event) => log('promise', 'error', event.reason?.message || String(event.reason)));
  window.addEventListener('online', () => log('network', 'info', 'соединение восстановлено; можно обновить данные при открытии'));

  updateDiagnostics().catch((error) => log('diagnostics', 'error', error.message));
  Promise.resolve(window.__kenigEventsPwaLabWorkerRegistration)
    .then((registration) => registration && waitForActivation(registration))
    .then(() => updateDiagnostics())
    .catch((error) => log('Lab worker', 'error', error.message));
  readCards(false).catch(() => setStatus(offlineStatus, 'Сохранённых demo-карточек пока нет.', 'info'));
  log('Lab', 'ready', window.isSecureContext ? 'страница готова' : 'небезопасный origin: SW, notifications и clipboard могут быть недоступны');
}
