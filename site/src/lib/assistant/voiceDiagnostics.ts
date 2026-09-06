/** Deliberately metadata-only. Never pass exception messages, recording IDs,
 * database rows, Auth data, URLs, or arbitrary objects into this trace. */
type Fields = { oldVersion?: number; newVersion?: number | null; version?: number; hidden?: boolean; persisted?: boolean; error?: string; attempt?: number };
const events: Array<{ event: string; ms: number } & Fields> = [];
const listeners = new Set<() => void>();
export function voiceTrace(event: string, fields: Fields = {}): void {
  events.push({ event, ms: Math.round(performance.now()), ...fields });
  if (events.length > 80) events.shift();
  for (const listener of listeners) listener();
}
const safeError = (error: unknown): string => error instanceof DOMException &&
  ['AbortError','InvalidStateError','NotAllowedError','NotFoundError','QuotaExceededError','SecurityError','UnknownError','VersionError'].includes(error.name) ? error.name : 'unavailable';
export const voiceErrorName = safeError;
function browserFamily(): string {
  const ua = navigator.userAgent;
  for (const [name, pattern] of [['Edge',/EdgA?\/(\d+)/],['Chrome',/(?:Chrome|CriOS)\/(\d+)/],['Firefox',/(?:Firefox|FxiOS)\/(\d+)/],['Safari',/Version\/(\d+).*Safari/]] as const) {
    const match = ua.match(pattern); if (match) return `${name} ${match[1]}${/; wv\)/.test(ua) ? ' WebView' : ''}`;
  }
  return 'other';
}
let probePending: Promise<void> | null = null;
/** Explicit diagnostic only: never probes, edits, or deletes user object stores. */
export function probeVoiceStorage(timeoutMs = 3000): Promise<void> {
  if (probePending) return probePending;
  probePending = (async () => {
    voiceTrace('probe_requested');
    // A unique, disposable name avoids queues for user databases. No user data
    // is written, and cleanup always targets this exact generated name only.
    const name = `kenigevents-voice-diagnostic-${crypto.randomUUID()}`;
    const temporary = new Promise<void>(resolve => {
      let ended = false;
      const done = () => { if (!ended) { ended = true; clearTimeout(timer); resolve(); } };
      const timer = setTimeout(() => { voiceTrace('probe_timeout'); done(); }, timeoutMs);
      let request: IDBOpenDBRequest;
      try { request = indexedDB.open(name, 1); }
      catch (error) { voiceTrace('probe_error', { error: safeError(error) }); done(); return; }
      request.onupgradeneeded = () => voiceTrace('probe_upgrade');
      request.onblocked = () => voiceTrace('probe_blocked');
      request.onerror = () => { voiceTrace('probe_error', { error: safeError(request.error) }); done(); };
      request.onsuccess = () => {
        voiceTrace(ended ? 'probe_late_success' : 'probe_success', { version: request.result.version });
        request.result.close();
        const cleanup = indexedDB.deleteDatabase(name);
        cleanup.onsuccess = () => voiceTrace('probe_cleanup_complete');
        cleanup.onerror = () => voiceTrace('probe_cleanup_error', { error: safeError(cleanup.error) });
        cleanup.onblocked = () => voiceTrace('probe_cleanup_blocked');
        done();
      };
    });
    const metadata = new Promise<void>(resolve => {
      if (typeof indexedDB.databases !== 'function') { voiceTrace('metadata_unsupported'); resolve(); return; }
      let ended = false;
      const timer = setTimeout(() => { ended = true; voiceTrace('metadata_timeout'); resolve(); }, timeoutMs);
      void indexedDB.databases().then(rows => {
        if (ended) return;
        const row = rows.find(row => row.name === 'kenigevents-voice-v1');
        voiceTrace(row ? 'metadata_voice_present' : 'metadata_voice_absent', row ? { version: row.version } : {});
      }).catch(error => { if (!ended) voiceTrace('metadata_error', { error: safeError(error) }); })
        .finally(() => { clearTimeout(timer); ended = true; resolve(); });
    });
    await Promise.all([temporary, metadata]);
  })().finally(() => { probePending = null; });
  return probePending;
}
export function mountVoiceDiagnostics(root: HTMLElement, container: HTMLElement): void {
  if (container.querySelector('[data-voice-diagnostics]')) return;
  const panel = document.createElement('details'); panel.dataset.voiceDiagnostics = '';
  const summary = document.createElement('summary'); summary.textContent = 'Диагностика запуска'; panel.append(summary);
  const note = document.createElement('p'); note.textContent = 'Только этапы запуска и состояние хранилища. Без аудио, запросов, аккаунта и токенов. Отчёт отправляется только вами.'; panel.append(note);
  const output = document.createElement('textarea'); output.readOnly = true; output.rows = 12; output.setAttribute('aria-label','Отчёт диагностики запуска'); output.style.width = '100%'; output.style.boxSizing = 'border-box'; panel.append(output);
  const bundle = new URL(import.meta.url).pathname.split('/').pop() || 'unknown';
  const render = () => { output.value = JSON.stringify({ contract: 'kenigevents.voice-startup-diagnostic.v1', build: 'storage-diagnostic-20260906-v1', bundle,
    browser: browserFamily(), startup: root.dataset.assistantStartup || 'mounting', startupError: root.dataset.assistantStartupError || null, events }, null, 2); };
  listeners.add(render); new MutationObserver(render).observe(root, { attributes: true, attributeFilter: ['data-assistant-startup','data-assistant-startup-error'] });
  const action = (label: string, run: () => void) => { const button = document.createElement('button'); button.type = 'button'; button.className = 'secondary-button'; button.textContent = label; button.addEventListener('click',run); panel.append(button); return button; };
  const status = document.createElement('p'); status.setAttribute('role','status');
  action('Копировать отчёт', () => {
    render(); if (!navigator.clipboard?.writeText) { output.focus(); output.select(); status.textContent = 'Выделен отчёт: скопируйте его вручную.'; return; }
    void navigator.clipboard.writeText(output.value).then(() => { status.textContent = 'Отчёт скопирован.'; }).catch(() => { output.focus(); output.select(); status.textContent = 'Скопируйте выделенный отчёт вручную.'; });
  });
  action('Скачать отчёт', () => { render(); const url = URL.createObjectURL(new Blob([output.value],{type:'application/json'})); const link = document.createElement('a'); link.href = url; link.download = 'voice-startup-diagnostic.json'; link.click(); setTimeout(() => URL.revokeObjectURL(url),1000); });
  const probe = action('Проверить отдельное хранилище', () => { probe.disabled = true; status.textContent = 'Проверяю отдельную временную базу, не записи пользователя…'; void probeVoiceStorage().catch(() => voiceTrace('probe_unavailable')).finally(() => { probe.disabled = false; status.textContent = 'Проверка завершена. Скопируйте отчёт.'; }); });
  panel.append(status); container.append(panel);
  voiceTrace('diagnostic_mounted', { hidden: document.hidden });
  document.addEventListener('visibilitychange', () => voiceTrace('visibilitychange', { hidden: document.hidden }));
  document.addEventListener('freeze', () => voiceTrace('freeze'));
  document.addEventListener('resume', () => voiceTrace('resume'));
  window.addEventListener('pageshow', event => voiceTrace('pageshow', { persisted: event.persisted }));
  window.addEventListener('pagehide', event => voiceTrace('pagehide', { persisted: event.persisted }));
  render();
}
