import { isStandaloneDisplay } from './pwa-install-controller.js';

const INSTALLATION_STORAGE_KEY = 'kenigevents:pwa-installation-id:v1';
const SESSION_STORAGE_KEY = 'kenigevents:pwa-session-id:v1';
const SESSION_RECORDED_KEY = 'kenigevents:pwa-session-recorded:v1';

function safeUuid(storage, key, cryptoRef) {
  try {
    const existing = storage?.getItem(key);
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(existing || '')) {
      return existing;
    }
    const created = cryptoRef?.randomUUID?.();
    if (!created) return '';
    storage?.setItem(key, created);
    return created;
  } catch {
    // Prefer missing one aggregate datapoint to generating unbounded,
    // non-deduplicable rows when browser storage is unavailable.
    return '';
  }
}

export function createPwaTelemetryController({
  windowRef,
  navigatorRef,
  endpoint,
  publishableKey,
  fetchRef = windowRef?.fetch?.bind(windowRef),
  cryptoRef = windowRef?.crypto,
  localStorageRef = windowRef?.localStorage,
  sessionStorageRef = windowRef?.sessionStorage,
}) {
  const url = String(endpoint || '').replace(/\/+$/u, '');
  const key = String(publishableKey || '');
  if (!windowRef || !navigatorRef || !url || !key || !fetchRef || navigatorRef.webdriver === true) return null;

  const installationId = safeUuid(localStorageRef, INSTALLATION_STORAGE_KEY, cryptoRef);
  const sessionId = safeUuid(sessionStorageRef, SESSION_STORAGE_KEY, cryptoRef);
  if (!installationId || !sessionId) return null;

  const record = async (eventKind) => {
    try {
      const response = await fetchRef(`${url}/rest/v1/rpc/record_pwa_lifecycle_v1`, {
        method:'POST',
        headers:{
          apikey:key,
          Authorization:`Bearer ${key}`,
          'Content-Type':'application/json',
        },
        body:JSON.stringify({
          p_installation_id:installationId,
          p_session_id:sessionId,
          p_event_kind:eventKind,
        }),
        keepalive:true,
        credentials:'omit',
        referrerPolicy:'no-referrer',
      });
      return response.ok;
    } catch {
      return false;
    }
  };

  const onAppInstalled = () => {
    void record('install');
  };

  windowRef.addEventListener('appinstalled', onAppInstalled);

  if (isStandaloneDisplay(windowRef, navigatorRef)) {
    let alreadyRecorded = false;
    try {
      alreadyRecorded = sessionStorageRef?.getItem(SESSION_RECORDED_KEY) === sessionId;
      if (!alreadyRecorded) sessionStorageRef?.setItem(SESSION_RECORDED_KEY, sessionId);
    } catch {
      alreadyRecorded = true;
    }
    if (!alreadyRecorded) void record('standalone_open');
  }

  return {
    destroy() {
      windowRef.removeEventListener('appinstalled', onAppInstalled);
    },
    record,
    installationId,
    sessionId,
  };
}

export function hydratePwaTelemetry({
  documentRef = document,
  windowRef = window,
  navigatorRef = navigator,
} = {}) {
  const config = documentRef.querySelector('[data-pwa-telemetry-config]');
  if (!config || config.dataset.pwaTelemetryBound === 'true') return null;
  config.dataset.pwaTelemetryBound = 'true';
  return createPwaTelemetryController({
    windowRef,
    navigatorRef,
    endpoint:config.dataset.pwaTelemetryEndpoint,
    publishableKey:config.dataset.pwaTelemetryKey,
  });
}
