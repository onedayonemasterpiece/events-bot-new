/**
 * Layout-agnostic identity/saved-events controller for every static page family.
 * It exposes state/events only; headers, cards and /izbrannoe/ own their rendering.
 */
const VERSION = 1;
const PREFIX = 'ke.site_identity.v1';
const KEYS = {
  device: `${PREFIX}.device`,
  email: `${PREFIX}.remembered_email`,
  emailTxn: `${PREFIX}.email_txn`,
  authIntent: `${PREFIX}.auth_intent`,
  activeAccount: `${PREFIX}.active_account`,
  change: `${PREFIX}.change`,
};
const CALLBACK_PARAMS = ['code', 'state', 'error', 'error_code', 'error_description', 'sb'];

function uuid(cryptoImpl = globalThis.crypto) {
  return cryptoImpl.randomUUID();
}
function base64url(bytes) {
  let raw = '';
  for (const byte of bytes) raw += String.fromCharCode(byte);
  return btoa(raw).replace(/\+/gu, '-').replace(/\//gu, '_').replace(/=+$/gu, '');
}
function newDevice(cryptoImpl = globalThis.crypto) {
  const bytes = new Uint8Array(32);
  cryptoImpl.getRandomValues(bytes);
  return { id: uuid(cryptoImpl), secret: base64url(bytes), created_at: new Date().toISOString() };
}
function read(storage, key) {
  try { return JSON.parse(storage.getItem(key) || 'null'); } catch { return null; }
}
function write(storage, key, value) {
  try { storage.setItem(key, JSON.stringify(value)); return true; } catch { return false; }
}
function remove(storage, key) { try { storage.removeItem(key); } catch { /* storage may be disabled */ } }
function normalizeEmail(value) { return String(value || '').trim().toLowerCase(); }
export function maskEmail(value) {
  const [local, domain] = normalizeEmail(value).split('@');
  return local && domain ? `${local.slice(0, 1)}***@${domain}` : '';
}
function validEmail(value) { return /^[^\s@]+@[^\s@]+\.[^\s@]+$/u.test(normalizeEmail(value)); }
function cleanUrl(input) {
  const url = new URL(input);
  for (const key of CALLBACK_PARAMS) url.searchParams.delete(key);
  url.hash = '';
  return url.toString();
}
function callbackCode(input) {
  const url = new URL(input);
  return { code: url.searchParams.get('code'), error: url.searchParams.get('error_description') || url.searchParams.get('error') };
}

export function createSiteIdentityController(options) {
  if (!options?.supabase) throw new Error('supabase client required');
  const supabase = options.supabase;
  const win = options.window || globalThis.window;
  const storage = options.storage || win?.localStorage;
  const clock = options.now || (() => Date.now());
  const cryptoImpl = options.crypto || globalThis.crypto;
  const apiUrl = String(options.identityControlUrl || '');
  const fetchImpl = options.fetch || globalThis.fetch;
  const provider = options.yandexProvider || 'custom:yandex';
  const emailTtlMs = options.emailTtlMs || 15 * 60_000;
  const emailCooldownMs = options.emailCooldownMs || 60_000;
  const maxEmailAttempts = options.maxEmailAttempts || 5;
  const mergeConsent = options.getConsentVersion || (() => options.consentVersion || null);
  const listeners = new Set();
  let authSubscription = null;
  let channel = null;
  let started = false;
  let state = {
    version: VERSION,
    status: 'anonymous_local',
    session: null,
    user: null,
    rememberedEmailMasked: maskEmail(read(storage, KEYS.email)?.email),
    savedCount: 0,
    error: null,
  };

  function snapshot() { return { ...state, session: state.session, user: state.user }; }
  function emit(patch = {}) {
    state = { ...state, ...patch };
    for (const listener of listeners) listener(snapshot());
  }
  function broadcast(kind) {
    const payload = { kind, at: clock(), nonce: uuid(cryptoImpl) };
    write(storage, KEYS.change, payload);
    channel?.postMessage(payload);
  }
  function device() {
    let current = read(storage, KEYS.device);
    if (!current?.id || !current?.secret) {
      current = newDevice(cryptoImpl);
      write(storage, KEYS.device, current);
    }
    return current;
  }
  function accountChanged(user) {
    const next = user?.id || null;
    const previous = read(storage, KEYS.activeAccount)?.id || null;
    if (previous && previous !== next) emit({ savedCount: 0 });
    write(storage, KEYS.activeAccount, { id: next, changed_at: new Date(clock()).toISOString() });
    return previous !== next;
  }
  async function refreshSavedCount() {
    if (!state.user) { emit({ savedCount: 0 }); return 0; }
    const { data, error } = await supabase.rpc('personalization_saved_count_v1');
    if (error) throw error;
    const count = Number(data || 0);
    emit({ savedCount: count });
    return count;
  }
  async function mergeAfterAuth(consentVersion, requestId = null) {
    if (!apiUrl || !state.session?.access_token || !consentVersion) return { skipped: true };
    const current = device();
    const requestKey = `${PREFIX}.merge_request.${state.user.id}`;
    if (!requestId) {
      requestId = read(storage, requestKey)?.id || uuid(cryptoImpl);
      write(storage, requestKey, { id: requestId, created_at: clock() });
    }
    const response = await fetchImpl(apiUrl, {
      method: 'POST',
      headers: { Authorization: `Bearer ${state.session.access_token}`, 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify({ action: 'merge_device', device_id: current.id, device_secret: current.secret, consent_version: consentVersion, request_id: requestId }),
    });
    const result = await response.json();
    if (!response.ok) throw Object.assign(new Error(result.error || 'merge_failed'), { code: result.error });
    emit({ status: 'linked' });
    await refreshSavedCount();
    broadcast('merge');
    return result;
  }
  function applySession(session, reason = 'restore') {
    const user = session?.user || null;
    const switched = accountChanged(user);
    emit({
      session: session || null,
      user,
      status: user ? (reason === 'callback' ? 'link_pending' : 'authenticated') : 'anonymous_local',
      savedCount: switched ? 0 : state.savedCount,
      error: null,
    });
    if (user) setTimeout(() => {
      refreshSavedCount().catch(() => emit({ error: 'saved_count_unavailable' }));
      const consent = mergeConsent();
      if (consent) mergeAfterAuth(consent).catch((error) => emit({ status: 'authenticated', error: error?.code || 'merge_failed' }));
    }, 0);
  }

  async function init() {
    if (started) return snapshot();
    started = true;
    const cb = callbackCode(win.location.href);
    if (cb.code || cb.error) {
      emit({ status: 'email_verification_pending' });
      win.history.replaceState({}, '', cleanUrl(win.location.href));
      if (cb.error) emit({ status: 'anonymous_local', error: 'auth_callback_failed' });
      else {
        const txn = read(storage, KEYS.emailTxn);
        if (txn?.consumed_at) emit({ status: 'anonymous_local', error: 'auth_callback_replayed' });
        else {
          const { data, error } = await supabase.auth.exchangeCodeForSession(cb.code);
          if (error || !data?.session) emit({ status: 'anonymous_local', error: 'auth_callback_failed' });
          else {
            if (data.session.access_token && data.session.refresh_token) await supabase.auth.setSession({ access_token: data.session.access_token, refresh_token: data.session.refresh_token });
            write(storage, KEYS.emailTxn, { ...(txn || {}), consumed_at: clock() });
            applySession(data.session, 'callback');
          }
        }
      }
    } else {
      const { data } = await supabase.auth.getSession();
      applySession(data?.session || null, 'restore');
    }
    const authResult = supabase.auth.onAuthStateChange((_event, session) => {
      // Never await Supabase inside this callback.
      applySession(session, 'auth_event');
      broadcast('auth');
    });
    authSubscription = authResult?.data?.subscription || authResult?.subscription || null;
    win.addEventListener('storage', (event) => {
      if (event.key === KEYS.change || event.key === KEYS.email || event.key === KEYS.activeAccount) {
        emit({ rememberedEmailMasked: maskEmail(read(storage, KEYS.email)?.email) });
        setTimeout(() => supabase.auth.getSession().then(({ data }) => applySession(data?.session || null, 'cross_tab')), 0);
      }
    });
    if (typeof win.BroadcastChannel === 'function') {
      channel = new win.BroadcastChannel('ke.site_identity.v1');
      channel.onmessage = () => setTimeout(() => supabase.auth.getSession().then(({ data }) => applySession(data?.session || null, 'cross_tab')), 0);
    }
    return snapshot();
  }

  async function loginYandex() {
    const redirectTo = cleanUrl(win.location.href);
    write(storage, KEYS.authIntent, { provider, redirect_to: redirectTo, started_at: clock() });
    emit({ status: 'link_pending', error: null });
    return supabase.auth.signInWithOAuth({ provider, options: { redirectTo } });
  }
  async function logout() {
    await supabase.auth.signOut();
    accountChanged(null);
    emit({ session: null, user: null, savedCount: 0, status: 'anonymous_local' });
    broadcast('logout');
  }
  async function requestEmailVerification(email) {
    email = normalizeEmail(email);
    if (!validEmail(email)) throw Object.assign(new Error('email_invalid'), { code: 'email_invalid' });
    const prior = read(storage, KEYS.emailTxn);
    if (prior && clock() - prior.requested_at < emailCooldownMs) throw Object.assign(new Error('email_rate_limited'), { code: 'email_rate_limited' });
    const transaction = { id: uuid(cryptoImpl), email, requested_at: clock(), expires_at: clock() + emailTtlMs, attempts: 0, consumed_at: null };
    write(storage, KEYS.emailTxn, transaction);
    // Supabase sends one OTP transaction. Its template must contain both {{ .Token }}
    // and {{ .ConfirmationURL }} so code and link consume the same one-time challenge.
    const { error } = await supabase.auth.signInWithOtp({ email, options: { emailRedirectTo: cleanUrl(win.location.href), shouldCreateUser: true } });
    if (error) { remove(storage, KEYS.emailTxn); throw error; }
    if (!read(storage, KEYS.email)?.email) write(storage, KEYS.email, { email, remembered_at: clock() });
    emit({ status: 'email_verification_pending', rememberedEmailMasked: maskEmail(email), error: null });
    broadcast('email_requested');
    return { transactionId: transaction.id, expiresAt: transaction.expires_at, maskedEmail: maskEmail(email) };
  }
  async function verifyEmailCode(code) {
    const transaction = read(storage, KEYS.emailTxn);
    if (!transaction || transaction.consumed_at) throw Object.assign(new Error('email_code_replayed'), { code: 'email_code_replayed' });
    if (clock() >= transaction.expires_at) throw Object.assign(new Error('email_code_expired'), { code: 'email_code_expired' });
    if (transaction.attempts >= maxEmailAttempts) throw Object.assign(new Error('email_attempts_exhausted'), { code: 'email_attempts_exhausted' });
    transaction.attempts += 1;
    write(storage, KEYS.emailTxn, transaction);
    const token = String(code || '').replace(/\s/gu, '');
    if (!/^\d{6,8}$/u.test(token)) throw Object.assign(new Error('email_code_invalid'), { code: 'email_code_invalid' });
    const { data, error } = await supabase.auth.verifyOtp({ email: transaction.email, token, type: 'email' });
    if (error || !data?.session) throw error || new Error('email_code_invalid');
    transaction.consumed_at = clock();
    write(storage, KEYS.emailTxn, transaction);
    applySession(data.session, 'callback');
    broadcast('email_verified');
    return snapshot();
  }
  function forgetEmailOnDevice() {
    remove(storage, KEYS.email);
    remove(storage, KEYS.emailTxn);
    emit({ rememberedEmailMasked: '', error: null });
    broadcast('email_forgotten');
  }
  async function materializeAnonymous(savedOccurrences, consentVersion) {
    if (!apiUrl) throw new Error('identity control URL required');
    const current = device();
    const response = await fetchImpl(apiUrl, {
      method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify({ action: 'materialize_device', device_id: current.id, device_secret: current.secret, consent_version: consentVersion, saved_occurrences: savedOccurrences }),
    });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || 'materialize_failed');
    emit({ status: 'anonymous_materialized' });
    return result;
  }
  async function unlinkDevice() {
    if (!state.session?.access_token || !apiUrl) throw new Error('authenticated identity required');
    const current = device();
    const response = await fetchImpl(apiUrl, { method: 'POST', headers: { Authorization: `Bearer ${state.session.access_token}`, 'Content-Type': 'application/json' }, body: JSON.stringify({ action: 'unlink_device', device_id: current.id, device_secret: current.secret }) });
    if (!response.ok) throw new Error('unlink_failed');
    remove(storage, KEYS.device);
    device();
    broadcast('unlink');
    return response.json();
  }
  async function saveOccurrence({ eventId, occurrenceKey, occurrenceStartsAt = null, saved = true }) {
    if (!state.user) throw Object.assign(new Error('auth_required'), { code: 'auth_required' });
    const { data, error } = await supabase.rpc('personalization_save_occurrence_v1', { p_event_id: eventId, p_occurrence_key: occurrenceKey, p_occurrence_starts_at: occurrenceStartsAt, p_saved: saved });
    if (error) throw error;
    const row = Array.isArray(data) ? data[0] : data;
    emit({ savedCount: Number(row?.unique_saved_event_count || 0) });
    broadcast('saved');
    return row;
  }
  async function setLike({ eventId, occurrenceKey, active }) {
    const { data, error } = await supabase.rpc('personalization_set_event_signal_v1', { p_event_id: eventId, p_occurrence_key: occurrenceKey, p_signal: 'like', p_active: active });
    if (error) throw error;
    return Boolean(data);
  }
  async function setReminder({ eventId, occurrenceKey, enabled, termsVersion }) {
    const { data, error } = await supabase.rpc('personalization_set_reminder_v1', { p_event_id: eventId, p_occurrence_key: occurrenceKey, p_enabled: enabled, p_terms_version: termsVersion, p_request_id: uuid(cryptoImpl) });
    if (error) throw error;
    return Array.isArray(data) ? data[0] : data;
  }
  function subscribe(listener) { listeners.add(listener); listener(snapshot()); return () => listeners.delete(listener); }
  function destroy() { authSubscription?.unsubscribe?.(); channel?.close?.(); listeners.clear(); }

  return { init, subscribe, snapshot, loginYandex, logout, requestEmailVerification, verifyEmailCode, forgetEmailOnDevice, materializeAnonymous, mergeAfterAuth, unlinkDevice, refreshSavedCount, saveOccurrence, setLike, setReminder, getDevice: device, destroy };
}

export const siteIdentityStorageKeys = Object.freeze({ ...KEYS });
