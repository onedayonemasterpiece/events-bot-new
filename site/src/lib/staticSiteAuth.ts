import { createClient, type SupabaseClient, type User } from '@supabase/supabase-js';
import {
  parseSupabaseTransportError,
  supabaseAuthStorageKey,
  type ResilientSupabaseTransport,
  type SupabaseTransportOutcome,
} from './resilientSupabaseTransport';
import { getResilientDataClient, type ResilientDataClient } from './resilientDataClient';
import { AUTH_INTENT_KEY, purgeStaticAuthStorage } from './staticAuthReset.ts';

export interface StaticSiteAuthConfig {
  supabaseUrl: string;
  relayUrl?: string;
  publishableKey: string;
  provider?: string;
}

export interface StaticSiteAuthSnapshot {
  status: 'checking' | 'signed_in' | 'signed_out' | 'error';
  user: User | null;
  message: string;
  callbackAttempted: boolean;
}

export type StaticSiteEmailOtpStatus = 'accepted' | 'rate_limited' | 'ambiguous' | 'request_failed';

export interface StaticSiteEmailOtpResult {
  status: StaticSiteEmailOtpStatus;
  accepted: boolean;
  message: string;
  attemptId: string;
  transport: StaticSiteAuthTransportReceipt | null;
}

export interface StaticSiteEmailVerifyResult {
  ok: boolean;
  status: 'verified' | 'invalid' | 'ambiguous' | 'request_failed';
  message: string;
  transport: StaticSiteAuthTransportReceipt | null;
}

export interface StaticSiteAuthTransportReceipt {
  operationId: string;
  route: 'direct' | 'relay' | null;
  outcome: 'definitive' | 'recovered' | 'ambiguous' | 'no_route' | 'transport_failure';
  status: number | null;
}

type AuthSubscriber = (snapshot: StaticSiteAuthSnapshot) => void;

const CONTROLLER_KEY = '__KENIGEVENTS_STATIC_SITE_AUTH_V1__';
const PKCE_COOKIE_PREFIX = 'ke_pkce_';
const CALLBACK_TIMEOUT_MS = 20_000;
const SESSION_TIMEOUT_MS = 8_000;
const LOCAL_RESET_TIMEOUT_MS = 1_500;
const CALLBACK_KEYS = ['code', 'error', 'error_code', 'error_description', 'state', 'sb', 'email_callback', 'focus_auth_attempt', 'token_hash', 'type'];

function authAttemptId(): string {
  try {
    if (typeof crypto.randomUUID === 'function') return crypto.randomUUID();
  } catch {
    // Fall through to a UUID-shaped, non-secret correlation identifier.
  }
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = [...bytes].map((item) => item.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function authTransportReceipt(outcome: SupabaseTransportOutcome | null): StaticSiteAuthTransportReceipt | null {
  if (!outcome) return null;
  return {
    operationId: outcome.operationId,
    route: outcome.finalRoute,
    outcome: outcome.kind,
    status: outcome.status,
  };
}

function failedVerificationState(receipt: StaticSiteAuthTransportReceipt | null): boolean | null {
  if (!receipt || ['ambiguous', 'no_route', 'transport_failure'].includes(receipt.outcome)) return null;
  return false;
}

function isPkceCodeVerifierKey(key: string): boolean {
  return key.endsWith('-code-verifier');
}

function pkceCookieName(key: string): string {
  return `${PKCE_COOKIE_PREFIX}${encodeURIComponent(key).replace(/%/g, '_')}`;
}

function setPkceCookie(key: string, value: string): void {
  if (!isPkceCodeVerifierKey(key)) return;
  try {
    document.cookie = `${pkceCookieName(key)}=${encodeURIComponent(value)}; Max-Age=900; Path=/; SameSite=Lax; Secure`;
  } catch {
    // localStorage remains the primary Supabase storage.
  }
}

function getPkceCookie(key: string): string | null {
  if (!isPkceCodeVerifierKey(key)) return null;
  const name = `${pkceCookieName(key)}=`;
  const item = document.cookie.split('; ').find((part) => part.startsWith(name));
  if (!item) return null;
  try {
    return decodeURIComponent(item.slice(name.length));
  } catch {
    return null;
  }
}

function removePkceCookie(key: string): void {
  if (!isPkceCodeVerifierKey(key)) return;
  try {
    document.cookie = `${pkceCookieName(key)}=; Max-Age=0; Path=/; SameSite=Lax; Secure`;
  } catch {
    // Best-effort verifier cleanup.
  }
}

const authStorage = {
  getItem(key: string): string | null {
    try {
      const value = window.localStorage.getItem(key);
      if (value != null) return value;
    } catch {
      // Continue to the short-lived PKCE verifier cookie.
    }
    return getPkceCookie(key);
  },
  setItem(key: string, value: string): void {
    try {
      window.localStorage.setItem(key, value);
    } catch {
      // The current page may still retain an in-memory session.
    }
    setPkceCookie(key, value);
  },
  removeItem(key: string): void {
    try {
      window.localStorage.removeItem(key);
    } catch {
      // Ignore storage cleanup failures.
    }
    removePkceCookie(key);
  },
};

function purgePkceCookies(): void {
  try {
    for (const part of document.cookie.split(';')) {
      const name = part.split('=', 1)[0]?.trim() || '';
      if (!name.startsWith(PKCE_COOKIE_PREFIX)) continue;
      document.cookie = `${name}=; Max-Age=0; Path=/; SameSite=Lax; Secure`;
    }
  } catch {
    // localStorage cleanup is the authoritative session reset.
  }
}

function withTimeout<T>(promise: PromiseLike<T>, timeoutMs: number, message: string): Promise<T> {
  let timeoutId = 0;
  const timeout = new Promise<never>((_resolve, reject) => {
    timeoutId = window.setTimeout(() => reject(new Error(message)), timeoutMs);
  });
  return Promise.race([Promise.resolve(promise), timeout]).finally(() => window.clearTimeout(timeoutId));
}

function callbackParams(): URLSearchParams {
  const merged = new URLSearchParams(window.location.search);
  if (window.location.hash.length > 1) {
    const hashParams = new URLSearchParams(window.location.hash.slice(1));
    for (const [key, value] of hashParams.entries()) {
      if (!merged.has(key)) merged.set(key, value);
    }
  }
  return merged;
}

export function cleanStaticAuthUrl(value = window.location.href): string {
  const url = new URL(value);
  for (const key of CALLBACK_KEYS) url.searchParams.delete(key);
  url.hash = '';
  return url.toString();
}

function cleanCallbackHistory(): void {
  const cleanUrl = cleanStaticAuthUrl();
  if (cleanUrl !== window.location.href) {
    window.history.replaceState(window.history.state, '', cleanUrl);
  }
}

function writeAuthIntent(state: string, extra: Record<string, unknown> = {}): void {
  try {
    window.localStorage.setItem(AUTH_INTENT_KEY, JSON.stringify({
      state,
      updated_at: new Date().toISOString(),
      ...extra,
    }));
  } catch {
    // UX hint only; the Supabase session remains authoritative.
  }
}

function clearAuthIntent(): void {
  try {
    window.localStorage.removeItem(AUTH_INTENT_KEY);
  } catch {
    // Ignore cleanup failures.
  }
}

function readAuthIntent(): Record<string, unknown> {
  try {
    const parsed = JSON.parse(window.localStorage.getItem(AUTH_INTENT_KEY) || '{}');
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

export function staticAuthDisplayName(user: User | null | undefined): string {
  const metadata = user?.user_metadata || {};
  const humanName = [metadata.name, metadata.full_name]
    .map((value) => String(value || '').trim())
    .find((value) => value.length >= 2);
  const email = String(user?.email || '').trim();
  const username = String(metadata.preferred_username || '').trim();
  return humanName
    || email
    || (username.length >= 2 ? username : '')
    || (user?.id ? `ID ${String(user.id).slice(0, 8)}` : 'пользователь');
}

export function staticAuthUserInitial(value: string): string {
  const normalized = String(value || '').trim();
  if (!normalized) return '?';
  if (typeof Intl?.Segmenter === 'function') {
    const segmenter = new Intl.Segmenter('ru-RU', { granularity: 'grapheme' });
    const grapheme = Array.from(segmenter.segment(normalized), (part) => part.segment)
      .find((part) => /\S/u.test(part));
    if (grapheme) return grapheme.toLocaleUpperCase('ru-RU');
  }
  return Array.from(normalized)[0]?.toLocaleUpperCase('ru-RU') || '?';
}

class StaticSiteAuthController {
  readonly config: Required<StaticSiteAuthConfig>;
  readonly client: SupabaseClient;
  readonly transport: ResilientSupabaseTransport;
  readonly dataClient: ResilientDataClient;
  private subscribers = new Set<AuthSubscriber>();
  private initialization: Promise<StaticSiteAuthSnapshot> | null = null;
  private pendingEmailAttemptId = '';
  private snapshot: StaticSiteAuthSnapshot = {
    status: 'checking',
    user: null,
    message: 'Проверяю сохранённый вход через Яндекс…',
    callbackAttempted: false,
  };

  constructor(config: StaticSiteAuthConfig) {
    this.config = {
      supabaseUrl: config.supabaseUrl.replace(/\/+$/u, ''),
      relayUrl: String(config.relayUrl || '').replace(/\/+$/u, ''),
      publishableKey: config.publishableKey,
      provider: config.provider || 'custom:yandex',
    };
    this.dataClient = getResilientDataClient({
      directUrl: this.config.supabaseUrl,
      relayUrl: this.config.relayUrl,
      publishableKey: this.config.publishableKey,
    });
    this.transport = this.dataClient.transport;
    this.client = createClient(this.config.supabaseUrl, this.config.publishableKey, {
      global: {
        fetch: this.dataClient.fetch,
      },
      auth: {
        persistSession: true,
        autoRefreshToken: true,
        detectSessionInUrl: false,
        flowType: 'pkce',
        storage: authStorage,
        storageKey: supabaseAuthStorageKey(this.config.supabaseUrl),
      },
    });
    this.client.auth.onAuthStateChange((_event, session) => {
      window.setTimeout(() => {
        if (session?.user) {
          writeAuthIntent('signed_in');
          this.publish({
            status: 'signed_in',
            user: session.user,
            message: `Вошли как ${staticAuthDisplayName(session.user)}`,
            callbackAttempted: this.snapshot.callbackAttempted,
          });
        } else if (this.snapshot.status !== 'checking' && this.snapshot.status !== 'error') {
          this.publish({
            status: 'signed_out',
            user: null,
            message: 'Войдите через Яндекс.',
            callbackAttempted: this.snapshot.callbackAttempted,
          });
        }
      }, 0);
    });
  }

  private publish(snapshot: StaticSiteAuthSnapshot): StaticSiteAuthSnapshot {
    this.snapshot = snapshot;
    for (const subscriber of this.subscribers) subscriber(snapshot);
    return snapshot;
  }

  private async recordAuthTransport(
    rpc: 'focus_auth_record_client_outcome_v1' | 'focus_auth_record_verification_v1',
    attemptId: string,
    receipt: StaticSiteAuthTransportReceipt | null,
    verified: boolean | null = null,
    accessToken = '',
  ): Promise<void> {
    if (!attemptId || !receipt) return;
    try {
      await this.dataClient.request(
        `${this.config.supabaseUrl}/rest/v1/rpc/${rpc}`,
        {
          method: 'POST',
          headers: {
            apikey: this.config.publishableKey,
            Authorization: `Bearer ${accessToken || this.config.publishableKey}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            p_attempt_id: attemptId,
            p_route: receipt.route,
            p_outcome: receipt.outcome,
            p_http_status: receipt.status,
            ...(rpc === 'focus_auth_record_verification_v1' ? { p_verified: verified } : {}),
          }),
        },
      );
    } catch {
      // Delivery and verification remain authoritative; telemetry is bounded best effort.
    }
  }

  private async resolveEmailDeliveryReceipt(attemptId: string): Promise<'accepted' | 'pending_or_ambiguous' | 'rejected' | 'missing'> {
    if (!attemptId) return 'missing';
    try {
      const response = await this.dataClient.request(
        `${this.config.supabaseUrl}/rest/v1/rpc/focus_auth_get_delivery_receipt_v1`,
        {
          method: 'POST',
          headers: {
            apikey: this.config.publishableKey,
            Authorization: `Bearer ${this.config.publishableKey}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ p_attempt_id: attemptId }),
        },
      );
      if (!response.ok) return 'missing';
      const payload = await response.json();
      const state = Array.isArray(payload) ? payload[0]?.delivery_state : null;
      return ['accepted', 'pending_or_ambiguous', 'rejected'].includes(state) ? state : 'missing';
    } catch {
      return 'missing';
    }
  }

  private async recordAuthMethodAttempt(
    attemptId: string,
    outcome: 'started' | 'verified' | 'failed' | 'ambiguous',
    accessToken = '',
  ): Promise<void> {
    if (!attemptId) return;
    try {
      await this.dataClient.request(
        `${this.config.supabaseUrl}/rest/v1/rpc/focus_auth_record_method_attempt_v1`,
        {
          method: 'POST',
          headers: {
            apikey: this.config.publishableKey,
            Authorization: `Bearer ${accessToken || this.config.publishableKey}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            p_attempt_id: attemptId,
            p_auth_method: 'custom:yandex',
            p_outcome: outcome,
          }),
        },
      );
    } catch {
      // PII-free measurement is best effort and never blocks account entry.
    }
  }

  subscribe(subscriber: AuthSubscriber): () => void {
    this.subscribers.add(subscriber);
    subscriber(this.snapshot);
    return () => this.subscribers.delete(subscriber);
  }

  initialize(): Promise<StaticSiteAuthSnapshot> {
    if (!this.initialization) this.initialization = this.initializeOnce();
    return this.initialization;
  }

  private async initializeOnce(): Promise<StaticSiteAuthSnapshot> {
    const params = callbackParams();
    const callbackIntent = readAuthIntent();
    const callbackAttemptId = params.get('focus_auth_attempt') || String(callbackIntent.attempt_id || '');
    const callbackState = String(callbackIntent.state || '');
    const emailCallback = callbackState.startsWith('email_') || callbackIntent.auth_method === 'email';
    const yandexCallback = !emailCallback;
    if (callbackAttemptId) this.pendingEmailAttemptId = callbackAttemptId;
    const callbackError = params.get('error_code') || params.get('error') || params.get('error_description');
    if (callbackError) {
      cleanCallbackHistory();
      writeAuthIntent('callback_failed', { attempt_id: callbackAttemptId });
      if (yandexCallback) void this.recordAuthMethodAttempt(callbackAttemptId, 'failed');
      return this.publish({
        status: 'error',
        user: null,
        message: yandexCallback
          ? 'Вход через Яндекс не завершён. Попробуйте ещё раз.'
          : 'Вход по ссылке из письма не завершён. Запросите новое письмо.',
        callbackAttempted: true,
      });
    }

    const code = params.get('code');
    const tokenHash = params.get('token_hash');
    const callbackType = params.get('type');
    if (tokenHash && callbackType) {
      const transportStartedAt = Date.now();
      this.publish({
        status: 'checking',
        user: null,
        message: 'Завершаю вход по ссылке из письма…',
        callbackAttempted: true,
      });
      try {
        const { data, error } = await this.client.auth.verifyOtp({
          token_hash: tokenHash,
          type: callbackType as any,
        });
        if (error) throw error;
        cleanCallbackHistory();
        if (data.session?.user) {
          writeAuthIntent('signed_in');
          const transport = authTransportReceipt(this.transport.latestOutcome('auth.verify', transportStartedAt));
          void this.recordAuthTransport(
            'focus_auth_record_verification_v1', callbackAttemptId, transport, true, data.session.access_token,
          );
          return this.publish({
            status: 'signed_in',
            user: data.session.user,
            message: `Вошли как ${staticAuthDisplayName(data.session.user)}`,
            callbackAttempted: true,
          });
        }
        throw new Error('email_callback_no_session');
      } catch {
        const transport = authTransportReceipt(this.transport.latestOutcome('auth.verify', transportStartedAt));
        void this.recordAuthTransport(
          'focus_auth_record_verification_v1',
          callbackAttemptId,
          transport,
          failedVerificationState(transport),
        );
        cleanCallbackHistory();
        writeAuthIntent('email_callback_failed', { attempt_id: callbackAttemptId });
        return this.publish({
          status: 'error',
          user: null,
          message: 'Ссылка устарела или уже использована. Запросите новое письмо.',
          callbackAttempted: true,
        });
      }
    }

    if (code) {
      const transportStartedAt = Date.now();
      this.publish({
        status: 'checking',
        user: null,
        message: emailCallback ? 'Завершаю вход по ссылке из письма…' : 'Завершаю вход через Яндекс…',
        callbackAttempted: true,
      });
      writeAuthIntent('callback_started', {
        attempt_id: callbackAttemptId,
        auth_method: emailCallback ? 'email' : 'custom:yandex',
      });
      try {
        const { data, error } = await withTimeout(
          this.client.auth.exchangeCodeForSession(code),
          CALLBACK_TIMEOUT_MS,
          'auth_callback_timeout',
        );
        if (error) throw error;
        if (data.session?.access_token && data.session.refresh_token) {
          await this.client.auth.setSession({
            access_token: data.session.access_token,
            refresh_token: data.session.refresh_token,
          });
        }
        cleanCallbackHistory();
        if (data.session?.user) {
          writeAuthIntent('signed_in');
          if (emailCallback) {
            const transport = authTransportReceipt(this.transport.latestOutcome('auth.token', transportStartedAt));
            void this.recordAuthTransport(
              'focus_auth_record_verification_v1', callbackAttemptId, transport, true, data.session.access_token,
            );
          } else {
            void this.recordAuthMethodAttempt(callbackAttemptId, 'verified', data.session.access_token);
          }
          return this.publish({
            status: 'signed_in',
            user: data.session.user,
            message: `Вошли как ${staticAuthDisplayName(data.session.user)}`,
            callbackAttempted: true,
          });
        }
        throw new Error('callback_no_browser_session');
      } catch (error) {
        const rawMessage = String((error as Error)?.message || error);
        const callbackTransport = emailCallback
          ? authTransportReceipt(this.transport.latestOutcome('auth.token', transportStartedAt))
          : null;
        cleanCallbackHistory();
        try {
          const { data: existing } = await withTimeout(
            this.client.auth.getSession(),
            SESSION_TIMEOUT_MS,
            'auth_session_timeout',
          );
          if (existing.session?.user) {
            writeAuthIntent('signed_in');
            if (emailCallback) {
              void this.recordAuthTransport(
                'focus_auth_record_verification_v1', callbackAttemptId, callbackTransport, true, existing.session.access_token,
              );
            } else {
              void this.recordAuthMethodAttempt(callbackAttemptId, 'verified', existing.session.access_token);
            }
            return this.publish({
              status: 'signed_in',
              user: existing.session.user,
              message: `Вошли как ${staticAuthDisplayName(existing.session.user)}`,
              callbackAttempted: true,
            });
          }
        } catch {
          // The callback-specific product error below remains authoritative.
        }
        if (emailCallback) {
          void this.recordAuthTransport(
            'focus_auth_record_verification_v1',
            callbackAttemptId,
            callbackTransport,
            failedVerificationState(callbackTransport),
          );
        } else {
          void this.recordAuthMethodAttempt(
            callbackAttemptId,
            /timeout|network|fetch|abort/iu.test(rawMessage) ? 'ambiguous' : 'failed',
          );
        }
        writeAuthIntent('callback_failed', { reason: rawMessage.slice(0, 120) });
        const message = emailCallback
          ? 'Вход по ссылке из письма не завершён. Запросите новое письмо.'
          : rawMessage === 'auth_callback_timeout'
            ? 'Вход через Яндекс не завершён: браузер не получил ответ. Попробуйте ещё раз.'
            : /code verifier|flow state|auth code|invalid|pkce/iu.test(rawMessage)
              ? 'Вход через Яндекс не завершён: сессия входа устарела. Попробуйте ещё раз с этой страницы.'
              : 'Вход через Яндекс не завершён. Попробуйте ещё раз.';
        return this.publish({
          status: 'error',
          user: null,
          message,
          callbackAttempted: true,
        });
      }
    }

    try {
      const { data } = await withTimeout(
        this.client.auth.getSession(),
        SESSION_TIMEOUT_MS,
        'auth_session_timeout',
      );
      if (data.session?.user) {
        writeAuthIntent('signed_in');
        return this.publish({
          status: 'signed_in',
          user: data.session.user,
          message: `Вошли как ${staticAuthDisplayName(data.session.user)}`,
          callbackAttempted: false,
        });
      }
      return this.publish({
        status: 'signed_out',
        user: null,
        message: 'Войдите через Яндекс.',
        callbackAttempted: false,
      });
    } catch {
      return this.publish({
        status: 'error',
        user: null,
        message: 'Не удалось проверить сохранённый вход. Перезагрузите страницу или войдите ещё раз.',
        callbackAttempted: false,
      });
    }
  }

  async getSession() {
    await this.initialize();
    const { data } = await withTimeout(
      this.client.auth.getSession(),
      SESSION_TIMEOUT_MS,
      'auth_session_timeout',
    );
    return data.session || null;
  }

  async signIn(): Promise<boolean> {
    await this.initialize();
    const attemptId = authAttemptId();
    const redirect = new URL(cleanStaticAuthUrl());
    redirect.searchParams.set('focus_auth_attempt', attemptId);
    const redirectTo = redirect.href;
    writeAuthIntent('login_started', { redirect_to: redirectTo, attempt_id: attemptId });
    await Promise.race([
      this.recordAuthMethodAttempt(attemptId, 'started'),
      new Promise<void>((resolve) => window.setTimeout(resolve, 1_200)),
    ]);
    this.publish({
      status: 'checking',
      user: null,
      message: 'Открываю вход через Яндекс…',
      callbackAttempted: false,
    });
    const { error } = await this.client.auth.signInWithOAuth({
      // Supabase supports dashboard-defined `custom:*` providers at runtime,
      // while the generated Provider union only enumerates built-in names.
      provider: this.config.provider as any,
      options: { redirectTo },
    });
    if (!error) return true;
    writeAuthIntent('login_start_failed', { reason: String(error.message || error).slice(0, 120), attempt_id: attemptId });
    void this.recordAuthMethodAttempt(attemptId, 'failed');
    this.publish({
      status: 'error',
      user: null,
      message: 'Не удалось открыть вход через Яндекс. Попробуйте ещё раз.',
      callbackAttempted: false,
    });
    return false;
  }

  async signInWithEmailOtp(email: string, redirectTo = cleanStaticAuthUrl()): Promise<StaticSiteEmailOtpResult> {
    await this.initialize();
    const normalizedEmail = String(email || '').trim().toLocaleLowerCase('en-US');
    const attemptId = authAttemptId();
    if (!normalizedEmail) return {
      status: 'request_failed', accepted: false, message: 'Введите адрес электронной почты.', attemptId, transport: null,
    };
    const attemptRedirect = new URL(redirectTo);
    attemptRedirect.searchParams.set('focus_auth_attempt', attemptId);
    this.pendingEmailAttemptId = attemptId;
    writeAuthIntent('email_login_started', { redirect_to: redirectTo, attempt_id: attemptId });
    this.publish({
      status: 'checking',
      user: null,
      message: 'Отправляю одноразовую ссылку на email…',
      callbackAttempted: false,
    });
    let error: unknown = null;
    const transportStartedAt = Date.now();
    try {
      // The transport owns the only request timeout. A caller-side Promise.race
      // cannot cancel or disambiguate the underlying OTP POST and previously
      // allowed a late request to finish after the UI had offered a resend.
      const result = await this.client.auth.signInWithOtp({
        email: normalizedEmail,
        options: {
          emailRedirectTo: attemptRedirect.href,
          shouldCreateUser: true,
        },
      });
      error = result.error;
    } catch (caught) {
      error = caught;
    }
    if (!error) {
      const transport = authTransportReceipt(this.transport.latestOutcome('auth.otp', transportStartedAt));
      void this.recordAuthTransport('focus_auth_record_client_outcome_v1', attemptId, transport);
      this.publish({
        status: 'signed_out',
        user: null,
        message: 'Письмо отправлено. Откройте одноразовую ссылку в этом браузере.',
        callbackAttempted: false,
      });
      return {
        status: 'accepted', accepted: true,
        message: 'Письмо отправлено. Откройте одноразовую ссылку в этом браузере.',
        attemptId, transport,
      };
    }
    const rawMessage = String((error as Error)?.message || error);
    const transportError = parseSupabaseTransportError(error);
    const ambiguous = transportError?.code === 'ambiguous';
    const noHealthyRoute = transportError?.code === 'no_route';
    const receiptState = ambiguous ? await this.resolveEmailDeliveryReceipt(attemptId) : 'missing';
    if (receiptState === 'accepted') {
      const transport = authTransportReceipt(this.transport.latestOutcome('auth.otp', transportStartedAt));
      void this.recordAuthTransport('focus_auth_record_client_outcome_v1', attemptId, transport);
      this.publish({
        status: 'signed_out', user: null,
        message: 'Письмо принято к отправке. Введите код из письма.',
        callbackAttempted: false,
      });
      return {
        status: 'accepted', accepted: true,
        message: 'Письмо принято к отправке. Введите код из письма.',
        attemptId, transport,
      };
    }
    writeAuthIntent(ambiguous ? 'email_login_ambiguous' : 'email_login_failed', { reason: rawMessage.slice(0, 120) });
    const status: StaticSiteEmailOtpStatus = ambiguous
      ? 'ambiguous'
      : /rate|too many|429/iu.test(rawMessage)
        ? 'rate_limited'
        : 'request_failed';
    const message = ambiguous
      ? 'Не получили подтверждение отправки. Запрос мог быть принят — не отправляйте его повторно сразу, сначала проверьте почту.'
      : noHealthyRoute
        ? 'Сейчас нет устойчивого соединения для отправки письма. Подождите немного и попробуйте ещё раз.'
        : status === 'rate_limited'
          ? 'Слишком много попыток подряд. Подождите немного и повторите отправку.'
          : 'Не удалось отправить письмо. Проверьте адрес и попробуйте ещё раз.';
    this.publish({
      status: 'error',
      user: null,
      message,
      callbackAttempted: false,
    });
    const transport = authTransportReceipt(this.transport.latestOutcome('auth.otp', transportStartedAt));
    void this.recordAuthTransport('focus_auth_record_client_outcome_v1', attemptId, transport);
    return { status, accepted: false, message, attemptId, transport };
  }

  async verifyEmailOtp(email: string, token: string): Promise<StaticSiteEmailVerifyResult> {
    await this.initialize();
    const normalizedEmail = String(email || '').trim().toLocaleLowerCase('en-US');
    const normalizedToken = String(token || '').replace(/\D/gu, '').slice(0, 6);
    const verificationAttemptId = this.pendingEmailAttemptId || String(readAuthIntent().attempt_id || '');
    if (!normalizedEmail || !/^\d{6}$/u.test(normalizedToken)) {
      return { ok: false, status: 'invalid', message: 'Проверьте адрес и код из письма.', transport: null };
    }
    const transportStartedAt = Date.now();
    try {
      const { data, error } = await this.client.auth.verifyOtp({
        email: normalizedEmail,
        token: normalizedToken,
        type: 'email',
      });
      if (error) throw error;
      if (!data.session?.user) throw new Error('email_otp_no_session');
      writeAuthIntent('signed_in');
      this.publish({
        status: 'signed_in',
        user: data.session.user,
        message: `Вошли как ${staticAuthDisplayName(data.session.user)}`,
        callbackAttempted: true,
      });
      const transport = authTransportReceipt(this.transport.latestOutcome('auth.verify', transportStartedAt));
      void this.recordAuthTransport(
        'focus_auth_record_verification_v1', verificationAttemptId, transport, true, data.session.access_token,
      );
      return { ok: true, status: 'verified', message: 'Код подтверждён.', transport };
    } catch (error) {
      const ambiguous = parseSupabaseTransportError(error)?.code === 'ambiguous';
      const transport = authTransportReceipt(this.transport.latestOutcome('auth.verify', transportStartedAt));
      void this.recordAuthTransport(
        'focus_auth_record_verification_v1',
        verificationAttemptId,
        transport,
        ambiguous ? null : false,
      );
      return ambiguous
        ? { ok: false, status: 'ambiguous', message: 'Ответ не получен. Не вводите код повторно сразу — сначала обновите страницу.', transport }
        : { ok: false, status: 'invalid', message: 'Код неверный, устарел или уже использован.', transport };
    }
  }

  async registerFocusGroupParticipant(input: { focusUpdatesConsent: boolean; sourceRoute: string }): Promise<boolean> {
    const session = await this.getSession();
    if (!session?.user || !session.access_token) return false;
    try {
      const response = await this.dataClient.request(
        `${this.config.supabaseUrl}/rest/v1/rpc/register_focus_group_participant_v1`,
        {
          method: 'POST',
          headers: {
            apikey: this.config.publishableKey,
            Authorization: `Bearer ${session.access_token}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            p_communication_opt_in: input.focusUpdatesConsent === true,
            p_source_route: String(input.sourceRoute || window.location.pathname).slice(0, 160),
          }),
        },
      );
      return response.ok || response.status === 409;
    } catch {
      return false;
    }
  }

  async resetForOnboardingTest(): Promise<boolean> {
    this.client.auth.stopAutoRefresh();
    const clearPersistedSession = () => {
      let cleared = false;
      try { cleared = purgeStaticAuthStorage(this.config.supabaseUrl, window.localStorage); } catch { cleared = false; }
      purgePkceCookies();
      return cleared;
    };
    // Clear the browser session before attempting any network-assisted Auth
    // cleanup. A blocked logout request must never preserve the old identity
    // while the reset page claims that a clean test has started.
    clearPersistedSession();
    try {
      await withTimeout(
        this.client.auth.signOut({ scope: 'local' }),
        LOCAL_RESET_TIMEOUT_MS,
        'local_auth_reset_timeout',
      );
    } catch {
      // The unconditional storage purge below remains authoritative.
    }
    const cleared = clearPersistedSession();
    this.transport.invalidate();
    clearAuthIntent();
    this.initialization = null;
    this.publish({
      status: 'signed_out',
      user: null,
      message: 'Можно пройти вход заново.',
      callbackAttempted: false,
    });
    cleanCallbackHistory();
    return cleared;
  }

  async linkYandexIdentity(): Promise<boolean> {
    const session = await this.getSession();
    if (!session?.user) return this.signIn();
    const redirectTo = cleanStaticAuthUrl();
    writeAuthIntent('identity_link_started', { redirect_to: redirectTo });
    this.publish({
      status: 'checking',
      user: session.user,
      message: 'Открываю Яндекс для привязки к текущему аккаунту…',
      callbackAttempted: false,
    });
    const { error } = await this.client.auth.linkIdentity({
      provider: this.config.provider as any,
      options: { redirectTo },
    });
    if (!error) return true;
    writeAuthIntent('identity_link_failed', { reason: String(error.message || error).slice(0, 120) });
    this.publish({
      status: 'error',
      user: session.user,
      message: 'Не удалось привязать Яндекс к текущему аккаунту. Попробуйте ещё раз.',
      callbackAttempted: false,
    });
    return false;
  }

  async signOut(): Promise<boolean> {
    await this.initialize();
    const signedInUser = this.snapshot.user;
    this.publish({
      status: 'checking',
      user: signedInUser,
      message: 'Завершаю сессию аккаунта…',
      callbackAttempted: false,
    });
    const { error } = await this.client.auth.signOut();
    if (error) {
      this.publish({
        status: 'error',
        user: signedInUser,
        message: 'Не удалось выйти из аккаунта. Проверьте соединение и попробуйте ещё раз.',
        callbackAttempted: false,
      });
      return false;
    }
    clearAuthIntent();
    this.publish({
      status: 'signed_out',
      user: null,
      message: 'Вы вышли из аккаунта.',
      callbackAttempted: false,
    });
    return true;
  }
}

declare global {
  interface Window {
    [CONTROLLER_KEY]?: StaticSiteAuthController;
  }
}

export function getStaticSiteAuth(config: StaticSiteAuthConfig): StaticSiteAuthController {
  const normalizedUrl = String(config.supabaseUrl || '').replace(/\/+$/u, '');
  const normalizedRelayUrl = String(config.relayUrl || '').replace(/\/+$/u, '');
  const publishableKey = String(config.publishableKey || '');
  if (!normalizedUrl || !publishableKey) {
    throw new Error('static_site_auth_public_config_missing');
  }
  const existing = window[CONTROLLER_KEY];
  if (existing) {
    if (
      existing.config.supabaseUrl !== normalizedUrl
      || existing.config.relayUrl !== normalizedRelayUrl
      || existing.config.publishableKey !== publishableKey
    ) {
      throw new Error('static_site_auth_config_conflict');
    }
    return existing;
  }
  const controller = new StaticSiteAuthController({
    supabaseUrl: normalizedUrl,
    relayUrl: normalizedRelayUrl,
    publishableKey,
    provider: config.provider,
  });
  window[CONTROLLER_KEY] = controller;
  return controller;
}
