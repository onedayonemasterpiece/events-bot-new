import { createClient, type SupabaseClient, type User } from '@supabase/supabase-js';
import {
  supabaseAuthStorageKey,
  type ResilientSupabaseTransport,
} from './resilientSupabaseTransport';
import { getResilientDataClient, type ResilientDataClient } from './resilientDataClient';

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
}

export interface StaticSiteEmailVerifyResult {
  ok: boolean;
  status: 'verified' | 'invalid' | 'ambiguous' | 'request_failed';
  message: string;
}

type AuthSubscriber = (snapshot: StaticSiteAuthSnapshot) => void;

const CONTROLLER_KEY = '__KENIGEVENTS_STATIC_SITE_AUTH_V1__';
const PKCE_COOKIE_PREFIX = 'ke_pkce_';
const AUTH_INTENT_KEY = 'ke_yandex_auth_intent_v1';
const CALLBACK_TIMEOUT_MS = 20_000;
const SESSION_TIMEOUT_MS = 8_000;
const CALLBACK_KEYS = ['code', 'error', 'error_code', 'error_description', 'state', 'sb', 'email_callback', 'token_hash', 'type'];

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
    const callbackError = params.get('error_code') || params.get('error') || params.get('error_description');
    if (callbackError) {
      cleanCallbackHistory();
      writeAuthIntent('callback_failed');
      return this.publish({
        status: 'error',
        user: null,
        message: 'Вход через Яндекс не завершён. Попробуйте ещё раз.',
        callbackAttempted: true,
      });
    }

    const code = params.get('code');
    const tokenHash = params.get('token_hash');
    const callbackType = params.get('type');
    if (tokenHash && callbackType) {
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
          return this.publish({
            status: 'signed_in',
            user: data.session.user,
            message: `Вошли как ${staticAuthDisplayName(data.session.user)}`,
            callbackAttempted: true,
          });
        }
        throw new Error('email_callback_no_session');
      } catch {
        cleanCallbackHistory();
        writeAuthIntent('email_callback_failed');
        return this.publish({
          status: 'error',
          user: null,
          message: 'Ссылка устарела или уже использована. Запросите новое письмо.',
          callbackAttempted: true,
        });
      }
    }

    if (code) {
      this.publish({
        status: 'checking',
        user: null,
        message: 'Завершаю вход через Яндекс…',
        callbackAttempted: true,
      });
      writeAuthIntent('callback_started');
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
          return this.publish({
            status: 'signed_in',
            user: data.session.user,
            message: `Вошли как ${staticAuthDisplayName(data.session.user)}`,
            callbackAttempted: true,
          });
        }
        throw new Error('callback_no_browser_session');
      } catch (error) {
        cleanCallbackHistory();
        const rawMessage = String((error as Error)?.message || error);
        try {
          const { data: existing } = await withTimeout(
            this.client.auth.getSession(),
            SESSION_TIMEOUT_MS,
            'auth_session_timeout',
          );
          if (existing.session?.user) {
            writeAuthIntent('signed_in');
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
        writeAuthIntent('callback_failed', { reason: rawMessage.slice(0, 120) });
        const message = rawMessage === 'auth_callback_timeout'
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
    const redirectTo = cleanStaticAuthUrl();
    writeAuthIntent('login_started', { redirect_to: redirectTo });
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
    writeAuthIntent('login_start_failed', { reason: String(error.message || error).slice(0, 120) });
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
    if (!normalizedEmail) return { status: 'request_failed', accepted: false, message: 'Введите адрес электронной почты.' };
    writeAuthIntent('email_login_started', { redirect_to: redirectTo });
    this.publish({
      status: 'checking',
      user: null,
      message: 'Отправляю одноразовую ссылку на email…',
      callbackAttempted: false,
    });
    let error: unknown = null;
    const requestStartedAt = Date.now();
    try {
      // The transport owns the only request timeout. A caller-side Promise.race
      // cannot cancel or disambiguate the underlying OTP POST and previously
      // allowed a late request to finish after the UI had offered a resend.
      const result = await this.client.auth.signInWithOtp({
        email: normalizedEmail,
        options: {
          emailRedirectTo: redirectTo,
          shouldCreateUser: true,
        },
      });
      error = result.error;
    } catch (caught) {
      error = caught;
    }
    if (!error) {
      this.publish({
        status: 'signed_out',
        user: null,
        message: 'Письмо отправлено. Откройте одноразовую ссылку в этом браузере.',
        callbackAttempted: false,
      });
      return { status: 'accepted', accepted: true, message: 'Письмо отправлено. Откройте одноразовую ссылку в этом браузере.' };
    }
    const rawMessage = String((error as Error)?.message || error);
    const ambiguous = this.transport.wasAmbiguousSince(requestStartedAt);
    const noHealthyRoute = this.transport.hadNoHealthyRouteSince(requestStartedAt);
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
    return { status, accepted: false, message };
  }

  async verifyEmailOtp(email: string, token: string): Promise<StaticSiteEmailVerifyResult> {
    await this.initialize();
    const normalizedEmail = String(email || '').trim().toLocaleLowerCase('en-US');
    const normalizedToken = String(token || '').replace(/\D/gu, '').slice(0, 6);
    if (!normalizedEmail || !/^\d{6}$/u.test(normalizedToken)) {
      return { ok: false, status: 'invalid', message: 'Проверьте адрес и код из письма.' };
    }
    const startedAt = Date.now();
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
      return { ok: true, status: 'verified', message: 'Код подтверждён.' };
    } catch (error) {
      const ambiguous = this.transport.wasAmbiguousSince(startedAt);
      return ambiguous
        ? { ok: false, status: 'ambiguous', message: 'Ответ не получен. Не вводите код повторно сразу — сначала обновите страницу.' }
        : { ok: false, status: 'invalid', message: 'Код неверный, устарел или уже использован.' };
    }
  }

  async registerFocusGroupParticipant(input: { focusUpdatesConsent: boolean; sourceRoute: string }): Promise<boolean> {
    const session = await this.getSession();
    if (!session?.user || !session.access_token) return false;
    try {
      const response = await this.dataClient.idempotentReplay(
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

  async resetForOnboardingTest(): Promise<void> {
    try { await this.client.auth.signOut({ scope: 'local' }); } catch { /* best effort test reset */ }
    clearAuthIntent();
    this.initialization = null;
    this.publish({
      status: 'signed_out',
      user: null,
      message: 'Можно пройти вход заново.',
      callbackAttempted: false,
    });
    cleanCallbackHistory();
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
