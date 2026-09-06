import {planVerifierSchema, planVerifierBatchSize,planVerifierBudgetMs,planVerifierPrompt,classifyPlanPayload,type SemanticPlan} from './assistant-plan-verification.ts';
// KenigEvents authorized vector search Edge Function.
// Runtime: Supabase Edge Functions / Deno.
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.108.2";
import {
  collapseOccurrenceFamilies,
  occurrenceFamilyKey,
  paginateOccurrenceFamilies,
} from "./occurrence-families.ts";
import {
  googleModelActionUrl,
  GoogleProviderAttemptError,
  GoogleQuotaBackend,
  GoogleQuotaKey,
  GoogleQuotaKeyCandidate,
  GoogleTokenUsage,
  resolveStrictGoogleQuotaPool,
  SharedGoogleQuotaError,
  withSharedGoogleQuotaAttempt,
} from "./google-quota.ts";
import { classifyVoiceSchemaPayload, voiceVerifierSchema, voiceCandidateFacts, verifyVoiceWindow, voiceVerifierAttemptTimeout, voiceVerifierPrompt } from "./assistant-verification.ts";
import { SEARCH_BACKEND_REVISION } from "./search-backend-revision.generated.ts";
import { handleAssistant, type AssistantDependencies, type AssistantRepository } from "./assistant-handler.ts";
import { assistantGenerator } from "./assistant-provider.ts";
import { assistantRepository } from "./assistant-repository.ts";
import { eligible as assistantEligible, cityName as assistantCityName, reject as assistantReject, type Intent as AssistantIntent } from "./assistant-intent.ts";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type, x-client-request-id",
  "Access-Control-Allow-Methods": "POST, GET, HEAD, OPTIONS",
  "Access-Control-Expose-Headers":
    "X-KenigEvents-Search-Contract, X-KenigEvents-Search-Revision",
};

const DEFAULT_LIMIT = 12;
const MAX_LIMIT = 24;
const EMBEDDING_DIM = 768;
const SEARCH_CONTRACT_VERSION = "event-search-contract-v2";
const EMBEDDING_POLICY_VERSION = "gemini-embedding-2-query-template-v2";
const LLM_POLICY_VERSION = "lite-first-gemma-overflow-v2";
const CACHE_POLICY_VERSION = "revision-bound-result-cache-v2";

const EXECUTION_MODES = [
  "cached_vector",
  "cold_vector",
  "cold_vector_llm",
  "degraded_vector_fallback",
] as const;

type ExecutionMode = (typeof EXECUTION_MODES)[number];

type SearchAttemptCounters = {
  embedding_provider_attempts: number;
  llm_provider_attempts: number;
  vector_rpc_attempts: number;
  result_cache_read_attempts: number;
  result_cache_hit_count: number;
  result_cache_write_attempts: number;
  query_embedding_cache_read_attempts: number;
  query_embedding_cache_hit_count: number;
};

type SearchRevisionSnapshot = {
  catalog_revision: string;
  corpus_revision: string;
  search_document_revision: string;
  document_count: number;
  embedding_count: number;
};

function emptyAttemptCounters(): SearchAttemptCounters {
  return {
    embedding_provider_attempts: 0,
    llm_provider_attempts: 0,
    vector_rpc_attempts: 0,
    result_cache_read_attempts: 0,
    result_cache_hit_count: 0,
    result_cache_write_attempts: 0,
    query_embedding_cache_read_attempts: 0,
    query_embedding_cache_hit_count: 0,
  };
}

function isExecutionMode(value: unknown): value is ExecutionMode {
  return (
    typeof value === "string" &&
    (EXECUTION_MODES as readonly string[]).includes(value)
  );
}

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      ...CORS_HEADERS,
      "Content-Type": "application/json; charset=utf-8",
    },
  });
}

function env(name: string, fallback = ""): string {
  const runtime = globalThis as typeof globalThis & { process?: { env?: Record<string, string | undefined> } };
  return (typeof Deno !== "undefined" ? Deno.env.get(name) : runtime.process?.env?.[name]) || fallback;
}

function googleModelId(value: string, fallback: string): string {
  return String(value || fallback || "")
    .replace(/^models\//, "")
    .trim();
}

function googleLimiterModelId(value: string): string {
  const model = googleModelId(value, "");
  return model.startsWith("gemma-") && model.endsWith("-it")
    ? model.slice(0, -3)
    : model;
}

function normalizeQuery(value: unknown): string {
  return String(value || "")
    .replace(/[\u0000-\u001F\u007F]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

const MAX_QUERY_LENGTH = 180;
const MAX_REQUEST_BODY_BYTES = 16 * 1024;

type QueryValidation =
  | { ok: true; query: string }
  | { ok: false; error: string; detail: string };

function validateSearchQuery(value: unknown): QueryValidation {
  const query = normalizeQuery(value);
  if (query.length < 3) {
    return {
      ok: false,
      error: "query_too_short",
      detail: "Введите хотя бы 3 символа.",
    };
  }
  if (query.length > MAX_QUERY_LENGTH) {
    return {
      ok: false,
      error: "query_too_long",
      detail: `Слишком длинный запрос: максимум ${MAX_QUERY_LENGTH} символов.`,
    };
  }
  const unsafePatterns: RegExp[] = [
    /<\s*\/?\s*(script|iframe|object|embed|svg|img|style|meta|link)/iu,
    /javascript\s*:/iu,
    /(?:--|\/\*|\*\/|;)/u,
    /(?:\{\{|\}\}|\$\{)/u,
    /\b(?:select|insert|update|delete|drop|alter|truncate|union)\b[\s\S]{0,48}\b(?:from|where|table|into|values|set)\b/iu,
    /\b(?:ignore|forget)\s+(?:all\s+)?(?:previous|prior)\s+(?:instructions?|rules?)\b/iu,
  ];
  if (unsafePatterns.some((pattern) => pattern.test(query))) {
    return {
      ok: false,
      error: "query_unsafe",
      detail:
        "Запрос похож на техническую команду. Опишите событие обычными словами.",
    };
  }
  if (/[^\p{L}\p{N}\p{M}\s.,!?«»"'()№:+\/\\\-–—]/u.test(query)) {
    return {
      ok: false,
      error: "query_bad_characters",
      detail:
        "В запросе есть неподдерживаемые символы. Оставьте обычный текст, дату, место или жанр.",
    };
  }
  return { ok: true, query };
}

type QueryFacets = {
  weekday_iso: number | null;
  weekday_ru: string | null;
  time_of_day: "morning" | "day" | "evening" | "night" | null;
  admission: "free" | "registration_required" | "paid" | null;
};

const WEEKDAY_ALIASES: Array<[RegExp, number, string]> = [
  [
    /(^|[^а-яa-z0-9])(пн|понедельник[аеу]?|понедельникам)(?=$|[^а-яa-z0-9])/u,
    1,
    "понедельник",
  ],
  [
    /(^|[^а-яa-z0-9])(вт|вторник[аеу]?|вторникам)(?=$|[^а-яa-z0-9])/u,
    2,
    "вторник",
  ],
  [/(^|[^а-яa-z0-9])(ср|сред[ауые]?|средам)(?=$|[^а-яa-z0-9])/u, 3, "среда"],
  [
    /(^|[^а-яa-z0-9])(чт|четверг[аеу]?|четвергам)(?=$|[^а-яa-z0-9])/u,
    4,
    "четверг",
  ],
  [
    /(^|[^а-яa-z0-9])(пт|пятниц[аеуы]?|пятницам)(?=$|[^а-яa-z0-9])/u,
    5,
    "пятница",
  ],
  [
    /(^|[^а-яa-z0-9])(сб|суббот[аеуы]?|субботам)(?=$|[^а-яa-z0-9])/u,
    6,
    "суббота",
  ],
  [
    /(^|[^а-яa-z0-9])(вс|воскресень[еяю]|воскресеньям)(?=$|[^а-яa-z0-9])/u,
    7,
    "воскресенье",
  ],
];

function parseQueryFacets(query: string): QueryFacets {
  const normalized = ` ${query.toLowerCase().replace(/ё/g, "е")} `;
  const weekday = WEEKDAY_ALIASES.find(([pattern]) => pattern.test(normalized));
  const timeOfDay =
    /(^|[^а-яa-z0-9])(утро|утром|утренн[а-яa-z0-9_-]*)(?=$|[^а-яa-z0-9])/u.test(
        normalized,
      )
      ? "morning"
      : /(^|[^а-яa-z0-9])(день|днем|дневн[а-яa-z0-9_-]*)(?=$|[^а-яa-z0-9])/u
          .test(
            normalized,
          )
      ? "day"
      : /(^|[^а-яa-z0-9])(вечер|вечером|вечерн[а-яa-z0-9_-]*)(?=$|[^а-яa-z0-9])/u
          .test(
            normalized,
          )
      ? "evening"
      : /(^|[^а-яa-z0-9])(ночь|ночью|ночн[а-яa-z0-9_-]*)(?=$|[^а-яa-z0-9])/u
          .test(
            normalized,
          )
      ? "night"
      : null;
  const admission =
    /(^|[^а-яa-z0-9])(бесплатн[а-яa-z0-9_-]*|свободн[а-яa-z0-9_-]+\s+вход|без\s+оплаты)(?=$|[^а-яa-z0-9])/u
        .test(
          normalized,
        )
      ? "free"
      : /(^|[^а-яa-z0-9])(регистрац[а-яa-z0-9_-]*|запис[ьи][а-яa-z0-9_-]*|по\s+записи)(?=$|[^а-яa-z0-9])/u
          .test(
            normalized,
          )
      ? "registration_required"
      : /(^|[^а-яa-z0-9])(билет[а-яa-z0-9_-]*|платн[а-яa-z0-9_-]*|купить)(?=$|[^а-яa-z0-9])/u
          .test(
            normalized,
          )
      ? "paid"
      : null;
  return {
    weekday_iso: weekday ? weekday[1] : null,
    weekday_ru: weekday ? weekday[2] : null,
    time_of_day: timeOfDay,
    admission,
  };
}

function clampInt(
  value: unknown,
  fallback: number,
  min: number,
  max: number,
): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, Math.trunc(parsed)));
}

function envInt(
  name: string,
  fallback: number,
  min: number,
  max: number,
): number {
  const raw = env(name).trim();
  return clampInt(raw === "" ? fallback : raw, fallback, min, max);
}

type GoogleApiKeyCandidate = {
  env_name: string;
};

type GoogleApiKeyGroups = {
  active: GoogleApiKeyCandidate[];
  reserve: GoogleApiKeyCandidate[];
};

const DEFAULT_EMBEDDING_KEY_ENVS = [
  "GOOGLE_API_KEY5",
  "GOOGLE_API_KEY4",
  "GOOGLE_API_KEY3",
  "GOOGLE_API_KEY2",
  "GOOGLE_API_KEY",
];

const DEFAULT_LLM_KEY_ENVS = [
  "GOOGLE_API_KEY5",
  "GOOGLE_API_KEY4",
  "GOOGLE_API_KEY3",
  "GOOGLE_API_KEY",
];

const DEFAULT_LLM_RESERVE_KEY_ENVS = ["GOOGLE_API_KEY2"];

function parseProviderKeyEnvNames(value: string, fallback: string[]): string[] {
  const rawNames = String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
  const names = rawNames.length > 0 ? rawNames : fallback;
  const out: string[] = [];
  const seen = new Set<string>();
  for (const name of names) {
    if (!name || seen.has(name)) continue;
    seen.add(name);
    out.push(name);
  }
  return out;
}

function googleProviderKeyCandidates(names: string[]): GoogleApiKeyCandidate[] {
  const keys: GoogleApiKeyCandidate[] = [];
  const seenValues = new Set<string>();
  for (const envName of names) {
    const value = env(envName).trim();
    if (!value || seenValues.has(value)) continue;
    seenValues.add(value);
    keys.push({ env_name: envName });
  }
  return keys;
}

function googleProviderKeyGroups(
  kind: "EMBEDDING" | "LLM",
  fallback = kind === "EMBEDDING"
    ? DEFAULT_EMBEDDING_KEY_ENVS
    : DEFAULT_LLM_KEY_ENVS,
): GoogleApiKeyGroups {
  const specific = env(`EVENT_SEARCH_${kind}_KEY_ENVS`);
  const shared = env("EVENT_SEARCH_GOOGLE_KEY_ENVS");
  const reserveSpecific = env(`EVENT_SEARCH_${kind}_RESERVE_KEY_ENVS`);
  const reserveShared = env("EVENT_SEARCH_GOOGLE_RESERVE_KEY_ENVS");
  const activeNames = parseProviderKeyEnvNames(specific || shared, fallback);
  const reserveNames = parseProviderKeyEnvNames(
    reserveSpecific || reserveShared,
    kind === "LLM" ? DEFAULT_LLM_RESERVE_KEY_ENVS : [],
  );
  const reserveNameSet = new Set(reserveNames);
  const reserve = googleProviderKeyCandidates(reserveNames);
  const reserveValueSet = new Set(
    reserve.map((key) => env(key.env_name).trim()).filter(Boolean),
  );
  const active = googleProviderKeyCandidates(activeNames).filter(
    (key) =>
      !reserveNameSet.has(key.env_name) &&
      !reserveValueSet.has(env(key.env_name).trim()),
  );
  return { active, reserve };
}

function providerKeyPool(kind: "EMBEDDING" | "LLM"): GoogleApiKeyCandidate[] {
  const groups = googleProviderKeyGroups(kind);
  return [...groups.active, ...groups.reserve];
}

function shouldTryNextGoogleKey(status: number): boolean {
  return status === 401 || status === 403 || status === 429 || status >= 500;
}

function shouldTryNextSharedQuotaKey(error: unknown): boolean {
  return (
    error instanceof SharedGoogleQuotaError &&
    error.stage === "reserve" &&
    ["rpm", "tpm", "rpd", "provider_429", "no_keys"].includes(
      error.blocked_reason || "",
    )
  );
}

async function fetchWithTimeout(
  url: string,
  init: RequestInit,
  timeoutMs: number,
  label: string,
): Promise<Response> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...init, signal: controller.signal });
  } catch (error) {
    if (controller.signal.aborted) throw new Error(`${label}_timeout`);
    throw new Error(`${label}_network:${errorMessage(error).slice(0, 120)}`);
  } finally {
    clearTimeout(timeoutId);
  }
}

async function sha256Hex(text: string): Promise<string> {
  const data = new TextEncoder().encode(text);
  const digest = await crypto.subtle.digest("SHA-256", data);
  return Array.from(new Uint8Array(digest))
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

function nowMs(): number {
  return Math.round(performance.now());
}

function shortHash(value: string): string {
  return value.slice(0, 16);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function servedListHash(
  queryHash: string,
  items: Candidate[],
  fallbackItems: Candidate[],
): Promise<string> {
  const ids = items.map(candidateId).filter((id) => id !== null);
  const fallbackIds = fallbackItems
    .map(candidateId)
    .filter((id) => id !== null);
  return sha256Hex(
    JSON.stringify({ query_hash: queryHash, ids, fallback_ids: fallbackIds }),
  );
}

function logEvent(name: string, payload: Record<string, unknown>): void {
  console.log(
    JSON.stringify({ event: name, ts: new Date().toISOString(), ...payload }),
  );
}

function bearerToken(header: string | null): string | null {
  const match = /^Bearer\s+(.+)$/i.exec(header || "");
  return match ? match[1] : null;
}

type EmbeddingResult = {
  values: number[];
  key_env: string;
  model: string;
  cache_status: "hit" | "miss" | "unavailable" | "store_failed";
};

type QueryEmbeddingCacheOptions = {
  supabaseUrl: string;
  queryHash: string;
  bypassRead?: boolean;
};

type SearchResultCacheOptions = {
  supabaseUrl: string;
  cacheKey: string;
  queryHash: string;
  embeddingModel: string;
  embeddingDocKind: string;
  requestSignature: string;
  ttlSeconds: number;
};

function personalizationServiceClient(supabaseUrl: string) {
  const serviceKey = env("SUPABASE_SERVICE_ROLE_KEY") ||
    env("PERSONALIZATION_SUPABASE_SECRET_KEY") ||
    env("PERSONALIZATION_SUPABASE_SERVICE_KEY") ||
    env("SUPABASE_SERVICE_KEY");
  if (!supabaseUrl || !serviceKey) return null;
  return createClient(supabaseUrl, serviceKey, {
    global: { headers: { Authorization: `Bearer ${serviceKey}` } },
    auth: { persistSession: false, autoRefreshToken: false },
  });
}

function sharedGoogleQuotaBackend(
  supabaseUrl: string,
): GoogleQuotaBackend | null {
  const service = personalizationServiceClient(supabaseUrl);
  if (!service) return null;
  return {
    async listActiveKeys(envNames: string[]) {
      const { data, error } = await service
        .from("google_ai_api_keys")
        .select("id,env_var_name,quota_scope,priority")
        .eq("is_active", true)
        .in("env_var_name", envNames)
        .order("priority", { ascending: true })
        .order("id", { ascending: true });
      if (error) {
        throw new Error(
          `google_key_metadata_rpc:${error.code || error.message || "unknown"}`,
        );
      }
      return (Array.isArray(data) ? data : []) as Array<{
        id: string;
        env_var_name: string;
        quota_scope: string;
        priority?: number | null;
      }>;
    },
    async rpc(name: string, payload: Record<string, unknown>) {
      const { data, error } = await service.rpc(name, payload);
      if (error) {
        throw new Error(`${name}:${error.code || error.message || "unknown"}`);
      }
      return data;
    },
  };
}

function googleTokenUsage(payload: Record<string, unknown>): GoogleTokenUsage {
  const usage =
    payload?.usageMetadata && typeof payload.usageMetadata === "object"
      ? (payload.usageMetadata as Record<string, unknown>)
      : {};
  const count = (value: unknown): number | null => {
    if (value === null || value === undefined || value === "") return null;
    const parsed = Number(value);
    return Number.isFinite(parsed) && parsed >= 0 ? Math.trunc(parsed) : null;
  };
  return {
    input_tokens: count(usage.promptTokenCount),
    output_tokens: count(usage.candidatesTokenCount),
    total_tokens: count(usage.totalTokenCount),
  };
}

function sharedGoogleAccountName(): string {
  return env("EVENT_SEARCH_GOOGLE_ACCOUNT_NAME", "event-search-edge");
}

function sharedGoogleConsumer(kind: "embedding" | "llm"): string {
  return env(
    `EVENT_SEARCH_${kind.toUpperCase()}_LIMITER_CONSUMER`,
    `event-search-edge-${kind}`,
  );
}

function parseEmbeddingValues(value: unknown): number[] | null {
  if (Array.isArray(value)) {
    const values = value.map((item) => Number(item));
    return values.length === EMBEDDING_DIM && values.every(Number.isFinite)
      ? values
      : null;
  }
  if (typeof value === "string") {
    const trimmed = value.trim().replace(/^\[/u, "").replace(/\]$/u, "");
    const values = trimmed
      .split(",")
      .map((item) => Number(item.trim()))
      .filter((item) => Number.isFinite(item));
    return values.length === EMBEDDING_DIM ? values : null;
  }
  return null;
}

async function queryEmbeddingCacheHash(query: string): Promise<string> {
  const salt = env(
    "EVENT_SEARCH_QUERY_HASH_SALT",
    "kenigevents-event-search-query-cache-v1",
  );
  return sha256Hex(`${salt}:${query.toLowerCase()}`);
}

async function readCachedQueryEmbedding(
  cache: QueryEmbeddingCacheOptions | undefined,
  model: string,
): Promise<number[] | null> {
  if (!cache?.supabaseUrl || !cache.queryHash) return null;
  const service = personalizationServiceClient(cache.supabaseUrl);
  if (!service) return null;
  try {
    const { data, error } = await service.rpc(
      "get_event_search_query_embedding_v1",
      {
        p_query_hash: cache.queryHash,
        p_embedding_model: model,
        p_embedding_dim: EMBEDDING_DIM,
      },
    );
    if (error || !Array.isArray(data) || data.length === 0) return null;
    return parseEmbeddingValues((data[0] as Candidate).embedding);
  } catch (_) {
    return null;
  }
}

async function storeCachedQueryEmbedding(
  cache: QueryEmbeddingCacheOptions | undefined,
  model: string,
  values: number[],
  keyEnv: string,
): Promise<boolean> {
  if (
    !cache?.supabaseUrl ||
    !cache.queryHash ||
    values.length !== EMBEDDING_DIM
  ) {
    return false;
  }
  const service = personalizationServiceClient(cache.supabaseUrl);
  if (!service) return false;
  try {
    const { error } = await service.rpc(
      "upsert_event_search_query_embedding_v1",
      {
        p_query_hash: cache.queryHash,
        p_embedding_model: model,
        p_embedding_dim: EMBEDDING_DIM,
        p_embedding: values,
        p_metadata: { key_env: keyEnv, source: "event-search-edge" },
      },
    );
    return !error;
  } catch (_) {
    return false;
  }
}

function resultCacheTtlSeconds(): number {
  return envInt("EVENT_SEARCH_RESULT_CACHE_TTL_SECONDS", 10800, 60, 21600);
}

async function searchResultCacheKey(parts: Record<string, unknown>): Promise<{
  cacheKey: string;
  requestSignature: string;
}> {
  const signature = JSON.stringify(parts);
  const digest = await sha256Hex(signature);
  return { cacheKey: digest, requestSignature: digest };
}

async function readCachedSearchResult(
  cache: SearchResultCacheOptions | null,
): Promise<Record<string, unknown> | null> {
  if (!cache?.supabaseUrl || !cache.cacheKey) return null;
  const service = personalizationServiceClient(cache.supabaseUrl);
  if (!service) return null;
  try {
    const { data, error } = await service.rpc(
      "get_event_search_result_cache_v1",
      { p_cache_key: cache.cacheKey },
    );
    if (error || !Array.isArray(data) || data.length === 0) return null;
    const response = (data[0] as Candidate)?.response;
    return response && typeof response === "object"
      ? (response as Record<string, unknown>)
      : null;
  } catch (_) {
    return null;
  }
}

async function storeCachedSearchResult(
  cache: SearchResultCacheOptions | null,
  response: Record<string, unknown>,
  metadata: Record<string, unknown>,
): Promise<boolean> {
  if (!cache?.supabaseUrl || !cache.cacheKey) return false;
  const service = personalizationServiceClient(cache.supabaseUrl);
  if (!service) return false;
  try {
    const { error } = await service.rpc("upsert_event_search_result_cache_v1", {
      p_cache_key: cache.cacheKey,
      p_query_hash: cache.queryHash,
      p_embedding_model: cache.embeddingModel,
      p_embedding_dim: EMBEDDING_DIM,
      p_embedding_doc_kind: cache.embeddingDocKind,
      p_request_signature: cache.requestSignature,
      p_response: response,
      p_ttl_seconds: cache.ttlSeconds,
      p_metadata: metadata,
    });
    return !error;
  } catch (_) {
    return false;
  }
}

async function embedQuery(
  query: string,
  quotaBackend: GoogleQuotaBackend | null,
  cache?: QueryEmbeddingCacheOptions,
  counters: SearchAttemptCounters = emptyAttemptCounters(),
  singleAttempt = false,
): Promise<EmbeddingResult> {
  const model = googleModelId(
    env("EVENT_SEARCH_EMBEDDING_MODEL"),
    "gemini-embedding-2",
  );
  if (!cache?.bypassRead) {
    counters.query_embedding_cache_read_attempts += 1;
    const cachedValues = await readCachedQueryEmbedding(cache, model);
    if (cachedValues) {
      counters.query_embedding_cache_hit_count += 1;
      return {
        values: cachedValues,
        key_env: "cache",
        model,
        cache_status: "hit",
      };
    }
  }

  const keys = await resolveStrictGoogleQuotaPool(
    quotaBackend,
    providerKeyPool("EMBEDDING") as GoogleQuotaKeyCandidate[],
  );

  const text = `task: search result | query: ${query}`;
  const timeoutMs = envInt(
    "EVENT_SEARCH_EMBEDDING_TIMEOUT_MS",
    8000,
    1000,
    20000,
  );
  const reservedTpm = envInt(
    "EVENT_SEARCH_EMBEDDING_RESERVED_TPM",
    512,
    1,
    30000,
  );
  const errors: string[] = [];
  const providerBlockedScopes = new Set<string>();
  for (const key of keys) {
    if (providerBlockedScopes.has(key.quota_scope)) continue;
    try {
      const numericValues = await withSharedGoogleQuotaAttempt({
        backend: quotaBackend,
        key,
        model: googleLimiterModelId(model),
        reservedTpm,
        consumer: sharedGoogleConsumer("embedding"),
        accountName: sharedGoogleAccountName(),
        readEnv: (name) => env(name),
        execute: async (apiKey, lease) => {
          counters.embedding_provider_attempts += 1;
          logEvent("event_search_google_provider_sent", {
            provider_kind: "embedding",
            model,
            limiter_model: lease.model,
            request_uid: lease.request_uid,
            api_key_id: lease.api_key_id,
            key_env: lease.limiter_env_name,
            minute_bucket: lease.minute_bucket,
            day_bucket: lease.day_bucket,
          });
          const response = await fetchWithTimeout(
            googleModelActionUrl(model, "embedContent"),
            {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                "x-goog-api-key": apiKey,
              },
              body: JSON.stringify({
                model: `models/${model}`,
                content: { parts: [{ text }] },
                outputDimensionality: EMBEDDING_DIM,
              }),
            },
            timeoutMs,
            "embedding_provider",
          );
          const payload = await response.json().catch(() => ({}));
          const usage = googleTokenUsage(payload as Record<string, unknown>);
          if (!response.ok) {
            throw new GoogleProviderAttemptError(
              `embedding_provider_${response.status}`,
              {
                provider_status: `http_${response.status}`,
                error_type: "provider_http",
                error_code: String(response.status),
                usage,
              },
            );
          }
          const values = (payload as Record<string, unknown>)?.embedding &&
              typeof (payload as Record<string, unknown>).embedding === "object"
            ? (
              (payload as Record<string, unknown>).embedding as Record<
                string,
                unknown
              >
            ).values
            : null;
          if (!Array.isArray(values) || values.length !== EMBEDDING_DIM) {
            throw new GoogleProviderAttemptError(
              `embedding_bad_dimension:${
                Array.isArray(values) ? values.length : "missing"
              }`,
              {
                provider_status: "succeeded_invalid_payload",
                error_type: "provider_payload",
                error_code: "bad_dimension",
                usage,
              },
            );
          }
          return {
            value: values.map((value: unknown) => Number(value)),
            provider_status: "succeeded",
            usage,
          };
        },
      });
      const stored = await storeCachedQueryEmbedding(
        cache,
        model,
        numericValues,
        key.limiter_env_name,
      );
      return {
        values: numericValues,
        key_env: key.limiter_env_name,
        model,
        cache_status: cache ? stored ? "miss" : "store_failed" : "unavailable",
      };
    } catch (error) {
      const message = errorMessage(error).slice(0, 120);
      errors.push(`${key.limiter_env_name}:${message}`);
      if (singleAttempt) throw error;
      if (shouldTryNextSharedQuotaKey(error)) {
        if (
          error instanceof SharedGoogleQuotaError &&
          error.blocked_reason === "provider_429"
        ) {
          providerBlockedScopes.add(key.quota_scope);
        }
        continue;
      }
      if (error instanceof SharedGoogleQuotaError) throw error;
      if (error instanceof GoogleProviderAttemptError) {
        if (Number(error.error_code) === 429) {
          providerBlockedScopes.add(key.quota_scope);
        }
        if (shouldTryNextGoogleKey(Number(error.error_code))) continue;
        throw error;
      }
      if (
        !message.includes("embedding_provider_timeout") &&
        !message.includes("embedding_provider_")
      ) {
        throw error;
      }
    }
  }
  throw new Error(
    `embedding_provider_all_keys_failed:${errors.slice(-3).join("|")}`,
  );
}

function extractGeminiText(payload: Record<string, unknown>): string {
  const parts = ((
    (payload?.candidates as Candidate[] | undefined)?.[0]?.content as
      | Candidate
      | undefined
  )?.parts as Candidate[] | undefined) || [];
  return parts
    .map((part) => (typeof part?.text === "string" ? part.text : ""))
    .filter(Boolean)
    .join("\n")
    .trim();
}

function extractJsonObjectText(text: string): string {
  const trimmed = String(text || "").trim();
  if (!trimmed) throw new Error("llm_empty_response");
  const unfenced = trimmed
    .replace(/^```(?:json)?\s*/iu, "")
    .replace(/\s*```$/u, "")
    .trim();
  if (unfenced.startsWith("{") && unfenced.endsWith("}")) return unfenced;
  const start = unfenced.indexOf("{");
  if (start < 0) {
    throw new Error(`llm_json_object_missing:${unfenced.slice(0, 60)}`);
  }
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let index = start; index < unfenced.length; index += 1) {
    const ch = unfenced[index];
    if (inString) {
      if (escaped) escaped = false;
      else if (ch === "\\") escaped = true;
      else if (ch === '"') inString = false;
      continue;
    }
    if (ch === '"') inString = true;
    else if (ch === "{") depth += 1;
    else if (ch === "}") {
      depth -= 1;
      if (depth === 0) return unfenced.slice(start, index + 1);
    }
  }
  throw new Error(`llm_json_object_unclosed:${unfenced.slice(0, 60)}`);
}

function parseLlmJson(text: string): Record<string, unknown> {
  return JSON.parse(extractJsonObjectText(text));
}

const LLM_VERIFIER_RESPONSE_SCHEMA = {
  type: "object",
  properties: {
    query_interpretation: {
      type: "string",
      description:
        "Short interpretation of the user's event-search intent: topic, genre, audience, scenario and constraints.",
    },
    exact_event_ids: {
      type: "array",
      items: { type: "integer" },
      description:
        "Provided candidate event ids that are strong exact matches, ordered by relevance.",
    },
    possible_event_ids: {
      type: "array",
      items: { type: "integer" },
      description:
        "Provided candidate event ids that are weak, partial or uncertain matches, ordered by plausibility.",
    },
    rejected_event_ids: {
      type: "array",
      items: { type: "integer" },
      description:
        "Provided candidate event ids that do not match the interpreted query.",
    },
  },
  required: [
    "query_interpretation",
    "exact_event_ids",
    "possible_event_ids",
    "rejected_event_ids",
  ],
  additionalProperties: false,
};

type Candidate = Record<string, unknown>;

function candidateId(candidate: Candidate): number | null {
  const raw = candidate?.event_id ??
    candidate?.id ??
    (candidate?.display as Candidate | undefined)?.event_id ??
    (candidate?.display as Candidate | undefined)?.id;
  const value = Number(raw);
  return Number.isFinite(value) ? value : null;
}

function truncateText(value: unknown, maxChars: number): string {
  const text = String(value || "")
    .replace(/\s+/gu, " ")
    .trim();
  if (text.length <= maxChars) return text;
  return `${text.slice(0, Math.max(0, maxChars - 1)).trim()}…`;
}

function compactSearchDigest(value: unknown): string | null {
  const text = String(value || "")
    .replace(/\r/gu, "")
    .trim();
  if (!text) return null;
  const wanted = ["Тип:", "Кратко:", "Описание:", "Темы:", "Условия:"];
  const lines = text
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
  const parts: string[] = [];
  for (const prefix of wanted) {
    const line = lines.find((candidate) => candidate.startsWith(prefix));
    if (!line) continue;
    const budget = prefix === "Описание:" ? 140 : 120;
    parts.push(`${prefix} ${truncateText(line.slice(prefix.length), budget)}`);
  }
  return parts.length
    ? truncateText(parts.join(" | "), 420)
    : truncateText(text, 420);
}

async function fetchCandidateDigests(
  supabaseUrl: string,
  eventIds: number[],
): Promise<Map<number, string>> {
  const serviceKey = env("SUPABASE_SERVICE_ROLE_KEY") ||
    env("PERSONALIZATION_SUPABASE_SECRET_KEY") ||
    env("PERSONALIZATION_SUPABASE_SERVICE_KEY") ||
    env("SUPABASE_SERVICE_KEY");
  const ids = Array.from(
    new Set(
      eventIds.filter((id) => Number.isFinite(id)).map((id) => Math.trunc(id)),
    ),
  ).slice(0, 60);
  if (!serviceKey || !supabaseUrl || ids.length === 0) return new Map();
  const select = "select=event_id,search_digest";
  const filter = `event_id=in.(${ids.join(",")})`;
  try {
    const response = await fetchWithTimeout(
      `${
        supabaseUrl.replace(/\/$/u, "")
      }/rest/v1/event_search_documents?${select}&${filter}`,
      {
        method: "GET",
        headers: {
          apikey: serviceKey,
          Authorization: `Bearer ${serviceKey}`,
          Accept: "application/json",
        },
      },
      envInt("EVENT_SEARCH_DIGEST_FETCH_TIMEOUT_MS", 1200, 250, 5000),
      "event_search_digest_fetch",
    );
    if (!response.ok) return new Map();
    const rows = await response.json();
    if (!Array.isArray(rows)) return new Map();
    const out = new Map<number, string>();
    for (const row of rows) {
      const id = Number((row as Candidate).event_id);
      const digest = (row as Candidate).search_digest;
      if (Number.isFinite(id) && typeof digest === "string" && digest.trim()) {
        out.set(id, digest);
      }
    }
    return out;
  } catch (_) {
    return new Map();
  }
}

function normalizeCandidate(
  row: Record<string, unknown>,
  index: number,
): Candidate {
  const snapshot = row.card_snapshot && typeof row.card_snapshot === "object"
    ? (row.card_snapshot as Candidate)
    : {};
  const eventId = Number(row.event_id ?? snapshot.event_id ?? snapshot.id);
  const display = snapshot.display && typeof snapshot.display === "object"
    ? (snapshot.display as Candidate)
    : {};
  const similarity = Number(row.similarity ?? 0);
  return {
    ...snapshot,
    event_id: eventId,
    id: eventId,
    title: snapshot.title || row.title || display.title || "Событие",
    category: snapshot.category || row.category || "event",
    city: row.city ?? snapshot.city,
    start_date: row.start_date ?? row.date_local ?? snapshot.start_date ?? snapshot.date,
    start_time: row.start_time ?? row.time_local ?? snapshot.start_time,
    audience_tags: row.audience_tags ?? snapshot.audience_tags,
    format_tags: row.format_tags ?? snapshot.format_tags,
    min_price: row.min_price ?? row.price_min ?? snapshot.min_price ?? snapshot.price_min,
    end_date: row.end_date ?? snapshot.end_date,
    time_of_day: row.time_of_day ?? snapshot.time_of_day,
    is_free: row.is_free ?? snapshot.is_free,
    ticket_kind: row.ticket_kind ?? snapshot.ticket_kind,
    admission_type: row.admission_type ?? snapshot.admission_type,
    lifecycle_status: row.lifecycle_status ?? snapshot.lifecycle_status,
    active: row.active ?? snapshot.active,
    tags: Array.isArray(snapshot.tags)
      ? snapshot.tags
      : Array.isArray(row.tags)
      ? row.tags
      : [],
    base_similarity: similarity,
    static_score: similarity,
    semantic_score: similarity,
    vector_distance: Number(row.distance ?? 0),
    reason_codes: Array.from(
      new Set([
        ...(Array.isArray(snapshot.reason_codes) ? snapshot.reason_codes : []),
        "retrieval:pgvector",
        `rank:${index + 1}`,
      ]),
    ),
    display: {
      ...display,
      id: eventId,
      event_id: eventId,
      title: display.title || snapshot.title || row.title || "Событие",
    },
  };
}

type LlmVerifyResult = {
  exact: Candidate[];
  possible: Candidate[];
  rejected_ids: number[];
  status: string;
  used: boolean;
  query_interpretation?: string;
  model?: string | null;
  policy?: string;
  attempts?: LlmAttempt[];
  prompt_chars?: number;
  prompt_fact_chars?: number;
  compact_candidate_count?: number;
  verification?: Record<string, any>;
};

type LlmVerifyOptions = {
  gemma_overflow_allowed: boolean;
  quota_backend: GoogleQuotaBackend | null;
  counters?: SearchAttemptCounters;
  reserve_canary_attempt?: () => Promise<boolean>;
  voiceIntent?: AssistantIntent;
  semanticPlan?: SemanticPlan;
  deadline?: number;
};

type LlmAttempt = {
  model: string;
  role: "primary" | "fallback";
  attempt: number;
  status: string;
  elapsed_ms: number;
  timeout_ms: number;
  prompt_chars: number;
  prompt_fact_chars: number;
  compact_candidate_count: number;
  key_env?: string;
};

type ParsedLlmClassification = {
  exact: Candidate[];
  possible: Candidate[];
  rejected_ids: number[];
  status: string;
  used: boolean;
  query_interpretation?: string;
};

function uniqueCandidatesByIds(
  ids: unknown[],
  allowed: Map<number | null, Candidate>,
): Candidate[] {
  const out: Candidate[] = [];
  const seen = new Set<number>();
  for (const rawId of Array.isArray(ids) ? ids : []) {
    const id = Number(rawId);
    if (!Number.isFinite(id) || seen.has(id)) continue;
    const candidate = allowed.get(id);
    if (!candidate) continue;
    out.push(candidate);
    seen.add(id);
  }
  return out;
}

function sanitizedLlmAttempts(attempts: LlmAttempt[] | undefined) {
  return (attempts || []).map(({ key_env: _keyEnv, ...attempt }) => attempt);
}

function sleepMs(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));
}

function parseModelList(value: string, fallback: string): string[] {
  const raw = String(value || fallback || "")
    .split(",")
    .map((item) => googleModelId(item, ""))
    .filter(Boolean);
  return Array.from(new Set(raw));
}

function adaptiveHalfCandidateProfile(total: number): number[] {
  if (total <= 0) return [];
  const out: number[] = [];
  let count = total;
  for (let step = 0; step < 3 && count >= 1; step += 1) {
    if (!out.includes(count)) out.push(count);
    if (count === 1) break;
    count = Math.max(1, Math.ceil(count / 2));
  }
  return out;
}

function parseCandidateCountProfile(
  value: string,
  total: number,
  fallback: number[],
): number[] {
  const raw = String(value || "")
    .split(",")
    .map((item) => Number(item.trim()))
    .filter((item) => Number.isFinite(item) && item > 0);
  const base = raw.length > 0 ? raw : fallback;
  const out: number[] = [];
  for (const count of base) {
    const clamped = Math.max(1, Math.min(total, Math.trunc(count)));
    if (!out.includes(clamped)) out.push(clamped);
  }
  if (out.length === 0 && total > 0) out.push(total);
  return out;
}

function parseTimeoutProfile(
  value: string,
  fallback: number[],
  minMs: number,
  maxMs: number,
): number[] {
  const raw = String(value || "")
    .split(",")
    .map((item) => Number(item.trim()))
    .filter((item) => Number.isFinite(item) && item > 0);
  const base = raw.length > 0 ? raw : fallback;
  const out = base.map((item) =>
    Math.max(minMs, Math.min(maxMs, Math.trunc(item)))
  );
  return out.length > 0 ? out : fallback;
}

function isRetryableLlmStatus(status: string): boolean {
  return (
    status.includes("timeout") ||
    /^degraded:quota_(?:rpm|tpm|rpd|no_keys)$/u.test(status) ||
    status.startsWith("degraded:provider_5") ||
    status === "degraded:provider_429" ||
    status === "degraded:over_approval" ||
    status === "degraded:empty_classification" ||
    status.startsWith("degraded:llm_") ||
    status.startsWith("degraded:provider_network")
  );
}

function classifyLlmPayload(
  text: string,
  candidates: Candidate[],
): ParsedLlmClassification {
  const parsed = parseLlmJson(text);
  const allowed = new Map(
    candidates.map((candidate) => [candidateId(candidate), candidate]),
  );
  const exact = uniqueCandidatesByIds(
    parsed.exact_event_ids as unknown[],
    allowed,
  ).map((candidate) => ({
    ...candidate,
    reason_codes: [
      ...((candidate.reason_codes as string[]) || []),
      "llm:exact",
    ],
  }));
  const exactIds = new Set(exact.map(candidateId));
  const possible = uniqueCandidatesByIds(
    parsed.possible_event_ids as unknown[],
    allowed,
  )
    .filter((candidate) => !exactIds.has(candidateId(candidate)))
    .map((candidate) => ({
      ...candidate,
      reason_codes: [
        ...((candidate.reason_codes as string[]) || []),
        "llm:possible",
      ],
    }));
  const rejectedIds = (
    Array.isArray(parsed.rejected_event_ids) ? parsed.rejected_event_ids : []
  )
    .map((rawId) => Number(rawId))
    .filter((id) => Number.isFinite(id));
  const classifiedCount = exact.length + possible.length + rejectedIds.length;
  const queryInterpretation = String(parsed.query_interpretation || "").slice(
    0,
    500,
  );
  const overApprovalDemoteEnabled = ["1", "true", "yes", "on"].includes(
    env("EVENT_SEARCH_LLM_OVER_APPROVAL_DEMOTE_ENABLED", "0").toLowerCase(),
  );
  const overApprovalRatio = Number(
    env("EVENT_SEARCH_LLM_OVER_APPROVAL_RATIO", "0.9"),
  );
  const exactApprovalLimit = Math.ceil(
    candidates.length *
      (Number.isFinite(overApprovalRatio) ? overApprovalRatio : 0.9),
  );
  if (
    overApprovalDemoteEnabled &&
    candidates.length >= 8 &&
    exact.length > exactApprovalLimit &&
    possible.length === 0 &&
    rejectedIds.length === 0
  ) {
    return {
      exact: [],
      possible: candidates.map((candidate) => ({
        ...candidate,
        reason_codes: [
          ...((candidate.reason_codes as string[]) || []),
          "llm:possible_over_approval",
        ],
      })),
      rejected_ids: rejectedIds,
      status: "degraded:over_approval",
      used: false,
      query_interpretation: queryInterpretation,
    };
  }
  if (classifiedCount === 0) {
    return {
      exact: [],
      possible: candidates,
      rejected_ids: [],
      status: "degraded:empty_classification",
      used: false,
      query_interpretation: queryInterpretation,
    };
  }
  return {
    exact,
    possible,
    rejected_ids: rejectedIds,
    status: "ok",
    used: true,
    query_interpretation: queryInterpretation,
  };
}

async function llmVerify(
  query: string,
  candidates: Candidate[],
  candidateDigests: Map<number, string> = new Map(),
  options: LlmVerifyOptions = {
    gemma_overflow_allowed: true,
    quota_backend: null,
  },
): Promise<LlmVerifyResult> {
  const enabled = ["1", "true", "yes", "on"].includes(
    env("EVENT_SEARCH_LLM_ENABLED", "").toLowerCase(),
  );
  if (candidates.length === 0) {
    return {
      exact: [],
      possible: [],
      rejected_ids: [],
      status: "skipped_no_candidates",
      used: false,
    };
  }
  if (!enabled) {
    return {
      exact: [],
      possible: candidates,
      rejected_ids: [],
      status: "disabled",
      used: false,
    };
  }
  const factCoverage = candidateDigests.size / Math.max(1, candidates.length);
  if (factCoverage < (options.voiceIntent ? 1 : 0.5)) {
    return {
      exact: [],
      possible: candidates,
      rejected_ids: [],
      status: "degraded:digest_insufficient",
      used: false,
    };
  }
  let llmKeys: GoogleQuotaKey[];
  try {
    llmKeys = await resolveStrictGoogleQuotaPool(
      options.quota_backend,
      providerKeyPool("LLM") as GoogleQuotaKeyCandidate[],
    );
  } catch (error) {
    return {
      exact: [],
      possible: candidates,
      rejected_ids: [],
      status: `degraded:${errorMessage(error).slice(0, 100)}`,
      used: false,
    };
  }
  const primaryModels = parseModelList(
    env("EVENT_SEARCH_LLM_LITE_MODELS") || env("EVENT_SEARCH_LLM_LITE_MODEL"),
    "gemini-3.1-flash-lite",
  );
  const fallbackModels = parseModelList(
    env("EVENT_SEARCH_LLM_GEMMA_OVERFLOW_MODELS") ||
      env("EVENT_SEARCH_LLM_GEMMA_OVERFLOW_MODEL") ||
      env("EVENT_SEARCH_LLM_FALLBACK_MODELS") ||
      env("EVENT_SEARCH_LLM_FALLBACK_MODEL") ||
      env("EVENT_SEARCH_LLM_MODEL"),
    "gemma-4-26b-a4b-it",
  );
  const fallbackEnabled = ["1", "true", "yes", "on"].includes(
    env("EVENT_SEARCH_LLM_GEMMA_OVERFLOW_ENABLED", "1").toLowerCase(),
  );
  const policy = "lite_first_gemma_overflow";
  const primaryAttempts = options.voiceIntent ? 1 : envInt("EVENT_SEARCH_LLM_LITE_ATTEMPTS", 1, 1, 4);
  const primaryTimeoutMs = options.voiceIntent ? envInt("EVENT_SEARCH_ASSISTANT_VERIFIER_TIMEOUT_MS", 15000, 1000, 30000) : envInt(
    "EVENT_SEARCH_LLM_LITE_TIMEOUT_MS",
    2600,
    500,
    12000,
  );
  const primaryTimeoutProfileMs = parseTimeoutProfile(
    env("EVENT_SEARCH_LLM_LITE_TIMEOUT_PROFILE_MS"),
    [primaryTimeoutMs, 1200, 700],
    300,
    12000,
  );
  const primaryTotalBudgetMs = options.voiceIntent ? primaryTimeoutMs : envInt(
    "EVENT_SEARCH_LLM_LITE_TOTAL_BUDGET_MS",
    4300,
    800,
    12000,
  );
  const fallbackTimeoutMs = envInt(
    "EVENT_SEARCH_LLM_GEMMA_OVERFLOW_TIMEOUT_MS",
    9000,
    1000,
    30000,
  );
  const retryBackoffMs = envInt(
    "EVENT_SEARCH_LLM_LITE_RETRY_BACKOFF_MS",
    250,
    0,
    5000,
  );
  const shouldTryFallback = options.gemma_overflow_allowed &&
    fallbackEnabled &&
    fallbackModels.length > 0;
  const factMaxChars = options.voiceIntent ? envInt("EVENT_SEARCH_ASSISTANT_VERIFIER_FACT_CHARS", 2400, 1000, 4000) : envInt("EVENT_SEARCH_LLM_FACT_MAX_CHARS", 180, 120, 800);
  const maxLlmCandidates = Math.min(
    candidates.length,
    options.voiceIntent ? candidates.length : envInt("EVENT_SEARCH_LLM_MAX_CANDIDATES", 20, 1, 60),
  );
  type LlmPromptProfile = {
    candidates: Candidate[];
    prompt: string;
    prompt_chars: number;
    prompt_fact_chars: number;
    compact_candidate_count: number;
  };
  const buildPromptProfile = (candidateCount: number): LlmPromptProfile => {
    const promptCandidates = candidates.slice(0, candidateCount);
    const compact = promptCandidates.map((candidate, index) => {
      const display = (candidate.display as Candidate | undefined) || {};
      const id = candidateId(candidate);
      const facts = options.semanticPlan ? voiceCandidateFacts(id === null ? null : candidateDigests.get(id), factMaxChars) : truncateText(
        options.voiceIntent ? voiceCandidateFacts(id === null ? null : candidateDigests.get(id), factMaxChars) : compactSearchDigest(id === null ? null : candidateDigests.get(id)),
        factMaxChars,
      );
      return {
        id,
        rank: index + 1,
        title: candidate.title,
        category: candidate.category,
        tags: candidate.tags,
        event_type: display.event_type || candidate.event_type || null,
        date: display.display_date_time || candidate.date,
        place: display.place || candidate.location_name,
        status: display.status_label || candidate.status || null,
        facts,
        ...(options.voiceIntent ? { start_date: candidate.start_date, end_date: candidate.end_date, start_time: candidate.start_time, city: candidate.city, is_free: candidate.is_free, min_price: candidate.min_price, audience_tags: candidate.audience_tags, format_tags: candidate.format_tags } : {}),
      };
    });
    const prompt = options.voiceIntent ? (options.semanticPlan ? planVerifierPrompt(options.semanticPlan,options.voiceIntent,compact) : voiceVerifierPrompt(options.voiceIntent, compact)) : [
      "Проверь результаты поиска афиши Калининграда.",
      "Каждый candidate ID отнеси ровно в один список: exact_event_ids, possible_event_ids, rejected_event_ids.",
      "exact: факты прямо соответствуют запросу; сомнения или мало фактов — possible; явное несоответствие — rejected.",
      "Не добавляй ID вне candidates. Верни только JSON по схеме.",
      `query=${JSON.stringify(query)}`,
      `candidates=${JSON.stringify(compact)}`,
    ].join("\n");
    return {
      candidates: promptCandidates,
      prompt,
      prompt_chars: prompt.length,
      prompt_fact_chars: compact.reduce(
        (sum, candidate) => sum + String(candidate.facts || "").length,
        0,
      ),
      compact_candidate_count: compact.length,
    };
  };
  const primaryCandidateCounts = options.voiceIntent ? [maxLlmCandidates] : parseCandidateCountProfile(
    env("EVENT_SEARCH_LLM_LITE_CANDIDATE_COUNTS"),
    maxLlmCandidates,
    adaptiveHalfCandidateProfile(maxLlmCandidates),
  );
  const fallbackCandidateCounts = parseCandidateCountProfile(
    env("EVENT_SEARCH_LLM_FALLBACK_CANDIDATE_COUNTS"),
    maxLlmCandidates,
    adaptiveHalfCandidateProfile(maxLlmCandidates).slice(-1),
  );
  const maxOutputTokens = options.voiceIntent ? (options.semanticPlan ? 8192 : 2048) : envInt(
    "EVENT_SEARCH_LLM_MAX_OUTPUT_TOKENS",
    384,
    128,
    4096,
  );
  const thinkingLevel = options.semanticPlan ? "MEDIUM" : env("EVENT_SEARCH_LLM_THINKING_LEVEL", "MINIMAL");

  const attempts: LlmAttempt[] = [];
  const providerBlockedScopesByModel = new Map<string, Set<string>>();
  let providerKeyCursor = 0;
  const runAttempt = async (
    model: string,
    role: "primary" | "fallback",
    attemptNumber: number,
    timeoutMs: number,
    profile: LlmPromptProfile,
  ): Promise<
    {
      result: ParsedLlmClassification;
      profile: LlmPromptProfile;
    } | null
  > => {
    const reservedTpm = envInt(
      "EVENT_SEARCH_LLM_RESERVED_TPM",
      Math.ceil(profile.prompt_chars / 2) + maxOutputTokens,
      1,
      240000,
    );
    const providerBlockedScopes = providerBlockedScopesByModel.get(model) ||
      new Set<string>();
    providerBlockedScopesByModel.set(model, providerBlockedScopes);
    for (let keyIndex = 0; keyIndex < llmKeys.length; keyIndex += 1) {
      const key = llmKeys[(providerKeyCursor + keyIndex) % llmKeys.length];
      if (providerBlockedScopes.has(key.quota_scope)) continue;
      const startedAt = performance.now();
      if (options.voiceIntent) {
        const admittedTimeout=voiceVerifierAttemptTimeout(timeoutMs,options.deadline);
        if(admittedTimeout===null){
          attempts.push({model,role,attempt:attemptNumber,status:'degraded:verification_budget_exhausted',elapsed_ms:0,timeout_ms:0,prompt_chars:profile.prompt_chars,prompt_fact_chars:profile.prompt_fact_chars,compact_candidate_count:profile.compact_candidate_count});
          return null;
        }
        timeoutMs=admittedTimeout;
      } else if (options.deadline) {
        const remaining = options.deadline - Date.now();
        if (remaining < 300) return null;
        timeoutMs = Math.min(timeoutMs, remaining);
      }
      if (options.reserve_canary_attempt) {
        let reserved = false;
        try {
          reserved = await options.reserve_canary_attempt();
        } catch (_) {
          reserved = false;
        }
        if (!reserved) {
          attempts.push({
            model,
            role,
            attempt: attemptNumber,
            status: "degraded:canary_daily_budget_exhausted",
            elapsed_ms: nowMs() - Math.round(startedAt),
            timeout_ms: timeoutMs,
            prompt_chars: profile.prompt_chars,
            prompt_fact_chars: profile.prompt_fact_chars,
            compact_candidate_count: profile.compact_candidate_count,
          });
          return null;
        }
      }
      try {
        const result = await withSharedGoogleQuotaAttempt({
          backend: options.quota_backend,
          key,
          model: googleLimiterModelId(model),
          reservedTpm,
          consumer: sharedGoogleConsumer("llm"),
          accountName: sharedGoogleAccountName(),
          readEnv: (name) => env(name),
          execute: async (apiKey, lease) => {
            if (options.counters) options.counters.llm_provider_attempts += 1;
            logEvent("event_search_google_provider_sent", {
              provider_kind: "llm",
              provider_role: role,
              provider_attempt: attemptNumber,
              model,
              limiter_model: lease.model,
              request_uid: lease.request_uid,
              api_key_id: lease.api_key_id,
              key_env: lease.limiter_env_name,
              minute_bucket: lease.minute_bucket,
              day_bucket: lease.day_bucket,
            });
            const response = await fetchWithTimeout(
              googleModelActionUrl(model, "generateContent"),
              {
                method: "POST",
                headers: {
                  "Content-Type": "application/json",
                  "X-Server-Timeout": String(
                    Math.max(1, Math.ceil(timeoutMs / 1000)),
                  ),
                  "x-goog-api-key": apiKey,
                },
                body: JSON.stringify({
                  contents: [{ parts: [{ text: profile.prompt }] }],
                  generationConfig: {
                    temperature: options.semanticPlan ? 1 : 0,
                    maxOutputTokens,
                    responseMimeType: "application/json",
                    responseJsonSchema: options.voiceIntent ? (options.semanticPlan ? planVerifierSchema(profile.candidates,options.semanticPlan) : voiceVerifierSchema(profile.candidates)) : LLM_VERIFIER_RESPONSE_SCHEMA,
                    thinkingConfig: {
                      includeThoughts: false,
                      thinkingLevel,
                    },
                  },
                }),
              },
              timeoutMs,
              "llm_provider",
            );
            const payload = (await response.json().catch(() => ({}))) as Record<
              string,
              unknown
            >;
            const usage = googleTokenUsage(payload);
            if (!response.ok) {
              throw new GoogleProviderAttemptError(
                `llm_provider_${response.status}`,
                {
                  provider_status: `http_${response.status}`,
                  error_type: "provider_http",
                  error_code: String(response.status),
                  usage,
                },
              );
            }
            const text = extractGeminiText(payload);
            return {
              value: options.voiceIntent ? (options.semanticPlan ? classifyPlanPayload(parseLlmJson(text),profile.candidates,options.semanticPlan,new Map(profile.candidates.map(c=>[Number(c.event_id),voiceCandidateFacts(candidateDigests.get(Number(c.event_id)),factMaxChars)]))) : classifyVoiceSchemaPayload(parseLlmJson(text), profile.candidates)) : classifyLlmPayload(text, profile.candidates),
              provider_status: "succeeded",
              usage,
            };
          },
        });
        providerKeyCursor = (providerKeyCursor + keyIndex + 1) % llmKeys.length;
        if (result.used && profile.candidates.length < candidates.length) {
          const alreadyClassified = new Set([
            ...result.exact.map(candidateId),
            ...result.possible.map(candidateId),
            ...result.rejected_ids,
          ]);
          result.possible.push(
            ...candidates
              .slice(profile.candidates.length)
              .filter(
                (candidate) => !alreadyClassified.has(candidateId(candidate)),
              )
              .map((candidate) => ({
                ...candidate,
                reason_codes: [
                  ...((candidate.reason_codes as string[]) || []),
                  "llm:possible_unverified_latency_tail",
                ],
              })),
          );
        }
        attempts.push({
          model,
          role,
          attempt: attemptNumber,
          status: result.status,
          elapsed_ms: nowMs() - Math.round(startedAt),
          timeout_ms: timeoutMs,
          prompt_chars: profile.prompt_chars,
          prompt_fact_chars: profile.prompt_fact_chars,
          compact_candidate_count: profile.compact_candidate_count,
          key_env: key.limiter_env_name,
        });
        if (result.used) return { result, profile };
        return null;
      } catch (error) {
        const message = errorMessage(error).slice(0, 80);
        const status = error instanceof GoogleProviderAttemptError
          ? `degraded:provider_${error.error_code || "error"}`
          : error instanceof SharedGoogleQuotaError &&
              error.stage === "reserve"
          ? `degraded:quota_${error.blocked_reason || "unavailable"}`
          : error instanceof SharedGoogleQuotaError
          ? `degraded:shared_limiter_${error.stage}`
          : `degraded:${message}`;
        attempts.push({
          model,
          role,
          attempt: attemptNumber,
          status,
          elapsed_ms: nowMs() - Math.round(startedAt),
          timeout_ms: timeoutMs,
          prompt_chars: profile.prompt_chars,
          prompt_fact_chars: profile.prompt_fact_chars,
          compact_candidate_count: profile.compact_candidate_count,
          key_env: key.limiter_env_name,
        });
        if (shouldTryNextSharedQuotaKey(error)) {
          if (
            error instanceof SharedGoogleQuotaError &&
            error.blocked_reason === "provider_429"
          ) {
            providerBlockedScopes.add(key.quota_scope);
          }
          continue;
        }
        if (error instanceof SharedGoogleQuotaError) return null;
        if (
          error instanceof GoogleProviderAttemptError &&
          shouldTryNextGoogleKey(Number(error.error_code))
        ) {
          if (Number(error.error_code) === 429) {
            providerBlockedScopes.add(key.quota_scope);
          }
          continue;
        }
        if (
          message.includes("llm_provider_timeout") ||
          message.includes("provider_network")
        ) {
          if (role === "primary") return null;
          continue;
        }
        return null;
      }
    }
    return null;
  };

  for (const model of primaryModels) {
    for (
      let attemptNumber = 1;
      attemptNumber <= primaryAttempts;
      attemptNumber += 1
    ) {
      const primaryBudgetStartedAt = performance.now();
      for (
        const [
          profileIndex,
          candidateCount,
        ] of primaryCandidateCounts.entries()
      ) {
        const elapsedBudgetMs = nowMs() - Math.round(primaryBudgetStartedAt);
        const remainingBudgetMs = primaryTotalBudgetMs - elapsedBudgetMs;
        if (remainingBudgetMs < 300) break;
        const profileTimeoutMs = options.voiceIntent ? Math.min(primaryTimeoutMs, remainingBudgetMs) : Math.min(
          primaryTimeoutProfileMs[
            Math.min(profileIndex, primaryTimeoutProfileMs.length - 1)
          ] || primaryTimeoutMs,
          remainingBudgetMs,
        );
        const profile = buildPromptProfile(candidateCount);
        const attemptResult = await runAttempt(
          model,
          "primary",
          attemptNumber,
          profileTimeoutMs,
          profile,
        );
        if (attemptResult?.result.used) {
          return {
            ...attemptResult.result,
            model,
            policy,
            attempts,
            prompt_chars: attemptResult.profile.prompt_chars,
            prompt_fact_chars: attemptResult.profile.prompt_fact_chars,
            compact_candidate_count:
              attemptResult.profile.compact_candidate_count,
          };
        }
        const lastStatus = attempts[attempts.length - 1]?.status || "";
        if (!isRetryableLlmStatus(lastStatus)) break;
      }
      const lastStatus = attempts[attempts.length - 1]?.status || "";
      if (!isRetryableLlmStatus(lastStatus)) break;
      if (attemptNumber < primaryAttempts) {
        await sleepMs(retryBackoffMs * attemptNumber);
      }
    }
  }

  if (shouldTryFallback) {
    for (const model of fallbackModels) {
      for (const candidateCount of fallbackCandidateCounts) {
        const profile = buildPromptProfile(candidateCount);
        const attemptResult = await runAttempt(
          model,
          "fallback",
          1,
          fallbackTimeoutMs,
          profile,
        );
        if (attemptResult?.result.used) {
          return {
            ...attemptResult.result,
            model,
            policy,
            attempts,
            prompt_chars: attemptResult.profile.prompt_chars,
            prompt_fact_chars: attemptResult.profile.prompt_fact_chars,
            compact_candidate_count:
              attemptResult.profile.compact_candidate_count,
          };
        }
      }
    }
  }

  const lastStatus = attempts.length > 0
    ? attempts[attempts.length - 1].status
    : "degraded:no_model_attempts";
  return {
    exact: [],
    possible: candidates,
    rejected_ids: [],
    status: `degraded:all_models_failed:${lastStatus}`.slice(0, 120),
    used: false,
    model: null,
    policy,
    attempts,
    prompt_chars: attempts[attempts.length - 1]?.prompt_chars,
    prompt_fact_chars: attempts[attempts.length - 1]?.prompt_fact_chars,
    compact_candidate_count: attempts[attempts.length - 1]
      ?.compact_candidate_count,
  };
}

async function verifyAssistantWindow(supabaseUrl: string, candidates: Candidate[], intent: AssistantIntent, counters = emptyAttemptCounters(), semanticPlan?: SemanticPlan): Promise<LlmVerifyResult> {
  if(semanticPlan?.scope==='all_events'&&semanticPlan.groups.length===0) {
    const checked=await verifyVoiceWindow(candidates,async batch=>({used:true,status:'ok',exact:batch,possible:[],rejected_ids:[]}));
    return {...checked,verification:{...checked.verification,policy:'voice-typed-constraints-v2',semantic_groups:[],candidate_fact_count:candidates.length},attempts:[],model:null};
  }
  const digests = await fetchCandidateDigests(supabaseUrl, candidates.map(candidateId).filter((id): id is number => id !== null));
  const result = await verifyVoiceWindow(candidates, async (batch, deadline) => {
    const ids = new Set(batch.map(candidateId));
    return await llmVerify(intent.goal, batch, new Map([...digests].filter(([id]) => ids.has(id))), {
      voiceIntent: intent, semanticPlan, deadline, gemma_overflow_allowed: false,
      quota_backend: sharedGoogleQuotaBackend(supabaseUrl), counters,
    });
  }, { batchSize: planVerifierBatchSize(semanticPlan), budgetMs: envInt("EVENT_SEARCH_ASSISTANT_VERIFIER_TOTAL_BUDGET_MS", planVerifierBudgetMs(semanticPlan), 1000, 90000) });
  return {...result, verification: {...result.verification, candidate_fact_count: digests.size, semantic_groups:semanticPlan?.groups||null},
    policy: result.verification.policy, attempts: result.verification.attempts,
    model: result.verification.attempts.at(-1)?.model || null};
}

async function recordSearchRequest(
  supabase: { rpc: (fn: string, args?: Record<string, unknown>) => unknown },
  userId: string,
  payload: Record<string, unknown>,
): Promise<void> {
  try {
    await supabase.rpc("record_event_search_request_internal_v1", {
      p_user_id: userId,
      ...payload,
    });
  } catch (_) {
    // Search telemetry must never break the user-facing search request.
  }
}

type RpcClient = {
  rpc: (
    fn: string,
    args?: Record<string, unknown>,
  ) => PromiseLike<{
    data: unknown;
    error: { code?: string; message?: string } | null;
  }>;
};

async function getSearchRevisionSnapshot(
  service: RpcClient,
  embeddingModel: string,
  embeddingDocKind: string,
): Promise<SearchRevisionSnapshot> {
  const { data, error } = await service.rpc(
    "get_event_search_revision_snapshot_internal_v1",
    {
      p_embedding_model: embeddingModel,
      p_embedding_dim: EMBEDDING_DIM,
      p_embedding_doc_kind: embeddingDocKind,
    },
  );
  if (error) {
    throw new Error(`revision_snapshot:${error.code || error.message}`);
  }
  const row = Array.isArray(data)
    ? (data[0] as Candidate | undefined)
    : (data as Candidate | null);
  if (!row) throw new Error("revision_snapshot:missing");
  return {
    catalog_revision: String(row.catalog_revision || "missing"),
    corpus_revision: String(row.corpus_revision || "missing"),
    search_document_revision: String(row.search_document_revision || "missing"),
    document_count: Math.max(0, Number(row.document_count || 0)),
    embedding_count: Math.max(0, Number(row.embedding_count || 0)),
  };
}

async function isSearchCanaryPrincipal(
  service: RpcClient,
  userId: string,
): Promise<boolean> {
  const { data, error } = await service.rpc(
    "is_event_search_canary_principal_internal_v1",
    { p_user_id: userId },
  );
  return !error && data === true;
}

async function reserveSearchCanaryLlmAttempt(
  service: RpcClient,
  userId: string,
  clientRequestId: string,
): Promise<boolean> {
  const { error } = await service.rpc(
    "reserve_event_search_canary_llm_budget_internal_v1",
    {
      p_user_id: userId,
      p_operation_id: crypto.randomUUID(),
      p_client_request_id: clientRequestId,
      p_attempts: 1,
    },
  );
  return !error;
}

function responseEventIds(body: Record<string, unknown>): number[] {
  // Mirror the UI's visible-items contract exactly: while another exact page
  // exists, fallback discovery cards are not rendered and therefore must not
  // appear in the owner receipt used for response↔DOM acceptance.
  const exact = Array.isArray(body.items) ? body.items : [];
  const lists = body.has_more === true && exact.length > 0
    ? [exact]
    : [exact, body.fallback_items];
  const ids: number[] = [];
  const seen = new Set<number>();
  for (const list of lists) {
    if (!Array.isArray(list)) continue;
    for (const raw of list) {
      const id = raw && typeof raw === "object"
        ? candidateId(raw as Candidate)
        : null;
      if (id === null || seen.has(id)) continue;
      seen.add(id);
      ids.push(id);
      if (ids.length >= 100) return ids;
    }
  }
  return ids;
}

async function recordSearchCanaryReceipt(
  service: RpcClient,
  options: {
    userId: string;
    requestId: string;
    clientRequestId: string;
    requestedMode: ExecutionMode;
    actualMode: ExecutionMode;
    terminalStatus: string;
    revisions: SearchRevisionSnapshot;
    counters: SearchAttemptCounters;
    responseBody?: Record<string, unknown>;
    errorCode?: string | null;
  },
): Promise<string | null> {
  const body = options.responseBody || {};
  const ids = responseEventIds(body);
  const servedListId = String(body.served_list_id || "").trim();
  const { data, error } = await service.rpc(
    "record_event_search_canary_receipt_internal_v1",
    {
      p_user_id: options.userId,
      p_request_id: options.requestId,
      p_client_request_id: options.clientRequestId,
      p_search_contract_version: SEARCH_CONTRACT_VERSION,
      p_requested_execution_mode: options.requestedMode,
      p_actual_execution_mode: options.actualMode,
      p_terminal_status: options.terminalStatus,
      p_catalog_revision: options.revisions.catalog_revision,
      p_corpus_revision: options.revisions.corpus_revision,
      p_search_document_revision: options.revisions.search_document_revision,
      p_embedding_policy_version: EMBEDDING_POLICY_VERSION,
      p_llm_policy_version: LLM_POLICY_VERSION,
      p_cache_policy_version: CACHE_POLICY_VERSION,
      p_embedding_provider_attempts:
        options.counters.embedding_provider_attempts,
      p_llm_provider_attempts: options.counters.llm_provider_attempts,
      p_vector_rpc_attempts: options.counters.vector_rpc_attempts,
      p_result_cache_read_attempts: options.counters.result_cache_read_attempts,
      p_result_cache_hit_count: options.counters.result_cache_hit_count,
      p_result_cache_write_attempts:
        options.counters.result_cache_write_attempts,
      p_query_embedding_cache_read_attempts:
        options.counters.query_embedding_cache_read_attempts,
      p_query_embedding_cache_hit_count:
        options.counters.query_embedding_cache_hit_count,
      p_result_count: ids.length,
      p_response_event_ids: ids,
      p_served_list_id: servedListId || null,
      p_error_code: options.errorCode || null,
    },
  );
  return error || typeof data !== "string" ? null : data;
}

function receiptContractFields(
  requestedMode: ExecutionMode,
  actualMode: ExecutionMode,
  revisions: SearchRevisionSnapshot,
  counters: SearchAttemptCounters,
): Record<string, unknown> {
  return {
    search_contract_version: SEARCH_CONTRACT_VERSION,
    search_backend_revision: SEARCH_BACKEND_REVISION,
    requested_execution_mode: requestedMode,
    actual_execution_mode: actualMode,
    catalog_revision: revisions.catalog_revision,
    corpus_revision: revisions.corpus_revision,
    search_document_revision: revisions.search_document_revision,
    embedding_policy_version: EMBEDDING_POLICY_VERSION,
    llm_policy_version: LLM_POLICY_VERSION,
    cache_policy_version: CACHE_POLICY_VERSION,
    policy_versions: {
      embedding: EMBEDDING_POLICY_VERSION,
      llm: LLM_POLICY_VERSION,
      cache: CACHE_POLICY_VERSION,
    },
    ...counters,
    request_counters: { ...counters },
  };
}

type ProgressStage = {
  stage: string;
  progress: number;
  label: string;
  detail?: string;
  data?: Record<string, unknown>;
};

type ProgressCallback = (stage: ProgressStage) => void | Promise<void>;

type SearchHandlerResult = {
  status: number;
  body: Record<string, unknown>;
};

async function runEventSearch(
  request: Request,
  requestId: string,
  requestStartedAt: number,
  progress?: ProgressCallback,
  assistantIntent?: AssistantIntent,
  assistantPlan?: SemanticPlan,
): Promise<SearchHandlerResult> {
  const recordVoiceAwareRequest = (...args: Parameters<typeof recordSearchRequest>) => {
    const [client, owner, record] = args;
    return recordSearchRequest(client, owner, assistantIntent ? { ...record,
      p_metadata: { ...(record.p_metadata as Record<string, unknown> || {}),
        traffic_class: "voice_preview", exclude_from_product_metrics: true } } : record);
  };

  await progress?.({ stage: "auth", progress: 5, label: "Проверяю вход" });
  const supabaseUrl = env("SUPABASE_URL") ||
    env("PERSONALIZATION_SUPABASE_URL");
  const supabaseAnonKey = env("SUPABASE_ANON_KEY") ||
    env("PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY");
  if (!supabaseUrl || !supabaseAnonKey) {
    return {
      status: 500,
      body: { error: "supabase_env_missing", request_id: requestId },
    };
  }

  const authHeader = request.headers.get("Authorization");
  const accessToken = bearerToken(authHeader);
  if (!accessToken) {
    return {
      status: 401,
      body: { error: "auth_required", request_id: requestId },
    };
  }

  const supabase = createClient(supabaseUrl, supabaseAnonKey, {
    global: { headers: { Authorization: `Bearer ${accessToken}` } },
    auth: { persistSession: false, autoRefreshToken: false },
  });

  const { data: userResult, error: userError } = await supabase.auth.getUser(
    accessToken,
  );
  if (userError || !userResult?.user) {
    return {
      status: 401,
      body: { error: "auth_required", request_id: requestId },
    };
  }
  const userId = userResult.user.id;
  const userHash = shortHash(await sha256Hex(userId));
  // Privileged RPC access is constructed only after the caller JWT has been
  // verified. Browser code never receives this key/client.
  const service = personalizationServiceClient(supabaseUrl);
  if (!service) {
    return {
      status: 500,
      body: { error: "supabase_service_env_missing", request_id: requestId },
    };
  }

  await progress?.({
    stage: "validate",
    progress: 10,
    label: "Проверяю запрос",
  });
  let body: Record<string, unknown> = {};
  try {
    const contentLength = Number(request.headers.get("Content-Length") || "0");
    if (
      Number.isFinite(contentLength) &&
      contentLength > MAX_REQUEST_BODY_BYTES
    ) {
      return {
        status: 413,
        body: { error: "request_too_large", request_id: requestId },
      };
    }
    const rawBody = await request.text();
    if (new TextEncoder().encode(rawBody).byteLength > MAX_REQUEST_BODY_BYTES) {
      return {
        status: 413,
        body: { error: "request_too_large", request_id: requestId },
      };
    }
    body = JSON.parse(rawBody) as Record<string, unknown>;
  } catch (_) {
    return {
      status: 400,
      body: { error: "invalid_json", request_id: requestId },
    };
  }

  const validation = validateSearchQuery(body.query);
  if (!validation.ok) {
    return {
      status: 400,
      body: {
        error: validation.error,
        detail: validation.detail,
        request_id: requestId,
      },
    };
  }

  const query = validation.query;
  const requestedOperationId = String(
    body.client_request_id || request.headers.get("X-Client-Request-Id") || "",
  ).trim();
  if (
    requestedOperationId &&
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu
      .test(
        requestedOperationId,
      )
  ) {
    return {
      status: 400,
      body: { error: "invalid_client_request_id", request_id: requestId },
    };
  }
  const quotaOperationId = requestedOperationId || requestId;
  const queryFacets: QueryFacets = assistantIntent
    ? { weekday_iso: null, weekday_ru: null, time_of_day: (assistantIntent.timeOfDay || null) as QueryFacets["time_of_day"], admission: assistantIntent.freeOnly ? "free" : null }
    : parseQueryFacets(query);
  const limit = clampInt(body.limit, DEFAULT_LIMIT, 1, assistantIntent ? 60 : MAX_LIMIT);
  const offset = clampInt(body.offset, 0, 0, 500);
  const verificationWindow = clampInt(
    body.candidate_window,
    envInt("EVENT_SEARCH_VERIFICATION_WINDOW", 10, 8, 60),
    limit,
    60,
  );
  const includeFallback = body.include_fallback !== false;
  const explicitExecutionMode = body.execution_mode !== undefined;
  if (explicitExecutionMode && !isExecutionMode(body.execution_mode)) {
    return {
      status: 400,
      body: { error: "invalid_execution_mode", request_id: requestId },
    };
  }
  const isCanary = explicitExecutionMode;
  if (isCanary && !(await isSearchCanaryPrincipal(service, userId))) {
    return {
      status: 403,
      body: { error: "search_canary_persona_required", request_id: requestId },
    };
  }
  const llmEnvironmentEnabled = ["1", "true", "yes", "on"].includes(
    env("EVENT_SEARCH_LLM_ENABLED", "").toLowerCase(),
  );
  const requestedExecutionMode: ExecutionMode = explicitExecutionMode
    ? (body.execution_mode as ExecutionMode)
    : body.use_llm_verifier !== false && llmEnvironmentEnabled
    ? "cold_vector_llm"
    : "cold_vector";
  const requestedLlm = requestedExecutionMode === "cold_vector_llm" ||
    requestedExecutionMode === "degraded_vector_fallback";
  const useLlmVerifier = requestedLlm && llmEnvironmentEnabled;
  const bypassResultCache = Boolean(assistantIntent) || (isCanary &&
    requestedExecutionMode !== "cached_vector");
  const bypassQueryEmbeddingCache = isCanary &&
    requestedExecutionMode !== "cached_vector";
  const deterministicLlmFailure = isCanary &&
    requestedExecutionMode === "degraded_vector_fallback";
  const quotaPlanId = isCanary ? "search_canary" : "registered";
  const queryHash = await sha256Hex(query.toLowerCase());
  const queryEmbeddingHash = await queryEmbeddingCacheHash(query);
  const embeddingModel = googleModelId(
    env("EVENT_SEARCH_EMBEDDING_MODEL"),
    "gemini-embedding-2",
  );
  const embeddingDocKind = env("EVENT_SEARCH_EMBEDDING_DOC_KIND", "search_v3");
  const counters = emptyAttemptCounters();
  let revisions: SearchRevisionSnapshot;
  try {
    revisions = await getSearchRevisionSnapshot(
      service,
      embeddingModel,
      embeddingDocKind,
    );
  } catch (error) {
    return {
      status: 503,
      body: {
        error: "search_revision_unavailable",
        detail: errorMessage(error).slice(0, 160),
        request_id: requestId,
      },
    };
  }
  const timings: Record<string, number> = {};
  const dateFrom = assistantIntent?.dateFrom || new Date().toISOString().slice(0, 10);
  const allowLlmFallback = body.allow_llm_fallback !== false;
  const llmPolicySignature = JSON.stringify({
    enabled: useLlmVerifier,
    allow_fallback: allowLlmFallback,
    lite_model: googleModelId(
      env("EVENT_SEARCH_LLM_MODEL"),
      "gemini-3.1-flash-lite",
    ),
    overflow_model: googleModelId(
      env("EVENT_SEARCH_LLM_FALLBACK_MODEL"),
      "gemma-4-26b-a4b-it",
    ),
  });
  const resultCacheKey = await searchResultCacheKey({
    v: 2,
    query_hash: queryEmbeddingHash,
    catalog_revision: revisions.catalog_revision,
    corpus_revision: revisions.corpus_revision,
    search_document_revision: revisions.search_document_revision,
    embedding_model: embeddingModel,
    embedding_doc_kind: embeddingDocKind,
    embedding_policy_version: EMBEDDING_POLICY_VERSION,
    llm_policy_version: LLM_POLICY_VERSION,
    cache_policy_version: CACHE_POLICY_VERSION,
    date_from: dateFrom,
    limit,
    offset,
    verification_window: verificationWindow,
    include_fallback: includeFallback,
    query_facets: queryFacets,
    llm_policy: llmPolicySignature,
  });
  const resultCache: SearchResultCacheOptions = {
    supabaseUrl,
    cacheKey: resultCacheKey.cacheKey,
    queryHash: queryEmbeddingHash,
    embeddingModel,
    embeddingDocKind,
    requestSignature: resultCacheKey.requestSignature,
    ttlSeconds: resultCacheTtlSeconds(),
  };

  await progress?.({
    stage: "result_cache",
    progress: 14,
    label: "Проверяю быстрый кэш",
  });
  const resultCacheStartedAt = performance.now();
  let cachedResult: Record<string, unknown> | null = null;
  if (!bypassResultCache) {
    counters.result_cache_read_attempts += 1;
    cachedResult = await readCachedSearchResult(resultCache);
    if (cachedResult) counters.result_cache_hit_count += 1;
  }
  timings.result_cache_ms = nowMs() - Math.round(resultCacheStartedAt);
  if (cachedResult) {
    const quotaStartedAt = performance.now();
    const { data: quotaRows } = await service.rpc(
      "get_event_search_quota_internal_v1",
      { p_user_id: userId, p_plan_id: quotaPlanId },
    );
    timings.quota_ms = nowMs() - Math.round(quotaStartedAt);
    timings.total_ms = nowMs() - Math.round(requestStartedAt);
    const quotaState = Array.isArray(quotaRows) ? quotaRows[0] : quotaRows;
    const bodyFromCache: Record<string, unknown> = {
      ...cachedResult,
      request_id: requestId,
      client_request_id: quotaOperationId,
      quota: quotaState || cachedResult.quota || null,
      result_cache_status: "hit",
      served_from_cache: true,
      ...receiptContractFields(
        requestedExecutionMode,
        "cached_vector",
        revisions,
        counters,
      ),
      timings_ms: {
        ...((cachedResult.timings_ms as Record<string, unknown> | undefined) ||
          {}),
        ...timings,
      },
    };
    if (isCanary) {
      bodyFromCache.receipt_id = await recordSearchCanaryReceipt(service, {
        userId,
        requestId,
        clientRequestId: quotaOperationId,
        requestedMode: requestedExecutionMode,
        actualMode: "cached_vector",
        terminalStatus: "ok",
        revisions,
        counters,
        responseBody: bodyFromCache,
      });
    }
    logEvent("event_search_result_cache_hit", {
      request_id: requestId,
      user_hash: userHash,
      query_hash: shortHash(queryHash),
      query_cache_hash: shortHash(queryEmbeddingHash),
      cache_key: shortHash(resultCache.cacheKey),
      limit,
      offset,
      verification_window: verificationWindow,
      timings_ms: timings,
    });
    return { status: 200, body: bodyFromCache };
  }

  await progress?.({
    stage: "quota",
    progress: 16,
    label: "Проверяю лимит поиска",
  });
  const quotaStartedAt = performance.now();
  // The protected voice lane is admitted against actual shared provider
  // capacity below, not the legacy small ordinary-search allowance. This is
  // an internal capability; the public POST body cannot select it.
  const { data: quotaRows, error: quotaError } = assistantIntent
    ? { data: [{ plan_id: "voice_shared_capacity", admission: "provider_reservation_required", llm_reserved: false }], error: null }
    : await service.rpc(
    "reserve_event_search_quota_internal_v1",
    {
      p_user_id: userId,
      p_client_request_id: quotaOperationId,
      p_plan_id: quotaPlanId,
      p_use_llm: !isCanary && useLlmVerifier && !deterministicLlmFailure,
    },
  );
  timings.quota_ms = nowMs() - Math.round(quotaStartedAt);
  if (quotaError) {
    await recordVoiceAwareRequest(service, userId, {
      p_request_kind: "vector_search",
      p_query_hash: queryHash,
      p_query_length: query.length,
      p_result_count: 0,
      p_llm_used: useLlmVerifier,
      p_status: "quota_exceeded",
      p_error_code: quotaError.code || "quota_error",
      p_metadata: { query_facets: queryFacets },
    });
    logEvent("event_search_quota_exceeded", {
      request_id: requestId,
      user_hash: userHash,
      query_hash: shortHash(queryHash),
      query_length: query.length,
      query_facets: queryFacets,
      use_llm_verifier: useLlmVerifier,
      error_code: quotaError.code || "quota_error",
      timings_ms: timings,
    });
    const quotaFailureMode: ExecutionMode = requestedLlm
      ? "degraded_vector_fallback"
      : "cold_vector";
    const quotaFailureBody: Record<string, unknown> = {
      error: "quota_exceeded",
      detail: quotaError.message?.includes("hourly")
        ? "Часовой лимит поисков закончился. Окно обновится в начале следующего часа; повтор уже найденного из кэша лимит не тратит."
        : "Лимит поисков на сегодня закончился. Повтор уже найденного из кэша лимит не тратит.",
      request_id: requestId,
      client_request_id: quotaOperationId,
      ...receiptContractFields(
        requestedExecutionMode,
        quotaFailureMode,
        revisions,
        counters,
      ),
    };
    if (isCanary) {
      quotaFailureBody.receipt_id = await recordSearchCanaryReceipt(service, {
        userId,
        requestId,
        clientRequestId: quotaOperationId,
        requestedMode: requestedExecutionMode,
        actualMode: quotaFailureMode,
        terminalStatus: "quota_exceeded",
        revisions,
        counters,
        responseBody: quotaFailureBody,
        errorCode: quotaError.code || "quota_error",
      });
    }
    return {
      status: 429,
      body: quotaFailureBody,
    };
  }

  const quotaState = Array.isArray(quotaRows) ? quotaRows[0] : quotaRows;
  const llmQuotaReserved = Boolean(
    (quotaState as Record<string, unknown> | null)?.llm_reserved,
  );
  // Voice admission allows an attempt, not a fabricated lease: llmVerify still
  // requires real shared google_ai_reserve/mark_sent before every provider send.
  const llmExecutionAllowed = useLlmVerifier && (Boolean(assistantIntent) || isCanary || llmQuotaReserved);
  const llmGemmaOverflowAllowed = llmExecutionAllowed &&
    !deterministicLlmFailure && allowLlmFallback;
  const googleQuotaBackend = sharedGoogleQuotaBackend(supabaseUrl);

  try {
    await progress?.({
      stage: "embedding",
      progress: 28,
      label: "Понимаю смысл запроса",
    });
    const embeddingStartedAt = performance.now();
    const embeddingResult = await embedQuery(
      query,
      googleQuotaBackend,
      {
        supabaseUrl,
        queryHash: queryEmbeddingHash,
        bypassRead: bypassQueryEmbeddingCache,
      },
      counters,
      Boolean(assistantIntent),
    );
    const embedding = embeddingResult.values;
    timings.embedding_ms = nowMs() - Math.round(embeddingStartedAt);

    await progress?.({
      stage: "vector_search",
      progress: 55,
      label: "Ищу похожие события",
    });
    const searchStartedAt = performance.now();
    counters.vector_rpc_attempts += 1;
    const { data: rows, error: searchError } = await service.rpc(
      "search_events_by_embedding_internal_v1",
      {
        p_user_id: userId,
        p_query_embedding: embedding,
        // Pagination is applied after reciprocal-family collapse below. Fetch
        // the complete ranked server window so a lower-ranked sibling cannot
        // reappear merely because it crossed a raw SQL offset boundary.
        p_match_count: 60,
        p_offset_count: 0,
        p_date_from: dateFrom,
        p_date_to: assistantIntent?.dateTo || null,
        p_city_filter: assistantIntent?.localityIds.length===1 ? (()=>{const city=assistantCityName(assistantIntent.localityIds[0]);return city ? city[0].toLocaleUpperCase('ru')+city.slice(1):null;})() : null,
        p_category_filter: null,
        p_embedding_model: embeddingModel,
        p_embedding_dim: EMBEDDING_DIM,
        p_weekday_iso: queryFacets.weekday_iso,
        p_time_of_day_filter: queryFacets.time_of_day,
        p_admission_filter: queryFacets.admission,
        p_embedding_doc_kind: embeddingDocKind,
      },
    );
    timings.search_rpc_ms = nowMs() - Math.round(searchStartedAt);
    if (searchError) throw new Error(`db_search:${searchError.message}`);
    let rankedCandidates = (Array.isArray(rows) ? rows : []).map(normalizeCandidate);
    if (assistantIntent) {
      // Refresh facts from the same primary catalog before strict filtering.
      // SQL returns a bounded ranked window, not the whole universe of events.
      const fresh = await assistantCurrentCards(service, rankedCandidates.map(candidate => String(candidate.event_id)));
      const facts = new Map(fresh.map(candidate => [String(candidate.event_id), candidate]));
      rankedCandidates = rankedCandidates.filter(candidate => {
        const current = facts.get(String(candidate.event_id));
        return current && assistantEligible(current, assistantIntent, true);
      }).map(candidate => ({ ...candidate, ...facts.get(String(candidate.event_id)), base_similarity: candidate.base_similarity }));
    }
    const familyPage = paginateOccurrenceFamilies(
      rankedCandidates,
      offset,
      verificationWindow,
    );
    let items = familyPage.items;
    const retrievedCount = familyPage.retrievedCount;
    const nextOffset = familyPage.nextOffset;
    const hasMore = familyPage.hasMore;

    await progress?.({
      stage: "vector_results",
      progress: 62,
      label: "Нашёл варианты по смыслу",
      data: {
        schema_version: "event-search-results-v1",
        surface: "authorized_event_search",
        algorithm_id: "pgvector_gemini_embedding_2_vector_first_v1",
        request_id: requestId,
        query_hash: queryHash,
        query_facets: queryFacets,
        embedding_cache_status: embeddingResult.cache_status,
        quota: quotaState,
        items: assistantIntent ? [] : items.slice(0, limit),
        fallback_items: [],
        has_more: hasMore,
        next_offset: nextOffset,
        retrieved_count: retrievedCount,
        verification_window: verificationWindow,
        llm_verifier: {
          requested: useLlmVerifier,
          used: false,
          status: llmExecutionAllowed ? "pending" : "disabled",
          model: null,
          policy: null,
          attempts: [],
          gemma_overflow_allowed: llmGemmaOverflowAllowed,
          prompt_chars: null,
          prompt_fact_chars: null,
          compact_candidate_count: null,
          candidate_fact_count: null,
          rejected_count: 0,
          query_interpretation: null,
        },
        timings_ms: {
          ...timings,
          total_ms: nowMs() - Math.round(requestStartedAt),
        },
      },
    });

    let llmResult: LlmVerifyResult = {
      exact: [],
      possible: items,
      rejected_ids: [],
      status: requestedLlm ? "llm_quota_exhausted" : "disabled",
      used: false,
    };
    let llmCandidateFactCount = 0;
    if (deterministicLlmFailure) {
      llmResult = {
        exact: [],
        possible: items,
        rejected_ids: [],
        status: "degraded:deterministic_canary_failure",
        used: false,
        model: null,
        policy: LLM_POLICY_VERSION,
        attempts: [],
      };
      timings.digest_ms = 0;
      timings.llm_ms = 0;
    } else if (assistantIntent) {
      const strictStartedAt = performance.now();
      llmResult = await verifyAssistantWindow(supabaseUrl, items, assistantIntent, counters,assistantPlan);
      llmCandidateFactCount = Number(llmResult.verification?.candidate_fact_count || 0);
      timings.voice_verification_ms = nowMs() - Math.round(strictStartedAt);
    } else if (llmExecutionAllowed) {
      await progress?.({
        stage: "llm_verify",
        progress: 72,
        label: "Проверяю релевантность",
      });
      const digestStartedAt = performance.now();
      const candidateDigests = await fetchCandidateDigests(
        supabaseUrl,
        items.map(candidateId).filter((id): id is number => id !== null),
      );
      llmCandidateFactCount = candidateDigests.size;
      timings.digest_ms = nowMs() - Math.round(digestStartedAt);
      const llmStartedAt = performance.now();
      llmResult = await llmVerify(query, items, candidateDigests, {
        gemma_overflow_allowed: llmGemmaOverflowAllowed,
        quota_backend: googleQuotaBackend,
        counters,
        reserve_canary_attempt: isCanary
          ? () =>
            reserveSearchCanaryLlmAttempt(service, userId, quotaOperationId)
          : undefined,
      });
      timings.llm_ms = nowMs() - Math.round(llmStartedAt);
    } else {
      timings.digest_ms = 0;
      timings.llm_ms = 0;
    }
    items = collapseOccurrenceFamilies(
      assistantIntent ? llmResult.exact : llmResult.used ? llmResult.exact : llmResult.possible,
    ).slice(0, limit);

    let fallbackItems: Candidate[] = llmResult.used && includeFallback
      ? collapseOccurrenceFamilies(
        llmResult.possible,
        new Set(items.map(occurrenceFamilyKey)),
      )
      : [];
    if (
      includeFallback &&
      fallbackItems.length === 0 &&
      retrievedCount < verificationWindow &&
      items.length === 0
    ) {
      await progress?.({
        stage: "fallback",
        progress: 88,
        label: "Подбираю запасные варианты",
      });
      const fallbackStartedAt = performance.now();
      const { data: fallbackRows } = await service.rpc(
        "event_search_fallback_cards_internal_v1",
        {
          p_user_id: userId,
          // Same bounded complete-pool rule as vector pagination: family
          // collapse happens before the result limit is applied.
          p_match_count: 60,
          p_offset_count: 0,
          p_date_from: dateFrom,
        },
      );
      const seenIds = new Set(items.map(candidateId));
      const seenFamilies = new Set(items.map(occurrenceFamilyKey));
      fallbackItems = collapseOccurrenceFamilies(
        (Array.isArray(fallbackRows) ? fallbackRows : [])
          .map((row: Record<string, unknown>, index: number) =>
            normalizeCandidate({ ...row, similarity: 0, distance: 1 }, index)
          )
          .filter((candidate) => !seenIds.has(candidateId(candidate))),
        seenFamilies,
      ).slice(0, limit);
      timings.fallback_rpc_ms = nowMs() - Math.round(fallbackStartedAt);
    }

    await progress?.({
      stage: "finalize",
      progress: 96,
      label: "Собираю карточки",
    });
    const servedListId = crypto.randomUUID();
    const servedHash = await servedListHash(queryHash, items, fallbackItems);
    timings.total_ms = nowMs() - Math.round(requestStartedAt);
    const actualExecutionMode: ExecutionMode = llmResult.used
      ? "cold_vector_llm"
      : requestedLlm
      ? "degraded_vector_fallback"
      : "cold_vector";
    const responseBody: Record<string, unknown> = {
      schema_version: "event-search-results-v1",
      surface: "authorized_event_search",
      algorithm_id: llmResult.used
        ? "pgvector_gemini_embedding_2_llm_high_match_v1"
        : "pgvector_gemini_embedding_2_possible_only_v1",
      request_id: requestId,
      client_request_id: quotaOperationId,
      ...receiptContractFields(
        requestedExecutionMode,
        actualExecutionMode,
        revisions,
        counters,
      ),
      served_list_id: servedListId,
      served_list_hash: servedHash,
      query_hash: queryHash,
      query_facets: queryFacets,
      embedding_cache_status: embeddingResult.cache_status,
      result_cache_status: "miss",
      served_from_cache: false,
      quota: quotaState,
      items,
      ...(assistantIntent ? { semantic_verification: llmResult.verification, verification_unavailable: !llmResult.used } : {}),
      fallback_items: fallbackItems,
      has_more: hasMore,
      next_offset: nextOffset,
      retrieved_count: retrievedCount,
      verification_window: verificationWindow,
      llm_verifier: {
        requested: useLlmVerifier,
        used: llmResult.used,
        status: llmResult.status,
        model: llmResult.model || null,
        policy: llmResult.policy || null,
        attempts: sanitizedLlmAttempts(llmResult.attempts),
        gemma_overflow_allowed: llmGemmaOverflowAllowed,
        prompt_chars: llmResult.prompt_chars ?? null,
        prompt_fact_chars: llmResult.prompt_fact_chars ?? null,
        compact_candidate_count: llmResult.compact_candidate_count ?? null,
        candidate_fact_count: llmCandidateFactCount,
        rejected_count: llmResult.rejected_ids.length,
        query_interpretation: llmResult.query_interpretation || null,
      },
      timings_ms: timings,
    };
    let resultCacheStatus = "skipped";
    const resultCacheWriteAllowed = !assistantIntent && (!isCanary ||
      requestedExecutionMode === "cached_vector" ||
      requestedExecutionMode === "cold_vector");
    if (resultCacheWriteAllowed && (llmResult.used || !useLlmVerifier)) {
      const cacheStoreStartedAt = performance.now();
      counters.result_cache_write_attempts += 1;
      const stored = await storeCachedSearchResult(resultCache, responseBody, {
        source: "event-search-edge",
        request_id: requestId,
        served_list_hash: servedHash,
        result_count: items.length,
        fallback_count: fallbackItems.length,
        llm_used: llmResult.used,
      });
      timings.result_cache_store_ms = nowMs() - Math.round(cacheStoreStartedAt);
      resultCacheStatus = stored ? "stored" : "store_failed";
    }
    timings.total_ms = nowMs() - Math.round(requestStartedAt);
    responseBody.result_cache_status = resultCacheStatus;
    responseBody.timings_ms = timings;
    Object.assign(responseBody, counters, {
      request_counters: { ...counters },
    });

    if (isCanary) {
      responseBody.receipt_id = await recordSearchCanaryReceipt(service, {
        userId,
        requestId,
        clientRequestId: quotaOperationId,
        requestedMode: requestedExecutionMode,
        actualMode: actualExecutionMode,
        terminalStatus: "ok",
        revisions,
        counters,
        responseBody,
      });
    }

    await recordVoiceAwareRequest(service, userId, {
      p_request_kind: llmResult.used ? "llm_rerank" : "vector_search",
      p_query_hash: queryHash,
      p_query_length: query.length,
      p_result_count: items.length,
      p_llm_used: llmResult.used,
      p_status: "ok",
      p_error_code: null,
      p_metadata: {
        request_id: requestId,
        served_list_id: servedListId,
        served_list_hash: servedHash,
        limit,
        offset,
        fallback_count: fallbackItems.length,
        retrieved_count: retrievedCount,
        verification_window: verificationWindow,
        next_offset: nextOffset,
        embedding_model: embeddingModel,
        embedding_key_env: embeddingResult.key_env,
        embedding_cache_status: embeddingResult.cache_status,
        result_cache_status: resultCacheStatus,
        result_cache_key: shortHash(resultCache.cacheKey),
        query_facets: queryFacets,
        llm_status: llmResult.status,
        llm_quota_reserved: llmQuotaReserved,
        llm_gemma_overflow_allowed: llmGemmaOverflowAllowed,
        llm_model: llmResult.model || null,
        llm_policy: llmResult.policy || null,
        llm_attempts: llmResult.attempts || [],
        llm_prompt_chars: llmResult.prompt_chars ?? null,
        llm_prompt_fact_chars: llmResult.prompt_fact_chars ?? null,
        llm_compact_candidate_count: llmResult.compact_candidate_count ?? null,
        llm_candidate_fact_count: llmCandidateFactCount,
        llm_rejected_count: llmResult.rejected_ids.length,
        query_interpretation: llmResult.query_interpretation || null,
        quota: quotaState,
        timings_ms: timings,
      },
    });

    logEvent("event_search_completed", {
      request_id: requestId,
      served_list_id: servedListId,
      served_list_hash: shortHash(servedHash),
      user_hash: userHash,
      query_hash: shortHash(queryHash),
      query_length: query.length,
      embedding_model: embeddingModel,
      embedding_key_env: embeddingResult.key_env,
      embedding_cache_status: embeddingResult.cache_status,
      result_cache_status: resultCacheStatus,
      result_cache_key: shortHash(resultCache.cacheKey),
      query_facets: queryFacets,
      result_count: items.length,
      retrieved_count: retrievedCount,
      fallback_count: fallbackItems.length,
      verification_window: verificationWindow,
      llm_status: llmResult.status,
      llm_used: llmResult.used,
      llm_quota_reserved: llmQuotaReserved,
      llm_gemma_overflow_allowed: llmGemmaOverflowAllowed,
      llm_model: llmResult.model || null,
      llm_policy: llmResult.policy || null,
      llm_attempts: llmResult.attempts || [],
      llm_prompt_chars: llmResult.prompt_chars ?? null,
      llm_prompt_fact_chars: llmResult.prompt_fact_chars ?? null,
      llm_compact_candidate_count: llmResult.compact_candidate_count ?? null,
      llm_candidate_fact_count: llmCandidateFactCount,
      llm_rejected_count: llmResult.rejected_ids.length,
      timings_ms: timings,
    });

    return { status: 200, body: responseBody };
  } catch (error) {
    await recordVoiceAwareRequest(service, userId, {
      p_request_kind: "vector_search",
      p_query_hash: queryHash,
      p_query_length: query.length,
      p_result_count: 0,
      p_llm_used: false,
      p_status: "provider_error",
      p_error_code: errorMessage(error).slice(0, 120),
      p_metadata: { query_facets: queryFacets },
    });
    timings.total_ms = nowMs() - Math.round(requestStartedAt);
    logEvent("event_search_failed", {
      request_id: requestId,
      user_hash: userHash,
      query_hash: shortHash(queryHash),
      query_length: query.length,
      query_facets: queryFacets,
      error_code: errorMessage(error).slice(0, 120),
      timings_ms: timings,
    });
    const failureMode: ExecutionMode = requestedLlm
      ? "degraded_vector_fallback"
      : "cold_vector";
    const failureBody: Record<string, unknown> = {
      error: "search_failed",
      detail: errorMessage(error).slice(0, 500),
      request_id: requestId,
      client_request_id: quotaOperationId,
      ...receiptContractFields(
        requestedExecutionMode,
        failureMode,
        revisions,
        counters,
      ),
      timings_ms: timings,
    };
    if (isCanary) {
      failureBody.receipt_id = await recordSearchCanaryReceipt(service, {
        userId,
        requestId,
        clientRequestId: quotaOperationId,
        requestedMode: requestedExecutionMode,
        actualMode: failureMode,
        terminalStatus: "provider_error",
        revisions,
        counters,
        responseBody: failureBody,
        errorCode: errorMessage(error).slice(0, 120),
      });
    }
    return {
      status: 502,
      body: failureBody,
    };
  }
}

function progressStreamResponse(
  request: Request,
  requestId: string,
  requestStartedAt: number,
): Response {
  const encoder = new TextEncoder();
  const stream = new ReadableStream({
    async start(controller) {
      const send = (event: Record<string, unknown>) => {
        controller.enqueue(encoder.encode(`${JSON.stringify(event)}\n`));
      };
      try {
        send({
          type: "progress",
          request_id: requestId,
          stage: "accepted",
          progress: 2,
          label: "Запрос принят",
          // Some mobile browser/proxy combinations delay tiny streaming chunks.
          // A one-time ignored padding field makes the first NDJSON frame large
          // enough to flush without changing the event contract.
          flush_pad: " ".repeat(2048),
        });
        const result = await runEventSearch(
          request,
          requestId,
          requestStartedAt,
          (stage) => {
            send({ type: "progress", request_id: requestId, ...stage });
          },
        );
        if (result.status >= 400) {
          send({
            type: "error",
            request_id: requestId,
            status: result.status,
            ...result.body,
          });
        } else {
          send({
            type: "result",
            request_id: requestId,
            progress: 100,
            label: "Готово",
            data: result.body,
          });
        }
      } catch (error) {
        send({
          type: "error",
          request_id: requestId,
          status: 500,
          error: "internal_error",
          detail: errorMessage(error).slice(0, 500),
        });
      } finally {
        controller.close();
      }
    },
  });
  return new Response(stream, {
    status: 200,
    headers: {
      ...CORS_HEADERS,
      "Content-Type": "application/x-ndjson; charset=utf-8",
      "Cache-Control": "no-store",
      "X-Accel-Buffering": "no",
    },
  });
}

async function assistantCurrentCards(service: any, ids: string[]): Promise<Record<string, any>[]> {
  if (!ids.length) return [];
  const { data, error } = await service.from("event_search_documents")
    .select("*").in("event_id", ids).eq("active", true).eq("is_public", true).eq("is_searchable", true);
  if (error) assistantReject("catalog_unavailable", 503);
  return (data || []).filter((row: any) =>
    !["cancelled", "postponed", "deleted"].includes(row.lifecycle_status) &&
    !["cancelled", "postponed"].includes(row.availability_status) &&
    row.is_public !== false && row.is_searchable !== false && row.visibility !== "private"
  ).map(normalizeCandidate);
}

export function createAssistantDependencies(repository?: AssistantRepository): AssistantDependencies {
  const supabaseUrl = env("SUPABASE_URL") || env("PERSONALIZATION_SUPABASE_URL");
  let service: any = null;
  const enabled = env("EVENT_SEARCH_ASSISTANT_ENABLED") === "1" && Boolean(env("EVENT_SEARCH_ASSISTANT_POLICY_REF"));
  return {
    enabled,
    structuredPlanEnabled: true,
    adaptivePlanEnabled: true,
    editorialEnabled: true,
    editorialFacts: async (_owner, ids) => {
      const cards=await assistantCurrentCards(service,ids);
      const digests=await fetchCandidateDigests(supabaseUrl,ids.map(Number));
      return cards.map(card=>({...card,search_digest:digests.get(Number(card.event_id))||''}));
    },
    // Fixed deployment origins; never trust an arbitrary supplied upstream.
    allowedOrigins: env("EVENT_SEARCH_ASSISTANT_ORIGINS", "https://kenigevents.ru").split(",").map(value => value.trim()),
    maxAudioBytes: envInt("EVENT_SEARCH_ASSISTANT_AUDIO_MAX_BYTES", 16 * 1024 * 1024, 1024, 64 * 1024 * 1024),
    async authenticate(req) {
      const token = bearerToken(req.headers.get("Authorization"));
      if (!token) assistantReject("auth_required", 401);
      const client = createClient(supabaseUrl, env("SUPABASE_ANON_KEY") || env("PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY"), {
        global: { headers: { Authorization: `Bearer ${token}` } }, auth: { persistSession: false, autoRefreshToken: false },
      });
      const { data, error } = await client.auth.getUser(token);
      if (error || !data?.user || data.user.is_anonymous) assistantReject("eligible_user_required", 401);
      const allowed = env("EVENT_SEARCH_ASSISTANT_PREVIEW_USER_IDS").split(",").map(value => value.trim());
      // This source slice is protected preview only. No public/owner-auth bypass.
      if (!allowed.includes(data.user.id)) assistantReject("assistant_preview_access_required", 403);
      service = personalizationServiceClient(supabaseUrl);
      if (!service) assistantReject("service_unavailable", 503);
      return { owner: data.user.id, repo: repository || assistantRepository(service) };
    },
    generate: assistantGenerator({backend: sharedGoogleQuotaBackend(supabaseUrl), keys: providerKeyPool("LLM"), env: name => env(name)}),
    async search(req, intent, operationId, parentCandidates, semanticPlan) {
      if (parentCandidates) {
        const counters = emptyAttemptCounters();
        const verified = await verifyAssistantWindow(supabaseUrl, parentCandidates, intent, counters,semanticPlan);
        return {items: verified.exact, fallback_items: [], has_more: false,
          semantic_verification: verified.verification, verification_unavailable: !verified.used,
          llm_verifier: {requested:true,used:verified.used,status:verified.status}, request_counters:counters};
      }
      const headers = new Headers(req.headers); headers.delete("Content-Length");
      const internal = new Request(req.url, {method: "POST", headers,
        body: JSON.stringify({query: intent.goal, limit: 60, candidate_window: 60, client_request_id: operationId,
          include_fallback: false, use_llm_verifier: true, allow_llm_fallback: false})});
      const result = await runEventSearch(internal, operationId, performance.now(), undefined, intent,semanticPlan);
      if (result.status >= 400) assistantReject(result.body.error === "quota_exceeded" ? "quota_exceeded" : "search_failed", result.status);
      return result.body;
    },
    currentCards: async (_owner, ids) => assistantCurrentCards(service, ids),
  };
}

async function runAssistant(request: Request): Promise<SearchHandlerResult> {
  return handleAssistant(request, createAssistantDependencies());
}

export async function handleSearchRequest(request: Request): Promise<Response> {
  const requestId = crypto.randomUUID();
  const requestStartedAt = performance.now();
  if (request.method === "OPTIONS") {
    return new Response("ok", { headers: CORS_HEADERS });
  }
  if (/\/event-search\/assistant\/(control|audio|status)$/.test(new URL(request.url).pathname)) {
    const result = await runAssistant(request);
    const response = jsonResponse(result.body, result.status);
    response.headers.set("Cache-Control", "no-store");
    return response;
  }
  // Side-effect-free release probe. It performs no Auth, quota, database,
  // provider or product Search work and exposes only the public contract id.
  if (request.method === "HEAD") {
    return new Response(null, {
      status: 200,
      headers: {
        ...CORS_HEADERS,
        "Cache-Control": "no-store",
        "X-KenigEvents-Search-Contract": SEARCH_CONTRACT_VERSION,
        "X-KenigEvents-Search-Revision": SEARCH_BACKEND_REVISION,
      },
    });
  }
  if (request.method !== "POST") {
    return jsonResponse(
      { error: "method_not_allowed", request_id: requestId },
      405,
    );
  }

  const accept = request.headers.get("Accept") ||
    request.headers.get("accept") || "";
  if (accept.includes("application/x-ndjson")) {
    return progressStreamResponse(request, requestId, requestStartedAt);
  }

  const result = await runEventSearch(request, requestId, requestStartedAt);
  return jsonResponse(result.body, result.status);
}

if (typeof Deno !== "undefined") Deno.serve(handleSearchRequest);
