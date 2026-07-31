export type GoogleQuotaKeyRow = {
  id: string;
  env_var_name: string;
  priority?: number | null;
};

export type GoogleQuotaBackend = {
  listActiveKeys(envNames: string[]): Promise<GoogleQuotaKeyRow[]>;
  rpc(name: string, payload: Record<string, unknown>): Promise<unknown>;
};

export type GoogleQuotaKeyCandidate = {
  env_name: string;
};

export type GoogleQuotaKey = {
  api_key_id: string;
  configured_env_name: string;
  limiter_env_name: string;
};

export type GoogleTokenUsage = {
  input_tokens: number | null;
  output_tokens: number | null;
  total_tokens: number | null;
};

export type GoogleAttemptResult<T> = {
  value: T;
  provider_status?: string;
  usage?: GoogleTokenUsage | null;
};

export type GoogleQuotaLease = GoogleQuotaKey & {
  request_uid: string;
  attempt_no: number;
  model: string;
  minute_bucket: string;
  day_bucket: string;
  quota_scope: string;
  limiter_contract: string;
};

export const REQUIRED_LIMITER_CONTRACT = "google_ai_project_model_atomic_v1";
const GOOGLE_GENERATIVE_LANGUAGE_BASE_URL =
  "https://generativelanguage.googleapis.com/v1beta/models";

export function googleModelActionUrl(
  model: string,
  action: "embedContent" | "generateContent",
): string {
  const normalizedModel = String(model || "").trim().replace(/^models\//u, "");
  if (!normalizedModel) {
    throw new SharedGoogleQuotaError("metadata", "google_provider_model_missing");
  }
  return `${GOOGLE_GENERATIVE_LANGUAGE_BASE_URL}/${encodeURIComponent(normalizedModel)}:${action}`;
}

type SharedGoogleQuotaErrorStage =
  | "backend"
  | "metadata"
  | "reserve"
  | "key"
  | "mark_sent"
  | "finalize";

export class SharedGoogleQuotaError extends Error {
  readonly stage: SharedGoogleQuotaErrorStage;
  readonly blocked_reason: string | null;
  readonly retry_after_ms: number | null;

  constructor(
    stage: SharedGoogleQuotaErrorStage,
    message: string,
    options: {
      cause?: unknown;
      blocked_reason?: string | null;
      retry_after_ms?: number | null;
    } = {},
  ) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    this.name = "SharedGoogleQuotaError";
    this.stage = stage;
    this.blocked_reason = options.blocked_reason || null;
    this.retry_after_ms = options.retry_after_ms === null ||
        options.retry_after_ms === undefined
      ? null
      : Number.isFinite(Number(options.retry_after_ms))
        ? Number(options.retry_after_ms)
        : null;
  }
}

export class GoogleProviderAttemptError extends Error {
  readonly provider_status: string;
  readonly error_type: string;
  readonly error_code: string | null;
  readonly usage: GoogleTokenUsage | null;

  constructor(
    message: string,
    options: {
      provider_status: string;
      error_type?: string;
      error_code?: string | null;
      usage?: GoogleTokenUsage | null;
      cause?: unknown;
    },
  ) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    this.name = "GoogleProviderAttemptError";
    this.provider_status = options.provider_status;
    this.error_type = options.error_type || "provider";
    this.error_code = options.error_code || null;
    this.usage = options.usage || null;
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function envAliases(name: string): string[] {
  const normalized = String(name || "").trim();
  if (!normalized) return [];
  const aliases = [normalized];
  const match = /^(GOOGLE_API_KEY)_?(\d+)$/u.exec(normalized);
  if (match) {
    const compact = `${match[1]}${match[2]}`;
    const underscored = `${match[1]}_${match[2]}`;
    if (!aliases.includes(compact)) aliases.push(compact);
    if (!aliases.includes(underscored)) aliases.push(underscored);
  }
  return aliases;
}

function isUuid(value: unknown): value is string {
  return typeof value === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(
      value,
    );
}

function asRecord(value: unknown): Record<string, unknown> | null {
  const row = Array.isArray(value) ? value[0] : value;
  return row && typeof row === "object"
    ? (row as Record<string, unknown>)
    : null;
}

function nullableTokenCount(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? Math.trunc(parsed) : null;
}

export function normalizeGoogleTokenUsage(
  usage: GoogleTokenUsage | null | undefined,
): GoogleTokenUsage {
  return {
    input_tokens: nullableTokenCount(usage?.input_tokens),
    output_tokens: nullableTokenCount(usage?.output_tokens),
    total_tokens: nullableTokenCount(usage?.total_tokens),
  };
}

export async function resolveStrictGoogleQuotaPool(
  backend: GoogleQuotaBackend | null,
  candidates: GoogleQuotaKeyCandidate[],
): Promise<GoogleQuotaKey[]> {
  if (!backend) {
    throw new SharedGoogleQuotaError(
      "backend",
      "shared_google_limiter_unavailable",
    );
  }
  const configuredNames = Array.from(
    new Set(candidates.map((item) => String(item.env_name || "").trim()).filter(Boolean)),
  );
  if (configuredNames.length === 0) {
    throw new SharedGoogleQuotaError(
      "metadata",
      "shared_google_key_pool_empty",
    );
  }
  const aliases = Array.from(new Set(configuredNames.flatMap(envAliases)));
  let rows: GoogleQuotaKeyRow[];
  try {
    rows = await backend.listActiveKeys(aliases);
  } catch (error) {
    throw new SharedGoogleQuotaError(
      "metadata",
      `shared_google_key_metadata_unavailable:${errorMessage(error).slice(0, 160)}`,
      { cause: error },
    );
  }
  if (!Array.isArray(rows)) {
    throw new SharedGoogleQuotaError(
      "metadata",
      "shared_google_key_metadata_invalid",
    );
  }

  const byEnv = new Map<string, GoogleQuotaKeyRow>();
  for (const row of rows) {
    const envName = String(row?.env_var_name || "").trim();
    if (!isUuid(row?.id) || !envName || !aliases.includes(envName)) continue;
    byEnv.set(envName, row);
  }

  const pool: GoogleQuotaKey[] = [];
  const seenIds = new Set<string>();
  const missing: string[] = [];
  for (const configuredName of configuredNames) {
    const row = envAliases(configuredName)
      .map((alias) => byEnv.get(alias))
      .find(Boolean);
    if (!row) {
      missing.push(configuredName);
      continue;
    }
    if (seenIds.has(row.id)) continue;
    seenIds.add(row.id);
    pool.push({
      api_key_id: row.id,
      configured_env_name: configuredName,
      limiter_env_name: row.env_var_name,
    });
  }
  if (missing.length > 0 || pool.length !== configuredNames.length) {
    throw new SharedGoogleQuotaError(
      "metadata",
      `shared_google_key_metadata_incomplete:${missing.join(",") || "duplicate_ids"}`,
    );
  }
  return pool;
}

function limiterEnvMatches(expected: string, actual: string): boolean {
  return envAliases(expected).includes(actual) || envAliases(actual).includes(expected);
}

async function finalizeLease(
  backend: GoogleQuotaBackend,
  lease: Pick<GoogleQuotaLease, "request_uid" | "attempt_no">,
  options: {
    durationMs: number;
    providerStatus: string;
    usage?: GoogleTokenUsage | null;
    errorType?: string | null;
    errorCode?: string | null;
    errorMessage?: string | null;
  },
): Promise<void> {
  const usage = normalizeGoogleTokenUsage(options.usage);
  await backend.rpc("google_ai_finalize", {
    p_request_uid: lease.request_uid,
    p_attempt_no: lease.attempt_no,
    p_usage_input_tokens: usage.input_tokens,
    p_usage_output_tokens: usage.output_tokens,
    p_usage_total_tokens: usage.total_tokens,
    p_duration_ms: Math.max(0, Math.trunc(options.durationMs)),
    p_provider_status: options.providerStatus,
    p_error_type: options.errorType || null,
    p_error_code: options.errorCode || null,
    p_error_message: options.errorMessage?.slice(0, 500) || null,
  });
}

async function cleanupUnsentReservation(
  backend: GoogleQuotaBackend,
  requestUid: string,
  attemptNo: number,
  startedAt: number,
  code: string,
): Promise<void> {
  await finalizeLease(
    backend,
    { request_uid: requestUid, attempt_no: attemptNo },
    {
      durationMs: performance.now() - startedAt,
      providerStatus: "not_sent",
      usage: { input_tokens: 0, output_tokens: 0, total_tokens: 0 },
      errorType: "limiter",
      errorCode: code,
      errorMessage: code,
    },
  );
}

export async function withSharedGoogleQuotaAttempt<T>(options: {
  backend: GoogleQuotaBackend | null;
  key: GoogleQuotaKey;
  model: string;
  reservedTpm: number;
  consumer: string;
  accountName: string;
  readEnv: (name: string) => string;
  execute: (apiKey: string, lease: GoogleQuotaLease) => Promise<GoogleAttemptResult<T>>;
}): Promise<T> {
  if (!options.backend) {
    throw new SharedGoogleQuotaError(
      "backend",
      "shared_google_limiter_unavailable",
    );
  }
  if (!options.key || !isUuid(options.key.api_key_id)) {
    throw new SharedGoogleQuotaError(
      "metadata",
      "shared_google_key_metadata_invalid",
    );
  }
  const model = String(options.model || "").trim();
  const reservedTpm = Math.trunc(Number(options.reservedTpm));
  if (!model || !Number.isFinite(reservedTpm) || reservedTpm < 1) {
    throw new SharedGoogleQuotaError(
      "reserve",
      "shared_google_reservation_input_invalid",
    );
  }

  const requestUid = crypto.randomUUID();
  const attemptNo = 1;
  const startedAt = performance.now();
  let reserveData: unknown;
  try {
    reserveData = await options.backend.rpc("google_ai_reserve", {
      p_request_uid: requestUid,
      p_attempt_no: attemptNo,
      p_consumer: options.consumer,
      p_account_name: options.accountName,
      p_model: model,
      p_reserved_tpm: reservedTpm,
      p_candidate_key_ids: [options.key.api_key_id],
    });
  } catch (error) {
    throw new SharedGoogleQuotaError(
      "reserve",
      `shared_google_reserve_unavailable:${errorMessage(error).slice(0, 160)}`,
      { cause: error },
    );
  }
  const reservation = asRecord(reserveData);
  if (!reservation || reservation.ok !== true) {
    const blockedReason = String(reservation?.blocked_reason || "unknown");
    throw new SharedGoogleQuotaError(
      "reserve",
      `shared_google_quota_blocked:${blockedReason}`,
      {
        blocked_reason: blockedReason,
        retry_after_ms: nullableTokenCount(reservation?.retry_after_ms),
      },
    );
  }

  const returnedKeyId = String(reservation.api_key_id || "");
  const returnedEnvName = String(reservation.env_var_name || "").trim();
  const minuteBucket = String(reservation.minute_bucket || "").trim();
  const dayBucket = String(reservation.day_bucket || "").trim();
  const quotaScope = String(reservation.quota_scope || "").trim();
  const limiterContract = String(reservation.limiter_contract || "").trim();
  if (
    returnedKeyId !== options.key.api_key_id ||
    !returnedEnvName ||
    !limiterEnvMatches(options.key.limiter_env_name, returnedEnvName) ||
    !minuteBucket ||
    !dayBucket ||
    !quotaScope ||
    limiterContract !== REQUIRED_LIMITER_CONTRACT
  ) {
    try {
      await cleanupUnsentReservation(
        options.backend,
        requestUid,
        attemptNo,
        startedAt,
        "reservation_metadata_invalid",
      );
    } catch (cleanupError) {
      throw new SharedGoogleQuotaError(
        "finalize",
        `shared_google_reservation_cleanup_failed:${errorMessage(cleanupError).slice(0, 160)}`,
        { cause: cleanupError },
      );
    }
    throw new SharedGoogleQuotaError(
      "metadata",
      limiterContract !== REQUIRED_LIMITER_CONTRACT
        ? `shared_google_limiter_contract_${limiterContract ? "incompatible" : "missing"}`
        : !quotaScope
        ? "shared_google_quota_scope_missing"
        : "shared_google_reservation_metadata_invalid",
    );
  }

  const lease: GoogleQuotaLease = {
    ...options.key,
    request_uid: requestUid,
    attempt_no: attemptNo,
    model,
    minute_bucket: minuteBucket,
    day_bucket: dayBucket,
    quota_scope: quotaScope,
    limiter_contract: limiterContract,
  };
  const apiKey = String(options.readEnv(options.key.configured_env_name) || "").trim();
  if (!apiKey) {
    try {
      await cleanupUnsentReservation(
        options.backend,
        requestUid,
        attemptNo,
        startedAt,
        "leased_key_unavailable",
      );
    } catch (cleanupError) {
      throw new SharedGoogleQuotaError(
        "finalize",
        `shared_google_reservation_cleanup_failed:${errorMessage(cleanupError).slice(0, 160)}`,
        { cause: cleanupError },
      );
    }
    throw new SharedGoogleQuotaError("key", "shared_google_leased_key_unavailable");
  }

  try {
    await options.backend.rpc("google_ai_mark_sent", {
      p_request_uid: requestUid,
      p_attempt_no: attemptNo,
    });
  } catch (error) {
    try {
      await cleanupUnsentReservation(
        options.backend,
        requestUid,
        attemptNo,
        startedAt,
        "mark_sent_failed",
      );
    } catch (cleanupError) {
      throw new SharedGoogleQuotaError(
        "finalize",
        `shared_google_mark_cleanup_failed:${errorMessage(cleanupError).slice(0, 160)}`,
        { cause: cleanupError },
      );
    }
    throw new SharedGoogleQuotaError(
      "mark_sent",
      `shared_google_mark_sent_failed:${errorMessage(error).slice(0, 160)}`,
      { cause: error },
    );
  }

  let execution: GoogleAttemptResult<T> | null = null;
  let providerError: unknown = null;
  try {
    execution = await options.execute(apiKey, lease);
  } catch (error) {
    providerError = error;
  }

  const typedProviderError = providerError instanceof GoogleProviderAttemptError
    ? providerError
    : null;
  const providerStatus = execution?.provider_status ||
    typedProviderError?.provider_status ||
    (providerError ? "failed" : "succeeded");
  try {
    await finalizeLease(options.backend, lease, {
      durationMs: performance.now() - startedAt,
      providerStatus,
      usage: execution?.usage || typedProviderError?.usage,
      errorType: providerError
        ? typedProviderError?.error_type || "provider"
        : null,
      errorCode: providerError
        ? typedProviderError?.error_code || errorMessage(providerError).slice(0, 120)
        : null,
      errorMessage: providerError ? errorMessage(providerError) : null,
    });
  } catch (error) {
    throw new SharedGoogleQuotaError(
      "finalize",
      `shared_google_finalize_failed:${errorMessage(error).slice(0, 160)}`,
      { cause: error },
    );
  }

  if (providerError) throw providerError;
  if (!execution) throw new Error("google_provider_execution_missing");
  return execution.value;
}
