// KenigEvents authorized vector search Edge Function.
// Runtime: Supabase Edge Functions / Deno.
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.108.2";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

const DEFAULT_LIMIT = 12;
const MAX_LIMIT = 24;
const EMBEDDING_DIM = 768;

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
  return Deno.env.get(name) || fallback;
}

function googleModelId(value: string, fallback: string): string {
  return String(value || fallback || "")
    .replace(/^models\//, "")
    .trim();
}

function normalizeQuery(value: unknown): string {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 500);
}

type QueryFacets = {
  weekday_iso: number | null;
  weekday_ru: string | null;
  time_of_day: "morning" | "day" | "evening" | "night" | null;
  admission: "free" | "registration_required" | "paid" | null;
};

const WEEKDAY_ALIASES: Array<[RegExp, number, string]> = [
  [/(^|[^а-яa-z0-9])(пн|понедельник[аеу]?|понедельникам)(?=$|[^а-яa-z0-9])/u, 1, "понедельник"],
  [/(^|[^а-яa-z0-9])(вт|вторник[аеу]?|вторникам)(?=$|[^а-яa-z0-9])/u, 2, "вторник"],
  [/(^|[^а-яa-z0-9])(ср|сред[ауые]?|средам)(?=$|[^а-яa-z0-9])/u, 3, "среда"],
  [/(^|[^а-яa-z0-9])(чт|четверг[аеу]?|четвергам)(?=$|[^а-яa-z0-9])/u, 4, "четверг"],
  [/(^|[^а-яa-z0-9])(пт|пятниц[аеуы]?|пятницам)(?=$|[^а-яa-z0-9])/u, 5, "пятница"],
  [/(^|[^а-яa-z0-9])(сб|суббот[аеуы]?|субботам)(?=$|[^а-яa-z0-9])/u, 6, "суббота"],
  [/(^|[^а-яa-z0-9])(вс|воскресень[еяю]|воскресеньям)(?=$|[^а-яa-z0-9])/u, 7, "воскресенье"],
];

function parseQueryFacets(query: string): QueryFacets {
  const normalized = ` ${query.toLowerCase().replace(/ё/g, "е")} `;
  const weekday = WEEKDAY_ALIASES.find(([pattern]) => pattern.test(normalized));
  const timeOfDay = /(^|[^а-яa-z0-9])(утро|утром|утренн[а-яa-z0-9_-]*)(?=$|[^а-яa-z0-9])/u.test(normalized)
    ? "morning"
    : /(^|[^а-яa-z0-9])(день|днем|дневн[а-яa-z0-9_-]*)(?=$|[^а-яa-z0-9])/u.test(normalized)
      ? "day"
      : /(^|[^а-яa-z0-9])(вечер|вечером|вечерн[а-яa-z0-9_-]*)(?=$|[^а-яa-z0-9])/u.test(normalized)
        ? "evening"
        : /(^|[^а-яa-z0-9])(ночь|ночью|ночн[а-яa-z0-9_-]*)(?=$|[^а-яa-z0-9])/u.test(normalized)
          ? "night"
          : null;
  const admission = /(^|[^а-яa-z0-9])(бесплатн[а-яa-z0-9_-]*|свободн[а-яa-z0-9_-]+\s+вход|без\s+оплаты)(?=$|[^а-яa-z0-9])/u.test(
      normalized,
    )
    ? "free"
    : /(^|[^а-яa-z0-9])(регистрац[а-яa-z0-9_-]*|запис[ьи][а-яa-z0-9_-]*|по\s+записи)(?=$|[^а-яa-z0-9])/u.test(normalized)
      ? "registration_required"
      : /(^|[^а-яa-z0-9])(билет[а-яa-z0-9_-]*|платн[а-яa-z0-9_-]*|купить)(?=$|[^а-яa-z0-9])/u.test(normalized)
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

async function embedQuery(query: string): Promise<number[]> {
  const apiKey =
    env("GOOGLE_API_KEY4") || env("GOOGLE_API_KEY") || env("GEMINI_API_KEY");
  if (!apiKey) throw new Error("embedding_api_key_missing");
  const model = googleModelId(
    env("EVENT_SEARCH_EMBEDDING_MODEL"),
    "gemini-embedding-2",
  );
  const text = `task: search result | query: ${query}`;
  const response = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:embedContent`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-goog-api-key": apiKey },
      body: JSON.stringify({
        model: `models/${model}`,
        content: { parts: [{ text }] },
        outputDimensionality: EMBEDDING_DIM,
      }),
    },
  );
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(
      `embedding_provider_${response.status}:${detail.slice(0, 300)}`,
    );
  }
  const payload = await response.json();
  const values = payload?.embedding?.values;
  if (!Array.isArray(values) || values.length !== EMBEDDING_DIM) {
    throw new Error(
      `embedding_bad_dimension:${Array.isArray(values) ? values.length : "missing"}`,
    );
  }
  return values.map((value: unknown) => Number(value));
}


function extractGeminiText(payload: Record<string, unknown>): string {
  const parts = (((payload?.candidates as Candidate[] | undefined)?.[0]?.content as Candidate | undefined)?.parts as Candidate[] | undefined) || [];
  return parts
    .map((part) => typeof part?.text === "string" ? part.text : "")
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
  if (start < 0) throw new Error(`llm_json_object_missing:${unfenced.slice(0, 60)}`);
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
    ordered_event_ids: {
      type: "array",
      items: { type: "integer" },
      description: "Event ids from the provided candidates, ordered by usefulness for the query.",
    },
    rejected_event_ids: {
      type: "array",
      items: { type: "integer" },
      description: "Provided candidate event ids that are clearly irrelevant.",
    },
  },
  required: ["ordered_event_ids", "rejected_event_ids"],
};

type Candidate = Record<string, unknown>;

function candidateId(candidate: Candidate): number | null {
  const raw =
    candidate?.event_id ??
    candidate?.id ??
    (candidate?.display as Candidate | undefined)?.event_id ??
    (candidate?.display as Candidate | undefined)?.id;
  const value = Number(raw);
  return Number.isFinite(value) ? value : null;
}

function normalizeCandidate(
  row: Record<string, unknown>,
  index: number,
): Candidate {
  const snapshot =
    row.card_snapshot && typeof row.card_snapshot === "object"
      ? (row.card_snapshot as Candidate)
      : {};
  const eventId = Number(row.event_id ?? snapshot.event_id ?? snapshot.id);
  const display =
    snapshot.display && typeof snapshot.display === "object"
      ? (snapshot.display as Candidate)
      : {};
  const similarity = Number(row.similarity ?? 0);
  return {
    ...snapshot,
    event_id: eventId,
    id: eventId,
    title: snapshot.title || row.title || display.title || "Событие",
    category: snapshot.category || row.category || "event",
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

async function llmVerify(
  query: string,
  candidates: Candidate[],
): Promise<{ items: Candidate[]; status: string; used: boolean }> {
  const enabled = ["1", "true", "yes", "on"].includes(
    env("EVENT_SEARCH_LLM_ENABLED", "").toLowerCase(),
  );
  if (!enabled || candidates.length <= 1)
    return {
      items: candidates,
      status: enabled ? "skipped_too_few_candidates" : "disabled",
      used: false,
    };
  const apiKey =
    env("GOOGLE_API_KEY4") || env("GOOGLE_API_KEY") || env("GEMINI_API_KEY");
  if (!apiKey)
    return { items: candidates, status: "api_key_missing", used: false };
  const model = googleModelId(
    env("EVENT_SEARCH_LLM_MODEL"),
    "gemma-4-26b-a4b-it",
  );
  const compact = candidates.slice(0, 24).map((candidate, index) => ({
    id: candidateId(candidate),
    rank: index + 1,
    title: candidate.title,
    category: candidate.category,
    tags: candidate.tags,
    date: candidate.date,
    place:
      (candidate.display as Candidate | undefined)?.place ||
      candidate.location_name,
  }));
  const prompt = [
    "Ты проверяешь результаты поиска событий. Нельзя добавлять новые события.",
    "Оставь только релевантные ID из списка и переупорядочь их по полезности для запроса.",
    'Если сомневаешься — оставь исходный порядок. Ответь только валидным JSON без Markdown. Первый символ ответа — { . Формат: {"ordered_event_ids":[123],"rejected_event_ids":[456]}',
    `Запрос пользователя: ${query}`,
    `Кандидаты: ${JSON.stringify(compact, null, 2)}`,
  ].join("\n\n");
  try {
    const response = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-goog-api-key": apiKey,
        },
        body: JSON.stringify({
          contents: [{ parts: [{ text: prompt }] }],
          generationConfig: {
            temperature: 0,
            responseMimeType: "application/json",
            responseSchema: LLM_VERIFIER_RESPONSE_SCHEMA,
          },
        }),
      },
    );
    if (!response.ok)
      return {
        items: candidates,
        status: `provider_${response.status}`,
        used: false,
      };
    const payload = await response.json();
    const text = extractGeminiText(payload);
    const parsed = parseLlmJson(text);
    const allowed = new Map(
      candidates.map((candidate) => [candidateId(candidate), candidate]),
    );
    const rejectedIds = new Set(
      (Array.isArray(parsed.rejected_event_ids) ? parsed.rejected_event_ids : [])
        .map((rawId) => Number(rawId))
        .filter((id) => Number.isFinite(id)),
    );
    const ordered: Candidate[] = [];
    const orderedIds = new Set<number>();
    for (const rawId of Array.isArray(parsed.ordered_event_ids)
      ? parsed.ordered_event_ids
      : []) {
      const id = Number(rawId);
      const candidate = allowed.get(id);
      if (candidate && !orderedIds.has(id) && !rejectedIds.has(id)) {
        ordered.push({
          ...candidate,
          reason_codes: [
            ...((candidate.reason_codes as string[]) || []),
            "llm:verified",
          ],
        });
        orderedIds.add(id);
      }
    }
    for (const candidate of candidates) {
      const id = candidateId(candidate);
      if (id !== null && !orderedIds.has(id) && !rejectedIds.has(id)) {
        ordered.push(candidate);
        orderedIds.add(id);
      }
    }
    return { items: ordered.length ? ordered : candidates, status: "ok", used: true };
  } catch (error) {
    return {
      items: candidates,
      status: `fallback:${String(error?.message || error).slice(0, 80)}`,
      used: false,
    };
  }
}

async function recordSearchRequest(
  supabase: ReturnType<typeof createClient>,
  payload: Record<string, unknown>,
): Promise<void> {
  try {
    await supabase.rpc("record_event_search_request_v1", payload);
  } catch (_) {
    // Search telemetry must never break the user-facing search request.
  }
}

Deno.serve(async (request) => {
  const requestId = crypto.randomUUID();
  const requestStartedAt = performance.now();
  if (request.method === "OPTIONS")
    return new Response("ok", { headers: CORS_HEADERS });
  if (request.method !== "POST")
    return jsonResponse(
      { error: "method_not_allowed", request_id: requestId },
      405,
    );

  const supabaseUrl =
    env("SUPABASE_URL") || env("PERSONALIZATION_SUPABASE_URL");
  const supabaseAnonKey =
    env("SUPABASE_ANON_KEY") || env("PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY");
  if (!supabaseUrl || !supabaseAnonKey)
    return jsonResponse(
      { error: "supabase_env_missing", request_id: requestId },
      500,
    );

  const authHeader = request.headers.get("Authorization");
  const accessToken = bearerToken(authHeader);
  if (!accessToken)
    return jsonResponse({ error: "auth_required", request_id: requestId }, 401);

  const supabase = createClient(supabaseUrl, supabaseAnonKey, {
    global: { headers: { Authorization: `Bearer ${accessToken}` } },
    auth: { persistSession: false, autoRefreshToken: false },
  });

  const { data: userResult, error: userError } =
    await supabase.auth.getUser(accessToken);
  if (userError || !userResult?.user)
    return jsonResponse({ error: "auth_required", request_id: requestId }, 401);
  const userHash = shortHash(await sha256Hex(userResult.user.id));

  let body: Record<string, unknown> = {};
  try {
    body = await request.json();
  } catch (_) {
    return jsonResponse({ error: "invalid_json", request_id: requestId }, 400);
  }

  const query = normalizeQuery(body.query);
  const queryFacets = parseQueryFacets(query);
  const limit = clampInt(body.limit, DEFAULT_LIMIT, 1, MAX_LIMIT);
  const offset = clampInt(body.offset, 0, 0, 500);
  const includeFallback = body.include_fallback !== false;
  const useLlmVerifier =
    body.use_llm_verifier !== false &&
    ["1", "true", "yes", "on"].includes(
      env("EVENT_SEARCH_LLM_ENABLED", "").toLowerCase(),
    );
  const queryHash = await sha256Hex(query.toLowerCase());
  const timings: Record<string, number> = {};

  if (query.length < 3)
    return jsonResponse(
      { error: "query_too_short", request_id: requestId },
      400,
    );

  const quotaStartedAt = performance.now();
  const { data: quotaRows, error: quotaError } = await supabase.rpc(
    "reserve_event_search_quota_v1",
    {
      p_plan_id: "registered",
      p_use_llm: useLlmVerifier,
    },
  );
  timings.quota_ms = nowMs() - Math.round(quotaStartedAt);
  if (quotaError) {
    await recordSearchRequest(supabase, {
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
    return jsonResponse(
      {
        error: "quota_exceeded",
        detail: quotaError.message,
        request_id: requestId,
      },
      429,
    );
  }

  try {
    const embeddingModel = googleModelId(
      env("EVENT_SEARCH_EMBEDDING_MODEL"),
      "gemini-embedding-2",
    );
    const embeddingStartedAt = performance.now();
    const embedding = await embedQuery(query);
    timings.embedding_ms = nowMs() - Math.round(embeddingStartedAt);
    const searchStartedAt = performance.now();
    const { data: rows, error: searchError } = await supabase.rpc(
      "search_events_by_embedding_v1",
      {
        p_query_embedding: embedding,
        p_match_count: limit,
        p_offset_count: offset,
        p_date_from: new Date().toISOString().slice(0, 10),
        p_date_to: null,
        p_city_filter: null,
        p_category_filter: null,
        p_embedding_model: embeddingModel,
        p_embedding_dim: EMBEDDING_DIM,
        p_weekday_iso: queryFacets.weekday_iso,
        p_time_of_day_filter: queryFacets.time_of_day,
        p_admission_filter: queryFacets.admission,
      },
    );
    timings.search_rpc_ms = nowMs() - Math.round(searchStartedAt);
    if (searchError) throw new Error(`db_search:${searchError.message}`);
    let items = (Array.isArray(rows) ? rows : []).map(normalizeCandidate);
    const llmStartedAt = performance.now();
    const llmResult = await llmVerify(query, items);
    timings.llm_ms = nowMs() - Math.round(llmStartedAt);
    items = llmResult.items;

    let fallbackItems: Candidate[] = [];
    if (includeFallback && items.length < limit) {
      const fallbackStartedAt = performance.now();
      const { data: fallbackRows } = await supabase.rpc(
        "event_search_fallback_cards_v1",
        {
          p_match_count: limit,
          p_offset_count: 0,
          p_date_from: new Date().toISOString().slice(0, 10),
        },
      );
      const seen = new Set(items.map(candidateId));
      fallbackItems = (Array.isArray(fallbackRows) ? fallbackRows : [])
        .map((row: Record<string, unknown>, index: number) =>
          normalizeCandidate({ ...row, similarity: 0, distance: 1 }, index),
        )
        .filter((candidate) => !seen.has(candidateId(candidate)))
        .slice(0, limit);
      timings.fallback_rpc_ms = nowMs() - Math.round(fallbackStartedAt);
    }

    const servedListId = crypto.randomUUID();
    const servedHash = await servedListHash(queryHash, items, fallbackItems);
    timings.total_ms = nowMs() - Math.round(requestStartedAt);

    await recordSearchRequest(supabase, {
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
        embedding_model: embeddingModel,
        query_facets: queryFacets,
        llm_status: llmResult.status,
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
      query_facets: queryFacets,
      result_count: items.length,
      fallback_count: fallbackItems.length,
      llm_status: llmResult.status,
      llm_used: llmResult.used,
      timings_ms: timings,
    });

    return jsonResponse({
      schema_version: "event-search-results-v1",
      surface: "authorized_event_search",
      algorithm_id: llmResult.used
        ? "pgvector_gemini_embedding_2_llm_verify_v1"
        : "pgvector_gemini_embedding_2_v1",
      request_id: requestId,
      served_list_id: servedListId,
      served_list_hash: servedHash,
      query_hash: queryHash,
      query_facets: queryFacets,
      quota: Array.isArray(quotaRows) ? quotaRows[0] : quotaRows,
      items,
      fallback_items: fallbackItems,
      has_more: items.length === limit,
      llm_verifier: {
        requested: useLlmVerifier,
        used: llmResult.used,
        status: llmResult.status,
      },
      timings_ms: timings,
    });
  } catch (error) {
    await recordSearchRequest(supabase, {
      p_request_kind: "vector_search",
      p_query_hash: queryHash,
      p_query_length: query.length,
      p_result_count: 0,
      p_llm_used: false,
      p_status: "provider_error",
      p_error_code: String(error?.message || error).slice(0, 120),
      p_metadata: { query_facets: queryFacets },
    });
    timings.total_ms = nowMs() - Math.round(requestStartedAt);
    logEvent("event_search_failed", {
      request_id: requestId,
      user_hash: userHash,
      query_hash: shortHash(queryHash),
      query_length: query.length,
      query_facets: queryFacets,
      error_code: String(error?.message || error).slice(0, 120),
      timings_ms: timings,
    });
    return jsonResponse(
      {
        error: "search_failed",
        detail: String(error?.message || error).slice(0, 500),
        request_id: requestId,
        timings_ms: timings,
      },
      502,
    );
  }
});
