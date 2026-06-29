// KenigEvents authorized vector search Edge Function.
// Runtime: Supabase Edge Functions / Deno.
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.108.2';

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
};

const DEFAULT_LIMIT = 12;
const MAX_LIMIT = 24;
const EMBEDDING_DIM = 768;

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json; charset=utf-8' },
  });
}

function env(name: string, fallback = ''): string {
  return Deno.env.get(name) || fallback;
}

function googleModelId(value: string, fallback: string): string {
  return String(value || fallback || '').replace(/^models\//, '').trim();
}

function normalizeQuery(value: unknown): string {
  return String(value || '')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 500);
}

function clampInt(value: unknown, fallback: number, min: number, max: number): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, Math.trunc(parsed)));
}

async function sha256Hex(text: string): Promise<string> {
  const data = new TextEncoder().encode(text);
  const digest = await crypto.subtle.digest('SHA-256', data);
  return Array.from(new Uint8Array(digest)).map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

function bearerToken(header: string | null): string | null {
  const match = /^Bearer\s+(.+)$/i.exec(header || '');
  return match ? match[1] : null;
}

async function embedQuery(query: string): Promise<number[]> {
  const apiKey = env('GOOGLE_API_KEY4') || env('GOOGLE_API_KEY') || env('GEMINI_API_KEY');
  if (!apiKey) throw new Error('embedding_api_key_missing');
  const model = googleModelId(env('EVENT_SEARCH_EMBEDDING_MODEL'), 'gemini-embedding-2');
  const text = `task: search result | query: ${query}`;
  const response = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:embedContent`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-goog-api-key': apiKey },
    body: JSON.stringify({
      model: `models/${model}`,
      content: { parts: [{ text }] },
      outputDimensionality: EMBEDDING_DIM,
    }),
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`embedding_provider_${response.status}:${detail.slice(0, 300)}`);
  }
  const payload = await response.json();
  const values = payload?.embedding?.values;
  if (!Array.isArray(values) || values.length !== EMBEDDING_DIM) {
    throw new Error(`embedding_bad_dimension:${Array.isArray(values) ? values.length : 'missing'}`);
  }
  return values.map((value: unknown) => Number(value));
}

type Candidate = Record<string, unknown>;

function candidateId(candidate: Candidate): number | null {
  const raw = candidate?.event_id ?? candidate?.id ?? (candidate?.display as Candidate | undefined)?.event_id ?? (candidate?.display as Candidate | undefined)?.id;
  const value = Number(raw);
  return Number.isFinite(value) ? value : null;
}

function normalizeCandidate(row: Record<string, unknown>, index: number): Candidate {
  const snapshot = (row.card_snapshot && typeof row.card_snapshot === 'object') ? row.card_snapshot as Candidate : {};
  const eventId = Number(row.event_id ?? snapshot.event_id ?? snapshot.id);
  const display = (snapshot.display && typeof snapshot.display === 'object') ? snapshot.display as Candidate : {};
  const similarity = Number(row.similarity ?? 0);
  return {
    ...snapshot,
    event_id: eventId,
    id: eventId,
    title: snapshot.title || row.title || display.title || 'Событие',
    category: snapshot.category || row.category || 'event',
    tags: Array.isArray(snapshot.tags) ? snapshot.tags : (Array.isArray(row.tags) ? row.tags : []),
    base_similarity: similarity,
    static_score: similarity,
    semantic_score: similarity,
    vector_distance: Number(row.distance ?? 0),
    reason_codes: Array.from(new Set([...(Array.isArray(snapshot.reason_codes) ? snapshot.reason_codes : []), 'retrieval:pgvector', `rank:${index + 1}`])),
    display: {
      ...display,
      id: eventId,
      event_id: eventId,
      title: display.title || snapshot.title || row.title || 'Событие',
    },
  };
}

async function llmVerify(query: string, candidates: Candidate[]): Promise<Candidate[]> {
  const enabled = ['1', 'true', 'yes', 'on'].includes(env('EVENT_SEARCH_LLM_ENABLED', '').toLowerCase());
  if (!enabled || candidates.length <= 1) return candidates;
  const apiKey = env('GOOGLE_API_KEY4') || env('GOOGLE_API_KEY') || env('GEMINI_API_KEY');
  if (!apiKey) return candidates;
  const model = googleModelId(env('EVENT_SEARCH_LLM_MODEL'), 'gemma-4-26b-a4b-it');
  const compact = candidates.slice(0, 24).map((candidate, index) => ({
    id: candidateId(candidate),
    rank: index + 1,
    title: candidate.title,
    category: candidate.category,
    tags: candidate.tags,
    date: candidate.date,
    place: (candidate.display as Candidate | undefined)?.place || candidate.location_name,
  }));
  const prompt = [
    'Ты проверяешь результаты поиска событий. Нельзя добавлять новые события.',
    'Оставь только релевантные ID из списка и переупорядочь их по полезности для запроса.',
    'Если сомневаешься — оставь исходный порядок. Ответь только JSON: {"ordered_event_ids":[123],"rejected_event_ids":[456]}',
    `Запрос пользователя: ${query}`,
    `Кандидаты: ${JSON.stringify(compact, null, 2)}`,
  ].join('\n\n');
  try {
    const response = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-goog-api-key': apiKey },
      body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }], generationConfig: { temperature: 0, responseMimeType: 'application/json' } }),
    });
    if (!response.ok) return candidates;
    const payload = await response.json();
    const text = payload?.candidates?.[0]?.content?.parts?.[0]?.text || '';
    const parsed = JSON.parse(text);
    const allowed = new Map(candidates.map((candidate) => [candidateId(candidate), candidate]));
    const ordered: Candidate[] = [];
    for (const rawId of Array.isArray(parsed.ordered_event_ids) ? parsed.ordered_event_ids : []) {
      const id = Number(rawId);
      const candidate = allowed.get(id);
      if (candidate && !ordered.includes(candidate)) ordered.push({ ...candidate, reason_codes: [...(candidate.reason_codes as string[] || []), 'llm:verified'] });
    }
    for (const candidate of candidates) {
      if (!ordered.includes(candidate)) ordered.push(candidate);
    }
    return ordered;
  } catch (_) {
    return candidates;
  }
}

async function recordSearchRequest(
  supabase: ReturnType<typeof createClient>,
  payload: Record<string, unknown>,
): Promise<void> {
  try {
    await supabase.rpc('record_event_search_request_v1', payload);
  } catch (_) {
    // Search telemetry must never break the user-facing search request.
  }
}

Deno.serve(async (request) => {
  if (request.method === 'OPTIONS') return new Response('ok', { headers: CORS_HEADERS });
  if (request.method !== 'POST') return jsonResponse({ error: 'method_not_allowed' }, 405);

  const supabaseUrl = env('SUPABASE_URL') || env('PERSONALIZATION_SUPABASE_URL');
  const supabaseAnonKey = env('SUPABASE_ANON_KEY') || env('PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY');
  if (!supabaseUrl || !supabaseAnonKey) return jsonResponse({ error: 'supabase_env_missing' }, 500);

  const authHeader = request.headers.get('Authorization');
  const accessToken = bearerToken(authHeader);
  if (!accessToken) return jsonResponse({ error: 'auth_required' }, 401);

  const supabase = createClient(supabaseUrl, supabaseAnonKey, {
    global: { headers: { Authorization: `Bearer ${accessToken}` } },
    auth: { persistSession: false, autoRefreshToken: false },
  });

  const { data: userResult, error: userError } = await supabase.auth.getUser(accessToken);
  if (userError || !userResult?.user) return jsonResponse({ error: 'auth_required' }, 401);

  let body: Record<string, unknown> = {};
  try { body = await request.json(); } catch (_) { return jsonResponse({ error: 'invalid_json' }, 400); }

  const query = normalizeQuery(body.query);
  const limit = clampInt(body.limit, DEFAULT_LIMIT, 1, MAX_LIMIT);
  const offset = clampInt(body.offset, 0, 0, 500);
  const includeFallback = body.include_fallback !== false;
  const useLlmVerifier = body.use_llm_verifier !== false && ['1', 'true', 'yes', 'on'].includes(env('EVENT_SEARCH_LLM_ENABLED', '').toLowerCase());
  const queryHash = await sha256Hex(query.toLowerCase());

  if (query.length < 3) return jsonResponse({ error: 'query_too_short' }, 400);

  const { data: quotaRows, error: quotaError } = await supabase.rpc('reserve_event_search_quota_v1', {
    p_plan_id: 'registered',
    p_use_llm: useLlmVerifier,
  });
  if (quotaError) {
    await recordSearchRequest(supabase, {
      p_request_kind: 'vector_search', p_query_hash: queryHash, p_query_length: query.length,
      p_result_count: 0, p_llm_used: useLlmVerifier, p_status: 'quota_exceeded', p_error_code: quotaError.code || 'quota_error', p_metadata: {},
    });
    return jsonResponse({ error: 'quota_exceeded', detail: quotaError.message }, 429);
  }

  try {
    const embeddingModel = googleModelId(env('EVENT_SEARCH_EMBEDDING_MODEL'), 'gemini-embedding-2');
    const embedding = await embedQuery(query);
    const { data: rows, error: searchError } = await supabase.rpc('search_events_by_embedding_v1', {
      p_query_embedding: embedding,
      p_match_count: limit,
      p_offset_count: offset,
      p_date_from: new Date().toISOString().slice(0, 10),
      p_date_to: null,
      p_city_filter: null,
      p_category_filter: null,
      p_embedding_model: embeddingModel,
      p_embedding_dim: EMBEDDING_DIM,
    });
    if (searchError) throw new Error(`db_search:${searchError.message}`);
    let items = (Array.isArray(rows) ? rows : []).map(normalizeCandidate);
    items = await llmVerify(query, items);

    let fallbackItems: Candidate[] = [];
    if (includeFallback && items.length < limit) {
      const { data: fallbackRows } = await supabase.rpc('event_search_fallback_cards_v1', {
        p_match_count: limit,
        p_offset_count: 0,
        p_date_from: new Date().toISOString().slice(0, 10),
      });
      const seen = new Set(items.map(candidateId));
      fallbackItems = (Array.isArray(fallbackRows) ? fallbackRows : [])
        .map((row: Record<string, unknown>, index: number) => normalizeCandidate({ ...row, similarity: 0, distance: 1 }, index))
        .filter((candidate) => !seen.has(candidateId(candidate)))
        .slice(0, limit);
    }

    await recordSearchRequest(supabase, {
      p_request_kind: useLlmVerifier ? 'llm_rerank' : 'vector_search',
      p_query_hash: queryHash,
      p_query_length: query.length,
      p_result_count: items.length,
      p_llm_used: useLlmVerifier,
      p_status: 'ok',
      p_error_code: null,
      p_metadata: { limit, offset, fallback_count: fallbackItems.length },
    });

    return jsonResponse({
      schema_version: 'event-search-results-v1',
      surface: 'authorized_event_search',
      algorithm_id: useLlmVerifier ? 'pgvector_gemini_embedding_2_llm_verify_v1' : 'pgvector_gemini_embedding_2_v1',
      query_hash: queryHash,
      quota: Array.isArray(quotaRows) ? quotaRows[0] : quotaRows,
      items,
      fallback_items: fallbackItems,
      has_more: items.length === limit,
    });
  } catch (error) {
    await recordSearchRequest(supabase, {
      p_request_kind: 'vector_search', p_query_hash: queryHash, p_query_length: query.length,
      p_result_count: 0, p_llm_used: false, p_status: 'provider_error', p_error_code: String(error?.message || error).slice(0, 120), p_metadata: {},
    });
    return jsonResponse({ error: 'search_failed', detail: String(error?.message || error).slice(0, 500) }, 502);
  }
});
