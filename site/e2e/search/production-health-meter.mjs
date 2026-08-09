export const SUPABASE_CLIENT_BYTE_TARGET = 48 * 1024;
export const SUPABASE_CLIENT_BYTE_HARD_LIMIT = 96 * 1024;

export const SUPABASE_CLIENT_BYTE_CLASSES = Object.freeze({
  AUTH: 'auth',
  EDGE: 'edge',
  DIRECT_REST: 'direct_rest',
  DIRECT_RPC: 'direct_rpc',
  EXCLUDED: 'excluded',
});

const normalizedOrigins = (values) => new Set((values || []).map((value) => {
  const parsed = new URL(String(value));
  if (!['https:', 'http:'].includes(parsed.protocol) || parsed.username || parsed.password) {
    throw new Error('search_health_meter_origin_invalid');
  }
  return parsed.origin;
}));

export function classifySupabaseClientUrl(value, { supabaseOrigins = [] } = {}) {
  let parsed;
  try {
    parsed = new URL(String(value));
  } catch {
    return SUPABASE_CLIENT_BYTE_CLASSES.EXCLUDED;
  }
  if (!normalizedOrigins(supabaseOrigins).has(parsed.origin)) return SUPABASE_CLIENT_BYTE_CLASSES.EXCLUDED;
  if (parsed.pathname === '/auth/v1' || parsed.pathname.startsWith('/auth/v1/')) {
    return SUPABASE_CLIENT_BYTE_CLASSES.AUTH;
  }
  if (parsed.pathname === '/functions/v1' || parsed.pathname.startsWith('/functions/v1/')) {
    return SUPABASE_CLIENT_BYTE_CLASSES.EDGE;
  }
  if (parsed.pathname === '/rest/v1/rpc' || parsed.pathname.startsWith('/rest/v1/rpc/')) {
    return SUPABASE_CLIENT_BYTE_CLASSES.DIRECT_RPC;
  }
  if (parsed.pathname === '/rest/v1' || parsed.pathname.startsWith('/rest/v1/')) {
    return SUPABASE_CLIENT_BYTE_CLASSES.DIRECT_REST;
  }
  return SUPABASE_CLIENT_BYTE_CLASSES.EXCLUDED;
}

const contentLength = (headers) => {
  if (!headers) return null;
  let raw;
  if (typeof headers.get === 'function') raw = headers.get('content-length');
  else {
    const entry = Object.entries(headers).find(([key]) => key.toLowerCase() === 'content-length');
    raw = entry?.[1];
  }
  if (raw == null || raw === '') return null;
  const value = Number(raw);
  return Number.isSafeInteger(value) && value >= 0 ? value : null;
};

const bodyByteLength = (body) => {
  if (body == null) return 0;
  if (typeof body === 'string') return new TextEncoder().encode(body).byteLength;
  if (body instanceof ArrayBuffer) return body.byteLength;
  if (ArrayBuffer.isView(body)) return body.byteLength;
  if (Number.isSafeInteger(body?.size) && body.size >= 0) return body.size;
  throw new Error('search_health_meter_body_unsupported');
};

export function observedResponseByteLength({ headers, body } = {}) {
  const headerLength = contentLength(headers);
  if (headerLength !== null) return Object.freeze({ bytes: headerLength, source: 'content_length' });
  return Object.freeze({ bytes: bodyByteLength(body), source: 'received_body' });
}

const budgetStatus = (bytes) => {
  if (bytes <= SUPABASE_CLIENT_BYTE_TARGET) return 'within_target';
  if (bytes <= SUPABASE_CLIENT_BYTE_HARD_LIMIT) return 'above_target';
  return 'hard_limit_exceeded';
};

export class SupabaseClientObservedByteMeter {
  #origins;
  #categories = {
    auth: 0,
    edge: 0,
    direct_rest: 0,
    direct_rpc: 0,
  };

  #sources = {
    content_length: 0,
    received_body: 0,
  };

  #excludedRequests = 0;

  constructor({ supabaseOrigins } = {}) {
    this.#origins = [...normalizedOrigins(supabaseOrigins)];
    if (this.#origins.length < 1) throw new Error('search_health_meter_origin_missing');
  }

  recordResponse({ url, headers, body } = {}) {
    const category = classifySupabaseClientUrl(url, { supabaseOrigins: this.#origins });
    if (category === SUPABASE_CLIENT_BYTE_CLASSES.EXCLUDED) {
      this.#excludedRequests += 1;
      return Object.freeze({ included: false, category, bytes: 0, source: 'excluded' });
    }
    const measured = observedResponseByteLength({ headers, body });
    this.#categories[category] += measured.bytes;
    this.#sources[measured.source] += measured.bytes;
    return Object.freeze({ included: true, category, ...measured });
  }

  snapshot() {
    const total = Object.values(this.#categories).reduce((sum, value) => sum + value, 0);
    const status = budgetStatus(total);
    return Object.freeze({
      schema_version: 'supabase_client_observed_bytes_v1',
      measurement_basis: 'client_observed_response_bytes',
      total_bytes: total,
      target_bytes: SUPABASE_CLIENT_BYTE_TARGET,
      hard_limit_bytes: SUPABASE_CLIENT_BYTE_HARD_LIMIT,
      budget_status: status,
      target_met: total <= SUPABASE_CLIENT_BYTE_TARGET,
      cost_guard_passed: total <= SUPABASE_CLIENT_BYTE_HARD_LIMIT,
      hard_limit_exceeded: status === 'hard_limit_exceeded',
      categories: Object.freeze({ ...this.#categories }),
      sources: Object.freeze({ ...this.#sources }),
      excluded_requests: this.#excludedRequests,
    });
  }
}
