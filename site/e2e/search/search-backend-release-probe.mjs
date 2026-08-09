const BACKEND_REVISION_RE = /^sha256:[0-9a-f]{64}$/u;
const CONTRACT_VERSION_RE = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$/u;

const boundedInteger = (value, fallback, min, max) => {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= min && parsed <= max ? parsed : fallback;
};

export function searchBackendProbeUrl(supabaseUrl) {
  const base = new URL(String(supabaseUrl || ''));
  if (base.protocol !== 'https:' || base.username || base.password || base.port
    || base.pathname !== '/' || base.search || base.hash
    || !/^[a-z0-9-]+\.supabase\.co$/u.test(base.hostname)) {
    throw new Error('search_backend_probe_origin_invalid');
  }
  return new URL('/functions/v1/event-search', base).href;
}

export async function waitForActiveSearchBackend({
  supabaseUrl,
  publishableKey,
  expectedRevision,
  fetchImpl = globalThis.fetch,
  sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  attempts = 6,
  delayMs = 5_000,
} = {}) {
  const expected = String(expectedRevision || '').trim();
  if (!BACKEND_REVISION_RE.test(expected)) throw new Error('search_backend_revision_invalid');
  const key = String(publishableKey || '').trim();
  if (!key || /[\r\n]/u.test(key)) throw new Error('search_backend_probe_key_invalid');
  if (typeof fetchImpl !== 'function') throw new Error('search_backend_probe_fetch_missing');
  const count = boundedInteger(attempts, 6, 1, 12);
  const waitMs = boundedInteger(delayMs, 5_000, 0, 10_000);
  const url = searchBackendProbeUrl(supabaseUrl);
  let observed = null;
  let observedContractVersion = null;
  for (let attempt = 1; attempt <= count; attempt += 1) {
    try {
      const response = await fetchImpl(url, {
        method: 'HEAD', redirect: 'error', cache: 'no-store',
        headers: { accept: 'application/json', apikey: key },
      });
      const rawRevision = String(response?.headers?.get?.('x-kenigevents-search-revision') || '').trim();
      const rawContractVersion = String(
        response?.headers?.get?.('x-kenigevents-search-contract') || '',
      ).trim();
      observed = BACKEND_REVISION_RE.test(rawRevision) ? rawRevision : null;
      observedContractVersion = CONTRACT_VERSION_RE.test(rawContractVersion)
        ? rawContractVersion : null;
      if (response?.status === 200 && observed === expected) {
        return Object.freeze({
          schema_version: 'search_backend_release_probe_v2', active: true,
          expected_revision: expected, observed_revision: observed,
          observed_contract_version: observedContractVersion,
          attempts: attempt, product_search_posts: 0, auth_requests: 0,
        });
      }
    } catch { observed = null; observedContractVersion = null; }
    if (attempt < count && waitMs > 0) await sleep(waitMs);
  }
  return Object.freeze({
    schema_version: 'search_backend_release_probe_v2', active: false,
    expected_revision: expected, observed_revision: observed,
    observed_contract_version: observedContractVersion,
    attempts: count, product_search_posts: 0, auth_requests: 0,
  });
}
