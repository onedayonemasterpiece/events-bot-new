const safeUrl = (raw) => {
  try { return new URL(String(raw)); } catch { return null; }
};

/** Reduce Chrome/Safari protocol logs to URL objects held only in memory. */
export function extractSanitizedNavigationResponses(logs) {
  const responses = [];
  const visited = new WeakSet();
  const visit = (value, depth = 0) => {
    if (depth > 10 || value == null) return;
    if (typeof value === 'string') {
      const text = value.trim();
      if (!text.startsWith('{') && !text.startsWith('[')) return;
      try { visit(JSON.parse(text), depth + 1); } catch { /* non-protocol log */ }
      return;
    }
    if (typeof value !== 'object' || visited.has(value)) return;
    visited.add(value);
    if (Array.isArray(value)) { value.forEach((item) => visit(item, depth + 1)); return; }
    if (String(value.method || '') === 'Network.responseReceived') {
      const params = value.params && typeof value.params === 'object' ? value.params : {};
      const response = params.response && typeof params.response === 'object' ? params.response : {};
      const url = safeUrl(response.url);
      const status = Number(response.status);
      if (url && Number.isInteger(status)) {
        responses.push({ origin: url.origin, pathname: url.pathname, status,
          resource_type: String(params.type || '').toLowerCase() });
      }
    }
    Object.values(value).forEach((child) => visit(child, depth + 1));
  };
  visit(logs);
  return responses;
}

export function buildSameOriginNavigationReceipt({ beforeUrl, expectedUrl, finalUrl,
  responses, networkSource } = {}) {
  const before = safeUrl(beforeUrl);
  const expected = safeUrl(expectedUrl);
  const final = safeUrl(finalUrl);
  if (!before || !expected || !final) throw new Error('mobile_card_route_invalid');
  if (expected.origin !== before.origin || final.origin !== before.origin) {
    throw new Error('mobile_card_route_cross_origin');
  }
  if (final.pathname !== expected.pathname) throw new Error('mobile_card_route_changed');
  const matching = (Array.isArray(responses) ? responses : []).filter((item) => (
    item?.origin === expected.origin && item?.pathname === expected.pathname
      && (!item.resource_type || item.resource_type === 'document')
  ));
  if (!matching.some((item) => item.status === 200)) throw new Error('mobile_card_http_200_missing');
  return Object.freeze({
    schema_version: 'mobile-card-open-v1',
    same_origin: true,
    http_status: 200,
    destination_class: 'event_detail',
    network_source: networkSource === 'safariNetwork' ? 'safariNetwork' : 'performance',
    raw_url_retained: false,
  });
}
