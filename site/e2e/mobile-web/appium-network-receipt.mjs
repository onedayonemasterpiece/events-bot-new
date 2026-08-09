const safeUrl = (raw) => {
  try { return new URL(String(raw)); } catch { return null; }
};

/** Reduce Chrome/Safari protocol logs to URL objects held only in memory. */
export function extractSanitizedNavigationResponses(logs) {
  const responses = [];
  const responseByRequestId = new Map();
  const terminalBytesByRequestId = new Map();
  const visited = new WeakSet();
  const applyTerminalBytes = (record, encodedBytes) => {
    if (!record || record.has_declared_length) return;
    if (encodedBytes > 0) record.item.encoded_bytes = encodedBytes;
    else delete record.item.encoded_bytes;
  };
  const appendResponse = (response, resourceType, requestId, terminalEligible) => {
    const url = safeUrl(response?.url);
    const status = Number(response?.status);
    if (!url || !Number.isInteger(status)) return;
    const declared = Number(response?.headers?.['content-length']
      ?? response?.headers?.['Content-Length']);
    const hasDeclaredLength = Number.isSafeInteger(declared) && declared >= 0;
    const partialEncoded = Number(response?.encodedDataLength);
    const encodedBytes = hasDeclaredLength
      ? declared : Number.isSafeInteger(partialEncoded) && partialEncoded >= 0 ? partialEncoded : 0;
    const item = { origin: url.origin, pathname: url.pathname, status,
      resource_type: String(resourceType || '').toLowerCase(),
      ...(encodedBytes > 0 ? { encoded_bytes: encodedBytes } : {}) };
    responses.push(item);
    const identity = String(requestId || '');
    if (terminalEligible && identity) {
      const record = { item, has_declared_length: hasDeclaredLength };
      responseByRequestId.set(identity, record);
      if (terminalBytesByRequestId.has(identity)) {
        applyTerminalBytes(record, terminalBytesByRequestId.get(identity));
      }
    }
  };
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
    const method = String(value.method || '');
    const params = value.params && typeof value.params === 'object' ? value.params : {};
    if (method === 'Network.requestWillBeSent'
      && params.redirectResponse && typeof params.redirectResponse === 'object') {
      appendResponse(params.redirectResponse, params.type, params.requestId, false);
    }
    if (method === 'Network.responseReceived') {
      const response = params.response && typeof params.response === 'object' ? params.response : {};
      appendResponse(response, params.type, params.requestId, true);
    }
    if (method === 'Network.loadingFinished') {
      const identity = String(params.requestId || '');
      const encodedBytes = Number(params.encodedDataLength);
      if (identity && Number.isSafeInteger(encodedBytes) && encodedBytes >= 0) {
        terminalBytesByRequestId.set(identity, encodedBytes);
        applyTerminalBytes(responseByRequestId.get(identity), encodedBytes);
      }
    }
    Object.values(value).forEach((child) => visit(child, depth + 1));
  };
  visit(logs);
  return responses;
}

/** Count unique physical event-search POSTs without retaining URL or body data. */
export function countEventSearchPostRequests(logs, seenRequestIds = new Set()) {
  let count = 0;
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
    if (String(value.method || '') === 'Network.requestWillBeSent') {
      const params = value.params && typeof value.params === 'object' ? value.params : {};
      const request = params.request && typeof params.request === 'object' ? params.request : {};
      const url = safeUrl(request.url);
      if (String(request.method || '').toUpperCase() === 'POST'
        && url?.pathname === '/functions/v1/event-search') {
        const identity = String(params.requestId || '');
        if (!identity || !seenRequestIds.has(identity)) {
          if (identity) seenRequestIds.add(identity);
          count += 1;
        }
      }
    }
    Object.values(value).forEach((child) => visit(child, depth + 1));
  };
  visit(logs);
  return count;
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
  const observed = Array.isArray(responses) ? responses : [];
  const documentResponses = observed.filter((item) => !item?.resource_type || item.resource_type === 'document');
  if (documentResponses.some((item) => Number(item?.status) >= 300 && Number(item?.status) < 400)) {
    throw new Error('mobile_card_route_redirected');
  }
  if (documentResponses.some((item) => item?.origin && item.origin !== expected.origin)) {
    throw new Error('mobile_card_route_cross_origin');
  }
  const matching = documentResponses.filter((item) => (
    item?.origin === expected.origin && item?.pathname === expected.pathname
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


/** Prove the pinned private Search target itself returned 2xx with no redirect. */
export function buildExactTargetNavigationReceipt({ expectedUrl, finalUrl, responses,
  networkSource } = {}) {
  const expected = safeUrl(expectedUrl);
  const final = safeUrl(finalUrl);
  if (!expected || !final || final.href !== expected.href) throw new Error('search_target_redirected');
  const observed = Array.isArray(responses) ? responses : [];
  const documentResponses = observed.filter((item) => !item?.resource_type || item.resource_type === 'document');
  if (documentResponses.some((item) => Number(item?.status) >= 300 && Number(item?.status) < 400)) {
    throw new Error('search_target_redirected');
  }
  if (documentResponses.some((item) => item?.origin && item.origin !== expected.origin)) {
    throw new Error('search_target_cross_origin');
  }
  const matching = documentResponses.filter((item) => item?.origin === expected.origin
    && item?.pathname === expected.pathname
  );
  if (!matching.some((item) => Number(item.status) >= 200 && Number(item.status) < 300)) {
    throw new Error('search_target_http_invalid');
  }
  return Object.freeze({
    schema_version: 'mobile-target-open-v1', same_origin: true, redirect_count: 0,
    http_status_class: '2xx',
    network_source: networkSource === 'safariNetwork' ? 'safariNetwork' : 'performance',
    raw_url_retained: false,
  });
}
