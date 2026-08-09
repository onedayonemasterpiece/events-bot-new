const safeUrl = (raw) => {
  try { return new URL(String(raw)); } catch { return null; }
};

export function assertCanonicalCandidateEventDestination({ searchUrl, eventUrl, finalUrl } = {}) {
  const search = safeUrl(searchUrl);
  const expected = safeUrl(eventUrl);
  const final = finalUrl == null ? expected : safeUrl(finalUrl);
  if (!search || !expected || !final) throw new Error('search_card_event_path_invalid');
  if (search.origin !== expected.origin || expected.origin !== final.origin) {
    throw new Error('search_card_route_cross_origin');
  }
  const prefixMatch = search.pathname.match(/^(.*\/)poisk\/$/u);
  if (!prefixMatch) throw new Error('search_card_candidate_prefix_invalid');
  const expectedPrefix = `${prefixMatch[1]}sobytiya/`;
  if (!expected.pathname.startsWith(expectedPrefix)
    || !/^([^/]+)\/$/u.test(expected.pathname.slice(expectedPrefix.length))
    || expected.search || expected.hash) {
    throw new Error('search_card_event_path_invalid');
  }
  if (final.href !== expected.href) throw new Error('search_card_route_changed');
  return Object.freeze({ destination_class: 'event_detail' });
}

/** Stateful sanitizer; protocol request IDs remain inside this closure only. */
export function createSanitizedNavigationResponseTracker() {
  const responses = [];
  const requests = [];
  const seenRequestIds = new Set();
  const responseByRequestId = new Map();
  const terminalBytesByRequestId = new Map();
  const untrackedTerminalRecords = [];
  const appendRequestStart = (request, resourceType, requestId) => {
    const url = safeUrl(request?.url);
    const identity = String(requestId || '');
    if (!url || !identity) return;
    if (!seenRequestIds.has(identity)) {
      seenRequestIds.add(identity);
      requests.push({ origin: url.origin, pathname: url.pathname,
        method: String(request?.method || 'GET').toUpperCase(),
        resource_type: String(resourceType || '').toLowerCase() });
    }
    responseByRequestId.set(identity, {
      item: { origin: url.origin, pathname: url.pathname,
        resource_type: String(resourceType || '').toLowerCase() },
      has_declared_length: false,
      response_seen: false,
      terminal: false,
    });
  };
  const applyTerminalBytes = (record, encodedBytes) => {
    if (!record || record.has_declared_length) return;
    // CDP loadingFinished.encodedDataLength is retained only as a conservative
    // on-wire observed-byte count. This tracker never fetches a response body.
    if (encodedBytes > 0) record.item.encoded_bytes = encodedBytes;
    else delete record.item.encoded_bytes;
    record.terminal = true;
  };
  const appendResponse = (response, resourceType, requestId, terminalEligible) => {
    const url = safeUrl(response?.url);
    const status = Number(response?.status);
    if (!url || !Number.isInteger(status)) return;
    const declared = Number(response?.headers?.['content-length']
      ?? response?.headers?.['Content-Length']);
    const hasDeclaredLength = Number.isSafeInteger(declared) && declared >= 0;
    const partialEncoded = Number(response?.encodedDataLength);
    const encodedBytes = hasDeclaredLength ? declared
      : !terminalEligible && Number.isSafeInteger(partialEncoded) && partialEncoded >= 0 ? partialEncoded : 0;
    const item = { origin: url.origin, pathname: url.pathname, status,
      resource_type: String(resourceType || '').toLowerCase(),
      ...(encodedBytes > 0 ? { encoded_bytes: encodedBytes } : {}) };
    responses.push(item);
    const identity = String(requestId || '');
    if (terminalEligible) {
      const record = responseByRequestId.get(identity) || {};
      Object.assign(record, { item, has_declared_length: hasDeclaredLength,
        response_seen: true, terminal: hasDeclaredLength });
      if (identity) {
        responseByRequestId.set(identity, record);
        if (terminalBytesByRequestId.has(identity)) {
          applyTerminalBytes(record, terminalBytesByRequestId.get(identity));
        }
      } else {
        untrackedTerminalRecords.push(record);
      }
    }
  };
  const consume = (logs) => {
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
    const method = String(value.method || '');
    const params = value.params && typeof value.params === 'object' ? value.params : {};
    if (method === 'Network.requestWillBeSent'
      && params.redirectResponse && typeof params.redirectResponse === 'object') {
      appendResponse(params.redirectResponse, params.type, params.requestId, false);
    }
    if (method === 'Network.requestWillBeSent') {
      appendRequestStart(params.request, params.type, params.requestId);
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
    return tracker;
  };
  const matches = (item, { origin, pathPrefix } = {}) => (
    (!origin || item.origin === origin)
    && (!pathPrefix || item.pathname === pathPrefix || item.pathname.startsWith(`${pathPrefix}/`))
  );
  const tracker = Object.freeze({
    consume,
    responses: () => responses.map((item) => Object.freeze({ ...item })),
    requests: () => requests.map((item) => Object.freeze({ ...item })),
    pendingTerminalCount(filter = {}) {
      let count = 0;
      for (const record of responseByRequestId.values()) {
        if (!record.terminal && matches(record.item, filter)) count += 1;
      }
      for (const record of untrackedTerminalRecords) {
        if (!record.terminal && matches(record.item, filter)) count += 1;
      }
      return count;
    },
  });
  return tracker;
}

/** Reduce one Chrome/Safari protocol batch without retaining request IDs. */
export function extractSanitizedNavigationResponses(logs) {
  const tracker = createSanitizedNavigationResponseTracker();
  tracker.consume(logs);
  return tracker.responses();
}

/** Count unique physical event-search POST dispatches without retaining URL or body data. */
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
        // CDP reuses requestId across redirects, but every requestWillBeSent is
        // a physical dispatch. Its monotonic timestamp identifies the hop and
        // also keeps replayed log entries idempotent across driver log drains.
        // Older protocol fixtures without timestamps keep the legacy initial
        // identity; a redirect still receives a distinct, sanitized key.
        const timestamp = Number(params.timestamp);
        const dispatchIdentity = !identity ? ''
          : Number.isFinite(timestamp) ? `${identity}@${timestamp}`
            : params.redirectResponse ? `${identity}@redirect` : identity;
        if (!dispatchIdentity || !seenRequestIds.has(dispatchIdentity)) {
          if (dispatchIdentity) seenRequestIds.add(dispatchIdentity);
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
  const destination = assertCanonicalCandidateEventDestination({
    searchUrl: before.href, eventUrl: expected.href, finalUrl: final.href,
  });
  if (expected.origin !== before.origin || final.origin !== before.origin) {
    throw new Error('mobile_card_route_cross_origin');
  }
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
    destination_class: destination.destination_class,
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
