export function createSanitizedNetworkRecorder(page, { directHost = '', relayHost = '' } = {}) {
  const starts = new WeakMap();
  const entries = [];
  let sequence = 0;
  const classify = (hostname) => {
    if (hostname === directHost) return 'supabase_direct';
    if (hostname === relayHost) return 'relay';
    if (hostname === 'kenigevents.ru') return 'kenigevents';
    return 'other';
  };
  page.on('request', (request) => starts.set(request, Date.now()));
  page.on('response', (response) => {
    const request = response.request();
    const url = new URL(response.url());
    entries.push({
      sequence: ++sequence,
      method: request.method(),
      host_class: classify(url.hostname),
      path: url.pathname,
      status: response.status(),
      duration_ms: Math.max(0, Date.now() - (starts.get(request) || Date.now())),
      failure_class: null,
    });
  });
  page.on('requestfailed', (request) => {
    const url = new URL(request.url());
    entries.push({
      sequence: ++sequence,
      method: request.method(),
      host_class: classify(url.hostname),
      path: url.pathname,
      status: null,
      duration_ms: Math.max(0, Date.now() - (starts.get(request) || Date.now())),
      failure_class: sanitizedFailureClass({ errorText: request.failure()?.errorText }),
    });
  });
  return {
    entries,
    count(method, suffix) {
      return entries.filter((item) => item.method === method && item.path.endsWith(suffix) && item.failure_class == null).length;
    },
    statuses(suffix) { return entries.filter((item) => item.path.endsWith(suffix)).map((item) => item.status); },
  };
}
import { sanitizedFailureClass } from './runtime-diagnostics.mjs';
