const PREVIEW_PATH = /^\/preview-[a-z0-9][a-z0-9-]{5,100}\/(?:[a-z0-9_()\-./]*)?$/u;
const PRODUCTION_PATH = /^\/fokus-gruppa\/priglashenie\/$/u;
const CONTROL_CHARS = /[\u0000-\u001f\u007f]/u;

export function validateFocusE2eTarget(value) {
  const raw = String(value || '').trim();
  if (!raw || CONTROL_CHARS.test(raw) || /%(?:0[0-9a-f]|1[0-9a-f]|7f)/iu.test(raw)) {
    throw new Error('target_url_invalid_characters');
  }
  let url;
  try { url = new URL(raw); } catch { throw new Error('target_url_invalid'); }
  if (url.protocol !== 'https:' || url.hostname !== 'kenigevents.ru' || url.port || url.username || url.password) {
    throw new Error('target_url_origin_not_allowed');
  }
  let pathname;
  try { pathname = decodeURIComponent(url.pathname); } catch { throw new Error('target_url_path_invalid'); }
  if (CONTROL_CHARS.test(pathname) || (!PREVIEW_PATH.test(pathname) && !PRODUCTION_PATH.test(pathname))) {
    throw new Error('target_url_path_not_allowed');
  }
  for (const key of url.searchParams.keys()) {
    if (!['install', 'focus_test_reset'].includes(key)) throw new Error('target_url_query_not_allowed');
  }
  if (url.hash && url.hash !== '#invite=focus-group-2026-announcements') {
    throw new Error('target_url_fragment_not_allowed');
  }
  return url;
}

export function targetEvidence(url) {
  const value = url instanceof URL ? url : validateFocusE2eTarget(url);
  return { target_origin: value.origin, target_path: value.pathname };
}

export function sanitizedRunId(value) {
  const cleaned = String(value || '').toLowerCase().replace(/[^a-z0-9-]+/gu, '-').replace(/^-+|-+$/gu, '').slice(0, 48);
  if (!/^[a-z0-9][a-z0-9-]{2,47}$/u.test(cleaned)) throw new Error('run_id_invalid');
  return cleaned;
}

export function recipientForRun(template, runId) {
  const safeRunId = sanitizedRunId(runId);
  const value = String(template || '').replace('{run_id}', safeRunId).trim().toLowerCase();
  if (!/^[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$/u.test(value)) {
    throw new Error('recipient_invalid');
  }
  return { recipient: value, coverage: String(template).includes('{run_id}') ? 'fresh_unique_identity' : 'returning_test_identity' };
}
