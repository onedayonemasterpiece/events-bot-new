import { mkdir, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';

const forbiddenKey = /^(query|query_text|card_text|card_title|title|email|otp|jwt|authorization|cookie|session|access_token|refresh_token|action_link|target_token|raw_hierarchy|har|trace|video)$/iu;
const forbiddenValuePatterns = [
  /\bBearer\s+[A-Za-z0-9._~+/-]+=*/iu,
  /\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b/u,
  /[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9.-]+\.[a-z]{2,}/iu,
  /\/preview-[a-z0-9][a-z0-9-]{5,}\//iu,
  /\/_review\/[A-Za-z0-9_-]{43}\//u,
];

export function sanitizedTargetPath(value) {
  const path = String(value || '');
  return path
    .replace(/^\/preview-[a-z0-9][a-z0-9-]{5,}\//iu, '/preview-<redacted>/')
    .replace(/^\/_review\/[A-Za-z0-9_-]{43}\//u, '/_review/<redacted>/');
}

function inspect(value, path = '$') {
  if (Array.isArray(value)) {
    value.forEach((item, index) => inspect(item, `${path}[${index}]`));
    return;
  }
  if (value && typeof value === 'object') {
    for (const [key, item] of Object.entries(value)) {
      if (forbiddenKey.test(key)) throw new Error(`search_evidence_forbidden_key:${path}.${key}`);
      inspect(item, `${path}.${key}`);
    }
    return;
  }
  if (typeof value === 'string') {
    for (const pattern of forbiddenValuePatterns) if (pattern.test(value)) throw new Error(`search_evidence_forbidden_value:${path}`);
  }
}

export function assertSanitizedSearchEvidence(value) {
  inspect(value);
  return true;
}

const xml = (value) => String(value ?? '').replace(/[<>&"']/gu, (char) => ({ '<': '&lt;', '>': '&gt;', '&': '&amp;', '"': '&quot;', "'": '&apos;' }[char]));

export async function writeSearchEvidence(directory, result) {
  assertSanitizedSearchEvidence(result);
  const root = resolve(directory);
  await mkdir(root, { recursive: true, mode: 0o700 });
  const summary = {
    schema_version: 'search-live-acceptance-v1',
    status: result.status,
    scenario: 'search.semantic_journey',
    platform: result.platform,
    execution_mode: result.execution_mode,
    target_origin: result.target_origin,
    target_path: result.target_path,
    target_repo_sha: result.target_repo_sha,
    counters: result.counters || {},
    query_cases: (result.query_cases || []).map((item) => ({
      query_id: item.query_id,
      pagination_required: item.pagination_required,
      pages: item.pages?.length || 0,
      cache_hit: item.cache_repeat?.response?.served_from_cache === true,
    })),
    redaction: { status: 'PASS', forbidden_artifacts: ['query_text', 'card_text', 'target_token', 'session', 'jwt', 'har', 'trace', 'video', 'raw_hierarchy'] },
  };
  assertSanitizedSearchEvidence(summary);
  const resultText = `${JSON.stringify(result, null, 2)}\n`;
  const summaryText = `${JSON.stringify(summary, null, 2)}\n`;
  await writeFile(join(root, 'result.json'), resultText, { mode: 0o600 });
  await writeFile(join(root, 'qa-summary.json'), summaryText, { mode: 0o600 });
  const failed = result.status === 'PASS' ? 0 : 1;
  const junit = `<?xml version="1.0" encoding="UTF-8"?>\n<testsuite name="search.semantic_journey" tests="1" failures="${failed}"><testcase classname="search" name="${xml(result.execution_mode)}">${failed ? `<failure type="${xml(result.error_code || 'search_acceptance_failed')}"/>` : ''}</testcase></testsuite>\n`;
  await writeFile(join(root, 'junit.xml'), junit, { mode: 0o600 });
  await writeFile(join(root, '.redaction-ok'), 'PASS\n', { mode: 0o600 });
  return { root, files: ['qa-summary.json', 'result.json', 'junit.xml', '.redaction-ok'] };
}
