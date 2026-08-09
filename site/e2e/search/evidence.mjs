import { createHash } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';

const forbiddenKey = /^(query|query_text|card_text|card_title|title|email|otp|jwt|authorization|cookie|session|access_token|refresh_token|action_link|target_token|url|href|raw_error|error_message|stack|raw_hierarchy|har|trace|video)$/iu;
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

const safeCount = (value) => Number.isSafeInteger(Number(value)) && Number(value) >= 0 ? Number(value) : 0;
const closedText = (value, pattern, fallback = null) => {
  const text = String(value ?? '');
  return pattern.test(text) ? text : fallback;
};

/** Build the only record shape permitted for scheduled production health. */
export function productionHealthEvidenceRecord(input = {}) {
  const target = input.target_immutable || {};
  const immutable = target.immutable_identity || {};
  const journey = input.journey || {};
  const meter = input.meter || {};
  const preflight = input.preflight || {};
  const auth = input.auth || {};
  const record = {
    schema_version: 'search_production_health_evidence_v1',
    platform: closedText(input.platform, /^(?:browser|android|ios)$/u, 'browser'),
    product_health: closedText(input.product_health, /^(?:HEALTHY|BROKEN|UNCONFIRMED)$/u, 'UNCONFIRMED'),
    execution_status: closedText(input.execution_status, /^(?:PASS|FAILED|BLOCKED)$/u, 'FAILED'),
    failure_class: input.failure_class == null
      ? null : closedText(input.failure_class, /^[A-Z][A-Z0-9_]{2,63}$/u, 'EVIDENCE_REDACTION_FAILED'),
    workflow_run_id: closedText(input.workflow_run_id, /^[1-9][0-9]{0,19}$/u),
    tested_at: closedText(input.tested_at, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/u),
    target: {
      source: closedText(target.source, /^current_accepted_pointer$/u, 'current_accepted_pointer'),
      accepted_release_id: closedText(target.accepted_release_id, /^[A-Za-z0-9][A-Za-z0-9._:-]{0,191}$/u),
      target_repo_sha: closedText(target.target_repo_sha, /^[0-9a-f]{40}$/u),
      target_url_sha256: closedText(target.target_url_sha256, /^[0-9a-f]{64}$/u),
      immutable_identity: {
        build_id: closedText(immutable.build_id, /^[A-Za-z0-9][A-Za-z0-9._:-]{0,191}$/u),
        run_id: closedText(immutable.run_id, /^[A-Za-z0-9][A-Za-z0-9._:-]{0,191}$/u),
        repo_sha: closedText(immutable.repo_sha, /^[0-9a-f]{40}$/u),
        snapshot_id: closedText(immutable.snapshot_id, /^[A-Za-z0-9][A-Za-z0-9._:-]{0,191}$/u),
        result_sha256: closedText(immutable.result_sha256, /^[0-9a-f]{64}$/u),
        manifest_sha256: closedText(immutable.manifest_sha256, /^[0-9a-f]{64}$/u),
        token_sha256: closedText(immutable.token_sha256, /^[0-9a-f]{64}$/u),
        input_fingerprint: closedText(immutable.input_fingerprint, /^[0-9a-f]{64}$/u),
      },
      target_superseded: input.target_superseded === true,
      retry_allowed: false,
    },
    preflight: {
      side_effect_free: preflight.side_effect_free === true,
      browser_ready: preflight.browser_ready === true,
      transport_ready: preflight.transport_ready === true,
      viewport_ready: preflight.viewport_ready === true,
      auth_requests: safeCount(preflight.auth_requests), search_posts: safeCount(preflight.search_posts),
      otp_requests: safeCount(preflight.otp_requests), supabase_requests: safeCount(preflight.supabase_requests),
    },
    auth: {
      no_mail: auth.real_mail_fallback === 'forbidden',
      get_user_verified: auth.get_user_verified === true,
      protected_probe_verified: auth.protected_probe_verified === true,
      protected_probe_request_count: safeCount(auth.protected_probe_request_count),
      product_otp_issue_count: safeCount(auth.product_otp_issue_count),
      external_mail_send_count: safeCount(auth.external_mail_send_count),
      external_mail_receipt_count: safeCount(auth.external_mail_receipt_count),
      cleanup_status: closedText(input.cleanup_status, /^(?:PASS|PENDING|FAIL)$/u, 'PENDING'),
    },
    search: {
      expected_backend_revision: closedText(input.expected_search_backend_revision, /^sha256:[0-9a-f]{64}$/u),
      ui_submission_count: safeCount(journey.search_post_count),
      physical_post_count: safeCount(journey.physical_search_post_count ?? journey.search_post_count),
      vector_only: journey.request_contract?.use_llm_verifier === false,
      limit: safeCount(journey.request_contract?.limit),
      explicit_execution_mode: journey.request_contract?.explicit_execution_mode === true,
      cache_state: closedText(journey.cache_state, /^[a-z0-9][a-z0-9_-]{0,31}$/u),
      provider_attempts: {
        embedding: safeCount(journey.provider_attempts?.embedding),
        vector: safeCount(journey.provider_attempts?.vector),
        llm: safeCount(journey.provider_attempts?.llm),
      },
      response: {
        request_id: closedText(journey.response_telemetry?.request_id, /^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$/u),
        http_status: safeCount(journey.response_telemetry?.http_status),
        route: closedText(journey.response_telemetry?.route, /^(?:direct|relay)$/u, 'direct'),
        search_contract_version: closedText(journey.response_telemetry?.search_contract_version, /^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$/u),
        search_backend_revision: closedText(journey.response_telemetry?.search_backend_revision, /^sha256:[0-9a-f]{64}$/u),
        catalog_revision: closedText(journey.response_telemetry?.catalog_revision, /^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$/u),
        corpus_revision: closedText(journey.response_telemetry?.corpus_revision, /^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$/u),
        search_document_revision: closedText(journey.response_telemetry?.search_document_revision, /^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$/u),
      },
      response_id_count: safeCount(journey.response_ids?.length),
      rendered_id_count: safeCount(journey.rendered_ids?.length),
      response_rendered_ids_match: Array.isArray(journey.response_ids)
        && Array.isArray(journey.rendered_ids)
        && JSON.stringify(journey.response_ids.map(String)) === JSON.stringify(journey.rendered_ids.map(String)),
      card_count: safeCount(journey.card_count), terminal_ui: journey.terminal_ui === true,
      latency_ms: safeCount(journey.latency_ms),
      real_scroll: {
        performed: journey.real_scroll?.performed === true,
        card_visible_after: journey.real_scroll?.card_visible_after === true,
        gesture_count: safeCount(journey.real_scroll?.gesture_count),
      },
      event_route: {
        same_origin: journey.event_route?.same_origin === true,
        http_status: safeCount(journey.event_route?.http_status),
        destination_class: closedText(journey.event_route?.destination_class, /^[a-z][a-z0-9_]{2,31}$/u, 'event_detail'),
      },
      forbidden_activity: {
        llm_calls: safeCount(journey.forbidden_activity?.llm_calls),
        pagination_requests: safeCount(journey.forbidden_activity?.pagination_requests),
        receipt_rpc_calls: safeCount(journey.forbidden_activity?.receipt_rpc_calls),
        storage_image_requests: safeCount(journey.forbidden_activity?.storage_image_requests),
      },
      console_error_count: safeCount(journey.diagnostics?.console_errors),
      network_error_count: safeCount(journey.diagnostics?.failed_requests) + safeCount(journey.diagnostics?.error_responses),
    },
    supabase_observed_bytes: {
      measurement_basis: 'client_observed_response_bytes',
      total_bytes: safeCount(meter.total_bytes), target_bytes: safeCount(meter.target_bytes),
      hard_limit_bytes: safeCount(meter.hard_limit_bytes),
      budget_status: closedText(meter.budget_status, /^(?:within_target|above_target|hard_limit_exceeded)$/u, 'hard_limit_exceeded'),
      target_met: meter.target_met === true, cost_guard_passed: meter.cost_guard_passed === true,
      categories: {
        auth: safeCount(meter.categories?.auth), edge: safeCount(meter.categories?.edge),
        direct_rest: safeCount(meter.categories?.direct_rest), direct_rpc: safeCount(meter.categories?.direct_rpc),
      },
    },
    redaction: {
      status: 'PASS',
      omitted: ['query_text', 'card_text', 'secret_target', 'session', 'raw_error', 'raw_network'],
    },
  };
  assertSanitizedSearchEvidence(record);
  return record;
}

export async function writeProductionHealthEvidence(directory, input) {
  const record = productionHealthEvidenceRecord(input);
  const root = resolve(directory);
  await mkdir(root, { recursive: true, mode: 0o700 });
  await writeFile(join(root, 'result.json'), `${JSON.stringify(record, null, 2)}\n`, { mode: 0o600 });
  const runtimeFingerprint = createHash('sha256').update(JSON.stringify({
    repo_sha: record.target.target_repo_sha,
    search_contract_version: record.search.response.search_contract_version,
    search_backend_revision: record.search.response.search_backend_revision,
    expected_backend_revision: record.search.expected_backend_revision,
  }), 'utf8').digest('hex');
  const summary = {
    schema_version: 'search_production_health_evidence_summary_v1', platform: record.platform,
    product_health: record.product_health, execution_status: record.execution_status,
    failure_class: record.failure_class,
    tested_at: record.tested_at,
    target_url_sha256: record.target.target_url_sha256,
    target_superseded: record.target.target_superseded,
    site_runtime_sha: record.target.target_repo_sha,
    search_backend_revision: record.search.response.search_backend_revision,
    content_generation_id: record.search.response.catalog_revision,
    search_index_generation_id: record.search.response.corpus_revision,
    search_contract_version: record.search.response.search_contract_version,
    request_id: record.search.response.request_id,
    search_post_count: record.search.physical_post_count,
    result_count: record.search.response_id_count,
    rendered_card_count: record.search.card_count,
    opened_route_status: record.search.event_route.http_status,
    latency_ms: record.search.latency_ms,
    cache_status: record.search.cache_state,
    provider_attempt_counts: record.search.provider_attempts,
    client_observed_supabase_bytes: record.supabase_observed_bytes.total_bytes,
    target_fingerprint: record.target.target_url_sha256,
    runtime_fingerprint: runtimeFingerprint,
  };
  assertSanitizedSearchEvidence(summary);
  await writeFile(join(root, 'qa-summary.json'), `${JSON.stringify(summary, null, 2)}\n`, { mode: 0o600 });
  const failed = record.execution_status === 'PASS' ? 0 : 1;
  const junit = `<?xml version="1.0" encoding="UTF-8"?>\n<testsuite name="search.production_health" tests="1" failures="${failed}"><testcase classname="search.${xml(record.platform)}" name="production_health">${failed ? `<failure type="${xml(record.failure_class || record.execution_status)}"/>` : ''}</testcase></testsuite>\n`;
  await writeFile(join(root, 'junit.xml'), junit, { mode: 0o600 });
  await writeFile(join(root, '.redaction-ok'), 'PASS\n', { mode: 0o600 });
  return { root, record, files: ['result.json', 'qa-summary.json', 'junit.xml', '.redaction-ok'] };
}
