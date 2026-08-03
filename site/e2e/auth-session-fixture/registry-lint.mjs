import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const REQUIRED_AUTH_MODES = [
  'anonymous',
  'anonymous_session',
  'mocked_ui',
  'session_fixture',
  'admin_otp_ui',
  'real_mail_otp',
  'yandex_oauth',
];

const REQUIRED_RELIABILITY_SCENARIOS = [
  'auth.session_fixture',
  'auth.no_mail_operation_matrix',
  'connectivity.yandex_partial_outage_truth',
  'diagnostics.yandex_dependency_labels',
  'transport.both_client_routes_unreachable',
  'transport.supabase_upstream_unavailable',
  'personalization.ydb_projection_outage',
  'personalization.outbox_reconnect_exactly_once',
  'focus.feedback.partial_component_delivery',
  'auth.yandex_oauth_unavailable_email_fallback',
  'email.postbox_unavailable_durable_outbox',
  'inbound.yandex_pipeline_unavailable_replay',
];

function section(text, startName, endName) {
  const start = text.indexOf(`${startName}:\n`);
  if (start < 0) return '';
  const end = endName ? text.indexOf(`\n${endName}:\n`, start) : -1;
  return text.slice(start, end < 0 ? text.length : end);
}

function namedBlocks(text, sectionName, endName) {
  const source = section(text, sectionName, endName);
  const matches = [...source.matchAll(/^  ([a-z0-9_.-]+):\n/gmu)];
  const result = new Map();
  for (let index = 0; index < matches.length; index += 1) {
    const match = matches[index];
    const end = matches[index + 1]?.index ?? source.length;
    result.set(match[1], source.slice(match.index, end));
  }
  return result;
}

export function lintStaticSiteAutotestRegistry(registryText, focusRegistryText = '') {
  const errors = [];
  const authModes = namedBlocks(registryText, 'auth_modes', 'auth_fixture_contract');
  const scenarios = namedBlocks(registryText, 'scenarios', 'policies');
  for (const mode of REQUIRED_AUTH_MODES) {
    if (!authModes.has(mode)) errors.push(`missing_auth_mode:${mode}`);
  }
  for (const mode of authModes.keys()) {
    if (!REQUIRED_AUTH_MODES.includes(mode)) errors.push(`unexpected_auth_mode:${mode}`);
  }
  for (const scenario of REQUIRED_RELIABILITY_SCENARIOS) {
    if (!scenarios.has(scenario)) errors.push(`missing_scenario:${scenario}`);
  }

  for (const [scenarioId, block] of scenarios) {
    const mode = block.match(/^    auth_mode: ([a-z0-9_]+)$/mu)?.[1] || '';
    const identitySensitive = /^  (auth\.|personal\.|personalization\.|search\.|focus\.)/u.test(block)
      && /auth-session|auth-otp|anonymous_session|yandex-oauth|focus-feedback|personalization/u.test(block);
    if (identitySensitive && !mode) errors.push(`missing_scenario_auth_mode:${scenarioId}`);
    if (mode && !authModes.has(mode)) errors.push(`unknown_scenario_auth_mode:${scenarioId}:${mode}`);
    if (mode === 'session_fixture') {
      if (scenarioId !== 'auth.session_fixture' && !/^    depends_on: \[auth\.session_fixture\]$/mu.test(block)) {
        errors.push(`missing_session_fixture_dependency:${scenarioId}`);
      }
      if (/trigger_tags: \[[^\]]*auth-otp/u.test(block)) errors.push(`session_fixture_has_auth_otp_trigger:${scenarioId}`);
    }
    if (mode === 'real_mail_otp' && scenarioId !== 'focus.otp.browser_tab') {
      errors.push(`unexpected_real_mail_scenario:${scenarioId}`);
    }
  }

  const fixture = scenarios.get('auth.session_fixture') || '';
  for (const contract of [
    'product_otp_issue_count: 0',
    'external_mail_send_count: 0',
    'external_mail_receipt_count: 0',
    'real_mail_fallback: forbidden',
  ]) {
    if (!fixture.includes(contract)) errors.push(`auth_fixture_contract_missing:${contract}`);
  }
  const matrix = scenarios.get('auth.no_mail_operation_matrix') || '';
  for (const operation of [
    'auth.verify: selected-once',
    'functions.event-search: selected-once',
    'rpc.set_saved_event_state_v1: selected-once',
    'rpc.submit_focus_group_feedback_v2: idempotent-replay',
  ]) {
    if (!matrix.includes(operation)) errors.push(`no_mail_matrix_operation_missing:${operation}`);
  }
  for (const policy of [
    'session_fixture_product_otp_issue_count: 0',
    'session_fixture_external_mail_send_count: 0',
    'session_fixture_real_mail_fallback: forbidden',
    'anonymous_session_raffle_eligibility: false',
    'no_mail_fault_matrix_duplicate_dispatch_count: 0',
  ]) {
    if (!registryText.includes(policy)) errors.push(`registry_policy_missing:${policy}`);
  }

  if (focusRegistryText) {
    for (const focusInvariant of [
      'schema: kenigevents.focus_group_release_scenarios.v5',
      'anonymous_provider: supabase_auth_anonymous',
      'anonymous_can_submit_feedback: true',
      'anonymous_can_enter_raffle: false',
      'upgrade_same_user_id_preferred: true',
      'merge_existing_permanent_account_required: true',
    ]) {
      if (!focusRegistryText.includes(focusInvariant)) errors.push(`focus_v5_invariant_missing:${focusInvariant}`);
    }
  }
  return errors;
}

async function main() {
  const root = resolve(fileURLToPath(new URL('../../..', import.meta.url)));
  const registry = await readFile(resolve(root, 'docs/testing/static-site-autotest-scenarios.v1.yml'), 'utf8');
  const focus = await readFile(resolve(root, 'docs/testing/focus-group-release-scenarios.v1.yml'), 'utf8');
  const errors = lintStaticSiteAutotestRegistry(registry, focus);
  if (errors.length) {
    for (const error of errors) console.error(`[static-site-autotest-registry] ${error}`);
    process.exitCode = 1;
    return;
  }
  console.log('[static-site-autotest-registry] PASS');
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) await main();
