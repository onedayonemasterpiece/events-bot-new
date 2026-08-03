import {
  ResilientSupabaseTransport,
  SupabaseAmbiguousWriteError,
  SupabaseNoHealthyRouteError,
  type SupabaseTransportOutcomeKind,
} from '../../src/lib/resilientSupabaseTransport.ts';

export const NO_MAIL_FAULT_PROFILES = [
  'normal',
  'client_supabase_direct_unreachable',
  'client_yandex_relay_unreachable',
  'both_client_routes_unreachable',
  'supabase_upstream_unavailable',
  'selected_once_response_body_ambiguous',
  'recovery_after_reload',
] as const;

export type NoMailFaultProfile = typeof NO_MAIL_FAULT_PROFILES[number];

export const NO_MAIL_OPERATION_MATRIX = Object.freeze({
  auth: {
    url: '/auth/v1/verify',
    init: { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{"type":"email","token_hash":"fixture-auth-action"}' },
    operation: 'auth.verify',
    actionId: 'fixture-auth-action',
    policy: 'selected-once',
  },
  search: {
    url: '/functions/v1/event-search',
    init: { method: 'POST', headers: { 'content-type': 'application/json', accept: 'application/json' }, body: '{"query_id":"fixture-search-action"}' },
    operation: 'functions.event-search',
    actionId: 'fixture-search-action',
    policy: 'selected-once',
  },
  personalization: {
    url: '/rest/v1/rpc/set_saved_event_state_v1',
    init: { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{"p_action_id":"fixture-personalization-action"}' },
    operation: 'rpc.set_saved_event_state_v1',
    actionId: 'fixture-personalization-action',
    policy: 'selected-once',
  },
  focus: {
    url: '/rest/v1/rpc/submit_focus_group_feedback_v2',
    init: { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{"p_client_request_id":"fixture-focus-action"}' },
    operation: 'rpc.submit_focus_group_feedback_v2',
    actionId: 'fixture-focus-action',
    policy: 'idempotent-replay',
  },
} as const);

type OperationName = keyof typeof NO_MAIL_OPERATION_MATRIX;
type OperationSpec = typeof NO_MAIL_OPERATION_MATRIX[OperationName];
type Route = 'direct' | 'relay';
type LocalState = 'pending' | 'committed';

const delay = (milliseconds: number) => new Promise((resolve) => setTimeout(resolve, milliseconds));

function routeOf(url: URL): Route {
  return url.hostname.startsWith('relay.') ? 'relay' : 'direct';
}

function isProbe(url: URL): boolean {
  return url.pathname === '/auth/v1/health'
    || url.pathname === '/rest/v1/rpc/transport_probe_v1'
    || url.pathname === '/functions/v1/transport-probe';
}

function clientRouteBlocked(profile: NoMailFaultProfile, route: Route, recovering: boolean): boolean {
  if (recovering) return false;
  return profile === 'both_client_routes_unreachable'
    || profile === 'recovery_after_reload'
    || (profile === 'client_supabase_direct_unreachable' && route === 'direct')
    || (profile === 'client_yandex_relay_unreachable' && route === 'relay');
}

function probePayload(url: URL, init?: RequestInit): Record<string, unknown> {
  if (url.pathname === '/auth/v1/health') return { version: 'fixture' };
  let request: { p_nonce?: string; nonce?: string } = {};
  try {
    request = JSON.parse(String(init?.body || '{}'));
  } catch {
    request = {};
  }
  return { schema: 1, nonce: request.p_nonce || request.nonce || '' };
}

function ambiguousBodyResponse(): Response {
  const encoder = new TextEncoder();
  let emitted = false;
  const body = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (!emitted) {
        emitted = true;
        controller.enqueue(encoder.encode('{"ok":'));
        return;
      }
      controller.error(new Error('fixture_response_body_ambiguous'));
    },
  });
  return new Response(body, {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });
}

interface OperationExecutionState {
  dispatchedRoutes: Route[];
  effectCounts: Map<string, number>;
  faultCodes: Set<string>;
}

interface CounterState {
  productOtpIssueCount: number;
  externalMailSendCount: number;
  externalMailReceiptCount: number;
}

function createOperationAttempt(
  profile: NoMailFaultProfile,
  spec: OperationSpec,
  state: OperationExecutionState,
  counters: CounterState,
  recovering: boolean,
): ResilientSupabaseTransport {
  const fetchImpl: typeof fetch = async (input, init) => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    const route = routeOf(url);

    if (url.pathname === '/auth/v1/otp') {
      counters.productOtpIssueCount += 1;
      throw new Error('unexpected_product_otp');
    }
    if (/\/(?:postbox|mail-provider)\/(?:send|messages)$/u.test(url.pathname)) {
      counters.externalMailSendCount += 1;
      throw new Error('unexpected_external_mail_send');
    }
    if (/\/(?:mail-trigger|mail-provider)\/receipts?$/u.test(url.pathname)) {
      counters.externalMailReceiptCount += 1;
      throw new Error('unexpected_external_mail_receipt');
    }

    if (clientRouteBlocked(profile, route, recovering)) {
      state.faultCodes.add(`${route}.probe.client_unreachable`);
      throw new TypeError(`fixture_network_reject:${route}`);
    }

    if (isProbe(url)) {
      // Make the relay-down fault execute rather than letting a synchronous
      // successful direct probe cancel the representative alternate probe.
      if (profile === 'client_yandex_relay_unreachable' && route === 'direct') await delay(5);
      return Response.json(probePayload(url, init));
    }

    state.dispatchedRoutes.push(route);
    if (profile === 'supabase_upstream_unavailable') {
      state.faultCodes.add(`${route}.operation.shared_upstream`);
      throw new TypeError('fixture_shared_supabase_upstream_unavailable');
    }

    const alreadyApplied = state.effectCounts.has(spec.actionId);
    if (!alreadyApplied) state.effectCounts.set(spec.actionId, 1);

    if (profile === 'selected_once_response_body_ambiguous' && state.dispatchedRoutes.length === 1) {
      state.faultCodes.add(`${route}.operation.response_body_ambiguous`);
      return ambiguousBodyResponse();
    }
    return Response.json({ ok: true, duplicate: alreadyApplied });
  };

  return new ResilientSupabaseTransport({
    directUrl: 'https://direct.supabase.co',
    relayUrl: 'https://relay.example.invalid',
    publishableKey: 'publishable-fixture',
    fetchImpl,
    probeStaggerMs: 1,
    persistentStorage: null,
    routeCacheNamespace: `${profile}:${spec.operation}:${recovering ? 'recovery' : 'initial'}`,
  });
}

export interface NoMailOperationReceipt {
  operation: string;
  expected_policy: string;
  outcome: string;
  selected_route: Route | null;
  dispatch_count: number;
  initial_dispatch_count: number;
  recovery_dispatch_count: number;
  dispatched_routes: Route[];
  effect_count: number;
  duplicate_effect_count: number;
  transport_outcome_kind: SupabaseTransportOutcomeKind | null;
  local_state: LocalState;
  reload_survived: boolean;
  stable_action_id_preserved: boolean;
  false_relay_recovery: boolean;
  fault_activation_codes: string[];
}

export interface NoMailMatrixReceipt {
  schema: 'static_site_no_mail_fault_matrix_receipt.v1';
  profile: NoMailFaultProfile;
  operations: Record<OperationName, NoMailOperationReceipt>;
  fault_activation: {
    expected: boolean;
    activated: boolean;
    codes: string[];
    sensitive_fields_omitted: true;
  };
  product_otp_issue_count: 0;
  external_mail_send_count: 0;
  external_mail_receipt_count: 0;
  duplicate_dispatch_count: number;
  duplicate_effect_count: number;
  selected_once_dispatch_violation_count: number;
  false_relay_recovery_count: number;
}

async function executeOperation(
  profile: NoMailFaultProfile,
  spec: OperationSpec,
  counters: CounterState,
): Promise<NoMailOperationReceipt> {
  const state: OperationExecutionState = {
    dispatchedRoutes: [],
    effectCounts: new Map(),
    faultCodes: new Set(),
  };
  let localState: LocalState = 'pending';
  let outcome = 'PASS';
  let initialDispatchCount = 0;
  let recoveryDispatchCount = 0;
  let reloadSurvived = false;
  let stableActionIdPreserved = false;
  let transportOutcomeKind: SupabaseTransportOutcomeKind | null = null;
  let selectedRoute: Route | null = null;

  const initial = createOperationAttempt(profile, spec, state, counters, false);
  try {
    await initial.fetch(`https://direct.supabase.co${spec.url}`, spec.init);
    localState = 'committed';
  } catch (error) {
    if (error instanceof SupabaseNoHealthyRouteError) {
      outcome = 'NO_HEALTHY_ROUTE';
    } else if (profile === 'supabase_upstream_unavailable') {
      outcome = 'SHARED_UPSTREAM_UNAVAILABLE';
    } else if (profile === 'selected_once_response_body_ambiguous'
      && error instanceof SupabaseAmbiguousWriteError) {
      outcome = 'AMBIGUOUS_SELECTED_ONCE';
      localState = state.effectCounts.has(spec.actionId) ? 'committed' : 'pending';
    } else {
      throw error;
    }
  }
  initialDispatchCount = state.dispatchedRoutes.length;
  let transportOutcome = initial.latestOutcome(spec.operation);

  if (profile === 'selected_once_response_body_ambiguous' && spec.policy === 'idempotent-replay') {
    outcome = 'RECOVERED_IDEMPOTENT';
    localState = 'committed';
  }

  if (profile === 'recovery_after_reload') {
    if (outcome !== 'NO_HEALTHY_ROUTE' || initialDispatchCount !== 0) {
      throw new Error(`recovery_fixture_precondition_failed:${spec.operation}`);
    }
    // Serialize only the pending local intent, then create a new transport as a
    // browser reload would. The stable action id never enters the receipt.
    const journal = JSON.stringify({ actionId: spec.actionId, state: localState });
    const reloaded = JSON.parse(journal) as { actionId: string; state: LocalState };
    reloadSurvived = reloaded.state === 'pending';
    stableActionIdPreserved = reloaded.actionId === spec.actionId;
    const recovery = createOperationAttempt(profile, spec, state, counters, true);
    if (reloaded.state === 'pending') {
      await recovery.fetch(`https://direct.supabase.co${spec.url}`, spec.init);
      recoveryDispatchCount = state.dispatchedRoutes.length - initialDispatchCount;
      reloaded.state = 'committed';
    }
    // A second reconnect callback sees committed state and performs no fetch.
    if (reloaded.state !== 'committed') throw new Error(`recovery_fixture_commit_missing:${spec.operation}`);
    localState = reloaded.state;
    outcome = 'RECOVERED_AFTER_RELOAD';
    transportOutcome = recovery.latestOutcome(spec.operation);
  }

  if (profile !== 'recovery_after_reload') {
    recoveryDispatchCount = Math.max(0, state.dispatchedRoutes.length - initialDispatchCount);
  }
  transportOutcomeKind = transportOutcome?.kind || null;
  selectedRoute = transportOutcome?.finalRoute || null;
  const falseRelayRecovery = profile === 'supabase_upstream_unavailable'
    && transportOutcomeKind === 'recovered';

  const effectCount = [...state.effectCounts.values()].reduce((total, count) => total + count, 0);
  const duplicateEffectCount = [...state.effectCounts.values()]
    .reduce((total, count) => total + Math.max(0, count - 1), 0);

  return {
    operation: spec.operation,
    expected_policy: spec.policy,
    outcome,
    selected_route: selectedRoute,
    dispatch_count: state.dispatchedRoutes.length,
    initial_dispatch_count: initialDispatchCount,
    recovery_dispatch_count: recoveryDispatchCount,
    dispatched_routes: [...state.dispatchedRoutes],
    effect_count: effectCount,
    duplicate_effect_count: duplicateEffectCount,
    transport_outcome_kind: transportOutcomeKind,
    local_state: localState,
    reload_survived: reloadSurvived,
    stable_action_id_preserved: stableActionIdPreserved,
    false_relay_recovery: falseRelayRecovery,
    fault_activation_codes: [...state.faultCodes].sort(),
  };
}

export async function runNoMailFaultMatrix(profile: NoMailFaultProfile): Promise<NoMailMatrixReceipt> {
  if (!NO_MAIL_FAULT_PROFILES.includes(profile)) throw new Error(`unknown_no_mail_fault_profile:${profile}`);
  const operations = {} as NoMailMatrixReceipt['operations'];
  const counters: CounterState = {
    productOtpIssueCount: 0,
    externalMailSendCount: 0,
    externalMailReceiptCount: 0,
  };

  for (const [name, spec] of Object.entries(NO_MAIL_OPERATION_MATRIX) as [OperationName, OperationSpec][]) {
    operations[name] = await executeOperation(profile, spec, counters);
  }

  if (counters.productOtpIssueCount !== 0
    || counters.externalMailSendCount !== 0
    || counters.externalMailReceiptCount !== 0) {
    throw new Error('no_mail_fault_matrix_side_effect_violation');
  }

  const allFaultCodes = [...new Set(Object.values(operations).flatMap((item) => item.fault_activation_codes))].sort();
  const selectedOnceDispatchViolations = Object.values(operations)
    .filter((item) => item.expected_policy === 'selected-once' && item.dispatch_count > 1).length;
  const duplicateEffectCount = Object.values(operations)
    .reduce((total, item) => total + item.duplicate_effect_count, 0);
  const falseRelayRecoveryCount = Object.values(operations)
    .filter((item) => item.false_relay_recovery).length;
  const expectedFault = profile !== 'normal';
  if (expectedFault && allFaultCodes.length === 0) throw new Error(`fault_profile_not_activated:${profile}`);

  return {
    schema: 'static_site_no_mail_fault_matrix_receipt.v1',
    profile,
    operations,
    fault_activation: {
      expected: expectedFault,
      activated: allFaultCodes.length > 0,
      codes: allFaultCodes,
      sensitive_fields_omitted: true,
    },
    product_otp_issue_count: counters.productOtpIssueCount,
    external_mail_send_count: counters.externalMailSendCount,
    external_mail_receipt_count: counters.externalMailReceiptCount,
    duplicate_dispatch_count: selectedOnceDispatchViolations,
    duplicate_effect_count: duplicateEffectCount,
    selected_once_dispatch_violation_count: selectedOnceDispatchViolations,
    false_relay_recovery_count: falseRelayRecoveryCount,
  };
}
