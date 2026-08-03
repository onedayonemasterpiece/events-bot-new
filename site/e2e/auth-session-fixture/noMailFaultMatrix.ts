import {
  ResilientSupabaseTransport,
  SupabaseNoHealthyRouteError,
} from '../../src/lib/resilientSupabaseTransport.ts';

export const NO_MAIL_FAULT_PROFILES = [
  'normal',
  'client_supabase_direct_unreachable',
  'client_yandex_relay_unreachable',
  'both_client_routes_unreachable',
] as const;

export type NoMailFaultProfile = typeof NO_MAIL_FAULT_PROFILES[number];

export const NO_MAIL_OPERATION_MATRIX = Object.freeze({
  auth: {
    url: '/auth/v1/verify',
    init: { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{"type":"email"}' },
    operation: 'auth.verify',
    policy: 'selected-once',
  },
  search: {
    url: '/functions/v1/event-search',
    init: { method: 'POST', headers: { 'content-type': 'application/json', accept: 'application/json' }, body: '{"query_id":"fixture"}' },
    operation: 'functions.event-search',
    policy: 'selected-once',
  },
  personalization: {
    url: '/rest/v1/rpc/set_saved_event_state_v1',
    init: { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{"p_action_id":"fixture"}' },
    operation: 'rpc.set_saved_event_state_v1',
    policy: 'selected-once',
  },
  focus: {
    url: '/rest/v1/rpc/submit_focus_group_feedback_v2',
    init: { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{"p_client_request_id":"fixture"}' },
    operation: 'rpc.submit_focus_group_feedback_v2',
    policy: 'idempotent-replay',
  },
} as const);

type OperationName = keyof typeof NO_MAIL_OPERATION_MATRIX;

function routeOf(url: URL): 'direct' | 'relay' {
  return url.hostname.startsWith('relay.') ? 'relay' : 'direct';
}

function blocked(profile: NoMailFaultProfile, route: 'direct' | 'relay'): boolean {
  return profile === 'both_client_routes_unreachable'
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

export interface NoMailMatrixReceipt {
  schema: 'static_site_no_mail_fault_matrix_receipt.v1';
  profile: NoMailFaultProfile;
  operations: Record<OperationName, {
    expected_policy: string;
    outcome: string;
    selected_route: string | null;
    dispatch_count: number;
  }>;
  product_otp_issue_count: 0;
  external_mail_send_count: 0;
  external_mail_receipt_count: 0;
  duplicate_dispatch_count: number;
}

export async function runNoMailFaultMatrix(profile: NoMailFaultProfile): Promise<NoMailMatrixReceipt> {
  if (!NO_MAIL_FAULT_PROFILES.includes(profile)) throw new Error(`unknown_no_mail_fault_profile:${profile}`);
  const operations = {} as NoMailMatrixReceipt['operations'];
  let duplicateDispatchCount = 0;
  let productOtpIssueCount = 0;
  let externalMailSendCount = 0;
  let externalMailReceiptCount = 0;

  for (const [name, spec] of Object.entries(NO_MAIL_OPERATION_MATRIX) as [OperationName, typeof NO_MAIL_OPERATION_MATRIX[OperationName]][]) {
    const dispatched: string[] = [];
    const fetchImpl: typeof fetch = async (input, init) => {
      const url = new URL(input instanceof Request ? input.url : String(input));
      const route = routeOf(url);
      if (blocked(profile, route)) throw new TypeError(`fixture_network_reject:${route}`);
      if (url.pathname === '/auth/v1/health'
        || url.pathname === '/rest/v1/rpc/transport_probe_v1'
        || url.pathname === '/functions/v1/transport-probe') {
        return Response.json(probePayload(url, init));
      }
      if (url.pathname === '/auth/v1/otp') {
        productOtpIssueCount += 1;
        throw new Error('unexpected_product_otp');
      }
      if (/\/(?:postbox|mail-provider)\/(?:send|messages)$/u.test(url.pathname)) {
        externalMailSendCount += 1;
        throw new Error('unexpected_external_mail_send');
      }
      if (/\/(?:mail-trigger|mail-provider)\/receipts?$/u.test(url.pathname)) {
        externalMailReceiptCount += 1;
        throw new Error('unexpected_external_mail_receipt');
      }
      dispatched.push(route);
      return Response.json({ ok: true });
    };
    const transport = new ResilientSupabaseTransport({
      directUrl: 'https://direct.supabase.co',
      relayUrl: 'https://relay.example.invalid',
      publishableKey: 'publishable-fixture',
      fetchImpl,
      probeStaggerMs: 0,
      persistentStorage: null,
      routeCacheNamespace: `${profile}:${name}`,
    });

    let outcome = 'PASS';
    try {
      await transport.fetch(`https://direct.supabase.co${spec.url}`, spec.init);
    } catch (error) {
      if (profile === 'both_client_routes_unreachable' && error instanceof SupabaseNoHealthyRouteError) {
        outcome = 'NO_HEALTHY_ROUTE';
      } else {
        throw error;
      }
    }
    const transportOutcome = transport.latestOutcome(spec.operation);
    const dispatchCount = dispatched.length;
    if (dispatchCount > 1) duplicateDispatchCount += dispatchCount - 1;
    operations[name] = {
      expected_policy: spec.policy,
      outcome,
      selected_route: transportOutcome?.finalRoute || null,
      dispatch_count: dispatchCount,
    };
  }

  if (productOtpIssueCount !== 0 || externalMailSendCount !== 0 || externalMailReceiptCount !== 0) {
    throw new Error('no_mail_fault_matrix_side_effect_violation');
  }

  return {
    schema: 'static_site_no_mail_fault_matrix_receipt.v1',
    profile,
    operations,
    product_otp_issue_count: productOtpIssueCount,
    external_mail_send_count: externalMailSendCount,
    external_mail_receipt_count: externalMailReceiptCount,
    duplicate_dispatch_count: duplicateDispatchCount,
  };
}
