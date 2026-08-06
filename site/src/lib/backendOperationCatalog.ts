export type BackendCapability =
  | 'auth'
  | 'data'
  | 'functions'
  | 'storage-small'
  | 'oauth-navigation';

export type BackendOperationSemantics =
  | 'safe-read'
  | 'selected-once'
  | 'idempotent-replay'
  | 'disposable';

export type BackendResponseMode = 'buffered-json' | 'empty' | 'stream';

export interface BackendOperationDefinition {
  name: string;
  capability: BackendCapability;
  semantics: BackendOperationSemantics;
  responseMode: BackendResponseMode;
  responseLimitBytes: number;
  routeSupport: readonly ('direct' | 'relay')[];
}

const AUTH_RESPONSE_LIMIT = 256 * 1024;
const DATA_RESPONSE_LIMIT = 1024 * 1024;
const FUNCTION_RESPONSE_LIMIT = 2 * 1024 * 1024;
const SMALL_STORAGE_RESPONSE_LIMIT = 256 * 1024;

const READ_ONLY_RPCS = new Set([
  'get_event_search_quota_v2',
  'get_listing_personal_feed_v1',
  'focus_auth_get_delivery_receipt_v1',
  'transport_probe_v1',
]);

const IDEMPOTENT_RPCS = new Set([
  'register_focus_group_participant_v1',
  'register_prelaunch_notification_v1',
  'submit_focus_group_feedback_v2',
]);

const DISPOSABLE_RPCS = new Set([
  'focus_auth_record_client_outcome_v1',
  'focus_auth_record_verification_v1',
  'focus_auth_record_method_attempt_v1',
  'ingest_transport_experiment_event_v1',
  'record_pwa_lifecycle_v1',
]);

const SELECTED_ONCE_RPCS = new Set([
  'record_event_search_feedback_v1',
  'set_saved_event_state_v1',
]);

function methodOf(input: RequestInfo | URL, init?: RequestInit): string {
  return String(init?.method || (input instanceof Request ? input.method : 'GET')).toUpperCase();
}

function headerValue(input: RequestInfo | URL, init: RequestInit | undefined, name: string): string {
  const headers = new Headers(input instanceof Request ? input.headers : undefined);
  new Headers(init?.headers).forEach((value, key) => headers.set(key, value));
  return headers.get(name) || '';
}

function definition(
  name: string,
  capability: BackendCapability,
  semantics: BackendOperationSemantics,
  responseMode: BackendResponseMode,
  responseLimitBytes: number,
  routeSupport: readonly ('direct' | 'relay')[] = ['direct', 'relay'],
): BackendOperationDefinition {
  return { name, capability, semantics, responseMode, responseLimitBytes, routeSupport };
}

export class UnclassifiedBackendOperationError extends Error {
  readonly code = 'backend_operation_unclassified';
  constructor(method: string, pathname: string) {
    super(`backend_operation_unclassified:${method}:${pathname}`);
    this.name = 'UnclassifiedBackendOperationError';
  }
}

export function classifyBackendOperation(
  input: RequestInfo | URL,
  init?: RequestInit,
): BackendOperationDefinition {
  const url = input instanceof Request ? new URL(input.url) : new URL(String(input));
  const method = methodOf(input, init);
  const path = url.pathname.replace(/\/+$/u, '') || '/';
  const empty = method === 'HEAD';

  if (path.startsWith('/auth/v1/')) {
    const action = path.slice('/auth/v1/'.length);
    if (method === 'GET' || method === 'HEAD') {
      if (action === 'authorize' || action === 'callback' || action === 'user/identities/authorize') {
        return definition(
          `auth.${action.replaceAll('/', '.')}`,
          'oauth-navigation',
          'selected-once',
          empty ? 'empty' : 'buffered-json',
          AUTH_RESPONSE_LIMIT,
          ['direct', 'relay'],
        );
      }
      if (action === 'health' || action === 'settings' || action === 'user') {
        return definition(
          `auth.${action.replaceAll('/', '.')}`,
          'auth',
          'safe-read',
          empty ? 'empty' : 'buffered-json',
          AUTH_RESPONSE_LIMIT,
        );
      }
      if (action === 'verify') {
        return definition('auth.verify-link', 'auth', 'selected-once', 'buffered-json', AUTH_RESPONSE_LIMIT);
      }
      throw new UnclassifiedBackendOperationError(method, path);
    }
    if (method === 'POST' && ['otp', 'verify', 'token', 'resend', 'logout'].includes(action)) {
      return definition(`auth.${action}`, 'auth', 'selected-once', 'buffered-json', AUTH_RESPONSE_LIMIT);
    }
    throw new UnclassifiedBackendOperationError(method, path);
  }

  if (path.startsWith('/rest/v1/rpc/')) {
    const rpc = decodeURIComponent(path.slice('/rest/v1/rpc/'.length));
    if (method !== 'POST') throw new UnclassifiedBackendOperationError(method, path);
    if (READ_ONLY_RPCS.has(rpc)) {
      return definition(`rpc.${rpc}`, 'data', 'safe-read', 'buffered-json', DATA_RESPONSE_LIMIT);
    }
    if (IDEMPOTENT_RPCS.has(rpc)) {
      return definition(`rpc.${rpc}`, 'data', 'idempotent-replay', 'buffered-json', DATA_RESPONSE_LIMIT);
    }
    if (DISPOSABLE_RPCS.has(rpc)) {
      return definition(`rpc.${rpc}`, 'data', 'disposable', 'buffered-json', DATA_RESPONSE_LIMIT);
    }
    if (SELECTED_ONCE_RPCS.has(rpc)) {
      return definition(`rpc.${rpc}`, 'data', 'selected-once', 'buffered-json', DATA_RESPONSE_LIMIT);
    }
    throw new UnclassifiedBackendOperationError(method, path);
  }

  if (path.startsWith('/rest/v1/')) {
    if (method !== 'GET' && method !== 'HEAD') throw new UnclassifiedBackendOperationError(method, path);
    return definition(
      `data.read.${path.slice('/rest/v1/'.length).split('/')[0] || 'root'}`,
      'data',
      'safe-read',
      empty ? 'empty' : 'buffered-json',
      DATA_RESPONSE_LIMIT,
    );
  }

  if (path === '/functions/v1/event-search') {
    if (method !== 'POST') throw new UnclassifiedBackendOperationError(method, path);
    const streaming = headerValue(input, init, 'accept').includes('application/x-ndjson');
    return definition(
      'functions.event-search',
      'functions',
      'selected-once',
      streaming ? 'stream' : 'buffered-json',
      FUNCTION_RESPONSE_LIMIT,
    );
  }

  if (path === '/functions/v1/transport-probe') {
    if (method !== 'POST') throw new UnclassifiedBackendOperationError(method, path);
    return definition('functions.transport-probe', 'functions', 'safe-read', 'buffered-json', AUTH_RESPONSE_LIMIT);
  }

  if (path.startsWith('/storage/v1/object/focus-feedback')) {
    if (!['POST', 'DELETE'].includes(method)) throw new UnclassifiedBackendOperationError(method, path);
    // Current product input permits files larger than API Gateway's 2.5 MiB
    // limit. Until deterministic pre-upload compression is shipped, Storage is
    // a declared direct-only capability rather than an unreliable relay path.
    return definition(
      'storage.focus-feedback.mutate',
      'storage-small',
      'selected-once',
      'buffered-json',
      SMALL_STORAGE_RESPONSE_LIMIT,
      ['direct'],
    );
  }

  throw new UnclassifiedBackendOperationError(method, path);
}

export function policyForOperation(operation: BackendOperationDefinition):
  'safe-read' | 'selected-once' | 'idempotent-replay' {
  if (operation.semantics === 'safe-read') return 'safe-read';
  if (operation.semantics === 'idempotent-replay' || operation.semantics === 'disposable') {
    return 'idempotent-replay';
  }
  return 'selected-once';
}

export const backendOperationCatalogSnapshot = Object.freeze({
  readOnlyRpcs: [...READ_ONLY_RPCS].sort(),
  idempotentRpcs: [...IDEMPOTENT_RPCS].sort(),
  disposableRpcs: [...DISPOSABLE_RPCS].sort(),
  selectedOnceRpcs: [...SELECTED_ONCE_RPCS].sort(),
});
