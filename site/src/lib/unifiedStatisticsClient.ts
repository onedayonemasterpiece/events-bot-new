export type StatisticsLane = 'product_fact' | 'product_observation' | 'operational';
export type StatisticsConsent = 'granted' | 'not_required';
export type StatisticsSource = 'browser_observation' | 'authoritative_receipt' | 'service_runtime';
export type StatisticsScalar = string | number | boolean | null;

export interface StatisticsEntity {
  kind: string;
  id: string;
}

export interface StatisticsReleaseContext {
  releaseSha?: string;
  pageRevision?: string;
  contentRevision?: string;
  featureVersion?: string;
}

export interface StatisticsInput {
  eventName: string;
  lane: StatisticsLane;
  source: StatisticsSource;
  consent: StatisticsConsent;
  occurredAt?: number;
  sessionId?: string;
  actorKey?: string;
  surface?: string;
  entity?: StatisticsEntity;
  release?: StatisticsReleaseContext;
  dimensions?: Record<string, StatisticsScalar>;
  counters?: Record<string, number>;
  maxima?: Record<string, number>;
  idempotencyKey?: string;
}

export interface StatisticsAggregate {
  eventName: string;
  lane: 'product_observation' | 'operational';
  source: 'browser_observation' | 'service_runtime';
  firstObservedAt: number;
  lastObservedAt: number;
  observationCount: number;
  sessionId?: string;
  actorKey?: string;
  surface?: string;
  entity?: StatisticsEntity;
  release?: StatisticsReleaseContext;
  dimensions?: Record<string, StatisticsScalar>;
  counters?: Record<string, number>;
  maxima?: Record<string, number>;
}

export interface StatisticsFact {
  eventName: string;
  lane: 'product_fact';
  source: 'authoritative_receipt';
  occurredAt: number;
  idempotencyKey: string;
  sessionId?: string;
  actorKey?: string;
  surface?: string;
  entity?: StatisticsEntity;
  release?: StatisticsReleaseContext;
  dimensions?: Record<string, StatisticsScalar>;
  counters?: Record<string, number>;
  maxima?: Record<string, number>;
}

export interface StatisticsBatchV1 {
  schemaVersion: 1;
  batchId: string;
  createdAt: number;
  facts: StatisticsFact[];
  observations: StatisticsAggregate[];
}

export interface StatisticsOutboxRecord {
  id: string;
  channel: string;
  payload: unknown;
}

export interface StatisticsOutbox {
  enqueue(input: StatisticsOutboxRecord): Promise<boolean>;
  flush(
    sender: (record: StatisticsOutboxRecord) => Promise<'sent' | 'retry' | 'drop' | 'skip'>,
  ): Promise<number>;
}

export interface StatisticsEventRule {
  lane: StatisticsLane;
  source: StatisticsSource;
  consentRequired?: boolean;
  idempotencyKeyRequired?: boolean;
  entityKind?: string;
  dimensions?: readonly string[];
  counters?: readonly string[];
  maxima?: readonly string[];
}

export type StatisticsEventCatalog = Readonly<Record<string, StatisticsEventRule>>;

export interface UnifiedStatisticsClientConfig {
  outbox: StatisticsOutbox;
  sender: (batch: StatisticsBatchV1) => Promise<boolean>;
  catalog?: StatisticsEventCatalog;
  now?: () => number;
  makeId?: () => string;
  maxAccumulatorEntries?: number;
  maxBatchBytes?: number;
}

export type StatisticsRecordResult =
  | 'accepted'
  | 'dropped_no_consent'
  | 'dropped_invalid'
  | 'dropped_oversize'
  | 'dropped_capacity';

const CHANNEL = 'unified-statistics-v1';
const DEFAULT_MAX_ACCUMULATOR_ENTRIES = 64;
const DEFAULT_MAX_BATCH_BYTES = 3800;
const MAX_ATTRIBUTE_KEYS = 24;
const MAX_STRING_BYTES = 96;
const MAX_COUNTER_ABS = 1_000_000;
const MAX_OBSERVATION_COUNT = 65_535;
const IDENTIFIER_RE = /^[a-z0-9][a-z0-9._:-]{0,95}$/u;
const EVENT_NAME_RE = /^[a-z][a-z0-9_.:-]{2,79}$/u;
const UUIDISH_RE = /^[a-z0-9][a-z0-9._:-]{7,119}$/u;
const SENSITIVE_KEY_RE = /(?:^|_)(?:email|phone|token|secret|password|jwt|otp|ip|user_agent|ua|url|href|query|text|body|html|selector)(?:_|$)/iu;
const URLISH_RE = /(?:https?:\/\/|www\.|mailto:|tel:)/iu;
const TOP_LEVEL_INPUT_KEYS = new Set([
  'eventName', 'lane', 'source', 'consent', 'occurredAt', 'sessionId', 'actorKey',
  'surface', 'entity', 'release', 'dimensions', 'counters', 'maxima', 'idempotencyKey',
]);
const ENTITY_KEYS = new Set(['kind', 'id']);
const RELEASE_KEYS = new Set(['releaseSha', 'pageRevision', 'contentRevision', 'featureVersion']);

const COMMON_DIMENSIONS = [
  'device_class',
  'app_mode',
  'auth_state_class',
  'surface_family',
  'entry_surface',
  'release_channel',
] as const;

export const DEFAULT_STATISTICS_EVENT_CATALOG: StatisticsEventCatalog = Object.freeze({
  session_summary: {
    lane: 'product_observation', source: 'browser_observation', consentRequired: true,
    counters: ['page_count', 'unique_event_count', 'large_cards_exposed', 'compact_cards_exposed', 'large_cards_opened', 'compact_cards_opened', 'intent_action_count'],
    maxima: ['max_large_position_bucket', 'max_compact_position_bucket', 'max_description_checkpoint', 'capability_maturity_tier'],
  },
  card_visible: {
    lane: 'product_observation', source: 'browser_observation', consentRequired: true, entityKind: 'event',
    dimensions: ['card_density', 'card_family', 'ordering_class'], counters: ['exposures'], maxima: ['position_bucket', 'dwell_bucket'],
  },
  card_opened: {
    lane: 'product_observation', source: 'browser_observation', consentRequired: true, entityKind: 'event',
    dimensions: ['card_density', 'card_family', 'ordering_class'], counters: ['opens'],
  },
  description_checkpoint: {
    lane: 'product_observation', source: 'browser_observation', consentRequired: true, entityKind: 'event',
    dimensions: ['description_length_bucket', 'media_family', 'page_family'], maxima: ['checkpoint', 'time_to_checkpoint_bucket'],
  },
  cta_stage: {
    lane: 'product_observation', source: 'browser_observation', consentRequired: true, entityKind: 'event',
    dimensions: ['cta_kind', 'cta_set_version', 'placement', 'stage'], counters: ['occurrences'],
  },
  action_receipt: {
    lane: 'product_fact', source: 'authoritative_receipt', idempotencyKeyRequired: true, entityKind: 'event',
    dimensions: ['action_kind', 'target_kind', 'stage', 'result_class', 'route_class'],
  },
  hero_talk_state: {
    lane: 'product_observation', source: 'browser_observation', consentRequired: true,
    dimensions: ['placement', 'chain_id', 'step_id', 'object_role', 'target_kind', 'state'], counters: ['occurrences'], maxima: ['checkpoint'],
  },
  keyboard_state: {
    lane: 'product_observation', source: 'browser_observation', consentRequired: true,
    dimensions: ['state', 'command_family', 'page_family'], counters: ['occurrences'],
  },
  personalization_state: {
    lane: 'product_observation', source: 'browser_observation', consentRequired: true,
    dimensions: ['state_class', 'model_version', 'profile_age_bucket', 'outcome_class'], counters: ['eligible', 'opened', 'value_reached'], maxima: ['cards_to_first_value_bucket'],
  },
  search_outcome: {
    lane: 'product_observation', source: 'browser_observation', consentRequired: true,
    dimensions: ['result_class', 'query_length_bucket', 'latency_bucket', 'transport_class'], counters: ['requests', 'value_reached'],
  },
  pwa_lifecycle: {
    lane: 'product_observation', source: 'browser_observation', consentRequired: true,
    dimensions: ['lifecycle_kind', 'display_mode', 'platform_class'], counters: ['occurrences'],
  },
  delivery_receipt: {
    lane: 'product_fact', source: 'authoritative_receipt', idempotencyKeyRequired: true,
    dimensions: ['purpose', 'provider_class', 'stage', 'result_class', 'route_class', 'latency_bucket'],
  },
  feature_state: {
    lane: 'product_observation', source: 'browser_observation', consentRequired: true,
    dimensions: ['feature', 'placement', 'version', 'state'], counters: ['eligible', 'visible', 'engaged', 'completed', 'value_reached'], maxima: ['checkpoint'],
  },
  ingest_health: {
    lane: 'operational', source: 'service_runtime',
    dimensions: ['route_class', 'result_class', 'failure_class', 'schema_version'],
    counters: ['accepted_batches', 'retry_batches', 'dropped_batches', 'quarantined_events', 'deduped_facts'],
    maxima: ['outbox_age_bucket', 'payload_bytes_bucket'],
  },
  storage_budget: {
    lane: 'operational', source: 'service_runtime',
    dimensions: ['store', 'table_class', 'budget_band'], counters: ['rows', 'bytes', 'read_units', 'write_units', 'expired_eligible', 'archived', 'deleted'], maxima: ['oldest_pending_age_bucket'],
  },
});

function byteLength(value: unknown): number {
  return new TextEncoder().encode(JSON.stringify(value)).byteLength;
}

function clampInteger(value: number, min: number, max: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.min(max, Math.max(min, Math.trunc(value)));
}

function validIdentifier(value: unknown, maxLength = 96): value is string {
  return typeof value === 'string'
    && value.length <= maxLength
    && IDENTIFIER_RE.test(value);
}

function sanitizeScalar(value: StatisticsScalar): StatisticsScalar | undefined {
  if (value === null || typeof value === 'boolean') return value;
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return undefined;
    return Math.abs(value) <= MAX_COUNTER_ABS ? value : undefined;
  }
  if (typeof value !== 'string' || URLISH_RE.test(value)) return undefined;
  if (new TextEncoder().encode(value).byteLength > MAX_STRING_BYTES) return undefined;
  return value;
}

function sanitizeRecord(
  value: Record<string, StatisticsScalar> | undefined,
): Record<string, StatisticsScalar> | undefined {
  if (!value) return undefined;
  const entries = Object.entries(value);
  if (entries.length > MAX_ATTRIBUTE_KEYS) return undefined;
  const result: Record<string, StatisticsScalar> = {};
  for (const [key, raw] of entries) {
    if (!validIdentifier(key, 64) || SENSITIVE_KEY_RE.test(key)) return undefined;
    const scalar = sanitizeScalar(raw);
    if (scalar === undefined) return undefined;
    result[key] = scalar;
  }
  return Object.keys(result).length ? result : undefined;
}

function sanitizeNumbers(
  value: Record<string, number> | undefined,
  mode: 'counter' | 'maximum',
): Record<string, number> | undefined {
  if (!value) return undefined;
  const entries = Object.entries(value);
  if (entries.length > MAX_ATTRIBUTE_KEYS) return undefined;
  const result: Record<string, number> = {};
  for (const [key, raw] of entries) {
    if (!validIdentifier(key, 64) || SENSITIVE_KEY_RE.test(key) || !Number.isFinite(raw)) return undefined;
    result[key] = mode === 'counter'
      ? clampInteger(raw, -MAX_COUNTER_ABS, MAX_COUNTER_ABS)
      : clampInteger(raw, 0, MAX_COUNTER_ABS);
  }
  return Object.keys(result).length ? result : undefined;
}

function sanitizeRelease(value: StatisticsReleaseContext | undefined): StatisticsReleaseContext | undefined {
  if (!value) return undefined;
  if (Object.keys(value).some((key) => !RELEASE_KEYS.has(key))) return undefined;
  const result: StatisticsReleaseContext = {};
  for (const [key, raw] of Object.entries(value)) {
    if (raw === undefined) continue;
    if (!validIdentifier(raw, 96)) return undefined;
    (result as Record<string, string>)[key] = raw;
  }
  return Object.keys(result).length ? result : undefined;
}

function sanitizeEntity(value: StatisticsEntity | undefined): StatisticsEntity | undefined {
  if (!value) return undefined;
  if (Object.keys(value).some((key) => !ENTITY_KEYS.has(key))) return undefined;
  if (!validIdentifier(value.kind, 48) || !validIdentifier(value.id, 96)) return undefined;
  return { kind: value.kind, id: value.id };
}

function keysAllowed(value: object | undefined, allowed: readonly string[] | undefined, includeCommon = false): boolean {
  if (!value) return true;
  const allowlist = new Set(includeCommon ? [...COMMON_DIMENSIONS, ...(allowed || [])] : (allowed || []));
  return Object.keys(value).every((key) => allowlist.has(key));
}

function normalizeInput(
  input: StatisticsInput,
  now: number,
  catalog: StatisticsEventCatalog,
): StatisticsInput | null {
  if (!input || typeof input !== 'object') return null;
  if (Object.keys(input).some((key) => !TOP_LEVEL_INPUT_KEYS.has(key))) return null;
  if (!EVENT_NAME_RE.test(String(input.eventName || ''))) return null;
  const rule = catalog[input.eventName];
  if (!rule) return null;
  if (input.lane !== rule.lane || input.source !== rule.source) return null;
  if (!['granted', 'not_required'].includes(input.consent)) return null;

  if (rule.consentRequired && input.consent !== 'granted') return null;
  if (input.lane === 'product_fact') {
    if (rule.idempotencyKeyRequired !== true || !UUIDISH_RE.test(String(input.idempotencyKey || ''))) return null;
  }
  if (input.lane === 'operational' && input.actorKey) return null;

  const dimensions = sanitizeRecord(input.dimensions);
  const counters = sanitizeNumbers(input.counters, 'counter');
  const maxima = sanitizeNumbers(input.maxima, 'maximum');
  const release = sanitizeRelease(input.release);
  const entity = sanitizeEntity(input.entity);
  if (input.dimensions && !dimensions) return null;
  if (input.counters && !counters) return null;
  if (input.maxima && !maxima) return null;
  if (input.release && !release) return null;
  if (input.entity && !entity) return null;
  if (!keysAllowed(dimensions, rule.dimensions, true)) return null;
  if (!keysAllowed(counters, rule.counters)) return null;
  if (!keysAllowed(maxima, rule.maxima)) return null;
  if (rule.entityKind && entity?.kind !== rule.entityKind) return null;
  if (input.sessionId && !validIdentifier(input.sessionId, 96)) return null;
  if (input.actorKey && !validIdentifier(input.actorKey, 96)) return null;
  if (input.surface && !validIdentifier(input.surface, 96)) return null;

  return {
    ...input,
    occurredAt: clampInteger(input.occurredAt ?? now, 0, Number.MAX_SAFE_INTEGER),
    dimensions,
    counters,
    maxima,
    release,
    entity,
  };
}

function stableObject(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(stableObject);
  if (!value || typeof value !== 'object') return value;
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .filter(([, item]) => item !== undefined)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, item]) => [key, stableObject(item)]),
  );
}

function aggregationKey(input: StatisticsInput): string {
  return JSON.stringify(stableObject({
    eventName: input.eventName,
    lane: input.lane,
    source: input.source,
    sessionId: input.sessionId,
    actorKey: input.actorKey,
    surface: input.surface,
    entity: input.entity,
    release: input.release,
    dimensions: input.dimensions,
  }));
}

function mergeNumbers(
  left: Record<string, number> | undefined,
  right: Record<string, number> | undefined,
  mode: 'sum' | 'max',
): Record<string, number> | undefined {
  if (!left && !right) return undefined;
  const result = { ...(left || {}) };
  for (const [key, value] of Object.entries(right || {})) {
    result[key] = mode === 'sum'
      ? clampInteger((result[key] || 0) + value, -MAX_COUNTER_ABS, MAX_COUNTER_ABS)
      : Math.max(result[key] || 0, value);
  }
  return Object.keys(result).length ? result : undefined;
}

function makeAggregate(input: StatisticsInput): StatisticsAggregate {
  const occurredAt = Number(input.occurredAt || 0);
  return {
    eventName: input.eventName,
    lane: input.lane as 'product_observation' | 'operational',
    source: input.source as 'browser_observation' | 'service_runtime',
    firstObservedAt: occurredAt,
    lastObservedAt: occurredAt,
    observationCount: 1,
    sessionId: input.sessionId,
    actorKey: input.actorKey,
    surface: input.surface,
    entity: input.entity,
    release: input.release,
    dimensions: input.dimensions,
    counters: input.counters,
    maxima: input.maxima,
  };
}

function makeFact(input: StatisticsInput): StatisticsFact {
  return {
    eventName: input.eventName,
    lane: 'product_fact',
    source: 'authoritative_receipt',
    occurredAt: Number(input.occurredAt || 0),
    idempotencyKey: String(input.idempotencyKey),
    sessionId: input.sessionId,
    actorKey: input.actorKey,
    surface: input.surface,
    entity: input.entity,
    release: input.release,
    dimensions: input.dimensions,
    counters: input.counters,
    maxima: input.maxima,
  };
}

export function createUnifiedStatisticsClient(config: UnifiedStatisticsClientConfig) {
  const now = config.now || (() => Date.now());
  let fallbackId = 0;
  const makeId = config.makeId || (() => globalThis.crypto?.randomUUID?.() || `batch-${now()}-${++fallbackId}`);
  const catalog = config.catalog || DEFAULT_STATISTICS_EVENT_CATALOG;
  const maxAccumulatorEntries = Math.min(
    256,
    Math.max(1, Math.trunc(config.maxAccumulatorEntries || DEFAULT_MAX_ACCUMULATOR_ENTRIES)),
  );
  const maxBatchBytes = Math.min(4096, Math.max(1024, Math.trunc(config.maxBatchBytes || DEFAULT_MAX_BATCH_BYTES)));
  const aggregates = new Map<string, StatisticsAggregate>();
  let serial: Promise<unknown> = Promise.resolve();

  const enqueueBatch = async (batch: StatisticsBatchV1, outboxId: string): Promise<StatisticsRecordResult> => {
    if (byteLength(batch) > maxBatchBytes) return 'dropped_oversize';
    const accepted = await config.outbox.enqueue({ id: outboxId, channel: CHANNEL, payload: batch });
    return accepted ? 'accepted' : 'dropped_capacity';
  };

  const flushOutbox = () => config.outbox.flush(async (record) => {
    if (record.channel !== CHANNEL) return 'skip';
    const batch = record.payload as StatisticsBatchV1;
    try {
      return await config.sender(batch) ? 'sent' : 'retry';
    } catch {
      return 'retry';
    }
  });

  const nextObservationChunk = (
    pending: Array<[string, StatisticsAggregate]>,
    batchId: string,
    createdAt: number,
  ): Array<[string, StatisticsAggregate]> => {
    const selected: Array<[string, StatisticsAggregate]> = [];
    for (const item of pending) {
      const candidate: StatisticsBatchV1 = {
        schemaVersion: 1,
        batchId,
        createdAt,
        facts: [],
        observations: [...selected.map(([, value]) => value), item[1]],
      };
      if (byteLength(candidate) > maxBatchBytes) break;
      selected.push(item);
    }
    return selected;
  };

  const flushAggregates = async (): Promise<StatisticsRecordResult> => {
    if (!aggregates.size) return 'accepted';
    let finalResult: StatisticsRecordResult = 'accepted';
    while (aggregates.size) {
      const pending = [...aggregates.entries()];
      const batchId = makeId();
      if (!UUIDISH_RE.test(batchId)) return 'dropped_capacity';
      const createdAt = now();
      const chunk = nextObservationChunk(pending, batchId, createdAt);
      if (!chunk.length) return 'dropped_oversize';
      const batch: StatisticsBatchV1 = {
        schemaVersion: 1,
        batchId,
        createdAt,
        facts: [],
        observations: chunk.map(([, value]) => value),
      };
      const result = await enqueueBatch(batch, `stats:obs:${batchId}`);
      if (result !== 'accepted') return result;
      for (const [key] of chunk) aggregates.delete(key);
      finalResult = result;
    }
    return finalResult;
  };

  const record = (input: StatisticsInput): Promise<StatisticsRecordResult> => {
    const task = serial.then(async () => {
      const rule = catalog[input.eventName];
      if (rule?.consentRequired && input.consent !== 'granted') return 'dropped_no_consent';
      const normalized = normalizeInput(input, now(), catalog);
      if (!normalized) return 'dropped_invalid';

      if (normalized.lane === 'product_fact') {
        const fact = makeFact(normalized);
        const batchId = `fact-${makeId()}`;
        if (!UUIDISH_RE.test(batchId)) return 'dropped_capacity';
        const batch: StatisticsBatchV1 = {
          schemaVersion: 1,
          batchId,
          createdAt: now(),
          facts: [fact],
          observations: [],
        };
        return enqueueBatch(batch, `stats:fact:${fact.idempotencyKey}`);
      }

      const key = aggregationKey(normalized);
      const existing = aggregates.get(key);
      if (existing) {
        existing.lastObservedAt = Math.max(existing.lastObservedAt, Number(normalized.occurredAt || 0));
        existing.firstObservedAt = Math.min(existing.firstObservedAt, Number(normalized.occurredAt || 0));
        existing.observationCount = clampInteger(existing.observationCount + 1, 1, MAX_OBSERVATION_COUNT);
        existing.counters = mergeNumbers(existing.counters, normalized.counters, 'sum');
        existing.maxima = mergeNumbers(existing.maxima, normalized.maxima, 'max');
        return 'accepted';
      }

      if (aggregates.size >= maxAccumulatorEntries) {
        const flushed = await flushAggregates();
        if (flushed !== 'accepted') return 'dropped_capacity';
      }
      const aggregate = makeAggregate(normalized);
      if (byteLength(aggregate) > maxBatchBytes) return 'dropped_oversize';
      aggregates.set(key, aggregate);
      return 'accepted';
    });
    serial = task.catch(() => undefined);
    return task;
  };

  const flush = (): Promise<number> => {
    const task = serial.then(async () => {
      await flushAggregates();
      return flushOutbox();
    });
    serial = task.catch(() => undefined);
    return task;
  };

  return {
    record,
    flush,
    inspectPending(): StatisticsAggregate[] {
      return [...aggregates.values()].map((value) => structuredClone(value));
    },
    channel: CHANNEL,
  };
}
