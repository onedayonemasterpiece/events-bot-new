import assert from 'node:assert/strict';
import test from 'node:test';

import {
  createUnifiedStatisticsClient,
  type StatisticsBatchV1,
  type StatisticsEventCatalog,
  type StatisticsOutboxRecord,
} from './unifiedStatisticsClient.ts';

class FakeOutbox {
  records = new Map<string, StatisticsOutboxRecord>();

  async enqueue(record: StatisticsOutboxRecord) {
    if (this.records.has(record.id)) return true;
    this.records.set(record.id, structuredClone(record));
    return true;
  }

  async flush(sender: (record: StatisticsOutboxRecord) => Promise<'sent' | 'retry' | 'drop' | 'skip'>) {
    let sent = 0;
    for (const [id, record] of [...this.records.entries()]) {
      const result = await sender(record);
      if (result === 'sent' || result === 'drop') this.records.delete(id);
      if (result === 'sent') sent += 1;
    }
    return sent;
  }
}

function fixture(sender: (batch: StatisticsBatchV1) => Promise<boolean> = async () => true) {
  const outbox = new FakeOutbox();
  let tick = 1_000;
  let id = 0;
  const client = createUnifiedStatisticsClient({
    outbox,
    sender,
    now: () => ++tick,
    makeId: () => `batch-000${++id}`,
  });
  return { outbox, client };
}

test('weak product observation requires explicit consent and never reaches the outbox otherwise', async () => {
  const { client, outbox } = fixture();
  assert.equal(await client.record({
    eventName:'card_visible',
    lane:'product_observation',
    source:'browser_observation',
    consent:'not_required',
    surface:'today',
    entity:{kind:'event', id:'event-6408'},
  }), 'dropped_no_consent');
  assert.equal(client.inspectPending().length, 0);
  assert.equal(outbox.records.size, 0);
});

test('repeated observations aggregate into one compact session fact', async () => {
  const batches: StatisticsBatchV1[] = [];
  const { client } = fixture(async (batch) => {
    batches.push(structuredClone(batch));
    return true;
  });
  for (const [position, dwell] of [[2, 600], [5, 900], [4, 400]] as const) {
    assert.equal(await client.record({
      eventName:'card_visible',
      lane:'product_observation',
      source:'browser_observation',
      consent:'granted',
      occurredAt:100 + position,
      sessionId:'session-12345678',
      actorKey:'actor-hmac-12345678',
      surface:'today',
      entity:{ kind:'event', id:'event-6408' },
      release:{ releaseSha:'abcdef12', pageRevision:'today-v4' },
      dimensions:{ card_density:'compact', card_family:'listing_compact' },
      counters:{ exposures:1 },
      maxima:{ position_bucket:position, dwell_bucket:dwell },
    }), 'accepted');
  }
  assert.equal(client.inspectPending().length, 1);
  const pending = client.inspectPending()[0];
  assert.equal(pending.observationCount, 3);
  assert.equal(pending.counters?.exposures, 3);
  assert.equal(pending.maxima?.position_bucket, 5);
  assert.equal(pending.maxima?.dwell_bucket, 900);

  assert.equal(await client.flush(), 1);
  assert.equal(batches.length, 1);
  assert.equal(batches[0].observations.length, 1);
  assert.equal(batches[0].facts.length, 0);
  assert.equal(client.inspectPending().length, 0);
});

test('catalog rejects unknown events and unknown fields', async () => {
  const { client } = fixture();
  assert.equal(await client.record({
    eventName:'made_up_event',
    lane:'product_observation',
    source:'browser_observation',
    consent:'granted',
  }), 'dropped_invalid');
  assert.equal(await client.record({
    eventName:'card_visible',
    lane:'product_observation',
    source:'browser_observation',
    consent:'granted',
    entity:{kind:'event', id:'event-6408'},
    dimensions:{ invented_dimension:'x' },
  }), 'dropped_invalid');
});

test('unknown top-level, entity and release fields fail closed', async () => {
  const { client } = fixture();
  const base = {
    eventName:'card_visible',
    lane:'product_observation' as const,
    source:'browser_observation' as const,
    consent:'granted' as const,
  };
  assert.equal(await client.record({ ...base, rogue:'x' } as never), 'dropped_invalid');
  assert.equal(await client.record({ ...base, entity:{kind:'event', id:'event-6408', email:'x@example.test'} } as never), 'dropped_invalid');
  assert.equal(await client.record({ ...base, release:{releaseSha:'abcdef12', rawUrl:'https://example.test'} } as never), 'dropped_invalid');
});

test('sensitive or unbounded attributes fail closed', async () => {
  const { client } = fixture();
  assert.equal(await client.record({
    eventName:'hero_talk_state',
    lane:'product_observation',
    source:'browser_observation',
    consent:'granted',
    dimensions:{ target_url:'https://example.test/private?token=x' },
  }), 'dropped_invalid');
  assert.equal(await client.record({
    eventName:'hero_talk_state',
    lane:'product_observation',
    source:'browser_observation',
    consent:'granted',
    dimensions:{ text:'full rendered Hero Talk copy' },
  }), 'dropped_invalid');
  assert.equal(client.inspectPending().length, 0);
});

test('strong product facts require an authoritative receipt and stable idempotency key', async () => {
  const { client, outbox } = fixture();
  const base = {
    eventName:'action_receipt',
    lane:'product_fact' as const,
    consent:'not_required' as const,
    occurredAt:10,
    surface:'event_detail',
    entity:{ kind:'event', id:'event-6408' },
    dimensions:{action_kind:'calendar_save', stage:'accepted'},
  };
  assert.equal(await client.record({
    ...base,
    source:'browser_observation',
    idempotencyKey:'receipt-12345678',
  }), 'dropped_invalid');
  assert.equal(await client.record({
    ...base,
    source:'authoritative_receipt',
    idempotencyKey:'short',
  }), 'dropped_invalid');
  assert.equal(await client.record({
    ...base,
    source:'authoritative_receipt',
    idempotencyKey:'receipt-12345678',
  }), 'accepted');
  assert.equal(outbox.records.size, 1);
  assert.ok(outbox.records.has('stats:fact:receipt-12345678'));
});

test('operational telemetry cannot carry an actor key', async () => {
  const { client } = fixture();
  assert.equal(await client.record({
    eventName:'ingest_health',
    lane:'operational',
    source:'service_runtime',
    consent:'not_required',
    actorKey:'actor-hmac-12345678',
    counters:{ retry_batches:1 },
  }), 'dropped_invalid');
  assert.equal(await client.record({
    eventName:'ingest_health',
    lane:'operational',
    source:'service_runtime',
    consent:'not_required',
    dimensions:{ route_class:'relay' },
    counters:{ retry_batches:1 },
  }), 'accepted');
});

test('failed delivery remains in the bounded outbox and succeeds on a later flush', async () => {
  let healthy = false;
  const { client, outbox } = fixture(async () => healthy);
  await client.record({
    eventName:'description_checkpoint',
    lane:'product_observation',
    source:'browser_observation',
    consent:'granted',
    sessionId:'session-12345678',
    surface:'event_detail',
    entity:{ kind:'event', id:'event-6408' },
    maxima:{ checkpoint:75 },
  });
  assert.equal(await client.flush(), 0);
  assert.equal(outbox.records.size, 1);
  healthy = true;
  assert.equal(await client.flush(), 1);
  assert.equal(outbox.records.size, 0);
});

test('accumulator capacity flushes before accepting a new key', async () => {
  const batches: StatisticsBatchV1[] = [];
  const outbox = new FakeOutbox();
  let id = 0;
  const catalog: StatisticsEventCatalog = {
    first_event:{lane:'product_observation', source:'browser_observation', consentRequired:true},
    second_event:{lane:'product_observation', source:'browser_observation', consentRequired:true},
    third_event:{lane:'product_observation', source:'browser_observation', consentRequired:true},
  };
  const client = createUnifiedStatisticsClient({
    outbox,
    catalog,
    sender:async (batch) => { batches.push(batch); return true; },
    now:() => 100,
    makeId:() => `batch-000${++id}`,
    maxAccumulatorEntries:2,
  });
  for (const eventName of ['first_event', 'second_event', 'third_event']) {
    assert.equal(await client.record({
      eventName,
      lane:'product_observation',
      source:'browser_observation',
      consent:'granted',
      surface:'home',
    }), 'accepted');
  }
  assert.equal(client.inspectPending().length, 1);
  assert.equal(outbox.records.size, 1);
  assert.equal(await client.flush(), 2);
  assert.equal(batches.length, 2);
});

test('large aggregate sets are split into several payload-safe batches', async () => {
  const batches: StatisticsBatchV1[] = [];
  const outbox = new FakeOutbox();
  let id = 0;
  const catalog: Record<string, {lane:'product_observation'; source:'browser_observation'; consentRequired:true; dimensions:['state']}> = {};
  for (let index = 0; index < 20; index += 1) catalog[`event_${index}`] = {lane:'product_observation', source:'browser_observation', consentRequired:true, dimensions:['state']};
  const client = createUnifiedStatisticsClient({
    outbox,
    catalog,
    sender:async (batch) => { batches.push(structuredClone(batch)); return true; },
    now:() => 100,
    makeId:() => `batch-${String(++id).padStart(8, '0')}`,
    maxBatchBytes:1024,
  });
  for (let index = 0; index < 20; index += 1) {
    assert.equal(await client.record({
      eventName:`event_${index}`,
      lane:'product_observation',
      source:'browser_observation',
      consent:'granted',
      surface:'home',
      dimensions:{state:`state-${index}`},
    }), 'accepted');
  }
  assert.ok((await client.flush()) > 1);
  assert.ok(batches.length > 1);
  for (const batch of batches) assert.ok(new TextEncoder().encode(JSON.stringify(batch)).byteLength <= 1024);
});
