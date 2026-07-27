const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/u;
const SAFE_ID = /^[a-z0-9][a-z0-9._:-]{2,127}$/iu;
const MAX_MANIFEST_ITEMS = 120;
const MAX_FEED_ITEMS = 30;

function stringField(value) {
  return typeof value === 'string' && value.trim().length > 0;
}

function validTimestamp(value) {
  return stringField(value) && Number.isFinite(Date.parse(value));
}

function validEvent(event, expectedId, today) {
  if (!event || typeof event !== 'object') return false;
  const endDate = stringField(event.end_date) ? event.end_date : event.start_date;
  return event.id === expectedId
    && stringField(event.slug)
    && stringField(event.title)
    && stringField(event.start_date)
    && ISO_DATE.test(event.start_date)
    && stringField(endDate)
    && endDate >= today
    && event.lifecycle_status === 'active'
    && Boolean(event.ticket && typeof event.ticket === 'object')
    && Array.isArray(event.image_assets)
    && Array.isArray(event.topics);
}

function validManifestEnvelope(manifest) {
  if (!manifest || typeof manifest !== 'object') return false;
  return stringField(manifest.schema_version)
    && stringField(manifest.build_id)
    && validTimestamp(manifest.generated_at)
    && stringField(manifest.source_snapshot_id)
    && stringField(manifest.hash)
    && stringField(manifest.taxonomy_version)
    && stringField(manifest.policy_version)
    && stringField(manifest.embedding_model)
    && stringField(manifest.revision)
    && Number.isInteger(manifest.dim)
    && manifest.dim > 0
    && stringField(manifest.doc_kind)
    && stringField(manifest.document_version)
    && stringField(manifest.prototype_bank_hash)
    && stringField(manifest.classifier_hash)
    && Boolean(manifest.quality_gate && typeof manifest.quality_gate === 'object')
    && typeof manifest.quality_gate.status === 'string'
    && Boolean(manifest.quality_gate.metrics && typeof manifest.quality_gate.metrics === 'object')
    && Array.isArray(manifest.items)
    && manifest.items.length <= MAX_MANIFEST_ITEMS;
}

function validManifestItem(item) {
  if (!item || typeof item !== 'object') return false;
  return Number.isInteger(item.event_id)
    && item.event_id > 0
    && SAFE_ID.test(String(item.concept_id || ''))
    && Number.isInteger(item.representative_event_id)
    && stringField(item.tier)
    && Number.isFinite(item.unusual_score)
    && item.unusual_score >= 0
    && item.unusual_score <= 1
    && Number.isFinite(item.confidence)
    && item.confidence >= 0
    && item.confidence <= 1
    && Array.isArray(item.families)
    && Array.isArray(item.reason_codes)
    && Array.isArray(item.prototype_evidence)
    && typeof item.notify_eligible === 'boolean'
    && stringField(item.content_hash)
    && ISO_DATE.test(String(item.date || ''))
    && item.lifecycle === 'active';
}

function rolloutBaseline(manifest) {
  const candidate = manifest.rollout_baseline_at
    || manifest.notification_baseline_at
    || manifest.rollout_baseline
    || manifest.quality_gate?.rollout_baseline_at
    || null;
  return validTimestamp(candidate) ? candidate : null;
}

export function resolveUnusualFeed(raw, catalog, today) {
  const unavailable = (status = 'unavailable') => ({
    approved:false,
    status,
    buildId:null,
    generatedAt:null,
    baselineAt:null,
    items:[],
    unreadCandidates:[],
  });
  if (!validManifestEnvelope(raw)) return unavailable('invalid_manifest');
  if (raw.quality_gate.status !== 'approved') return unavailable(raw.quality_gate.status || 'unavailable');
  const byId = new Map(catalog.map((event) => [event.id, event]));
  const concepts = new Set();
  const events = new Set();
  const resolved = [];

  for (const item of raw.items) {
    if (!validManifestItem(item) || concepts.has(item.concept_id) || events.has(item.event_id)) continue;
    const candidate = validEvent(item.event_snapshot, item.event_id, today)
      ? item.event_snapshot
      : byId.get(item.event_id);
    if (!validEvent(candidate, item.event_id, today)) continue;
    concepts.add(item.concept_id);
    events.add(item.event_id);
    resolved.push({
      conceptId:item.concept_id,
      tier:item.tier,
      score:item.unusual_score,
      confidence:item.confidence,
      families:item.families.filter(stringField).slice(0, 8),
      reasonCodes:item.reason_codes.filter(stringField).slice(0, 12),
      firstPublishedAt:validTimestamp(item.first_published_at) ? item.first_published_at : null,
      notifyEligible:item.notify_eligible,
      event:candidate,
    });
    if (resolved.length >= MAX_FEED_ITEMS) break;
  }

  const baselineAt = rolloutBaseline(raw);
  const unreadCandidates = baselineAt ? resolved
    .filter((item) => item.tier === 'core_unusual'
      && item.notifyEligible
      && Boolean(item.firstPublishedAt)
      && Date.parse(item.firstPublishedAt) > Date.parse(baselineAt))
    .map((item) => ({ conceptId:item.conceptId, firstPublishedAt:item.firstPublishedAt })) : [];
  return {
    approved:true,
    status:'approved',
    buildId:raw.build_id,
    generatedAt:raw.generated_at,
    baselineAt,
    items:resolved,
    unreadCandidates,
  };
}
