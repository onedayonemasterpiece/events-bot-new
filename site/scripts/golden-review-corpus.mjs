import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
export const GOLDEN_CORPUS_PATH = join(scriptDir, 'golden-review-corpus.v1.json');

const REQUIRED_STRESS_TAGS = [
  'media-cover',
  'media-contain',
  'media-multiple',
  'media-missing',
  'media-error',
  'long-copy',
  'admission-free',
  'admission-ticket',
  'admission-registration',
  'admission-phone',
  'admission-source',
  'calendar-single',
  'calendar-range',
  'lifecycle-cancelled',
  'lifecycle-rescheduled',
];

function fail(message) {
  throw new Error(`Golden review corpus contract: ${message}`);
}

function assert(condition, message) {
  if (!condition) fail(message);
}

function unique(values) {
  return new Set(values).size === values.length;
}

function addUtcDays(value, days) {
  const date = new Date(`${value}T12:00:00Z`);
  assert(Number.isFinite(date.getTime()), `invalid date ${value}`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function weekday(value) {
  return new Date(`${value}T12:00:00Z`).getUTCDay();
}

function sameMembers(left, right) {
  if (left.length !== right.length) return false;
  const a = [...left].sort((x, y) => x - y);
  const b = [...right].sort((x, y) => x - y);
  return a.every((value, index) => value === b[index]);
}

export function sha256(value) {
  return createHash('sha256').update(value).digest('hex');
}

export function loadGoldenCorpus(path = GOLDEN_CORPUS_PATH) {
  const raw = readFileSync(path);
  let corpus;
  try {
    corpus = JSON.parse(raw.toString('utf8'));
  } catch (error) {
    fail(`cannot parse ${path}: ${error?.message || error}`);
  }
  validateGoldenCorpus(corpus);
  return { corpus, raw, digest: sha256(raw) };
}

export function validateGoldenCorpus(corpus) {
  assert(corpus && typeof corpus === 'object', 'root must be an object');
  assert(corpus.schema_version === 'kenigevents.golden-review-corpus.v1', 'unexpected schema_version');
  assert(/^golden-review-[a-z0-9-]+-v\d+$/u.test(String(corpus.corpus_id || '')), 'invalid corpus_id');

  const clock = corpus.frozen_clock || {};
  assert(clock.timezone === 'Europe/Kaliningrad', 'timezone must be Europe/Kaliningrad');
  assert(clock.current_date === clock.friday, 'current_date must be the frozen Friday');
  assert(addUtcDays(clock.friday, 1) === clock.saturday, 'Saturday must immediately follow Friday');
  assert(addUtcDays(clock.saturday, 1) === clock.sunday, 'Sunday must immediately follow Saturday');
  assert(weekday(clock.friday) === 5, `${clock.friday} is not Friday`);
  assert(weekday(clock.saturday) === 6, `${clock.saturday} is not Saturday`);
  assert(weekday(clock.sunday) === 0, `${clock.sunday} is not Sunday`);
  assert(Number.isFinite(Date.parse(clock.reference_iso)), 'reference_iso must be an ISO timestamp');

  const lockedAt = Date.parse(corpus.stability?.locked_at || '');
  const appendOnlyUntil = Date.parse(corpus.stability?.append_only_until || '');
  assert(Number.isFinite(lockedAt) && Number.isFinite(appendOnlyUntil), 'stability timestamps are required');
  assert(appendOnlyUntil - lockedAt === 14 * 24 * 60 * 60 * 1000, 'append-only window must be exactly two weeks');
  assert(/immutable/u.test(String(corpus.stability?.policy || '')), 'stability policy must state immutability');

  const events = corpus.events;
  assert(Array.isArray(events), 'events must be an array');
  assert(events.length === 16, `expected 16 target specimens, received ${events.length}`);
  assert(unique(events.map((event) => event.id)), 'event ids must be unique');
  assert(unique(events.map((event) => event.slug)), 'event slugs must be unique');
  assert(unique(events.map((event) => event.source_identity)), 'source identities must be unique');

  const minimumId = Number(corpus.reserved_event_ids?.minimum);
  const maximumId = Number(corpus.reserved_event_ids?.maximum);
  assert(Number.isInteger(minimumId) && Number.isInteger(maximumId) && minimumId <= maximumId, 'invalid reserved id range');

  const assets = corpus.pinned_assets;
  assert(Array.isArray(assets) && assets.length > 0, 'pinned_assets are required');
  assert(unique(assets.map((asset) => asset.id)), 'pinned asset ids must be unique');
  assert(unique(assets.map((asset) => asset.path)), 'pinned asset paths must be unique');
  const assetIds = new Set(assets.map((asset) => asset.id));
  for (const asset of assets) {
    assert(/^\/assets\//u.test(String(asset.path || '')), `asset ${asset.id} must use a public /assets path`);
    assert(/^[0-9a-f]{40}$/u.test(String(asset.git_blob_sha || '')), `asset ${asset.id} needs a Git blob SHA`);
    assert(asset.kind === 'image' || asset.kind === 'svg', `asset ${asset.id} has invalid kind`);
    if (asset.sha256 !== undefined) assert(/^[0-9a-f]{64}$/u.test(asset.sha256), `asset ${asset.id} has invalid SHA-256`);
  }

  for (const event of events) {
    assert(Number.isInteger(event.id) && event.id >= minimumId && event.id <= maximumId, `event ${event.id} is outside the reserved range`);
    assert(/^golden-[a-z0-9-]+-\d+$/u.test(String(event.slug || '')), `event ${event.id} has an invalid slug`);
    assert(event.source_identity === `golden:n0:${event.id}`, `event ${event.id} has an unstable source identity`);
    assert([clock.friday, clock.saturday, clock.sunday].includes(event.date), `event ${event.id} is outside the three-day corpus`);
    assert(event.event_type && event.event_type.toLocaleLowerCase('ru-RU') !== 'выставка', `event ${event.id} must stay on date-listing surfaces`);
    assert(!event.topics?.includes('EXHIBITIONS'), `event ${event.id} may not become an exhibition lookalike`);
    assert(Array.isArray(event.stress_tags) && event.stress_tags.length > 0, `event ${event.id} lacks stress tags`);
    assert(event.admission && ['free', 'ticket', 'registration', 'phone', 'source'].includes(event.admission.kind), `event ${event.id} has invalid admission`);
    if (event.media) {
      const ids = event.media.asset_ids || [];
      const paths = event.media.asset_paths || [];
      assert(ids.length + paths.length > 0, `event ${event.id} media has no asset`);
      for (const id of ids) assert(assetIds.has(id), `event ${event.id} references unknown asset ${id}`);
      if (paths.length) assert(event.media.intentional_missing === true, `event ${event.id} raw asset paths must be intentional missing-media probes`);
      assert(['cover', 'contain'].includes(event.media.fit), `event ${event.id} media fit is invalid`);
      assert(['visual_only', 'ocr_text', 'unknown'].includes(event.media.image_text_mode), `event ${event.id} image_text_mode is invalid`);
      assert(['pending', 'classified', 'error', 'stale'].includes(event.media.semantic_status), `event ${event.id} semantic status is invalid`);
    }
  }

  const byDate = (date) => events.filter((event) => event.date === date).map((event) => event.id);
  const fridayIds = byDate(clock.friday);
  const saturdayIds = byDate(clock.saturday);
  const sundayIds = byDate(clock.sunday);
  const target = corpus.density?.target || {};
  const minimum = corpus.density?.minimum || {};
  assert(fridayIds.length === target.friday && fridayIds.length >= minimum.friday, 'Friday density does not meet 5/4 target/minimum');
  assert(saturdayIds.length === target.saturday && saturdayIds.length >= minimum.saturday, 'Saturday density does not meet 6/5 target/minimum');
  assert(sundayIds.length === target.sunday && sundayIds.length >= minimum.sunday, 'Sunday density does not meet 5/4 target/minimum');

  const routes = corpus.route_contract || {};
  assert(routes.today?.path === '/segodnya/' && routes.today.date === clock.friday, 'today route contract is invalid');
  assert(routes.tomorrow?.path === '/zavtra/' && routes.tomorrow.date === clock.saturday, 'tomorrow route contract is invalid');
  assert(routes.sunday?.path === `/date-${clock.sunday}/` && routes.sunday.date === clock.sunday, 'Sunday route contract is invalid');
  assert(routes.weekend?.path === '/vyhodnye/', 'current weekend path is invalid');
  assert(routes.dated_weekend?.path === `/vyhodnye/${clock.saturday}/`, 'dated weekend path is invalid');
  assert(routes.free_collection?.path === '/podborki/besplatnye-sobytiya/', 'free collection path is invalid');
  assert(sameMembers(routes.today.event_ids, fridayIds), 'today route ids must be the Friday ids');
  assert(sameMembers(routes.tomorrow.event_ids, saturdayIds), 'tomorrow route ids must be the Saturday ids');
  assert(sameMembers(routes.sunday.event_ids, sundayIds), 'Sunday route ids must be the Sunday ids');
  const weekendIds = [...saturdayIds, ...sundayIds];
  assert(sameMembers(routes.weekend.event_ids, weekendIds), 'weekend must reuse the exact Saturday/Sunday occurrences');
  assert(sameMembers(routes.dated_weekend.event_ids, weekendIds), 'dated weekend must reuse the exact Saturday/Sunday occurrences');
  const freeIds = events
    .filter((event) => event.lifecycle_status !== 'cancelled' && event.admission.is_free)
    .map((event) => event.id);
  assert(sameMembers(routes.free_collection.event_ids, freeIds), 'free collection ids must match active free/registration specimens');

  const stressTags = new Set(events.flatMap((event) => event.stress_tags));
  for (const tag of REQUIRED_STRESS_TAGS) assert(stressTags.has(tag), `required stress tag ${tag} is missing`);
  return corpus;
}

function displayDate(value) {
  return new Intl.DateTimeFormat('ru-RU', {
    day: 'numeric',
    month: 'long',
    timeZone: 'Europe/Kaliningrad',
  }).format(new Date(`${value}T12:00:00+02:00`));
}

function materializeMedia(spec, assetMap) {
  if (!spec.media) return { imageUrl: null, assets: [] };
  const media = spec.media;
  const resolved = [
    ...(media.asset_ids || []).map((id) => {
      const asset = assetMap.get(id);
      assert(asset, `event ${spec.id} references missing asset ${id}`);
      return asset;
    }),
    ...(media.asset_paths || []).map((path) => ({
      id: `intentional-missing:${spec.id}:${path}`,
      path,
      width: media.width,
      height: media.height,
    })),
  ];
  const focal = String(media.object_position || '50% 50%').match(/([\d.]+)%\s+([\d.]+)%/u);
  const focalPoint = focal
    ? { x: Number(focal[1]) / 100, y: Number(focal[2]) / 100 }
    : { x: 0.5, y: 0.5 };
  const assets = resolved.map((asset) => ({
    src: asset.path,
    width: Number(asset.width || media.width || 1000),
    height: Number(asset.height || media.height || 1000),
    alt: spec.title,
    image_text_mode: media.image_text_mode,
    media_role: media.media_role,
    media_role_confidence: 1,
    media_semantic_status: media.semantic_status,
    image_kind: media.image_text_mode === 'ocr_text' ? 'poster' : 'photo',
    thumbnail_sources: [],
    ocr_boxes: media.image_text_mode === 'ocr_text'
      ? [{ x: 0.08, y: 0.08, w: 0.84, h: 0.24, confidence: 1 }]
      : [],
    face_boxes: [],
    saliency_boxes: [],
    focal_point: focalPoint,
    recommended_object_position: media.object_position || '50% 50%',
    recommended_hero_fit: media.fit,
    safe_crop: Boolean(media.safe_crop),
    ...(asset.sha256 ? {
      current_pixel_sha256: asset.sha256,
      geometry_pixel_sha256: asset.sha256,
      geometry_status: 'classified',
      geometry_coordinate_space: 'normalized_0_1',
      geometry_source_width: Number(asset.width),
      geometry_source_height: Number(asset.height),
      geometry_reason_code: 'golden_pinned_asset',
    } : {}),
    quality_score: media.intentional_missing ? 0 : 100,
  }));
  return { imageUrl: assets[0]?.src || null, assets, focalPoint };
}

function materializeEvent(spec, corpus, assetMap) {
  const lifecycleStatus = spec.lifecycle_status || 'active';
  const statusLabel = spec.status_label || 'Актуально';
  const startsAt = spec.time ? `${spec.date}T${spec.time}:00+02:00` : null;
  const endDate = spec.end_date || null;
  const endAt = spec.end_time
    ? `${spec.end_date || spec.date}T${spec.end_time}:00+02:00`
    : null;
  const media = materializeMedia(spec, assetMap);
  const likes = spec.id % 19;
  const ticketHref = spec.admission.kind === 'phone' ? 'tel:+74012000000' : null;
  const descriptionHtml = spec.description_html || `<p>${spec.summary}</p>`;
  return {
    id: spec.id,
    title: spec.title,
    slug: spec.slug,
    event_type: spec.event_type,
    festival: spec.event_type === 'Фестиваль' ? 'Golden Review' : null,
    organizer_names: ['Golden Corpus'],
    participants: [],
    status_label: statusLabel,
    lifecycle_status: lifecycleStatus,
    starts_at: startsAt,
    start_date: spec.date,
    start_time: spec.time,
    end_date: endDate,
    end_at: endAt,
    time_range_end: spec.end_time,
    duration_forecast_minutes: startsAt && endAt
      ? Math.max(1, Math.round((Date.parse(endAt) - Date.parse(startsAt)) / 60000))
      : null,
    transport_end_basis: startsAt && endAt ? 'source_duration' : undefined,
    timezone: corpus.frozen_clock.timezone,
    display_date: displayDate(spec.date),
    display_time: spec.time,
    city: spec.city,
    venue_name: spec.venue,
    address: null,
    map_query: [spec.venue, spec.city].filter(Boolean).join(', '),
    ticket: {
      kind: spec.admission.kind,
      label: spec.admission.label,
      href: ticketHref,
      status: lifecycleStatus === 'cancelled' ? 'cancelled' : 'confirmed',
      is_free: Boolean(spec.admission.is_free),
      price_label: spec.admission.price_label,
    },
    age_restriction: spec.age_restriction || '6+',
    age_restriction_status: 'declared',
    age_restriction_provenance: spec.source_identity,
    age_restriction_decision_version: corpus.schema_version,
    age_recommendation: spec.age_restriction || '6+',
    age_recommendation_label: spec.age_restriction || '6+',
    source_url: null,
    source_urls: [],
    source_count: 1,
    question_cta: null,
    telegraph_url: null,
    image_url: media.imageUrl,
    image_alt: spec.title,
    image_text_mode: spec.media?.image_text_mode || 'unknown',
    image_media_role: spec.media?.media_role,
    image_assets: media.assets,
    video_assets: [],
    face_boxes: [],
    valuable_region: null,
    ocr_boxes: media.assets[0]?.ocr_boxes || [],
    focal_point: media.focalPoint,
    image_object_position: spec.media?.object_position || null,
    safe_crop: Boolean(spec.media?.safe_crop),
    summary: spec.summary,
    meta_description: spec.summary.slice(0, 180),
    description_html: descriptionHtml,
    topics: spec.topics,
    likes_count: likes,
    source_likes_count: likes,
    service_likes_count: 0,
    source_views_count: 100 + spec.id % 37,
    source_engagement_sources_count: 1,
    shares_count: spec.id % 5,
    popularity_reason_codes: [],
    popularity_signal_score: likes,
    pushkin_card: Boolean(spec.pushkin_card),
    other_date_ids: [],
    source_prod_id: spec.id,
    data_quality_notes: [spec.source_identity, ...spec.stress_tags.map((tag) => `golden:${tag}`)],
    updated_at: corpus.frozen_clock.reference_iso,
  };
}

export function materializeGoldenPreviewData(corpus, basePreviewData) {
  validateGoldenCorpus(corpus);
  assert(basePreviewData && Array.isArray(basePreviewData.events), 'base preview data must contain events');
  const assetMap = new Map(corpus.pinned_assets.map((asset) => [asset.id, asset]));
  const reserved = corpus.reserved_event_ids;
  const retainedRealCanaries = basePreviewData.events.filter((event) => {
    const id = Number(event.id);
    if (id >= reserved.minimum && id <= reserved.maximum) return false;
    const finalDate = String(event.end_date || event.start_date || '');
    return finalDate < corpus.frozen_clock.friday;
  });
  const goldenEvents = corpus.events.map((event) => materializeEvent(event, corpus, assetMap));
  const events = [...retainedRealCanaries, ...goldenEvents].sort((left, right) => {
    const a = left.starts_at || left.start_date;
    const b = right.starts_at || right.start_date;
    return String(a).localeCompare(String(b)) || Number(left.id) - Number(right.id);
  });
  assert(unique(events.map((event) => Number(event.id))), 'materialized preview contains duplicate ids');
  return {
    ...basePreviewData,
    build: {
      ...(basePreviewData.build || {}),
      generated_at: corpus.frozen_clock.reference_iso,
      source: `golden-review-corpus:${corpus.corpus_id}`,
      current_date: corpus.frozen_clock.current_date,
      notes: [
        ...(Array.isArray(basePreviewData.build?.notes) ? basePreviewData.build.notes : []),
        `Golden corpus ${corpus.corpus_id}`,
        `Frozen clock ${corpus.frozen_clock.reference_iso}`,
        'Ordinary Astro routes and canonical component roots; no second UI implementation.',
      ],
    },
    events,
  };
}

export function goldenEventMap(corpus) {
  validateGoldenCorpus(corpus);
  return new Map(corpus.events.map((event) => [event.id, event]));
}
