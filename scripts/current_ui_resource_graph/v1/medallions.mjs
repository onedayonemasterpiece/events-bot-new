import { createHash } from 'node:crypto';

export const MEDALLION_SCHEMA = 'current_ui_medallion_decoder_v1';
export const MEDALLION_LANE_ID = 'medallion-as-is';
export const MEDALLION_SOURCE_SHA = 'ef7aa62e45c60f7a12da6160f490719c0721ec03';
export const MEDALLION_LOGICAL_PATH = 'src/components/EventTokenMedallions.astro';

export const MEDALLION_LAYOUTS = Object.freeze(['inline', 'desktop-slots']);
export const MEDALLION_SLOTS = Object.freeze(['top', 'inline']);
export const MEDALLION_ROLES = Object.freeze(['main', 'secondary']);
export const MEDALLION_KINDS = Object.freeze(['organizer', 'source', 'program', 'pushkin', 'badge', 'pill']);
export const MEDALLION_IDENTITY_CATEGORIES = Object.freeze(['venue_brand', 'festival_brand', 'festival', 'organizer']);
export const MEDALLION_IDENTITY_RESOLUTIONS = Object.freeze(['resolved', 'conflicting_source_identity', 'ambiguous_venue_identity']);
export const MEDALLION_LIMITS = Object.freeze({ resolved_identities: 3, visible_tokens: 6, top_slot_tokens: 1 });
export const MEDALLION_RESPONSIVE_CONTEXTS = Object.freeze([
  { id: 'responsive.medallions-desktop-min-1024', query: '(min-width:1024px)', boundary: 1024, probe_widths: [1023, 1024], owner: 'src/components/DesktopEventPage.astro' },
  { id: 'responsive.medallions-desktop-compact-height-720', query: '(min-width:1024px) and (max-height:720px)', boundary: 720, probe_heights: [720, 721], owner: 'src/components/DesktopEventPage.astro' },
  { id: 'responsive.medallions-desktop-band-max-1279', query: '(min-width:1024px) and (max-width:1279px)', boundary: 1280, probe_widths: [1279, 1280], owner: 'src/components/DesktopEventPage.astro', scope: 'consumer-composition-context' },
  { id: 'responsive.medallions-wide-min-1440', query: '(min-width:1440px)', boundary: 1440, probe_widths: [1439, 1440], owner: 'src/components/DesktopEventPage.astro', scope: 'consumer-composition-context' },
]);

const CIRCULAR_KINDS = new Set(['organizer', 'source', 'program', 'badge']);
const DESKTOP_KINDS = new Set(['organizer', 'source', 'program', 'pushkin', 'badge']);
const LOCAL_ASSET = /^\/(?!\/)[^?#\s]+$/u;

function sha(value) { return createHash('sha256').update(String(value)).digest('hex'); }
function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map((key) => [key, stable(value[key])]));
  return value;
}
function clone(value) { return JSON.parse(JSON.stringify(stable(value))); }
function assertEnum(value, values, name) {
  if (!values.includes(value)) throw new Error(`Invalid medallion ${name}: ${value}`);
}
function assertArray(value, name, maximum = 64) {
  if (!Array.isArray(value)) throw new Error(`Medallion ${name} must be an array`);
  if (value.length > maximum) throw new Error(`Medallion ${name} exceeds bounded input limit ${maximum}`);
}
function componentId(path = MEDALLION_LOGICAL_PATH) { return `component.${sha(path).slice(0, 16)}`; }
function planId(prefix, key) { return `specimen-plan.medallions-${prefix}-${String(key).replace(/[^a-z0-9-]+/giu, '-').toLowerCase()}`; }
function notMerged(record) {
  return { ...record, decision: 'NOT_MERGED', recommendation: 'unresolved', normalization_allowed: false };
}

function normalizedIdentity(candidate, index) {
  if (!candidate || typeof candidate !== 'object') throw new Error(`Invalid medallion identity at ${index}`);
  assertEnum(candidate.category, MEDALLION_IDENTITY_CATEGORIES, 'identity category');
  if (!candidate.key || typeof candidate.key !== 'string') throw new Error(`Medallion identity ${index} requires a key`);
  return { key: candidate.key, category: candidate.category, source_order: candidate.source_order ?? index };
}

/**
 * Models the final cap in resolveEventMedallions after candidates have already
 * been ranked by the source resolver. It deliberately does not normalize or
 * merge identities.
 */
export function selectResolvedMedallionIdentities(candidates, resolution = 'resolved') {
  assertArray(candidates, 'identity candidates');
  assertEnum(resolution, MEDALLION_IDENTITY_RESOLUTIONS, 'identity resolution');
  const ranked = candidates.map(normalizedIdentity);
  if (resolution === 'conflicting_source_identity') return [];
  const eligible = resolution === 'ambiguous_venue_identity'
    ? ranked.filter((item) => item.category !== 'venue_brand')
    : ranked;
  return eligible.slice(0, MEDALLION_LIMITS.resolved_identities);
}

export function medallionPrimaryImage({ image_url: imageUrl, fallback_image_url: fallbackImageUrl = null }) {
  if (!imageUrl || typeof imageUrl !== 'string' || !LOCAL_ASSET.test(imageUrl)) throw new Error('Medallion image_url must be a local root-relative asset');
  if (fallbackImageUrl !== null && (typeof fallbackImageUrl !== 'string' || !LOCAL_ASSET.test(fallbackImageUrl))) throw new Error('Medallion fallback_image_url must be a local root-relative asset');
  const webp = imageUrl.endsWith('.webp');
  return {
    primary_image_src: webp ? (fallbackImageUrl || imageUrl) : imageUrl,
    webp_source_srcset: webp && fallbackImageUrl ? imageUrl : null,
    fallback_used_as_img_src: Boolean(webp && fallbackImageUrl),
  };
}

function normalizedToken(token, index) {
  if (!token || typeof token !== 'object') throw new Error(`Invalid medallion token at ${index}`);
  assertEnum(token.kind, MEDALLION_KINDS, 'token kind');
  const role = token.role ?? 'secondary';
  assertEnum(role, MEDALLION_ROLES, 'token role');
  if (!token.key || typeof token.key !== 'string') throw new Error(`Medallion token ${index} requires a key`);
  if (token.identity_category !== undefined && token.identity_category !== null) assertEnum(token.identity_category, MEDALLION_IDENTITY_CATEGORIES, 'identity category');
  const media = token.image_url ? medallionPrimaryImage(token) : null;
  return {
    key: token.key,
    kind: token.kind,
    role,
    identity_category: token.identity_category ?? null,
    media,
    render_kind: CIRCULAR_KINDS.has(token.kind) && media ? 'circle' : token.kind === 'pushkin' && media ? 'pushkin-frame' : 'pill',
  };
}

function visibleTokenProjection(tokens) {
  if (tokens.length <= MEDALLION_LIMITS.visible_tokens) return tokens;
  const admission = tokens.find((token) => token.kind === 'badge' && token.key === 'free-admission');
  const head = tokens.slice(0, MEDALLION_LIMITS.visible_tokens);
  return admission && !head.includes(admission) ? [...tokens.slice(0, 5), admission] : head;
}

/** Replays only the observable grouping/cap/fallback rules of the exact source. */
export function projectEventTokenMedallions(input = {}) {
  const layout = input.layout ?? 'inline';
  const allowTopSlot = input.allow_top_slot ?? true;
  const resolution = input.identity_resolution ?? 'resolved';
  assertEnum(layout, MEDALLION_LAYOUTS, 'layout');
  assertEnum(resolution, MEDALLION_IDENTITY_RESOLUTIONS, 'identity resolution');
  if (typeof allowTopSlot !== 'boolean') throw new Error('Medallion allow_top_slot must be boolean');
  assertArray(input.tokens ?? [], 'tokens');
  const tokens = (input.tokens ?? []).map(normalizedToken);
  if (resolution === 'conflicting_source_identity' && tokens.some((token) => token.kind === 'organizer')) {
    throw new Error('conflicting_source_identity cannot retain organizer identity tokens');
  }
  const visible = visibleTokenProjection(tokens);
  const desktop = visible.filter((token) => DESKTOP_KINDS.has(token.kind));
  const resolvedMain = layout === 'desktop-slots' ? desktop.find((token) => token.role === 'main') : undefined;
  const main = allowTopSlot ? resolvedMain : undefined;
  const groups = layout === 'desktop-slots'
    ? [
        ...(main ? [{ slot: 'top', tokens: [main] }] : []),
        ...(desktop.some((token) => token !== main) ? [{ slot: 'inline', tokens: desktop.filter((token) => token !== main) }] : []),
      ]
    : (visible.length ? [{ slot: 'inline', tokens: visible }] : []);
  const result = notMerged({
    schema_version: MEDALLION_SCHEMA,
    component_id: componentId(),
    logical_path: MEDALLION_LOGICAL_PATH,
    layout,
    allow_top_slot: allowTopSlot,
    identity_resolution: resolution,
    source_token_count: tokens.length,
    visible_token_keys: visible.map((token) => token.key),
    desktop_eligible_token_keys: desktop.map((token) => token.key),
    removed_for_desktop_keys: layout === 'desktop-slots' ? visible.filter((token) => !DESKTOP_KINDS.has(token.kind)).map((token) => token.key) : [],
    resolved_main_token_key: resolvedMain?.key ?? null,
    groups: groups.map((group) => ({ slot: group.slot, token_keys: group.tokens.map((token) => token.key), tokens: group.tokens })),
    rendered: groups.length > 0,
    proof_label: 'source-projection-not-browser-observation',
  });
  assertMedallionProjection(result);
  return clone(result);
}

export function assertMedallionProjection(record) {
  if (record.schema_version !== MEDALLION_SCHEMA || record.logical_path !== MEDALLION_LOGICAL_PATH) throw new Error('Invalid medallion projection schema');
  assertEnum(record.layout, MEDALLION_LAYOUTS, 'projection layout');
  assertEnum(record.identity_resolution, MEDALLION_IDENTITY_RESOLUTIONS, 'projection resolution');
  assertArray(record.groups, 'projection groups', 2);
  if (record.visible_token_keys.length > MEDALLION_LIMITS.visible_tokens) throw new Error('Medallion visible token cap violated');
  const top = record.groups.filter((group) => group.slot === 'top');
  if (top.length > 1 || (top[0]?.token_keys.length ?? 0) > MEDALLION_LIMITS.top_slot_tokens) throw new Error('Medallion top slot cap violated');
  if (top[0] && top[0].tokens[0]?.role !== 'main') throw new Error('Medallion top slot may contain Main only');
  if (record.layout === 'inline' && record.groups.some((group) => group.slot !== 'inline')) throw new Error('Inline medallions cannot emit a top slot');
  if (record.layout === 'desktop-slots' && record.groups.flatMap((group) => group.tokens).some((token) => token.kind === 'pill')) throw new Error('Desktop slots cannot contain pills');
  if (record.decision !== 'NOT_MERGED' || record.recommendation !== 'unresolved' || record.normalization_allowed !== false) throw new Error('Medallion normalization STOP violated');
  return true;
}

export function classifyMedallionViewport(width, height) {
  if (!Number.isInteger(width) || width < 1 || !Number.isInteger(height) || height < 1) throw new Error('Medallion viewport requires positive integer width and height');
  const desktop = width >= 1024;
  return clone({
    width,
    height,
    event_page_surface: desktop ? 'desktop' : 'mobile',
    event_component_layout: desktop ? 'desktop-slots' : 'inline',
    desktop_height_treatment: desktop ? (height <= 720 ? 'compact-height-72px-image-tokens' : 'regular-height-clamped-image-tokens') : 'not-applicable',
    desktop_width_context: !desktop ? 'below-1024' : width <= 1279 ? '1024-1279' : width < 1440 ? '1280-1439' : '1440-plus',
    boundary_evidence: width === 1023 || width === 1024 || height === 720 || height === 721,
    decision: 'NOT_MERGED',
    recommendation: 'unresolved',
    normalization_allowed: false,
  });
}

const CAPTURE_SELECTORS = Object.freeze({
  root: '.event-token-layout[data-medallion-layout]',
  top_slot: '.event-token-section[data-medallion-slot="top"]',
  inline_slot: '.event-token-section[data-medallion-slot="inline"]',
  token: '.event-token[data-medallion-role]',
  main: '.event-token[data-medallion-role="main"]',
  secondary: '.event-token[data-medallion-role="secondary"]',
  categories: '.event-token[data-medallion-category]',
  identity_resolution: '.event-token-section[data-identity-resolution]',
  circle: '.event-token__circle',
  pushkin_frame: '.event-token__pushkin-frame',
  pill: '.event-token__pill',
});

export function buildMedallionCaptureContract() {
  return clone(notMerged({
    id: 'capture-contract.event-token-medallions',
    schema_version: MEDALLION_SCHEMA,
    component_id: componentId(),
    selectors: CAPTURE_SELECTORS,
    capture_owners: [
      { owner: MEDALLION_LOGICAL_PATH, facts: ['DOM grouping', 'state data attributes', 'token kind markup', 'media fallback markup'] },
      { owner: 'src/layouts/EventLayout.astro', facts: ['base event-token geometry and rendering'] },
      { owner: 'src/components/DesktopEventPage.astro', facts: ['desktop slot placement', 'desktop token geometry', 'height compression', 'consumer override'] },
      { owner: 'src/components/MobileEventProductionStyles.astro', facts: ['production mobile token geometry', 'mobile consumer override'] },
    ],
    required_fields: ['element screenshot', 'DOM summary', 'computed styles', 'geometry', 'CSS variables', 'override source', 'accessibility', 'breakpoint context'],
    viewport_matrix: [[320, 844], [390, 844], [1023, 900], [1024, 900], [1280, 720], [1280, 721], [1440, 900]],
    proof_labels: { production_route: 'exact-candidate-browser-element', controlled_specimen: 'controlled-specimen-browser-element', source_only: 'source-projection-not-browser-observation' },
    observation_status: 'not-captured-by-this-lane',
  }));
}

const EVENT_ROUTES = Object.freeze([
  [2601, 'vystavka-donbass-proshloe-i-nastoyaschee-kaliningrad-2601', 'no-token baseline', ['none']],
  [5336, 'maestro-nasledie-n-n-bocharova-kaliningrad-5336', 'one Main; desktop top enabled', ['organizer:main']],
  [6856, 'aleksey-poluboyarov-kaliningrad-6856', 'one Main; desktop top disabled by consumer media policy', ['organizer:main', 'top-off']],
  [698, 'drevnie-voiny-yantarnogo-kraya-kaliningrad-698', 'Main plus Pushkin', ['organizer:main', 'pushkin']],
  [6994, 'enogastronomicheskiy-festival-grozd-svetlogorsk-6994', 'Main plus venue, RZD and free', ['organizer:main', 'venue_brand:secondary', 'program', 'badge']],
  [7040, 'vecherinka-paket-kasset-svetlogorsk-7040', 'secondary RZD plus free; no Main', ['program:secondary', 'badge']],
  [6591, 'lektsiya-kofeynaya-geometriya-kaliningrad-6591', 'identity conflict; mobile price pill and desktop empty', ['conflicting_source_identity', 'pill-mobile-only']],
  [6562, 'revuschiy-lev-poyuschiy-los-kaliningrad-6562', 'organizer, Pushkin, price and kids on mobile; image-like tokens only on desktop', ['organizer:main', 'pushkin', 'pill-mobile-only']],
  [6990, 'den-vseh-igr-dvi-kaliningrad-6990', 'kids and charity pills; desktop empty', ['pill-mobile-only']],
  [5829, 'ekskursiya-zakulise-teatra-kaliningrad-5829', 'sold-out pill removed on desktop after image-like tokens are retained', ['pill-mobile-only', 'sold-out']],
  [5278, 'letnie-kontserty-fortepiannoy-muzyki-pianissimo-kaliningrad-5278', 'festival fallback pill candidate on mobile', ['pill-mobile-only', 'festival-fallback']],
]);

export function buildMedallionProductionRoutePlan() {
  const rows = EVENT_ROUTES.map(([eventId, slug, expected, axes]) => notMerged({
    id: planId('route', eventId),
    schema_version: MEDALLION_SCHEMA,
    component_id: componentId(),
    logical_path: MEDALLION_LOGICAL_PATH,
    plan_status: 'representative-real-page-verification-required',
    route_family: 'event-detail',
    route_reference: { event_id: eventId, candidate_relative_path: `sobytiya/${slug}/index.html` },
    expected_as_is_state: expected,
    state_axes: axes,
    required_contexts: eventId === 2601 ? ['390x844', '1024x900'] : ['390x844', '1280x721'],
    selectors: CAPTURE_SELECTORS,
    capture_owner: 'exact-candidate-production-route-browser',
    observation_status: 'planned-not-captured',
    proof_label: 'planned-production-route-verification-not-observed',
    evidence_claim_limit: 'no-production-observed-claim-until-browser-capture-is-bound',
  }));
  assertMedallionPlan(rows);
  return clone(rows);
}

function token(key, kind, role = 'secondary', extras = {}) { return { key, kind, role, ...extras }; }
function identity(key, category) { return { key, category }; }

const SPECIMEN_CASES = Object.freeze([
  { key: 'defaults-empty', input: {}, axes: ['layout-default:inline', 'allowTopSlot-default:true', 'zero'] },
  { key: 'inline-main', input: { tokens: [token('organizer:a', 'organizer', 'main', { identity_category: 'organizer', image_url: '/fixtures/a.webp', fallback_image_url: '/fixtures/a.png' })] }, axes: ['inline', 'one', 'main', 'image-fallback'] },
  { key: 'desktop-main-top-on', input: { layout: 'desktop-slots', tokens: [token('organizer:a', 'organizer', 'main', { identity_category: 'organizer', image_url: '/fixtures/a.svg' })] }, axes: ['desktop-slots', 'top-on', 'main'] },
  { key: 'desktop-main-top-off', input: { layout: 'desktop-slots', allow_top_slot: false, tokens: [token('organizer:a', 'organizer', 'main', { identity_category: 'festival', image_url: '/fixtures/a.svg' })] }, axes: ['desktop-slots', 'top-off', 'main-inline'] },
  { key: 'secondary-only', input: { layout: 'desktop-slots', tokens: [token('program:rzd', 'program', 'secondary', { image_url: '/fixtures/rzd.webp', fallback_image_url: '/fixtures/rzd.png' }), token('free-admission', 'badge', 'secondary', { image_url: '/fixtures/free.svg' })] }, axes: ['desktop-slots', 'secondary', 'no-top'] },
  { key: 'kind-matrix', input: { tokens: [token('organizer:a', 'organizer', 'main', { identity_category: 'venue_brand', image_url: '/fixtures/a.svg' }), token('source:a', 'source', 'secondary', { image_url: '/fixtures/source.svg' }), token('program:a', 'program', 'secondary', { image_url: '/fixtures/program.svg' }), token('pushkin-card', 'pushkin', 'secondary', { image_url: '/fixtures/pushkin.webp', fallback_image_url: '/fixtures/pushkin.png' }), token('free-admission', 'badge', 'secondary', { image_url: '/fixtures/free.svg' }), token('price', 'pill')] }, axes: ['all-kinds', 'inline', 'six'] },
  { key: 'desktop-pill-filter', input: { layout: 'desktop-slots', tokens: [token('price', 'pill'), token('kids-family', 'pill'), token('charity', 'pill')] }, axes: ['desktop-slots', 'pill-filter', 'empty'] },
  { key: 'token-overflow-head-six', input: { tokens: Array.from({ length: 7 }, (_, index) => token(`pill:${index}`, 'pill')) }, axes: ['many', 'overflow', 'six-cap', 'no-free'] },
  { key: 'token-overflow-free-retained', input: { tokens: [...Array.from({ length: 6 }, (_, index) => token(`pill:${index}`, 'pill')), token('free-admission', 'badge', 'secondary', { image_url: '/fixtures/free.svg' })] }, axes: ['many', 'overflow', 'six-cap', 'free-retention'] },
  { key: 'conflicting-source', input: { identity_resolution: 'conflicting_source_identity', tokens: [token('price', 'pill')] }, axes: ['identity-conflict', 'fail-closed', 'mobile-pill'] },
  { key: 'ambiguous-venue', input: { identity_resolution: 'ambiguous_venue_identity', tokens: [token('organizer:a', 'organizer', 'main', { identity_category: 'organizer', image_url: '/fixtures/a.svg' })] }, axes: ['identity-conflict', 'ambiguous-venue', 'nonvenue-retained'] },
  { key: 'media-webp-fallback', input: { tokens: [token('source:a', 'source', 'secondary', { image_url: '/fixtures/source.webp', fallback_image_url: '/fixtures/source.png' })] }, axes: ['media', 'webp-source', 'png-img-fallback'] },
  { key: 'media-webp-no-fallback', input: { tokens: [token('source:a', 'source', 'secondary', { image_url: '/fixtures/source.webp' })] }, axes: ['media', 'webp-no-fallback'] },
  { key: 'media-vector-primary', input: { tokens: [token('organizer:a', 'organizer', 'main', { identity_category: 'festival_brand', image_url: '/fixtures/a.svg' })] }, axes: ['media', 'non-webp-primary'] },
]);

export function buildMedallionControlledSpecimenPlan() {
  const rows = SPECIMEN_CASES.map((specimen) => notMerged({
    id: planId('controlled', specimen.key),
    schema_version: MEDALLION_SCHEMA,
    component_id: componentId(),
    logical_path: MEDALLION_LOGICAL_PATH,
    plan_status: 'controlled-specimen-required',
    specimen_key: specimen.key,
    fixture_input: specimen.input,
    expected_projection: projectEventTokenMedallions(specimen.input),
    state_axes: specimen.axes,
    required_contexts: ['390x844', '1023x900', '1024x900', '1280x720', '1280x721'],
    selectors: CAPTURE_SELECTORS,
    capture_owner: 'temporary-controlled-specimen-harness',
    observation_status: 'planned-not-captured',
    proof_label: 'controlled-specimen-plan-not-observed',
    evidence_claim_limit: 'controlled-specimen-only-not-production-observed',
  }));
  assertMedallionPlan(rows);
  return clone(rows);
}

export function assertMedallionPlan(rows) {
  assertArray(rows, 'plan rows', 32);
  const ids = new Set();
  for (const row of rows) {
    if (ids.has(row.id)) throw new Error(`Duplicate medallion plan id: ${row.id}`);
    ids.add(row.id);
    if (row.schema_version !== MEDALLION_SCHEMA || row.component_id !== componentId()) throw new Error(`Invalid medallion plan schema: ${row.id}`);
    if (!['controlled-specimen-required', 'representative-real-page-verification-required'].includes(row.plan_status)) throw new Error(`Invalid medallion plan status: ${row.id}`);
    if (row.observation_status !== 'planned-not-captured' || !['planned-production-route-verification-not-observed', 'controlled-specimen-plan-not-observed'].includes(row.proof_label)) throw new Error(`Medallion plan falsely claims observation: ${row.id}`);
    if (row.decision !== 'NOT_MERGED' || row.recommendation !== 'unresolved' || row.normalization_allowed !== false) throw new Error(`Medallion plan normalization STOP violated: ${row.id}`);
  }
  return true;
}

export function buildMedallionResourceCandidates() {
  const resources = [
    ['event-detail', MEDALLION_LOGICAL_PATH, 'production-reachable-not-observed', 'canonical event-detail token renderer; inline and desktop-slots'],
    ['listing-card', 'src/components/listings/ListingEventCard.astro', 'production-reachable-not-observed', 'distinct overlay/external/free listing medallion resource'],
    ['mobile-listing-rail', 'src/components/listings/MobileListingRailRow.astro', 'production-reachable-not-observed', 'distinct mobile listing identity/free medallion resource'],
    ['exhibition-row', 'src/components/ExhibitionPrototypeRow.astro', 'production-reachable-not-observed', 'distinct exhibition seal using the shared resolver'],
    ['medallion-catalog-lab', 'src/pages/lab/medallions/index.astro', 'lab-only', 'manual catalog markup; not an EventTokenMedallions instance'],
    ['design-system-lab-instance', 'src/pages/lab/design-system/index.astro', 'lab-only', 'actual EventTokenMedallions consumer but not production proof'],
  ].map(([family, sourcePath, reachability, separation]) => notMerged({
    id: `medallion-resource.${family}`,
    schema_version: MEDALLION_SCHEMA,
    resource_family: family,
    source_path: sourcePath,
    reachability,
    equivalence_status: 'NOT_MERGED',
    separation_basis: separation,
    proof_label: 'source-consumer-resource-candidate',
  }));
  return clone(resources);
}

export function buildMedallionMismatches() {
  return clone([
    notMerged({
      id: 'mismatch.medallions-organizer-count-28-vs-stale-27',
      schema_version: MEDALLION_SCHEMA,
      kind: 'inventory-count',
      channels: [
        { channel: 'exact-candidate-organizer-manifest', value: 28, source_path: 'src/data/organizerMedallions.json' },
        { channel: 'exact-candidate-regression-test', value: 28, source_path: 'site/tests/event-detail-runtime-regressions.test.mjs' },
        { channel: 'prior-decoder-review-note', value: 27, status: 'stale' },
      ],
      conclusion: 'current exact candidate is 28; any retained count of 27 is stale evidence and must not constrain the snapshot',
      proof_label: 'source-count-reconciliation-not-browser-observation',
    }),
    notMerged({
      id: 'mismatch.medallions-detail-geometry-doc-vs-consumer-css',
      schema_version: MEDALLION_SCHEMA,
      kind: 'geometry-documentation',
      channels: [
        { channel: 'documentation', value: 'detail desktop clamp(88px,23vw,112px)', source_path: 'docs/features/static-site-pages/event-token-medallions.md' },
        { channel: 'desktop-inline-consumer-css', value: 'clamp(72px,7vw,94px)', source_path: 'src/components/DesktopEventPage.astro' },
        { channel: 'desktop-top-consumer-css', value: 'clamp(88px,7.4vw,108px)', source_path: 'src/components/DesktopEventPage.astro' },
        { channel: 'mobile-production-consumer-css', value: 'clamp(84px,23vw,92px)', source_path: 'src/components/MobileEventProductionStyles.astro' },
      ],
      conclusion: 'geometry is consumer- and slot-scoped; documentation cannot be promoted to one computed contract without browser evidence',
      proof_label: 'source-style-reconciliation-not-browser-observation',
    }),
    notMerged({
      id: 'mismatch.medallions-lab-catalog-not-component-equivalent',
      schema_version: MEDALLION_SCHEMA,
      kind: 'lab-equivalence',
      channels: [
        { channel: 'lab-medallions', value: 'manual event-token class markup; no EventTokenMedallions import', source_path: 'src/pages/lab/medallions/index.astro' },
        { channel: 'design-system-lab', value: 'imports and renders EventTokenMedallions', source_path: 'src/pages/lab/design-system/index.astro' },
        { channel: 'production-consumers', value: 'event detail has separate mobile and desktop consumers', source_path: 'src/pages/sobytiya/[slug].astro' },
      ],
      conclusion: 'lab catalog and production component are not equivalent evidence surfaces',
      proof_label: 'source-consumer-reconciliation-not-browser-observation',
    }),
  ]);
}

export function buildMedallionUnresolved() {
  return clone([
    notMerged({ id: 'unresolved.medallions-controlled-specimens', schema_version: MEDALLION_SCHEMA, kind: 'evidence-gap', reason: 'controlled component specimens and element captures are planned but not captured by this lane', blocks_handoff: true }),
    notMerged({ id: 'unresolved.medallions-production-route-binding', schema_version: MEDALLION_SCHEMA, kind: 'evidence-gap', reason: 'representative exact-candidate routes require source-to-element binding and visual review', blocks_handoff: true }),
    notMerged({ id: 'unresolved.medallions-computed-geometry', schema_version: MEDALLION_SCHEMA, kind: 'mismatch-followup', reason: 'slot- and consumer-scoped computed geometry must reconcile documentation and source CSS', blocks_handoff: true }),
    notMerged({ id: 'unresolved.medallions-resource-equivalence', schema_version: MEDALLION_SCHEMA, kind: 'separation-required', reason: 'event-detail, listing, exhibition and lab resources remain separate candidates', blocks_handoff: false }),
  ]);
}

export function buildMedallionStateRecords() {
  const axes = [
    ['layout', MEDALLION_LAYOUTS, 'inline'],
    ['slot', MEDALLION_SLOTS, null],
    ['role', MEDALLION_ROLES, 'secondary'],
    ['kind', MEDALLION_KINDS, null],
    ['identity-category', MEDALLION_IDENTITY_CATEGORIES, 'organizer'],
    ['identity-resolution', MEDALLION_IDENTITY_RESOLUTIONS, 'resolved'],
    ['identity-cardinality', ['zero', 'one', 'many', 'overflow'], 'zero'],
    ['top-slot-permission', ['enabled', 'disabled'], 'enabled'],
    ['media-fallback', ['webp-with-fallback', 'webp-without-fallback', 'non-webp-primary'], null],
  ];
  return clone(axes.flatMap(([axis, values, defaultValue]) => values.map((value) => notMerged({
    id: `medallion-state.${axis}.${value}`,
    schema_version: MEDALLION_SCHEMA,
    component_id: componentId(),
    axis,
    value,
    default: value === defaultValue,
    proof_label: 'source-state-axis-not-browser-observation',
  }))));
}

export function buildMedallionTransitionRecords() {
  return clone([
    ['identity-candidate-cap', 'ranked identity candidates', 'resolved identities', 'take first 3 after fail-closed venue/source conflict handling'],
    ['visible-token-cap', 'source-order tokens', 'visible tokens', 'take first 6; if free-admission is later, retain first 5 plus free'],
    ['desktop-kind-filter', 'visible tokens', 'desktop eligible tokens', 'retain organizer/source/program/pushkin/badge and remove pill'],
    ['desktop-top-slot', 'desktop eligible tokens', 'top plus inline groups', 'first Main moves to top only when allowTopSlot; at most one'],
    ['media-fallback', 'token image assets', 'picture source and img src', 'WebP plus fallback uses WebP source and fallback img; otherwise primary asset remains img'],
  ].map(([key, from, to, rule]) => notMerged({
    id: `medallion-transition.${key}`,
    schema_version: MEDALLION_SCHEMA,
    component_id: componentId(),
    from,
    to,
    rule,
    transition_kind: 'source-derived-projection-not-user-interaction',
    proof_label: 'source-transition-not-browser-observation',
  })));
}

export function buildMedallionSpecimenPlan() {
  return clone([...buildMedallionProductionRoutePlan(), ...buildMedallionControlledSpecimenPlan()]);
}

export const buildMedallionMismatchRecords = buildMedallionMismatches;
export const buildMedallionUnresolvedRecords = buildMedallionUnresolved;

export function buildMedallionDecoderLane() {
  const result = notMerged({
    schema_version: MEDALLION_SCHEMA,
    lane_id: MEDALLION_LANE_ID,
    pinned_source_sha: MEDALLION_SOURCE_SHA,
    component_id: componentId(),
    logical_path: MEDALLION_LOGICAL_PATH,
    axes: {
      layouts: MEDALLION_LAYOUTS,
      slots: MEDALLION_SLOTS,
      roles: MEDALLION_ROLES,
      kinds: MEDALLION_KINDS,
      identity_categories: MEDALLION_IDENTITY_CATEGORIES,
      identity_resolutions: MEDALLION_IDENTITY_RESOLUTIONS,
      cardinalities: ['zero', 'one', 'many', 'overflow'],
      media: ['local-vector-or-raster-primary', 'webp-with-fallback', 'webp-without-fallback'],
    },
    defaults: { layout: 'inline', allow_top_slot: true, token_role: 'secondary', identity_category: 'organizer-when-omitted-by-definition' },
    limits: MEDALLION_LIMITS,
    state_contract_status: 'candidate-as-is-source-model',
    responsive_contexts: MEDALLION_RESPONSIVE_CONTEXTS,
    state_records: buildMedallionStateRecords(),
    transition_records: buildMedallionTransitionRecords(),
    production_route_plan: buildMedallionProductionRoutePlan(),
    controlled_specimen_plan: buildMedallionControlledSpecimenPlan(),
    specimen_plan: buildMedallionSpecimenPlan(),
    specimen_observations: [],
    resource_candidates: buildMedallionResourceCandidates(),
    capture_contract: buildMedallionCaptureContract(),
    mismatches: buildMedallionMismatches(),
    unresolved: buildMedallionUnresolved(),
    proof_label: 'source-model-and-plan-not-browser-observation',
    constraints: {
      as_is_only: true, browser_capture_claimed: false, private_corpus_run_claimed: false,
      merge: false, split: false, normalization: false, tokenization: false,
      penpot_mutation: false, astro_css_mutation: false,
    },
  });
  assertMedallionDecoderLane(result);
  return clone(result);
}

export function assertMedallionDecoderLane(record) {
  if (record.schema_version !== MEDALLION_SCHEMA || record.lane_id !== MEDALLION_LANE_ID || record.pinned_source_sha !== MEDALLION_SOURCE_SHA || record.component_id !== componentId()) throw new Error('Invalid medallion lane schema');
  assertMedallionPlan(record.production_route_plan);
  assertMedallionPlan(record.controlled_specimen_plan);
  assertMedallionPlan(record.specimen_plan);
  if (record.specimen_plan.length !== record.production_route_plan.length + record.controlled_specimen_plan.length) throw new Error('Medallion combined specimen plan incomplete');
  if (record.specimen_observations.length !== 0 || record.constraints.browser_capture_claimed !== false || record.constraints.private_corpus_run_claimed !== false) throw new Error('Medallion lane falsely claims capture or corpus execution');
  if (record.resource_candidates.length < 6 || !record.resource_candidates.every((item) => item.equivalence_status === 'NOT_MERGED')) throw new Error('Medallion resources were collapsed');
  if (record.mismatches.length < 3 || record.unresolved.length < 4) throw new Error('Medallion mismatch or unresolved register incomplete');
  if (record.decision !== 'NOT_MERGED' || record.recommendation !== 'unresolved' || record.normalization_allowed !== false) throw new Error('Medallion lane normalization STOP violated');
  const serialized = JSON.stringify(record);
  if (/(?:https?:\/\/|authorization|bearer|_review\/)/iu.test(serialized)) throw new Error('Unsafe URL or credential-shaped value in medallion lane');
  return true;
}

export const validateMedallionDecoderLane = assertMedallionDecoderLane;

export function stableSerializeMedallionLane(record = buildMedallionDecoderLane()) {
  assertMedallionDecoderLane(record);
  return `${JSON.stringify(stable(record))}\n`;
}
