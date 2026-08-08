import { createHash } from 'node:crypto';
import {
  TRANSPORT_AXIS_DEFINITIONS,
  TRANSPORT_BREAKPOINT_CONTEXTS,
  TRANSPORT_INVALID_COMBINATIONS,
  TRANSPORT_SOURCE_PATHS,
  buildTransportDecoderLane,
} from './transport.mjs';
import {
  MEDALLION_LOGICAL_PATH,
  buildMedallionDecoderLane,
} from './medallions.mjs';
import {
  AMBER_SYSTEM_ID,
  FOCUS_SYSTEM_ID,
  buildArtifactDecoderLane,
} from './artifacts.mjs';

export const CANDIDATE_CONTRACT_SCHEMA = 'current_ui_candidate_as_is_contract_v1';
export const CANDIDATE_CONTRACT_VERSION = '0.1.0-candidate';
export const SOURCE_SHA = 'ef7aa62e45c60f7a12da6160f490719c0721ec03';
export const RELATIONSHIP_KINDS = Object.freeze(['one-to-one', 'one-to-many', 'many-to-one', 'missing', 'unresolved']);
export const CONFIDENCE_LEVELS = Object.freeze(['deterministic', 'observed', 'inferred', 'unresolved']);
export const DECISIONS = Object.freeze(['NOT_MERGED']);
export const RECONCILIATION_CONCLUSIONS = Object.freeze([
  'match', 'source/specimen mismatch', 'specimen/page mismatch', 'local production override',
  'page-only state', 'specimen-only state', 'duplicated implementation',
  'missing normalized component', 'unresolved mapping',
]);

const REQUIRED_CONTRACT_FIELDS = Object.freeze([
  'version', 'semantic_role', 'anatomy', 'props', 'slots', 'variant_axes', 'state_axes',
  'valid_combinations', 'invalid_combinations', 'nested_component_refs', 'token_refs',
  'responsive_contract', 'media_contract', 'accessibility_contract', 'fixture_classes',
]);

function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map((key) => [key, stable(value[key])]));
  return value;
}
function clone(value) { return JSON.parse(JSON.stringify(stable(value))); }
function digest(value) { return createHash('sha256').update(JSON.stringify(stable(value))).digest('hex'); }
function source(path, relation = 'definition', confidence = 'deterministic') {
  return { source_sha: SOURCE_SHA, logical_path: path, relation, confidence, proof_label: 'exact-candidate-source-derived' };
}
function fact(id, statement, sourcePaths, confidence = 'deterministic') {
  return { id, statement, source_bindings: sourcePaths.map((path) => source(path)), confidence, observation_scope: 'source-only-until-reconciled' };
}
function stop(record) {
  const unsigned = {
    ...record,
    schema_version: CANDIDATE_CONTRACT_SCHEMA,
    decision: 'NOT_MERGED', recommendation: 'unresolved', normalization_allowed: false,
    normative_status: 'candidate-as-is-not-accepted', human_review_status: 'pending',
  };
  return clone({ ...unsigned, detached_contract_sha256: digest(unsigned) });
}
function contract({ id, role, bindings, relationship, confidence, anatomy, props = {}, slots = {}, variants = {}, states = {}, valid = [], invalid = [], nested = [], responsive = [], media = [], a11y = [], fixtures = [], evidence = [], consumers = [], reachability, overrides = [], gaps = [], blockers = [], alternatives = [] }) {
  return stop({
    id, candidate_component_id: id, source_bindings: bindings, relationship_kind: relationship,
    evidence, confidence,
    candidate_contract: {
      version: CANDIDATE_CONTRACT_VERSION, semantic_role: role, anatomy, props, slots,
      variant_axes: variants, state_axes: states, valid_combinations: valid,
      invalid_combinations: invalid, nested_component_refs: nested,
      token_refs: [], token_evidence_status: 'raw-source-references-only-no-tokenization',
      responsive_contract: responsive, media_contract: media, accessibility_contract: a11y,
      fixture_classes: fixtures,
    },
    production_consumers: consumers, reachability, local_overrides: overrides,
    normalization_gaps: gaps, promotion_blockers: blockers, unresolved_alternatives: alternatives,
  });
}

function presentationContracts(eventPresentationRecords) {
  const records = [...eventPresentationRecords].sort((a, b) => String(a.id).localeCompare(String(b.id)));
  const recordRefs = records.map((item) => ({ id: item.id, status: item.status, decision: item.decision, source_component_ids: item.source_component_ids || [], runtime_route_count: item.runtime_route_count || 0 }));
  const observedCount = records.filter((item) => item.status === 'observed').length;
  const commonEvidence = records.length
    ? [fact('fact.event-presentation-existing-records', `${records.length} existing Event Detail presentation records supplied; ${observedCount} runtime-observed`, ['src/components/DesktopEventPage.astro', 'src/lib/desktopEventPresentation.ts'], observedCount ? 'observed' : 'deterministic')]
    : [fact('fact.event-presentation-records-pending', 'Event presentation record binding is pending decoder integration; no runtime observation is claimed by this contract lane', ['src/components/DesktopEventPage.astro', 'src/lib/desktopEventPresentation.ts'], 'unresolved')];
  const presentation = contract({
    id: 'candidate.event-detail-presentation', role: 'Desktop Event Detail presentation selected from current source-derived media conditions',
    bindings: [source('src/components/DesktopEventPage.astro'), source('src/lib/desktopEventPresentation.ts', 'state-resolver'), source('src/pages/sobytiya/[slug].astro', 'production-consumer')],
    relationship: 'one-to-many', confidence: records.length ? 'observed' : 'deterministic',
    anatomy: ['presentation-shell', 'primary-media-frame', 'content-column', 'action-panel-placement', 'medallion-slot-context'],
    props: { presentation: { required: true, source: 'buildDesktopEventPresentation result' }, event: { required: true }, shareImage: { required: false }, desktopRelatedEvents: { required: false, default: [] } },
    variants: { desktop_family: ['editorial', 'split'], presentation_reason: ['editorial-landscape', 'split-portrait-or-square-visual', 'split-low-resolution-portrait-viewer', 'split-no-image-fallback'] },
    states: { media_presence: ['image', 'no-image-fallback'], action_layout: ['stacked', 'inline'] },
    valid: ['editorial => action_layout=stacked', 'split => action_layout=inline', 'split-no-image-fallback => desktop_family=split'],
    invalid: ['editorial with split inline action placement', 'split with editorial side action placement'],
    nested: ['candidate.button-cta-fragmented', 'candidate.event-media', 'candidate.event-token-medallions'],
    responsive: [{ kind: 'consumer-viewport', boundary: 1024, below: 'mobile-event-production-surface', at_or_above: 'desktop-event-page' }],
    media: [{ kind: 'orientation-sensitive-layout', values: ['landscape-editorial', 'portrait-or-square-split', 'no-image-fallback'] }],
    a11y: ['preserve source landmark and action semantics per rendered branch'], fixtures: ['editorial-landscape', 'split-portrait-poster', 'split-no-image-fallback'],
    evidence: [...commonEvidence, ...recordRefs.map((item) => ({ ...item, record_id: item.id, id: `existing.${item.id}`, confidence: item.status === 'observed' ? 'observed' : 'deterministic' }))],
    consumers: [{ logical_path: 'src/pages/sobytiya/[slug].astro', route_family: 'page-family.event-detail', proof_label: 'exact-candidate-source-consumer' }],
    reachability: records.length ? 'production-reachable-records-supplied' : 'production-reachable-record-binding-pending',
    overrides: [{ source_path: 'src/components/DesktopEventPage.astro', scope: 'branch-local component CSS and consumer composition', conclusion: 'unresolved mapping' }],
    gaps: ['editorial and split remain distinct AS-IS branches; equivalence is not asserted'], blockers: ['component specimen and real-page reconciliation still required'],
    alternatives: ['one candidate family with two variants', 'two independent presentation components'],
  });
  const media = contract({
    id: 'candidate.event-media', role: 'Event Detail primary and companion media presentation',
    bindings: [source('src/components/DesktopEventPage.astro'), source('src/components/EventMediaRail.astro')], relationship: 'one-to-many', confidence: 'deterministic',
    anatomy: ['primary-large-frame', 'poster-companion', 'remaining-photo-preview-rail'],
    props: { presentation: { required: true }, image_set: { required: true }, poster_identity: { required: false } },
    variants: { layout_context: ['editorial', 'split'], resource_format: ['primary-large-frame', 'editorial-large-poster-companion', 'editorial-small-photo-rail', 'editorial-small-companion-previews', 'split-small-photo-rail'] },
    states: { primary_media: ['available', 'fallback'], selection: ['first', 'alternate'] },
    valid: ['large poster companion remains distinct from small remaining-photo previews'], invalid: ['treating every media item as an equal-size preview'],
    responsive: [{ kind: 'consumer-scoped', owner: 'src/components/DesktopEventPage.astro', status: 'source-extraction-required' }],
    media: [{ kind: 'fit', values: ['cover-primary-photo', 'contain-identity-poster', 'aspect-aware-small-preview'] }],
    a11y: ['preserve meaningful alt text and active preview semantics from source'], fixtures: ['editorial-poster-companion', 'split-poster-thumbnail-rail', 'no-image-fallback'],
    evidence: [fact('fact.media-distinct-formats', 'Large poster companion and small remaining-photo previews are separate AS-IS resources', ['src/components/DesktopEventPage.astro'])],
    consumers: [{ logical_path: 'src/components/DesktopEventPage.astro', route_family: 'page-family.event-detail' }], reachability: 'production-reachable',
    overrides: [{ source_path: 'src/components/DesktopEventPage.astro', scope: 'layout-branch-specific media CSS', conclusion: 'local production override' }],
    gaps: ['isolated component boundary and computed geometry pending'], blockers: ['visual and real-page reconciliation pending'], alternatives: ['internal resources of DesktopEventPage', 'separate media candidates'],
  });
  return [presentation, media];
}

function buttonContract() {
  return contract({
    id: 'candidate.button-cta-fragmented', role: 'Button-like action family spanning generic Button and Event Detail CTA implementations',
    bindings: [source('src/components/design-system/Button.astro'), source('src/components/DesktopEventActionPanel.astro'), source('src/components/CalendarLink.astro', 'nested-action')],
    relationship: 'unresolved', confidence: 'deterministic', anatomy: ['interactive-root', 'label-slot-or-derived-label', 'optional-spinner-or-icon', 'optional-live-status'],
    props: {
      variant: { required: false, default: 'primary', union: ['primary', 'secondary', 'quiet', 'inverse', 'danger'], owner: 'Button' },
      size: { required: false, default: 'default', union: ['compact', 'default', 'large'], owner: 'Button' },
      state: { required: false, default: 'default', union: ['default', 'hover', 'focus', 'pressed', 'loading', 'disabled'], owner: 'Button' },
      href: { required: false, owner: 'Button' }, type: { required: false, default: 'button', union: ['button', 'submit', 'reset'], owner: 'Button' },
      iconOnly: { required: false, default: false, owner: 'Button' }, family: { required: false, default: 'editorial', union: ['split', 'editorial'], owner: 'DesktopEventActionPanel' },
    }, slots: { default: { owner: 'Button', required: true } },
    variants: { implementation: ['design-system-button', 'desktop-event-primary-action', 'calendar-link', 'phone-copy', 'disabled-status'], action_family: ['editorial', 'split'] },
    states: { interaction: ['default', 'hover', 'focus', 'pressed', 'loading', 'disabled'], phone_copy: ['hidden', 'busy', 'success', 'error'], semantic_root: ['anchor', 'button', 'status-span'] },
    valid: ['Button href => anchor', 'Button !href => button', 'loading or disabled => disabled semantics', 'split action family => inline layout', 'editorial action family => stacked layout'],
    invalid: ['claiming source implementations are equivalent before reconciliation', 'loading link retaining active href'], nested: [],
    responsive: [{ kind: 'consumer-scoped', owner: 'DesktopEventActionPanel', detail: 'phone-copy toast path checks max-width 759' }], media: [],
    a11y: ['aria-busy for loading/copy', 'aria-disabled and tabindex=-1 for disabled link', 'disabled attribute for disabled button', 'aria-live status for phone copy', 'aria-pressed where applicable'],
    fixtures: ['generic-button-state-matrix', 'editorial-stacked-cta', 'split-inline-cta', 'phone-copy-success-error', 'disabled-and-loading'],
    evidence: [fact('fact.button-closed-axes', 'Button declares closed variant, size and state unions with defaults', ['src/components/design-system/Button.astro']), fact('fact.cta-distinct-family', 'DesktopEventActionPanel selects stacked/editorial versus inline/split placement', ['src/components/DesktopEventActionPanel.astro'])],
    consumers: [{ logical_path: 'src/components/DesktopEventPage.astro', route_family: 'page-family.event-detail' }], reachability: 'mixed-production-and-lab-reachability-needs-consumer-reconciliation',
    overrides: [{ source_path: 'src/components/DesktopEventPage.astro', scope: 'desktop action placement CSS', conclusion: 'local production override' }],
    gaps: ['fragmented implementations are recorded, not merged', 'consumer inventory and specimens pending'], blockers: ['human review of fragmentation', 'source/specimen/page reconciliation'],
    alternatives: ['one generic Button with CTA compositions', 'separate action candidates by semantics', 'retain duplicated implementation'],
  });
}

function transportContracts(lane) {
  const definitions = {
    rail: { id: 'candidate.transport-rail', path: TRANSPORT_SOURCE_PATHS.rail, role: 'Event rail outbound and return schedule', anatomy: ['route-heading', 'outbound-group', 'return-group', 'cutoff-or-warning-state'], a11y: ['source landmark/heading semantics pending capture'] },
    bus: { id: 'candidate.transport-bus', path: TRANSPORT_SOURCE_PATHS.bus, role: 'Event bus outbound and return schedule', anatomy: ['route-heading', 'outbound-group', 'return-group', 'boarding-stop-detail'], a11y: ['source landmark/heading semantics pending capture'] },
    kaup: { id: 'candidate.transport-kaup', path: TRANSPORT_SOURCE_PATHS.kaup, role: 'Kaup official transfer and public return schedule', anatomy: ['transport-shell', 'official-transfer', 'public-bus-return', 'journey-alerts', 'optional-experiment-host'], a11y: ['disclosure expanded state', 'route and alert semantics pending capture'] },
  };
  return Object.entries(definitions).map(([family, spec]) => {
    const axes = TRANSPORT_AXIS_DEFINITIONS[family];
    const states = lane.state_records.filter((item) => item.family === `transport.${family}`);
    const plans = lane.specimen_plan.filter((item) => item.specimen_family === `transport.${family}`);
    return contract({
      id: spec.id, role: spec.role, bindings: [source(spec.path), ...(family === 'kaup' ? [source(TRANSPORT_SOURCE_PATHS.experiment, 'source-only-experiment-wrapper')] : [])],
      relationship: family === 'kaup' ? 'one-to-many' : 'one-to-one', confidence: 'deterministic', anatomy: spec.anatomy,
      props: { source_props: { status: 'state-aware-extraction-bound-by-logical-source-record' } }, variants: family === 'kaup' ? { timetable_treatment: axes.treatment } : {}, states: axes,
      valid: states.map((item) => ({ state_record_id: item.id, axes: item.axes, reachability: item.reachability })),
      invalid: TRANSPORT_INVALID_COMBINATIONS.filter((item) => item.family === `transport.${family}`).map((item) => ({ id: item.id, rule: item.rule })),
      nested: family === 'kaup' ? ['candidate.transport-kaup:departure_board_v1', 'candidate.transport-kaup:route_strips_v1', 'candidate.transport-kaup:next_departure_queue_v1'] : [],
      responsive: TRANSPORT_BREAKPOINT_CONTEXTS.filter((item) => item.family === family).map((item) => ({ ...item })), media: family === 'bus' ? [{ kind: 'route-map', state: 'optional-consumer-media' }] : [], a11y: spec.a11y,
      fixtures: plans.map((item) => item.id), evidence: [fact(`fact.transport-${family}-lane`, `${states.length} source-derived state records and ${plans.length} planned specimens`, [spec.path])],
      consumers: lane.real_route_representatives.filter((item) => item.family === family && Number.isInteger(item.route_id)).map((item) => ({ route_id: item.route_id, proof_label: item.proof_label })),
      reachability: family === 'kaup' ? 'production-baseline-plus-experiment-off-source-implementations' : 'production-reachable-not-observed-by-this-lane',
      overrides: [], gaps: ['component-scoped capture pending'], blockers: ['controlled specimen and real-page reconciliation pending'],
      alternatives: family === 'kaup' ? ['baseline and experiment implementations remain NOT_MERGED'] : [],
    });
  });
}

function medallionContract(lane) {
  return contract({
    id: 'candidate.event-token-medallions', role: 'Event identity, source, program, admission and badge tokens',
    bindings: [source(MEDALLION_LOGICAL_PATH), ...lane.resource_candidates.map((item) => source(item.source_path, 'related-resource', item.reachability === 'production-observed' ? 'observed' : 'deterministic'))],
    relationship: 'one-to-many', confidence: 'deterministic', anatomy: ['token-groups', 'optional-top-main-token', 'inline-token-group', 'token-media-or-pill'],
    props: { event: { required: true }, layout: { required: false, default: lane.defaults.layout, union: lane.axes.layouts }, allowTopSlot: { required: false, default: lane.defaults.allow_top_slot, union: [true, false] } },
    variants: { layout: lane.axes.layouts, slot: lane.axes.slots, role: lane.axes.roles, kind: lane.axes.kinds, identity_resolution: lane.axes.identity_resolutions, cardinality: lane.axes.cardinalities },
    states: { rendered: [false, true], media: lane.axes.media }, valid: lane.transition_records.map((item) => ({ id: item.id, rule: item.rule })),
    invalid: ['desktop-slots must not retain pill tokens', 'conflicting source identity must not emit organizer identity tokens', 'top slot contains at most one Main token'], nested: [],
    responsive: lane.responsive_contexts, media: [{ kind: 'image-fallback', states: lane.axes.media }], a11y: ['image alt and link semantics pending component capture'], fixtures: lane.specimen_plan.map((item) => item.id),
    evidence: [fact('fact.medallion-projection', 'Source projection caps identities at 3, visible tokens at 6 and top-slot tokens at 1', [MEDALLION_LOGICAL_PATH])],
    consumers: lane.production_route_plan.map((item) => ({ id: item.id, context: item.context, reachability: item.reachability })), reachability: 'mixed-production-lab-and-source-only-resources',
    overrides: lane.mismatches.filter((item) => item.kind === 'geometry-documentation').map((item) => ({ source_path: 'src/components/DesktopEventPage.astro', scope: 'consumer-and-slot geometry', conclusion: 'local production override' })),
    gaps: ['related medallion-like resources remain distinct', 'consumer-scoped geometry requires captures'], blockers: ['identity conflicts and overflow require controlled specimens'],
    alternatives: lane.resource_candidates.map((item) => ({ resource_family: item.resource_family, equivalence_status: item.equivalence_status })),
  });
}

function artifactContracts(lane) {
  return lane.systems.map((system) => contract({
    id: system.id === FOCUS_SYSTEM_ID ? 'candidate.artifacts-focus-egg' : 'candidate.artifacts-amber', role: system.semantic_scope,
    bindings: system.source_bindings.map((item) => source(item.source_path, 'definition-or-consumer')), relationship: 'one-to-many', confidence: 'deterministic',
    anatomy: system.id === FOCUS_SYSTEM_ID ? ['artifact-glyph', 'state-label', 'find-action', 'found-label', 'catalog-card-or-saved-demo'] : ['mobile-rail-artifact', 'collection-slot', 'open-detail-action', 'dialog'],
    props: system.id === FOCUS_SYSTEM_ID ? { eggId: { required: true }, title: { required: true }, state: { required: true, union: system.state_axes.artifact }, compact: { required: false, default: false } } : { source_contract: { status: 'multiple-components-and-runtime-persistence' } },
    variants: system.state_axes, states: system.state_axes, valid: system.gates, invalid: system.id === AMBER_SYSTEM_ID ? ['production site mode can never enable Amber rail or active collection'] : ['unknown IDs or payloads over 4096 bytes are rejected'],
    nested: [], responsive: lane.responsive_contexts.filter((item) => item.system_id === system.id), media: [{ kind: 'glyph-or-illustrated-collectible', capture_status: 'pending' }],
    a11y: system.id === FOCUS_SYSTEM_ID ? ['keyboard focus-visible on eligible find action', 'state/status text updates'] : ['aria-pressed transition', 'dialog close and focus restoration', 'live-region collection status'],
    fixtures: lane.specimen_plan.filter((item) => item.system_id === system.id).map((item) => item.id), evidence: system.source_bindings,
    consumers: system.route_contexts, reachability: system.reachability.status, overrides: [],
    gaps: ['source/specimen/page reconciliation pending'], blockers: system.reachability.production_observed ? [] : ['must not be promoted to production-observed'],
    alternatives: [system.relationship_to_other_artifact_systems],
  }));
}

export function buildCandidateContracts({ eventPresentationRecords = [] } = {}) {
  if (!Array.isArray(eventPresentationRecords)) throw new TypeError('eventPresentationRecords must be an array');
  const transport = buildTransportDecoderLane();
  const medallions = buildMedallionDecoderLane();
  const artifacts = buildArtifactDecoderLane();
  const records = [
    ...presentationContracts(eventPresentationRecords), buttonContract(), ...transportContracts(transport),
    medallionContract(medallions), ...artifactContracts(artifacts),
  ].sort((a, b) => a.id.localeCompare(b.id));
  assertCandidateContracts(records);
  return clone(records);
}

export function assertCandidateContract(record) {
  if (!record || record.schema_version !== CANDIDATE_CONTRACT_SCHEMA) throw new Error('Invalid candidate contract schema');
  if (!RELATIONSHIP_KINDS.includes(record.relationship_kind)) throw new Error(`Unknown relationship kind: ${record.relationship_kind}`);
  if (!CONFIDENCE_LEVELS.includes(record.confidence)) throw new Error(`Unknown confidence: ${record.confidence}`);
  if (record.decision !== 'NOT_MERGED' || record.recommendation !== 'unresolved' || record.normalization_allowed !== false || record.normative_status !== 'candidate-as-is-not-accepted') throw new Error('Candidate contract normalization STOP violated');
  for (const field of REQUIRED_CONTRACT_FIELDS) if (!(field in (record.candidate_contract || {}))) throw new Error(`Candidate contract field missing: ${field}`);
  if (!String(record.candidate_contract.version).startsWith('0.')) throw new Error('Candidate contract version must remain 0.x');
  if (!Array.isArray(record.source_bindings) || !record.source_bindings.length || record.source_bindings.some((item) => item.source_sha !== SOURCE_SHA || !item.logical_path)) throw new Error('Candidate source binding is missing or unpinned');
  if (record.candidate_contract.token_refs.length !== 0 || record.candidate_contract.token_evidence_status !== 'raw-source-references-only-no-tokenization') throw new Error('Token normalization is forbidden');
  const { detached_contract_sha256, ...unsigned } = record;
  if (!/^[a-f0-9]{64}$/u.test(detached_contract_sha256 || '') || detached_contract_sha256 !== digest(unsigned)) throw new Error('Detached candidate contract hash mismatch');
  const serialized = JSON.stringify(record);
  if (/(?:"decision":"(?:MERGED|ACCEPTED)"|"normalization_allowed":true|"normative_status":"accepted")/u.test(serialized)) throw new Error('Candidate contract contains a TO-BE acceptance claim');
  return true;
}

export function assertCandidateContracts(records) {
  if (!Array.isArray(records) || records.length < 9) throw new Error('Candidate contract suite must include nine AS-IS candidates');
  if (new Set(records.map((item) => item.id)).size !== records.length) throw new Error('Duplicate candidate contract ID');
  for (const record of records) assertCandidateContract(record);
  return true;
}

export function stableSerializeCandidateContracts(records = buildCandidateContracts()) {
  assertCandidateContracts(records);
  return `${records.map((record) => JSON.stringify(stable(record))).join('\n')}\n`;
}
