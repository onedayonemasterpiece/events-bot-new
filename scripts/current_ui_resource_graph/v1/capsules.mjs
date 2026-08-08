import { createHash } from 'node:crypto';
import { buildTransportDecoderLane } from './transport.mjs';
import { buildMedallionDecoderLane } from './medallions.mjs';
import { buildArtifactDecoderLane } from './artifacts.mjs';
import {
  RECONCILIATION_CONCLUSIONS,
  SOURCE_SHA,
  assertCandidateContracts,
  buildCandidateContracts,
} from './contracts.mjs';

export { RECONCILIATION_CONCLUSIONS };

export const CAPSULE_SCHEMA = 'current_ui_decoder_reconciliation_capsule_v1';
export const CAPSULE_REVIEW_STATUSES = Object.freeze(['pending']);
export const CAPSULE_DIRECTORIES = Object.freeze([
  '01-event-presentation-states', '02-button-cta', '03-media-heavy',
  '04-transport', '05-medallions', '06-artifacts',
]);
export const CAPSULE_FILES = Object.freeze([
  'capsule.json', 'source-facts.jsonl', 'candidate-contract-ref.json',
  'specimen-observation-refs.jsonl', 'real-page-verification-refs.jsonl',
  'state-token-dependency-map.json', 'override-findings.jsonl', 'mismatch-refs.jsonl',
  'unresolved-refs.jsonl', 'evidence-index.json', 'REVIEW.md',
]);

function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map((key) => [key, stable(value[key])]));
  return value;
}
function clone(value) { return JSON.parse(JSON.stringify(stable(value))); }
function sha(value) { return createHash('sha256').update(JSON.stringify(stable(value))).digest('hex'); }
function sourceFact(id, statement, paths, confidence = 'deterministic') {
  return { id, statement, source_sha: SOURCE_SHA, source_paths: paths, confidence, evidence_channel: 'exact-candidate-source', browser_observation_claimed: false };
}
function planned(id, componentRef, contexts, fixtureClass) {
  return {
    id, component_ref: componentRef, fixture_class: fixtureClass, contexts,
    status: 'planned-not-captured', observation_ref: null,
    proof_label: 'controlled-specimen-plan-not-observed',
  };
}
function pagePending(routeFamily, refs = []) {
  return {
    route_family: routeFamily, references: refs, status: refs.length ? 'source-route-reference-not-visually-reviewed' : 'pending-selection-and-capture',
    screenshot_observation_refs: [], production_observed_by_capsule: false,
  };
}
function makeCapsule({ number, slug, title, contractIds, facts, inference, openQuestions, decision, specimens, realPage, mapping, overrides = [] }) {
  const directory = `${String(number).padStart(2, '0')}-${slug}`;
  const id = `capsule.${directory}`;
  const files = {
    'capsule.json': {
      schema_version: CAPSULE_SCHEMA, id, title, directory, source_sha: SOURCE_SHA,
      contract_ids: contractIds, evidence_status: 'planned-not-captured', review_status: 'pending',
      decision: 'NOT_MERGED', recommendation: 'unresolved', normalization_allowed: false,
    },
    'source-facts.jsonl': [
      ...facts.map((item) => ({ ...item, record_kind: 'observed-source-fact' })),
      ...inference.map((statement, index) => ({ id: `${id}.inference.${index + 1}`, record_kind: 'inferred-interpretation', statement, confidence: 'inferred' })),
      { id: `${id}.as-is-decision`, record_kind: 'decoder-boundary-decision', statement: decision, decision: 'NOT_MERGED' },
    ],
    'candidate-contract-ref.json': { contract_ids: contractIds, contract_hashes_attached_at_materialization: false },
    'specimen-observation-refs.jsonl': specimens.map((item) => ({ ...item, evidence_kind: 'planned-specimen-reference', observation_attached: false })),
    'real-page-verification-refs.jsonl': [{ ...realPage, evidence_kind: 'real-page-verification-reference' }],
    'state-token-dependency-map.json': { ...mapping, token_mapping_status: 'raw-source-references-only-no-tokenization' },
    'override-findings.jsonl': overrides.map((item, index) => ({ id: `${id}.override.${index + 1}`, ...item, style_divergence_policy: 'source and computed-style divergences are evidence only; they are not mismatches without reviewed reconciliation' })),
    'mismatch-refs.jsonl': [],
    'unresolved-refs.jsonl': openQuestions.map((statement, index) => ({ id: `${id}.unresolved.${index + 1}`, statement, blocks_handoff: true, status: 'open' })),
    'evidence-index.json': { status: 'materialized-by-snapshot-writer', entries: [] },
    'REVIEW.md': `# ${title}\n\nStatus: **PENDING HUMAN VISUAL AND SEMANTIC REVIEW**\n\nNo acceptance, normalization, merge, split, or Penpot materialization is authorized.\n`,
  };
  const unsigned = {
    schema_version: CAPSULE_SCHEMA, id, directory, title, canonical_files: [...CAPSULE_FILES], files,
    decision: 'NOT_MERGED', recommendation: 'unresolved', normalization_allowed: false,
  };
  return clone({ ...unsigned, detached_capsule_sha256: sha(unsigned) });
}

function contractById(contracts, id) {
  const result = contracts.find((item) => item.id === id);
  if (!result) throw new Error(`Missing candidate contract for capsule: ${id}`);
  return result;
}

export function buildReconciliationCapsules({ candidateContracts = buildCandidateContracts() } = {}) {
  assertCandidateContracts(candidateContracts);
  const transport = buildTransportDecoderLane();
  const medallions = buildMedallionDecoderLane();
  const artifacts = buildArtifactDecoderLane();
  const capsules = [
    makeCapsule({
      number: 1, slug: 'event-presentation-states', title: 'Event Detail explicit presentation states',
      contractIds: ['candidate.event-detail-presentation'],
      facts: [
        sourceFact('capsule-fact.event.editorial', 'DesktopEventPage has an editorial branch with a separate side action placement', ['src/components/DesktopEventPage.astro', 'src/components/DesktopEventActionPanel.astro']),
        sourceFact('capsule-fact.event.split', 'DesktopEventPage has a distinct split portrait/poster branch with inline action placement', ['src/components/DesktopEventPage.astro', 'src/lib/desktopEventPresentation.ts']),
      ],
      inference: ['Editorial and split can be represented as one candidate with a closed axis, but that structure is not accepted or normalized.'],
      openQuestions: ['Do source/specimen/page geometry and a11y reconcile for both branches?', 'Are branch-local overrides intentional component contracts or page composition?'],
      decision: 'Keep both AS-IS states distinct; NOT_MERGED.',
      specimens: [planned('capsule-plan.event-editorial', 'candidate.event-detail-presentation', ['1023', '1024', '1728'], 'editorial-landscape'), planned('capsule-plan.event-split', 'candidate.event-detail-presentation', ['1023', '1024', '1728'], 'split-portrait-poster')],
      realPage: pagePending('page-family.event-detail'),
      mapping: { state_axes: contractById(candidateContracts, 'candidate.event-detail-presentation').candidate_contract.state_axes, token_refs: [], dependencies: ['candidate.button-cta-fragmented', 'candidate.event-media', 'candidate.event-token-medallions'] },
      overrides: [{ source_path: 'src/components/DesktopEventPage.astro', scope: 'branch-local layout and action placement', conclusion: 'unresolved mapping', review_status: 'pending' }],
    }),
    makeCapsule({
      number: 2, slug: 'button-cta', title: 'Button and CTA fragmentation', contractIds: ['candidate.button-cta-fragmented'],
      facts: [
        sourceFact('capsule-fact.button.generic', 'Button declares generic variant/size/state axes and renders anchor or button roots', ['src/components/design-system/Button.astro']),
        sourceFact('capsule-fact.button.event-action', 'DesktopEventActionPanel separately renders anchor, button, CalendarLink or disabled status action states', ['src/components/DesktopEventActionPanel.astro', 'src/components/CalendarLink.astro']),
      ],
      inference: ['These implementations may form a family, but visual or semantic equivalence is not asserted.'],
      openQuestions: ['Which consumers use generic Button versus local action markup?', 'Which differences are local production overrides versus duplicated implementations?'],
      decision: 'Record fragmentation and alternatives; do not merge.',
      specimens: [planned('capsule-plan.button-matrix', 'candidate.button-cta-fragmented', ['390', '1024', '1728'], 'generic-button-state-matrix'), planned('capsule-plan.cta-layouts', 'candidate.button-cta-fragmented', ['1024', '1728'], 'editorial-stacked-and-split-inline')],
      realPage: pagePending('page-family.event-detail'),
      mapping: { state_axes: contractById(candidateContracts, 'candidate.button-cta-fragmented').candidate_contract.state_axes, token_refs: [], dependencies: ['src/components/CalendarLink.astro'] },
      overrides: [{ source_path: 'src/components/DesktopEventPage.astro', scope: 'action placement and local CTA styling', conclusion: 'local production override', review_status: 'pending' }],
    }),
    makeCapsule({
      number: 3, slug: 'media-heavy', title: 'Media-heavy Event Detail formats', contractIds: ['candidate.event-media'],
      facts: [sourceFact('capsule-fact.media.formats', 'Primary large frame, large poster companion and small remaining-photo previews are distinct AS-IS formats', ['src/components/DesktopEventPage.astro', 'src/components/EventMediaRail.astro'])],
      inference: ['Media resources may later materialize as separate components, but this decoder does not choose that boundary.'],
      openQuestions: ['Do computed fit, crop and fallback rules match the source at all selected breakpoints?', 'Which media rail interactions are page-only?'],
      decision: 'Preserve large and small resource formats separately; NOT_MERGED.',
      specimens: [planned('capsule-plan.media-companion', 'candidate.event-media', ['1024', '1728'], 'editorial-poster-companion'), planned('capsule-plan.media-split-rail', 'candidate.event-media', ['1024', '1728'], 'split-poster-thumbnail-rail')],
      realPage: pagePending('page-family.event-detail'),
      mapping: { state_axes: contractById(candidateContracts, 'candidate.event-media').candidate_contract.state_axes, token_refs: [], dependencies: ['src/components/DesktopEventPage.astro', 'src/components/EventMediaRail.astro'] },
      overrides: [{ source_path: 'src/components/DesktopEventPage.astro', scope: 'editorial/split media treatment CSS', conclusion: 'local production override', review_status: 'pending' }],
    }),
    makeCapsule({
      number: 4, slug: 'transport', title: 'Rail, bus and Kaup transport state families',
      contractIds: ['candidate.transport-rail', 'candidate.transport-bus', 'candidate.transport-kaup'],
      facts: [
        sourceFact('capsule-fact.transport.separate', 'Rail, bus and Kaup have separate source definitions and closed state axes', [transport.source_paths.rail, transport.source_paths.bus, transport.source_paths.kaup]),
        sourceFact('capsule-fact.transport.experiment-off', 'Kaup alternative timetable treatments are source-only/controlled and the production baseline experiment mode is off', [transport.source_paths.kaup, transport.source_paths.experiment]),
      ],
      inference: ['The three transport definitions share a domain label but are not asserted to be visual variants of one component.'],
      openQuestions: ['Do planned warning/tight/open states match component-scoped captures?', 'Do exact route representatives cover consumer overrides?'],
      decision: 'Keep three families and experiment-off separation; NOT_MERGED.',
      specimens: transport.specimen_plan.map((item) => ({ id: item.id, component_ref: item.component_id, fixture_class: item.specimen_family, contexts: item.viewport_widths || item.required_contexts, status: item.observation_status || 'planned-not-captured', observation_ref: null, proof_label: item.proof_label })),
      realPage: pagePending('page-family.event-detail', transport.real_route_representatives.filter((item) => Number.isInteger(item.route_id)).map((item) => ({ route_id: item.route_id, family: item.family, source_reference_only: true }))),
      mapping: { state_axes: transport.axis_definitions, token_refs: [], dependencies: Object.values(transport.source_paths), invalid_combinations: transport.invalid_combinations.map((item) => item.id) },
    }),
    makeCapsule({
      number: 5, slug: 'medallions', title: 'Event token medallion layouts and identity states', contractIds: ['candidate.event-token-medallions'],
      facts: [
        sourceFact('capsule-fact.medallion.layouts', 'EventTokenMedallions declares inline and desktop-slots layouts with optional top slot', ['src/components/EventTokenMedallions.astro']),
        sourceFact('capsule-fact.medallion.resources', 'Listing, exhibition and lab medallion-like resources remain distinct and NOT_MERGED', medallions.resource_candidates.map((item) => item.source_path)),
      ],
      inference: ['Shared identity data does not prove shared visual component identity.'],
      openQuestions: ['How do 0/1/many/overflow and image fallback render in controlled specimens?', 'Which consumer geometry differences are intentional?'],
      decision: 'Preserve resource candidates and slot/layout axes; NOT_MERGED.',
      specimens: medallions.specimen_plan.map((item) => ({ id: item.id, component_ref: item.component_id, fixture_class: item.context, contexts: item.required_contexts || [], status: item.observation_status, observation_ref: null, proof_label: item.proof_label })),
      realPage: pagePending('page-family.event-detail', medallions.production_route_plan.map((item) => ({ id: item.id, context: item.context, source_reference_only: true }))),
      mapping: { state_axes: medallions.axes, token_refs: [], dependencies: medallions.resource_candidates.map((item) => item.source_path), transition_records: medallions.transition_records.map((item) => item.id) },
      overrides: [{ source_path: 'src/components/DesktopEventPage.astro', scope: 'slot-specific medallion geometry', conclusion: 'local production override', review_status: 'pending' }],
    }),
    makeCapsule({
      number: 6, slug: 'artifacts', title: 'Focus Egg and Amber artifact systems', contractIds: ['candidate.artifacts-focus-egg', 'candidate.artifacts-amber'],
      facts: [
        sourceFact('capsule-fact.artifact.independent', 'Focus Egg and Amber use different IDs, persistence, gates, states and reachability', artifacts.systems.flatMap((item) => item.component_paths)),
        sourceFact('capsule-fact.artifact.production-gate', 'Amber active collection/rail is hard-blocked in production; the unavailable shell is the production route result', ['src/lib/artifacts.mjs', 'src/pages/artefakty/index.astro']),
      ],
      inference: ['A future collectibles parent is possible but unresolved; the two systems are not variants today.'],
      openQuestions: ['Do eligible-to-found glyph and counter inconsistencies reproduce in controlled captures?', 'Does keyboard focus and dialog focus restoration match source expectations?'],
      decision: 'Keep the systems independent; do not synthesize a parent or production state.',
      specimens: artifacts.specimen_plan.map((item) => ({ id: item.id, component_ref: item.component_id, fixture_class: item.context, contexts: item.required_contexts, status: item.observation_status, observation_ref: null, proof_label: item.proof_label })),
      realPage: pagePending('mixed-lab-and-source-only', artifacts.systems.flatMap((item) => item.route_contexts).map((item) => ({ ...item, source_reference_only: true }))),
      mapping: { state_axes: Object.fromEntries(artifacts.systems.map((item) => [item.id, item.state_axes])), token_refs: [], dependencies: artifacts.systems.flatMap((item) => item.component_paths), transition_records: artifacts.transition_records.map((item) => item.id) },
    }),
  ];
  assertReconciliationCapsules(capsules, candidateContracts);
  return clone(capsules);
}

function allowedMismatchConclusion(record) {
  if (RECONCILIATION_CONCLUSIONS.includes(record.conclusion)) return record.conclusion;
  if (record.kind === 'geometry-documentation') return 'local production override';
  if (record.kind === 'lab-equivalence') return 'duplicated implementation';
  return 'unresolved mapping';
}

export function buildConsolidatedMismatchRecords() {
  const medallions = buildMedallionDecoderLane();
  const artifacts = buildArtifactDecoderLane();
  const rows = [...medallions.mismatches, ...artifacts.mismatches].map((item) => ({
    ...item,
    source_lane_conclusion: item.conclusion,
    conclusion: allowedMismatchConclusion(item),
    review_status: 'pending',
    evidence_limit: 'source-derived-until-specimen-and-page-reconciliation',
    decision: 'NOT_MERGED', recommendation: 'unresolved', normalization_allowed: false,
  })).sort((a, b) => a.id.localeCompare(b.id));
  assertMismatchRecords(rows);
  return clone(rows);
}

export function buildConsolidatedUnresolvedRecords() {
  const transport = buildTransportDecoderLane();
  const medallions = buildMedallionDecoderLane();
  const artifacts = buildArtifactDecoderLane();
  return clone([
    ...medallions.unresolved,
    ...artifacts.unresolved,
    { id: 'unresolved.contracts.button-cta-boundary', kind: 'candidate-boundary', reason: 'Generic Button and local CTA implementations are not reconciled.', blocks_handoff: true },
    { id: 'unresolved.contracts.event-presentation-record-binding', kind: 'evidence-binding', reason: 'Existing event-presentation record IDs and real-page captures must be attached by the integration run.', blocks_handoff: true },
    { id: 'unresolved.contracts.transport-experiment-source-only', kind: 'reachability', reason: 'Kaup alternative treatments remain experiment-off/source-only and must not be promoted.', blocks_handoff: false, state_record_ids: transport.state_records.filter((item) => ['source-only', 'experiment-off'].includes(item.reachability)).map((item) => item.id) },
    { id: 'unresolved.contracts.human-capsule-review', kind: 'human-review', reason: 'All six capsules require visual and semantic review after captures are attached.', blocks_handoff: true },
  ].map((item) => ({ ...item, decision: 'NOT_MERGED', recommendation: 'unresolved', normalization_allowed: false })).sort((a, b) => a.id.localeCompare(b.id)));
}

export function buildConsolidatedSpecimenPlan() {
  const transport = buildTransportDecoderLane();
  const medallions = buildMedallionDecoderLane();
  const artifacts = buildArtifactDecoderLane();
  const generic = [
    planned('specimen-plan.contracts.event-editorial-and-split', 'candidate.event-detail-presentation', ['1023', '1024', '1728'], 'event-presentation-pair'),
    planned('specimen-plan.contracts.button-cta', 'candidate.button-cta-fragmented', ['390', '1024', '1728'], 'button-cta-representative-matrix'),
    planned('specimen-plan.contracts.media-heavy', 'candidate.event-media', ['1024', '1728'], 'large-poster-and-small-photo-previews'),
    planned('specimen-plan.contracts.event-card-listing', 'candidate.event-card-listing-representations', ['390', '720', '1024', '1728'], 'card-and-row-contexts'),
    planned('specimen-plan.contracts.search-results', 'candidate.search-results', ['390', '768', '1728'], 'empty-loading-results-error'),
    planned('specimen-plan.contracts.favorites', 'candidate.favorites-saved-events', ['390', '768', '1728'], 'empty-populated-storage-unavailable'),
  ];
  return clone([...generic, ...transport.specimen_plan, ...medallions.specimen_plan, ...artifacts.specimen_plan].map((item) => ({ ...item, evidence_original_status: item.observation_status || item.status || item.plan_status || 'planned', observation_status: item.observation_status || item.status || 'planned-not-captured' })).sort((a, b) => a.id.localeCompare(b.id)));
}

export function buildDecoderReconciliationBundle({ eventPresentationRecords = [] } = {}) {
  const candidate_contracts = buildCandidateContracts({ eventPresentationRecords });
  const bundle = {
    schema_version: CAPSULE_SCHEMA, source_sha: SOURCE_SHA,
    candidate_contracts,
    capsules: buildReconciliationCapsules({ candidateContracts: candidate_contracts }),
    specimen_plan: buildConsolidatedSpecimenPlan(), specimen_observations: [],
    mismatches: buildConsolidatedMismatchRecords(), unresolved: buildConsolidatedUnresolvedRecords(),
    constraints: { as_is_only: true, capture_claimed: false, human_review_claimed: false, merge: false, split: false, normalization: false, tokenization: false, penpot_mutation: false, astro_css_mutation: false },
    decision: 'NOT_MERGED', recommendation: 'unresolved', normalization_allowed: false,
  };
  assertDecoderReconciliationBundle(bundle);
  return clone(bundle);
}

export function assertMismatchRecords(rows) {
  if (new Set(rows.map((item) => item.id)).size !== rows.length) throw new Error('Duplicate mismatch ID');
  for (const item of rows) {
    if (!RECONCILIATION_CONCLUSIONS.includes(item.conclusion)) throw new Error(`Unsupported mismatch conclusion: ${item.conclusion}`);
    if (item.decision !== 'NOT_MERGED' || item.normalization_allowed !== false) throw new Error('Mismatch normalization STOP violated');
    if (/809/u.test(String(item.id)) || item.kind === 'style-divergence') throw new Error('Raw style divergence cannot automatically become a mismatch');
  }
  return true;
}

export function assertReconciliationCapsules(capsules, contracts = buildCandidateContracts()) {
  assertCandidateContracts(contracts);
  if (!Array.isArray(capsules) || capsules.length !== 6) throw new Error('Exactly six reconciliation capsules are required');
  const contractIds = new Set(contracts.map((item) => item.id));
  if (new Set(capsules.map((item) => item.directory)).size !== 6 || CAPSULE_DIRECTORIES.some((directory) => !capsules.some((item) => item.directory === directory))) throw new Error('Canonical capsule directory set is incomplete');
  for (const capsule of capsules) {
    if (capsule.schema_version !== CAPSULE_SCHEMA || capsule.decision !== 'NOT_MERGED' || capsule.normalization_allowed !== false) throw new Error(`Capsule STOP violated: ${capsule.id}`);
    if (CAPSULE_FILES.some((file) => !(file in capsule.files))) throw new Error(`Capsule canonical file missing: ${capsule.id}`);
    if (capsule.files['capsule.json'].review_status !== 'pending' || !capsule.files['REVIEW.md'].includes('PENDING HUMAN VISUAL')) throw new Error(`Capsule falsely claims human review: ${capsule.id}`);
    if (capsule.files['specimen-observation-refs.jsonl'].some((item) => item.observation_attached !== false)) throw new Error(`Capsule falsely claims specimen observation: ${capsule.id}`);
    if (capsule.files['real-page-verification-refs.jsonl'].some((item) => item.production_observed_by_capsule !== false)) throw new Error(`Capsule falsely claims page observation: ${capsule.id}`);
    for (const id of capsule.files['candidate-contract-ref.json'].contract_ids) if (!contractIds.has(id)) throw new Error(`Dangling capsule contract ref: ${id}`);
    const { detached_capsule_sha256, ...unsigned } = capsule;
    if (detached_capsule_sha256 !== sha(unsigned)) throw new Error(`Detached capsule hash mismatch: ${capsule.id}`);
  }
  return true;
}

export function assertDecoderReconciliationBundle(bundle) {
  assertCandidateContracts(bundle.candidate_contracts);
  assertReconciliationCapsules(bundle.capsules, bundle.candidate_contracts);
  assertMismatchRecords(bundle.mismatches);
  if (bundle.specimen_observations.length !== 0 || bundle.constraints.capture_claimed !== false || bundle.constraints.human_review_claimed !== false) throw new Error('Bundle falsely claims capture or review');
  if (!['merge', 'split', 'normalization', 'tokenization', 'penpot_mutation', 'astro_css_mutation'].every((key) => bundle.constraints[key] === false)) throw new Error('Bundle normalization STOP violated');
  const planIds = bundle.specimen_plan.map((item) => item.id);
  if (new Set(planIds).size !== planIds.length) throw new Error('Duplicate consolidated specimen plan ID');
  if (!bundle.unresolved.some((item) => item.id === 'unresolved.contracts.human-capsule-review' && item.blocks_handoff)) throw new Error('Pending human review blocker missing');
  return true;
}

export function stableSerializeDecoderReconciliationBundle(bundle = buildDecoderReconciliationBundle()) {
  assertDecoderReconciliationBundle(bundle);
  return `${JSON.stringify(stable(bundle))}\n`;
}
