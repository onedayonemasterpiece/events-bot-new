import { createHash } from 'node:crypto';

export const ARTIFACT_LANE_SCHEMA = 'current_ui_component_decoder_v1_artifacts';
export const ARTIFACT_LANE_ID = 'artifact-easter-egg-as-is';
export const FOCUS_SYSTEM_ID = 'artifact-system.focus-egg-prototype-v1';
export const AMBER_SYSTEM_ID = 'artifact-system.amber-research-collectible-v1';
export const ARTIFACT_PARENT_ID = 'artifact-parent.collectibles-unresolved';

export const FOCUS_STATES = Object.freeze(['locked', 'eligible', 'found', 'unavailable']);
export const AMBER_RAIL_STATES = Object.freeze(['idle', 'awake', 'collecting', 'collected']);
export const AMBER_COLLECTION_STATES = Object.freeze(['empty', 'found', 'dialog-open']);

const SOURCE_SHA = 'ef7aa62e45c60f7a12da6160f490719c0721ec03';
const DECISION = 'NOT_MERGED';
const FOCUS_COMPONENTS = Object.freeze([
  'src/components/FocusEggArtifact.astro',
  'src/components/FocusEggCollectionCard.astro',
  'src/components/FocusEggSavedListDemo.astro',
]);
const AMBER_COMPONENTS = Object.freeze([
  'src/components/artifacts/ArtifactCollection.astro',
  'src/components/listings/AmberRailArtifact.astro',
]);

function sha(value) { return createHash('sha256').update(String(value)).digest('hex'); }
function componentId(path) { return `component.${sha(path).slice(0, 16)}`; }
function clone(value) { return JSON.parse(JSON.stringify(value)); }
function sortObject(value) {
  if (Array.isArray(value)) return value.map(sortObject);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map((key) => [key, sortObject(value[key])]));
  return value;
}
function proof(kind, sourcePath, source_scope = null) {
  return { kind, source_sha: SOURCE_SHA, source_path: sourcePath, source_scope };
}

export const ARTIFACT_RESPONSIVE_CONTEXTS = Object.freeze([
  {
    id: 'responsive.focus-artifact-max-420', system_id: FOCUS_SYSTEM_ID,
    query: '(max-width: 420px)', boundary: 420, probe_widths: [419, 421],
    source: proof('source-css-breakpoint-exact-candidate', 'src/components/FocusEggArtifact.astro'),
  },
  {
    id: 'responsive.focus-saved-min-760', system_id: FOCUS_SYSTEM_ID,
    query: '(min-width: 760px)', boundary: 760, probe_widths: [759, 761],
    source: proof('source-css-breakpoint-exact-candidate', 'src/components/FocusEggSavedListDemo.astro'),
  },
  {
    id: 'responsive.focus-catalog-min-680', system_id: FOCUS_SYSTEM_ID,
    query: '(min-width: 680px)', boundary: 680, probe_widths: [679, 681],
    source: proof('source-css-breakpoint-exact-candidate', 'src/pages/fokus-gruppa/kollektsiya/index.astro'),
  },
  {
    id: 'responsive.focus-catalog-min-920', system_id: FOCUS_SYSTEM_ID,
    query: '(min-width: 920px)', boundary: 920, probe_widths: [919, 921],
    source: proof('source-css-breakpoint-exact-candidate', 'src/pages/fokus-gruppa/kollektsiya/index.astro'),
  },
  {
    id: 'responsive.amber-collection-max-430', system_id: AMBER_SYSTEM_ID,
    query: '(max-width: 430px)', boundary: 430, probe_widths: [429, 431],
    source: proof('source-css-breakpoint-exact-candidate', 'src/components/artifacts/ArtifactCollection.astro'),
  },
  {
    id: 'responsive.amber-collection-max-850', system_id: AMBER_SYSTEM_ID,
    query: '(max-width: 850px)', boundary: 850, probe_widths: [849, 851],
    source: proof('source-css-breakpoint-exact-candidate', 'src/components/artifacts/ArtifactCollection.astro'),
  },
  {
    id: 'responsive.amber-rail-consumer-max-720', system_id: AMBER_SYSTEM_ID,
    query: '(max-width: 720px)', boundary: 720, probe_widths: [719, 721],
    source: proof('source-css-breakpoint-exact-candidate', 'src/components/listings/MobileListingRailSurface.astro', 'consumer-visibility'),
  },
]);

function focusSystem() {
  return {
    id: FOCUS_SYSTEM_ID,
    title: 'FocusEgg prototype collection',
    semantic_scope: 'focus-group laboratory prototype',
    relationship_to_other_artifact_systems: 'independent-not-a-variant',
    source_bindings: [
      ...FOCUS_COMPONENTS,
      'src/lib/focus-easter-eggs.ts',
      'src/pages/fokus-gruppa/kollektsiya/index.astro',
    ].map((path) => proof('source-ast-exact-candidate', path)),
    component_paths: [...FOCUS_COMPONENTS],
    route_contexts: [{ path: '/fokus-gruppa/kollektsiya/', classification: 'lab-only', proof_label: 'source-route-binding-exact-candidate' }],
    reachability: {
      status: 'lab-only', baseline_included: false, production_observed: false,
      executable_scope: 'FG-E12-saved-list-demo-only',
      note: 'The catalogue statically demonstrates all four states; only FG-E12 has a client collection transition.',
    },
    state_axes: {
      artifact: [...FOCUS_STATES], density: ['regular', 'compact'],
      contexts: ['standalone-artifact', 'catalog-card', 'saved-list-demo'],
    },
    state_resolution_precedence: ['found', 'unavailable', 'eligible', 'locked'],
    persistence: {
      storage_key: 'kenigevents:focus-eggs:prototype:v1', schema_version: 1,
      collection_version: 'focus-eggs-v1', max_bytes: 4096,
      accepted_ids: Array.from({ length: 12 }, (_unused, index) => `FG-E${String(index + 1).padStart(2, '0')}`),
    },
    catalog_baseline: {
      by_id: {
        'FG-E01': 'found', 'FG-E02': 'eligible', 'FG-E03': 'locked', 'FG-E04': 'locked',
        'FG-E05': 'eligible', 'FG-E06': 'unavailable', 'FG-E07': 'eligible', 'FG-E08': 'found',
        'FG-E09': 'eligible', 'FG-E10': 'locked', 'FG-E11': 'locked', 'FG-E12': 'locked',
      },
      counts: { found: 2, eligible_state: 4, locked: 5, unavailable: 1, progress_found: 2, progress_denominator: 11 },
      proof: proof('source-static-state-map-exact-candidate', 'src/pages/fokus-gruppa/kollektsiya/index.astro', 'prototypeStates'),
    },
    gates: [
      { id: 'focus.saved-list.distinct-renderable-count', condition: 'distinct_renderable_event_ids >= 3', result: 'FG-E12 eligible-or-found' },
      { id: 'focus.saved-list.below-threshold', condition: 'distinct_renderable_event_ids < 3', result: 'FG-E12 absent' },
      { id: 'focus.storage-validity', condition: 'schema=1, collectionVersion=focus-eggs-v1, payload<=4096 bytes, known IDs only', result: 'accept-and-deduplicate-found-ids' },
    ],
    responsive_context_ids: ARTIFACT_RESPONSIVE_CONTEXTS.filter((item) => item.system_id === FOCUS_SYSTEM_ID).map((item) => item.id),
    proof_label: 'source-model-exact-candidate-no-browser-claim',
    decision: DECISION, recommendation: 'unresolved', normalization_allowed: false,
  };
}

function amberSystem() {
  return {
    id: AMBER_SYSTEM_ID,
    title: 'Amber research collectible',
    semantic_scope: 'non-production research-only collectible',
    relationship_to_other_artifact_systems: 'independent-not-a-variant',
    source_bindings: [
      ...AMBER_COMPONENTS,
      'src/components/listings/MobileListingRailRow.astro',
      'src/components/listings/MobileListingRailSurface.astro',
      'src/components/listings/WeekendListingSurface.astro',
      'src/lib/artifacts.mjs',
      'src/pages/artefakty/index.astro',
      'src/pages/vyhodnye/index.astro',
    ].map((path) => proof('source-ast-exact-candidate', path)),
    component_paths: [...AMBER_COMPONENTS],
    route_contexts: [
      { path: '/vyhodnye/', role: 'tail-rail-discovery', public_production: 'hard-blocked' },
      { path: '/artefakty/', role: 'collection-or-unavailable-shell', public_production: 'unavailable-shell-only' },
    ],
    reachability: {
      status: 'source-only', baseline_included: false, production_observed: false,
      activation_scope: 'non-production-build-and-tail-flag-only',
      current_page_evidence_limit: 'Prior inventory summarized only empty 0/5; this lane attaches no capture and makes no browser-observed claim.',
    },
    state_axes: {
      rail: [...AMBER_RAIL_STATES], collection: [...AMBER_COLLECTION_STATES],
      motion: ['regular', 'reduced'], focus: ['not-focused', 'keyboard-focus'],
    },
    persistence: {
      storage_key: 'ke_artifact_collection_v1', legacy_storage_key: 'ke_amber_artifact_prototype_v1:tail',
      schema_version: 1, collection_id: 'kaliningrad_artifacts_v1', active_artifact_id: 'amber_cosmonaut',
      accepted_record_status: 'found', cross_tab_storage_listener: true,
    },
    collection_baseline: {
      slot_count: 5, active_slot_count: 1, reserved_slot_count: 4,
      states: ['empty', 'found'], initial_progress: '0/5', found_progress: '1/5',
      proof: proof('source-static-state-map-exact-candidate', 'src/lib/artifacts.mjs', 'ARTIFACT_COLLECTION_SLOTS'),
    },
    gates: [
      {
        id: 'amber.production-hard-block', expression: "siteMode !== 'production' && flag === 'tail'",
        truth_table: [
          { site_mode: 'production', flag: 'off', enabled: false },
          { site_mode: 'production', flag: 'tail', enabled: false },
          { site_mode: 'secret_candidate', flag: 'off', enabled: false },
          { site_mode: 'secret_candidate', flag: 'tail', enabled: true },
        ],
        invariant: 'No production flag value enables the Amber rail or collection.',
        proof: proof('source-route-gate-exact-candidate', 'src/lib/artifacts.mjs', 'isAmberArtifactResearchEnabled'),
      },
      { id: 'amber.assignment', condition: 'enabled and at least one unique valid event on weekend boundary dates', result: 'one deterministic event ID or null' },
      { id: 'amber.rail-placement', condition: "placement='tail'", result: 'render at mobile event rail tail only' },
    ],
    responsive_context_ids: ARTIFACT_RESPONSIVE_CONTEXTS.filter((item) => item.system_id === AMBER_SYSTEM_ID).map((item) => item.id),
    proof_label: 'source-model-exact-candidate-no-browser-claim',
    decision: DECISION, recommendation: 'unresolved', normalization_allowed: false,
  };
}

export function buildArtifactSystemRecords() { return clone([focusSystem(), amberSystem()]); }

export function buildArtifactStateRecords() {
  const records = [
    ...FOCUS_STATES.map((state) => ({
      id: `artifact-state.focus.${state}`, system_id: FOCUS_SYSTEM_ID, axis: 'artifact-state', value: state,
      selectors: [`[data-focus-egg-artifact][data-egg-state="${state}"]`, `.focus-egg-card[data-egg-state="${state}"]`],
      proof: proof('source-state-branch-exact-candidate', 'src/components/FocusEggArtifact.astro'),
    })),
    ...AMBER_RAIL_STATES.map((state) => ({
      id: `artifact-state.amber.rail.${state}`, system_id: AMBER_SYSTEM_ID, axis: 'rail-state', value: state,
      selectors: state === 'idle' ? ['[data-amber-artifact]:not(.is-awake):not(.is-collected)'] : [`[data-amber-artifact].is-${state}`],
      proof: proof('source-state-branch-exact-candidate', 'src/components/listings/AmberRailArtifact.astro'),
    })),
    ...AMBER_COLLECTION_STATES.map((state) => ({
      id: `artifact-state.amber.collection.${state}`, system_id: AMBER_SYSTEM_ID, axis: 'collection-state', value: state,
      selectors: state === 'dialog-open' ? ['[data-artifact-dialog][open]'] : [`[data-artifact-slot="amber_cosmonaut"][data-artifact-state="${state}"]`],
      proof: proof('source-state-branch-exact-candidate', 'src/components/artifacts/ArtifactCollection.astro'),
    })),
  ];
  return clone(records.map((item) => ({ ...item, decision: DECISION, normalization_allowed: false })));
}

export function buildArtifactTransitionRecords() {
  return clone([
    {
      id: 'artifact-transition.focus.saved-threshold', system_id: FOCUS_SYSTEM_ID,
      from: 'absent', event: 'third-distinct-renderable-event', to: 'eligible-or-found-from-storage',
      observable_updates: ['anchor.hidden=false', 'accessible-equivalent.hidden=false', 'data-visible-count', 'status-text'],
      proof: proof('source-client-transition-exact-candidate', 'src/components/FocusEggSavedListDemo.astro', 'render'),
    },
    {
      id: 'artifact-transition.focus.eligible-to-found', system_id: FOCUS_SYSTEM_ID,
      from: 'eligible', event: 'keyboard-or-pointer-click-on-[data-focus-egg-find]', to: 'found',
      persistence: 'kenigevents:focus-eggs:prototype:v1', emitted_event: 'focus-egg-found',
      observable_updates: ['data-egg-state=found', 'find.hidden=true', 'found-label.hidden=false', 'state-label=found', 'status-text'],
      omitted_update: 'focus-egg-artifact__glyph text remains eligible glyph',
      proof: proof('source-client-transition-exact-candidate', 'src/components/FocusEggSavedListDemo.astro', 'click-handler'),
    },
    {
      id: 'artifact-transition.focus.catalog-storage-override', system_id: FOCUS_SYSTEM_ID,
      from: 'static-prototype-state', event: 'page-bootstrap-or-focus-egg-found', to: 'found-when-ID-in-local-storage',
      observable_updates: ['data-egg-state=found', 'state-label=found', 'found-count', 'eligible-count', 'meter'],
      omitted_updates: ['catalog-mark-glyph', 'cross-tab-storage-event'],
      proof: proof('source-client-transition-exact-candidate', 'src/pages/fokus-gruppa/kollektsiya/index.astro', 'renderLocalCollection'),
    },
    {
      id: 'artifact-transition.amber.idle-to-awake', system_id: AMBER_SYSTEM_ID,
      from: 'idle', event: 'intersection-ratio-at-least-0.72-or-reduced-motion-or-no-observer', to: 'awake',
      proof: proof('source-client-transition-exact-candidate', 'src/components/listings/AmberRailArtifact.astro', 'IntersectionObserver'),
    },
    {
      id: 'artifact-transition.amber.awake-to-collected', system_id: AMBER_SYSTEM_ID,
      from: 'awake', event: 'first-click', through: 'collecting', to: 'collected',
      persistence: 'ke_artifact_collection_v1', emitted_event: 'kenigevents:artifact-collected',
      observable_updates: ['is-collecting', 'is-collected', 'aria-pressed=true', 'aria-label', 'live-region'],
      timer_ms: { regular: 460, reduced_motion: 0 },
      proof: proof('source-client-transition-exact-candidate', 'src/components/listings/AmberRailArtifact.astro', 'click-handler'),
    },
    {
      id: 'artifact-transition.amber.collected-to-detail', system_id: AMBER_SYSTEM_ID,
      from: 'collected', event: 'subsequent-click', to: 'collection-detail-navigation',
      proof: proof('source-client-transition-exact-candidate', 'src/components/listings/AmberRailArtifact.astro', 'click-handler'),
    },
    {
      id: 'artifact-transition.amber.collection-empty-to-found', system_id: AMBER_SYSTEM_ID,
      from: 'empty', event: 'bootstrap-or-storage-or-kenigevents:artifact-collected', to: 'found',
      observable_updates: ['slot.data-artifact-state=found', 'empty.hidden=true', 'open.hidden=false', 'count=1'],
      proof: proof('source-client-transition-exact-candidate', 'src/components/artifacts/ArtifactCollection.astro', 'render'),
    },
    {
      id: 'artifact-transition.amber.found-to-dialog-open', system_id: AMBER_SYSTEM_ID,
      from: 'found', event: 'open-button-or-found-hash-bootstrap', to: 'dialog-open',
      close_contract: ['close-button', 'backdrop-click', 'hash-cleanup', 'focus-restored-to-last-trigger'],
      proof: proof('source-client-transition-exact-candidate', 'src/components/artifacts/ArtifactCollection.astro', 'dialog-handlers'),
    },
  ].map((item) => ({ ...item, decision: DECISION, normalization_allowed: false })));
}

const CAPTURE_CHANNELS = Object.freeze([
  'element-screenshot', 'bounded-dom-summary', 'computed-styles', 'geometry', 'css-variables',
  'accessibility-state', 'focus-state', 'hidden-open-expanded-disabled-state', 'breakpoint-context', 'override-source',
]);

function plan({ id, system, path, context, axes, widths, selectors, interaction = null, note = null }) {
  return {
    id: `specimen-plan.artifacts.${id}`, component_id: componentId(path), logical_path: path,
    system_id: system, context, axes, plan_status: 'controlled-specimen-required',
    required_contexts: widths.map(String), viewport_widths: widths,
    selectors: { root: selectors[0], required: selectors }, interaction,
    capture_requirements: [...CAPTURE_CHANNELS], observation_status: 'not-captured',
    proof_label: 'controlled-specimen-planned-not-observed', evidence_claim_limit: 'as-is-source-derived-no-production-observation',
    note, decision: DECISION, recommendation: 'unresolved', normalization_allowed: false,
  };
}

export function buildArtifactSpecimenPlan() {
  return clone([
    plan({ id: 'focus-artifact-locked-regular', system: FOCUS_SYSTEM_ID, path: FOCUS_COMPONENTS[0], context: 'standalone-artifact', axes: { state: 'locked', density: 'regular' }, widths: [419, 421], selectors: ['[data-focus-egg-artifact]', '[data-focus-egg-state-label]', '[data-focus-egg-find][hidden]', '[data-focus-egg-found-label][hidden]'] }),
    plan({ id: 'focus-artifact-unavailable-compact', system: FOCUS_SYSTEM_ID, path: FOCUS_COMPONENTS[0], context: 'standalone-artifact', axes: { state: 'unavailable', density: 'compact' }, widths: [419, 421], selectors: ['[data-focus-egg-artifact]', '.focus-egg-artifact--compact', '[data-focus-egg-state-label]'], note: 'Compact is source-only and must not be labelled route-observed.' }),
    plan({ id: 'focus-saved-below-threshold', system: FOCUS_SYSTEM_ID, path: FOCUS_COMPONENTS[2], context: 'saved-list-demo', axes: { visible_count: 2, artifact: 'absent' }, widths: [759, 761], selectors: ['[data-focus-egg-demo]', '[data-fg-e12-anchor][hidden]', '[data-fg-e12-equivalent][hidden]', '[data-egg-demo-status]'] }),
    plan({ id: 'focus-saved-eligible-keyboard', system: FOCUS_SYSTEM_ID, path: FOCUS_COMPONENTS[2], context: 'saved-list-demo', axes: { visible_count: 3, state: 'eligible', input: 'keyboard' }, widths: [759, 761], selectors: ['[data-focus-egg-demo]', '[data-fg-e12-anchor]:not([hidden])', '[data-focus-egg-find]'], interaction: ['select-saved-count-3', 'focus-find-button'] }),
    plan({ id: 'focus-saved-eligible-to-found', system: FOCUS_SYSTEM_ID, path: FOCUS_COMPONENTS[2], context: 'saved-list-demo', axes: { visible_count: 3, transition: 'eligible-to-found' }, widths: [419, 421], selectors: ['[data-focus-egg-demo]', '[data-focus-egg-artifact][data-egg-state="found"]', '[data-focus-egg-found-label]:not([hidden])', '.focus-egg-artifact__glyph'], interaction: ['clear-focus-storage', 'select-saved-count-3', 'activate-find-button'], note: 'Capture the stale eligible glyph after the state transition.' }),
    plan({ id: 'focus-catalog-state-set-small', system: FOCUS_SYSTEM_ID, path: FOCUS_COMPONENTS[1], context: 'catalog', axes: { states: [...FOCUS_STATES], storage: 'empty' }, widths: [679, 681], selectors: ['[data-focus-collection]', '.focus-egg-card[data-egg-state]', '[data-collection-found]', '[data-collection-eligible]', '[data-collection-meter]'] }),
    plan({ id: 'focus-catalog-storage-overrides-large', system: FOCUS_SYSTEM_ID, path: FOCUS_COMPONENTS[1], context: 'catalog', axes: { storage: 'FG-E06-and-FG-E12-found', transition: 'bootstrap' }, widths: [919, 921], selectors: ['[data-focus-collection]', '.focus-egg-card[data-egg-id="FG-E06"]', '.focus-egg-card__mark', '[data-collection-meter]'], interaction: ['seed-focus-storage', 'reload'], note: 'Capture unavailable-to-found visual/count disagreement and stale card mark.' }),
    plan({ id: 'amber-production-unavailable', system: AMBER_SYSTEM_ID, path: AMBER_COMPONENTS[0], context: 'artifact-route', axes: { site_mode: 'production', flag: 'tail', surface: 'unavailable-shell' }, widths: [429, 431, 849, 851], selectors: ['[data-artifact-collection-unavailable]'], note: 'Hard gate: ArtifactCollection must be absent even when the flag is tail.' }),
    plan({ id: 'amber-collection-empty', system: AMBER_SYSTEM_ID, path: AMBER_COMPONENTS[0], context: 'non-production-artifact-route', axes: { state: 'empty', stored: false }, widths: [429, 431, 849, 851], selectors: ['[data-artifact-collection]', '[data-artifact-slot="amber_cosmonaut"][data-artifact-state="empty"]', '[data-artifact-found-count]'] }),
    plan({ id: 'amber-collection-found-dialog', system: AMBER_SYSTEM_ID, path: AMBER_COMPONENTS[0], context: 'non-production-artifact-route', axes: { state: 'found', dialog: 'open', input: 'keyboard' }, widths: [429, 431, 849, 851], selectors: ['[data-artifact-collection]', '[data-artifact-slot="amber_cosmonaut"][data-artifact-state="found"]', '[data-artifact-open]', '[data-artifact-dialog][open]', '[data-artifact-close]'], interaction: ['seed-amber-storage', 'focus-open', 'activate-open', 'activate-close', 'assert-focus-restored'] }),
    plan({ id: 'amber-rail-idle-focus', system: AMBER_SYSTEM_ID, path: AMBER_COMPONENTS[1], context: 'weekend-mobile-rail', axes: { state: 'idle', motion: 'regular', input: 'keyboard' }, widths: [719, 721], selectors: ['[data-amber-artifact]', '[data-amber-artifact][aria-pressed="false"]'], interaction: ['focus-artifact'], note: 'At 721 the mobile consumer is hidden; retain that as boundary evidence, not missing component evidence.' }),
    plan({ id: 'amber-rail-awake-collected', system: AMBER_SYSTEM_ID, path: AMBER_COMPONENTS[1], context: 'weekend-mobile-rail', axes: { transitions: ['idle-to-awake', 'awake-to-collecting', 'collecting-to-collected'], motion: 'regular' }, widths: [719], selectors: ['[data-amber-artifact].is-awake', '[data-amber-artifact].is-collecting', '[data-amber-artifact].is-collected', '.amber-artifact__live'], interaction: ['intersect-at-0.72', 'activate-first-time', 'wait-460ms'] }),
    plan({ id: 'amber-rail-reduced-motion', system: AMBER_SYSTEM_ID, path: AMBER_COMPONENTS[1], context: 'weekend-mobile-rail', axes: { state: 'collected', motion: 'reduced' }, widths: [719], selectors: ['[data-amber-artifact].is-awake.is-collected', '[data-amber-artifact][aria-pressed="true"]'], interaction: ['emulate-reduced-motion', 'activate-first-time', 'assert-collecting-removed-without-delay'] }),
  ]);
}

export function buildArtifactMismatchRecords() {
  return clone([
    {
      id: 'mismatch.artifacts.focus-saved-transition-glyph', system_id: FOCUS_SYSTEM_ID,
      component_id: componentId(FOCUS_COMPONENTS[2]), logical_path: FOCUS_COMPONENTS[2],
      channels: ['source-static-render', 'source-client-transition'], conclusion: 'unresolved mapping',
      evidence_status: 'source-derived-potential-mismatch-awaiting-controlled-capture',
      observed_fact: 'eligible-to-found changes dataset, labels and visibility but not the glyph text rendered for eligible',
    },
    {
      id: 'mismatch.artifacts.focus-catalog-mark', system_id: FOCUS_SYSTEM_ID,
      component_id: componentId(FOCUS_COMPONENTS[1]), logical_path: FOCUS_COMPONENTS[1],
      channels: ['source-static-render', 'source-client-storage-override'], conclusion: 'unresolved mapping',
      evidence_status: 'source-derived-potential-mismatch-awaiting-controlled-capture',
      observed_fact: 'storage override changes data-egg-state and state label but not the pre-rendered mark glyph',
    },
    {
      id: 'mismatch.artifacts.focus-unavailable-found-count', system_id: FOCUS_SYSTEM_ID,
      component_id: componentId(FOCUS_COMPONENTS[1]), logical_path: FOCUS_COMPONENTS[1],
      source_paths: [FOCUS_COMPONENTS[1], 'src/pages/fokus-gruppa/kollektsiya/index.astro'],
      channels: ['source-prototype-state', 'source-client-storage-override', 'source-progress-calculation'], conclusion: 'unresolved mapping',
      evidence_status: 'source-derived-potential-mismatch-awaiting-controlled-capture',
      observed_fact: 'FG-E06 can be visually changed from unavailable to found by storage while the denominator and found count still exclude it',
    },
    {
      id: 'mismatch.artifacts.focus-no-storage-listener', system_id: FOCUS_SYSTEM_ID,
      component_id: componentId(FOCUS_COMPONENTS[1]), logical_path: FOCUS_COMPONENTS[1],
      source_paths: [FOCUS_COMPONENTS[1], 'src/pages/fokus-gruppa/kollektsiya/index.astro'],
      channels: ['source-client-event-listeners', 'local-storage-contract'], conclusion: 'page-only state',
      observed_fact: 'catalog listens for focus-egg-found only and does not listen for the browser storage event',
    },
    {
      id: 'mismatch.artifacts.amber-false-transport-family', system_id: AMBER_SYSTEM_ID,
      component_id: componentId(AMBER_COMPONENTS[1]), logical_path: AMBER_COMPONENTS[1],
      channels: ['legacy-family-classifier', 'source-production-surface-contract'], conclusion: 'unresolved mapping',
      observed_fact: 'AmberRailArtifact was falsely associated with family.transport; source contract binds it to artifacts.collection',
      prohibited_family_claim: 'family.transport',
    },
    {
      id: 'mismatch.artifacts.amber-page-evidence-state-gap', system_id: AMBER_SYSTEM_ID,
      component_id: componentId(AMBER_COMPONENTS[0]), logical_path: AMBER_COMPONENTS[0],
      channels: ['prior-page-evidence-summary', 'source-state-model'], conclusion: 'specimen-only state',
      observed_fact: 'prior page inventory summarized only collection empty 0/5; found, dialog and rail transitions still require controlled captures',
      proof_label: 'prior-summary-only-no-capture-attached',
    },
  ].map((item) => ({ ...item, decision: DECISION, recommendation: 'unresolved', normalization_allowed: false })));
}

export function buildArtifactUnresolvedRecords() {
  return clone([
    {
      id: ARTIFACT_PARENT_ID, kind: 'possible-parent-concept', title: 'Collectibles / artifacts parent',
      child_system_ids: [FOCUS_SYSTEM_ID, AMBER_SYSTEM_ID], relationship: 'unresolved-not-a-variant-contract',
      reason: 'The two sources have different IDs, storage schemas, eligibility gates, routes, states, semantics and production reachability.',
      blocks_handoff: false, synthesis_allowed: false, merge_allowed: false,
    },
    {
      id: 'unresolved.artifacts.focus-non-e12-execution', kind: 'source-only-state-model', system_id: FOCUS_SYSTEM_ID,
      reason: 'The catalogue demonstrates 12 definitions and four states, but only FG-E12 has an executable placement and collection transition.',
      blocks_handoff: false, synthesis_allowed: false,
    },
    {
      id: 'unresolved.artifacts.amber-production-state', kind: 'hard-gated-source-system', system_id: AMBER_SYSTEM_ID,
      reason: 'Production always renders the unavailable collection shell and cannot enable the Amber rail; active states require a controlled non-production specimen.',
      blocks_handoff: false, synthesis_allowed: false,
    },
  ].map((item) => ({ ...item, decision: DECISION, recommendation: 'unresolved', normalization_allowed: false })));
}

export function buildArtifactDecoderLane() {
  const lane = {
    schema_version: ARTIFACT_LANE_SCHEMA,
    lane_id: ARTIFACT_LANE_ID,
    pinned_source_sha: SOURCE_SHA,
    systems: buildArtifactSystemRecords(),
    responsive_contexts: clone(ARTIFACT_RESPONSIVE_CONTEXTS),
    state_records: buildArtifactStateRecords(),
    transition_records: buildArtifactTransitionRecords(),
    specimen_plan: buildArtifactSpecimenPlan(),
    specimen_observations: [],
    mismatches: buildArtifactMismatchRecords(),
    unresolved: buildArtifactUnresolvedRecords(),
    constraints: {
      as_is_only: true, browser_capture_claimed: false, private_corpus_run_claimed: false,
      merge: false, split: false, normalization: false, tokenization: false,
      penpot_mutation: false, astro_css_mutation: false,
    },
    decision: DECISION,
  };
  validateArtifactDecoderLane(lane);
  return lane;
}

function assert(condition, message) { if (!condition) throw new Error(`Artifact decoder lane invariant failed: ${message}`); }
function assertUnique(records, label) {
  const ids = records.map((item) => item.id);
  assert(new Set(ids).size === ids.length, `${label} IDs are not unique`);
}

export function validateArtifactDecoderLane(lane) {
  assert(lane?.schema_version === ARTIFACT_LANE_SCHEMA, 'schema version');
  assert(lane?.decision === DECISION, 'lane decision must remain NOT_MERGED');
  assert(lane.systems.length === 2, 'exactly two independent systems');
  assert(new Set(lane.systems.map((item) => item.id)).size === 2, 'system IDs must be distinct');
  assert(lane.systems.every((item) => item.relationship_to_other_artifact_systems === 'independent-not-a-variant'), 'systems must not be variants');
  assert(lane.systems.every((item) => item.decision === DECISION && item.normalization_allowed === false), 'system STOP boundary');
  const focus = lane.systems.find((item) => item.id === FOCUS_SYSTEM_ID);
  const amber = lane.systems.find((item) => item.id === AMBER_SYSTEM_ID);
  assert(focus && amber, 'both named systems required');
  assert(!focus.component_paths.some((path) => amber.component_paths.includes(path)), 'system component bindings overlap');
  const hardGate = amber.gates.find((item) => item.id === 'amber.production-hard-block');
  assert(hardGate && hardGate.truth_table.filter((item) => item.site_mode === 'production').every((item) => item.enabled === false), 'Amber production hard block');
  assert(amber.reachability.production_observed === false, 'Amber must not claim production observation');
  assert(focus.reachability.executable_scope === 'FG-E12-saved-list-demo-only', 'Focus executable scope');
  assertUnique(lane.responsive_contexts, 'responsive context');
  assertUnique(lane.state_records, 'state record');
  assertUnique(lane.transition_records, 'transition record');
  assertUnique(lane.specimen_plan, 'specimen plan');
  assertUnique(lane.mismatches, 'mismatch');
  assertUnique(lane.unresolved, 'unresolved');
  assert(new Set(lane.state_records.filter((item) => item.system_id === FOCUS_SYSTEM_ID && item.axis === 'artifact-state').map((item) => item.value)).size === FOCUS_STATES.length, 'all Focus states');
  assert(FOCUS_STATES.every((state) => lane.state_records.some((item) => item.system_id === FOCUS_SYSTEM_ID && item.value === state)), 'Focus state values');
  assert(AMBER_RAIL_STATES.every((state) => lane.state_records.some((item) => item.system_id === AMBER_SYSTEM_ID && item.axis === 'rail-state' && item.value === state)), 'Amber rail states');
  assert(AMBER_COLLECTION_STATES.every((state) => lane.state_records.some((item) => item.system_id === AMBER_SYSTEM_ID && item.axis === 'collection-state' && item.value === state)), 'Amber collection states');
  assert(lane.specimen_plan.length <= 16, 'specimen plan must stay bounded');
  assert(lane.specimen_plan.every((item) => item.observation_status === 'not-captured' && item.proof_label === 'controlled-specimen-planned-not-observed'), 'specimen proof claims');
  assert(lane.specimen_plan.every((item) => CAPTURE_CHANNELS.every((channel) => item.capture_requirements.includes(channel))), 'component capture channels');
  const widths = new Set(lane.specimen_plan.flatMap((item) => item.viewport_widths));
  for (const context of lane.responsive_contexts) for (const width of context.probe_widths) assert(widths.has(width), `missing boundary probe ${context.id}/${width}`);
  for (const id of [
    'mismatch.artifacts.focus-saved-transition-glyph', 'mismatch.artifacts.focus-catalog-mark',
    'mismatch.artifacts.focus-unavailable-found-count', 'mismatch.artifacts.focus-no-storage-listener',
    'mismatch.artifacts.amber-false-transport-family',
  ]) assert(lane.mismatches.some((item) => item.id === id), `missing known mismatch ${id}`);
  const parent = lane.unresolved.find((item) => item.id === ARTIFACT_PARENT_ID);
  assert(parent?.decision === DECISION && parent.merge_allowed === false && parent.synthesis_allowed === false, 'unresolved parent must not merge or synthesize');
  assert(lane.constraints.as_is_only && !lane.constraints.browser_capture_claimed && !lane.constraints.private_corpus_run_claimed, 'AS-IS/no-capture claim');
  assert(['merge', 'split', 'normalization', 'tokenization', 'penpot_mutation', 'astro_css_mutation'].every((key) => lane.constraints[key] === false), 'normalization STOP');
  return true;
}

export function stableSerializeArtifactLane(lane = buildArtifactDecoderLane()) {
  validateArtifactDecoderLane(lane);
  return `${JSON.stringify(sortObject(lane))}\n`;
}
