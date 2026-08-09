export const SPECIMEN_SCHEMA = 'current_ui_decoder_controlled_specimen_v1';
export const PINNED_CANDIDATE_SHA = 'ef7aa62e45c60f7a12da6160f490719c0721ec03';
export const REQUIRED_CAPSULES = Object.freeze([
  '01-event-presentation-states', '02-button-cta', '03-media-heavy',
  '04-transport', '05-medallions', '06-artifacts',
]);

const common = {
  schema_version: SPECIMEN_SCHEMA,
  route_kind: 'controlled-specimen',
  claim_scope: 'controlled-candidate-source-render-only',
  trace_kind: 'source-to-controlled-specimen',
  state_equivalence: 'not-yet-reconciled-with-real-page',
  production_state_claimed: false,
  environment: { transport_experiment_mode: 'off', reduced_motion: true },
  viewport: { width: 390, height: 900 },
  container: { width: 358, height: 'auto' },
  evidence_parts: ['element-screenshot', 'bounded-dom', 'aria', 'computed-style', 'geometry', 'css-vars', 'pseudo', 'focus', 'open-hidden', 'media'],
};

function specimen(value) {
  return Object.freeze({ ...common, ...value });
}

export const CONTROLLED_SPECIMENS = Object.freeze([
  specimen({
    id: 'button-primary-default', capsule_ids: ['02-button-cta'], renderer: 'button',
    source_paths: ['src/components/design-system/Button.astro'], consumer_paths: [], fixture_ref: null, fixture_delta: {},
    props: { variant: 'primary', size: 'default', state: 'default' }, root_selector: '[data-specimen-root] .ke-button',
    part_selectors: ['.ke-button'], actions: [], expected_markers: ['.ke-button'],
  }),
  specimen({
    id: 'button-secondary-focus', capsule_ids: ['02-button-cta'], renderer: 'button',
    source_paths: ['src/components/design-system/Button.astro'], consumer_paths: [], fixture_ref: null, fixture_delta: {},
    props: { variant: 'secondary', size: 'compact', state: 'focus' }, root_selector: '[data-specimen-root] .ke-button',
    part_selectors: ['.ke-button'], actions: [{ kind: 'focus', selector: '.ke-button' }], expected_markers: ['.ke-button', '[data-preview-state="focus"]'],
  }),
  specimen({
    id: 'button-primary-disabled', capsule_ids: ['02-button-cta'], renderer: 'button',
    source_paths: ['src/components/design-system/Button.astro'], consumer_paths: [], fixture_ref: null, fixture_delta: {},
    props: { variant: 'primary', size: 'large', state: 'disabled' }, root_selector: '[data-specimen-root] .ke-button',
    part_selectors: ['.ke-button'], actions: [], expected_markers: ['.ke-button', '[disabled]'],
  }),
  ...['locked', 'eligible', 'found', 'unavailable'].map((state, index) => specimen({
    id: `focus-egg-${state}`, capsule_ids: ['06-artifacts'], renderer: 'focus-egg',
    source_paths: ['src/components/FocusEggArtifact.astro'], consumer_paths: ['src/components/FocusEggCollectionCard.astro'],
    fixture_ref: null, fixture_delta: {}, props: { eggId: `FG-E0${index + 1}`, title: `Evidence ${state}`, state, compact: state === 'found' },
    root_selector: '[data-focus-egg-artifact]', part_selectors: ['[data-focus-egg-state-label]', '[data-focus-egg-find]', '[data-focus-egg-found-label]'],
    actions: state === 'eligible' ? [{ kind: 'focus', selector: '[data-focus-egg-find]' }] : [],
    expected_markers: ['[data-focus-egg-artifact]', `[data-egg-state="${state}"]`],
  })),
  specimen({
    id: 'amber-before-and-after-collect', capsule_ids: ['06-artifacts'], renderer: 'amber',
    source_paths: ['src/components/listings/AmberRailArtifact.astro', 'src/lib/artifacts.mjs'],
    consumer_paths: ['src/components/listings/MobileListingRailRow.astro'], fixture_ref: null, fixture_delta: {},
    props: { eventId: 7048, placement: 'tail' }, root_selector: '[data-amber-artifact]',
    part_selectors: ['.amber-artifact__visual', '.amber-artifact__found'], actions: [{ kind: 'click', selector: '[data-amber-artifact]' }],
    expected_markers: ['[data-amber-artifact]', '[aria-pressed]'], capture_steps: ['before-action', 'after-action'],
  }),
  specimen({
    id: 'artifact-collection-empty', capsule_ids: ['06-artifacts'], renderer: 'artifact-collection',
    source_paths: ['src/components/artifacts/ArtifactCollection.astro', 'src/lib/artifacts.mjs'], consumer_paths: ['src/pages/artefakty.astro'],
    fixture_ref: null, fixture_delta: {}, props: {}, root_selector: '[data-artifact-collection]',
    part_selectors: ['[data-artifact-slot="amber_cosmonaut"]', '[data-artifact-dialog]'], actions: [],
    expected_markers: ['[data-artifact-collection]', '[data-artifact-state="empty"]'],
  }),
  specimen({
    id: 'artifact-collection-found-dialog', capsule_ids: ['06-artifacts'], renderer: 'artifact-collection',
    source_paths: ['src/components/artifacts/ArtifactCollection.astro', 'src/lib/artifacts.mjs'], consumer_paths: ['src/pages/artefakty.astro'],
    fixture_ref: null, fixture_delta: {}, props: {}, root_selector: '[data-artifact-collection]',
    part_selectors: ['[data-artifact-slot="amber_cosmonaut"]', '[data-artifact-dialog]'],
    before_navigation: [{ kind: 'seed-amber-found', event_id: 7048 }], actions: [{ kind: 'click', selector: '[data-artifact-open]' }],
    expected_markers: ['[data-artifact-collection]', '[data-artifact-state="found"]', '[data-artifact-dialog][open]'], capture_steps: ['after-action'],
  }),
  specimen({
    id: 'rail-explicit-real-event', capsule_ids: ['04-transport'], renderer: 'rail',
    source_paths: ['src/components/EventTransportSchedule.astro', 'src/lib/eventTransport.ts'], consumer_paths: ['src/components/DesktopEventPage.astro'],
    fixture_ref: { catalog: 'preview-events', event_id: 6939 }, fixture_delta: {}, props: {}, root_selector: '[data-event-transport-schedule]',
    part_selectors: ['[data-transport-direction="outbound"]', '[data-transport-direction="return"]'], actions: [],
    expected_markers: ['[data-event-transport-schedule]', '[data-event-end-basis="explicit"]'], viewport: { width: 720, height: 1000 }, container: { width: 700, height: 'auto' },
  }),
  specimen({
    id: 'rail-cutoff-real-event', capsule_ids: ['04-transport'], renderer: 'rail',
    source_paths: ['src/components/EventTransportSchedule.astro', 'src/lib/eventTransport.ts'], consumer_paths: ['src/components/DesktopEventPage.astro'],
    fixture_ref: { catalog: 'preview-events', event_id: 6976 }, fixture_delta: {}, props: {}, root_selector: '[data-event-transport-schedule]',
    part_selectors: ['[data-return-schedule-cutoff]'], actions: [],
    expected_markers: ['[data-event-transport-schedule]', '[data-event-end-basis="schedule_cutoff"]'], viewport: { width: 540, height: 1000 }, container: { width: 540, height: 'auto' },
  }),
  specimen({
    id: 'rail-forecast-controlled-delta', capsule_ids: ['04-transport'], renderer: 'rail',
    source_paths: ['src/components/EventTransportSchedule.astro', 'src/lib/eventTransport.ts'], consumer_paths: ['src/components/DesktopEventPage.astro'],
    fixture_ref: { catalog: 'preview-events', event_id: 6939 }, fixture_delta: { transport_end_basis: 'forecast' },
    props: {}, root_selector: '[data-event-transport-schedule]', part_selectors: ['.event-transport__forecast-note'], actions: [],
    expected_markers: ['[data-event-transport-schedule]', '[data-event-end-basis="forecast"]'],
    state_equivalence: 'controlled-delta-source-model-only-until-real-page-reconciliation',
  }),
  specimen({
    id: 'kaup-baseline-real-event', capsule_ids: ['04-transport'], renderer: 'kaup',
    source_paths: ['src/components/KaupTransportSchedule.astro', 'src/lib/eventKaupTransport.ts'], consumer_paths: ['src/components/DesktopEventPage.astro'],
    fixture_ref: { catalog: 'preview-events', event_id: 5374 }, fixture_delta: {}, props: { compact: false }, root_selector: '[data-kaup-transport]',
    part_selectors: ['[data-kaup-official-transfer]', '[data-kaup-public-bus]', '[data-kaup-car-route]'],
    actions: [{ kind: 'toggle-open', selector: '[data-kaup-official-transfer] details' }], expected_markers: ['[data-kaup-transport]'],
    viewport: { width: 700, height: 1100 }, container: { width: 560, height: 'auto' },
  }),
  specimen({
    id: 'kaup-compact-real-event', capsule_ids: ['04-transport'], renderer: 'kaup',
    source_paths: ['src/components/KaupTransportSchedule.astro', 'src/lib/eventKaupTransport.ts'], consumer_paths: ['src/components/DesktopEventPage.astro'],
    fixture_ref: { catalog: 'preview-events', event_id: 5374 }, fixture_delta: {}, props: { compact: true }, root_selector: '[data-kaup-transport]',
    part_selectors: ['[data-kaup-public-bus]'], actions: [], expected_markers: ['[data-kaup-transport]', '[data-kaup-compact]'],
    viewport: { width: 420, height: 1000 }, container: { width: 360, height: 'auto' },
  }),
  ...[
    ['medallions-inline-zero-real-event', 2601, 'inline', true],
    ['medallions-inline-one-real-event', 5336, 'inline', true],
    ['medallions-desktop-slots-top', 6856, 'desktop-slots', true],
    ['medallions-desktop-slots-no-top', 6994, 'desktop-slots', false],
  ].map(([id, eventId, layout, allowTopSlot]) => specimen({
    id, capsule_ids: ['05-medallions'], renderer: 'medallions',
    source_paths: ['src/components/EventTokenMedallions.astro', 'src/lib/eventMedallions.ts'], consumer_paths: ['src/components/DesktopEventPage.astro'],
    fixture_ref: { catalog: 'preview-events', event_id: eventId }, fixture_delta: {}, props: { layout, allowTopSlot },
    root_selector: eventId === 2601 ? '[data-specimen-root]' : '[data-medallion-layout]', part_selectors: ['[data-medallion-layout]', '[data-medallion-slot]', '.event-token'], actions: [],
    expected_markers: eventId === 2601 ? ['[data-specimen-root]'] : ['[data-medallion-layout]'],
    expected_absent_markers: eventId === 2601 ? ['[data-medallion-layout]'] : [], component_presence: eventId === 2601 ? 'expected-absent-zero-token-state' : 'expected-present',
    viewport: { width: layout === 'desktop-slots' ? 1728 : 720, height: 1000 },
    container: { width: layout === 'desktop-slots' ? 720 : 540, height: 'auto' },
  })),
]);

export const SOURCE_MODEL_ONLY_CASES = Object.freeze([
  {
    id: 'bus-no-outbound-groups', family: 'transport.bus', state: 'no-groups',
    reachability: 'source-model-only', production_state_claimed: false,
    reason: 'Pinned exact candidate data has no valid event fixture that reaches the no-groups branch; synthetic schedule data is forbidden.',
    source_paths: ['src/components/EventBusTransportSchedule.astro', 'src/lib/eventBusTransport.ts'],
  },
]);

function realRoute(value) {
  return Object.freeze({
    schema_version: SPECIMEN_SCHEMA, route_kind: 'exact-real-route-verification',
    claim_scope: 'exact-candidate-route-capture-pending', trace_kind: 'source-to-real-page',
    production_state_claimed: false, state_equivalence: 'route-binding-selected-capture-not-yet-reviewed',
    contexts: [
      { name: 'mobile', viewport: { width: 390, height: 844 } },
      { name: 'desktop', viewport: { width: 1728, height: 1000 } },
    ], expected_absent_selectors: [], ...value,
  });
}

export const REAL_ROUTE_VERIFICATIONS = Object.freeze([
  ...[
    ['event-editorial-7052', 7052, ['01-event-presentation-states', '03-media-heavy']],
    ['event-split-7301', 7301, ['01-event-presentation-states', '03-media-heavy']],
    ['event-poster-previews-7048', 7048, ['03-media-heavy', '06-artifacts']],
    ['event-media-alternate-7186', 7186, ['03-media-heavy']],
    ['event-no-image-6996', 6996, ['03-media-heavy']],
  ].map(([id, eventId, capsuleIds]) => realRoute({
    id, capsule_ids: capsuleIds, event_id: eventId, route_template: '/sobytiya/{slug}/',
    selectors: ['[data-desktop-clean-event]', '[data-media-frame]', '.mobile-event-production'],
    source_paths: ['src/pages/sobytiya/[slug].astro', 'src/components/DesktopEventPage.astro'],
  })),
  ...[6939, 6976, 6710, 6365, 5374].map((eventId) => realRoute({
    id: `transport-${eventId}`, capsule_ids: ['04-transport'], event_id: eventId, route_template: '/sobytiya/{slug}/',
    selectors: ['[data-event-transport-schedule],[data-event-bus-schedule],[data-kaup-transport]', '.mobile-event-production'],
    source_paths: ['src/pages/sobytiya/[slug].astro', 'src/components/EventTransportSchedule.astro', 'src/components/EventBusTransportSchedule.astro', 'src/components/KaupTransportSchedule.astro'],
  })),
  ...[2601, 5336, 6856, 6994, 6591, 698, 7040, 6562, 6990, 5829, 5278].map((eventId) => realRoute({
    id: `medallions-${eventId}`, capsule_ids: ['05-medallions'], event_id: eventId, route_template: '/sobytiya/{slug}/',
    selectors: eventId === 2601 ? ['[data-medallion-layout]'] : ['[data-medallion-layout]', '.mobile-event-production'],
    expected_absent_selectors: eventId === 2601 ? ['[data-medallion-layout]'] : [],
    source_paths: ['src/pages/sobytiya/[slug].astro', 'src/components/EventTokenMedallions.astro'],
  })),
  realRoute({
    id: 'artifact-catalog', capsule_ids: ['06-artifacts'], event_id: null, route_template: '/artefakty/',
    selectors: ['[data-artifact-collection-unavailable]'], expected_absent_selectors: ['[data-artifact-collection]'],
    source_paths: ['src/pages/artefakty/index.astro', 'src/components/artifacts/ArtifactCollection.astro'],
  }),
  realRoute({
    id: 'artifact-weekend', capsule_ids: ['06-artifacts'], event_id: null, route_template: '/vyhodnye/',
    selectors: ['[data-date-listing="weekend"]'], expected_absent_selectors: ['[data-amber-artifact]'],
    source_paths: ['src/pages/vyhodnye/index.astro', 'src/components/listings/WeekendListingSurface.astro', 'src/components/listings/AmberRailArtifact.astro'],
  }),
  ...[
    ['free', 'cta-free-calendar-invariant'], ['phone', 'cta-phone-invariant'], ['registration', 'cta-registration-invariant'],
  ].map(([variant, scenario]) => realRoute({
    id: `cta-lab-${variant}`, capsule_ids: ['02-button-cta'], event_id: null,
    route_template: `/lab/event-desktop/examples/${scenario}/`, selectors: ['[data-desktop-action-panel]'],
    contexts: [{ name: 'desktop', viewport: { width: 1728, height: 1000 } }],
    source_paths: ['src/pages/lab/event-desktop/examples/[scenario].astro', 'src/components/DesktopEventActionPanel.astro'],
  })),
]);

export function buildSpecimenRegistry() {
  return {
    schema_version: SPECIMEN_SCHEMA,
    pinned_candidate_sha: PINNED_CANDIDATE_SHA,
    policy: { normalization_allowed: false, production_claim_requires_real_page_review: true, cartesian_product_forbidden: true },
    controlled_specimens: CONTROLLED_SPECIMENS.map((row) => ({ ...row })),
    real_route_verifications: REAL_ROUTE_VERIFICATIONS.map((row) => ({ ...row })),
    source_model_only_cases: SOURCE_MODEL_ONLY_CASES.map((row) => ({ ...row })),
  };
}
