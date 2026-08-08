import { createHash } from 'node:crypto';

export const TRANSPORT_SCHEMA = 'current_ui_component_decoder_v1';
export const TRANSPORT_MODEL_VERSION = 'transport_state_model_v1';

export const TRANSPORT_SOURCE_PATHS = Object.freeze({
  rail: 'src/components/EventTransportSchedule.astro',
  bus: 'src/components/EventBusTransportSchedule.astro',
  kaup: 'src/components/KaupTransportSchedule.astro',
  experiment: 'src/components/transport/TransportTimetableExperiment.astro',
  departure_board: 'src/components/transport/DepartureBoardTimetable.astro',
  route_strips: 'src/components/transport/RouteStripsTimetable.astro',
  next_departure_queue: 'src/components/transport/NextDepartureQueueTimetable.astro',
  journey_alerts: 'src/components/transport/TransportJourneyAlerts.astro',
  route_heading: 'src/components/transport/TransportRouteHeading.astro',
});

export const TRANSPORT_EXACT_MARKER_ALLOWLIST = Object.freeze({
  rail: Object.freeze([
    'data-event-transport-schedule', 'data-event-city', 'data-outbound-count',
    'data-return-count', 'data-event-end-basis', 'data-transport-direction',
    'data-train-number', 'data-return-schedule-cutoff', 'data-last-same-day-return',
    'data-return-access-minutes',
  ]),
  bus: Object.freeze([
    'data-event-bus-schedule', 'data-bus-route', 'data-bus-outbound',
    'data-bus-number', 'data-terminal-departure', 'data-boarding-stop',
    'data-bus-return', 'data-bus-return-number',
  ]),
  kaup: Object.freeze([
    'data-kaup-transport', 'data-kaup-compact', 'data-kaup-official-transfer',
    'data-kaup-public-bus', 'data-kaup-bus-origin', 'data-kaup-car-route',
    'data-transport-action', 'data-transport-treatment', 'data-transport-baseline',
    'data-transport-trip-id', 'data-departure-at', 'data-transport-disclosure',
    'data-transport-experiment', 'data-experiment-key', 'data-experiment-version',
    'data-experiment-algorithm', 'data-experiment-config-hash', 'data-experiment-mode',
    'data-event-id', 'data-release-id', 'data-transport-snapshot-hash',
    'data-transport-treatment-set', 'data-transport-journey-alerts',
    'data-kaup-last-mile', 'data-transport-next-slot', 'data-transport-next-queue',
    'data-transport-c-trip', 'data-transport-past', 'data-terminal-departure',
  ]),
});

// These attributes exist in source, but may contain deployment endpoints or a key.
// They are intentionally excluded from every decoded evidence record.
export const TRANSPORT_FORBIDDEN_EVIDENCE_MARKERS = Object.freeze([
  'data-supabase-url', 'data-supabase-relay-url', 'data-supabase-key',
]);

export const TRANSPORT_BREAKPOINT_CONTEXTS = Object.freeze([
  Object.freeze({ family: 'rail', kind: 'container', name: 'event-rail', below: 539, at: 540 }),
  Object.freeze({ family: 'bus', kind: 'container', name: 'event-bus-primary', below: 539, at: 540 }),
  Object.freeze({ family: 'bus', kind: 'container', name: 'event-bus-map', below: 699, at: 700 }),
  Object.freeze({ family: 'bus', kind: 'viewport-media', name: 'event-bus-map-picture', below: 720, at: 721 }),
  Object.freeze({ family: 'kaup', kind: 'container', name: 'kaup-narrow', below: 360, at: 361 }),
  Object.freeze({ family: 'kaup', kind: 'container', name: 'kaup-timetable', below: 390, at: 391 }),
  Object.freeze({ family: 'kaup', kind: 'container', name: 'kaup-shell', below: 560, at: 561 }),
  Object.freeze({ family: 'consumer', kind: 'viewport', name: 'desktop-event-switch', below: 1023, at: 1024 }),
]);

export const TRANSPORT_REAL_ROUTE_REPRESENTATIVES = Object.freeze([
  Object.freeze({ family: 'rail', route_id: 6939, state: 'explicit', proof_label: 'exact-candidate-real-route-representative' }),
  Object.freeze({ family: 'rail', route_id: 6976, state: 'schedule_cutoff', proof_label: 'exact-candidate-real-route-representative' }),
  Object.freeze({ family: 'bus', route_id: 6710, state: 'groups-present', proof_label: 'exact-candidate-real-route-representative' }),
  Object.freeze({ family: 'bus', route_id: 6365, state: 'groups-present', proof_label: 'exact-candidate-real-route-representative' }),
  Object.freeze({ family: 'kaup', route_id: 5374, state: 'baseline-off', proof_label: 'exact-candidate-real-route-representative' }),
  Object.freeze({ family: 'kaup', route_id: null, state: 'lab-companion-arrival', proof_label: 'lab-route-representative-not-production' }),
]);

export const TRANSPORT_PINNED_OBSERVATIONS = Object.freeze({
  source_sha: 'ef7aa62e45c60f7a12da6160f490719c0721ec03',
  corpus_event_count: 288,
  rail: Object.freeze({ suggestions: 26, end_basis: Object.freeze({ explicit: 2, forecast: 0, schedule_cutoff: 24 }), outbound_counts: Object.freeze({ one: 4, two: 22 }), return_counts: Object.freeze({ zero: 24, two: 2 }) }),
  bus: Object.freeze({ suggestions: 2, outbound_groups_present: 2, return_groups_present: 2 }),
  kaup: Object.freeze({ suggestions: 1, outbound_present: 1, departure_estimated: true, tight: false, public_return_available: false, experiment_mode: 'off' }),
  proof_label: 'pinned-exact-candidate-structural-scan',
});

const RAIL_END_BASIS = Object.freeze(['explicit', 'forecast', 'schedule_cutoff']);
const EXPERIMENT_MODES = Object.freeze(['off', 'qa', 'focus_group', 'live']);
const TREATMENTS = Object.freeze(['departure_board_v1', 'route_strips_v1', 'next_departure_queue_v1']);
const ALLOWED_REACHABILITY = Object.freeze(['production-reachable-not-observed', 'controlled-specimen-only', 'experiment-off', 'source-only']);

function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map((key) => [key, stable(value[key])]));
  return value;
}

function sourcePathComponentId(path) {
  return `component.${createHash('sha256').update(path).digest('hex').slice(0, 16)}`;
}

function freezeRecord(family, key, axes, extra = {}) {
  return Object.freeze({
    schema_version: TRANSPORT_SCHEMA,
    record_kind: TRANSPORT_MODEL_VERSION,
    id: `transport-state.${family}.${key}`,
    family: `transport.${family}`,
    axes: Object.freeze({ ...axes }),
    decision: 'NOT_MERGED',
    normalization_allowed: false,
    ...extra,
  });
}

function kaupTreatmentSource(treatment) {
  return ({
    departure_board_v1: TRANSPORT_SOURCE_PATHS.departure_board,
    route_strips_v1: TRANSPORT_SOURCE_PATHS.route_strips,
    next_departure_queue_v1: TRANSPORT_SOURCE_PATHS.next_departure_queue,
  })[treatment];
}

export function validateRailState(axes) {
  if (!axes || !RAIL_END_BASIS.includes(axes.event_end_basis)) throw new Error('Rail event_end_basis must be explicit, forecast or schedule_cutoff');
  for (const name of ['outbound_present', 'return_present', 'event_end_present', 'estimated_end', 'next_day_return', 'warning']) {
    if (typeof axes[name] !== 'boolean') throw new TypeError(`Rail ${name} must be boolean`);
  }
  if (axes.event_end_basis === 'schedule_cutoff' && (axes.event_end_present || axes.return_present || axes.next_day_return || axes.estimated_end)) {
    throw new Error('schedule_cutoff cannot claim event end, proposed returns, next-day return or estimated end');
  }
  if (axes.event_end_basis === 'forecast' && (!axes.event_end_present || !axes.estimated_end || axes.next_day_return)) {
    throw new Error('forecast requires an estimated event end and forbids a next-day proposal');
  }
  if (axes.event_end_basis === 'explicit' && (!axes.event_end_present || axes.estimated_end)) {
    throw new Error('explicit requires a non-estimated event end');
  }
  if (axes.next_day_return && !axes.return_present) throw new Error('A next-day return must be present');
  if (axes.warning && (axes.return_present || axes.event_end_basis === 'schedule_cutoff')) throw new Error('Rail empty-return warning is distinct from schedule_cutoff and a populated return');
  return true;
}

export function validateBusState(axes) {
  if (!axes) throw new TypeError('Bus axes are required');
  for (const name of ['outbound_present', 'return_present', 'boarding_estimated']) {
    if (typeof axes[name] !== 'boolean') throw new TypeError(`Bus ${name} must be boolean`);
  }
  return true;
}

export function validateKaupState(axes) {
  if (!axes || !EXPERIMENT_MODES.includes(axes.experiment_mode)) throw new Error('Unknown Kaup experiment mode');
  if (!TREATMENTS.includes(axes.treatment)) throw new Error('Unknown Kaup timetable treatment');
  for (const name of ['compact', 'outbound_present', 'departure_estimated', 'tight', 'public_return_available', 'transfer_details_open', 'experiment_host_present', 'initial_hidden']) {
    if (typeof axes[name] !== 'boolean') throw new TypeError(`Kaup ${name} must be boolean`);
  }
  if (!axes.outbound_present && (axes.departure_estimated || axes.tight)) throw new Error('An absent Kaup departure cannot be estimated or tight');
  if (axes.experiment_mode === 'off' && axes.treatment !== 'departure_board_v1') throw new Error('Production-off mode renders only the departure-board baseline');
  if (axes.experiment_mode === 'off' && (axes.experiment_host_present || axes.initial_hidden)) throw new Error('Production-off mode has no experiment host and renders its baseline visibly');
  if (axes.experiment_mode !== 'off' && !axes.experiment_host_present) throw new Error('Enabled source modes require the experiment host');
  if (axes.experiment_mode !== 'off' && axes.treatment === 'departure_board_v1' && axes.initial_hidden) throw new Error('The QA departure-board baseline starts visible');
  if (axes.experiment_mode !== 'off' && axes.treatment !== 'departure_board_v1' && !axes.initial_hidden) throw new Error('Alternative treatments start in the hidden treatment set');
  return true;
}

export function buildTransportStateRecords() {
  const railAxes = [
    ['explicit-populated', { event_end_basis: 'explicit', outbound_present: true, return_present: true, event_end_present: true, estimated_end: false, next_day_return: false, warning: false }],
    ['explicit-next-day', { event_end_basis: 'explicit', outbound_present: true, return_present: true, event_end_present: true, estimated_end: false, next_day_return: true, warning: false }],
    ['explicit-no-outbound', { event_end_basis: 'explicit', outbound_present: false, return_present: true, event_end_present: true, estimated_end: false, next_day_return: false, warning: false }],
    ['explicit-return-warning', { event_end_basis: 'explicit', outbound_present: true, return_present: false, event_end_present: true, estimated_end: false, next_day_return: false, warning: true }],
    ['forecast-populated', { event_end_basis: 'forecast', outbound_present: true, return_present: true, event_end_present: true, estimated_end: true, next_day_return: false, warning: false }],
    ['forecast-return-warning', { event_end_basis: 'forecast', outbound_present: true, return_present: false, event_end_present: true, estimated_end: true, next_day_return: false, warning: true }],
    ['cutoff-outbound', { event_end_basis: 'schedule_cutoff', outbound_present: true, return_present: false, event_end_present: false, estimated_end: false, next_day_return: false, warning: false }],
    ['cutoff-no-outbound', { event_end_basis: 'schedule_cutoff', outbound_present: false, return_present: false, event_end_present: false, estimated_end: false, next_day_return: false, warning: false }],
  ];
  const rail = railAxes.map(([key, axes]) => {
    validateRailState(axes);
    return freezeRecord('rail', key, axes, {
      source_path: TRANSPORT_SOURCE_PATHS.rail,
      reachability: ['explicit-populated', 'cutoff-outbound'].includes(key) ? 'production-reachable-not-observed' : 'controlled-specimen-only',
      proof_label: ['explicit-populated', 'cutoff-outbound'].includes(key) ? 'source-plus-exact-candidate-route-representative' : 'controlled-specimen-plan-not-observed',
    });
  });

  const busAxes = [
    ['groups-present-exact', { outbound_present: true, return_present: true, boarding_estimated: false }],
    ['groups-present-estimated', { outbound_present: true, return_present: true, boarding_estimated: true }],
    ['no-outbound', { outbound_present: false, return_present: true, boarding_estimated: false }],
    ['no-return', { outbound_present: true, return_present: false, boarding_estimated: true }],
  ];
  const bus = busAxes.map(([key, axes]) => {
    validateBusState(axes);
    return freezeRecord('bus', key, axes, {
      source_path: TRANSPORT_SOURCE_PATHS.bus,
      reachability: key.startsWith('groups-present') ? 'production-reachable-not-observed' : 'controlled-specimen-only',
      proof_label: key.startsWith('groups-present') ? 'source-plus-exact-candidate-route-representative' : 'controlled-specimen-plan-not-observed',
    });
  });

  const kaupAxes = [
    ['regular-baseline-off', { compact: false, outbound_present: true, departure_estimated: true, tight: false, public_return_available: false, transfer_details_open: false, experiment_host_present: false, initial_hidden: false, experiment_mode: 'off', treatment: 'departure_board_v1' }],
    ['compact-baseline-off', { compact: true, outbound_present: true, departure_estimated: true, tight: false, public_return_available: false, transfer_details_open: false, experiment_host_present: false, initial_hidden: false, experiment_mode: 'off', treatment: 'departure_board_v1' }],
    ['no-trip', { compact: false, outbound_present: false, departure_estimated: false, tight: false, public_return_available: false, transfer_details_open: false, experiment_host_present: false, initial_hidden: false, experiment_mode: 'off', treatment: 'departure_board_v1' }],
    ['tight-return-present', { compact: false, outbound_present: true, departure_estimated: true, tight: true, public_return_available: true, transfer_details_open: false, experiment_host_present: false, initial_hidden: false, experiment_mode: 'off', treatment: 'departure_board_v1' }],
    ['transfer-details-open', { compact: false, outbound_present: true, departure_estimated: false, tight: false, public_return_available: true, transfer_details_open: true, experiment_host_present: false, initial_hidden: false, experiment_mode: 'off', treatment: 'departure_board_v1' }],
    ['qa-departure-board', { compact: false, outbound_present: true, departure_estimated: true, tight: false, public_return_available: false, transfer_details_open: false, experiment_host_present: true, initial_hidden: false, experiment_mode: 'qa', treatment: 'departure_board_v1' }],
    ['qa-route-strips', { compact: false, outbound_present: true, departure_estimated: true, tight: true, public_return_available: false, transfer_details_open: false, experiment_host_present: true, initial_hidden: true, experiment_mode: 'qa', treatment: 'route_strips_v1' }],
    ['qa-next-queue', { compact: false, outbound_present: true, departure_estimated: false, tight: false, public_return_available: true, transfer_details_open: false, experiment_host_present: true, initial_hidden: true, experiment_mode: 'qa', treatment: 'next_departure_queue_v1' }],
  ];
  const kaup = kaupAxes.map(([key, axes]) => {
    validateKaupState(axes);
    const qa = axes.experiment_mode === 'qa';
    return freezeRecord('kaup', key, axes, {
      source_path: TRANSPORT_SOURCE_PATHS.kaup,
      implementation_source_path: kaupTreatmentSource(axes.treatment),
      consumer_source_paths: [TRANSPORT_SOURCE_PATHS.kaup, TRANSPORT_SOURCE_PATHS.experiment],
      reachability: qa ? 'controlled-specimen-only' : key.endsWith('baseline-off') ? 'production-reachable-not-observed' : 'controlled-specimen-only',
      proof_label: qa ? 'controlled-candidate-qa-never-production-observed' : key.endsWith('baseline-off') ? 'source-plus-exact-candidate-route-representative' : 'controlled-specimen-plan-not-observed',
      implementation_reachability: qa ? 'experiment-off' : 'production-source',
    });
  });

  const sourceOnly = ['focus_group', 'live'].flatMap((mode) => TREATMENTS.map((treatment) => freezeRecord('kaup', `${mode}-${treatment}`, {
    compact: false, outbound_present: true, departure_estimated: false, tight: false,
    public_return_available: false, transfer_details_open: false, experiment_host_present: true,
    initial_hidden: treatment !== 'departure_board_v1', experiment_mode: mode, treatment,
  }, {
    source_path: TRANSPORT_SOURCE_PATHS.kaup,
    implementation_source_path: kaupTreatmentSource(treatment),
    consumer_source_paths: [TRANSPORT_SOURCE_PATHS.kaup, TRANSPORT_SOURCE_PATHS.experiment],
    reachability: 'source-only',
    proof_label: 'experiment-mode-enum-source-only-not-specimen-not-production',
    implementation_reachability: 'experiment-off',
  })));
  const records = [...rail, ...bus, ...kaup, ...sourceOnly];
  assertTransportStateRecordInvariants(records);
  return records;
}

function breakpointContexts(family) {
  return TRANSPORT_BREAKPOINT_CONTEXTS.filter((item) => item.family === family || item.family === 'consumer')
    .flatMap((item) => [item.below, item.at]);
}

function rootMarker(family) {
  return ({ rail: 'data-event-transport-schedule', bus: 'data-event-bus-schedule', kaup: 'data-kaup-transport' })[family];
}

export function buildTransportSpecimenPlan() {
  const records = buildTransportStateRecords().filter((record) => record.reachability !== 'source-only');
  return records.map((record) => ({
    schema_version: TRANSPORT_SCHEMA,
    record_kind: 'transport_specimen_plan_v1',
    id: `specimen-plan.${record.id}`,
    component_id: sourcePathComponentId(record.source_path),
    implementation_component_id: record.implementation_source_path ? sourcePathComponentId(record.implementation_source_path) : null,
    logical_path: record.source_path,
    implementation_logical_path: record.implementation_source_path || null,
    consumer_source_paths: record.consumer_source_paths || [record.source_path],
    specimen_family: record.family,
    state_record_id: record.id,
    state_axes: record.axes,
    plan_status: 'controlled-specimen-required',
    representative_strategy: 'bounded-reviewed-representative-with-pairwise-axis-coverage',
    root_selector: `[${rootMarker(record.family.slice('transport.'.length))}]`,
    required_markers: [rootMarker(record.family.slice('transport.'.length))],
    required_contexts: breakpointContexts(record.family.slice('transport.'.length)),
    capture_requirements: ['element-screenshot', 'sanitized-dom-summary', 'computed-style', 'geometry', 'css-variables', 'accessibility', 'focus', 'expanded-hidden-state', 'container-and-viewport-context', 'override-source'],
    evidence_claim_limit: record.proof_label,
    reachability: record.reachability,
    decision: 'NOT_MERGED',
    normalization_allowed: false,
  })).sort((a, b) => a.id.localeCompare(b.id));
}

export function transportCaptureRequirements() {
  return Object.freeze({
    schema_version: TRANSPORT_SCHEMA,
    record_kind: 'transport_capture_requirements_v1',
    selectors: Object.freeze({
      rail: '[data-event-transport-schedule]',
      bus: '[data-event-bus-schedule]',
      kaup: '[data-kaup-transport]',
      treatments: '[data-transport-treatment]',
    }),
    marker_allowlist: TRANSPORT_EXACT_MARKER_ALLOWLIST,
    forbidden_markers: TRANSPORT_FORBIDDEN_EVIDENCE_MARKERS,
    breakpoint_contexts: TRANSPORT_BREAKPOINT_CONTEXTS,
    evidence_fields: Object.freeze(['element_screenshot', 'dom_summary', 'computed_styles', 'geometry', 'css_variables', 'override_source', 'accessibility', 'focus', 'expanded', 'hidden', 'viewport', 'container']),
    max_elements_per_specimen: 4,
    full_html_retained: false,
    endpoint_or_key_attributes_retained: false,
    decision: 'NOT_MERGED',
    normalization_allowed: false,
  });
}

export function assertTransportStateRecordInvariants(records) {
  const ids = new Set();
  for (const record of records) {
    if (ids.has(record.id)) throw new Error(`Duplicate transport state id: ${record.id}`);
    ids.add(record.id);
    if (record.family === 'family.transport' || record.family === 'transport') throw new Error('Broad legacy family.transport is forbidden');
    if (!['transport.rail', 'transport.bus', 'transport.kaup'].includes(record.family)) throw new Error(`Unknown transport family: ${record.family}`);
    if (!ALLOWED_REACHABILITY.includes(record.reachability)) throw new Error(`Unknown transport reachability: ${record.reachability}`);
    if (record.decision !== 'NOT_MERGED' || record.normalization_allowed !== false) throw new Error('Transport decisions must remain NOT_MERGED with normalization disabled');
    if (record.family === 'transport.rail') validateRailState(record.axes);
    if (record.family === 'transport.bus') validateBusState(record.axes);
    if (record.family === 'transport.kaup') validateKaupState(record.axes);
    if (record.axes.experiment_mode === 'qa' && (record.reachability !== 'controlled-specimen-only' || record.proof_label !== 'controlled-candidate-qa-never-production-observed')) {
      throw new Error('QA transport treatments may only be controlled specimens and never production observations');
    }
    if (['focus_group', 'live'].includes(record.axes.experiment_mode) && record.reachability !== 'source-only') {
      throw new Error('Unobserved experiment modes must remain source-only');
    }
  }
  return true;
}

export function assertTransportEvidenceRecord(record) {
  if (!record || record.family === 'family.transport' || record.family === 'transport') throw new Error('Broad legacy family.transport evidence is forbidden');
  const family = String(record.family || '').replace(/^transport\./u, '');
  if (!Object.hasOwn(TRANSPORT_EXACT_MARKER_ALLOWLIST, family)) throw new Error(`Unknown transport evidence family: ${record.family}`);
  for (const marker of record.markers || []) {
    if (TRANSPORT_FORBIDDEN_EVIDENCE_MARKERS.includes(marker)) throw new Error(`Sensitive transport marker is forbidden: ${marker}`);
    if (!TRANSPORT_EXACT_MARKER_ALLOWLIST[family].includes(marker)) throw new Error(`Unreviewed transport marker: ${marker}`);
  }
  const serialized = JSON.stringify(record);
  if (/(?:https?:\/\/|authorization|bearer|token|secret|password|supabase-(?:url|relay-url|key))/iu.test(serialized)) throw new Error('Unsafe transport evidence value');
  if ('html' in record || 'outerHTML' in record || 'innerHTML' in record) throw new Error('Full HTML is forbidden in transport evidence');
  if (record.proof_label?.includes('qa') && record.reachability !== 'controlled-specimen-only') throw new Error('QA evidence cannot be production-observed');
  if (record.decision !== 'NOT_MERGED' || record.normalization_allowed !== false) throw new Error('Transport evidence cannot make a merge or normalization decision');
  return true;
}

export function buildTransportDecoderLane() {
  const state_records = buildTransportStateRecords();
  const specimen_plan = buildTransportSpecimenPlan();
  return stable({
    schema_version: TRANSPORT_SCHEMA,
    record_kind: TRANSPORT_MODEL_VERSION,
    families: ['transport.rail', 'transport.bus', 'transport.kaup'],
    rejected_legacy_families: ['family.transport'],
    source_paths: TRANSPORT_SOURCE_PATHS,
    exact_marker_allowlist: TRANSPORT_EXACT_MARKER_ALLOWLIST,
    forbidden_evidence_markers: TRANSPORT_FORBIDDEN_EVIDENCE_MARKERS,
    breakpoint_contexts: TRANSPORT_BREAKPOINT_CONTEXTS,
    real_route_representatives: TRANSPORT_REAL_ROUTE_REPRESENTATIVES,
    pinned_observations: TRANSPORT_PINNED_OBSERVATIONS,
    state_records,
    specimen_plan,
    capture_requirements: transportCaptureRequirements(),
    decision: 'NOT_MERGED',
    normalization_allowed: false,
  });
}
