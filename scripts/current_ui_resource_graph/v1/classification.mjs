import { createHash } from 'node:crypto';

export const DISPOSITIONS = Object.freeze([
  'production-ui', 'composition-layout', 'lab-only', 'experiment-only', 'support-data',
  'nonvisual', 'dead-unreachable', 'needs-verification',
]);
export const REACHABILITY = Object.freeze([
  'production-observed', 'production-reachable-not-observed', 'controlled-specimen-only',
  'lab-only', 'experiment-off', 'source-only', 'dead-or-unreachable',
]);
export const CANONICAL_DISPOSITION_COUNTS = Object.freeze({
  'production-ui': 51, 'composition-layout': 20, 'lab-only': 20, 'experiment-only': 4,
  'support-data': 1, nonvisual: 8, 'dead-unreachable': 3, 'needs-verification': 0,
});

const GROUPS = Object.freeze({
  'composition-layout': `
src/components/DesktopEventPage.astro
src/components/ExhibitionsPersonalSurface.astro
src/components/FavoritesSurface.astro
src/components/FreeCollectionSurface.astro
src/components/GastronomyCollectionSurface.astro
src/components/HomeColdStartFeed.astro
src/components/OptimizedEventCardGrid.astro
src/components/PersonalFeedSlot.astro
src/components/Reference4MobileMenu.astro
src/components/UnusualListingSurface.astro
src/components/artifacts/ArtifactCollection.astro
src/components/listings/DateListingSurface.astro
src/components/listings/ListingDiscoveryRail.astro
src/components/listings/MobileListingRailSurface.astro
src/components/listings/PopularBehaviorRows.astro
src/components/listings/PopularListingSurface.astro
src/components/listings/PopularMobileAdaptiveRows.astro
src/components/listings/PopularMobileBehaviorRows.astro
src/components/listings/WeekendEditorialTimeline.astro
src/components/listings/WeekendListingSurface.astro`,
  'lab-only': `
src/components/EventCtaPanel.astro
src/components/EventListItem.astro
src/components/EventMediaRail.astro
src/components/FocusConnectivityDiagnostic.astro
src/components/FocusEggArtifact.astro
src/components/FocusEggCollectionCard.astro
src/components/FocusEggSavedListDemo.astro
src/components/FocusGroupFeedback.astro
src/components/FocusGroupInviteIntake.astro
src/components/FocusGroupInviteShare.astro
src/components/FocusGroupLabPanel.astro
src/components/FocusGroupThankYou.astro
src/components/FocusLabBadge.astro
src/components/FocusPwaInstallAction.astro
src/components/design-system/Badge.astro
src/components/design-system/Button.astro
src/components/design-system/CopyAction.astro
src/components/design-system/Field.astro
src/components/design-system/StatePanel.astro
src/components/lab/MobileEventReviewPage.astro`,
  'experiment-only': `
src/components/transport/DepartureBoardTimetable.astro
src/components/transport/NextDepartureQueueTimetable.astro
src/components/transport/RouteStripsTimetable.astro
src/components/transport/TransportTimetableExperiment.astro`,
  'support-data': `src/components/clubCatalogNavigation.mjs`,
  nonvisual: `
src/components/ClubCatalogKeyboard.astro
src/components/KeyboardEventNavigation.astro
src/components/KeyboardEventNavigationPrototype.astro
src/components/MobileEventProductionStyles.astro
src/components/PwaTelemetry.astro
src/components/UnusualUnreadRuntime.astro
src/components/auth/StaticSiteAuthRuntime.astro
src/components/personalization/PersonalizationRuntime.astro`,
  'dead-unreachable': `
src/components/MobileSearchBottomNav.astro
src/components/listings/PopularCategoryFilter.astro
src/components/listings/WeekendTimeMatrix.astro`,
  'needs-verification': ``,
});

const DISPOSITION_BY_PATH = new Map();
for (const [disposition, paths] of Object.entries(GROUPS)) {
  for (const path of paths.trim().split(/\s+/u).filter(Boolean)) DISPOSITION_BY_PATH.set(path, disposition);
}

function sha(value) { return createHash('sha256').update(value).digest('hex'); }

function dispositionFor(path) {
  if (DISPOSITION_BY_PATH.has(path)) return { value: DISPOSITION_BY_PATH.get(path), basis: 'reviewed-path-registry-v1' };
  if (path.startsWith('src/components/')) return { value: 'production-ui', basis: 'reviewed-candidate-complement-v1' };
  return { value: 'needs-verification', basis: 'unrecognized-logical-component-path' };
}

function runtimeEvidence(bindingIds, runtime, plane) {
  const ids = new Set(bindingIds);
  return runtime.filter((item) => item.plane === plane && (item.component_candidates || []).some((id) => ids.has(id))).map((item) => item.id).sort();
}

function reachabilityFor(disposition, candidateIds, rootIds, runtime, bindings) {
  const rootObserved = runtimeEvidence(rootIds, runtime, 'current_root_prelaunch');
  if (['production-ui', 'composition-layout'].includes(disposition)) {
    if (rootObserved.length) return { value: 'production-observed', basis: 'exact-public-root-route-transitive-import', runtime_evidence: rootObserved };
    const candidateObserved = runtimeEvidence(candidateIds, runtime, 'latest_checked_kaggle_candidate');
    const hasConsumer = bindings.some((item) => item.consumers.length > 0);
    if (candidateObserved.length || hasConsumer) return { value: 'production-reachable-not-observed', basis: candidateObserved.length ? 'exact-candidate-route-not-public-root-observation' : 'source-consumer-graph', runtime_evidence: candidateObserved };
    return { value: 'source-only', basis: 'no-runtime-or-consumer-evidence', runtime_evidence: [] };
  }
  if (disposition === 'lab-only') return { value: 'lab-only', basis: 'reviewed-lab-surface-classification', runtime_evidence: [] };
  if (disposition === 'experiment-only') return { value: 'experiment-off', basis: 'reviewed-disabled-transport-experiment', runtime_evidence: [] };
  if (disposition === 'dead-unreachable') return { value: 'dead-or-unreachable', basis: 'reviewed-zero-consumer-obsolete-source', runtime_evidence: [] };
  return { value: 'source-only', basis: disposition === 'needs-verification' ? 'unresolved-consumer-runtime-mismatch' : 'non-visual-source-evidence-only', runtime_evidence: [] };
}

function dispositionNote(disposition) {
  return ({
    'production-ui': 'Visual implementation retained AS-IS; classification is not a normalization decision.',
    'composition-layout': 'Composition/layout implementation retained separately from leaf visual components.',
    'lab-only': 'Lab/prototype surface; never promoted to production-observed by source presence.',
    'experiment-only': 'Transport timetable treatment is source-only with the production experiment disabled.',
    'support-data': 'Support/data module, not a visual component contract.',
    nonvisual: 'Runtime/style/keyboard behavior without an independent visual surface contract.',
    'dead-unreachable': 'Reviewed obsolete or unreachable implementation; retained as evidence.',
    'needs-verification': 'Source/runtime/consumer evidence is insufficient or contradictory.',
  })[disposition];
}

export function classifyLogicalComponents(sourceRecords, runtimeObservations) {
  const groups = new Map();
  for (const record of sourceRecords.filter((item) => item.type === 'component')) {
    if (!groups.has(record.path)) groups.set(record.path, []);
    groups.get(record.path).push(record);
  }
  const components = [];
  for (const [path, records] of [...groups].sort(([a], [b]) => a.localeCompare(b))) {
    const disposition = dispositionFor(path);
    const bindings = records.sort((a, b) => a.plane.localeCompare(b.plane)).map((record) => ({
      plane: record.plane, source_id: record.id, content_sha256: record.content_sha256,
      consumers: record.consumers, direct_dependencies: record.direct_dependencies,
      parser_status: record.evidence?.parser_status || 'unknown', state_parser_status: record.source_state?.parser_status || 'not_available',
    }));
    const candidateIds = bindings.filter((item) => item.plane === 'latest_checked_kaggle_candidate').map((item) => item.source_id);
    const rootIds = bindings.filter((item) => item.plane === 'current_root_prelaunch').map((item) => item.source_id);
    const reachability = reachabilityFor(disposition.value, candidateIds, rootIds, runtimeObservations, bindings);
    components.push({
      id: `component.${sha(path).slice(0, 16)}`, logical_path: path, name: records[0].name,
      disposition: disposition.value, disposition_basis: disposition.basis,
      reachability: reachability.value, reachability_basis: reachability.basis,
      plane_bindings: bindings, runtime_evidence: reachability.runtime_evidence,
      source_state_by_plane: Object.fromEntries(records.map((item) => [item.plane, item.source_state || null])),
      classification_note: dispositionNote(disposition.value),
      proof_label: reachability.value === 'production-observed' ? 'public-root-runtime-observed' :
        reachability.value === 'production-reachable-not-observed' ? 'source-or-candidate-reachable-not-production-observed' : 'source-classification-only',
      decision: 'NOT_MERGED', recommendation: 'unresolved',
    });
  }
  return components;
}

export function assertClassificationInvariants(components, { canonical = false } = {}) {
  const paths = new Set();
  for (const component of components) {
    if (paths.has(component.logical_path)) throw new Error(`Duplicate logical component path: ${component.logical_path}`);
    paths.add(component.logical_path);
    if (!DISPOSITIONS.includes(component.disposition)) throw new Error(`Unknown component disposition: ${component.disposition}`);
    if (!REACHABILITY.includes(component.reachability)) throw new Error(`Unknown component reachability: ${component.reachability}`);
    if (!component.disposition_basis || !component.reachability_basis || !component.classification_note) throw new Error(`Unexplained component disposition: ${component.logical_path}`);
    if (component.reachability === 'production-observed' && component.proof_label !== 'public-root-runtime-observed') throw new Error(`Production observation proof mismatch: ${component.logical_path}`);
    if (component.disposition === 'experiment-only' && component.reachability !== 'experiment-off') throw new Error(`Experiment promoted outside experiment-off: ${component.logical_path}`);
  }
  if (canonical) {
    if (components.length !== 107) throw new Error(`Canonical logical component coverage must be 107/107; observed ${components.length}`);
    const counts = classificationCounts(components).dispositions;
    for (const [disposition, expected] of Object.entries(CANONICAL_DISPOSITION_COUNTS)) {
      if (counts[disposition] !== expected) throw new Error(`Canonical disposition coverage mismatch for ${disposition}: expected ${expected}, observed ${counts[disposition]}`);
    }
  }
}

export function classificationCounts(components) {
  return {
    total: components.length,
    dispositions: Object.fromEntries(DISPOSITIONS.map((value) => [value, components.filter((item) => item.disposition === value).length])),
    reachability: Object.fromEntries(REACHABILITY.map((value) => [value, components.filter((item) => item.reachability === value).length])),
  };
}

export function knownExceptions() {
  return [
    { id: 'exception.labs-preview-special', classification: 'lab-only', current_status: 'intentional-desktop-only-baseline-excluded', synthesis_allowed: false },
    { id: 'exception.editorial-collections', classification: 'absent-as-is-future-requirement', current_status: 'not-observed', synthesis_allowed: false },
    { id: 'exception.legal', classification: 'absent-as-is-future-requirement', current_status: 'not-observed', synthesis_allowed: false },
    { id: 'exception.hero-talk-page-end', classification: 'absent-as-is-future-requirement', current_status: 'not-observed', synthesis_allowed: false },
    { id: 'exception.transport-timetable-experiment', classification: 'experiment-off-source-only', current_status: 'not-a-production-variant', synthesis_allowed: false },
    { id: 'exception.mobile-search-bottom-nav', classification: 'dead-unreachable-contract-mismatch', current_status: 'zero-consumer-zero-runtime-marker-in-exhaustive-pinned-scan', synthesis_allowed: false },
  ];
}
