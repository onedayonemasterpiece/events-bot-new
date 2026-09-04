import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import {
  AUTHORED_AGAINST_SOURCE,
  CANONICAL_A0_V0_MATRIX,
  CONTRACT_VERSION,
  OWNERS,
  PRODUCT_SCOPE,
  PUBLISHED_TARGETS,
  SELECTORS,
  auditContract,
  classifyObservation,
  isNonProductAuditRoute,
} from '../scripts/v0-browser-audit-contract.mjs';

const target = {
  previewUrl: 'https://kenigevents.ru/example/__preview/',
  sourceSha: AUTHORED_AGAINST_SOURCE,
  dataMode: 'real',
  contractVersion: CONTRACT_VERSION,
};

const document = (overrides = {}) => ({
  routeKey: 'free',
  route: '/podborki/besplatnye-sobytiya/',
  width: 375,
  httpStatus: 200,
  clientWidth: 375,
  documentScrollWidth: 375,
  bodyScrollWidth: 375,
  visibleH1Count: 1,
  ...overrides,
});

const classify = (doc, extra = {}) => classifyObservation({
  target,
  documents: [doc],
  ...extra,
});

const defect = (result, code) => result.defects.find((item) => item.code === code);

test('contract 1.10 routes each current domain to its lowest owner without creating data-ui identity', () => {
  assert.equal(CONTRACT_VERSION, '1.10.0');
  assert.equal(auditContract.authoredAgainstSource, AUTHORED_AGAINST_SOURCE);
  assert.equal(auditContract.checkpointSemantics, 'NON_GATING');
  assert.deepEqual(OWNERS, {
    acceptance: 'N0',
    foundations: 'F0',
    framing: 'FR0',
    components: 'M0',
    routes: 'A0',
  });
  assert.match(SELECTORS.auxiliaryTarget, /data-ds-family="EventCard"/u);
  assert.doesNotMatch(JSON.stringify(auditContract), /data-ui-(?:root|role)/u);
});

test('the executable overlay references rather than replaces the canonical A0-V0 matrix and release gate', () => {
  assert.equal(CANONICAL_A0_V0_MATRIX.relationship, 'EXECUTABLE_OVERLAY_NOT_REPLACEMENT');
  assert.equal(CANONICAL_A0_V0_MATRIX.path, 'catalog/normalization/evidence/a0-v0-acceptance-matrix.v1.json');
  assert.equal(CANONICAL_A0_V0_MATRIX.blobSha, 'e81a75a77a62c2f2efc2b9f0c72625f56c1fc38b');
  assert.equal(auditContract.executionBoundary.startsBrowser, false);
  assert.equal(auditContract.executionBoundary.replacesExistingReleaseGate, false);
  assert.equal(auditContract.executionBoundary.replacesCanonicalA0V0Matrix, false);
  assert.equal(auditContract.executionBoundary.existingLocalReleaseGate, 'site/scripts/check-browser-release-gate.mjs');
  assert.equal(auditContract.flowControl.fr0CutoverRequiredBeforeFramingWrites, true);
  assert.equal(auditContract.flowControl.fr0MustNotDelayAlreadyReadySuccessor, true);
});

test('published target pointer follows the newest exact R0 HTTP-200 real preview without erasing history', () => {
  assert.equal(PUBLISHED_TARGETS.real.url, 'https://kenigevents.ru/preview-real-1bc6d9cb-normalized-20260903-v1/__preview/');
  assert.equal(PUBLISHED_TARGETS.real.sourceSha, AUTHORED_AGAINST_SOURCE);
  assert.equal(PUBLISHED_TARGETS.real.supersedes, 'https://kenigevents.ru/preview-real-4536847f-fresh-20260903-v1/__preview/');
  assert.match(PUBLISHED_TARGETS.real.status, /UNAUDITED_BY_V0/u);
});

test('authored source is evidence, not a hard gate for a newer or older exact preview target', () => {
  const result = classifyObservation({
    target: { ...target, sourceSha: '4536847f9fbdaa27326ebb3ec9ec1c825736e107' },
    documents: [document()],
  });
  assert.equal(result.verdict, 'PASS');
  assert.equal(result.authoredAgainstSource, AUTHORED_AGAINST_SOURCE);
  assert.equal(result.checkpointSemantics, 'NON_GATING');
  assert.equal(defect(result, 'SOURCE_SHA_MISMATCH'), undefined);
});

test('lab and Preview-directory documents are ignored instead of becoming product drift', () => {
  assert.deepEqual(PRODUCT_SCOPE.excludedRoutePrefixes, ['/lab/', '/__preview/']);
  assert.equal(isNonProductAuditRoute({ route: '/lab/design-system/' }), true);
  assert.equal(isNonProductAuditRoute({ route: '/preview-real-sha/lab/hero/' }), true);
  assert.equal(isNonProductAuditRoute({ routeKey: 'preview', route: '/preview-real-sha/__preview/' }), true);
  assert.equal(isNonProductAuditRoute({ route: '/segodnya/' }), false);

  const result = classifyObservation({
    target,
    documents: [
      document({ routeKey: 'lab-design-system', route: '/lab/design-system/', httpStatus: 500, documentScrollWidth: 900 }),
      document({ routeKey: 'today', route: '/segodnya/' }),
    ],
  });
  assert.equal(result.verdict, 'PASS');
  assert.equal(result.summary.documentsObserved, 1);
  assert.equal(result.summary.documentsIgnored, 1);
  assert.deepEqual(result.ignoredDocuments, [{
    routeKey: 'lab-design-system',
    route: '/lab/design-system/',
    reason: 'NON_PRODUCT_QA_ROUTE',
  }]);
});

test('classification does not mutate the browser observation payload', () => {
  const inputDocument = {
    routeKey: 'free',
    width: 375,
    httpStatus: 200,
    clientWidth: 375,
    documentScrollWidth: 375,
    bodyScrollWidth: 375,
    visibleH1Count: 1,
  };
  const snapshot = structuredClone(inputDocument);
  classifyObservation({ target, documents: [inputDocument] });
  assert.deepEqual(inputDocument, snapshot);
  assert.equal(Object.hasOwn(inputDocument, 'route'), false);
});

test('44px auxiliary target passes while route, component and foundation failures keep distinct owners', () => {
  const pass = classify(document({ auxiliaryTargets: [{ visible: true, height: 44, ownerHint: 'route' }] }));
  assert.equal(pass.verdict, 'PASS');

  const route = classify(document({ auxiliaryTargets: [{ visible: true, height: 36.28, ownerHint: 'route' }] }));
  assert.equal(defect(route, 'EVENT_CARD_AUXILIARY_TARGET_BELOW_44PX').owner, 'A0');

  const component = classify(document({ auxiliaryTargets: [{ visible: true, height: 43.99, ownerHint: 'component' }] }));
  assert.equal(defect(component, 'EVENT_CARD_AUXILIARY_TARGET_BELOW_44PX').owner, 'M0');

  const foundation = classify(document({ auxiliaryTargets: [{ visible: true, height: 40, ownerHint: 'foundation' }] }));
  assert.equal(defect(foundation, 'EVENT_CARD_AUXILIARY_TARGET_BELOW_44PX').owner, 'F0');
});

test('canonical identity uses data-ds markers and routes EventMediaRail or MediaFrame roots to FR0', () => {
  const result = classify(document({
    identityExpectations: [
      { family: 'EventLayout', scope: 'route', present: false },
      { family: 'EventCard', scope: 'component', present: false },
      { family: 'MediaFrame', scope: 'component', present: false },
      { family: 'EventMediaRail', scope: 'component', present: false },
    ],
  }));
  const missing = result.defects.filter((item) => item.code === 'CANONICAL_IDENTITY_MISSING');
  assert.deepEqual(missing.map((item) => item.owner), ['A0', 'M0', 'FR0', 'FR0']);
});

test('target=_blank is neutral when both safety tokens exist and A0 drift otherwise', () => {
  const safe = classify(document({
    externalTargets: [{ target: '_blank', href: 'https://example.test', rel: 'noreferrer noopener' }],
  }));
  assert.equal(safe.verdict, 'PASS');

  const unsafe = classify(document({
    externalTargets: [{ target: '_blank', href: 'https://example.test', rel: 'noopener' }],
  }));
  assert.equal(defect(unsafe, 'UNSAFE_BLANK_TARGET').owner, 'A0');
  assert.deepEqual(defect(unsafe, 'UNSAFE_BLANK_TARGET').evidence.missing, ['noreferrer']);
});

test('popular shelf may own internal scrolling but may not widen the document', () => {
  const pass = classify(document({
    routeKey: 'popular',
    route: '/populyarnoe/',
    width: 1366,
    clientWidth: 1366,
    documentScrollWidth: 1366,
    bodyScrollWidth: 1366,
    popularRows: [{ visible: true, clientWidth: 1200, scrollWidth: 1540, flexWrap: 'nowrap', overflowX: 'auto', documentOverflow: 0 }],
  }));
  assert.equal(pass.verdict, 'PASS');

  const drift = classify(document({
    routeKey: 'popular',
    route: '/populyarnoe/',
    width: 1366,
    clientWidth: 1366,
    documentScrollWidth: 1512,
    bodyScrollWidth: 1512,
    popularRows: [{ visible: true, clientWidth: 1200, scrollWidth: 1540, flexWrap: 'nowrap', overflowX: 'visible', documentOverflow: 146 }],
  }));
  assert.equal(defect(drift, 'POPULAR_ROW_OVERFLOW_ESCAPES_OWNER').owner, 'A0');
  assert.equal(defect(drift, 'POPULAR_DOCUMENT_OVERFLOW').owner, 'A0');
});

test('MediaFrame fit, geometry and interaction defects are routed to FR0', () => {
  const result = classify(document({
    mediaFrames: [{
      visible: true,
      surface: 'event-hero',
      kind: 'document',
      fit: 'cover',
      cropPermission: 'allowed',
      computedObjectFit: 'contain',
      frameTag: 'button',
      interactionOwner: 'MediaFrame',
      frameBox: { x: 0, y: 0, width: 300, height: 400 },
      imageBox: { x: -4, y: 0, width: 308, height: 400 },
    }],
  }));
  assert.ok(result.defects.length >= 3);
  assert.ok(result.defects.every((item) => item.owner === 'FR0'));
});

test('AdaptiveEventCardGrid occupancy, cardinality and remainder defects are M0; route wrappers remain A0', () => {
  const result = classify(document({
    adaptiveGrids: [{
      visible: true,
      mode: 'flow',
      layoutEngine: 'grid',
      display: 'grid',
      flexWrap: 'nowrap',
      rowSize: 3,
      renderedCount: 5,
      directVisibleChildCount: 4,
      allChildrenCanonical: false,
      remainderCount: 0,
      remainderVariant: 'complete',
      remainderPolicy: 'phantom-track',
      rootContentWidth: 1000,
      finalLineWidthSum: 740,
      documentOverflow: 20,
      equalHeightApplies: true,
      equalHeightDelta: 4,
      flowOrder: '2,1',
      sourceOrder: '1,2',
      consumerWrapperCount: 1,
    }],
  }));
  assert.equal(defect(result, 'ADAPTIVE_GRID_LAYOUT_ENGINE_DRIFT').owner, 'M0');
  assert.equal(defect(result, 'ADAPTIVE_GRID_REMAINDER_DRIFT').owner, 'M0');
  assert.equal(defect(result, 'ADAPTIVE_GRID_FINAL_LINE_OCCUPANCY_DRIFT').owner, 'M0');
  assert.equal(defect(result, 'ROUTE_LOCAL_GRID_WRAPPER_PRESENT').owner, 'A0');
});

test('shell and general focus remain A0/F0 while EventMediaRail accessibility belongs to FR0', () => {
  const result = classify(document({
    width: 981,
    shell: { desktopNavigationVisible: false, mobileNavigationVisible: true },
    keyboard: {
      skipLinkVisibleOnFocus: false,
      skipLinkBeforeHeader: false,
      hiddenFocusableCount: 2,
      nestedInteractiveCount: 1,
      unnamedMediaRailButtonCount: 1,
      heroPressedCoherent: false,
      focusIndicatorVisible: false,
    },
  }));
  assert.equal(defect(result, 'SHELL_RESPONSIVE_TRANSITION').owner, 'A0');
  assert.equal(defect(result, 'SKIP_LINK_FOCUS_ORDER_DRIFT').owner, 'A0');
  assert.equal(defect(result, 'MEDIA_RAIL_ACCESSIBILITY_DRIFT').owner, 'FR0');
  assert.equal(defect(result, 'FOCUS_INDICATOR_FOUNDATION_DRIFT').owner, 'F0');
});

test('case-specific completeness fails closed without inventing a route-by-width Cartesian product', () => {
  const result = classifyObservation({
    target,
    complete: true,
    expectedPairs: [
      { caseId: 'PM0-37-04', routeKey: 'free', width: 390 },
      { caseId: 'PM0-37-04', routeKey: 'free', width: 1728 },
    ],
    documents: [document({ caseId: 'PM0-37-04', width: 390, clientWidth: 390, documentScrollWidth: 390, bodyScrollWidth: 390 })],
  });
  assert.equal(result.verdict, 'INCOMPLETE');
  assert.deepEqual(result.auditGaps, [{
    code: 'ROUTE_VIEWPORT_PAIR_MISSING',
    caseId: 'PM0-37-04',
    routeKey: 'free',
    width: 1728,
  }]);
});

test('CLI classifies a browser observation from stdin for direct R0/V0 consumption', () => {
  const scriptPath = fileURLToPath(new URL('../scripts/v0-browser-audit-contract.mjs', import.meta.url));
  const execution = spawnSync(process.execPath, [scriptPath, '--classify', '-'], {
    encoding: 'utf8',
    input: JSON.stringify({ target, documents: [document()] }),
  });
  assert.equal(execution.status, 0, execution.stderr);
  const result = JSON.parse(execution.stdout);
  assert.equal(result.verdict, 'PASS');
  assert.equal(result.checkpointSemantics, 'NON_GATING');
});

test('machine-readable overlay aligns with executable seams and canonical PM0-37 authority', async () => {
  const matrix = JSON.parse(await readFile(new URL('./fixtures/v0-browser-audit-matrix.v1.json', import.meta.url), 'utf8'));
  assert.equal(matrix.schema, 'kenigevents.v0-browser-audit-matrix.v1');
  assert.equal(matrix.contract_version, CONTRACT_VERSION);
  assert.equal(matrix.authored_against_source, AUTHORED_AGAINST_SOURCE);
  assert.equal(matrix.checkpoint_semantics, 'NON_GATING');
  assert.deepEqual(matrix.owners, OWNERS);
  assert.equal(matrix.authority.relationship, 'EXECUTABLE_OVERLAY_NOT_REPLACEMENT');
  assert.equal(matrix.authority.canonical_a0_v0_matrix.path, CANONICAL_A0_V0_MATRIX.path);
  assert.equal(matrix.authority.canonical_a0_v0_matrix.blob_sha, CANONICAL_A0_V0_MATRIX.blobSha);
  assert.equal(matrix.authority.canonical_a0_v0_matrix.ref, CANONICAL_A0_V0_MATRIX.ref);
  assert.equal(matrix.published_targets.real.url, PUBLISHED_TARGETS.real.url);
  assert.equal(matrix.published_targets.real.source_sha, PUBLISHED_TARGETS.real.sourceSha);
  assert.deepEqual(matrix.viewports.map((item) => item.width), auditContract.viewports.map((item) => item.width));
  assert.deepEqual(matrix.viewports.map((item) => item.id), auditContract.viewports.map((item) => item.id));
  assert.equal(matrix.viewports.length, 19);
  assert.equal(matrix.selectors.media_frames, SELECTORS.mediaFrames);
  assert.equal(matrix.gates.control_minimum_px, 44);
  assert.equal(matrix.pm0_item_37_overlay.denominator, 19);
  assert.equal(matrix.pm0_item_37_overlay.cases.length, 19);
  assert.equal(new Set(matrix.pm0_item_37_overlay.cases.map((item) => item.id)).size, 19);
  assert.equal(matrix.rollback.product_behavior_effect, 'none');
  assert.equal(matrix.execution_boundary.replaces_existing_release_gate, false);
  assert.equal(matrix.execution_boundary.replaces_canonical_a0_v0_matrix, false);
});
