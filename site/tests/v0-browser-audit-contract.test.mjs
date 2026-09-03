import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import {
  CONTRACT_VERSION,
  OWNERS,
  SELECTORS,
  SOURCE_CHECKPOINT,
  auditContract,
  classifyObservation,
} from '../scripts/v0-browser-audit-contract.mjs';

const target = {
  previewUrl: 'https://kenigevents.ru/example/__preview/',
  sourceSha: SOURCE_CHECKPOINT,
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

test('contract is pinned to 1.10.0 and routes each domain to the current lowest owner', () => {
  assert.equal(CONTRACT_VERSION, '1.10.0');
  assert.equal(auditContract.sourceCheckpoint, SOURCE_CHECKPOINT);
  assert.deepEqual(OWNERS, {
    acceptance: 'N0',
    foundations: 'F0',
    framing: 'FR0',
    components: 'M0',
    routes: 'A0',
  });
  assert.match(SELECTORS.eventCardAuxiliaryTarget, /data-ds-family="EventCard"/u);
  assert.doesNotMatch(JSON.stringify(auditContract), /data-ui-(?:root|role)/u);
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

test('canonical identity uses data-ds markers and routes missing roots by scope', () => {
  const result = classify(document({
    identityExpectations: [
      { family: 'EventLayout', scope: 'route', present: false },
      { family: 'EventCard', scope: 'component', present: false },
      { family: 'MediaFrame', scope: 'framing', present: false },
    ],
  }));
  const missing = result.defects.filter((item) => item.code === 'CANONICAL_IDENTITY_MISSING');
  assert.deepEqual(missing.map((item) => item.owner), ['A0', 'M0', 'FR0']);
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

test('shell, keyboard and foundation focus checks retain separate ownership', () => {
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
  assert.equal(defect(result, 'MEDIA_RAIL_ACCESSIBILITY_DRIFT').owner, 'M0');
  assert.equal(defect(result, 'FOCUS_INDICATOR_FOUNDATION_DRIFT').owner, 'F0');
});

test('complete audits fail closed when a route/viewport pair is missing', () => {
  const result = classify(document(), {
    complete: true,
    expectedRouteKeys: ['free'],
    expectedWidths: [375, 390],
  });
  assert.equal(result.verdict, 'INCOMPLETE');
  assert.deepEqual(result.auditGaps, [{ code: 'ROUTE_VIEWPORT_PAIR_MISSING', routeKey: 'free', width: 390 }]);
});

test('machine-readable matrix stays aligned with the executable contract', async () => {
  const matrix = JSON.parse(await readFile(new URL('./fixtures/v0-browser-audit-matrix.v1.json', import.meta.url), 'utf8'));
  assert.equal(matrix.schema, 'kenigevents.v0-browser-audit-matrix.v1');
  assert.equal(matrix.contract_version, CONTRACT_VERSION);
  assert.equal(matrix.source_checkpoint, SOURCE_CHECKPOINT);
  assert.deepEqual(matrix.owners, OWNERS);
  assert.deepEqual(matrix.viewports.map((item) => item.width), auditContract.viewports.map((item) => item.width));
  assert.equal(matrix.selectors.media_frames, SELECTORS.mediaFrames);
  assert.equal(matrix.gates.control_minimum_px, 44);
});
