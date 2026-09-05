import { readFileSync } from 'node:fs';

const freeze = Object.freeze;
const number = (value, fallback = 0) => Number.isFinite(Number(value)) ? Number(value) : fallback;
const lower = (value) => String(value || '').trim().toLowerCase();
const visible = (value) => value?.visible !== false;
const key = (id, width) => `${id}@${width}`;

export const SCHEMA = 'kenigevents.v0-browser-audit.v1';
export const CONTRACT_VERSION = '1.10.0';
export const AUTHORED_AGAINST_SOURCE = '1bc6d9cb4c122046f4782532381de953727c1da6';
export const OWNERS = freeze({ acceptance:'N0', foundations:'F0', framing:'FR0', components:'M0', routes:'A0' });
export const PRODUCT_SCOPE = freeze({
  authority:'site/src/data/design-system-production-surface-contract.v1.json',
  excludedRoutePrefixes:freeze(['/lab/', '/__preview/']),
  rule:'Only user-facing product archetypes enter V0 verdicts; generated QA/lab routes are non-product diagnostics.',
});
export const CANONICAL_A0_V0_MATRIX = freeze({
  repository: 'onedayonemasterpiece/lovekgd-design-system',
  ref: 'a59c8745ca21a39ef085bb912bbd611d6394f1a1',
  path: 'catalog/normalization/evidence/a0-v0-acceptance-matrix.v1.json',
  blobSha: 'e81a75a77a62c2f2efc2b9f0c72625f56c1fc38b',
  schemaVersion: 'kenigevents.a0-v0-acceptance-matrix.v1',
  recordedContractVersion: '1.9.0',
  relationship: 'EXECUTABLE_OVERLAY_NOT_REPLACEMENT',
});
export const PUBLISHED_TARGETS = freeze({
  real: freeze({ url:'https://kenigevents.ru/preview-real-5862a4ec6-normalized-20260904-v1/__preview/', sourceSha:'5862a4ec6728359548e6c4af76f97f1d9b94fb27', dataMode:'real', status:'HTTP_200_CURRENT_PUBLIC_PENDING_INDEPENDENT_V0', supersedes:'https://kenigevents.ru/preview-real-557019d68-normalized-20260904-v1/__preview/' }),
  golden: freeze({ url:'https://kenigevents.ru/preview-golden-84504f30-20270604-v1/__preview/', sourceSha:'84504f30eebc334deba46e94365601c3d572c5c0', dataMode:'golden', status:'V0_DRIFT_5527892153' }),
});
export const VIEWPORTS = freeze([
  [360,812,'mobile'], [375,812,'mobile'], [390,844,'mobile'], [430,932,'mobile-canonical'], [620,900,'mobile-wide'],
  [720,900,'mobile-seam-below'], [721,900,'mobile-seam-above'], [768,1024,'tablet'], [980,900,'desktop-seam-below'],
  [981,900,'desktop-seam-above'], [1024,768,'desktop-small'], [1180,900,'content-seam-below'], [1181,900,'content-seam-above'],
  [1280,800,'desktop-canonical'], [1366,768,'desktop'], [1440,900,'desktop'], [1536,864,'fhd-125-percent'],
  [1728,900,'desktop-wide-canonical'], [1920,1080,'fhd'],
].map(([width, height, cls]) => freeze({ id:`${cls}-${width}x${height}`, width, height, class:cls })));
export const SELECTORS = freeze({
  auxiliaryTarget: '[data-ds-family="EventCard"] .event-card__utility-row .feedback-button--negative[data-feedback-action="not_interested"]',
  externalTargets: 'a[target="_blank"]',
  popularRows: '.ke-popular-desktop .ke-popular-behavior__row',
  mediaFrames: '[data-media-frame][data-media-frame-contract="v1"]',
  adaptiveGrid: '[data-adaptive-event-card-grid][data-ds-family="AdaptiveEventCardGrid"]',
  adaptiveGridCards: '[data-adaptive-event-card-grid] > [data-event-card]',
  desktopNavigation: '.site-header nav[aria-label]',
  mobileNavigation: '.site-header details > summary[aria-label]',
  skipLink: '.skip-link',
});

const ownerForIdentity = ({ family, scope } = {}) => {
  const name = lower(family);
  if (['route','shell','consumer'].includes(scope)) return OWNERS.routes;
  if (scope === 'framing' || ['mediaframe','eventmediarail'].includes(name)) return OWNERS.framing;
  if (scope === 'foundation') return OWNERS.foundations;
  return OWNERS.components;
};
const ownerForTarget = ({ ownerHint, styleOwner } = {}) => {
  if (ownerHint === 'route' || styleOwner === 'EventLayout') return OWNERS.routes;
  if (ownerHint === 'foundation' || styleOwner === 'foundations') return OWNERS.foundations;
  return OWNERS.components;
};
const escapes = (frame, image, tolerance = 1) => frame && image && (
  number(image.x) < number(frame.x) - tolerance
  || number(image.y) < number(frame.y) - tolerance
  || number(image.x) + number(image.width) > number(frame.x) + number(frame.width) + tolerance
  || number(image.y) + number(image.height) > number(frame.y) + number(frame.height) + tolerance
);
const routePath = (value) => {
  const raw = String(value || '').trim();
  if (!raw) return '';
  try { return new URL(raw, 'https://kenigevents.invalid').pathname; } catch { return raw; }
};
export const isNonProductAuditRoute = (input = {}) => {
  if (input.routeKey === 'preview') return true;
  const path = routePath(input.route);
  return PRODUCT_SCOPE.excludedRoutePrefixes.some((prefix) => path === prefix.slice(0, -1)
    || path.startsWith(prefix)
    || path.includes(prefix));
};
const remainder = (count, rowSize) => {
  const size = Math.max(1, Math.floor(number(rowSize, 1)));
  const rest = Math.max(0, Math.floor(number(count))) % size;
  return { count:rest, variant:rest === 0 ? 'complete' : `regular-${rest}-of-${size}` };
};

export function classifyObservation(observation = {}) {
  const defects = [];
  const auditGaps = [];
  const ignoredDocuments = [];
  const target = observation.target || {};
  const documents = Array.isArray(observation.documents) ? observation.documents : [];
  const add = (document, code, owner, selector, evidence = {}) => defects.push({ code, owner, routeKey:document.routeKey, route:document.route, width:document.width, selector, evidence });

  if (!target.previewUrl || !target.sourceSha || !target.dataMode) auditGaps.push({ code:'TARGET_IDENTITY_INCOMPLETE', required:['previewUrl','sourceSha','dataMode'] });
  if (target.contractVersion && target.contractVersion !== CONTRACT_VERSION) defects.push({ code:'CONTRACT_VERSION_MISMATCH', owner:OWNERS.acceptance, evidence:{ expected:CONTRACT_VERSION, actual:target.contractVersion } });

  for (const input of documents) {
    const document = { ...input, route:input.route || '<unknown>' };
    if (isNonProductAuditRoute(document)) {
      ignoredDocuments.push({ routeKey:document.routeKey, route:document.route, reason:'NON_PRODUCT_QA_ROUTE' });
      continue;
    }
    const clientWidth = number(document.clientWidth ?? document.width);
    const documentOverflow = Math.max(number(document.documentScrollWidth, clientWidth), number(document.bodyScrollWidth, clientWidth)) - clientWidth;
    if (number(document.httpStatus) !== 200) add(document, 'HTTP_NOT_200', document.routeKey === 'preview' ? OWNERS.acceptance : OWNERS.routes, 'document', { httpStatus:document.httpStatus });
    if (documentOverflow > 1) add(document, 'DOCUMENT_HORIZONTAL_OVERFLOW', OWNERS.routes, 'html,body', { clientWidth, documentOverflow });
    if (document.expectsHeading !== false && Number.isFinite(Number(document.visibleH1Count)) && number(document.visibleH1Count) !== 1) add(document, 'VISIBLE_H1_COUNT', OWNERS.routes, 'h1:visible', { expected:1, actual:number(document.visibleH1Count) });

    if (document.shell) {
      const desktop = document.shell.desktopNavigationVisible === true;
      const mobile = document.shell.mobileNavigationVisible === true;
      const expectDesktop = number(document.width) >= 981;
      if (desktop === mobile || desktop !== expectDesktop) add(document, 'SHELL_RESPONSIVE_TRANSITION', OWNERS.routes, `${SELECTORS.desktopNavigation},${SELECTORS.mobileNavigation}`, { expectDesktop, desktop, mobile });
    }

    for (const control of document.auxiliaryTargets || []) if (visible(control) && (number(control.height) < 44 || control.clipped === true)) add(document, 'EVENT_CARD_AUXILIARY_TARGET_BELOW_44PX', ownerForTarget(control), SELECTORS.auxiliaryTarget, { height:number(control.height), minHeight:control.minHeight, clipped:control.clipped === true, ownerHint:control.ownerHint, styleOwner:control.styleOwner });
    for (const identity of document.identityExpectations || []) if (identity.present === false) add(document, 'CANONICAL_IDENTITY_MISSING', ownerForIdentity(identity), identity.selector || `[data-ds-family="${identity.family}"]`, { family:identity.family, scope:identity.scope, required:['data-ds-version','data-ds-variant','data-ds-state'] });
    for (const link of document.externalTargets || []) if (lower(link.target) === '_blank') {
      const tokens = new Set(lower(link.rel).split(/\s+/u).filter(Boolean));
      const missing = ['noopener','noreferrer'].filter((token) => !tokens.has(token));
      if (missing.length) add(document, 'UNSAFE_BLANK_TARGET', OWNERS.routes, SELECTORS.externalTargets, { href:link.href, accessibleName:link.accessibleName, rel:[...tokens].sort(), missing });
    }

    for (const row of document.popularRows || []) if (visible(row)) {
      const internalOverflow = number(row.scrollWidth) > number(row.clientWidth) + 1;
      if (lower(row.flexWrap) !== 'nowrap') add(document, 'POPULAR_ROW_NOT_NOWRAP', OWNERS.routes, SELECTORS.popularRows, { flexWrap:row.flexWrap });
      if (internalOverflow && !['auto','scroll'].includes(lower(row.overflowX))) add(document, 'POPULAR_ROW_OVERFLOW_ESCAPES_OWNER', OWNERS.routes, SELECTORS.popularRows, { clientWidth:number(row.clientWidth), scrollWidth:number(row.scrollWidth), overflowX:row.overflowX, fifthCardRight:row.fifthCardRight });
      if (number(row.documentOverflow) > 1) add(document, 'POPULAR_DOCUMENT_OVERFLOW', OWNERS.routes, SELECTORS.popularRows, { documentOverflow:number(row.documentOverflow), overflowX:row.overflowX });
    }

    for (const frame of document.mediaFrames || []) if (visible(frame)) {
      const kind = lower(frame.kind); const fit = lower(frame.fit); const permission = lower(frame.cropPermission); const computed = lower(frame.computedObjectFit);
      if (['document','unknown','fallback'].includes(kind) && (fit !== 'contain' || permission !== 'forbidden')) add(document, 'MEDIA_FRAME_FAIL_CLOSED_VIOLATION', OWNERS.framing, SELECTORS.mediaFrames, { surface:frame.surface, kind, fit, permission, cropReason:frame.cropReason });
      if (kind === 'visual' && fit === 'cover' && !['allowed','reviewed','reviewed-bounded'].includes(permission)) add(document, 'MEDIA_FRAME_COVER_PERMISSION_MISSING', OWNERS.framing, SELECTORS.mediaFrames, { surface:frame.surface, permission });
      if (computed && fit && computed !== fit) add(document, 'MEDIA_FRAME_COMPUTED_FIT_MISMATCH', OWNERS.framing, SELECTORS.mediaFrames, { surface:frame.surface, fit, computed });
      if (escapes(frame.frameBox, frame.imageBox)) add(document, 'MEDIA_FRAME_IMAGE_ESCAPES_FRAME', OWNERS.framing, SELECTORS.mediaFrames, { surface:frame.surface, frameBox:frame.frameBox, imageBox:frame.imageBox });
      if (['a','button'].includes(lower(frame.frameTag)) || lower(frame.interactionOwner) !== 'caller') add(document, 'MEDIA_FRAME_INTERACTION_OWNER_VIOLATION', OWNERS.framing, SELECTORS.mediaFrames, { surface:frame.surface, frameTag:frame.frameTag, interactionOwner:frame.interactionOwner });
    }

    for (const grid of document.adaptiveGrids || []) if (visible(grid)) {
      const expected = remainder(grid.renderedCount, grid.rowSize);
      if (lower(grid.layoutEngine) !== 'grid-subgrid' || lower(grid.display) !== 'grid') add(document, 'ADAPTIVE_GRID_LAYOUT_ENGINE_DRIFT', OWNERS.components, SELECTORS.adaptiveGrid, { layoutEngine:grid.layoutEngine, display:grid.display, flexWrap:grid.flexWrap });
      if (grid.allChildrenCanonical === false || number(grid.renderedCount) !== number(grid.directVisibleChildCount)) add(document, 'ADAPTIVE_GRID_CHILD_CARDINALITY_DRIFT', OWNERS.components, SELECTORS.adaptiveGridCards, { renderedCount:number(grid.renderedCount), directVisibleChildCount:number(grid.directVisibleChildCount), allChildrenCanonical:grid.allChildrenCanonical });
      if (number(grid.remainderCount) !== expected.count || grid.remainderVariant !== expected.variant || lower(grid.remainderPolicy) !== 'regular-column') add(document, 'ADAPTIVE_GRID_REMAINDER_DRIFT', OWNERS.components, SELECTORS.adaptiveGrid, { expected, actual:{ count:number(grid.remainderCount), variant:grid.actualVariant, policy:grid.actualPolicy } });
      if (Number.isFinite(Number(grid.finalLineWidthSum)) && !Number.isFinite(Number(grid.expectedFinalLineWidth))) add(document, 'ADAPTIVE_GRID_ORDINARY_WIDTH_EVIDENCE_MISSING', OWNERS.components, SELECTORS.adaptiveGrid);
      if (Number.isFinite(Number(grid.finalLineWidthSum)) && Number.isFinite(Number(grid.expectedFinalLineWidth)) && Math.abs(number(grid.finalLineWidthSum) - number(grid.expectedFinalLineWidth)) > 1) add(document, 'ADAPTIVE_GRID_FINAL_LINE_OCCUPANCY_DRIFT', OWNERS.components, SELECTORS.adaptiveGrid, { finalLineWidthSum:number(grid.finalLineWidthSum), rootContentWidth:number(grid.rootContentWidth) });
      if (number(grid.documentOverflow) > 1) add(document, 'ADAPTIVE_GRID_DOCUMENT_OVERFLOW', OWNERS.components, SELECTORS.adaptiveGrid, { documentOverflow:number(grid.documentOverflow) });
      if (grid.equalHeightApplies === true && number(grid.equalHeightDelta) > 1) add(document, 'ADAPTIVE_GRID_EQUAL_HEIGHT_DRIFT', OWNERS.components, SELECTORS.adaptiveGridCards, { equalHeightDelta:number(grid.equalHeightDelta) });
      if (grid.mode === 'flow' && grid.flowOrder !== grid.sourceOrder) add(document, 'ADAPTIVE_GRID_FLOW_ORDER_DRIFT', OWNERS.components, SELECTORS.adaptiveGrid, { flowOrder:grid.flowOrder, sourceOrder:grid.sourceOrder });
      if (grid.mode === 'packed' && grid.packedDeterministic === false) add(document, 'ADAPTIVE_GRID_PACKED_ORDER_NONDETERMINISTIC', OWNERS.components, SELECTORS.adaptiveGrid);
      if (number(grid.consumerWrapperCount) > 0) add(document, 'ROUTE_LOCAL_GRID_WRAPPER_PRESENT', OWNERS.routes, SELECTORS.adaptiveGrid, { consumerWrapperCount:number(grid.consumerWrapperCount) });
    }

    if (document.keyboard) {
      const keyboard = document.keyboard;
      if (keyboard.skipLinkVisibleOnFocus === false || keyboard.skipLinkBeforeHeader === false) add(document, 'SKIP_LINK_FOCUS_ORDER_DRIFT', OWNERS.routes, SELECTORS.skipLink, keyboard);
      if (number(keyboard.hiddenFocusableCount) > 0) add(document, 'HIDDEN_FOCUSABLES_PRESENT', OWNERS.routes, ':focusable', { count:number(keyboard.hiddenFocusableCount) });
      if (number(keyboard.nestedInteractiveCount) > 0) add(document, 'NESTED_INTERACTIVE_CONTROLS', keyboard.nestedInteractiveOwner || OWNERS.routes, ':is(a,button) :is(a,button)', { count:number(keyboard.nestedInteractiveCount) });
      if (number(keyboard.unnamedMediaRailButtonCount) > 0 || keyboard.heroPressedCoherent === false) add(document, 'MEDIA_RAIL_ACCESSIBILITY_DRIFT', OWNERS.framing, '[data-event-media-rail] button', { unnamed:number(keyboard.unnamedMediaRailButtonCount), heroPressedCoherent:keyboard.heroPressedCoherent });
      if (keyboard.focusIndicatorVisible === false) add(document, 'FOCUS_INDICATOR_FOUNDATION_DRIFT', OWNERS.foundations, ':focus-visible');
    }
  }

  if (observation.complete === true) {
    const expectedPairs = (Array.isArray(observation.expectedPairs) ? observation.expectedPairs : [])
      .filter((pair) => !isNonProductAuditRoute(pair));
    if (!expectedPairs.length) auditGaps.push({ code:'EXPECTED_PAIRS_REQUIRED', authority:CANONICAL_A0_V0_MATRIX.path });
    const present = new Set(documents.map((document) => key(document.caseId || document.routeKey, document.width)));
    for (const pair of expectedPairs) if (!present.has(key(pair.caseId || pair.routeKey, pair.width))) auditGaps.push({ code:'ROUTE_VIEWPORT_PAIR_MISSING', caseId:pair.caseId, routeKey:pair.routeKey, width:pair.width });
  }

  return {
    schema:SCHEMA,
    contractVersion:CONTRACT_VERSION,
    authoredAgainstSource:AUTHORED_AGAINST_SOURCE,
    checkpointSemantics:'NON_GATING',
    target,
    verdict:auditGaps.length ? 'INCOMPLETE' : defects.length ? 'DRIFT' : 'PASS',
    defects,
    auditGaps,
    ignoredDocuments,
    summary:{ documentsObserved:documents.length - ignoredDocuments.length, documentsIgnored:ignoredDocuments.length, defectCount:defects.length, auditGapCount:auditGaps.length, defectsByOwner:defects.reduce((out, item) => ({ ...out, [item.owner]:(out[item.owner] || 0) + 1 }), {}) },
  };
}

export const auditContract = freeze({
  schema:SCHEMA,
  contractVersion:CONTRACT_VERSION,
  authoredAgainstSource:AUTHORED_AGAINST_SOURCE,
  checkpointSemantics:'NON_GATING',
  canonicalA0V0Matrix:CANONICAL_A0_V0_MATRIX,
  owners:OWNERS,
  publishedTargets:PUBLISHED_TARGETS,
  viewports:VIEWPORTS,
  selectors:SELECTORS,
  productScope:PRODUCT_SCOPE,
  executionBoundary:freeze({ startsBrowser:false, browserExecutor:'my-browser-bridge-or-existing-release-gate', existingLocalReleaseGate:'site/scripts/check-browser-release-gate.mjs', replacesExistingReleaseGate:false, replacesCanonicalA0V0Matrix:false }),
  flowControl:freeze({ fr0CutoverRequiredBeforeFramingWrites:true, fr0MustNotDelayAlreadyReadySuccessor:true }),
  gates:freeze({ documentOverflowTolerancePx:1, controlMinimumPx:44, equalHeightTolerancePx:1, finalLineOccupancyTolerancePx:1, desktopNavigationBreakpointPx:981, safeBlankRelTokens:freeze(['noopener','noreferrer']), canonicalIdentityAttributes:freeze(['data-ds-family','data-ds-version','data-ds-variant','data-ds-state']), mediaFrameContract:'v1', adaptiveGridLayoutEngine:'grid-subgrid', adaptiveGridRemainderPolicy:'regular-column' }),
});

const classifyIndex = process.argv.indexOf('--classify');
if (classifyIndex >= 0) {
  const inputPath = process.argv[classifyIndex + 1];
  if (!inputPath || inputPath.startsWith('--')) throw new Error('Usage: node v0-browser-audit-contract.mjs --classify <observation.json|->');
  const result = classifyObservation(JSON.parse(inputPath === '-' ? readFileSync(0, 'utf8') : readFileSync(inputPath, 'utf8')));
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  process.exitCode = result.verdict === 'DRIFT' ? 1 : result.verdict === 'INCOMPLETE' ? 2 : 0;
} else if (process.argv.includes('--json')) process.stdout.write(`${JSON.stringify(auditContract, null, 2)}\n`);
