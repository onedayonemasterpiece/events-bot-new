const freeze = (value) => Object.freeze(value);

export const SCHEMA = 'kenigevents.v0-browser-audit.v1';
export const CONTRACT_VERSION = '1.10.0';
export const SOURCE_CHECKPOINT = '1bc6d9cb4c122046f4782532381de953727c1da6';

export const OWNERS = freeze({
  acceptance: 'N0',
  foundations: 'F0',
  framing: 'FR0',
  components: 'M0',
  routes: 'A0',
});

export const PUBLISHED_TARGETS = freeze({
  real: freeze({
    url: 'https://kenigevents.ru/preview-real-4536847f-fresh-20260903-v1/__preview/',
    sourceSha: '4536847f9fbdaa27326ebb3ec9ec1c825736e107',
    dataMode: 'real',
    status: 'HTTP_200_UNAUDITED_BY_V0',
  }),
  golden: freeze({
    url: 'https://kenigevents.ru/preview-golden-84504f30-20270604-v1/__preview/',
    sourceSha: '84504f30eebc334deba46e94365601c3d572c5c0',
    dataMode: 'golden',
    status: 'V0_DRIFT_5527892153',
  }),
});

export const ROUTES = freeze([
  freeze({ key: 'home', path: '/', discovery: 'fixed' }),
  freeze({ key: 'preview', path: '/__preview/', discovery: 'fixed' }),
  freeze({ key: 'today', path: '/segodnya/', discovery: 'fixed' }),
  freeze({ key: 'tomorrow', path: '/zavtra/', discovery: 'fixed' }),
  freeze({ key: 'date', path: '/date-YYYY-MM-DD/', discovery: 'first-http-200-linked-date' }),
  freeze({ key: 'weekend', path: '/vyhodnye/', discovery: 'fixed' }),
  freeze({ key: 'popular', path: '/populyarnoe/', discovery: 'fixed' }),
  freeze({ key: 'exhibitions', path: '/vystavki/', discovery: 'fixed' }),
  freeze({ key: 'festivals', path: '/festivali/', discovery: 'fixed' }),
  freeze({ key: 'free', path: '/podborki/besplatnye-sobytiya/', discovery: 'fixed' }),
  freeze({ key: 'event-detail', path: '/sobytiya/<live-slug>/', discovery: 'first-live-event-href-from-preview' }),
]);

export const VIEWPORTS = freeze([
  freeze({ width: 360, height: 812, class: 'mobile' }),
  freeze({ width: 375, height: 812, class: 'mobile' }),
  freeze({ width: 390, height: 844, class: 'mobile' }),
  freeze({ width: 620, height: 900, class: 'mobile-wide' }),
  freeze({ width: 720, height: 900, class: 'mobile-seam-below' }),
  freeze({ width: 721, height: 900, class: 'mobile-seam-above' }),
  freeze({ width: 768, height: 1024, class: 'tablet' }),
  freeze({ width: 980, height: 900, class: 'desktop-seam-below' }),
  freeze({ width: 981, height: 900, class: 'desktop-seam-above' }),
  freeze({ width: 1024, height: 768, class: 'desktop-small' }),
  freeze({ width: 1180, height: 900, class: 'content-seam-below' }),
  freeze({ width: 1181, height: 900, class: 'content-seam-above' }),
  freeze({ width: 1366, height: 768, class: 'desktop' }),
  freeze({ width: 1440, height: 900, class: 'desktop' }),
  freeze({ width: 1536, height: 864, class: 'fhd-125-percent' }),
  freeze({ width: 1920, height: 1080, class: 'fhd' }),
]);

export const SELECTORS = freeze({
  shell: freeze({
    header: '.site-header',
    skipLink: '.skip-link',
    desktopNavigation: 'nav[aria-label="Основная навигация"]',
    mobileNavigation: 'details summary[aria-label="Открыть навигацию афиши"]',
  }),
  eventCardAuxiliaryTarget: '[data-ds-family="EventCard"] .event-card__utility-row .feedback-button--negative[data-feedback-action="not_interested"]',
  identities: freeze({
    eventCard: '[data-ds-family="EventCard"][data-ds-version][data-ds-variant][data-ds-state]',
    listingEventCard: '[data-ds-family="ListingEventCard"][data-ds-version][data-ds-variant][data-ds-state]',
    adaptiveGrid: '[data-ds-family="AdaptiveEventCardGrid"][data-ds-version][data-ds-variant][data-ds-state]',
    mediaRail: '[data-ds-family="EventMediaRail"][data-ds-version][data-ds-variant][data-ds-state]',
  }),
  externalTargets: 'a[target="_blank"]',
  popularRows: '.ke-popular-desktop .ke-popular-behavior__row',
  popularCards: '.ke-popular-desktop .ke-popular-behavior__row > [data-ds-family="ListingEventCard"]',
  mediaFrames: '[data-media-frame][data-media-frame-contract="v1"]',
  mediaFrameImage: '[data-media-frame-image]',
  mediaFrameFallback: '[data-media-frame-fallback]',
  adaptiveGrid: '[data-adaptive-event-card-grid][data-ds-family="AdaptiveEventCardGrid"]',
  adaptiveGridCards: '[data-adaptive-event-card-grid] > [data-event-card]',
  focusables: '.skip-link, a[href]:not([hidden]), button:not([disabled]):not([hidden]), [tabindex]:not([tabindex="-1"]):not([hidden]), [data-event-media-rail] button, [data-event-card]',
});

const finite = (value) => Number.isFinite(Number(value));
const n = (value, fallback = 0) => finite(value) ? Number(value) : fallback;
const visible = (record) => record?.visible !== false;
const relTokens = (value) => new Set(String(value || '').trim().toLowerCase().split(/\s+/u).filter(Boolean));
const lower = (value) => String(value || '').trim().toLowerCase();

const ownerForAuxiliaryTarget = (record) => {
  if (record.ownerHint === 'route' || record.styleOwner === 'EventLayout') return OWNERS.routes;
  if (record.ownerHint === 'foundation' || record.styleOwner === 'foundations') return OWNERS.foundations;
  return OWNERS.components;
};

const ownerForIdentityScope = (scope) => {
  if (scope === 'route' || scope === 'shell' || scope === 'consumer') return OWNERS.routes;
  if (scope === 'framing') return OWNERS.framing;
  if (scope === 'foundation') return OWNERS.foundations;
  return OWNERS.components;
};

const addDefect = (defects, document, code, owner, selector, evidence = {}) => {
  defects.push({
    code,
    owner,
    routeKey: document.routeKey,
    route: document.route,
    width: document.width,
    selector,
    evidence,
  });
};

const boxEscapes = (frame, image, tolerance = 1) => {
  if (!frame || !image) return false;
  const frameLeft = n(frame.x);
  const frameTop = n(frame.y);
  const frameRight = frameLeft + n(frame.width);
  const frameBottom = frameTop + n(frame.height);
  const imageLeft = n(image.x);
  const imageTop = n(image.y);
  const imageRight = imageLeft + n(image.width);
  const imageBottom = imageTop + n(image.height);
  return imageLeft < frameLeft - tolerance
    || imageTop < frameTop - tolerance
    || imageRight > frameRight + tolerance
    || imageBottom > frameBottom + tolerance;
};

const expectedRemainderVariant = (count, rowSize) => {
  const safeSize = Math.max(1, Math.floor(n(rowSize, 1)));
  const remainder = Math.max(0, Math.floor(n(count))) % safeSize;
  return remainder === 0 ? 'complete' : `stretch-${remainder}-of-${safeSize}`;
};

const routeViewportKey = (routeKey, width) => `${routeKey}@${width}`;

export function classifyObservation(observation = {}) {
  const defects = [];
  const auditGaps = [];
  const documents = Array.isArray(observation.documents) ? observation.documents : [];
  const target = observation.target || {};

  if (!target.previewUrl || !target.sourceSha || !target.dataMode) {
    auditGaps.push({ code: 'TARGET_IDENTITY_INCOMPLETE', required: ['previewUrl', 'sourceSha', 'dataMode'] });
  }
  if (target.contractVersion && target.contractVersion !== CONTRACT_VERSION) {
    defects.push({
      code: 'CONTRACT_VERSION_MISMATCH',
      owner: OWNERS.acceptance,
      evidence: { expected: CONTRACT_VERSION, actual: target.contractVersion },
    });
  }

  for (const document of documents) {
    const routeSelector = document.route || ROUTES.find((item) => item.key === document.routeKey)?.path || '<unknown>';
    document.route = routeSelector;

    if (n(document.httpStatus, 0) !== 200) {
      addDefect(defects, document, 'HTTP_NOT_200', document.routeKey === 'preview' ? OWNERS.acceptance : OWNERS.routes, 'document', {
        httpStatus: document.httpStatus,
      });
    }

    const viewportWidth = n(document.clientWidth ?? document.documentClientWidth ?? document.width);
    const documentScrollWidth = n(document.documentScrollWidth ?? document.scrollWidth, viewportWidth);
    const bodyScrollWidth = n(document.bodyScrollWidth, viewportWidth);
    if (documentScrollWidth > viewportWidth + 1 || bodyScrollWidth > viewportWidth + 1) {
      addDefect(defects, document, 'DOCUMENT_HORIZONTAL_OVERFLOW', OWNERS.routes, 'html, body', {
        clientWidth: viewportWidth,
        documentScrollWidth,
        bodyScrollWidth,
      });
    }

    if (document.expectsHeading !== false && finite(document.visibleH1Count) && n(document.visibleH1Count) !== 1) {
      addDefect(defects, document, 'VISIBLE_H1_COUNT', OWNERS.routes, 'h1:visible', {
        expected: 1,
        actual: n(document.visibleH1Count),
      });
    }

    const shell = document.shell;
    if (shell) {
      const expectDesktop = n(document.width) >= 981;
      const desktopVisible = shell.desktopNavigationVisible === true;
      const mobileVisible = shell.mobileNavigationVisible === true;
      if (desktopVisible === mobileVisible || desktopVisible !== expectDesktop) {
        addDefect(defects, document, 'SHELL_RESPONSIVE_TRANSITION', OWNERS.routes, `${SELECTORS.shell.desktopNavigation}, ${SELECTORS.shell.mobileNavigation}`, {
          expectDesktop,
          desktopVisible,
          mobileVisible,
        });
      }
    }

    for (const control of document.auxiliaryTargets || []) {
      if (!visible(control)) continue;
      if (n(control.height) < 44 || control.clipped === true) {
        addDefect(defects, document, 'EVENT_CARD_AUXILIARY_TARGET_BELOW_44PX', ownerForAuxiliaryTarget(control), SELECTORS.eventCardAuxiliaryTarget, {
          height: n(control.height),
          minHeight: control.minHeight,
          paddingBlock: control.paddingBlock,
          lineHeight: control.lineHeight,
          clipped: control.clipped === true,
          ownerHint: control.ownerHint,
          styleOwner: control.styleOwner,
        });
      }
    }

    for (const expectation of document.identityExpectations || []) {
      if (expectation.present === false) {
        addDefect(defects, document, 'CANONICAL_IDENTITY_MISSING', ownerForIdentityScope(expectation.scope), expectation.selector || `[data-ds-family="${expectation.family}"]`, {
          family: expectation.family,
          scope: expectation.scope,
          requiredAttributes: ['data-ds-version', 'data-ds-variant', 'data-ds-state'],
        });
      }
    }

    for (const link of document.externalTargets || []) {
      if (lower(link.target) !== '_blank') continue;
      const tokens = relTokens(link.rel);
      if (!tokens.has('noopener') || !tokens.has('noreferrer')) {
        addDefect(defects, document, 'UNSAFE_BLANK_TARGET', OWNERS.routes, SELECTORS.externalTargets, {
          href: link.href,
          accessibleName: link.accessibleName,
          rel: [...tokens].sort(),
          missing: ['noopener', 'noreferrer'].filter((token) => !tokens.has(token)),
        });
      }
    }

    for (const row of document.popularRows || []) {
      if (!visible(row)) continue;
      const rowOverflows = n(row.scrollWidth) > n(row.clientWidth) + 1;
      const overflowX = lower(row.overflowX);
      if (lower(row.flexWrap) !== 'nowrap') {
        addDefect(defects, document, 'POPULAR_ROW_NOT_NOWRAP', OWNERS.routes, SELECTORS.popularRows, {
          flexWrap: row.flexWrap,
        });
      }
      if (rowOverflows && !['auto', 'scroll'].includes(overflowX)) {
        addDefect(defects, document, 'POPULAR_ROW_OVERFLOW_ESCAPES_OWNER', OWNERS.routes, SELECTORS.popularRows, {
          clientWidth: n(row.clientWidth),
          scrollWidth: n(row.scrollWidth),
          overflowX,
          fifthCardRight: row.fifthCardRight,
          viewportRight: row.viewportRight,
        });
      }
      if (n(row.documentOverflow) > 1) {
        addDefect(defects, document, 'POPULAR_DOCUMENT_OVERFLOW', OWNERS.routes, SELECTORS.popularRows, {
          documentOverflow: n(row.documentOverflow),
          clientWidth: n(row.clientWidth),
          scrollWidth: n(row.scrollWidth),
          overflowX,
        });
      }
    }

    for (const frame of document.mediaFrames || []) {
      if (!visible(frame)) continue;
      const kind = lower(frame.kind);
      const fit = lower(frame.fit);
      const computedFit = lower(frame.computedObjectFit);
      const cropPermission = lower(frame.cropPermission);
      if (['document', 'unknown', 'fallback'].includes(kind) && (fit !== 'contain' || cropPermission !== 'forbidden')) {
        addDefect(defects, document, 'MEDIA_FRAME_FAIL_CLOSED_VIOLATION', OWNERS.framing, SELECTORS.mediaFrames, {
          surface: frame.surface,
          kind,
          fit,
          cropPermission,
          cropReason: frame.cropReason,
        });
      }
      if (kind === 'visual' && fit === 'cover' && !['allowed', 'reviewed', 'reviewed-bounded'].includes(cropPermission)) {
        addDefect(defects, document, 'MEDIA_FRAME_COVER_PERMISSION_MISSING', OWNERS.framing, SELECTORS.mediaFrames, {
          surface: frame.surface,
          kind,
          fit,
          cropPermission,
          cropReason: frame.cropReason,
        });
      }
      if (computedFit && fit && computedFit !== fit) {
        addDefect(defects, document, 'MEDIA_FRAME_COMPUTED_FIT_MISMATCH', OWNERS.framing, SELECTORS.mediaFrameImage, {
          surface: frame.surface,
          fit,
          computedObjectFit: computedFit,
        });
      }
      if (boxEscapes(frame.frameBox, frame.imageBox)) {
        addDefect(defects, document, 'MEDIA_FRAME_IMAGE_ESCAPES_FRAME', OWNERS.framing, SELECTORS.mediaFrames, {
          surface: frame.surface,
          frameBox: frame.frameBox,
          imageBox: frame.imageBox,
        });
      }
      if (['a', 'button'].includes(lower(frame.frameTag)) || lower(frame.interactionOwner) !== 'caller') {
        addDefect(defects, document, 'MEDIA_FRAME_INTERACTION_OWNER_VIOLATION', OWNERS.framing, SELECTORS.mediaFrames, {
          surface: frame.surface,
          frameTag: frame.frameTag,
          interactionOwner: frame.interactionOwner,
        });
      }
    }

    for (const grid of document.adaptiveGrids || []) {
      if (!visible(grid)) continue;
      const expectedVariant = expectedRemainderVariant(grid.renderedCount, grid.rowSize);
      const expectedRemainder = Math.max(0, Math.floor(n(grid.renderedCount))) % Math.max(1, Math.floor(n(grid.rowSize, 1)));
      if (lower(grid.layoutEngine) !== 'flex-lines' || lower(grid.display) !== 'flex' || lower(grid.flexWrap) !== 'wrap') {
        addDefect(defects, document, 'ADAPTIVE_GRID_LAYOUT_ENGINE_DRIFT', OWNERS.components, SELECTORS.adaptiveGrid, {
          layoutEngine: grid.layoutEngine,
          display: grid.display,
          flexWrap: grid.flexWrap,
        });
      }
      if (grid.allChildrenCanonical === false || n(grid.renderedCount) !== n(grid.directVisibleChildCount)) {
        addDefect(defects, document, 'ADAPTIVE_GRID_CHILD_CARDINALITY_DRIFT', OWNERS.components, SELECTORS.adaptiveGridCards, {
          allChildrenCanonical: grid.allChildrenCanonical,
          renderedCount: n(grid.renderedCount),
          directVisibleChildCount: n(grid.directVisibleChildCount),
        });
      }
      if (n(grid.remainderCount) !== expectedRemainder || grid.remainderVariant !== expectedVariant || lower(grid.remainderPolicy) !== 'stretch') {
        addDefect(defects, document, 'ADAPTIVE_GRID_REMAINDER_DRIFT', OWNERS.components, SELECTORS.adaptiveGrid, {
          rowSize: n(grid.rowSize),
          renderedCount: n(grid.renderedCount),
          expectedRemainder,
          actualRemainder: n(grid.remainderCount),
          expectedVariant,
          actualVariant: grid.remainderVariant,
          remainderPolicy: grid.remainderPolicy,
        });
      }
      if (finite(grid.finalLineWidthSum) && finite(grid.rootContentWidth) && Math.abs(n(grid.finalLineWidthSum) - n(grid.rootContentWidth)) > 1) {
        addDefect(defects, document, 'ADAPTIVE_GRID_FINAL_LINE_OCCUPANCY_DRIFT', OWNERS.components, SELECTORS.adaptiveGrid, {
          finalLineWidthSum: n(grid.finalLineWidthSum),
          rootContentWidth: n(grid.rootContentWidth),
          delta: Math.abs(n(grid.finalLineWidthSum) - n(grid.rootContentWidth)),
        });
      }
      if (n(grid.documentOverflow) > 1) {
        addDefect(defects, document, 'ADAPTIVE_GRID_DOCUMENT_OVERFLOW', OWNERS.components, SELECTORS.adaptiveGrid, {
          documentOverflow: n(grid.documentOverflow),
        });
      }
      if (grid.equalHeightApplies === true && n(grid.equalHeightDelta) > 1) {
        addDefect(defects, document, 'ADAPTIVE_GRID_EQUAL_HEIGHT_DRIFT', OWNERS.components, SELECTORS.adaptiveGridCards, {
          equalHeightDelta: n(grid.equalHeightDelta),
        });
      }
      if (grid.mode === 'flow' && grid.flowOrder !== grid.sourceOrder) {
        addDefect(defects, document, 'ADAPTIVE_GRID_FLOW_ORDER_DRIFT', OWNERS.components, SELECTORS.adaptiveGrid, {
          flowOrder: grid.flowOrder,
          sourceOrder: grid.sourceOrder,
        });
      }
      if (grid.mode === 'packed' && grid.packedDeterministic === false) {
        addDefect(defects, document, 'ADAPTIVE_GRID_PACKED_ORDER_NONDETERMINISTIC', OWNERS.components, SELECTORS.adaptiveGrid, {});
      }
      if (n(grid.consumerWrapperCount) > 0) {
        addDefect(defects, document, 'ROUTE_LOCAL_GRID_WRAPPER_PRESENT', OWNERS.routes, SELECTORS.adaptiveGrid, {
          consumerWrapperCount: n(grid.consumerWrapperCount),
        });
      }
    }

    const keyboard = document.keyboard;
    if (keyboard) {
      if (keyboard.skipLinkVisibleOnFocus === false || keyboard.skipLinkBeforeHeader === false) {
        addDefect(defects, document, 'SKIP_LINK_FOCUS_ORDER_DRIFT', OWNERS.routes, SELECTORS.shell.skipLink, {
          skipLinkVisibleOnFocus: keyboard.skipLinkVisibleOnFocus,
          skipLinkBeforeHeader: keyboard.skipLinkBeforeHeader,
        });
      }
      if (n(keyboard.hiddenFocusableCount) > 0) {
        addDefect(defects, document, 'HIDDEN_FOCUSABLES_PRESENT', OWNERS.routes, SELECTORS.focusables, {
          hiddenFocusableCount: n(keyboard.hiddenFocusableCount),
        });
      }
      if (n(keyboard.nestedInteractiveCount) > 0) {
        addDefect(defects, document, 'NESTED_INTERACTIVE_CONTROLS', keyboard.nestedInteractiveOwner || OWNERS.routes, SELECTORS.focusables, {
          nestedInteractiveCount: n(keyboard.nestedInteractiveCount),
        });
      }
      if (n(keyboard.unnamedMediaRailButtonCount) > 0 || keyboard.heroPressedCoherent === false) {
        addDefect(defects, document, 'MEDIA_RAIL_ACCESSIBILITY_DRIFT', OWNERS.components, '[data-event-media-rail] button', {
          unnamedMediaRailButtonCount: n(keyboard.unnamedMediaRailButtonCount),
          heroPressedCoherent: keyboard.heroPressedCoherent,
        });
      }
      if (keyboard.focusIndicatorVisible === false) {
        addDefect(defects, document, 'FOCUS_INDICATOR_FOUNDATION_DRIFT', OWNERS.foundations, ':focus-visible', {});
      }
    }
  }

  if (observation.complete === true) {
    const expectedRouteKeys = observation.expectedRouteKeys || ROUTES.map((route) => route.key);
    const expectedWidths = observation.expectedWidths || VIEWPORTS.map((viewport) => viewport.width);
    const present = new Set(documents.map((document) => routeViewportKey(document.routeKey, document.width)));
    for (const routeKey of expectedRouteKeys) {
      for (const width of expectedWidths) {
        const key = routeViewportKey(routeKey, width);
        if (!present.has(key)) auditGaps.push({ code: 'ROUTE_VIEWPORT_PAIR_MISSING', routeKey, width });
      }
    }
  }

  return {
    schema: SCHEMA,
    contractVersion: CONTRACT_VERSION,
    sourceCheckpoint: SOURCE_CHECKPOINT,
    target,
    verdict: auditGaps.length > 0 ? 'INCOMPLETE' : defects.length > 0 ? 'DRIFT' : 'PASS',
    defects,
    auditGaps,
    summary: {
      documentsObserved: documents.length,
      defectCount: defects.length,
      auditGapCount: auditGaps.length,
      defectsByOwner: defects.reduce((result, defect) => {
        result[defect.owner] = (result[defect.owner] || 0) + 1;
        return result;
      }, {}),
    },
  };
}

export const auditContract = freeze({
  schema: SCHEMA,
  contractVersion: CONTRACT_VERSION,
  sourceCheckpoint: SOURCE_CHECKPOINT,
  owners: OWNERS,
  publishedTargets: PUBLISHED_TARGETS,
  routes: ROUTES,
  viewports: VIEWPORTS,
  selectors: SELECTORS,
  gates: freeze({
    documentOverflowTolerancePx: 1,
    controlMinimumPx: 44,
    equalHeightTolerancePx: 1,
    finalLineOccupancyTolerancePx: 1,
    desktopNavigationBreakpointPx: 981,
    safeBlankRelTokens: freeze(['noopener', 'noreferrer']),
    canonicalIdentityAttributes: freeze(['data-ds-family', 'data-ds-version', 'data-ds-variant', 'data-ds-state']),
    mediaFrameContract: 'v1',
    adaptiveGridLayoutEngine: 'flex-lines',
    adaptiveGridRemainderPolicy: 'stretch',
  }),
});

if (process.argv.includes('--json')) {
  process.stdout.write(`${JSON.stringify(auditContract, null, 2)}\n`);
}
