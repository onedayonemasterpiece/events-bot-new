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
  real: freeze({ url:'https://kenigevents.ru/preview-real-1bc6d9cb-normalized-20260903-v1/__preview/', sourceSha:'1bc6d9cb4c122046f4782532381de953727c1da6', dataMode:'real', status:'HTTP_200_CURRENT_PUBLIC_PRE_F0_BASELINE_UNAUDITED_BY_V0', supersedes:'https://kenigevents.ru/preview-real-4536847f-fresh-20260903-v1/__preview/' }),
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
  desktopNavigation: 'nav[aria-label="ÐžÑÐ½Ð¾Ð²Ð½Ð°Ñ Ð½Ð°Ð²Ð¸Ð³Ð°Ñ†Ð¸Ñ"]',
  mobileNavigation: 'details summary[aria-label="ÐžÑ‚ÐºÑ€Ñ‹Ñ‚ÑŒ Ð½Ð°Ð²Ð¸Ð³Ð°Ñ†Ð¸ÑŽ Ð°Ñ„Ð¸ÑˆÐ¸"]',
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
const remainder = (count, rowSize) => {
  const size = Math.max(1, Math.floor(number(rowSize, 1)));
  const rest = Math.max(0, Math.floor(number(count))) % size;
  return { count:rest, variant:rest === 0 ? 'complete' : `stretch-${rest}-of-${size}` };
};

export function classifyObservation(observation = {}) {
  const defects = [];
  const auditGaps = [];
  const target = observation.target || {};
  const documents = Array.isArray(observation.documents) ? observation.documents : [];
  const add = (document, code, owner, selector, evidence = {}) => defects.push({ code, owner, routeKey:document.routeKey, route:document.route, width:document.width, selector, evidence });

  if (!target.previewUrl || !target.sourceSha || !target.dataMode) auditGaps.push({ code:'TARGET_IDENTITY_INCOMPLETE', required:['previewUrl','sourceSha','dataMode'] });
  if (target.contractVersion && target.contractVersion !== CONTRACT_VERSION) defects.push({ code:'CONTRACT_VERSION_MISMATCH', owner:OWNERS.acceptance, evidence:{ expected:CONTRACT_VERSION, actual:target.contractVersion } });

  for (const input of documents) {
    const document = { ...input, route:input.route || '<unknown>' };
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
      if (missing.length)²È="25Ù•Èœ€˜˜€…l…±±½Ý•œ°É•Ù¥•Ý•œ°É•Ù¥•Ý•µ‰½Õ¹‘•t¹¥¹±Õ‘•Ì¡Á•Éµ¥ÍÍ¥½¸¤¤…‘¡‘½Õµ•¹Ð°€5%}I5}=YI}AI5%MM%=9}5%MM%9œ°=]9IL¹™É…µ¥¹œ°M1Q=IL¹µ•‘¥…É…µ•Ì°ìÍÕÉ™…”é™É…µ”¹ÍÕÉ™…”°Á•Éµ¥ÍÍ¥½¸ô¤ì(€€€€€¥˜€¡½µÁÕÑ•€˜˜™¥Ð€˜˜½µÁÕÑ•€„ôô™¥Ð¤…‘¡‘½Õµ•¹Ð°€5%}I5}=5AUQ}%Q}5%M5Q œ°=]9IL¹™É…µ¥¹œ°M1Q=IL¹µ•‘¥…É…µ•Ì°ìÍÕÉ™…”é™É…µ”¹ÍÕÉ™…”°™¥Ð°½µÁÕÑ•ô¤ì(€€€€€¥˜€¡•Í…Á•Ì¡™É…µ”¹™É…µ•	½à°™É…µ”¹¥µ…•	½à¤¤…‘¡‘½Õµ•¹Ð°€5%}I5}%5}MAM}I5œ°=]9IL¹™É…µ¥¹œ°M1Q=IL¹µ•‘¥…É…µ•Ì°ìÍÕÉ™…”é™É…µ”¹ÍÕÉ™…”°™É…µ•	½àé™É…µ”¹™É…µ•	½à°¥µ…•	½àé™É…µ”¹¥µ…•	½àô¤ì(€€€€€¥˜€¡l„œ°‰ÕÑÑ½¸t¹¥¹±Õ‘•Ì¡±½Ý•È¡™É…µ”¹™É…µ•Q…œ¤¤ñð±½Ý•È¡™É…µ”¹¥¹Ñ•É…Ñ¥½¹=Ý¹•È¤€„ôô€…±±•Èœ¤…‘¡‘½Õµ•¹Ð°€5%}I5}%9QIQ%=9}=]9I}Y%=1Q%=8œ°=]9IL¹™É…µ¥¹œ°M1Q=IL¹µ•‘¥…É…µ•Ì°ìÍÕÉ™…”é™É…µ”¹ÍÕÉ™…”°™É…µ•Q…œé™É…µ”¹™É…µ•Q…œ°¥¹Ñ•É…Ñ¥½¹=Ý¹•Èé™É…µ”¹¥¹Ñ•É…Ñ¥½¹=Ý¹•Èô¤ì(€€€ô((€€€™½È€¡½¹ÍÐÉ¥½˜‘½Õµ•¹Ð¹…‘…ÁÑ¥Ù•É¥‘Ìñðmt¤¥˜€¡Ù¥Í¥‰±”¡É¥¤¤ì(€€€€€½¹ÍÐ•áÁ•Ñ•€ôÉ•µ…¥¹‘•È¡É¥¹É•¹‘•É•‘½Õ¹Ð°É¥¹É½ÝM¥é”¤ì(€€€€€¥˜€¡±½Ý•È¡É¥¹±…å½ÕÑ¹¥¹”¤€„ôô€™±•àµ±¥¹•Ìœñð±½Ý•È¡É¥¹‘¥ÍÁ±…ä¤€„ôô€™±•àœñð±½Ý•È¡É¥¹™±•á]É…À¤€„ôô€ÝÉ…Àœ¤…‘¡‘½Õµ•¹Ð°€AQ%Y}I%}1e=UQ}9%9}I%Pœ°=]9IL¹½µÁ½¹•¹ÑÌ°M1Q=IL¹…‘…ÁÑ¥Ù•É¥°ì±…å½ÕÑ¹¥¹”éÉ¥¹±…å½ÕÑ¹¥¹”°‘¥ÍÁ±…äéÉ¥¹‘¥ÍÁ±…ä°™±•á]É…ÀéÉ¥¹™±•á]É…Àô¤ì(€€€€€¥˜€¡É¥¹…±±¡¥±‘É•¹…¹½¹¥…°€ôôô™…±Í”ñð¹Õµ‰•È¡É¥¹É•¹‘•É•‘½Õ¹Ð¤€„ôô¹Õµ‰•È¡É¥¹‘¥É•ÑY¥Í¥‰±•¡¥±‘½Õ¹Ð¤¤…‘¡‘½Õµ•¹Ð°€AQ%Y}I%}!%1}I%91%Qe}I%Pœ°=]9IL¹½µÁ½¹•¹ÑÌ°M1Q=IL¹…‘…ÁÑ¥Ù•É¥‘…É‘Ì°ìÉ•¹‘•É•‘½Õ¹Ðé¹Õµ‰•È¡É¥¹É•¹‘•É•‘½Õ¹Ð¤°‘¥É•ÑY¥Í¥‰±•¡¥±‘½Õ¹Ðé¹Õµ‰•È¡É¥¹‘¥É•ÑY¥Í¥‰±•¡¥±‘½Õ¹Ð¤°…±±¡¥±‘É•¹…¹½¹¥…°éÉ¥¹…±±¡¥±‘É•¹…¹½¹¥…°ô¤ì(€€€€€¥˜€¡¹Õµ‰•È¡É¥¹É•µ…¥¹‘•É½Õ¹Ð¤€„ôô•áÁ•Ñ•¹½Õ¹ÐñðÉ¥¹É•µ…¥¹‘•ÉY…É¥…¹Ð€„ôô•áÁ•Ñ•¹Ù…É¥…¹Ðñð±½Ý•È¡É¥¹É•µ…¥¹‘•ÉA½±¥ä¤€„ôô€ÍÑÉ•Ñ œ¤…‘¡‘½Õµ•¹Ð°€AQ%Y}I%}I5%9I}I%Pœ°=]9IL¹½µÁ½¹•¹ÑÌ°M1Q=IL¹…‘…ÁÑ¥Ù•É¥°ì•áÁ•Ñ•°…ÑÕ…°éì½Õ¹Ðé¹Õµ‰•È¡É¥¹É•µ…¥¹‘•É½Õ¹Ð¤°Ù…É¥…¹ÐéÉ¥¹É•µ…¥¹‘•ÉY…É¥…¹Ð°Á½±¥äéÉ¥¹É•µ…¥¹‘•ÉA½±¥äôô¤ì(€€€€€¥˜€¡9Õµ‰•È¹¥Í¥¹¥Ñ”¡9Õµ‰•È¡É¥¹™¥¹…±1¥¹•]¥‘Ñ¡MÕ´¤¤€˜˜5…Ñ ¹…‰Ì¡¹Õµ‰•È¡É¥¹™¥¹…±1¥¹•]¥‘Ñ¡MÕ´¤€´¹Õµ‰•È¡É¥¹É½½Ñ½¹Ñ•¹Ñ]¥‘Ñ ¤¤€ø€Ä¤…‘¡‘½Õµ•¹Ð°€AQ%Y}I%}%91}1%9}=UA9e}I%Pœ°=]9IL¹½µÁ½¹•¹ÑÌ°M1Q=IL¹…‘…ÁÑ¥Ù•É¥°ì™¥¹…±1¥¹•]¥‘Ñ¡MÕ´é¹Õµ‰•È¡É¥¹™¥¹…±1¥¹•]¥‘Ñ¡MÕ´¤°É½½Ñ½¹Ñ•¹Ñ]¥‘Ñ é¹Õµ‰•È¡É¥¹É½½Ñ½¹Ñ•¹Ñ]¥‘Ñ ¤ô¤ì(€€€€€¥˜€¡¹Õµ‰•È¡É¥¹‘½Õµ•¹Ñ=Ù•É™±½Ü¤€ø€Ä¤…‘¡‘½Õµ•¹Ð°€AQ%Y}I%}=U59Q}=YI1=\œ°=]9IL¹½µÁ½¹•¹ÑÌ°M1Q=IL¹…‘…ÁÑ¥Ù•É¥°ì‘½Õµ•¹Ñ=Ù•É™±½Üé¹Õµ‰•È¡É¥¹‘½Õµ•¹Ñ=Ù•É™±½Ü¤ô¤ì(€€€€€¥˜€¡É¥¹•ÅÕ…±!•¥¡ÑÁÁ±¥•Ì€ôôôÑÉÕ”€˜˜¹Õµ‰•È¡É¥¹•ÅÕ…±!•¥¡Ñ•±Ñ„¤€ø€Ä¤…‘¡‘½Õµ•¹Ð°€AQ%Y}I%}EU1}!%!Q}I%Pœ°=]9IL¹½µÁ½¹•¹ÑÌ°M1Q=IL¹…‘…ÁÑ¥Ù•É¥‘…É‘Ì°ì•ÅÕ…±!•¥¡Ñ•±Ñ„é¹Õµ‰•È¡É¥¹•ÅÕ…±!•¥¡Ñ•±Ñ„¤ô¤ì(€€€€€¥˜€¡É¥¹µ½‘”€ôôô€™±½Üœ€˜˜É¥¹™±½Ý=É‘•È€„ôôÉ¥¹Í½ÕÉ•=É‘•È¤…‘¡‘½Õµ•¹Ð°€AQ%Y}I%}1=]}=II}I%Pœ°=]9IL¹½µÁ½¹•¹ÑÌ°M1Q=IL¹…‘…ÁÑ¥Ù•É¥°ì™±½Ý=É‘•ÈéÉ¥¹™±½Ý=É‘•È°Í½ÕÉ•=É‘•ÈéÉ¥¹Í½ÕÉ•=É‘•Èô¤ì(€€€€€¥˜€¡É¥¹µ½‘”€ôôô€Á…­•œ€˜˜É¥¹Á…­•‘•Ñ•Éµ¥¹¥ÍÑ¥Œ€ôôô™…±Í”¤…‘¡‘½Õµ•¹Ð°€AQ%Y}I%}A-}=II}9=9QI5%9%MQ%œ°=]9IL¹½µÁ½¹•¹ÑÌ°M1Q=IL¹…‘…ÁÑ¥Ù•É¥¤ì(€€€€€¥˜€¡¹Õµ‰•È¡É¥¹½¹ÍÕµ•É]É…ÁÁ•É½Õ¹Ð¤€ø€À¤…‘¡‘½Õµ•¹Ð°€I=UQ}1=1}I%}]IAAI}AIM9Pœ°=]9IL¹É½ÕÑ•Ì°M1Q=IL¹…‘…ÁÑ¥Ù•É¥°ì½¹ÍÕµ•É]É…ÁÁ•É½Õ¹Ðé¹Õµ‰•È¡É¥¹½¹ÍÕµ•É]É…ÁÁ•É½Õ¹Ð¤ô¤ì(€€€ô((€€€¥˜€¡‘½Õµ•¹Ð¹­•å‰½…É¤ì(€€€€€½¹ÍÐ­•å‰½…É€ô‘½Õµ•¹Ð¹­•å‰½…Éì(€€€€€¥˜€¡­•å‰½…É¹Í­¥Á1¥¹­Y¥Í¥‰±•=¹½ÕÌ€ôôô™…±Í”ñð­•å‰½…É¹Í­¥Á1¥¹­	•™½É•!•…‘•È€ôôô™…±Í”¤…‘¡‘½Õµ•¹Ð°€M-%A}1%9-}=UM}=II}I%Pœ°=]9IL¹É½ÕÑ•Ì°M1Q=IL¹Í­¥Á1¥¹¬°­•å‰½…É¤ì(€€€€€¥˜€¡¹Õµ‰•È¡­•å‰½…É¹¡¥‘‘•¹½ÕÍ…‰±•½Õ¹Ð¤€ø€À¤…‘¡‘½Õµ•¹Ð°€!%9}=UM	1M}AIM9Pœ°=]9IL¹É½ÕÑ•Ì°€œé™½ÕÍ…‰±”œ°ì½Õ¹Ðé¹Õµ‰•È¡­•å‰½…É¹¡¥‘‘•¹½ÕÍ…‰±•½Õ¹Ð¤ô¤ì(€€€€€¥˜€¡¹Õµ‰•È¡­•å‰½…É¹¹•ÍÑ•‘%¹Ñ•É…Ñ¥Ù•½Õ¹Ð¤€ø€À¤…‘¡‘½Õµ•¹Ð°€9MQ}%9QIQ%Y}=9QI=1Lœ°­•å‰½…É¹¹•ÍÑ•‘%¹Ñ•É…Ñ¥Ù•=Ý¹•Èñð=]9IL¹É½ÕÑ•Ì°€œé¥Ì¡„±‰ÕÑÑ½¸¤€é¥Ì¡„±‰ÕÑÑ½¸¤œ°ì½Õ¹Ðé¹Õµ‰•È¡­•å‰½…É¹¹•ÍÑ•‘%¹Ñ•É…Ñ¥Ù•½Õ¹Ð¤ô¤ì(€€€€€¥˜€¡¹Õµ‰•È¡­•å‰½…É¹Õ¹¹…µ•‘5•‘¥…I…¥±	ÕÑÑ½¹½Õ¹Ð¤€ø€Àñð­•å‰½…É¹¡•É½AÉ•ÍÍ•‘½¡•É•¹Ð€ôôô™…±Í”¤…‘¡‘½Õµ•¹Ð°€5%}I%1}MM%	%1%Qe}I%Pœ°=]9IL¹™É…µ¥¹œ°€m‘…Ñ„µ•Ù•¹Ðµµ•‘¥„µÉ…¥±t‰ÕÑÑ½¸œ°ìÕ¹¹…µ•é¹Õµ‰•È¡­•å‰½…É¹Õ¹¹…µ•‘5•‘¥…I…¥±	ÕÑÑ½¹½Õ¹Ð¤°¡•É½AÉ•ÍÍ•‘½¡•É•¹Ðé­•å‰½…É¹¡•É½AÉ•ÍÍ•‘½¡•É•¹Ðô¤ì(€€€€€¥˜€¡­•å‰½…É¹™½ÕÍ%¹‘¥…Ñ½ÉY¥Í¥‰±”€ôôô™…±Í”¤…‘¡‘½Õµ•¹Ð°€=UM}%9%Q=I}=U9Q%=9}I%Pœ°=]9IL¹™½Õ¹‘…Ñ¥½¹Ì°€œé™½ÕÌµÙ¥Í¥‰±”œ¤ì(€€€ô(€ô((€¥˜€¡½‰Í•ÉÙ…Ñ¥½¸¹½µÁ±•Ñ”€ôôôÑÉÕ”¤ì(€€€½¹ÍÐ•áÁ•Ñ•‘A…¥ÉÌ€ôÉÉ…ä¹¥ÍÉÉ…ä¡½‰Í•ÉÙ…Ñ¥½¸¹•áÁ•Ñ•‘A…¥ÉÌ¤€ü½‰Í•ÉÙ…Ñ¥½¸¹•áÁ•Ñ•‘A…¥ÉÌ€èmtì(€€€¥˜€ …•áÁ•Ñ•‘A…¥ÉÌ¹±•¹Ñ ¤…Õ‘¥Ñ…ÁÌ¹ÁÕÍ ¡ì½‘”èaAQ}A%IM}IEU%Iœ°…ÕÑ¡½É¥Ñäé9=9%1}Á}XÁ}5QI%`¹Á…Ñ ô¤ì(€€€½¹ÍÐÁÉ•Í•¹Ð€ô¹•ÜM•Ð¡‘½Õµ•¹ÑÌ¹µ…À ¡‘½Õµ•¹Ð¤€ôø­•ä¡‘½Õµ•¹Ð¹…Í•%ñð‘½Õµ•¹Ð¹É½ÕÑ•-•ä°‘½Õµ•¹Ð¹Ý¥‘Ñ ¤¤¤ì(€€€™½È€¡½¹ÍÐÁ…¥È½˜•áÁ•Ñ•‘A…¥ÉÌ¤¥˜€ …ÁÉ•Í•¹Ð¹¡…Ì¡­•ä¡Á…¥È¹…Í•%ñðÁ…¥È¹É½ÕÑ•-•ä°Á…¥È¹Ý¥‘Ñ ¤¤¤…Õ‘¥Ñ…ÁÌ¹ÁÕÍ ¡ì½‘”èI=UQ}Y%]A=IQ}A%I}5%MM%9œ°…Í•%éÁ…¥È¹…Í•%°É½ÕÑ•-•äéÁ…¥È¹É½ÕÑ•-•ä°Ý¥‘Ñ éÁ…¥È¹Ý¥‘Ñ ô¤ì(€ô((€É•ÑÕÉ¸ì(€€€Í¡•µ„éM!5°(€€€½¹ÑÉ…ÑY•ÉÍ¥½¸é=9QIQ}YIM%=8°(€€€…ÕÑ¡½É•‘…¥¹ÍÑM½ÕÉ”éUQ!=I}%9MQ}M=UI°(€€€¡•­Á½¥¹ÑM•µ…¹Ñ¥Ìè9=9}Q%9œ°(€€€Ñ…É•Ð°(€€€Ù•É‘¥Ðé…Õ‘¥Ñ…ÁÌ¹±•¹Ñ €ü€%9=5A1Qœ€è‘•™•ÑÌ¹±•¹Ñ €ü€I%Pœ€è€AMLœ°(€€€‘•™•ÑÌ°(€€€…Õ‘¥Ñ…ÁÌ°(€€€ÍÕµµ…Éäéì‘½Õµ•¹ÑÍ=‰Í•ÉÙ•é‘½Õµ•¹ÑÌ¹±•¹Ñ °‘•™•Ñ½Õ¹Ðé‘•™•ÑÌ¹±•¹Ñ °…Õ‘¥Ñ…Á½Õ¹Ðé…Õ‘¥Ñ…ÁÌ¹±•¹Ñ °‘•™•ÑÍ	å=Ý¹•Èé‘•™•ÑÌ¹É•‘Õ” ¡½ÕÐ°¥Ñ•´¤€ôø€¡ì€¸¸¹½ÕÐ°m¥Ñ•´¹½Ý¹•Étè¡½ÕÑm¥Ñ•´¹½Ý¹•Étñð€À¤€¬€Äô¤°íô¤ô°(€ôì)ô()•áÁ½ÉÐ½¹ÍÐ…Õ‘¥Ñ½¹ÑÉ…Ð€ô™É••é”¡ì(€Í¡•µ„éM!5°(€½¹ÑÉ…ÑY•ÉÍ¥½¸é=9QIQ}YIM%=8°(€…ÕÑ¡½É•‘…¥¹ÍÑM½ÕÉ”éUQ!=I}%9MQ}M=UI°(€¡•­Á½¥¹ÑM•µ…¹Ñ¥Ìè9=9}Q%9œ°(€…¹½¹¥…±ÁXÁ5…ÑÉ¥àé9=9%1}Á}XÁ}5QI%`°(€½Ý¹•ÉÌé=]9IL°(€ÁÕ‰±¥Í¡•‘Q…É•ÑÌéAU	1%M!}QIQL°(€Ù¥•ÝÁ½ÉÑÌéY%]A=IQL°(€Í•±•Ñ½ÉÌéM1Q=IL°(€•á•ÕÑ¥½¹	½Õ¹‘…Éäé™É••é”¡ìÍÑ…ÉÑÍ	É½ÝÍ•Èé™…±Í”°‰É½ÝÍ•Éá•ÕÑ½Èèµäµ‰É½ÝÍ•Èµ‰É¥‘”µ½Èµ•á¥ÍÑ¥¹œµÉ•±•…Í”µ…Ñ”œ°•á¥ÍÑ¥¹1½…±I•±•…Í•…Ñ”èÍ¥Ñ”½ÍÉ¥ÁÑÌ½¡•¬µ‰É½ÝÍ•ÈµÉ•±•…Í”µ…Ñ”¹µ©Ìœ°É•Á±…•Íá¥ÍÑ¥¹I•±•…Í•…Ñ”é™…±Í”°É•Á±…•Í…¹½¹¥…±ÁXÁ5…ÑÉ¥àé™…±Í”ô¤°(€™±½Ý½¹ÑÉ½°é™É••é”¡ì™ÈÁÕÑ½Ù•ÉI•ÅÕ¥É•‘	•™½É•É…µ¥¹]É¥Ñ•ÌéÑÉÕ”°™ÈÁ5ÕÍÑ9½Ñ•±…å±É•…‘åI•…‘åMÕ•ÍÍ½ÈéÑÉÕ”ô¤°(€…Ñ•Ìé™É••é”¡ì‘½Õµ•¹Ñ=Ù•É™±½ÝQ½±•É…¹•AàèÄ°½¹ÑÉ½±5¥¹¥µÕµAàèÐÐ°•ÅÕ…±!•¥¡ÑQ½±•É…¹•AàèÄ°™¥¹…±1¥¹•=ÕÁ…¹åQ½±•É…¹•AàèÄ°‘•Í­Ñ½Á9…Ù¥…Ñ¥½¹	É•…­Á½¥¹ÑAàèäàÄ°Í…™•	±…¹­I•±Q½­•¹Ìé™É••é”¡l¹½½Á•¹•Èœ°¹½É•™•ÉÉ•Èt¤°…¹½¹¥…±%‘•¹Ñ¥ÑåÑÑÉ¥‰ÕÑ•Ìé™É••é”¡l‘…Ñ„µ‘Ìµ™…µ¥±äœ°‘…Ñ„µ‘ÌµÙ•ÉÍ¥½¸œ°‘…Ñ„µ‘ÌµÙ…É¥…¹Ðœ°‘…Ñ„µ‘ÌµÍÑ…Ñ”t¤°µ•‘¥…É…µ•½¹ÑÉ…ÐèØÄœ°…‘…ÁÑ¥Ù•É¥‘1…å½ÕÑ¹¥¹”è™±•àµ±¥¹•Ìœ°…‘…ÁÑ¥Ù•É¥‘I•µ…¥¹‘•ÉA½±¥äèÍÑÉ•Ñ œô¤°)ô¤ì()½¹ÍÐ±…ÍÍ¥™å%¹‘•à€ôÁÉ½•ÍÌ¹…ÉØ¹¥¹‘•á=˜ œ´µ±…ÍÍ¥™äœ¤ì)¥˜€¡±…ÍÍ¥™å%¹‘•à€øô€À¤ì(€½¹ÍÐ¥¹ÁÕÑA…Ñ €ôÁÉ½•ÍÌ¹…ÉÙm±…ÍÍ¥™å%¹‘•à€¬€Åtì(€¥˜€ …¥¹ÁÕÑA…Ñ ñð¥¹ÁÕÑA…Ñ ¹ÍÑ…ÉÑÍ]¥Ñ  œ´´œ¤¤Ñ¡É½Ü¹•ÜÉÉ½È UÍ…”è¹½‘”ØÀµ‰É½ÝÍ•Èµ…Õ‘¥Ðµ½¹ÑÉ…Ð¹µ©Ì€´µ±…ÍÍ¥™ä€ñ½‰Í•ÉÙ…Ñ¥½¸¹©Í½¹ð´øœ¤ì(€½¹ÍÐÉ•ÍÕ±Ð€ô±…ÍÍ¥™å=‰Í•ÉÙ…Ñ¥½¸¡)M=8¹Á…ÉÍ”¡¥¹ÁÕÑA…Ñ €ôôô€œ´œ€üÉ•…‘¥±•Må¹Œ À°€ÕÑ˜àœ¤€èÉ•…‘¥±•Må¹Œ¡¥¹ÁÕÑA…Ñ °€ÕÑ˜àœ¤¤¤ì(€ÁÉ½•ÍÌ¹ÍÑ‘½ÕÐ¹ÝÉ¥Ñ”¡€‘í)M=8¹ÍÑÉ¥¹¥™ä¡É•ÍÕ±Ð°¹Õ±°°€È¥õq¹€¤ì(€ÁÉ½•ÍÌ¹•á¥Ñ½‘”€ôÉ•ÍÕ±Ð¹Ù•É‘¥Ð€ôôô€I%Pœ€ü€Ä€èÉ•ÍÕ±Ð¹Ù•É‘¥Ð€ôôô€%9=5A1Qœ€ü€È€è€Àì)ô•±Í”¥˜€¡ÁÉ½•ÍÌ¹…ÉØ¹¥¹±Õ‘•Ì œ´µ©Í½¸œ¤¤ÁÉ½•ÍÌ¹ÍÑ‘½ÕÐ¹ÝÉ¥Ñ”¡€‘í)M=8¹ÍÑÉ¥¹¥™ä¡…Õ‘¥Ñ½¹ÑÉ…Ð°¹Õ±°°€È¥õq¹€¤ì(