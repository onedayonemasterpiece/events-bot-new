import assert from 'node:assert/strict';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, '../..');

export const A0_CONSUMER_CLOSURE_PATHS = [
  'site/src/components/InterestClubCard.astro',
  'site/src/pages/kluby-po-interesam/[slug]/index.astro',
  'site/src/pages/festivali/index.astro',
  'site/src/components/ExhibitionsPersonalSurface.astro',
  'site/src/pages/fokus-gruppa/kollektsiya/index.astro',
  'site/src/pages/zakrytaya-afisha/index.astro',
];

const count = (source, value) => source.split(value).length - 1;

const replaceOnceIfPresent = (source, from, to, label = from) => {
  const occurrences = count(source, from);
  assert.ok(occurrences <= 1, `${label}: expected at most one pre-state occurrence, found ${occurrences}`);
  return occurrences === 1 ? source.replace(from, to) : source;
};

const replaceAllIfPresent = (source, from, to) => source.split(from).join(to);

const ensureImport = (source, importLine, afterLine) => {
  if (source.includes(importLine)) return source;
  assert.ok(source.includes(afterLine), `cannot insert ${importLine}; missing anchor ${afterLine}`);
  return source.replace(afterLine, `${afterLine}\n${importLine}`);
};

const ensureFoundationConsumer = (source, stateLine, consumer) => {
  const marker = `data-ke-foundation-consumer="${consumer}"`;
  if (source.includes(marker)) return source;
  assert.ok(source.includes(stateLine), `cannot add ${marker}; state line is missing`);
  return source.replace(stateLine, `${stateLine}\n    ${marker}`);
};

const replaceInsideRule = (source, selector, replacements) => {
  const open = `  ${selector} {`;
  const start = source.indexOf(open);
  assert.notEqual(start, -1, `missing CSS rule ${selector}`);
  const end = source.indexOf('\n  }', start + open.length);
  assert.notEqual(end, -1, `unterminated CSS rule ${selector}`);
  const endExclusive = end + '\n  }'.length;
  let block = source.slice(start, endExclusive);
  for (const [from, to] of replacements) block = replaceAllIfPresent(block, from, to);
  return `${source.slice(0, start)}${block}${source.slice(endExclusive)}`;
};

const transformInterestClubCard = (input) => {
  let source = input;
  const replacements = [
    ['rgba(255,255,255,.035)', 'var(--ke-color-club-card-fallback-orbit-ring-inner)'],
    ['rgba(255,255,255,.025)', 'var(--ke-color-club-card-fallback-orbit-ring-outer)'],
    ['rgba(255,255,255,.3)', 'var(--ke-color-club-card-fallback-orbit-line)'],
    ['rgba(255,255,255,.24)', 'var(--ke-color-club-card-fact-divider)'],
  ];
  for (const [from, to] of replacements) source = replaceAllIfPresent(source, from, to);
  return source;
};

const transformClubDetail = (input) => {
  let source = input;
  source = ensureImport(
    source,
    "import SemanticIcon from '../../../components/design-system/SemanticIcon.astro';",
    "import Breadcrumbs from '../../../components/Breadcrumbs.astro';",
  );
  source = ensureImport(
    source,
    "import '../../../components/design-system/product-contour-foundations.css';",
    "import SemanticIcon from '../../../components/design-system/SemanticIcon.astro';",
  );
  source = replaceOnceIfPresent(
    source,
    '<span aria-hidden="true">←</span> Все клубы',
    '<SemanticIcon name="arrow-left" role="inline" /> Все клубы',
    'club-detail back glyph',
  );

  const replacements = [
    ['.club-detail { display:grid; gap:1rem; }', '.club-detail { display:grid; gap:var(--ke-club-detail-gap); }'],
    ['.club-detail__hero { background:linear-gradient(135deg, #fffdf8, rgba(215,240,236,.7)); }', '.club-detail__hero { background:linear-gradient(135deg, var(--ke-color-club-detail-hero-start), var(--ke-color-club-detail-hero-end)); }'],
    ['.club-detail__hero h1 { max-width:19ch; font-size:clamp(2.2rem, 6vw, 4.8rem); line-height:.98; }', '.club-detail__hero h1 { max-width:var(--ke-club-detail-hero-heading-max); font-size:var(--ke-club-detail-hero-heading-size); line-height:var(--ke-club-detail-hero-heading-line); }'],
    ['.club-detail__facts { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:.65rem; margin:1.3rem 0 0; }', '.club-detail__facts { display:grid; grid-template-columns:var(--ke-club-detail-facts-columns); gap:var(--ke-club-detail-facts-gap); margin:var(--ke-club-detail-facts-margin-top) 0 0; }'],
    ['.club-detail__facts div { padding:.8rem; border:1px solid rgba(121,48,20,.11); border-radius:16px; background:rgba(255,255,255,.72); }', '.club-detail__facts div { padding:var(--ke-club-detail-fact-padding); border:var(--ke-shape-border-hairline) solid var(--ke-color-club-detail-fact-border); border-radius:var(--ke-club-detail-fact-radius); background:var(--ke-color-club-detail-fact-surface); }'],
    ['dt { color:var(--muted); font-size:.84rem; }', 'dt { color:var(--muted); font-size:var(--ke-club-detail-term-size); }'],
    ['dd { margin:.15rem 0 0; font-weight:850; overflow-wrap:anywhere; }', 'dd { margin:.15rem 0 0; font-weight:var(--ke-club-detail-value-weight); overflow-wrap:anywhere; }'],
    ['.meeting-list { display:grid; gap:.7rem; margin:1rem 0 0; padding:0; list-style:none; }', '.meeting-list { display:grid; gap:var(--ke-club-detail-meeting-gap); margin:1rem 0 0; padding:0; list-style:none; }'],
    ['.meeting-list li { display:grid; grid-template-columns:minmax(7rem, 10rem) minmax(0,1fr); gap:1rem; padding:1rem 0; border-top:1px solid rgba(121,48,20,.12); }', '.meeting-list li { display:grid; grid-template-columns:var(--ke-club-detail-meeting-columns); gap:var(--ke-club-detail-meeting-column-gap); padding:var(--ke-club-detail-meeting-padding-block) 0; border-top:var(--ke-shape-border-hairline) solid var(--ke-color-club-detail-meeting-border); }'],
    ['.meeting-list h3 { margin:0; font-size:1.2rem; }', '.meeting-list h3 { margin:0; font-size:var(--ke-club-detail-meeting-title-size); }'],
    ['.meeting-list h3 a { min-height:44px; display:inline-flex; align-items:center; color:var(--primary-strong); }', '.meeting-list h3 a { min-height:var(--ke-club-detail-action-min); display:inline-flex; align-items:center; color:var(--primary-strong); }'],
    ['.meeting-empty { padding:1rem; border-radius:18px; background:#fff8ed; }', '.meeting-empty { padding:var(--ke-club-detail-empty-padding); border-radius:var(--ke-club-detail-empty-radius); background:var(--ke-color-club-detail-empty-surface); }'],
    ['.club-detail__note { padding:1rem 1.15rem; border-left:4px solid var(--accent); border-radius:0 16px 16px 0; background:rgba(215,240,236,.58); }', '.club-detail__note { padding:var(--ke-club-detail-note-padding-block) var(--ke-club-detail-note-padding-inline); border-left:var(--ke-club-detail-note-border) solid var(--ke-color-club-detail-note-border); border-radius:var(--ke-club-detail-note-radius); background:var(--ke-color-club-detail-note-surface); }'],
    ['.club-detail__back { min-height:44px; display:inline-flex; align-items:center; gap:.4rem; color:var(--primary-strong); font-weight:900; }', '.club-detail__back { min-height:var(--ke-club-detail-action-min); display:inline-flex; align-items:center; gap:.4rem; color:var(--primary-strong); font-weight:900; }'],
  ];
  for (const [from, to] of replacements) source = replaceOnceIfPresent(source, from, to, `club-detail ${from}`);
  return source;
};

const transformFestivals = (input) => {
  let source = input;
  source = ensureImport(
    source,
    "import SemanticIcon from '../../components/design-system/SemanticIcon.astro';",
    "import Icon from '../../components/Icon.astro';",
  );
  source = ensureImport(
    source,
    "import '../../components/design-system/product-contour-foundations.css';",
    "import SemanticIcon from '../../components/design-system/SemanticIcon.astro';",
  );
  source = replaceAllIfPresent(source, '<Icon name="heart" />', '<SemanticIcon name="heart" role="control" />');
  if (!source.includes('<Icon ')) {
    source = replaceOnceIfPresent(source, "import Icon from '../../components/Icon.astro';\n", '', 'unused festival Icon import');
  }
  source = replaceOnceIfPresent(
    source,
    '<span class="festival-guide__icon" aria-hidden="true">↗</span>',
    '<span class="festival-guide__icon" aria-hidden="true"><SemanticIcon name="link" role="control" /></span>',
    'festival link guide glyph',
  );
  source = replaceOnceIfPresent(
    source,
    '<span class="festival-guide__icon" aria-hidden="true">＋</span>',
    '<span class="festival-guide__icon" aria-hidden="true"><SemanticIcon name="calendar" role="control" /></span>',
    'festival calendar guide glyph',
  );
  source = replaceOnceIfPresent(
    source,
    'rel={item.isExternal ? \'noreferrer\' : undefined}',
    'rel={item.isExternal ? \'noopener noreferrer\' : undefined}',
    'festival safe external relation',
  );

  if (!source.includes('data-ds-family="FestivalsTimelineRouteComposition"')) {
    source = replaceOnceIfPresent(
      source,
      '  <main id="main" class="festival-page" data-festival-timeline data-festival-count={resolvedItems.length}>',
      `  <main\n    id="main"\n    class="festival-page"\n    data-ds-family="FestivalsTimelineRouteComposition"\n    data-ds-version="1"\n    data-ds-variant="annual-timeline"\n    data-ds-state={resolvedItems.length > 0 ? 'populated' : 'empty'}\n    data-festival-timeline\n    data-festival-count={resolvedItems.length}\n  >`,
      'festival route identity',
    );
  }
  source = ensureFoundationConsumer(
    source,
    "    data-ds-state={resolvedItems.length > 0 ? 'populated' : 'empty'}",
    'festival-route',
  );

  source = replaceInsideRule(source, '.festival-guide__icon--heart', [
    ['background: rgba(165, 72, 33, 0.1);', 'background: var(--ke-color-festival-guide-like-surface);'],
  ]);
  source = replaceInsideRule(source, '.festival-month__categories li', [
    ['flex: 0 0 28px;', 'flex: 0 0 var(--ke-festival-category-container-size);'],
    ['width: 28px;', 'width: var(--ke-festival-category-container-size);'],
    ['height: 28px;', 'height: var(--ke-festival-category-container-size);'],
    ['background: rgba(165, 72, 33, 0.1);', 'background: var(--ke-color-festival-category-surface);'],
  ]);
  source = replaceInsideRule(source, '.festival-month__categories i', [
    ['width: 21px;', 'width: var(--ke-festival-category-asset-size);'],
    ['height: 21px;', 'height: var(--ke-festival-category-asset-size);'],
  ]);
  source = replaceInsideRule(source, '.festival-card__like', [
    ['width: clamp(2rem, 2.35vw, 2.2rem);', 'width: var(--ke-festival-like-target-min);'],
    ['height: clamp(2rem, 2.35vw, 2.2rem);', 'height: var(--ke-festival-like-target-min);'],
    ['border: 1px solid rgba(255, 255, 255, 0.34);', 'border: var(--ke-shape-border-hairline) solid var(--ke-color-festival-like-line);'],
    ['background: rgba(18, 14, 12, 0.58);', 'background: var(--ke-color-festival-like-surface);'],
    ['box-shadow: 0 3px 12px rgba(0, 0, 0, 0.22);', 'box-shadow: var(--ke-elevation-festival-like);'],
  ]);

  const replacements = [
    ['width: min(1240px, calc(100% - 2rem));', 'width: min(var(--ke-festival-page-max), calc(100% - 2 * var(--ke-festival-page-gutter)));'],
    ['padding: clamp(0.85rem, 1.7vw, 1.65rem) 0 clamp(5.5rem, 8vw, 7rem);', 'padding: var(--ke-festival-page-padding-start) 0 var(--ke-festival-page-padding-end);'],
    ['grid-template-columns: minmax(0, 1.35fr) minmax(360px, 0.65fr);', 'grid-template-columns: var(--ke-festival-hero-columns);'],
    ['gap: clamp(1.5rem, 3.5vw, 3.5rem);', 'gap: var(--ke-festival-hero-gap);'],
    ['padding: clamp(1.35rem, 2.15vw, 2rem);', 'padding: var(--ke-festival-hero-padding);'],
    ['border: 1px solid rgba(121, 48, 20, 0.13);', 'border: var(--ke-shape-border-hairline) solid var(--ke-color-festival-hero-border);'],
    ['border-radius: clamp(24px, 3vw, 36px);', 'border-radius: var(--ke-festival-hero-radius);'],
    ['radial-gradient(circle at 82% 22%, rgba(15, 118, 110, 0.16), transparent 19rem),\n      linear-gradient(135deg, #fffdf8, #f0e3d2)', 'radial-gradient(circle at 82% 22%, var(--ke-color-festival-hero-glow), transparent 19rem),\n      linear-gradient(135deg, var(--ke-color-festival-hero-surface-start), var(--ke-color-festival-hero-surface-end))'],
    ['box-shadow: 0 24px 64px rgba(72, 45, 25, 0.1);', 'box-shadow: var(--ke-elevation-festival-hero);'],
    ['width: 27rem;\n    height: 27rem;\n    border: 4.5rem solid rgba(165, 72, 33, 0.06);', 'width: var(--ke-festival-hero-orbit-size);\n    height: var(--ke-festival-hero-orbit-size);\n    border: var(--ke-festival-hero-orbit-border) solid var(--ke-color-festival-hero-orbit);'],
    ['font-size: 0.68rem;\n    font-weight: 850;\n    letter-spacing: 0.12em;', 'font-size: var(--ke-festival-eyebrow-size);\n    font-weight: var(--ke-festival-eyebrow-weight);\n    letter-spacing: var(--ke-festival-eyebrow-letter);'],
    ['font-size: clamp(2.4rem, 3.25vw, 3.6rem);\n    font-weight: 850;\n    letter-spacing: -0.055em;\n    line-height: 0.94;', 'font-size: var(--ke-festival-heading-size);\n    font-weight: var(--ke-festival-heading-weight);\n    letter-spacing: var(--ke-festival-heading-letter);\n    line-height: var(--ke-festival-heading-line);'],
    ['font-size: clamp(0.88rem, 1.05vw, 0.98rem);\n    line-height: 1.42;', 'font-size: var(--ke-festival-copy-size);\n    line-height: var(--ke-festival-copy-line);'],
    ['border-radius: 18px;\n    background: rgba(255, 253, 248, 0.76);\n    box-shadow: 0 12px 34px rgba(72, 45, 25, 0.07);', 'border-radius: var(--ke-festival-guide-radius);\n    background: var(--ke-color-festival-surface-translucent);\n    box-shadow: var(--ke-elevation-festival-guide);'],
    ['grid-template-columns: 1.9rem minmax(0, 1fr);', 'grid-template-columns: var(--ke-festival-guide-icon-container-size) minmax(0, 1fr);'],
    ['width: 1.9rem;\n    height: 1.9rem;', 'width: var(--ke-festival-guide-icon-container-size);\n    height: var(--ke-festival-guide-icon-container-size);'],
    ['background: rgba(15, 118, 110, 0.09);\n    color: #0f766e;\n    font-size: 0.95rem;', 'background: var(--ke-color-festival-guide-icon-surface);\n    color: var(--ke-color-festival-guide-icon);\n    font-size: var(--ke-festival-guide-icon-size);'],
    ['z-index: 60;', 'z-index: var(--ke-festival-live-layer);'],
    ['border-radius: 12px;\n    background: rgba(255, 253, 248, 0.97);\n    box-shadow: 0 14px 40px rgba(36, 24, 16, 0.2);', 'border-radius: var(--ke-festival-live-radius);\n    background: var(--ke-color-festival-surface-raised);\n    box-shadow: var(--ke-elevation-festival-live);'],
    ['grid-template-columns: 132px minmax(0, 1fr);', 'grid-template-columns: var(--ke-festival-month-rail-width) minmax(0, 1fr);'],
    ['width: 0.82rem;\n    height: 0.82rem;', 'width: var(--ke-festival-month-marker-size);\n    height: var(--ke-festival-month-marker-size);'],
    ['gap: clamp(0.62rem, 1vw, 0.88rem);', 'gap: var(--ke-festival-row-gap);'],
    ['margin-top: clamp(0.62rem, 1vw, 0.88rem);', 'margin-top: var(--ke-festival-row-gap);'],
    ['border: 1px solid rgba(57, 39, 27, 0.08);', 'border: var(--ke-shape-border-hairline) solid var(--ke-color-festival-card-border);'],
    ['border-radius: clamp(8px, 0.78vw, 11px);', 'border-radius: var(--ke-festival-card-radius);'],
    ['background: #31261f;\n    box-shadow: 0 5px 16px rgba(52, 32, 18, 0.12);', 'background: var(--ke-color-festival-card-surface);\n    box-shadow: var(--ke-elevation-festival-card);'],
    ['transition: border-color 160ms ease, box-shadow 160ms ease;', 'transition: border-color var(--ke-festival-motion-fast), box-shadow var(--ke-festival-motion-fast);'],
    ['border-color: rgba(57, 39, 27, 0.16);\n    box-shadow: 0 7px 20px rgba(52, 32, 18, 0.16);', 'border-color: var(--ke-color-festival-card-border-hover);\n    box-shadow: var(--ke-elevation-festival-card-hover);'],
    ['border-color: rgba(255, 255, 255, 0.72);\n    background: rgba(18, 14, 12, 0.76);', 'border-color: var(--ke-color-festival-like-line-hover);\n    background: var(--ke-color-festival-like-surface-hover);'],
    ['border-color: rgba(255, 255, 255, 0.86);', 'border-color: var(--ke-color-festival-like-line-selected);'],
    ['background: #e8f5e9;\n    color: #24743b;\n    box-shadow: 0 2px 7px rgba(34, 20, 12, 0.12);', 'background: var(--ke-color-festival-status-confirmed-surface);\n    color: var(--ke-color-festival-status-confirmed-text);\n    box-shadow: var(--ke-elevation-festival-status);'],
    ['background: #fff1bd;\n    color: #76540a;', 'background: var(--ke-color-festival-status-pending-surface);\n    color: var(--ke-color-festival-status-pending-text);'],
    ['color: rgba(255, 255, 255, 0.94);', 'color: var(--ke-color-festival-card-caption);'],
    ['text-shadow: 0 1px 8px rgba(0, 0, 0, 0.36);', 'text-shadow: var(--ke-elevation-festival-caption-copy);'],
    ['text-shadow: 0 2px 14px rgba(0, 0, 0, 0.4);', 'text-shadow: var(--ke-elevation-festival-caption-heading);'],
    ['background: rgba(15, 15, 15, 0.66);\n    color: rgba(255, 255, 255, 0.96);', 'background: var(--ke-color-festival-card-theme-surface);\n    color: var(--ke-color-festival-card-theme-text);'],
    ['.festival-card__like { top: auto; right: 0.65rem; bottom: 0.65rem; width: 1.8rem; height: 1.8rem; }', '.festival-card__like { top: auto; right: 0.65rem; bottom: 0.65rem; width: var(--ke-festival-like-target-min); height: var(--ke-festival-like-target-min); }'],
  ];
  for (const [from, to] of replacements) source = replaceAllIfPresent(source, from, to);
  source = replaceOnceIfPresent(
    source,
    '  .festival-guide__icon :global(svg) { width: 0.95rem; height: 0.95rem; fill: currentColor; }\n',
    '',
    'festival guide local icon square',
  );
  source = replaceOnceIfPresent(
    source,
    '  .festival-card__like :global(svg) { width: 1.12rem; height: 1.12rem; fill: currentColor; }\n',
    '',
    'festival favorite local icon square',
  );
  return source;
};

const transformExhibitions = (input) => {
  let source = input;
  source = ensureImport(
    source,
    "import './design-system/product-contour-foundations.css';",
    "import ExhibitionPrototypeRow from './ExhibitionPrototypeRow.astro';",
  );
  if (!source.includes('data-ds-family="ExhibitionsPersonalSurface"')) {
    source = replaceOnceIfPresent(
      source,
      '<main id="main" class="ex-page" data-exhibitions-prototype>',
      `<main\n  id="main"\n  class="ex-page"\n  data-ds-family="ExhibitionsPersonalSurface"\n  data-ds-version="1"\n  data-ds-variant="ranked-personal-catalog"\n  data-ds-state={newItems.length + priorityItems.length + tailItems.length > 0 ? 'personal all-topics populated' : 'personal all-topics empty'}\n  data-exhibitions-prototype\n>`,
      'exhibitions route identity',
    );
  }
  source = ensureFoundationConsumer(
    source,
    "  data-ds-state={newItems.length + priorityItems.length + tailItems.length > 0 ? 'personal all-topics populated' : 'personal all-topics empty'}",
    'exhibitions-personal-surface',
  );

  const aliases = {
    '--ex-bg': '--ke-color-exhibitions-background',
    '--ex-surface': '--ke-color-exhibitions-surface',
    '--ex-raised': '--ke-color-exhibitions-raised',
    '--ex-border': '--ke-color-exhibitions-border',
    '--ex-text': '--ke-color-exhibitions-text',
    '--ex-muted': '--ke-color-exhibitions-muted',
    '--ex-blue': '--ke-color-exhibitions-blue',
    '--ex-orange': '--ke-color-exhibitions-orange',
    '--ex-red': '--ke-color-exhibitions-red',
    '--ex-gray': '--ke-color-exhibitions-gray',
    '--ex-yellow': '--ke-color-exhibitions-yellow',
    '--ex-purple': '--ke-color-exhibitions-purple',
    '--ex-green': '--ke-color-exhibitions-green',
    '--ex-ease-cinematic': '--ke-exhibitions-ease-cinematic',
    '--ex-ease-emphasis': '--ke-exhibitions-ease-emphasis',
    '--ex-motion-fast': '--ke-exhibitions-motion-fast',
    '--ex-motion-base': '--ke-exhibitions-motion-base',
  };
  for (const [from, to] of Object.entries(aliases)) {
    source = replaceAllIfPresent(source, `var(${from})`, `var(${to})`);
  }
  source = source.replace(
    /\n  :root \{\n[\s\S]*?--ex-motion-base:420ms;\n  \}\n(?=  body:has\(\.ex-page\))/u,
    '\n',
  );

  const replacements = [
    ['color:var(--ke-color-exhibitions-text); font-size:16px; line-height:1.45;', 'color:var(--ke-color-exhibitions-text); font-size:var(--ke-exhibitions-page-font-size); line-height:var(--ke-exhibitions-page-line);'],
    ['width:min(1320px,calc(100% - clamp(2rem,5vw,6rem))); margin:0 auto; padding:clamp(2rem,5vw,4.8rem) 0 6rem;', 'width:min(var(--ke-exhibitions-shell-max),calc(100% - var(--ke-exhibitions-shell-gutter))); margin:0 auto; padding:var(--ke-exhibitions-shell-padding-start) 0 var(--ke-exhibitions-shell-padding-end);'],
    ['min-height:76px;', 'min-height:var(--ke-exhibitions-controls-min-height);'],
    ['border-color:#697078;', 'border-color:var(--ke-color-exhibitions-category-selected-border);'],
    ['background:#24272a;', 'background:var(--ke-color-exhibitions-category-selected-surface);'],
    ['border:1px solid #3c4247;', 'border:1px solid var(--ke-color-exhibitions-keyboard-help-border);'],
    ['background:linear-gradient(145deg,#15181a,#111315);', 'background:linear-gradient(145deg,var(--ke-color-exhibitions-keyboard-help-start),var(--ke-color-exhibitions-keyboard-help-end));'],
    ['border:1px solid #52585d;', 'border:1px solid var(--ke-color-exhibitions-action-border);'],
    ['background:#131619;', 'background:var(--ke-color-exhibitions-action-hover-surface);'],
    ['text-decoration-color:rgba(168,173,178,.35);', 'text-decoration-color:var(--ke-color-exhibitions-action-underline);'],
    ['min-height:132px;', 'min-height:var(--ke-exhibitions-row-min-height);'],
  ];
  for (const [from, to] of replacements) source = replaceAllIfPresent(source, from, to);
  return source;
};

const transformFocusCollection = (input) => {
  let source = input;
  if (!source.includes('data-ds-family="FocusEggCollectionRouteComposition"')) {
    source = replaceOnceIfPresent(
      source,
      '    <main id="main" class="focus-collection" data-focus-collection>',
      `    <main\n      id="main"\n      class="focus-collection"\n      data-ds-family="FocusEggCollectionRouteComposition"\n      data-ds-version="1"\n      data-ds-variant="collection-prototype"\n      data-ds-state={\`found-\${collectionProgress.found}-of-\${collectionProgress.eligible}\`}\n      data-focus-collection\n    >`,
      'focus collection identity',
    );
  }
  if (!source.includes('root.dataset.dsState = `found-${found}-of-${eligible}`;')) {
    source = replaceOnceIfPresent(
      source,
      `      const foundOutput = root.querySelector<HTMLElement>('[data-collection-found]');`,
      `      root.dataset.dsState = \`found-\${found}-of-\${eligible}\`;\n      const foundOutput = root.querySelector<HTMLElement>('[data-collection-found]');`,
      'focus collection runtime state',
    );
  }
  return source;
};

const transformClosedFocusHub = (input) => {
  let source = input;
  if (!source.includes('data-ds-family="ClosedFocusHubRouteComposition"')) {
    source = replaceOnceIfPresent(
      source,
      '    <main id="main" class="focus-secret" data-focus-secret data-root-href={withBase(\'/\')}>',
      `    <main\n      id="main"\n      class="focus-secret"\n      data-ds-family="ClosedFocusHubRouteComposition"\n      data-ds-version="1"\n      data-ds-variant="participant-hub"\n      data-ds-state="checking"\n      data-focus-secret\n      data-root-href={withBase('/')}\n    >`,
      'closed focus hub identity',
    );
  }
  if (!source.includes("root.dataset.dsState = marker?.status === 'active' ? 'available' : 'locked';")) {
    source = replaceOnceIfPresent(
      source,
      '    if (checking) checking.hidden = true;\n',
      "    if (checking) checking.hidden = true;\n    root.dataset.dsState = marker?.status === 'active' ? 'available' : 'locked';\n",
      'closed focus hub runtime state',
    );
  }
  return source;
};

const transformers = new Map([
  ['site/src/components/InterestClubCard.astro', transformInterestClubCard],
  ['site/src/pages/kluby-po-interesam/[slug]/index.astro', transformClubDetail],
  ['site/src/pages/festivali/index.astro', transformFestivals],
  ['site/src/components/ExhibitionsPersonalSurface.astro', transformExhibitions],
  ['site/src/pages/fokus-gruppa/kollektsiya/index.astro', transformFocusCollection],
  ['site/src/pages/zakrytaya-afisha/index.astro', transformClosedFocusHub],
]);

export function transformA0Consumer(path, source) {
  const transform = transformers.get(path);
  assert.ok(transform, `unsupported A0 consumer path: ${path}`);
  return transform(source);
}

export function assertA0ConsumerPostconditions(path, source) {
  switch (path) {
    case 'site/src/components/InterestClubCard.astro':
      for (const raw of ['rgba(255,255,255,.035)', 'rgba(255,255,255,.025)', 'rgba(255,255,255,.3)', 'rgba(255,255,255,.24)']) {
        assert.ok(!source.includes(raw), `${path} retains ${raw}`);
      }
      for (const token of ['fallback-orbit-ring-inner', 'fallback-orbit-ring-outer', 'fallback-orbit-line', 'fact-divider']) {
        assert.ok(source.includes(`--ke-color-club-card-${token}`), `${path} misses ${token}`);
      }
      break;
    case 'site/src/pages/kluby-po-interesam/[slug]/index.astro':
      assert.match(source, /import SemanticIcon/u);
      assert.match(source, /product-contour-foundations\.css/u);
      assert.match(source, /<SemanticIcon name="arrow-left" role="inline" \/>/u);
      for (const token of ['--ke-color-club-detail-hero-start', '--ke-color-club-detail-note-surface', '--ke-club-detail-action-min']) {
        assert.ok(source.includes(`var(${token})`), `${path} misses ${token}`);
      }
      assert.ok(!source.includes('<span aria-hidden="true">←</span>'), `${path} retains text arrow`);
      break;
    case 'site/src/pages/festivali/index.astro':
      assert.match(source, /data-ds-family="FestivalsTimelineRouteComposition"/u);
      assert.match(source, /data-ke-foundation-consumer="festival-route"/u);
      assert.match(source, /product-contour-foundations\.css/u);
      assert.ok(count(source, '<SemanticIcon name="heart" role="control" />') >= 2, `${path} must use semantic heart twice`);
      for (const token of [
        '--ke-festival-page-max',
        '--ke-festival-hero-radius',
        '--ke-festival-guide-icon-size',
        '--ke-festival-like-target-min',
        '--ke-color-festival-guide-like-surface',
        '--ke-color-festival-category-surface',
      ]) assert.ok(source.includes(`var(${token})`), `${path} misses ${token}`);
      for (const forbidden of [
        '<Icon name="heart" />',
        '.festival-guide__icon :global(svg) { width: 0.95rem; height: 0.95rem;',
        'width: clamp(2rem, 2.35vw, 2.2rem);',
        'height: clamp(2rem, 2.35vw, 2.2rem);',
      ]) assert.ok(!source.includes(forbidden), `${path} retains ${forbidden}`);
      assert.match(source, /\.festival-guide__icon--heart \{[\s\S]*background: var\(--ke-color-festival-guide-like-surface\);/u);
      assert.match(source, /\.festival-month__categories li \{[\s\S]*background: var\(--ke-color-festival-category-surface\);/u);
      assert.ok(source.includes("rel={item.isExternal ? 'noopener noreferrer' : undefined}"), `${path} misses safe external rel`);
      break;
    case 'site/src/components/ExhibitionsPersonalSurface.astro':
      assert.match(source, /data-ds-family="ExhibitionsPersonalSurface"/u);
      assert.match(source, /data-ke-foundation-consumer="exhibitions-personal-surface"/u);
      assert.match(source, /product-contour-foundations\.css/u);
      assert.ok(!source.includes('--ex-'), `${path} retains private --ex-* ownership`);
      for (const token of ['--ke-color-exhibitions-background', '--ke-color-exhibitions-text', '--ke-exhibitions-motion-base', '--ke-exhibitions-shell-max']) {
        assert.ok(source.includes(`var(${token})`), `${path} misses ${token}`);
      }
      break;
    case 'site/src/pages/fokus-gruppa/kollektsiya/index.astro':
      assert.match(source, /data-ds-family="FocusEggCollectionRouteComposition"/u);
      assert.match(source, /data-ds-version="1"/u);
      assert.match(source, /data-ds-variant="collection-prototype"/u);
      assert.match(source, /data-ds-state=\{`found-\$\{collectionProgress\.found\}-of-\$\{collectionProgress\.eligible\}`\}/u);
      assert.match(source, /root\.dataset\.dsState = `found-\$\{found\}-of-\$\{eligible\}`;/u);
      break;
    case 'site/src/pages/zakrytaya-afisha/index.astro':
      assert.match(source, /data-ds-family="ClosedFocusHubRouteComposition"/u);
      assert.match(source, /data-ds-version="1"/u);
      assert.match(source, /data-ds-variant="participant-hub"/u);
      assert.match(source, /data-ds-state="checking"/u);
      assert.match(source, /marker\?\.status === 'active' \? 'available' : 'locked'/u);
      break;
    default:
      assert.fail(`unverified A0 consumer path: ${path}`);
  }
}

const assertIntegratedF0Inputs = () => {
  const required = [
    'site/src/components/design-system/route-theme-foundations.css',
    'site/src/components/design-system/f0-route-theme-bindings.v1.json',
    'site/src/components/design-system/interest-club-card-continuity-foundations.css',
    'site/src/components/design-system/f0-interest-club-theme-decision.v1.json',
    'site/src/components/design-system/f0-festival-semantic-separation.v1.json',
  ];
  for (const path of required) assert.ok(existsSync(resolve(repoRoot, path)), `missing integrated F0 input ${path}`);
  const productContour = readFileSync(resolve(repoRoot, 'site/src/components/design-system/product-contour-foundations.css'), 'utf8');
  assert.ok(productContour.includes('@import "./route-theme-foundations.css";'), 'product contour misses route theme import');
  assert.ok(productContour.includes('@import "./interest-club-card-continuity-foundations.css";'), 'product contour misses club continuity import');
  const routeThemes = readFileSync(resolve(repoRoot, 'site/src/components/design-system/route-theme-foundations.css'), 'utf8');
  assert.ok(routeThemes.includes('--ke-color-festival-guide-like-surface:'), 'missing festival guide action surface');
  assert.ok(routeThemes.includes('--ke-color-festival-category-surface:'), 'missing festival taxonomy surface');
};

export async function runA0ConsumerClosure({ checkOnly = false } = {}) {
  assertIntegratedF0Inputs();
  const results = [];
  for (const path of A0_CONSUMER_CLOSURE_PATHS) {
    const absolute = resolve(repoRoot, path);
    const before = readFileSync(absolute, 'utf8');
    const after = transformA0Consumer(path, before);
    assertA0ConsumerPostconditions(path, after);
    assert.equal(transformA0Consumer(path, after), after, `${path} transform is not idempotent`);
    if (!checkOnly && before !== after) writeFileSync(absolute, after);
    results.push({ path, changed: before !== after, mode: checkOnly ? 'check' : 'apply' });
  }
  const receipt = {
    schema: 'kenigevents.a0-current-successor-consumer-closure.v1',
    contract: 'launch-normalized-ui.v1@1.10.0',
    festival_semantic_separation: 'guide-like-action != editorial-category-taxonomy',
    check_only: checkOnly,
    targets: results,
    m0_fr0_or_f0_roots_mutated: false,
  };
  console.log(JSON.stringify(receipt, null, 2));
  return receipt;
}

const invokedAsScript = process.argv[1]
  && pathToFileURL(resolve(process.argv[1])).href === import.meta.url;
if (invokedAsScript) {
  runA0ConsumerClosure({ checkOnly: process.argv.includes('--check') }).catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
