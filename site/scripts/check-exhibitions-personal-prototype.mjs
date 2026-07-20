import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';

const siteRoot = path.resolve(import.meta.dirname, '..');
const pageSource = fs.readFileSync(path.join(siteRoot, 'src/pages/lab/exhibitions-personal/index.astro'), 'utf8');
const rowSource = fs.readFileSync(path.join(siteRoot, 'src/components/ExhibitionPrototypeRow.astro'), 'utf8');
const iconSource = fs.readFileSync(path.join(siteRoot, 'src/components/Icon.astro'), 'utf8');
const layoutSource = fs.readFileSync(path.join(siteRoot, 'src/layouts/EventLayout.astro'), 'utf8');
const outputPath = path.join(siteRoot, 'dist/lab/exhibitions-personal/index.html');

if (!fs.existsSync(outputPath)) throw new Error(`Build output is missing: ${outputPath}`);
const html = fs.readFileSync(outputPath, 'utf8');
const eventIds = [...html.matchAll(/<article[^>]+data-exhibition-row[^>]+data-event-id="(\d+)"/gu)].map((match) => match[1]);
const uniqueIds = new Set(eventIds);

const checks = [
  ['12 curated exhibition rows', eventIds.length === 12],
  ['no duplicate event rows', uniqueIds.size === eventIds.length],
  ['3 new inbox rows', (html.match(/data-new="true"/gu) || []).length === 3],
  ['personal mode precedes all mode', html.indexOf('data-mode="personal"') < html.indexOf('data-mode="all"')],
  ['new inbox precedes priority and tail', html.indexOf('data-list-section="new"') < html.indexOf('data-list-section="priority"') && html.indexOf('data-list-section="priority"') < html.indexOf('data-list-section="tail"')],
  ['tail is progressively disclosed', /data-tail-toggle[^>]+aria-expanded="false"/u.test(html) && /data-list-section="tail" hidden/u.test(html)],
  ['mode switch is a complete radio group', /role="radiogroup"/u.test(html) && /role="radio"/u.test(html) && pageSource.includes("['ArrowLeft','ArrowRight','Home','End']")],
  ['keyboard safeguards editable controls', pageSource.includes("input,textarea,select,[contenteditable=\"true\"]") && pageSource.includes("target?.closest('button')")],
  ['like and reject expose pressed state', rowSource.includes('data-aggregate-count={String(likeCount)} aria-pressed="false"') && rowSource.includes('data-reject aria-pressed="false"')],
  ['rejection keeps an undo stub', rowSource.includes('data-hidden-stub') && rowSource.includes('data-undo')],
  ['gallery uses native modal dialog', /<dialog[^>]+data-gallery/u.test(html) && pageSource.includes('showModal()')],
  ['shared header is selected for exhibitions', pageSource.includes('headerCurrent="exhibitions"') && !pageSource.includes('<header class="ex-header"')],
  ['shared header owns the responsive badge', pageSource.includes("headerBadge={{ key: 'exhibitions'") && layoutSource.includes('data-header-badge={item.key}') && html.includes('data-header-badge="exhibitions"') && html.includes('3 новых')],
  ['responsive and reduced-motion contracts exist', pageSource.includes('@media (max-width:820px)') && pageSource.includes('@media (prefers-reduced-motion:reduce)')],
  ['mobile uses the shared immersive discovery drawer', layoutSource.includes('data-mobile-discovery-menu') && pageSource.includes('heroChrome="immersive"') && !pageSource.includes('>.site-header .site-nav { display:none; }')],
  ['shared mobile navigation exposes current section for badge extension', layoutSource.includes("aria-current={headerBadge && headerCurrent === 'exhibitions' ? 'page' : undefined}")],
  ['photo deck keeps real source geometry and responsive sources', rowSource.includes('width={asset.width}') && rowSource.includes('height={asset.height}') && rowSource.includes('thumbnailSrcset')],
  ['smart crop is evidence-gated and uses named wide tokens', rowSource.includes("const ratioTokens = { P: 4 / 5, S: 1, W: 4 / 3, L: 3 / 2 }") && rowSource.includes("asset.media_role === 'event_photo'") && rowSource.includes("asset.safe_crop === true") && rowSource.includes("'document-natural'")],
  ['smart crop selects the nearest named token instead of forcing 4:3 photos into 3:2', rowSource.includes('Math.sqrt(ratioTokens.W * ratioTokens.L)') && !rowSource.includes("sourceRatio >= 1.2 ? 'L' : 'W'")],
  ['crop decisions are inspectable in markup', rowSource.includes('data-media-token={asset.token}') && rowSource.includes('data-crop-loss={asset.cropLoss.toFixed(5)}') && rowSource.includes('data-crop-reason={asset.cropReason}')],
  ['bounded deck is edge-to-edge without contain fields', pageSource.includes('.ex-deck__images {') && pageSource.includes('inset:0;') && pageSource.includes('object-fit:cover') && !/\.ex-deck__frame img\s*\{[^}]*object-fit:contain/su.test(pageSource)],
  ['deck uses real conditional right-overlap frames', !rowSource.includes('ex-deck__edge-stack') && rowSource.includes('data-deck-frame') && pageSource.includes("frame.dataset.deckState = 'stack'") && pageSource.includes('rightEdge - depthWidth') && pageSource.includes('String(120 - depth)')],
  ['desktop rows reserve one stable media column without dynamic deck width', pageSource.includes('--ex-media-column:clamp(420px,42vw,680px)') && pageSource.includes('grid-template-columns:112px var(--ex-media-column)') && pageSource.includes('const available = deck.clientWidth') && !pageSource.includes('deck.style.inlineSize =')],
  ['deck depth uses shrinking directional planes while hover remains geometrically static', pageSource.includes('const depthScale = 1 / (1 + depth * .05)') && pageSource.includes('frame.style.blockSize = `${depthHeight}px`') && pageSource.includes('calc(7px - var(--deck-depth,1) * .8px)') && !pageSource.includes('scale(1.04)')],
  ['deck depth progressively grades real images', pageSource.includes("'--deck-saturation'") && pageSource.includes("'--deck-brightness'") && pageSource.includes("'--deck-blur'") && pageSource.includes('.ex-deck__frame[data-deck-state="stack"] img')],
  ['sixth deck plane is neutral and contains no image', rowSource.includes("data-deck-visual={index >= 5 ? 'depth-tail' : 'media'}") && rowSource.includes('index < 5 ? (') && rowSource.includes('ex-deck__depth-plane')],
  ['long desktop rows stretch media height without changing column width', pageSource.includes('min-block-size:114px') && pageSource.includes('block-size:auto') && pageSource.includes('new ResizeObserver(scheduleDeckLayout)') && pageSource.includes('document.fonts?.ready.then(scheduleDeckLayout)')],
  ['discussion marker is outside title flow in the fixed aside', rowSource.indexOf('<div class="ex-row__aside">') < rowSource.indexOf('<span class="ex-discussed"') && rowSource.indexOf('<span class="ex-discussed"') > rowSource.indexOf('</div>\n\n  <div class="ex-row__aside">') && pageSource.includes('.ex-row__aside { width:98px;')],
  ['timeline rail is visually outside the row surface', rowSource.includes('ex-row__dot') && pageSource.includes('--ex-surface-start:calc(112px') && pageSource.includes('inset:0 0 0 var(--ex-surface-start)') && pageSource.includes('.ex-row__rail::after') && !pageSource.includes('.ex-row::before { content:')],
  ['timeline connector is short and cannot cross date copy', pageSource.includes('left:-14px; width:20px; height:2px') && pageSource.includes('.ex-row__rail>* { position:relative; z-index:1; }')],
  ['rejected stub is clipped to the exhibition surface', pageSource.includes('.ex-row.is-rejected { min-height:132px; background:transparent; }') && pageSource.includes('.ex-row__hidden { position:absolute; inset:0 0 0 var(--ex-surface-start)') && pageSource.includes('--ex-surface-start:calc(95px')],
  ['mobile removes keyboard affordances', pageSource.includes('.ex-page kbd,.ex-help-open,.ex-keyboard-help { display:none!important; }')],
  ['media and title are honest matching detail links without a painted gallery trigger', rowSource.includes('class="ex-deck"\n    href={detailHref}\n    data-deck\n    data-gallery-open') && rowSource.includes('class="ex-row__title"\n        href={detailHref}') && !rowSource.includes('class="ex-gallery-trigger"') && !pageSource.includes('.ex-gallery-trigger')],
  ['desktop media gallery preserves native mobile and modified-click navigation', pageSource.includes('const galleryOpener = target.closest(\'[data-gallery-open]\')') && pageSource.includes('!compactDeck.matches && !modifiedClick') && pageSource.includes('event.preventDefault();\n          openGallery(galleryOpener,row);')],
  ['social signals use real shares, qualitative discussion and one aggregate like count', (rowSource.match(/data-like-count/gu) || []).length === 1 && rowSource.includes('const shareCount = Number(event.shares_count || 0)') && rowSource.includes("popularity_reason_codes.includes('discussed')") && rowSource.includes('<Icon name="share" />') && rowSource.includes('<Icon name="comment" />') && !rowSource.includes('item.discussions') && !rowSource.includes('item.mentions') && !iconSource.includes("| 'mention'")],
  ['mobile timeline dot and connector compensate for row padding', pageSource.includes('left:calc(-17.5px - .65rem)') && pageSource.includes('left:calc(-10.5px - .65rem); width:calc(10.5px + .65rem)')],
  ['deck images expose skeleton, cached-load, and error lifecycle', rowSource.includes('data-image-state={index >= 5') && rowSource.includes('data-image-skeleton') && pageSource.includes('if (image.complete) finish(image.naturalWidth > 0') && pageSource.includes("finish('error')")],
  ['gallery image loading is skeleton-backed without empty src', !pageSource.includes('data-gallery-image src=""') && pageSource.includes('data-gallery-media data-image-state="idle"') && pageSource.includes("galleryMedia.dataset.imageState = 'loading'") && pageSource.includes("galleryMedia.dataset.imageState = state")],
  ['deck counter means actual non-full media', rowSource.includes('data-media-total') && pageSource.includes('total - fullCount') && pageSource.includes('count.textContent = `+${overflowCount}`')],
  ['row halo has hover and keyboard parity', rowSource.includes('ex-row__halo') && rowSource.includes('ex-row__edge-light') && pageSource.includes('.ex-row:focus-within .ex-row__halo')],
  ['hover/focus changes light without geometric movement', !pageSource.includes('transform:translate3d(0,-2px,0) scale') && !pageSource.includes('.ex-row:hover .ex-deck__frame img') && pageSource.includes('.ex-row:hover .ex-deck,.ex-row:focus-within .ex-deck')],
  ['metadata emphasis avoids movement and grid-row reflow', pageSource.includes('.ex-row__expanded { display:grid; opacity:') && !pageSource.includes('.ex-row:hover .ex-row__expanded,.ex-row:focus-within .ex-row__expanded { opacity:1; transform:') && !pageSource.includes('transition:grid-template-rows')],
  ['cinematic motion and reduced-motion scrolling exist', pageSource.includes('cubic-bezier(.16,1,.3,1)') && pageSource.includes("behavior:reducedMotion.matches ? 'auto' : 'smooth'")],
  ['reduced motion keeps positioning transforms intact', !pageSource.includes('animation:none!important; transform:none!important') && pageSource.includes('.ex-row__edge-light { opacity:.55;')],
  ['roving tabindex follows global and inner arrow focus', pageSource.includes('const setRovingTitle = (preferred = null)') && pageSource.includes('const focusAdjacentRow = (direction)') && pageSource.includes("event.key === 'ArrowDown' || event.key === 'ArrowUp'") && pageSource.includes("row.classList.add('is-keyboard-active')")],
];

const failures = checks.filter(([, passed]) => !passed);
for (const [label, passed] of checks) console.log(`${passed ? 'PASS' : 'FAIL'} ${label}`);
if (failures.length) {
  console.error(`\n${failures.length} exhibitions prototype contract check(s) failed.`);
  process.exit(1);
}
console.log(`\nPASS exhibitions prototype contract (${checks.length} checks, ${uniqueIds.size} unique events)`);
