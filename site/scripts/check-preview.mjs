import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';
import eventsData from '../src/data/preview-events.json' with { type: 'json' };
import relatedData from '../src/data/preview-related.json' with { type: 'json' };

const siteDir = resolve(new URL('..', import.meta.url).pathname);
const distDir = join(siteDir, 'dist');
const buildId = process.env.PREVIEW_BUILD_ID || readdirSync(distDir).find((name) => name.startsWith('preview-'));
if (!buildId) throw new Error('No preview-* folder found in dist');
const root = join(distDir, buildId);
const required = [
  '__preview/index.html',
  'segodnya/index.html',
  'vyhodnye/index.html',
  'sitemap.xml',
  'robots.txt',
  'favicon.svg',
  'preview-build.json',
  'lab/hero/index.html',
  'lab/hero/review/index.html',
  'lab/hero/review/5878-poster-billboard/index.html',
  'lab/hero/review/5878-poster-attached-card/index.html',
  'lab/hero/review/6322-photo-parallax-sheet/index.html',
  ...eventsData.events.flatMap((event) => [
    `sobytiya/${event.slug}/index.html`,
    `sobytiya/${event.slug}/event.ics`,
    `data/discovery/${event.id}.json`,
  ]),
];
for (const rel of required) {
  const path = join(root, rel);
  if (!existsSync(path) || !statSync(path).isFile()) throw new Error(`Missing required file: ${rel}`);
}
const control = eventsData.events.find((event) => event.id === 5878);
if (!control) throw new Error('Missing control event 5878');
if (control.slug !== 'pesni-sssr-svetlogorsk-5878') throw new Error(`Unexpected control slug: ${control.slug}`);
const controlHtml = readFileSync(join(root, `sobytiya/${control.slug}/index.html`), 'utf8');
const stripGeneratedCode = (html) => html.replace(/<script[\s\S]*?<\/script>/giu, '').replace(/<style[\s\S]*?<\/style>/giu, '');
const controlVisibleHtml = stripGeneratedCode(controlHtml);
if (!controlHtml.includes('noindex,nofollow,noarchive')) throw new Error('Missing preview robots meta');
if (controlHtml.includes('https://kenigevents.ru/sobytiya/pesni-sssr-svetlogorsk-5878/')) throw new Error('Production canonical leaked into preview page');
if (!controlHtml.includes(`https://kenigevents.ru/${buildId}/sobytiya/pesni-sssr-svetlogorsk-5878/`)) throw new Error('Preview canonical missing for control page');
if (/\bnull\b/.test(controlVisibleHtml)) throw new Error('Rendered HTML contains literal null outside scripts/styles');
if (controlHtml.includes('<a class="event-card"')) throw new Error('Nested-link-prone event-card anchor leaked');
if (!controlHtml.includes('data-card-href=')) throw new Error('Event cards must expose full-card navigation href');
if (!controlHtml.includes('event-card__media')) throw new Error('Control related cards do not expose visual media slot');
if (!controlHtml.includes('event-card__media-shell--preserve')) throw new Error('Text/OCR poster cards must preserve full natural poster ratio without crop');
if (!controlHtml.includes('event-card__media-shell--cover')) throw new Error('Visual-only poster cards must keep 4:5 cover media shell');
if (controlHtml.includes('media-backdrop') || controlHtml.includes('image-backdrop') || controlHtml.includes('--poster-image') || /background-image:\s*linear-gradient\([^;]*var\(--poster-image\)|blur\(/u.test(controlHtml)) throw new Error('Duplicate/backdrop poster fill leaked into event page');
if (/data-(?:feedback|share)-count[^>]*>0<\/span>/u.test(controlHtml)) throw new Error('Zero like/share counters must be hidden, not rendered as 0');
if (/event-card__media-shell--preserve[\s\S]{0,500}object-fit:\s*contain/iu.test(controlHtml)) throw new Error('OCR-safe card media must use natural image ratio, not contain over a fixed frame');
if (controlHtml.includes('event-card__actions')) throw new Error('Old separate card action row leaked');
if (controlVisibleHtml.includes('feedback-button--calendar')) throw new Error('Variant A control feed unexpectedly shows calendar buttons');
if (!controlVisibleHtml.includes('data-feed-card-variant="overlay-controls"') || !controlVisibleHtml.includes('event-card__feedback event-card__feedback--overlay')) throw new Error('Control page must expose A/B variant A overlay-controls cards');
if (!controlHtml.includes('data-feedback-action="like"') || !controlHtml.includes('data-feedback-count')) throw new Error('Control related cards miss explicit like buttons');
if (controlHtml.includes('data-source-likes-count') || controlHtml.includes('data-service-likes-count') || controlHtml.includes('data-like-origin-label') || controlHtml.includes('feedback-origin-label') || controlHtml.includes('event-card__social-proof') || /ист\.\s*\+|из источников|в сервисе/u.test(controlHtml)) throw new Error('Technical source/service like breakdown leaked into public HTML/UI');
if (!controlHtml.includes('feedback-button--share') || !controlHtml.includes('data-share-count')) throw new Error('Control related cards miss explicit share button/count');
if (!controlHtml.includes('M11.996 3.725') || controlHtml.includes('M4.2 16.1c3.45-4.8')) throw new Error('Share/repost icon must use the VK-like outline path, not the old arrow stroke');
if (!controlHtml.includes('data-feedback-action="not_interested"')) throw new Error('Control related cards miss not-interested buttons');
if (!controlHtml.includes('share-label') || !controlHtml.includes('is-share-prompt')) throw new Error('Control related cards miss post-like share prompt expansion');
if (controlHtml.includes('double_tap_like_event')) throw new Error('Double-tap like must not conflict with full-card navigation');
if (!controlHtml.includes('data-event-hero') || !controlHtml.includes('data-hero-mode="poster-stage"') || !controlHtml.includes('data-hero-composition="poster-billboard"') || !controlHtml.includes('data-hero-image-text-mode="ocr_text"')) throw new Error('Control event must render OCR-safe poster-billboard decision hero');
if (!controlHtml.includes('brand__name--mobile-tag') || !controlHtml.includes('is-past-hero')) throw new Error('Immersive event pages must include mobile brand tag and after-hero header transition');
if ((controlVisibleHtml.match(/<h1\b/giu) || []).length !== 1) throw new Error('Event page must expose exactly one visible H1');
if (!controlVisibleHtml.includes('event-hero__decision') || !controlVisibleHtml.includes('event-hero__actions')) throw new Error('Event hero must include decision block and first-screen actions in HTML');
if (controlVisibleHtml.indexOf('data-event-hero') > controlVisibleHtml.indexOf('crumbs--after-hero')) throw new Error('Hero must render before mobile/after-hero breadcrumbs in event HTML');
for (const [id, expectedMode] of [[5370, 'visual_only'], [6322, 'visual_only'], [4512, 'visual_only'], [3730, 'visual_only'], [4913, 'visual_only'], [5878, 'ocr_text'], [6093, 'ocr_text'], [6437, 'ocr_text'], [6438, 'ocr_text']]) {
  const item = eventsData.events.find((event) => event.id === id);
  if (!item || item.image_text_mode !== expectedMode) throw new Error(`Event ${id} image_text_mode must be ${expectedMode} for media regression guard`);
}
const jsonLdItems = [...controlHtml.matchAll(/<script type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/giu)].map((match) => JSON.parse(match[1]));
if (!jsonLdItems.some((item) => typeof item['@type'] === 'string' && item['@type'].endsWith('Event'))) throw new Error('Control page must contain parseable Event-class JSON-LD');
if (!jsonLdItems.some((item) => item['@type'] === 'BreadcrumbList')) throw new Error('Control page must contain parseable BreadcrumbList JSON-LD');
if (!controlHtml.includes('ke_like_share_prompt_count_v1')) throw new Error('Control page misses post-like share prompt limiter');
if (!controlHtml.includes('anchorEventId')) throw new Error('Control page misses stable anchored rerank logic');
if (!controlHtml.includes('sessionPinnedNotInterested')) throw new Error('Control page misses current-page not-interested plate persistence');
if (!controlHtml.includes('data-discovery-feed') || controlHtml.includes('data-personalized-feed') || !controlHtml.includes('data-discovery-src') || !controlHtml.includes('data-discovery-load-more') || !controlHtml.includes('hydrateDiscoveryFeeds')) throw new Error('Control page misses static-10 + personalization JSON discovery hydration contract');
if (!controlHtml.includes('ke_personalization_profile') || controlHtml.includes('ke_profile_id_v1') || controlHtml.includes('anon-${')) throw new Error('Control page must use compatible UUID personalization profile, not legacy/prefixed ids');
if (!controlHtml.includes('isCompatibleProfile') || !controlHtml.includes('rankEventDetailRelated') || !controlHtml.includes('served_list_id') || !controlHtml.includes('createServedListSummary')) throw new Error('Control page misses event_detail_related local-rerank/served-list contract');
if (!controlHtml.includes('event_detail_related') || !controlHtml.includes('local_related_rerank_v1_fallback')) throw new Error('Control page misses event_detail_related surface/algorithm markers');
if (!controlHtml.includes('/favicon.svg')) throw new Error('Control page misses favicon link');
const footerSocialUrls = [
  'https://t.me/kenigevents',
  'https://t.me/kldevents',
  'https://vk.com/kenigeventsofficial',
  'https://vk.com/klgdevents',
  'https://vk.ru/im/channels/-239844596',
  'https://max.ru/join/do_4eLW85-yK_dXcc6f2cmKp9utJuFl_hCo0cxnJ1QA',
];
for (const url of footerSocialUrls) {
  if (!controlHtml.includes(url)) throw new Error(`Footer social URL missing: ${url}`);
}
for (const cls of ['site-footer__social', 'social-icon--telegram', 'social-icon--vk', 'social-icon--max']) {
  if (!controlHtml.includes(cls)) throw new Error(`Footer social icon/class missing: ${cls}`);
}
if (!controlHtml.includes('data-prefetch')) throw new Error('Control page misses fast-navigation prefetch markers');
if (!controlHtml.includes('data-sticky-cta') || !controlHtml.includes('data-hide-sticky-after')) throw new Error('Control page misses sticky CTA feed-hide markers');
if (!controlHtml.includes('Смотрите дальше')) throw new Error('Control page misses single discovery feed heading');
if (!controlHtml.includes('Preview A/B:')) throw new Error('Control page misses visible A/B preview variant note');
if (controlHtml.includes('Похожие события') || controlHtml.includes('Попробовать другое') || controlHtml.includes('Открыть новое')) throw new Error('Control page still exposes split/exploration labels instead of one neutral discovery feed');
if (controlHtml.includes('Уточнить регистрацию')) throw new Error('Ambiguous registration CTA leaked');
if (controlHtml.includes('class="share-list"')) throw new Error('Duplicate share-list UI leaked');
if (/download="kenigevents-/u.test(controlHtml)) throw new Error('Calendar links still force download instead of opening .ics');
if (controlHtml.includes('cards-grid--feed')) throw new Error('Control page still uses horizontal related rail class');
if (controlHtml.includes('<details class="details-disclosure"')) throw new Error('Control description is hidden in a details disclosure');
if (controlHtml.includes('11 июля 2026')) throw new Error('Visible current-year date should omit year in event UI');
const discoveryJson = JSON.parse(readFileSync(join(root, `data/discovery/${control.id}.json`), 'utf8'));
if (discoveryJson.preload_target !== 10 || discoveryJson.page_size !== 10) throw new Error('Discovery JSON must declare 10-item preload/page contract');
if (discoveryJson.schema_version !== 'event-detail-related-v1' || discoveryJson.feature_schema_version !== 'event-detail-related-v1') throw new Error('Discovery JSON must use event-detail-related schema contract');
if (discoveryJson.taxonomy_version !== 'event-taxonomy-v1' || discoveryJson.surface !== 'event_detail_related' || discoveryJson.algorithm_id !== 'static_related_v1') throw new Error('Discovery JSON misses surface/taxonomy/algorithm contract');
if (!discoveryJson.current_event || discoveryJson.current_event.event_id !== control.id) throw new Error('Discovery JSON must include current_event summary');
if (!Array.isArray(discoveryJson.related_static) || discoveryJson.related_static.length < 5) throw new Error('Discovery JSON must contain related_static candidate manifest for light client hydration');
if ('events' in discoveryJson) throw new Error('Discovery JSON must expose related_static manifest, not legacy events payload');
for (const item of discoveryJson.related_static) {
  if (item.event_id === control.id) throw new Error('Discovery JSON must not include current event');
  for (const field of ['event_id', 'title', 'category', 'tags', 'audience_exclusion_tags', 'status', 'lifecycle_status', 'base_similarity', 'reason_codes', 'display']) {
    if (!(field in item)) throw new Error(`Discovery candidate missing ${field}`);
  }
  for (const field of ['calendar_href', 'calendar_eligible']) {
    if (!(field in item.display)) throw new Error(`Discovery candidate display missing ${field}`);
  }
  if (!Array.isArray(item.tags) || !Array.isArray(item.audience_exclusion_tags) || !Array.isArray(item.reason_codes)) throw new Error('Discovery candidate tag/reason fields must be arrays');
  if (typeof item.base_similarity !== 'number' || item.base_similarity < 0 || item.base_similarity > 1) throw new Error('Discovery candidate base_similarity must be 0..1');
  if (/2026\b/u.test(item.display?.display_date || '') && !/2027\b/u.test(item.display?.display_date || '')) throw new Error('Discovery JSON display dates should omit current year unless crossing year');
}

const splitControl = eventsData.events.find((event) => event.id === 6322);
if (!splitControl) throw new Error('Missing split-actions A/B event 6322');
const splitHtml = readFileSync(join(root, `sobytiya/${splitControl.slug}/index.html`), 'utf8');
const splitVisibleHtml = stripGeneratedCode(splitHtml);
if (!splitVisibleHtml.includes('data-feed-card-variant="split-actions"')) throw new Error('A/B variant B page must render split-actions cards');
if (!splitVisibleHtml.includes('event-card__utility-row')) throw new Error('A/B variant B page misses utility row inside cards');
if (!splitVisibleHtml.includes('event-card__feedback event-card__feedback--under')) throw new Error('A/B variant B page misses under-card right-aligned action row');
if (!splitVisibleHtml.includes('feedback-button--calendar')) throw new Error('A/B variant B page must expose feed calendar buttons for one-day eligible cards');
if (!splitHtml.includes('data-feed-card-variant="split-actions"')) throw new Error('A/B variant B page must keep variant marker for hydrated JSON cards');

const ics = readFileSync(join(root, `sobytiya/${control.slug}/event.ics`), 'utf8');
for (const needle of ['BEGIN:VCALENDAR', 'VERSION:2.0', 'BEGIN:VEVENT', 'DTSTART:20260711T193000Z', 'SUMMARY:Песни СССР', 'END:VCALENDAR']) {
  if (!ics.includes(needle)) throw new Error(`Control ICS missing ${needle}`);
}
if (/^DTEND:/m.test(ics)) throw new Error('Control ICS must not include DTEND without reliable duration');
for (const event of eventsData.events) {
  if (!Number.isInteger(event.likes_count) || event.likes_count < 0) throw new Error(`Event ${event.id} has invalid likes_count`);
  if (!Number.isInteger(event.source_likes_count) || event.source_likes_count < 0) throw new Error(`Event ${event.id} has invalid source_likes_count`);
  if (!Number.isInteger(event.service_likes_count) || event.service_likes_count < 0) throw new Error(`Event ${event.id} has invalid service_likes_count`);
  if (event.likes_count !== event.source_likes_count + event.service_likes_count) throw new Error(`Event ${event.id} likes_count must equal source_likes_count + service_likes_count`);
  if (!['ocr_text', 'visual_only', 'unknown'].includes(event.image_text_mode)) throw new Error(`Event ${event.id} has invalid image_text_mode`);
  const eventIcs = readFileSync(join(root, `sobytiya/${event.slug}/event.ics`), 'utf8');
  for (const needle of ['BEGIN:VCALENDAR', 'BEGIN:VEVENT', 'UID:', 'SUMMARY:', 'URL:', 'END:VCALENDAR']) {
    if (!eventIcs.includes(needle)) throw new Error(`ICS for ${event.id} missing ${needle}`);
  }
  if (!/^DTSTART(?:;VALUE=DATE)?:/m.test(eventIcs)) throw new Error(`ICS for ${event.id} missing DTSTART`);
}
const robots = readFileSync(join(root, 'robots.txt'), 'utf8').trim();
if (robots !== 'User-agent: *\nDisallow: /') throw new Error(`Unexpected robots.txt: ${robots}`);
const sitemap = readFileSync(join(root, 'sitemap.xml'), 'utf8');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/__preview/`)) throw new Error('Sitemap misses preview index URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/segodnya/`)) throw new Error('Sitemap misses today listing URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/vyhodnye/`)) throw new Error('Sitemap misses weekend listing URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/sobytiya/${control.slug}/`)) throw new Error('Sitemap misses control event URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/lab/hero/`)) throw new Error('Sitemap misses hero lab URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/lab/hero/review/`)) throw new Error('Sitemap misses hero viewport review URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/lab/hero/review/5878-poster-billboard/`)) throw new Error('Sitemap misses same-event hero review case URL');
const cssFiles = readdirSync(join(root, '_astro')).filter((name) => name.endsWith('.css'));
const css = cssFiles.map((name) => readFileSync(join(root, '_astro', name), 'utf8')).join('\n');
if (/native-share-button\{display:none/u.test(css)) throw new Error('Native share button is hidden by default');
if (/media-backdrop|image-backdrop|--poster-image|background-image:\s*linear-gradient\([^;]*var\(--poster-image\)|blur\(/u.test(css)) throw new Error('Duplicate/backdrop poster fill leaked into CSS');
if (/event-card__media-shell--preserve[^}]*object-fit:\s*contain/iu.test(css)) throw new Error('OCR-safe card media must not use contain over a fixed frame');
if (!/event-hero--poster-stage \.event-hero__image\{[^}]*object-fit:\s*contain/iu.test(css)) throw new Error('Poster-stage hero must contain OCR/text posters without crop');
if (!/event-hero--photo-cover \.event-hero__image\{[^}]*object-fit:\s*cover/iu.test(css)) throw new Error('Photo-cover hero must crop only visual-safe images');
if (!/event-hero--poster-billboard[\s\S]*?event-hero__visual[\s\S]*?width:\s*100vw/iu.test(css)) throw new Error('Poster billboard hero must make the hero visual full viewport width on mobile');
if (!/event-hero--poster-billboard\.event-hero--poster-stage \.event-hero__image[\s\S]*?\{[^}]*width:\s*100vw/iu.test(css)) throw new Error('Poster billboard hero image itself must be full viewport width on mobile');
if (!/body\.hero-chrome-immersive \.brand\{[^}]*background:\s*linear-gradient\([^}]*#793014[^}]*#a54821/iu.test(css) || !/body\.hero-chrome-immersive\.is-past-hero \.site-header/iu.test(css)) throw new Error('Immersive mobile header must use a site-palette top brand tag and transition to full header after hero');
if (!/event-hero--photo-parallax-sheet/iu.test(css) || !controlHtml.includes('hydrateHeroParallax')) throw new Error('Hero parallax experiment and reduced-motion-aware hydrator must be present');
if (!/--hero-parallax-scale/iu.test(css) || !controlHtml.includes('progress * 0.065')) throw new Error('Hero parallax must include stronger zoom-scale variables, not only a barely visible translate');
if (/100vh/u.test(css)) throw new Error('Hero CSS must not use fragile 100vh units');
if (!/event-card--split-actions \.event-card__feedback \.feedback-button\{[^}]*background:\s*transparent[^}]*border-color:\s*transparent/iu.test(css)) throw new Error('Split-actions under-card share/like must be icon-style, not pill buttons');
if (!/aspect-ratio:4\/5/u.test(css.replace(/\s+/g, ''))) throw new Error('Visual-only cover media must use vertical 4:5 ratio');
if (/aspect-ratio:3\/4/u.test(css.replace(/\s+/g, ''))) throw new Error('Old 3:4 visual-only ratio leaked into CSS');

const eventsById = new Map(eventsData.events.map((event) => [event.id, event]));
for (const event of eventsData.events) {
  const related = relatedData.related[String(event.id)] || { similar: [], explore: [] };
  const excluded = new Set([event.id, ...event.other_date_ids]);
  for (const [kind, ids] of Object.entries(related)) {
    for (const id of ids) {
      const candidate = eventsById.get(id);
      if (excluded.has(id)) throw new Error(`Related ${kind} for ${event.id} includes current/other-date ${id}`);
      if (candidate?.other_date_ids.includes(event.id)) throw new Error(`Related ${kind} for ${event.id} includes reverse other-date ${id}`);
    }
  }
  if (event.ticket.kind === 'source' && !event.ticket.is_free && /Билеты в продаже/u.test(event.status_label)) {
    throw new Error(`Source-only paid event ${event.id} pretends direct ticket sale`);
  }
  if (/#/u.test(event.venue_name || '')) throw new Error(`Venue contains hashtag for ${event.id}`);
}

const badHtmlPatterns = [
  ['raw facts marker', /\*\*facts\*\*/u],
  ['raw markdown separator', /\*\*\*/u],
  ['literal escaped newline', /\\n/u],
  ['literal null', /\bnull\b/u],
  ['literal undefined', /\bundefined\b/u],
  ['literal NaN', /\bNaN\b/u],
  ['preview diagnostic copy', /Preview показывает/u],
  ['orphan variation selector', /\ufe0f/u],
  ['empty heading', /<h[1-6][^>]*>\s*<\/h[1-6]>/iu],
];
for (const event of eventsData.events) {
  const html = readFileSync(join(root, `sobytiya/${event.slug}/index.html`), 'utf8');
  const visibleHtml = stripGeneratedCode(html);
  const heroExpected = event.image_url ? (event.image_text_mode === 'visual_only' ? 'data-hero-mode="photo-cover"' : 'data-hero-mode="poster-stage"') : 'data-hero-mode="fallback-art"';
  if (!html.includes(heroExpected)) throw new Error(`Event ${event.id} hero mode mismatch for ${event.image_text_mode}`);
  if (!html.includes('data-hero-composition=')) throw new Error(`Event ${event.id} hero misses composition marker`);
  if (!html.includes(`data-hero-image-text-mode="${event.image_text_mode}"`)) throw new Error(`Event ${event.id} hero misses image_text_mode marker`);
  if (/data-(?:feedback|share)-count[^>]*>0<\/span>/u.test(html)) throw new Error(`Event ${event.id} renders zero reaction counter`);
  const ownCalendarHref = `sobytiya/${event.slug}/event.ics`;
  const calendarEligible = !event.end_date || event.end_date === event.start_date;
  if (calendarEligible && !html.includes(ownCalendarHref)) throw new Error(`Short event ${event.id} misses own calendar link`);
  if (!calendarEligible && html.includes(ownCalendarHref)) throw new Error(`Multi-day event ${event.id} must not expose own calendar link`);
  for (const [label, pattern] of badHtmlPatterns) {
    if (pattern.test(visibleHtml)) throw new Error(`Rendered page ${event.id} contains ${label}`);
  }
  if (!event.address && html.includes('Открыть на карте')) throw new Error(`Weak-address event ${event.id} shows map CTA`);
  if (event.ticket.kind === 'source' && !event.ticket.is_free && html.includes('Билеты в продаже')) {
    throw new Error(`Source-only page ${event.id} shows misleading ticket-sale copy`);
  }
}
console.log(`Preview checks passed for ${buildId}`);
