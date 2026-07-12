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
  'zavtra/index.html',
  'vyhodnye/index.html',
  'vystavki/index.html',
  'populyarnoe/index.html',
  'poisk/index.html',
  'partnerstvo/index.html',
  'partners/index.html',
  'sitemap.xml',
  'robots.txt',
  'favicon.svg',
  'assets/transport/kppk-lastochka.webp',
  'assets/transport/bus-svgrepo-337651.svg',
  'assets/transport/romanovo-holmogorye-route-square.png',
  'assets/transport/romanovo-holmogorye-route-portrait.png',
  'sobytiya/kontsert-kaver-gruppy-diskodyadi-svetlogorsk-6397/transport/to-svetlogorsk-20260712-6725.ics',
  'sobytiya/kontsert-kaver-gruppy-diskodyadi-svetlogorsk-6397/transport/to-kaliningrad-20260712-6730.ics',
  'sobytiya/kontsert-kaver-gruppy-diskodyadi-svetlogorsk-6397/transport/to-kaliningrad-20260713-6700.ics',
  'sobytiya/kontsert-posvyaschenie-muslimu-magomaevu-i-anne-german-svetlogorsk-6510/transport/to-svetlogorsk-20260712-6717.ics',
  'sobytiya/kontsert-posvyaschenie-muslimu-magomaevu-i-anne-german-svetlogorsk-6510/transport/to-svetlogorsk-20260712-7213.ics',
  'sobytiya/kontsert-posvyaschenie-muslimu-magomaevu-i-anne-german-svetlogorsk-6510/transport/to-kaliningrad-20260712-6722.ics',
  'sobytiya/kontsert-posvyaschenie-muslimu-magomaevu-i-anne-german-svetlogorsk-6510/transport/to-kaliningrad-20260712-7220.ics',
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
let transportIcsTotal = 0;
for (const event of eventsData.events) {
  const transportDir = join(root, `sobytiya/${event.slug}/transport`);
  const files = existsSync(transportDir) ? readdirSync(transportDir).filter((name) => name.endsWith('.ics')).sort() : [];
  const html = readFileSync(join(root, `sobytiya/${event.slug}/index.html`), 'utf8');
  const linked = [...html.matchAll(/href="[^"]*\/transport\/([^"?#]+\.ics)(?:[?#][^"]*)?"/gu)].map((match) => match[1]);
  const linkedUnique = [...new Set(linked)].sort();
  const fileCeiling = html.includes('data-transport-dual-origin') ? 6 : 4;
  if (files.length > fileCeiling) throw new Error(`Event ${event.id} exceeds its ${fileCeiling}-file transport ICS ceiling: ${files.length}`);
  if (files.join('\n') !== linkedUnique.join('\n')) throw new Error(`Event ${event.id} transport ICS files must match interactive calendar links exactly`);
  for (const name of files) {
    if (!/^to-[a-z0-9-]+-\d{8}-[a-z0-9-]+\.ics$/u.test(name)) throw new Error(`Transport ICS filename is not concise semantic ASCII: ${name}`);
  }
  transportIcsTotal += files.length;
}
if (transportIcsTotal === 0) throw new Error('Preview must generate transport ICS files for the regression events');
const kgd80Events = eventsData.events.filter((event) => String(event.festival || '').trim() === '80 историй о главном');
for (const event of kgd80Events) {
  const html = readFileSync(join(root, `sobytiya/${event.slug}/index.html`), 'utf8');
  if (!html.includes('event-token--kgd80')) throw new Error(`80 Stories event ${event.id} misses KGD80 festival medallion`);
  if (!html.includes('event-token--znanie-russia')) throw new Error(`80 Stories event ${event.id} misses curated Znanie organizer medallion`);
}
const control = eventsData.events.find((event) => event.id === 5878);
if (!control) {
  if (!eventsData.events.length) throw new Error('Preview fixture must contain at least one event');
  if (relatedData.strict_verified_related) {
    const relatedValues = Object.values(relatedData.related || {});
    if (!relatedValues.length) throw new Error('Strict related preview must contain related map entries');
    for (const entry of relatedValues) {
      for (const candidateId of entry.similar || []) {
        const chainItem = (entry.chain || []).find((item) => Number(item.event_id) === Number(candidateId));
        if (!chainItem || chainItem.llm_semantic_score === undefined || Number(chainItem.llm_semantic_score) < 0.72 || chainItem.gemma_reject) {
          throw new Error(`Strict related similar candidate is not Gemma-verified: ${candidateId}`);
        }
      }
    }
  }
  console.log(`Preview check passed without control fixture: ${eventsData.events.length} events, strict_related=${Boolean(relatedData.strict_verified_related)}`);
  process.exit(0);
}
if (control.slug !== 'pesni-sssr-svetlogorsk-5878') throw new Error(`Unexpected control slug: ${control.slug}`);
const controlHtml = readFileSync(join(root, `sobytiya/${control.slug}/index.html`), 'utf8');
const stripGeneratedCode = (html) => html.replace(/<script[\s\S]*?<\/script>/giu, '').replace(/<style[\s\S]*?<\/style>/giu, '');
const controlVisibleHtml = stripGeneratedCode(controlHtml);
if (!controlVisibleHtml.includes('data-event-transport-schedule') || !controlVisibleHtml.includes('data-event-city="Светлогорск"')) throw new Error('Svetlogorsk control event misses the event transport block');
if (!controlVisibleHtml.includes('/assets/transport/kppk-lastochka.webp') || !controlVisibleHtml.includes('Как добраться на электричке')) throw new Error('Event transport block misses supplied Lastochka artwork or neutral heading');
if (controlVisibleHtml.includes('Партнёрский маршрут') || controlVisibleHtml.includes('АО «КППК»')) throw new Error('Event transport block must not present the schedule as a partner route');
if ((controlVisibleHtml.match(/>КППК<\/a>/gu) || []).length !== 1) throw new Error('Event transport block must mention КППК exactly once in the laconic footer');
if (controlVisibleHtml.includes('Проверить расписание') || controlVisibleHtml.includes('rasp.yandex')) throw new Error('Event transport block must not expose schedule verification links');
if (!controlVisibleHtml.includes('К началу концерта в 21:30')) throw new Error('Transport heading must inflect the public event type');
if (!controlVisibleHtml.includes('Калининград-Северный ⇄ Светлогорск-2') || !controlVisibleHtml.includes('event-transport__train-link') || controlVisibleHtml.includes('class="event-transport__calendar"')) throw new Error('Train rows must be compact whole-card calendar links with one bidirectional route label');
if (controlVisibleHtml.indexOf('data-event-transport-schedule') > controlVisibleHtml.indexOf('>Коротко</h2>')) throw new Error('Event transport block must render immediately after the description and before “Коротко”');
if (!controlHtml.includes('noindex,nofollow,noarchive')) throw new Error('Missing preview robots meta');
if (controlHtml.includes('https://kenigevents.ru/sobytiya/pesni-sssr-svetlogorsk-5878/')) throw new Error('Production canonical leaked into preview page');
if (!controlHtml.includes(`https://kenigevents.ru/${buildId}/sobytiya/pesni-sssr-svetlogorsk-5878/`)) throw new Error('Preview canonical missing for control page');
if (/\bnull\b/.test(controlVisibleHtml)) throw new Error('Rendered HTML contains literal null outside scripts/styles');
if (controlHtml.includes('<a class="event-card"')) throw new Error('Nested-link-prone event-card anchor leaked');
if (!controlHtml.includes('data-card-href=')) throw new Error('Event cards must expose full-card navigation href');
if (!/<div class="event-card__body">\s*<a class="event-card__title"[\s\S]*?<div class="event-card__meta-row">/u.test(controlVisibleHtml)) throw new Error('Event cards must show title before time/status meta for mobile feed scanning');
if (!controlHtml.includes('event-card__media')) throw new Error('Control related cards do not expose visual media slot');
if (!controlHtml.includes('event-card__media-shell--preserve')) throw new Error('Text/OCR poster cards must preserve full natural poster ratio without crop');
if (!controlHtml.includes('event-card__media-shell--cover')) throw new Error('Visual-only poster cards must keep 4:5 cover media shell');
if (controlHtml.includes('media-backdrop') || controlHtml.includes('image-backdrop') || controlHtml.includes('--poster-image') || /background-image:\s*linear-gradient\([^;]*var\(--poster-image\)|blur\(/u.test(controlHtml)) throw new Error('Duplicate/backdrop poster fill leaked into event page');
if (/data-(?:feedback|share)-count[^>]*>0<\/span>/u.test(controlHtml)) throw new Error('Zero like/share counters must be hidden, not rendered as 0');
if (/event-card__media-shell--preserve[\s\S]{0,500}object-fit:\s*contain/iu.test(controlHtml)) throw new Error('OCR-safe card media must use natural image ratio, not contain over a fixed frame');
if (/Вход[\s\S]{0,180}Билеты(?:\s+доступны)?/u.test(controlVisibleHtml)) throw new Error('Bare ticket availability must not be rendered as an admission value');
if (controlHtml.includes('event-card__actions')) throw new Error('Old separate card action row leaked');
if (!controlVisibleHtml.includes('data-feed-card-variant="split-actions"') || !controlVisibleHtml.includes('event-card__feedback event-card__feedback--under')) throw new Error('Control event page must use split-actions baseline cards');
if (controlVisibleHtml.includes('event-card__feedback event-card__feedback--overlay')) throw new Error('Overlay-controls cards must not appear on normal event pages');
if (!controlVisibleHtml.includes('feedback-button--calendar')) throw new Error('Split-actions baseline must expose feed calendar buttons for eligible candidates');
if (!controlHtml.includes('data-feedback-action="like"') || !controlHtml.includes('data-feedback-count')) throw new Error('Control related cards miss explicit like buttons');
if (controlHtml.includes('data-source-likes-count') || controlHtml.includes('data-service-likes-count') || controlHtml.includes('data-like-origin-label') || controlHtml.includes('feedback-origin-label') || controlHtml.includes('event-card__social-proof') || /ист\.\s*\+|из источников|в сервисе/u.test(controlHtml)) throw new Error('Technical source/service like breakdown leaked into public HTML/UI');
if (!controlHtml.includes('feedback-button--share') || !controlHtml.includes('data-share-count')) throw new Error('Control related cards miss explicit share button/count');
if (controlHtml.includes('data-share-experiment') || controlHtml.includes('Поделиться эксперимент') || controlHtml.includes('data-copy-rich-share') || controlHtml.includes('Скопировать HTML-пост')) throw new Error('Temporary share experiment buttons must not leak into production-like event UI');
if (!controlHtml.includes('data-native-share') || !controlHtml.includes('data-share-image=') || !controlHtml.includes('data-share-image-type=') || !controlHtml.includes('data-share-file-name=') || !controlHtml.includes('navigator.canShare') || !controlHtml.includes('sharePayload(button, [file])') || !controlHtml.includes('createGeneratedShareImage') || !controlHtml.includes('1080') || !controlHtml.includes('1350')) throw new Error('Control event page misses primary Web Share file/text/url path with generated 4:5 share-image fallback');
if (!controlHtml.includes('og:image:secure_url') || !controlHtml.includes('og:image:type')) throw new Error('Control event page misses strengthened Open Graph image metadata for share previews');
if (!controlHtml.includes('data-feedback-scope') || !/data-event-hero[\s\S]{0,6500}data-feedback-action="like"/u.test(controlHtml)) throw new Error('Event hero must expose a first-party like button/count for the current event');
if (!controlHtml.includes('M11.996 3.725') || controlHtml.includes('M4.2 16.1c3.45-4.8')) throw new Error('Share/repost icon must use the VK-like outline path, not the old arrow stroke');
if (!controlHtml.includes('data-feedback-action="not_interested"')) throw new Error('Control related cards miss not-interested buttons');
if (!/data-nosnippet[^>]*data-feedback-action="not_interested"|data-feedback-action="not_interested"[^>]*data-nosnippet/u.test(controlHtml) || !/data-nosnippet[^>]*data-native-share|data-native-share[^>]*data-nosnippet/u.test(controlHtml)) throw new Error('Service controls such as not-interested/share must be marked data-nosnippet');
if (!controlHtml.includes('share-label') || !controlHtml.includes('is-share-prompt')) throw new Error('Control related cards miss post-like share prompt expansion');
if (controlHtml.includes('double_tap_like_event')) throw new Error('Double-tap like must not conflict with full-card navigation');
if (!controlHtml.includes('data-event-hero') || !controlHtml.includes('data-hero-mode="poster-stage"') || !controlHtml.includes('data-hero-composition="poster-billboard"') || !controlHtml.includes('data-hero-image-text-mode="ocr_text"')) throw new Error('Control event must render OCR-safe poster-billboard decision hero');
if (!controlHtml.includes('data-hero-gallery-open="hero-gallery-5878"') || !controlHtml.includes('data-hero-gallery') || !controlHtml.includes('hero-gallery__slide--cta') || !controlHtml.includes('Смотреть похожее')) throw new Error('Event hero must expose fullscreen image gallery with final similar-event CTA slide');
if (!controlVisibleHtml.includes('<a class="hero-gallery__brand"') || !controlVisibleHtml.includes('Полюбить Калининград') || !controlVisibleHtml.includes('hero-gallery__slide') || !controlVisibleHtml.includes('hero-gallery__caption') || !controlVisibleHtml.includes('Фото события')) throw new Error('Hero gallery must keep the service tag as a navigable link and one fixed readable bottom title stripe');
if (!controlVisibleHtml.includes('event-hero__gallery-hint') || !/(Открыть фото|Фото \d+)/u.test(controlVisibleHtml)) throw new Error('Event hero must expose a visible photo-view CTA/count over the image');
if (!controlHtml.includes('data-gallery-src=') || /class="hero-gallery__image"[^>]*\ssrc=/u.test(controlHtml)) throw new Error('Fullscreen gallery images must be lazy hydrated from data-gallery-src, not eagerly loaded in hidden HTML');
if (!controlHtml.includes('data-mobile-discovery-menu') || !controlHtml.includes('mobile-discovery-menu__panel') || !controlHtml.includes('mobile-discovery-menu__links') || !controlHtml.includes('is-past-hero')) throw new Error('Immersive event pages must include mobile discovery drawer and stable after-hero state contract');
if (controlHtml.includes('mobile-discovery-menu__brand-icon') || controlHtml.includes('/brand-mark.svg')) throw new Error('Mobile discovery tag must not expose the rejected brand icon/brand-mark animation');
if (!controlHtml.includes('mobile-discovery-menu__label') || !controlHtml.includes('hydrateMobileBrandSway') || !controlHtml.includes('--brand-sway-x') || !controlVisibleHtml.includes('/zavtra/')) throw new Error('Mobile discovery/navigation must expose tomorrow link and the calculated title-sway label contract');
if (controlHtml.includes('mobile-discovery-menu__chevron') || controlHtml.includes('⌄')) throw new Error('Mobile discovery drawer handle must not expose chevron/up/down icons');
if ((controlVisibleHtml.match(/<h1\b/giu) || []).length !== 1) throw new Error('Event page must expose exactly one visible H1');
if (!controlVisibleHtml.includes('event-hero__decision') || !controlVisibleHtml.includes('event-hero__actions')) throw new Error('Event hero must include decision block and first-screen actions in HTML');
if (controlVisibleHtml.indexOf('data-event-hero') > controlVisibleHtml.indexOf('crumbs--after-hero')) throw new Error('Hero must render before mobile/after-hero breadcrumbs in event HTML');
let checkedMediaRegressionEvents = 0;
for (const [id, expectedMode] of [[5370, 'visual_only'], [6322, 'visual_only'], [4512, 'visual_only'], [3730, 'visual_only'], [4913, 'visual_only'], [5878, 'ocr_text'], [6093, 'ocr_text'], [6437, 'ocr_text'], [6438, 'ocr_text']]) {
  const item = eventsData.events.find((event) => event.id === id);
  if (!item) continue;
  checkedMediaRegressionEvents += 1;
  if (item.image_text_mode !== expectedMode) throw new Error(`Event ${id} image_text_mode must be ${expectedMode} for media regression guard`);
}
if (checkedMediaRegressionEvents < 4) throw new Error(`Media regression guard needs at least 4 present control events, got ${checkedMediaRegressionEvents}`);
const tretyakovEvent = eventsData.events.find((event) => event.id === 5370);
if (!tretyakovEvent) throw new Error('Missing 5370 ticket/paid regression event');
if (!Array.isArray(tretyakovEvent.image_assets) || tretyakovEvent.image_assets.length < 5) throw new Error('Event 5370 must carry multi-image gallery assets for hero fullscreen review');
const tretyakovHtml = readFileSync(join(root, `sobytiya/${tretyakovEvent.slug}/index.html`), 'utf8');
const tretyakovVisibleHtml = stripGeneratedCode(tretyakovHtml);
if (tretyakovVisibleHtml.includes('data-event-transport-schedule')) throw new Error('Kaliningrad event 5370 must not render a coastal train schedule block');
const tretyakovEyebrow = (tretyakovVisibleHtml.match(/<p class="event-hero__eyebrow">([^<]*)<\/p>/u) || [null, ''])[1];
if (!tretyakovEvent.ticket.is_free && tretyakovEvent.ticket.kind === 'ticket' && !tretyakovEvent.ticket.price_label) {
  if (/Билеты|Платный вход/u.test(tretyakovEyebrow)) throw new Error('Paid/generic admission copy must not be shown above the event title');
  if (!tretyakovVisibleHtml.includes('По билетам')) throw new Error('Paid/ticketed event without exported price must show admission as “По билетам”, not “Платный вход” or “Вход Билеты”');
}
if (
  tretyakovEvent.ticket.is_free
  || /event-hero__eyebrow[^>]*>[^<]*Бесплатно/u.test(tretyakovVisibleHtml)
  || /event-info-admission[\s\S]{0,180}Бесплатно/u.test(tretyakovVisibleHtml)
) throw new Error('Event 5370 must remain paid/ticketed in the preview fixture, not inherit the false-free source merge');
const tretyakovJsonLd = [...tretyakovHtml.matchAll(/<script type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/giu)].map((match) => JSON.parse(match[1])).find((item) => typeof item['@type'] === 'string' && item['@type'].endsWith('Event'));
if (tretyakovJsonLd?.isAccessibleForFree !== Boolean(tretyakovEvent.ticket.is_free)) throw new Error('Event 5370 JSON-LD must expose the same free/paid state as exported production data');
if (!Array.isArray(tretyakovJsonLd?.image) || tretyakovJsonLd.image.length < 5) throw new Error('Event 5370 JSON-LD must connect gallery images to the event for SEO/GEO');
if (!tretyakovHtml.includes(`Фото ${tretyakovEvent.image_assets.length}`)) throw new Error('Event 5370 hero must show photo count CTA on the image');
const warriorEvent = eventsData.events.find((event) => event.id === 698);
if (!warriorEvent) throw new Error('Missing event 698 gallery regression event');
const warriorHtml = readFileSync(join(root, `sobytiya/${warriorEvent.slug}/index.html`), 'utf8');
const warriorVisibleHtml = stripGeneratedCode(warriorHtml);
const warriorEyebrow = (warriorVisibleHtml.match(/<p class="event-hero__eyebrow">([^<]*)<\/p>/u) || [null, ''])[1];
if (/Платный вход|Билеты/u.test(warriorEyebrow)) throw new Error('Event 698 hero eyebrow must not expose paid/generic admission copy above the title');
if (/data-gallery-src="https:\/\/files\.catbox\.moe/iu.test(warriorHtml)) throw new Error('Hero fullscreen gallery must not emit unreliable catbox slides; mirror or skip them before publishing');
const jsonLdItems = [...controlHtml.matchAll(/<script type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/giu)].map((match) => JSON.parse(match[1]));
if (!jsonLdItems.some((item) => typeof item['@type'] === 'string' && item['@type'].endsWith('Event'))) throw new Error('Control page must contain parseable Event-class JSON-LD');
if (!jsonLdItems.some((item) => item['@type'] === 'BreadcrumbList')) throw new Error('Control page must contain parseable BreadcrumbList JSON-LD');
const eventJsonLd = jsonLdItems.find((item) => typeof item['@type'] === 'string' && item['@type'].endsWith('Event'));
if (eventJsonLd?.offers?.validFrom && !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/u.test(eventJsonLd.offers.validFrom)) {
  throw new Error(`Event JSON-LD offer validFrom must be ISO 8601 with timezone, got: ${eventJsonLd.offers.validFrom}`);
}
if (!controlHtml.includes('ke_like_share_prompt_count_v1')) throw new Error('Control page misses post-like share prompt limiter');
if (!controlHtml.includes('data-reset-personalization') || !controlHtml.includes('Персонализация сброшена')) throw new Error('Control page misses technical local personalization reset button/handler');
if (!controlHtml.includes('anchorEventId')) throw new Error('Control page misses stable anchored rerank logic');
if (!controlHtml.includes('sessionPinnedNotInterested')) throw new Error('Control page misses current-page not-interested plate persistence');
if (!controlHtml.includes('data-discovery-feed') || controlHtml.includes('data-personalized-feed') || !controlHtml.includes('data-discovery-src') || !controlHtml.includes('data-discovery-load-more') || !controlHtml.includes('hydrateDiscoveryFeeds')) throw new Error('Control page misses static-10 + personalization JSON discovery hydration contract');
if (!controlHtml.includes('ke_personalization_profile') || controlHtml.includes('ke_profile_id_v1') || controlHtml.includes('anon-${')) throw new Error('Control page must use compatible UUID personalization profile, not legacy/prefixed ids');
if (!controlHtml.includes('isCompatibleProfile') || !controlHtml.includes('rankEventDetailRelated') || !controlHtml.includes('served_list_id') || !controlHtml.includes('createServedListSummary')) throw new Error('Control page misses event_detail_related local-rerank/served-list contract');
if (!controlHtml.includes('event_detail_related') || !controlHtml.includes('local_related_rerank_v1_fallback')) throw new Error('Control page misses event_detail_related surface/algorithm markers');
if (!controlHtml.includes('/favicon.svg')) throw new Error('Control page misses favicon link');
const faviconSvg = readFileSync(join(root, 'favicon.svg'), 'utf8');
if (!faviconSvg.includes('brand-k-mark') || !faviconSvg.includes('signal-bar-high') || !faviconSvg.includes('heart-block') || !faviconSvg.includes('#2d3035') || !faviconSvg.includes('#af481f') || /<image\b|data:image\//iu.test(faviconSvg)) throw new Error('Favicon must use the warm two-color PK monogram vector SVG without embedded raster');
const footerSocialUrls = [
  'https://t.me/kenigevents',
  'https://t.me/kldevents',
  'https://vk.com/kenigeventsofficial',
  'https://vk.com/klgdevents',
  'https://vk.ru/im/channels/-239844596',
  'https://max.ru/channel_kenigevents',
];
for (const url of footerSocialUrls) {
  if (!controlHtml.includes(url)) throw new Error(`Footer social URL missing: ${url}`);
}
if (!controlHtml.includes('mailto:info@kenigevents.ru')) throw new Error('Footer contact email missing');
for (const cls of ['site-footer__social', 'social-icon--telegram', 'social-icon--vk', 'social-icon--max']) {
  if (!controlHtml.includes(cls)) throw new Error(`Footer social icon/class missing: ${cls}`);
}
if (!controlHtml.includes('data-prefetch')) throw new Error('Control page misses fast-navigation prefetch markers');
if (!controlHtml.includes('data-sticky-cta') || !controlHtml.includes('data-hide-sticky-after')) throw new Error('Control page misses sticky CTA feed-hide markers');
if (!controlHtml.includes('Смотрите дальше')) throw new Error('Control page misses single discovery feed heading');
if (controlVisibleHtml.includes('Preview A/B:') || controlVisibleHtml.includes('В HTML сразу предзагружены')) throw new Error('Normal event page must not expose preview/A-B/debug discovery copy');
if (controlHtml.includes('Похожие события') || controlHtml.includes('Попробовать другое') || controlHtml.includes('Открыть новое')) throw new Error('Control page still exposes split/exploration labels instead of one neutral discovery feed');
if (controlHtml.includes('Уточнить регистрацию')) throw new Error('Ambiguous registration CTA leaked');
if (controlVisibleHtml.includes('Telegraph')) throw new Error('Event pages must not expose Telegraph link as a user-facing source');
if (controlVisibleHtml.includes('Просмотры в источниках') || controlVisibleHtml.includes('Источники')) throw new Error('Source count/views are gated and must not be shown in public compact facts');
if (!controlVisibleHtml.includes('Все источники, упоминания и расширенная статистика события будут доступны зарегистрированным пользователям')) throw new Error('Event page must show registered-user gate notice for sources/mentions/extended stats');
if (!controlVisibleHtml.includes('event-info-block') || !controlVisibleHtml.includes('event-info-item__icon')) throw new Error('Compact facts must render icon-based event info block');
if (!controlVisibleHtml.includes('event-source-gate--section') || !/<div class="event-info-block"[\s\S]*?<\/dl>\s*<\/div>\s*(?:<p class="event-description-meta"[^>]*>[\s\S]*?<\/p>\s*)?<\/div>\s*<p class="event-source-gate event-source-gate--section"/u.test(controlVisibleHtml)) throw new Error('Source/mentions auth gate must belong to the parent details section, not to the compact facts block');
if (controlVisibleHtml.includes('event-hero__facts')) throw new Error('Hero must not duplicate the compact facts block as a second info block');
if (controlHtml.includes('class="share-list"')) throw new Error('Duplicate share-list UI leaked');
if (/download="kenigevents-/u.test(controlHtml)) throw new Error('Calendar links still force download instead of opening .ics');
if (controlHtml.includes('cards-grid--feed')) throw new Error('Control page still uses horizontal related rail class');
if (controlHtml.includes('<details class="details-disclosure"')) throw new Error('Control description is hidden in a details disclosure');
if (controlHtml.includes('11 июля 2026')) throw new Error('Visible current-year date should omit year in event UI');
const discoveryJson = JSON.parse(readFileSync(join(root, `data/discovery/${control.id}.json`), 'utf8'));
if (discoveryJson.preload_target !== 10 || discoveryJson.page_size !== 10) throw new Error('Discovery JSON must declare 10-item preload/page contract');
if (discoveryJson.schema_version !== 'event-detail-related-v1' || discoveryJson.feature_schema_version !== 'event-detail-related-v1') throw new Error('Discovery JSON must use event-detail-related schema contract');
if (discoveryJson.taxonomy_version !== 'event-taxonomy-v1' || discoveryJson.surface !== 'event_detail_related' || !['static_related_v1', 'event_sparse_related_chain_v1', 'event_pgvector_related_chain_v1', 'event_pgvector_related_chain_v2_two_doc'].includes(discoveryJson.algorithm_id)) throw new Error('Discovery JSON misses surface/taxonomy/algorithm contract');
if (discoveryJson.algorithm_id === 'event_sparse_related_chain_v1' && (discoveryJson.strategy !== 'event_sparse_related_chain_v1_manifest' || !discoveryJson.related_static.some((item) => item.slot_type && 'lexical_similarity' in item))) throw new Error('Sparse related chain must surface honest lexical candidate evidence and slot_type in static manifests');
if (
  (discoveryJson.algorithm_id === 'event_pgvector_related_chain_v1' || discoveryJson.algorithm_id === 'event_pgvector_related_chain_v2_two_doc')
  && (
    !['event_pgvector_related_chain_v1_manifest', 'event_pgvector_related_chain_v2_manifest'].includes(discoveryJson.strategy)
    || !discoveryJson.related_static.some((item) => item.slot_type && 'vector_similarity' in item)
  )
) throw new Error('pgvector related chain must surface semantic vector evidence and slot_type in static manifests');
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
  if (/2026\b/u.test(item.display?.display_date || '') && !/2027\b/u.test(item.display?.display_date || '') && String(item.date || '').startsWith('2026-')) throw new Error('Discovery JSON display dates should omit current year unless crossing year');
}

const splitControl = eventsData.events.find((event) => event.id === 6322);
const splitHtml = splitControl ? readFileSync(join(root, `sobytiya/${splitControl.slug}/index.html`), 'utf8') : controlHtml;
const splitVisibleHtml = stripGeneratedCode(splitHtml);
if (!splitVisibleHtml.includes('data-feed-card-variant="split-actions"')) throw new Error('Split-actions baseline must render split-actions cards');
if (!splitVisibleHtml.includes('event-card__utility-row')) throw new Error('Split-actions page misses utility row inside cards');
if (!splitVisibleHtml.includes('event-card__feedback event-card__feedback--under')) throw new Error('Split-actions page misses under-card action row');
if (!splitVisibleHtml.includes('feedback-button--calendar')) throw new Error('Split-actions page must expose feed calendar buttons for one-day eligible cards');
if (!splitHtml.includes('data-feed-card-variant="split-actions"')) throw new Error('Split-actions page must keep variant marker for hydrated JSON cards');
if (splitControl && (!splitHtml.includes('icon--phone') || !splitHtml.includes('M14.05 6c.98.19'))) throw new Error('Phone CTA must use a clear vector phone/call icon');
if (controlVisibleHtml.includes('>Sitemap</a>')) throw new Error('Sitemap must not be exposed in user-facing event navigation');
const pushkinEvent = eventsData.events.find((event) => event.id === 4913);
if (pushkinEvent) {
  const pushkinHtml = readFileSync(join(root, `sobytiya/${pushkinEvent.slug}/index.html`), 'utf8');
  if (!pushkinHtml.includes('event-info-chip--pushkin') || !pushkinHtml.includes('✓') || pushkinHtml.includes('>возможна</dd>')) throw new Error('Pushkin card fact must render as a compact admission property check mark, not value copy');
}
const freeEvent = eventsData.events.find((event) => event.id === 4512);
if (freeEvent) {
  const freeHtml = readFileSync(join(root, `sobytiya/${freeEvent.slug}/index.html`), 'utf8');
  if (!freeHtml.includes('Бесплатно') || !freeHtml.includes('вход свободный') || /<dd[^>]*>\s*Бесплатно\s*<\/dd>/u.test(freeHtml)) throw new Error('Free admission must keep the word “Бесплатно” and render the free-entry subtype, not bare value copy');
}
if (!/event-card__utility-row[\s\S]*feedback-button--negative/u.test(splitVisibleHtml) || !/event-card__feedback event-card__feedback--under[\s\S]*feedback-button--share[\s\S]*feedback-button--like/u.test(splitVisibleHtml)) throw new Error('Split-actions must keep not-interested in the card utility row and share/like in the under-card row');

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
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/zavtra/`)) throw new Error('Sitemap misses tomorrow listing URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/vyhodnye/`)) throw new Error('Sitemap misses weekend listing URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/vystavki/`)) throw new Error('Sitemap misses exhibitions listing URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/populyarnoe/`)) throw new Error('Sitemap misses popular listing URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/poisk/`)) throw new Error('Sitemap misses authorized search URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/partnerstvo/`)) throw new Error('Sitemap misses partnership URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/partners/`)) throw new Error('Sitemap misses info partners URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/sobytiya/${control.slug}/`)) throw new Error('Sitemap misses control event URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/lab/hero/`)) throw new Error('Sitemap misses hero lab URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/lab/hero/review/`)) throw new Error('Sitemap misses hero viewport review URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/lab/hero/review/5878-poster-billboard/`)) throw new Error('Sitemap misses same-event hero review case URL');


const searchHtml = readFileSync(join(root, 'poisk/index.html'), 'utf8');
const searchVisibleHtml = stripGeneratedCode(searchHtml);
if (!searchHtml.includes('data-authorized-search') || !searchHtml.includes('data-search-login') || !searchHtml.includes('custom:yandex') || !searchHtml.includes('data-supabase-url')) throw new Error('Authorized search page must render Yandex/Supabase search UI when public env is provided');
const bundledJs = readdirSync(join(root, '_astro')).filter((name) => name.endsWith('.js')).map((name) => readFileSync(join(root, '_astro', name), 'utf8')).join('\n');
if (!bundledJs.includes('flowType:"pkce"') || !bundledJs.includes('detectSessionInUrl:!1') || !bundledJs.includes('exchangeCodeForSession') || !bundledJs.includes('error_description') || !/searchParams\.delete\([^)]*\)/u.test(bundledJs) || !bundledJs.includes('hash=""')) throw new Error('Authorized search must use PKCE OAuth and clean same-page redirect URLs before Yandex login');
const bundledCss = readdirSync(join(root, '_astro')).filter((name) => name.endsWith('.css')).map((name) => readFileSync(join(root, '_astro', name), 'utf8')).join('\n');
if (!/\[hidden\][^{]*\{[^}]*display:\s*none\s*!important/iu.test(bundledCss)) throw new Error('Authorized search build must include a strong hidden rule so unauthenticated form/results/buttons stay hidden');
if (!searchVisibleHtml.includes('authorized-search__yandex-icon') || !searchVisibleHtml.includes('>Я</span>') || !searchVisibleHtml.includes('Войти через Яндекс')) throw new Error('Authorized search login button must expose recognizable Yandex branding/icon text');
if (searchVisibleHtml.includes('Пока без запроса') || searchVisibleHtml.includes('cards-grid') || /<article class="event-card/u.test(searchVisibleHtml)) throw new Error('Dedicated search page must not show prefilled static result cards before a query');
if (!searchVisibleHtml.includes('Поисковые теги') || !bundledJs.includes('ke_search_feedback_queue_v1') || !bundledJs.includes('record_event_search_feedback_v1')) throw new Error('Search page must include feedback/tag candidate UX and RPC wiring');
if (!bundledJs.includes('stream_stalled') || !bundledJs.includes('stream_rescue') || !bundledJs.includes('Поток прогресса не дошёл до браузера')) throw new Error('Authorized search must include mobile stream-stall JSON rescue fallback');
if (!controlHtml.includes('/poisk/')) throw new Error('Mobile/desktop navigation must expose the authorized search page link');
if (!controlHtml.includes('/vystavki/') || !controlHtml.includes('/populyarnoe/') || !controlHtml.includes('/partnerstvo/')) throw new Error('Navigation must expose exhibitions, popular and partnership pages');
if (!controlHtml.includes('/partners/') || !controlHtml.includes('Инфопартнёры')) throw new Error('Footer/mobile navigation must expose the info partners page link');

const partnersHtml = readFileSync(join(root, 'partners/index.html'), 'utf8');
const partnersVisibleHtml = stripGeneratedCode(partnersHtml);
for (const needle of ['Информационные партнёры', 'КППК', 'Знание', '80 историй', 'Кантата', 'Акт Опус']) {
  if (!partnersVisibleHtml.includes(needle)) throw new Error(`Info partners page misses ${needle}`);
}
for (const needle of [
  'Полюбить Калининград Анонсы выступает информационным партнёром организаций',
  'АО «КППК»',
  'Просветительский фестиваль к 80-летию Калининградской области',
  'Образовательная программа фестиваля',
]) {
  if (!partnersVisibleHtml.includes(needle)) throw new Error(`Info partners page misses required heading/caption: ${needle}`);
}
for (const staleLabel of ['Пригородные маршруты', 'Просветительские события', 'Фестиваль 80-летия', 'Лекции и музыка', 'Театр и премьеры', 'КППК / РЖД', 'Информационный партнёр просветительских событий', 'Информационный партнёр театральной афиши', 'партнёр по образовательной программе']) {
  if (partnersVisibleHtml.includes(staleLabel)) throw new Error(`Info partners page must not render stale/category/wrong caption: ${staleLabel}`);
}
if (!partnersHtml.includes('/assets/partners/kppk-rzd-red.svg')) throw new Error('Info partners КППК tile must use the sourced RZD/KPPK mark, not an invented text-only logo');
const expectedPartnerUrls = ['https://www.kppk39.ru/', 'https://znanierussia.ru/', 'https://kgd80.ru/', 'https://kantatafest.ru/obrazovatelnaya-programma', 'https://actop.us/plays'];
for (const url of expectedPartnerUrls) {
  if (!partnersHtml.includes(url)) throw new Error(`Info partners page misses partner URL: ${url}`);
}
const renderedPartnerUrls = [...partnersHtml.matchAll(/<a class="partner-tile[^"]*"[^>]+href="(https?:\/\/[^"]+)"/giu)].map((match) => match[1]).sort();
const expectedSortedPartnerUrls = [...expectedPartnerUrls].sort();
if (JSON.stringify(renderedPartnerUrls) !== JSON.stringify(expectedSortedPartnerUrls)) throw new Error(`Info partners page must render exactly the approved partner URL set, got ${JSON.stringify(renderedPartnerUrls)}`);
if (!/rel="nofollow noopener noreferrer"/u.test(partnersHtml)) throw new Error('Info partners external links must be nofollow/noopener/noreferrer');
if (!partnersHtml.includes('class="partner-tile ') || !partnersHtml.includes('partner-tile__logo')) throw new Error('Info partners page must render compact full-tile partner links');
if (partnersHtml.includes('partner-card') || partnersVisibleHtml.includes('Сайт партнёра')) throw new Error('Info partners page must not render oversized partner cards or separate site CTA copy');
const compactPartnersCss = bundledCss.replace(/\s+/gu, '');
if (/\.partner-tile(?:\[[^\]]+\])?\{[^{}]*(box-shadow|border:|background:)/u.test(compactPartnersCss) || compactPartnersCss.includes('radial-gradient(circleat16%0%')) throw new Error('Info partners must stay a flat logo board without heavy card borders/backgrounds/shadows');
if (!compactPartnersCss.includes('grid-template-columns:repeat(4,minmax(0,1fr))')) throw new Error('Info partners mobile layout must keep a compact four-column bento grid');
if (!compactPartnersCss.includes('@media(min-width:980px)') || !compactPartnersCss.includes('grid-template-columns:repeat(8,minmax(0,1fr))')) throw new Error('Info partners desktop layout must keep an eight-column aspect-aware grid');
if (!partnersHtml.includes('--partner-col-start: 1') || !partnersHtml.includes('--partner-col-span: 4') || !partnersHtml.includes('--partner-row-span: 2') || !partnersHtml.includes('--partner-mobile-col-start: 3') || !partnersHtml.includes('--partner-mobile-col-span: 2')) throw new Error('Info partners tiles must keep explicit bento placement variables for greedy logo spans');
const exhibitionsHtml = readFileSync(join(root, 'vystavki/index.html'), 'utf8');
if (!exhibitionsHtml.includes('Выставки и долгие форматы') || !exhibitionsHtml.includes('listing-stack')) throw new Error('Exhibitions listing must exist as a separate section/page');
const popularHtml = readFileSync(join(root, 'populyarnoe/index.html'), 'utf8');
if (!popularHtml.includes('Популярное') || !popularHtml.includes('listing-stack')) throw new Error('Popular listing must exist as a separate section/page');
const partnershipHtml = readFileSync(join(root, 'partnerstvo/index.html'), 'utf8');
if (!partnershipHtml.includes('Информационное партнёрство') || !partnershipHtml.includes('Ласточка')) throw new Error('Partnership page must keep the current reference/test block');

const transportDemoEvent = eventsData.events.find((event) => event.id === 6510);
if (!transportDemoEvent) throw new Error('Missing real event 6510 transport schedule regression event');
if (transportDemoEvent.start_date !== '2026-07-12' || transportDemoEvent.start_time !== '17:00' || transportDemoEvent.time_range_end !== '18:10') throw new Error('Event 6510 must retain its source-verified 2026-07-12 17:00 start and 1h10m duration');
if (!transportDemoEvent.title.includes('Муслиму Магомаеву') || !transportDemoEvent.title.includes('Анне Герман') || transportDemoEvent.ticket?.href !== 'https://янтарьхолл.рф/afisha/kontsertnaya-programma-khity-lyubimykh-artistov%2012%2007/') throw new Error('Event 6510 must remain connected to the real Yantar Hall event and ticket page');
const transportDemoHtml = stripGeneratedCode(readFileSync(join(root, `sobytiya/${transportDemoEvent.slug}/index.html`), 'utf8'));
if (!transportDemoHtml.includes('data-event-transport-schedule') || !transportDemoHtml.includes('data-outbound-count="2"') || !transportDemoHtml.includes('data-return-count="2"')) throw new Error('Event 6510 must expose two outbound and two return options for its real 17:00–18:10 window');
if (!/data-transport-direction="outbound"[^>]*data-train-number="6717"[\s\S]*?<time[^>]*>15:11<\/time>[\s\S]*?<time[^>]*>16:05<\/time>/u.test(transportDemoHtml)) throw new Error('Event 6510 second outbound suggestion must be train 6717, 15:11→16:05 (55 minutes before start)');
if (!/data-transport-direction="outbound"[^>]*data-train-number="7213"[\s\S]*?<time[^>]*>15:43<\/time>[\s\S]*?<time[^>]*>16:29<\/time>/u.test(transportDemoHtml)) throw new Error('Event 6510 outbound suggestion must be train 7213, 15:43→16:29 (31 minutes before start)');
if (!/data-transport-direction="return"[^>]*data-train-number="6722"[\s\S]*?<time[^>]*>18:54<\/time>[\s\S]*?<time[^>]*>19:48<\/time>/u.test(transportDemoHtml)) throw new Error('Event 6510 first return suggestion must be train 6722, 18:54→19:48');
if (!/data-transport-direction="return"[^>]*data-train-number="7220"[\s\S]*?<time[^>]*>19:33<\/time>[\s\S]*?<time[^>]*>20:19<\/time>/u.test(transportDemoHtml)) throw new Error('Event 6510 second return suggestion must be train 7220, 19:33→20:19');
for (const [trip, expected] of [
  ['to-svetlogorsk-20260712-6717', ['UID:transport-6510-outbound-2026-07-12-6717@kenigevents.ru', 'DTSTART:20260712T131100Z', 'DTEND:20260712T140500Z', 'TRIGGER:-PT30M']],
  ['to-svetlogorsk-20260712-7213', ['UID:transport-6510-outbound-2026-07-12-7213@kenigevents.ru', 'DTSTART:20260712T134300Z', 'DTEND:20260712T142900Z', 'TRIGGER:-PT30M', 'LOCATION:Калининград-Северный']],
  ['to-kaliningrad-20260712-6722', ['UID:transport-6510-return-2026-07-12-6722@kenigevents.ru', 'DTSTART:20260712T165400Z', 'TRIGGER:-PT30M']],
  ['to-kaliningrad-20260712-7220', ['UID:transport-6510-return-2026-07-12-7220@kenigevents.ru', 'DTSTART:20260712T173300Z', 'TRIGGER:-PT30M']],
]) {
  const href = `/sobytiya/${transportDemoEvent.slug}/transport/${trip}.ics`;
  if (!transportDemoHtml.includes(`href="${buildId ? `/${buildId}` : ''}${href}"`)) throw new Error(`Event 6510 misses calendar CTA for ${trip}`);
  const tripIcs = readFileSync(join(root, href.slice(1)), 'utf8');
  for (const needle of expected) if (!tripIcs.includes(needle)) throw new Error(`Transport ICS ${trip} missing ${needle}`);
  if (tripIcs.includes('Проверить расписание') || tripIcs.includes('rasp.yandex')) throw new Error(`Transport ICS ${trip} must not expose a schedule verification link`);
}

const cutoffEvent = eventsData.events.find((event) => event.id === 6397);
if (!cutoffEvent) throw new Error('Missing real event 6397 schedule-cutoff regression event');
const cutoffHtml = stripGeneratedCode(readFileSync(join(root, `sobytiya/${cutoffEvent.slug}/index.html`), 'utf8'));
if (!cutoffHtml.includes('data-event-end-basis="schedule_cutoff"') || !cutoffHtml.includes('data-outbound-count="1"') || !cutoffHtml.includes('data-return-count="0"')) throw new Error('Event 6397 must expose one outbound train and a factual schedule cutoff');
if (!/data-transport-direction="outbound"[^>]*data-train-number="6725"[\s\S]*?<time[^>]*>19:26<\/time>[\s\S]*?<time[^>]*>20:23<\/time>/u.test(cutoffHtml)) throw new Error('Event 6397 outbound must be 6725, 19:26→20:23 (67 minutes before start)');
for (const needle of ['Обратно после события', 'Последний поезд в день события — в 22:40', '22:40 → 23:35', 'Ночных рейсов нет', 'Первый поезд 13 июля — в 06:25', 'Уточните время окончания у организатора']) {
  if (!cutoffHtml.includes(needle)) throw new Error(`Event 6397 schedule-cutoff block missing ${needle}`);
}
for (const trip of ['to-svetlogorsk-20260712-6725', 'to-kaliningrad-20260712-6730', 'to-kaliningrad-20260713-6700']) {
  const href = `/sobytiya/${cutoffEvent.slug}/transport/${trip}.ics`;
  if (!cutoffHtml.includes(`href="${buildId ? `/${buildId}` : ''}${href}"`)) throw new Error(`Event 6397 misses calendar CTA for ${trip}`);
  const tripIcs = readFileSync(join(root, href.slice(1)), 'utf8');
  if (!tripIcs.includes('BEGIN:VCALENDAR') || tripIcs.includes('Проверить расписание') || tripIcs.includes('rasp.yandex')) throw new Error(`Event 6397 transport ICS ${trip} is invalid or leaks a verification link`);
}
if (cutoffHtml.includes('типовую длительность') || cutoffHtml.includes('Ориентировочно после 23:30') || cutoffHtml.includes('подходящего поезда в Калининград нет')) throw new Error('Event 6397 must not infer a no-return state from a default duration');

const busDemoEvent = eventsData.events.find((event) => event.id === 6710);
if (!busDemoEvent) throw new Error('Missing real event 6710 Romanovo bus regression event');
const busDemoHtml = stripGeneratedCode(readFileSync(join(root, `sobytiya/${busDemoEvent.slug}/index.html`), 'utf8'));
if (!busDemoHtml.includes('data-event-bus-schedule') || !busDemoHtml.includes('data-bus-route="romanovo-holmogorye"') || !busDemoHtml.includes('class="event-bus__body"')) throw new Error('Event 6710 must expose the compact Romanovo bus block');
if (busDemoHtml.includes('data-event-transport-schedule')) throw new Error('Romanovo event 6710 must not expose a train block');
for (const needle of ['<img class="event-bus__symbol"', 'bus-svgrepo-337651.svg', 'До Романово — около 1 часа в автобусе', 'От автовокзала до остановки «Северный вокзал» у всех маршрутов — ориентировочно 10–15 минут', 'data-bus-number="118/118А"', '07:40', '08:00', '08:40', 'data-bus-number="119"', '08:10', 'data-bus-return-number="118/118А"', '≈ 13:15', '≈ 17:55', 'data-bus-return-number="119"', '≈ 13:45', '≈ 16:05', 'после 1 ч 15 мин на площадке', 'Открыто по субботам с 11:00 до 16:00', 'romanovo-holmogorye-route-square.png', 'romanovo-holmogorye-route-portrait.png', 'rtt=pd']) {
  if (!busDemoHtml.includes(needle)) throw new Error(`Event 6710 bus block missing ${needle}`);
}
if ((busDemoHtml.match(/Открыть пеший маршрут/gu) || []).length !== 1 || busDemoHtml.includes('Северный ≈') || busDemoHtml.includes('>06:00</time>') || busDemoHtml.includes('>06:20</time>') || busDemoHtml.includes('>06:55</time>') || busDemoHtml.includes('≈ 11:55') || busDemoHtml.includes('≈ 12:35') || busDemoHtml.includes('≈ 18:35') || busDemoHtml.includes('≈ 21:55') || busDemoHtml.includes('≈ 22:35') || busDemoHtml.includes('≈ 46 мин в автобусе')) throw new Error('Event 6710 bus block must avoid too-early outbound/return options, duplicated labels and pointless late returns');
if (!busDemoHtml.includes('<picture') || !busDemoHtml.includes('media="(max-width: 720px)"') || !busDemoHtml.includes('loading="eager"') || !busDemoHtml.includes('Открыть пеший маршрут') || !busDemoHtml.includes('Точка на карте')) throw new Error('Event 6710 bus block must eagerly use responsive route-focused maps and one external route link');
if (busDemoHtml.includes('avl39.ru/routes/reg/kaliningrad/')) throw new Error('Event 6710 bus block must not expose a schedule verification link');

const todayHtml = readFileSync(join(root, 'segodnya/index.html'), 'utf8');
if (/Мосийенко|Мосиенко/u.test(todayHtml)) throw new Error('Today listing must not show the false long-range Evgeny Mosiyenko lecture/exhibition item');
if (!todayHtml.includes('listing-daypart--continuing') || !todayHtml.includes('Идут сейчас')) throw new Error('Today listing must separate continuing multi-day exhibitions when they would overcrowd the fast daypart list');
const tomorrowHtml = readFileSync(join(root, 'zavtra/index.html'), 'utf8');
for (const [name, html] of [['today', todayHtml], ['tomorrow', tomorrowHtml]]) {
  for (const label of ['Утро', 'День', 'Вечер', 'Ночь']) {
    if (!html.includes(`>${label}</h3>`)) throw new Error(`${name} listing misses daypart section ${label}`);
  }
  if (!html.includes('listing-daypart') || !html.includes('listing-item__media listing-item__media--cover')) throw new Error(`${name} listing misses plaque/cropped media listing contract`);
  if (!html.includes('data-listing-filter') || !html.includes('data-listing-filter-bar') || !html.includes('listing-mode-switch') || !html.includes('role="radiogroup"') || !html.includes('data-listing-mode-button="personal"') || !html.includes('data-listing-hidden-count')) throw new Error(`${name} listing misses global All/For me personalization switch and hidden count UI`);
  const listingArticles = [...html.matchAll(/<article class="listing-item"[\s\S]*?<\/article>/giu)].map((match) => match[0]);
  if (!listingArticles.length) throw new Error(`${name} listing has no listing articles`);
  if (listingArticles.some((article) => !article.includes('data-listing-item') || !article.includes('data-linked-event-ids'))) throw new Error(`${name} listing items must expose compact ids for local personalization filter`);
  if (listingArticles.some((article) => !/<a class="listing-item__title"[\s\S]*?<div class="listing-item__meta">/u.test(article))) throw new Error(`${name} listing cards must show title before date/admission meta`);
  if (listingArticles.some((article) => {
    const hrefs = [...article.matchAll(/<a[^>]+href="(https?:\/\/[^"]+)"/giu)].map((match) => match[1]);
    return hrefs.some((href) => !href.startsWith('https://static.kenigevents.ru/ics/'));
  })) throw new Error(`${name} listing card leaks direct external http link`);
  if (listingArticles.some((article) => article.includes('Открыть пост организатора') || article.includes('Уточнить регистрацию'))) throw new Error(`${name} listing exposes source/ambiguous external CTA copy`);
}

const weekendHtml = readFileSync(join(root, 'vyhodnye/index.html'), 'utf8');
for (const [name, html] of [['today', todayHtml], ['tomorrow', tomorrowHtml], ['weekend', weekendHtml]]) {
  if (!html.includes('data-personal-feed-section') || !html.includes('data-personal-feed-slot')) throw new Error(`${name} listing misses dynamic personal-feed slot`);
  if (!html.includes('hidden') || !html.includes('Личная лента')) throw new Error(`${name} personal feed must be hidden until backend/cache returns cards`);
}
if (!controlHtml.includes('ke_listing_personal_feed_cache_v1') || !controlHtml.includes('get_listing_personal_feed_v1') || !controlHtml.includes('/rest/v1/rpc/')) throw new Error('Layout misses Supabase RPC/localStorage personal feed preparation');
if (!controlHtml.includes('ke_listing_mode_v1') || !controlHtml.includes('syncListingPersonalFilter') || !controlHtml.includes('data-listing-hidden-count') || !controlHtml.includes("explicitMode || (hiddenCount > 0 ? 'personal' : 'all')") || !controlHtml.includes('hydrateListingFilterFooterGuard')) throw new Error('Layout misses local listing personalization switch/hide/footer-guard contract');
const assetBaseUrl = (process.env.PUBLIC_ASSET_BASE_URL || '').replace(/\/+$/u, '');
const icsBaseUrl = (process.env.PUBLIC_ICS_BASE_URL || (assetBaseUrl ? `${assetBaseUrl}/ics` : '')).replace(/\/+$/u, '');
const astroAssetBaseUrl = (process.env.PUBLIC_ASTRO_ASSET_BASE_URL || '')
  .replace(/\{buildId\}/g, buildId)
  .replace(/\{BUILD_ID\}/g, buildId)
  .replace(/\/+$/u, '');
if (assetBaseUrl) {
  if (controlHtml.includes('https://storage.yandexcloud.net/kenigevents/')) throw new Error('CDN-enabled HTML must not emit raw Object Storage image URLs');
  if (!controlHtml.includes(`${assetBaseUrl}/p/`)) throw new Error('CDN-enabled event HTML must emit event image URLs through PUBLIC_ASSET_BASE_URL');
  if (!JSON.stringify(eventJsonLd?.image || []).includes(`${assetBaseUrl}/p/`)) throw new Error('CDN-enabled JSON-LD Event.image must use PUBLIC_ASSET_BASE_URL');
  if (controlHtml.includes(`rel="canonical" href="${assetBaseUrl}`)) throw new Error('Canonical URL must remain on kenigevents.ru, not asset CDN');
  if (!controlHtml.includes(`href="${icsBaseUrl}/${control.id}.ics"`)) throw new Error('CDN-enabled pages must link calendar CTA to stable /ics/<event_id>.ics');
}
if (astroAssetBaseUrl) {
  if (!controlHtml.includes(`href="${astroAssetBaseUrl}/_astro/`)) throw new Error('Astro CSS/JS assets must use PUBLIC_ASTRO_ASSET_BASE_URL when enabled');
  if (controlHtml.includes(`rel="canonical" href="${astroAssetBaseUrl}`)) throw new Error('Canonical URL must remain on kenigevents.ru, not static asset CDN');
  if (!assetBaseUrl && controlHtml.includes(`${astroAssetBaseUrl}/p/`)) throw new Error('PUBLIC_ASTRO_ASSET_BASE_URL must not rewrite event media images; use PUBLIC_ASSET_BASE_URL only for a media CDN');
}
const cssFiles = readdirSync(join(root, '_astro')).filter((name) => name.endsWith('.css'));
const css = cssFiles.map((name) => readFileSync(join(root, '_astro', name), 'utf8')).join('\n');
if (/native-share-button\{display:none/u.test(css)) throw new Error('Native share button is hidden by default');
if (/media-backdrop|image-backdrop|--poster-image|background-image:\s*linear-gradient\([^;]*var\(--poster-image\)|blur\(/u.test(css)) throw new Error('Duplicate/backdrop poster fill leaked into CSS');
if (/event-card__media-shell--preserve[^}]*object-fit:\s*contain/iu.test(css)) throw new Error('OCR-safe card media must not use contain over a fixed frame');
if (!/event-hero--poster-stage \.event-hero__image\{[^}]*object-fit:\s*contain/iu.test(css)) throw new Error('Poster-stage hero must contain OCR/text posters without crop');
if (!/event-hero--photo-cover \.event-hero__image\{[^}]*object-fit:\s*cover/iu.test(css)) throw new Error('Photo-cover hero must crop only visual-safe images');
if (!/event-hero--poster-billboard[\s\S]*?event-hero__visual[\s\S]*?width:\s*100vw/iu.test(css)) throw new Error('Poster billboard hero must make the hero visual full viewport width on mobile');
if (!/event-hero--poster-billboard\.event-hero--poster-stage \.event-hero__image[\s\S]*?\{[^}]*width:\s*100vw/iu.test(css)) throw new Error('Poster billboard hero image itself must be full viewport width on mobile');
if (!/mobile-discovery-menu__summary\{[^}]*background:\s*linear-gradient\([^}]*#793014[^}]*#a54821/iu.test(css) || !/body\.hero-chrome-immersive\.is-past-hero \.site-header/iu.test(css)) throw new Error('Immersive mobile header must use a site-palette discovery drawer handle and stable after-hero state');
if (
  !/mobile-discovery-menu\{[^}]*--drawer-rail-h[^}]*position:\s*fixed[^}]*transform:\s*translate3d\(0,\s*calc\(-1\s*\*\s*var\(--drawer-rail-h\)\s*-\s*env\(safe-area-inset-top\)\),\s*0\)/iu.test(css)
  || !/mobile-discovery-menu\[open\]\{[^}]*transform:\s*(?:translate3d\(0,\s*0,\s*0\)|translateZ\(0\))/iu.test(css)
  || !/mobile-discovery-menu__summary\{[^}]*top:\s*calc\(var\(--drawer-rail-h\)\s*\+\s*env\(safe-area-inset-top\)\s*-\s*7px\)/iu.test(css)
  || !/mobile-discovery-menu__panel\{[^}]*position:\s*absolute[^}]*width:\s*100vw[^}]*transform:\s*translateZ\(0\)[^}]*visibility:\s*hidden/iu.test(css)
  || !/@starting-style/iu.test(css)
  || !controlHtml.includes('closeMenu')
) throw new Error('Mobile discovery navigation must be a monolithic drawer: one root object slides down/up, with the handle attached to the panel and no transitional gap');
if (!/mobile-discovery-menu__panel\{[^}]*border-radius:\s*0/iu.test(css) || !/mobile-discovery-menu__links a\{[^}]*border:\s*0[^}]*border-radius:\s*0[^}]*background:\s*transparent/iu.test(css)) throw new Error('Mobile discovery drawer menu must be a flat rail with plain text links, not rounded/pill buttons');
if (/mobile-discovery-menu__brand-icon|brand-icon-mask|mobile-brand-icon-cycle|mobile-brand-text-cycle/iu.test(css)) throw new Error('Rejected brand icon animation must not remain in mobile discovery CSS');
if (!/mobile-discovery-menu__label\{[^}]*animation:\s*mobile-brand-title-sway/iu.test(css) || !/@keyframes\s+mobile-brand-title-sway/iu.test(css) || !/--brand-sway-x:\s*0px/iu.test(css) || !/translate3d\(var\(--brand-sway-x\),0,0\)/iu.test(css.replace(/\s+/g, ''))) throw new Error('Mobile discovery handle must keep only a calculated two-state title-sway animation');
if (!/mobile-discovery-menu__label\{[^}]*width:\s*100%[^}]*max-width:\s*100%/iu.test(css) || !/mobile-discovery-menu__summary\{[^}]*min-height:\s*calc\(5\.35rem\+env\(safe-area-inset-top\)\)/iu.test(css.replace(/\s+/g, ''))) throw new Error('Mobile discovery tag must preserve the gallery tag geometry and wrap the two-line service kicker instead of clipping it');
if (!/listing-item\{[^}]*grid-template-columns:\s*minmax\(132px,18%\)minmax\(0,1fr\)[^}]*padding:\s*0[^}]*overflow:\s*hidden/iu.test(css.replace(/\s+/g, '')) || !/listing-item__body\{[^}]*border-left:\s*1px solid/iu.test(css) || !/listing-item__media--cover \.listing-item__image\{[^}]*object-fit:\s*cover/iu.test(css)) throw new Error('Date listing cards must use parent-level plaque media crop with a straight separator');
if (!/event-hero--photo-cinematic-sheet\.event-hero--photo-cover \.event-hero__image[\s\S]*?event-hero--photo-parallax-sheet\.event-hero--photo-cover \.event-hero__image/iu.test(css) || !controlHtml.includes('hydrateHeroParallax')) throw new Error('Hero parallax must be enabled for visual-only cinematic/parallax heroes with reduced-motion-aware hydrator');
if (!/--hero-parallax-y/iu.test(css) || !/--hero-poster-parallax-y/iu.test(css) || !controlHtml.includes('const maxOffset = isPosterStage ? 56 : 64') || controlHtml.includes('--hero-parallax-scale')) throw new Error('Hero parallax must use stronger constant-scale vertical motion without dynamic zoom-scale jumps and OCR posters must move as one full-width visual without gray internal gaps');
if (!/hero-gallery\{[^}]*position:\s*fixed[^}]*z-index:\s*80/iu.test(css) || !/hero-gallery__image\{[^}]*height:\s*100%[^}]*object-fit:\s*contain/iu.test(css) || !controlHtml.includes('hydrateHeroGallery') || !controlHtml.includes('data-hero-gallery-next')) throw new Error('Hero fullscreen gallery must be fixed, full-height, controlled and preserve OCR/text images with the base contain mode');
if (!/hero-gallery__image\[data-image-text-mode=["']?visual_only["']?\]\{[^}]*object-fit:\s*cover/iu.test(css) || !/hero-gallery\[data-auto-pan=forward\][^}]*gallery-pan-forward/iu.test(css) || !/hero-gallery\[data-auto-pan=backward\][^}]*gallery-pan-backward/iu.test(css) || !/hero-gallery__viewport,\s*\.hero-gallery__track\{[^}]*touch-action:\s*none/iu.test(css)) throw new Error('Hero fullscreen gallery must crop visual-only photos with cover, one-way forward pan and reverse pan for manual back gestures');
if (!/@keyframes\s*gallery-pan-forward\{(?:from|0%)\{object-position:38%center\}to\{object-position:64%center\}/u.test(css.replace(/\s+/g, '')) || !/@keyframes\s*gallery-pan-backward\{(?:from|0%)\{object-position:64%center\}to\{object-position:38%center\}/u.test(css.replace(/\s+/g, ''))) throw new Error('Fullscreen gallery pan direction must be forward 38%→64% (right-to-left visual motion) and backward 64%→38%');
if (!/hero-gallery__topbar\{[^}]*padding:\s*0/iu.test(css) || !/hero-gallery__brand\{[^}]*pointer-events:\s*auto/iu.test(css)) throw new Error('Fullscreen gallery brand tag must be top-flush and clickable');
if (!/hero-gallery__caption\{[^}]*display:\s*block[^}]*padding:\s*0/iu.test(css) || !/hero-gallery__caption span\{[^}]*display:\s*block/iu.test(css) || !/hero-gallery__caption strong\{[^}]*box-decoration-break:\s*clone/iu.test(css)) throw new Error('Fullscreen gallery caption must be one fixed overlay with separate-line inline/subline text stripes, not per-slide flicker or a full-width bottom slab');
if (!controlHtml.includes('loadGalleryMedia') || !controlHtml.includes('preloadAdjacentGalleryMedia') || !controlHtml.includes('.decode().catch') || !controlHtml.includes('nextImageIndex') || !controlHtml.includes('animationend') || !controlHtml.includes('galleryPanTimer') || !controlHtml.includes('8880') || !/gallery-pan-forward 17\.9s/iu.test(css) || !controlHtml.includes('swipeSurface') || !controlHtml.includes('touchstart') || !controlHtml.includes('touchmove') || !controlHtml.includes('pointermove') || !/gallery-pan-forward/iu.test(css)) throw new Error('Hero gallery must lazy-load but pre-decode adjacent slides, keep ~40% slower pan, and auto-advance after the shorter non-dead viewing interval plus pointer/touch swipe');
if (!controlHtml.includes('data-not-interested-plate') || !/event-card__not-interest-plate/iu.test(css)) throw new Error('Not-interested feedback must keep an explicit undo plate instead of turning the card into an accidental navigation target');
if (/100vh/u.test(css)) throw new Error('Hero CSS must not use fragile 100vh units');
if (!/event-card--split-actions \.event-card__feedback\{[^}]*justify-content:\s*flex-end/iu.test(css)) throw new Error('Split-actions under-card row must cluster share text near the right-thumb like action');
if (!/event-card--split-actions \.event-card__feedback \.feedback-button\{[^}]*background:\s*transparent[^}]*border-color:\s*transparent/iu.test(css)) throw new Error('Split-actions under-card share/like must be icon-style, not pill buttons');
if (!/event-card--split-actions \.event-card__feedback \.feedback-button--share \.share-label\{[^}]*position:\s*static/iu.test(css)) throw new Error('Split-actions share must keep visible text under the card');
if (!/listing-filter-bar\{[^}]*position:\s*fixed[^}]*bottom:\s*0/iu.test(css) || !/listing-filter-bar\.is-visible\{[^}]*display:\s*block/iu.test(css) || !/body\.is-footer-visible \.listing-filter-bar/iu.test(css) || !/listing-mode-switch button\[aria-pressed=true\]/iu.test(css.replace(/\"/g, ''))) throw new Error('Listing personalization switch must be a fixed mobile segmented switch with footer overlap guard');
if (!/aspect-ratio:4\/5/u.test(css.replace(/\s+/g, ''))) throw new Error('Visual-only cover media must use vertical 4:5 ratio');
if (/aspect-ratio:3\/4/u.test(css.replace(/\s+/g, ''))) throw new Error('Old 3:4 visual-only ratio leaked into CSS');

const eventsById = new Map(eventsData.events.map((event) => [event.id, event]));
const currentDate = eventsData.build?.current_date;
const exactTodayEvents = eventsData.events.filter((event) => event.start_date === currentDate);
const exactTodayTypes = new Set(exactTodayEvents.map((event) => event.event_type || 'unknown'));
if (exactTodayEvents.length < 5 || exactTodayTypes.size < 4) throw new Error(`Preview fixture must include a diverse real same-day slice for /segodnya/, got ${exactTodayEvents.length} events and ${exactTodayTypes.size} types`);
const priceLinkedEvent = eventsData.events.find((event) => event.ticket?.price_label && /^https?:\/\//iu.test(event.ticket?.href || ''));
if (priceLinkedEvent) {
  const priceHtml = readFileSync(join(root, `sobytiya/${priceLinkedEvent.slug}/index.html`), 'utf8');
  if (!priceHtml.includes('event-info-admission__main') || !priceHtml.includes(priceLinkedEvent.ticket.price_label) || !/rel="[^"]*nofollow[^"]*"/iu.test(priceHtml)) throw new Error(`Priced event ${priceLinkedEvent.id} must render its price as a nofollow ticket link, not extra CTA copy`);
}
for (const event of eventsData.events) {
  const related = relatedData.related[String(event.id)] || { similar: [], explore: [] };
  const excluded = new Set([event.id, ...event.other_date_ids]);
  for (const [kind, ids] of Object.entries(related).filter(([, value]) => Array.isArray(value))) {
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
  const ownCalendarHrefCandidates = [
    icsBaseUrl ? `${icsBaseUrl}/${event.id}.ics` : null,
    `https://static.kenigevents.ru/ics/${event.id}.ics`,
    `/sobytiya/${event.slug}/event.ics`,
  ].filter(Boolean);
  const ownCalendarHrefPattern = new RegExp(`href=\"[^\"]*(?:/ics/${event.id}\\.ics|/sobytiya/${event.slug}/event\\.ics)(?:[?#][^\"]*)?\"`, 'u');
  const hasOwnCalendarLink = ownCalendarHrefCandidates.some((href) => html.includes(href)) || ownCalendarHrefPattern.test(html);
  const calendarEligible = !event.end_date || event.end_date === event.start_date;
  if (calendarEligible && !hasOwnCalendarLink) throw new Error(`Short event ${event.id} misses own calendar link`);
  if (!calendarEligible && hasOwnCalendarLink) throw new Error(`Multi-day event ${event.id} must not expose own calendar link`);
  for (const [label, pattern] of badHtmlPatterns) {
    if (pattern.test(visibleHtml)) throw new Error(`Rendered page ${event.id} contains ${label}`);
  }
  if (!event.address && html.includes('Открыть на карте')) throw new Error(`Weak-address event ${event.id} shows map CTA`);
  if (event.ticket.kind === 'source' && !event.ticket.is_free && html.includes('Билеты в продаже')) {
    throw new Error(`Source-only page ${event.id} shows misleading ticket-sale copy`);
  }
}
console.log(`Preview checks passed for ${buildId}`);
