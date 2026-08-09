import { createHash } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const sha = (value) => createHash('sha256').update(String(value)).digest('hex');
const lineNumber = (source, offset) => source.slice(0, offset).split('\n').length;
const bounded = (value, limit = 480) => String(value).replace(/\s+/gu, ' ').trim().slice(0, limit);
const DECODER_REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '../../..');
const MAX_MATCHED_RULES_PER_DOCUMENT = 256;
const NON_UI_CREDENTIAL_LINE = /(?:\b[A-Z][A-Z0-9_]{2,}_(?:KEY|TOKEN|SECRET|PASSWORD|URL|URI|ENDPOINT|AUTHORIZATION)\d*\b|\b(?:access|refresh|authorization|api|publishable|service)[_-]?(?:key|token|secret)\b|https?:\/\/\S+)/iu;
export const sanitizeRequirementEvidence = (value, limit = 480) => {
  const raw = String(value);
  if (NON_UI_CREDENTIAL_LINE.test(raw)) return `<redacted-non-ui-credential-line:${sha(raw).slice(0,16)}:${raw.length}>`;
  return bounded(raw
  .replace(/((?:[A-Z0-9_]*(?:KEY|TOKEN|SECRET|PASSWORD|URL|URI|ENDPOINT|AUTHORIZATION)[A-Z0-9_]*)\s*=\s*)([^\s\\]+)/giu,
    (_match, prefix, sensitive) => `${prefix}<redacted:${sha(sensitive).slice(0,16)}:${String(sensitive).length}>`)
  .replace(/\bsb_(?:secret|publishable)[A-Za-z0-9_.-]*/giu,
    (sensitive) => `<redacted:${sha(sensitive).slice(0,16)}:${sensitive.length}>`), limit);
};

export const REQUIREMENT_STATUS = Object.freeze([
  'accepted-current', 'implemented-current', 'accepted-not-implemented',
  'experiment-unresolved', 'historical-replaced', 'proposal-only', 'conflict', 'unresolved',
]);

export const REQUIREMENT_SOURCES = Object.freeze([
  ['docs/features/static-site-pages/page-archetype-source-map.md', 'page-archetype-authority'],
  ['docs/features/static-site-pages/image-framing.md', 'media-policy'],
  ['docs/features/static-site-pages/README.md', 'page-archetype-authority'],
  ['docs/features/static-site-pages/release-ui-contract.md', 'page-archetype-authority'],
  ['docs/features/static-site-pages/schedule-user-requirements.md', 'schedule-and-favorites'],
  ['docs/features/static-site-pages/rail-multimodal-directory.md', 'transport'],
  ['docs/features/static-site-pages/event-page-product-design.md', 'event-detail'],
  ['docs/features/static-site-pages/event-desktop-media-families-2026-07-12.md', 'desktop-event-media'],
  ['docs/features/static-site-pages/event-card-ui-ab-2026-06-27.md', 'event-card'],
  ['docs/features/static-site-pages/event-page-merged-skeleton.md', 'event-detail'],
  ['docs/features/static-site-pages/event-mobile-ui-lab-2026-07-15.md', 'event-detail-mobile'],
  ['docs/features/static-site-pages/mobile-shell.md', 'mobile-shell'],
  ['docs/features/static-site-pages/event-transport-schedule.md', 'transport'],
  ['docs/features/static-site-pages/listing-personal-feed.md', 'personalization'],
  ['docs/features/static-site-pages/personalizaion/requirements.md', 'personalization'],
  ['docs/features/static-site-pages/personalizaion/personalization-to-be.md', 'personalization'],
  ['docs/features/static-site-pages/personalizaion/personalization-implementation-contract.md', 'personalization'],
  ['docs/features/static-site-pages/personalizaion/personalization-current-runtime-audit-2026-08-02.md', 'personalization'],
  ['docs/features/unsigned-personalization/requirements.md', 'personalization'],
  ['docs/features/unsigned-personalization/personal-feed-architecture.md', 'personalization'],
  ['docs/features/static-site-pages/smart-vector-search/smart-vector-search-requirements.md', 'search'],
  ['docs/features/static-site-pages/smart-vector-search/README.md', 'search'],
  ['docs/features/unsigned-personalization/authorized-event-search.md', 'search'],
  ['docs/features/static-site-pages/listing-surfaces-v14-product.md', 'listing'],
  ['docs/features/static-site-pages/listing-surfaces-v16-product.md', 'listing'],
  ['docs/features/static-site-pages/listing-surfaces-v17-product.md', 'listing'],
  ['docs/features/static-site-pages/listing-surfaces-v18-product.md', 'listing'],
  ['docs/features/static-site-pages/listing-surfaces-v19-product.md', 'listing'],
  ['docs/features/static-site-pages/listing-surfaces-v20-mobile-popular.md', 'listing'],
  ['docs/features/static-site-pages/listing-surfaces-v21-mobile-popular.md', 'listing'],
  ['docs/features/static-site-pages/listing-surfaces-v22-popular-breakpoint-restore.md', 'listing'],
  ['docs/features/static-site-pages/listing-surfaces-v23-mobile-adaptive.md', 'listing'],
  ['docs/features/static-site-pages/listing-surfaces-v24-mobile-pinch.md', 'listing'],
  ['docs/features/static-site-pages/listing-surfaces-v25-mobile-context.md', 'listing'],
  ['docs/features/static-site-pages/listing-surfaces-v26-mobile-sticky-groups.md', 'listing'],
  ['docs/features/static-site-pages/listing-surfaces-v27-desktop-recovery.md', 'listing'],
  ['docs/features/static-site-pages/listing-surfaces-v28-desktop-popular.md', 'listing'],
]);

const DOCUMENT_DEFAULT_STATUS = Object.freeze({
  'docs/features/static-site-pages/page-archetype-source-map.md': 'accepted-current',
  'docs/features/static-site-pages/image-framing.md': 'accepted-current',
  'docs/features/static-site-pages/README.md': 'accepted-current',
  'docs/features/static-site-pages/release-ui-contract.md': 'accepted-current',
  'docs/features/static-site-pages/schedule-user-requirements.md': 'accepted-current',
  'docs/features/static-site-pages/rail-multimodal-directory.md': 'accepted-current',
  'docs/features/static-site-pages/event-page-product-design.md': 'accepted-current',
  'docs/features/static-site-pages/event-desktop-media-families-2026-07-12.md': 'accepted-current',
  'docs/features/static-site-pages/event-card-ui-ab-2026-06-27.md': 'historical-replaced',
  'docs/features/static-site-pages/event-page-merged-skeleton.md': 'historical-replaced',
  'docs/features/static-site-pages/event-mobile-ui-lab-2026-07-15.md': 'experiment-unresolved',
  'docs/features/static-site-pages/mobile-shell.md': 'accepted-current',
  'docs/features/static-site-pages/event-transport-schedule.md': 'accepted-current',
  'docs/features/static-site-pages/listing-personal-feed.md': 'conflict',
  'docs/features/static-site-pages/personalizaion/requirements.md': 'accepted-current',
  'docs/features/static-site-pages/personalizaion/personalization-to-be.md': 'accepted-not-implemented',
  'docs/features/static-site-pages/personalizaion/personalization-implementation-contract.md': 'accepted-not-implemented',
  'docs/features/static-site-pages/personalizaion/personalization-current-runtime-audit-2026-08-02.md': 'conflict',
  'docs/features/unsigned-personalization/requirements.md': 'historical-replaced',
  'docs/features/unsigned-personalization/personal-feed-architecture.md': 'conflict',
  'docs/features/static-site-pages/smart-vector-search/smart-vector-search-requirements.md': 'historical-replaced',
  'docs/features/static-site-pages/smart-vector-search/README.md': 'accepted-current',
  'docs/features/unsigned-personalization/authorized-event-search.md': 'implemented-current',
  'docs/features/static-site-pages/listing-surfaces-v14-product.md': 'historical-replaced',
  'docs/features/static-site-pages/listing-surfaces-v16-product.md': 'historical-replaced',
  'docs/features/static-site-pages/listing-surfaces-v17-product.md': 'historical-replaced',
  'docs/features/static-site-pages/listing-surfaces-v18-product.md': 'conflict',
  'docs/features/static-site-pages/listing-surfaces-v19-product.md': 'conflict',
  'docs/features/static-site-pages/listing-surfaces-v20-mobile-popular.md': 'historical-replaced',
  'docs/features/static-site-pages/listing-surfaces-v21-mobile-popular.md': 'historical-replaced',
  'docs/features/static-site-pages/listing-surfaces-v22-popular-breakpoint-restore.md': 'conflict',
  'docs/features/static-site-pages/listing-surfaces-v23-mobile-adaptive.md': 'accepted-current',
  'docs/features/static-site-pages/listing-surfaces-v24-mobile-pinch.md': 'accepted-current',
  'docs/features/static-site-pages/listing-surfaces-v25-mobile-context.md': 'accepted-current',
  'docs/features/static-site-pages/listing-surfaces-v26-mobile-sticky-groups.md': 'accepted-current',
  'docs/features/static-site-pages/listing-surfaces-v27-desktop-recovery.md': 'accepted-current',
  'docs/features/static-site-pages/listing-surfaces-v28-desktop-popular.md': 'accepted-current',
});

// Manually reconciled rules are deliberately small and exact.  The broader
// line inventory below remains useful discovery evidence, but cannot override
// these reviewed requirements with filename/keyword heuristics.
const CURATED_REQUIREMENTS = Object.freeze([
  ['authority.redirect-stub','docs/features/static-site-pages/requitements.md',1,7,null,null,'page-archetype-authority','The legacy requitements.md file is a redirect stub and is not current authority.','historical-replaced'],
  ['authority.page-archetype-map','docs/features/static-site-pages/page-archetype-source-map.md',1,139,'3a729432',null,'page-archetype-authority','The accepted source map binds product requirements to verified current routes while explicitly refusing visual-archetype acceptance; its requirement plane is newer than the ef7 UI source pin and remains separately provenance-bound.','accepted-current'],
  ['authority.release-contract','docs/features/static-site-pages/release-ui-contract.md',3,20,null,null,'page-archetype-authority','The release UI contract is the current cross-page acceptance authority; labs and historical previews do not redefine it.','accepted-current'],
  ['schedule.user-requirements','docs/features/static-site-pages/schedule-user-requirements.md',1,null,'5ac1b488',null,'schedule-and-favorites','The source requirements for saved-event reminders, calendar continuity and schedule behavior remain requirement evidence; implementation must be proven independently.','accepted-current'],
  ['transport.multimodal-directory','docs/features/static-site-pages/rail-multimodal-directory.md',1,null,'9d669856',null,'TransportDirectory','The official-source rail and multimodal directory is current reference evidence, with public rail UI bounded to its documented reviewed destinations and exact-date exports.','accepted-current'],
  ['event-detail.two-desktop-families','docs/features/static-site-pages/event-desktop-media-families-2026-07-12.md',39,72,'c7d7459',81,'DesktopEventPage','Desktop Event Detail retains separate Editorial and Split AS-IS presentation families.','accepted-current'],
  ['event-detail.editorial-gate','site/src/lib/desktopEventPresentation.ts',60,92,'fe2e358',97,'DesktopEventPage','Editorial requires visual_only media with width >=1280, height >=720, and ratio >=1.25; portrait, document and low-resolution cases route Split.','implemented-current'],
  ['event-detail.cta-binding','site/src/components/DesktopEventActionPanel.astro',57,64,'322d9d4',125,'DesktopEventActionPanel','Split binds inline CTA; Editorial binds stacked CTA. This is current behavior, not an unresolved CTA experiment.','implemented-current'],
  ['event-detail.mobile-lab-status','docs/features/static-site-pages/event-mobile-ui-lab-2026-07-15.md',1,5,null,null,'MobileEventPage','The mobile event UI lab preserves reviewed preview variants but explicitly is not the production contract; it remains experiment evidence.','experiment-unresolved'],
  ['media.hero-gallery','docs/features/static-site-pages/image-framing.md',7,35,'5b987a5',169,'EventHero','Hero/gallery visual_only media fills; OCR, text and unknown large media preserve document identity.','accepted-current'],
  ['media.mobile-rail','docs/features/static-site-pages/image-framing.md',135,161,'5b987a5',169,'MobileListingRailRow','Crop-safe classified visual media uses a 140x112 5:4 rail; OCR, unknown and unsafe media preserve authored geometry.','accepted-current'],
  ['media.non-rail-card','site/src/components/EventCard.astro',64,99,'7d96b42',125,'EventCard','Non-rail mobile visual cards use vertical 4:5 cover; OCR and unknown media use intrinsic geometry.','implemented-current'],
  ['media.desktop-related','docs/features/static-site-pages/image-framing.md',18,28,'5b987a5',169,'relatedCardLayout','Desktop related rows target horizontal 5:4; documents may crop only inside the bounded row policy.','accepted-current'],
  ['media.search','docs/features/unsigned-personalization/authorized-event-search.md',133,155,'4bebc80',144,'AuthorizedEventSearch','Search visual results use 5:4; document results preserve intrinsic ratio; unknown dimensions start reserved then reconcile natural dimensions.','accepted-current'],
  ['media.exhibitions','docs/features/static-site-pages/exhibitions-personal-prototype.md',160,204,null,null,'ExhibitionsPersonalSurface','Exhibition photo frames use 4:5, 1:1, 4:3 and 3:2 buckets; documents stay intrinsic.','implemented-current'],
  ['media.2x3-status',null,null,null,null,null,'cross-surface-media','2:3 is an intrinsic source orientation in the pinned corpus, not a normative universal frame token.','unresolved'],
  ['media.crop-policy-conflict','site/src/lib/relatedCardLayout.mjs',1,null,null,null,'relatedCardLayout','Current consumers use different crop gates; related visual cards remain fill-first while protected geometry improves focal positioning rather than vetoing cover.','conflict'],
  ['media.upscale-policy-conflict','site/src/components/EventHero.astro',1,null,null,null,'cross-surface-media','There is no global no-upscale rule: Hero, Listing, Split low-resolution and compact-card consumers retain different source-size policies.','conflict'],
  ['loading.search','docs/features/unsigned-personalization/authorized-event-search.md',173,224,'4bebc80',144,'AuthorizedEventSearch','Search skeleton is shown only for a real request and resolves into canonical EventCard results with progress, empty and error states.','accepted-current'],
  ['loading.favorites','site/src/components/FavoritesSurface.astro',7,230,null,null,'FavoritesSurface','Favorites starts with three skeleton cards, resolves local data first, and optionally enriches from cloud without discarding local content on failure.','implemented-current'],
  ['loading.personal-feed','site/src/components/PersonalFeedSlot.astro',18,46,'cc61805',163,'PersonalFeedSlot','Personal feed has a live status and optional hidden region but no resolved-card-shaped skeleton.','implemented-current'],
  ['loading.static-rerank','docs/features/static-site-pages/README.md',59,75,null,144,'HomeColdStartFeed','Reranking and continuation must preserve useful static first-paint content.','accepted-current'],
  ['loading.personal-feed-doc-conflict','docs/features/static-site-pages/listing-personal-feed.md',40,86,'cc61805',163,'PersonalFeedSlot','The older feed document overstates mobile visibility and cross-navigation hint restoration relative to current source.','conflict'],
  ['loading.exhibitions','site/src/components/ExhibitionsPersonalSurface.astro',139,1515,null,null,'ExhibitionsPersonalSurface','Exhibition deck and dialog images have real loading, loaded and error states with stable geometry.','implemented-current'],
  ['search.source-brief-replaced','docs/features/static-site-pages/smart-vector-search/smart-vector-search-requirements.md',1,5,'6fb85f7f',null,'AuthorizedEventSearch','The original Search brief is retained as source evidence, while its own header delegates current authority to smart-vector-search/README.md.','historical-replaced'],
  ['search.canonical-contract','docs/features/static-site-pages/smart-vector-search/README.md',1,8,'60eda72a',372,'AuthorizedEventSearch','The Smart Search README is the current consolidated product, architecture and operating contract; runtime implementation remains separately evidenced.','accepted-current'],
  ['selection.version-conflict','docs/features/static-site-pages/design-system/README.md',83,96,'c6a679d',340,'ListingPersonalFilter','Documentation names v2 candidate while catalog names v3 candidate and current consumers request v2 behavior inside the v3 shell.','conflict'],
  ['personalization.durable-status','docs/features/static-site-pages/personalizaion/personalization-current-runtime-audit-2026-08-02.md',3,247,'93f5849',316,'PersonalizationRuntime','The local runtime is useful AS-IS behavior but remains NO-GO as a durable personalization system.','conflict'],
  ['personalization.owner-requirements','docs/features/static-site-pages/personalizaion/requirements.md',1,null,'c4fe6a37',null,'PersonalizationRuntime','The owner-authored personalization requirements are current product intent; individual implementation claims require separate source/runtime evidence.','accepted-current'],
  ['personalization.target-blueprint','docs/features/static-site-pages/personalizaion/personalization-to-be.md',1,34,null,null,'PersonalizationRuntime','The target personalization system is an implementation blueprint and explicitly not a description of a completed production loop.','accepted-not-implemented'],
  ['personalization.implementation-contract','docs/features/static-site-pages/personalizaion/personalization-implementation-contract.md',1,8,'52566838',null,'PersonalizationRuntime','The normative contract applies to new personalization work, while its own header records that the production durable loop is absent.','accepted-not-implemented'],
  ['personalization.unsigned-requirements','docs/features/unsigned-personalization/requirements.md',1,5,null,null,'PersonalizationRuntime','The earlier anonymous-personalization draft is historical input, not current authority over the newer owner requirements and implementation contract.','historical-replaced'],
  ['personalization.personal-feed-architecture','docs/features/unsigned-personalization/personal-feed-architecture.md',1,5,'b01b02ae',null,'PersonalFeedSlot','The static-catalog personal-feed MVP is implemented in bounded preview/source planes, while backend top-up and some prose claims remain optional, future or in conflict with the pinned runtime.','conflict'],
  ['personalization.remote-write','docs/features/static-site-pages/README.md',503,503,null,null,'PersonalizationRuntime','Durable first-party feedback persistence is accepted but not implemented; current likes/profile writes remain local.','accepted-not-implemented'],
  ['mobile-shell.current','docs/features/static-site-pages/mobile-shell.md',1,108,'64dd872',144,'Reference4MobileMenu','One Reference4 menu, bottom navigation, toast and auth runtime form the current mobile shell.','accepted-current'],
  ['listing.v14-v15-replaced','docs/features/static-site-pages/listing-surfaces-v14-product.md',1,5,null,null,'listing-surfaces','The historical path records the V14 to V15 correction; V16 explicitly superseded its layout decisions.','historical-replaced'],
  ['listing.v16-replaced','docs/features/static-site-pages/listing-surfaces-v16-product.md',1,5,null,null,'listing-surfaces','V16 is retained as history and is explicitly superseded by V17 except for rules later carried forward.','historical-replaced'],
  ['listing.v17-replaced','docs/features/static-site-pages/listing-surfaces-v17-product.md',1,5,null,null,'listing-surfaces','V17 layout is explicitly superseded by V18; retained data-truth rules are reconciled through later current documents rather than promoting the whole file.','historical-replaced'],
  ['listing.v18-mixed','docs/features/static-site-pages/listing-surfaces-v18-product.md',1,10,null,null,'listing-surfaces','V18 contains retained date-listing composition and media-truth rules, but V19 and later documents override bounded parts; the whole document cannot be labelled wholly current or wholly replaced.','conflict'],
  ['listing.v19-mixed','docs/features/static-site-pages/listing-surfaces-v19-product.md',1,7,null,null,'listing-surfaces','V19 remains a desktop regression baseline carried into V20/V22, while later V27/V28 contracts govern current shell and Popular behavior.','conflict'],
  ['listing.v20-replaced','docs/features/static-site-pages/listing-surfaces-v20-mobile-popular.md',1,6,null,null,'listing-surfaces','V20 mobile presentation was superseded; only explicitly inherited desktop/ranking evidence survives through later contracts.','historical-replaced'],
  ['listing.v21-rejected','docs/features/static-site-pages/listing-surfaces-v21-mobile-popular.md',1,10,null,null,'listing-surfaces','V21 explicitly rejects its Popular-only large-card reconstruction and is superseded by V22/V23.','historical-replaced'],
  ['listing.v22-mixed','docs/features/static-site-pages/listing-surfaces-v22-popular-breakpoint-restore.md',1,16,null,null,'listing-surfaces','V22 remains the accepted desktop Popular baseline, but its equal-column mobile claim is superseded by V23 and later mobile refinements.','conflict'],
  ['listing.current-chain','docs/features/static-site-pages/README.md',441,459,null,null,'listing-surfaces','Current supersession is consumer-scoped: retained V18/V19 date-listing rules, V22 desktop Popular, V23 canonical EventCard reuse, V24/V25 mobile density/context, V26 sticky groups, V27 desktop shell recovery and V28 current desktop Popular/eligibility.','accepted-current'],
  ['transport.no-winner','.codex/lanes/ab-transport-experiment/RESULTS.md',1,324,'e2f5a2b',69,'TransportTimetableExperiment','Three treatments remain registered; production is forced off and no winner receipt exists.','experiment-unresolved'],
]);

export const DYNAMIC_REGIONS = Object.freeze([
  { id:'search',path:'site/src/components/AuthorizedEventSearch.astro',route:'/poisk/',data_source:'authenticated Supabase Edge Function POST, JSON or NDJSON',trigger:'submit, Enter, restored PKCE draft, or More',resolved_component:'canonical EventCard split-actions',initial_html:'form and semantic progress plus hidden two-full-and-one-peek skeletons/results',runtime_wait:true,states:['idle','checking_auth','validation','loading','success','empty','error'],fallback:'honest error; no automatic retry for ambiguous cost-bearing POST',skeleton_status:'implemented-current',geometry_status:'shape-similar-not-exact-cls-proof',offline_status:'not-implemented',evidence_scope:'controlled-runtime-required' },
  { id:'favorites',path:'site/src/components/FavoritesSurface.astro',route:'/izbrannoe/',data_source:'same-origin static catalog plus local ids and optional my_saved_events_v1 cloud ids',trigger:'hydration, auth subscription, storage and saved-event events',resolved_component:'canonical EventCard',initial_html:'loading root with three skeleton cards',runtime_wait:true,states:['loading','ready','empty','error','cloud-loading','cloud-ready','cloud-error'],fallback:'local-first content survives cloud failure',skeleton_status:'implemented-current',geometry_status:'column-compatible-not-exact-card-height',offline_status:'not-implemented',evidence_scope:'controlled-runtime-required' },
  { id:'personal-feed',path:'site/src/components/PersonalFeedSlot.astro',route:'listing and event-detail consumers',data_source:'same-origin personal-feed JSON, optional resilient RPC, adjacent discovery manifest',trigger:'IntersectionObserver rootMargin 320px; idle fallback; desktop media-query retry',resolved_component:'canonical EventCard clones',initial_html:'live pending status and empty slot; listing contexts hidden',runtime_wait:true,states:['pending','personal','popular_fallback','unavailable'],fallback:'listing hides; desktop event detail uses popular fallback or honest unavailable',skeleton_status:'not-present-current',geometry_status:'not-reserved',offline_status:'not-implemented',evidence_scope:'controlled-runtime-required' },
  { id:'discovery-rerank',path:'site/src/layouts/EventLayout.astro',route:'event detail discovery feeds',data_source:'same-origin /data/discovery/<event>.json',trigger:'layout script initialization and More',resolved_component:'existing server EventCards reordered and appended',initial_html:'useful server-rendered discovery cards',runtime_wait:true,states:['static','reranked','appended','failure-static-fallback'],fallback:'static HTML remains; More hidden on failure',skeleton_status:'not-applicable-usable-static-content',geometry_status:'existing-card-geometry',offline_status:'not-implemented',evidence_scope:'candidate-runtime-or-controlled-runtime' },
  { id:'home-rerank',path:'site/src/components/HomeColdStartFeed.astro',route:'/',data_source:'local profile only',trigger:'hydration, storage and feedback events',resolved_component:'existing server EventCards reordered in place',initial_html:'all canonical cards server rendered',runtime_wait:false,states:['static_fallback','local_rerank'],fallback:'static order',skeleton_status:'not-applicable-no-runtime-wait',geometry_status:'existing-card-geometry-but-first-hydration-reorder-possible',offline_status:'not-applicable',evidence_scope:'candidate-runtime' },
  { id:'popular-personalized-row',path:'site/src/components/listings/PopularPersonalizedRow.astro',route:'/populyarnoe/',data_source:'local profile and server-rendered candidates',trigger:'desktop hydration at >=981px',resolved_component:'five existing candidate cards',initial_html:'section and candidates server-rendered but hidden',runtime_wait:false,states:['hidden','visible-five'],fallback:'fail-closed hidden',skeleton_status:'not-applicable-no-runtime-wait',geometry_status:'section-appears-after-hydration',offline_status:'not-applicable',evidence_scope:'controlled-runtime-required' },
  { id:'weather',path:'site/src/components/WeatherDateContext.astro',route:'date and weekend listing consumers',data_source:'same-origin no-store pointer then immutable force-cache snapshot',trigger:'component runtime initialization',resolved_component:'WeatherDateContext forecast',initial_html:'loading/busy aside with reserved min-height but visibility hidden',runtime_wait:true,states:['loading','ready','degraded','hidden-unavailable'],fallback:'all validation/fetch failures hide and collapse the mount',skeleton_status:'not-present-current',geometry_status:'reserved-while-hidden-then-collapses-on-failure',offline_status:'not-implemented',evidence_scope:'controlled-runtime-required' },
  { id:'exhibitions-gallery',path:'site/src/components/ExhibitionsPersonalSurface.astro',route:'/vystavki/',data_source:'server exhibition catalog; browser image loads only',trigger:'deck/fullscreen image load and gallery controls',resolved_component:'same exhibition deck/gallery image',initial_html:'static deck and cards with per-image skeletons',runtime_wait:true,states:['loading','loaded','error','dialog-closed','dialog-open'],fallback:'explicit broken-photo message in dialog; stable deck error surface',skeleton_status:'implemented-current',geometry_status:'fixed frame and intrinsic bucket dependent',offline_status:'not-applicable-image-error',evidence_scope:'candidate-runtime-or-controlled-runtime' },
  { id:'event-card-media',path:'site/src/components/EventCard.astro',route:'all canonical EventCard consumers',data_source:'browser image resource',trigger:'decode/load/error',resolved_component:'same EventCard media',initial_html:'intrinsic or policy-bound media shell aria-busy with shimmer',runtime_wait:true,states:['loading','loaded','missing','error-fallback'],fallback:'generated semantic fallback, broken image hidden',skeleton_status:'implemented-current',geometry_status:'ratio-or-intrinsic-reserved',offline_status:'not-applicable-image-error',evidence_scope:'candidate-runtime-or-controlled-runtime' },
  { id:'listing-card-media',path:'site/src/components/listings/ListingEventCard.astro',route:'desktop listing consumers',data_source:'browser image resource',trigger:'decode/load/error',resolved_component:'same ListingEventCard media',initial_html:'ke-skeleton frame',runtime_wait:true,states:['loading','loaded','error'],fallback:'error removes skeleton but has no explicit visible broken-media substitute',skeleton_status:'implemented-current',geometry_status:'source CSS and dimensions',offline_status:'not-applicable-image-error',evidence_scope:'controlled-runtime-required' },
  { id:'mobile-listing-media',path:'site/src/components/listings/MobileListingRailSurface.astro',route:'mobile listing routes',data_source:'browser image resources for server-rendered rows',trigger:'decode/load/error',resolved_component:'MobileListingRailRow media',initial_html:'fixed-height 112px rail media frame',runtime_wait:true,states:['loading','loaded','error'],fallback:'neutral stable frame on error',skeleton_status:'implemented-current',geometry_status:'112px fixed height and policy-derived width',offline_status:'not-applicable-image-error',evidence_scope:'candidate-runtime-or-controlled-runtime' },
  { id:'auth-runtime',path:'site/src/components/auth/StaticSiteAuthRuntime.astro',route:'shared EventLayout routes',data_source:'Supabase auth state plus local saved-event runtime',trigger:'layout initialization and auth events',resolved_component:'existing account and saved-event controls',initial_html:'static signed-out controls and canonical content',runtime_wait:true,states:['checking','signed_out','signed_in','error'],fallback:'signed-out or honest status; some initializer failures are swallowed',skeleton_status:'not-present-current',geometry_status:'existing-controls',offline_status:'not-implemented',evidence_scope:'controlled-runtime-required' },
  { id:'personalization-runtime',path:'site/src/components/personalization/PersonalizationRuntime.astro',route:'shared EventLayout routes',data_source:'local profile and shadow characterization',trigger:'layout initialization and action events',resolved_component:'hidden runtime markers; visible consumers act separately',initial_html:'usable static content remains',runtime_wait:false,states:['off','characterize','local-shadow'],fallback:'static content',skeleton_status:'not-applicable-hidden-behavior-runtime',geometry_status:'not-visible',offline_status:'not-applicable',evidence_scope:'source-and-controlled-runtime' },
]);

const CURATED_MEDIA_POLICIES = Object.freeze([
  {key:'event-card-4x5',surface:'EventCard non-rail mobile',semantic_media_type:'visual event photo',ratio:'4:5',fit:'cover',crop:'fill-first visual branch',ocr_document_restriction:'OCR/unknown uses intrinsic natural-height geometry',focal_point:'metadata or CSS focal',safe_area:'consumer metadata; no universal protected-geometry veto',upscaling:'no independent anti-upscale guarantee',fallback:'stable semantic fallback; broken img hidden',art_direction:'desktop event-detail continuation overrides to 1:1.04',status:'implemented-current'},
  {key:'mobile-rail-5x4',surface:'MobileListingRailRow',semantic_media_type:'classified safe event photo',ratio:'5:4',fit:'cover',crop:'only visual_only event_photo confidence>=0.9 safe_crop with focal',ocr_document_restriction:'OCR/unknown/unsafe is intrinsic contain',focal_point:'required for cover branch',safe_area:'safe_crop and classification gate',upscaling:'no independent CSS anti-upscale guarantee',fallback:'neutral stable 112px frame',art_direction:'physical 140x112 at <=720; each asset evaluated independently',status:'accepted-current'},
  {key:'listing-3x2',surface:'ListingEventCard',semantic_media_type:'safe listing photo',ratio:'3:2',fit:'cover-or-intrinsic',crop:'semantic gate plus crop retention >=70% portrait and >=80% landscape unless reviewed',ocr_document_restriction:'poster/OCR/unknown intrinsic contain',focal_point:'explicit focal where safe cover',safe_area:'consumer-local semantic gate',upscaling:'normal source >=256x180; bounded tiny fallback >=256x160 stays natural',fallback:'tiny natural fallback; no invented portrait',art_direction:'desktop media height 221; Weekend 178; mobile rail is separate component',status:'accepted-current'},
  {key:'split-2x3',surface:'DesktopEventPage Split',semantic_media_type:'portrait event photo or poster',ratio:'2:3',fit:'contain-or-natural',crop:'source orientation; not a universal frame token',ocr_document_restriction:'documents and unclassified media stay natural',focal_point:'not required for contain',safe_area:'source-preserving',upscaling:'low-resolution branches are case-specific',fallback:'event fallback art if missing',art_direction:'Split desktop family; portrait viewer contains',status:'unresolved'},
  {key:'exhibition-square',surface:'ExhibitionsPersonalSurface',semantic_media_type:'safe exhibition photo / identity document',ratio:'1:1',fit:'cover-or-intrinsic',crop:'only classified visual event_photo safe_crop with focal',ocr_document_restriction:'square OCR/identity stays intrinsic contain',focal_point:'required for crop-safe photo',safe_area:'consumer-local safe crop',upscaling:'minimum 640x438 for crop bucket',fallback:'1200x900 4:3 natural fallback',art_direction:'nearest P/S/W/L bucket; modal always contain',status:'implemented-current'},
  {key:'exhibition-4x3',surface:'ExhibitionsPersonalSurface',semantic_media_type:'safe exhibition photo',ratio:'4:3',fit:'cover',crop:'exact named W bucket; old force to 3:2 rejected',ocr_document_restriction:'documents remain intrinsic',focal_point:'explicit',safe_area:'safe crop',upscaling:'minimum 640x438 for crop bucket',fallback:'4:3 natural fallback',art_direction:'named bucket',status:'implemented-current'},
  {key:'intrinsic-documents',surface:'cross-surface document branches',semantic_media_type:'OCR/poster/document/unknown',ratio:'intrinsic/source',fit:'contain-or-natural',crop:'forbidden except bounded desktop row optimizer case',ocr_document_restriction:'identity and text legibility preserved',focal_point:'not used',safe_area:'whole source is protected',upscaling:'consumer-specific; never infer a global guarantee',fallback:'consumer-specific semantic missing state',art_direction:'mobileFlow, Listing, Split natural, Exhibition document-natural',status:'accepted-current'},
  {key:'editorial-primary',surface:'DesktopEventPage Editorial',semantic_media_type:'strong landscape visual photo',ratio:'>=5:4 source; common 3:2',fit:'cover',crop:'Editorial gate visual_only width>=1280 height>=720 ratio>=1.25',ocr_document_restriction:'document/portrait/low-resolution routes Split',focal_point:'metadata object-position; bottom-safe may force 50% 100%',safe_area:'family-local routing and crop evidence',upscaling:'strong-source gate',fallback:'Split or fallback art',art_direction:'separate desktop family with stacked CTA',status:'implemented-current'},
  {key:'event-hero',surface:'EventHero mobile',semantic_media_type:'hero photo/poster',ratio:'16:10-or-intrinsic',fit:'cover-or-contain',crop:'visual_only fill-first even without bbox; OCR/unknown contain',ocr_document_restriction:'poster stage width 100vw, auto height, top-centered contain',focal_point:'photo focal metadata',safe_area:'no global bbox requirement for visual_only hero',upscaling:'no-upscale cap only for single weak non-visual portrait',fallback:'deterministic presentation-only fallback art',art_direction:'accepted mobile may be auto-height or 64svh; no universal token',status:'implemented-current'},
  {key:'desktop-discovery',surface:'EventCard desktop event-detail continuation',semantic_media_type:'related event photo',ratio:'1:1.04',fit:'cover-or-document-policy',crop:'consumer override',ocr_document_restriction:'document optimizer retained',focal_point:'consumer',safe_area:'consumer',upscaling:'no global guarantee',fallback:'EventCard fallback',art_direction:'desktop >=1024 only',status:'implemented-current'},
  {key:'search-result',surface:'AuthorizedEventSearch result',semantic_media_type:'visual photo or document',ratio:'5:4',fit:'cover-or-intrinsic',crop:'visual fixed 5:4 cover',ocr_document_restriction:'document intrinsic contain; missing dimensions initially reserve then reconcile',focal_point:'related-card metadata',safe_area:'consumer-local',upscaling:'no separate promise',fallback:'EventCard missing fallback',art_direction:'mobile search result policy',status:'accepted-current'},
  {key:'search-skeleton',surface:'AuthorizedEventSearch skeleton',semantic_media_type:'loading placeholder',ratio:'5:4',fit:'not-applicable',crop:'not-applicable',ocr_document_restriction:'not-applicable',focal_point:'not-applicable',safe_area:'not-applicable',upscaling:'not-applicable',fallback:'hidden outside real wait',art_direction:'current exact source; older 4:5 wording is replaced',status:'implemented-current'},
  {key:'festival-16x10',surface:'Festival cards mobile',semantic_media_type:'photo or bounded document',ratio:'16:10',fit:'cover-or-bounded-document',crop:'visual fill; document <=20% crop policy',ocr_document_restriction:'document remains bounded',focal_point:'consumer',safe_area:'row policy',upscaling:'unresolved',fallback:'consumer fallback',art_direction:'additional .95 and 1.86 responsive variants exist',status:'implemented-current'},
  {key:'festival-desktop',surface:'Festival editorial desktop rows',semantic_media_type:'photo/document mosaic',ratio:'16:9-or-variable',fit:'cover-or-bounded-document',crop:'dynamic-programming row targets',ocr_document_restriction:'documents preserve bounded crop',focal_point:'consumer',safe_area:'row policy',upscaling:'unresolved',fallback:'consumer fallback',art_direction:'singleton 16:9; terminal singleton 3:1 or 2.3:1; row targets vary',status:'implemented-current'},
  {key:'medallion',surface:'EventTokenMedallions',semantic_media_type:'identity medallion/badge',ratio:'1:1-or-intrinsic',fit:'cover-or-contain',crop:'circle identity cover; badge/free contain; Pushkin authored frame contain',ocr_document_restriction:'not event-document media',focal_point:'center',safe_area:'authored asset',upscaling:'source and docs sizing conflict retained',fallback:'source-specific',art_direction:'listing/detail/top-slot sizes differ',status:'conflict'},
  {key:'artifact',surface:'Amber artifacts',semantic_media_type:'transparent collectible asset',ratio:'74:96',fit:'contain',crop:'forbidden',ocr_document_restriction:'not applicable',focal_point:'center',safe_area:'full transparent art',upscaling:'1x/2x/3x srcset',fallback:'question mark empty slot; broken-image fallback not explicit',art_direction:'rail canvas 94x112; collection/dialog sizes differ',status:'implemented-current'},
  {key:'focus-invite',surface:'Focus invite visual',semantic_media_type:'prototype illustration/QR',ratio:'1:1.18-or-1:1',fit:'contain',crop:'forbidden',ocr_document_restriction:'QR remains 1:1',focal_point:'center',safe_area:'whole source',upscaling:'unresolved',fallback:'prototype-specific',art_direction:'lab/focus scope',status:'proposal-only'},
  {key:'reference-menu-distortion',surface:'Reference4MobileMenu',semantic_media_type:'menu illustration',ratio:'1:1',fit:'fill',crop:'none but geometric distortion possible',ocr_document_restriction:'not applicable',focal_point:'not applicable',safe_area:'unresolved',upscaling:'112x112 from 333x332 source',fallback:'unresolved',art_direction:'mobile menu only',status:'conflict'},
  {key:'event-media-rail',surface:'EventMediaRail',semantic_media_type:'generic thumbnail rail',ratio:'1:1',fit:'cover',crop:'blind cover',ocr_document_restriction:'no semantic OCR gate',focal_point:'center',safe_area:'not implemented',upscaling:'small derivative selected',fallback:'consumer-specific',art_direction:'54px detail thumbnails; lab-only reachability',status:'conflict'},
]);

function gitFact(repoRoot, path) {
  try {
    const raw = execFileSync('git', ['-C', repoRoot, 'log', '-1', '--format=%H%x1f%cs%x1f%s', '--', path], { encoding: 'utf8' }).trim();
    const [commit, date, subject] = raw.split('\x1f');
    return { commit: commit || null, commit_date: date || null, commit_subject: subject || null, pr: subject?.match(/#(\d+)/u)?.[1] || null };
  } catch { return { commit: null, commit_date: null, commit_subject: null, pr: null }; }
}

function statusFor(path, line) {
  const documentStatus = DOCUMENT_DEFAULT_STATUS[path] || 'unresolved';
  if (/superseded|rejected|no longer used|historical\/rejected|deprecated|замен|отклон/iu.test(line)) return 'historical-replaced';
  if (/conflict|contradict|mismatch|расхожд|конфликт/iu.test(line)) return 'conflict';
  // A line-level keyword may narrow a current document, but it must never
  // promote a historical, mixed or proposal document back to current status.
  if (['historical-replaced', 'proposal-only', 'conflict'].includes(documentStatus)) return documentStatus;
  if (/experiment|a\/b|comparison|variant/iu.test(line) && !/accepted as the baseline|accepted-current/iu.test(line)) return 'experiment-unresolved';
  if (/accepted but not implemented|accepted-not-implemented|ещ[её] не реализ/iu.test(line)) return 'accepted-not-implemented';
  if (/proposal|candidate|prototype|draft|to-be|target/iu.test(line) && !/implemented|accepted|production contract|pixel-current/iu.test(line)) return 'proposal-only';
  if (/implemented|production event-page contract|pixel-current|accepted .*production|canonical contract/iu.test(line)) return 'accepted-current';
  return documentStatus;
}

export function buildRequirementsProvenance({ sourceRoot, requirementsRoot = DECODER_REPO_ROOT }) {
  const implementationRepoRoot = resolve(sourceRoot, '..');
  const requirementRepoRoot = resolve(requirementsRoot);
  const pattern = /(?:page|route|archetype|aspect|ratio|crop|cover|contain|object-position|focal|poster|photo|ocr|document|image|media|skeleton|loading|spinner|stale|refresh|empty|error|retry|offline|rail|scroll|sticky|fixed|pinned|cta|action|transport|menu|popover|filter|personal|recommend|favorite|calendar|search)/iu;
  const rows = [];
  for (const [path, surface] of REQUIREMENT_SOURCES) {
    const absolute = join(requirementRepoRoot, path);
    if (!existsSync(absolute)) {
      rows.push({
        id:`requirement.missing.${sha(path).slice(0,18)}`, source_path:path, section:null, line:null,
        commit:null, commit_date:null, commit_subject:null, pr:null, surface, component:null,
        rule:'Required archaeology document is absent from the decoder requirements plane.', status:'unresolved',
        evidence_kind:'missing-requirement-document', current_authority_claimed:false, decision:'NOT_MERGED',
      });
      continue;
    }
    const content = readFileSync(absolute, 'utf8'); const git = gitFact(requirementRepoRoot, path);
    const documentStatus = DOCUMENT_DEFAULT_STATUS[path] || 'unresolved';
    rows.push({
      id:`requirement.document.${sha(path).slice(0,18)}`, source_path:path, section:null, line:1, ...git,
      surface, component:null, rule:`Required archaeology document included with bounded line extraction (maximum ${MAX_MATCHED_RULES_PER_DOCUMENT} matched rules).`,
      status:documentStatus, evidence_kind:'requirement-document-inventory', current_authority_claimed:documentStatus==='accepted-current', decision:'NOT_MERGED',
    });
    const lines = content.split('\n'); let section = null; let matchedRules = 0;
    for (let index = 0; index < lines.length; index += 1) {
      const line = lines[index].trim(); if (/^#{1,6}\s/u.test(line)) section = bounded(line.replace(/^#{1,6}\s*/u, ''), 180);
      if (!line || !pattern.test(line)) continue;
      if (matchedRules >= MAX_MATCHED_RULES_PER_DOCUMENT) break;
      matchedRules += 1;
      const status = statusFor(path, line);
      rows.push({
        id: `requirement.${sha(`${path}\0${index + 1}\0${line}`).slice(0, 18)}`,
        source_path: path, section, line: index + 1, ...git, surface, component: null,
        rule: sanitizeRequirementEvidence(line), status, evidence_kind: 'pinned-requirement-document',
        current_authority_claimed: status === 'accepted-current', decision: 'NOT_MERGED',
      });
    }
  }
  for (const [key,path,line_start,line_end,commit,pr,component,rule,status] of CURATED_REQUIREMENTS) {
    rows.push({
      id:`requirement.curated.${key}`,
      source_path:path,
      section:null,
      line:line_start,
      line_end,
      commit,
      commit_date:null,
      commit_subject:null,
      pr:pr ? String(pr) : null,
      surface:component || 'cross-surface',
      component,
      rule,
      status,
      evidence_kind:path?.startsWith('site/src/') ? 'reviewed-pinned-source-rule' : path ? 'reviewed-requirement-reconciliation' : 'reviewed-corpus-absence',
      current_authority_claimed:status==='accepted-current',
      decision:'NOT_MERGED',
    });
  }
  const sourcePaths = new Set(DYNAMIC_REGIONS.map((item) => item.path));
  for (const path of sourcePaths) {
    const absolute = join(implementationRepoRoot, path); if (!existsSync(absolute)) continue;
    const content = readFileSync(absolute, 'utf8'); const git = gitFact(implementationRepoRoot, path);
    for (const match of content.matchAll(/[^\n]*(?:data-[\w-]*(?:loading|skeleton|state|error|empty)|fetch\(|aria-(?:busy|live)|object-fit|aspect-ratio|position:\s*(?:sticky|fixed))[^\n]*/giu)) {
      rows.push({
        id: `requirement.${sha(`${path}\0${match.index}\0${match[0]}`).slice(0, 18)}`,
        source_path: path, section: null, line: lineNumber(content, match.index), ...git,
        surface: DYNAMIC_REGIONS.find((item) => item.path === path)?.id || 'runtime', component: path.split('/').at(-1),
        rule: sanitizeRequirementEvidence(match[0]), status: 'implemented-current', evidence_kind: 'pinned-source-implementation',
        current_authority_claimed: false, decision: 'NOT_MERGED',
      });
    }
  }
  return [...new Map(rows.map((row) => [row.id, row])).values()].sort((a, b) => a.id.localeCompare(b.id));
}

function stateHits(content, state) {
  const matches = [...content.matchAll(new RegExp(`(?:data-[\\w-]*${state}|\\b${state}\\b)`, 'giu'))];
  return matches.slice(0, 24).map((match) => lineNumber(content, match.index));
}

export function buildDynamicRegionMatrix({ sourceRoot, provenance }) {
  const repoRoot = resolve(sourceRoot, '..');
  return DYNAMIC_REGIONS.map((region) => {
    const absolute = join(repoRoot, region.path); const content = existsSync(absolute) ? readFileSync(absolute, 'utf8') : '';
    const stateNames=[...new Set(['idle','loading','skeleton','spinner','optimistic','partial','stale','refreshing','empty','error','retry','offline','unavailable','success',...(region.states||[])])];
    const states = Object.fromEntries(stateNames
      .map((state) => [state, stateHits(content, state)]));
    const hasRuntimeWait = region.runtime_wait === true || /fetch\(|await\s|loading|skeleton/iu.test(content);
    const skeletonSelectors = [...content.matchAll(/(?:class|data-[\w-]+)=["'{][^\n>]*(?:skeleton|spinner)[^\n>]*/giu)].map((match) => bounded(match[0], 220)).slice(0, 24);
    return {
      ...region, id: `dynamic-region.${region.id}`,
      source_sha256: content ? sha(content) : null,
      fetch_trigger: region.trigger,
      usable_static_or_stale_fallback:region.fallback||(/static|existing|usable|canonical/iu.test(region.initial_html)?'present-by-contract':'requires-runtime-verification'),
      predictable_geometry:region.geometry_status||(/skeleton|reserved|fixed|ratio-bound|static cards/iu.test(`${region.initial_html} ${content}`)?'source-observed':'unresolved'),
      lifecycle_states: states,
      skeleton: skeletonSelectors.length ? { status: region.skeleton_status||'implemented-current', selectors: skeletonSelectors, resolved_component: region.resolved_component } : { status: region.skeleton_status||(hasRuntimeWait?'missing-or-unresolved':'not-applicable'), reason: hasRuntimeWait ? 'runtime wait exists but no explicit skeleton marker was found' : 'no runtime wait found', resolved_component:region.resolved_component },
      transition_sequence:region.states||[],
      offline_disposition:region.offline_status||'unresolved',
      accessibility_announcements: [...content.matchAll(/aria-(?:live|busy|atomic)=[^\s>]+/giu)].map((match) => bounded(match[0], 120)).slice(0, 24),
      runtime_evidence: region.evidence_scope||'behavior-packet-required', reachability: existsSync(absolute) ? 'source-implemented-runtime-verification-pending' : 'missing-pinned-source',
      provenance_ids: provenance.filter((item) => item.source_path === region.path).map((item) => item.id).slice(0, 64),
      decision: 'NOT_MERGED',
    };
  }).sort((a, b) => a.id.localeCompare(b.id));
}

function normalizeRatio(value) {
  const match = String(value).match(/(\d+(?:\.\d+)?)\s*\/\s*(\d+(?:\.\d+)?)/u);
  if (!match) return value === '1' ? '1:1' : value === 'auto' ? 'intrinsic/source' : bounded(value, 120);
  return `${Number(match[1])}:${Number(match[2])}`;
}

export function buildMediaPolicyMatrix({ media, provenance }) {
  const rows = [];
  for (const item of media) {
    const ratioValue = item.css_property === 'aspect-ratio' ? normalizeRatio(item.css_value) : item.intrinsic_dimensions?.width && item.intrinsic_dimensions?.height ? `${item.intrinsic_dimensions.width}:${item.intrinsic_dimensions.height}` : 'intrinsic/source-or-runtime';
    const policyText = `${item.selector || ''} ${item.css_property || ''} ${item.css_value || ''}`;
    rows.push({
      id: `media-policy.${sha(item.id).slice(0, 18)}`, media_observation_id: item.id, contract_id: item.contract_id,
      source_path: item.path, source_line: item.line, surface: item.contract_id, semantic_media_type: /medallion/iu.test(policyText) ? 'identity-medallion' : /poster|ocr|document/iu.test(policyText) ? 'poster-or-document' : /rail|preview|thumb/iu.test(policyText) ? 'preview-or-rail' : /hero/iu.test(policyText) ? 'hero' : 'event-media',
      ratio: ratioValue, orientation: ratioValue === 'intrinsic/source' || ratioValue.includes('runtime') ? 'source-dependent' : null,
      fit: /object-fit\s*contain|contain/iu.test(policyText) ? 'contain' : /object-fit\s*cover|cover/iu.test(policyText) ? 'cover' : 'source-or-runtime-dependent',
      crop: /cover/iu.test(policyText) ? 'crop-permitted-by-source-selector' : /contain/iu.test(policyText) ? 'crop-forbidden' : 'unresolved',
      ocr_document_restriction: /ocr|document|poster/iu.test(policyText) ? 'source-selector-observed' : 'requirement-provenance-dependent',
      focal_point: /object-position/iu.test(policyText) ? bounded(item.css_value) : 'consumer-or-metadata-dependent',
      face_text_safe_area: 'requirement-provenance-required', object_position: item.css_property === 'object-position' ? item.css_value : null,
      upscaling_tiny_source_policy: 'requirement-provenance-required', missing_broken_fallback: item.fallback || 'consumer-dependent',
      responsive_art_direction: item.responsive || 'consumer-dependent', requirement_provenance_ids: provenance.filter((row) => row.source_path?.endsWith('image-framing.md') || row.surface === 'desktop-event-media').map((row) => row.id).slice(0, 48),
      source_runtime_reachability: item.reachability, lifecycle_status: 'implemented-current-source-runtime-verification-pending', decision: 'NOT_MERGED',
    });
  }
  for(const policy of CURATED_MEDIA_POLICIES){
    rows.push({
      id:`media-policy.curated.${policy.key}`,
      media_observation_id:null,
      contract_id:null,
      source_path:null,
      source_line:null,
      surface:policy.surface,
      semantic_media_type:policy.semantic_media_type,
      ratio:policy.ratio,
      orientation:/intrinsic|source/iu.test(policy.ratio)?'source-dependent':policy.ratio,
      fit:policy.fit,
      crop:policy.crop,
      ocr_document_restriction:policy.ocr_document_restriction,
      focal_point:policy.focal_point,
      face_text_safe_area:policy.safe_area,
      object_position:'consumer-and-state-specific',
      upscaling_tiny_source_policy:policy.upscaling,
      missing_broken_fallback:policy.fallback,
      responsive_art_direction:policy.art_direction,
      requirement_provenance_ids:provenance.filter((row)=>row.surface===policy.surface||row.component===policy.surface||(/media/iu.test(row.surface)&&/media|Event|Listing|Search|Festival|Exhibition|Medallion|Artifact/iu.test(policy.surface))).map((row)=>row.id).slice(0,48),
      source_runtime_reachability:'reviewed-pinned-source-runtime-capture-required',
      lifecycle_status:policy.status,
      decision:'NOT_MERGED',
    });
  }
  for (const ratio of ['4:5','5:4','3:2','2:3','1:1','intrinsic/source']) if (!rows.some((row) => row.ratio === ratio)) rows.push({
    id: `media-policy.required-ratio-${ratio.replace(/[^a-z0-9]+/giu, '-')}`, media_observation_id: null, contract_id: null,
    source_path: null, source_line: null, surface: 'cross-surface-requirement', semantic_media_type: 'required-ratio-reconciliation', ratio,
    orientation: 'requirement-defined', fit: 'unresolved', crop: 'unresolved', ocr_document_restriction: 'unresolved', focal_point: 'unresolved',
    face_text_safe_area: 'unresolved', object_position: null, upscaling_tiny_source_policy: 'unresolved', missing_broken_fallback: 'unresolved', responsive_art_direction: 'unresolved',
    requirement_provenance_ids: provenance.filter((row) => row.rule.includes(ratio.replace(':', '/')) || row.rule.includes(ratio)).map((row) => row.id),
    source_runtime_reachability: 'requires-reconciliation', lifecycle_status: 'unresolved', decision: 'NOT_MERGED',
  });
  return rows.sort((a, b) => a.id.localeCompare(b.id));
}
