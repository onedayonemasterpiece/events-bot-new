# Astro SSG preview — event pages

> **Status:** implemented preview vertical slice, production rollout pending.  
> **Build ID:** current full-catalog review target `preview-20260702t1536-merged-vector-medallions` (399 active/future events from the 2026-07-02 production snapshot through event id `6613`, merged vector-identity gate + medallion SVG branches, `search_v3` + `related_v1`, Supabase pgvector, CDN media/ICS, smart-search UI). Historical same-day target: `preview-20260702t0755-fresh-ui-fixes`; historical full-catalog target: `preview-20260630-event-pages-v62-two-vector-gemma-full`; historical focus canary: `preview-20260629-event-pages-v59-related-gemma50`.
> **Preview index target:** <https://kenigevents.ru/preview-20260702t1536-merged-vector-medallions/__preview/>. Public Object Storage/CDN access and `static.kenigevents.ru` TLS are part of the deploy verification gate; current preview HTML/CSS/JS, partner logos, medallion assets and stable `https://static.kenigevents.ru/ics/<event_id>.ics` files are publicly testable after `npm --prefix site run deploy:preview`.

This is the first real Astro SSG implementation for `kenigevents.ru` event detail pages in `events-bot-new`. It is intentionally a preview-only static slice: no Supabase page-view write path, no personalization telemetry persistence on ordinary views, and no LLM fragments in rendered HTML. The first event-detail discovery hydration is a static same-origin JSON manifest; v59 uses Supabase pgvector only during the offline build/search sidecar pipeline, not as a live page-view ranking service. The authorized search UI is enabled on the preview when built with browser-safe Supabase/Yandex envs and remains gated per user by a valid Supabase/Yandex session. Listing personal-feed slots are hidden unless a cached list or configured backend RPC returns compact card projections.

## Public URLs

Required URLs for the current preview:

- Preview index: <https://kenigevents.ru/preview-20260702t1536-merged-vector-medallions/__preview/>
- Today listing: <https://kenigevents.ru/preview-20260702t1536-merged-vector-medallions/segodnya/>
- Tomorrow listing: <https://kenigevents.ru/preview-20260702t1536-merged-vector-medallions/zavtra/>
- Weekend listing: <https://kenigevents.ru/preview-20260702t1536-merged-vector-medallions/vyhodnye/>
- Search page: <https://kenigevents.ru/preview-20260702t1536-merged-vector-medallions/poisk/>
- Exhibitions/long-running listing: <https://kenigevents.ru/preview-20260702t1536-merged-vector-medallions/vystavki/>
- Popular-by-source-engagement listing: <https://kenigevents.ru/preview-20260702t1536-merged-vector-medallions/populyarnoe/>
- Information partnership/reference block page: <https://kenigevents.ru/preview-20260702t1536-merged-vector-medallions/partnerstvo/>
- Information partners directory: <https://kenigevents.ru/preview-20260702t1536-merged-vector-medallions/partners/>
- Event-token medallion QA lab: <https://kenigevents.ru/preview-20260702t1536-merged-vector-medallions/lab/medallions/>
- Date-block design QA lab: `<preview>/lab/date-block/` shows the 2026-07-04 Pinterest-derived comparison table, Gemini Pro/a-opus votes, and the selected HTML variants P13/P14/P01/P03/P04 plus the current split-badge control.
- Event decision-block mobile lab: `<preview>/lab/event-decision-block/` preserves the V2 full-width date and older markers as rollback controls, while the active 2026-07-10 V3 surface compares Compact Date Tile, Calm Date Bar, Split Date / Free Band and Ticket Cluster. All active candidates keep H1 in the hero-overlap sheet, show exact address and truthful price/free state, use accessible calendar/share/like icon+count controls and retain 90–108px horizontal medallion shelves. The 2026-07-11 product shortlist is A/D; D also exposes anchored, inline and micro-label one-at-a-time onboarding probes for calendar/like/share. The active correction keeps the action row fixed and unfolds a light, playful calendar hint below it. Current real-event review target: `preview-20260711t-real-events-ticket-cluster-d`, built from a fresh 2026-07-11 Fly SQLite snapshot with `PUBLIC_EVENT_PAGE_DECISION_VARIANT=ticket-cluster`; append `?onboarding=calendar` to force the hint during review.
- Desktop event-page media lab: `preview-20260713t-desktop-media-polish-v5/lab/event-desktop/` corrects the preserved v4 desktop compositions without touching mobile: Editorial parallax now travels upward more slowly with downward scroll, its six previews are one compact fullscreen-opening row, Split OCR and portrait+Bento use a harmonious `50/50` stage, Bento uses square base cells plus a real `2×1` photo, and `Смотрите дальше` uses light inverse cards with transparent share/like actions on graphite. Preserved v4: `preview-20260712t-desktop-continuous-scroll-v4/lab/event-desktop/`; preserved v3: `preview-20260712t-desktop-scroll-compositions-v3/lab/event-desktop/`; preserved v2: `preview-20260712t-desktop-clean-pages-v2/lab/event-desktop/`. Canonical notes: `event-desktop-media-families-2026-07-12.md`.
- V3-D real-event probes: known price/phone CTA `.../sobytiya/ekskursiya-po-byvshey-pivovarne-ponart-kaliningrad-6750/`, free `.../sobytiya/veloden-kaliningrad-6345/`, unknown price `.../sobytiya/master-klass-rospis-farforovoy-tarelochki-svetlogorsk-6678/`, long title `.../sobytiya/hity-lyubimyh-artistov-kontsert-posvyaschenie-muslimu-magomaevu-i-anne-ger-svetlogorsk-6510/`, and no-medallion `.../sobytiya/master-klassy-po-zhivopisi-maslom-na-svobodnuyu-temu-kaliningrad-6567/`.
- Broken-image regression event: <https://kenigevents.ru/preview-20260702t1536-merged-vector-medallions/sobytiya/festival-pianissimo-kaliningrad-5264/>
- Fresh merged-branch event: <https://kenigevents.ru/preview-20260702t1536-merged-vector-medallions/sobytiya/detskaya-igrovaya-programma-s-animatorami-kaliningrad-6601/>
- Fresh VK auto-import event: <https://kenigevents.ru/preview-20260702t1536-merged-vector-medallions/sobytiya/semeynaya-sreda-atomy-semi-kaliningrad-6605/>
- Latest snapshot event: <https://kenigevents.ru/preview-20260702t1536-merged-vector-medallions/sobytiya/master-klass-po-igre-na-barabanah-ot-sergeya-lukinova-kaliningrad-6613/>
- Golden related discovery JSON: <https://kenigevents.ru/preview-20260702t1536-merged-vector-medallions/data/discovery/5264.json>
- Preview sitemap: <https://kenigevents.ru/preview-20260702t1536-merged-vector-medallions/sitemap.xml>
- Preview robots: <https://kenigevents.ru/preview-20260702t1536-merged-vector-medallions/robots.txt>
- Yandex Object Storage website fallback: <http://kenigevents.ru.website.yandexcloud.net/preview-20260702t1536-merged-vector-medallions/__preview/>

CDN media/ICS verification for current previews: event images in rendered HTML/JSON-LD use `https://static.kenigevents.ru/p/...`, raw legacy `https://storage.yandexcloud.net/kenigevents/...` image URLs do not leak into HTML, calendar CTAs point to stable `https://static.kenigevents.ru/ics/<event_id>.ics`. v59 discovery JSON uses `event_pgvector_related_chain_v1`; v62 and the 2026-07-02 recovery preview use `event_pgvector_related_chain_v2_two_doc` with `embedding_document_version=related_v1`; the 2026-07-02 recovery preview has Gemma strict verification disabled for the fast end-of-day rebuild and keeps the pgvector chain/audit metadata transparent in `preview-related.json`.

## Code layout

```text
site/
  package.json
  astro.config.mjs
  tsconfig.json
  scripts/build-preview.mjs
  scripts/check-preview.mjs
  scripts/deploy-preview-yc.mjs
  src/pages/[preview]/index.astro        # emits /__preview/
  src/pages/segodnya/index.astro
  src/pages/zavtra/index.astro
  src/pages/vyhodnye/index.astro
  src/pages/vystavki/index.astro
  src/pages/populyarnoe/index.astro
  src/pages/poisk/index.astro
  src/pages/partnerstvo/index.astro
  src/pages/partners/index.astro
  src/pages/sobytiya/[slug].astro
  src/pages/sobytiya/[slug]/event.ics.ts
  src/pages/data/discovery/[eventId].json.ts
  src/pages/lab/medallions/index.astro
  src/pages/lab/date-block/index.astro
  src/pages/lab/event-decision-block/index.astro
  src/pages/lab/hero/index.astro
  src/pages/lab/hero/review/index.astro
  src/pages/sitemap.xml.ts
  src/pages/robots.txt.ts
  src/layouts/EventLayout.astro
  src/components/EventHero.astro
  src/components/EventCtaPanel.astro
  src/components/EventFacts.astro
  src/components/EventCard.astro
  src/components/EventListItem.astro
  src/components/PersonalFeedSlot.astro
  src/components/CalendarLink.astro
  src/components/Icon.astro
  src/lib/assets.ts
  public/favicon.svg
  src/data/preview-events.json
  src/data/preview-related.json
```


## 2026-07-02 merged vector-gate + medallion preview

`preview-20260702t1536-merged-vector-medallions` supersedes `preview-20260702t0755-fresh-ui-fixes` for the Static Site MVP review because it merges the parallel medallion SVG upgrade and the Smart Update vector identity gate branch before exporting from the latest 2026-07-02 production SQLite snapshot.

Evidence:

- exported `399` active/future public events, max event id `6613`; the build includes production events `6601`–`6605` created after the vector-identity gate rollout;
- related chains use `event_pgvector_related_chain_v2_two_doc` / `related_v1` over Supabase pgvector; the fresh sync upserted `399` documents and `101` changed/new embeddings, while `697` embeddings were skipped as unchanged;
- build/check target: `PREVIEW_BUILD_ID=preview-20260702t1536-merged-vector-medallions`, `npm --prefix site run build:preview`, `npm --prefix site run check:preview`;
- deploy target: `s3://kenigevents.ru/preview-20260702t1536-merged-vector-medallions/` plus stable `s3://kenigevents.ru/ics/<event_id>.ics` files; deploy verification reported `Public preview verification: ok` and `Stable CDN ICS uploaded: 399`;
- public HTTP smoke returned `200` for `__preview/`, `/segodnya/`, `/zavtra/`, `/vyhodnye/`, `/vystavki/`, `/poisk/`, `/partners/`, `/lab/medallions/`, `sitemap.xml`, `robots.txt`, sample event pages `5264`, `6585`, `6601`, `6605`, `6613`, and stable ICS files for those ids;
- authorized-search readiness passed with live Edge CORS, Supabase Auth redirect config, `custom:yandex` provider and `yandex-userinfo` adapter probes; mocked UI smoke and real Edge Playwright smoke passed (`интересно детям` returned rendered cards and quota status);
- `/vystavki/` vector-identity regression audit with `--since-date 2026-07-02` returned `high_confidence_duplicate_count=0`; identity-gate rollout audit on the same snapshot reports `env_readiness.ready=true`, `identity_gate_vector_available_count=13`, and the only `vector_error_count=2` rows are the pre-secret 14:26/14:33 decisions.

OCR/poster text contract for this preview: raw poster OCR is not embedded directly into `search_v3` or `related_v1`. It can affect search only indirectly if Smart Update has already promoted source-grounded poster facts into canonical public fields (`title`, `description`/`search_digest`, venue/address, topics). This prevents commercial venue names printed on posters from dominating `/poisk/` or static related cards unless the venue was canonically accepted by the event extraction/update pipeline.


## 2026-07-02 recovery preview: fresh data + UI repair

`preview-20260702t0755-fresh-ui-fixes` supersedes the earlier same-day UI-only preview because the first rebuild still carried `current_date=2026-07-01` data. The accepted preview is exported from the 2026-07-02 production SQLite snapshot and the personalization Supabase pgvector sidecar:

- exported `376` active/future public events, max event id `6585`, including late ids `6566–6585` from the latest production snapshot;
- related chains use `event_pgvector_related_chain_v2_two_doc` / `related_v1` over Supabase pgvector; the fresh sync upserted `376` documents and `64` changed/new embeddings, while `688` embeddings were skipped as unchanged;
- build/check target: `PREVIEW_BUILD_ID=preview-20260702t0755-fresh-ui-fixes`, `npm --prefix site run build:preview`, `npm --prefix site run check:preview`;
- deploy target: `s3://kenigevents.ru/preview-20260702t0755-fresh-ui-fixes/` plus stable `s3://kenigevents.ru/ics/<event_id>.ics` files; deploy verification reported `Public preview verification: ok` and `Stable CDN ICS uploaded: 376`;
- public HTTP smoke returned `200` for `__preview/`, `/poisk/`, `/partners/`, the Pianissimo regression event `5264`, fresh event `6585`, and stable ICS files `5264.ics` / `6585.ics`;
- public Playwright visual smoke passed: `5` broken upstream related-card images were converted to fallback surfaces with `0` visible broken icons, the mobile tag did not overlap nav links, four partner logos loaded, search submit/progress/avatar geometry matched the recovered UI, and the footer partner item was a transparent plain link;
- UI regression fixes included in the build: broken related-card images fall back to a neutral image surface instead of showing raw alt text/broken icons, the mobile terracotta drawer rail is tall enough not to overlap the top navigation, `/poisk/` restores the account avatar/search-button progress polish, `/partners/` is a logo-first minimalist page, and footer partner navigation is a plain link rather than a pill button.

Yandex Cloud CLI auth was not reinitialized for this recovery: the existing local profile/cache (`/home/dev/yandex-cloud/bin/yc`, `/home/dev/.config/yandex-cloud/`, profile `artkoder`) was verified with a control-plane API call. Static deploy still uses S3/Object Storage credentials from `.env`; no new browser `yc init` flow is part of the static-preview rebuild.

Database routing remains dual-DB: canonical event data comes from Fly SQLite `/data/db.sqlite`; personalization/search data lives in the separate Supabase/Postgres project. No tracked code or `.env.example` wiring for Yandex YDB exists as of this recovery pass.

## v46d regression and related-chain evidence

`preview-20260628-event-pages-v47-sparse-fixes` was generated on Kaggle CPU from the 2026-06-28 production SQLite snapshot with `--current-date 2026-06-29` and 50 real tomorrow/future events. Kaggle result: `ok=true`, event count `50`, archive `preview-20260628-event-pages-v47-sparse-fixes.tar.gz`, runtime 19:40:00–20:07:18 UTC.

UI fixes included in this preview:

- personalization reset now records and displays `Последний сброс: DD.MM, HH:mm`;
- sold-out/unavailable events render `Билеты закончились` and do not expose `Купить билет`;
- hero `Фото` hint is positioned above the decision sheet and no longer overlaps the content below;
- markdown-like source descriptions no longer become multi-paragraph bold blocks;
- opened mobile tag/drawer closes automatically after the user scrolls/continues the page;
- event pages display last update time in Kaliningrad time;
- OCR/poster hero images keep parallax transforms instead of disabling parallax.

Related/discovery changes:

- related chains in this preview are built by `event_sparse_related_chain_v1` with honest lexical/sparse retrieval (`local_tfidf_sparse_v1`) plus deterministic/facet scoring; this is not semantic vector search;
- Gemma 4 26B verification ran only through `GoogleAIClient` + Supabase limiter. The full Kaggle run audited 45 anchors with 45 provider calls and ended `partial` because 4 provider calls timed out at 45s and 1 response was malformed; those anchors fall back to vector chains. The persisted cache is nevertheless usable for rebuild stability; a rerun with the same cache reported `cache_hit_no_provider`, `provider_calls=0`, `cache_hits=50`;
- Kaggle no longer relies on UI secrets for API-started runs: the runner attaches encrypted split secret datasets and deletes them after the waited run.

Verification evidence: Kaggle `npm run check:preview` passed; public `curl -I` returned HTTP 200; `artifacts/codex/static-site-builder/playwright-v46d-public-check.cjs` passed against the published URL.

## v48 Supabase pgvector semantic related canary

`preview-20260628-event-pages-v48-pgvector-gemma-kaggle` was the first pgvector focus preview for the 2026-06-28/29 data slice. It was built on Kaggle CPU from a production SQLite snapshot with 70 real events, synced compact search documents/vectors into the separate personalization Supabase project, built related chains through Supabase pgvector and deployed the checked artifact to the `kenigevents.ru` bucket/CDN path.

Retrieval contract:

- `algorithm_id=event_pgvector_related_chain_v1`;
- `strategy=event_pgvector_related_chain_v1_manifest`;
- `retrieval_method=supabase_pgvector_hnsw_cosine_v1`;
- `semantic_embeddings=true`;
- model/dimension: `gemini-embedding-2`, `vector(768)`;
- ordinary event-page views still read static JSON; no page-view Supabase/embedding/LLM call is introduced.

Evidence from 2026-06-29 UTC:

- local vector sync processed 70 docs and wrote 12 changed/new vectors after weekday/category hardening;
- live personalization Supabase contains 76 search documents and 76 embeddings for `gemini-embedding-2/vector(768)`;
- local Gemma 4 26B verifier canary completed `status=ok`, `audited_anchors=15`, `provider_calls=7`, `cache_hits=8`, `errors=[]`;
- Kaggle CPU run `preview-20260628-event-pages-v48-pgvector-gemma-kaggle` completed with `ok=true`, `event_count=70`, vector sync `provider_calls=0` because the Supabase vectors were already current, and `npm run check:preview` passed inside the notebook;
- live public smoke for `/data/discovery/6447.json` returns `6310` “Архитектурно-урбанистическая студия...” as first candidate with `vector_similarity≈0.8592`, `llm_semantic_score=0.92`, fixing the earlier “Музыка нашего города” lexical false-positive.

Remaining production gate after v48 was automatic Smart Update → Kaggle → CDN promotion after artifact checks; v59 below supersedes v48 for strict related-quality review.

## v59 strict pgvector + Gemma 4 related preview

`preview-20260629-event-pages-v59-related-gemma50` is the current strict related-events canary. It was generated from a read-only production SQLite snapshot on 2026-06-29 with `--current-datetime 2026-06-29T21:30`, prioritising events starting on 2026-06-30 and 2026-07-01. The two-day focus window contained 21 eligible one-day/short events, so the exporter supplemented later active future events to reach a 50-event review slice.

Related/publication contract:

- event-to-event retrieval starts with Supabase `pgvector` over `gemini-embedding-2/vector(768)` search documents;
- Gemma 4 26B (`models/gemma-4-26b-a4b-it`) then sees the top retrieved candidates, rejects unrelated events and returns the final order;
- public `similar[]` / `related_static[]` contain only candidates explicitly accepted by Gemma with `llm_semantic_score >= 0.72`; weak 0.55–0.71 candidates may remain only as adjacent/explore metadata, not as “similar” cards;
- the raw pgvector chain is still stored in the manifest for audit/debug, but Astro consumes the strict verified list when `strict_verified_related=true`;
- already-started same-day events and past/cancelled/deleted/duplicate events are excluded during export, so a related card disappears from new builds when it is no longer actionable.

Performance evidence from the v59 local canary:

| Step | Result |
|---|---:|
| Focus export | 50 events |
| Pgvector/vector sync | 44 new/changed embeddings, 6 unchanged; 32.59s wall |
| Pgvector chains | 50 anchors, 40 raw candidates per anchor |
| Gemma strict audit | 50 successful anchors, 60 total attempts after retries |
| Gemma wall time | 22m53s first pass + 3m53s fill-missing |
| Gemma timings | p50 ≈ 18.3s, avg ≈ 17.0s for successful first-pass calls |
| Final cached export | 0 provider calls, 0.47s |
| Astro build | 66 pages, ≈5.7s |

Golden check: event `6447` («Как договориться о будущем города») now shows only event `6310` («Архитектурно-урбанистическая студия...») as strict similar in the first slot (`llm_semantic_score=0.88`), instead of letting a lexical “город” music false-positive into the related feed.

Verification evidence:

- `npm run check:preview` passed for a non-control focused build and verifies strict Gemma score metadata;
- public HTTP checks returned 200 for `__preview/`, event `6447` and `/data/discovery/6447.json`;
- Playwright mobile smoke against the public URL verified that the first visible related card for `6447` is `6310` and that the discovery JSON carries `llm_semantic_score=0.88`.

### v61 full-catalog Gemma verifier prompt audit

The first full future-catalog stress attempt exposed a Gemma verifier I/O issue:
embeddings and pgvector retrieval were cached/reusable, but the old verbose
Gemma prompt produced many invalid/truncated JSON retries. The related verifier
now uses the compact v4 contract documented in
`docs/features/unsigned-personalization/semantic-vector-retrieval.md`:

- Gemma sees compact 10-candidate batches by default (`6..12` allowed);
- the static related audit runs 2 passes by default, so it can inspect up to
  20 strongest pgvector candidates without one large fragile JSON response;
- the model returns `event_id`, `llm_semantic_score`, `similarity_class`,
  `confidence` and `reject`; verbose reason-code explanations stay out of the
  output;
- app code can rescue only fully complete verdict objects from a truncated JSON
  tail;
- strict “Похожие” remains LLM-verified; lower pgvector candidates should be a
  separately headed discovery section, not silently mixed into similar cards.

Evidence is in `artifacts/codex/related-gemma-prompt-audit-20260630/`: Gemini
3.1 Pro review completed, Opus review is explicitly blocked (empty `a-opus`/`agy`
outputs and Claude `401`), and local live smoke on anchors `6447`, `5878`, `5370`
returned valid compact Gemma JSON in `6.22–8.20s`; after restoring
model-provided `similarity_class`/`confidence`, the synthetic smoke returned
valid JSON in `7.00s`. A new full Kaggle run is still needed to measure the
statistical retry/error reduction on all anchors.


## v62 full-catalog two-document pgvector + Gemma run

`preview-20260630-event-pages-v62-two-vector-gemma-full` is the first full future-catalog stress run for the implemented two-document retrieval split:

- `search_v3` vectors remain optimized for authorized `/poisk/`;
- `related_v1` vectors are used by static event-to-event related chains;
- Supabase RPC `event_related_candidates_by_event_id_v1(..., p_embedding_doc_kind => 'related_v1')` is the recall layer;
- Gemma 4 26B validates/reorders the top candidate windows offline;
- public `similar[]` is strict: only candidates with Gemma verdict and `llm_semantic_score >= 0.72`; weak/provisional material belongs only under a separate discovery heading.

Incremental contract:

- `scripts/sync_event_search_vectors_to_supabase.py` skips unchanged vectors independently for `search_v3` and `related_v1`; after the initial related-vector backfill, ordinary rebuilds should generate provider embeddings only for new/changed event documents.
- The related cache stores raw pgvector chains and Gemma verdicts keyed by event/candidate fingerprints and policy signature. If a new event appears, only anchors whose top candidate window changes, plus the new anchor, need new Gemma calls; unchanged anchor/candidate pairs are cache hits.
- The first v62 run is necessarily heavier than a steady-state rebuild because it changes the document kind/cache schema and has to validate anchors not present in the old v59/v61 cache.

Local preflight on 2026-06-30 before the Kaggle run:

- personalization Supabase size: about 25 MiB; `event_search_documents≈3.8 MiB`, `event_embeddings≈9.4 MiB`;
- vectors present: `search_v3=404`, `related_v1=343`;
- full sync from the v61 production snapshot processed 343 future events and created the remaining `related_v1` vectors with `293` embedding provider calls;
- after the sync, reruns skipped unchanged embeddings by kind;
- local Gemma cache preflight verified 20 anchors with valid JSON and reused cached verdicts on rerun; golden `6447` ordered `4759` then `6310`, while unrelated music false positives stayed out of strict similar.

The first Kaggle v62 run produced the expensive reusable related cache but ended with Kaggle `ERROR` before a compact archive/result could be accepted because the failed notebook left a large `node22` dependency tree in `/kaggle/working`. The recovered cache was then reused locally with `--pgvector-max-provider-calls 0` / `--gemma-related-max-anchors 0`: `343` anchors exported, `343` Gemma cache hits, `0` provider calls, `npm run check:preview` passed, and the preview tree was uploaded to `s3://kenigevents.ru/preview-20260630-event-pages-v62-two-vector-gemma-full/`. The Kaggle kernel now cleans transient `node22`/extracted site paths on both success and failure while preserving recoverable outputs such as `event_related_chain_cache.json` and `events.sqlite`. The exporter also has a shrink guard so a 50-event canary cannot overwrite a larger expensive related cache unless `STATIC_SITE_ALLOW_RELATED_CACHE_SHRINK=1` is set.

v62 verification evidence on 2026-06-30:

- S3 listing confirms `1060` uploaded objects for the v62 prefix, including `343` event detail pages and `343` discovery JSON files;
- authenticated S3 listing confirms `__preview/index.html`, `/poisk/index.html`, golden event pages and stable ICS files exist in the bucket;
- public HTTP is currently blocked: `https://kenigevents.ru/preview-20260630-event-pages-v62-two-vector-gemma-full/__preview/` returns `404`; `https://static.kenigevents.ru/ics/5878.ics` fails TLS validation because the CDN certificate is still for `*.yccdn.cloud.yandex.net`; bucket public-read policy and CDN certificate/domain binding must be fixed before user-facing review;
- built discovery JSON for `6447` has `algorithm_id=event_pgvector_related_chain_v2_two_doc`, `strategy=event_pgvector_related_chain_v2_manifest`, and only two strict related cards: `4759` (`llm_semantic_score=0.85`, `llm_confidence=0.95`) and `6310` (`0.75`, `0.90`);
- built discovery JSON for `5878` has music/retro/concert candidates first (`3398`, `5777`, `6488`, `6481`, `5733`);
- built discovery JSON for `5370` has art/exhibition candidates first (`6214`, `5969`, `6080`, `5391`);
- mocked browser smoke: `authorized_search_ui_smoke=ok`, result cards scroll and shared like/share/not-interested/calendar actions render;
- real Edge smoke: `authorized_search_real_edge_smoke=ok`, 12 cards rendered for `концерт классической музыки`, first event `5668`, scrolled event `5667`, quota text returned.

## v47 sparse terminology, related-order and CDN verification

`preview-20260628-event-pages-v47-sparse-fixes` is the historical sparse-baseline preview for the 2026-06-28 data slice. It was generated from the production SQLite snapshot with 70 real events and deployed to the `kenigevents.ru` bucket with CDN asset settings.

User-visible fixes included in this preview:

- the forbidden admission phrase `Платный вход` is no longer emitted by the exporter/runtime UI; paid/ticketed events without a reliable price render `По билетам`, while real price/range values are shown as the value and may be a nofollow ticket link;
- registration event `5077` keeps the expected `kgd80.ru/.../?register=1` registration CTA;
- related/feed cards show a compact event-type hashtag so a title without an explicit type is still understandable;
- source count and last update moved to the end of the event description/details block before the related feed;
- `Показать ещё` is shown only when there are eligible not-yet-rendered candidates and appends from the same static discovery JSON chain;
- the Pianissimo image regression (`5201`) is covered by an explicit valid image override.

Related-chain contract:

- generated manifests now use `schema_version=event_sparse_related_chain_v1`, `algorithm=event_sparse_related_chain_v1`, `retrieval_method=local_tfidf_sparse_v1`, `semantic_embeddings=false`;
- candidates carry `lexical_similarity`, mandatory `slot_type` and reason codes; legacy `event_vector_related_chain_v2` is compatibility-only for reading old artifacts;
- popularity/source likes do not boost candidates into `pure_related`;
- the 6447 golden-anchor regression is fixed in the sparse baseline: `Архитектурно-урбанистическая студия` ranks before `Музыка нашего города`. This is still a lexical/facet fix, not real semantic vector search.

CDN/ICS verification for v47:

- `scripts/migrate_static_media_to_cdn_bucket.py --active-on 2026-06-28 --apply` found 957 referenced legacy `/p/...` keys and 0 missing in bucket `kenigevents.ru`;
- deployed pages load `_astro/*` and rewritten event media from `https://static.kenigevents.ru`;
- stable calendar files are uploaded to `https://static.kenigevents.ru/ics/<event_id>.ics`;
- `npm run check:preview` and public Playwright regression passed (`artifacts/codex/static-site-builder/playwright-v47-public-check.cjs`).

Production caveat: Smart Update currently schedules/runs the Kaggle static-site builder artifact path. Automatic promotion of a checked Kaggle artifact to CDN/Object Storage is still a separate production gate; manual preview deploy verifies the bucket/CDN path but does not close the full Smart Update → CDN publication loop.

## Fixture coverage

The v43 preview uses 80 real production event rows exported read-only from the 2026-06-28 Fly SQLite snapshot under `artifacts/codex/static-site-builder/prod-db-20260628.sqlite` and committed as a bounded production-like static fixture. The export deliberately prioritizes the real same-day slice before long-running continuing events: `/segodnya/` now has 49 events starting on 2026-06-28 across 14 event types, so mobile QA is not limited to exhibitions.

- `5878` — «Песни СССР», paid sale, control slug `pesni-sssr-svetlogorsk-5878`;
- `698` — «Древние воины Янтарного края», multi-image fullscreen-gallery regression event;
- `6438` — «Водные битвы с аниматорами», same-day listing/card/hero QA event;
- free / registration with link;
- registration/source-only without direct ticket link;
- phone-only CTA;
- unknown/source-only CTA;
- long Russian title wrapping in main column;
- no local image hero fallback;
- weak/missing address fallback;
- static `/segodnya/`, `/zavtra/` and `/vyhodnye/` listing pages from the same fixture;
- related “Другие даты” pair `6437`/`6438`;
- one static neutral `Смотрите дальше` discovery feed; diversification is an internal ranking constraint, not a separate user-facing block;
- up to 10 preloaded discovery candidates in static HTML, plus a same-origin `/data/discovery/<event_id>.json` `event_detail_related` manifest (`schema_version=event-detail-related-v1`, `related_static[]`) for one automatic client hydration after JS applies a consented compatible local profile; further expansion is explicit through `Показать ещё`;
- explicit card reactions: like count + toggle like/unlike, “Не интересно”, local compact raw log/report for the current anonymous browser profile;
- honest like baseline: visible `likes_count` is `source_likes_count + service_likes_count`; `source_likes_count` is aggregated from available production TG/VK source-post metrics, while `service_likes_count` is the future first-party KenigEvents counter and remains `0` in this static preview; public HTML/UI shows only the total count; source/service split is technical and must not be rendered as copy or data attributes;
- detail-page calendar action links open `.ics` directly rather than forcing a download, but only for one-day/short events. If a short event is free and has no purchase/registration CTA, `В календарь` may become the primary CTA with a calendar icon; otherwise it remains secondary to the ticket/registration action. Feed/preview cards keep calendar out of the main right-thumb row and may expose it only as a quieter utility for eligible candidates.
- `image_text_mode` (`ocr_text` / `visual_only` / `unknown`) is a required export field. This preview does **not** run OCR during Astro build; it consumes the fixture value that must be produced by the existing media/OCR pipeline in production export. If this field is missing, the safe default is `unknown` → natural-ratio no-crop rendering.
- visible Russian dates omit the current year when both boundaries are in the build current year; cross-year ranges keep both years.
- `/segodnya/` and `/zavtra/` are grouped into `Утро / День / Вечер / Ночь`; no-time events fall into `День`. Mobile list cards use a compact plaque: cropped left photo column at parent-card level, straight separator to the text column, no direct outbound ticket/source links. This keeps production listings indexable and pushes external actions to detail pages where context and `rel` can be controlled.
- v43 keeps property-label polish, the taller mobile drawer tag, the split-card utility/action layout, hidden listing personal-feed slots, local `Все / Для меня` filter, CDN-aware `_astro/*` asset URLs and strengthened Open Graph metadata; temporary share lab controls are removed, the single production `Поделиться` action uses Web Share file + text + URL with generated 1080×1350 fallback, paid `price_label` chips can link directly to the ticket URL with `rel="nofollow"`, and the fullscreen gallery preloads/decodes adjacent slides before auto-advance to avoid black flicker.

No future active sold-out/cancelled/postponed event is intentionally showcased as a product state in this slice, so those optional states still need separate QA when the fixture/export includes reliable examples.

## Build size and timing evidence

Measured locally on 2026-06-28 with `PUBLIC_ASTRO_ASSET_BASE_URL=https://static.kenigevents.ru/{buildId}`:

| Slice | Events | Static pages | Files | Output size | Build wall time | Max RSS |
|---|---:|---:|---:|---:|---:|---:|
| v43 focus preview | 80 | 95 | 261 | 28 MiB | 0:06.57 | ~387 MiB |
| full active snapshot estimate | 386 | 403 | 1185 | 128 MiB | 0:20.16 | ~522 MiB |

The full estimate was produced by exporting all active future/intersecting events from the same 2026-06-28 snapshot and building locally under a temporary `preview-20260628-event-pages-full-local-estimate` id. It excludes future media mirroring/CDN image transformations; bucket upload time will scale mostly with file count/bytes and is expected to dominate the Astro render time once full publication is enabled.

## SEO/GEO and preview safety

- All preview HTML has `meta name="robots" content="noindex,nofollow,noarchive"`.
- Prefix robots is exactly:

```text
User-agent: *
Disallow: /
```

- Preview canonical and `og:url` include `/preview-20260628-event-pages-v48-pgvector-gemma-kaggle/`; production canonical is not emitted by the preview build.
- Event pages render `schema.org/Event` / `MusicEvent` JSON-LD from visible event facts; for multi-image events, JSON-LD `image[]` includes the hero/gallery image assets even when the fullscreen gallery lazy-loads them after user action, so SEO/GEO crawlers can still tie the images to the event.
- The control `.ics` is a no-JS link and contains `DTSTART:20260711T193000Z`; it deliberately has no `DTEND` because reliable duration/end was not exported for event `5878`.


## Kaggle CPU builder / Smart Update handoff

The static-site production build path now has a Kaggle CPU runner that reuses the existing events-bot Kaggle infrastructure instead of inventing a separate execution path:

```bash
python scripts/run_static_site_builder_kaggle.py \
  --db /data/db.sqlite \
  --status-db /data/db.sqlite \
  --status-callback-url https://events-bot-new-wngqia.fly.dev/internal/kaggle/run-event \
  --limit 50 \
  --current-date YYYY-MM-DD \
  --build-id preview-YYYYMMDDHHMM-event-pages-prod50-kaggle \
  --download-output
```

Contract:

- input data is a unique per-run private Kaggle dataset, matching the CherryFlash/session-dataset pattern;
- the site source is uploaded as `site_source.tarball` (gzip tar content with a neutral extension) because Kaggle dataset ingestion auto-extracts `.tar.gz` and can break Astro dynamic route filenames;
- the kernel extracts the site to `/tmp/kenigevents-static-site`, not read-only `/kaggle/src`;
- Kaggle CPU currently provides Node 20, while Astro 6 requires Node `>=22.12.0`, so the kernel installs local `node@22.12.0` before build/check;
- output is intentionally minimal: `<build_id>.tar.gz`, `static_site_build_result.json`, and the kernel log; `node_modules` is not left under `/kaggle/working`;
- when `--status-db` and callback URL are provided, the launcher creates `kaggle_run.json` via `create_kaggle_run_config(...)`, uploads a status dataset via `create_kaggle_status_dataset(...)`, and adds it to `dataset_sources`; inside the kernel `kaggle_status_client` emits `kernel_started`, `preflight_ok`, `alive` progress, and `report_written`;
- the resource lease key is `static_site:builder`, so a production status-aware run can block parallel static-site builds.

Verified artifact on 2026-06-28: `preview-20260628-event-pages-prod50-kaggle-v44` built 50 real production-snapshot events on Kaggle CPU and passed `npm run check:preview`. This was a local manual run without production callback env, so status dataset creation was intentionally skipped; the production outbox path must pass `/data/db.sqlite` and the Fly callback to make it visible in `kaggle_run_ledger`/poller.

Outbox integration: `schedule_event_update_tasks(...)` enqueues a coalesced `JobTask.static_site_build` with key `static_site_build:prod` for 15 minutes after the last Smart Update when `ENABLE_STATIC_SITE_KAGGLE_BUILDER=1`. The handler launches the runner with `/data/db.sqlite`, status DB/callback, `--download-output`, CDN asset/ICS base envs, browser-safe AuthorizedEventSearch public envs, and the configured related mode. For the pgvector path set `STATIC_SITE_RELATED_MODE=pgvector`, `STATIC_SITE_SYNC_PGVECTOR_VECTORS=1`, `STATIC_SITE_PGVECTOR_EMBEDDING_MODEL=gemini-embedding-2`, `STATIC_SITE_PGVECTOR_EMBEDDING_KEY_ENV=GOOGLE_API_KEY4`, and optionally `STATIC_SITE_GEMMA_RELATED_VERIFY=1` / `STATIC_SITE_GEMMA_RELATED_MAX_ANCHORS=50`. For focus-group builds that should show Yandex login/search, also set `STATIC_SITE_PUBLIC_PERSONALIZATION_SUPABASE_URL`, `STATIC_SITE_PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY` and `STATIC_SITE_PUBLIC_YANDEX_AUTH_PROVIDER=custom:yandex`; only URL + publishable key are exposed to the browser. CDN/object-storage publication is still a follow-up handoff after CDN is enabled; full page rebuilds are for content/lifecycle changes, not every counter tick.

## Build and deploy

```bash
cd site
npm install
PREVIEW_BUILD_ID=preview-20260628-event-pages-v48-pgvector-gemma-kaggle PUBLIC_ASTRO_ASSET_BASE_URL='https://static.kenigevents.ru/{buildId}' npm run build:preview
PREVIEW_BUILD_ID=preview-20260628-event-pages-v48-pgvector-gemma-kaggle PUBLIC_ASTRO_ASSET_BASE_URL='https://static.kenigevents.ru/{buildId}' npm run check:preview
PREVIEW_BUILD_ID=preview-20260628-event-pages-v48-pgvector-gemma-kaggle npm run deploy:preview
```

`deploy:preview` reads only the `KENIGEVENTS_SITE_YC_*` variables from the root `.env` and uploads `site/dist/<build-id>/` to the same prefix in the `kenigevents.ru` bucket. Calendar files are re-uploaded with `text/calendar; charset=utf-8` and `Content-Disposition: inline; filename="event.ics"` metadata so mobile clients can open the `.ics` instead of treating it only as a forced download.

## Visual review passes

The first public preview (`preview-20260627-event-pages-v1`) was superseded after visual review. v43/v47 keep the event page mobile/feed-oriented and feedback-aware, and replaces the v19 safe image block with the v20 hero composition lab from `event-hero-lab-2026-06-27.md`:

The feed-card A/B has been resolved for normal event pages: `split-actions` is now the baseline for all event detail discovery feeds. The old overlay variant remains documented only as a rejected/historical comparison in `event-card-ui-ab-2026-06-27.md`.

- recommendation cards now have large image-led feed cards instead of text-only cards;
- event hero keeps deterministic media modes (`poster-stage` for OCR/unknown, `photo-cover` for verified `visual_only`, `fallback-art` for no image), but now adds explicit composition variants (`poster-billboard`, `poster-attached-card`, `photo-cinematic-sheet`, `photo-parallax-sheet`, `compact-ticketing`); mobile hero visual breaks out to 100vw where appropriate, H1/CTA remain HTML in a decision sheet, OCR/unknown posters are not cropped, and visual-only images may use cover. Cards/listings keep the OCR-safe v15 rule: `visual_only` cover/crops inside a strict vertical 4:5 frame; `ocr_text|unknown` renders the actual image at natural aspect ratio with no crop, no fixed cover frame, no duplicate/backdrop underlay and no blur fill;
- duplicated facts/source/debug notes were removed from the first screen;
- the long description is visible HTML, followed by a compact icon facts block; public source count/views and source links are hidden until auth exists, with a temporary notice that sources, mentions and extended statistics will be available to registered users;
- native mobile share is attempted by one visible `Поделиться` button; duplicate Telegram/VK/WhatsApp share pills were removed, and fallback copies the URL when system share is unavailable.
- footer social navigation mirrors the Telegraph editorial footer and adds Max: Telegram `@kenigevents` + `@kldevents`, VK `kenigeventsofficial` + `klgdevents` + `vk.ru/im/channels/-239844596`, and `max.ru/join/...`; site footer uses visible Telegram/VK/Max SVG icons, while Telegraph remains plain links only.
- a tightly cropped transparent graphite/terracotta PK-monogram favicon is emitted from `site/public/favicon.svg` with `sizes="any"`; it has no white/cream plate and occupies the full tab-icon width while preserving transparent corners;
- footer exposes compact social navigation and `mailto:info@kenigevents.ru`.


After direct product review, `preview-20260628-event-pages-v43` rolls back the UX regressions introduced by the split recommendation rails and adds the first static-seed/client-hydration discovery contract:

- event description is visible HTML again, not hidden behind a collapsed `<details>` block;
- event continuation is one vertical mobile-first discovery feed (`Смотрите дальше`), not two horizontal scroll blocks and not a visible “try something else” module;
- the first continuation surface is static-first: the generated HTML contains up to 10 candidates; after JS activation, only a consented compatible local profile (`ke_personalization_profile`, UUID ids, `event-detail-related-v1` / `event-taxonomy-v1`) may hide/rerank preloaded cards by `hidden_event_ids`, `not_interested_event_ids` and strong `negative_interest_tags`, then the page performs one lightweight same-origin fetch to `/data/discovery/<event_id>.json` and top-ups relevant candidates; after that, more cards are loaded only when the user presses `Показать ещё`;
- desktop keeps the same continuation content as a grid, matching desktop expectations instead of mobile horizontal rails;
- `Поделиться` is visible always: it calls `navigator.share()` when the browser/webview supports native system share and falls back to copying the URL when native share is unavailable.
- diversity/anti-bubble is only a ranking/composition rule inside `Смотрите дальше`; the UI does not label cards as “Попробовать другое” or “Открыть новое”.
- explicit likes are the strongest positive signal: after consent the preview stores a DB-compatible anonymous browser profile in `ke_personalization_profile` and compact local strong-action records in `ke_event_feedback_log_v1`; likes/unlikes update `liked_event_ids` and `positive_tags`, while visible counts increment only for the current visitor.
- “Не интересно” is the explicit negative signal; the preview dims and demotes the card instead of inventing a visible anti-bubble block.
- the bottom sticky CTA is hidden after the discovery feed enters the viewport.
- same-origin event links have lightweight prefetch markers so static page transitions can warm the next HTML document.
- media rendering consumes the same `image_text_mode` export but differs by surface: hero uses `poster-stage` for OCR/unknown and `photo-cover` for `visual_only`; cards/listings use `visual_only` cover in a vertical 4:5 frame and `ocr_text|unknown` natural aspect ratio, not `contain` inside a fixed card frame. Duplicate same-poster underlays, blurred fills, repeated edges and OCR crop are forbidden.
- The share action uses a VK-like outlined repost/share arrow adapted from `@vkontakte/icons` `Icon24ShareOutline` (MIT), accessible `Поделиться` label and share count when count is positive. Zero like/share counts are not rendered as `0`. After a successful like the share action is highlighted instead of showing a floating bubble. Variant A keeps one overlay row with `Не интересно`, share and like; Variant B moves share/like under the card as transparent icon actions and may keep `В календарь` as an inside-card utility only for one-day events. The old explicit `Открыть` card button is removed because media/title links plus full-card JS navigation preserve crawlable SEO/GEO links while reducing UI noise.
- Calendar remains available on the event detail page / primary transaction block only when `end_date` is empty or equals `start_date`. In the feed it is absent from Variant A; Variant B may show it as an inside-card utility for one-day/calendar-eligible events only.
- The like button shows only the total like count. The source/service split is kept in the fixture/DB for consistency and audit, but is not rendered into the public page.
- single tap/click on a non-interactive part of a card navigates to the event detail page immediately. Double-tap like is intentionally removed because it raced with navigation and could not be made reliable without harming SEO/GEO-friendly full-card navigation; likes are explicit button actions.
- marking `Не интересно` turns the current card into a grey explanatory plate (`Вы пометили: не интересует`) with an explicit `Отменить` button; tapping the plate itself does not navigate, and later personalization may remove/demote similar events on subsequent surfaces.
- explicit-feedback rerank is viewport-stable: after a user action, the acted-on card and all cards above it keep their positions; only cards below the action anchor may be re-ordered.
- same-year visible dates omit the year (`11 июля · 21:30`), while cross-year ranges keep the year on both sides (`12 июня 2026 — до 28 марта 2027`).

After consultant review, `preview-20260628-event-pages-v43` additionally hardens the first discovery layer:

- header links now open real static `/segodnya/`, `/zavtra/` and `/vyhodnye/` pages, not QA anchors;
- related cards use a no-nested-anchor poster-card component with mandatory image/generated visual slot and direct page link; `.ics` calendar action is deliberately kept on the detail page, not in feed cards;
- `6437`/`6438` same-occurrence duplicates are excluded from “Похожие события” and remain only in “Другие даты”;
- source-only paid/ticketed events use honest `По билетам` copy instead of the forbidden generic phrase `Платный вход` or implying direct purchase;
- weak-address pages do not show “Открыть на карте”;
- raw markdown/facts artifacts, hashtags in venue names, `null`/`undefined`/`NaN`, sitemap entries and all event `.ics` files are covered by `npm run check:preview`.



### v37 product/UI corrections

`preview-20260628-event-pages-v43` adds the current product corrections:

- public event pages show only an auth-gate notice for sources, mentions and extended stats; actual source lists/statistics are not rendered until registered-user access exists;
- the event page now has one product-level facts block, not two: hero keeps only a compact date/place meta line, while `Коротко` is the single icon fact block with date, venue+address, entry/status, optional Pushkin card/festival;
- detail CTA hierarchy supports calendar-as-primary only for one-day free/no-purchase events, while paid/registration events keep ticket/registration primary and calendar secondary when eligible;
- hero gallery has an on-image transparent `Фото N` CTA, lazy-loads gallery slides from `data-gallery-src` after open/navigation, shows the service tag in fullscreen, makes visual-only fullscreen photos cover 100% viewport height with no side fields, auto-pans right-to-left and then advances to the next photo, pauses forward auto-advance after a manual backward swipe, keeps OCR/text images in contain, and JSON-LD `image[]` lists the gallery assets for SEO/GEO;
- the mobile top drawer is now one monolithic sliding object: rail and handle move together with no visible gap; the closed handle remains visible after scrolling, instead of disappearing;
- the sources/mentions registered-user notice moved out of `Коротко` and is rendered as the bottom strip of the parent details section, so it no longer visually belongs to the compact facts block;
- event `5370` is a documented fixture override: production currently marks the long-running exhibition «Точка и линия» free because a free curator round-table source was merged into it. The v40 fixture renders it as paid/ticketed for preview correctness, while production DB repair remains a separate source-of-truth task.

### v44 CDN media/ICS and Kaggle-published preview

`preview-20260628-event-pages-v44-cdn-kaggle` is the first public preview built by the Kaggle StaticSiteBuilder after enabling the media CDN path. The run used the 2026-06-28 production SQLite snapshot, exported 80 real active events, passed `npm run check:preview` inside Kaggle, downloaded `static_site_build_result.json` + tar.gz, and was then deployed to Yandex Object Storage bucket `kenigevents.ru`. Before the build, legacy active media keys from `s3://kenigevents/p/...` were mirrored into `s3://kenigevents.ru/p/...`; verification found `957` needed active keys and `0` missing in the target bucket. Deploy also uploaded `80` stable calendar files to `s3://kenigevents.ru/ics/<event_id>.ics`.

Local post-deploy checks on 2026-06-28:

- `https://kenigevents.ru/preview-20260628-event-pages-v44-cdn-kaggle/__preview/` → `200`;
- `https://kenigevents.ru/preview-20260628-event-pages-v44-cdn-kaggle/segodnya/` → `200`;
- `https://kenigevents.ru/preview-20260628-event-pages-v44-cdn-kaggle/sobytiya/pesni-sssr-svetlogorsk-5878/` → `200`;
- `https://static.kenigevents.ru/ics/5878.ics` → `200`, `content-type: text/calendar; charset=utf-8`;
- sample CDN poster `https://static.kenigevents.ru/p/...webp` → `200`;
- control event HTML contains CDN `/p/...` media and stable `/ics/5878.ics`, and does not contain old raw `storage.yandexcloud.net/kenigevents/` image URLs.

### v43 share, carousel, price-link and today-fixture corrections

`preview-20260628-event-pages-v43` closes the share experiment. Rich/markdown hidden links are not a Web Share capability, so temporary experiment buttons are removed. The main `Поделиться` action remains the tested production path: image file + plain text + separate URL, with generated 1080×1350 PNG fallback and text/URL copy fallback. Fullscreen visual-photo gallery pan is slowed by ~40% (`17.9s`) while auto-advance now fires after a shorter `8.88s` interval, keeping the slower leftward motion but removing the dead wait. Paid events with missing price must not render `Билеты` as an admission value; they show `Цена уточняется`/`По билетам`, while real `price_label` values render the exact price or range and are also reflected in JSON-LD offers when possible.

v43 adds two user-visible fixes on top: the event-page mobile brand tag now wraps exactly like the fullscreen-gallery tag (`Полюбить / Калининград` on the kicker lines instead of clipping the text), and gallery auto-advance preloads/decodes the next image slides before moving to them so the transition does not begin over a black empty slide. If a paid event has a real price/range and a ticket URL, the price chip in the compact `Вход` fact and CTA panel may itself be the ticket link (`rel="noopener noreferrer nofollow"`, `data-nosnippet`) instead of adding another noisy label. The production export now selects exact same-day events first, then upcoming short events, then continuing long-running events, so `/segodnya/` is diverse and testable.

Local build verification (`npm run build:preview` + `npm run check:preview`) passed for v43; the current public focus preview is v47.

## v16/v17 personalization-contract correction + v18 UI A/B + v20 hero composition lab

`preview-20260628-event-pages-v43` keeps the discovery implementation aligned with the documented `event_detail_related` contract:

- `/data/discovery/<event_id>.json` now returns `schema_version`, `feature_schema_version`, `taxonomy_version`, `surface`, `algorithm_id`, `current_event` and `related_static[]` candidates with `category`, `tags`, `audience_exclusion_tags`, `base_similarity`, `reason_codes` and nested display data.
- Static HTML still preloads up to 10 cards; production target is 10 when enough eligible future events exist.
- Without consent or without a compatible profile, the static order remains the fallback and no profile is created.
- With consent and a compatible profile, browser JS runs the local `rankEventDetailRelated` formula: static related similarity remains dominant, explicit likes boost, `hidden_event_ids`/`not_interested_event_ids` hard-filter, strong `negative_interest_tags` remove unsuitable cards, and one same-origin JSON top-up restores the visible pool before the `Показать ещё` button takes over.
- Browser strong actions carry `served_list_id` / `served_list_hash` in the compact local log, matching the future Supabase `personalization_served_list_summary` write path.

## Verified on 2026-06-29

`npm run check:preview` passed inside the Kaggle CPU run for `preview-20260628-event-pages-v48-pgvector-gemma-kaggle`. The check covers the normal event-page/static contracts from v47 plus the pgvector discovery manifest contract: `algorithm_id=event_pgvector_related_chain_v1`, `strategy=event_pgvector_related_chain_v1_manifest`, `related_static[]`, mandatory `slot_type`, and at least one candidate carrying `vector_similarity`.

Live public smoke additionally verified HTTP 200 for the preview index and the golden anchor JSON `data/discovery/6447.json`; that JSON returns `6310` as the first related candidate with `vector_similarity≈0.8592` and `llm_semantic_score=0.92`. Like/profile writes remain local-only preview behavior; authorized search UI source is present but live search remains gated on Yandex OAuth and Edge Function deployment.


## Verified on 2026-07-01

Recovery preview `preview-20260701t2341-recovery-full` combines the restored full static-site branch work with the `feature/smart-search-quota-key5-site` authorized-search quota/key rollout. It was exported from the refreshed production SQLite snapshot for `2026-07-01`, generated `380` public events, passed `npm run build:preview` and `npm run check:preview`, and was deployed to `https://kenigevents.ru/preview-20260701t2341-recovery-full/__preview/` with `380` stable CDN ICS files. The refresh includes the late production events `6563`, `6564` and `6565`, verified by public HTTP checks against their generated event pages.

The export includes a narrow prompt-leak publication guard: rows whose title is obvious prompt/debug leakage are skipped before static pages and search fixtures are built. This is only a stopgap for preview/publication safety; the canonical production row still needs source/Smart Update repair. The recovery run used this guard to exclude event `6518` from the static preview and removed its stale `event_search_documents` / `event_embeddings` personalization rows.

Live search/auth verification for the same build passed through the deployed Supabase `event-search` Edge Function. The browser smoke on `/poisk/` queried `интересно детям`, rendered `18` cards, and the latest audit row recorded `request_kind=llm_rerank`, `status=ok`, `result_count=12`, `embedding_model=gemini-embedding-2`, `embedding_key_env=GOOGLE_API_KEY5`, `llm_model=gemini-3.1-flash-lite`, `llm_policy=lite_first_gemma_overflow`, and first Lite attempt key `GOOGLE_API_KEY3`. This confirms the recovered branch is not running on a single KEY5 lane.

Related/discovery data in this preview uses the two-document pgvector chain `event_pgvector_related_chain_v2_two_doc` with `embedding_document_version=related_v1`. Gemma strict related verification was not rerun for this full end-of-day refresh, so `strict_verified_related=false`; event pages still read the static related JSON and do not spend online embedding/LLM quota on page view.

## Counter freshness plan

Counter freshness is documented in [Event reaction counters](reaction-counters.md). The decision is manifest-first: static HTML keeps a build-time baseline for SEO/no-JS, while a small same-origin counter manifest should patch counters after first paint. Full page rebuilds are for event content/lifecycle changes, not for every like tick.
