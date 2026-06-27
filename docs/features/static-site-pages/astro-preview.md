# Astro SSG preview — event pages

> **Status:** implemented preview vertical slice, production rollout pending.  
> **Build ID:** `preview-20260627-event-pages-v24`
> **Public preview index:** <https://kenigevents.ru/preview-20260627-event-pages-v24/__preview/>

This is the first real Astro SSG implementation for `kenigevents.ru` event detail pages in `events-bot-new`. It is intentionally a preview-only static slice: no Supabase write path, no personalization telemetry persistence, no server-side personalization API and no LLM fragments in rendered HTML. The first discovery hydration is a static same-origin JSON manifest, not a live ranking service.

## Public URLs

Required openable URLs for the current preview:

- Preview index: <https://kenigevents.ru/preview-20260627-event-pages-v24/__preview/>
- Today listing: <https://kenigevents.ru/preview-20260627-event-pages-v24/segodnya/>
- Weekend listing: <https://kenigevents.ru/preview-20260627-event-pages-v24/vyhodnye/>
- Control event page: <https://kenigevents.ru/preview-20260627-event-pages-v24/sobytiya/pesni-sssr-svetlogorsk-5878/>
- Control event calendar file: <https://kenigevents.ru/preview-20260627-event-pages-v24/sobytiya/pesni-sssr-svetlogorsk-5878/event.ics>
- Control discovery JSON: <https://kenigevents.ru/preview-20260627-event-pages-v24/data/discovery/5878.json>
- Preview sitemap: <https://kenigevents.ru/preview-20260627-event-pages-v24/sitemap.xml>
- Preview robots: <https://kenigevents.ru/preview-20260627-event-pages-v24/robots.txt>
- Hero composition lab: <https://kenigevents.ru/preview-20260627-event-pages-v24/lab/hero/>
- Hero viewport review: <https://kenigevents.ru/preview-20260627-event-pages-v24/lab/hero/review/>
- Yandex Object Storage website fallback: <http://kenigevents.ru.website.yandexcloud.net/preview-20260627-event-pages-v24/__preview/>

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
  src/pages/vyhodnye/index.astro
  src/pages/sobytiya/[slug].astro
  src/pages/sobytiya/[slug]/event.ics.ts
  src/pages/data/discovery/[eventId].json.ts
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
  src/components/CalendarLink.astro
  src/components/Icon.astro
  public/favicon.svg
  src/data/preview-events.json
  src/data/preview-related.json
```

## Fixture coverage

The preview uses 10 real production event rows exported read-only from Fly SQLite artifacts under `artifacts/codex/static-event-page-preview-2026-06-27/` and committed as a compact static fixture:

- `5878` — «Песни СССР», paid sale, control slug `pesni-sssr-svetlogorsk-5878`;
- free / registration with link;
- registration/source-only without direct ticket link;
- phone-only CTA;
- unknown/source-only CTA;
- long Russian title wrapping in main column;
- no local image hero fallback;
- weak/missing address fallback;
- static `/segodnya/` and `/vyhodnye/` listing pages from the same fixture;
- related “Другие даты” pair `6437`/`6438`;
- one static neutral `Смотрите дальше` discovery feed; diversification is an internal ranking constraint, not a separate user-facing block;
- up to 10 preloaded discovery candidates in static HTML, plus a same-origin `/data/discovery/<event_id>.json` `event_detail_related` manifest (`schema_version=event-detail-related-v1`, `related_static[]`) for one automatic client hydration after JS applies a consented compatible local profile; further expansion is explicit through `Показать ещё`;
- explicit card reactions: like count + toggle like/unlike, “Не интересно”, local compact raw log/report for the current anonymous browser profile;
- honest like baseline: visible `likes_count` is `source_likes_count + service_likes_count`; `source_likes_count` is aggregated from available production TG/VK source-post metrics, while `service_likes_count` is the future first-party KenigEvents counter and remains `0` in this static preview; public HTML/UI shows only the total count; source/service split is technical and must not be rendered as copy or data attributes;
- detail-page calendar action links open `.ics` directly rather than forcing a download, but only for one-day/short events; feed/preview cards do not show the calendar icon because the mobile bottom action row is limited to `Не интересно` + `Поделиться` + like.
- `image_text_mode` (`ocr_text` / `visual_only` / `unknown`) is a required export field. This preview does **not** run OCR during Astro build; it consumes the fixture value that must be produced by the existing media/OCR pipeline in production export. If this field is missing, the safe default is `unknown` → natural-ratio no-crop rendering.
- visible Russian dates omit the current year when both boundaries are in the build current year; cross-year ranges keep both years.

No future active sold-out/cancelled/postponed event was present in the active export used for this slice, so that optional state is not represented yet.

## SEO/GEO and preview safety

- All preview HTML has `meta name="robots" content="noindex,nofollow,noarchive"`.
- Prefix robots is exactly:

```text
User-agent: *
Disallow: /
```

- Preview canonical and `og:url` include `/preview-20260627-event-pages-v24/`; production canonical is not emitted by the preview build.
- Event pages render `schema.org/Event` / `MusicEvent` JSON-LD only from visible facts.
- The control `.ics` is a no-JS link and contains `DTSTART:20260711T193000Z`; it deliberately has no `DTEND` because reliable duration/end was not exported for event `5878`.

## Build and deploy

```bash
cd site
npm install
PREVIEW_BUILD_ID=preview-20260627-event-pages-v24 npm run build:preview
PREVIEW_BUILD_ID=preview-20260627-event-pages-v24 npm run check:preview
PREVIEW_BUILD_ID=preview-20260627-event-pages-v24 npm run deploy:preview
```

`deploy:preview` reads only the `KENIGEVENTS_SITE_YC_*` variables from the root `.env` and uploads `site/dist/<build-id>/` to the same prefix in the `kenigevents.ru` bucket. Calendar files are re-uploaded with `text/calendar; charset=utf-8` and `Content-Disposition: inline; filename="event.ics"` metadata so mobile clients can open the `.ics` instead of treating it only as a forced download.

## Visual review passes

The first public preview (`preview-20260627-event-pages-v1`) was superseded after visual review. The current `preview-20260627-event-pages-v24` keeps the event page mobile/feed-oriented and feedback-aware, and replaces the v19 safe image block with the v20 hero composition lab from `event-hero-lab-2026-06-27.md`:

A/B feed-card UI remains part of the preview contract: event `5878` uses Variant A (`overlay-controls`), event `6322` uses Variant B (`split-actions`) with calendar utility action inside eligible cards and right-aligned share + like under the card. See `event-card-ui-ab-2026-06-27.md`.

- recommendation cards now have large image-led feed cards instead of text-only cards;
- event hero keeps deterministic media modes (`poster-stage` for OCR/unknown, `photo-cover` for verified `visual_only`, `fallback-art` for no image), but now adds explicit composition variants (`poster-billboard`, `poster-attached-card`, `photo-cinematic-sheet`, `photo-parallax-sheet`, `compact-ticketing`); mobile hero visual breaks out to 100vw where appropriate, H1/CTA remain HTML in a decision sheet, OCR/unknown posters are not cropped, and visual-only images may use cover. Cards/listings keep the OCR-safe v15 rule: `visual_only` cover/crops inside a strict vertical 4:5 frame; `ocr_text|unknown` renders the actual image at natural aspect ratio with no crop, no fixed cover frame, no duplicate/backdrop underlay and no blur fill;
- duplicated facts/source/debug notes were removed from the first screen;
- the long description is visible HTML, followed by facts and sources;
- native mobile share is attempted by one visible `Поделиться` button; duplicate Telegram/VK/WhatsApp share pills were removed, and fallback copies the URL when system share is unavailable.
- footer social navigation mirrors the Telegraph editorial footer and adds Max: Telegram `@kenigevents` + `@kldevents`, VK `kenigeventsofficial` + `klgdevents` + `vk.ru/im/channels/-239844596`, and `max.ru/join/...`; site footer uses visible Telegram/VK/Max SVG icons, while Telegraph remains plain links only.
- a favicon is emitted from the selected SVG calendar/heart motif so browser/share surfaces have a site icon.


After direct product review, `preview-20260627-event-pages-v24` rolls back the UX regressions introduced by the split recommendation rails and adds the first static-seed/client-hydration discovery contract:

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
- marking `Не интересно` turns the current card into a grey explanatory plate (`Вы пометили: не интересует`) and keeps it in place until the next page/reload; later personalization may remove/demote similar events on subsequent surfaces.
- explicit-feedback rerank is viewport-stable: after a user action, the acted-on card and all cards above it keep their positions; only cards below the action anchor may be re-ordered.
- same-year visible dates omit the year (`11 июля · 21:30`), while cross-year ranges keep the year on both sides (`12 июня 2026 — до 28 марта 2027`).

After consultant review, `preview-20260627-event-pages-v24` additionally hardens the first discovery layer:

- header links now open real static `/segodnya/` and `/vyhodnye/` pages, not QA anchors;
- related cards use a no-nested-anchor poster-card component with mandatory image/generated visual slot and direct page link; `.ics` calendar action is deliberately kept on the detail page, not in feed cards;
- `6437`/`6438` same-occurrence duplicates are excluded from “Похожие события” and remain only in “Другие даты”;
- source-only paid events use honest `Платный вход` copy instead of implying direct ticket purchase;
- weak-address pages do not show “Открыть на карте”;
- raw markdown/facts artifacts, hashtags in venue names, `null`/`undefined`/`NaN`, sitemap entries and all event `.ics` files are covered by `npm run check:preview`.


## v16/v17 personalization-contract correction + v18 UI A/B + v20 hero composition lab

`preview-20260627-event-pages-v24` keeps the discovery implementation aligned with the documented `event_detail_related` contract:

- `/data/discovery/<event_id>.json` now returns `schema_version`, `feature_schema_version`, `taxonomy_version`, `surface`, `algorithm_id`, `current_event` and `related_static[]` candidates with `category`, `tags`, `audience_exclusion_tags`, `base_similarity`, `reason_codes` and nested display data.
- Static HTML still preloads up to 10 cards (the compact 10-event fixture can yield fewer for the control page because the current event is excluded; production target is 10 when enough eligible future events exist).
- Without consent or without a compatible profile, the static order remains the fallback and no profile is created.
- With consent and a compatible profile, browser JS runs the local `rankEventDetailRelated` formula: static related similarity remains dominant, explicit likes boost, `hidden_event_ids`/`not_interested_event_ids` hard-filter, strong `negative_interest_tags` remove unsuitable cards, and one same-origin JSON top-up restores the visible pool before the `Показать ещё` button takes over.
- Browser strong actions carry `served_list_id` / `served_list_hash` in the compact local log, matching the future Supabase `personalization_served_list_summary` write path.

## Verified on 2026-06-27

`npm run check:preview` passed for `preview-20260627-event-pages-v24`: the control discovery JSON is checked for `schema_version=event-detail-related-v1`, `surface=event_detail_related`, `algorithm_id=static_related_v1`, `related_static[]`, display calendar fields, source/social counter consistency, footer social links/icons, parseable Event-class JSON-LD and both feed-card UI variants, the hero lab/review routes, poster/photo hero modes, explicit composition markers, one-H1 contract and split-actions icon styling. Playwright smoke evidence for the v24 hero/menu/parallax pass is stored locally under `artifacts/codex/hero-impact-v24/` (not committed): public control/sliding-row screenshots, visual-only parallax screenshot and `hero-menu-parallax-smoke-public-result.json`.

## Counter freshness plan

Counter freshness is documented in [Event reaction counters](reaction-counters.md). The decision is manifest-first: static HTML keeps a build-time baseline for SEO/no-JS, while a small same-origin counter manifest should patch counters after first paint. Full page rebuilds are for event content/lifecycle changes, not for every like tick.
