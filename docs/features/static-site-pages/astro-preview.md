# Astro SSG preview — event pages

> **Status:** implemented preview vertical slice, production rollout pending.  
> **Build ID:** `preview-20260627-event-pages-v5`
> **Public preview index:** <https://kenigevents.ru/preview-20260627-event-pages-v5/__preview/>

This is the first real Astro SSG implementation for `kenigevents.ru` event detail pages in `events-bot-new`. It is intentionally a preview-only static slice: no Supabase write path, no personalization telemetry, no client recommendation API and no LLM fragments in rendered HTML.

## Public URLs

Required openable URLs for the current preview:

- Preview index: <https://kenigevents.ru/preview-20260627-event-pages-v5/__preview/>
- Today listing: <https://kenigevents.ru/preview-20260627-event-pages-v5/segodnya/>
- Weekend listing: <https://kenigevents.ru/preview-20260627-event-pages-v5/vyhodnye/>
- Control event page: <https://kenigevents.ru/preview-20260627-event-pages-v5/sobytiya/pesni-sssr-svetlogorsk-5878/>
- Control event calendar file: <https://kenigevents.ru/preview-20260627-event-pages-v5/sobytiya/pesni-sssr-svetlogorsk-5878/event.ics>
- Preview sitemap: <https://kenigevents.ru/preview-20260627-event-pages-v5/sitemap.xml>
- Preview robots: <https://kenigevents.ru/preview-20260627-event-pages-v5/robots.txt>
- Yandex Object Storage website fallback: <http://kenigevents.ru.website.yandexcloud.net/preview-20260627-event-pages-v5/__preview/>

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
  src/pages/sitemap.xml.ts
  src/pages/robots.txt.ts
  src/layouts/EventLayout.astro
  src/components/EventHero.astro
  src/components/EventCtaPanel.astro
  src/components/EventFacts.astro
  src/components/EventCard.astro
  src/components/EventListItem.astro
  src/components/CalendarLink.astro
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
- one static neutral `Смотрите дальше` discovery feed; diversification is an internal ranking constraint, not a separate user-facing block.

No future active sold-out/cancelled/postponed event was present in the active export used for this slice, so that optional state is not represented yet.

## SEO/GEO and preview safety

- All preview HTML has `meta name="robots" content="noindex,nofollow,noarchive"`.
- Prefix robots is exactly:

```text
User-agent: *
Disallow: /
```

- Preview canonical and `og:url` include `/preview-20260627-event-pages-v5/`; production canonical is not emitted by the preview build.
- Event pages render `schema.org/Event` / `MusicEvent` JSON-LD only from visible facts.
- The control `.ics` is a no-JS link and contains `DTSTART:20260711T193000Z`; it deliberately has no `DTEND` because reliable duration/end was not exported for event `5878`.

## Build and deploy

```bash
cd site
npm install
PREVIEW_BUILD_ID=preview-20260627-event-pages-v5 npm run build:preview
PREVIEW_BUILD_ID=preview-20260627-event-pages-v5 npm run check:preview
PREVIEW_BUILD_ID=preview-20260627-event-pages-v5 npm run deploy:preview
```

`deploy:preview` reads only the `KENIGEVENTS_SITE_YC_*` variables from the root `.env` and uploads `site/dist/<build-id>/` to the same prefix in the `kenigevents.ru` bucket. Calendar files are re-uploaded with `text/calendar; charset=utf-8` metadata.

## Visual review passes

The first public preview (`preview-20260627-event-pages-v1`) was superseded after visual review. `preview-20260627-event-pages-v5` makes the event page more mobile/feed-oriented:

- recommendation cards now have large image-led feed cards instead of text-only cards;
- the hero poster uses `object-fit: contain` so the poster is not cropped;
- duplicated facts/source/debug notes were removed from the first screen;
- the long description is visible HTML, followed by facts and sources;
- native mobile share is attempted by the visible `Поделиться` button, with Telegram/VK/WhatsApp/copy fallback links still present.


After direct product review, `preview-20260627-event-pages-v5` rolls back the UX regressions introduced by the split recommendation rails:

- event description is visible HTML again, not hidden behind a collapsed `<details>` block;
- event continuation is one vertical mobile-first discovery feed (`Смотрите дальше`), not two horizontal scroll blocks and not a visible “try something else” module;
- desktop keeps the same continuation content as a grid, matching desktop expectations instead of mobile horizontal rails;
- `Поделиться` is visible always: it calls `navigator.share()` when the browser/webview supports native system share and falls back to copying the URL when native share is unavailable.
- diversity/anti-bubble is only a ranking/composition rule inside `Смотрите дальше`; the UI does not label cards as “Попробовать другое” or “Открыть новое”.

After consultant review, `preview-20260627-event-pages-v5` additionally hardens the first discovery layer:

- header links now open real static `/segodnya/` and `/vyhodnye/` pages, not QA anchors;
- related cards use a no-nested-anchor poster-card component with mandatory image/generated visual slot, direct page link, `.ics` calendar action and share link;
- `6437`/`6438` same-occurrence duplicates are excluded from “Похожие события” and remain only in “Другие даты”;
- source-only paid events use honest `Платный вход` copy instead of implying direct ticket purchase;
- weak-address pages do not show “Открыть на карте”;
- raw markdown/facts artifacts, hashtags in venue names, `null`/`undefined`/`NaN`, sitemap entries and all event `.ics` files are covered by `npm run check:preview`.

## Verified on 2026-06-27

`curl` checks returned HTTP 200 for the preview index, `/segodnya/`, `/vyhodnye/`, control event, source-only paid event `6437`, weak-address event `5690`, control `event.ics`, `sitemap.xml`, and `robots.txt`. The control ICS was checked for `DTSTART:20260711T193000Z` and absence of `DTEND`. Public HTML spot checks confirmed visual related cards, copy-link fallback, `Платный вход` for source-only paid events, and no map CTA on weak-address pages.
