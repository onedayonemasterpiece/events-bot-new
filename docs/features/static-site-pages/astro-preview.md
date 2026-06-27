# Astro SSG preview — event pages

> **Status:** implemented preview vertical slice, production rollout pending.  
> **Build ID:** `preview-20260627-event-pages-v2`  
> **Public preview index:** <https://kenigevents.ru/preview-20260627-event-pages-v2/__preview/>

This is the first real Astro SSG implementation for `kenigevents.ru` event detail pages in `events-bot-new`. It is intentionally a preview-only static slice: no Supabase write path, no personalization telemetry, no client recommendation API and no LLM fragments in rendered HTML.

## Public URLs

Required openable URLs for the current preview:

- Preview index: <https://kenigevents.ru/preview-20260627-event-pages-v2/__preview/>
- Control event page: <https://kenigevents.ru/preview-20260627-event-pages-v2/sobytiya/pesni-sssr-svetlogorsk-5878/>
- Control event calendar file: <https://kenigevents.ru/preview-20260627-event-pages-v2/sobytiya/pesni-sssr-svetlogorsk-5878/event.ics>
- Preview sitemap: <https://kenigevents.ru/preview-20260627-event-pages-v2/sitemap.xml>
- Preview robots: <https://kenigevents.ru/preview-20260627-event-pages-v2/robots.txt>
- Yandex Object Storage website fallback: <http://kenigevents.ru.website.yandexcloud.net/preview-20260627-event-pages-v2/__preview/>

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
  src/pages/sobytiya/[slug].astro
  src/pages/sobytiya/[slug]/event.ics.ts
  src/pages/sitemap.xml.ts
  src/pages/robots.txt.ts
  src/layouts/EventLayout.astro
  src/components/EventHero.astro
  src/components/EventCtaPanel.astro
  src/components/EventFacts.astro
  src/components/EventCard.astro
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
- related “Другие даты” pair `6437`/`6438`;
- static “Похожие события” plus separate anti-bubble “Попробовать другое”.

No future active sold-out/cancelled/postponed event was present in the active export used for this slice, so that optional state is not represented yet.

## SEO/GEO and preview safety

- All preview HTML has `meta name="robots" content="noindex,nofollow,noarchive"`.
- Prefix robots is exactly:

```text
User-agent: *
Disallow: /
```

- Preview canonical and `og:url` include `/preview-20260627-event-pages-v2/`; production canonical is not emitted by the preview build.
- Event pages render `schema.org/Event` / `MusicEvent` JSON-LD only from visible facts.
- The control `.ics` is a no-JS link and contains `DTSTART:20260711T193000Z`; it deliberately has no `DTEND` because reliable duration/end was not exported for event `5878`.

## Build and deploy

```bash
cd site
npm install
PREVIEW_BUILD_ID=preview-20260627-event-pages-v2 npm run build:preview
PREVIEW_BUILD_ID=preview-20260627-event-pages-v2 npm run check:preview
PREVIEW_BUILD_ID=preview-20260627-event-pages-v2 npm run deploy:preview
```

`deploy:preview` reads only the `KENIGEVENTS_SITE_YC_*` variables from the root `.env` and uploads `site/dist/<build-id>/` to the same prefix in the `kenigevents.ru` bucket. Calendar files are re-uploaded with `text/calendar; charset=utf-8` metadata.

## Visual review pass v2

The first public preview (`preview-20260627-event-pages-v1`) was superseded after visual review. `preview-20260627-event-pages-v2` makes the event page more mobile/feed-oriented:

- recommendation cards now have large image-led feed cards instead of text-only cards;
- the hero poster uses `object-fit: contain` so the poster is not cropped;
- duplicated facts/source/debug notes are collapsed into one `Описание и факты` disclosure;
- native mobile share is a Web Share API enhancement behind a real `Поделиться` button, with Telegram/VK/WhatsApp fallback links still present;
- the anti-bubble block label is now `Попробовать другое`, replacing the unnatural `Другие жанры рядом`.

## Verified on 2026-06-27

`curl` checks returned HTTP 200 for the preview index, control event, control `event.ics`, `sitemap.xml`, `robots.txt`, and the Yandex website endpoint fallback. The control ICS was checked for `DTSTART:20260711T193000Z` and absence of `DTEND`.
