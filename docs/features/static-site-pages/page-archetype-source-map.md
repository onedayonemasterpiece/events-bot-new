# Page archetype source map

> Status: accepted source map.  
> Source snapshot: `onedayonemasterpiece/events-bot-new@0bfbc3f94a6a8bebd9d7c849c3699e3358efde30`.  
> Target: LoveKGD Resource Graph, `60 — Page archetypes`.

## Purpose and authority

This map preserves the product requirements that preceded implementation and binds them to the Astro routes that actually exist at the pinned commit. Referenced feature documents remain authoritative for product meaning; `site/src/pages` is authoritative for route existence. The map is research context, not acceptance of a visual archetype.

## Verified families

### Home

**Original requirement:** explain the service quickly; provide fast discovery entries; render a useful static cold-start feed before personalization; keep the feed bounded.

- Requirements: `docs/features/static-site-pages/README.md`, `listing-personal-feed.md`
- Current route: `/`
- Runtime source: `site/src/pages/index.astro`

### Today, tomorrow and dated listings

**Original requirement:** show a concrete day's events in time order; keep date, time, place and actions readable; remain useful without dynamic services; support calendar/reminder continuity.

- Requirements: `listing-surfaces-v14-product.md`, `listing-personal-feed.md`, `schedule-user-requirements.md`
- Current routes:
  - `/segodnya/` → `site/src/pages/segodnya/index.astro`
  - `/zavtra/` → `site/src/pages/zavtra/index.astro`
  - `/date-{date}/` → `site/src/pages/date-[date].astro`

### Weekend and special listing surfaces

**Original requirement:** show a weekend as one two-day surface while retaining day/time structure and range navigation. Popular and other special surfaces may have their own composition and must not be classified as an ordinary collection merely because they contain cards.

- Requirements: `listing-surfaces-v14-product.md`, `listing-surfaces-v20-mobile-popular.md`, `listing-surfaces-v28-desktop-popular.md`
- Current routes:
  - `/vyhodnye/` → `site/src/pages/vyhodnye/index.astro`
  - `/vyhodnye/{start}/` → `site/src/pages/vyhodnye/[start].astro`
  - `/populyarnoe/` → `site/src/pages/populyarnoe/index.astro`
  - `/neobychnoe/` → `site/src/pages/neobychnoe/index.astro`
- Historical only: `/weekends`

### Search

**Original requirement:** accept a natural-language need; return relevant events with honest streamed progress and recovery; use shared cards, occurrence-family dedupe and current-build links; model auth/vector/LLM paths as states of one search journey.

- Requirements: `smart-vector-search/smart-vector-search-requirements.md`, `smart-vector-search/README.md`, `mobile-shell.md`
- Current route: `/poisk/`
- Runtime source: `site/src/pages/poisk/index.astro`
- Obsolete route: `/search`

### Event detail

**Original requirement:** in 5–10 seconds help the visitor understand the event, decide whether to go and perform the main action; expose honest date, time, place, status, admission, registration/tickets, other dates, calendar, share and provenance; then provide a finite relevant continuation.

- Requirements: `event-page-product-design.md`, `event-page-merged-skeleton.md`, `event-desktop-media-families-2026-07-12.md`, `event-mobile-ui-lab-2026-07-15.md`
- Current routes:
  - `/sobytiya/{slug}/` → `site/src/pages/sobytiya/[slug].astro`
  - `/sobytiya/{slug}/event.ics` → `site/src/pages/sobytiya/[slug]/event.ics.ts`
- Obsolete route: `/events/{id}`

### Collections, festivals, exhibitions and clubs

**Original requirement:** distinguish automatic list collections, future editorial selections and special product surfaces; preserve stable URLs and honest empty/dormant/error states; do not merge families merely because their pages look list-like.

- Requirements: `podborki.md`, `podborki-to-be.md`, `gastronomy-collection.md`, `festival-timeline.md`, `exhibitions-personal-prototype.md`
- Current routes:
  - `/podborki/` → `site/src/pages/podborki/index.astro`
  - `/podborki/{slug}/` → `site/src/pages/podborki/[slug]/index.astro`
  - `/podborki/gastronomiya/` → `site/src/pages/podborki/gastronomiya/index.astro`
  - `/festivali/` → `site/src/pages/festivali/index.astro`
  - `/vystavki/` → `site/src/pages/vystavki/index.astro`
  - `/kluby-po-interesam/` → `site/src/pages/kluby-po-interesam/index.astro`
  - `/kluby-po-interesam/{slug}/` → `site/src/pages/kluby-po-interesam/[slug]/index.astro`
- Obsolete route: `/collections/...`
- Not present in current Astro: `/festivali/{slug}/`, `/vystavki/{slug}/`

### Favorites and personal feed

**Original requirement:** Favorites preserves explicit user choices and calendar continuity. “Для меня” provides explainable preference-based discovery with a safe fallback and remains distinct from Favorites.

- Requirements: `schedule-user-requirements.md`, `listing-personal-feed.md`, `exhibitions-personal-prototype.md`
- Current routes:
  - `/izbrannoe/` → `site/src/pages/izbrannoe/index.astro`
  - `/dlya-menya/` → `site/src/pages/dlya-menya/index.astro`
- Obsolete route: `/personal`

### Focus group

**Original requirement:** explain the programme, invite and onboard a participant, preserve 30-day continuity, attach feedback to normal page families and keep diagnostics separate from the user happy path.

- Requirements: `docs/features/static-site-pages/focus-group.md`, `docs/features/static-site-focus-group/README.md`
- Current routes:
  - `/fokus-gruppa/`
  - `/fokus-gruppa/priglashenie/`
  - `/fokus-gruppa/zavershenie/`
  - `/fokus-gruppa/diagnostika/`
  - `/fokus-gruppa/diagnostika-ustoychivost/`
  - `/fokus-gruppa/kollektsiya/`
- Runtime roots: corresponding `site/src/pages/fokus-gruppa/**/index.astro` files.

### Partners and registration

**Original requirement:** separate partner discovery/partnership from the event page. Event registration must remain linked to event detail, prefill known identity, validate name data and support registration/check-in states.

- Current routes:
  - `/partners/` → `site/src/pages/partners/index.astro`
  - `/partnerstvo/` → `site/src/pages/partnerstvo/index.astro`
- Gap: no dedicated event-registration Astro route was found. `/events/{id}/register` is not current route evidence.

### Special, unavailable and closed states

**Original requirement:** explain restricted, closed, ended or unavailable states honestly and offer a safe next step; do not disguise data/auth failures as a product empty state.

- Requirements: `README.md`, `release-plan.md`, `focus-group.md`
- Current dedicated route: `/zakrytaya-afisha/` → `site/src/pages/zakrytaya-afisha/index.astro`
- Other empty/error/offline states belong in state matrices and product representations, not invented routes.

## Cross-cutting requirements for every archetype

- Responsive behavior must record real compositional changes across mobile, tablet and desktop, not scaled screenshots.
- Every archetype links product meaning and acceptance scenarios from Product Atlas; Resource Graph does not create a second backlog.
- Every runtime consumer is pinned to an exact repository SHA.
- Requirement, route implementation, visual contract and runtime evidence are independent dimensions.
- Missing evidence is an explicit gap, never an inferred positive status.

## Route corrections that must remain visible in Penpot

| Historical or assumed | Current evidence |
|---|---|
| `/search` | `/poisk/` |
| `/events/{id}` | `/sobytiya/{slug}/` |
| `/personal` | `/dlya-menya/` |
| `/collections/...` | `/podborki/`, `/podborki/{slug}/` |
| `/festivali/{slug}/` | absent; only `/festivali/` exists |
| `/vystavki/{slug}/` | absent; only `/vystavki/` exists |
| `/events/{id}/register` | unverified requirement gap |

## Synchronization contract

The design-system repository owns the machine-readable mapping and Penpot materialization. Every Penpot source-requirements overlay must retain:

- source repository and pinned SHA `0bfbc3f94a6a8bebd9d7c849c3699e3358efde30`;
- referenced requirement paths;
- verified current routes and runtime files;
- historical/unverified routes separately;
- `authority_mode: reconstructed`;
- `canonical: false`;
- an explicit statement that source context does not accept the visual archetype.
