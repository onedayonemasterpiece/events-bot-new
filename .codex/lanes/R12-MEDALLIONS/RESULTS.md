# R12-MEDALLIONS results

Status: **Done**

## Delivered

- R12-02: bound event `7018` (`Воскресник в Озёрске`, 2026-07-26, `центр «Крупорушка»`) to the existing `ruin-keepers` organizer medallion through explicit manifest field `listingEventIds`. Resolver evidence is `event_id` / `curated_event`; description prose, title, city, and the unrelated venue cannot infer this identity.
- R12-03: projected the existing `rzd-lastochka` medallion only when the event's real `getEventTransportSuggestion(desktopEventWithExplicitEnd(event))` payload exists. The token is always `secondary`, hence InlineSlot and never TopSlot.
- Reused accepted artwork; introduced no generated or replacement logo assets.

## Supplier provenance

- RZD Lastochka: `c9a9bae61ad970d461324ff7376b670682644795` (`origin/feature/rzd-lastochka-medallion-20260723`).
- Complete medallion inventory: `68576d5b`.
- Source-faithful Ruin Keepers artwork: `fa367ea372e3b6f9608c7c407198a0ed08cb1df1` (`origin/integration/static-site-medallions-release-20260712`).

## Validation

- Focused resolver/transport tests: **19/19 passed**.
- Preview `preview-r12-medallions-feasibility`: **431 pages built** with canonical frozen clock `2026-07-24T10:00:00+02:00`.
- Canonical preview check: **passed**, 288 events.
- Generated-page runtime regressions: **18/18 passed**.
- Browser QA at 1440px and 390px: no viewport overflow; relevant WebP assets loaded at non-zero natural width. Event 6529 showed RZD beside MUMOD on mobile and in the desktop inline row. Event 7018 showed Ruin Keepers in its horizontally revealed mobile listing rail.

Visual evidence (ignored artifacts):

- `artifacts/codex/R12-MEDALLIONS/6529-desktop-medallions.png`
- `artifacts/codex/R12-MEDALLIONS/6529-mobile-medallions.png`
- `artifacts/codex/R12-MEDALLIONS/7018-mobile-listing-medallion.png`

Documentation and `CHANGELOG.md` are intentionally left to the integration owner per lane ownership.
