# Current UI Decoder v1 — manual visual review r2

## Coverage

All **157/157** source rasters from Actions run `31293484656` were opened and
inspected visually at original detail:

- 46 page screenshots;
- 111 component/controlled-specimen screenshots;
- 23 page contact sheets and 28 component contact sheets;
- 18 additional native-height segments covering the isolated `EventHero` and
  both exceptionally tall no-image Event Detail captures.

The review ledger SHA-256 is
`88eeaf712a8d7534d53ffabaa0ab98c6eaa54f3e8dea54c2086d8d5c69f7165b`.
It binds every reviewed raster to its own file SHA-256. Perceptual stability was
used only as capture assistance; it did not replace human inspection.

## Findings

| Evidence family | Human observation |
|---|---|
| Event presentation | Editorial/landscape and split/portrait-poster are visibly different desktop anatomies with different CTA placement. Mobile is independently composed. |
| Event Hero | The real definition renders its source image/content, but isolated fallback media/calendar/share elements become oversized without consumer/layout CSS; classified `consumer-exists-only`, not `match`. |
| CTA | Editorial side/stacked and split inline placement are distinct; free, phone and registration controlled states were also reviewed. |
| Media | A large primary/poster presentation and smaller remaining-photo previews are distinct. The real `EventMediaRail` specimen visibly renders three previews and `+7 фото`. |
| Transport | Rail explicit return, cutoff/last-train, controlled forecast, bus and KAUP compact/desktop formats are visible and readable. |
| Medallions | Top-slot, inline, zero, main/secondary and badge/pill states are visible; isolated geometry is not automatically promoted to page equivalence. |
| Artifacts | Amber and Focus Egg are visibly and behaviorally separate systems; they are not a shared variant family. |
| Page coverage | Exhibitions, For Me, Search, Favorites, Clubs, Festivals, Popular, Day and Weekend are present. |
| Known lab exception | The desktop-only CTA lab has an intentional mobile shell without a production body and is excluded from the production baseline. |
| Tall no-image page | Mobile fallback illustration, desktop no-image layout, CTA, facts and related-card continuation remain visible through the full native-height segments; no accidental clipping or blank tail was found. |

The visual review is observational. It confirms that the evidence depicts the
claimed AS-IS structures; it does not confirm component equivalence or approve
normalization.
