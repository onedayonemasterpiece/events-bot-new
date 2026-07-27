# R15 execution matrix — unusual/static-site product integration

Base: `origin/main@551941bf9fc6ec3a647a0801fc704410cfc42761`
Integration branch: `integration/unusual-static-site-r15-20260727`

| ID | Requirement | Primary area | Dependencies | Conflict risk | Lane | Initial status |
|---|---|---|---|---|---|---|
| R01 | Mobile rail visual-only media is horizontal 5:4, cover-cropped, no bands; Pianissimo regression | EventCard/rail CSS + media contract/tests | current image_text_mode and safe-crop export | High: shared card CSS | L04 | Pending |
| R02 | Free collection has large right-side Free medallion and compact sticky shelf state | free collection route + medallion/shelf component | current materialized free collection | Medium | L04 | Pending |
| R03 | Menu/footer share image is rebuilt at least daily through existing Kaggle builder/render handoff | StaticSiteBuilder runner/kernel + share asset renderer/manifest | scheduler/coalesced build, object storage | High: builder shared files | L03 | Pending |
| R04 | Noindex Favorites: future calendar-added first, then liked; Supabase-auth hydration with skeleton/static shell | Astro page + shared auth runtime + personalization RPC/view | Supabase RLS/Data API, future event snapshot | Medium | L05 | Pending |
| R05 | Calendar navigates through furthest month containing events and makes empty dates hard to press accidentally | calendar/listing components + event availability manifest | generated date inventory | Medium | L04 | Pending |
| R06 | Home = hero-talk, quick navigation, cold-start feed up to 30 with progressive personalization | home route + personal feed/static manifest | R04 shared profile/auth runtime | Medium | L04/L05 | Pending |
| R07 | Mobile menu replaces Children with Collections submenu; Children/Unusual/Free/Clubs; Free remains top-level; coherent SVGRepo icons | Reference4MobileMenu + local sourced SVG assets/provenance | R08/R10 route availability | High: shared menu | L04 | Pending |
| R08 | Shared-BGE unusual taxonomy/prototype bank/scorer/calibration/concept dedup with hash-bound fail-closed gates | semantic module + calibration fixtures | L01 map; shared related document/vector contract | High | L02 | Pending |
| R09 | Coalesced StaticSiteBuilder integration, vector reuse, cache/atomic manifest/last-good, zero provider calls, observability | exporter/kernel/runner/cache schema | R08 API + current builder/BGE base | High | L03 | Pending |
| R10 | `/neobychnoe/` approved static feed, concept red-dot UX, nav, tests/docs/canary/no root cutover | Astro page/nav + manifest + Playwright + docs | R08/R09 outputs; R07 menu | High | L04/L06/L07 | Pending |
