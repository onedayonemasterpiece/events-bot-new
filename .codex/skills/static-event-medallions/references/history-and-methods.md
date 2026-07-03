# Static medallion history and methods

Use this reference for medallion archaeology and method selection.

## Known commits / branches

| SHA | Date | Commit | What it established |
|---|---:|---|---|
| `d62464ee` | 2026-06-29 | `docs(static-site): design event medallions` | Canonical design doc: event-detail-only row, circle/pill system, LLM-first semantic badges, accessibility/SEO contract. |
| `8404c3b2` | 2026-06-29 | `feat(static-site): add organizer medallion avatars` | First organizer manifest and assets: World Ocean, KOIHM, Philharmonic, Kant Island; official SVG/PNG sources; WebP/PNG runtime. |
| `9bc23f9e` | 2026-06-29 | `style(static-site): match philharmonic medallion yellow` | Philharmonic background sampled from Telegram avatar (`#FAB534`). |
| `f0560599` | 2026-06-30 | `Improve static related full-build readiness` | Added early `EventTokenMedallions.astro` rendering on event pages. |
| `e5cb73e3` | 2026-07-01 | `feat(static-site): merge medallions with smart search quota` | Added Dom Kitoboya, Tretyakovka, KОNB, MEOW Афиша; social-avatar palette and local raster recomposition patterns. |
| `1d5a82cc` | 2026-07-02 | `feat(static-site): serve organizer medallions as SVG` | SVG-primary pass for several organizer medallions; left complex raster exceptions. |
| `5cc539c7` | 2026-07-02 | `fix(static-site): prefer webp for raster medallions` | WebP-first runtime with PNG fallback via `<picture>`. |
| `fb2570dc` | 2026-07-02 | `feat(static-site): add partner medallions and logo grid` | Partner/brand medallions and visual grid/lab. |
| `01a85a35` | 2026-07-02 | `fix(static-site): correct KGD80 organizer medallions` | KGD80/Act Opus/Znanie corrections; official blue for Znanie; curated KGD80 matching. |
| `aeb5f0d0` | 2026-07-02 | `fix(static-site): tune organizer medallion visuals` | Optical tuning: insets, clipping, tighter viewBox. |
| `1959dad5` | 2026-07-02 | `fix(static-site): recenter brand medallion artwork` | Further optical recentering/downward nudges. |
| `58b73ae7` | 2026-07-03 | `Add static event medallion workflow skill` | Prior version of this workflow skill. |
| `0e4b70bf` | 2026-07-03 | `feat(static-site): add festival medallions and date block` | Festival manifest/assets, festival rendering, Bahosluzhenie fallback, date block; branch `feature/event-issue-report-artkodex-20260703`. |

Recently relevant branches observed in history:

- `origin/feature/event-issue-report-artkodex-20260703`
- `origin/feature/static-medallion-svg-upgrade`
- `origin/agent/static-medallions-visual-tune-20260702`
- `origin/recovery/static-site-smart-search-full-20260701`

## Proven sourcing methods

### Official SVG / vector-first

Used for World Ocean, Philharmonic, Kant Island, KGD80/80 Stories, Street Food, Grozd, More Vnutri, Kaup, and some Znanie/KGD80-related marks. Pattern: preserve the official mark, wrap/crop into circle, choose brand background/ring, and document source URL/retrieval date.

### Social avatar as source or palette

Used or checked for Philharmonic palette, Dom Kitoboya palette, Tretyakovka avatar mark, MEOW Афиша source badge, City Jazz festival avatar. VK public/mobile pages often failed to expose clean festival-specific avatars, so prefer Telegram avatar when it is public and clean.

### Raster crop/recompose

Used for KOIHM official white PNG, Dom Kitoboya stacked wordmark, KОNB local reference, Act Opus raster wordmark, Kantata, Bahosluzhenie, Koroche, Ostrova, Simfoniya Vetra, Tolkin Fest. Pattern: retain source-faithful artwork, remove irrelevant text/background when needed, fit optically in circle, export WebP/PNG or PNG-only when transparency/wordmark requires it.

### WebP/PNG fallback

After `5cc539c7`, raster medallions should be WebP primary with PNG fallback/QA where possible. SVG medallions do not need a PNG fallback unless older client QA requires it.


## How historical SVG conversion actually happened

The SVG-upgrade history did **not** use `contour_svg`; it used several narrower methods:

1. **Official SVG path extraction/wrapping** — for example Philharmonic and Kant Island: the official SVG mark/path was placed inside a new circular medallion SVG with background/ring and adjusted `viewBox`/position.
2. **Manual/simple SVG primitive reconstruction** — World Ocean was rebuilt from simple triangles/circle for the `ММО` mark; Tretyakovka was rebuilt from rectangles/polygons for the gold `Т` mark.
3. **Local raster-to-path vectorization** — KOIHM had no working public SVG candidate; the accepted white PNG medallion was converted into a large SVG `<path>` and documented as locally vectorized from the source-faithful PNG. The exact command was not recorded in commit metadata; keep this as a fallback pattern, not the default.
4. **SVG container with embedded trusted raster** — Act Opus became an SVG file that embeds a base64 PNG wordmark inside the circular medallion. This is useful for layout/self-contained delivery but is not true vectorization; manifest used `assetFormat: svg-embedded-source-png`.
5. **Existing project SVG reuse** — Znanie/KGD80 used existing SVG/shared assets plus circles/backgrounds and optical recentering.

When documenting a new asset, distinguish `assetFormat: svg`, `svg-local-vectorized`, `svg-embedded-source-png`, and raster exceptions.

## `contour_svg` / `countur_svg` finding

No evidence was found that `contour_svg`, `countur_svg`, or `counter_svg` was used to create event-token medallions. The repo has a separate `contour_svg/` feature for photo-to-contour line-art. Its own docs warn against raw edge/vectorization as final output. For medallions, use source-faithful official SVG/raster handling; only consider `contour_svg` for non-logo architectural line-art program badges with explicit user intent and QA.

## Venue gap snapshot from 2026-07-03 production probe

Production probe date: 2026-07-03, `6197` event rows. Highest-priority missing organizer/venue medallions by future/current events:

| Venue | Future / total | Best visible source evidence | Likely source quality |
|---|---:|---|---|
| Янтарь холл | 39 / 218 | `@yantarholl`, `янтарьхолл.рф`, official SVG assets such as `Logo_YantarHall.svg` | Strong |
| Драматический театр | 17 / 227 | `@dramteatr39`, `dramteatr39.ru/img/logo.svg` | Strong |
| Музыкальный театр | 15 / 129 | `@muztear39`, `muzteatr39.ru` logo PNG | Strong |
| Калининградский театр эстрады / Дом искусств | 13 / 37 | VK `teatrestrady39`, official domains/ticket pages | Strong/medium |
| Калининград Сити Джаз Клуб | 10 / 35 | VK `jazzpub`, `pyramida.info`; festival medallion already exists separately | Medium |
| Ростех Арена | 9 / 27 | `@rostec_arena`, `vk.com/rostec_arena`, official site/social avatar | Medium/strong |
| Бар Бастион | 7 / 81 | VK `bar_bastion` | Medium |
| Сигнал | 5 / 153 | `@signalkld`, Timepad | Medium |
| Музей курортной моды | 5 / 19 | `mumod.ru/images/logo.svg`, `@muzeymod`, VK `mumod` | Strong |
| Калининградский зоопарк | 4 / 80 | `@kldzoo`, `kldzoo.ru/local/templates/s1/img/logo.png` | Strong |
| Стендап клуб Локация | 4 / 68 | `@locostandup`, official site/social avatars | Medium/strong |
| Музей Изобразительных искусств | 2 / 57 | `@kaliningradartmuseum`, official site/VK | Strong |
| Дом-музей Германа Брахерта | 2 / 32 | `brachert.ru`/`hbrachert.ru` from event links | Strong if site reachable |

Treat `Янтарь холл, Ленина 11, Светлогорск` as an alias of `Янтарь холл`.
