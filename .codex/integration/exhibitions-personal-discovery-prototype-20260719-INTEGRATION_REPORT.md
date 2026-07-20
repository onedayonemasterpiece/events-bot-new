# Exhibitions Personal Discovery Prototype — Integration Report

| Lane | Requirement IDs | Branch | Status | Head SHA | Merge/cherry-pick | Evidence |
|---|---|---|---|---|---|---|
| repository map | R01, R03–R07 | read-only | accepted | — | no write | Static-site route/components/personalization and incident coupling mapped. |
| UX/reference audit | R02, R08, R09 | read-only | accepted | — | no write | PNG tokens, timeline/deck transfer, responsive and keyboard/a11y contract applied. |
| integrator | R01–R09 | `integration/exhibitions-personal-discovery-prototype-20260719` | committed | `baebee6272dd273fe7445f983e08344b8d5dcd9d` | direct integration commit | Astro build, 17 static checks, Playwright three-viewport/interaction evidence, Gemini final Accept. |

## Requirement closure

| ID | Requirement | Status | Evidence | Missing / risk |
|---|---|---|---|---|
| R01 | Separate new prototype | Done | `/lab/exhibitions-personal/`; production `/vystavki/` diff empty | Lab-only until a later promotion decision. |
| R02 | Reuse supplied color/design approach thoughtfully | Done | Dark museum palette, timeline, photo decks, restrained borders, lifecycle/discussion markers | Not a pixel copy; readability and web behavior adapted. |
| R03 | `Для меня` before `Все` | Done | Complete radio group, cold-start fallback, F/A and arrow controls | Local fixture only. |
| R04 | Like / not interested form interests | Done | Local like tags, exact rejection, zero-shift undo stub and persistence | No Supabase write by design. |
| R05 | New/relevant navigation indicator | Done | Cold `3 новых`, personalized `2 новых`, soft `загляните`, meaningful-review clearing | Global site visit count needs integration later. |
| R06 | New first, then popular/ending, old tail demoted | Done | Hard-pinned inbox, explained priority mix, collapsed long tail | Production ranker remains a documented next slice. |
| R07 | Source likes/discussions/mentions | Done | Real exported likes/shares plus qualitative discussed reason; invented numeric discussion/mention fixtures removed | `shares_count` is source forwards/reposts, not guaranteed unique people. |
| R08 | Keyboard-first section navigation | Done | Skip link, roving ↑/↓, Enter/G/L/X/F/A, dialog arrows/Escape, input guard | Future dedicated keyboard skill can refine without changing baseline. |
| R09 | Responsive and visual verification | Done | 375/768/1440 screenshots, zero overflow, 44px targets, no console errors | Browser evidence stays ignored under artifacts. |

## External consultant

Gemini 3.1 Pro (High) via `agy` was used before implementation, for critical acceptance, and for final re-gate. Initial acceptance found one mobile-navigation P0 and three P1 issues; all four were fixed and the final gate returned `Accept` with no P0.

## Incident regression evidence

`INC-2026-07-02-exhibition-duplicates-static-site` was treated as a regression guard because the feature concerns the exhibition surface. This change does not alter import, Smart Update, production filters or production `/vystavki/`; therefore full replay/DB/public-surface incident closure is out of scope. Prototype-specific checks passed: unique curated ids, exact `event_type=выставка`, committed fixture presence, no duplicate DOM rows, and unchanged production route.

## v5 adaptive-height / perspective / loading closure

| ID | Requirement | Status | Evidence |
|---|---|---|---|
| R10 | Fixed desktop media width; tall row grows media height | Done | All visible desktop decks are `604.796875px`; event 4913 is `210.515625px` high vs `123.703125px` regular. |
| R11 | `Обсуждают` does not reduce title width | Done | Marker moved to fixed right aside; `markerInTitle=false`; title width is `412.328125px` across desktop rows. |
| R12 | Left timeline closer to reference | Done | Date rail outside bordered surface; date-pinned lifecycle dot and colored connector. |
| R13 | Skeleton during image loading | Done | Delayed response shows skeleton; cached/load cleanup; gallery error fallback; geometry delta `0`. |
| R14 | Perspective overflow and gray terminal plane | Done | Right edges/z are monotonic, depth heights decrease, first five previews contain images, sixth plane does not; hover delta `0`. |

V5 verification: Astro build `381` pages; prototype contract `38/38`; Playwright at `1440×1000`, `900×900`, `768×1024`, and `375×812` reported zero horizontal overflow and no console/page errors. Final `agy` review with Gemini 3.1 Pro (High): `ACCEPT`, no P0/P1. Its sole P2 about retained rejected-row height is superseded by the intentional zero-jump undo-stub contract.

## v6 timeline / keyboard / engagement closure

| ID | Requirement | Status | Evidence |
|---|---|---|---|
| R15 | Colored timeline must not cross date copy | Done | Connector is a bounded `20px` segment ending before the date; dot and spine share one x-axis; Playwright confirms `connectorStartsAtDot=0.5px`. |
| R16 | Up/down visibly selects a row; Enter opens it | Done | Global and in-row arrows move the single roving title link and apply whole-row halo; native Enter navigates to the event URL. |
| R17 | Rejection message stays centered inside exhibition surface | Done | Hidden stub begins at `--ex-surface-start`; desktop text and surface centers both measure `792.1953125px`; undo stays inside. |
| R18 | Mobile hides keyboard UI and strengthens timeline accents | Done | At `768px` and `375px`, keyboard/help counts are zero; dots are `14px` with two-layer colored glow; no overflow. |
| R19 | Explain and unify engagement metrics | Superseded | V6 used comment/@ presentation metrics; V7-R2 replaces them with exported shares and qualitative discussion only. |
| R20 | Mobile media opens event rather than image preview | Done | Media and title share the same honest event href; gallery trigger is desktop-only; Playwright confirms direct navigation and closed dialog on mobile. |

V6 local verification: Astro build `381` pages; prototype contract `43/43`; Playwright at `1440×1000`, `900×900`, `768×1024`, and `375×812` reported zero horizontal overflow and no console/page errors. Hover geometry delta is `0`; the local heart preference does not falsely increment the presentation aggregate. Final `agy` acceptance with `Gemini 3.1 Pro (High)`: `ACCEPT` across R1–R7, no P0/P1/P2.

## v7 media / counter truth / mobile-axis closure

| ID | Requirement | Status | Evidence |
|---|---|---|---|
| V7-R1 | Remove parasitic bottom-left image icon | Done | `.ex-gallery-trigger` and row image icon removed; Playwright finds zero painted triggers. Plain desktop media activation still opens gallery; modified click preserves href; mobile opens detail. |
| V7-R2 | Explain/remove mentions and show grounded shares | Done | Hard-coded numeric discussions/mentions removed. `likes_count` remains in heart; `shares_count > 0` renders as source repost/forward count; `Обсуждают` is numberless and gated by `popularity_reason_codes`. |
| V7-R3 | Align mobile bullet to the vertical line | Done | At `375`, `390`, `768px`, every visible row measures `dotX-spineX=0`; connector start/end rounding is under `0.01px`; overflow is zero. |

V7 local verification: Astro build `381` pages; prototype contract `45/45`; Playwright desktop/mobile interaction and screenshot gate reported zero console/page errors and hover geometry delta `0`. Predesign and final `agy` gates with `Gemini 3.1 Pro (High)`: `ACCEPT`; final R1–R3 had no P0/P1/P2.

## v8 physical shortcuts / contact stack / experimental paging closure

| ID | Requirement | Status | Evidence |
|---|---|---|---|
| V8-R1 | `L/X` keep working with a Russian keyboard layout | Done | Code-first `KeyL/KeyX` mapping with Latin key fallback; Playwright dispatches `key=д/code=KeyL` and `key=ч/code=KeyX`; modifier/repeat guards preserve state. `G/F/A` use the same contract. |
| V8-R2 | Remove the conditional blank between full photographs and the right deck | Done | First stack plane contacts/overlaps the last full card; full cards stay above the pile and remain readable. Playwright reports no gap, overflow or count-equation violation at `375, 768, 820, 821, 900, 1020, 1021, 1045, 1100, 1440px`. |
| V8-R3 | Experimental `←/→` group deal with cinematic motion | Done | Per-deck cursor/history, bounded queue, seven rebound shells, `190ms` exit + `470ms` staggered FLIP arrival, no loop, exact reverse, resize cancellation, instant reduced-motion and mobile no-op/direct-detail. |

The pre-design `agy` run with `Gemini 3.1 Pro (High)` returned `REVISE` and
identified four concrete risks: rendering every media source, simultaneous
conveyor-like motion, broad arrow interception and resize/WAAPI races. The
integrated solution bounds each deck to seven physical shells, staggers the
deal, intercepts `←/→` only from the selected row title or its own media link,
and cancels active animations before responsive relayout. The final Pro gate
returned `ACCEPT`, R1–R3 `PASS`, no P0/P1. Its sole P2 asked for fractional
zoom coverage; Playwright at `100/125/150%` on `1020/1440px` kept every frame
inside the deck with no positive gap and an unchanged `+N` equation.

V8 local verification: Astro build `381` pages; prototype contract `50/50`;
built-dist Playwright confirms Russian-layout actions, exact forward/reverse
cursor and counter state, stable row/deck/hover rectangles, responsive no-gap
geometry, bounded rapid input, instant reduced motion, delayed-image skeleton
without geometry shift, mobile direct-detail behavior, zero horizontal overflow
and zero prototype console/page errors.

Published from clean pushed commit `56f4ac91` as immutable noindex preview
`preview-20260720-exhibitions-personal-v8-56f4ac91`. Preview build produced
`383` pages, `check:preview` passed for `303` exported events, deployment
reported `Public preview verification: ok`, and the public lab route returned
HTTP `200`. Public Playwright repeated the `0 → 2` cursor / `+10 → +8`
transition, Russian-layout like, `-7.98px` deck contact, zero overflow and zero
console/page errors; mobile kept cursor `0`, hid all keyboard chrome, preserved
dot delta `0` and did not open the gallery.

## v9 soft personalization / terminal batch / full tail / footer closure

| ID | Requirement | Status | Evidence |
|---|---|---|---|
| V9-R1 | Liking `Диалоги` must not hide `Окна времени` | Done | Likes never hard-filter. One-like Playwright state keeps all 22 rows unfiltered and `Окна времени` visible; three current likes only stable-rerank the priority/tail buckets. Unseen badge drops from 3 to 2 only because the liked new row was meaningfully seen. `likedTags` rebuild after unlike and reject. |
| V9-R2 | Final photo batch must retain prior cards rather than leave a left blank | Done | The resolver selects the earliest fitting terminal suffix strictly after the prior cursor, start-aligns it, retains shared shells through FLIP and stores exact history. At `900/1020px`, `[8,9] → [9,10,11]`, retained id `9`, first left `0`, and reverse snapshots are exact. |
| V9-R3 | Down from the bottom must open the complete old-exhibition list with no repeats | Done | Current build derives every 21+ day exhibition not already featured, then removes normalized title repeats: 13 unique tail rows / 22 total. `↓` from `3216` expands and focuses `698`; `↑` returns to `3216` without collapsing. |
| V9-R4 | Reuse the common footer | Done | The prototype-local `display:none` was removed; the existing production `EventLayout → SiteFooter service-v1` path remains the only implementation. Desktop/mobile have zero overflow; footer focus retains `ArrowDown` instead of jumping to the exhibition list. |

V9 local verification: Astro build `381` pages; prototype contract `56/56` with
`22` unique event ids; Chromium Playwright covered one/three-like behavior,
physical keyboard navigation, terminal batching at `900/1020/1440px`, exact
reverse history, derived-tail disclosure, shared footer, mobile pager no-op,
zero horizontal overflow and zero console/page errors. The final external gate
through `agy`, model `Gemini 3.1 Pro (High)`, returned `ACCEPT`: R1–R4 `PASS`,
no P0/P1/P2.

Shared-footer tech debt is recorded but not duplicated into this fix:
production-wide `check-production.mjs` does not yet mirror the secret-candidate
`service-v1` assertions, and the accepted component retains legacy
`site-footer-prototype__*` CSS names.

Published from clean pushed commit `a6b4d662` as immutable noindex preview
`preview-20260720-exhibitions-personal-v9-a6b4d662`. Preview build produced
`383` pages; `check:preview` passed for `303` events and deployment reported
`Public preview verification: ok`. The public lab route returned HTTP `200`.
Public Chromium Playwright repeated the 22-row one-like/no-hide and
three-like/rerank states, the 13-row unique tail disclosure, retained terminal
batch at `900/1020px`, exact reverse history, footer-local arrow focus and
mobile cursor `0`, with zero console/page errors.
