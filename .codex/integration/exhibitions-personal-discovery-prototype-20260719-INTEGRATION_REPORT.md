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
| R07 | Source likes/discussions/mentions | Done | Visible aggregate reason/signals and product-metric contract | Discussion/mention counts are presentation-only in prototype. |
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
