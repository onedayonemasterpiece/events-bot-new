# Static unified prototype corrections R2 — integration report

Base: `bf35dddbd3a9abfd6e88f302b7d39e4b75dfa572`

Integration branch:
`integration/static-unified-prototype-corrections-20260723`

Verification owner: `/root`

## Lane reconciliation

| Lane | Requirements | Worker head | Integration commit | Status | Evidence |
|---|---|---|---|---|---|
| L01 Search | R01 | `b35a5780` | `a4681d42` | merged | `.codex/lanes/L01-R2/RESULTS.md` |
| L02 cards/navigation | R02, R04 | `6b33e7ca` | `a1a3b640` | merged | `.codex/lanes/L02-R2/RESULTS.md` |
| L03 duration/API | R03 | `d9ab97c8`, `d56bff95` | `6ab0beb4`, `46e7dfb4` | merged | `.codex/lanes/L03-R2/RESULTS.md` |
| L04 desktop chrome | R05, R06 | `b5ca0220` | `f38c8fc2` | merged | `.codex/lanes/L04-R2/RESULTS.md` |
| L05 integration | R07 | integration branch | pending final receipt | in progress | this report |

No worker branch was merged wholesale. All completed worker worktrees were
clean at handoff; only the listed commits were cherry-picked.

## Closure audit

| ID | Requirement | Status | Evidence / remaining gate |
|---|---|---|---|
| R01 | Editable, real authorized Search | Done | safe preview env resolver, saved PKCE intent, real Edge smoke; final generated/live UI gate pending |
| R02 | Exact compact crop/full-row contract | Done | source-keyed reviewed 6764 asset; 6686 row is full `5:4`/cover/equal-height; final generated/live gate pending |
| R03 | API-key duration automation; clean public copy | Done | keyed provider result, cached fail-closed script and automatic Kaggle stage; final generated/live gate pending |
| R04 | Predictable visual-order hotkeys and focused-only `K` | Done | named previews mount V7; 6529 spatial regression; final generated/live gate pending |
| R05 | Desktop photo breadcrumbs and rounded sheet | Done | desktop chrome contract; final generated/live screenshot gate pending |
| R06 | Leather desktop tag with immediate fallback | Done | deterministic WebP/metadata and forced-failure test; final generated/live screenshot gate pending |
| R07 | Immutable noindex preview and Gemini Pro acceptance | In progress | build, generated/live browser gates, deployment and final `agy` review remain |

## Final validation

- Focused Node integration suite: **81/81 passed**, including occurrence
  resolver/formatter, Search, crop/packing, keyboard and desktop chrome.
- Python duration/Kaggle plus
  `INC-2026-07-18-dramteatr-same-day-event-glue`: **27/27 passed**.
- Strict authorized preview build: **389 pages**.
- `check:preview`: passed for **288 events**.
- `check:unified-prototype`: passed for 18 primary routes, 288 event pages and
  373 checked related cards.
- Local Chromium at `1440×1000` and `390×844`: Search accepts input with an
  initially hidden skeleton; 6686's reviewed visual trio is a full horizontal
  `5:4` cover row with equal 302.39px media / 555.75px card heights; 6529
  contains 18:56/19:43 but no next-morning or model/service copy; hotkeys move
  `7032 → 6955` visually and expose exactly one focused `K`; breadcrumbs end
  4px above the 28px-radius sheet; the leather tag stays 240×88 and forced
  asset failure reveals `rgb(152,64,31)`; all checked surfaces have zero
  horizontal overflow.
- Immutable deployment, live replay and Gemini Pro acceptance remain pending.
