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
| L05 integration | R07 | integration branch | final receipt below | completed | this report |

No worker branch was merged wholesale. All completed worker worktrees were
clean at handoff; only the listed commits were cherry-picked.

## Closure audit

| ID | Requirement | Status | Evidence / remaining gate |
|---|---|---|---|
| R01 | Editable, real authorized Search | Partial | safe preview env resolver, saved PKCE intent, real Edge smoke and live editable UI pass; final Yandex login/return requires the reviewer's real session |
| R02 | Exact compact crop/full-row contract | Done | source-keyed reviewed 6764 asset; live 6686 row is full `5:4`/cover/equal-height |
| R03 | API-key duration automation; clean public copy | Done | keyed provider result, cached fail-closed script, automatic Kaggle stage and clean live UI |
| R04 | Predictable visual-order hotkeys and focused-only `K` | Done | live named preview mounts V7; 6529 moves `7032 → 6955` and exposes one focused hint |
| R05 | Desktop photo breadcrumbs and rounded sheet | Done | live desktop/mobile geometry and screenshot gate |
| R06 | Leather desktop tag with immediate fallback | Done | live material plus deterministic forced-failure check |
| R07 | Immutable noindex preview and Gemini Pro acceptance | Partial | prefix is published and live gates pass; valid Gemini Pro verdict is CONDITIONAL only on the R01 human OAuth round-trip |

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
- Immutable deployment:
  <https://kenigevents.ru/preview-20260723-unified-corrections-r2/__preview/>.
  Eight primary review routes returned HTTP 200 with `noindex`; upload was
  restricted to `s3://kenigevents.ru/preview-20260723-unified-corrections-r2/`
  and the deploy tool confirmed stable `/ics/*` were untouched.
- The same Chromium contract replay passed against the public origin.
- Final external acceptance used `agy --model gemini-3.1-pro-low`; the provider
  log resolves it to **Gemini 3.1 Pro (Low)**, exit 0, empty stderr. Verdict:
  **CONDITIONAL** solely because a headless reviewer cannot complete the real
  user's Yandex OAuth session. R02–R07 are PASS; no further code correction is
  required before the user performs that explicit R01 round-trip.
- Browser reports/screenshots and Gemini prompt/response/provider log are
  retained under
  `artifacts/codex/static-unified-corrections-r2-20260723/` (ignored by git).
