# R11 integration report

## Release scope

- Branch: `integration/unified-corrections-r11-20260724`
- Base: `956683719543667f4042f3b81a88b1b5b7605ef8`
- Preview build: `preview-20260724-unified-corrections-r11`
- Scope: immutable noindex prototype only; production generation is unchanged.

## Requirement closure

| ID | Status | Evidence |
|---|---|---|
| R11-01 | Done | The first negative rail swipe opens a device-local consent dialog; cancel does not persist, a successful canonical negative action does, subsequent swipes are direct, and the existing Undo toast remains. Covered by `mobile-listing-rails.test.mjs` and the 320/390 Playwright gate. |
| R11-02 | Done | The compact date is revealed at the left of the existing sticky discovery shelf only after the hero leaves. Browser evidence: `artifacts/codex/r11-validation/date-desktop-sticky.png`. |
| R11-03 | Done | Desktop continuation/personal feeds use contiguous three-card packing and the shared row crop resolver; mobile media decisions remain independent. The OCR fail-closed contract was not weakened. Covered by `personal-feed-surface.test.mjs`. |
| R11-04 | Done | Telegram thread messages 669–671 and screenshots 670/671 were inspected through the approved E2E human session. Evidence: `artifacts/codex/r11-search-map/`. |
| R11-05 | Done | Popular uses one lifecycle predicate on desktop/mobile, keeps ranges through inclusive `end_date`, rejects ended/non-public events, and the preview uses its actual 2026-07-24 build clock rather than stale snapshot metadata. |
| R11-06 | Done | Generated output contains `24, 25 июл, 27 сен`; the multi-day rail tile is `8–9 августа` with a full accessible label. |
| R11-07 | Done | Today muting is computed at runtime and applies to ended/old mobile rail media only, not the whole row or desktop. |
| R11-08 | Done | Search has header, per-read idle, and overall watchdogs, one bounded JSON rescue, epoch-owned cleanup, and a deterministic name/email identity. Mocked authenticated browser recovery passes; the real authenticated Edge smoke remains green. |

The accepted noindex-only amber tail artifact remains enabled for this preview
with `PUBLIC_ENABLE_AMBER_ARTIFACT_RESEARCH=tail`; the production hard block is
covered by the static rail contract.

## Integration order

1. `2e8bc6ae` — shared personal-feed media packing.
2. `8bed801f` — Popular lifecycle and occurrence compaction.
3. `6901fbfe` — swipe consent, sticky date context, Today muting, compact rail range.
4. `a62346fd` — authenticated Search watchdogs and identity.
5. `894a540c` — current preview clock, final identity fallback, canonical docs and changelog.
6. `40bca4fd` — browser gates made build-clock and project-ref aware.

Worker evidence is recorded in:

- `.codex/lanes/R11-CROP/RESULTS.md`
- `.codex/lanes/R11-LIFECYCLE/RESULTS.md`
- `.codex/lanes/R11-LISTING/RESULTS.md`
- `.codex/lanes/R11-SEARCH/RESULTS.md`

## Validation

- Astro preview build: **431 pages**, Search configured.
- `check:preview`: **PASS**, 288 events.
- `check:unified-prototype`: **PASS**, 18 primary routes / 288 event pages.
- Integrated Node suites: **43/43 PASS**.
- Mobile listing Playwright: **PASS** at 320px and 390px.
- Search recovery Playwright: **PASS**.
- Generated canaries:
  - `24, 25 июл, 27 сен`;
  - `8–9 августа`;
  - active range `Точка и линия`.
- Real authenticated Edge smoke: **PASS**, 9 cards plus pagination; see
  `artifacts/codex/r11-search-map/real-edge-smoke.txt`.
- Public noindex smoke: **PASS** on the preview hub, date, Today, Weekend,
  Popular and Search routes; desktop 1536px and mobile 390px browser loads
  returned 200 with no page/request errors.

## Handoff

- Published preview:
  `https://kenigevents.ru/preview-20260724-unified-corrections-r11/__preview/`
- Upload verification: main domain and website endpoint **PASS**; all writes
  remained under the versioned preview prefix.
- Telegram topic handoff: message `687`, reply to topic root `548`, verified
  through the approved E2E human session.

## External review

Early product/architecture consultation completed with **Gemini 3.1 Pro
(High)** and is stored at
`artifacts/codex/r11-gemini/early-review.md`. Its progressive-disclosure,
sticky-date, runtime-Today, and identity recommendations were implemented while
its generic media suggestion was rejected where it conflicted with the
canonical OCR fail-closed crop contract.

The final post-build Antigravity acceptance attempt was blocked before model
execution by the provider account/location eligibility check. `a-opus` was
blocked by the same Antigravity eligibility check and the Claude Code `Opus`
fallback was not logged in. Exact redacted evidence is retained in
`artifacts/codex/r11-gemini/final-acceptance*.md`; no lower-class model was
substituted or reported as a completed final review.
