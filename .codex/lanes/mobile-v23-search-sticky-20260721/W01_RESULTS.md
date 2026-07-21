# W01 — mobile calendar v23 implementation result

**Lane:** ignored `artifacts/codex/mobile-calendar-v23-research-20260721/**` only.
**Tracked deliverable:** this report only.
**Status:** PASS / ready for integration and independent acceptance.

## Implemented

1. **Today passed-start/no-end**
   - At the deterministic `2026-07-21 14:40 Europe/Kaliningrad` state, event 6804 keeps semantic `.is-started` and receives the accepted visual state: summary `#f1ece4`, title `#655e58`, image `grayscale(.68) saturate(.24)`, factual `Уже началось`.
   - No end time or duration is inferred.
   - `day_program` / `all_day` is not dimmed and is excluded from the initial marker target.
   - Initial marker/autoscroll targets the first future timed event (6968 at 16:00). A deterministic 08:00 negative control targets 6804 at 11:00 and marks nothing as started.

2. **Orpheus explicit occurrence projection**
   - Event 5511 retains the exact three-line block: `19:00 / 24 июля / 25 июля 17:00`.
   - Restored one recap after digest and immediately before medallions: `Ещё даты / 25 июля · 17:00`.
   - DTO is derived from mutual explicit links only and exposes exactly `data-linked-event-ids="5511,5512"`; 5525/5528 are excluded.
   - Rail/CTA remains on current event 5511. Event 5512 has the reciprocal IDs but no backward 24 July recap.

3. **Sticky Layout A**
   - Atomic pin remains within the existing 64px header.
   - Row 1: `24 июля · 20 событий`, fixed to one line; date 18/18 strong, count 10.5/12 muted.
   - Row 2: city 10.5/12 muted; no pill/background/border; only arrow is terracotta.
   - Scoped `:scope`/direct-child CSS prevents the previous nested-span font/display leak.
   - 320px and 390px gates verify the brand safe lane and zero page overflow.
   - Multi-city selection uses compact `Калининград +2`; the full city list remains in `aria-label`.

4. **Search boundary**
   - Bottom-nav Search href is exactly `https://kenigevents.ru/preview-20260721-mobile-search-donor-v23/poisk/` and is excluded from calendar query/city URL rewriting.
   - The local fake `public/poisk/` surface is deliberately omitted.

5. **Accepted v22 behavior retained**
   - v22 build was used as the donor; rail/crop/parallax/city/medallion/swipe behavior remains intact.
   - Explicit regression probes cover the 6764 5:4 crop and parallax factor/monotonicity.

## Artifact

- Builder: `artifacts/codex/mobile-calendar-v23-research-20260721/build-v23.py`
- Validator: `artifacts/codex/mobile-calendar-v23-research-20260721/validate-v23.cjs`
- Notes: `artifacts/codex/mobile-calendar-v23-research-20260721/README-v23.md`
- Output: `artifacts/codex/mobile-calendar-v23-research-20260721/public/`
- Report: `artifacts/codex/mobile-calendar-v23-research-20260721/v23-local-validation.json`

## Validation

- Inherited v22: **106/106 PASS** (`v22-local-validation.json`).
- Focused v23 Playwright: **36/36 PASS**.
- Browser isolation/closure follows the official Playwright BrowserContext lifecycle contract: <https://playwright.dev/docs/browser-contexts>.
- An initial resource-heavy sequential gate was unstable while the machine was under concurrent browser/build load; after two similar closed-target failures, the validator was narrowed to the explicit v23 acceptance cases plus the inherited 106/106 report rather than trial-and-error retries.

Visual evidence inspected:

- `v23-today-started-row-390x112-2x.png` — the muted 11:00 row and `Уже началось`.
- `v23-date24-orpheus-recap-scrolled-390x112-2x.png` — recap visible between digest and medallion.
- `v23-sticky-layout-a-320x667-2x.png`.
- `v23-sticky-layout-a-390x844-2x.png`.

## Scope / handoff

- No publishing, Telegram operation, production file, canonical docs, or CHANGELOG update was performed in this worker lane.
- Root integrator owns publication, public validation, Gemini acceptance, docs/CHANGELOG, Telegram, and branch integration.
