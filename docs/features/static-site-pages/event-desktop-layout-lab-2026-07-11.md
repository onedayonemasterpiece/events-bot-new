# Event-page desktop layout lab — 2026-07-11

Status: **noindex review lab; no desktop option promoted to the default event page yet**.

Public surface: <https://kenigevents.ru/preview-20260711t-desktop-event-layouts/lab/event-desktop/>.

The same fresh preview was generated from a 2026-07-11 production Fly SQLite snapshot with the local Kaliningrad cutoff `2026-07-11T23:20`: `312` active/future events through id `6832`, including the four CTA/media stress cases `6345`, `6510`, `6678` and `6750`.

## Product correction before layout work

The CTA must state the action the service can actually complete:

| Source-grounded state | Primary action | Admission copy |
|---|---|---|
| Free, no explicit registration | `Добавить в календарь` | `Бесплатно` |
| Explicit `ticket.kind=registration` or phone booking | registration/phone source action | `Бесплатно · регистрация/по записи` or known paid state |
| Paid, known price/range | source action (`Купить билет`, phone, etc.) | exact price/range |
| Paid, unknown price, real ticket URL | `Билеты` | no invented price |
| Sold out | disabled sold-out state | `Билеты закончились` |

`Узнать цену` is rejected: clicking a ticket destination does not guarantee that the user will learn a price. `Открыть условия` is rejected for free events without explicit registration: it invents a condition and competes with the useful calendar action. `Билеты` is a deliberately neutral temporary label for a real ticket destination; later refinement may distinguish ticket inventory/box office, but only from structured source evidence.

Calendar/share/like remain visible mandatory actions. The current trial uses shared SVG icons at `28–30px` in `48px+` targets. The calendar has no `+` glyph and no fabricated count.

## Why desktop is a separate layout problem

The previous `>=1024px` composition showed event facts in the hero and repeated date, place, admission and CTA in a sticky right card. It also hid the hero's like action. This is not solved by enlarging the mobile sheet: each desktop option must assign one owner to every fact and action.

The accepted mobile contract remains unchanged: full-bleed/parallax hero, one light overlapping decision sheet with H1 inside it, then large horizontally scrolling medallions. Desktop experiments are isolated to the lab until one horizontal/square and one portrait option are selected.

## Six implemented review options

### Horizontal / square

1. **H1 Editorial Slab** — full-width contained hero; a light overlapping fact slab and separate graphite action rail. Strongest editorial candidate.
2. **H2 Split Canvas** — equal image and decision canvases. Safest long-title/control option.
3. **H3 Immersive Bottom Horizon** — image-first stage with one bottom fact/action horizon. Highest impact and highest contrast/overlay risk.

### Portrait OCR

1. **P1 Gallery Exhibition** — full-height contained poster left, fact/action flow right. Safest OCR candidate.
2. **P2 Billboard + Action Rail** — large contained poster billboard; the rail owns only admission/actions.
3. **P3 Typographic Lead** — accessible HTML title/date lead on the left and a fully contained poster on the right. Best stress case for very long H1.

Every prototype demonstrates the transition into normal description flow and the unchanged standard `Смотрите дальше` module. Sticky behavior described by a selected option must be bounded by the hero/details wrapper and end before recommendations.

## Media contract

- `ocr_text` and `unknown` use `object-fit: contain`; poster text is never cropped.
- Event `6510` is a known source-grounded classifier miss: the selected 1080×1080 poster visibly contains large event typography while its production OCR payload is absent. Export applies an explicit `FORCE_OCR_IMAGE_MODE_IDS` QA override so the poster remains whole until upstream classification is repaired.
- `visual_only` may use cover only when the asset has no meaningful poster text.
- Portrait desktop canvases normalize useful vertical space (`~calc(100vh - chrome)`) but the wrapper eventually scrolls away; there is no page-long fixed poster.

## Onboarding modes

Onboarding is not a permanent or guaranteed first-view element:

- default `PUBLIC_EVENT_ACTION_ONBOARDING_MODE=off`;
- `adaptive` waits until at least a later event-page view and still respects use, session, impression and cooldown suppression;
- `calendar-first` allows the current first stable eligible calendar hint behavior for an explicit experiment;
- `?onboarding=calendar` is a review-only force override.

Only one hint may appear, below the unchanged action row. The current production capability remains `.ics` only; no UI copy may claim that a site save or email notification already happened.

## Consultant evidence

- Gemini Pro class: `Gemini 3.1 Pro (High)` / Gemini 3.1 Pro completed the six-option specification and ranked H1/P1 first. Local ignored artifact: `artifacts/codex/event-page-desktop-20260711/gemini-spec.md`.
- `a-opus` (`Claude Opus 4.6 (Thinking)`) was requested on 2026-07-11 and returned `Individual quota reached … Resets in 110h…`; local evidence: `opus.stderr`.
- Claude Code project agent `Opus`, effort `max`, was attempted as the allowed fallback and returned `Not logged in · Please run /login`; local evidence: `opus-claude-code.md`.
- No Sonnet/Haiku/Flash/Lite response was substituted, and the lab does not claim completed Opus review.

## Acceptance gate

1. No duplicated date/place/admission/primary/action cluster within a desktop option.
2. Exactly one primary CTA for each ticket state.
3. `Узнать цену`, `Открыть условия` and calendar `+` are absent from the trial.
4. All action targets are at least `48×48px`; icons are `28–30px` and retain accessible names.
5. OCR/unknown images are fully visible at 1024, 1280, 1440 and 1920 widths.
6. Long H1 does not overlap the action owner or leave the fold without key date/place/CTA.
7. Hero/sticky state ends before `Смотрите дальше`.
8. Existing 320/390 mobile hero-overlap geometry has no fundamental change or page-level horizontal overflow.

Verified evidence for the published preview: static build `336` pages; `check:preview` passed; public HTTP returned `200` for the lab, index, search and three representative event pages; local and public Chromium Playwright each passed four scenarios covering all six variants, 390px free-event CTA/onboarding, forced QA onboarding, 390/1440 OCR containment, icon size and horizontal overflow.
