# Consultant notes — event-page medallions — 2026-06-29

> **Status:** external-consultant synthesis for [event-token-medallions.md](event-token-medallions.md).  
> **Consultants:** `a-opus` / Opus 4.6 Thinking and `gemini --model gemini-3.1-pro-preview`.  
> **Raw local artifacts:** `artifacts/codex/site-personalization-tokens-20260629/` (not committed).

## What both consultants agreed on

- Use a **circle + pill medallion system on the event detail page**, not small text chips and not emoji.
- Keep the event-page row large: about `56px` detail medallions on desktop and `44px` on mobile; compact card tokens are only a deferred, separately approved option.
- Avoid horizontal token carousels on the event page; card-level compact medallions are not part of P0 after the user clarification.
- Store organizer/Pushkin assets locally and optimize them; do not rely on remote image URLs at runtime.
- The Pushkin-card image should be background-cleaned locally and rendered with `overflow: visible` so the text can protrude to the right while staying vertically inside the circle.
- Show weekday in card dates and render event type as plain text without `#`.
- Use LLM-first extraction for charity/kids/video status with confidence thresholds and source evidence; deterministic patterns are only support/guardrails.

## Accepted decisions

| Point | Decision |
| --- | --- |
| Token shape | Circle medallion for identity/logo, pill medallion when a text label is needed. |
| Row placement | Detail token row belongs after hero/title/summary in normal document flow, not over OCR poster text. |
| Card density | Removed from P0 scope: list/search/related cards do not get medallion rows now. |
| Token order | Organizer first, then Pushkin, charity, kids/family, video/online, accessible/free/other. Organizer first better matches the user request: visual recognition of the institution. |
| Pushkin asset | Use the provided bgtk.org image only as a source asset; process locally and commit the optimized derivative with source provenance. |
| Organization avatars | Start with the four requested organizations and official/local assets only. Unknown organizations use initials/fallback, not guessed marks. |
| LLM thresholds | Public token requires evidence and high confidence (`>=0.80`), review lane for `0.50–0.79`, reject below. |
| First implementation slice | Weekday + no-hashtag card type can ship independently as a list-card formatting fix; it is not the medallion surface. |

## Modified decisions

| Consultant suggestion | Modification |
| --- | --- |
| Put tokens on top of hero image. | Rejected as default because many event posters contain OCR text. Overlay only after per-image visual QA; P0 keeps row in document flow. |
| Make Pushkin a pill by default. | The requested visual is circle-first with partial right protrusion; docs define circle medallion with overflow and a text fallback. |
| Add `free` as a token in P0. | Deferred unless it adds information beyond CTA/quick facts; otherwise duplicates existing admission label. |
| Astro content-collection Zod as canonical model. | Current site uses JSON fixture/export, not content collections; docs express a projection contract that can be mapped to JSON first and Zod later if needed. |

## Deferred / not P0

- Tooltip/popover explanations.
- Clickable organizer/token filter pages.
- Accessibility, online, outdoor, language and tourist-friendly tokens unless source-grounded fields already exist.
- Badge-driven personalization controls; future work must use the existing controlled taxonomy/profile contract.
- Medallion rows inside event-list/search/related cards unless separately requested and designed.
- Any OpenAI image generation/editing for badges without explicit current-thread consent.

## Regression / acceptance gates to add with implementation

- Preview card HTML contains short weekday labels (`Пн`, `Вт`, etc.) and no `#` inside `.event-card__tag`.
- Token rows render as list semantics and include `aria-label` for icon-only medallions.
- Detail page token row has no horizontal scroll at 375px and 1440px.
- Public `charity`, `kids/family`, and `video` tokens each carry source evidence or a curated manual override.
- Organizer avatars are local optimized assets and have fallback initials.
