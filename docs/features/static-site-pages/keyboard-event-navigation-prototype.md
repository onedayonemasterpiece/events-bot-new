# Desktop keyboard event navigation prototype

> **Status:** lab prototype, not a production-wide keyboard override  
> **Route:** `/lab/keyboard-event-navigation/`  
> **Fixture:** event `6408`, «Спектакль „Собака на сене“»  
> **Scope:** one desktop event-detail page (`min-width: 1024px`)

## Published review

Current public noindex prototype:

<https://kenigevents.ru/preview-20260719-keyboard-event-navigation-v4/lab/keyboard-event-navigation/>

V4 supersedes the visually obstructed v1 review and the v2/v3 interaction
passes. Its preview prefix contains exactly one new HTML object and reuses the
immutable v1 page assets; it does not publish listing/event catalogs, modify
production root or touch stable `/ics/*`.

## Product hypothesis

The prototype tests whether a visitor can reject the current event and start
surfing alternatives with one keystroke, without turning the whole site into a
custom keyboard application.

The current-event CTA panel and each related-event card form a scoped composite
navigator. Header, footer controls, form controls and other page regions keep
their native browser behavior. `Tab` remains available for links and card
actions. `Space` remains native page scrolling except for the deliberately
active final recommendation slide inside the fullscreen gallery.

The lab route intentionally focuses the current-event CTA surface on load. A
production rollout must not auto-focus arbitrary direct visits; it should
activate only after explicit keyboard entry or a restored listing-to-detail
journey.

## Key contract

| Focus context | Key | Result |
| --- | --- | --- |
| Current-event CTA | `ArrowLeft` / `ArrowRight` | Previous / next hero image |
| Current-event CTA | `ArrowUp` | Open the existing fullscreen hero gallery |
| Current-event CTA | `ArrowDown` | Focus the first related event |
| No focused control (`body`) | `ArrowDown` | Re-enter navigation at the first related event |
| Current-event CTA | `Enter` | Run the visible primary CTA |
| Current-event CTA | `L` / `K` | Like / calendar for the current event |
| Current-event CTA | `S` | Copy exactly the event title and canonical URL; never open Web Share |
| Related card or inner action | `ArrowLeft` / `ArrowRight` | Previous / next card in visual DOM order, including row wrap |
| Related card or inner action | `ArrowUp` / `ArrowDown` | Nearest card in the adjacent visual row |
| First related row | `ArrowUp` | Return to the current-event CTA |
| Related-card inner action | `Escape` | Return focus to the card root |
| Related card | `Home` / `End` | First / last related card |
| Related card | `L` / `K` | Like / calendar for that card |
| Related card | `S` | Copy that card's title and canonical URL |
| Like consent dialog | `Escape` / `Enter` | Activate `Пока нет` / `ОК` through the existing dialog actions |
| Fullscreen gallery final CTA slide | `Enter` / `Space` | Follow the real `Смотреть похожее` event link |
| Footer service-share context | `P` / `S` | Copy the PNG service card / copy service text and canonical link |
| Any ordinary navigator surface | `Space` | Native page scroll; never intercepted |

Letter shortcuts use physical key codes (`KeyL`, `KeyK`, `KeyS`, `KeyP`) so
they work with Russian and Latin layouts. Repeated keydown and modified
shortcuts are ignored. Inputs, textareas, selects, editable content and IME
composition are excluded. The consent dialog and fullscreen gallery receive
priority over page-level shortcuts.

Existing components remain the source of truth for like, consent, calendar and
footer copying. On desktop only, the prototype intentionally replaces event
Web Share with deterministic text copy (`title + newline + URL`) and reports
clipboard completion itself.

## Visual feedback and accessibility

- initial focus is on the existing dark CTA panel, not the title block;
- tiny low-contrast `Enter`/`K`/`S`/`L` badges live inside CTA controls;
- each CTA action retains native `title` hover help naming its shortcut;
- CTA badges stay visible while learning, hide after six uses within 14 days,
  and return after a 14-day lapse;
- a calendar becomes green only after the existing ICS path records it as added,
  and its label becomes `Добавлено`;
- a consent-replayed like remains `aria-pressed="true"`, increments its local
  count and becomes red in the current CTA panel;
- successful event and footer copies show a small auto-dismissing toast while
  preserving focus; no persistent fixed help overlay exists;
- footer actions show inline `P` (service PNG card) and `S` (service text plus
  canonical link) keycaps;
- the in-flow `Попробуй быструю навигацию` section remains immediately before
  the footer;
- the route is always `noindex,nofollow,noarchive`;
- widths below `1024px` receive no prototype UI or keyboard behavior.

Footer context normally follows focus. To recover the user-observed scroll case,
when the footer is visible and the previously focused event control is now
offscreen, `P`/`S` target the visible footer without requiring a hidden Tab step.
A visibly focused event/card still owns its event shortcuts.

## Local acceptance

```bash
cd site
npm run build
python3 -m http.server 4321 --directory dist

# in another shell
STATIC_SITE_REVIEW_BASE_URL=http://127.0.0.1:4321 \
  npm run check:keyboard-event-navigation
```

The focused Playwright check proves CTA initial focus, closed-hero arrows,
ArrowUp fullscreen opening, Enter/Space activation of the final related-event
link, lost-focus re-entry, spatial card movement, consent `Escape`/`Enter`, red
persisted like state, green persisted calendar state, exact event-copy payload
with zero Web Share calls and a timed toast, Russian-layout physical
`KeyP`/`KeyS` footer clipboard results, adaptive badge hide/return behavior,
native ordinary `Space` scrolling, untouched focus outside the navigator,
noindex metadata and zero horizontal overflow at `1536×864`.

## External product review

All V4 requirements were reviewed together through agy using approved
`gemini-3.1-pro-preview` (`Gemini 3.1 Pro (High)`). Accepted conditions include
physical key codes, modal/gallery priority, a real anchor for the final
recommendation, clipboard-success-only feedback, `aria-pressed` for likes and
non-ambiguous calendar wording. The stale-offscreen-focus footer recovery above
is the narrow product-led exception to Gemini's stricter focus-only preference.

## Deliberate non-goals

- no keyboard changes on listing pages;
- no production event-route integration;
- no mobile behavior;
- no replacement of native `Tab` navigation.

Production acceptance should be based on time-to-next-interesting-event,
shortcut discovery, clipboard success and accidental-trigger rate, not only
technical key tests.
