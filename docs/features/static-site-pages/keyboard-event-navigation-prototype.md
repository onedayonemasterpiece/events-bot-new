# Desktop keyboard event navigation prototype

> **Status:** lab prototype, not a production-wide keyboard override  
> **Route:** `/lab/keyboard-event-navigation/`  
> **Fixture:** event `6408`, «Спектакль „Собака на сене“»  
> **Scope:** one desktop event-detail page (`min-width: 1024px`)

## Published review

Current public noindex prototype:

<https://kenigevents.ru/preview-20260719-keyboard-event-navigation-v3/lab/keyboard-event-navigation/>

V3 supersedes the visually obstructed v1 review and the initial clean v2
interaction pass. Its preview prefix contains
exactly one new HTML object and reuses the immutable v1 page assets; it does not
publish listing/event catalogs, modify production root or touch stable
`/ics/*`. The floating service dock and title-panel hint are removed.

## Product hypothesis

The prototype tests one narrow question: can a visitor reject the current event
and start surfing alternatives with one keystroke, without turning the whole
site into a custom keyboard application?

The current-event CTA panel and each related-event card form a scoped
composite navigator. Arrow keys are intercepted only while focus is inside this
navigator. Header, footer, form controls and other page regions keep their
native browser behavior. `Tab` remains available for links and card actions;
`Space` remains native page scrolling.

The lab route intentionally focuses the current-event surface on load so the
one-page experiment can be tested immediately. A production rollout must not
auto-focus on arbitrary direct visits; it should activate the surface only
after an explicit keyboard entry or restored listing-to-detail journey.

## Key contract

| Focus context | Key | Result |
| --- | --- | --- |
| Current-event CTA | `ArrowLeft` / `ArrowRight` | Previous / next hero image |
| Current-event CTA | `ArrowDown` | Focus the first related event |
| No focused control (`body`) | `ArrowDown` | Re-enter navigation at the first related event |
| Current-event surface | `Enter` | Run the visible primary CTA |
| Current-event surface | `L` / `K` / `S` | Like / calendar / share for the current event |
| Related card or its inner action | `ArrowLeft` / `ArrowRight` | Previous / next card in visual DOM order, including row wrap |
| Related card or its inner action | `ArrowUp` / `ArrowDown` | Nearest card in the adjacent visual row |
| First related row | `ArrowUp` | Return to the current-event surface |
| Related card inner action | `Escape` | Return focus to the card root |
| Related card | `Home` / `End` | First / last related card |
| Related card | `L` / `K` / `S` | Like / calendar / share for that card |
| Like consent dialog | `Escape` / `Enter` | Activate `Пока нет` / `ОК` through the existing dialog actions |
| Footer service-share action | `P` / `S` | Copy the service card / copy its text and link |
| Any navigator surface | `Space` | Native page scroll; never intercepted |

Letter shortcuts use physical key codes (`KeyL`, `KeyK`, `KeyS`) so the
prototype remains usable with Russian and Latin layouts. Repeated keydown is
ignored for action shortcuts. Existing gallery dialogs retain ownership of
their arrows and `Escape` while open. Inputs, textareas, selects, editable
content and IME composition are excluded.

Existing action components remain the source of truth. The prototype dispatches
their normal click behavior and announces that the command was handed off; it
does not duplicate like consent, calendar generation, clipboard or share
success logic.

## Visual feedback and accessibility

- the initial focus moves to the existing dark CTA panel instead of outlining
  the event title block;
- tiny low-contrast `Enter`/`K`/`S`/`L` badges live inside the CTA controls and
  remain visible in full-label and compact-icon layouts;
- every CTA action keeps native `title` hover help naming its shortcut, even
  when its visual badge is hidden;
- CTA badges are adaptive: they stay visible while learning, hide after six
  shortcut uses within 14 days, and return after a 14-day lapse; a post-lapse
  action starts a fresh learning count;
- the footer's existing service-copy actions show inline `P` (card) and `S`
  (text plus link) keycaps next to their labels;
- no fixed or overlapping prototype overlay is rendered;
- an in-flow `Попробуй быструю навигацию` section immediately before the footer
  explains the keys and provides one `↓` start action;
- a visually hidden instruction block is connected to the current-event group;
- action availability/results are reported through the existing action UI plus
  a polite prototype status region;
- the route is always `noindex,nofollow,noarchive`, including production builds;
- widths below `1024px` receive no prototype UI or keyboard behavior.

## Local acceptance

Build and serve the static site, then run the focused Playwright check:

```bash
cd site
npm run build
python3 -m http.server 4321 --directory dist

# in another shell
STATIC_SITE_REVIEW_BASE_URL=http://127.0.0.1:4321 \
  npm run check:keyboard-event-navigation
```

The check proves CTA-panel initial focus, closed-hero left/right navigation,
absence of floating service UI, CTA shortcut badges and native hover titles,
the in-flow quick-start section, one-keystroke entry and lost-focus re-entry
into related events, spatial movement, first-row return, consent-dialog
`Escape`/`Enter`, contextual action dispatch, footer `P`/`S` dispatch, adaptive
badge hide-and-return behavior, native `Space` scrolling, untouched focus
outside the navigator, noindex metadata and zero horizontal overflow at
`1536×864`.

## Deliberate non-goals

- no keyboard changes on listing pages;
- no production event-route integration;
- no global interception of arrows or letter keys;
- no mobile behavior;
- no new persistence, personalization or action-result implementation;
- no replacement of native `Tab` navigation.

Production acceptance should be based on observed time-to-next-interesting-event,
shortcut discovery and accidental-trigger rate, not only technical key tests.
