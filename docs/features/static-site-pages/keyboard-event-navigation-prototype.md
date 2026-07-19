# Desktop keyboard event navigation prototype

> **Status:** lab prototype, not a production-wide keyboard override  
> **Route:** `/lab/keyboard-event-navigation/`  
> **Fixture:** event `6408`, «Спектакль „Собака на сене“»  
> **Scope:** one desktop event-detail page (`min-width: 1024px`)

## Published review

Public noindex prototype:

<https://kenigevents.ru/preview-20260719-keyboard-event-navigation-v1/lab/keyboard-event-navigation/>

The immutable preview prefix was built from source commit `da4e2cae` and does
not modify production root or stable `/ics/*`. Public HTTP returned `200`; the
focused Playwright contract passed again against the public URL at `1536×864`.

## Product hypothesis

The prototype tests one narrow question: can a visitor reject the current event
and start surfing alternatives with one keystroke, without turning the whole
site into a custom keyboard application?

The current-event title panel and each related-event card form a scoped
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
| Current-event surface | `ArrowDown` | Focus the first related event |
| Current-event surface | `Enter` | Run the visible primary CTA |
| Current-event surface | `L` / `K` / `S` | Like / calendar / share for the current event |
| Related card or its inner action | `ArrowLeft` / `ArrowRight` | Previous / next card in visual DOM order, including row wrap |
| Related card or its inner action | `ArrowUp` / `ArrowDown` | Nearest card in the adjacent visual row |
| First related row | `ArrowUp` | Return to the current-event surface |
| Related card inner action | `Escape` | Return focus to the card root |
| Related card | `Home` / `End` | First / last related card |
| Related card | `L` / `K` / `S` | Like / calendar / share for that card |
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

- the current event and selected card receive a strong visible focus ring;
- a fixed desktop-only cheat sheet explains the nonstandard keys;
- a visually hidden instruction block is connected to the current-event group;
- action availability/results are reported through the existing action UI plus
  a polite prototype status region;
- the route is always `noindex,nofollow,noarchive`, including production builds;
- widths below `1024px` show a desktop-only note instead of pretending the
  keyboard experiment applies to mobile.

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

The check proves initial focus, one-keystroke entry into related events,
horizontal and vertical spatial movement, first-row return, inner-control
collapse, native `Space` scrolling, untouched focus outside the navigator,
noindex metadata and zero horizontal overflow at `1536×864`.

## Deliberate non-goals

- no keyboard changes on listing pages;
- no production event-route integration;
- no global interception of arrows or letter keys;
- no mobile behavior;
- no new persistence, personalization or action-result implementation;
- no replacement of native `Tab` navigation.

Production acceptance should be based on observed time-to-next-interesting-event,
shortcut discovery and accidental-trigger rate, not only technical key tests.
