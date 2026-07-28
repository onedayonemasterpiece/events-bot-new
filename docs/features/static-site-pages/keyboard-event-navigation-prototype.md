# Desktop keyboard event navigation V7

> **Status:** reviewed V7 is integrated into the primary event template for named immutable noindex previews; production-root rollout remains disabled
> **Scope:** every secret-candidate event detail at `min-width: 1024px`, plus two frozen regression fixtures
> **Fixtures:** `6408` Split / multi-image and `6593` Editorial / one-image

## Published review

Current public noindex prototypes:

- <https://kenigevents.ru/preview-20260719-keyboard-event-navigation-v7/sobytiya/spektakl-sobaka-na-sene-kaliningrad-6408/>
- <https://kenigevents.ru/preview-20260719-keyboard-event-navigation-v7/sobytiya/spektakl-elementarno-hadson-delo-o-sobake-b-kaliningrad-6593/>

The immutable prefix contains exactly these two new HTML objects and reuses V1
assets. It does not publish a catalog, change production event pages, touch
stable `/ics/*`, or enable behavior below `1024px`.

## Product model

The CTA panel, the ten related cards and the hydrated six-card `Ещё события`
continuation form one scoped desktop navigator.
`Tab`, ordinary links, native scrolling, inputs and modal controls stay intact.
Letter shortcuts use physical `KeyboardEvent.code`, so Latin/Russian layout
changes do not break `L/K/S/C/P` positions.

Recommendation optimization may change CSS `grid-row`/`grid-column` without
changing DOM order. Arrow navigation therefore derives a visual matrix from
the rendered card rectangles: rows are ordered top-to-bottom, cards inside a
row left-to-right, horizontal arrows wrap at the visible row boundary, and
vertical arrows choose the nearest card center in the adjacent row. This also
defines deterministic transitions through a short final row and between
`Смотрите дальше` and `Ещё события`; DOM adjacency is never the spatial order.

The historical lab deliberately focuses the CTA surface on load. The shared
secret-candidate production mount does not: it enters the mode only after
meaningful keyboard intent. The same command router drives both surfaces; the
production wrapper does not reimplement a second “similar” navigator.

## Key contract

| Context | Key | Result |
| --- | --- | --- |
| Current-event CTA | one fresh `ArrowDown` | One ordinary controlled scroll step; focus stays on CTA |
| Current-event CTA | second released `ArrowDown` within `430 ms` | Focus first related card |
| Related section already at the viewport boundary | one `ArrowDown` | Focus first related card |
| No focused control (`body`) | `ArrowDown` | Re-enter at first related card |
| Fresh/reloaded multi-image event with `body` focus | first `ArrowLeft` / `ArrowRight` | Enter the current-event surface without load-time autofocus and move the hero |
| Current-event CTA | `ArrowLeft` / `ArrowRight` | Previous / next hero image; safe no-op for one-image Editorial |
| Current-event CTA | fresh `ArrowUp` | Open existing fullscreen gallery |
| Keyboard-opened gallery | `Escape` | Close and restore the logical CTA owner; arrows work again immediately |
| Open gallery | fresh `ArrowDown` | Close, restore the logical owner and do not scroll the covered page |
| First related row | `ArrowUp` | Focus CTA and scroll to page top; held repeat cannot open gallery |
| Current-event CTA | `Enter` | Visible primary CTA |
| Current-event CTA | `L` / `K` / `S` | Like / calendar / copy title plus canonical URL |
| Lost DOM focus (`body`) after a managed surface/card or inert current-event click | `L` / `K` / `S` / `Enter` | Re-enter the recorded logical owner and execute like / calendar / event copy / primary CTA once |
| Current-event CTA or description copy group | `C` | Copy title, rendered lead/body and canonical URL |
| Current-event CTA or description copy group | physical `P` | Copy the canonical event poster as PNG |
| Fresh/provenance-armed event page with natural `body` focus | physical `C` / `P` | Re-enter the current-event surface without scroll and copy description / poster; works in Latin and Russian layouts |
| Related or `Ещё события` card | Left/Right | Previous/next card in visual row order, wrapping to the adjacent visible row and then between card zones |
| Related or `Ещё события` card | Up/Down | Nearest horizontal center in the visible row above/below; deterministic bridge between both card zones |
| Any managed card | `Enter` / `L` / `K` / `S` | Open / like / calendar / copy selected event |
| Managed-card inner action | `Escape` | Return focus to card root |
| Gallery final recommendation | `Enter` / `Space` | Follow the real related-event link |
| Consent dialog | `Escape` / `Enter` | Focus enters the lazy dialog; decline / accept, then restore its logical owner |
| Focus inside service-share controls | physical `P` / `S` | Copy service PNG / service text and link |
| Card or unrelated focus | `P` | No action; event/service image ownership stays unambiguous |
| Ordinary document context | `Space` | Native page scroll |

The Down burst accepts only two distinct key gestures separated by keyup.
Every physical arrow code has a pressed latch: the first keydown may make at
most one semantic step and repeat keydowns are consumed until keyup. Focus
movement never clears this latch, so a held Up cannot chain cards → page top →
gallery. IME composition, modifiers, editing controls and open dialogs do not
qualify. One normal surface Down performs exactly one controlled 40–72 px
scroll step; this avoids Chromium dropping the first default scroll while a
fullscreen overlay finishes closing. The visible `Перейти к похожим` button
remains the timing-independent alternative.

Lost-focus recovery is deliberately narrower than ordinary global hotkeys. It
accepts physical `KeyL`, `KeyK`, `KeyS` and `Enter` from `body`/the document
element, after modal, IME, modifier and editor exclusions, and only while
provenance is armed by the last meaningful focus/pointer owner being the CTA
surface, a managed card or inert content inside the current-event root. A
pointer/focus in the header, footer, editor or unrelated control disarms it.
The logical owner is resolved and focused without scrolling before the action
runs. `C` and `P` accept only fresh or provenance-armed event BODY ownership,
after footer arbitration, and first re-enter the event surface without scroll;
they never use localized `event.key` aliases. A fresh page has a separate
one-use event entry: it never autofocuses, but the first physical Left/Right
can enter a multi-image hero and physical C/P can invoke the current event;
any unrelated pointer/focus, window blur or hidden document revokes it.

The continuation is a separate semantic zone outside
`[data-desktop-clean-event]`. Its section is observed idempotently because the
personalization renderer may replace the slot element itself. Down from
the last related row focuses the nearest first continuation card after
hydration; Up bridges back. A rerender restores the same event ID or the nearest
surviving zone/index. Runtime continuation URLs are normalized to canonical
`/sobytiya/` links rather than leaking the V1 asset prefix.

## Copy controls and visual feedback

A small secondary action group is appended in normal flow immediately after the
rendered desktop description:

- `Скопировать описание C` copies title, visible lead, complete visible prose,
  then the canonical event URL;
- `Скопировать афишу P` keeps one canonical initial hero even if
  the closed carousel later moves, prefetches through CORS, converts to PNG and
  writes exactly one `ClipboardItem`;
- unsupported or failed image clipboard fails closed and says so; it never
  silently copies a URL or opens Web Share.

The CTA keeps subtle `Enter/K/S/L` keycaps and hover/focus titles. Related and
continuation calendar controls receive non-focusable `K` keycap markup when
their card container is at least `310px`, but all card keycaps are visually
hidden by default. Exactly the card that owns focus (`:focus-within`) reveals
its `K`; hover alone reveals nothing. The hidden badge keeps its layout width,
so changing focus does not move neighbouring actions, and it survives the
green `Добавлено` state. `aria-keyshortcuts="K"` remains on every eligible
calendar control even while the decorative badge is hidden. Three mastered
days still suppress the badge entirely.

Continuation cards may be inserted asynchronously by the personal-feed
renderer. A keyboard transition enhances its destination synchronously before
moving focus instead of waiting for a later `MutationObserver` turn. Therefore
the focused continuation card immediately owns the single visible `K` hint;
the previous row never remains the visual shortcut owner.

Likes remain red after consent replay. Successful copy operations use the
existing short visual toast plus one polite hidden status without replacing
icons/counts or moving focus.

The footer learning block is split into three accurate situations: `Событие`,
`Выбранная карточка`, `Поделиться сервисом`. Every letter is decoded in text;
the service commands are described by their action, not by footer location.

## Privacy-minimal shortcut facts

The prototype replaces the raw use counter with a bounded local fact ledger:

```json
{
  "v": 2,
  "days": { "2026-07-19": ["calendar_add", "copy_description"] },
  "reported_days": { "2026-07-19": ["calendar_add"] }
}
```

Contract:

- key: `ke_keyboard_shortcut_daily_v2`;
- day: `Europe/Kaliningrad` calendar day;
- value: allowlisted completed key-action facts only, set semantics, no counts;
- retention: 35 days;
- no event/card ID, title, URL, copied content, route, timestamp, interval,
  scroll position, UA or error details;
- per-action mastery: three distinct usage days in the trailing 14 days hides
  that action's visual CTA/card badge; a lapse restores it;
- before consent, facts are local only and are never retroactively uploaded;
- after a compatible personalization consent, the first completed use of an
  action/day emits one `kenigevents:shortcut-daily-fact` event containing only
  `{schema_version, action_code}`. A future trusted same-origin collector must
  derive subject/day server-side and upsert a boolean `(subject, day, action)`
  fact. The preview intentionally has no remote collector, so prototype traffic
  cannot pollute production analytics.

Production storage must be a daily-deduped fact table, not a raw event log or a
browser-direct table write. Use a dataset-scoped server HMAC subject, a closed
smallint action dictionary, `INSERT ... ON CONFLICT DO NOTHING`, 30-day normal
retention / hard delete by day 35, no public SELECT, and an independently
reviewed same-origin/RPC abuse gate. Pseudonymous behavior is not described as
anonymous data.

## Two-template compatibility

The shared component attaches to semantic desktop landmarks under
`[data-desktop-clean-event]` plus the event-detail personal-feed slot:

| Fixture | Family | CTA | Hero/gallery | Shared downstream contract |
| --- | --- | --- | --- | --- |
| 6408 | Split | inline in long flow | 7 images + CTA | description, facts, 10 related + 6 continuation cards |
| 6593 | Editorial | side panel | 1 image + CTA | same description, facts, 10 related + 6 continuation cards |

This avoids depending on hero geometry. One-image left/right is a stable no-op;
all other keyboard, clipboard, fact and adaptive-hint behavior is identical.

## Build and acceptance

Build only the two allowlisted objects:

```bash
python3 site/scripts/build-keyboard-event-navigation-prototypes.py
python3 -m http.server 4321 --directory site/dist
```

Run the same full regression on each route:

```bash
cd site
STATIC_SITE_REVIEW_BASE_URL=http://127.0.0.1:4321/preview-20260719-keyboard-event-navigation-v7 \
KEYBOARD_NAVIGATION_ROUTE=sobytiya/spektakl-sobaka-na-sene-kaliningrad-6408/ \
  npm run check:keyboard-event-navigation

STATIC_SITE_REVIEW_BASE_URL=http://127.0.0.1:4321/preview-20260719-keyboard-event-navigation-v7 \
KEYBOARD_NAVIGATION_ROUTE=sobytiya/spektakl-elementarno-hadson-delo-o-sobake-b-kaliningrad-6593/ \
  npm run check:keyboard-event-navigation
```

The parameterized Playwright gate covers single/slow-double/quick-double/
boundary Down, per-arrow held-repeat suppression, page-top return without
gallery escalation, gallery Down close without background scroll, gallery
Escape focus restoration and immediate reopening,
single/multi-image hero behavior, gallery recommendation Enter/Space, scoped
event/service/body P, exact description/event/service payloads, real PNG poster
preparation, real focus entry into the lazy consent dialog plus decline/accept
owner restoration, repeatable lost-focus `L` recovery from `body`, red like,
green calendar,
related→continuation hydration and reverse bridge, unified continuation
Enter/L/K/S/Escape/Home/End, slot replacement and feedback-rerank owner
restoration, CSS-reordered visual Left/Right/Up/Down including a ragged final
row, zero K badges before focus/hover and exactly one on a focused card whose
calendar action is available (zero on a calendar-ineligible range card),
focused-card KeyK ownership, canonical links, daily dedupe,
adaptive mastery/lapse, no Web Share, noindex and horizontal overflow.

## External review and project skill

V7 received a new stable-diff acceptance through agy from approved
`gemini-3.1-pro-preview` (`Gemini 3.1 Pro (High)`) on 2026-07-19. The primary
run (`14:11:15Z–14:12:24Z`) and same-model clarification
(`14:14:22Z–14:14:51Z`) both exited 0 with empty stderr. The reviewed diff
`5bc932aaaf3eb4185d1d48c449a0e1076701ad2c9a392cb452bddafa460e1a29`
passed R1–R4 and received **SHIP** for exactly two immutable noindex V7
objects. Production remains **NOT READY** pending the lifecycle, cross-browser,
accessibility and telemetry gates below. The publication contract requires the
exact HTML robots meta; an `X-Robots-Tag` header is not required.

The reusable project skill now lives at
`.codex/skills/keyboard-interface-navigation/`. It is intentionally broader
than this prototype and covers command ownership, physical key codes, modal
priority, dynamic graph focus, lost-focus re-entry, repeat latches, visible and
accessible hints, privacy-minimal daily facts and the required acceptance
matrix. Its `agents/openai.yaml` makes it discoverable in future Codex work.

## Secret-candidate production integration

The reviewed source commit is `d0027a53`; branch head `db5310e8` retains the
review evidence. Production uses the shared router in
`site/src/lib/keyboardEventNavigation.mjs`. Both
`KeyboardEventNavigationPrototype.astro` and the event-route wrapper
`KeyboardEventNavigation.astro` mount that exact implementation. Generated lab
HTML is not copied into the SSG, and the prototype-only Astro lab route is not
part of either the production or secret-candidate artifact tree.

The primary `/sobytiya/<slug>/` template mounts the wrapper only when all of
these are true:

- the build is an immutable secret candidate;
- the route is an event detail;
- the desktop media query matches;
- `PUBLIC_KEYBOARD_EVENT_NAVIGATION_ENABLED` is not `0`.

The current preproduction default is enabled for every named immutable
`/preview-*` candidate; setting the flag to `0` is the immediate rollback.
This prevents a newly assembled review prefix from silently losing the
accepted interaction merely because a one-off force flag was omitted.
Root-form production builds,
listings and mobile install no keyboard router. There is no remote shortcut
collector: the allowlisted daily facts remain local and contain no event, URL,
title, key history or precise timestamp.

The `6529` generated-page regression is spatial rather than DOM-order based:
after the optimizer repacks cards, `ArrowDown` enters the first visible related
card and `ArrowRight` moves to its visible right-hand neighbour. Calendar `K`
keycaps remain hidden in ambient cards and appear only for the focused
card/action owner.

The module exposes an explicit `init()`/`destroy()` controller, owns global
listeners with `AbortController`, disconnects mutation observers and cancels
timers/animation frames on teardown. Production removes prototype autofocus and
disarms lost-focus `L` provenance after blur/hidden until a new managed
focus/pointer owner exists. Because the canonical feedback controller may await
card/feed state before it creates the consent dialog, the router arms the
logical owner before clicking the existing like control and captures the real
dialog from the shared DOM lifecycle; it does not create parallel consent
state.

Every mutated pre-existing `title`, `aria-keyshortcuts`, focusability and scope
attribute is snapshotted and restored by `destroy()`; teardown therefore does
not erase component-owned labels such as the phone CTA title. Consent ownership
has no fixed timeout: it ends when the real dialog is captured, the like action
completes, or focus/pointer/page lifecycle establishes a different owner.

### Reviewed source and historical handoff

### What to integrate

Use branch `agent/keyboard-event-navigation-prototype` as the immutable reviewed
source when auditing parity. The reusable behavior has been extracted from
`site/src/components/KeyboardEventNavigationPrototype.astro` into the shared
module above; the builder and two generated pages remain lab packaging, not
production architecture. The
parameterized regression is
`site/scripts/check-keyboard-event-navigation-playwright.sh`, and the canonical
product/engineering contract is this document.

Production integration should retain these semantic DOM contracts rather than
hero geometry or copied markup:

- `[data-desktop-clean-event]` — current event root;
- visible `[data-desktop-action-panel]` — current-event keyboard surface;
- `[data-related-start] [data-event-card]` — explicit similar-event zone;
- `[data-personal-feed-section][data-listing-context="event-detail"]` and its
  replaceable `[data-personal-feed-slot]` — continuation zone;
- `[data-hero-gallery-open]` / `[data-hero-gallery]` /
  `[data-hero-gallery-close]` — existing gallery lifecycle;
- existing like, calendar and share data actions — the shortcut router calls
  real controls and does not implement parallel business state.

### Resolver priority and state

Keep one ordered command router with this priority:

1. desktop/media eligibility, composition, modifier and editable exclusions;
2. active gallery or consent/other topmost dialog;
3. explicitly focused service-share control;
4. current-event CTA surface;
5. managed related/continuation card;
6. one-use cold `body` hero Left/Right or provenance-gated `body` re-entry for
   `L/K/S/Enter`;
7. otherwise leave the event untouched and preserve browser behavior.

Persist logical owner as `{kind, eventId, zone, index}` plus a connected node
only as a fast path. Resolve after every rerender by connected node → same event
ID → nearest surviving zone/index → CTA. Overlay sessions need a monotonic token,
logical owner and opener. On every overlay close clear pressed-arrow and
double-Down state before restoring focus. Keep a per-physical-code pressed latch
until matching `keyup`; also clear it on pointer ownership changes, window blur
and hidden `visibilitychange`.
In the production extraction, also disarm lost-focus action provenance on
window blur/hidden visibility and re-arm only from a new managed focus/pointer
owner; this is a lifecycle/assistive-technology hardening gate rather than a
prototype publication blocker.

The dynamic personal-feed **section**, not its current slot node, must be
observed: the renderer can replace the slot itself. Re-enhance new cards
idempotently, normalize their canonical URLs, reconnect action-state observers,
and restore focus in `requestAnimationFrame` after the mutation batch settles.
The desktop similar grid is itself a canonical `[data-discovery-feed]`, so the
same candidate store, served-list attribution and feedback controller used by
the mobile/continuation cards applies there. An in-flight desktop-only manifest
request may be marked consumed only while its media query still matches; a
mobile resize must leave it eligible for the next desktop retry. If that shared
controller reorders still-connected related cards, the router restores focus to
the same logical event after the mutation batch rather than leaving it on BODY.

### Implemented extraction contract

1. The inline router is extracted into a production module with an explicit
   `init(root, options)` / `destroy()` lifecycle. Use an `AbortController` or
   equivalent cleanup for document listeners and disconnect all observers.
2. Executable commands, displayed keycaps, `title`,
   `aria-keyshortcuts`, help copy and analytics IDs from one command registry.
3. Prototype-only autofocus is removed. Activate on the first recognized keyboard
   intent, explicit `Перейти к похожим`, or a restored listing→detail journey.
4. Keep the navigator behind the desktop build flag during secret-candidate
   acceptance and a later gradual root cohort gate;
   do not enable it on listing pages or mobile as a side effect.
5. Reuse the current gallery, feedback, calendar, clipboard and service-share
   implementations. Add lifecycle events to those shared components where a
   stable event is safer than observing classes.
6. The two-fixture regression remains the oracle and production routes add a
   separate live smoke suite. Preserve both the Split/multi-image and
   Editorial/single-image fixtures.

### Analytics boundary

The browser ledger remains an allowlisted boolean set of successful actions per
Kaliningrad day with 35-day maximum local retention. It must not include event
ID, card ID, title, URL, route, copied content, raw key, count, precise time,
focus trail, scroll position, UA or failure payload. Before consent it stays
local. A future same-origin collector should derive day and a dataset-scoped
HMAC subject server-side and insert one boolean `(subject, day, action)` fact via
`ON CONFLICT DO NOTHING`; never expose direct browser table writes or public
reads. Collector/RPC schema, abuse controls and deletion are a separate reviewed
production change.

### Production release gates

- dual-fixture Chromium regression plus manual Firefox/Safari desktop checks;
- Latin and Cyrillic physical-code checks;
- keyboard-only and mouse→keyboard mixed journeys, including intentional blur;
- gallery close through `Down`, `Escape`, close button and route change;
- consent accept/decline, rerank and slot replacement with focus evidence;
- no regression to `Tab`, `Shift+Tab`, `Space`, editors, OS/browser chords,
  screen-reader commands, high contrast, zoom/reflow or reduced motion;
- feature-flag rollback, cleanup on navigation and telemetry privacy review.

The Chromium generated-tree gate is required for every candidate containing
the router. It covers fresh load and reload, real inert pointer click, explicit
Cyrillic `key` values with physical `code`, header/editor and single-image
negative controls, gallery handoff and footer ownership. The same executable
accepts `--browser firefox` and `--browser webkit` on hosts with Playwright
system libraries; Playwright WebKit is only an approximation. Native
Firefox/Safari, screen-reader, high-contrast
and zoom/reflow evidence are still required before any production-root rollout;
secret-candidate integration is not that approval. A collector is optional and
must be separately reviewed rather than blocking the current local-only facts.

## Deliberate non-goals

- no listing-page keyboard changes;
- no production-root event-route rollout or remote telemetry collector;
- no mobile behavior;
- no removal/replacement of native `Tab` navigation.
