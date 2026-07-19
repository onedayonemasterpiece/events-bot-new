# Desktop keyboard event navigation prototype

> **Status:** V5 lab prototype, not a production-wide keyboard override
> **Scope:** two desktop event-detail fixtures at `min-width: 1024px`
> **Fixtures:** `6408` Split / multi-image and `6593` Editorial / one-image

## Published review

Current public noindex prototypes:

- <https://kenigevents.ru/preview-20260719-keyboard-event-navigation-v5/sobytiya/spektakl-sobaka-na-sene-kaliningrad-6408/>
- <https://kenigevents.ru/preview-20260719-keyboard-event-navigation-v5/sobytiya/spektakl-elementarno-hadson-delo-o-sobake-b-kaliningrad-6593/>

The immutable prefix contains exactly these two new HTML objects and reuses V1
assets. It does not publish a catalog, change production event pages, touch
stable `/ics/*`, or enable behavior below `1024px`.

## Product model

The CTA panel and the related-event grid form one scoped desktop navigator.
`Tab`, ordinary links, native scrolling, inputs and modal controls stay intact.
Letter shortcuts use physical `KeyboardEvent.code`, so Latin/Russian layout
changes do not break `L/K/S/C/P` positions.

The lab deliberately focuses the CTA surface on load. Production must instead
enter the mode after explicit keyboard intent or a restored listing-to-detail
journey.

## Key contract

| Context | Key | Result |
| --- | --- | --- |
| Current-event CTA | one fresh `ArrowDown` | Native browser scroll; no cancellation |
| Current-event CTA | second released `ArrowDown` within `430 ms` | Focus first related card |
| Related section already at the viewport boundary | one `ArrowDown` | Focus first related card |
| No focused control (`body`) | `ArrowDown` | Re-enter at first related card |
| Current-event CTA | `ArrowLeft` / `ArrowRight` | Previous / next hero image; safe no-op for one-image Editorial |
| Current-event CTA | fresh `ArrowUp` | Open existing fullscreen gallery |
| First related row | `ArrowUp` | Focus CTA and scroll to the true event top; held repeat cannot open gallery |
| Current-event CTA | `Enter` | Visible primary CTA |
| Current-event CTA | `L` / `K` / `S` | Like / calendar / copy title plus canonical URL |
| Current-event CTA or description copy group | `C` | Copy title, rendered lead/body and canonical URL |
| Related card | arrows | Spatial card movement, including row changes |
| Related card | `Enter` / `L` / `K` / `S` | Open / like / calendar / copy selected event |
| Related-card inner action | `Escape` | Return focus to card root |
| Gallery final recommendation | `Enter` / `Space` | Follow the real related-event link |
| Consent dialog | `Escape` / `Enter` | Existing decline / accept actions |
| Footer context | physical `P` / `S` | Copy service PNG / service text and link |
| Ordinary document context | `Space` | Native page scroll |

The Down burst accepts only two distinct key gestures separated by keyup.
`event.repeat`, IME composition, modifiers, editing controls and open dialogs do
not qualify. One normal Down never calls `preventDefault()`. The visible
`Перейти к похожим` button remains the timing-independent alternative.

## Copy controls and visual feedback

A small secondary action group is appended in normal flow immediately after the
rendered desktop description:

- `Скопировать описание C` copies title, visible lead, complete visible prose,
  then the canonical event URL;
- `Скопировать афишу` is button-only, keeps one canonical initial hero even if
  the closed carousel later moves, prefetches through CORS, converts to PNG and
  writes exactly one `ClipboardItem`;
- unsupported or failed image clipboard fails closed and says so; it never
  silently copies a URL or opens Web Share.

The CTA keeps subtle `Enter/K/S/L` keycaps and hover/focus titles. Related
calendar controls receive one non-focusable `K` keycap only when their card
container is at least `310px`; the badge survives the green `Добавлено` state.
Likes remain red after consent replay. Successful copy operations use the
existing short visual toast plus one polite hidden status without replacing
icons/counts or moving focus.

The footer learning block is split into three concise situations: `Наверху`,
`На похожем`, `У подвала`. It is in normal flow, not an overlay.

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

The shared component attaches only to semantic desktop landmarks under
`[data-desktop-clean-event]`:

| Fixture | Family | CTA | Hero/gallery | Shared downstream contract |
| --- | --- | --- | --- | --- |
| 6408 | Split | inline in long flow | 7 images + CTA | description, facts, 10 related cards |
| 6593 | Editorial | side panel | 1 image + CTA | same description, facts, 10 related cards |

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
STATIC_SITE_REVIEW_BASE_URL=http://127.0.0.1:4321/preview-20260719-keyboard-event-navigation-v5 \
KEYBOARD_NAVIGATION_ROUTE=sobytiya/spektakl-sobaka-na-sene-kaliningrad-6408/ \
  npm run check:keyboard-event-navigation

STATIC_SITE_REVIEW_BASE_URL=http://127.0.0.1:4321/preview-20260719-keyboard-event-navigation-v5 \
KEYBOARD_NAVIGATION_ROUTE=sobytiya/spektakl-elementarno-hadson-delo-o-sobake-b-kaliningrad-6593/ \
  npm run check:keyboard-event-navigation
```

The parameterized Playwright gate covers single/double/boundary Down, released
ArrowUp return, held-repeat suppression, single/multi-image hero behavior,
gallery recommendation Enter/Space, exact description/event/service payloads,
real PNG poster preparation, consent, red like, green calendar, card K badges,
layout-independent footer keys, local daily dedupe, consent-gated compact event,
per-action mastery/lapse, no Web Share, noindex and horizontal overflow.

## External review and project-skill gate

V5 received final agy acceptance from approved `gemini-3.1-pro-preview`
(`Gemini 3.1 Pro (High)`) on 2026-07-19. R1–R8 were all accepted and the verdict
was `SHIP` for exactly the two immutable noindex prototypes, with no P0 blocker.
This is not production-rollout approval: the remote collector/RPC, live human
acceptance and additional compact-K / held-Down hardening stay separate gates.

Do **not** create a project keyboard-navigation skill yet. The user proposed it
after successful live testing; Gemini agreed to wait. After both public fixtures
receive user acceptance, extract the stable rules: scoped ownership, physical
key codes, visible keycaps/tooltips, layout-safe hints, teaching blocks, compact
daily facts, modal priority and dual-template regression.

## Deliberate non-goals

- no listing-page keyboard changes;
- no production event-route integration or remote telemetry collector;
- no mobile behavior;
- no removal/replacement of native `Tab` navigation.
