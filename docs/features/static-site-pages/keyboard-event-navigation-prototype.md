# Desktop keyboard event navigation prototype

> **Status:** V6 lab prototype, not a production-wide keyboard override
> **Scope:** two desktop event-detail fixtures at `min-width: 1024px`
> **Fixtures:** `6408` Split / multi-image and `6593` Editorial / one-image

## Published review

Current public noindex prototypes:

- <https://kenigevents.ru/preview-20260719-keyboard-event-navigation-v6/sobytiya/spektakl-sobaka-na-sene-kaliningrad-6408/>
- <https://kenigevents.ru/preview-20260719-keyboard-event-navigation-v6/sobytiya/spektakl-elementarno-hadson-delo-o-sobake-b-kaliningrad-6593/>

The immutable prefix contains exactly these two new HTML objects and reuses V1
assets. It does not publish a catalog, change production event pages, touch
stable `/ics/*`, or enable behavior below `1024px`.

## Product model

The CTA panel, the ten related cards and the hydrated six-card `Ещё события`
continuation form one scoped desktop navigator.
`Tab`, ordinary links, native scrolling, inputs and modal controls stay intact.
Letter shortcuts use physical `KeyboardEvent.code`, so Latin/Russian layout
changes do not break `L/K/S/C/P` positions.

The lab deliberately focuses the CTA surface on load. Production must instead
enter the mode after explicit keyboard intent or a restored listing-to-detail
journey.

## Key contract

| Context | Key | Result |
| --- | --- | --- |
| Current-event CTA | one fresh `ArrowDown` | One ordinary controlled scroll step; focus stays on CTA |
| Current-event CTA | second released `ArrowDown` within `430 ms` | Focus first related card |
| Related section already at the viewport boundary | one `ArrowDown` | Focus first related card |
| No focused control (`body`) | `ArrowDown` | Re-enter at first related card |
| Current-event CTA | `ArrowLeft` / `ArrowRight` | Previous / next hero image; safe no-op for one-image Editorial |
| Current-event CTA | fresh `ArrowUp` | Open existing fullscreen gallery |
| Keyboard-opened gallery | `Escape` | Close and restore the logical CTA owner; arrows work again immediately |
| First related row | `ArrowUp` | Focus CTA and scroll to page top; held repeat cannot open gallery |
| Current-event CTA | `Enter` | Visible primary CTA |
| Current-event CTA | `L` / `K` / `S` | Like / calendar / copy title plus canonical URL |
| Current-event CTA or description copy group | `C` | Copy title, rendered lead/body and canonical URL |
| Current-event CTA or description copy group | physical `P` | Copy the canonical event poster as PNG |
| Related or `Ещё события` card | arrows | One spatial step per released press; bridge between both card zones |
| Any managed card | `Enter` / `L` / `K` / `S` | Open / like / calendar / copy selected event |
| Managed-card inner action | `Escape` | Return focus to card root |
| Gallery final recommendation | `Enter` / `Space` | Follow the real related-event link |
| Consent dialog | `Escape` / `Enter` | Focus enters the lazy dialog; decline / accept, then restore its logical owner |
| Focus inside service-share controls | physical `P` / `S` | Copy service PNG / service text and link |
| Card, body or unrelated focus | `P` | No action; event/service image ownership stays unambiguous |
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
continuation calendar controls receive one non-focusable `K` keycap only when
their card container is at least `310px`; the badge survives the green
`Добавлено` state.
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
STATIC_SITE_REVIEW_BASE_URL=http://127.0.0.1:4321/preview-20260719-keyboard-event-navigation-v6 \
KEYBOARD_NAVIGATION_ROUTE=sobytiya/spektakl-sobaka-na-sene-kaliningrad-6408/ \
  npm run check:keyboard-event-navigation

STATIC_SITE_REVIEW_BASE_URL=http://127.0.0.1:4321/preview-20260719-keyboard-event-navigation-v6 \
KEYBOARD_NAVIGATION_ROUTE=sobytiya/spektakl-elementarno-hadson-delo-o-sobake-b-kaliningrad-6593/ \
  npm run check:keyboard-event-navigation
```

The parameterized Playwright gate covers single/slow-double/quick-double/
boundary Down, per-arrow held-repeat suppression, page-top return without
gallery escalation, gallery Escape focus restoration and immediate reopening,
single/multi-image hero behavior, gallery recommendation Enter/Space, scoped
event/service/body P, exact description/event/service payloads, real PNG poster
preparation, real focus entry into the lazy consent dialog plus decline/accept
owner restoration, red like, green calendar,
related→continuation hydration and reverse bridge, unified continuation
Enter/L/K/S/Escape/Home/End, slot replacement and feedback-rerank owner
restoration, dynamic K badges, canonical links, daily dedupe,
adaptive mastery/lapse, no Web Share, noindex and horizontal overflow.

## External review and project-skill gate

V6 received product/engineering consultation and a stable-diff final acceptance
through agy from approved `gemini-3.1-pro-preview` (`Gemini 3.1 Pro (High)`) on
2026-07-19. The final run (`13:22:41Z–13:24:10Z`, exit 0, stderr 0) accepted
R1–R6 and returned **SHIP** for exactly these two immutable noindex prototypes,
with no pre-publication P0. Its required contract—logical overlay owner
restoration, per-code pressed arrows, explicit two-zone bridging, strict-focus
P ownership and exact Russian learning copy—is implemented. This is not
production-rollout approval: the remote collector/RPC and live human acceptance
stay separate gates.

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
