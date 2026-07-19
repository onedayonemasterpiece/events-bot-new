# R01 — transport infographic research handoff

Status: **complete**
Branch: `agent/static-event-v11/transport-infographic-research`
Baseline: `d5dab75a`
Scope: read-only product/design research plus ignored artifacts; no `site/`, `docs/`, or `CHANGELOG.md` edits.

## Outcome

Recommend a **hybrid departure board**: Alternative A (dense one-trip-per-row table) as the default, with Alternative B (route strip) available only as expanded row detail. This follows the strongest transport-design precedent and is the only proposed pattern that remains concise with two departures and scales to many departures without duplicating the same route diagram.

The core hierarchy should be:

1. route `119` + direction;
2. explicit boarding place `Северный вокзал`;
3. stable columns `Северный → Романово → KAUP` with tabular times;
4. progressive disclosure for additional departures;
5. one shared walking leg `4 км · ≈53 мин`;
6. one separate no-return warning.

## Pinterest funnel and self-review

Collector completed successfully using the saved authenticated Pinterest session. **No auth or collection limitation.**

- diversified queries: **12**
- candidates collected: **120**
- board-level self-review: **120/120**
- detailed contact-sheet review: **18**
- recorded decisions: **keep 10 / maybe 8 / reject 102**
- shortlist rate: **8.3%**, reflecting a strict relevance bar

Shared library collection:

`/home/dev/projects/pinterest-idea-library/collections/20260717-kenigevents-transport-infographic-scalable-bus-timetable-v11/`

Task artifact copy:

`/home/dev/.codex/worktrees/events-bot-new/static-event-v11-transport-research/artifacts/codex/static-site-v11-transport-phone-carousel/research/pinterest-board/`

Key files:

- full 120-pin board: `pinterest-board/board.png`
- board HTML: `pinterest-board/board.html`
- thumbnails: `pinterest-board/thumbs/`
- source URLs + review decisions: `pinterest-board/pins.json`
- 18-item detailed contact sheet: `pinterest-selected-contact.png`

### Best Pinterest links

1. https://www.pinterest.com/pin/23855073020949082/ — route strip + adjacent timetable
2. https://www.pinterest.com/pin/71353975327308961/ — time-first departure rows
3. https://www.pinterest.com/pin/326440673010022139/ — canonical dense departure board
4. https://www.pinterest.com/pin/40673202879116728/ — route strip + departure grid
5. https://www.pinterest.com/pin/197595502406485035/ — compact many-trip timetable
6. https://www.pinterest.com/pin/6262886976702875/ — expanded mobile trip cards
7. https://www.pinterest.com/pin/33425222229838978/ — single-origin departure table
8. https://www.pinterest.com/pin/352336370866942758/ — digital bus screen hierarchy
9. https://www.pinterest.com/pin/3799980931201905/ — vertical journey timeline
10. https://www.pinterest.com/pin/162622236541805078/ — shuttle stop sequence

## Broader internet references

Primary/authoritative design patterns reviewed:

1. Transport for West Midlands timetable pattern — https://designsystem.tfwm.org.uk/patterns/timetables/
   - closest match: selecting a departure reveals the complete route; warnings remain separate.
2. TfL Line Diagram Standard — https://content.tfl.gov.uk/tfl-line-diagram-standard.pdf
   - route strip is for choosing/confirming route, not for repeating every departure.
3. TfL Bus Stop Graphics Standard — https://content.tfl.gov.uk/buses-bus-stop-graphics-standard-issue02.pdf
   - stable hierarchy of route number, stop and onward direction.
4. Transport for Ireland public-transport information guidelines — https://www.transportforireland.ie/transitData/Design%20guidelines%20for%20the%20Creation%20of%20Public%20Transport%20Information_v1.pdf
   - explicit departure-time layout system.
5. Transit app departure guidance — https://help.transitapp.com/article/445-how-to-track-departures-on-your-transit-line
   - next departures first, “More departures” disclosure, scheduled/live distinction.
6. GTFS Schedule best practices — https://gtfs.org/documentation/schedule/schedule-best-practices/
   - trip/ordered-stop data model and estimated/interpolated times.
7. Île-de-France Mobilités passenger screen prescriptions — https://prim.iledefrance-mobilites.fr/en/chartes-et-prescriptions/prescriptions-afficheurs-digitaux
   - prioritise content and isolate disruptions.
8. MBTA Screen case study — https://owenthe.dev/mbta-screen
   - useful dense information hierarchy, though too live-data-heavy to copy directly.

## Three deterministic alternatives

All prototypes show only the two supplied KAUP calculations, not invented services:

- `16:45 → 17:35 → 18:28`
- `18:10 → 19:00 → 19:53`
- walking leg `4 км / ≈53 мин`
- no return bus after the event

Artifact directory:

`/home/dev/.codex/worktrees/events-bot-new/static-event-v11-transport-research/artifacts/codex/static-site-v11-transport-phone-carousel/research/`

Files:

- source: `transport-alternatives.html`
- all variants desktop: `transport-alternatives-desktop.png`
- A mobile: `alt-a-mobile.png`
- B mobile: `alt-b-mobile.png`
- C mobile: `alt-c-mobile.png`
- full rationale and source notes: `research-summary.md`

### Alternative A — departure board (**recommended base**)

Pros: most compact; one trip per row; fast comparison; semantic table/list; predictable with 2 or 20+ buses.
Cons: the shared walking leg needs a footer/legend; responsive stop labels need a 320px contract.

### Alternative B — repeated route strips (**recommended expanded detail**)

Pros: clearest physical journey and mode change.
Cons: repeats route topology for every departure and becomes vertically expensive with many buses.

### Alternative C — next trip + queue

Pros: strongest immediate answer to “which bus should I take?”
Cons: requires current time, event time, expiry/status logic and accurate schedule semantics; unsuitable as the static default until those contracts exist.

## Self-review notes

- Full Pinterest board reviewed before any external/agy review: **yes**.
- Prototype sheet reviewed at desktop and 390px: **yes**.
- Keep: stable row scan, tabular numerals, shared route/footpath facts, warnings outside the grid.
- Maybe: “next trip” emphasis once time-aware data is reliable.
- Reject: one-card-per-trip stacks, duplicated walking explanations, decorative event schedules, airline split-flap styling without passenger semantics, and route strips repeated for every bus.
- Required implementation QA: 320px stop headers, approximate-time semantics (`≈`/legend), 44px disclosure target, semantic headers, and no colour-only status.

## Tool limitation

`ui-ux-pro-max` could not run because its installed `scripts` entry is a text pointer to missing `../../../src/ui-ux-pro-max/scripts`. No recommendation was attributed to that tool. The lane relied on its own visual review and the linked transport/design-system sources. No OpenAI image generation was used.

## External review

No agy/Gemini/Opus review was run in this lane. Per assignment, root owns the external comparison after this in-agent self-review.
