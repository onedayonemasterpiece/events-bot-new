# Event transport guidance

> Status: integrated presentation candidate, publicly verified in the full-catalog preview. Public scope: rail for Светлогорск/Зеленоградск and a bus example for Сказочное Холмогорье in Романово. The official-source reference for the next multimodal localities is documented in [rail-multimodal-directory.md](rail-multimodal-directory.md).

## Product rule

The event page offers neutral travel help inside the reading flow:

- origin: `Калининград-Северный`;
- destinations: `Светлогорск-2` for Светлогорск and `Зеленоградск-новый` for Зеленоградск;
- placement: after the compact `Коротко` facts and before description metadata/source gate; on mobile the block returns to one vertical flow, while desktop may use a container-query split inside the transport block itself;
- public copy: no `Партнёрский маршрут` or prominent carrier promotion; the train footer contains one terse `Перевозчик — КППК` line;
- visual: the supplied side-view Lastochka artwork, not the official carrier logo; each row still names its actual train type;
- options: at most two trains in each direction.

This is a release feature of the event page, not a standalone campaign. It is included in the [official presentation checklist](presentation-release-checklist.md).

## 2026-07-15 integration evidence

The current accepted full-data candidate is
<https://kenigevents.ru/preview-20260715t-production-mobile-v8-related-transport-v3/__preview/>.
The rail regression is event `5789` in Светлогорск; the bus regression is event
`6710` in Романово. Public Playwright acceptance covered `1920×1080` and
`390×844`, both with zero horizontal overflow. The rail page rendered two
outbound rows, one return row and three transport calendar links; all three
transport files and the event calendar returned `200 text/calendar`. Directory
checks passed for `17` bus localities / `26` venues / `21` stops and `13`
official rail pages / `9` routes / `17` locality policies / `10` service
patterns. The v3 public gate additionally proves that the Lastochka image is
visible in both responsive surfaces and that related-event navigation remains
inside the same generated preview prefix.

Gemini 3.1 Pro (High) reviewed the responsive composition. Applied decisions:
keep transport after the compact facts, use container queries rather than a
page-wide breakpoint, keep one-column mobile reading order, split rail columns
only from `540px`, and split the bus schedule/map from `540px`. The original
candidate delayed the decorative train until `700px`; production acceptance
overrides that detail because the accepted desktop split column and phone
viewport are both narrower. The Lastochka image is now visible at every block
width (`max-height:76px`, then `92px` from `540px`) and remains `contain` rather
than cropped. The raw review remains a local non-committed artifact
under `artifacts/codex/static-site-production-integration-20260715/`.

## 2026-07-17 desktop KAUP and explicit-duration remediation

The desktop event renderer has one exact-venue KAUP block for aliases of
`Поселение викингов Кауп`. It is intentionally not a generic Romanovo matcher
and does not change the accepted mobile transport surface.

- the recommended mode is the venue's official round-trip transfer: `600 ₽`,
  boarding information and vehicle number are emailed after `19:00` on the day
  before, and boarding requires a pre-purchased printed ticket;
- pickup points shown from the venue are Дом Советов in Kaliningrad, Lenina 10
  by the Zelenogradsk bus terminal and Lenina 33 by Svetlogorsk-2;
- the booking CTA uses the current Radario destination linked by KAUP;
- public bus `119` is shown separately. Arrival uses the reviewed 65-minute
  ride plus about `4 km / 53 min` on foot from Romanovo; the UI does not invent
  a short pedestrian entrance;
- the `4 km` last-mile warning is shown before the bus options. The pedestrian
  CTA is explicitly named `Маршрут от остановки до Кауп` and routes from the
  Romanovo stop to the venue (`rtt=pd`); the separate car CTA is named
  `Маршрут на авто из Калининграда`. A generic `Построить маршрут` label is
  forbidden here because it hides both the starting point and travel mode;
- for the evening Epidemia regression there is no confirmed public-bus return,
  so the block recommends the official transfer or a car;
- the visible route diagram and map links point to KAUP and a route from
  Kaliningrad.

Primary source: [official KAUP site](https://www.kaup39.ru/). Bus timetable:
[АО «Автовокзал» route table](https://avl39.ru/routes/reg/kaliningrad/).

Desktop may also repair a missing `time_range_end` from an explicit
source-labelled phrase such as `Продолжительность спектакля – 1 час 40 минут`.
This narrow helper accepts a label plus punctuation/`составляет`, never infers
duration from event type or generic prose, and is not applied to the mobile
renderer. For event `3103` it produces `19:40`. A venue-specific return-access
profile then adds `30` minutes for leaving Янтарь холл, the approximately
15-minute walk to Светлогорск-2 and a boarding reserve. The first eligible
departure is therefore after `20:10`: the desktop card shows trains `6726` and
`6728`, not the unsafe `19:57` train `6724`, and does not suggest waiting until
the next morning.

### Duration-evidence and return-safety hierarchy

The page must not turn category statistics into a precise promise that the
visitor will catch a train. The current hierarchy is:

1. source-explicit end time or a source-labelled exact duration;
2. a trusted structured official duration imported into the canonical event;
3. no exact end: show factual schedule boundaries and ask the visitor to
   confirm the end with the organizer; do not emit a precise return shortlist.

An exact source value always overrides any future category/venue prior. This
is important for long outliers such as six-hour operas. The current 303-event
preview is not statistically adequate for a defensible duration model: only
`17/303` events have either an explicit end or a narrowly extractable labelled
duration, and only one of them is a spectacle. These figures are release-audit
evidence, not a permanent product constant. A future probabilistic model may
rank uncertainty, but it may not display an exact catchability claim without
source-grounded end evidence.

Because the accepted mobile renderer deliberately keeps the exported event
unchanged, static rail calendar generation takes the deduplicated union of the
mobile shortlist and this desktop explicit-duration shortlist. Thus every
visible desktop or mobile train row has a matching `.ics` file without forcing
the desktop-only duration repair into the mobile page.

The 2026-07-15 v3 corpus gate rendered all `282` future/ongoing event pages,
found `21` rail-enabled pages and verified the illustration has a non-zero
painted rectangle on every one. A real `390×844` route additionally decoded the
`500px` source and measured it at `324×65.4px`; the same component remains
visible in the accepted desktop shell. This is a generated-page contract, not a
laboratory-only rule.

## Matching contract

`site/src/lib/eventTransport.ts` performs deterministic build-time matching against `site/src/data/transportSchedules.json`:

1. Normalize the event city and require exact `Светлогорск` or `Зеленоградск`.
2. Require a single-day event with a reliable start time; a multi-day event has no unambiguous travel date.
3. Keep only trains operating on `event.start_date` whose destination arrival is **20–90 minutes before** the event start.
4. Rank the closest arrival to a 30-minute buffer and return no more than two options.
5. Calculate the trip back from an explicit `time_range_end`. The exporter may derive that field only from a source-labeled `Продолжительность: …` value.
6. If the source has no end, do not invent one from the event type. Show factual schedule boundaries instead: the last same-day return, whether a night service exists, and the first next-day return. Ask the visitor to confirm the organizer’s end time.
7. Use the event type only to inflect the outbound heading (`К началу концерта / спектакля / мастер-класса …`), never to calculate time.
8. With an explicit end, add the reviewed venue-access buffer before filtering departures. Янтарь холл currently uses `30` minutes; the generic supported-venue fallback is `25` minutes. Then keep the first two trains whose departure is both after that ready time and within the configured maximum wait from the event end. A categorical no-return message is allowed only when the service calendar covers the relevant dates.
9. Hide the whole block when neither a usable outbound option nor a matched return option exists. For an unknown end there is no matched return by definition, so a supported page needs at least one outbound train; schedule cutoffs enrich that block but do not make an otherwise empty block appear.
10. Show the rail route once in the header. Each compact train row is itself the calendar link; it keeps the time pair, one metadata line, a calendar icon, keyboard focus and a 44px-or-larger interactive target without repeating the route or a separate `В календарь` button.
11. Every rendered train, including a factual last/night return, links to its own static `.ics` containing departure, arrival, route and a `VALARM` 30 minutes before departure.

This is the current renderer contract, not the full routing policy. Пионерский and seasonal Балтийск are rail-primary in the prepared directory; sparse inland destinations compare train and bus and may use a different mode each way. They stay out of the public block until exact-date service calendars and venue access have passed the same checks.

The prepared directory also contains one intentional Kaliningrad exception: events at ДС «Янтарный» may use `о.п. Елизаветинская`, about 8–10 minutes on foot from the arena. The compact row offers both `Южный` (15–18 min) and `Северный` (7–8 min) boarding times for the same train, states the current `35 ₽` city-distance fare once and creates an origin-specific calendar link. Matching is by reviewed venue alias/address, never by city alone, and remains gated until an exact-date timetable export and real-event regression exist.

The selector and calendar files are static. Public UI and `.ics` descriptions do not contain schedule-verification links.

## ICS budget and filenames

Static generation follows the visible actions, not the size of the timetable:

- generate an ICS only when the event page renders the corresponding calendar link;
- standard event: at most two outbound + two return/cutoff actions, therefore **4 transport ICS files**;
- future dual-origin ДС «Янтарный»: at most four outbound origin choices + two returns, therefore **6 files**;
- unknown-end pages generate the last same-day return and either the first night train or the first next-day train, never unused alternatives as hidden files;
- deduplicate by destination, optional boarding origin, service date and train number;
- preview acceptance compares generated files with actual `href` values, rejects orphan files and enforces the per-event ceiling.

On the 2026-07-12 regression catalog the full build contains `400` ordinary event calendars and `166` transport calendars for `44` eligible events: `566` ICS files total, no orphan transport files and no event above four transport files. The rendered-action rule removed three unused transport files from the previous `169`-file output. There are `127` unique date/destination/train keys; the other `39` files intentionally retain different event context in `SUMMARY`, `DESCRIPTION`, `URL` and `UID`. Replacing them with global trip files would currently save little while making the calendar entry less clear. A mandatory global-sharing review is triggered if transport ICS count reaches `1000`. This is a validation snapshot, not a fixed production count.

The first filename segment identifies the calendar artifact. Rail paths use short semantic ASCII names:

- `rzd-svetlogorsk-20260712-6717.ics`;
- `rzd-kaliningrad-20260712-6722.ics`;
- future dual-origin example: `rzd-elizavetinskaya-south-20260727-6725.ics`.

The saved filename adds the event id, for example `rzd-svetlogorsk-20260712-6717-e6510.ics`. Bus calendar artifacts follow `bus-<route>-<destination>-<date>-<departure>-e<event-id>.ics`, for example `bus-118-romanovo-20260725-0740-e6710.ics`. The event itself downloads as `event-<short-topic>-<date>-e<event-id>.ics`, for example `event-kontsert-posvyaschenie-muslimu-magomaevu-20260712-e6510.ics`. This bounded topic is derived from the already transliterated event page slug; it is readable without allowing an unbounded full title into the filename. The preview uploader applies the same names to Object Storage `Content-Disposition`, including the stable `/ics/<event_id>.ics` alias, rather than falling back to `event.ics`. Changing any readable filename does **not** change the existing VEVENT `UID`, preventing a renamed file from becoming a duplicate calendar entry.

## Bus example: Сказочное Холмогорье

The build-time coverage/topology and venue last-mile reference is maintained separately in [bus-transport-directory.md](bus-transport-directory.md). It currently inventories 30 active events across 14 logical localities and 21 venues; this does **not** automatically enable the public block. A locality needs reviewed target-stop times, a service calendar and a safe venue access leg before it can move into `busTransportSchedules.json`.

`site/src/data/busTransportSchedules.json` and `site/src/lib/eventBusTransport.ts` activate only for a source-backed `11:00–16:00` event at `Холмогорье / Сказочное Холмогорье`, Романово:

- the official route registry shows that `118`, `118А` and `119` share the same corridor from the Kaliningrad bus terminal through `Северный вокзал` and up to `Романовский поворот`; the public UI therefore uses one shared `около 1 часа в автобусе` estimate and one shared `Северный — примерно через 10–15 минут` note instead of different per-route or per-chip estimates;
- OSM/Valhalla checks give `3.47km / 8.7m` free-flow to Северный, `30.22km / 46.5m` to the turn and `32.66km / 49.3m` to central Romanovo. These are map-model driving times, not a bus timetable; the public one-hour band includes stops, boarding and traffic;
- `119` enters the settlement and leaves about `2km / 27m` on foot; `118/118А` stop at `Романовский поворот` and leave about `3.9km / 52m`. The paths are not identical, but the UI keeps one preferred interactive walking link from central Romanovo and leaves the different walk legs as text;
- the decorative SVG bus icon is large and unboxed. Outbound and return departures use one-line rounded time chips;
- the committed outbound snapshot stores the full day. A conservative `65m bus + route-specific walk` estimate keeps only arrivals 20–90 minutes before the event; for an `11:00` start this removes the 06:00/06:20/06:55 departures and leaves `07:40, 08:00, 08:40` for `118/118А` plus `08:10` for `119`;
- the committed return snapshot also stores the full day. `eventBusTransport.ts` requires at least `75m` on the venue, then adds the route-specific walk and keeps departures only through `event end + walk + 75m`. Thus the visitor can leave early, but the page never offers a bus they cannot reach or a pointless late-night option;
- responsive bundled maps use a square crop on desktop/tablet and a portrait crop on mobile because the preferred route is mostly north–south. On desktop the outbound and return schedules share the left side of one compact grid while the map occupies the right side; below `980px` the sections return to the natural vertical reading order. Both map variants visibly draw the same route; the map area has one walking CTA plus the venue-coordinate link.

Primary timetable: [official АО «Автовокзал» Kaliningrad route table](https://avl39.ru/routes/reg/kaliningrad/) and [route registry](https://avl39.ru/carriers/registry/). The shared corridor is also checked against OSM relations [`118`](https://www.openstreetmap.org/relation/13129809) and [`119`](https://www.openstreetmap.org/relation/13130074). Venue hours come from the [official Холмогорье site](https://xn----8sbgbk8ahdkccbcdbxc4f6g.xn--p1ai/). The committed bus calculation is a demonstration snapshot, not a live journey planner.

## Schedule snapshot and source boundary

The committed schedule is a compact service-calendar snapshot checked on **2026-07-11** against Yandex Расписания route pages that identify the carrier as АО «Калининградская ППК», then compared on **2026-07-12** with the official КППК coastal matrices effective from 3 July. The comparison found minute-level differences (for example official `6722` reaches Северный at `19:50`, while the current API snapshot has `19:48`), so the current public snapshot is deliberately **not** relabeled as officially synchronized and is not partially overwritten. Every rendered service stores train number, departure, arrival, duration and per-month operating-day bitsets. The source URL and retrieval metadata remain in the committed data/provenance contract but are not shown as public schedule-verification links.

The wider official audit and source-image hashes are in `site/src/data/railRouteDirectory.json`; see [the multimodal directory](rail-multimodal-directory.md). That reference covers all 13 direction/product pages on the carrier index and makes the origin trip-specific: inland routes use `Калининград-Южный`, while a coastal option may use `Северный` only when that train actually calls there.

The earlier July prototype at `https://static.kenigevents.ru/reference/transport/lastochka-svetlogorsk-test.json` was an **Object Storage/CDN test fixture**, not YDB. It is not used here: it was marked test-only, contained departures without arrivals/service calendars, covered only Светлогорск and used `Калининград-Южный`.

On 2026-07-11 the accessible YC YDB databases (`events-bot-acq-discovery`, `postbox-events`, `pharmastaff-forms`) were inspected read-only. No transport/schedule table or kind and no `Калининград-Северный`/`КППК` schedule contract were present. Therefore this MVP must not be described as YDB-backed. If another credential/database lane owns an authoritative transport table, the static builder may later export it into the same JSON contract without changing page rendering.

## Assets

- supplied source/provenance: `site/src/assets/transport/source/kppk-lastochka.png` and `README.md`;
- lossless browser derivative: `site/public/assets/transport/kppk-lastochka.webp`;
- official КППК/RZD partner logo remains separate under `site/public/assets/partners/`.

## Acceptance example

Real active event `6510`, `Хиты любимых артистов: Концерт-посвящение Муслиму Магомаеву и Анне Герман`, Янтарь холл, Светлогорск, `2026-07-12 17:00–18:10` is the deterministic regression page. The `18:10` end is derived from the source's explicit `Продолжительность: 1 час 10 мин.`, not from a category default:

- outbound: trains `6717`, `15:11 → 16:05` (55 minutes before), and `7213`, `15:43 → 16:29` (31 minutes before);
- return: trains `6722`, `18:54 → 19:48`, and `7220`, `19:33 → 20:19`.

`tests/test_static_site_preview_duration.py` guards the narrow labeled-duration extractor. `site/scripts/check-preview.mjs` guards the real event identity/ticket link, those train rows and calendar files, placement after the description, the supplied artwork/laconic footer and absence of the block on a Kaliningrad event.

Additional release scenarios:

- event `6397`, Светлогорск, `2026-07-12 21:30`: train `6725` arrives at `20:23` (67 minutes before); because the end is unknown the page reports the factual last same-day train at `22:40`, absence of night service and first next-day train at `06:25`, without inferring a two-hour duration or claiming no return;
- production event `6710`, Сказочное Холмогорье, `2026-07-25 11:00–16:00`: compact bus chips for `118/118А/119`, one shared corridor/Северный estimate, hours-aware earlier return choices, a drawn preferred walking route and one route link. Re-confirm the organizer date before the official presentation because the venue's aggregate site has a conflicting day label.
- production event `3103`, Янтарь холл, `2026-08-15 18:00`, labelled duration `1 ч 40 мин`: end `19:40`, safe station-ready time `20:10`, return trains `6726` and `6728`; train `6724` at `19:57` is an explicit negative regression because it leaves no realistic exit/walk/boarding time.

## TD-STATIC-TRANSPORT-001 — automated schedule refresh before presentation

> Status: **OPEN / P0 presentation blocker**.

The current committed rail/bus data is a reviewed snapshot. Before the official presentation, implement a Kaggle refresh patterned after `ParseTheatres`:

1. `scheduling.py` starts nightly and manual `transport_schedule_refresh` with `max_instances=1`, `coalesce=True` and resource lease `transport_schedule:refresh`.
2. A `KaggleClient` runner pushes `ParseTransportSchedule`, uses the shared status dataset/heartbeats/report contract and downloads normalized rail+bus JSON.
3. Validator requires source page and exact image URL/hash, `effective_from`, `fetched_at`, timezone, route/trip/stop identity, service calendar, Russian production-calendar semantics, dated override precedence, monotonic departure/arrival, non-empty output and bounded diff size.
4. Resolve or create an authoritative YDB current+history lane; the 2026-07-11 accessible databases had no transport table, so it must not be described as already YDB-backed.
5. Publish atomically only after validation. Empty/partial output keeps last-known-good, records stale age and alerts an operator.
6. A changed validated content hash exports the existing static JSON contract and enqueues one coalesced `static_site_build:prod`; release manifest records schedule snapshot ID/hash/fetched time.
7. Acceptance covers rail-primary Пионерский/Балтийск, parallel rail+bus inland routes, mixed train/bus directions for Знаменск, the Ладушкин transfer safety block for Бранденбург, exact-date Краснолесье, unknown-end schedule cutoffs, explicit-end no-return, intermediate-stop non-inference, stale source, partial failure, semantic ASCII filenames, stable UIDs, orphan-free ICS generation, per-event file ceilings and public transport ICS MIME/alarms.

Until this debt is closed, the release checklist remains blocked: the presentation candidate must be refreshed and validated manually, without adding public schedule-verification links.
