# Event transport guidance

> Status: presentation-candidate MVP. Scope: rail for Светлогорск/Зеленоградск and a bus example for Сказочное Холмогорье in Романово.

## Product rule

The event page offers neutral travel help after the description:

- origin: `Калининград-Северный`;
- destinations: `Светлогорск-2` for Светлогорск and `Зеленоградск-новый` for Зеленоградск;
- placement: immediately after the event description and before `Коротко`;
- public copy: no `Партнёрский маршрут` or prominent carrier promotion; the train footer contains one terse `Перевозчик — КППК` line;
- visual: the supplied side-view Lastochka artwork, not the official carrier logo; each row still names its actual train type;
- options: at most two trains in each direction.

This is a release feature of the event page, not a standalone campaign. It is included in the [official presentation checklist](presentation-release-checklist.md).

## Matching contract

`site/src/lib/eventTransport.ts` performs deterministic build-time matching against `site/src/data/transportSchedules.json`:

1. Normalize the event city and require exact `Светлогорск` or `Зеленоградск`.
2. Require a single-day event with a reliable start time; a multi-day event has no unambiguous travel date.
3. Keep only trains operating on `event.start_date` whose destination arrival is **20–90 minutes before** the event start.
4. Rank the closest arrival to a 30-minute buffer and return no more than two options.
5. Calculate the trip back from an explicit `time_range_end`. The exporter may derive that field only from a source-labeled `Продолжительность: …` value.
6. If the source has no end, do not invent one from the event type. Show factual schedule boundaries instead: the last same-day return, whether a night service exists, and the first next-day return. Ask the visitor to confirm the organizer’s end time.
7. Use the event type only to inflect the outbound heading (`К началу концерта / спектакля / мастер-класса …`), never to calculate time.
8. With an explicit end, keep the first two return trains departing within three hours. A categorical no-return message is allowed only when the service calendar covers the relevant dates.
9. Hide the whole block when neither a usable outbound option nor a matched return option exists. For an unknown end there is no matched return by definition, so a supported page needs at least one outbound train; schedule cutoffs enrich that block but do not make an otherwise empty block appear.
10. Every rendered train, including a factual last/night return, links to its own static `.ics` containing departure, arrival, route and a `VALARM` 30 minutes before departure.

The selector and calendar files are static. Public UI and `.ics` descriptions do not contain schedule-verification links.

## Bus example: Сказочное Холмогорье

`site/src/data/busTransportSchedules.json` and `site/src/lib/eventBusTransport.ts` activate only for a source-backed `11:00–16:00` event at `Холмогорье / Сказочное Холмогорье`, Романово:

- `119` enters the settlement: calculated `46m` ride to `Романово`, then about `2km / 27m` on foot;
- `118/118А` stop at `Романовский поворот`: calculated `60–65m` ride, then about `3.9km / 52m` on foot;
- outbound departures render as compact rounded time chips; every chip adds the estimated `Северный вокзал` time (`автовокзал + 15m`) because all three routes serve that stop;
- return times render as compact chips grouped by `118/118А` and `119`; the `≈` marker is retained for calculated intermediate-stop times;
- the bundled map visibly draws the pedestrian route, while links open walking directions from each stop and the venue point in Yandex Maps.

Primary timetable: [official АО «Автовокзал» Kaliningrad route table](https://avl39.ru/routes/reg/kaliningrad/). Venue/map coordinates are checked against the regional tourism portal. The committed bus calculation is a demonstration snapshot, not a live journey planner.

## Schedule snapshot and source boundary

The committed schedule is a compact service-calendar snapshot checked on **2026-07-11** against Yandex Расписания route pages that identify the carrier as АО «Калининградская ППК». Every service stores train number, departure, arrival, duration and per-month operating-day bitsets. The source URL and retrieval metadata remain in the committed data/provenance contract but are not shown as public schedule-verification links.

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
- production event `6710`, Сказочное Холмогорье, `2026-07-25 11:00–16:00`: compact bus chips for `118/118А/119`, estimated journey legs, `Северный +15m`, a drawn walking route and interactive map links. Re-confirm the organizer date before the official presentation because the venue's aggregate site has a conflicting day label.

## TD-STATIC-TRANSPORT-001 — automated schedule refresh before presentation

> Status: **OPEN / P0 presentation blocker**.

The current committed rail/bus data is a reviewed snapshot. Before the official presentation, implement a Kaggle refresh patterned after `ParseTheatres`:

1. `scheduling.py` starts nightly and manual `transport_schedule_refresh` with `max_instances=1`, `coalesce=True` and resource lease `transport_schedule:refresh`.
2. A `KaggleClient` runner pushes `ParseTransportSchedule`, uses the shared status dataset/heartbeats/report contract and downloads normalized rail+bus JSON.
3. Validator requires source URL, `effective_from`, `fetched_at`, timezone, route/trip/stop identity, service calendar, monotonic departure/arrival, non-empty output and bounded diff size.
4. Resolve or create an authoritative YDB current+history lane; the 2026-07-11 accessible databases had no transport table, so it must not be described as already YDB-backed.
5. Publish atomically only after validation. Empty/partial output keeps last-known-good, records stale age and alerts an operator.
6. A changed validated content hash exports the existing static JSON contract and enqueues one coalesced `static_site_build:prod`; release manifest records schedule snapshot ID/hash/fetched time.
7. Acceptance covers rail, bus, unknown-end schedule cutoffs, explicit-end no-return, intermediate-stop inference, stale source, partial failure and public transport ICS MIME/alarms.

Until this debt is closed, the release checklist remains blocked: the presentation candidate must be refreshed and validated manually, without adding public schedule-verification links.
