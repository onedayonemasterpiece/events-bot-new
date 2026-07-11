# Event transport schedule / КППК campaign block

> Status: implemented static-site MVP. Scope: event-detail pages for Светлогорск and Зеленоградск.

## Product rule

The product owner explicitly enabled a scoped carrier campaign on event-detail pages for the two coastal cities:

- origin: `Калининград-Северный`;
- destinations: `Светлогорск-2` for Светлогорск and `Зеленоградск-новый` for Зеленоградск;
- placement: immediately after the event description and before `Коротко`;
- disclosure: `Партнёрский маршрут · АО «КППК»` is always visible;
- visual: the supplied side-view Lastochka artwork, not the official carrier logo; each row still names its actual train type;
- options: at most two trains in each direction.

This city rule is an explicit MVP campaign decision, not a partner inferred from the event title or organizer. A later generic promo implementation should model the surface as `site_transport`, carry city/route targeting in campaign metadata and count its exposure separately from organic recommendations.

## Matching contract

`site/src/lib/eventTransport.ts` performs deterministic build-time matching against `site/src/data/transportSchedules.json`:

1. Normalize the event city and require exact `Светлогорск` or `Зеленоградск`.
2. Require a single-day event with a reliable start time; a multi-day event has no unambiguous travel date.
3. Keep only trains operating on `event.start_date` whose destination arrival is **20–40 minutes before** the event start.
4. Rank the closest arrival to a 30-minute buffer and return no more than two options.
5. Calculate the trip back from an explicit `time_range_end`. If the time range is absent, the production exporter may derive it only from a source-labeled `Продолжительность: …` value; free-form duration guesses are forbidden.
6. Keep the first two return trains departing within three hours after the stated end. The selector checks both the event date and the following calendar date.
7. If the start/end or a matching service is absent, render an honest empty state instead of inventing a train.

The block is static HTML. It never calls YDB, the carrier or Yandex from the visitor's browser.

## Schedule snapshot and source boundary

The committed schedule is a compact service-calendar snapshot checked on **2026-07-11** against Yandex Расписания route pages that identify the carrier as АО «Калининградская ППК». Every service stores train number, departure, arrival, duration and per-month operating-day bitsets. Public rows include links to re-check live operational changes before travel.

The earlier July prototype at `https://static.kenigevents.ru/reference/transport/lastochka-svetlogorsk-test.json` was an **Object Storage/CDN test fixture**, not YDB. It is not used here: it was marked test-only, contained departures without arrivals/service calendars, covered only Светлогорск and used `Калининград-Южный`.

On 2026-07-11 the accessible YC YDB databases (`events-bot-acq-discovery`, `postbox-events`, `pharmastaff-forms`) were inspected read-only. No transport/schedule table or kind and no `Калининград-Северный`/`КППК` schedule contract were present. Therefore this MVP must not be described as YDB-backed. If another credential/database lane owns an authoritative transport table, the static builder may later export it into the same JSON contract without changing page rendering.

## Assets

- supplied source/provenance: `site/src/assets/transport/source/kppk-lastochka.png` and `README.md`;
- lossless browser derivative: `site/public/assets/transport/kppk-lastochka.webp`;
- official КППК/RZD partner logo remains separate under `site/public/assets/partners/`.

## Acceptance example

Real active event `6510`, `Хиты любимых артистов: Концерт-посвящение Муслиму Магомаеву и Анне Герман`, Янтарь холл, Светлогорск, `2026-07-12 17:00–18:10` is the deterministic regression page. The `18:10` end is derived from the source's explicit `Продолжительность: 1 час 10 мин.`, not from a category default:

- outbound: train `7213`, `15:43 → 16:29` (arrival 31 minutes before start);
- return: trains `6722`, `18:54 → 19:48`, and `7220`, `19:33 → 20:19`.

`tests/test_static_site_preview_duration.py` guards the narrow labeled-duration extractor. `site/scripts/check-preview.mjs` guards the real event identity/ticket link, those train rows, placement after the description, the supplied artwork/disclosure and absence of the block on a Kaliningrad event.
