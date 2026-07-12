# Rail and multimodal transport directory

> Status: official-source reference implemented; public rail UI remains enabled only for Светлогорск and Зеленоградск until the reviewed matrices are exported into exact-date service calendars.

## Storage and source boundary

`site/src/data/railRouteDirectory.json` is the versioned build-time reference for rail topology, official source images, service patterns, locality priority and special last-mile rules. Like the [bus directory](bus-transport-directory.md), it stays in JSON rather than YDB because static generation needs a reviewable, atomic input and no runtime database query.

The audit on **2026-07-12** covered every direction/product page linked from the [official КППК schedule index](https://www.kppk39.ru/raspisanie/): both coastal directions, Гурьевск, Чкаловск, Багратионовск, Балтийск, Мамоново, Советск/Неман, Чернышевское, Железнодорожный, Краснолесье, the heritage train and Морской экспресс. The JSON retains each page URL, exact carrier image URL, effective date, review state and SHA-256. `npm --prefix site run check:rail-directory` verifies source provenance, references, priority invariants, timetable anchors and safety exclusions.

КППК publishes timetable matrices as raster images and operational changes as separate posts. The refresh precedence is:

1. exact-date cancellation or exception;
2. the latest manually reviewed base whose `effective_from` is not later than the event date;
3. an older base only when no newer base applies.

A base without an official end date remains valid until superseded. Do not invent `30 September` or another season end. `Раб.`, `Вых.` and `празд.` must be resolved with the Russian production calendar before carrier add/remove dates are applied. A temporary notice must never replace the reusable base table.

## Product priority by locality

| Locality | Rail source / current pattern | Mode policy | Public state |
|---|---|---|---|
| Светлогорск | frequent Ласточка via Переславское or Зеленоградск; base from 3 Jul | **rail primary** | enabled |
| Зеленоградск | frequent Ласточка; base from 3 Jul | **rail primary** | enabled |
| Пионерский | frequent stop `Пионерский Курорт` on both coastal matrices | **rail primary**, bus only as fallback | exact-date export needed |
| Балтийск | daily РА-2, 5 outbound + 6 return in the base from 29 Jun | **rail primary while the multi-pair table is current** | exact-date export needed |
| Гвардейск / Знаменск | sparse stopping trains on the Чернышевское line | rail and bus together; allow different modes each way | exact-date rail/bus match needed |
| Черняховск | several usable weekend trains, fewer useful weekday pairs | rail and bus together | exact-date export needed |
| Гусев / Нестеров | useful weekend tourist pair, commuter-shaped otherwise | rail and bus together | exact-date export needed |
| Железнодорожный | one seasonal weekend/holiday round trip | rail and bus together | exact-date export + bus directory needed |
| Багратионовск | limited regular РА-2 plus Tue/Thu heritage overlay | rail and bus together | exact-date export + bus directory needed |
| Мамоново | two outbound / three return patterns depending on day class | rail and bus together | exact-date export + bus directory needed |
| Ушаково / Замок Бранденбург | no rail stop; nearest usable corridor stop is Ладушкин | route `117` first; train only as **rail + reviewed road transfer** | bus remains the only enabled direct option |
| Краснолесье | one weekend/holiday tourist round trip | exact-date special rail plus bus fallback | event/service match needed |
| Полесск / Советск / Неман | limited РА-2 on the northern corridor | rail and bus together | exact-date export needed |
| Калининград — ДС «Янтарный» | new `Елизаветинская` platform on the Светлогорск line | optional **venue-specific** rail suggestion | exact-date export + real-event regression needed |

“Rail primary” does not mean that the block appears merely because a town has a station. A suggestion still needs an operating trip for the exact event date, arrival in the configured pre-event window, a reachable station/venue leg and a useful way back. Limited rail never suppresses feasible buses.

## Verified timetable anchors

- **Пионерский:** the official [direct coastal table from 3 July](https://www.kppk39.ru/raspisanie/kaliningrad-svetlogorsk-cherez-pereslavskoe/) provides frequent exact `Калининград-Северный ↔ Пионерский Курорт` cells. Express columns with an empty Pionersky cell are not inferred as stops. The [Зеленоградск coastal table](https://www.kppk39.ru/raspisanie/kaliningrad-zelenogradsk-svetlogorsk/) is secondary and keeps `Зеленоградск-Новый` distinct from `о.п. Зеленоградск-2` and transfer-marked columns.
- **Балтийск:** current [daily table from 29 June](https://www.kppk39.ru/raspisanie/kaliningrad-baltiysk/) contains South departures `08:10, 09:48, 11:30, 13:05, 17:45` and returns `06:34, 09:49, 13:08, 14:30, 16:20, 19:05`. This meets the product's current “several summer trains” rule without guessing an official end date.
- **Eastern corridor:** the [base from 1 June](https://www.kppk39.ru/raspisanie/kaliningrad-chernyakhovsk-chernyshevskoe/) uses РА-2 from `Калининград-Южный`. Tourist trains `6582/6591`, `6572`, `6594/6583` and `6571` skip Гвардейск and Знаменск; geography must not create a fake stop.
- **Железнодорожный:** weekends/holidays, `09:15 → 11:35` and `15:25 → 17:46`, with a 15-minute Черняховск stop. The page's 27–28 June cancellation is historical and does not replace the [1 May base](https://www.kppk39.ru/raspisanie/kaliningrad-zheleznodorozhnyy/).
- **Багратионовск:** the [base from 23 June](https://www.kppk39.ru/raspisanie/kaliningrad-bagrationovsk/) has regular day-class-dependent service. `6320/6321` are the same Tue/Thu timetable rows with a heritage-steam overlay, not duplicate journeys.
- **Мамоново / Ладушкин:** the [base from 15 April](https://www.kppk39.ru/raspisanie/kaliningrad-mamonovo/) is sparse. There is no `Ушаково` station in that table; the UI must never rename a Ладушкин arrival to Ушаково.
- **Краснолесье:** the [1 May tourist table](https://www.kppk39.ru/raspisanie/kaliningrad-krasnolesye-cherez-chernyahovsh/) runs on weekends/holidays, `09:55 → 12:35` and `18:25 → 21:00`. It gives a fixed visit window of about 5h50 and requires the carrier's passport/border-zone warning for nearby park travel.

## Venue-specific decisions

### Ферма Тюниных, Знаменск

The venue's published point is `54.6189793, 21.2272530`; station `Знаменск` is `54.6149360, 21.2148737`. OSM pedestrian routing gives **1.057 km / about 14 minutes**. A Saturday/holiday `08:15 → 09:07` train works well outbound, but the later same-day rail return is `13:41`; Sunday has no equivalent daytime outbound stopping service. The journey model must support **train there + bus back**, not require the same mode in both directions.

### Замок Бранденбург, Ушаково

Official route `117` reaches the correct Ushakovo corridor and remains the direct public option. A Mamonovo train can only be considered to `Ладушкин`, followed by a separately reviewed bus/taxi leg. Until that transfer has exact stop times and geometry, the rail alternative stays disabled; it is never described as a train to Ушаково.

### Дворец спорта «Янтарный», Калининград

The platform `о.п. Елизаветинская` opened on **3 July 2026** at the Елизаветинская / Генерала Челнокова intersection. It appears as an explicit row in the current official Светлогорск-via-Переславское matrices; express columns with an empty cell still must not be treated as stopping services.

This is a useful exception to the general “no transport block for Kaliningrad events” rule. OSM/Valhalla walking over the published intersection and current pedestrian network gives approximately **627 m / 8–10 minutes** to the venue at `Согласия, 39`. A stopping train takes approximately **15–18 minutes from Калининград-Южный** or **7–8 minutes from Калининград-Северный**. Both pairs are within the official 2026 `до 10 км` band, so the adult one-way fare is **35 ₽**.

The match must be venue-specific: exact aliases `ДС Янтарный`, `Дворец спорта Янтарный`, `Спорткомплекс Янтарный`, or the canonical address. It must never activate for unrelated Калининград events. Public rendering still waits for exact event-date calendars and a real event regression; when enabled, hide the option if no useful arrival or return survives the normal filters.

Do not duplicate the same train into separate South and North cards. The compact outbound row contains two origin choices and one arrival:

```text
[19:18 Южный]  [19:26 Северный]  →  19:33 Елизаветинская
35 ₽ · затем 8–10 мин пешком
```

Each origin-time chip is its own calendar link because the reminder time depends on the boarding station. The return row has one departure reminder and two arrivals:

```text
23:29 Елизаветинская  →  23:36 Северный · 23:43 Южный
```

On mobile the same row wraps after the origin chips; it does not become two full cards. The shared header/meta line states the fare and both travel-time bands once.

The two origin actions are the only justified duplication: their `DTSTART` and `LOCATION` differ. Their concise files will be named `to-elizavetinskaya-south-<date>-<train>.ics` and `to-elizavetinskaya-north-<date>-<train>.ics`. The return produces one `to-kaliningrad-<date>-<train>.ics`, not separate North/South copies, because the departure reminder is identical.

## Runtime/export boundary

The reference is deliberately broader than current UI support. A locality moves from `needs_service_calendar_export` to public rendering only after the refresh supplies:

- exact stop-level trips for the event date and Russian workday/holiday semantics;
- current base plus applicable dated exceptions;
- venue/station walking or transfer time;
- feasible arrival and return filtering, including mixed modes;
- a transport `.ics` and regression page.

The current `transportSchedules.json` coastal calendar snapshot was additionally compared with the two official 3 July matrices. Minute-level differences exist, so the API snapshot is not partially rewritten and must not be described as officially synchronized. The official tables also do not justify its long future calendar by themselves; existing service bitsets remain sourced from the schedule API until `TD-STATIC-TRANSPORT-001` replaces the whole manual snapshot atomically.
