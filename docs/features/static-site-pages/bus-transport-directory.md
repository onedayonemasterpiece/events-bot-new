# Bus transport coverage directory

> Status: implemented static reference; public timetable UI remains enabled only for `romanovo-holmogorye` until each corridor has reviewed stop times.

## Storage decision

The directory belongs in versioned JSON, not YDB:

- it is consumed only during static-page generation;
- a reviewed diff, content hash and atomic site release are more useful than runtime queries;
- the site must build without a live database dependency;
- a bad refresh can retain the last-known-good JSON snapshot;
- timetable topology, venue access and trip snapshots can evolve independently.

YDB becomes justified only if this turns into a multi-writer operational dataset with history queries or a live journey API. A future Kaggle refresh may use YDB as an optional current/history store, but its accepted output must still be an atomic JSON build input.

Committed contracts:

- `site/src/data/busRouteDirectory.json` — localities, aliases, official route corridors and timetable readiness;
- `site/src/data/busVenueAccess.json` — stops, venue aliases/coordinates and reviewed last-mile measurements;
- `site/src/data/busTransportSchedules.json` — actual trip snapshot; currently only the reviewed Romanovo example;
- `npm --prefix site run check:bus-directory` — referential, count and safety validation.

When a bus row gains a calendar action, both its static path and downloaded file start with `bus-`: `bus-<route>-<latin-destination>-<YYYYMMDD>-<HHMM>.ics`, with `-e<event_id>` appended to the download name. Example: `bus-118-romanovo-20260725-0740-e6710.ics`. This is the prepared naming contract; the current Romanovo UI still renders timetable chips without bus ICS links. The VEVENT `UID` must remain stable if only the readable filename changes.

Rail priority and combined-mode decisions are not duplicated here. They live in [rail-multimodal-directory.md](rail-multimodal-directory.md), which cross-links these stable bus locality ids for destinations where rail must not suppress buses.

## Production event inventory — 2026-07-11

Read-only production SQLite selection: canonical active events with `date >= 2026-07-11` or `end_date >= 2026-07-11`, excluding Калининград, Зеленоградск and Светлогорск. Result: **30 events, 14 logical localities and 21 logical venues**.

| Locality | Events / venues | Direct route evidence | Timetable readiness |
|---|---:|---|---|
| Балтийск | 2 / 1 | `107` | endpoint ready; venue last mile remains conditional |
| Гвардейск | 2 / 1 | `111`, `211Э`, local AVL page | ready |
| Гурьевск | 2 / 2 | `103`, `104`, `152`, `245Э`, `345` | intermediate stop times needed |
| Гусев | 5 / 3 | `580`, `583`, `680Э`, local AVL page | ready |
| Медведевка | 1 / 1 | `116` | intermediate time and exact venue point needed |
| Пионерский | 2 / 2 | `118`, `118А`, `119` | endpoint ready |
| Полесск | 1 / 1 | `152`, `245Э`, `345` | intermediate stop times needed |
| Приморье | 1 / 1 | `125`, `125А` | intermediate stop times needed |
| Романово / пос. Романово | 5 / 2 | `118`, `118А`, `119` | reviewed snapshot exists for Холмогорье |
| Рыбачий | 1 / 1 | `593` | intermediate stop times needed |
| Советск | 2 / 1 | `523`, `601Э`, local AVL page | ready |
| Ушаково | 2 / 1 | `117` | intermediate stop time needed |
| Янтарный | 3 / 3 | `120`, explicitly marked до/из Янтарного rows | ready with partial-endpoint filtering |
| пос. Донское | 1 / 1 | `125`, `125А` | endpoint ready |

All 14 localities have an official one-seat corridor from Kaliningrad, but only **8** currently have timetable evidence suitable for exact trip matching. The other 6 must not be enabled by estimating an intermediate arrival from the final-route duration.

## Venue-to-stop reference

Pedestrian figures use Valhalla over OSM geometry. The table deliberately distinguishes exact measurements, coordinate proxies and blocked rows.

| Locality / venue | Served stop | Last mile | State |
|---|---|---:|---|
| Балтийск — глэмпинг «Территория Я» | `10-й километр`, `107` | 2.611 km / 32 min | conditional: organizer must confirm a safe entrance/path; otherwise show transfer/taxi only |
| Гвардейск — Замок Тапиау | Автостанция Гвардейск | 421 m / 6 min | measured |
| Гурьевск — Замок Нойхаузен | `ул. Заречная` | 17 m / 1 min | measured; stop coordinate is a medium-confidence platform proxy |
| Гурьевск — Центр культуры и досуга | `Поликлиника` | 360 m / 5 min | measured to official-stop POI proxy |
| Гусев — Гусевский музей | `Площадь Победы` | 737 m / 10 min | measured; the official Gumbinnen visitor page confirms this in-city alighting option |
| Гусев — кафе «АРТеФАКТ39» | `Площадь Победы` | 737 m / 10 min | same building as the museum |
| Гусев — Gumbinnen | `Площадь Победы` | 356 m / 5 min | measured; venue itself recommends `680Э` and this stop |
| Медведевка — `посёлок Медведевка` | `Медведевка`, `116` | not published | **blocked:** event has no venue point; a locality centroid is not a venue |
| Пионерский — Городской парк | `Пионерский курорт`, `119` | 517 m / 7 min | measured |
| Пионерский — Сцена у моря | `Пионерский курорт`, `119` | 1.004 km / 12 min | approximate stage point; confirm with organizer |
| Полесск — Замок Лабиау | `Полесск` | 107 m / 2 min | measured; schedule still waits for intermediate time |
| Приморье — сквер у озера | `Приморье`, `125/125А` | 247 m / 4 min | address/stop proxy; schedule still conditional |
| Романово — Сказочное Холмогорье | `Романово`, `119` | 2 km / 27 min | reviewed public example |
| Романово — Сказочное Холмогорье | `Романовский поворот`, `118/118А` | 3.9 km / 52 min | reviewed public example |
| Романово — Поселение викингов Кауп | `Романово-2`, `119` | 4.306 km / 53 min | **do not enable:** OSM pedestrian graph produces a detour; entrance/path needs field/Yandex confirmation |
| Рыбачий — `посёлок Рыбачий` | `Рыбачий`, `593` | 0–800 m / 0–10 min | locality band only; exact event point missing |
| Советск — ОЦК ТеплоСеть | Автовокзал Советск | 1.056 km / 13 min | measured |
| Ушаково — Замок Бранденбург | `Ушаково-1`, `117` | 499 m / 6 min | measured; timetable still conditional |
| Янтарный — Музей «Штольня» | `Памятник советским воинам`, `120` | 307 m / 4 min | measured to address proxy `Советская 63` |
| Янтарный — Общественная приёмная Губернатора | `Универмаг`, `120` | 258 m / 4 min | measured |
| Янтарный — Стадион им. А.А. Остренко | `Памятник советским воинам`, `120` | 622 m / 8 min | measured |
| пос. Донское — Молодёжный образовательно-досуговый центр | `Донское`, `125/125А` | 229 m / 3 min | medium confidence: address candidate must be confirmed |

### Critical safety corrections

- **Ушаково homonym:** route `110` goes to another Ушаково via Родники/Низовье. Brandenburg is on the Мамоново corridor `117`. The validator forbids attaching `110` to this venue.
- **Территория Я:** `10 км` in the source address is a road-location label, not a ten-kilometre walking distance. The mapped route from stop `10-й километр` is 2.611 km.
- **Медведевка:** route and stop exist, but no exact event venue exists in the data. Numeric walking guidance is suppressed.
- **Кауп:** straight-line intuition is unsafe; the routable pedestrian graph gives more than 4 km. Public guidance remains off pending an entrance/path check.

## Prepared catalog candidates

The committed directory also keeps non-active/static-catalog candidates so the next matching event does not restart research:

- Черняховск: Администрация `1.075 km / 13 min`, Инстербург `1.546 km / 18 min` from the bus station; КФХ «Калина» is actually in Калиновка and is blocked as a transfer trip;
- Залесье: кирха Меляукена `141 m / 2 min` from the route `345` stop;
- Некрасово: АгроПарк `750 m / 10 min` to the building proxy; enable only after entrance confirmation and only on route `116` trips that serve Некрасово.

## Source and refresh boundary

Primary transport evidence:

- [official Kaliningrad bus-terminal timetable](https://avl39.ru/routes/reg/kaliningrad/);
- [official regional station-page index](https://avl39.ru/routes/reg/);
- [official intermunicipal route registry](https://avl39.ru/carriers/registry/), including the linked 2026-04-23 XLSX;
- OSM/Photon for stop/venue coordinates and Valhalla for pedestrian routes.

The official XLSX is useful but not infallible: isolated route names and ordered-stop cells disagree. Accepted refreshes must preserve raw notes, require route-name/stop QA and use station pages/timetable rows for actual trip times. No intermediate stop time may be inferred from total trip duration.
