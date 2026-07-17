# Аудит каталога «Клубы по интересам» — 17 июля 2026

## Статус и ответ

**Read-only каталог-аудит выполнен, но owner и технологический BGE gates остаются незакрытыми.** На замороженном production-каталоге вручную проверено **52 candidate clusters**:

- **20 `confirmed`**;
- **14 `probable`**;
- **8 `needs_evidence`**;
- **10 `rejected`**.

`confirmed + probable` дают **34 club identities** и **203 уникальных surviving canonical event ids**: **198 прошлых** и **5 будущих** относительно 17 июля 2026 года. В ledger 204 memberships: одно событие может принадлежать двум совместно проводящим identities. Все 34 принятые identities имеют минимум две атомарные даты; 17 имеют не менее трёх, 13 — не менее пяти. Это **не означает, что в регионе доказано ровно 34 клуба**: независимо замороженная discovery-only проверка уже нашла пропуски первой ручной версии pool, после чего семь identities и шесть event memberships были source-проверены и добавлены. Полная семантическая полнота всё ещё не доказана; paired BGE/Gemini head-to-head на одном frozen corpus не выполнен.

Для owner review безопасный shortlist сейчас таков:

- **20 confirmed** можно обсуждать как основу shadow-каталога;
- **14 probable** нельзя публиковать без дополнительной source-проверки;
- **8 needs_evidence** должны fail closed;
- **10 rejected clusters** фиксируют основные false-positive классы.

Текущий product/technology вывод: **GO только на следующий offline shadow benchmark и owner review; NO-GO на публичные страницы, DB schema, scheduler, Smart Update integration и выбор BGE как production architecture.**

## 1. Воспроизводимость и границы данных

### Git и снимок

- Ветка: `feature/interest-clubs-postrelease`.
- Base: `origin/main@100892d87c56f9fa465c4f10bcb712fda27fbbeb`.
- Первый research-contract commit: `ce7397e2ba79bd6102c9a19e0e43618f087c6b6e`.
- Production SQLite: Fly `/data/db.sqlite`, открыт URI `mode=ro`.
- Schema/read time: `2026-07-17T07:49:12Z`; catalog extract: `2026-07-17T07:51:35Z`.
- `PRAGMA quick_check = ok`; DB size на момент probe: `260,362,240` bytes.
- Сжатый frozen export: `6,527` event rows, JSONL SHA-256 `f93c7a4a79272a683f6eadff92800089ff8424885e16ffb764323a8f3c6e2fe0`.
- Аналитический timezone: `Europe/Kaliningrad`; `event.date` трактуется как локальная календарная дата.
- Raw root, не коммитится: `artifacts/codex/interest-clubs-audit-20260717/`.

Production DB не изменялась. Supabase vector sidecar читался service-side ключом только через read-only REST select; значение ключа не сохранялось. Raw export содержит полный production `source_text`, публичные контакты из анонсов и ticket-widget значения, поэтому это локальный restricted research material: artifact root имеет mode `0700`, файлы `0600`, всё находится под gitignore и не должно коммититься. В tracked report/review fixture закрытые payloads и секреты не включены.

### Окна

| Окно | Правило | Eligible atomic rows |
|---|---|---:|
| Discovery | до `2026-06-01` | 4,667 |
| Holdout elapsed | `2026-06-01`…`2026-07-16` | 1,400 |
| Future | с `2026-07-17` | 258 |
| **Всего primary scope** | canonical, active, non-silent, non-merged, atomic date | **6,325** |

В primary denominator вошли только `identity_status=canonical`, `merged_into_event_id IS NULL`, `lifecycle_status=active`, `silent=0` и parseable atomic dates. Отдельно учтены, но не использованы как положительная club-history:

- 108 `cancelled`, 20 `postponed`, 18 `merged`, 18 `archived`, 3 `duplicate`, 1 `skipped` lifecycle rows;
- 161 silent rows;
- 11 range-date rows вида `YYYY-MM-DD..YYYY-MM-DD`;
- 2 повреждённых date rows;
- festival/programme rows;
- linked occurrences, same-date/source duplicate leakage и non-canonical identity rows.

Завершённые historical meetings остаются с `lifecycle_status=active`, поэтому past/current/future определяется датой, а не lifecycle. Legacy dedup иногда физически удалял merged rows; текущий снимок не может восстановить полный «ever ingested» denominator. В отчёте `unique canonical events` означает surviving canonical ids на момент снимка.

## 2. Карта данных и качество

Канонические данные принадлежат Fly SQLite:

- `event`: title/description/dates/place/city/festival/topics/search digest, lifecycle, identity, `linked_event_ids` и public URLs;
- `event_source`: multi-source URL, platform, public source alias, source text, trust;
- `event_source_fact`: source-grounded facts;
- `festival`: edition metadata, но `event.festival` остаётся denormalized text;
- `event_identity_decision_log` / `event_identity_lock`: duplicate/identity evidence, не club relation.

First-class organizer relation нет. Source/venue нельзя автоматически объявлять club identity. `linked_event_ids` означает одну программу на других датах и должен использоваться как confounder, а не как club membership.

Ключевые quality-наблюдения:

- `event_source` содержит 9,086 rows и покрывает 5,968/6,527 events (**91.4%**), но 5,251/9,086 source rows (**57.8%**) не имеют заполненного `trust_level`;
- 986/6,527 events (**15.1%**) имеют `festival` text;
- 1,298/6,527 (**19.9%**) несут непустой `linked_event_ids`;
- SQL `min(date)/max(date)` по строке оказался ненадёжным из-за mixed legacy formats и повреждённой строки; после schema-aware parsing фактические atomic bounds: `2023-03-16`…`2027-04-23`;
- в принятом club ledger public source URL присутствует у 202/204 event memberships (**99.0%**); один canonical event участвует в co-hosted identities, поэтому memberships больше 203 unique ids.

В таблицах `events_total` означает число retained canonical event memberships в кандидате, а не число встреч после схлопывания. `distinct_dates` считается отдельно; строки одной даты не объявляются ни независимыми встречами, ни дублями без source/slot adjudication.

## 3. Как формировался candidate pool

High-recall surfaces были объединены, но не использовались как semantic verdict:

1. public source/community handles и URL aliases;
2. явные именованные club/community entities в title и source evidence;
3. recurrence на разных атомарных датах;
4. title normalization и cadence;
5. venue/city continuity или объяснимая миграция;
6. существующие related/identity/vector контракты как candidate evidence;
7. ручная source-grounded adjudication исследовательским агентом с fail-closed verdict; это не калиброванный production-LLM run.

Raw recall дал:

- 882 event hits по широкому club/community evidence (включая source-text contamination);
- 510 normalized-title groups с минимум двумя датами;
- 621 recurring source groups.

Первый ручной pass свёл recall surfaces к 45 clusters, но полного item→candidate/exclusion funnel не сохранил, поэтому его нельзя называть исчерпывающим high-recall каталогом. Независимый агент, видевший только 4,667 discovery rows до cutoff, заморозил ещё 29 hypotheses. Source-review пересечения и новых находок добавил к финальному ledger семь identities и шесть пропущенных memberships; итог — 52 reviewed clusters. Это прямо показывает остаточный false-split/completeness risk. **Детерминированный слой не решает смысл:** keyword `клуб` даёт ночные площадки и стендап-шоу, substring даёт `клубника`, source-only grouping сливает независимые сообщества одной площадки.

`confidence` в таблицах — только ordinal `high/medium/low`, а не калиброванная вероятность. До owner-approved gold числовая calibration невозможна.

## 4. Полный shortlist `confirmed`

| Кандидат | Интерес | Verdict/confidence | events total/past/future; dates | Период/cadence | Города; площадки | Sources/aliases | Event evidence | Rationale / confounders | Blind future recognition |
|---|---|---|---:|---|---|---|---|---|---|
| Rep Chess Kaliningrad | шахматы | `confirmed` / high | 22/22/0; 20 дат | 2026-04-23…2026-07-11; observed gaps 1–10 days; median 4 days | Калининград, Медведевка; Адрес: ул. Куйбышева, Арт-пространство Сигнал, Бар Татьяна, Библиотека Чехова, ВС, Винный факультет, Закхаймские ворота, Коммуналка, Форма пицца-бар, Гаражная 2б, Калининград, бар Краны и стаканы, посёлок Медведевка, ул. Профессора Баранова, 2Б | RepChessKld, kraftmarket39, repchesskld, winnie39ru | IDs `4056,4107,4243,4323,4529,4615,4723,4762,4763,5056,5072,5099,5240,5338,5696,5762,5932,6250…`; [#4056 2026-04-23 Турнир по шахматам в Винном факультете](https://t.me/repchesskld/318)<br>[#6657 2026-07-05 Шахматный турнир (рапид)](https://t.me/repchesskld/379) | Один публичный source alias сопровождает турниры, тренировки и мастер-классы на многих датах и площадках; меняется формат, но сохраняется шахматная identity.<br>Риски: same-day duplicate/source-bundle possible for 2026-04-26 and 2026-05-10 | нет future-row |
| Game Vibes | настольные игры | `confirmed` / high | 25/23/2; 19 дат | 2026-03-13…2026-07-26; observed gaps 1–28 days; median 7 days | Калининград; Клуб настольных игр «Газебо», Клуб настольных игр Газебо, ОКЦ на Горького, Сигнал, Сигнал, Леонова 22, Калининград, арт-пространство "ОКЦ" | BabilonTheGame, gamevibes_chat, gamevibes_kld, kulturnaya_chaika, okcng, signalkld, terkatalk | IDs `2897,2915,3275,3327,3376,3441,3442,3443,3522,3653,3735,3945,4073,4080,4280,5337,5677,6330…`; [#2897 2026-03-14 Вавилон](https://t.me/signalkld/9929)<br>[#6929 2026-07-19 Настольные игры](https://t.me/signalkld/11317) | Публичное игровое сообщество объединяет Codenames, ДВИ, квизы, D&D и другие игры; одинаковый source alias связывает разные темы и площадки.<br>Риски: same-day programme rows inside ДВИ; venue Signal is not identity by itself; excluded by scope: #3377 (lifecycle_not_active/silent) | exact-name 0/2; name/source candidate 2/2 |
| Westside Movie Club | киноклуб | `confirmed` / high | 35/35/0; 29 дат | 2025-09-27…2026-07-12; observed gaps 1–28 days; median 9 days | Зеленоградск, Калининград; Малиновка, ОКЦ на Горького, Остров Канта, Пространство Тёрка, Сигнал, Сигнал, Леонова 22, Калининград, Суспирия, новый ОКЦ, пространство Сигнал | kinokloob, klassster, kulturnaya_chaika, meowafisha, okcng, signalkld, terkatalk, westside_movieclub | IDs `805,1031,1088,1307,1410,1580,1656,1873,2269,2757,2761,2762,2763,2815,2903,3353,3354,3378…`; [#805 2025-09-27 🕺 Киномарафон: Человек-паук от Рэйми](https://vk.com/wall-211997788_2419)<br>[#6754 2026-07-12 🎬 Кинопоказ «10 причин моей ненависти»](https://vk.com/wall-218351015_168) | Именованный киноклуб имеет собственный Telegram/VK source и проводит разные показы на разных площадках; identity не равна одному фильму или месту.<br>Риски: cross-post duplicates on same date/title; exclude unrelated concerts cross-posted by venue; excluded by scope: #4465 (lifecycle_not_active/silent),#4466 (lifecycle_not_active/silent),#5933 (lifecycle_not_active/silent),#5934 (lifecycle_not_active/silent),#5936 (lifecycle_not_active/silent) | нет future-row |
| Клуб исследователей нейронок | ИИ и нейросети | `confirmed` / high | 9/9/0; 8 дат | 2026-02-11…2026-07-04; observed gaps 7–35 days; median 21 days | Калининград; Арт-пространство Сигнал, Сигнал, Сигнал, Леонова 22, Калининград, а начали новый формат — совместную лабораторию, где вместе тестируем нейросети, обсуждаем промптинг и ищем, как сделать обучение полезнее для новичков 💡, встречу в субботу, где ломается и как использовать лучше 🧠 | signalkld, terkatalk | IDs `2533,2951,3019,3174,3982,4049,4516,6221,6662`; [#2533 2026-02-11 🤖 Клуб Исследователей Нейронок: Создаём сайт с помощью ИИ](https://vk.com/wall-211997788_2757)<br>[#6662 2026-07-04 Клуб исследователей нейронок](https://t.me/signalkld/11229) | Одинаковая именованная identity и публичный source Сигнала связывают лаборатории/встречи с разными практическими темами.<br>Риски: two canonical rows on 2026-03-11; 4049 has changed title but explicit continuation evidence | нет future-row |
| Клуб исследователей технологий | электроника и технологии | `confirmed` / high | 11/11/0; 10 дат | 2026-03-14…2026-06-10; observed gaps 7–21 days; median 7 days | Калининград; Сигнал, Сигнал, Леонова 22, Калининград, 🔧✨ В эту среду | signalkld | IDs `3032,3516,3603,3825,4057,4275,4479,4579,5328,5565,5806`; [#3032 2026-03-14 Оживляем устройства! Мастер-класс по Arduino](https://t.me/signalkld/9975)<br>[#5806 2026-06-10 Клуб исследователей технологий](https://t.me/signalkld/10945) | Повторяющиеся открытые встречи по пайке/Arduino/электронике сохраняют именованную identity и source при смене конкретного задания.<br>Риски: same-day cross-source duplicate on 2026-05-06 | нет future-row |
| Клуб «… про … отношения» | отношения и коммуникация | `confirmed` / high | 8/8/0; 7 дат | 2026-01-29…2026-04-12; observed gaps 1–49 days; median 7 days | Калининград; Арт-пространство Сигнал, Сигнал, Сигнал, Леонова 22, Калининград, Форма пицца-бар, Гаражная 2б, Калининград | relationclubnews, signalkld | IDs `2371,2806,3105,3106,3137,3461,3462,3463`; [#2371 2026-01-29 🗣️ Первая очная вечерняя встреча Клуба «… про … отношения»](https://vk.com/wall-211997788_2719)<br>[#3463 2026-04-12 Обнимательная встреча от Кати](https://t.me/signalkld/10193) | Собственный source relationclubnews и явное название связывают разные разговорные/творческие форматы вокруг темы отношений.<br>Риски: events are heterogeneous; source-only merging requires bounded evidence | нет future-row |
| Клуб «С тобой всё в порядке» | психологическая взаимопомощь | `confirmed` / high | 9/9/0; 7 дат | 2026-04-04…2026-06-27; observed gaps 7–21 days; median 14 days | Калининград; «Зеленый дом», Зеленый дом, Кофейня «Зелёный дом», ОКЦ, клуб «С тобой всё в порядке», кофейня «Зелёный дом» | mila_druzhinina_psy, s_toboi_vse_okei, signalkld, terkatalk | IDs `3488,3923,4115,4459,4460,5182,5682,5748,6381`; [#3488 2026-04-04 Встреча клуба «С тобой всё в порядке» «Где брать силы, когда нервы на пре…](https://t.me/signalkld/10228)<br>[#6381 2026-06-27 Игра «С тобой всё в порядке»](https://t.me/signalkld/11163) | Именованный клуб, собственный source и повторяющиеся встречи/игры сохраняют identity при смене темы и площадки.<br>Риски: same-day cross-source duplicate rows on 2026-06-13 | нет future-row |
| English Cafe | английский разговорный клуб | `confirmed` / high | 11/11/0; 11 дат | 2026-03-07…2026-07-04; observed gaps 7–42 days; median 7 days | Калининград; English Cafe, English cafe, Сигнал, Сигнал, Леонова 22, Калининград | okcng, signalkld | IDs `2798,3229,3385,3566,3949,4137,4385,5893,6137,6454,6572`; [#2798 2026-03-07 English Cafe: Обсуждение риска и опасности](https://t.me/signalkld/9887)<br>[#6572 2026-07-04 Personal Growth](https://t.me/signalkld/11218) | Серия разговорных встреч с устойчивым названием English Cafe, меняющимися темами и одним публичным source.<br>Риски: generic English events at other venues must not merge | нет future-row |
| Beer-Lingual Club | английский разговорный клуб | `confirmed` / high | 5/5/0; 5 дат | 2026-03-06…2026-06-12; observed gaps 14–49 days; median 21 days | Kaliningrad, Калининград; Mesto Sily Bar, mesto_sily_bar, Место Силы, Место Силы, Галицкого 18, Калининград | mesto_sily_bar, terkatalk | IDs `2809,3372,3773,4139,5853`; [#2809 2026-03-06 Beer-lingual Party](https://t.me/mesto_sily_bar/1680)<br>[#5853 2026-06-12 Beer Lingual Club](https://t.me/mesto_sily_bar/1829) | Стабильная брендированная разговорная встреча в Mesto Sily повторяется на разных датах и прямо называется club/party.<br>Риски: venue events are not club; include only Beer-Lingual identity | нет future-row |
| Клуб поющих друзей | совместное пение | `confirmed` / high | 3/3/0; 3 дат | 2026-03-22…2026-06-14; observed gaps 32–52 days; median 52 days | Калининград; Сигнал, Сигнал, Леонова 22, Калининград | signalkld | IDs `3003,4060,4061`; [#3003 2026-03-22 Клуб поющих друзей: вечер Булата Окуджавы](https://t.me/signalkld/9961)<br>[#4061 2026-04-23 Клуб поющих друзей: репетиции](https://t.me/signalkld/10487) | Явно именованный клуб имеет репетицию, тематический вечер и концерт на разных датах.<br>Риски: concert is output of the same club, not automatically a meeting | нет future-row |
| Клуб городских исследований | городские исследования | `confirmed` / high | 2/2/0; 2 дат | 2026-03-09…2026-03-26; observed gaps 17–17 days; median 17 days | Калининград; Горького, 116, Сигнал, Леонова 22, Калининград | signalkld | IDs `2808,3318`; [#2808 2026-03-09 Открытая встреча клуба Городских исследований](https://t.me/signalkld/9890)<br>[#3318 2026-03-26 Встреча Клуба городских исследований](https://t.me/signalkld/10097) | Две явные открытые встречи одного именованного клуба; смена площадки подтверждает, что identity не равна venue.<br>Риски: only two observed dates | нет future-row |
| Киноклуб Kinokloob | киноклуб | `confirmed` / high | 6/6/0; 6 дат | 2026-03-13…2026-07-16; observed gaps 1–84 days; median 12 days | Калининград; Бэкъярд, Велоателье, Сигнал, Сигнал, Леонова 22, Калининград | kinokloob, meowafisha, signalkld, terkatalk, westside_movieclub | IDs `2903,3033,5574,6095,6383,6776`; [#2903 2026-03-14 Малхолланд Драйв](https://t.me/signalkld/9934)<br>[#6776 2026-07-16 🎬 «Кино-пикник»](https://t.me/meowafisha/7942) | Собственный публичный source сопровождает разные показы/кинопикники на нескольких датах.<br>Риски: same source cross-posted yoga/music; excluded as non-club evidence | нет future-row |
| АвтоРетроКлуб Калининград | ретроавтомобили | `confirmed` / high | 6/5/1; 5 дат | 2026-03-28…2026-07-18; observed gaps 15–49 days; median 27 days | Гусев, Калининград, Полесск, Янтарный; АвтоРетроКлуб, Калининград Сити Джаз Клуб, Калининград Сити Джаз Клуб, Мира 33-35, Калининград, Полесск, Янтарный | festdir | IDs `3265,3814,3815,4876,5276,6853`; [#3265 2026-03-28 День открытых дверей в АвтоРетроКлубе](https://vk.com/wall-127107743_14519)<br>[#6853 2026-07-18 Выставка ретроавтомобилей на Дне города в Янтарном](https://vk.com/wall-127107743_14707) | Публичный автоклуб проводит открытые дни, сезонные встречи, пробег и выставки; название и единый public VK source устойчивы.<br>Риски: some events are partner/city programmes; same-day 3814/3815 likely duplicate/publication split; festival screening #834 excluded as venue leakage; excluded by scope: #6691 (lifecycle_not_active/silent) | past-only identity отсутствовала: 0/1 |
| Калининградский клуб кинолюбителей | киноклуб | `confirmed` / high | 4/4/0; 4 дат | 2025-09-10…2026-05-20; observed gaps 9–228 days; median 15 days | Калининград; Дом молодёжи, Остров Канта | festdir, meowafisha | IDs `600,4209,4235,4871`; [#600 2025-09-10 🎬 Показ фильма «Романс о влюблённых»](https://telegra.ph/Pokaz-filma-Romans-o-vlyublyonnyh-09-08)<br>[#4871 2026-05-20 Показ документального фильма «Мой папа — ГЕРОЙ!»](https://t.me/festdir/4327) | Именованный киноклуб провёл меняющиеся показы с обсуждением на нескольких площадках; source text прямо называет встречи клуба.<br>Риски: one event appears inside a larger island programme; count only the atomic club screening, not the whole schedule | нет future-row |
| Киноклуб «НЕсмотрел» | киноклуб | `confirmed` / high | 5/5/0; 5 дат | 2025-10-18…2026-03-21; observed gaps 7–63 days; median 56 days | Советск; Кофейня Теплосеть, ОКЦ ТеплоСеть, ОКЦ Теплосеть, ОЦК ТеплоСеть | public URLs only | IDs `1018,1744,2238,2931,3241`; [#1018 2025-10-18 🎬 Цвета времени](https://vk.com/wall-212233232_1466)<br>[#3241 2026-03-21 Частная жизнь](https://vk.com/wall-212233232_1691) | Точное название, единый публичный VK source и повторяющиеся показы с обсуждением сохраняют identity.<br>Риски: venue ТеплоСеть also hosts unrelated events | нет future-row |
| Литературный клуб «PROЧитано» | книжный клуб | `confirmed` / high | 2/2/0; 2 дат | 2025-11-20…2026-01-28; observed gaps 69–69 days; median 69 days | Калининград; Калининградская областная научная библиотека, Научная библиотека | public URLs only | IDs `1385,2378`; [#1385 2025-11-20 📚 Читаем «Сканеры» Роберта М. Зоннтага](https://vk.com/wall-30777579_14007)<br>[#2378 2026-01-28 📚 Литературный клуб PROЧитано: «Милый друг»](https://vk.com/wall-30777579_14477) | Две встречи одного явно названного библиотечного клуба обсуждают разные книги.<br>Риски: only two dates; unrelated “Прочитано” projects must not merge | нет future-row |
| Speaking Club в Bar Sovetov | разговорный языковой клуб | `confirmed` / high | 2/2/0; 2 дат | 2025-10-29…2026-04-01; observed gaps 154–154 days; median 154 days | Калининград; Бар Sovetov, Бар Советов, Мира 118, Калининград | public URLs only | IDs `1127,3510`; [#1127 2025-10-29 🗣️ Speaking club](https://vk.com/wall-223666016_357)<br>[#3510 2026-04-01 Speaking club в БАР SOVETOV](https://vk.com/wall-223666016_408) | Явно повторяющийся разговорный клуб с одинаковой площадкой/source и языковой практикой.<br>Риски: only two captured dates; do not merge with Beer-Lingual or English Cafe | нет future-row |
| Литературное сообщество «ПоэтиКа» | поэзия | `confirmed` / high | 3/3/0; 3 дат | 2025-09-07…2026-05-23; observed gaps 35–223 days; median 223 days | Калининград; Библиотека им. космонавта А. А. Леонова, Библиотека имени космонавта А. А. Леонова, У памятника Высоцкому | public URLs only | IDs `586,957,5143`; [#586 2025-09-07 🎤 Творческая встреча «ПоэтиКА опен эйр»](https://telegra.ph/Tvorcheskaya-vstrecha-PoehtiKA-open-ehjr-09-06)<br>[#5143 2026-05-23 📖 Встреча «Презентация альманаха «ПоэтиКа»»](https://vk.com/wall-32547811_10800) | Именованное литературное сообщество проводит собственные творческие встречи и презентацию альманаха.<br>Риски: generic common-noun “поэтика” is not an identity match | нет future-row |
| Клуб «Наше кино» | киноклуб | `confirmed` / high | 2/2/0; 2 дат | 2026-04-10…2026-05-08; observed gaps 28–28 days; median 28 days | Калининград, Черняховск; Библиотека, Библиротека им. Лунина, Калинина 4, Черняховск | public URLs only | IDs `3652,4548`; [#3652 2026-04-10 Наследница Ники](https://vk.com/wall-38920007_14434)<br>[#4548 2026-05-08 На войне как на войне](https://vk.com/wall-38920007_14564) | Два явных показа с обсуждением проходят под одним названием библиотечного клуба.<br>Риски: only two dates; generic Russian-film screenings excluded | нет future-row |
| СИНЕМАНГО | киноклуб | `confirmed` / high | 5/5/0; 5 дат | 2025-09-13…2026-03-28; observed gaps 1–91 days; median 77 days | Калининград; Калининградская областная научная библиотека, Научная библиотека, Научная библиотека, Мира 9, Калининград | public URLs only | IDs `577,1694,2170,2189,3393`; [#577 2025-09-13 🎬 СИНЕМАНГО: Путешествие по Азии - Станция "Сеул"](https://telegra.ph/SINEMANGO-Puteshestvie-po-Azii---Stanciya-Seul-09-05)<br>[#3393 2026-03-28 СИНЕМАНГО: Путешествие по Азии — «Маленький лес»](https://vk.com/wall-30777579_14867) | Пять встреч прямо описаны как встречи «клуба любителей азиатского кино»/киноклуба СИНЕМАНГО; фильмы меняются, а community identity и обсуждение сохраняются.<br>Риски: same library hosts unrelated screenings; match only explicit СИНЕМАНГО/club evidence | нет future-row |

## 5. Полный shortlist `probable`

| Кандидат | Интерес | Verdict/confidence | events total/past/future; dates | Период/cadence | Города; площадки | Sources/aliases | Event evidence | Rationale / confounders | Blind future recognition |
|---|---|---|---:|---|---|---|---|---|---|
| «Свои Чужие Стихи» | поэзия | `probable` / medium | 3/3/0; 3 дат | 2025-08-09…2026-05-17; observed gaps 64–217 days; median 217 days | Калининград; МЕСТО СИЛЫ, Место Силы | mesto_sily_bar | IDs `396,2978,4973`; [#396 2025-08-09 👨‍💼🍺 Свои Чужие Стихи: Лихие 90-е_](https://telegra.ph/Svoih-CHuzhih-Stihah-Lihie-90-e-08-05)<br>[#4973 2026-05-17 СВОИ ЧУЖИЕ СТИХИ: День семьи](https://t.me/mesto_sily_bar/1802) | Брендированная поэтическая встреча повторяется в одной публичной среде на трёх датах с меняющимися темами.<br>Риски: could be a recurring programme rather than membership community; no dedicated community source | нет future-row |
| Книжный клуб Gumbinnen | книжный клуб | `probable` / medium | 2/2/0; 2 дат | 2026-05-06…2026-05-14; observed gaps 8–8 days; median 8 days | Гусев; Дизайн-резиденция Gumbinnen | gumbinnen | IDs `4597,4807`; [#4597 2026-05-06 Книжный клуб в Gumbinnen](https://vk.com/wall-211015009_864)<br>[#4807 2026-05-14 Встреча книжного клуба: «Посторонний» Альбера Камю](https://vk.com/wall-211015009_870) | Две встречи одного книжного клуба в дизайн-резиденции с разными книгами и одним source.<br>Риски: only two dates; venue may own the programme | нет future-row |
| Книжный клуб психологической литературы | книжный клуб | `probable` / medium | 2/1/1; 2 дат | 2026-06-17…2026-07-29; observed gaps 42–42 days; median 42 days | Калининград; Сигнал | signalkld | IDs `5998,6854`; [#5998 2026-06-17 Книжный клуб психологической литературы](https://t.me/signalkld/11022)<br>[#6854 2026-07-29 Книжный клуб психологической литературы](https://t.me/signalkld/11297) | Одинаковое имя, venue и source; разные даты, одна встреча находится в будущем.<br>Риски: no discovery-period meeting before 2026-06-01 | past-only identity отсутствовала: 0/1 |
| Разговорный клуб испанского языка в Сигнале | испанский язык | `probable` / medium | 2/2/0; 2 дат | 2026-06-14…2026-07-12; observed gaps 28–28 days; median 28 days | Калининград; Сигнал | signalkld | IDs `5876,6765`; [#5876 2026-06-14 🇪🇸 Клуб разговорного испанского](https://vk.com/wall-211997788_3061)<br>[#6765 2026-07-12 Разговорный клуб испанского языка](https://t.me/signalkld/11265) | Две одинаково названные встречи в Сигнале с общим source.<br>Риски: only holdout dates; generic language clubs can false-merge | нет future-row |
| «Коллективное Сознательное» | психология и чтение | `probable` / medium | 2/2/0; 2 дат | 2026-06-09…2026-06-23; observed gaps 14–14 days; median 14 days | Калининград; ОКЦ на Горького | okcng, signalkld, terkatalk | IDs `5700,6148`; [#5700 2026-06-09 Коллективное Сознательное: Бегущая с волками](https://t.me/okcng/427)<br>[#6148 2026-06-23 Коллективное Сознательное: Возвращение к интуиции](https://t.me/okcng/524) | Брендированная серия из двух встреч с разными темами и общими sources.<br>Риски: may be programme series rather than stable membership community | нет future-row |
| QuizПросвет | интеллектуальные игры | `probable` / medium | 2/1/1; 2 дат | 2026-06-21…2026-07-20; observed gaps 29–29 days; median 29 days | Калининград; КЛДскоп, ОКЦ на Горького | QuizProsvet, kldscope_news, okcng, quizprosvet, signalkld | IDs `6206,6890`; [#6206 2026-06-21 Первая игра сообщества QuizПросвет](https://t.me/okcng/527)<br>[#6890 2026-07-20 Первая интеллектуальная битва QuizПросвет](https://t.me/kldscope_news/21) | Собственный source QuizProsvet связывает две публичные игры; вторая является будущим событием.<br>Риски: only two dates; title “первая” conflicts across rows | past-only identity отсутствовала: 0/1 |
| Клуб знакомств «Через…» / FaceToFace | знакомства через совместные занятия | `probable` / medium | 2/2/0; 2 дат | 2026-06-19…2026-06-26; observed gaps 7–7 days; median 7 days | Калининград; ОКЦ на Горького | facetofaceclub, okcng | IDs `6166,6299`; [#6166 2026-06-19 Открытая встреча по формированию команды Клуба знакомств через...](https://t.me/okcng/525)<br>[#6299 2026-06-26 Клуб знакомств через...](https://t.me/okcng/581) | Формирование команды и первая публичная встреча связаны отдельным source FaceToFace.<br>Риски: one row is organisational formation, not regular meeting; no third date | нет future-row |
| «Эмоциональное чтение с психологом» | чтение и психология | `probable` / medium | 2/2/0; 2 дат | 2025-12-12…2026-05-22; observed gaps 161–161 days; median 161 days | Калининград; Научная библиотека | public URLs only | IDs `1665,5176`; [#1665 2025-12-12 📚 Эмоциональное чтение с психологом](https://vk.com/wall-30777579_14145)<br>[#5176 2026-05-22 📖 Встреча клуба «Эмоциональное чтение с психологом»](https://vk.com/wall-30777579_15254) | Две одноимённые встречи на разных датах в библиотеке.<br>Риски: large five-month gap; could be repeat programme rather than community | нет future-row |
| Семейный книжный клуб Дома семьи | семейное чтение | `probable` / medium | 2/2/0; 2 дат | 2025-09-25…2025-10-23; observed gaps 28–28 days; median 28 days | Калининград; Дом Семьи, Дом семьи | public URLs only | IDs `755,1074`; [#755 2025-09-25 📚 Встреча Семейного книжного клуба: «Что такое буллинг? Как с ним боротьс…](https://vk.com/wall-228087066_514)<br>[#1074 2025-10-23 📚 Семейный книжный клуб](https://vk.com/wall-228087066_556) | Две явные книжные встречи на разных датах в Доме семьи.<br>Риски: same venue runs many unrelated programmes; no independent club source | нет future-row |
| Калининградский фотоклуб | фотография | `probable` / medium | 2/2/0; 2 дат | 2025-09-07…2026-01-04; observed gaps 119–119 days; median 119 days | Калининград; Барн, Сигнал | public URLs only | IDs `443,1946`; [#443 2025-09-07 📷 Встреча Калининградского фотоклуба](https://telegra.ph/Vstrecha-Kaliningradskogo-fotokluba-08-16)<br>[#1946 2026-01-04 📸 Встреча Калининградского фотоклуба](https://vk.com/wall-211997788_2676) | Две явно названные встречи на разных датах и площадках.<br>Риски: old rows have weak/legacy provenance; no dedicated source alias captured | нет future-row |
| Death Cafe Kaliningrad | разговоры о смерти и жизни | `probable` / medium | 2/2/0; 2 дат | 2026-03-21…2026-06-17; observed gaps 88–88 days; median 88 days | Калининград; Сигнал, Сигнал, Леонова 22, Калининград | signalkld | IDs `3220,6009`; [#3220 2026-03-21 Death Cafe](https://t.me/signalkld/10034)<br>[#6009 2026-06-17 Death Cafe](https://t.me/signalkld/11039) | Две встречи одного формата в Сигнале на разных датах.<br>Риски: Death Cafe can be a licensed/open format rather than stable local community | нет future-row |
| Konig Boombap Tape Club | хип-хоп и музыка | `probable` / medium | 2/2/0; 2 дат | 2026-03-27…2026-05-30; observed gaps 64–64 days; median 64 days | Калининград; Old School Bar, Форма пицца-бар | kbbtclub, meowafisha | IDs `3384,5335`; [#3384 2026-03-27 Konig Boombap Tape Club](https://t.me/meowafisha/7008)<br>[#5335 2026-05-30 Твердый Мелл и Mad Stoopa](https://t.me/meowafisha/7468) | Собственный source kbbtclub связывает два музыкальных события.<br>Риски: two events only; could be promoter/label rather than meeting club | нет future-row |
| Клуб «Просто поэты» | поэзия | `probable` / medium | 2/2/0; 2 дат | 2025-11-26…2026-05-10; observed gaps 165–165 days; median 165 days | Калининград; Бар Бастион, Библиотека Чехова | public URLs only | IDs `1450,4746`; [#1450 2025-11-26 🎤 Рифмоток: День рождения клуба](https://vk.com/wall-149955604_20671)<br>[#4746 2026-05-10 Концерт «Сквозь пожары к Победному маю»](https://vk.com/wall-32547811_10762) | Именованное сообщество проводит клубную встречу и собственную публичную поэтическую программу.<br>Риски: second event is community-produced output rather than a regular meeting | нет future-row |
| Клуб достаточно хороших родителей | родительство | `probable` / medium | 2/2/0; 2 дат | 2025-10-10…2026-06-04; observed gaps 237–237 days; median 237 days | Калининград; Городское пространство для молодых семей, Дом семьи | molod_kld | IDs `881,5219`; [#881 2025-10-10 Клуб достаточно хороших родителей с Милой Дружининой](https://vk.com/wall-231920894_535)<br>[#5219 2026-06-04 Встреча для родителей о травле среди школьников](https://t.me/molod_kld/3675) | Две source-grounded психологические встречи одного явно названного клуба прошли на разных датах.<br>Риски: no dedicated club source; host/venue changed | нет future-row |

## 6. `needs_evidence`
Эти кандидаты fail closed и не входят в accepted counts.


| Кандидат | Интерес | Verdict/confidence | events total/past/future; dates | Период/cadence | Города; площадки | Sources/aliases | Event evidence | Rationale / confounders | Blind future recognition |
|---|---|---|---:|---|---|---|---|---|---|
| Клубы общения на английском при библиотеке Чехова | английский язык | `needs_evidence` / low | 4/4/0; 4 дат | 2025-11-05…2025-12-07; observed gaps 1–28 days; median 3 days | Калининград; Библиотека А. П. Чехова, Библиотека им. А.П. Чехова | public URLs only | IDs `1196,1539,1540,1541`; [#1196 2025-11-05 🗣️ Клуб общения на английском языке](https://vk.com/wall-32547811_9964)<br>[#1541 2025-12-07 🗣️ Клуб общения на английском языке для детей (10-13 лет)](https://vk.com/wall-32547811_10113) | Повторяется общий ярлык клуба.<br>Риски: A1/B1/детская аудитория могут быть разными clubs; one monthly source packet | нет future-row |
| Клуб путешественников / «Вокруг света» ММО | путешествия | `needs_evidence` / low | 0/0/0; 0 дат | нет atomic primary-scope dates | —; — | public URLs only | нет eligible primary-scope rows | Название повторяется в месячных музейных программах.<br>Риски: date ranges are non-atomic; one row may encode many slots; venue-program leakage; excluded by scope: #1599 (date_not_atomic),#1601 (date_not_atomic),#2471 (date_not_atomic) | нет future-row |
| Клуб «Плетуны» | рукоделие | `needs_evidence` / low | 1/1/0; 1 дат | 2026-03-11…2026-03-11; insufficient atomic dates | Советск; ОЦК ТеплоСеть | public URLs only | IDs `2929`; [#2929 2026-03-11 🧶 Встреча — Встреча клуба "Плетуны"](https://vk.com/wall-212233232_1680) | Явное имя клуба присутствует.<br>Риски: only one observed meeting | нет future-row |
| Писательский клуб «Сад слов» | писательская практика | `needs_evidence` / low | 1/1/0; 1 дат | 2026-05-21…2026-05-21; insufficient atomic dates | Калининград; Сигнал | signalkld | IDs `5054`; [#5054 2026-05-21 Писательский клуб "Сад слов"](https://t.me/signalkld/10709) | Явное имя клуба присутствует.<br>Риски: only one observed meeting | нет future-row |
| Литературный клуб «Поэты 39» | поэзия | `needs_evidence` / low | 1/1/0; 1 дат | 2026-05-15…2026-05-15; insufficient atomic dates | Калининград; Историко-художественный музей | koihm | IDs `4722`; [#4722 2026-05-15 Открытый микрофон Литературного клуба Поэты 39](https://t.me/koihm/5559) | Публичный клуб назван в событии.<br>Риски: only one explicitly attributable club meeting in catalog | нет future-row |
| Клуб «Город женщин» | книги и фильмы | `needs_evidence` / low | 1/1/0; 1 дат | 2026-03-28…2026-03-28; insufficient atomic dates | Калининград; Арт-пространство «Сигнал» | signalkld | IDs `3335`; [#3335 2026-03-28 Клуб «Город женщин»: встреча о книгах и фильмах](https://t.me/signalkld/10123) | Публичный клуб назван в событии.<br>Риски: only one attributable meeting; later generic event not safely linkable | нет future-row |
| Клуб EEE | искусство и среда | `needs_evidence` / low | 1/1/0; 1 дат | 2026-06-22…2026-06-22; insufficient atomic dates | Калининград; Сигнал | club_eee, kulturnaya_chaika, signalkld, terkatalk | IDs `6226`; [#6226 2026-06-22 Что питает искусство? Диалог о среде и художественной жизни](https://t.me/signalkld/11102) | Dedicated source club_eee присутствует.<br>Риски: one event only in catalog | нет future-row |
| Летний клуб «СветлоУмка» | детское развитие | `needs_evidence` / low | 1/1/0; 1 дат | 2026-05-21…2026-05-21; insufficient atomic dates | Светлогорск; Телеграф | public URLs only | IDs `5196`; [#5196 2026-05-21 Летний клуб «СветлоУмка»](https://vk.com/wall-171050617_1752) | Явное название летнего клуба.<br>Риски: could be fixed-term camp/programme; one event only | нет future-row |

## 7. Rejected clusters и confounders
Эти строки — проверенные confounder-классы, а не клубы.


| Кандидат | Интерес | Verdict/confidence | events total/past/future; dates | Период/cadence | Города; площадки | Sources/aliases | Event evidence | Rationale / confounders | Blind future recognition |
|---|---|---|---:|---|---|---|---|---|---|
| Шоу «Клуб знакомств» | комедийное шоу | `rejected` / low | 13/12/1; 10 дат | 2026-02-17…2026-07-19; observed gaps 1–29 days; median 16 days | Калининград; Винный факультет, Стендап клуб Локация, Топ-3 факта, которые поднимут настроение:, Форма пицца-бар, Гаражная 2б, Калининград | locostandup, terkatalk | IDs `2438,3040,3109,3111,3231,3490,3520,3957,4509,4575,5233,6078,6866`; [#2438 2026-02-17 🎭 Клуб знакомств](https://vk.com/wall-214027639_10579)<br>[#6866 2026-07-19 Клуб знакомств](https://t.me/locostandup/3670) | Повторяется коммерческий сценический формат, а не identity сообщества участников.<br>Риски: commercial-series false positive | past-only identity отсутствовала: 0/1 |
| «Стендап клуб Локация» как venue | стендап | `rejected` / low | 10/8/2; 9 дат | 2026-04-09…2026-07-24; observed gaps 2–28 days; median 13 days | Калининград; Калининградский Стендап Клуб, Стендап клуб Локация | locostandup | IDs `3619,4574,4850,4854,5445,5487,5858,6670,6731,6856`; [#3619 2026-04-09 Калининградский Стендап Клуб](https://vk.com/wall-78172842_7342)<br>[#6856 2026-07-24 Алексей Полубояров](https://vk.com/wall-214027639_11589) | Слово club является частью названия площадки/продюсера; события — разные концерты.<br>Риски: venue leakage | past-only identity отсутствовала: 0/2 |
| YALTA / Вагонка / Склад / Universal как club venues | ночная музыка | `rejected` / low | 13/12/1; 11 дат | 2025-09-27…2026-07-25; observed gaps 7–122 days; median 25 days | Калининград; UNIVERSAL, YALTA, Yalta Club, Вагонка (клуб), Вагонка (клуб), Станочная 12, Калининград, Заря, Клуб "iO", Клуб "Склад", СКЛАD, Универсал (пространство), Мира 41-43, Калининград | kulturnaya_chaika, meowafisha, zaryakinoteatr | IDs `290,637,2773,2813,3079,3095,3117,3245,3434,4055,4587,4957,5836`; [#290 2025-09-27 🎸 ROCK PRIVET в клубе 'YALTA'](https://telegra.ph/ROCK-PRIVET-v-klube-YALTA-07-27)<br>[#5836 2026-06-26 Drum'N'Bass](https://kaliningrad.qtickets.events/239308-drumnbass-for-mass-06-26) | Club обозначает ночную площадку, а не клуб по интересам.<br>Риски: venue/nightlife leakage; excluded by scope: #5239 (lifecycle_not_active/silent) | past-only identity отсутствовала: 0/1 |
| Музейный лекторий/кинолекторий | просветительская программа | `rejected` / low | 2/2/0; 2 дат | 2025-11-05…2025-11-11; observed gaps 6–6 days; median 6 days | Гусев; Гусевский музей | public URLs only | IDs `1194,1195`; [#1194 2025-11-05 📚 Первый штурм Гумбиннена. Октябрь 1944-го](https://vk.com/wall-168966993_19336)<br>[#1195 2025-11-11 📚 В Компьенском лесу](https://vk.com/wall-168966993_19336) | Это программная линия учреждения/месячный schedule, а не доказанная club identity.<br>Риски: venue-programme leakage; range-date leakage; excluded by scope: #1602 (date_not_atomic),#2469 (date_not_atomic),#2472 (date_not_atomic) | нет future-row |
| Одна программа в нескольких датах | театры/показы | `rejected` / low | 11/11/0; 10 дат | 2025-08-07…2026-05-16; observed gaps 1–246 days; median 6 days | Зеленоградск, Калининград, Некрасово; Остановка "Рыбная деревня", Остров Канта, Театральная гостиная Солёная ворона, замок Шаакен | public URLs only | IDs `89,90,147,148,174,175,4205,4206,4207,4208,4210`; [#89 2025-08-08 🎙🎭🤘 Та самая рок-опера «Моцарт»](https://t.me/kenigevents/869)<br>[#4210 2026-05-16 Кинопоказ калининградских фильмов на Острове Канта](https://vk.com/wall-182104060_6924) | Повторы одной программы/слота относятся к linked occurrences, не к club identity.<br>Риски: linked-occurrence leakage | нет future-row |
| Фестиваль и его программа | фестивали | `rejected` / low | 2/2/0; 2 дат | 2026-06-26…2026-07-12; observed gaps 16–16 days; median 16 days | Калининград; Железнодорожные ворота, Закхаймские ворота | terkatalk | IDs `6314,6742`; [#6314 2026-06-26 🥳 Детский фестиваль «Матушка-земля»](https://vk.com/wall-190663987_8934)<br>[#6742 2026-07-12 однодневный фестиваль фотографических сообществ Калининграда](https://t.me/terkatalk/5122) | Festival identity и programme rows не являются клубом.<br>Риски: festival leakage; corrupt/future duplicate date example; excluded by scope: #107 (lifecycle_not_active/silent/date_not_atomic),#6743 (identity_not_canonical/merged_into_event/lifecycle_not_active/silent) | нет future-row |
| Все события арт-пространства «Сигнал» | разные интересы | `rejected` / low | 10/10/0; 10 дат | 2026-03-08…2026-04-18; observed gaps 1–17 days; median 3 days | Калининград; «Сигнал» (мастерская), Сигнал, Леонова 22, Калининград, пространство Сигнал | gamevibes_kld, meowafisha, signalkld, terkatalk, westside_movieclub | IDs `2739,2808,2951,3003,3110,3220,3327,3385,3516,3903`; [#2739 2026-03-08 Эверделл](https://vk.com/wall-211997788_2815)<br>[#3903 2026-04-18 Проект «Аве Мария»](https://t.me/signalkld/10419) | Одна площадка содержит множество независимых communities; venue cannot be club identity.<br>Риски: venue leakage; false merge of multiple real clubs | нет future-row |
| Все события «Дома семьи» | семейные программы | `rejected` / low | 19/19/0; 17 дат | 2025-08-09…2025-12-20; observed gaps 1–47 days; median 3 days | Калининград; Дом Семьи, Дом семьи | public URLs only | IDs `385,409,495,706,754,756,757,835,837,870,882,883,911,1073,1075,1739,1740,1741…`; [#385 2025-08-09 🗣️ Активное слушание. Практика общения](https://telegra.ph/Aktivnoe-slushanie-Praktika-obshcheniya-08-04)<br>[#1742 2025-12-20 👂 Практика общения: Активное слушание](https://vk.com/wall-228087066_591) | Один organizer/venue выпускает разные занятия; объединение их в один club — false merge.<br>Риски: organizer/venue leakage | нет future-row |
| «Клубника» как lexical club match | агротуризм | `rejected` / low | 9/9/0; 7 дат | 2026-05-17…2026-07-05; observed gaps 4–15 days; median 7 days | Гурьевск, Калининград, Некрасово, Светлогорск; АгроПарк «Некрасово поле», АгроПарк Некрасово поле, Агропарк «Некрасово Поле», Козья горка, ферма «Козья горка» | agropark39, kozia_gorka | IDs `4907,5044,5059,5499,5627,6253,6439,6486,6516`; [#4907 2026-05-30 Праздник клубники на ферме «Козья горка»](https://t.me/kozia_gorka/1425)<br>[#6516 2026-06-30 Последний день самосбора клубники](https://t.me/agropark39/1888) | Русская лексема «клубника» создаёт ложное совпадение с «клуб».<br>Риски: keyword/substring false positive | нет future-row |
| Случайные серии мастер-классов одной тематики | мастер-классы | `rejected` / low | 7/7/0; 6 дат | 2025-10-01…2026-05-30; observed gaps 1–196 days; median 7 days | Калининград; Дом семьи, Историко-художественный музей, Понарт, Студия Хара Хура | garazhka_kld | IDs `625,626,911,1219,1220,5179,5389`; [#625 2025-10-01 🎨 Мастер-класс «Калининградский пейзаж»](https://telegra.ph/Master-klass-Kaliningradskij-pejzazh-09-11)<br>[#5389 2026-05-30 Кружка с авторским рисунком](https://t.me/garazhka_kld/1402) | Тематика/venue повторяются без именованной community identity.<br>Риски: topic similarity; organizer leakage | нет future-row |

## 8. Обязательные агрегаты

### Verdict, события, recurrence

| Метрика | Значение | Denominator / смысл |
|---|---:|---|
| Confirmed | 20 | из 52 reviewed candidate clusters |
| Probable | 14 | из 52 |
| Needs evidence | 8 | из 52 |
| Rejected | 10 | из 52 |
| Confirmed + probable | 34 | publishable не автоматически; owner review обязателен |
| Unique accepted canonical events | 203 | union event ids; 198 past, 5 future |
| Accepted с ≥2 distinct dates | 34 | из 34 |
| Accepted с ≥3 distinct dates | 17 | из 34 |
| Accepted с ≥5 distinct dates | 13 | из 34 |
| Candidates, зависящие от одного слабого evidence | 15 | из 52; probable/needs, ≤1 alias и ≤2 dates |

Самые крупные тематические сегменты: шесть киноклубов, три книжных клуба, три поэтических сообщества и три разговорных языковых формата. Остальные identities распределены по настольным играм, шахматам, ИИ, электронике, отношениям, психологии, городским исследованиям, ретроавтомобилям, фотографии и интеллектуальным играм. 32/34 accepted identities имеют хотя бы один event city `Калининград`; также представлены Советск, Черняховск, Гусев, Зеленоградск, Полесск, Янтарный и Медведевка. Нормализация city требует отдельной работы (`Kaliningrad` и settlement/venue drift присутствуют).

Свежесть:

- 4 identities имеют future event;
- 14 без будущей встречи, но наблюдались с 1 июня;
- 16 последний раз наблюдались до 1 июня и не должны автоматически попадать в «актуальные» без stale policy.

### Leakage и ошибки

- **Same-date leakage proxy:** 18 candidate/date groups, 20 surplus event memberships. Это смесь surviving cross-source duplicates и допустимых same-day sessions; до подсчёта meetings нужно adjudicate/collapse.
- **Linked-occurrence leakage proxy:** 40/204 accepted memberships имеют непустой `linked_event_ids`. Сам факт link не отменяет club membership, но linked siblings нельзя считать независимыми доказательствами клуба.
- **Festival leakage proxy:** 11/204 accepted memberships имеют festival text. Они остаются только там, где независимая club/source identity доказана; festival identity не переносится на club.
- **False merge:** 10 rejected candidate clusters представляют измеренный candidate-pool count, не population rate. Классы: venue, organizer/source, commercial show, nightclub, festival, museum programme, linked occurrence, generic topic и lexical substring.
- **False split:** честный population rate невозможен без независимо полного gold denominator. В ledger сохранены alias reconciliations для Westside, Game Vibes, Rep Chess, `С тобой всё в порядке`, AutoRetroClub и других identities.

## 9. Split-safe time-split proxy

Первоначальный post-hoc показатель `3/5` был отклонён closure-review как leakage-prone и удалён. Вместо него отдельный агент получил **только** discovery export до `2026-06-01`: 4,667 eligible rows, uncompressed JSONL SHA-256 `e5cb7f481b035623c006777e39e1de29dea8fb3750d2eaa08ca310f5f993fb16`. Он не видел holdout, future, ledger или report и заморозил partial policy из 29 hypotheses. Policy SHA-256: `68aff7de5347a0c3632e5c11c5b9691bb8fd602ba792ce19eb2be5558d6dcef2`.

После freeze root применил неизменные exact name/source anchors ко всем 1,658 eligible post-cutoff rows (1,400 elapsed holdout + 258 future), затем сравнил с финальным researcher-adjudicated ledger:

- exact frozen-name recognition: **24/56 (42.9%)** accepted post-cutoff events;
- exact name **или** source-anchor candidate retrieval: **36/56 (64.3%)**;
- future exact-name recognition: **0/5**;
- future name/source candidate retrieval: **2/5 (40%)**, оба — Game Vibes `#6835` и `#6929`;
- AutoRetroClub `#6853` отсутствовал в blind past-only hypotheses; psychology book `#6854` и QuizПросвет `#6890` появились только после discovery cutoff.

Source lane намеренно не является verdict: generic venue/library anchors дали 226 rows и 66 multi-identity ambiguities. Exact-name lane дал 26 rows: 24 совпали с accepted identities, два остались вне accepted ledger после source review (`Поэты39` open mic как выступление организатора и EEE-упоминание вне доказанной club identity); неправильных identity matches среди accepted rows — 0.

Это **не population precision/recall**: post-cutoff labels размечены тем же исследовательским процессом, sampling frame не owner-approved и freeze был остановлен как partial. Метрики честно называются coverage proxies. Они показывают, что exact anchors устойчивы, но недостаточны для новых/переименованных встреч, а source-only recall слишком загрязнён для автоматического merge.

## 10. Сравнение baseline и BGE/Kaggle CPU

| Lane | Фактический результат | Ограничение | Решение |
|---|---|---|---|
| Frozen source/name policy | blind discovery-only freeze; exact-name 24/56, name/source candidate 36/56; future 0/5 strict и 2/5 candidate | partial policy; aliases и новые clubs; generic source anchors дают 66 ambiguities | оставить identity-anchor baseline, но не semantic verdict |
| Existing `gemini-embedding-2 related_v1` | live sidecar: 298 vectors; accepted-ledger coverage 5/203 (**2.5%**) | zero covered same-club peer, поэтому recall@K математически не определён; sidecar prunes history | нельзя использовать как historical comparator без frozen re-embed |
| BGE-M3 on Kaggle CPU | repository feasibility уже доказана внешними BGE canaries: 12 events/191.2 s, 15 events/162.1 s; model revision pinned | в этом аудите **нет interest-club quality run**, peak RSS и catalog throughput не измерены на frozen corpus | GO только на отдельный measured benchmark; NO-GO architecture |
| Evidence-grounded semantic adjudication | 52 source-grounded clusters просмотрены исследовательским агентом, ordinal fail-closed verdict сохранён | это не отдельный калиброванный production-LLM run; owner/human gold ещё не подписан | owner-approved fixture и LLM-first adjudication нужны до автоматизации |

Existing vector baseline нельзя объявлять «хуже»: его историческое покрытие недостаточно для сравнения. У 5 покрытых accepted events не было ни одной пары same-club peer, поэтому `recall@5/10/20/40` имеет denominator 0. Future Game Vibes/AutoRetro vectors также не имели retained discovery member в sidecar. Это storage/scope gap, не quality verdict модели.

Единственный обоснованный BGE candidate: [`BAAI/bge-m3`](https://huggingface.co/BAAI/bge-m3) на revision `5617a9f61b028005a4858fdac845db406aefb181`, dense-only 1024d, FP32, `FlagEmbedding==1.4.0`. Официальный model repo около 4.59 GB, PyTorch weights около 2.27 GB; raw vector — 4 KiB/event, на 33.3% больше Gemini 768d. Репозиторий уже имеет CPU Kaggle bootstrap/status/checkpoint pattern, но age-assessment vectors не переиспользуются: другой text contract и задача.

### Стенд известного клуба: Gemma 4 31B → Gemini 3.1 Flash-Lite

После owner feedback СИНЕМАНГО исправлен с `rejected` на `confirmed`: пять исходных постов прямо называют его киноклубом/клубом любителей азиатского кино. Все counts и fixture выше пересчитаны.

Первый direct-REST smoke на 8 cases действительно был слишком мал и обходил gateway limiter. Поэтому выполнен новый controlled stand на **48 реальных event/candidate pairs**: 24 accepted memberships из reviewed ledger и 24 researcher-adjudicated hard negatives (другая identity на той же площадке/источнике, похожее имя, venue leakage, упоминание, festival/program child). Это ещё не owner-approved gold.

Попытка решать все случаи одним универсальным prompt дала false positives. Финальный безопасный contract поэтому разбит на три очень коротких lane:

```text
source=yes уже доказан. v=no ТОЛЬКО если title — отдельный пункт общей программы (артист/йога и т.п.) или чужой crosspost; иначе v=yes. q=дословно 3–8 слов INPUT.
```

```text
name=yes,source=no. v=yes только если CLUB явно организует title. CLUB лишь venue/инициатор/упоминание/список активностей → no. Сомнение=unclear. q=дословно 3–8 слов INPUT.
```

Если нет ни canonical-source, ни curated-name/alias match, relation fail-closes в `no` **без LLM-вызова**. Это не широкая keyword-классификация смысла: deterministic слой только отсекает candidate без identity evidence, а спорную organizer/program semantics решает LLM. URL targets удаляются из bounded packet, остальной текст не переписывается. Native schema остаётся `v + q`; `thinking=minimal`, `temperature=0`, `maxOutputTokens=96`.

Финальный Gemma run:

| Метрика | Результат |
|---|---:|
| Frozen cases | 48 = 24 positive + 24 hard negative |
| Deterministic no-match skips | 17 |
| Gemma provider calls | 31/31 succeeded |
| Accepted positives (`yes` + verbatim packet substring) | **22/24 = 91.7%** |
| Unsafe false positives | **0/24** |
| Safe relation decisions overall | **46/48 = 95.8%** |
| Quote validation failures | **0** |
| Provider latency p50 / p95 / max | **1.682 / 2.242 / 5.586 s** |
| Supabase NO_WAIT blocks handled outside client | 1 |

Два false negatives — `АвтоРетроКлуб #6853` и `relation_club #3463`. Для `#6853` fail-closed результат объясним: source в основном рассказывает о прошедшем выезде в Светлый и лишь тизерит следующую дату в Янтарном. Для `#3463` название конкретной встречи не несёт identity клуба достаточно явно. Автоматического relation для них пока быть не должно; они идут в review/deferred, а не исправляются расширением prompt ценой false positives.

На 12 самых рискованных cases сделаны ещё два Gemma повтора (24 calls): **12/12 cases дали одинаковый, правильный и grounded verdict во всех трёх наблюдениях**. Итого именно финальный acceptance run прошёл через `GoogleAIClient` с `55/55` успешными `google_ai_reserve`, `google_ai_mark_sent` и `google_ai_finalize`; reserve/local/model fallback были выключены, key lane был строго `GOOGLE_API_KEY`. После перехода на controlled runner весь накопленный stand ledger за день содержит 549 succeeded Gemma requests и 131 succeeded Lite requests; пять Lite rows завершены как `failed_provider`, незавершённых `sent` rows после cleanup нет.

Для Lite сохранён полноценный earlier controlled shadow на 48 cases: `48/48` provider success, но только `39/48` correct+grounded, с **4 false positives**, 3 false negatives и 3 quote failures. Повтор final-v7 shadow не форсировался: default Lite lane достиг Supabase `RPD=450/450`, и limiter остановил новые calls; overflow/bypass не включался. Этого уже достаточно для NO-GO на positive fallback.

**Вывод:** `gemma-4-31b-it` можно брать primary только в таком split-lane, evidence-bound и fail-closed verifier. Даже Gemma не создаёт relation при `no/unclear` или invalid quote. `gemini-3.1-flash-lite` не подтверждает positive relation: provider failure → `deferred`/повтор Gemma, а Lite допустим только как отрицательный/review probe. Это сохраняет regression contract `INC-2026-05-05-smart-update-gemma3-fallback-hallucination`: никакого широкого writer fallback.

Tracked label manifest: `tests/fixtures/interest_clubs_known_match_eval_v1.json`. Raw runner/results: `artifacts/codex/interest-clubs-audit-20260717/benchmarks/` (не коммитятся).

Честный следующий benchmark обязан:

1. подать один и тот же frozen `club_retrieval_doc_v1` в Gemini и BGE;
2. зафиксировать hashes, model revision, runtime image, package versions, max length и batch;
3. считать recall@K/purity/false merge/split на owner-approved fixture;
4. отдельно измерить cold/warm wall time, peak RSS, model bytes, vector/index bytes и incremental changed-only run;
5. не строить club identity через unreviewed connected components;
6. завершать решение source-grounded LLM adjudication.

## 11. Product и technology решения до реализации

Owner должен явно утвердить:

1. **Public threshold:** только `confirmed`, либо часть `probable`; минимум две или три distinct dates; нужна ли future meeting.
2. **Freshness:** скрывать после 30/60/90 дней без новой встречи или сохранять архив.
3. **Identity ownership:** кто принимает merge/rename/split и управляет стабильным slug.
4. **Co-hosting:** может ли один event принадлежать двум clubs (пример совместных киноклубных показов).
5. **Meetings vs activity:** считать ли концерт/выставку, созданные клубом, его event evidence или только собственно встречи.
6. **Children/closed groups:** какой public-source evidence достаточен; никаких participant lists и sensitive audience inference.
7. **Retrieval architecture:** deterministic-only, Gemini, BGE, union или neither — только после paired benchmark.
8. **Storage budget:** компактная identity/projection + canonical event ids; raw source dumps и all-pairs matrices не хранить в Supabase.

Рекомендуемый минимальный public policy для следующего shadow этапа: `confirmed`, ≥3 distinct atomic dates **или** ≥2 dates + dedicated public community source, active/future within 90 days, zero unresolved venue/festival/linked leakage, owner-approved identity. Это proposal, не принятое product решение.

## 12. Следующий этап

### Предлагаемый R1-shadow slice

- owner review 20 confirmed + 14 probable;
- исправить/подтвердить 8 needs-evidence identities;
- превратить `interest_clubs_review_fixture_v1` в независимый owner-approved split-safe gold с hard negatives;
- frozen paired Gemini/BGE-M3 Kaggle CPU run через существующий unique run id/status/heartbeat contract;
- blind LLM adjudication одинаковых candidate packets;
- ADR: identity owner, thresholds, freshness, merge/split, storage/retention и failure/rollback;
- только после GO — отдельные current-main-based implementation PRs.

Не выполнять в этой ветке без нового owner решения: DB migrations/writes, `/kluby-po-interesam/`, scheduler, Smart Update integration, personalization, mass backfill или публикацию.

## 13. Артефакты и команды проверки

Machine-readable, не коммитятся:

- `raw/prod-schema.json`, `raw/prod-catalog-metadata.json`, `raw/prod-events.jsonl.gz`;
- `analysis/dataset_profile.json`, `candidate_ledger.json{,l}`, `candidate_ledger.csv`, `aggregates.json`, `quality_metrics.json`;
- `analysis/past_only_frozen_policy.json`, `past_only_holdout_evaluation.json`;
- `benchmarks/existing_gemini_vector_evaluation.json`, `existing_gemini_vector_top10.json`;
- `benchmarks/run_known_club_llm_stand.py`, `known-club-llm-stand-20260717.json`;
- `lanes/R0-repo-data-map.md`, `lanes/R3-past-only-freeze.md`, `lanes/R4-bge-feasibility.md`, `lanes/R5-closure-review.md`.

Коммитится минимальный safe review fixture: `tests/fixtures/interest_clubs_review_fixture_v1.json`. Он содержит только bounded public event facts/URLs и **не** является independent/owner-approved gold или production dump.

Воспроизведение локальных агрегатов:

```bash
python3 artifacts/codex/interest-clubs-audit-20260717/analysis/build_recall_catalog.py
python3 artifacts/codex/interest-clubs-audit-20260717/analysis/build_candidate_ledger.py
python3 artifacts/codex/interest-clubs-audit-20260717/analysis/summarize_quality_metrics.py
python3 artifacts/codex/interest-clubs-audit-20260717/analysis/evaluate_existing_vectors.py
python3 artifacts/codex/interest-clubs-audit-20260717/analysis/evaluate_past_only_policy.py
python3 -m json.tool tests/fixtures/interest_clubs_review_fixture_v1.json >/dev/null
git diff --check
```

## 14. Источники

- Canonical feature contract: `docs/backlog/features/interest-clubs/README.md` и `research-prompt.md`.
- Linked occurrences: `docs/features/linked-events/README.md`.
- Festival semantics: `docs/features/festivals/README.md` (путь `static-site-release.md` из prompt устарел и отсутствует на base SHA).
- Smart Update / LLM-first: `docs/features/smart-event-update/README.md`, `docs/llm/request-guide.md`.
- Google AI: [Gemma 4 через Gemini API](https://ai.google.dev/gemma/docs/core/gemma_on_gemini_api), [Gemini 3.1 Flash-Lite](https://ai.google.dev/gemini-api/docs/models/gemini-3.1-flash-lite), [structured outputs](https://ai.google.dev/gemini-api/docs/structured-output).
- Vector sidecar: `docs/features/unsigned-personalization/authorized-event-search.md`, `semantic-vector-retrieval.md`, `scripts/inspect/audit_future_event_vectors.py`.
- BGE official: [model card](https://huggingface.co/BAAI/bge-m3), [pinned files](https://huggingface.co/BAAI/bge-m3/tree/5617a9f61b028005a4858fdac845db406aefb181), [FlagEmbedding v1.4.0](https://github.com/FlagOpen/FlagEmbedding/releases/tag/v1.4.0).
