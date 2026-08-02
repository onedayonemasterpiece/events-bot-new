# Промпты для дальнейшей работы в ChatGPT и Gemini

Промпты рассчитаны на агента с доступом к репозиторию. Перед началом агент обязан прочитать файлы, а не опираться на пересказ из сообщения.

## 1. Общий контекст для любого запроса

```text
Репозиторий: onedayonemasterpiece/events-bot-new

Канонический пакет:
- docs/features/editorial-collections/README.md
- docs/features/editorial-collections/temporal-editions.md
- docs/features/editorial-collections/pilot-unusual-events.md
- docs/features/editorial-collections/editorial-content-unusual.md
- docs/features/editorial-collections/mockup-lab.md
- docs/features/editorial-collections/prompts.md
- docs/features/editorial-collections/tasks/README.md

Числовой evidence временных окон:
- docs/features/editorial-collections/evidence/temporal-window-analysis-2026-08-02.json

Runtime fixture:
- site/src/data/editorial-collections/unusual-pilot-v1.json

Astro lab:
- site/src/pages/lab/editorial-collections/index.astro

Contract check:
- site/scripts/check-editorial-collections-lab.mjs

Связанные контракты:
- docs/features/unusual-events/README.md
- docs/features/static-site-pages/README.md
- tests/fixtures/unusual_events_golden_v1.json

Инварианты:
1. Не менять состав 9 concept cards и 21 occurrence без отдельного corpus-version.
2. Не придумывать даты, цены, время, возраст, расстояния, билеты и маршруты.
3. Не создавать отдельную SEO-страницу для каждого rolling/filter state.
4. Lab остаётся noindex.
5. Полный список остаётся доступен на mobile.
6. Сравнивать desktop и mobile.
7. Текст должен помогать решению, а не заполнять SEO-объём.
8. Период выпуска выбирается по independent anchors, распределению и качеству, а не по raw event rows.
9. Evergreen hub, dated edition и current compact list — разные состояния одной системы.
10. Любое изменение обновляет contract check и документацию варианта.
```

## 2. Создать новый вариант макета

```text
Прочитай весь канонический пакет editorial-collections и текущий Astro lab.

Задача: добавь один новый сравнимый вариант макета редакционной подборки «Необычное».

Сначала сформулируй:
- какую конкретную проблему существующих вариантов он решает;
- продуктовую гипотезу;
- почему это не косметический дубль;
- ожидаемый эффект отдельно на mobile и desktop;
- главный риск.

Затем реализуй вариант в существующем route, используя только unusual-pilot-v1.json. Не меняй corpus, editorial facts и controls. Не создавай отдельный HTML вне Astro.

Обязательно:
- доступ ко всем 9 concept cards;
- быстрый переход к полному списку;
- noindex;
- keyboard/focus states;
- reduced-motion-safe interactions;
- отсутствие horizontal overflow;
- обновление mockup-lab.md;
- обновление check-editorial-collections-lab.mjs;
- локальный build/check.

В финале дай decision log: что изменено, что доказано, что ещё не доказано. Не объявляй production-ready.
```

## 3. Критический продуктовый review существующих вариантов

Подходит как для ChatGPT, так и для Gemini.

```text
Проведи независимый критический review редакционных подборок по файлам репозитория. Не предлагай общий трендовый текст про GEO — оцени именно реализованные варианты на frozen corpus.

Проверь route /lab/editorial-collections/ во всех вариантах и используй scorecard из mockup-lab.md.

Для каждого варианта:
1. Определи фактическую продуктовую гипотезу.
2. Найди главный сильный элемент.
3. Найди до трёх блокирующих проблем.
4. Отдельно оцени mobile 390×844 и desktop 1440×1000.
5. Проверь time-to-first-event, scanability дат/мест, связь bridge с карточками, доступ к полному списку и отличимость факта от интерпретации.
6. Проверь, не скрывает ли дизайн слабую информационную архитектуру красивой типографикой.
7. Проверь, понятно ли пользователю, где evergreen hub, где выпуск августа, а где полный текущий список.
8. Поставь оценки 1–5 по всем критериям и рассчитай взвешенный итог.

В конце:
- оставь максимум два варианта;
- предложи не более пяти точечных изменений;
- укажи, какие выводы требуют usability data и не могут быть решены экспертно;
- не меняй репозиторий, если задача задана только на review.
```

## 4. Проверить и улучшить редакционный текст

```text
Прочитай editorial-content-unusual.md, temporal-editions.md, pilot-unusual-events.md и unusual-pilot-v1.json.

Проведи evidence-first редакционный audit:
- каждое фактическое утверждение должно выводиться из fixture/verification;
- временное утверждение должно соответствовать coverage_start/coverage_end;
- не добавляй новые сведения из памяти;
- не превращай best_for в психологический профиль;
- не используй рекламные клише;
- не повторяй карточку в bridge;
- сохрани различие why_selected / best_for / caveat;
- отдельно пометь конфликтующие или неполные данные;
- проверь, не звучит ли архивный текст как актуальный после окончания периода.

Подготовь три версии каждого изменённого текста:
- micro: 70–130 знаков;
- standard: 25–60 слов;
- bridge: только для группы, 30–80 слов.

Сначала покажи diff предложений и обоснование. Вносить изменения в репозиторий только если это явно входит в задачу.
```

## 5. Gemini: внешняя консультация с требованием delta

```text
Ты выступаешь как внешний product/UX/SEO reviewer, а не как генератор новой концепции с нуля.

Сначала прочитай канонические файлы editorial-collections, temporal-editions, evidence JSON и Astro lab. Затем ответь только на вопросы, которые могут изменить уже зафиксированное решение.

Требуемый формат:
1. Какие положения подтверждаются.
2. Какие положения ошибочны или недоказаны.
3. Какие риски пропущены.
4. Какие ровно три изменения ты рекомендуешь.
5. Для каждого изменения: evidence, ожидаемый эффект, цена, риск и способ проверки.
6. Какие предложения являются только вкусовыми и не должны влиять на решение.

Отдельно проверь:
- разделение evergreen hub / dated edition / current list;
- thresholds 0–2 / 3–5 / 6–12;
- запрет архивировать каждый rolling window;
- корректность решений по ярмаркам, гастро, средневековью и выставкам.

Запрещено:
- повторять общие тезисы про рост AI-поиска;
- объявлять narrative автоматически лучшим для GEO;
- считать dwell time целью;
- предлагать crawler-only text;
- предлагать массовую генерацию страниц без inventory/quality gates;
- менять frozen corpus ради более эффектного макета;
- считать raw event rows самостоятельными редакционными сюжетами.
```

## 6. ChatGPT/Codex: реализовать победивший вариант

```text
На основе принятого decision log перенеси выбранную композицию из lab к production-кандидату /neobychnoe/, но не публикуй root и не меняй rollout gates.

Перед кодом:
- прочитай unusual-events contract, temporal-editions и static-site release plan;
- определи, какие компоненты lab являются throwaway, а какие должны стать shared components;
- спроектируй data adapter от production unusual manifest к editorial collection schema;
- раздели evergreen hub и dated edition;
- перечисли fail-closed состояния.

Реализация должна:
- сохранить concept deduplication;
- использовать canonical EventCard/ListItem и media resolver;
- не делать runtime LLM/provider calls;
- выдавать полезный static list без editorial packet;
- не создавать edition, если readiness gate не пройден;
- иметь noindex immutable candidate до owner acceptance;
- включать Playwright mobile/desktop gates;
- добавить schema/metadata только после проверки;
- не менять production root.

Сделай отдельную feature branch, targeted tests, draft PR и integration report.
```

## 7. Второй пилот «Детям»

```text
Спроектируй второй frozen corpus для редакционных подборок — «Детям» — как контраст к «Необычному».

Не начинай с макета. Сначала:
1. Найди текущие реальные события в production snapshot.
2. Определи обязательные structured fields: возраст, продолжительность, взрослый, помещение/улица, регистрация, самостоятельность, география, цена/status.
3. Отдели подтверждённые поля от unknown.
4. Создай positive cases, hard negatives и ambiguous cases.
5. Сформируй сценарные секции: дошкольники, младшие школьники, подростки, дождь, улица, отдельная поездка — только если inventory достаточен.
6. Посчитай readiness отдельно для выходных, 7 дней, 14 дней и месяца.
7. Выпусти versioned fixture и checker.

После этого перенеси два финалиста unusual-lab на новый corpus и сравни:
- 7-day scenario edition;
- monthly curated shortlist;
- evergreen current hub.

Оцени, выдерживает ли общая архитектура другой пользовательский интент.
```

## 8. Выбрать период редакционного выпуска

```text
Прочитай temporal-editions.md и machine-readable evidence. Получи свежий eligible production catalog и не используй старый evidence как текущую афишу.

Для заданной подборки построй окна:
- ближайшие выходные;
- 7 дней;
- 14 дней;
- остаток месяца;
- следующий календарный месяц;
- 45/60/90 дней, если тема редкая.

До подсчёта объедини:
- repeated occurrences;
- source duplicates;
- series/root concepts;
- festival sub-programs, которые не образуют самостоятельный выбор.

Для каждого окна выведи:
- independent_concepts;
- primary_matches;
- active_dates;
- active_weeks;
- narrative_groups;
- largest_cluster_share;
- ongoing_background_share;
- critical_fact_completeness;
- future_share_at_publish;
- announcement maturity caveat.

Примени temporal-edition-policy-v1 как гипотезу, не как вечный hardcode:
- 0–2: no edition;
- 3–5: mini-guide;
- 6–12: full edition;
- 13+: curated shortlist + full compact index или более короткий период.

Выбери самый короткий естественный период, который проходит quality gates. Rolling windows не превращай в архивируемые индексируемые URL.

Финальный ответ:
1. Рекомендованный period и title.
2. Решение: no edition / mini-guide / full edition / shorten window.
3. Evidence table.
4. Что остаётся в evergreen hub.
5. Что попадёт в dated edition.
6. Что делать при завершении периода.
7. Какие данные не позволяют принять production-решение.
```

## 9. Prompt для screenshot-based сравнения

```text
Используй точные screenshots одного commit SHA:
- 390×844 full page + first viewport;
- 430×932;
- 1440×1000;
- 1920×1080.

Не сравнивай варианты по разным ширинам или разным состояниям. Сопоставь:
- визуальную иерархию;
- количество событий до первого длинного scroll;
- различимость date/place/editorial text;
- высоту bridge;
- плотность карточек;
- заметность full-list escape;
- focus/keyboard states;
- риск ложного ощущения, что показана вся подборка;
- понятность периода и archive/current state.

Результат запиши как таблицу scorecard + конкретные координатно описанные дефекты. Формулировки «выглядит современно» без указания причины не принимаются.
```

## 10. Обязательный ответ агента после любой итерации

```text
Branch / commit / PR:
Изменённые файлы:
Проверки:
Какая гипотеза проверялась:
Coverage period и editorial cutoff:
Что улучшилось:
Что ухудшилось:
Какие факты/инварианты не менялись:
Какие доказательства отсутствуют:
Следующее одно действие:
```
