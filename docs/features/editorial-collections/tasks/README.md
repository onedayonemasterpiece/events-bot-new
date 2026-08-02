# Управляющий план редакционных подборок

> Этот список управляет исследованием и прототипированием. Он не подменяет production release plan статического сайта.

## Статусы

- `DONE` — зафиксировано и проверяемо в репозитории;
- `READY` — можно выполнять без дополнительного проектирования;
- `BLOCKED` — нужен внешний вход или предшествующая задача;
- `DEFERRED` — сознательно не входит в текущий этап.

## P0. Исследовательский пакет

| Задача | Статус | Evidence |
|---|---|---|
| Зафиксировать продуктовую концепцию | DONE | `../README.md` |
| Выбрать пилотный архетип | DONE | `Необычное` |
| Собрать frozen real-event corpus | DONE | `unusual-pilot-v1.json` |
| Добавить hard negative и non-event | DONE | IDs 7153, 6885 |
| Подготовить editorial benchmark | DONE | `../editorial-content-unusual.md` |
| Спроектировать сравнимые варианты | DONE | `../mockup-lab.md` |
| Сделать Astro noindex lab | DONE | `/lab/editorial-collections/` |
| Добавить contract check | DONE | `check-editorial-collections-lab.mjs` |
| Подготовить prompts ChatGPT/Gemini | DONE | `../prompts.md` |
| Зарегистрировать пакет в docs index | DONE | `docs/features/README.md` |
| Зарегистрировать канонический route manifest | READY | `docs/routes.yml` |

## P1. Первый визуальный раунд

| Задача | Статус | Acceptance |
|---|---|---|
| Собрать site | READY | `npm run build` |
| Запустить contract check | READY | `npm run check:editorial-collections-lab` |
| Снять 6 × 5 viewport matrix | READY | Один commit SHA, full + viewport screenshots |
| Провести ChatGPT review | READY | Scorecard, максимум 2 финалиста |
| Провести Gemini review | READY | Delta-only консультация |
| Зафиксировать decision log | BLOCKED | Нужны screenshots и два review |

## P2. Уточнение двух финалистов

| Задача | Статус | Acceptance |
|---|---|---|
| Выбрать editorial-first финалиста | BLOCKED | P1 decision log |
| Выбрать browse-first финалиста | BLOCKED | P1 decision log |
| Подключить реальные shared card primitives | BLOCKED | Не менять data corpus |
| Подключить реальные media assets | BLOCKED | Единый media resolver, visual audit |
| Проверить keyboard/200%/reduced motion | BLOCKED | Playwright/manual evidence |
| Проверить 5/12/30 concept scaling | BLOCKED | Synthetic count only, facts unchanged |

## P3. Usability и аналитика

| Задача | Статус | Acceptance |
|---|---|---|
| Сценарий «найти участие» | BLOCKED | Completion + explanation accuracy |
| Сценарий «быстро выбрать 8 августа» | BLOCKED | Time to first valid event action |
| Сценарий «орган: обычный vs экскурсия» | BLOCKED | Boundary understood |
| Сценарий multi-date | BLOCKED | Dates found without duplicate misconception |
| Определить lab telemetry | DEFERRED | Только после privacy/release contract |

## P4. Контрастный пилот «Детям»

| Задача | Статус | Acceptance |
|---|---|---|
| Извлечь реальный corpus | BLOCKED | Current production snapshot |
| Проверить age/duration/adult/indoors fields | BLOCKED | Unknown остаются видимыми |
| Создать controls | BLOCKED | Hard negatives + ambiguous cases |
| Перенести 2 финалиста | BLOCKED | Без отдельного UI с нуля |
| Определить общий pattern | BLOCKED | Выдерживает оба архетипа |

## P5. Production architecture

| Задача | Статус | Acceptance |
|---|---|---|
| Версионированная collection schema | BLOCKED | Два пилота |
| Build-time writer | BLOCKED | Structured output, provider audit |
| Evidence validator | BLOCKED | No new facts, number/date checks |
| Fingerprint/cache | BLOCKED | Warm no-call proof |
| Last-known-good/fallback | BLOCKED | Hash-bound, bounded age |
| Adapter к unusual manifest | BLOCKED | Concept dedupe preserved |
| SEO metadata/schema | BLOCKED | Canonical + leaf Event boundary |
| Immutable noindex candidate | BLOCKED | Existing release pipeline |
| Owner acceptance | BLOCKED | Visual, browser, content gates |
| Production root promotion | BLOCKED | Отдельное решение владельца |

## P6. Проверенные маршруты

`DEFERRED` до появления детерминированных duration/travel/availability contracts. Нельзя использовать LLM для создания логистических фактов.

## Definition of done текущего research-этапа

Research-этап завершён, когда:

- пакет и lab находятся в main через PR;
- contract check проходит;
- есть screenshot matrix;
- выполнены два независимых review;
- выбраны максимум два варианта;
- decision log указывает, что проверено и что требует реальных пользователей.
