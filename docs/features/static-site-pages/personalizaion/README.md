# Персонализация статического сайта

> **Статус:** стратегический пакет реализации.  
> **Область:** сквозная персонализация статического сайта KenigEvents, кроме основного порядка явно календарных/хронологических списков.

## Канонические документы

1. [`requirements.md`](requirements.md) — ручные требования владельца продукта; высший продуктовый приоритет.
2. [`personalization-to-be.md`](personalization-to-be.md) — **главная целевая продуктовая, исследовательская и модельная архитектура**.
3. [`personalization-research-traceability.md`](personalization-research-traceability.md) — нормативная трассировка всех целевых наработок в реализационные волны и guard против подмены исследований legacy-кодом.
4. [`personalization-implementation-contract.md`](personalization-implementation-contract.md) — техническая детализация целевой системы: единый runtime, surface policy, компактное хранение, транспорт, физическая модель БД, rollout и release gates. Он заполняет инженерные пробелы, но не может менять продуктовый или модельный смысл `personalization-to-be.md`.
5. [`identity-linking-personalization.md`](identity-linking-personalization.md) — методология anonymous→account link, conflict merge, logout, login другим аккаунтом, cross-device consistency и account-switch hard barriers.
6. [`longitudinal-e2e-personalization.md`](longitudinal-e2e-personalization.md) — сценарный пакет длительного surfing/E2E: повторные визиты, golden personas, profile horizons, DOM-трансформация, GitHub Actions evidence и калибровка модели.
7. [`temporal-profile-simulation.md`](temporal-profile-simulation.md) — методология synthetic time в offline replay и staging DB/primary-store simulation для проверки session/short/mid/long horizons без ожидания реальных месяцев.
8. [`personalization-test-report-template.md`](personalization-test-report-template.md) — формат сводного отчёта: planned vs actual, persona scorecards, timeline профиля, factor ablation, hard invariants и next-iteration plan.
9. [`personal-selection-quality-feedback.md`](personal-selection-quality-feedback.md) — нормативная механика оценки конкретной персональной выдачи: first relevant within 30, score 0–10, reason codes, served-list provenance, trigger-bias control и калибровочный backlog.
10. [`focus-group-interest-questionnaire-prompt.md`](focus-group-interest-questionnaire-prompt.md) — prompt для отдельной проработки optional анкеты фокус-группы по интересам и ограничениям.
11. [`personalization-current-runtime-audit-2026-08-02.md`](personalization-current-runtime-audit-2026-08-02.md) — аудит фактического runtime в `main` и список расхождений, которые нельзя переносить в production.
12. [`tasks/personalization-wave-0.md`](tasks/personalization-wave-0.md) — готовое задание кодовому агенту на первый безопасный реализационный PR.
13. [`implementation-status.yml`](implementation-status.yml) — машиночитаемый контроль волн, зависимостей и доказательств.

## Машиночитаемые контракты

- [`schemas/personalization-browser-state-v1.schema.json`](schemas/personalization-browser-state-v1.schema.json)
- [`schemas/personalization-action-batch-v1.schema.json`](schemas/personalization-action-batch-v1.schema.json)
- [`schemas/personalization-profile-projection-v1.schema.json`](schemas/personalization-profile-projection-v1.schema.json)
- [`collection-surfaces-v1.example.json`](collection-surfaces-v1.example.json)

JSON-файлы в этом пакете являются **контрактными примерами**, а не автоматически включённой production-конфигурацией. Реальный runtime обязан валидировать совместимость версий и fail closed при неизвестной surface/schema.

## Приоритет при конфликте

1. `requirements.md` — явные актуальные продуктовые решения владельца.
2. Утверждённый юридический/localization release-gate — только для права, оснований обработки, локализации и публичных документов.
3. `personalization-to-be.md` — продуктовая, исследовательская и модельная цель.
4. `personalization-implementation-contract.md` — способ технической реализации цели; он не может переопределять пункт 3.
5. `personalization-research-traceability.md`, identity/linking, longitudinal/temporal E2E, quality-feedback/report docs и `implementation-status.yml` — контроль полноты, качества и доказательств.
6. Старые `unsigned-personalization`, prototype, demo и фактические скрипты — только migration evidence и источник characterization tests.

Если целевой документ оставляет вопрос открытым, ответ нельзя молча брать из старого кода. Он фиксируется как hypothesis/owner/legal decision и закрывается исследованием, benchmark или отдельным решением.

Переходные документы, допускающие browser → public Supabase RPC с передачей полного локального профиля, не являются целевым контрактом durable-персонализации. Такой путь допустим только для обезличенного публичного каталога без private profile state; durable actions/profile обслуживаются через утверждённый same-origin API.

## Карантин текущего runtime

Текущий `EventLayout.astro` и его формулы не являются «первой версией целевой модели». Wave 0 может вынести их только в явно переходные модули `legacy/profile-v1.ts` и `legacy/scorer-v1.ts`, чтобы доказать безопасный refactor. Целевой `scorer.ts` строится позднее из `personalization-to-be.md`, target fixtures и model bake-off; простое переименование legacy-модуля запрещено.

## Текущее состояние

На дату 2026-08-04 production-ready сквозной цикл ещё не реализован. В `main` есть полезный локальный прототип, но он содержит legacy consent-dialog, inline-скореры, прямой RPC fallback, локальный reset и несинхронизированный feedback log. Реализация начинается с **Wave 0: quarantine + characterization + extraction**, а не с добавления новых таблиц, весов или продуктовых решений в существующий inline-скрипт.
