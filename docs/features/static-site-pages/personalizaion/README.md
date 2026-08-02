# Персонализация статического сайта

> **Статус:** стратегический пакет реализации.  
> **Область:** сквозная персонализация статического сайта KenigEvents, кроме основного порядка явно календарных/хронологических списков.

## Канонические документы

1. [`requirements.md`](requirements.md) — ручные требования владельца продукта; высший продуктовый приоритет.
2. [`personalization-to-be.md`](personalization-to-be.md) — целевая продуктовая и модельная архитектура.
3. [`personalization-implementation-contract.md`](personalization-implementation-contract.md) — нормативная детализация реализации: единый runtime, surface policy, компактное хранение, транспорт, физическая модель БД, rollout и release gates.
4. [`personalization-current-runtime-audit-2026-08-02.md`](personalization-current-runtime-audit-2026-08-02.md) — аудит фактического runtime в `main` и список расхождений, которые нельзя переносить в production.
5. [`tasks/personalization-wave-0.md`](tasks/personalization-wave-0.md) — готовое задание кодовому агенту на первый безопасный реализационный PR.
6. [`implementation-status.yml`](implementation-status.yml) — машиночитаемый контроль волн, зависимостей и доказательств.

## Машиночитаемые контракты

- [`schemas/personalization-browser-state-v1.schema.json`](schemas/personalization-browser-state-v1.schema.json)
- [`schemas/personalization-action-batch-v1.schema.json`](schemas/personalization-action-batch-v1.schema.json)
- [`schemas/personalization-profile-projection-v1.schema.json`](schemas/personalization-profile-projection-v1.schema.json)
- [`collection-surfaces-v1.example.json`](collection-surfaces-v1.example.json)

JSON-файлы в этом пакете являются **контрактными примерами**, а не автоматически включённой production-конфигурацией. Реальный runtime обязан валидировать совместимость версий и fail closed при неизвестной surface/schema.

## Приоритет при конфликте

1. `requirements.md`;
2. утверждённый юридический release-gate и фактические публичные документы;
3. `personalization-implementation-contract.md` для технических деталей реализации;
4. `personalization-to-be.md` для продуктовой/модельной целевой системы;
5. старые `unsigned-personalization`, prototype и demo-контуры — только как источник кода и тестовых идей.

Переходные документы, допускающие browser → public Supabase RPC с передачей полного локального профиля, не являются целевым контрактом durable-персонализации. Такой путь допустим только для обезличенного публичного каталога без private profile state; durable actions/profile обслуживаются через утверждённый same-origin API.

## Текущее состояние

На дату 2026-08-02 production-ready сквозной цикл ещё не реализован. В `main` есть полезный локальный прототип, но он содержит legacy consent-dialog, inline-скореры, прямой RPC fallback, локальный reset и несинхронизированный feedback log. Реализация начинается с **Wave 0: characterization + extraction**, а не с добавления новых таблиц или весов в существующий inline-скрипт.
