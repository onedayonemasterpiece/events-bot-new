# Передача унифицированных связанных событий в рабочую ветку

## Donor

- Branch: `feature/related-events-compact-unified-20260721`
- Base: `origin/main@3d0af26cbe`
- Scope: explicit occurrence resolver, compact/rail formatter, shared label and
  selector components, surface wiring, synthetic lab, tests и канонические docs.
- Это integration donor, не свидетельство production deploy.

Не делайте wholesale merge старых лабораторий:

| Ref | Использование |
| --- | --- |
| `origin/feature/static-related-occurrence-final-templates@a7f80b67` | только accepted композиция 03/04/05; код уже осознанно портирован |
| artifact-only listing v19 / screenshot 10 | только принятая representative-card механика; renderer не переносить |
| `origin/integration/popular-desktop-v28-20260720@40b309cb` | только research evidence; heuristic title/type/venue grouping не переносить |
| 01/02/11/12/13 | отклонённые occurrence baselines; не восстанавливать |

## Короткий prompt для другого окна

```text
Используй feature/related-events-compact-unified-20260721 как contract/donor для текущей ветки. Не merge старые labs wholesale и не затирай локальные изменения. Перенеси единый eventOccurrences resolver/formatter и EventOccurrenceLabel/EventOccurrenceNav; family создавай только из взаимных explicit other_date_ids (позже occurrence_group_id), без inference по title/type/venue. На date-bounded lists используй per-date, на Popular/search/recommendations/personal — per-family, на event detail — selector без full-card grid. Точные подписи: «2, 9 ноября 19:00» и «4 ноября 17:00, 19:00»; rail — те же данные в две строки с полным aria-label. Прими визуальные 03/04/05/10, не восстанавливай 01/02/11/12/13. Сохрани canonical URL каждой occurrence, используй synthetic reciprocal fixtures и прогони occurrence tests, Astro build/check-preview и regression INC-2026-07-18-dramteatr-same-day-event-glue. Обнови canonical docs и CHANGELOG по фактической интеграции.
```

## Порядок интеграции

1. Обновить рабочую ветку от актуального `origin/main`; разрешать конфликты в
   пользу её текущих product features, но в пользу donor для occurrence identity
   и formatting contract.
2. Перенести pure module и unit tests, затем DTO fields, затем dumb components,
   затем конкретные surfaces. Не копировать локальные formatter/helper variants.
3. Для каждой list surface явно выбрать mode:
   - конкретная дата/день: `per-date`;
   - Popular/search/recommendation/personal: `per-family`;
   - event detail selector: `none`.
4. Проверить, что SSR и hydrated cards получают одинаковый compact text и
   member ids, а `Также:` и full-card `Другие даты` отсутствуют.
5. Обновить этот handoff, `requirements.md`, surface-specific canonical doc и
   `[Unreleased]` только по реально перенесённому поведению.

Stable rollout отдельно блокируется, пока production/export audit не покажет
достаточную coverage взаимных explicit links и не даст backfill/review receipt.
В текущем committed preview links отсутствуют; donor сознательно не маскирует
этот data gap browser inference.

## Обязательная проверка

```bash
cd site
npm ci
npm run test:occurrences
npm run build:preview
npm run check:preview
cd ..
pytest -q tests/test_smart_update_merge_identity_gate.py
```

Manual review:

- simple same-time/multiple-date family;
- same-day/multiple-time family;
- cross-month and mixed schedule fallback;
- no family for same title/type/venue without explicit reciprocal ids;
- explicit-link coverage/backfill receipt для production catalog;
- canonical href/CTA/calendar follows selected occurrence;
- date list keeps different dates; entity list keeps one ranked representative;
- mobile/desktop selector matches 03/04/05; listing compact mechanic matches 10;
- incident negative replay keeps theatre excursion and play separate.
