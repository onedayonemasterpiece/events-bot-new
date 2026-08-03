# P13N-00: research delta и integration handoff

> Статус: **реализация lane завершена, общий mount и browser acceptance ожидают интегратора**.
> Baseline: `origin/main` at `0bc8482dcf9f74b835743f72d57aa5dcce755c28` (2026-08-03).
> Production effect этой lane: **нет**.

## Нормативный приоритет

Целевая продуктовая и модельная система по-прежнему определяется
[`personalization-to-be.md`](personalization-to-be.md). Текущий inline runtime,
старые числовые веса и fixtures используются только как migration evidence по
[`personalization-research-traceability.md`](personalization-research-traceability.md).
Legacy parity не является доказательством качества, target acceptance или
выбором модели.

## Реализовано в P13N-00 lane

- typed target contracts без target scorer и model weights;
- quarantine старого profile parser и related/personal-feed scorer под
  `site/src/lib/personalization/legacy/`;
- fail-closed registry `collection-surfaces-v1`;
- pure shadow presenter plan: ничего не применяет к DOM;
- `off | characterize | local-shadow`, причём все три режима non-applying;
- bounded sanitized test API без identity, token, raw profile и action log;
- один runtime component, который не читает/пишет storage и не делает network;
- build-time route inventory и source/quarantine guard;
- pure characterization/contract tests и minimal storage byte report.

## Намеренно `not started`

| Target item | Статус после P13N-00 | Владелец следующей волны |
|---|---|---|
| Activation вместо `consent_ok`, compact atomic state, undo/hidden recovery | `not started` | P13N-01 |
| Target scorer и model weights | `not started` | P13N-02 после target fixtures/model bake-off |
| Golden personas и `unknown` mass | `not started` | P13N-05 |
| Session/short/mid/long materializer | `not started` | P13N-05 |
| Graph, adjacent-interest expansion, exploration, anti-bubble/rescue | `not started` | P13N-05/06 |
| Durable outbox/ACK/reconcile/same-origin API | `not started` | P13N-03 после legal gate |
| Primary DB schema, RLS, materializer, ETag projection | `not started` | P13N-04/05 |
| `/dlya-menya/` production model, A/B, causal success | `not started` | P13N-06 |

Ни один legacy weight не перенесён в target policy/default. Известная ошибка
`exact hide → negative facet` остаётся только в legacy comparator и должна быть
устранена P13N-01, а не закреплена как target semantics.

## Transport/YDB boundary

P13N-00 не добавляет transport operation, endpoint, retry, outbox, DB migration,
Supabase/YDB read или remote write. Существующий resilient transport остаётся
отдельной общей инфраструктурой. Future strong actions обязаны использовать
same-origin private API с idempotent ACK/reconcile; публичный profile RPC не
является fallback. YDB может получить только будущую асинхронную обезличенную
TTL-аналитику и не становится вторым владельцем current profile или
подтверждением action ACK.

## Обязательный mount у интегратора

Эта lane не владеет `EventLayout.astro`. Интегратор должен сделать минимальный
mount без переноса логики:

```astro
---
import PersonalizationRuntime from '../components/personalization/PersonalizationRuntime.astro';
---

<body ...>
  <PersonalizationRuntime pathname={Astro.url.pathname} />
  ...
</body>
```

Компонент должен присутствовать ровно один раз. Нельзя импортировать
`legacy/scorer-v1.ts` в layout или production target path.

После mount обязательны production-like Astro build, generated route inventory
и browser characterization: тот же visible order, те же localStorage keys/bytes,
те же existing requests, like/hide/share/reset и число handlers. Evidence явно
фиксирует `remote writes=0`, `DB changes=0`, `production behavior change=0`.
Только после этого `implementation-status.yml` переводится в `done`.

`legacy_characterization.*` tests — только parity/migration evidence. Generated
`artifacts/personalization-route-inventory.json` остаётся минимальным untracked
artifact. Отсутствие browser acceptance до общего mount является integration
blocker, а не скрывается unit PASS.
