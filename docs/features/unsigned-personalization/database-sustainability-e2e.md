# Database sustainability gate для product E2E

> **Статус:** обязательный design/acceptance contract для будущего real-data longitudinal E2E. Он не означает, что telemetry ingest, rollup, YDB projection или cleanup уже реализованы.

Цель gate — доказать не только функциональность persona, но и то, что повторяющиеся реальные сессии не превращают Supabase/YDB в неограниченный event log, не дублируют canonical catalog и не смешивают владельцев данных.

## 1. Границы владельцев

| Контур | Разрешено | Запрещено в этом E2E |
|---|---|---|
| Fly SQLite `/data/db.sqlite` | Единственный canonical source реальных events/sources/lifecycle; read-only snapshot input | Записывать persona actions, profiles, E2E markers; менять production events ради сценария |
| Personalization Supabase/Postgres | Текущий compact anonymous/profile state; accepted strong actions; bounded session/served summaries; isolated E2E tenant/run evidence | Полный canonical event catalog, raw scroll firehose, bulky traces/video, second analytics history, browser service-role key |
| YDB analytics | Асинхронная de-identified high-volume/history projection, агрегаты и raw weak telemetry только с TTL, если сбор действительно нужен | Current profile authority, stable raw user identity/PII, send/rank eligibility, full canonical event copy, browser direct writes |
| Object Storage / ignored artifacts | Реальные snapshot bundles, traces, screenshots/video, redacted evidence packets | Использование как mutable profile source of truth |

Каноника ownership: [`docs/architecture/personalization-data-ownership.md`](../../architecture/personalization-data-ownership.md). Изолированный YDB personal-data contour для VK proof не входит в persona E2E и не может использоваться как analytics/profile store.

Контролируемые UI-действия разрешены только в отдельном E2E namespace/tenant с `e2e_run_id`, `test_actor=true`, dedicated credentials и cleanup. Event data остаются реальными; action identity — тестовая. Обычные product profiles, counters и analytics исключают эти строки по trusted server marker, а не по browser-supplied флагу.

## 2. Current implementation status

На reviewed branch этот gate в основном **design-only**:

- applied migrations для `personalization_session_summary`, `personalization_served_list_summary`, `visitor_profile_snapshot`, `telemetry_quarantine`, trusted `e2e_run_id/test_actor` и cleanup-by-run не найдены;
- personalization → YDB analytics writer/outbox/checkpoint/TTL path не реализован; TTL другого comment-feedback sidecar не является доказательством;
- vector projections имеют stable event keys и pruning path, а result cache имеет `expires_at`/purge — это полезные bounded exceptions;
- `event_search_requests`, quota ledgers, query embedding cache и feedback/tag-candidate tables требуют отдельной retention проверки; их рост нельзя приписывать golden-persona telemetry;
- Supabase project также содержит Auth/email control-plane relations, поэтому project total без per-relation attribution недостаточен.

Следовательно, первый implementation slice обязан добавить observability/isolation/cleanup до заявления о longitudinal E2E pass.

## 3. Что измеряется до/после каждого run

### Supabase/Postgres

Service-side probe сохраняет, как минимум:

- `pg_database_size(current_database())`;
- `pg_total_relation_size`, `pg_table_size`, `pg_indexes_size` и estimated/live row count по каждой personalization relation;
- delta inserts/updates/deletes, accepted/deduplicated/quarantined counts по `e2e_run_id`;
- bytes на accepted strong action, session summary, served-list summary и retained profile revision;
- dead tuples/autovacuum/bloat indicators после cleanup;
- cache/embedding growth отдельно от persona telemetry, чтобы sidecar не скрывал причину роста.

### YDB

Service-side probe сохраняет:

- `resources.storage.table.used_bytes`, `resources.storage.used_bytes` и configured `resources.storage.limit_bytes`;
- rows/bytes по E2E-related analytics tables, включая service data/secondary-index amplification;
- writes и query stats на один accepted projection;
- expired-eligible rows и физически оставшиеся rows после TTL grace period;
- hot-partition/overload evidence и число secondary indexes, реально требуемых queries.

YDB TTL асинхронен: логически expired rows фильтруются в query немедленно, а физическое удаление проверяется после документированного grace window. TTL success нельзя объявлять в момент наступления `expires_at`.

## 4. Производные показатели

Для каждой БД и relation/table:

```text
run_growth_bytes       = size_after_run - size_before_run
cleanup_residual_bytes = size_after_cleanup_grace - size_before_run
bytes_per_action       = positive(run_growth_bytes) / accepted_unique_actions
index_amplification    = index_bytes_delta / table_bytes_delta
retention_debt_rate    = expired_rows_still_present / expired_rows_eligible
projected_365_bytes    = current_bytes + p95(daily_net_growth_bytes) * 365
headroom_ratio         = projected_365_bytes / configured_budget_bytes
```

Отдельно считаются 30/90/365-day projections для normal traffic, E2E traffic и catalog/vector refresh. Если нет хотя бы нескольких real measurements, projection имеет статус `INSUFFICIENT_BASELINE`, а не зелёный pass.

Абсолютные budgets не зашиваются в этот документ: они берутся из versioned environment/capacity manifest для текущего plan и каждой БД. Provider quota/plan может измениться. Gate использует относительные bands:

- **green:** `projected_365 <= 0.60 * configured_budget`;
- **review:** `0.60 < ratio <= 0.80`;
- **block:** `ratio > 0.80`, budget неизвестен, retention не действует или ownership нарушен.

Это project guard bands, не утверждение о provider limits.

## 5. Long-horizon sustainability scenarios

| ID | Run | Обязательный результат |
|---|---|---|
| `PERS-DB-001` | Одна 14-day session schedule по всем personas на одном real snapshot | Все строки коррелируют с run; Fly не изменён; growth attribution сходится с accepted counts |
| `PERS-DB-002` | Тот же run/seed повторён | Idempotency/dedupe не создаёт второй набор strong actions/served summaries |
| `PERS-DB-003` | 30/90/365-day equivalent replay вне браузера + browser sentinels | Growth linear/bounded; профиль хранит current state, а не все полные revisions без retention |
| `PERS-DB-004` | Cleanup run + Supabase vacuum/TTL grace observation без `VACUUM FULL` в hot path | E2E detail rows удалены/expired; остаётся только минимальный run summary и audit decision |
| `PERS-DB-005` | YDB projection outage/recovery | Supabase action не откатывается; outbox bounded; recovery не дублирует projection |
| `PERS-DB-006` | Catalog/vector rebuild на том же real event set | Upsert replaces stable IDs; нет append-only full catalog copies; obsolete vector/cache rows уходят |
| `PERS-DB-007` | Abort/crash посередине E2E | Cleanup по trusted run ownership удаляет orphan rows; чужие/production rows не затронуты |

Массовый 365-day replay выполняется process/integration layer, а Playwright проверяет representative UI sessions и сеть. Хранить миллионы browser screenshots/telemetry rows ради «реализма» запрещено.

## 6. Hard acceptance gates

Run имеет `PASS` только когда одновременно:

1. Fly SQLite остался byte/logically unchanged, кроме отдельного read-only snapshot artifact.
2. Supabase не получил полный canonical event payload; foreign event ID + minimal feature snapshot достаточно.
3. YDB не получил current profile, raw PII/stable subject или второй control plane.
4. Все тестовые writes имеют server-authenticated isolation marker; production aggregates их не учитывают.
5. Dedupe retry не увеличивает logical accepted count.
6. Cleanup proof показывает exact remaining rows/bytes; отсутствие cleanup evidence — failure.
7. TTL/retention debt укладывается в declared grace window.
8. 365-day projection ниже configured block threshold; budget/version присутствуют в artifact.
9. Static page/CTA остаются usable при отказе обеих dynamic DB paths.
10. Full evidence bundle не содержит secrets, raw emails, auth storage state или private source payloads.

## 7. Initial real baseline и operational probe

Read-only probe 2026-07-14 через `.codex/skills/events-bot-dual-db/scripts/check_personalization_db.py` показал point-in-time Supabase database size около **36 MB**. Крупнейшие public relations в этом срезе: `event_embeddings` около **17 MB**, `event_search_documents` около **3.9 MB**; это уже показывает, почему vector/catalog growth нужно отделять от persona telemetry. Baseline сохранён локально в ignored artifact `artifacts/codex/personalization-e2e-real-data-audit/supabase-size-baseline-2026-07-14.json` и **не является** capacity budget или трендом.

Для release evidence нужен минимум ряд измерений, а не один snapshot. YDB baseline также обязателен до включения projection; пока его нет, YDB sustainability status — `NOT_MEASURED`.

## 8. Implementation order

1. Capacity manifest с project refs, budgets, TTL/retention и owner contacts без secrets.
2. Read-only baseline collectors для Supabase и YDB.
3. Trusted E2E isolation marker + RLS/grants/service-only cleanup.
4. Per-run accounting и artifact schema.
5. 14-day real-snapshot replay, dedupe и cleanup.
6. Process-level soak + 30/90/365 projection.
7. Browser sentinel и failure recovery.
8. Только после этого — release/canary gate.

## 9. Primary references

- [Supabase: database and disk size](https://supabase.com/docs/guides/platform/database-size)
- [Supabase: database inspect/monitoring](https://supabase.com/docs/guides/database/inspect)
- [Supabase: deletion, bloat and vacuum considerations](https://supabase.com/docs/guides/database/postgres/data-deletion)
- [YDB: storage metrics](https://ydb.tech/docs/en/reference/observability/metrics/)
- [YDB: TTL semantics and delayed physical deletion](https://ydb.tech/docs/en/concepts/ttl)
- [YDB: secondary-index storage/write cost](https://ydb.tech/docs/en/concepts/query_execution/secondary_indexes)
