# Temporal profile simulation для long-term персонализации

> **Статус:** предварительная методология для P13N-05/P13N-06.  
> **Дата среза:** 2026-08-04.  
> **Цель:** формально проверять session/short/mid/long profile horizons в GitHub Actions и staging DB без ожидания реальных месяцев и без загрязнения organic data.

## 1. Проблема

Для долгосрочного профиля недостаточно открыть сайт один раз. Нужно проверить:

- как repeated visits формируют session/short/mid/long horizons;
- как materializer учитывает `occurred_at`, ingestion time и trusted provenance;
- что long-term affinity не возникает от одного like/share/open;
- что разные временные окна калибруются и объяснимы;
- что DB state, projection и DOM действительно меняются согласованно.

GitHub Actions может эмулировать браузерное время, но профиль формируется в БД.
Поэтому нужен тестовый контур, где synthetic evidence вводится в approved primary
store или staging equivalent через специальный test-only API, а не через правку
production timestamps руками.

## 2. Два уровня проверки

### 2.1. Offline temporal replay

Без БД, быстро и дешёво:

```text
frozen catalog + event features + persona script + timestamped actions
  → materializer/evaluator pure function
  → expected profile snapshots and ranks
```

Проверяет модель, decay, горизонты, counterfactuals и метрики качества.

### 2.2. DB-backed temporal staging

С БД/primary store, медленнее, но доказывает фактический сервисный контур:

```text
synthetic actor + timestamped action import
  → same materializer job with as_of clock
  → profile_snapshot/projection rows
  → browser refresh
  → transformed DOM + metrics
```

Этот слой используется только в isolated staging/test namespace и с
`training_eligible=false`.

## 3. Test actor model

Каждый longitudinal run создаёт actors:

```text
actor_id = p13n_test_<run_id>_<persona_key>
training_eligible = false
synthetic = true
retention_policy = test_short_ttl
cleanup_required = true
```

Нельзя использовать реальные focus-group аккаунты для time travel. Для реальных
фокус-групп можно собирать фактическую telemetry/feedback, но long-term horizon
эмулируется только synthetic actor или offline replay.

## 4. Временные поля

Разделять минимум три времени:

| Поле | Смысл |
|---|---|
| `occurred_at` | Когда действие якобы произошло в timeline пользователя |
| `ingested_at` | Когда staging/test API принял запись |
| `materializer_as_of` | На какую дату materializer строит snapshot |

Правило:

- `occurred_at` может быть synthetic в test-only import;
- `ingested_at` всегда реальное server time;
- production client не может свободно присылать прошлые даты без validation;
- materializer использует controlled `as_of` только при `P13N_TEST_CLOCK_ENABLED=true`.

## 5. Test-only import API

Staging-only endpoint/RPC:

```text
POST /api/personalization/test/v1/import-actions
POST /api/personalization/test/v1/run-materializer
POST /api/personalization/test/v1/cleanup-actor
```

Требования:

- доступ только из protected GitHub Actions environment/service role;
- запрещён в production config;
- actor id обязан иметь `p13n_test_` prefix;
- все rows получают `training_eligible=false`;
- TTL/cleanup обязательны;
- sensitive/free-text fields запрещены;
- import payload соответствует normal action schema + explicit synthetic
  timestamp envelope;
- materializer code path тот же, что в сервисе, за исключением controlled clock.

## 6. Если primary store — Supabase/Postgres

Использовать отдельную schema или tenant prefix:

```text
p13n_test_subject
p13n_test_action_recent
p13n_test_current
p13n_test_projection
```

или production tables с жёсткими test flags, если RLS/retention гарантируют
изоляцию. Предпочтительно отдельная schema для ранней отладки.

Проверки:

- `pg_total_relation_size` до/после run;
- `pg_column_size` для current/projection rows;
- cleanup удаляет actor rows;
- test actor не попадает в organic analytics/materializer queue;
- projection read доступен только test harness или browser с test credential.

## 7. Если approved primary store будет YDB

YDB может быть primary только если это принято отдельным ownership/legal решением.
Тогда temporal simulation создаёт isolated test tables/namespace:

```text
/p13n_test/subjects
/p13n_test/actions
/p13n_test/current
/p13n_test/projections
/p13n_test/materializer_runs
```

Требования:

- отдельный service account/IAM;
- actor prefix `p13n_test_`;
- TTL на action/import evidence;
- explicit cleanup step;
- no browser YDB credentials;
- same-origin API остаётся единственным browser boundary;
- YDB analytics contour не используется как current profile SOR.

Если YDB остаётся только analytics/history projection, то в YDB сохраняются
только longitudinal logs and metrics, а профиль формируется в утверждённом
primary store. Нельзя эмулировать production profile в analytics YDB и объявлять
это доказательством profile loop.

## 8. Persona temporal scripts

Пример script:

```json
{
  "persona": "science_learning_local",
  "timeline": [
    {"day": 0, "actions": [{"type":"like_set", "event":"lecture-1"}]},
    {"day": 3, "actions": [{"type":"save_set", "event":"museum-1"}]},
    {"day": 14, "actions": [{"type":"cta_registration", "event":"science-2"}]},
    {"day": 45, "actions": [{"type":"like_set", "event":"lecture-3"}]},
    {"day": 190, "actions": [{"type":"attendance_confirmed", "event":"science-4"}]}
  ],
  "as_of_days": [0, 1, 7, 14, 45, 190, 210]
}
```

Workflow прогоняет каждый `as_of_day`:

1. import actions with `occurred_at = base_time + day`;
2. run materializer with `materializer_as_of = base_time + as_of_day`;
3. fetch projection;
4. open pages in browser;
5. save DOM/rank/metrics artifacts.

## 9. Horizon expectations

| Horizon | Как тестируется |
|---|---|
| `session` | одна browser session, быстрый overlay, без materializer обязательного refresh |
| `short` | 0–7 дней synthetic repeated actions |
| `mid` | 8–179 дней repeated evidence, decay и stability |
| `long` | 180+ дней, только repeated strong/trusted evidence |

Long horizon gate:

- один like/share/open не создаёт long facet;
- repeated attendance or repeated strong evidence over 6+ months can;
- provenance содержит evidence ids/classes;
- sensitive/campaign events не materialize long facets;
- account/device conflict не перезаписывает long без merge policy.

## 10. GitHub Actions matrix

```yaml
persona:
  - science_learning_local
  - family_weekend_curator
  - music_jazz_theatre_mixed
  - price_sensitive_free_seeker
  - campaign_artifact_hunter
  - sensitive_interaction_control
horizon:
  - session
  - short
  - mid
  - long
store_mode:
  - offline_replay
  - staging_primary_store
```

Nightly может запускать все комбинации. PR запускает subset по затронутым files.
Manual workflow_dispatch позволяет выбрать persona/horizon/store_mode.

## 11. Metrics для temporal correctness

- `horizon_expected_facets_present`;
- `long_facet_from_single_action_count` — target 0;
- `trusted_provenance_rate` for long facets;
- `decay_monotonicity_violations`;
- `projection_revision_timeline_valid`;
- `materializer_coalescing_rate`;
- `db_rows_after_cleanup` — target 0 for test actors;
- `profile_bytes_by_horizon`;
- `cards_to_first_relevant_by_as_of_day`;
- `rank_delta_by_horizon`.

## 12. Evidence artifacts

```text
temporal-script.json
imported-actions.ndjson
materializer-runs.ndjson
profile-snapshots-by-as-of.json
projection-timeline.ndjson
db-size-before-after.json
cleanup-report.json
rank-delta-by-horizon.json
long-horizon-provenance.json
```

## 13. Safety / NO-GO

- test clock enabled in production;
- synthetic actor lacks `training_eligible=false`;
- test data remains after cleanup without explicit retained artifact reason;
- YDB analytics projection used as primary profile proof;
- production materializer accepts arbitrary historical client timestamps;
- long-term facet created from one weak signal;
- cleanup deletes non-test actor data;
- report hides DB cleanup failure.

## 14. Implementation dependency

Temporal DB simulation starts after:

- P13N-03 durable transport skeleton;
- P13N-04 staging primary-store schema;
- test-only import/cleanup endpoint reviewed;
- legal/localization boundary respected;
- longitudinal report format in place.

Before that, only offline replay and browser-local projection fixtures are
allowed.
