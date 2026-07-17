# INC-2026-07-15 Fly `/data` crossed the critical readiness floor

Status: monitoring
Severity: sev1
Service: Fly production `events-bot-new-wngqia` (`/data`, `/healthz`, `/webhook`)
Opened: 2026-07-15
Closed: —
Owners: events-bot production / release owner
Related incidents: `INC-2026-04-16-prod-disk-pressure-runtime-logs`, `INC-2026-07-13-runtime-logging-recurring-event-quality`
Related docs: `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`, `docs/operations/incident-management.md`

## Summary

Во время pre-deploy проверки общей production-интеграции статического сайта публичный `/healthz` перестал быть ready и Fly health check отвечал HTTP 503. Приложение и SQLite оставались работоспособны, но на 1 GiB volume `/data` осталось около 255 MiB — ниже критического floor `256 MiB`. Fly proxy как минимум один раз не смог выбрать healthy candidate для `/webhook`, поэтому это production incident, а не только capacity warning.

## User / Business Impact

- единственная production Machine исключалась из healthy routing, пока свободное место было ниже критического floor;
- входящий Telegram webhook мог получить proxy failure, поэтому пользовательская команда или update могли быть задержаны либо потребовать повторной доставки;
- planned exact-main deploy и статический release были остановлены до восстановления readiness.

## Detection

- pre-deploy `curl https://events-bot-new-wngqia.fly.dev/healthz` завис по timeout;
- `fly checks list` показал HTTP 503 при `ready=true`, `db=ok`, но `disk:critical_free_space` (`free_mb=255`, warning `350`, critical `256`);
- `fly logs` показал повторные 503 health checks и `could not find a good candidate within 40 attempts at load balancing` для `/webhook`;
- capacity alert был корректным, но 1 GiB volume оставлял слишком узкий operational headroom при нормальном размере DB, media evidence и bounded runtime logs.

## Timeline

- 2026-07-15 02:30 UTC — pre-deploy public health probe timed out.
- 2026-07-15 02:31 UTC — Fly checks локализовали отказ readiness до free-space floor; остальные readiness-компоненты были healthy.
- 2026-07-15 02:35 UTC — `df`/`du`, volume metadata, snapshots и runtime mirror были проверены; разрушительная очистка не выполнялась.
- 2026-07-15 02:40 UTC — planned static-site deploy поставлен на hold; выбран безопасный mitigation: extend 1→2 GiB и bounded auto-extension, а не удаление канонической DB/media/runtime evidence.
- 2026-07-15 03:12 UTC — attached encrypted volume расширен 1→2 GiB без restart; Fly сохранил пять daily snapshots.
- 2026-07-15 03:13 UTC — свободно `1223 MiB`, `PRAGMA quick_check=ok`, runtime mirror `39.58 MiB` и растёт; публичный `/healthz` вернулся к HTTP 200, Fly check `1/1 passing`, свежих disk-full/proxy ошибок после resize нет.
- 2026-07-17 08:59 UTC — clean detached `origin/main@faaaa659` deployed as Fly release `v1685`; post-deploy volume remained `2 GiB` with about `936 MiB` free, `PRAGMA quick_check=ok`, `/healthz` ready and Fly check `1/1 passing`.

## Root Cause

1. Production `/data` оставался 1 GiB, хотя его штатные durable данные выросли примерно до: SQLite 245 MiB, `guide_media` 280 MiB, retained monitoring bundle 86 MiB и bounded runtime logs 41 MiB.
2. Readiness contract намеренно переводит `/healthz` в critical ниже 256 MiB, чтобы предотвратить SQLite/disk-full corruption; фактический остаток пересёк этот floor.
3. В `fly.toml` не было bounded volume auto-extension, поэтому корректная retention политика не могла компенсировать общий рост durable product data.

## Contributing Factors

- warning floor `350 MiB` на 1 GiB volume означает небольшой интервал между warning и routing-critical состоянием;
- единственная Machine с единственным attached volume не оставляет healthy replica для webhook routing;
- release preflight обнаружил проблему раньше planned deploy, но не раньше первого proxy error.

## Automation Contract

### Treat as regression guard when

- меняются Fly volume/mount config, runtime-log budgets, health thresholds, SQLite/media retention или deploy path;
- выполняется любой production deploy, daily/scheduled recovery или volume resize;
- `/healthz` timeout/not-ready либо Fly proxy сообщает отсутствие healthy candidate.

### Affected surfaces

- `fly.toml` `[[mounts]]` и production Fly volume `vol_4m83jjyewxmjn6gr`;
- `/data/db.sqlite`, `/data/runtime_logs`, `/data/guide_media`, monitoring result retention;
- `/healthz`, Fly service health checks и `/webhook` routing;
- exact-main release/deploy workflow.

### Mandatory checks before closure or deploy

- сохранить pre/post volume metadata и snapshot list; не удалять неизвестные или canonical paths;
- `df -h /data`, top-level `du`, `PRAGMA quick_check=ok`;
- volume не меньше 2 GiB и свободное место выше warning floor `350 MiB`;
- `/healthz` HTTP 200, `ready=true`, `db=ok`, disk status not warning/critical;
- Fly health checks pass `1/1`; в свежем окне нет новых `Errno 28`, `database or disk is full` или `/webhook` no-candidate ошибок;
- runtime mirror существует, укладывается в budget и продолжает расти;
- `fly config validate` принимает bounded auto-extension (`80%`, `+1 GiB`, limit `3 GiB`);
- deployed SHA достижим из `origin/main`;
- если daily/scheduled slot в incident window был потерян, выполнить compensating catch-up и проверить текущий день.

### Required evidence

- ignored artifacts under `artifacts/codex/INC-2026-07-15-fly-volume-critical/`;
- PR/merge SHA с prevention config и incident record;
- Fly volume/show, snapshots, health, logs, SQLite quick-check and exact-main deploy output;
- post-deploy public `/healthz` and Fly checks.

## Immediate Mitigation

- planned deploy остановлен до восстановления readiness;
- destructive cleanup не выполнялся: DB, media, monitoring и runtime evidence сохранены;
- выполнен irreversible-growth-only Fly operation 1→2 GiB; restart не потребовался, encrypted volume и пять daily snapshots сохранены.

## Corrective Actions

- [x] расширить attached volume до 2 GiB;
- [x] добавить в `fly.toml` bounded automatic extension at 80%, +1 GiB, maximum 3 GiB;
- [x] выполнить exact-main deploy и обязательные post-resize/post-deploy checks.

## Follow-up Actions

- [ ] production owner — наблюдать weekly `/data` component growth и пересмотреть 3 GiB cap только через отдельный capacity review.
- [ ] production owner — оценить independent DB backup/restore drill; Fly snapshots не являются полной заменой application-level backup.

## Release And Closure Evidence

- deployed SHA: `faaaa6599681523fa083d5b936a0e5f74000a7e4`, reachable from `origin/main` via PR [#55](https://github.com/onedayonemasterpiece/events-bot-new/pull/55)
- deploy path: manual `flyctl deploy --remote-only` from a clean detached exact `origin/main`; Fly release `v1685`, machine version `1685`, image `deployment-01KXQMQZPTJH45EYJK65R4MPAV`
- regression checks: `fly config validate` passed; post-resize `df` reports `1223 MiB` free; SQLite `quick_check=ok`; runtime mirror `39.58 MiB <= 64 MiB` and active mtime advanced; no fresh `Errno 28`, SQLite disk-full or webhook no-candidate match.
- post-deploy verification: `/data` is `2 GiB` with about `936 MiB` free, SQLite `quick_check=ok`, runtime mirror is active and bounded at about `61 MiB`, public `/healthz` is HTTP 200 with `ready=true`, Fly is `1/1 passing`, and the fresh runtime window contains no `Errno 28`, SQLite disk-full or webhook no-candidate errors.

## Prevention

- bounded volume auto-extension добавляется как availability guard;
- log budget, free-space floors, snapshots and DB/media hygiene остаются обязательными и не заменяются дополнительной ёмкостью;
- incident становится автоматическим regression contract для последующих Fly/deploy/storage изменений.
