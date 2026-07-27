# INC-2026-07-27 Future-event source coverage drop

Status: open
Severity: sev1
Service: production event inventory / scheduled source parsing / Philharmonia and Qtickets
Opened: 2026-07-27
Closed: —
Owners: events-bot maintainer / operations
Related incidents: `INC-2026-07-08-prod-root-overlay-disk-full.md`, `INC-2026-07-10-future-event-semantic-audit.md`, `INC-2026-06-24-future-event-date-default-venue-regressions.md`
Related docs: `docs/features/source-parsing/README.md`, `docs/features/source-parsing/sources/philharmonia/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

Пользователь заметил резкое сокращение будущих событий и почти полное отсутствие
филармонии. Production audit подтвердил падение плотности: на `2026-07-27` было
`216` активных будущих событий, из них `71` на ближайшие семь дней и `39` на
следующую неделю; отдельные дни содержали одно–три события.

Это не только сезонность. С `2026-07-17` большинство дневных `/parse` запусков
теряли Philharmonia и Qtickets из-за параллельных транзакций status ledger, но
общий `ops_run` оставался `success`. У Philharmonia одновременно устарел сам
notebook: он использовал прежний URL/DOM и содержал обращение к неопределённым
переменным до добавления результата. В production не было ни одного события с
parser provenance `philharmonia`, а все `18` будущих позиций официальной афиши
на момент проверки отсутствовали.

## User / Business Impact

- Ближайшая афиша выглядит беднее фактического предложения; на некоторых днях
  пользователю показывается буквально несколько событий.
- Все `18` будущих концертов филармонии с `2026-07-27` по `2026-08-29` отсутствовали.
- Qtickets не имел успешного parser refresh после `2026-07-16`.
- Исторический импорт филармонии через VK/Telegram маскировал отсутствие
  dedicated parser provenance и не гарантировал полноту официальной афиши.
- Снижение имеет смешанную природу: летняя дальняя афиша действительно реже, но
  parser failures создали дополнительный подтверждённый дефицит.

## Detection

- Инцидент обнаружен по пользовательскому наблюдению, а не автоматическим alert.
- `ops_run.status=success` был ложнозелёным: `details_json.errors` содержал ошибки
  отдельных kernels, но итоговый status их не учитывал.
- Не было source-level freshness/coverage gate, который сопоставляет официальную
  будущую афишу с parser provenance в production.
- Runtime file mirror неожиданно был включён (`ENABLE_RUNTIME_FILE_LOGGING=1`),
  несмотря на regression contract `INC-2026-07-08`; это config drift, но не
  первопричина текущей потери событий.

## Inventory Evidence

### Production snapshot, 2026-07-27 UTC

- active future total: `216`;
- next 7 days: `71`; next 14 days: `110`; next 30 days: `145`;
- immediate daily counts: `3, 6, 7, 8, 17, 19, 11` for 27 July–2 August;
- next week: `1, 3, 5, 1, 8, 14, 7` for 3–9 August;
- current future parser inventory by earliest source: Qtickets `22`,
  Muzteatr `19`, Sobor `17`, Tretyakov `12`, Dramteatr `6`, Philharmonia `0`.

Approximate as-of known next-30-day inventory fell from `284` on 1 June to
`193` on 29 June, `210` on 6 July, `170` on 13 July, `155` on 20 July and `145`
on 27 July. Weekly additions fell from roughly `208–262` in April–June to `166`,
`126`, and `150` in the three complete July weeks preceding detection.

Raw evidence is retained outside git under
`artifacts/codex/INC-2026-07-27-future-event-inventory-drop/`.

## Timeline

- `2026-07-16`: last observed successful Qtickets parser result (`34` processed).
- `2026-07-17`–`2026-07-26`: most daytime parse runs reported Philharmonia and
  Qtickets `cannot start a transaction within a transaction`, while top-level
  runs remained `success`.
- `2026-07-19`: all three parser kernels failed with `Errno 28` during the known
  disk-pressure period.
- `2026-07-24`: theatre errors plus the same Philharmonia/Qtickets transaction
  failures produced a green run with zero source output.
- `2026-07-27`: user report triggered incident workflow; production DB, ops
  ledger, runtime mirror, parse debug logs, deployed code and official
  Philharmonia DOM were audited.
- `2026-07-27`: local deterministic three-kernel reproduction confirmed the
  shared-connection transaction race.

## Root Cause

1. `run_source_parsing()` starts Theatres, Philharmonia and Qtickets concurrently
   and passed the same `Database` instance to every runner. Each runner called
   `create_kaggle_run_config()`, which began `BEGIN IMMEDIATE` on the shared
   cached `Database.raw_conn()`. Concurrent begins on one SQLite connection
   failed, usually after the theatre runner acquired the transaction first.
2. The Philharmonia kernel targeted obsolete `https://filarmonia39.ru/?event`
   month pages and obsolete `.afisha_list_item`/detail selectors. The current
   source is `/afisha/` with `article.entry[data-date-iso]` cards.
3. The stale notebook referenced undefined `i` and `months_to_scan` from
   `parse_current_list()`, so even a matching card could fail before append.
4. `run_source_parsing()` initialized the `ops_run` as `success` and only changed
   it on a fatal outer exception; source-specific `result.errors` did not affect
   the terminal status.

## Contributing Factors

- No alert enforced nonzero/fresh parser provenance per enabled official source.
- Morning parsing is commonly skipped during the nightly heavy-operation lease;
  the daytime compensating slot ran, but its partial source loss was hidden.
- Telegram and VK importers continued operating, creating enough events to hide
  that official site parsers were incomplete.
- Runtime logging secret drift increased retained data on a known disk-pressure
  surface; `/data` and `/tmp` still had free space at detection.

## Automation Contract

### Treat as regression guard when

- changing source parsing orchestration, Kaggle status ledger writes or SQLite
  transaction ownership;
- changing Philharmonia/Qtickets/theatre parser kernels, source DOM selectors or
  output parsing;
- changing `ops_run` status semantics or parser scheduling/coverage monitoring;
- responding to a future-inventory decline or missing official-source report.

### Affected surfaces

- `source_parsing/handlers.py`, `source_parsing/philharmonia.py`;
- `kaggle_status.py` and three parallel parser runners;
- `kaggle/ParsePhilharmonia/` and official `filarmonia39.ru` DOM;
- `ops_run`, `kaggle_run_ledger`, `event_source` provenance;
- Fly `/data`, `/tmp`, runtime mirror and scheduled source-parser slots.

### Mandatory checks before closure or deploy

- Run the concurrent three-parser run-config regression and confirm all three
  ledger rows commit without transaction errors.
- Replay the current Philharmonia listing/detail fixtures through the kernel
  parser boundary; reject zero future output and missing full description.
- Execute a live production catch-up for Philharmonia and Qtickets (or full
  `/parse`) through Smart Update after deploy.
- Reconcile official future Philharmonia cards against production events and
  `event_source` provenance; account explicitly for every missing/skipped item.
- Confirm no new `cannot start a transaction within a transaction` in parse logs
  or `ops_run.details_json`.
- Confirm injected/real per-source errors finish the parse `partial` or `error`,
  never `success`.
- Verify `/healthz`, `PRAGMA quick_check`, `df` for `/`, `/tmp`, `/data`, and a
  `/tmp` write probe.
- Restore `ENABLE_RUNTIME_FILE_LOGGING=0` unless temporary incident logging has an
  explicit bounded budget.
- Because scheduled slots were missed today, closure requires compensating
  catch-up and verification of today's restored data, not deploy alone.

### Required evidence

- commit reachable from `origin/main` and deployed production SHA;
- targeted pytest/compile results and live Kaggle run IDs;
- post-catch-up `ops_run` metrics/errors and source counts;
- exact official-vs-production Philharmonia reconciliation;
- post-deploy health/disk/SQLite/runtime-env probes.

## Immediate Mitigation

- Prepared isolated transaction writes for Kaggle run-config creation.
- Replaced the stale browser/month parser with a current HTTP listing/detail
  parser and fail-closed zero-output/detail checks.
- Changed terminal parse status to `partial`/`error` when a source is lost.

## Corrective Actions

- [x] isolate parallel Kaggle status run-config transactions;
- [x] update Philharmonia listing/detail parser and production normalization;
- [x] add DOM replay and three-run concurrency regressions;
- [x] make source-specific parse errors visible in `ops_run.status`;
- [ ] merge to `origin/main`, deploy and run production catch-up;
- [ ] disable unexpected runtime file mirror after bounded incident evidence;
- [ ] add durable per-source freshness/coverage alerting.

## Follow-up Actions

- [ ] operations: alert when an enabled parser has no successful/provenance
  refresh for two scheduled daytime slots.
- [ ] source parsing: add official-count reconciliation metrics for small,
  enumerable sources such as Philharmonia.
- [ ] product/data: review seasonal source calendars before interpreting all
  long-horizon decline as an ingestion regression.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending (`origin/main` → Fly)
- regression checks: pending
- post-deploy verification: pending

## Prevention

Regression tests now cover the actual source DOM and concurrent ledger writer
shape. Terminal operational status is source-aware rather than outer-exception
only. The remaining prevention gap is an automated source freshness/coverage
alert, tracked above.
