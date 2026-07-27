# INC-2026-07-27 Future-event source coverage drop

Status: mitigating (reopened for additional official-source coverage)
Severity: sev1
Service: production event inventory / scheduled source parsing / official venue catalogs
Opened: 2026-07-27
Closed: 2026-07-27 11:33 UTC
Reopened: 2026-07-27 11:40 UTC
Owners: events-bot maintainer / operations
Related incidents: `INC-2026-07-08-prod-root-overlay-disk-full.md`, `INC-2026-07-13-runtime-logging-recurring-event-quality.md`, `INC-2026-07-10-future-event-semantic-audit.md`, `INC-2026-06-24-future-event-date-default-venue-regressions.md`
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

После исправлений, production catch-up и точной сверки в системе присутствуют
все `18/18` будущих позиций официальной афиши с provenance
`parser:philharmonia`. Общий future inventory вырос с `216` до `247`, а
next-30 — с `145` до `170`. Низкая плотность на отдельных августовских днях
сохранилась и после полного восстановления источников, поэтому остаток спада
отнесён к фактической сезонности/разреженности опубликованных календарей, а не
к продолжающейся потере филармонии.

После первого closure пользователь запросил расширенную проверку Собора,
Калининградского театра эстрады, Янтарь холла и театров. Reconciliation с
официальными каталогами показал вторую часть того же customer-visible coverage
incident:

- Собор: `17/17` официальных будущих occurrences совпали по date/time/title;
- Драмтеатр: parser выполнился, но четыре экскурсии `14:30` были схлопнуты с
  другими спектаклями `18:00` на тех же датах;
- Музтеатр: из `20` официальных occurrences отсутствовал второй сеанс
  «Бродский. Обещание любви» `2026-10-25 14:00`, схлопнутый с `17:00`;
- Театр эстрады: legacy Дом искусств parser был только URL-scoped для старых
  спецпроектов; полного каталога переименованного театра не существовало.
  Полный обход future month links дал `23` official occurrences; в production
  корректно совпали шесть, ещё одна карточка была вне текущего official
  каталога, `17` отсутствовали;
- Янтарь холл: `@yantarholl` действительно сканировался ежедневно, но Telegram
  posts не покрывали официальный каталог. Отдельного website parser не было:
  на официальном сайте опубликовано `77` future occurrences, из которых
  `18` были представлены с корректным date/time, ещё шесть имели
  пропущенные/ошибочные anchors, а `53` отсутствовали;
- Третьяковка: parser исполнялся, но production provenance показал тот же
  occurrence-collapse класс (несколько date/time ticket URLs на одном event).

Root cause второй волны — отсутствие двух catalog sources и слишком широкое
разрешение canonical parser менять время: при одной performance URL для
нескольких сеансов Smart Update мог трактовать новый explicit time как
коррекцию уже parser-backed карточки, а не отдельную occurrence.

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
- Runtime file mirror был штатно включён (`ENABLE_RUNTIME_FILE_LOGGING=1`) по
  более новому bounded-logging contract `INC-2026-07-13` и сохранил нужное
  evidence; размеры и free-space floors требовали отдельной проверки.

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
- `2026-07-27 08:40 UTC`: corrected code from `origin/main` was deployed as
  Fly release `v1753`; health, disk, scratch write and SQLite checks passed.
- `2026-07-27 08:43 UTC`: production still had zero future Philharmonia rows.
  The safe same-process catch-up remained pending: the approved E2E identity
  was correctly non-admin, so no silent temporary privilege grant was made.
- `2026-07-27 08:57 UTC`: after explicit authorization, the exact E2E user row
  was snapshotted, temporarily granted superadmin, and restored immediately
  after the live UI run. `ops_run=4678` correctly finished `error`, exposing a
  second blocker: Kaggle rejected both per-run status-dataset creations with
  `dataset_create_new ... Invalid token`; neither primary kernel was pushed.
- `2026-07-27 09:02 UTC`: a same-credential Kaggle 2.2.4 probe could create a
  small dataset, but private status/delete APIs returned `403`; this confirmed
  that parser availability must not depend on the dataset control plane. The
  probe left `zigomaro/status-probe-1785142966` because the credential itself
  was denied `datasets.delete`.
- `2026-07-27 09:13 UTC`: the second authorized UI catch-up passed the former
  status-dataset gate. Qtickets was pushed and entered `RUNNING`; Philharmonia
  reached `kernels_push`, which rejected changing the existing kernel from
  notebook to script (`You cannot change the editor type of a kernel`). The
  temporary superadmin grant was again restored exactly immediately after
  command acceptance.
- `2026-07-27 10:00`–`11:03 UTC`: full compensating catch-up
  `ops_run=4683` completed `success`: Philharmonia `18` processed
  (`17` created, `1` skipped) and Qtickets `31` processed (`2` created,
  `29` updated), with zero source errors. The one Philharmonia skip exposed the
  over-broad festival guard described below.
- `2026-07-27 11:17`–`11:21 UTC`: targeted `ops_run=4694` completed the missing
  occurrence (`1` created, `17` updated, zero errors); its data result was
  successful, but the oversized Telegram report exposed the last UI-only
  failure.
- `2026-07-27 11:24 UTC`: final fixes from `origin/main` were deployed as Fly
  release `v1761`, image
  `deployment-01KYHN1WKZWBQG185KAT2SA4J5`.
- `2026-07-27 11:25`–`11:31 UTC`: final authorized live UI validation
  `/parse philharmonia` completed as `ops_run=4697 status=success`: all `18`
  official cards updated, `0` failed/skipped/errors, and the long operator
  report was delivered in two bounded Telegram messages. Exact privilege
  restoration, source reconciliation, health, SQLite, disk and runtime-log
  checks passed; the incident was closed.

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
5. The daytime change guard still fingerprinted the obsolete `/?event` URL
   instead of `/afisha/`, so current catalog changes could be skipped.
6. Parser startup treated a per-run private status-dataset as a mandatory input.
   Kaggle rejected its upload token, so the primary source kernels were never
   pushed even though callback telemetry is not required to parse events.
7. The corrected Philharmonia notebook still loaded
   `philharmonia_parser.py` as a sibling file, but Kaggle `kernels_push` sends
   only `kernel-metadata.json.code_file`. A remote run therefore required a
   self-contained script rather than a notebook referencing an unshipped module.
8. Kaggle kernel editor type is immutable for an existing slug, so converting
   `zigomaro/parse-philharmonia` from notebook to script was rejected. The
   self-contained script must be published under a new slug.
9. The first successful script-kernel catch-up exposed a latent
   production-boundary type mismatch: Philharmonia converted
   `normalized_date` to `datetime.date`, while Smart Update requires ISO text
   and calls `.split()` on the value. All 18 parsed items therefore reached
   processing but failed before persistence with
   `'datetime.date' object has no attribute 'split'`.
10. A rolling deploy cancelled that incomplete catch-up. Because the parse run
    initialized `ops_status=success` before doing work and `CancelledError` does
    not enter its ordinary `Exception` handler, the `finally` block recorded the
    interrupted run as green. Parse status must start fail-closed and become
    success/partial only after completed result evaluation.
11. The corrected catch-up then exposed an over-broad festival guard:
    the source-native individual concert «Аве Мария» was classified
    `festival_post` because it closes «Бахослужение» and was skipped. Official
    parser candidates are already structured occurrences, so the whole-program
    skip must remain limited to non-parser/social candidates.
12. The first implementation of that guard was inserted before the final
    slotted `EventCandidate` fields. Python accepted the following indented
    declarations as local annotations in the helper, silently removing
    `organizer_names`, `links_payload` and `age_semantic_decision` from the
    dataclass. The next live create therefore failed with
    `'EventCandidate' object has no attribute 'organizer_names'`; the helper was
    moved below the complete schema and the existing organizer persistence
    tests were added to the regression gate.
13. The fail-closed status sentinel correctly made the next failed-item run
    `partial`, but remained in `details_json.fatal_error` after normal terminal
    evaluation. Completed runs now explicitly clear the sentinel; only
    cancellation retains it.
14. The successful final 18/18 data catch-up produced a report longer than one
    Telegram message. The unchunked Markdown send and equally unchunked
    plain-text fallback both failed with `Bad Request: message is too long`, so
    UI showed an error despite `ops_run=4694 status=success`. Manual and
    scheduled reports now use the existing bounded line chunker.

## Contributing Factors

- No alert enforced nonzero/fresh parser provenance per enabled official source.
- Morning parsing is commonly skipped during the nightly heavy-operation lease;
  the daytime compensating slot ran, but its partial source loss was hidden.
- Telegram and VK importers continued operating, creating enough events to hide
  that official site parsers were incomplete.
- Наличие bounded runtime mirror помогло восстановить картину; при этом closure
  всё равно требует подтвердить hard budget, retention и свободное место по
  `INC-2026-07-13`.

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
- Verify `ENABLE_RUNTIME_FILE_LOGGING=1` and the bounded production contract:
  8 MiB/file, 64 MiB total, 48-hour ceiling and 256 MiB free-space floor.
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
- [x] point the daytime source-change guard at the current `/afisha/` catalog;
- [x] add DOM replay and three-run concurrency regressions;
- [x] make source-specific parse errors visible in `ops_run.status`;
- [x] merge the fix to `origin/main` and deploy;
- [x] make primary parser launch independent of Kaggle status-dataset creation
  and package Philharmonia as a self-contained script;
- [x] run and reconcile the production catch-up through the authorized live UI
  flow, restoring the exact temporary user grant after command acceptance;
- [x] verify the expected bounded runtime mirror remains enabled, within its
  byte/retention budget and above its free-space floor;
- [ ] add durable per-source freshness/coverage alerting.
- [ ] add direct full-catalog parsers for the renamed Theatre of Variety and
  Yantar Hall;
- [ ] prevent same-parser explicit-time conflicts from collapsing distinct
  occurrences;
- [ ] deploy from `origin/main`, run targeted production catch-ups and reconcile
  Cathedral/Dram/Muz/Estrada/Yantar/Tretyakov official inventories.

## Follow-up Actions

- [ ] operations: alert when an enabled parser has no successful/provenance
  refresh for two scheduled daytime slots.
- [ ] source parsing: add official-count reconciliation metrics for small,
  enumerable sources such as Philharmonia.
- [ ] product/data: review seasonal source calendars before interpreting all
  long-horizon decline as an ingestion regression.

## Release And Closure Evidence

- deployed SHA: `5907a0020fce007d075e4477f97bbe492596fdd6`,
  reachable from `origin/main`; delivery included PRs `#126`–`#133` for the
  control-plane-independent parser launch, self-contained new Philharmonia
  kernel slug, ISO date boundary, fail-closed run status, parser occurrence
  handling, complete candidate schema, cleared terminal sentinel and chunked
  operator reports.
- deploy path: clean detached `origin/main` worktree → `flyctl deploy
  --remote-only` → Fly release `v1761`, image
  `deployment-01KYHN1WKZWBQG185KAT2SA4J5`.
- regression checks: final focused suite
  `tests/test_source_parsing_commands.py`,
  `tests/test_source_parsing_status.py`, `tests/test_festival_context.py`,
  `tests/test_event_organizer_names.py` and
  `tests/test_philharmonia_parser.py` — `24 passed`; earlier expanded relevant
  suite — `70 passed` plus one unrelated date-sensitive replay whose fixture
  date is now in the past. Compile, JSON and diff checks passed. A fresh live
  official fetch returned exactly `18` future cards.
- full production catch-up: `ops_run=4683 status=success`, total `49`;
  Philharmonia processed `18` (`17` created) and Qtickets processed `31`
  (`2` created, `29` updated), with zero source errors. Targeted completion
  `ops_run=4694 status=success` created the one occurrence initially skipped by
  the festival guard.
- final live E2E: exact bot `@events_love39_bot`, command message `33291`,
  acknowledgement `33292`, terminal/report messages `33295` and `33296`;
  `ops_run=4697 status=success`, Philharmonia `18` processed, `0` created,
  `18` updated, `0` failed/skipped/errors, `fatal_error=null`. The exact
  pre-grant row was restored after command acceptance:
  `user_id=8336351413`, `is_superadmin=0`, `is_partner=0`, `blocked=0`.
- official reconciliation: `18/18` current Philharmonia URLs match active
  production events and exact `event_source.source_type=parser:philharmonia`
  provenance; unmatched URLs `0`. Future parser inventory is Philharmonia
  `18` events / `18` URLs and Qtickets `33` events / `34` URLs.
- restored inventory: active future `247` (`+31` from detection), next 7 days
  `78` (`+7`), next 14 `123` (`+13`), next 30 `170` (`+25`). The remaining
  sparse dates are 3 August `1`, 4 August `3`, and 6 August `3`; they remain
  sparse after exact official-source reconciliation.
- post-deploy verification: `/healthz ok=true ready=true db=ok issues=[]`;
  `PRAGMA quick_check=ok`; `/data` free `1518 MiB`, `/tmp` free `7508 MiB` and
  tempfile probe `ok`. Runtime mirror is enabled with the expected
  `8 MiB/file`, `64 MiB total`, `48h`, `256 MiB` floor and remains within the
  byte/free-space budget. A corrected timestamped scan of `1,407` runtime lines
  for the final run plus its exact parse debug log found no transaction,
  dataset-token, date-type, candidate-schema, persistence or report-length
  errors.
- raw reconciliation, E2E, health, release and log-window evidence is retained
  outside git under
  `artifacts/codex/INC-2026-07-27-future-event-inventory-drop/`.

## Prevention

Regression tests now cover the actual source DOM and concurrent ledger writer
shape. Terminal operational status is source-aware rather than outer-exception
only. Parser launches no longer depend on the failing status-dataset API, and
operator reports are bounded for Telegram delivery. The remaining prevention
gap is an automated source freshness/coverage alert, tracked above.
