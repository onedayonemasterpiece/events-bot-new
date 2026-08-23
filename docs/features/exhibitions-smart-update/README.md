# Smart Update: Выставки

Цель: выставки должны создаваться и обновляться через единый пайплайн Smart Update из всех источников (`telegram`, `vk`, `parser:*`) без отдельной логики рендера страниц.

## Что считается реализованным

- Все источники передают данные в `smart_event_update.smart_event_update` через `EventCandidate`:
  - Telegram Monitoring: `source_parsing/telegram/handlers.py`
  - VK (ручной/автоимпорт): `vk_intake.py`, `vk_auto_queue.py`
  - `/parse`: `source_parsing/handlers.py`
- Тип события `выставка` сохраняется в `event.event_type`.
- `end_date` учитывается как дата завершения выставки и используется в:
  - `/exhibitions`
  - секции `Постоянные выставки` на страницах выходных и месяца.
- Текст страницы события в Telegraph формируется тем же Smart Update-потоком (через `event.description` + LLM), без source-specific рендереров.

## Специфика merge для выставок

- Матчинг событий использует пересечение диапазонов дат:
  - существующее событие с `date=2026-01-15` и `end_date=2026-03-01` матчит кандидат с `date=2026-02-20`.
- Если у выставки в источнике есть дата начала, но нет даты закрытия, Smart Update ставит `end_date` по умолчанию как `date + 1 календарный месяц`.
- Исключение: если карточка озаглавлена как открытие выставки (`Открытие выставки...`) и источник не даёт явный период/дату закрытия, fallback-период не ставится — это атомарное событие открытия. Для диапазона карточка должна быть нормализована как сама выставка или иметь source-grounded `end_date`.
- Это правило относится только к `выставка`. Для `ярмарка` fallback-период по умолчанию не ставится: ярмарка без явного сигнала о сроке окончания остаётся однодневной.
- Такой `end_date` считается служебным fallback: он нужен для month/weekend-страниц и merge по периоду, но не должен показываться пользователю как подтверждённая дата закрытия.
- Для длинных событий (`выставка`, `ярмарка`) продление `end_date` разрешено, если trust кандидата не ниже накопленного trust события.
- Если trust кандидата ниже, `end_date` не перезаписывается, а в лог источников пишется конфликт.
- Для long-running выставок есть отдельный deterministic rescue по `exact title + overlapping range`, который срабатывает до агрессивного shortlist narrowing и не даёт плодить дубли:
  - у одного и того же поста с несколькими слотами экскурсий (`12:00` / `15:00`);
  - у более поздних cross-source постов про уже идущую выставку внутри того же периода.

## Инварианты

- Не создаём дубль выставки, если новый источник попадает в текущий период уже созданной выставки.
- Обновления `end_date` прозрачны в `event_source_fact`:
  - успешное обновление идёт как `added` (`Дата окончания: ...`);
  - отклонённое по trust идёт как `conflict`.
- Если `end_date` поставлен по умолчанию, более поздний источник с явной датой закрытия должен обновлять это поле без создания дубля.
- Если реальная дата закрытия уже известна, более поздний источник без неё не должен затирать подтверждённый `end_date` служебным fallback'ом.
- Правила работы с фактами/описанием те же, что и для остальных событий: факты через LLM, итоговый текст через LLM.
- В summary-блоке Telegraph выставки показываются как период:
  - `10-20 февраля`,
  - `с 10 февраля по 28 марта`,
  - `по 28 марта` (если выставка уже идёт).
- Если дата закрытия пока только fallback (`date + 1 месяц`), summary-блок Telegraph показывает только начало: `с 10 февраля`.
- В пользовательских списках (`/exhibitions`, month/weekend pages, отдельная month exhibitions page) существующие duplicate rows дополнительно схлопываются на рендере, чтобы legacy-клоны из БД не дублировались в выдаче.
- В агрегированных month/weekend-страницах секция `Постоянные выставки` остаётся обзорной: выводится не больше `12` активных выставок, и каждая карточка использует короткое описание (`short_description`/`search_digest`/one-sentence fallback), а не полный Telegraph-текст. Полные тексты остаются на individual event pages и в специализированных exhibition surfaces.

## Тестовое покрытие

- Unit:
  - `tests/test_smart_event_update_exhibitions.py`
    - merge по пересечению периода (без дубля),
    - trust-aware блокировка продления `end_date`.
  - `tests/test_month_split_regressions.py`
    - month-page split дробит exhibition tail и переходит в minimal mode, если exhibition section всё ещё превышает Telegraph limit;
    - month-page renderer ограничивает и compact-рендерит `Постоянные выставки`.
- E2E (offline, Behave):
  - `tests/e2e/features/smart_event_update.feature`
    - `Выставка не дублируется при новом источнике внутри периода`
    - `Выставка не принимает продление периода от источника с более низким trust`

## Enforce acceptance monitor for `/vystavki/`

After `SMART_UPDATE_IDENTITY_GATE=enforce`, the 14-day acceptance gate for the
current exhibition-duplicate incident is the read-only SQLite monitor:

```bash
python3 scripts/inspect/audit_public_exhibition_duplicates.py \
  --db /data/db.sqlite \
  --current-date <YYYY-MM-DD> \
  --since-days 14 \
  --format both \
  --fail-on-high-confidence
```

The monitor applies a schema-adaptive public gate (`identity_status=canonical`,
`merged_into_event_id IS NULL`, active lifecycle, valid ISO dates). Overlapping
range/title/source/venue similarity is **recall only**. Every candidate is then
partitioned as `CONFIRMED_DUPLICATE`, `KEEP_DISTINCT`, or `UNRESOLVED` from a
pair-correlated final/manual identity-ledger verdict. `KEEP_DISTINCT` requires
confidence ≥0.8, concrete source-grounded evidence and a concrete blocking
conflict. IDs only, intermediate decisions, stale/conflicting evidence and
`linked_event_ids` never authorize that disposition. The audit exposes
`candidate_pair_count`, `confirmed_duplicate_count`, `keep_distinct_count` and
`unresolved_count` for the complete current corpus, whose last three always sum
to the first. The separate `*_window_count` fields and Prometheus
`*_since_total` series describe the rollout window but never weaken the
full-current success gate. When `event.added_at` exists, the window includes pairs where either
row was added inside the explicit `since_date`/`EXHIBITION_DUPLICATE_AUDIT_SINCE_DATE` rollout start, or `current_date - since_days` when no explicit start is set. If a legacy/test schema lacks
`added_at`, the monitor fails closed and counts the pair in the window. Rollout
success requires the full-current JSON counts and total series to be zero;
window series remain diagnostic only:

- `confirmed_duplicate_count=0` and `unresolved_count=0`;
- `events_public_exhibition_confirmed_duplicate_pairs_total 0`;
- `events_public_exhibition_unresolved_pairs_total 0`;
- `events_public_exhibition_confirmed_duplicate_pairs_since_total{window_days="14"} 0`;
- `events_public_exhibition_unresolved_pairs_since_total{window_days="14"} 0`;
- no critical false positives on recurring/multi-session controls;
- any detected pair is treated as an incident/regression, not auto-merged by the
  monitor.

The same acceptance check can be scheduled after enforce:

```env
SMART_UPDATE_IDENTITY_GATE=enforce
ENABLE_EXHIBITION_DUPLICATE_AUDIT=1
EXHIBITION_DUPLICATE_AUDIT_TIME_LOCAL=07:45
EXHIBITION_DUPLICATE_AUDIT_TZ=Europe/Kaliningrad
EXHIBITION_DUPLICATE_AUDIT_SINCE_DAYS=14
```

Scheduled runs are read-only, write `ops_run(kind='exhibition_duplicate_audit')`,
alert the superadmin/admin chat on confirmed or unresolved pairs, and by default
raise after recording `status='failed'`. A candidate set consisting entirely of
grounded `KEEP_DISTINCT` verdicts records `status='success'`.
 The scheduled audit also embeds Smart Update identity-gate rollout counters from
`event_identity_decision_log` (decision/veto/fail-safe/vector-error counts) and
secret-safe env-readiness booleans into `ops_run.metrics_json` and
`details_json.identity_gate`, so the 14-day evidence can show both public
duplicate absence and gate/vector health.
