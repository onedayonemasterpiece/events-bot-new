# INC-2026-04-10 Telegram Monitoring Festival Bool

Status: closed
Severity: sev2
Service: `tg_monitoring` server-import / Telegram payload normalization
Opened: 2026-04-10
Closed: 2026-04-10
Owners: Telegram monitoring / smart update pipeline
Related incidents: —
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/operations/cron.md`

## Summary

Recovery-import `tg_monitoring` завершался как `partial`, хотя `telegram_results.json` был успешно скачан и большая часть событий импортировалась. Причина оказалась не в бизнес-логике импорта, а в нестрогой нормализации optional text fields: boolean в `festival` и падение диагностического helper на `.strip()`.

## User / Business Impact

- scheduled `tg_monitoring` мог формально запускаться, но частично терять импорт постов;
- recovery path становился ненадёжным именно тогда, когда должен был добрать пропущенный результат;
- upstream schema drift превращался в повторяемый production incident.

## Detection

- в `ops_run` и runtime-логах повторялись ошибки вида `'bool' object has no attribute 'strip'`;
- affected examples: `kulturnaya_chaika/7525`, `meowafisha/7100`, `festkantata/1426`;
- инцидент проявлялся на server-import boundary после успешного Kaggle output download.

## Timeline

- `2026-04-10`: recovery-import `tg_monitoring` завершился как `partial`.
- Разбор показал, что `festival` из Kaggle payload может приходить boolean.
- Выяснилось, что importer прокидывает значение без строгой нормализации, а `_clip_title(...)` в `smart_event_update` безусловно вызывает `.strip()`.
- В ответ были добавлены guardrails на import boundary и в diagnostic helper, плюс regression tests.

## Root Cause

1. На import boundary optional text field `festival` не нормализовался до безопасного string-or-None контракта.
2. Diagnostic helper `_clip_title(...)` предполагал string и не был устойчив к boolean/non-string values.
3. Один malformed payload field не валил весь run, но переводил его в `partial` и терял часть импортов.

## Contributing Factors

- слишком слабый runtime contract для optional text fields из Kaggle/remote extractors;
- отсутствие regression guard на non-string diagnostic values;
- incident initially выглядел как частный payload дефект, хотя на самом деле вскрывал общий import-boundary gap.

## Automation Contract

### Treat as regression guard when

- меняется `tg_monitoring`, `source_parsing/telegram/`, `smart_event_update.py`, payload normalization или contract между Kaggle и server-import;
- меняются diagnostic/logging helpers, которые работают с сырыми extractor values;
- меняются scheduled/recovery paths для Telegram monitoring.

### Affected surfaces

- `source_parsing/telegram/handlers.py`
- `source_parsing/telegram/service.py`
- `smart_event_update.py`
- Kaggle payload contract для optional text fields
- scheduled / recovery `tg_monitoring` paths

### Mandatory checks before closure or deploy

- regression test, что `festival=true` не валит `process_telegram_results(...)` и нормализуется в `None`;
- regression test, что diagnostic helpers не падают на boolean/non-string values;
- smoke или targeted verification, что malformed optional fields не переводят run в `partial` из-за типовой `.strip()`-ошибки;
- docs/features contract по Telegram monitoring синхронизирован с runtime guardrails.

### Required evidence

- ссылки на targeted regression tests;
- evidence из логов/`ops_run`, что ошибка типа больше не воспроизводится;
- deployed SHA, если fix выпускался в production.

## Immediate Mitigation

- `festival` на import boundary нормализован как optional text;
- `_clip_title(...)` перестал падать на boolean/non-string diagnostic values.

## Corrective Actions

- в `source_parsing/telegram/handlers.py` boolean и `None` для `festival` отбрасываются в `None`;
- в `smart_event_update.py` diagnostic helper работает безопасно для boolean/non-string values;
- добавлены regression tests на importer boundary и diagnostic helper;
- feature docs уточнены: malformed optional fields не должны валить server-import.

## Follow-up Actions

- [ ] При каждом изменении Kaggle payload schema для Telegram monitoring явно проверять optional text normalization на boundary.
- [ ] При добавлении новых diagnostic helpers использовать string-safe contract по умолчанию.

## Release And Closure Evidence

- regression checks: targeted tests на `process_telegram_results(...)` и `_clip_title(True/False)`;
- closure condition: malformed optional fields больше не должны переводить `tg_monitoring` в `partial` из-за type crash;
- production release evidence должно храниться вместе с deploy SHA / linked change.

## Prevention

- optional text fields из Kaggle/remote extractors должны нормализоваться на import boundary;
- diagnostic/logging helpers не имеют права падать на неожиданных типах;
- upstream schema drift должен приводить к business-level handling decision, а не к incidental `.strip()` crash.
