# INC-2026-07-31-poster-candidate-url Smart Update падал на `PosterCandidate.url`

Status: open
Severity: sev1
Service: Smart Update / scheduled source parsing / VK auto-import / Telegram Monitoring
Opened: 2026-07-31
Closed: —
Owners: events-bot
Related incidents: `INC-2026-07-31-false-kgd80-festival-link`, `INC-2026-07-27-future-event-source-coverage-drop`, `INC-2026-07-20-tg-monitor-stale-s22-lease`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/telegram-monitoring/video-quality.md`, `docs/features/source-parsing/README.md`, `docs/operations/incident-management.md`, `docs/operations/release-governance.md`

## Summary

После добавления центрального KGD80-grounding в Smart Update новый код начал
читать `poster.url`, но slotted dataclass `PosterCandidate` хранит только
`supabase_url` и `catbox_url`. Любой импорт кандидата с poster evidence падал с
`AttributeError` до записи/merge события. Дефект затронул дневной source parser,
VK auto-import и live-проверку Telegram Monitoring с `@meowafisha`.

## User / Business Impact

- Дневной source parsing обработал 230 кандидатов, но 122 poster-bearing
  кандидата из семи источников не прошли Smart Update.
- VK auto-import получил как минимум один полный failed run и следующий run с
  восемью `persist_failed`; соответствующие текущие события не были
  созданы/обновлены.
- Telegram Monitoring успешно подтвердил событие и один раз проанализировал
  вертикальное видео `@meowafisha/8101`, но сервер не создал event/video M:N
  rows из-за того же исключения.
- Ошибка не повреждала уже существующие строки: транзакция Smart Update не
  доходила до semantic mutation, однако сегодняшний импорт остался неполным.

## Detection

- Инцидент обнаружен live release-validation новой video-quality фичи на
  `@meowafisha`: `ops_run.id=4951` завершился `partial` с
  `events_imported=0`.
- Production runtime file mirror был включён
  (`ENABLE_RUNTIME_FILE_LOGGING=1`, `/data/runtime_logs`, retention 48h) и
  показал массовое повторение того же traceback.
- `ops_run` и source-level report выявили более широкий blast radius, который
  не был виден из одной Telegram проверки.

## Timeline

- 2026-07-31 до 10:00 UTC — commit `3d2720fb` с KGD80-grounding уже находится
  в production и содержит обращение к отсутствующему `poster.url`.
- 2026-07-31 10:00:00–10:00:53 UTC — scheduled VK auto-import
  `ops_run.id=4926` падает на первом poster candidate.
- 2026-07-31 12:15:12–13:07:29 UTC — scheduled source parsing
  `ops_run.id=4932` завершается `partial`: 230 кандидатов, 108 успешных
  update, 122 failed.
- 2026-07-31 13:30:00–13:50:08 UTC — VK auto-import `ops_run.id=4938`
  получает восемь `persist_failed` с тем же исключением.
- 2026-07-31 16:58:48–17:03:01 UTC — scoped Telegram Monitoring
  `video-release-20260731-meowafisha` подтверждает событие и принимает видео,
  но server import падает до event/video persistence.
- 2026-07-31 17:10 UTC — runtime mirror + `ops_run` audit формализует инцидент
  как sev1 и запускает hotfix/replay/catch-up workflow.

## Root Cause

1. `PosterCandidate` — `@dataclass(slots=True)` с отдельными полями
   `supabase_url` и `catbox_url`; общего атрибута `url` в его контракте не было.
2. KGD80 source-evidence adapter в `_smart_event_update_impl` начал строить
   payload через `poster.url` без совместимого accessor или regression replay с
   реальным `PosterCandidate`.
3. Исключение возникало в общей Smart Update boundary до create/merge, поэтому
   одинаково блокировало все import surfaces с poster evidence.

Root cause механический (API/transport compatibility), а не semantic: fix не
принимает смысловых решений и не заменяет LLM broad regex/keyword логикой.

## Contributing Factors

- Regression suite KGD80 проверяла festival semantics, но replay не подставлял
  fallback poster candidate, который присутствовал в production import.
- Source parsing мог завершиться `partial` с `errors_count=0`, хотя per-source
  counters содержали 122 failed; aggregate monitoring требует отдельного
  follow-up.
- У producer и server-import разные успешные границы: принятый видео sidecar/CDN
  object не означал, что canonical event/video rows уже записаны.

## Automation Contract

### Treat as regression guard when

- меняются поля/accessors `PosterCandidate` или poster evidence для Smart Update;
- меняется `ground_kgd80_festival` / `_smart_event_update_impl` pre-write path;
- меняются Telegram/VK/source-parser server import boundaries;
- меняется Telegram video persistence после Smart Update.

### Affected surfaces

- `smart_event_update.py` (`PosterCandidate`, `_smart_event_update_impl`);
- `source_parsing/handlers.py` и `source_parsing/telegram/handlers.py`;
- VK auto-import persistence;
- core Fly SQLite `event`, `event_source`, `video_asset`, `event_video_link`,
  `ops_run`;
- Kaggle Telegram Monitoring handoff, encrypted SHA cache и Yandex CDN object;
- scheduler catch-up and release path.

### Mandatory checks before closure or deploy

- provenance helper предпочитает managed `supabase_url`, сохраняет fallback
  на `catbox_url` и безопасно возвращает `None` без URL;
- exact raw `@meowafisha` replay проходит
  `process_telegram_results` → настоящий `smart_event_update`, создаёт event,
  `video_asset` и `event_video_link`, `events_errored=0`;
- replay включает opposite controls и отключает только outbox/LLM side effects,
  а не production import boundary;
- релевантные KGD80, Smart Update, source-parsing и video-persistence tests
  проходят;
- deployed SHA достижим из `origin/main`, worktree clean, `/healthz` ready;
- live повтор `@meowafisha/8101` даёт SHA cache hit и не создаёт новый physical
  `tg_monitor_video_quality` provider send;
- выполнить и проверить compensating source-parsing rerun за текущий день;
- повторно обработать текущий VK auto-import backlog и подтвердить отсутствие
  свежих `PosterCandidate.url` traceback.

### Required evidence

- exact replay fixture в `tests/replays/INC-2026-07-31-poster-candidate-url/`;
- pytest outputs и pre/post shadow-DB assertions;
- deployed SHA, Fly image/machine version, PR/CI и main reachability;
- redacted runtime/`ops_run` evidence в
  `artifacts/codex/INC-2026-07-31-poster-candidate-url/`;
- post-deploy Telegram run, limiter ledger, DB M:N rows, CDN/sidecar headers;
- source-parsing/VK catch-up `ops_run` rows and no fresh matching errors.

## Immediate Mitigation

- Сохранён exact Kaggle handoff с уже принятым видео и no-event negative post в
  incident replay fixture.
- Подготовлен узкий provenance helper: managed CDN URL имеет приоритет, source
  URL остаётся fallback, отсутствие URL даёт `None`.
- Новые конкурирующие S22-запуски не стартуют, пока не проверен текущий
  guide/Telegram resource state.

## Corrective Actions

- Использовать `_poster_candidate_evidence_url` вместо несуществующего общего
  поля, не меняя отдельные provenance/storage fields.
- Провести exact replay через Telegram server-import и реальный Smart Update;
  проверить video M:N persistence в shadow SQLite.
- Доставить fix только через clean main-reachable Fly release.
- После deploy выполнить Telegram cache-hit validation и текущие source/VK
  catch-up runs.

## Follow-up Actions

- [ ] Исправить source-parsing aggregate status/`errors_count`, чтобы
  per-source failed candidates не выглядели как ноль ошибок.
- [ ] Добавить health/ops alert на повторяющийся Smart Update exception across
  import kinds вместо ожидания ручной live-проверки.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: canonical incident tests `3 passed` (включая exact
  Telegram replay); релевантный Smart Update, KGD80, Telegram, source-status и
  video persistence/export набор `111 passed`; release CI pending
- post-deploy verification: pending

## Prevention

Incident replay обязан пересекать тот же server-import/Smart Update boundary и
включать реальный poster object, а не только semantic festival payload. Любой
adapter, которому нужен единый provenance URL, использует узкий helper с
явным managed-first contract.
