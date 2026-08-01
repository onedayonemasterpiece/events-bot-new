# INC-2026-07-31-poster-candidate-url Smart Update падал на `PosterCandidate.url`

Status: closed
Severity: sev1
Service: Smart Update / scheduled source parsing / VK auto-import / Telegram Monitoring
Opened: 2026-07-31
Closed: 2026-08-01 00:21 UTC
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
- 2026-07-31 19:44 UTC — release 1815 (`6fa882ab`, включая poster provenance,
  Gemma 4 minimal-thinking и poster-OCR eventness fixes) активирован на Fly;
  `/healthz` ready, SHA достижим из последующего `origin/main`.
- 2026-07-31 20:23–20:33 UTC — точечные VK catch-up runs восстановили inbox
  `11019`, `11022` и `11028`; были созданы события `7350`, `7353`, `7354` и
  обновлены связанные существующие события.
- 2026-07-31 21:26–21:27 UTC — main-reachable release 1823 (`6775815d`)
  добавил conservative bundle repair и bounded partial-roundup retry. Во время
  короткого окна снятого deploy lease параллельный release 1822 успел
  завершиться, но через 39 секунд был заменён release 1823; после этого machine
  lease был установлен снова.
- 2026-07-31 21:31–21:32 UTC — оптимизированный production replay inbox
  `11033` переиспользовал по точному source/date/start-time события `7355` и
  `7356`, создал недостающие `7357` и `7358`, связал все четыре строки через
  `vk_inbox_import_event` и завершил `ops_run.id=4993` без ошибок.
- 2026-07-31 21:41–23:00 UTC — три compensating source-parsing попытки
  (`ops_run.id=4997`, `5005`, `5007`) были последовательно прерваны внешними
  Fly releases 1825, 1827 и 1828. Completed Kaggle outputs были сохранены на
  volume, чтобы больше не перезапускать remote kernels.
- 2026-07-31 23:09 UTC — source-level replay `ops_run.id=5009` остановлен после
  обнаружения, что уже восстановленная строка снова платно проходит полный
  Smart Update из-за presentation-title mismatch при точном official ticket
  URL/date/time. Подготовлен узкий idempotency fast path и opposite controls;
  общий/безвременной URL по-прежнему направляется в LLM identity gate.
- 2026-07-31 23:22–23:31 UTC — releases 1830/1832 доставили exact ticket-slot
  lookup и полную идемпотентность неизменившегося ticket replay: повтор больше
  не перестраивает Telegraph и не ставит публичные страницы в очередь.
- 2026-07-31 23:31–2026-08-01 00:15 UTC — семь последовательных cached
  compensating runs (`5014`, `5015`, `5017`, `5018`, `5019`, `5021`, `5022`)
  завершились успешно: 210 актуальных/пограничных кандидатов проверены, 208
  восстановлены/обновлены, два уже прошедших события пропущены, failed/errors
  отсутствуют.
- 2026-08-01 00:05 UTC — release 1833 (`e39d536a`, exact `origin/main`)
  распространил тот же exact-slot contract на специализированные Philharmonia
  и Qtickets processors; final health ready.

## Root Cause

1. `PosterCandidate` — `@dataclass(slots=True)` с отдельными полями
   `supabase_url` и `catbox_url`; общего атрибута `url` в его контракте не было.
2. KGD80 source-evidence adapter в `_smart_event_update_impl` начал строить
   payload через `poster.url` без совместимого accessor или regression replay с
   реальным `PosterCandidate`.
3. Исключение возникало в общей Smart Update boundary до create/merge, поэтому
   одинаково блокировало все import surfaces с poster evidence.

Follow-up replay также обнаружил отдельный mechanical partial-commit gap:
multi-event VK row ранее мог успешно записать первые дочерние события, затем
получить semantic rejection следующего child и пометить весь inbox `rejected`,
не сохранив M:N links. Теперь committed children связываются, а row получает
bounded deferred retry; semantic решение по каждому child остаётся в Smart
Update и не заменяется regex/keyword логикой.

Deploy-interrupted source catch-up обнаружил второй mechanical idempotency gap:
Smart Update мог стилизовать terse parser title, после чего следующий exact
official ticket/date/time replay не проходил strict normalized-title fast path
и заново оплачивал весь semantic merge. Fast path теперь принимает такой
presentation mismatch только при точном canonical ticket URL и explicit slot;
aggregate source URLs, другое время и `00:00` остаются за LLM gate.

Контрольный replay обнаружил и третий mechanical no-op gap: лёгкий parser path
безусловно перестраивал Telegraph и планировал public page jobs даже при
неизменившихся `ticket_status` и `ticket_link`. Теперь такой повтор обновляет
только freshness provenance; реальное изменение билетов сохраняет прежний
rebuild contract. Philharmonia/Qtickets используют тот же exact verifier, а не
собственный широкий legacy shortcut.

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

- final deployed SHA: `e39d536a` (Fly release 1833; exact SHA в `origin/main`)
- deploy path: clean integration release after main reconciliation; active
  machine `48e419df93e078`, `/healthz` ready
- regression checks: canonical incident tests `3 passed` (включая exact
  Telegram replay); финальный Smart Update/VK/native-schema набор `107 passed`;
  final source/Antigravity/gateway набор `148 passed`; provider-path audit
  `824 files`, `allowlisted_debt=0`, `unapproved=0`
- post-deploy VK verification: `11019`, `11022`, `11028`, `11033` восстановлены;
  финальный `ops_run.id=4993`, `inbox_imported=1`, `events_created=2`,
  `events_updated=2`, `errors=[]`; mapping `11033 -> 7355,7356,7357,7358`
- runtime mirror from release 1823 activation (`2026-07-31 21:27 UTC`) through
  closure: новых `PosterCandidate.url` и native GenAI schema `ValidationError`
  нет; exact roundup scope/bundle reviews прошли
- source catch-up: `5014 yantarhall 71/71`, `5015 dramteatr 21/21`, `5017
  muzteatr 20/20`, `5018 sobor 30/30`, `5019 tretyakov 20 updated + 1 stale
  skip`, `5021 philharmonia 14/14`, `5022 qtickets 32 updated + 1 stale skip`;
  во всех семи final runs `failed_items=0`, `errors_count=0`
- release health: machine `48e419df93e078`, release 1833, `/healthz`
  `ok=true`, `ready=true`, SQLite and scheduler checks green

## Prevention

Incident replay обязан пересекать тот же server-import/Smart Update boundary и
включать реальный poster object, а не только semantic festival payload. Любой
adapter, которому нужен единый provenance URL, использует узкий helper с
явным managed-first contract.
