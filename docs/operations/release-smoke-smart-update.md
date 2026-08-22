# Smoke (prod/test) — релиз Smart Update + VK/TG + Festivals

## P0 release/deploy/recovery runbook

This section remains the regression checklist for
`INC-2026-08-10-smart-update-identity-terminal-loss`. PR #494 was merged as
`69ec40342`; the historical Draft-only restriction no longer applies. Any
current incident must follow its own mutation authorization and the repository
release-governance contract. For
`INC-2026-08-22-sos-dedup-veto-location-tyunin-farm`, prevention deploy must
precede census repair, and repair must use its reviewed manifest rather than
the generic duplicate utility.

### A. Review-ready, no production mutation

1. `git fetch origin --prune`; require a clean worktree based on the intended
   review head with no lost newer commits. Record final HEAD and `origin/main`.
2. Run T01–T76 targeted/static receipts, the full relevant suite,
   `python3 scripts/inspect/audit_google_ai_provider_paths.py`, compile and
   `git diff --check`. No unapproved provider path is allowed.
3. Run `Database.init()` twice and rollback rehearsal on a clone of the latest
   read-only production bundle. Require quick-check before/after, zero new FK/
   identity/occurrence conflict, allowlisted count changes only, original bundle
   byte unchanged and an explicit plan for any pre-existing conflict.
4. Run the acute A–T census and recovery command twice with strict
   `--read-only --include-discovery-misses`, a half-open `[since, until)` window
   and bounded batch (`--read-only`, `--dry-run`, `--apply` are mutually
   exclusive).
   Require identical semantic hashes and zero changes. Exact model-derived
   occurrence recovery counts may remain unavailable when raw evidence is
   missing; report carriers, occurrences and lifecycle actions separately and
   do not substitute carrier/source counts.
5. Keep the incident open while any required receipt is Partial/Blocked. Green
   CI alone is not production readiness.

### B. Future merge/deploy gate (separate approval required)

1. Merge only after review, then verify the implementation commit is reachable
   from `origin/main`. Deploy only a clean exact-main checkout and record image,
   release and in-container/repository SHA.
2. Health gate: `/healthz`, SQLite quick-check, schema/index inventory, shared
   limiter capability, scheduler configuration, runtime mirror and backlog. Stop
   on any mismatch; do not apply recovery.
3. Run one bounded configured-source canary. Prove raw packet before semantic
   selection; one primary parse or exact replay; complete manifest for no-event;
   mandatory closed `SourceNoEventReason` only for confirmed no-event; at most
   one fact-only verifier that preserves positives; accepted-only downstream;
   no technical terminal; balanced carrier/children. Prove VK raw envelope v1
   outer/copy/attachment completeness and deleted replay; legacy-incomplete
   evidence must retry.
4. Force continuation pages that repeat their fingerprint, consist only of
   duplicates, and make no deeper `(date, post_id)` progress. Require typed
   `OFFSET_DRIFT`/`NO_PROGRESS` retry/rebase, never `done`; then prove only
   empty/short/horizon/original-cursor terminal states and legacy-row reopening.
5. Observe backlog/oldest due, rate limits, reservation/actual ratio and created
   events against comparable baseline. Capacity shortage increases backlog/
   quota request; it never re-enables a semantic prefilter.
6. Because daily slots were lost, execute a bounded current-day compensating
   crawl/Telegram/parser/ticket/festival catch-up and verify canonical events and
   public fanout before declaring deploy complete.

### C. Future recovery apply (another explicit approval)

1. Freeze the read-only plan/hash and evidence availability; take a fresh backup.
2. Apply only durable requeue/source-refresh transitions in bounded batches. Do
   not direct-INSERT Event. Normal raw→OCR→typed LLM→Smart Update owns replay.
3. After each batch, compare planned/selected/resolved/retry/unavailable carrier,
   occurrence and action counts; ensure idempotent repeats and zero balance gap.
4. Stop on new unknown reason, semantic terminal without complete LLM evidence,
   technical terminal, false merge, nonshrinking backlog or DB integrity change.

### Rollback

Rollback code first. Pause ingestion rather than run a binary that cannot read
new packet/candidate/occurrence keys. Keep additive tables/nullable columns and
never restore global one-canonical-URL/one-event uniqueness without a conflict
audit. Production-data repair/reversal is a separately reviewed transaction
with before/after hashes and quick-check.

Цель: перед релизом (на **тесте**) и сразу после выката (на **проде**) подтвердить, что:
- Smart Update используется во всех потоках (`/vk_auto_import`, Telegram Monitoring, `/parse`);
- источники Telegram настроены корректно (trust + фестивальные каналы);
- импорт даёт валидные отчёты с `Telegraph`/`/log`/`ICS`, и страницы Telegraph содержат изображения (Supabase WebP / Catbox fallback);
- ticket‑сайты (pyramida/dom/qtickets), найденные в Telegram Monitoring постах, могут обогащать события через ticket-sites queue;
- операционный суточный отчёт `/general_stats` корректно работает в ручном и плановом режимах;
- обратная совместимость UI сохранена (ручные флоу не сломаны, включая добавление события через меню);
- добавление фестиваля работает.

Канонический E2E сценарий smoke: `tests/e2e/features/release_smoke_smart_update.feature` (live, через Telegram UI).

## 0) Preflight (доступы + базовые ENV)

Проверьте в `.env` (или окружении деплоя), что есть минимум:
- Telegram бот: `TELEGRAM_BOT_TOKEN`
- Live E2E (Telethon): `TELEGRAM_API_ID`/`TELEGRAM_API_HASH` (или `TG_API_ID`/`TG_API_HASH`) и одна из: `TELEGRAM_AUTH_BUNDLE_E2E` или `TELEGRAM_SESSION`
- Gemma/Gateway: `GOOGLE_API_KEY`
- Kaggle: `KAGGLE_USERNAME`, `KAGGLE_KEY`

Перед `fly deploy` проверьте `.dockerignore`: локальные дампы и кэши
(`artifacts/`, `backups/`, `tmp/`, `__pycache__/`, `.pytest_cache/`) не
должны попадать в build context. Иначе сборка и загрузка образа резко
увеличиваются, а мусор оказывается внутри `/app` на проде.

Для Telegram Monitoring также должны быть настроены delivery secrets в Kaggle (см. `docs/operations/kaggle-secrets.md`).

Быстрый локальный preflight (проверяет наличие базовых ENV и доступность Catbox/Telegram Bot API, без печати секретов):
```bash
python scripts/preflight_release_smoke.py
```

Проверка «права бота на постинг/редактирование в канале» (если используете постинг/пины):
- `python scripts/check_bot_perms.py` (требует `TELEGRAM_BOT_TOKEN`)

## 1) TG‑каналы мониторинга (UI: наличие + trust + festival)

Канонический список источников и их настройки: `docs/features/telegram-monitoring/sources.md`.

Требования smoke:
- сначала синхронизировать источники: `/tg` → «🧩 Синхронизировать источники» (добавит новые каналы на целевой БД);
- официальные источники: `trust=high`;
- фестивальные каналы: заполнен `🎪 Фестиваль → <series>` (не `—`).

## 2) Ограниченный прогон авторазбора (VK: 10 постов, TG: минимум 10 событий)

VK:
- запуск: `/vk_auto_import --limit=10`
- ожидание: есть унифицированный отчёт Smart Update (название события кликабельно и ведёт на Telegraph), есть строки `Лог:` и `ICS:`; затем финал `🏁 VK auto import завершён`

Telegram Monitoring:
- запуск: `/tg` → `🚀 Запустить мониторинг`
- ожидание: итоговый отчёт `🕵️ Telegram Monitor`
- критерий: `Событий извлечено >= 10` и `Создано + Смёрджено >= 10` (если меньше — либо лимиты/источники/доступы, либо мониторинг не видит события)

Ticket-sites queue (после мониторинга, если в постах есть ticket‑ссылки):
- запуск: `/ticket_sites_queue --info` (проверить, что очередь не пуста) и `/ticket_sites_queue` (обработка)
- критерий: для затронутого события в `/log` появляется дополнительный `parser:*` источник с ticket‑URL

## 3) `/parse` работает через Smart Update

Запустить `/parse <source> --from <YYYY-MM-DD> --to <YYYY-MM-DD>` и убедиться, что в отчёте есть секция `Smart Update (детали событий)`, название события кликабельно (Telegraph) и есть строки `Лог:` и `ICS:`.

## 4) Контрольные multi‑source события (VK + TG + /parse)

Перед финальным прогоном на проде подготовьте список «не устареющих к релизу» событий, которые гарантированно матчятся из нескольких источников.

Как подготовить:
- прогоните шаги 2–3 на тесте;
- соберите кандидатов из БД (будущие события с `event_source` содержащим одновременно `vk` + `telegram` + `site/parser`);
- зафиксируйте 3–5 контрольных событий (title/date + ссылки источников).

Шаблон отдельного сценария под контрольные события: `tests/e2e/features/release_multisource_control.feature`.

## 5) Telegraph‑картинки: Supabase (WebP) / Catbox fallback

Smoke обязан подтвердить:
- на страницах Telegraph есть изображения;
- изображения грузятся с Supabase Storage (public) (`*.supabase.co/storage/...`); Catbox (`files.catbox.moe`) допустим как fallback;
- формат постеров в Supabase по умолчанию — `image/webp` (постеры дедуплицируются по перцептивному хешу; при проблемах с Telegram cached_page/Instant View может потребоваться non‑WEBP cover через Catbox/Telegraph fallback).

## 6) Обратная совместимость (ручные флоу)

Часть проверок остаётся ручной (UI), если это часть релиза:
- ручной разбор VK‑очереди и добавление выбранного события вручную;
- прочие редкие/операторские ветки, которые не покрыты behave.

Автоматизировано в behave (live smoke):
- `/start` → «Добавить событие» → отправка фото + текста → проверка, что импорт пишет source-log через Smart Update:
  - в `/log` есть `bot:` источник,
  - в `/log` есть факты (bullets) и факт `Добавлена афиша: <URL>`.

## 7) `/general_stats` (test + prod)

Проверяем отдельно на тесте и на проде.

Минимальные ENV для планового режима:
- `ENABLE_GENERAL_STATS=1`
- `GENERAL_STATS_TIME_LOCAL=07:30`
- `GENERAL_STATS_TZ=Europe/Kaliningrad`
- валидные `OPERATOR_CHAT_ID` и `ADMIN_CHAT_ID`

Проверка на **тесте**:
- вручную вызвать `/general_stats` из superadmin-чата;
- убедиться, что бот отвечает отчётом (не `Not authorized`, не пустой ответ);
- проверить, что в блоке периода указаны `start`/`end` в `Europe/Kaliningrad` и окно ровно 24 часа;
- проверить наличие ключевых секций: `VK`, `Telegram monitoring`, `/parse runs`, `/3di runs`, `Events`, `Geo`, `Festivals`, `Tech`;
- убедиться, что в run-списках показываются статусы запусков (включая нулевые успешные прогоны, если такие были).

Проверка на **проде** (после выката):
- сразу после релиза вручную вызвать `/general_stats` и повторить проверки выше;
- дождаться ближайшего планового запуска (по `GENERAL_STATS_TIME_LOCAL`) и подтвердить доставку отчёта в оба чата: `OPERATOR_CHAT_ID` и `ADMIN_CHAT_ID`;
- если один из чатов невалиден/недоступен, проверить warning в логах и факт доставки хотя бы в доступный чат.

## Smoke прогон (behave)

Рекомендуемый профиль переменных для smoke:
```bash
export TG_MONITORING_LIMIT=10
export TG_MONITORING_DAYS_BACK=3
```

Запуск:
```bash
behave tests/e2e/features/release_smoke_smart_update.feature --no-capture
```

Важно:
- Live E2E должен анализировать **сообщения/кнопки/отчёты** в Telegram UI; любые `Результат: ошибка ...` считаются провалом (см. `docs/operations/e2e-testing.md`).
- Если в рамках релиза есть миграции схемы (например поля `telegram_source.festival_source/festival_series`) — smoke запускайте **после** применения миграции на целевой среде.
