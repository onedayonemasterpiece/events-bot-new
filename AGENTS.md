# Agent Routes (project navigation)

Цель: чтобы **агент** и **человек** быстро находили единственный актуальный документ по фиче/задаче и не плодили дубли.

## Старт

1. Открой `docs/README.md` (человеческий индекс).
2. Для “быстрого роутинга” используй `docs/routes.yml` (машиночитаемая карта).
3. Для задач по E2E всегда сверяйся с `docs/operations/e2e-scenarios.md` и поддерживай этот индекс актуальным при изменении сценариев.
4. Для задач по инцидентам или при упоминании `INC-*` сразу открывай `docs/operations/incident-management.md` и `docs/reports/incidents/README.md`.

## Incident Mode (critical)

- Упоминание конкретного incident ID (`INC-*`) само по себе достаточно, чтобы агент перешёл в incident workflow.
- Production недоступность или user-visible деградация тоже автоматически включает incident workflow даже без готового `INC-*`: `/healthz` timeout/not ready, Fly proxy `/webhook` errors, бот не отвечает на `/start` или другие базовые команды, critical scheduled slot сорван/завис.
- В incident workflow агент обязан:
  - открыть канонический incident record;
  - трактовать его как regression contract;
  - выполнить incident-specific checks до closure/deploy;
  - если баг затронул daily/scheduled production task за текущий день, не останавливаться на фиксе и deploy: агент обязан довести инцидент до компенсирующего rerun/catch-up и проверить, что сегодняшние данные/публикация восстановлены;
  - в финальном ответе явно отчитаться по regression checks и release evidence.
- Если изменение затрагивает surface из известного incident record, агент должен поднять этот record как regression-check даже без явной просьбы пользователя.
- Если incident record отсутствует, его нужно создать из `docs/reports/incidents/TEMPLATE.md`; без этого задача по инциденту не считается корректно формализованной.

## E2E по умолчанию (важно)

- Если пользователь просит “сделай/запусти E2E” без уточнений — это **live E2E прогон**: реальные запросы и проверка через **UI в Telegram** (а не offline/фикстуры).
- При прогоне `behave` сценариев нужно **анализировать ответы из Telegram UI** (сообщения/кнопки/отчёты/логи) и результаты `behave`:
  - если в UI/логах есть ошибки/инциденты/неожиданные статусы — **расследовать и пытаться исправить** в рамках текущей задачи, а не ждать отдельного репорта от пользователя;
  - фиксировать первопричину (код/конфиг/данные), добавлять минимальные тесты/доки по месту.
- Каноническая инструкция по E2E: `docs/operations/e2e-testing.md` (подготовка/запуск/ENV) + индекс сценариев `docs/operations/e2e-scenarios.md`.
- ENV для live E2E хранится в `.env` в корне репозитория (не коммитится; шаблон — `.env.example`):
  - обязательны `TELEGRAM_BOT_TOKEN` и (`TELEGRAM_API_ID`/`TELEGRAM_API_HASH` **или** `TG_API_ID`/`TG_API_HASH`) и одна из: `TELEGRAM_AUTH_BUNDLE_E2E` или `TELEGRAM_SESSION`.
  - `behave`/`pytest` E2E подхватывают `.env` автоматически (best-effort) и **не** перетирают уже заданные переменные окружения.
  - если запускаешь бота руками из терминала, `.env` не подгружается автоматически: используй `set -a; source .env; set +a` перед `python main.py`.

## Session Boundaries (critical)

- Telegram auth bundles are **role-scoped** and must not be repurposed without explicit user permission.
- `TELEGRAM_AUTH_BUNDLE_S22` is reserved for **Kaggle / remote monitoring** runs.
- `TELEGRAM_AUTH_BUNDLE_E2E` (or `TELEGRAM_SESSION`) is reserved for **local live E2E / Telethon human client** runs.
- Never switch Kaggle guide monitoring from `TELEGRAM_AUTH_BUNDLE_S22` to `TELEGRAM_AUTH_BUNDLE_E2E` on your own, even as a temporary workaround.
- Never run the same auth bundle concurrently in multiple places when one of them is Kaggle/remote, because Telegram can invalidate the auth key with `AuthKeyDuplicatedError`.
- If the intended bundle is broken or missing, stop and report it clearly instead of borrowing another bundle.

## Правила раскладки

- **Фича** → `docs/features/<feature>/README.md` + дочерние файлы в этой же папке.
- **Операции/эксплуатация** → `docs/operations/`.
- **Архитектура** → `docs/architecture/`.
- **Пайплайны/парсеры** → `docs/pipelines/`.
- **LLM/промпты** → `docs/llm/`.
- **Справочники** (локации/праздники/шаблоны) → `docs/reference/`.
- **Бэклог/задачи** (ещё не реализовано) → `docs/backlog/`.
- **Отчёты/планы/ретроспективы** → `docs/reports/`.
- **Тулзы/шпаргалки** → `docs/tools/`.

## Обязательное сопровождение изменений

- Любое изменение кода/поведения должно сопровождаться обновлением канонической документации в `docs/` по этой фиче (без дублей, только в одном актуальном месте).
- Любое изменение кода/поведения должно сопровождаться записью в `CHANGELOG.md` в секции `[Unreleased]` (кратко и по существу: Added/Changed/Fixed).
- Задача считается незавершённой, если код изменён, а документация и `CHANGELOG.md` не синхронизированы.

## LLM‑first обработка текста (важно)

Если задача касается качества/смысла данных событий (например `title`, `description`, `search_digest`, `is_free`, `ticket_status`, work-hours/non-event классификация, venue/title semantics, duplicate/match решения), приоритет у обработки **через LLM** (промпты в `docs/llm/`, provider prompts вроде `kaggle/TelegramMonitor/telegram_monitor.py`, и LLM‑пасс в Smart Update).

Детерминированные функции допустимы как поддержка (санитайзеры, нормализация, извлечение дат/времени, узкие consistency/safety guardrail‑проверки), но они **не должны менять смысл** текста или подменять LLM‑решение широкими regex/keyword правилами.

Каноническая политика: `docs/llm/request-guide.md` (секция про LLM‑first).

## Claude / Opus policy

- Для Claude Code в этом репозитории используется только `Opus`.
- Effort для Claude Code должен быть только `high`.
- Для сложных консультаций, архитектурного разбора, deep-dive debugging и нетривиального redesign допускается временно повышать effort до `max`.
- Проектный shared-config хранится в `.claude/settings.json`; проектные инструкции Claude — в `CLAUDE.md`.
- Для консультаций, архитектурной критики, prompt review и нетривиальных доработок используй проектный subagent alias `Opus` из `.claude/agents/Opus.md`.
- Если задача LLM-first упирается в качество extraction/writer output, используй `Opus` прежде всего как эксперта по prompt design: проси prompt-family audit, конкретные prompt diffs, schema tightening и stage split по `lollipop`-принципу небольших self-contained запросов, а не общий абстрактный architecture advice.
- В shared-config запрещены встроенные Claude subagents, чтобы делегация не уходила в `Haiku`/`Sonnet`; для делегации оставляй только `Opus`.
- Не переключай Claude на `Sonnet`/`Haiku`, если пользователь явно не попросил изменить эту политику.

## Артефакты и временные файлы

- Любые результаты прогонов, дампы, логи, pid, локальные sqlite, выгрузки и т.п. → `artifacts/` (см. `artifacts/README.md`).
- Отчёты/черновики Codex CLI по умолчанию складывай в `artifacts/codex/` (см. `docs/tools/codex-cli.md`).
- Не коммить артефакты. Если нужно сохранить пример — клади **минимальный** fixture в `tests/fixtures/` (если такой паттерн уже есть).

## Runtime Logs (critical)

- Для production/scheduled/Kaggle расследований сразу открывай `docs/operations/runtime-logs.md`.
- Перед заявлением, что логи отсутствуют или потеряны, агент обязан проверить production file mirror на volume: фактические env `ENABLE_RUNTIME_FILE_LOGGING` / `RUNTIME_LOG_DIR`, директорию `/data/runtime_logs`, активный файл и rotated файлы.
- Ищи не одним grep: используй `run_id`, `ops_run` id, job kind, Kaggle kernel ref, source username, временное окно и класс ошибки.
- Если file mirror выключен или retention уже удалил нужный период, явно напиши это как найденный факт и переходи к fallback evidence: `fly logs`, Kaggle output/logs, `ops_run.details_json`, production DB rows и `artifacts/codex/`.
- Для длинных расследований сохраняй минимальные релевантные выдержки логов и JSON в `artifacts/codex/<task-or-run-id>/`; не коммить артефакты.

## Git / Push Policy

- Канонический workflow для branch/worktree и безопасной изоляции параллельной разработки: `docs/operations/repository-workflow.md`.
- Держи облачный репозиторий разумно актуальным в ходе обычной работы, а не только в конце длинной серии правок.
- После durable-изменений по текущей задаче stage/commit/push их в `origin`, если нет явного запрета пользователя и если задача не находится в промежуточной несогласованной стадии.
- Перед любым push и deploy обязательно смотри `git status` и stage файлы явно.
- По умолчанию stage/commit/push только файлы, напрямую относящиеся к текущему запросу.
- Никогда не считай грязный worktree нормальной базой для production deploy.
- Если текущий checkout грязный из-за другой незавершённой работы, это не причина бросать prod-bound задачу на полпути:
  - сначала привяжи существующую незавершённую работу к явной branch/origin-state, если она ещё не привязана;
  - затем изолируй текущую задачу в отдельный linked worktree от явной базы (`origin/main` или уже запушенной integration branch);
  - не переноси production fix из “локальной призрачной базы”, которую нельзя воспроизвести из `origin`.

## Release / Deploy Governance

- `origin/main` — единственный steady-state source of truth для production. Каноника: `docs/operations/release-governance.md`.
- `release/*` и `hotfix/*` допустимы только как короткоживущие ветки; prod-fix не считается доставленным, пока commit не достижим из `origin/main`.
- Не оставляй production-значимые фиксы только в side-ветках и не закрывай инцидент до back-merge в `main`.
- Для prod-bound задач агент обязан сам привести deploy/tooling в рабочее состояние:
  - сначала проверить стандартные локальные пути и user-level install locations для нужных CLI (`flyctl`, `gh`, и т.п.), а не только текущий `PATH`;
  - если CLI найден вне `PATH`, использовать абсолютный путь или экспортировать корректный `PATH` в текущем процессе;
  - если CLI действительно отсутствует, агент должен установить его или предложить минимальный reproducible bootstrap, а не объявлять отсутствие инструмента достаточным оправданием остановки;
  - фразы вида "локально нет `flyctl`" не считаются допустимым closure/release explanation, если агент ещё не попытался self-bootstrap tooling.
- Перед deploy обязательно:
  - `git fetch origin --prune`
  - проверить branch, чистоту worktree и связь с `origin/main`
  - сверить релевантные пункты `CHANGELOG.md` с реальными commit/SHA
  - проверить, нет ли `release/*` / `hotfix/*`, которые всё ещё ahead of `origin/main`
- GitHub Actions deploy допустим только если workflow явно checkout-ит и проверяет `main`.
- Ручной `flyctl deploy` допустим только из clean worktree; если deploy emergency и идёт не из `main`, branch должен быть запушен, SHA зафиксирован, а тот же fix обязан вернуться в `main` в рамках того же инцидента.
- Для daily/scheduled prod-задач (`cron`, ежедневные публикации, daily import/rebuild jobs) deploy не считается closure сам по себе: если из-за бага сегодняшний слот уже был пропущен или завершился аварийно, после доставки фикса нужно выполнить compensating rerun/catch-up и проверить, что текущий день больше не потерян.

## Избегаем дубликатов

- Один факт/инструкция — один “канонический” документ.
- Старые пути допускаются только как **короткие redirect-stub файлы** без повторения контента (“Актуально тут: …”).
