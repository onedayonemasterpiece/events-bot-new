# Agent Routes (project navigation)

Цель: чтобы **агент** и **человек** быстро находили единственный актуальный документ по фиче/задаче и не плодили дубли.

## Старт

1. Открой `docs/README.md` (человеческий индекс).
2. Для “быстрого роутинга” используй `docs/routes.yml` (машиночитаемая карта).
3. Для задач по E2E всегда сверяйся с `docs/operations/e2e-scenarios.md` и поддерживай этот индекс актуальным при изменении сценариев.
4. Для задач по инцидентам или при упоминании `INC-*` сразу открывай `docs/operations/incident-management.md` и `docs/reports/incidents/README.md`.

## Task-channel execution contract (critical)

- Если пользователь назначил Telegram topic/chat или другой внешний канал каналом задачи, обратной связи или приёмки, новый actionable-комментарий в нём считается продолжением текущей задачи, а не запросом на подтверждение получения.
- Нельзя отправлять acknowledgement-only/status-only сообщения и завершать ход после чтения неблокирующего feedback. В том же активном ходе нужно довести итерацию по цепочке: `прочитать feedback → обновить acceptance checklist → реализовать → проверить → опубликовать запрошенный preview/artifact → отправить один acceptance handoff`.
- Acceptance handoff должен содержать проверенные публичные ссылки/артефакты, краткое соответствие изменениям из feedback, результат проверок и только вопросы, требующие продуктового решения. Обещание «следующую версию отправлю» не является результатом и не может быть terminal response.
- Промежуточное сообщение допустимо только при реальном blocker, требующем действия/согласия пользователя: отсутствующее разрешение/секрет, destructive или paid step, либо неустранимая неоднозначность, меняющая scope. В одном сообщении укажи blocker evidence и точное требуемое действие; не проси пользователя отдельно «запустить» продолжение после неблокирующего checkpoint.
- Когда активный ход читает следующую порцию corrective feedback после handoff, он запускает следующую полную итерацию; подтверждение получения включается в новый результат, а не отправляется отдельно.
- Это правило не означает фоновый polling: не обещай always-on мониторинг внешнего канала, если отдельный watcher действительно не настроен.
- Для preview-задач `publish` означает новый изолированный noindex preview/artifact, а не production promotion. Production/destructive публикация остаётся под release/consent gates.
5. Для задач про фестивальный мониторинг, фестивальную очередь, `/start` → «Добавить событие» в связке с публикациями, или VK-посты фестивалей сразу открывай `docs/backlog/features/festival-monitoring-debt/README.md` и regression record `docs/reports/incidents/INC-2026-06-08-festival-vk-aggregate-regression.md`.
6. Для production Telegram UI E2E (`@events_love39_bot`, `/tg`, `/vk_auto_import`, `/fest_queue`, live button checks) используй project skill `prod-telegram-e2e` и секцию `Production Telegram UI E2E` в `docs/operations/e2e-testing.md`.
7. Если пользователь даёт ссылку на Telegram-пост/канал (`t.me/...`, `https://t.me/...`) и нужно прочитать фактическое содержимое сообщения, по умолчанию используй Telethon human session (`TELEGRAM_AUTH_BUNDLE_E2E`/`TELEGRAM_SESSION`) через project skill `telegram-link-inspection`; публичный HTML `t.me/s/...` допустим только как fallback/быстрая эвристика и должен быть явно назван fallback.
8. Для любого production incident, user-visible regression, missed/failed publication, wrong/duplicate event data или сообщения, начинающегося с `Инцидент`, сначала используй project skill `events-bot-incident-response`; затем подключай более узкие skills (`events-bot-runtime-logs`, `fly-prod-db-access`, `telegram-link-inspection`, VK/Kaggle skills) по evidence surface.
9. Для любого изменения интерфейса статического сайта используй project skill `static-site-design-system` и канонический каталог `/lab/design-system/`.

## Static-site design system (critical)

- Новые страницы обязаны собираться из зарегистрированных токенов и компонентов дизайн-системы; page-local визуальный fork утверждённого компонента запрещён.
- Материальная переработка утверждённого компонента создаёт следующую явную версию (`vN+1`) в реестре и каталоге. Старая и новая версии показываются рядом до sign-off; старая получает `deprecated` и ссылку на замену.
- Изменение не завершено, пока все production consumers не переведены на новую версию, либо временное сосуществование не оформлено feature flag, списком consumers, owner и сроком удаления. Тихая смесь версий блокирует release.
- В том же commit обновляются runtime-компонент, `/lab/design-system/`, version/migration contract checks, каноническая документация, test scenarios, release evidence и `CHANGELOG.md`.

### Cross-repository UI round trip (critical)

- Полный authority/lifecycle-контракт находится в
  `onedayonemasterpiece/lovekgd-design-system`, документ
  `docs/ui-source-of-truth-roundtrip.md`. Локальный bridge —
  `docs/features/static-site-pages/design-system/README.md`. Не создавай второй
  независимый вариант этого процесса в `events-bot-new`.
- До promotion конкретного family этот репозиторий и exact generated runtime
  остаются источником факта о текущем AS-IS UI, но не нормализованной дизайн-
  системой. После promotion компонент должен приходить из pinned versioned
  design-system package; локально редактируемый fork запрещён.
- Первичная реконструкция идёт только как `exact Astro/runtime → Git SoT UI в
  lovekgd-design-system → native Penpot candidate → owner review`.
- Комментарии Penpot не реализуются напрямую в Astro. Обязательная цепочка:
  `file-scoped comment ingestion/dedupe → owner disposition → Git SoT first →
  Penpot reconciliation/read-back → owner Penpot acceptance`.
- Фактическая интеграция изменения в `events-bot-new` разрешена только после
  явного owner acceptance ограниченного Penpot candidate и фиксации точного
  contract version/hash в Git SoT.
- После Penpot acceptance изменение реализуется в isolated branch/package
  candidate, проходит three-way conformance и публикуется как immutable noindex
  preview с exact Git/package SHA для отдельного phone/desktop review. Preview
  не является production promotion.
- Для каждого page archetype обязателен visual-parity gate из канонического
  `lovekgd-design-system/docs/ui-source-of-truth-roundtrip.md`: зафиксировать
  exact Astro SHA/route/fixture/viewport/DPR/browser/fonts/state, снять
  детерминированный screenshot, поместить его в Penpot как locked source
  evidence, рядом собрать тот же экран только из linked components, экспортировать
  reconstruction в том же размере и отсмотреть side-by-side, 50% overlay/blink
  и diff. Необъяснённое отличие возвращает работу к SoT/component gate; raster
  и archetype-local patch не становятся источником истины.
- При обратной интеграции isolated Astro candidate повторно снимается на тех же
  fixtures и сравнивается с accepted Penpot reconstruction до phone/desktop
  review. Без этой сверки production generation запрещена.
- Production generation/deploy разрешены только после явного owner approval
  browser/device результата, полной миграции consumers, всех design-system и
  release gates и post-deploy conformance. Penpot acceptance, resolved comment
  или green preview сами по себе production не разрешают.
- Срочный production incident допускает только reversible mitigation по
  incident/release governance. Он не становится новым design baseline и обязан
  породить последующую сверку `runtime evidence → Git SoT → Penpot → owner
  disposition` до окончательного закрытия.

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
- Production Telegram UI E2E запускается только в `@events_love39_bot`; локальный `.env` используется только для human Telethon session/API id/hash. Не определяй production bot через локальный `TELEGRAM_BOT_TOKEN`, потому что он может указывать на тестовый `@eventsbotTestBot`.
- Перед production admin UI E2E сверяй Telethon `get_me()` с production DB `/data/db.sqlite`, таблица `user`, `is_superadmin=1`; если grant отсутствует, запроси явное разрешение перед изменением prod DB.
- Если production bot молчит на команду, сразу смотри `/data/runtime_logs/events-bot.log` и rotated logs по времени/user_id/update id/команде; webhook `200` без ответа часто означает штатный access-check return, а не сетевую поломку.

## Session Boundaries (critical)

- Telegram auth bundles are **role-scoped** and must not be repurposed without explicit user permission.
- `TELEGRAM_AUTH_BUNDLE_S22` is reserved for **Kaggle / remote monitoring** runs.
- `TELEGRAM_AUTH_BUNDLE_E2E` (or `TELEGRAM_SESSION`) is reserved for **local live E2E / Telethon human client** runs.
- Never switch Kaggle guide monitoring from `TELEGRAM_AUTH_BUNDLE_S22` to `TELEGRAM_AUTH_BUNDLE_E2E` on your own, even as a temporary workaround.
- Never run the same auth bundle concurrently in multiple places when one of them is Kaggle/remote, because Telegram can invalidate the auth key with `AuthKeyDuplicatedError`.
- For `guide_monitoring`, an existing `kaggle_registry` entry with Kaggle status `UNKNOWN` or Kaggle API status errors (especially `GetKernelSessionStatus` HTTP 5xx) must be treated as an active remote Telethon session. Do not remove that registry entry and do not start a new guide Kaggle run until there is terminal Kaggle evidence, fresh output has been imported, or the user explicitly confirms the old auth bundle/session can be abandoned after replacement.
- If the intended bundle is broken or missing, stop and report it clearly instead of borrowing another bundle.

## Правила раскладки

- **Фича** → `docs/features/<feature>/README.md` + дочерние файлы в этой же папке.
- **Операции/эксплуатация** → `docs/operations/`.
- **Архитектура** → `docs/architecture/`.
- **Пайплайны/парсеры** → `docs/pipelines/`.
- **LLM/промпты** → `docs/llm/`.
- **Справочники** (локации/праздники/шаблоны) → `docs/reference/`.
- **Бэклог/задачи** (ещё не реализовано) → `docs/backlog/`.
- **Техдолг фестивального мониторинга** → `docs/backlog/features/festival-monitoring-debt/README.md`.
- **Отчёты/планы/ретроспективы** → `docs/reports/`.
- **Тулзы/шпаргалки** → `docs/tools/`.

## Обязательное сопровождение изменений

- Любое изменение кода/поведения должно сопровождаться обновлением канонической документации в `docs/` по этой фиче (без дублей, только в одном актуальном месте).
- Любое изменение кода/поведения должно сопровождаться записью в `CHANGELOG.md` в секции `[Unreleased]` (кратко и по существу: Added/Changed/Fixed).
- Задача считается незавершённой, если код изменён, а документация и `CHANGELOG.md` не синхронизированы.

## LLM‑first обработка текста (важно)

### Google AI provider boundary (critical)

- Любой реальный Google/Gemini/Gemma/Antigravity вызов из агента, локального
  скрипта, Fly, Kaggle или Edge Function обязан пройти через общий атомарный
  limiter contract `google_ai_project_model_atomic_v1` (`reserve → mark_sent →
  provider → finalize`).
- Агентам запрещено делать диагностические `urllib`/`requests`/SDK-вызовы
  напрямую с `GOOGLE_API_KEY*`, даже «один тестовый запрос». Нельзя добавлять
  dangerous/manual override для обхода ledger. При недоступном limiter вызов
  fail-closed; это blocker evidence, а не повод использовать сырой ключ.
- Перед изменением или запуском Google consumer выполнять офлайн-аудит:
  `python3 scripts/inspect/audit_google_ai_provider_paths.py`. Результат должен
  иметь `unapproved=0` и `allowlisted_debt=0`.
- `quota_scope` означает Google Cloud project, а не локальный alias ключа.
  Несопоставленные ключи остаются в одном консервативном scope; разделять их
  можно только после проверенной инвентаризации key → project.

Если задача касается качества/смысла данных событий (например `title`, `description`, `search_digest`, `is_free`, `ticket_status`, work-hours/non-event классификация, venue/title semantics, duplicate/match решения), приоритет у обработки **через LLM** (промпты в `docs/llm/`, provider prompts вроде `kaggle/TelegramMonitor/telegram_monitor.py`, и LLM‑пасс в Smart Update).

Детерминированные функции допустимы как поддержка (санитайзеры, нормализация, извлечение дат/времени, узкие consistency/safety guardrail‑проверки), но они **не должны менять смысл** текста или подменять LLM‑решение широкими regex/keyword правилами.

В incident workflow это обязательный gate: перед prevention/fix нужно явно отделить семантическую проблему события/текста от механической проблемы транспорта/очереди/API. Семантические инциденты чинятся LLM-first; deterministic code допустим только как fail-closed/grounding/routing guard с negative controls.

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

## External consultant policy (Gemini Pro / Opus)

- Для внешних консультаций, архитектурной критики, deep review и acceptance/gate review по проектным решениям допускаются только:
  - **Gemini Pro class:** `gemini-3-pro-preview` или `gemini-3.1-pro-preview`;
  - **Opus through Antigravity/agy:** локальная команда `a-opus`;
  - **Opus through Claude Code:** проектный alias `Opus` из `.claude/agents/Opus.md` (если тариф/доступ активен).
- Gemini Flash / Flash-Lite / Lite / Gemma / embeddings / OpenAI и прочие модели можно использовать как вспомогательные probes, smoke checks или bulk enrichment, но **нельзя** представлять их результат как полноценное external consultant review.
- Если `gemini-3-pro-preview` и `gemini-3.1-pro-preview` недоступны (`429`, `503`, quota/capacity/billing), зафиксируй blocker evidence: точный model id, HTTP/status, provider error, redacted key lane/env name, дату и ссылку на artifact. Не подменяй Pro-review Flash/Lite-ответом и не закрывай задачу как “Gemini review complete”.
- Если Gemini Pro недоступен, допустимая замена для внешней консультации — `a-opus` или Claude `Opus`. Если Opus тоже недоступен из-за тарифа/авторизации, это отдельный blocker, а не повод понижать класс модели.
- В документации явно маркируй невалидные или низкоклассовые ответы как `supplementary probe material`, а не `external consultant review`.

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
- По умолчанию новая плановая работа идёт в отдельном worktree на
  `feature/<topic>` от свежего `origin/main`; короткие неаварийные багфиксы
  допустимы в `fix/<topic>`.
- `hotfix/<topic>` используй только для активного production incident /
  emergency-fix. Если после mitigation задача стала плановой настройкой,
  продолжай её в новой `feature/<topic>` ветке, а не в старой hotfix-ветке.
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
  - для Fly auth сначала подгрузить общий devserver token, если он есть: `set -a; . /home/dev/.config/fly/release.env; set +a`; файл должен быть `0600`, значение токена не печатать;
  - для Fly auth нельзя останавливаться на `flyctl auth whoami` / `no access token available`: обязательно проверить `~/.fly/config.yml` на `access_token` и попробовать process-local `FLY_ACCESS_TOKEN=<redacted>` или `FLY_API_TOKEN=<redacted>`; токен не печатать, в отчёте писать только факт наличия/отсутствия и результат `whoami`;
  - отсутствие именно `FLY_API_TOKEN` в `.env` не означает отсутствие release auth, если есть user-level Fly config;
  - если `/home/dev/.config/fly/release.env` отсутствует, а `~/.fly/config.yml` внезапно не содержит `access_token`, проверить, не был ли файл перезаписан WireGuard-only состоянием: `stat ~/.fly/config.yml`, `~/.fly/agent-logs/`, `~/.fly/logs/`; затем искать сохранённый токен в Codex/Claude session history без вывода секрета (`~/.codex/sessions/`, `~/.codex/logs_2.sqlite*`, `~/.claude/projects/-home-dev-projects-events-bot-new/`, `~/.claude/file-history/`) и проверять кандидатов только через process-local `FLY_ACCESS_TOKEN`/`FLY_API_TOKEN` + `flyctl auth whoami`;
  - перед процедурными вопросами пользователю по Fly auth агент обязан исчерпать user-level config/env/session-history recovery; для этого репозитория уже известен прецедент: 2026-06-06 утренний deploy читал Fly token из local config/session path, а позже `~/.fly/config.yml` был перезаписан без usable auth;
  - если после проверки user-level config/env Fly auth действительно отсутствует, следующий bootstrap — интерактивный `flyctl auth login`; не переключайся на GitHub Actions и не называй production fix доставленным до успешного manual `flyctl deploy`;
  - если CLI действительно отсутствует, агент должен установить его или предложить минимальный reproducible bootstrap, а не объявлять отсутствие инструмента достаточным оправданием остановки;
  - фразы вида "локально нет `flyctl`" не считаются допустимым closure/release explanation, если агент ещё не попытался self-bootstrap tooling.
- Перед deploy обязательно:
  - `git fetch origin --prune`
  - проверить branch, чистоту worktree и связь с `origin/main`
  - сверить релевантные пункты `CHANGELOG.md` с реальными commit/SHA
  - проверить, нет ли `release/*` / `hotfix/*`, которые всё ещё ahead of `origin/main`
- Production deploy выполняется только вручную через `flyctl deploy` из clean worktree; GitHub Actions deploy для этого репозитория не используется и не является допустимым release path.
- Ручной `flyctl deploy` допустим только из clean worktree; если deploy emergency и идёт не из `main`, branch должен быть запушен, SHA зафиксирован, а тот же fix обязан вернуться в `main` в рамках того же инцидента.
- Для daily/scheduled prod-задач (`cron`, ежедневные публикации, daily import/rebuild jobs) deploy не считается closure сам по себе: если из-за бага сегодняшний слот уже был пропущен или завершился аварийно, после доставки фикса нужно выполнить compensating rerun/catch-up и проверить, что текущий день больше не потерян.

## Избегаем дубликатов

- Один факт/инструкция — один “канонический” документ.
- Старые пути допускаются только как **короткие redirect-stub файлы** без повторения контента (“Актуально тут: …”).
