# Release Governance

Каноническая политика для production release, hotfix и emergency deploy.

## Source Of Truth

- `origin/main` — единственный steady-state источник истины для production.
- `release/*` и `hotfix/*` допустимы только как короткоживущие координационные ветки.
- Prod-fix не считается доставленным, пока его commit не достижим из `origin/main`.
- Запись в `CHANGELOG.md` не заменяет back-merge: если пункт changelog есть, а commit не достижим из `origin/main`, это release drift и инцидент процесса.

## Allowed Deploy Paths

- Единственный штатный production deploy path для этого репозитория —
  `scripts/deploy_fly_main.sh` из clean checkout exact `origin/main`. Скрипт
  сам выполняет fetch/clean/SHA gates и передаёт этот SHA в Docker build;
  прямой `flyctl deploy` запрещён, потому что он может собрать актуальный код
  с устаревшей mutable-меткой версии для StaticSiteBuilder.
- Search validation marker у этого скрипта по умолчанию `none`. Только явные
  `--search-validation-profile standard|full` после успешного Fly deploy
  отправляют один `search-runtime-deployed` payload с exact site SHA, bounded
  backend revision, deployment id и telemetry-only changed surfaces. Эти
  аргументы не передаются `flyctl`; static/data/Kaggle publication marker не
  создаёт. `full` дополнительно может запросить одну selective qualification.
  `search_backend_revision` — точный `sha256:<64>` digest deployable
  `supabase/functions/event-search/` source tree, а не contract version, git SHA
  или придуманный provider deploy SHA.
- Изменения `supabase/functions/event-search/` не доставляются Fly deploy. Их
  governed release выполняется отдельно из того же clean exact `origin/main`
  через pinned local Supabase CLI и project ref:
  `supabase functions deploy event-search --project-ref <ref> --no-verify-jwt --use-api`.
  Перед deploy обязательны
  `node scripts/generate_event_search_revision.mjs` и повторный
  `node scripts/generate_event_search_revision.mjs --check`; generated constant
  входит в deploy, но исключён из собственного deterministic digest.
  После deploy обязательны side-effect-free `HEAD /functions/v1/event-search`
  с publishable `apikey`, exact `X-KenigEvents-Search-Revision`, совместимый
  `X-KenigEvents-Search-Contract` и нулём Auth/Search POST. Только затем
  deploy/health marker может ссылаться на эту backend revision. Обычный Search
  response также возвращает exact `search_backend_revision`; именно observed
  response revision попадает в health evidence даже для manual/schedule run без
  marker. Только другой валидный digest после ранее успешного HEAD блокирует
  qualification без повторного Search и без product incident; missing/malformed
  revision в error response остаётся Search failure. Edge failure не
  компенсируется повторным Fly deploy.
- Перед активацией Search production-health broker policy должна одновременно
  разрешать exact main refs legacy/health/qualification workflows и event
  classes `workflow_dispatch,schedule,repository_dispatch`. После двух manual
  HEALTHY/PASS proofs repository variable включается только при точном равенстве
  их target fingerprint/immutable tuple, site runtime SHA, backend revision и
  content/index generation ids; это fail-closed проверка «продукт между
  прогонами не менялся».
- Broker schema/RPC changes are applied only from merged exact `origin/main` in
  migration order. After the platform/replay migration
  `20260809143602_static_site_auth_broker_platform_claims.sql`, follow-up
  `20260809191607_static_site_auth_broker_short_active_claim.sql` shortens only
  a successfully completed claim to the two-minute replay TTL; applying it
  needs no Fly restart because the deployed broker calls the same v2 RPC.
  Verify `persona_busy` before expiry and `new` after expiry in the ephemeral
  SQL contract before production apply.
- GitHub Actions deploy не используется и не является допустимым release path. Если в репозитории появляется workflow, который деплоит Fly app на push/workflow_dispatch, это process drift: его нужно удалить или отключить до следующего production-bound task.
- Emergency deploy из отдельной ветки допустим только для быстрого восстановления production, если одновременно выполняются все условия:
  - ветка создана от актуального `origin/main`;
  - в ветке только релевантные fix-коммиты;
  - branch уже запушен в `origin`;
  - зафиксирован точный deployed SHA;
  - сразу после восстановления prod тот же SHA возвращается в `main` через PR / merge.
- Нельзя деплоить из грязного worktree.
- Нельзя держать прод-значимые фиксы только в `release/*` или `hotfix/*` без обратного возврата в `main`.

## Pull Request CI Gate

- PR в `main` запускает incident regression suite из `.github/workflows/ci.yaml`; эти проверки не отключаются для документационных или release-веток, потому что PR проверяется вместе с текущей базой `main`.
- Job ограничен 20 минутами и запускает pytest в verbose-режиме, чтобы зависший тест был виден по последнему имени, а runner не оставался занятым на шестичасовой лимит GitHub Actions.
- Тесты retry/backoff обязаны использовать ограниченный deterministic clock или реальный bounded sleep. Нельзя сочетать no-op mock для `asyncio.sleep` с реальным monotonic deadline: это создаёт busy loop и log storm, а не быстрый unit test.
- Тесты, создающие `Database`, обязаны закрывать SQLAlchemy engine после проверки; оставшийся worker `aiosqlite` может удерживать уже завершившийся pytest-процесс без нового вывода.
- Если CI не завершился, merge без зелёного результата допустим только после локального воспроизведения тех же файлов с явным timeout и записи blocker evidence; отменённый или timed-out run не считается успешной проверкой.

## Pre-Deploy Checklist

1. `git fetch origin --prune`
2. Убедиться, что рабочая ветка понятна и ожидаема: `git branch --show-current`
3. Проверить чистоту дерева: `git status --short`
4. Проверить локальный deploy-tooling:
   - искать нужные CLI не только в текущем `PATH`, но и в стандартных user-level install locations (`~/.fly/bin/flyctl`, `~/.local/bin`, и т.п.);
   - если CLI найден вне `PATH`, использовать абсолютный путь или экспортировать корректный `PATH` до начала deploy;
   - для Fly auth сначала подгрузить общий devserver token, если он есть: `set -a; . /home/dev/.config/fly/release.env; set +a`. Файл должен быть доступен user-level агентам на этом devserver, иметь права `0600`, а значение токена нельзя печатать в логах/ответах;
   - для Fly сначала проверить `flyctl auth whoami`; если он отвечает `no access token available`, это **не** blocker само по себе: проверить `~/.fly/config.yml` на наличие `access_token` и повторить команду с process-local export `FLY_ACCESS_TOKEN=<redacted-token>` (или `FLY_API_TOKEN=<redacted-token>`). Значение токена не печатать и не коммитить; в отчёте указывать только факт `~/.fly/config.yml access_token present` и результат `whoami`;
   - также проверить `.env` / shell env на альтернативные service-token имена, но не считать отсутствие именно `FLY_API_TOKEN` доказательством отсутствия Fly release auth;
   - если `/home/dev/.config/fly/release.env` отсутствует, а `~/.fly/config.yml` есть, но `access_token` исчез, считать это release-auth recovery case, а не поводом задавать пользователю процедурный вопрос. Сначала проверить `stat ~/.fly/config.yml` и Fly agent/client логи (`~/.fly/agent-logs/`, `~/.fly/logs/`) на момент перезаписи. Затем искать сохранённый Fly token в local session history без вывода значения: `~/.codex/sessions/`, `~/.codex/logs_2.sqlite*`, `~/.claude/projects/-home-dev-projects-events-bot-new/`, `~/.claude/file-history/`. Кандидаты проверять только process-local переменными (`FLY_ACCESS_TOKEN` / `FLY_API_TOKEN`) через `flyctl auth whoami`, в evidence писать только fingerprint/источник и `whoami`;
   - известный прецедент: 2026-06-06 утренний manual Fly deploy успешно использовал local Fly token, но позже `~/.fly/config.yml` был перезаписан WireGuard-only состоянием без usable auth. Поэтому для этого devserver `/home/dev/.config/fly/release.env` и session-history recovery являются обязательными шагами перед любым запросом к пользователю о Fly token;
   - если user-level config/env действительно не содержат Fly auth, выполнить интерактивный bootstrap `flyctl auth login` и только после успешного `flyctl auth whoami` продолжать manual deploy; GitHub Actions не является fallback для отсутствующей локальной Fly auth;
   - отсутствие CLI или auth не считается достаточным оправданием остановки, пока не проверены user-level install/config paths и не предпринят self-bootstrap/install или не подготовлен минимальный reproducible bootstrap.
5. Проверить, что deploy-ветка не потеряла связь с `origin/main`
6. Сверить релевантные пункты `CHANGELOG.md` с реальными commit/SHA
7. Поднять релевантные incident records из `docs/reports/incidents/README.md` для всех затронутых prod-поверхностей и выполнить их mandatory regression checks
8. Проверить, нет ли удалённых `release/*` / `hotfix/*`, которые всё ещё ahead of `origin/main`
9. Запускать release только через `scripts/deploy_fly_main.sh`; Dockerfile
   fail-closed без `STATIC_SITE_IMAGE_REPO_SHA`, а runtime читает SHA из
   immutable файла образа и игнорирует отличающийся legacy secret.

## Emergency Hotfix Flow

1. Создать короткую ветку от актуального `origin/main`
2. Внести только incident-related fix
3. Прогнать таргетные тесты и smoke checks
4. Запушить ветку в `origin`
5. После merge/back-merge и зелёного CI задеплоить exact `origin/main` через
   `scripts/deploy_fly_main.sh`; штатный скрипт намеренно отвергает не-main SHA
6. Если инцидент затронул daily/scheduled prod-задачу за текущий день, сразу после deploy выполнить compensating rerun/catch-up и убедиться, что сегодняшние данные/публикация восстановлены
7. Открыть или обновить PR в `main`
8. Не закрывать инцидент, пока deployed SHA не достижим из `origin/main`

## Branch Drift Audit

- Перед release и после emergency fix нужно отдельно проверять:
  - какие `release/*` / `hotfix/*` ветки ahead of `origin/main`;
  - есть ли в `CHANGELOG.md` пункты про прод-поведение, чьи commits не достижимы из `origin/main`;
  - нет ли нескольких конкурирующих “prod-like” веток с разными фиксациями одного и того же бага.
- Если такие ветки найдены, это не “нормальная рабочая грязь”, а incident process gap.

## Evidence To Record

- incident ID(s), если deploy связан с инцидентом или затрагивает известный incident surface
- deployed SHA
- branch name
- способ deploy: `flyctl` (manual)
- ссылка на PR / merge commit, который вернул fix в `main`
- краткий список выполненных incident regression checks и где лежит их evidence
- краткая заметка, если deploy был emergency и почему нельзя было ждать обычного merge
