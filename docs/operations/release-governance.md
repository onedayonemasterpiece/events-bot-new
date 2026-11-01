# Release Governance

Каноническая политика для production release, hotfix и emergency deploy.

## Source Of Truth

- `origin/main` — единственный steady-state источник истины для production.
- `release/*` и `hotfix/*` допустимы только как короткоживущие координационные ветки.
- Prod-fix не считается доставленным, пока его commit не достижим из `origin/main`.
- Запись в `CHANGELOG.md` не заменяет back-merge: если пункт changelog есть, а commit не достижим из `origin/main`, это release drift и инцидент процесса.

## Active runtime and deploy path

Production с 2026-10-03 — **RunCoveer**, SSH `runcoveer-production`, Compose
`/opt/runcoveer/deployment/compose.yaml`, service `events` / container
`deployment-events-1`. Канонический runbook: [RunCoveer deployment](../../deploy/runcoveer/README.md).
Fly — legacy/rollback-only после миграции; `scripts/deploy_fly_main.sh`,
`fly.toml`, старая Fly DB и Fly billing не относятся к текущему выпуску.
Возврат на Fly требует отдельного согласованного single-writer rollback.

1. Проверь живой контейнер: image ID, command, mounts, image revision и
   `/app/.static-site-repo-sha`; readiness — `http://127.0.0.1:8081/healthz`.
2. Собирай clean exact `origin/main`; текущий runtime SHA обязан быть его
   предком. Сохрани текущий immutable image для rollback.
3. Используй exact Git archive и `STATIC_SITE_IMAGE_REPO_SHA`. Source-layer
   reuse разрешена при неизменных Dockerfile и dependency inputs, с полной
   проверкой tracked tree и обновлением immutable provenance.
4. Сохрани live env и `/data`; согласуй image pin в repository Compose и
   активном Compose. Переключай только Events:
   `sudo docker compose -f /opt/runcoveer/deployment/compose.yaml up -d --no-deps --no-build --pull never events`.
   Сохрани production `DEPLOY_PREFLIGHT=0`; не запускай общий двухсервисный
   `deploy.sh` для Events-only обновления. Kotopogoda и её source mount не меняются.
5. Проверь readiness, scheduler и реальный product effect. Политика логов —
   [runtime-logs.md](runtime-logs.md). Исходный runbook содержит preflight,
   single-writer и rollback contracts; не подменяй их Fly-командами.

Search validation markers остаются отдельной явной операцией после успешной
доставки exact runtime SHA. Само обновление контейнера не означает Search
qualification и не публикует такой marker.

- Изменения `supabase/functions/event-search/` не доставляются обновлением Events runtime. Их
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
  компенсируется повторным Events deploy.
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
  needs no Events restart because the deployed broker calls the same v2 RPC.
  Verify `persona_busy` before expiry and `new` after expiry in the ephemeral
  SQL contract before production apply.
- GitHub Actions выполняет CI; production cutover выполняется вручную на RunCoveer.
- Не деплой грязный worktree или код, отсутствующий в `origin/main`.

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
4. Проверить SSH `runcoveer-production`, текущий контейнер/image/SHA/mounts и место на диске; production env читается только в редактированном виде. Fly auth для этого не нужен.
5. Проверить, что deploy-ветка не потеряла связь с `origin/main`
6. Сверить релевантные пункты `CHANGELOG.md` с реальными commit/SHA
7. Поднять релевантные incident records из `docs/reports/incidents/README.md` для всех затронутых prod-поверхностей и выполнить их mandatory regression checks
8. Проверить, нет ли удалённых `release/*` / `hotfix/*`, которые всё ещё ahead of `origin/main`
9. Выполнить RunCoveer release по active-runtime процедуре выше. Runtime SHA берётся из immutable файла образа, а не mutable env или remembered SHA.

## Emergency Hotfix Flow

1. Создать короткую ветку от актуального `origin/main`
2. Внести только incident-related fix
3. Прогнать таргетные тесты и smoke checks
4. Запушить ветку в `origin`
5. После merge/back-merge и зелёного CI задеплоить exact `origin/main` через
   RunCoveer Events-only Compose
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
- способ deploy: RunCoveer Events-only Compose, image tag и image ID
- ссылка на PR / merge commit, который вернул fix в `main`
- краткий список выполненных incident regression checks и где лежит их evidence
- краткая заметка, если deploy был emergency и почему нельзя было ждать обычного merge
