# Release Governance

Каноническая политика для production release, hotfix и emergency deploy.

## Source Of Truth

- `origin/main` — единственный steady-state источник истины для production.
- `release/*` и `hotfix/*` допустимы только как короткоживущие координационные ветки.
- Prod-fix не считается доставленным, пока его commit не достижим из `origin/main`.
- Запись в `CHANGELOG.md` не заменяет back-merge: если пункт changelog есть, а commit не достижим из `origin/main`, это release drift и инцидент процесса.

## Allowed Deploy Paths

- GitHub Actions deploy допустим только если workflow всегда checkout-ит именно `main` и явно проверяет SHA `origin/main`.
- Ручной `flyctl deploy` допустим из clean worktree, который явно проверен относительно `origin/main`.
- Emergency deploy из отдельной ветки допустим только для быстрого восстановления production, если одновременно выполняются все условия:
  - ветка создана от актуального `origin/main`;
  - в ветке только релевантные fix-коммиты;
  - branch уже запушен в `origin`;
  - зафиксирован точный deployed SHA;
  - сразу после восстановления prod тот же SHA возвращается в `main` через PR / merge.
- Нельзя деплоить из грязного worktree.
- Нельзя держать прод-значимые фиксы только в `release/*` или `hotfix/*` без обратного возврата в `main`.

## Pre-Deploy Checklist

1. `git fetch origin --prune`
2. Убедиться, что рабочая ветка понятна и ожидаема: `git branch --show-current`
3. Проверить чистоту дерева: `git status --short`
4. Проверить, что deploy-ветка не потеряла связь с `origin/main`
5. Сверить релевантные пункты `CHANGELOG.md` с реальными commit/SHA
6. Поднять релевантные incident records из `docs/reports/incidents/README.md` для всех затронутых prod-поверхностей и выполнить их mandatory regression checks
7. Проверить, нет ли удалённых `release/*` / `hotfix/*`, которые всё ещё ahead of `origin/main`

## Emergency Hotfix Flow

1. Создать короткую ветку от актуального `origin/main`
2. Внести только incident-related fix
3. Прогнать таргетные тесты и smoke checks
4. Запушить ветку в `origin`
5. Задеплоить через `flyctl` из этой clean branch
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
- способ deploy (`flyctl` или GitHub Actions)
- ссылка на PR / merge commit, который вернул fix в `main`
- краткий список выполненных incident regression checks и где лежит их evidence
- краткая заметка, если deploy был emergency и почему нельзя было ждать обычного merge
