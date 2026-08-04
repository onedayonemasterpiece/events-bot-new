# Codex: только запуск gate и публикация новой secret link

## Цель

Не разрабатывать и не дорабатывать prelaunch-страницу. Визуальная реализация, сравнение с референсом, CSS, browser evidence и исправления выполняются вне этой задачи.

Твоя работа состоит только из двух операторских операций:

1. запустить уже готовый GitHub Actions workflow на актуальном head PR №296;
2. при зелёном результате опубликовать уже готовый immutable secret candidate и вернуть открываемую браузером ссылку.

## Репозиторий

- Repository: `onedayonemasterpiece/events-bot-new`
- PR: `#296`
- Branch: `agent/prelaunch-landing-20260803`
- Workflow: `.github/workflows/prelaunch-visual-review.yml`
- Старую immutable ссылку не переиспользовать и не перезаписывать:
  `https://kenigevents.ru/_review/XncDLSqNNhWylKNyipcd7jtXsuDTER8-0ntLOrsqfGI/`

Всегда получать наиболее свежий head PR №296 непосредственно перед запуском. Не использовать SHA из старых отчётов.

## Жёсткая граница задачи

Не изменять никакие файлы. В частности, не изменять:

- `site/src/components/PrelaunchLanding.astro`;
- `site/src/styles/prelaunch-motion.css`;
- `.github/workflows/prelaunch-visual-review.yml`;
- `site/scripts/check-prelaunch-browser.mjs`;
- тесты и документацию;
- Supabase migration/RPC;
- catalog/data ledger;
- production root;
- stable ICS;
- `main`.

Не анализировать и не исправлять дизайн. Не создавать новую страницу, ветку или PR. Не выполнять merge.

## Операция 1: запустить visual gate

Для latest head PR №296 проверить, существует ли завершённый run workflow `Prelaunch visual review` именно для этого SHA.

Если run для latest SHA отсутствует, вручную выполнить `workflow_dispatch` файла:

```text
.github/workflows/prelaunch-visual-review.yml
```

на ref:

```text
agent/prelaunch-landing-20260803
```

Использовать существующий GitHub Actions механизм, например эквивалент:

```bash
gh workflow run prelaunch-visual-review.yml \
  --repo onedayonemasterpiece/events-bot-new \
  --ref agent/prelaunch-landing-20260803
```

После dispatch определить созданный run ID и дождаться завершения.

Workflow сам:

- собирает страницу;
- запускает 23 source/release tests;
- снимает screenshots `1200×1200`, `1440×900`, `390×844`;
- сохраняет DOM и computed-style JSON;
- проверяет reduced motion и sparse motion;
- загружает artifact `prelaunch-visual-evidence-<run-id>`;
- публикует run ID, SHA и artifact name комментарием в PR №296.

Тебе не нужно скачивать, смотреть или анализировать screenshots.

### Результат gate

Публикацию продолжать только при одновременном выполнении условий:

1. run относится к latest head SHA PR №296;
2. `Prelaunch visual review` — `success`;
3. обычный `CI` для того же SHA — `success`;
4. artifact `prelaunch-visual-evidence-<run-id>` существует.

Если gate завершился `failure`, не исправлять код и не запускать публикацию. Вернуть только:

```text
Публикация заблокирована, изменений не вносил.
SHA: <sha>
Visual review run ID: <run-id>
Status: failure
Ошибка: <первая точная ошибка gate>
Artifact: prelaunch-visual-evidence-<run-id>
Новый candidate не создавался и не публиковался.
```

## Операция 2: публикация при зелёном gate

Использовать только существующий production/candidate pipeline и существующие GitHub Secrets. Не создавать другой deployment-механизм.

Существующие команды:

```bash
npm --prefix site run build:production
npm --prefix site run build:secret-candidate
npm --prefix site run check:secret-candidate
npm --prefix site run plan:secret-candidate
npm --prefix site run publish:secret-candidate
```

Source of truth:

- `site/scripts/build-production.mjs`;
- `site/scripts/build-secret-candidate.mjs`;
- `site/scripts/check-secret-candidate.mjs`;
- `site/scripts/deploy-secret-candidate-yc.mjs`.

### Последовательность

1. Checkout exact latest green SHA PR №296.
2. Сгенерировать новый криптографически случайный 256-bit base64url token без padding.
3. Собрать checked production artifact с `PUBLIC_PRELAUNCH_MODE=on`.
4. Собрать и проверить secret candidate с новым token.
5. Выполнить publication plan.
6. Из manifest получить `build_id` и `token_sha256`.
7. Установить fail-closed подтверждение:

```text
SECRET_CANDIDATE_CONFIRM=publish-secret:<build_id>:<token_sha256>
```

8. Опубликовать существующим YC Object Storage pipeline.
9. Дождаться встроенной public hash/MIME verification.
10. Проверить опубликованный URL через Chromium: HTTP 200 и успешная загрузка root CSS/JS/images.

Ожидаемый URL:

```text
https://kenigevents.ru/_review/<new-token>/
```

## Secrets

Использовать только существующие repository/environment secrets:

- `KENIGEVENTS_SITE_YC_BUCKET`;
- `KENIGEVENTS_SITE_YC_ACCESS_KEY_ID`;
- `KENIGEVENTS_SITE_YC_SECRET_ACCESS_KEY`;
- существующие endpoint, region и public-base variables при наличии.

Не печатать credentials или secret key в логах.

Если production build требует уже принятого provenance/config из прежнего успешного candidate workflow, переиспользовать тот же механизм. Не обходить checks через самодельные значения или изменение ledger.

## Инварианты

- новый object prefix;
- create-only, без overwrite;
- старая secret link неизменна;
- production root не изменяется;
- stable ICS не изменяются;
- `noindex` и `no-referrer` сохранены;
- PR не merged;
- files changed by this task: none.

## Definition of Done

Задача завершена только после фактической публикации и публичной проверки.

Финальный ответ при успехе:

```text
Готово.
Новая секретная ссылка:
https://kenigevents.ru/_review/<new-token>/

PR: #296
Published SHA: <40-char SHA>
Publish workflow run: <run ID>
Visual review run: <run ID>
Visual artifact: prelaunch-visual-evidence-<run-id>
CI: success
Public hash/MIME verification: success
Public Chromium smoke: success
Production root changed: no
Stable ICS changed: no
Files changed by this task: none
```

Не заканчивать результатом «готово к публикации»: при зелёном gate требуется именно работающий URL.
