# Codex: только публикация новой secret link

## Цель

Не разрабатывать и не дорабатывать prelaunch-страницу. Вся визуальная работа, screenshots, DOM и browser evidence выполняются отдельно через GitHub Actions.

Твоя задача — **только взять уже принятую зелёную версию из PR №296, собрать новый immutable secret candidate, опубликовать его и вернуть открываемую браузером ссылку**.

## Репозиторий и существующая работа

- Repository: `onedayonemasterpiece/events-bot-new`
- PR: `#296`
- Branch: `agent/prelaunch-landing-20260803`
- Не использовать старую ссылку и не пытаться перезаписать её:
  `https://kenigevents.ru/_review/XncDLSqNNhWylKNyipcd7jtXsuDTER8-0ntLOrsqfGI/`

Всегда checkout наиболее свежего head PR №296. Не откатываться к SHA из старых отчётов.

## Жёсткая граница задачи

Не изменять:

- `site/src/components/PrelaunchLanding.astro`;
- `site/src/styles/prelaunch-motion.css`;
- `site/scripts/check-prelaunch-browser.mjs`;
- visual tests и documentation;
- Supabase migration/RPC;
- production root;
- stable ICS;
- `main`.

Не исправлять дизайн, CSS, screenshots или browser gate. Не создавать новую реализацию страницы. Не выполнять merge PR.

## Условие старта публикации

Перед публикацией проверить latest head PR №296:

1. `CI` — success;
2. `Prelaunch visual review` — success;
3. artifact visual review существует.

Если visual review ещё выполняется — дождаться завершения. Если он упал, **не чинить его** и не публиковать: вернуть только SHA, run ID и точную ошибку.

## Единственная рабочая задача

Использовать существующий candidate pipeline и существующие GitHub Secrets. Предпочтительно повторить инфраструктурный путь предыдущей успешной публикации старой secret link, меняя только checkout SHA и новый token.

Существующие команды:

```bash
npm --prefix site run build:production
npm --prefix site run build:secret-candidate
npm --prefix site run check:secret-candidate
npm --prefix site run plan:secret-candidate
npm --prefix site run publish:secret-candidate
```

Существующие scripts являются source of truth:

- `site/scripts/build-production.mjs`;
- `site/scripts/build-secret-candidate.mjs`;
- `site/scripts/check-secret-candidate.mjs`;
- `site/scripts/deploy-secret-candidate-yc.mjs`.

### Порядок

1. Checkout latest green head PR №296.
2. Сгенерировать новый криптографически случайный 256-bit base64url token без padding.
3. Собрать checked production artifact в prelaunch mode.
4. Собрать и проверить secret candidate с новым token.
5. Выполнить publication plan.
6. Из manifest получить `build_id` и `token_sha256`.
7. Установить точное fail-closed подтверждение:

```text
SECRET_CANDIDATE_CONFIRM=publish-secret:<build_id>:<token_sha256>
```

8. Опубликовать через существующий YC Object Storage pipeline.
9. Дождаться встроенной public hash/MIME verification.
10. Открыть опубликованный URL через Playwright/Chromium и проверить HTTP 200 и загрузку root assets.

Ожидаемый URL:

```text
https://kenigevents.ru/_review/<new-token>/
```

## Secrets

Использовать только уже существующие repository/environment secrets, в частности:

- `KENIGEVENTS_SITE_YC_BUCKET`;
- `KENIGEVENTS_SITE_YC_ACCESS_KEY_ID`;
- `KENIGEVENTS_SITE_YC_SECRET_ACCESS_KEY`;
- при наличии — endpoint, region и public base URL из существующего workflow.

Никогда не печатать значения credentials или сам secret key в лог.

Если текущий production build требует особого provenance/config из предыдущего успешного candidate workflow, переиспользовать **точно тот же проверенный механизм**. Не менять catalog/data ledger и не обходить release checks новым самодельным способом. При отсутствии необходимого секрета или workflow закончить задачу точным blocker report, не расширяя scope.

## Инварианты публикации

- новый object prefix;
- create-only, без overwrite;
- старая secret link остаётся неизменной;
- production root не изменяется;
- stable ICS не изменяются;
- `noindex` и `no-referrer` сохранены;
- PR не merged.

## Definition of Done

Задача завершена только после фактической публикации и публичной проверки.

Финальный ответ:

```text
Готово.
Новая секретная ссылка:
https://kenigevents.ru/_review/<new-token>/

PR: #296
Published SHA: <40-char SHA>
Publish workflow run: <run ID>
Visual review run: <run ID>
Visual artifact: <artifact name>
CI: success
Public hash/MIME verification: success
Public Chromium smoke: success
Production root changed: no
Stable ICS changed: no
Files changed by this task: none / <only publication workflow file if unavoidable>
```

Не заканчивать результатом «готово к публикации»: требуется именно работающий URL.
