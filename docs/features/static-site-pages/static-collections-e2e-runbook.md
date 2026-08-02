# Static collections E2E: GitHub Actions runbook

Статус: **target skeleton**, 2026-08-02.
Workflow: `.github/workflows/static-collections-quality-e2e.yml`.

## 1. Что именно проверяет этот контур

E2E разделён на независимые planes.

### Data/contract plane

Работает без production secrets и запускается на каждом релевантном PR:

- semantic labels остаются fail-closed;
- provisional seed не может быть publication truth;
- label rows структурно полны, уникальны и не пересекаются внутри label;
- declared counts совпадают с rows;
- policy и review labels согласованы;
- legacy `gold` naming, отсутствие family/provenance/raw scores отражаются как
  warnings в baseline и как errors в strict mode;
- browser contract/parser имеет собственные behavior tests.

Этот plane не утверждает, что ручная редакционная разметка правильна. Он
контролирует contracts и предотвращает случайную публикацию.

### Browser/release plane

Работает против immutable secret candidate или production root:

- public/shadow/blocked route state;
- navigation и sitemap;
- robots/noindex/canonical;
- наличие ожидаемого collection root;
- отсутствие duplicate `event_id` и `family_id`;
- существование event links;
- отсутствие console errors и page errors;
- загрузка изображений или принятого fallback;
- desktop/mobile overflow;
- report + screenshots/HTML evidence.

Зелёный browser E2E не заменяет semantic quality metrics. Зелёный data gate не
заменяет browser E2E.

## 2. Файлы

```text
.github/workflows/static-collections-quality-e2e.yml
scripts/validate_static_collections_quality.py
site/scripts/check-static-collections-e2e.mjs
site/scripts/static-collections-e2e.behavior.test.mjs
site/scripts/static-collections-e2e.contract.v1.json
```

## 3. Workflow triggers

- `pull_request` по collection/policy/manifest/Astro/E2E paths;
- `push` в `main` по тем же paths;
- ежедневный `schedule`;
- ручной `workflow_dispatch` с optional `base_url` и `strict`.

Scheduled workflow начинает существовать только после merge workflow-файла в
default branch.

## 4. Repository configuration

### Secret

```text
STATIC_COLLECTIONS_E2E_BASE_URL
```

Значение:

- предпочтительно стабильная immutable/noindex candidate URL;
- после root rollout допустим `https://kenigevents.ru/`;
- URL с review token является bearer secret и не должен попадать в Git, logs,
  artifact filenames или report.

Workflow передаёт secret только через environment. Checker не печатает base URL
и не записывает его в JSON report.

### Variable

```text
STATIC_COLLECTIONS_E2E_REQUIRED
```

Значения:

- `false` или отсутствует — bootstrap: browser job явно сообщает, что URL не
  настроен, contract plane остаётся обязательным;
- `true` — target: отсутствие URL или browser skip считается failure.

Нельзя оставлять `false` после появления стабильного candidate URL.

Дополнительно допустима переменная:

```text
STATIC_COLLECTIONS_E2E_ON_PUSH=true
```

Она включает remote browser run после релевантного push в `main`; schedule и
manual run работают независимо от неё.

### Branch protection

После первого успешного run сделать required check:

```text
Static collections quality E2E / gate
```

Не делать browser sub-job отдельным required check: единый `gate` учитывает
bootstrap/required state и не ломает PR из fork без secrets.

## 5. Route contract

`site/scripts/static-collections-e2e.contract.v1.json` — expected presentation
state, а не источник membership.

Пример:

```json
{
  "label": "science",
  "path": "/nauka/",
  "state": "blocked",
  "navigation": false,
  "sitemap": false,
  "minimum_cards": 0
}
```

States:

### `blocked`

- отсутствует в collection navigation;
- отсутствует в sitemap;
- direct route: 404/410 либо 200 с `noindex` и
  `data-publication-status="blocked"`;
- browser checker не требует event cards.

### `shadow`

- route отвечает 200;
- `noindex`;
- отсутствует в navigation и sitemap;
- имеет `data-static-collection-page`;
- содержит minimum cards и не содержит duplicate family.

### `public`

- route отвечает 200;
- нет `noindex`;
- canonical соответствует route;
- navigation/sitemap соответствуют contract booleans;
- cards и links проходят checks.

State меняется только в PR, который содержит release evidence данного label.
Нельзя автоматически вычислять expected state из production output — тест
должен обнаруживать незапланированное включение.

`enabled=false` означает только то, что route ещё не вошёл в browser suite.
Каждый такой placeholder должен быть включён в том же PR, где появляется Astro
route. Нельзя использовать `enabled=false` для сокрытия regression уже
существующей страницы.

## 6. HTML testability contract

Collection root:

```html
<main
  data-static-collection-page
  data-collection-label="..."
  data-publication-status="public|shadow|blocked"
  data-collection-state="ready|empty|last-good|blocked|degraded"
  data-catalog-hash="..."
  data-manifest-hash="..."
>
```

Event card:

```html
<article
  data-event-card
  data-event-id="..."
  data-family-id="..."
>
  <a href="/sobytiya/.../">...</a>
</article>
```

Navigation:

```html
<nav data-static-collection-nav>...</nav>
```

Атрибуты являются regression contract. Они не должны зависеть от CSS class или
визуального текста.

## 7. Baseline и strict mode

### Baseline

Нужен, пока legacy provisional fixture ещё называется `gold`.

Baseline:

- требует fail-closed;
- проверяет existing rows/counts/IDs;
- legacy naming, missing family/provenance/scores пишет как warning;
- возвращает success при отсутствии hard contract errors.

### Strict

Включается одним PR после migration ontology/data contracts:

- review-seed schema и path;
- отдельный owner gold;
- family IDs;
- raw source quotes/refs;
- all-event scores;
- hashes;
- family disjointness;
- owner gold minimum supply.

Переключение workflow на strict без миграции запрещено; сохранение baseline
после миграции также запрещено.

## 8. Browser execution model

Checker получает base URL с path prefix. Поэтому поддерживаются:

```text
https://kenigevents.ru/
https://kenigevents.ru/_review/<token>/
```

Все contract paths разрешаются относительно base URL. Token не выводится.
Sitemap index раскрывается до дочерних sitemap-файлов также внутри candidate
prefix.

Для каждого route проверяются два viewport:

```text
1440x900
390x844
```

На failure сохраняются:

- screenshot;
- HTML с удалённым candidate prefix/token;
- redacted JSON report;
- route label/state;
- console/page error count.

Artifacts хранятся ограниченный срок, заданный workflow.

## 9. Sitemap и navigation

Contract задаёт:

```text
sitemap_path
navigation_entrypoint
navigation_selector
```

Checker:

1. загружает sitemap или sitemap index;
2. нормализует URL к route path без candidate prefix;
3. загружает navigation entrypoint;
4. извлекает только links из `data-static-collection-nav`;
5. сравнивает каждый expected route.

Broad selector `nav a` не используется: иначе unrelated site navigation может
создать ложные совпадения.

До реализации `data-static-collection-nav` browser E2E должен падать в required
mode, а не silently использовать эвристику.

## 10. Failure classification

| Code | Значение | Release |
|---|---|---|
| `policy_publication_leak` | semantic label включён без gate | NO-GO |
| `provisional_seed_publishable` | provisional seed разрешает publication | NO-GO |
| `route_unexpectedly_indexable` | blocked/shadow route indexable | NO-GO |
| `route_missing` | public/shadow route отсутствует | NO-GO |
| `navigation_mismatch` | route включён/исключён не по contract | NO-GO |
| `sitemap_mismatch` | indexability не соответствует sitemap | NO-GO |
| `duplicate_event_id` | событие дважды на странице | NO-GO |
| `duplicate_family_id` | occurrence family показана несколькими карточками | NO-GO |
| `event_link_broken` | canonical event URL не отвечает | NO-GO |
| `image_broken` | видимое изображение не загрузилось и нет fallback | NO-GO |
| `browser_console_error` | runtime error | NO-GO |
| `horizontal_overflow` | overflow > 1px | NO-GO |
| `e2e_not_configured` | нет base URL при required=false | bootstrap only |
| `e2e_url_missing` | нет base URL при required=true | NO-GO |

## 11. Local commands

Contract:

```bash
python3 scripts/validate_static_collections_quality.py \
  --mode baseline \
  --json-report /tmp/static-collections-quality.json \
  --markdown-report /tmp/static-collections-quality.md
```

Behavior:

```bash
python3 -m unittest discover -s tests \
  -p 'test_static_collection_quality_validator.py'
node --test site/scripts/static-collections-e2e.behavior.test.mjs
```

Live:

```bash
cd site
npm ci
npx playwright install chromium
node scripts/check-static-collections-e2e.mjs \
  --base-url "$STATIC_COLLECTIONS_E2E_BASE_URL" \
  --contract scripts/static-collections-e2e.contract.v1.json \
  --report /tmp/static-collections-e2e.json \
  --artifact-dir /tmp/static-collections-e2e-artifacts
```

## 12. First enablement checklist

- [ ] workflow merged into `main`;
- [ ] contract job successful;
- [ ] candidate URL exists and is stable for the run;
- [ ] secret `STATIC_COLLECTIONS_E2E_BASE_URL` created;
- [ ] route contract reflects actual blocked/shadow/public state;
- [ ] Astro emits required data attributes;
- [ ] browser run successful;
- [ ] scheduled run successful;
- [ ] variable `STATIC_COLLECTIONS_E2E_REQUIRED=true`;
- [ ] unified `gate` made required;
- [ ] report retention and bearer-token redaction reviewed.

## 13. Operational rule

A scheduled failure does not automatically roll production back, but it blocks
the next promotion and creates an investigation requirement. The previous
last-good release remains active until a verified candidate passes all gates.

Repeated scheduled failure must not be “fixed” by:

- changing route state to blocked without owner decision;
- reducing minimum card counts;
- removing hard negatives;
- disabling the workflow;
- setting `STATIC_COLLECTIONS_E2E_REQUIRED=false`;
- hiding console errors;
- accepting duplicate occurrences as different families.
