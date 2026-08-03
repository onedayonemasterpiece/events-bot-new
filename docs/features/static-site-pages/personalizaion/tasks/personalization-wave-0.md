# Задание кодовому агенту: P13N-00 legacy quarantine + characterization + extraction

> **Статус:** готово к исполнению после merge/rebase документационного PR.
> **Цель:** получить один импортируемый cross-page personalization runtime skeleton и доказанный surface inventory **без изменения пользовательского поведения и без remote writes**.
> **Запрещено:** считать эту волну готовой персонализацией или превращать старую формулу в целевую модель.

## 1. Исходные документы

Прочитать до изменений:

1. `docs/features/static-site-pages/personalizaion/requirements.md`;
2. `docs/features/static-site-pages/personalizaion/personalization-to-be.md`;
3. `docs/features/static-site-pages/personalizaion/personalization-research-traceability.md`;
4. `docs/features/static-site-pages/personalizaion/personalization-implementation-contract.md`;
5. `docs/features/static-site-pages/personalizaion/personalization-current-runtime-audit-2026-08-02.md`;
6. `docs/features/static-site-pages/personalizaion/implementation-status.yml`;
7. `docs/architecture/personalization-data-ownership.md`;
8. открытый legal gate PR #266 и его фактический merge status;
9. `site/src/layouts/EventLayout.astro`;
10. `site/src/lib/resilientDataClient.ts`;
11. `site/src/lib/resilientSupabaseTransport.ts`;
12. `site/src/lib/backendOperationCatalog.ts`;
13. personal-feed/related/search tests и canonical `EventCard` runtime.

Сначала обновить branch от актуального `main`; baseline SHA в отчёте должен быть
полным и фактическим, а не скопированным из этого задания.

### 1.1. Нормативная иерархия

- `personalization-to-be.md` определяет целевую продуктовую и модельную систему.
- implementation contract определяет инженерный способ её реализации.
- current runtime определяет только то, что требуется безопасно перенести,
  охарактеризовать, заменить или удалить.
- старые веса, `consent_ok`, текущая hide-семантика и существующий RPC не могут
  заполнять открытые вопросы целевой модели.
- если target-решение отсутствует в `personalization-to-be.md`, зафиксировать
  hypothesis/gap; не выбирать автоматически текущее поведение.

Перед кодом составить короткий `research-delta` список: какие пункты
`personalization-research-traceability.md` затрагивает Wave 0, какие остаются
`not started`, и почему legacy parity не является target acceptance.

## 2. Граница волны

### Входит

- characterization current behavior;
- quarantine legacy profile/scorer;
- pure-module extraction переходного поведения;
- typed target contracts без target scoring implementation;
- declarative surface registry skeleton;
- generated public HTML route inventory;
- fail-closed unknown surface;
- feature flags до `local-shadow`;
- legacy storage parser без destructive migration;
- storage-size fixture/report;
- source guard против нового giant-inline personalization code и против утечки
  legacy в target namespace.

### Не входит

- новый activation UX;
- удаление legacy consent dialog;
- изменение текущих weights/order/hide timing;
- target scorer/model weights;
- Golden personas, horizons materializer, graph, exploration или anti-bubble;
- DB migrations/RLS/RPC;
- remote action writes;
- profile materializer;
- новый localStorage format в production;
- Android/iOS live personalization run;
- изменение канонического card UI;
- изменение legal/public documents.

Если extraction требует behavior change, остановить этот кусок и оформить
отдельный follow-up P13N-01, а не прятать изменение в refactor.

## 3. Обязательная структура кода

Создать минимум:

```text
site/src/lib/personalization/contract.ts
site/src/lib/personalization/legacy/profile-v1.ts
site/src/lib/personalization/legacy/scorer-v1.ts
site/src/lib/personalization/surface-policy.ts
site/src/lib/personalization/presenter-plan.ts
site/src/lib/personalization/test-api.ts
site/src/components/personalization/PersonalizationRuntime.astro
```

В Wave 0 **не создавать**:

```text
site/src/lib/personalization/scorer.ts
site/src/lib/personalization/model.ts
```

Эти имена зарезервированы для целевой реализации по
`personalization-to-be.md`, target fixtures и model bake-off. Простое
переименование `legacy/scorer-v1.ts` в `scorer.ts` в следующих волнах запрещено.

Разрешается добавить более узкие файлы, но запрещён новый универсальный
`utils.ts`, куда складывается несвязанная логика.

### `contract.ts`

- literal unions/enums для surface, action, mode и compatibility versions;
- no `any` на public interfaces;
- явные `LegacyProfileV1`, `LegacyRankCandidateV1`,
  `LegacyRankPlanV1`, `SurfacePolicyV1`;
- отдельно определить target-facing contracts без старых numeric defaults;
- type guards/normalizers с bounded arrays/maps;
- никаких network/storage side effects.

### `legacy/profile-v1.ts`

- read-only parser текущего `ke_personalization_profile`;
- reproduces current compatibility rules только для characterization;
- не создаёт `consent_ok`;
- не удаляет legacy key;
- выдаёт sanitized fixture snapshot;
- invalid/corrupt/oversized profile → `null` + diagnostic code;
- exports и diagnostics имеют явный `legacy` marker;
- unit fixtures: empty, valid, incompatible versions, invalid UUID, legacy
  `negative_tags`, oversized arrays/maps, malformed JSON.

### `legacy/scorer-v1.ts`

Перенести без изменения behavior только как migration comparator:

- static candidate score;
- tag affinity;
- negative-interest penalty;
- price/time match;
- fatigue;
- eligibility;
- related scoring;
- personal-feed scoring;
- diversity.

Требования:

- имя файла, exports, fixtures и diagnostic ids содержат `legacy`/`v1`;
- pure deterministic functions;
- старые weights передаются только как `LegacyScoringConfigV1`;
- no DOM, localStorage, Date.now, fetch, window;
- stable tie break;
- characterization fixtures доказывают parity с current inline path;
- current semantic flaw exact-hide→negative facet **не исправлять в Wave 0**,
  но локализовать за legacy adapter и пометить P13N-01 test as expected legacy;
- target runtime не импортирует этот module для новых policies;
- новые веса, personas, horizons, graph, exploration и target business rules в
  этот module не добавляются;
- module не становится baseline winner только потому, что воспроизводит current
  code.

### `surface-policy.ts`

- registry version `collection-surfaces-v1`;
- минимум policies: unknown-static, calendar-exact-only,
  calendar-personal-tail, thematic-weak, popular-tiebreak,
  search-query-first, related-anchor-first, for-me-strong;
- unknown surface → static/no-signal;
- policies берутся из целевого документа, а не выводятся из текущей DOM-логики;
- Wave 0 может сравнивать target policy с legacy behavior и выдавать diagnostic
  drift, но не применять target rank к production DOM;
- production DOM mutation под новой policy пока выключена.

### `presenter-plan.ts`

- строит абстрактный target rank plan без DOM mutation;
- принимает current order, target ranks, frozen ids;
- гарантирует frozen prefix stability;
- unknown/static/calendar identity plan;
- unit property tests на отсутствие duplicate/drop и стабильность frozen ids;
- фактический legacy DOM presenter остаётся до следующей волны;
- legacy scorer output может подаваться только fixture comparator, а не как
  автоматический target rank source.

### `test-api.ts`

В test/preview build предоставляет только sanitized snapshot:

```text
mode
surface inventory
target policy id/version
diagnostic codes
legacy profile byte size
legacy parity plan ids/scores rounded
target shadow plan, только если он не использует legacy defaults
network request counters supplied by harness
```

Не выставляет raw profile, tokens, email, anon/session ids или full action log.
В production test API отсутствует либо возвращает только inert build marker.

### `PersonalizationRuntime.astro`

В Wave 0:

- монтируется через общий layout рядом с существующим runtime;
- не дублируется;
- mode default `characterize` только в preview/test, `off` в production;
- не меняет DOM и не пишет remote/local state;
- читает surface metadata;
- публикует sanitized diagnostic marker;
- legacy inline runtime продолжает владеть текущим behavior до P13N-01/02;
- новый component не импортирует `legacy/scorer-v1.ts` в production bundle,
  кроме явно tree-shaken preview/test characterization adapter.

Нельзя называть этот компонент production personalization engine до удаления
legacy owner и включения target scorer/materializer path.

## 4. Route/surface inventory

Создать build-time/CI checker, например:

```text
site/scripts/check-personalization-route-inventory.mjs
```

Он сканирует фактический `dist`/immutable preview и формирует:

```text
artifacts/personalization-route-inventory.json
```

Artifact не обязан коммититься, но CI summary обязан содержать counts.

Для каждого public HTML:

- relative path;
- page family;
- runtime marker count;
- declared surface ids;
- resolved target policies;
- explicit `static-only` reason;
- duplicate/missing/unknown status;
- legacy behavior mismatch diagnostic без автоматического принятия legacy.

Исключить machine files и явно documented admin/test artifacts.

Hard gate:

```text
public_html_missing_runtime = 0
public_html_duplicate_runtime = 0
collections_unknown_surface = 0
calendar_primary_non_identity_policy = 0
legacy_policy_promoted_to_target = 0
```

Если текущий сайт не может закрыть `collections_unknown_surface=0`, Wave 0
должна перечислить все gaps и fail CI только для новых regressions через
checked-in baseline. Нельзя молча классифицировать всё как thematic и нельзя
вывести target policy из текущего order.

## 5. Characterization tests

### 5.1. Legacy scorer fixtures

Снять realistic fixtures из static candidate manifest без private data:

- no profile;
- like-heavy music;
- hidden event;
- negative tag;
- free preference;
- seen event/venue fatigue;
- diversity overflow;
- stable tie;
- current event excluded;
- cancelled/postponed excluded.

Для каждого сохранить current inline output ids/scores/reasons и доказать parity
`legacy/scorer-v1.ts` с допустимой точностью fixed rounding.

Fixture path/test names содержат `legacy_characterization`. Они не используются
как target quality judgements и не задают новые expected ranks после P13N-02.

### 5.2. Legacy profile fixtures

Проверить caps, invalid versions, corrupt JSON и byte report.

### 5.3. Target surface fixtures

- unknown → static;
- calendar → identity after exact exclusions;
- thematic target policy существует, но Wave 0 shadow-only;
- related/search/popular policy ids различны;
- same route может иметь calendar primary и отдельный personal tail;
- policy sources ссылаются на target contract, а не на legacy rank function.

### 5.4. Browser characterization

На immutable preview:

- current visible order до/после mounting нового component одинаков;
- network requests одинаковы;
- localStorage bytes/keys одинаковы;
- like/not-interest/share/reset current behavior не изменён;
- no duplicate handlers;
- runtime marker ровно один;
- no console error;
- no mobile/desktop overflow regression.

Эти тесты фиксируют legacy behavior, но не превращают его в целевой acceptance.
Каждый test name должен содержать `legacy_characterization`, если защищает
известно переходное поведение.

### 5.5. Research guard fixtures

Проверить автоматически:

- `personalization-to-be.md` указан как target product/model source;
- Wave 0 не создаёт target scorer;
- legacy module недоступен новым target policies;
- отсутствующие Golden personas/horizons/exploration отмечены `not started`, а
  не «не нужны»;
- ни один старый numeric weight не попал в target config default;
- research-delta report присутствует в PR evidence.

## 6. Source guard

Добавить тест, запрещающий после Wave 0:

- новые `function rank*`, `function score*`, `localStorage.setItem` и
  personalization fetch/RPC внутри `EventLayout.astro` вне явно отмеченного
  legacy adapter block;
- новый `consent_ok`;
- новые `ke_personalization_*` keys;
- direct profile payload в новых RPCs;
- второй runtime marker;
- импорт `legacy/scorer-v1.ts` вне legacy adapter/characterization tests;
- создание target `scorer.ts` копированием или re-export legacy module;
- добавление новых model weights в legacy module;
- использование legacy fixture ranks как target quality judgements.

Цель guard — заставить дальнейшие изменения идти в target shared modules. Он не
должен ломаться от несвязанных CSS/markup изменений layout.

## 7. Feature flags

Добавить typed resolver:

```text
PUBLIC_P13N_RUNTIME_MODE=off|characterize|local-shadow
```

Wave 0 semantics:

- production default: `off`;
- secret preview/test default: `characterize`;
- `characterize`: сравнивает legacy behavior и target contracts, ничего не
  применяет;
- `local-shadow`: строит только допустимый target plan, но не меняет DOM и не
  пишет state/network;
- неизвестное значение: `off` + diagnostic;
- flag не читается произвольно в каждом module.

Server flags в этой волне не добавлять либо держать hard false без endpoints.

## 8. Проверки

Минимум:

```text
node --test <new personalization unit/contract tests>
npm/astro build used by the repository
personalization route inventory checker
research/legacy quarantine guard
existing personal-feed/runtime regression tests
existing authorized-search/card tests affected by extraction
git diff --check
```

Если полный build не может быть запущен, PR остаётся draft и честно отмечает
blocker. Regex-only unit PASS недостаточен.

## 9. Evidence в PR

Приложить:

- base/head full SHA;
- research-delta review по `personalization-research-traceability.md`;
- список quarantined/extracted legacy functions;
- подтверждение, что target scorer/model не создан;
- generated route inventory summary;
- before/after JS bytes (layout inline, legacy test chunk и target runtime
  skeleton отдельно);
- before/after localStorage keys/bytes на fixtures;
- before/after network request count;
- legacy scorer parity fixture report с явной пометкой `not target quality`;
- screenshots только если DOM/geometry неожиданно изменились;
- exact tests/runs/artifacts;
- explicit `remote writes=0`, `DB changes=0`, `production behavior change=0`;
- future research items, остающиеся `not started`;
- known P0/P1 findings, которые остаются для следующих волн.

## 10. Definition of Done

Wave 0 закрыта только если:

- legacy profile/scorer находятся в явно переходном namespace;
- target contracts импортируемы и покрыты pure tests;
- target `scorer.ts`/model weights не созданы;
- current behavior доказанно не изменился;
- route inventory воспроизводим;
- unknown surface fail closed в новом runtime;
- giant inline script больше не является разрешённой точкой расширения;
- legacy module нельзя импортировать из новых target paths;
- ни одного remote write/DB migration;
- status ledger обновлён на `P13N-00: done` с evidence links;
- research-delta review подтверждает сохранность будущих наработок;
- следующий PR P13N-01 может удалить legacy consent/storage, а P13N-02 —
  реализовать target scorer по `personalization-to-be.md` без переименования
  старой формулы.

## 11. Краткий prompt для запуска агента

```text
Реализуй P13N-00 строго по
`docs/features/static-site-pages/personalizaion/tasks/personalization-wave-0.md`.
Начни с актуального main и прочитай requirements, personalization-to-be,
personalization-research-traceability, implementation contract и открытый legal
PR #266. Помести текущие profile/scoring формулы только в
`personalization/legacy/profile-v1.ts` и `legacy/scorer-v1.ts` для
characterization; не создавай target `scorer.ts`, не переноси старые веса в
целевую модель и не выводи продуктовую истину из EventLayout. Сделай target
contracts, route/surface inventory и fail-closed runtime skeleton без изменения
production behavior, DB/remote writes или tuning. Обязательны research-delta
review, source guard против импорта legacy в target paths, realistic legacy
parity fixtures, real Astro build и честный evidence report. Не выдавай legacy
parity, regex-only tests, mock transport или draft component за работающую
production персонализацию.
```
