# Возрастная маркировка события на всех публичных поверхностях

> Статус на 2026-07-17: **M5 — обязательный release blocker; canonical
> declared/assessed pipeline, Smart Update/backfill и static declared-only export
> достигли `origin/main` (`aa95900a` и последующие fixes), но shared visible
> renderer и all-surface RC evidence отсутствуют**.
>
> Current implementation authority: [Event age rating](../event-age-rating/README.md).
> Этот файл владеет именно release-surface parity и не дублирует pipeline/runbook.
>
> Scope: если у события есть подтверждённое возрастное ограничение, оно одинаково и явно отображается в каждом публичном представлении этого события. Если подтверждённого значения нет, сайт не придумывает `0+` и не извлекает значение на этапе рендера.

## Найденное состояние

Canonical model/provenance, declared-vs-assessed reconciliation, backfill/sweep и
declared-only static export уже реализованы. Production terminal sweep зафиксировал
`291/291` processed rows; это evidence pipeline, но не публичной маркировки.

Оставшийся разрыв находится после projection boundary: нет одного shared visible
formatter/component и SHA-bound evidence, что accepted declared value одинаково
показано на каждой карточке/detail/search/related/personal/favorite/festival/
transport/share surface и не расходится с JSON-LD/ICS. Поэтому M5 остаётся release
blocker, но задача больше не начинается с создания canonical field с нуля.

## Семантический контракт

Canonical event получает nullable структурированное значение, совместимое с уже используемым source key `age_restriction`:

- допустимые нормализованные значения: `0+`, `6+`, `12+`, `16+`, `18+`;
- `null` означает «подтверждённая маркировка неизвестна», а не `0+`;
- хранятся provenance/source evidence, confidence/decision version и время последнего подтверждения;
- изменение значения является effectful event update и должно запускать обычный F1/F13 static rebuild/promotion;
- age rating не смешивается с рекомендацией «для детей», возрастом участника, возрастом героя/артиста, цензом отдельной программы, `DC/FC`, требованием сопровождения взрослого или рекламной фразой «для всех возрастов»;
- если источник задаёт разные ограничения для программы и входа, Smart Update сохраняет/показывает их как разные source-grounded условия либо fail closed; нельзя молча брать максимальное число широким deterministic rule.

## LLM-first ownership

Smart Update владеет смысловым extraction/reconciliation:

1. structured source field, source text и подтверждённый OCR являются evidence, а не независимыми публичными истинами;
2. LLM решает, относится ли значение к самому событию и какое ограничение является canonical;
3. deterministic normalization допустима только для формата уже принятого значения (`«12 лет и старше»` → `12+`) и schema validation;
4. неоднозначность, конфликт источников или unsupported значение не заменяются regex/default — событие остаётся без публичной маркировки и попадает в bounded quality audit/retry;
5. описание страницы не является fallback-базой для runtime extraction: renderer читает только canonical projection.

## Публичный UI-контракт

Маркировка является коротким фактом события:

- compact form: видимый text badge `12+`;
- detail/facts form: `Возрастное ограничение · 12+` или компактный факт `Возраст · 12+`;
- значение не кодируется только цветом или иконкой;
- accessible name: `Возрастное ограничение 12+`;
- badge не перекрывает title/date/image/action и не создаёт отдельный интерактивный control;
- один shared formatter/component исключает разные подписи и значения между карточками.

Возрастная маркировка не является age-gate/авторизацией и сама по себе не скрывает страницу. Любое отдельное ограничение доступа требует другого product/legal contract.

## Обязательное покрытие

Если event projection содержит non-null `age_restriction`, маркировка обязательна на каждом event-bearing public surface:

1. главная и её общая лента, включая H1-linked event previews, если H1 будет принят;
2. `/segodnya/`, `/zavtra/`, `/vyhodnye/`, `/vystavki/`, `/populyarnoe/` и остальные category/listing pages;
3. smart-search results и public saved-search tag pages;
4. related/similar/continuation cards;
5. personal feed, personal-secret recommendation page и email-linked static selections;
6. `/izbrannoe/` и другие lifecycle-aware saved-event rows;
7. festival programme/event rows и transport-enabled event representations;
8. event detail: hero/meta or immediately adjacent quick facts, full facts block and any generated gallery CTA card that repeats event facts;
9. generated event share image/text payloads and other first-party event cards that claim to summarize the event;
10. machine derivatives where the product already repeats event facts: ICS description and structured-data/SEO projection only through a supported, validated mapping with visible-page parity.

Navigation-only controls without a concrete event, raw media thumbnails inside the same event gallery and machine endpoints that do not expose event facts do not need a repeated badge.

## Оставшиеся data/rendering changes

- подтвердить preservation/backfill regression при create/update/merge/split/source refresh;
- довести discovery/related/personal/tag/favorite projections и fingerprints до единого declared-only contract;
- one Astro formatter/component is consumed by `EventCard`, `EventListItem`, detail facts and other event-summary renderers;
- share/ICS/structured data consume the same projection, never reparse description text;
- event merge/rebuild invalidates stale derivatives so an old `6+` cannot survive after canonical `12+`;
- storage remains one compact canonical value plus bounded provenance, not a permanent raw-copy table in Supabase.

## QA и release evidence

### Data audit

- read-only full active/future audit inventories canonical values, structured source fields and source-text/OCR evidence;
- classify gaps as `source_absent|canonical_missing|conflict|invalid|projection_lost|renderer_missing`;
- every `canonical_missing` with reliable source evidence becomes a Smart Update root-cause/backfill task;
- audit separately checks false positives where an unrelated `6+`/`12+` appears in programme/description text;
- result is bound to catalog hash and RC SHA.

### Automated contract tests

- accepted `0+|6+|12+|16+|18+` survives source → Smart Update → DB → export → every projection;
- `null`, invalid and ambiguous values never become a default badge;
- merge/split/source correction changes every derivative consistently;
- listing/detail/search/related/favorite/personal/tag/festival/transport/share fixtures use the same value;
- rendered structured data and ICS cannot contradict visible HTML;
- `check:preview` fails if a non-null projected rating is absent on an inventoried event renderer.

### Playwright/visual matrix

- inventory every actual event-bearing component/page family rather than testing one event detail;
- capture `0+`, `6+`, `12+`, `16+`, `18+`, unknown and long-title/compact-card cases;
- mobile `320/360/390`, tablet `768`, desktop `1366/1440`;
- zero clipping, overlap, unreadable contrast, horizontal overflow or badge/action ambiguity;
- keyboard/screen-reader check proves useful spoken text without duplicate announcements;
- no-JS output contains the same marking.

## Release acceptance

- [ ] canonical nullable field and LLM-first reconciliation are in `origin/main` with migration/backfill/rollback;
- [ ] full RC audit has no unresolved reliable-source → canonical loss and no confirmed false age rating;
- [ ] `100%` of events with non-null canonical rating show it on `100%` of inventoried event-bearing public HTML surfaces;
- [ ] unknown events show no invented/default rating;
- [ ] every projection, share/ICS derivative and supported structured-data mapping agrees with canonical value;
- [ ] age-rating create/update/merge triggers checked static regeneration and atomic promotion;
- [ ] Playwright plus `check:preview` coverage is bound to the same RC SHA;
- [ ] owner approves the compact badge/detail-fact appearance in the frozen UI;
- [ ] monitoring reports new `canonical_missing|projection_lost|renderer_missing|conflict` incidents and routes semantic fixes back to Smart Update.

M5 closes only after source-to-public coverage is proven. Finding `12+` somewhere inside the description, adding a badge only to detail pages or defaulting missing values to `0+` does not satisfy the release requirement.
