# Floating Islands — предварительный исполняемый вариант, 2026-09-05

Статус: **draft source / не production, не завершённый FI-P1 и не A=S=P**. Owner correction уточняет существующий `pattern.detached-chrome-control-islands` (DS #47). Integration base: `9bed6f5c20078f9ec934e817662d9dbbba2bd8eb`; отдельная ветка `work/floating-islands-owner-preview-20260905`, target `agent/static-site-single-kaggle-contract`, без merge/deploy.

## Прямое уточнение владельца

Брендовое меню «Полюбить Калининград Анонсы» **не участвует в компактизации**. Его исходники, место, внешний вид и собственное поведение не изменяются. Предложенные ранее adaptive label/glyph/motion варианты global brand menu из DS v1.1 этим уточнением отменены. Для окружающей композиции это неизменяемая внешняя область, которую можно измерять, но нельзя переставлять.

Медальон может сам выполнять роль острова идентичности, если его смысл полностью эквивалентен названию. На точной подборке «Бесплатные события» отдельная надпись в floating context лишняя. Это не удаление H1, названия документа или доступного имени; «Бесплатно с детьми/на побережье» не эквивалентно одному `0 ₽` и должно сохранить уточнение.

Полка городов: compact trigger показывает «Все города», выбранный город, количество выбранных городов либо честное «Города не выбраны». При открытии — прямоугольный блок до420px с читабельными названиями, исходными checkbox controls, числом событий, явным закрытием. Выбор применяется сразу, как в существующем controller. В очень малом viewport fallback в обычный поток, не обрезка controls. No-JS сохраняет исходную полку.

Desktop нижний dock — та же семантика четырёх destinations, **не копия mobile geometry**. Первый сравниваемый вариант: шире, непрозрачная подложка из existing surface role, более различимая граница/тень, подпись рядом с иконкой, canonical `feature=32px` icons. Mobile presentation не меняется. Это гипотеза для review, не утверждение, что весь прежний dock был невидим. Не поднимать z-index поверх modal/gallery ради заметности.

## Что делает текущий код

`FloatingIslandsReview.astro` — внутренний preview adapter, подключённый через существующий MobileBottomNav. Включается **только** на non-production base `/preview-islands-…`. Обычные prefixes и production не меняют представление. `?islands=off` внутри того же preview — baseline comparison. UI не читает/пишет новый профиль, не делает fetch/telemetry, не создаёт второй menu/filter/nav controller.

- Исходный city fieldset переносится внутрь раскрытия того же `[data-listing-controls]`, не клонируется. Existing filtering, storage, count, empty-state и action semantics сохраняются. Геометрию нижних препятствий сообщает существующий shell snapshot.
- На точном Free route без дополнительных query-параметров существующая page-context button получает тот же canonical SVG медальона; её дублирующие текст/старый compact mark скрыты. Existing return-to-heading/focus/visibility controller остаётся владельцем. Hero/H1 сохраняются.
- Desktop-only стили изменяют вид существующего dock, но не его destination table, responsive mobile appearance или правила modal/CTA suppression. Дополнительное место снизу предотвращает сокращение запаса под выросшую поверхность; окончательная reachability проверяется в browser.

**Ещё не выполнено:** полная однострочная сборка всех верхних ролей, свежий same-corpus Kaggle owner preview, native P, complete family/source-projection migration и приёмка всех states. Предварительный код не объявляется merge-ready или принятым новым компонентом. Новый marker `FloatingIslandsReview` — candidate, его включение в canonical family registry и generated impact graphs должно быть завершено перед интеграцией; это не вторая registry authority.

## Проверки и доступ

Локально выполнены 15 Node pure/source tests, syntax check runtime и browser harness. Они проверяют prefix guard, semantic equivalence, summary, rectangle bounds, отсутствие brand selectors/новых сетевых функций и mobile/desktop scoping. Это не browser PASS.

`site/e2e/islands/review.mjs` запускает Chromium над **реально сгенерированными Astro-страницами**: baseline/candidate brand geometry, H1 и nav links, city controls/open/select/close/Escape, Free medallion, desktop feature icons. JSON содержит source/data/viewport identities и отдельные failures. Сетевые продуктовые операции в этом диагностическом harness запрещены; это anonymous layout, не Auth/backend acceptance.

Веточный `floating-islands-diagnostic.yml` использует существующий `local:focused`, отдельно для Popular и Free на committed fixtures, с read-only GitHub permission. Он не публикует сайт, не получает deployment secrets, не создаёт другой Kaggle/publisher. Артефакты — только PNG/JSON/TAP, **без font binaries**, private DB, cookies или credentials. Это временный draft diagnostic, не новый принятый `/qa run` scenario. Canonical registry/control-plane integration остаётся явным условием promotion; не объявляется выполненной по наличию CI YAML.

В текущем ChatGPT GitHub работает. my-data-hub/browser/Penpot установлены, но не предоставляют callable methods; штатный Kaggle preview вызвать не удалось. Поэтому эта ветка не содержит выдуманного public URL или A=S=P verdict. Далее существующий preview owner использует canonical runner, текущий frozen snapshot, prefix `preview-islands-…`, `--publish-preview` только из проверенного Kaggle artifact. Production/root/current/ICS не меняются.

## Интеграционное сопровождение и отзыв

Draft patch не меняет `Reference4MobileMenu`, brand/wordmark, EventLayout, FreeCollectionSurface, ListingControls или shared foundations. Отзыв — отключение preview-prefix/`?islands=off` либо удаление вызова adapter, не schema migration.

Для existing `[Unreleased]` root CHANGELOG подготовлена следующая запись (исторический большой changelog не заменялся неполным fetch):

> Added: preview-only Floating Islands owner candidate with protected branded menu, equivalent Free medallion identity, original-checkbox rectangular city disclosure and distinct desktop dock; no production adoption. Added focused generated-page diagnostic and source guards.

Перед интеграцией вне draft ветки эта запись переносится в корневой CHANGELOG вместе с canonical family/impact/scenario updates. Не выдавать этот explicit pending work за завершённую нормализацию. Владелец должен оценивать фактический candidate на страницах; таблица требований или вычислительная модель не заменяет публикацию.
