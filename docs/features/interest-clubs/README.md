# Клубы по интересам

## Статус

**Implementation RC approved; production disabled.** Владелец 2026-07-17 подтвердил переход от принятого research baseline PR #54 / `e1a14afc` к полной реализации release candidate. Это разрешение на код, fixtures, локальный/shadow pipeline и preview, но **не** разрешение на production migration, Smart Update enablement или публикацию.

Канонические документы:

- архитектурное решение: [ADR-001](adr-001-identity-pipeline.md);
- точный staged rollout, gates и rollback: [release plan](release-plan.md);
- исходные measured evidence и ограничения выборки: [catalog audit 2026-07-17](../../reports/interest-clubs-catalog-audit-2026-07-17.md).

## Цель и объект

Раздел `/kluby-po-interesam/` помогает найти устойчивые публичные сообщества региона и их будущие встречи. Клуб — это сохраняющаяся identity группы вокруг общего интереса, подтверждённая несколькими встречами в разные даты. Название, программа, площадка и организаторы отдельной встречи могут меняться; одна лишь похожая тема или общий источник identity не доказывает.

Не являются клубом автоматически:

- повторы одной программы/сеанса — это [linked occurrences](../linked-events/README.md);
- фестиваль или его программа — это [festival identity](../festivals/README.md);
- площадка, источник или организатор со всеми своими событиями;
- одноразовый мастер-класс, лекция, концерт, экскурсия или многодневное событие;
- тематически похожая коммерческая серия без именованного сообщества;
- закрытая частная группа без достаточного публичного evidence;
- дубли и несколько публикаций об одной встрече.

## RC product contract

### Публичная проекция

- индекс `/kluby-po-interesam/` показывает только owner-approved `confirmed` identities, прошедшие freshness gate;
- detail `/kluby-po-interesam/<stable-slug>/` показывает имя, краткую доказуемую тему, устойчивые город/площадку только при наличии evidence, ближайшие встречи и дату последней подтверждённой активности;
- event relation не переписывает смысл, title или поля canonical event;
- одна встреча может относиться к двум клубам только при отдельном явном evidence для каждого (co-hosting);
- `probable`, `needs_evidence`, `deferred`, `rejected` и private identities не попадают в public manifest;
- HTML остаётся полезным без JS; canonical/sitemap/JSON-LD строятся только из принятой static projection.

### Pipeline и LLM-first boundary

Обработка идемпотентна и versioned: canonical event change → bounded candidate retrieval → evidence packet → semantic verdict → relation/projection change → coalesced static rebuild.

Детерминированный слой может нормализовать identity anchors, искать кандидатов, проверять exact quote, схлопывать дубли/linked siblings и **отказать**. Он не имеет права семантически решить, что два события образуют один клуб. Спорные source/name/organizer/program случаи проверяет `gemma-4-31b-it` коротким split-lane prompt с bounded source packet. Relation создаётся только при `yes` и дословной supporting quote. `no`, `unclear`, invalid quote, timeout, quota/provider failure и отсутствие curated anchor дают `deferred/review` либо fail-closed `no`, но не relation. Lite не является positive fallback.

Research stand дал 22/24 grounded positives, 0/24 unsafe false positives и 46/48 safe decisions; это acceptance baseline, а не owner-approved population recall. Перед production обязателен frozen owner-approved gold и gates из [release plan](release-plan.md).

### Identity lifecycle

- **active:** approved identity с будущей встречей либо verified meeting не старше 90 дней;
- **dormant:** нет будущей встречи, последняя verified meeting старше 90, но не старше 365 дней; доступна в контролируемом архиве, не в актуальной выдаче;
- **archived:** нет verified активности более 365 дней или владелец подтвердил завершение;
- новая grounded meeting может вернуть approved identity из dormant/archive в active; история и прежний slug сохраняются;
- freshness считается по distinct meeting dates после collapse дубликатов и linked occurrences, а не по числу source posts.

### Merge, rename и split

- identity имеет неизменяемый internal id, stable canonical slug, aliases и version/audit history;
- rename по умолчанию меняет display name, не slug; смена slug допускается только с permanent redirect со всех прежних slug;
- merge выполняет reviewer: выбирает surviving id/slug, сохраняет aliases/redirects и переносит только проверенные relations; merged identity становится tombstone, а не исчезает;
- split никогда не выполняется автоматически: reviewer создаёт новые identities и распределяет relations по source evidence; спорные relations уходят в review;
- один общий event не сливает clubs: при co-hosting остаются две identities и две независимо доказанные relations.

### Linked occurrences и festivals

Linked siblings одного event identity могут все вести пользователя к доступным датам, но дают не больше одного evidence unit на distinct meeting/date. Club relation не меняет `linked_event_ids` и не заменяет event/festival identity.

Festival parent/child не становится клубом. Встреча внутри фестиваля может быть связана с клубом только если источник независимо называет club identity или явное участие/организацию клуба. Название фестиваля, площадка и program proximity недостаточны.

### Owner review

Reviewer получает candidate identity, proposed relation, normalized anchors, distinct dates, bounded source excerpts, exact quotes, source/public URLs, model/policy versions и boundary warnings (linked/festival/co-hosting/rename). Только reviewer может:

1. повысить candidate до public `confirmed`;
2. принять merge/split или canonical slug change;
3. разрешить неоднозначный co-hosting;
4. исправить relation и записать reason/audit trail;
5. архивировать identity или вернуть её в active.

Автоматика может добавлять grounded meetings к уже approved identity, но любой conflict, invalidation или identity drift возвращает relation в review и сохраняет last-good public projection до следующего принятого решения.

## Privacy и storage

- Fly SQLite владеет canonical club identity, relation, bounded provenance, review/audit state и projection version; Supabase не становится второй canonical базой клубов.
- Не храним списки участников, профили, членство, закрытые контакты, inferred demographics или «характер аудитории».
- Публично используем только source-backed сведения о сообществе и встречах; private/removed source не цитируется.
- Не копируем полные тексты событий и raw provider payloads: relation ссылается на canonical event/source ids и хранит короткую evidence quote/hash/policy/model metadata.
- Embeddings, replay matrices и benchmark payloads — versioned rebuildable artifacts с явной retention; они не публикуются и не переносятся в Supabase по умолчанию.
- Повтор с тем же catalog hash, policy/model version и evidence packet обязан быть идемпотентным.

## Вне первого production release

Первый release не включает персональное членство/подписку, CRM клубов, private recommendations, self-service создание клуба и редакционные acquisition-модули. Сценарии обнаружения через navigation, search, event detail, recommendations, editorial/deep links и продуктовый концепт **Hero Talk** сохранены как отдельный postrelease design track в [release plan](release-plan.md#postrelease-product-design-discovery-and-entry-surfaces). Hero Talk пока не является реализованным компонентом.
