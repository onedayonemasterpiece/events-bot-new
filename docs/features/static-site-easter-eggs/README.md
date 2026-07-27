# Пасхалки интерфейса и артефакты Калининградской области

> **Статус:** public registry prototype / отдельный post-release track; collection
> progress, placements, application form и draw backend отсутствуют.
> **Planning branch:** `feature/static-site-artifacts-registry-20260727`.
> **Предыдущий scaffold:** `feature/static-site-easter-eggs-design-20260721@24795bf4`;
> ветка сохранена как исходный снимок, но новая проработка начата от актуального
> `origin/main`, чтобы не переносить старую release-doc chain.
> **Исходный тред:** Telegram UI review
> [`Идеи пасхалок`](https://t.me/c/4337049383/484), сообщения `485–519`,
> прочитаны через разрешённую E2E human session 2026-07-21. Redacted receipt:
> `artifacts/codex/easter-eggs-product-20260721/telegram-topic-484-receipt.json`
> (не коммитится; session secrets и полный private thread исключены), full retrieval
> SHA-256 `c4751c7d9b82766bf580682685a05c30e76ce8c4f72334eb3d8b8c0966f645eb`.

## Решение в одном абзаце

Развивать идею стоит только как **конечный, режиссируемый и измеримый формат
промо-кампании**, который знакомит с Калининградом и приводит к полезному
исследованию событий. Пасхалки не являются новой North Star и не должны
оптимизироваться по кликам, времени на сайте или completion любой ценой. Первый
technical pilot остаётся без материального приза. Целевая первая публичная
коллекция — фиксированные `8` артефактов, доступные одновременно; `60%`, то есть
`5 из 8`, открывают **форму заявки** на розыгрыш билетов, но не автоматический
entry или выигрыш. Перед prize release нужны owner acceptance, approved rules,
durable ledger, privacy/a11y/IP/legal/anti-abuse gates и holdout.

Полный критический разбор, автоматические правила и KPI:
[product-analysis.md](product-analysis.md). Готовый запрос для независимого
глубокого исследования: [external-research-brief.md](external-research-brief.md).
Результат и критическая разметка принятого/отклонённого из `agy` Gemini Pro:
[gemini-consultation-2026-07-21.md](gemini-consultation-2026-07-21.md).

## Какую задачу решает механика

1. **Региональное знание:** короткая source-grounded история о месте, объекте,
   человеке, природе или событии области.
2. **Исследование продукта:** необязательный повод открыть разные полезные
   поверхности сайта, не пряча основной контент и CTA.
3. **Кампанийный сюжет:** связать события/фестиваль/партнёра в конечную
   тематическую главу с прозрачным периодом и частотой.
4. **Возврат по смыслу:** вернуться за новой главой или продолжением уже начатого,
   а не ради бесконечной шкалы и искусственного DAU.

Главный продукт KenigEvents по-прежнему помогает быстро найти жизнеспособное
событие. Поэтому `collection_completion` — диагностическая метрика. Основной
результат пилота — incremental meaningful event/campaign action относительно
holdout при non-inferior time-to-value и основных CTA.

## Предлагаемый MVP

- одна коллекция на одну кампанию;
- proposed first set: **8 объектов**, из них одна обучающая, пять обычных и две
  сложные находки;
- все 8 существуют одновременно в опубликованном `14d` collection window;
- `60%` вычисляется как `ceil(N × 0.60)`: для первой коллекции это `5 из 8`;
- первая находка доступна без логина и без email; после неё можно предложить вход
  только как способ сохранить прогресс между устройствами;
- коллекция и правила доступны заранее; альбом прогресса открывается после первой
  находки;
- подсказки выбираются пользователем, а ограниченные proactive hints включаются
  только по автоматическому safety-контракту;
- найденный предмет не сгорает; после кампании история остаётся в архиве;
- fixed symbolic unlock: история, визуальный токен, связанная подборка/маршрут;
- первый технический pilot остаётся non-prize; ticket drawing включается только
  отдельным rules/application release;
- threshold открывает явную подачу одной заявки; completion, скорость, share,
  like, покупка или ticket click не повышают odds;
- один небольшой treatment и явный holdout на полный цикл плюс минимум неделю
  после него.

## Пользовательский цикл

```text
доступная глава кампании
→ ненавязчивый сигнал
→ добровольное исследование или запрос подсказки
→ доступная находка
→ короткая региональная история
→ прогресс коллекции
→ связанное событие/подборка как обычный CTA
→ следующая глава по опубликованному расписанию
→ completion и архив
```

Основные состояния: `latent → discovered → exploring → near_complete → completed
→ archived`; пользователь в любой момент может выбрать `hidden` для текущей
кампании. Переходы и автоматические правила определены в
[product-analysis.md](product-analysis.md#режиссура-как-конечный-автомат).

## Появление и discovery

- placement создаётся как новый вид `promo_activity`, чтобы campaign window,
  priority, caps, pause/archive, disclosure и отчётность использовали общий
  control plane;
- контент пасхалки не подделывается под `event.id`: существующая модель
  `promo_exposure` event-centric и требует отдельного first-class subject/ledger;
- пасхалка может появляться в длинной ленте между карточками, но визуально не
  считается событием или органической рекомендацией и не занимает event-impression
  ordinal;
- она не размещается внутри ticket/booking/registration CTA и не перекрывает
  основные факты/навигацию;
- обязательная коллекция имеет эквивалентный touch/keyboard/screen-reader маршрут;
  device-specific discovery допустим только как необязательный бонус без влияния
  на completion/eligibility;
- keyboard-only никогда не является единственным способом получить обязательную
  награду;
- `prefers-reduced-motion`, no-audio, no-hover и no-precision-target paths
  обязательны;
- если runtime/profile/analytics недоступны, основной static site остаётся
  полностью полезным, а слой пасхалок fail-closed исчезает.

### Стабильное назначение и найденное состояние

Пасхалка не «убегает»: после первого назначения её логический
`placement_bundle_id + placement_version` и заранее заданные anchors для
mobile/desktop/accessibility закреплены за пользователем/устройством до find или
expiry. Reload, новый визит, hint, dislike, card reorder и смена viewport не дают
reroll. Только подтверждённый safety/legal/technical blocker разрешает audited
relocation в заранее проверенный equivalent slot.

После find объект остаётся в том же месте как спокойный static marker
**«Найдено — открыть историю»** до expiry. Он больше не pulse/shimmer и не
собирается повторно; пользователь может явно скрыть found markers, не удаляя
коллекцию. `dislike` записывает оценку и Undo, но не меняет progress, место или
eligibility. Полный data/KPI/state/motion contract:
[measurement-and-state-contract.md](measurement-and-state-contract.md).

### Одинаковое место или персональное

В треде сообщения `518–519` добавили два требования: операторская страница всех
прошлых/текущих/будущих пасхалок с placement links и выбор между одинаковым и
разным местом для пользователей.

Рекомендуется не одна глобальная политика, а три campaign modes:

| Mode | Что общее | Что варьируется | Когда применять |
|---|---|---|---|
| `communal` | точная поверхность/место | только время в разрешённом окне | совместное городское обсуждение и share clues |
| `cohort` | page family и сюжетная зона | один из нескольких заранее проверенных slots по стабильному bucket | default: уменьшает spoiler effect и сохраняет сравнимую аналитику |
| `personal` | глава, факты, доступные альтернативы | verified placement pool по состоянию прогресса | только после fairness/privacy review; не в MVP |

Default MVP — `cohort`. Публичный share-card предлагает рассказать впечатление и
может раскрывать тему/тип страницы, но не обязан публиковать точную координату.
После завершения кампании можно открыть полный маршрут в архиве.

## Коллекция и «Моё»

- **коллекция** — versioned ограниченный набор, одновременно доступный одной
  аудитории в одном published window;
- текущие route, H1 и mobile navigation используют единое имя **«Для меня»**
  (`/dlya-menya/`). Вариант **«Моё»** резервируется только для будущего более
  широкого личного кабинета и сейчас не смешивается с названием страницы;
- первая строка content area — компактный teaser active collection: на телефоне
  три слота в один ряд, `Открыто X из N`, `Ещё N−X скрыто` и ссылка
  **«Открыть коллекцию»**;
- неизвестный слот не раскрывает имя или место; в current noindex
  `/dlya-menya/` sample progress явно маркирован как пример, а не реальные данные;
- коллекции образуют отдельный блок/фильтр и **не** смешиваются с хронологическим
  списком событий;
- найденные пасхалки никогда не увеличивают badge сохранённых предстоящих событий;
- закрытые slots показывают реальное число оставшихся элементов без ответа и,
  где применимо, дату окончания campaign window;
- найденный элемент хранит `egg_id`, `collection_id`, campaign/rules version,
  `found_at`, provenance/version и reward/claim state;
- anonymous/device progress явно помечается `Только на этом устройстве`; вход —
  необязательный способ синхронизации;
- anonymous→authorized merge идемпотентен и не создаёт вторую находку, completion
  или заявку;
- публичный CDN HTML не содержит частный прогресс, email или profile identifiers.

Полный контракт threshold/application/fairness:
[collection-contract.md](collection-contract.md).

## Познавательная карточка и единый реестр

Каждая единица имеет имя, source-grounded короткую историю, provenance, доступное
описание, визуальный токен масштаба медальона и один необязательный связанный
маршрут к событию/подборке. Региональная связь не может быть декоративной
догадкой.

Telegram thread до сообщения `707` дал `139` дедуплицированных candidate concepts.
Они больше не копируются списками между документами:

- public HTML: `/artefakty/`;
- public JSON: `/data/artifacts.json`;
- canonical source:
  [`site/src/data/artifactRegistry.json`](../../../site/src/data/artifactRegistry.json);
- schema/versioning:
  [artifact-registry.md](artifact-registry.md);
- source analysis:
  [telegram-ideas-analysis-2026-07-27.md](telegram-ideas-analysis-2026-07-27.md).

До production каждый candidate проходит `accept|merge|defer|reject` по фактам,
источникам, trademark/IP, визуальным правам, безопасности, доступности и freshness.
Трендовый персонаж или животное имеет ограниченный freshness window.

## Обратная связь и предложение партнёра

Страница коллекции содержит два разных сценария.

### 1. Оценить находку / сообщить о проблеме

Минимальный контракт:

- `интересно | слишком легко | слишком сложно | непонятно`;
- problem tags: `не работает | устарело/ошибка в факте | небезопасно |
  недоступно | другое`;
- необязательный комментарий и необязательный контакт;
- явное подтверждение получения; опасный physical-location report получает
  повышенный приоритет и может автоматически приостановить placement;
- feedback не требует маркетингового consent и не публикуется автоматически.

### 2. Предложить свою пасхалку

На странице всегда есть CTA **«Предложить артефакт региона»**. Первый release может
использовать прозрачный fallback:

```text
mailto:info@kenigevents.ru
?subject=Предложение артефакта Калининградской области
```

Рядом показан сам адрес `info@kenigevents.ru` и короткий шаблон: объект/место,
региональная связь, проверяемый источник, организация и контакт, права на
текст/изображение, доступность/безопасность, связь с событием или кампанией.

Целевой flow после email-MVP:

```text
получено → первичный triage → нужны детали → fact/IP/safety review
→ принято | отложено | не подходит
→ только принятый объект может быть привязан администратором к promo campaign
```

Partner proposal не создаёт кампанию автоматически, не покупает редакционное
принятие и не публикуется без модерации. Marketing consent отделён от обработки
обращения; email хранится только по утверждённой retention policy.

## Admin/control-plane

Сообщение `518` требует отдельной operator surface. Она должна показывать:

- определения и версии всех прошлых/текущих/будущих объектов и коллекций;
- campaign/activity state, schedule, caps, disclosure и rules version;
- проверенные placement IDs/URLs/page families, а не только свободный текст;
- фактические exposure/find/hint/completion и downstream metrics;
- paused/unsafe/outdated/fact/IP states;
- партнёрские предложения и audit trail решения;
- immutable prize/eligibility evidence, если отдельный legal release когда-либо
  разрешит материальную награду.

Партнёр видит только свои submissions и разрешённую агрегированную статистику;
админская inventory surface не становится публичной картой активных hidden slots.

## Promo contract

Reuse ограничен control-plane семантикой. Нужны новые first-class сущности
`collectible_definition`, `collectible_collection`, campaign binding и idempotent
find/application ledger. Bare `artifact` внутри backend не используется, чтобы не
путать domain object со static build artifacts.
Не следует подставлять fake `event_id` в текущий `promo_exposure`.

Предлагаемый activity surface: `site_easter_egg`. Версионированный config содержит:

- `collection_id`, `egg_id`/chapter;
- `discovery_mode` и `placement_pool_id`;
- eligible page families, slots и accessible alternatives;
- release schedule, campaign/local-time window, cooldown и frequency caps;
- hint ladder и fatigue/hidden behavior;
- sponsorship/editorial disclosure;
- safety stop thresholds.

`eligible`, `inserted`, `viewable`, `opened`, `collected`, `hinted` и
`completed` — разные события. Eligibility или вставка вне viewport не являются
exposure. Promo-показ не обучает organic preference model.

## Награды, social share и vector search

- Completion может открыть **заявку**, но не обещает победу.
- В первой proposed collection заявка доступна после `5 из 8`; сбор `8 из 8`,
  скорость и отсутствие hints не создают дополнительного entry.
- Любой материальный приз — отдельный release с утверждёнными organizer,
  eligibility, сроками, правилами выбора, числом призов, consent, audit,
  privacy/tax/legal и anti-fraud contracts.
- Social share доброволен. Без отдельной юридической и anti-abuse проверки он не
  меняет вероятность выигрыша и не является скрытым условием.
- Share-card не раскрывает имя/прогресс пользователя по умолчанию.
- Frontend vector search по пасхалкам остаётся research-вопросом вне MVP, пока не
  доказаны русское качество, размер, mobile performance, cache/privacy/a11y и
  преимущество над статическим поиском/фильтрами.

## Release gates

До реализации должны быть закрыты:

1. owner verdict и публичное название механики;
2. утверждённый первый набор с sources/IP/freshness;
3. clickable mobile/desktop/keyboard/reduced-motion prototype;
4. activity/subject/ledger/data-ownership architecture;
5. privacy, retention, abuse, safety, accessibility и disclosure review;
6. analytics schema, holdout, baseline/A-A, primary KPI, MDE/traffic feasibility,
   novelty-aware duration и stop rules;
7. admin inventory/kill switch и partner moderation flow;
8. separate legal gate до любого приза;
9. public-HTML/performance/SEO regression gate и independent consultant rerun
   на immutable candidate;
10. owner acceptance exact branch/SHA и documented rollback/kill plan.

Stage 13 остаётся post-release и не блокирует первую публичную презентацию. До
этих gates нельзя добавлять hidden production objects или менять scheduler.

## Открытые owner decisions

1. Copy-test hybrid naming: «пасхалки интерфейса» для discovery, «артефакты
   Калининградской области» для единиц, `/artefakty/` для реестра; текущий
   product default — принять.
2. Owner acceptance состава `Знаки Янтарного края`, `8/5` и окна `14d + 48h`.
3. Exact MVP placements и default `communal|cohort` mode.
4. Навигационный entry point к уже выбранной `/artefakty/`; private progress
   продолжает жить отдельно в `Моё`.
5. Retention/consent для обычного feedback и partner proposals.
6. Кто владеет editorial/fact/IP/safety triage и SLA ответа партнёру.
7. Exact experiment traffic, MDE и non-inferiority thresholds.
8. Перед merge объединить public registry с отдельным noindex research-прототипом
   интерактивной янтарной находки: один `/artefakty/` не должен молча вытеснить
   другой. Рекомендуемая развязка — public registry как landing, личный progress
   как отдельный дочерний route/блок в `Моё`.
8. Остаётся ли prize/social-share полностью вне первого года механики.

## Связанные документы

- [Критическая продуктовая аналитика](product-analysis.md)
- [Экологичная аналитика, KPI и state/motion contract](measurement-and-state-contract.md)
- [Единый реестр и public projection](artifact-registry.md)
- [Collection/threshold/application contract](collection-contract.md)
- [Telegram source analysis 2026-07-27](telegram-ideas-analysis-2026-07-27.md)
- [Fresh external gate receipt 2026-07-27](external-gate-2026-07-27.md)
- [Focused Gemini KPI/state prompt](gemini-kpi-state-followup-brief-2026-07-21.md)
- [Gemini Pro KPI/state consultation и disposition](gemini-kpi-state-consultation-2026-07-21.md)
- [Промпт для внешнего deep research](external-research-brief.md)
- [Gemini Pro consultation и disposition](gemini-consultation-2026-07-21.md)
- [Promo campaigns](../promo-campaigns/README.md)
- [Партнёрское промо](../promo-campaigns/partner-promo.md)
- [Мои события](../event-favorites-calendar/README.md)
- [Personalization data ownership](../../architecture/personalization-data-ownership.md)
- [Static-site design system](../static-site-pages/design-system/README.md)
- [Release readiness / Stage 13](../../reports/static-personal-announcements-release-readiness-2026-07-11.md#stage-13--feature-discovery-пасхалки-о-калининграде)
