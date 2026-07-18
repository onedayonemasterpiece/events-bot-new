# Сквозная продуктовая система статического сайта KenigEvents

> Дата анализа: 2026-07-18
> Статус: product decision memo / implementation backlog
> Каноническая база аудита: `docs/static-site-video-guides-20260718@1c74d65a`
> Внешняя критика: `agy`, `Gemini 3.1 Pro (High)`; итог синтезирован, а не принят автоматически.

## 1. Executive verdict

Текущего набора **недостаточно как целостного продукта**, хотя фундамент уже сильный.

Сейчас хорошо проработана вертикаль `ссылка на событие → страница события → CTA → похожие`, есть статические страницы `Сегодня / Завтра / Выходные`, отдельные `Популярное`, `Выставки` и авторизованный умный поиск. Но это пока набор leaf/listing surfaces, а не замкнутая система выбора и возврата.

Нужно строить не «ещё один каталог афиш», а **event decision engine**:

> из любого входа довести пользователя до подходящего активного события за минимальное число осмысленно просмотренных карточек, дать сохранить намерение и понятно показать, что нового появилось при следующем визите.

Главный продуктовый разрыв находится не в количестве типов страниц. Не замкнут цикл:

```text
acquisition
  → landing
  → discovery continuation
  → qualified action
  → durable/user-local signal
  → next feed edition
  → newness/update awareness
  → return/reactivation
  → новый qualified action
```

Следовательно, страницы `Сегодня / Завтра / Выходные / Популярное / Поиск` необходимы, но сами по себе недостаточны.

## 2. Что уже есть и что реально не закрыто

### Сильная база

- статические event detail pages, ICS, JSON-LD и related manifests;
- дата-листинги `Сегодня / Завтра / Выходные`;
- отдельные поверхности `Выставки`, `Популярное`, `Поиск`;
- local-first профиль и явные `like / not interested / share`;
- static-first fallback: публичный HTML не обязан зависеть от Supabase;
- принятый контракт global identity, favorites, exactly-three recommendation email и bearer personal page;
- строгая архитектурная граница Fly SQLite / Supabase / YDB;
- KPI-контракт mature golden persona: `30` valid impressions, `5` strong positives, `2` explicit negatives, не менее `3` сессий; релевантное событие должно быть достигнуто за `<=20` валидно изученных карточек при наличии релевантного supply.

### Критические разрывы

| Gap | Severity | Фактическое состояние | Product impact | Что закрывает gap |
|---|---:|---|---|---|
| Нет настоящей главной | P0 | `/` — noindex test landing; `/__preview/` — техническая страница | вход через logo/SEO не даёт discovery | `/` как статическая «Афиша»: быстрые даты, категории, подборки, начало стабильной ленты |
| Production SEO выключен | P0 | `noindex,nofollow,noarchive`, `robots: Disallow /`, preview/lab в sitemap | SEO как acquisition-канал фактически не запущен | production build profile, allowlisted sitemap, truthful `lastmod`, stable canonical/slug registry |
| Соцсети не используют один canonical resolver | P0 | действующие издатели могут продолжать вести на source/Telegraph | event/collection deep links не образуют измеримый funnel | один public-link resolver, current promoted manifest, UTM/campaign attribution, TG/VK/MAX unfurl gate |
| Нет единой системы подборок | P0 | маршруты и правила заданы по одному | сайт, соцсети и email могут выбирать разное | versioned `collection_manifest` как единый selection contract |
| Нет навигации по жанрам | P0 | есть только отдельная `Выставки` | пользователь не может перейти от события/даты к устойчивому интересу | `/kategorii/` + bounded canonical category leaves |
| Поиск заметен не везде и закрыт логином | P0/P1 | semantic search существует, anonymous quota `0`; desktop header не даёт равноправного входа | free discovery сломан до авторизации | один заметный search entry; анонимный zero-cost catalog search + authenticated semantic upgrade |
| Персонализация не замкнута | P0/P1 | `seen_event_ids` не заполняется; нет trusted valid-impression/CTA ingest; listing mostly hides dislikes | нельзя доказать «подходящее в первых 20–30» | exposure observer, outcome signals, profile maturity, served-list linkage, real next-session rerank |
| Нет durable «В планы» | P0/P1 | ICS есть, favorite/cross-device state отсутствует | пользователь не может вернуться к намерению; calendar ≠ управляемое сохранение | один durable saved-event state, `/izbrannoe/`, lifecycle updates |
| Нет модели «что изменилось» | P1 | нет visit/build watermark, new/updated state и update center | частая возвратность не имеет видимой причины | feed editions, `last_acknowledged_build`, `new_for_user`, `materially_updated`, отдельный newness indicator |
| Identity не глобальна | P1 | Yandex auth живёт в search; verified-email/profile link partial | search, save, email и personal page выглядят разными продуктами | общий identity shell на всех HTML page families |
| Recommendation email только в design | P1 | control plane силён, генератор/page publisher/worker выключены | нет owned reactivation-канала | exactly 3 + already-published larger personal page + canary `<=200` |
| Меню не отражает целевой lifecycle | P0 | mobile/desktop наборы различаются; brand/«Все анонсы» ведут в preview | пользователь не видит search, categories, saved/new | единая IA, adaptive geometry, utility `Моё избранное` и отдельный newness indicator |

## 3. Целевая продуктовая модель

### 3.1. Не одна лента, а связанный граф surfaces

Показатель `cards_to_first_relevant` должен считаться по **уникальным валидным касаниям всего journey**, а не только внутри одной бесконечной ленты:

- главная;
- дата-листинг;
- подборка/категория;
- search results;
- related block event detail;
- personal continuation;
- personal email page.

Одно событие, показанное на трёх surfaces, остаётся одним уникальным касанием для данного decision journey. Для этого нужны `journey_id`, `served_list_id`, `surface`, `event_id`, `position`, `algorithm_id`, `build_id`.

### 3.2. Сквозные маршруты

**Социальный event deep link**

```text
TG/VK/MAX event post
→ event detail
→ ticket/register/calendar/save
→ related by same intent + «Вся подборка»
→ category/date/collection continuation
→ save/profile signal
→ next edition / return
```

Если событие не подошло, страница не должна быть тупиком. Но решением не обязан быть uncontrolled infinite scroll: лучше видимая, ограниченная continuation-секция, затем явный `Показать ещё` и переход в исходную подборку.

**Социальный collection deep link**

```text
TG/VK/MAX digest
→ stable collection page
→ event card/detail
→ qualified action
→ сохранить тему / перейти в соседнюю подборку
→ новое по этой теме при следующем визите
```

**SEO**

```text
category / intent / venue / event query
→ indexable static landing
→ current event set + transparent freshness
→ event detail
→ qualified action
→ adjacent intent/date hub
```

**Search**

```text
visible one-line query
→ anonymous local catalog result
→ optional semantic refinement/login only when valuable
→ event detail / «нашёл» feedback
→ useful-query candidate
→ automatic fail-closed collection promotion
```

**Email**

```text
verified purpose-specific opt-in
→ exactly 3 events in email
→ already-published larger personal page (proposed canary: 12–24 events)
→ qualified action / feedback
→ next issue suppresses repeats and uses the same profile/feed revision
```

## 4. Итоговая информационная архитектура и меню

### 4.1. Канонический mental model

Основные destinations и порядок:

1. **Афиша** — настоящая главная `/`;
2. **Сегодня**;
3. **Завтра**;
4. **Выходные**;
5. **Категории**;
6. **Подборки**;
7. **Поиск**.

Отдельный utility cluster:

- **Моё избранное** — только distinct durable saved events и их lifecycle;
- **Профиль** — identity, email, consent/preferences, reset/export/delete.

Канонический badge `Моё избранное` показывает только число distinct durable saved events при `N>0`; likes, reminders, transport legs и newness его не увеличивают. `Новое для вас` — отдельный contextual update chip/dot на главной и в раскрытом меню с собственной семантикой. Эти два индикатора нельзя объединять.

### 4.2. Desktop

В header постоянно видны:

```text
Афиша · Сегодня · Завтра · Выходные · Категории · Подборки · Поиск
                                      Моё избранное · Профиль
```

Search желательно показывать не только ссылкой, но и как заметный короткий input/command entry на главной и крупных листингах.

### 4.3. Mobile

Постоянная компактная shortcut-навигация может быть:

```text
Афиша · Сегодня · Подборки · Поиск · Избранное
```

Раскрытая navigation sheet обязана сохранять полный канонический порядок и destinations, включая `Завтра`, `Выходные`, `Категории`. Это shortcut layer, а не другая IA. Внутри `Сегодня`/`Афиша` остаётся заметный date switch `Сегодня / Завтра / Выходные`.

### 4.4. Что не класть в top-level menu

- `Выставки` — leaf в `Категории`;
- `Популярное`, `Новое`, `Бесплатно`, `С детьми`, `Пушкинская карта` — quick links в `Подборки`;
- площадки, организаторы и города — entity/internal-link layer, не постоянные пункты;
- клубы — category/community surface после отдельного release gate;
- partners/about/privacy — footer.

## 5. Итоговые типы статических страниц

| Family | Route example | Основной intent | Источник | Index policy | Refresh | Primary next action |
|---|---|---|---|---|---|---|
| Главная | `/` | «помоги выбрать» | static collection manifests + optional local rerank | index | каждый promoted build | дата, категория, поиск, event |
| Event occurrence | `/sobytiya/<slug>/` | решение по конкретному событию | Fly canonical export | index while current; lifecycle policy after | effectful update + expiry rebuild | ticket/register/save/calendar + continuation |
| Date | `/segodnya/`, `/zavtra/`, `/vyhodnye/` | срочно/планирование | occurrence-aware selection | index | daily/build | event |
| Calendar horizon | `/na-nedele/`, later month/date | планирование заранее | occurrence manifest | index only with useful supply | daily | event/date |
| Category hub | `/kategorii/` | обзор жанров/форматов | versioned taxonomy | index | taxonomy/build | category leaf |
| Category leaf | `/kategorii/koncerty/` | устойчивый жанровый intent | normalized taxonomy + active events | index if quality/inventory gate | build | event/related collection |
| Collections hub | `/podborki/` | задачи и ограничения | collection registry | index | build | collection leaf |
| Intent/editorial leaf | `/podborki/besplatno/`, `/podborki/s-detmi/` | concrete constraint | collection manifest | index if stable useful intent | build/daily | event |
| Promoted search collection | `/podborki/dzhaz-vecherom/` | long-tail reusable intent | accepted automatic query candidate | index only after novelty/quality gate | build | event/search refinement |
| Algorithmic | `/populyarnoe/`, `/novoe/` | social proof / freshness | versioned projections | index if deterministic/explainable | daily/build | event |
| Search shell | `/poisk/` | ad-hoc intent | local index + optional semantic backend | page can be index/noindex by content; result query URLs noindex | current catalog | event/save query |
| Saved | `/izbrannoe/` | мои планы | Supabase/local fallback shell | noindex, auth/privacy-safe | dynamic | event/update/reminder |
| Personal update center | `/dlya-vas/` | что нового для меня | profile/feed issue | noindex | new edition | event/acknowledge |
| Bearer email page | `/dlya-vas/<opaque-token>/` | larger email selection | immutable issue artifact | noindex,nofollow,noarchive; never sitemap | per issue | event/feedback |
| Entity hub/leaf | `/mesta/`, `/mesta/<slug>/`, organizers/cities | navigation/trust/long-tail | normalized entity registry | later, only after identity cleanup | build | event/follow |
| Festival edition | `/festivali/<edition>/` | program planning | festival entity/program | separate post-release gate | build | program event |
| Service/legal | privacy, recommendation disclosure, about, contacts, partners | trust/compliance | authored static docs | index as appropriate | rare | preferences/contact |
| Email/account service | verify/callback/preferences/unsubscribe | control | Supabase identity/control plane | noindex | dynamic | return to initiating route |
| Machine/lifecycle | sitemap index, robots, ICS, 404/410, redirect map | crawl/runtime | build manifest | machine policy | build | canonical fallback |

### 5.1. Не создавать SEO-комбинаторику

Нельзя материализовывать `category × city × date × free × audience`. Query filters остаются `noindex,follow` и canonical указывает на допустимую базовую landing. Indexable registry — allowlist.

Публиковать collection leaf можно только если одновременно выполнены:

- устойчивый человеческий intent;
- минимум около `5` активных релевантных событий;
- уникальный набор относительно существующего leaf;
- полезное вводное описание, не шаблонный SEO-текст;
- стабильный slug;
- нет unsafe/private/raw query;
- текущие результаты проходят lifecycle and quality gate.

При истончении страницы её не надо мгновенно превращать в новый пустой URL: сначала `noindex` + полезный переход к parent/sibling; затем redirect/410 по lifecycle policy.

## 6. Единая модель подборки

Сайт, социальный пост и email не должны иметь три независимых selection algorithm. Нужен `collection_manifest`:

```text
collection_id
slug
kind: time | category | constraint | editorial | algorithmic | promoted_query
name / short_description
selection_rule_version | editorial_event_ids
catalog_build_id / generated_at / next_refresh_at / content_hash
min_results / max_results
exclusions / lifecycle_policy / diversity_policy
index_policy / canonical
social_copy_variant / OG asset
campaign_key
```

Один manifest используется для:

- HTML collection page;
- ссылок и карточек TG/VK/MAX;
- related continuation «вся подборка»;
- email candidate generation;
- analytics and QA.

Это защищает от ситуации, когда пост обещает одну подборку, сайт показывает другую, а email повторяет третью.

## 7. Какие подборки публиковать в TG/VK/MAX

Нужна небольшая повторяемая editorial matrix, а не десятки постоянных рубрик.

| Рубрика | Cadence | User intent | Роль |
|---|---:|---|---|
| Сегодня вечером | ежедневно утром при достаточном supply | спонтанный выход | быстрый conversion |
| Завтра | вечером предыдущего дня | короткое планирование | bridge к date page |
| Выходные | четверг; optional Friday reminder без повтора тех же cards | планирование | главный регулярный digest |
| Новое за неделю | 1 раз/неделю | причина вернуться | freshness discovery |
| Бесплатно | 1 раз/неделю или чередовать | price constraint | широкий acquisition |
| С детьми / семьёй | 1 раз/неделю или чередовать | family planning | устойчивый сегмент |
| Выставки и long-running | 1 раз в 1–2 недели | «можно выбрать дату» | evergreen discovery |
| Ротация жанра | 1 слот/неделю | concerts / theatre / lectures / workshops / cinema / excursions / festivals | обучение breadth каталога |
| Сезонная/погодная | только при реальном контексте | rain/outdoor/holiday/tourist day | editorial relevance |

### Не делать `Популярное` основной соцсетевой рубрикой

Социальные реакции уже участвуют в popularity. Постоянно публикуя «популярное» обратно в соцсети, продукт создаёт self-reinforcing loop и подавляет новое/нишевое. `Популярное` остаётся полезным site surface и редким social proof post, но не заменяет `Новое`, exploration и тематическую ротацию.

Все каналы используют один `collection_id`, но адаптируют текст/медиа под TG/VK/MAX. Canonical URL очищен от attribution params; analytics хранит channel/post/campaign/build отдельно.

## 8. Жанры и свободный умный поиск

### 8.1. Жанры: гибрид taxonomy + язык задачи

Пользователь думает и жанрами, и задачами. Поэтому нужны два разных слоя:

**Категории** — что это. V1 не вводит новую скрытую онтологию: UI leaves явно отображаются на canonical `event-taxonomy-v1`:

| UI label/route | Canonical category v1 |
|---|---|
| Концерты и музыка | `music` |
| Театр | `theatre` |
| Выставки | `exhibition` |
| Детские события | `kids` |
| Спорт | `sport` |
| Экскурсии | `excursion` |
| Лекции | `lecture` |
| Мастер-классы | `workshop` |
| Кино | `cinema` |
| Фестивали | `festival` |
| Ярмарки | `market` |
| Ночная жизнь | `nightlife` |
| Гастрономия | `food` |
| Другое | `other` |

`Встречи`, `шоу`, `краеведение`, `игры` и `outdoor` могут быть user-facing tags/collection intents, но не молча новыми primary categories. Любое объединение leaves в одну UI-группу — только presentation grouping над сохранёнными canonical ids. Изменение самих ids/семантики требует versioned taxonomy migration, profile/manifest compatibility и E2E, а не правки menu copy.

**Подборки** — зачем/при каких условиях:

- сегодня вечером;
- бесплатно;
- с детьми;
- Пушкинская карта;
- на улице / в дождь;
- новое;
- популярное;
- туристу на один день;
- approved user-language intents.

Жанр появляется:

- в global navigation через `Категории`;
- на главной как bounded category rail;
- в event breadcrumbs/medallions;
- в related reason и переходе «Все концерты»;
- в search suggestions;
- в отдельном `Новое для вас`/saved-topic surface, когда появится durable state; badge `Моё избранное` при этом остаётся только счётчиком сохранённых событий.

### 8.2. Search должен быть видим до логина

Целевой UX — одно поле, два уровня:

1. **Anonymous instant search:** title, venue, category, date, city, approved collection aliases, local fuzzy/full-text index. Zero runtime LLM cost.
2. **Semantic upgrade:** естественный запрос, vector + verifier; логин/квота нужны только там, где действительно расходуется backend и сохраняется история/профиль.

Нельзя показывать пользователю лишь login wall вместо поиска. Даже запрос «выставки завтра» должен дать полезный анонимный ответ из уже опубликованного каталога/collection manifests.

Search entry points:

- visible desktop header action/input;
- mobile bottom/icon action;
- large but non-blocking input on homepage;
- search suggestion at the end of a collection and empty state;
- optional contextual query prefill from event/category.

### 8.3. Search → static collection governance

Pipeline:

```text
query + results
→ explicit «нашёл» + qualified downstream action
→ private candidate aggregate
→ automatic LLM normalization
→ duplicate/result-overlap merge
→ novelty, safety, privacy, supply and quality gates
→ accepted collection manifest
→ static page on next build
→ lifecycle monitoring
```

Контракт проекта уже требует fully automatic LLM-first fail-closed curation. Provider failure или ambiguity = `pending`, не публикация слабой страницы.

Кандидат не проходит, если он:

- персональный/приватный или содержит PII;
- сводится к одному событию/площадке без самостоятельного intent;
- дублирует существующую category/collection;
- имеет почти тот же verified result set;
- нестабилен и пустеет между builds;
- требует небезопасной смысловой интерпретации.

Accepted query должен жить в namespace `/podborki/<slug>/`, а не создавать параллельные `/t/`, `/tag/` и `/search/landing/` онтологии.

## 9. Персонализация и проверка идеи «30 карточек»

### 9.1. Корректная формулировка обещания

Не:

> после 30 карточек пользователь точно найдёт интересное.

А:

> для зрелого профиля и при наличии хотя бы одного ground-truth релевантного активного события в кандидатном пуле, KenigEvents должен показать событие с meaningful action не позже 20-го уникального valid impression; `30` — верхний пользовательский budget и maturity threshold, а не гарантия supply.

### 9.2. Что считать valid impression

- distinct event;
- карточка видна не менее чем на 50%;
- не менее 0.8–1.2 секунды;
- вкладка активна;
- нет fast scroll/layout transition;
- не идёт смена density mode;
- повторный показ того же события в journey не увеличивает unique count.

### 9.3. Иерархия outcome

1. `ticket / register / phone click` — strongest observable intent;
2. `save / calendar add` — сильное намерение;
3. `share` — сильный положительный сигнал;
4. `detail open + meaningful dwell` — средний;
5. `like` — preference, но не план посещения;
6. `not interested` — exact hard hide + мягкое обобщение;
7. `quick skip` — только очень слабый повторяющийся negative, никогда не hard hide.

Коррелированные действия по одному событию не складываются бесконечно: хранится highest/capped outcome.

### 9.4. North Star и guardrails

```text
qualified_success@20 =
eligible mature journeys with ticket/register/phone or save/calendar
within first 20 unique valid impressions
/
eligible mature journeys with relevant active supply
```

Дополнительно:

- `success@10 / @20 / @30`;
- median/P75 `cards_to_first_qualified_action`;
- time to first qualified action;
- relevant-supply coverage;
- profile-to-next-feed application rate;
- D7/D30 return after genuinely new relevant supply;
- repeated-seen rate;
- not-interested top-10/top-20;
- category/venue concentration;
- stale/cancelled leakage;
- no-consent trusted writes = `0`;
- page/feed latency.

Все срезается по source, surface, viewport, density, cold/warm/mature, profile/algorithm/taxonomy/build version.

### 9.5. Progressive ranking

- cold: context/date + popular + fresh + broad exploration;
- warming: растущая доля profile affinity, но всё ещё заметная exploration;
- mature: примерно 60–70% exploitation, 10–20% exploration, остальное context/freshness;
- diversity caps по category, venue, date, format;
- current viewport никогда не пересортировывается после like/hide.

## 10. Стабильные, но умные ленты

Нужна не постоянно меняющаяся ranking function, а **feed edition**:

```text
feed_edition_id
catalog_build_id
profile_revision
algorithm_id
generated_at / expires_at
ordered_event_ids
```

Правила:

1. Порядок фиксирован для текущей edition/session.
2. Like/hide меняет state карточки, но rerank применяется только ниже scroll anchor или в следующей edition.
3. Новый build показывает `Появилось N новых`, но не вставляет cards над пользователем.
4. Safety/lifecycle change — отмена, перенос, sold out — патчится сразу.
5. Tie-break детерминирован по visitor/surface/edition seed.
6. 30-minute cache может остаться техническим cache, но product edition живёт достаточно долго, чтобы пользователь узнавал ленту.
7. Static HTML/canonical остаётся детерминированным; personal layer — progressive enhancement.

### Состояния карточки

- `unseen_new`;
- `seen`;
- `opened`;
- `liked`;
- `saved/planned`;
- `calendar_added`;
- `ticket_clicked`;
- `not_interested`;
- `materially_updated`;
- `expired/cancelled`.

`Updated` означает изменение даты, времени, места, цены, ticket/lifecycle status после последнего просмотра. Косметическая правка описания не должна создавать badge.

## 11. Система возвратности: где что обновилось

На возврате продукт должен сообщать не «лента стала другой», а конкретный digest:

- `С прошлого визита: 8 новых для вас`;
- `2 события из ваших планов изменились`;
- `На выходные появилось 5 новых`;
- `Скоро начнутся 3 сохранённых события`;
- `Новое в теме «джаз вечером»`.

Newness показывается отдельным contextual chip/dot `Новое для вас`, ведущим в группы:

1. Новое для вас;
2. Изменения моих планов;
3. Скоро начнётся;
4. Новое в сохранённых темах;
5. Продолжить просмотр.

Глобальное `Моё избранное` остаётся отдельным destination `/izbrannoe/`; его badge считает только distinct durable saved events и никогда не используется для newness/reminders.

Минимальная data model:

```text
visitor_surface_watermark(surface, last_build_id, acknowledged_at)
visitor_event_state(event_id, first_seen_at, last_seen_revision_hash,
                    liked_at, saved_at, calendar_added_at,
                    ticket_clicked_at, hidden_at)
event_revision(first_published_at, material_updated_at,
               revision_kind, build_id)
```

Reactivation priority:

1. on-site update center and saved-plan changes;
2. transactional reminder/change mail only after explicit event consent;
3. recommendation email after separate consent;
4. browser push only after demonstrated value and explicit gesture;
5. social channels remain broad editorial acquisition, not personal notification transport.

## 12. Email: итоговое решение

### 12.1. Базовый продукт

Сохраняется уже принятое правило:

- **ровно 3 разных события в письме**;
- одно может быть visual hero, ещё два — compact;
- CTA ведёт на заранее опубликованную более крупную персональную страницу;
- все три события также есть на этой странице; proposed canary size `12–24` требует отдельного owner/data-quality threshold approval;
- recommendations идут только через NotiSend;
- transactional event reminders идут только через Postbox;
- consent, suppression и send eligibility принадлежат Supabase;
- hard launch cap — `<=200` active consented recipients.

Таким образом, идеи «3 предложения» и «1 hero + personal page» не взаимоисключающие: письмо может визуально иметь **один hero + два compact предложения**, оставаясь exactly-three.

### 12.2. Что делать с одним мгновенным предложением

Не запускать второй recurring stream. Позже можно A/B-тестировать:

- immediate 3-event welcome issue;
- immediate 1-event `welcome_selection` + personal page.

Второй вариант требует отдельного product kind, consent/cadence/template/SQL contract и не должен молча нарушать `exactly_three`.

### 12.3. Signup и frequency

Числовые cadence/fatigue/retention thresholds ниже — **предлагаемые canary hypotheses, не принятые release decisions**. Их должны утвердить product/legal owners после deliverability, complaint и click-delay evidence.

Показывать предложение после demonstrated value:

- save/calendar;
- successful search;
- like/qualified action;
- candidate trigger около 10 valid impressions;
- конец полезной подборки.

Не на первом page load.

Предлагаемый canary preference set:

- `2 раза в неделю`;
- `1 раз в неделю`;
- `пауза`;
- timezone/local send window;
- weekend-only preference;
- optional free/family/city facets;
- `next_due_at`, `last_issue_at`, `welcome_sent_at`;
- максимум 2 recommendation messages / 7 days;
- no-repeat одного события 14–30 дней;
- автоматический downshift после 4–6 проигнорированных выпусков.

### 12.4. Personal page security

Route example: `/dlya-vas/<128-bit-opaque-token>/`.

- назвать `Ваша подборка`, а не обещать криптографическую «секретность»;
- forwardable bearer access;
- hash/HMAC token at rest;
- revocable/rotatable;
- TTL/retention остаётся open product/legal decision; canary hypothesis — 14 days с последующей проверкой click-delay distribution;
- `noindex,nofollow,noarchive`, restrictive `Referrer-Policy: no-referrer`;
- token отсутствует в canonical, OG, outbound referrer/UTM/analytics payload;
- никакого email, user id, raw profile/tags/vectors/internal scores;
- page/click/unsubscribe/feedback tokens различны;
- страница остаётся читаемой при отказе feedback API.

### 12.5. Email success

Primary: ticket/register/phone or save/calendar. Candidate attribution windows `72h/7d` должны быть откалиброваны и утверждены; это не текущий release threshold. Like is secondary. Open rate is delivery diagnostic, not outcome. Нужен holdout, иначе рост действий нельзя приписать письму.

## 13. Pinch-to-density

Идея ценна как optional accelerator, но не как основной или единственный control.

Риски web pinch:

- почти нулевая discoverability;
- конфликт с browser zoom и accessibility;
- accidental activation;
- сложность корректного impression tracking при reflow;
- пользователь может потерять scroll position и ощущение стабильности.

Целевой контракт:

```text
presentation_density = large | compact | dense
```

Обязательный control — видимый переключатель `Крупно / Компактно / Список` или одна cycle-button с понятным label. Настройка сохраняется per device/surface.

Pinch можно тестировать позже только как shortcut:

- он не отключает browser zoom;
- не считается taste signal;
- сохраняет event order и scroll anchor;
- не создаёт новые impressions во время reflow и ещё около 500 ms после;
- имеет onboarding hint и доступную альтернативу;
- analytics разделяет density и viewport.

На P0 достаточно visible density control. Pinch не должен блокировать release.

## 14. Roadmap

### P0 — замкнуть публичный discovery loop

Deliverables:

1. production root `/` как Афиша;
2. единая navigation IA;
3. production canonical/sitemap/robots profile;
4. public URL resolver и TG/VK/MAX switch with rollback;
5. `collection_manifest` и обязательные time/category/collection pages;
6. заметный anonymous catalog search;
7. один card projection с крупным/compact/list presentation;
8. event → collection/category continuation без тупика;
9. valid-impression + qualified CTA baseline analytics;
10. occurrence-aware date selection and copy.

Exit evidence:

- 100% social target URLs существуют в promoted manifest;
- no preview/lab/private URLs в sitemap;
- zero dead-end event pages in E2E journey;
- baseline `success@20/@30` измеряется, а не симулируется;
- page families usable without auth/JS optional services.

### P1 — персонализация, сохранение и newness

Deliverables:

1. trusted compact action ingest;
2. seen/valid impression, detail+dwell, save/calendar, ticket/register/phone capture;
3. profile maturity/confidence;
4. real personal candidate feed and next-session rerank;
5. feed editions and scroll-anchor stability;
6. durable favorite `/izbrannoe/`;
7. global identity shell and profile linking;
8. отдельный `Новое для вас` / update center / material changes без изменения badge `Моё избранное`;
9. end-to-end golden personas.

Exit evidence:

- every eligible mature golden persona: relevant meaningful action `<=20`;
- `100%` accepted/deduped valid fixture telemetry and `0` without consent;
- changed profile visibly changes the next eligible served list;
- no hidden/stale/cancelled recurrence;
- no prior-user leakage on account switch/logout.

### P2 — owned retention and controlled SEO scale

Deliverables:

1. exactly-three email generator;
2. larger bearer personal pages; proposed canary size 12–24 after owner approval;
3. consent/preferences/unsubscribe/fatigue/due-state;
4. NotiSend seed canary then `<=200` beta;
5. saved-query → automatic collection pipeline;
6. entity pages after normalization;
7. browser notifications only after proof of value;
8. cohort/holdout evaluation and SEO/GEO gate.

Exit evidence:

- zero send without current verified purpose consent;
- no stale/cancelled event in sent three;
- deliverability/suppression/replay gates pass;
- email incremental qualified action lift proven against holdout;
- promoted collections meet supply/novelty/quality/index gates over repeated builds.

## 15. Anti-roadmap

Сейчас не делать:

1. infinite scroll как единственный discovery pattern;
2. runtime LLM ranking/generation на каждый page view;
3. indexation raw search results или facet Cartesian product;
4. отдельную recurring one-event email stream;
5. push permission на первом визите;
6. quick skip как hard negative;
7. pinch как единственный density control или запрет browser zoom;
8. социальные механики friends/attendance до решения «куда пойти мне»;
9. десятки venue/city/artist pages до entity/alias normalization;
10. homepage motion/«Городской обзор» вместо полезных categories/search/feed;
11. popularity-only optimization, создающую feedback echo chamber;
12. raw per-view telemetry в Supabase без compaction/TTL.

## 16. Критика Gemini Pro и принятые решения

Независимый `agy` review (`Gemini 3.1 Pro (High)`) подтвердил основные системные проблемы:

- leaf-page bias и dead-end deep links;
- отсутствие visible newness/return loop;
- необходимость global search/categories/favorites/newness;
- feed stability и explicit refresh;
- опасность pinch-only UX;
- недоказуемость слова «точно» в 30-card promise.

После repo reconciliation скорректированы предложения консультанта:

- **не принято** делать infinite scroll обязательным продолжением каждого event page; используем bounded continuation + explicit `Показать ещё`;
- **не принято** считать dwell `>3s` implicit like: это средний noisy signal, а не preference truth;
- **не принято** требовать auth для обычного ICS/calendar action; auth нужен для durable saved state/cross-device, публичный fallback сохраняется;
- **не принято** объявлять one-hero email winner: канонический контракт уже фиксирует exactly three; один hero может быть одним из трёх;
- **уточнено**: past/cancelled route lifecycle не решается blanket 404 после падения inventory; нужны redirects/410/noindex and current alternatives по типу изменения.

Сырой review, brief и exact invocation/model evidence сохранены локально в `artifacts/codex/static-site-product-system-20260718/` и не коммитятся согласно artifact policy.

## 17. Итоговые решения

1. Продукт — не каталог, а система быстрого выбора и возврата.
2. Главный KPI — qualified success within first 20 unique valid touches for mature eligible journeys; 30 — budget/maturity, не гарантия supply.
3. Главная IA: `Афиша / Сегодня / Завтра / Выходные / Категории / Подборки / Поиск`, отдельно `Моё избранное / Профиль`; newness имеет собственный indicator.
4. Жанры живут в `Категории`; потребности и ограничения — в `Подборки`.
5. Search всегда обнаружим и полезен до логина; expensive semantic layer может требовать identity/quota.
6. Сайт, TG/VK/MAX и email используют один versioned collection contract.
7. Feed стабилен как edition; newness показывается явно и по запросу пользователя.
8. Like — слабее calendar/save/ticket; durable `В планы` обязателен.
9. Email V1 — exactly 3 + larger already-published personal page; one-event — только later welcome experiment.
10. Pinch-density — optional delight, visible accessible density control — обязательный baseline.
