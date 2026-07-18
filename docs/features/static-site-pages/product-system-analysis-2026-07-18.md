# Feed-first продуктовая модель статического сайта KenigEvents

> Дата: 2026-07-18
> Статус: revised feed-first recommendation after owner correction; final navigation acceptance pending owner confirmation
> Каноническая база: `docs/static-site-video-guides-20260718@191a4019`
> Внешняя критика: два review через `agy`, `Gemini 3.1 Pro (High)`; выводы сверены с repo и решением владельца.

## 1. Исправление прежнего анализа

Прежняя версия переусложняла продукт. Она превращала технические понятия — `hub`, `leaf`, taxonomy, entity page, collection type — в пользовательское меню.

Это неверный центр проектирования.

Основной трафик будет мобильным. Главная ценность KenigEvents — сильная персонализация по мере накопления сигналов. Поэтому продукт должен восприниматься как **одна продолжающаяся лента**, а статические event/selection/SEO/email pages — как разные точки входа в неё.

Новый тезис:

> KenigEvents — это одна адаптивная мобильная feed-сессия. Внешняя ссылка задаёт её начало и контекст, а последующий скролл собирает сигналы, постепенно персонализирует продолжение и приводит к событию, которое пользователь сохранит, добавит в календарь или откроет для покупки.

Статический сайт нужен не для строительства портала-энциклопедии. Он обеспечивает:

- быстрые и надёжные social/SEO deep links;
- полезный no-JS/static fallback;
- grounded narrative pages;
- индексируемые входы;
- начальный candidate set для ленты.

## 2. Минимальная пользовательская модель

### Три постоянных назначения

1. **Лента** — основная Афиша;
2. **Поиск** — свободный запрос и быстрые темы;
3. **Моё** — сохранённые события и минимальные пользовательские настройки.

`Сегодня`, `Завтра`, `Выходные`, `Бесплатно`, `С детьми`, `Новое` и другие срезы — не отдельные уровни меню. Это **контекстные chips/presets**, меняющие начало и дальнейший состав ленты.

### Mobile

Bottom navigation:

```text
Лента · Поиск · Моё
```

В верхней части ленты — горизонтальные chips:

```text
Сегодня · Завтра · Выходные · Бесплатно · С детьми · Новое · Ещё
```

Нажатие на chip не должно ощущаться переходом в другой продукт. Даже если технически загружается другой статический URL, сохраняются shell, card language, density preference и feed-session semantics.

### Desktop

Desktop показывает те же три назначения и больше chips одновременно. Третье назначение называется **`Мои события`**; accessible name на mobile тоже `Мои события`, хотя короткий visible label — `Моё`. Desktop не получает отдельную информационную архитектуру.

### Семантика `Моё` / `Мои события`

Это уже принятый единый personal hub:

- mobile label: `Моё`;
- desktop/accessibility label: `Мои события`;
- canonical route: `/moi-sobytiya/`;
- `/izbrannoe/` — compatibility entry в тот же shell с filter `Избранное`;
- filters: `Предстоящие`, `Прошедшие`, `В календаре`, `Избранное`.

Badge считает distinct **current upcoming union** calendar + favorite. Событие в обоих состояниях учитывается один раз. Past, removed, merged-away, likes, reminders, raw repeated ICS downloads и transport-only state badge не увеличивают. Newness имеет отдельную семантику.

## 3. Любой mobile-вход превращается в feed session

### 3.1. Event deep link

```text
пост TG/VK/MAX
→ полная карточка/страница конкретного события как anchor
→ CTA: билет / регистрация / календарь / сохранить
→ несколько действительно связанных событий
→ обычное персонализируемое продолжение ленты
```

Если событие не подошло, пользователь не упирается в footer или каталог. После event anchor начинается продолжение.

### 3.2. Social selection link

```text
пост «Бесплатно на выходных»
→ короткий selection header
→ отобранные события
→ расширение в персональную ленту с сохранённым контекстом
```

### 3.3. SEO narrative entry

```text
поисковый запрос «куда пойти в дождь в Калининграде»
→ grounded narrative intro
→ текст ведёт от события к событию
→ после завершения сценария начинается лента
```

### 3.4. Email personal page

```text
exactly-three email
→ три верхних персональных события
→ более широкая ranked selection
→ дальнейшая feed edition
```

### 3.5. Возврат из event detail

Если event detail был открыт из ленты, browser Back обязан восстановить:

- scroll position;
- feed edition/order;
- выбранный chip;
- density mode;
- уже загруженные карточки;
- локальные saved/hidden states.

Это P0. Без этого лента распадается на отдельные страницы.

## 4. Не «Категории» и «Подборки», а темы ленты

Пользователю не нужно объяснять разницу между category, filter, intent и collection. Всё это — способы попросить другую ленту.

### Пользовательские темы

- Сегодня;
- Завтра;
- Выходные;
- Концерты;
- Выставки;
- С детьми;
- Бесплатно;
- На улице;
- Новое;
- Популярное;
- другие доказавшие спрос темы.

Внутри остаётся canonical taxonomy v1, но она не диктует навигацию. Taxonomy нужна ranking/export/compatibility, а не для показа пользователю технического рубрикатора.

### Нужна ли отдельная страница со всеми темами

Только как простой overflow sheet `Все темы`, если chips перестают помещаться. Не нужен отдельный портал с двумя хабами `Категории` и `Подборки`.

### Зачем всё ещё нужны стабильные URL

У темы может быть статический URL для:

- ссылки из соцсети;
- SEO;
- share;
- canonical/no-JS fallback;
- восстановления состояния.

Но URL — техническая точка входа в feed preset, а не причина создавать ещё один пользовательский раздел.

## 5. Четыре типа selection surface

### 5.1. Автоматический срез

Примеры:

- Бесплатно;
- С детьми;
- Под открытым небом;
- Сегодня;
- Завтра;
- Выходные;
- Пушкинская карта.

Свойства:

- deterministic filter над canonical facts;
- обновляется при каждой генерации сайта;
- обычно не требует длинного текста;
- сразу продолжается общей лентой;
- может комбинировать не больше двух устойчивых условий при достаточном supply.

### 5.2. Алгоритмическая лента

Примеры:

- Новое;
- Популярное;
- Для вас;
- related continuation.

Свойства:

- versioned ranking/projection;
- `Популярное` уже существует и не требует нового продуктового раздела;
- порядок фиксируется в feed edition;
- static fallback остаётся полезным;
- персонализация применяется как progressive enhancement.

### 5.3. Автоматически создаваемое повествование

Примеры:

- Сегодня вечером;
- Куда пойти в дождь;
- День туриста в Калининграде;
- План прогулки перед концертом;
- Семейная суббота.

Это не обычная сетка карточек и не обязательно ручная редакционная статья. Это **build-time LLM-first narrative**, автоматически создаваемый из отобранных событий.

### 5.4. Персональная feed edition

Это основная поверхность продукта:

- учитывает накопленные сигналы;
- использует context/source/date;
- сохраняет порядок на текущую сессию;
- содержит controlled exploration;
- продолжает любой из трёх предыдущих типов.

## 6. Contract автоматического narrative

### 6.1. Сначала selection, потом writer

LLM не выбирает события из сырого каталога произвольно.

```text
canonical active events
→ deterministic/context candidate filter
→ diversity and route/time compatibility
→ grounded event fact pack
→ narrative writer
→ independent fact/sequence verifier
→ SSG artifact
```

### 6.2. Writer contract

Writer получает только подтверждённые данные выбранных event IDs:

- title;
- date/time;
- venue/address/city;
- price/admission;
- duration if known;
- canonical summary;
- transport/travel facts only if separately verified;
- media and canonical URL.

Writer строит связное повествование:

- объясняет общий замысел;
- вводит события в естественном порядке;
- связывает переходы между ними;
- не пересказывает каждую карточку;
- не придумывает расстояния, погоду, атмосферу, длительность или доступность;
- не обещает, что пользователь физически успеет между событиями без verified route/time calculation.

### 6.3. Формат страницы

Не `введение → 20 одинаковых карточек`, а:

```text
короткий grounded lead
→ событие 1
→ переход/почему дальше
→ событие 2
→ следующий поворот сценария
→ событие 3
→ optional alternatives
→ персонализируемое продолжение ленты
```

Текст и event modules образуют одно повествование.

### 6.4. Lifecycle

Narrative rebuild запускается, если:

- событие закончилось/отменено/перенесено;
- изменились критические факты;
- selection перестал быть связным;
- появился существенно лучший candidate set;
- истёк freshness window данного сценария.

Если writer/verifier недоступен или coherence gate не пройден:

- старая заведомо устаревшая статья не публикуется;
- surface деградирует в честный автоматический список;
- ambiguity остаётся pending;
- runtime LLM на page view не вызывается.

### 6.5. Защита от SEO-спама

Narrative создаётся только если:

- есть повторяемый пользовательский intent;
- достаточно подходящих активных событий;
- события образуют объяснимую последовательность;
- результат заметно отличается от существующих stories;
- URL и intent стабильны;
- текст проходит fact/duplication/quality gates.

Не создаются автоматические тексты для каждой комбинации фильтров.

## 7. Что автоматизировать максимально

### Безопасная автоматика

- today/tomorrow/weekend;
- free;
- kids/family;
- outdoor;
- Pushkin card;
- new;
- popular;
- canonical category/type slices;
- пары устойчивых признаков при достаточном supply;
- related/top-up;
- rebuild/expiry/noindex lifecycle.

### Автоматический LLM-writer с gates

- rainy-day plan;
- evening itinerary;
- tourist day;
- date/romantic plan;
- seasonal scenario;
- event-to-event thematic story.

Это тоже автоматизация, но не простой SQL/filter.

### Не создавать

- страницы по любой случайной фразе;
- сочетания трёх и более фильтров без доказанного спроса;
- пустые/sparse stories;
- страницы площадок или организаторов только ради SEO;
- тексты, где связь событий держится на выдуманном writer context.

## 8. Площадки, организаторы и города

Отдельные entity pages удаляются из обычного roadmap.

KenigEvents не должен подменять сайт организатора, энциклопедию площадки или городской справочник.

Площадка/организатор остаются:

- фактом события;
- фильтром/поисковым признаком;
- medallion/trust link;
- ссылкой на официальный ресурс, если это полезно;
- ranking feature.

Отдельный static surface появляется только при доказанном самостоятельном пользовательском сценарии, например:

- пользователь действительно ищет расписание одной крупной площадки;
- у площадки постоянно достаточно актуального supply;
- страница решает задачу лучше, чем фильтр или официальный сайт;
- есть продуктовая метрика, а не только гипотетический SEO-трафик.

Городские страницы также не нужны по умолчанию. Для текущей региональной афиши город — фильтр/chip. Фестиваль может иметь отдельную программу, потому что это самостоятельный много-событийный planning task, а не справочник организатора.

## 9. Что конкретно находится в `Моё`

### Обязательно

1. **`Мои события`**: единый upcoming union календаря и избранного;
2. filters `В календаре` и `Избранное`, collapsed `Прошедшие`;
3. статус изменений сохранённых событий;
4. email recommendation subscription: включена/пауза/частота после утверждения product policy;
5. transactional reminder opt-ins;
6. вход/выход и masked verified identity;
7. `Сбросить персонализацию`;
8. `Не использовать историю для рекомендаций` / consent state;
9. доступ к privacy/export/delete flows.

### Контекстно, не отдельные разделы

- сохранить событие — на card/detail;
- выбрать density — на ленте;
- выбрать тему — chips;
- добавить/проверить email — в момент save/subscription;
- объяснение `Почему показано` — на карточке;
- `Новое для вас` — блок/chip в ленте.

### Не нужно

- публичный user profile;
- bio/avatar/preferences questionnaire;
- отдельный центр всех уведомлений на MVP;
- ручное редактирование десятков inferred interests;
- настройки алгоритма с техническими коэффициентами.

## 10. Feed session contract

### Anchor

Первые элементы строго соответствуют источнику входа. Social/SEO promise нельзя сразу заменить общей персонализацией.

### Continuation

После anchor идут:

1. ближайшие по исходному intent;
2. profile-aware candidates;
3. exploration/new supply;
4. явная stop-line или расширение горизонта.

### Stable edition

```text
feed_edition_id
entry_context
catalog_build_id
profile_revision
algorithm_id
ordered_event_ids
```

- current viewport не пересортировывается;
- actions влияют ниже anchor или на следующую edition;
- новый build предлагает refresh, но не вставляет cards сверху;
- отмена/перенос/sold-out патчатся немедленно;
- Back восстанавливает edition и scroll.

### Не бесконечный doomscroll

Лента содержит естественные stop-lines:

- `Это всё на сегодня`;
- `Посмотреть завтра`;
- `Расширить интересы`;
- `Вы посмотрели все новые события`;
- `Показать ещё`.

### Density

Baseline — видимый переключатель `Крупно / Компактно / Список`. Pinch может быть later shortcut, но не sole control и не должен ломать browser zoom.

### Valid impression

Считается только distinct event, реально находившийся в viewport достаточно долго без fast scroll/reflow. Density transition не создаёт impressions.

## 11. Персонализация остаётся ядром

Упрощение IA не означает упрощение ranking loop.

Сигналы по силе:

1. ticket/register/phone click;
2. save/calendar;
3. share;
4. detail + meaningful dwell;
5. like;
6. explicit not interested;
7. repeated quick skip — только слабый negative.

Главный contract:

> Для зрелого профиля и при наличии релевантного active supply meaningful action должен появиться не позже 20-го unique valid impression. Тридцать impressions — maturity/user budget, а не гарантия supply.

Лента обязана сохранять exploration. Долю нельзя фиксировать навсегда без production evidence; она калибруется по diversity, hide rate и qualified success.

## 12. Newness и возвратность внутри ленты

Не нужен отдельный сложный `/dlya-vas/` как обязательный MVP-раздел.

На новом визите сверху появляется понятный блок:

```text
Новое для вас
8 новых событий · 2 изменения в сохранённых
```

Действия:

- показать новые;
- посмотреть изменения планов;
- продолжить предыдущую edition;
- скрыть/acknowledge block.

Badge `Моё` / `Мои события` считает только distinct current upcoming union calendar + favorite; dual state считается один раз. Newness имеет отдельный chip/dot.

## 13. Email

Принятый контракт сохраняется:

- exactly three events;
- одно может быть hero;
- ссылка на уже опубликованную более широкую personal selection;
- recommendation consent отдельно от transactional consent;
- ordinary browsing/ICS без auth;
- personal page продолжается в feed session.

Email — reactivation lane, а не отдельная продуктовая вселенная.

## 14. Что принять и отклонить из второго Gemini review

### Принято

- прежняя IA была overengineered;
- 3 постоянных назначения сильнее 7 разделов;
- categories/selections лучше представить chips;
- entity pages не нужны по умолчанию;
- все входы должны превращаться в feed session;
- scroll/back restore — P0;
- narrative генерируется build-time и проверяется;
- stop-lines нужны против doomscroll.

### Скорректировано

Gemini предложил полностью `Delete` hubs. Пользовательского hub действительно не нужно, но технические статические URLs/presets сохраняются для social/SEO/no-JS и восстановления состояния.

Gemini предложил создавать entity page при SEO demand и `>5` events. Одного SEO demand недостаточно: страница появляется только при самостоятельной пользовательской задаче и доказанной ценности относительно фильтра/официального сайта.

Gemini назвал personal feed «бесконечным discovery». Бесконечность не является целью: нужны stop-lines, horizons и explicit continuation.

Gemini предложил смешивать density modes алгоритмом. Режим плотности должен выбирать пользователь; алгоритм может варьировать emphasis, но не самовольно менять выбранную плотность.

## 15. Упрощённый roadmap

### P0 — одна работающая mobile feed session

- `Лента / Поиск / Моё`;
- contextual chips;
- event/social/static entry anchors;
- continuous continuation under anchor;
- Back/scroll/feed-edition restore;
- visible density control;
- valid impression and strong-action baseline;
- static/no-JS fallback;
- production canonical resolver.

### P1 — сильная персонализация и возврат

- trusted compact action ingest;
- next-edition rerank;
- durable save;
- newness block;
- material update state;
- global lightweight identity only when needed;
- exactly-three email canary;
- golden-persona `<=20` E2E.

### P2 — автоматическое расширение входов

- build-time narrative writer + verifier;
- automated intent discovery from successful searches;
- safe selection factory;
- SEO/social narrative lifecycle;
- semantic search upgrade;
- rare entity/program surfaces only after proven user task.

## 16. Итоговые решения

1. Продукт — одна feed session, а не портал разделов.
2. Постоянная навигация — `Лента / Поиск / Моё`.
3. Время, жанры и ограничения — chips/presets ленты.
4. `Категории` и `Подборки` не показываются как два пользовательских hubs.
5. Stable URLs остаются техническими social/SEO entry points.
6. Автоматические filters покрывают простые grounded intents.
7. Narrative intents тоже автоматизируются, но через build-time LLM writer + verifier.
8. Event, social selection, SEO story и email page становятся anchors одной ленты.
9. Back/scroll/feed restore — P0.
10. Entity pages удалены из roadmap по умолчанию.
11. `Моё` / `Мои события` открывает `/moi-sobytiya/` с union календаря/избранного; account, consent/subscription и reset доступны как вторичные actions.
12. Newness живёт внутри ленты, не требует отдельного портала.
13. Feed edition стабильна; current viewport не пересортировывается.
14. Лента имеет stop-lines и controlled exploration.
15. Главная метрика — meaningful action within first 20 unique valid impressions при наличии relevant supply.
