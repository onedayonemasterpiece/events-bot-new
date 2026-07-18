# Сквозная продуктовая модель KenigEvents

> Дата: 2026-07-18
>
> Статус: продуктовая рекомендация после уточнения владельца; не означает готовность production-реализации
>
> Решение владельца: фиксированные поверхности и Hero Talk не пересматриваются
>
> Внешняя критика: свежий независимый review через `agy`, `Gemini 3.1 Pro (High)`, 2026-07-18; prompt, raw response и invocation evidence сохранены в `artifacts/codex/static-site-product-system-20260718/`

## Executive Summary

Набор зафиксированных страниц **достаточен как продуктовый каркас**, но пока недостаточен как законченный продукт.

Детальная модель только главной вынесена в [mobile/desktop homepage product contract](homepage-product-model-2026-07-18.md), чтобы не смешивать home feed lifecycle с полной системой страниц.

Проблема не в отсутствии ещё десяти разделов. Не хватает общего механизма, который связывает уже принятые точки входа в один накапливающий ценность путь:

```text
повод открыть KenigEvents
→ подходящий сценарий выбора
→ жизнеспособное событие
→ уверенность «мне подходит»
→ билет / регистрация / календарь / сохранение / share
→ новый сигнал о пользователе
→ следующий выбор становится короче и точнее
```

KenigEvents не должен быть ни «порталом-каталогом», ни одной растянутой мобильной лентой. Это **сеть разных способов решать задачу выбора события с общей памятью интересов и общей ответственностью за результат**.

Фиксированные поверхности выполняют разные работы:

- Hero Talk снимает неопределённость прямого входа и маршрутизирует кампанию или пользовательский сценарий;
- `Сегодня`, `Завтра`, `Выходные` отвечают на временной intent;
- `Популярное` помогает cold-start через социальное доказательство;
- поиск отвечает на сформулированный сложный запрос;
- event page помогает принять решение по конкретному кандидату;
- похожие сохраняют контекст;
- продолжение рекомендаций должно догнать интересное, если первый маршрут не сработал.

Главный продуктовый контракт:

> Для зрелого совместимого профиля, если в актуальном каталоге существует подходящее событие, хотя бы одно релевантное event family должно появиться среди первых 30 уникальных валидных показов во всём discovery journey, независимо от того, через какие страницы прошёл пользователь. Отдельно доказывается, что показ превратился в пользовательскую ценность.

Этот контракт нельзя выполнить отдельной «лентой на 30 карточек». Нужны общий exposure ledger, дедупликация, сохранение intent, context-aware ranking, измерение supply и единая атрибуция действий на всех поверхностях.

### Главные выводы

1. **Новых постоянных разделов сейчас не требуется.** Сначала нужно замкнуть путь между уже принятыми страницами.
2. **Hero Talk имеет самостоятельную роль**, но только как редакционный диспетчер и campaign router. Он не является recommender, каталогом или чат-игрушкой.
3. **Персонализация и текущая задача — две разные оси.** Временной/search intent текущего journey не должен теряться и не должен безусловно переписывать долговременный вкус.
4. **30 карточек — сквозной SLO, а не размер одного модуля.** Повторы и невидимые карточки не считаются новой возможностью.
5. **Возвратность строится не на doomscroll-привычке**, а на новом релевантном inventory, изменениях сохранённых планов и доказанно лучшем следующем выборе.
6. **Поиск без первой пользы до авторизации — критический activation blocker.** Авторизация нужна для сохранения и синхронизации, но не как входной билет в поиск.
7. **`Выходные` должны помогать выбрать 1–3 жизнеспособных плана**, а не просто показывать все записи, начавшиеся в субботу и воскресенье.

## 1. Решение, которое принимается сейчас

### Продуктовая формула

```text
актуальный региональный inventory
× понятный текущий intent
× накопленная память интересов
× непрерывность между поверхностями
= жизнеспособный план с минимальным числом лишних карточек
```

Преимущество KenigEvents не в наличии страниц `Сегодня` или `Выходные`: такие входы уже являются базовой гигиеной рынка. Например, Afisha.ru публично поддерживает отдельные страницы [на сегодня](https://www.afisha.ru/kaliningrad/events/na-segodnya/) и [на выходные](https://www.afisha.ru/kaliningrad/events/na-vyhodnye/), а Яндекс.Афиша описывает себя как сервис поиска событий и покупки билетов на них на [официальной странице продукта](https://yandex.kz/support/afisha/ru/).

Дифференциация KenigEvents должна возникать **после входа**:

- не потерять обещание конкретного social/SEO link;
- не заставить пользователя заново начинать поиск на следующей странице;
- учесть полученные сигналы;
- не повторять уже отвергнутое;
- вовремя сменить стратегию, если первые рекомендации не сработали;
- честно показать отсутствие подходящего supply вместо filler.

### Достаточно ли нынешней базы

| Уровень | Вердикт | Почему |
|---|---|---|
| Набор основных пользовательских работ | **Да** | direct/campaign routing, temporal planning, popular cold-start, explicit search и event decision покрыты |
| Набор постоянных страниц | **Да** | новые top-level hubs не решат главные разрывы |
| Сквозной journey | **Нет** | surface context, seen-state, profile learning и action attribution пока разорваны |
| Обещание «интересное в 30» | **Нет доказательств** | нет production valid-impression ledger и eligible-supply evaluation |
| Возвратность | **Нет замкнутого loop** | нет `new relevant since last meaningful visit` и полноценного saved-plan product |
| Production acquisition | **Не готово** | текущая ветка описывает preview/static contracts, а не подтверждённый live funnel |

## 2. Не один funnel, а две независимые оси

Линейная модель `cold → mature` недостаточна. Каждое решение определяется пересечением двух состояний.

### Ось A — зрелость памяти

| Состояние | Что известно | Задача продукта |
|---|---|---|
| **Cold** | нет надёжных personal signals | быстро дать первую ценность через входной context, время и разнообразие |
| **Activated** | найдено хотя бы одно реально рассматриваемое событие и совершено осмысленное действие | не потерять результат и объяснить следующий полезный шаг |
| **Learning** | есть несколько совместимых сигналов, но выводы ещё хрупкие | собирать высокоинформативные сигналы, не зажимая пользователя в раннюю гипотезу |
| **Mature** | есть повторяемые предпочтения и отрицательные ограничения | выполнять 30-card SLO при eligible supply и сохранять controlled exploration |
| **Returning** | существует память прошлого выбора | показать материальную дельту или продолжить незавершённый план, а не начать каталог заново |

Активацией **не являются** Hero Talk completion, карточный click, scroll depth или лайк в одиночку. Активация начинается с `interest hit` и подтверждается действием, которое означает реальное рассмотрение события: билет/регистрация/телефон, календарь/сохранение, share или доказанная связка detail+dwell с последующим действием.

### Ось B — текущая работа

- срочно найти вариант сегодня;
- заранее выбрать завтра;
- собрать shortlist на выходные;
- проверить конкретное событие;
- найти что-то по сложному свободному запросу;
- получить вдохновение без сформулированного желания;
- вернуться к сохранённому плану;
- увидеть новое релевантное с прошлого визита.

Текущая работа имеет приоритет над долговременным профилем в пределах своего контекста. Если зрелый любитель рока один раз ищет детский спектакль для племянника, этот search intent должен влиять на текущую выдачу, но не превращать весь long-term profile в детский.

## 3. Роли фиксированных поверхностей

| Поверхность | Пользовательская работа | Роль в системе | Успех | Опасный анти-паттерн |
|---|---|---|---|---|
| **Главная + Hero Talk** | «С чего начать именно сейчас?» | снимает неопределённость и выбирает следующий сценарий | downstream interest/value, а не CTR реплики | баннер, псевдо-чат или дублирование меню |
| **Сегодня** | «Что ещё реально успеть сегодня?» | срочный time-bound candidate set | жизнеспособный event/action | смешать прошедшее, недоступное и абстрактно популярное |
| **Завтра** | «Что выбрать заранее на завтра?» | ближнее планирование | shortlist/event action | показывать только события, формально стартующие завтра |
| **Выходные** | «Куда можно отправиться; что сравнить?» | быстро формирует shortlist 1–3 вариантов | shortlist, share, calendar/save, ticket | полный каталог без помощи в выборе |
| **Популярное** | «Что уже привлекло внимание других?» | cold-start/social-proof rescue | переход к жизнеспособному кандидату | принять popularity за personal relevance |
| **Поиск** | «Найди по моим словам и ограничениям» | explicit intent и сильный session signal | релевантный result + event value | требовать логин до первой выдачи |
| **Event detail** | «Подходит ли мне именно это?» | проверка фактов и принятие решения | ticket/register/phone/save/calendar/share | тупик после неподходящего anchor |
| **Похожие** | «Покажи ещё такого рода» | context rescue | второй подходящий кандидат | бесконечно усиливать неудачный anchor |
| **Продолжение** | «Если здесь не нашлось — догоните интересное» | общий recovery layer | relevant exposure/value до 30 | одинаковая глобальная лента без исходного intent |

### Сквозные пути

```text
TG/VK/MAX event link
→ event detail
→ action ИЛИ related
→ broader continuation

social/SEO selection
→ Today/Tomorrow/Weekend/Popular
→ shortlist/event
→ context-preserving continuation

direct home
→ Hero Talk
→ один фиксированный маршрут / search / event
→ event decision

explicit need
→ public search
→ event
→ current-query continuation
```

Любая поверхность может быть началом, но ни одна не должна становиться тупиком.

## 4. Hero Talk как продукт, а не интерфейсный трюк

### Самостоятельная роль

Hero Talk — **автоматизированный редакционный диспетчер главной**. Он выбирает один наиболее уместный повод и маршрут из уже существующего inventory:

1. временной сценарий: сегодня, завтра, выходные;
2. cold-start: популярное или разнообразный старт;
3. конкретная promo campaign;
4. сложный intent, который лучше передать в поиск;
5. возврат: новое релевантное или изменение плана;
6. конкретное необычное событие, если оно проходит eligibility и campaign gates.

Hero Talk не владеет каталогом и не заменяет ranker. Его output — проверенный route token и ограниченный grounded narrative, а не свободный разговор без конца.

### Decision contract

```text
entry/source context
+ current time/date
+ profile maturity
+ saved-plan state
+ new eligible inventory
+ active campaign eligibility/fatigue
→ один основной сценарий
→ максимум один вторичный следующий шаг
→ реальный destination с подходящим supply
```

### По состояниям

| Состояние | Что делает Hero Talk |
|---|---|
| Cold direct | предлагает одну понятную работу: сегодня/выходные/популярное или начать поиск |
| Learning | помогает уточнить текущий intent, но не проводит анкету интересов |
| Mature | ведёт к наиболее сильному невиданному candidate set или новому relevant delta |
| Returning | сообщает только материальную дельту: новое релевантное, изменение сохранённого, незавершённый план |
| Promo-eligible | встраивает disclosure-aware кампанию, не обходя relevance, fatigue, lifecycle и hide |

### Как Hero Talk испортить

- оптимизировать количество сообщений или CTR самого блока;
- показывать кампанию без подходящего inventory;
- заставлять отвечать перед доступом к страницам;
- изображать runtime-ИИ, хотя решение build-time/static-first;
- повторять навигацию словами без добавочной ценности;
- считать promo exposure органическим preference signal;
- позволять кампании вытеснить сильный персональный кандидат.

### Его KPI

- downstream `interest_hit_rate`;
- time/cards from Hero Talk exposure to first value;
- доля dead-end маршрутов;
- campaign incremental value против holdout;
- повтор/усталость/скрытие;
- P30 после входа через Hero Talk.

## 5. 30-card promise: точный продуктовый контракт

### Что обещаем

> У зрелого eligible пользователя первый ground-truth relevant event family должен появиться не позже 30-го уникального валидного показа во всём текущем discovery journey, если подходящее активное событие существует и достижимо в каталоге. После показа отдельно измеряется realized value.

Это внешний ceiling, сформулированный владельцем. Существующий E2E contract дополнительно содержит более строгий внутренний golden-persona gate `cards_to_first_relevant <=20`. Его следует сохранить как regression/optimization target; он не отменяет пользовательскую формулировку 30.

### Единица счёта

- **Journey/episode:** один пользовательский intent от входа до success, явной смены задачи или длительной неактивности.
- **Valid impression:** карточка действительно находилась в активном viewport достаточно долго. Конкретный threshold фиксируется только после A/A instrumentation baseline; DOM render не равен показу.
- **Unique:** одна canonical event family/linked occurrence group расходует одну возможность.
- **Direct event landing:** если пользователь пришёл на конкретное событие и оно дало value, это `landing_value` rank 0; рекомендации начинаются с rank 1.
- **Cross-surface:** переход `Выходные → event → related → Popular` не обнуляет счётчик.
- **Cross-tab desktop:** параллельные вкладки принадлежат одному journey, если сохраняют тот же intent/episode.

### Две метрики вместо одной

```text
relevant_exposure@30 =
  первое human/golden-relevant event family показано на rank <= 30

realized_value@30 =
  value action произошло по event family,
  впервые показанному на rank <= 30
```

`relevant_exposure@30` проверяет retrieval/ranking. `realized_value@30` проверяет весь продукт, включая объяснение, доверие и CTA. Простой card click не является достаточным success label.

### Иерархия outcome signals

1. ticket / registration / phone CTA;
2. save / favorite / calendar;
3. успешный share/copy;
4. explicit `нашёл / подходит` с event attribution;
5. detail + meaningful dwell и последующее связанное действие;
6. like — слабый fallback, а не план и не покупка.

### Диагностика провала

| Класс | Что произошло | Кто отвечает |
|---|---|---|
| `supply_gap` | в актуальном каталоге нет подходящего события | acquisition/supply coverage |
| `retrieval_failure` | событие есть, но не попало ни в один доступный candidate pool | retrieval/content projection |
| `ranking_failure` | событие было в pool, но оказалось после 30 | ranking/orchestration |
| `realization_failure` | событие показано, но пользователь не признал ценность | presentation/trust/CTA/label |

Supply gap нельзя исключать из общего product-health отчёта, но нельзя выдавать за ошибку ranker. Равным образом early exit до 30 нельзя просто удалить из denominator: это отдельный failure/censoring cut.

### Recovery до 30

Стартовая проверяемая гипотеза, а не навсегда зафиксированный алгоритм:

| Окно | Стратегия |
|---|---|
| 1–10 | максимально сильное пересечение текущего intent, profile affinity, actionability и diversity |
| 11–20 | уменьшить узкую similarity, добавить соседние форматы, свежие невиданные события и controlled exploration |
| 21–30 | не добавлять filler; показать лучшие оставшиеся unseen candidates и явный rescue: уточнить constraint или перейти к свободному поиску |

Смена поверхности не должна сбрасывать окно. `Популярное` может быть одним из recovery inputs, но не универсальной заменой релевантности.

### Что нужно построить

1. общий `discovery_episode_id`;
2. общий exposure ledger с `event_family_id`, first valid rank и seen/hide state;
3. единый action contract на event/list/search/related/popular/Hero Talk;
4. context token текущей задачи и source/campaign promise;
5. candidate reservoir: context/related → profile → fresh/popular → exploration → bounded promo;
6. hard lifecycle/actionability filters до ranking;
7. дедуп canonical programme, linked occurrences и повторов между страницами;
8. golden/human relevance pack и supply availability matrix;
9. A/A instrumentation gate до ranker A/B.

## 6. Fast weekend planning

### Job

> «За несколько минут понять, что реально доступно в ближайшие выходные, выбрать 1–3 жизнеспособных варианта и при необходимости согласовать их с другим человеком».

Этот сценарий должен работать у cold user, без авторизации и без накопленной персонализации.

### Product output

Не «страница просмотрена», а:

- пользователь понял ширину доступных вариантов;
- отсеял несовместимое по дню/времени/месту/цене/компании;
- сформировал shortlist из 1–3 событий;
- открыл detail и совершил calendar/save/share/ticket action хотя бы по одному;
- если ничего не подошло, continuation сохранил weekend intent.

### Критический текущий разрыв

Текст `/vyhodnye/` обещает события, которые начинаются или продолжаются в интервале, но current helper выбирает только записи со `start_date` в субботу/воскресенье и исключает long-running. Аналогичный смысловой конфликт есть на `/zavtra/`.

Это не UI-деталь и не A/B-гипотеза. Нужно принять честное продуктовое правило:

- либо page job — «что можно посетить», тогда продолжающиеся выставки/фестивали/долгие форматы включаются отдельным объяснимым слоем;
- либо page job — «что начинается», тогда promise и название результата должны это прямо говорить.

Для заданного пользователем planning job рекомендуется первое.

### Метрики

- time to first viable candidate;
- shortlist creation rate;
- weekend value action rate;
- cards to first interest;
- no-result/exhaustion rate;
- share-to-return/group-decision rate;
- доля ongoing/long-running supply, ошибочно исключённого или продублированного.

## 7. Mobile и desktop: одни страницы, разные способы жить

Это не две информационные архитектуры и не одна одинаково растянутая лента.

### Mobile

- чаще вход из Telegram/VK/MAX на event или selection;
- последовательный просмотр и короткая прерываемая сессия;
- календарь/save/share помогают не потерять результат;
- event detail естественно продолжается вертикально;
- возвращение назад обязано сохранять место, intent и seen-state;
- pinch-density может ускорять сканирование, но не заменяет доступный явный control и не должен ломать browser zoom.

### Desktop

- чаще direct/SEO/search/planning;
- сравнение 2–4 кандидатов и несколько вкладок;
- value может возникнуть после сопоставления, а не первого click;
- related/continuation живут как grid/list/modules, а не как имитация mobile feed;
- shortlist, cross-tab exposure dedupe и сохранение исходного intent важнее бесконечного scroll.

### Общее

- один event/profile/candidate meaning;
- один canonical seen/hide state;
- один journey KPI;
- разные правила valid impression и presentation context;
- обязательный разрез метрик по `viewport_class`, `layout_mode`, surface и acquisition path.

До появления identity/sync обещание персонализации честно ограничено одним browser profile. Cross-device continuity следует подключать после первой ценности — например, при save, email subscription или желании открыть shortlist на другом устройстве, а не требовать account на входе.

## 8. Возвратность: не ежедневный feed, а новый повод

Событийная афиша не обязана становиться ежедневной привычкой для всех. Её естественная частота — occasion-driven и weekly planning. Поэтому возвращать нужно не фразой «зайдите ещё», а новой полезностью.

### Четыре причины вернуться

1. **Новое релевантное:** пересечение `new/meaningfully changed × unseen × relevant × active/actionable`.
2. **Сохранённый план:** событие приближается, переносится, отменяется или требует действия.
3. **Стало лучше:** накопленный профиль способен дать существенно более точный следующий set.
4. **Запрошенная подборка:** weekly/weekend email, на который пользователь отдельно подписался.

### Обязательные петли

```text
discovery → strong signal → better next ranking → fewer cards to value

save/calendar → reliable plan state → change/reminder → trusted return

meaningful visit → relevant catalog delta → Hero Talk/email route
→ new value → refreshed profile
```

### Email

Принятый контракт сохраняется:

- ровно три сильных события;
- если трёх нет, рекомендационное письмо не отправляется;
- consent рекомендаций отделён от transactional consent;
- событие перепроверяется перед отправкой;
- один experiment сравнивает три прямые event links с одним входом на `noindex` secret personal selection;
- success — incremental qualified return и value после перехода, не open rate.

Email не должен компенсировать слабый P30. Generic Web Push откладывается до доказанной ценности и явного opt-in.

### Метрики возвратности

- qualified R7/R14/R28 после первого confirmed interest;
- active planning weeks в rolling 4 weeks;
- first-success → second-success conversion;
- return-to-interest rate и cards-to-interest на возврате;
- saved-plan revisit/update delivery;
- relevant-new inventory coverage per profile/week;
- repeat-exposure и exhausted-feed rate;
- email incremental value по user-level holdout;
- unsubscribe/complaint/no-action-send streak.

D1 retention и количество сессий сами по себе не являются north star.

## 9. Контентная система вокруг базы

Дополнительные static pages допустимы не как новая верхнеуровневая IA, а как автоматически включаемые acquisition/destination assets.

### Автоматические fact-based selections

Примеры:

- бесплатно;
- с детьми;
- под открытым небом;
- новое;
- Пушкинская карта;
- доказавшие спрос комбинации не более двух устойчивых условий.

Они публикуются только при достаточном чистом supply, имеют стабильный URL для social/SEO/share и не обязаны становиться пунктами меню.

### Grounded narrative selections

Примеры:

- сегодня вечером;
- в дождь;
- туристу на день;
- семейная суббота.

Их формат может быть текстовым повествованием, ведущим от события к событию. Это не отменяется рекомендацией Gemini «не делать тяжёлые статьи»: пользователь прямо задаёт narrative job, и он полезен при situational planning. Ограничение другое — narrative создаётся build-time только из проверенного candidate set и не публикуется на thin/incoherent inventory.

### Saved-search static pages

Удачные поисковые intents могут становиться регулярно обновляемыми публичными страницами только после нормализации, дедупликации, novelty/demand proof и supply gate. Это acquisition expansion, а не утечка частной истории.

### Personal selection pages

High-entropy `noindex` page допустима как email/return destination и experiment format. Она не должна превращаться в публичный профиль или единственный способ получить рекомендации.

### Что не создавать по умолчанию

- страницы площадок/организаторов только ради SEO;
- городские справочники без самостоятельного job;
- страницы любой случайной комбинации фильтров;
- автоматически написанные истории при недостаточном supply;
- top-level hubs `Категории` и `Подборки` только ради раскладки taxonomy;
- новые постоянные разделы до доказанного провала существующих путей.

## 10. Supply reality

Committed production-like snapshot этой ветки был построен 2026-07-02 и содержит 399 real active/future rows:

- 303 Калининград;
- 52 Светлогорск;
- 159 концертов;
- 70 выставок;
- 41 с Пушкинской картой;
- 199 с unknown ticket status;
- 204/399 с ненулевым source engagement input;
- related manifest покрывает 399 anchors, имеет 12–40 unique similar/explore candidates, median 30.

Это доказывает номинальный объём, но не доказывает:

- наличие хотя бы одного релевантного события для каждой persona × week × city × time × price × audience;
- отсутствие canonical duplicates;
- actionability кандидата;
- human relevance top-30;
- пригодность popularity inputs для свежего/long-tail inventory.

Нужна coverage matrix за 8–12 недель с p10/p50/p90 eligible canonical programmes по основным scenarios и golden facets. Пока её нет, абсолютное «точно найдём» является внутренней целью, а не публичным marketing claim.

## 11. KPI framework

### North star

```text
qualified_event_finds =
  discovery journeys с подтверждённым interest/value event
```

North star обязательно сопровождается эффективностью:

- `relevant_exposure@30`;
- `realized_value@30`;
- median/p75 cards-to-value;
- time-to-value.

### Driver tree

```text
qualified_event_finds
├─ eligible relevant supply
├─ retrieval coverage
├─ ranking within 30
├─ valid exposure
├─ event-page confidence/actionability
└─ return with relevant catalog delta
```

### Guardrails

- stale/cancelled/sold-out leakage;
- duplicate/linked occurrence waste;
- hidden-event recurrence;
- category/venue/source concentration;
- not-interested rate in top 30;
- promo share/fatigue;
- search no-result/mismatch;
- page/CTA latency and fallback rate;
- no-consent/privacy failures;
- email complaints/unsubscribe;
- cold vs mature, mobile vs desktop и supply-present vs supply-gap не объединяются одной средней.

### Hero Talk, email и search не получают локальных vanity north stars

- Hero Talk completion/CTR — диагностика;
- email open rate — диагностика;
- search query count — диагностика;
- like rate — learning signal.

Их успех — downstream qualified event find и сокращение cards/time-to-value.

## 12. Приоритетный roadmap

### P0 — превратить fixed surfaces в один измеримый продукт

1. Зафиксировать surface jobs и включить продолжающиеся события в честный contract `Завтра/Выходные`.
2. Дать public unauthenticated search value; identity запрашивать только для save/sync/subscription.
3. Выпустить минимальный static-first Hero Talk как router/campaign dispatcher без runtime AI и без длинного диалога.
4. Ввести общий `discovery_episode_id`, valid impressions, canonical event-family dedupe и cross-surface exposure ledger.
5. Унифицировать action attribution на listing/search/event/related/popular/Hero Talk.
6. Передавать в continuation реальный surface/source/campaign/search context, а не один общий `listing_personal_feed`.
7. Провести A/A instrumentation gate и baseline P30/supply measurement до A/B ranker claims.
8. Исправить production acquisition/canonical/static publication gates отдельно от продуктовой концепции.

### P1 — замкнуть learning и return loops

1. Context-aware cross-surface candidate orchestrator с unseen/fatigue/diversity/recovery.
2. Обучение профиля strong signals со всех поверхностей, а не только related prototype.
3. `Сохранить / Мои события` как надёжный plan state, отдельный от like и ICS attempt.
4. Лёгкая identity/profile sync после первой ценности, особенно для mobile ↔ desktop.
5. `new relevant since last meaningful visit` и material saved-event updates.
6. Golden/human relevance pack, eligible supply matrix и внутренний `<=20` regression target при внешнем ceiling 30.
7. Exactly-three email canary с user-level holdout и no-send при слабом set.

### P2 — автоматическое расширение acquisition

1. Factory fact-based selections с demand/supply/quality gates.
2. Build-time grounded narrative writer + verifier.
3. Автоматическая публикация нормализованных доказавших ценность search intents.
4. Secret personal-page email experiment.
5. Более тонкая multi-horizon персонализация и cross-device return orchestration после доказательства P0/P1.

## 13. Пять приоритетных экспериментов

1. **Cross-surface personalization:** current static/context order против shared-profile + unseen ledger + context-aware ranker. Primary: mature eligible `realized_value@30`; diagnostic: `relevant_exposure@30`.
2. **No-success recovery:** смена стратегии после 10/20 impressions против неизменного ranking; сравнить explicit constraint/search rescue и silent diversity expansion.
3. **Weekend job:** current chronological listing против planning-oriented candidate/shortlist contract с ongoing supply; success — viable shortlist/value, не raw CTR.
4. **Hero Talk orchestration:** разные grounded router scenarios и campaign arbitration; измерять downstream value и P30, а не animation/CTR. Наличие capability не пересматривается, тестируется её работа.
5. **Return trigger:** обычный вход против `new relevant since last meaningful visit`, а затем email holdout и format split `3 links` vs `secret page` только после прохождения relevance gate.

Перед ними обязательна A/A-проверка episode stitching, dedupe, impression validity, consent/bot exclusion и orphan action rate. Размер выборки и uplift не назначаются до baseline.

## 14. Anti-roadmap

Пока не строить:

- generic infinite feed и продуктовую цель «ежедневный doomscroll»;
- обязательную анкету жанров при первом входе;
- auth wall перед поиском/просмотром;
- комментарии, публичные профили и social network;
- собственный ticket checkout;
- generic push без конкретного opt-in;
- десятки постоянных category/intent pages в меню;
- entity pages площадок/организаторов без самостоятельного user job;
- narrative/SEO pages без demand, supply и fact gates;
- ranker training на одном факте promo exposure или разовом search intent;
- обещание cross-device personalization до появления identity/sync.

## 15. Минимальная итоговая навигация как следствие продукта

Навигация не является центром анализа, но из модели следует простой target:

```text
Главная (логотип / Hero Talk)
Сегодня · Завтра · Выходные · Популярное · Поиск · Мои события
```

- `Мои события` появляется как полноценное назначение вместе с durable save/plan state; до этого нельзя показывать пустую имитацию профиля.
- automatic и narrative selections распространяются через social/SEO/Hero Talk/search и не обязаны становиться постоянными пунктами.
- mobile и desktop используют те же назначения и названия; отличаются доступность, геометрия и последовательный/сравнительный режим работы.
- жанр остаётся поисковым/контекстным способом сузить результат, а не обязательным новым верхним hub.

## 16. Итоговые типы статических страниц

### Постоянное ядро

1. главная с Hero Talk и grounded static fallback;
2. `Сегодня`;
3. `Завтра`;
4. `Выходные`;
5. `Популярное`;
6. публичный search shell/results;
7. event detail;
8. static related/continuation manifests;
9. static shell `Мои события` без private CDN HTML.

### Генерируемые acquisition pages

10. fact-based automatic selections;
11. approved normalized saved-search intents;
12. grounded narrative selections;
13. campaign destinations, когда отдельная страница действительно нужна;
14. festival/program pages только для самостоятельного multi-event planning job.

### Непубличные/персональные static shells

15. `noindex` secret personal selection;
16. lifecycle-aware saved-plan shell с client-side private hydration.

## 17. Что принято и скорректировано из Gemini Pro review

### Принято

- концепция связна, но фактически surfaces пока разорваны;
- auth-gated search — P0 blocker;
- long-running exclusion ломает planning job;
- общий exposure ledger обязателен;
- surface context нельзя сводить к одному generic feed;
- browser-local maturity не равна cross-device product memory;
- Hero Talk должен измеряться downstream value;
- email — ровно три сильных события или no-send;
- infinite feed, social network и собственный checkout не нужны.

### Скорректировано

- Gemini назвал простой переход в раздел активацией. Здесь активация требует `interest hit` и meaningful follow-up action.
- Gemini предложил гарантировать взаимодействие внутри 30. Здесь отдельно гарантируется relevant exposure и измеряется realized value: интерфейс не может заставить человека кликнуть.
- Gemini предложил «остановить счётчик» при supply gap. Здесь gap фиксируется честно; filler не добавляется, но product-health failure не исчезает.
- Gemini свёл Hero Talk прежде всего к многомерным запросам. Здесь его роль шире: direct-entry router, catalog-delta narrator и bounded campaign dispatcher; сложные запросы передаются в поиск.
- Gemini поставил Hero Talk в P2. Само наличие уже принято владельцем, поэтому минимальный static router входит в P0; персональные/сложные сценарии могут идти позже.
- Gemini предложил A/B long-running inclusion. Это не эксперимент, а исправление несоответствия page promise; экспериментировать нужно с planning presentation после корректной выборки.
- Gemini отверг тяжёлые narrative pages в целом. Здесь grounded narrative сохраняется для явных situational jobs, но публикуется только при достаточном inventory и fact gates.

## 18. Caveats и дальнейшие вопросы

### Ограничения анализа

- Нет production behavioral dataset и baseline conversion/retention.
- Snapshot от 2026-07-02 — production-like, но не доказывает состояние каталога 2026-07-18.
- Количественные thresholds recovery, impression duration, profile maturity и email cadence остаются preregistered hypotheses до A/A/A/B evidence.
- Внешняя критика — один свежий независимый Gemini Pro review; это expert evidence, не пользовательская валидация.

### Решения, которые нужны после baseline, а не сейчас

1. Какой calibrated profile maturity threshold действительно предсказывает P30.
2. Как различать один journey и реальную смену intent.
3. Какая доля exploration максимизирует P30 и последующий return без concentration.
4. Нужна ли cross-device sync всем или только пользователям save/email.
5. Когда именно personal secret page выигрывает у трёх прямых links.
6. Какие automatic/narrative intents имеют достаточный повторяемый demand и p10 supply.

## Финальное решение

База страниц правильная и достаточная. Недостающий продукт — не ещё один раздел, а **сквозная система выбора**:

```text
Hero Talk / date / popular / search / event entry
→ сохранённый task context
→ unique candidate opportunities across surfaces
→ relevant exposure <= 30 when supply exists
→ realized value
→ profile + plan state
→ relevant catalog delta
→ квалифицированный возврат
```

Если построить именно эту связность, KenigEvents станет продуктом, который сначала помогает новому пользователю решить понятную задачу без входного барьера, а затем использует накопленную память, чтобы у зрелого пользователя интересное действительно «догнало» его по любому из естественных маршрутов. Если ограничиться страницами и отдельными лентами, получится качественная афиша, но не обещанная персональная система поиска события.
