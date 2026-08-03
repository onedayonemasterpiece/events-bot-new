# Согласование стандартного онбординга с каноникой Hero-talk

> **Статус:** нормативная корректировка strategy v0.3 → v0.4.
> **Дата:** 2026-08-03.
> **Источник Hero-talk:** `docs/features/hero-talk/README.md` в stacked PR
> [#291](https://github.com/onedayonemasterpiece/events-bot-new/pull/291),
> ветка `agent/hero-talk-chain-research-20260803`.
> **Обновляемые документы:** [каноническая onboarding strategy](README.md) и
> [варианты стратегии](strategy-options.md).

## 1. Итог

Utility-first вывод исследований сохраняется: обычному пользователю не нужен
обязательный тур, tutorial wall, checklist или admission wall. Однако strategy
v0.3 допустила две новые ошибки уже после сверки с Hero-talk:

1. назвала текущий `HomeHeroTalk` rollback-версией будущего механизма;
2. вынесла артефакты-пасхалки в отдельный необязательный post-release слой C и
   тем самым исключила их из канонического онбординга.

Обе формулировки отменены.

Каноническая граница теперь такова:

```text
onboarding strategy
  owns eligibility, competency, result truth, dismissal and suppression;

Hero-talk
  owns typed briefing, cursor, tile media, narrative chain,
  page/entity context, bounded thread memory and served plan;

artifact programme
  owns collectible identity, placement, collection progress,
  provenance, accessibility and campaign/rules state;

Клуб друзей Анонсов
  owns membership and regular ticket-raffle lifecycle,
  eligibility/application, published rules, draw and claim state.
```

Артефакты являются **частью стандартного онбординга и продуктовой идентичности**,
но не являются гейтом доступа к базовой афише. Hero-talk уже содержит
канонический onboarding-сценарий `Первый артефакт`, а также intents
`artifact_hint` и `club_discovery`.

## 2. Исправленные расхождения

| Ошибочная формулировка v0.3 | Фактическая/каноническая модель | Корректировка v0.4 |
|---|---|---|
| `current HomeHeroTalk` сохраняется как rollback | Текущий компонент — существующий static skeleton и исходная точка миграции | Legacy-компонент не объявляется rollback-контрактом. Rollback/fail-safe — generic static first scene внутри нового Hero-talk renderer/served-plan механизма |
| Артефакты не входят в onboarding MVP | Каноника Hero-talk прямо включает подсказку о первом артефакте; в интерфейсе уже существуют `/artefakty/`, коллекция и размещённая находка | Discovery первого артефакта входит в onboarding scope; дальнейшее раскрытие прогрессирует от точной подсказки к самостоятельному поиску |
| Cultural layer C можно полностью удалить без изменения A/B | Артефакты — один из ключевых брендовых и retention-контуров, связанный с Клубом друзей Анонсов и регулярными розыгрышами билетов | Отдельный вариант C удалён. Артефактный контур входит в вариант A; вариант B меняет богатство narrative/return chains, а не наличие артефактов |
| Любые prize mechanics относятся только к фокус-группе | Focus-group scoring/feedback missions действительно изолированы, но стандартный клубный розыгрыш — отдельное owner-approved product направление | Не переносить leaderboard, NPS/feedback scoring и исследовательские missions; при этом не запрещать регулярный raffle programme Клуба друзей Анонсов |
| Артефакт допустим только после mastery core UI | Первый артефакт может знакомить с характером сервиса уже в раннем освоении, если основная ценность и CTA не заблокированы | Eligibility определяется journey/context и non-interference, а не универсальным требованием `mastered` |
| Artifact find не относится к onboarding | Find не доказывает mastery утилитарной функции, но доказывает освоение самой механики артефактов | Вводится отдельная artifact competency/collection state; она не загрязняет taste profile и не подменяет event activation |

## 3. `HomeHeroTalk`: правильная модель миграции

```text
current HomeHeroTalk.astro
→ shared Hero-talk renderer
→ useful static first scene в том же механизме
→ optional precompiled contextual chain
```

Текущий компонент:

- является фактической текущей реализацией зоны;
- служит donor/baseline для фактов, CTA и layout constraints;
- не является вторым продуктом рядом с Hero-talk;
- не фиксируется как постоянная rollback-ветка.

При сбое compiler, profile state, JavaScript или media новый Hero-talk обязан
показать собственную generic static first scene. Откат осуществляется kill
switch/served-plan fallback, а не возвратом к legacy-компоненту как отдельной
архитектуре.

## 4. Артефакты как часть онбординга

### 4.1. Роль

Артефакты одновременно решают четыре задачи:

1. знакомят с локальной идентичностью и характером сервиса;
2. мягко проводят через разные полезные поверхности сайта;
3. формируют коллекционный повод вернуться;
4. ведут к Клубу друзей Анонсов и его регулярным розыгрышам билетов.

Это не означает, что event discovery превращается в игру. Факты события,
навигация, поиск, сохранение и переход к организатору остаются доступны без
находок и клубного статуса.

### 4.2. Прогрессивное обучение механике

```text
первое знакомство с сайтом и получение базовой ценности
→ Hero-talk: точная подсказка о первом артефакте
→ доступная находка в стабильном placement
→ точный action echo и открытие истории
→ коллекция и объяснение правил
→ следующие подсказки могут быть менее прямыми
→ threshold / добровольная заявка по опубликованным правилам
→ Клуб друзей Анонсов / регулярный raffle lifecycle
```

Первая подсказка должна быть точной и доступной. Сложность может расти только
после доказанного понимания механики. Нельзя требовать hover, motion, точного
наведения, второго устройства или недоступного route.

### 4.3. Отдельные state domains

```text
core capability state
  save/share/search/date/etc.

artifact state
  discovered/found/collection progress/hint/dismissal

club state
  invited/applied/member/suspended/left

raffle state
  rules accepted/application submitted/eligible snapshot/draw/claim
```

Ни один домен не выводится автоматически из другого. В частности:

- artifact find не становится taste signal;
- onboarding exposure не становится находкой;
- threshold не подаёт заявку автоматически;
- membership не означает consent на promo;
- скорость, share, like и покупка не должны тайно менять odds;
- event facts и core actions не блокируются отсутствием артефактов.

Точная формула регулярных розыгрышей принадлежит отдельному rules/legal/
anti-abuse contract. Стратегия фиксирует саму связь, но не выдумывает количество
билетов, частоту, географию, возраст, selection method или claim procedure.

## 5. Hero-talk integration

Артефактные и клубные сообщения используют тот же Hero-talk, а не отдельную
«говорящую» систему:

```yaml
intent: artifact_hint
origin: system | editorial_program
collection_id: <active collection>
artifact_id: <eligible artifact>
placement: home_hero | *_page_end
```

```yaml
intent: club_discovery
origin: system
antecedent: artifact_progress | explicit_interest
placement: collection_page_end | club_page_end | home_hero
```

Пример chain:

```text
На сайте спрятан первый артефакт.
→ Начните со страницы выходных.
→ Найденные истории соберутся в коллекции.
```

После появления опубликованных клубных правил допустима другая chain:

```text
В коллекции уже пять находок.
→ Этого достаточно, чтобы открыть добровольную заявку.
→ Условия розыгрыша — в Клубе друзей Анонсов.
```

Последний пример запрещён до фактического rules/application release.

## 6. Что по-прежнему не переносится из фокус-группы

- обязательные исследовательские задания;
- баллы за NPS, feedback, likes/dislikes или посещение page families;
- leaderboard исследовательского cohort;
- обязанность давать обратную связь;
- преимущество за интенсивность кликов;
- смешение исследования с обычным клубным членством.

Изоляция этих механик не отменяет самостоятельный стандартный raffle programme.

## 7. Варианты стратегии после корректировки

- **A — выбранный baseline:** полноценный Hero-talk, utility-first onboarding и
  обязательный artifact-discovery arc с точной первой подсказкой; клубный/raffle
  результат включается только после собственных release gates.
- **B — challenger:** та же основа плюс richer bounded return, personal,
  editorial, artifact-series и club continuity chains после HT-6/HT-7/HT-10.

Отдельного варианта C больше нет.

## 8. Что не меняется

- первая event value возникает до login, PWA, Push и profile setup;
- inline recovery и немедленный action echo важнее feature promotion;
- exposure не доказывает competence и не становится taste signal;
- одна новая смысловая задача продвигается за раз;
- dismissal/cooldown уважаются;
- Search, identity, reminder и personalization нельзя продвигать до release;
- Hero-talk runtime не вызывает LLM;
- no-JS/reduced-motion получают полный статический смысл;
- основной event CTA не перекрывается артефактом, клубом или розыгрышем.

## 9. Release dependency

```text
HT-1 static single-scene baseline
→ HT-2 deterministic handwritten chains
→ HT-4 contextual page-end
→ HT-5 onboarding + first-artifact integration
→ artifact collection/rules/ledger gate
→ Friends Club membership + regular raffle gate
→ HT-6 bounded return/club continuity
→ HT-7 editorial/campaign arcs
→ HT-10 controlled comparison
```

Документация и noindex prototype не являются production acceptance, но их
наличие не позволяет классифицировать артефакты как «выброшенный потом слой».
