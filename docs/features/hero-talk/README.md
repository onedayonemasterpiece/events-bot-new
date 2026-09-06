# Hero-talk

> **Current implementation cut: 2026-09-06, #643 — partial, not activated.**
> This canonical target contract and its companions are restored from the exact
> owner-assigned `85766e63` documentation, without restoring reverted UI code.
> [Autofill MVP](autofill-mvp.md) and the
> [implementation assignment](autofill-implementation-prompt.md) supersede older
> research proposals where they differ. Historical dates/examples are not live
> acceptance or approval of public content.
>
> Source now has existing-promo Hero activity gates and a pure locked-token
> compiler (`hero_talk/compiler.py`) plus canonical draft/version storage
> (`hero_talk/store.py`) and default-OFF owner verbatim draft MCP tools.
> Other content operations, actual model writer/reviewer,
> immutable storage publication, live control permits, browser
> analytics and full native/rendered acceptance are not delivered by this slice.
> MCP source/recovery and R0-only production evidence remain in the existing
> [MCP operational contract](../../operations/private-events-mcp.md).
>
> **Latest owner renderer decision (2026-09-06):** `home_hero` and `page_end`
> use the same typing/cursor/mosaic renderer and different contextual decks.
> Page-end is not a separate service CTA card. Two–three-line copy, mobile media
> and slower word reveal belong to the shared renderer; this backend does not
> hard-code animation/viewport geometry or claim Astro=SoT=Penpot acceptance.
>
> Companions: [owner MCP](owner-mcp-mvp.md), [release](release-plan.md),
> [testing](testing.md), [research](deep-research-prompt.md).

## Implemented compiler boundary (#643)

The internal normalized program has one shared `home_hero`/`page_end` placement
contract. Initial chains contain 1–3 forward-linked nodes. `text` fragments keep
Unicode code points exactly; `fact_token` and `link_token` resolve only through a
trusted, revision-bound canonical packet. Compiled fragments are
`{text, href?, accent?, breakAfter?}`; consumers must concatenate exact text, not
insert/trim spaces. No synthetic Event is needed for editorial/capability media.

`compile_program` is pure: no provider, DB, object-storage or browser calls. An
exact-input semantic acceptance receipt is mandatory; its trusted upstream issuer
is not implemented here. The digest covers program, packet and schema/style/
compiler/prompt/model/renderer policy versions. Syntax/graph/reference/dependency
expiry and bounded pack checks do not substitute for semantic review or actual
rendered overflow inspection. Same inputs produce identical content SHA and gzip
bytes, with no model calls or writes. Output contains no actor/private brief.

Media comes from trusted managed-media receipts, not arbitrary URL downloading:
role, public/rights/geometry acceptance, the exact encoded-object SHA and matching
current/geometry **pixel** SHA are required. Encoded-byte SHA and decoded-pixel SHA
are different identities; the trusted public-object receipt must bind both. Event/festival roles additionally bind a canonical dependency; an
editorial image does not impersonate an event photo. Unknown/private/unsigned
media cannot enter a public pack; an explicitly optional image may fall back to
text. Public CDN readback and durable media materialization remain upstream gates.

A campaign pack keeps its `campaign_id/activity_id` binding and cannot relabel
itself editorial. Compilation never changes campaign status or grants a permit;
current rights, shared promo caps and ≤60-second live authorization remain
required delivery gates. A compiled pack is not an active publication.

Component evidence: `tests/test_hero_talk_compiler.py`. All full #643 acceptance
IDs remain in the existing
[MCP v2 registry](../../testing/private-events-mcp-event-operations-scenarios.v2.yml)
with partial/missing/not-run statuses. No full feature/live gate is inferred from
compiler fixtures.

## Implemented draft storage boundary (#643)

`hero_talk_program` and `hero_talk_change_log` are additive canonical SQLite
content/history tables from the assigned domain design. They do not own promo
budgets, partner rights or campaign lifecycle. Initialization performs no seed,
backfill, generation, activation or publication. Browser telemetry remains in the
existing personalization pipeline, never one SQLite row per impression.

`HeroProgramStore.prepare_draft` persists a ten-minute frozen preparation and a
program identity if needed, but advances neither desired nor active revision.
`commit_draft` accepts only that exact actor-bound ref/digest and applies the
stored payload with current revision CAS. `put_draft` is an internal convenience
that executes those same two phases. Both require a host-injected current authorization
callback on the exact SQLite connection under `BEGIN IMMEDIATE`. The host owns
actual OAuth/current campaign policy; the storage helper does not manufacture
an identity or infer a grant from an ID. Actor/client/resource metadata and
hashed idempotency keys remain private history, not compiled output. A same-key
same-request replay rechecks current authorization and returns the original
operation; changed payload conflicts. Desired revisions use optimistic CAS;
concurrent edits have one winner. Version history, origin and campaign/activity
binding cannot silently change. Saving a new draft never changes active revision
or resumes a paused program/campaign.

This helper stores bounded drafts, not semantic acceptance or ready packs.
Current campaign existence/rights, compiler/reviewer acceptance, durable media,
public readback and live permits remain required application gates. There are no
automatic workers enabled by these tables. Tests:
`tests/test_hero_talk_store.py` cover restart, preparation expiry, replay/revocation,
concurrent CAS, rollback, preserved history and immutable origin binding.

### Default-OFF owner draft tools

`PRIVATE_EVENTS_MCP_HERO_DRAFTS_ENABLED=1` adds `hero_talk_prepare`,
`hero_talk_commit`, `hero_talk_get` (one exact program) and
`hero_talk_operation_get` to the existing owner ChatGPT/OpenCode resource.
Prepare currently supports **only** `action=upsert_draft`, `origin=editorial_program`,
`author_mode=verbatim`. It is a bounded private draft operation, not a publication
or semantic-acceptance promise. Other actions, campaign mutation, generation,
asset staging, rendered preview and analytics are not advertised by this slice.

Exact owner subject/client/resource/current expiry and explicit `hero:write` or
`hero:read` are required again inside each DB transaction. `events:write`, social
publish scopes, partner actors and Codex do not inherit these rights. Existing
OAuth default grants remain unchanged; Hero scopes require explicit consent.
Strict nested Pydantic schemas reject unknown authority/HTML-code fields and
coercions without echoing rejected private input values. Literal text is neither
trimmed nor rewritten. Commit receives only the durable preparation ref/digest;
changing client payload cannot change the frozen draft. Operation readback marks
expired preparations blocked and obsolete bases superseded without applying them.
No provider/LLM, Event,
PromoCampaign, JobOutbox, active revision or public asset is written by these tools.

Real local OAuth PKCE → MCP prepare → commit → restart → exact Unicode readback
is covered by `tests/test_private_events_mcp_hero_drafts.py`. This is private draft
acceptance, not live owner/publication/native acceptance. Production flag remains
OFF and staged release gates are unchanged.

## 1. Определение

**Hero-talk** — единый редакционный механизм короткой контекстной коммуникации
на статическом сайте KenigEvents.

У него два основных placement:

```text
Hero-talk
├── home_hero — основная верхняя зона главной страницы
└── page_end  — контекстное завершение поддерживаемой страницы
```

`Hero-talk` — общее маркетинговое и продуктовое название механизма. В
техническом контракте placement уточняется отдельно, например:

```text
hero-talk / home_hero
hero-talk / event_page_end
hero-talk / collection_page_end
```

Hero-talk не делится на отдельные продукты `Onboarding Talk`, `Campaign Talk`,
`Return Briefing` или `Club Talk`. Это разные **смыслы сообщений** внутри одного
механизма.

## 2. Неизменяемые признаки Hero-talk

Компонент нельзя называть Hero-talk только потому, что он находится в hero или
содержит рекламный текст. У Hero-talk есть конкретная узнаваемая грамматика.

### 2.1. Typed briefing

- текст появляется ускоренно по **смысловым фрагментам**, создавая ощущение
  печатающегося briefing;
- буквальная медленная печать каждого символа не является основным режимом;
- первый полезный фрагмент доступен сразу;
- текст после появления остаётся неподвижным и читаемым;
- ссылки появляются атомарно и имеют стабильную область нажатия;
- hover, keyboard focus, pointerdown или явная пауза немедленно завершают
  текущую фразу и останавливают переход;
- на первом касании ссылка открывается, а не только ставит анимацию на паузу.

### 2.2. Курсор

Курсор — часть семантики состояния, а не бесконечный декор.

- во время появления смысловых фрагментов допустим vertical bar или underscore;
- после полного текста курсор остаётся только если следующий переход реально
  запланирован;
- в terminal/manual/paused state курсор исчезает;
- после последней сцены допустимо несколько завершающих миганий, но не
  бесконечное обещание продолжения;
- no-JS и `prefers-reduced-motion` получают немигающий статический вариант.

### 2.3. Медиа-мозаика

Не у каждой сцены есть изображение. Если изображение используется, оно может
появляться и исчезать квадратными фрагментами:

```text
text-only scene
или
text + tile mosaic built from one exact source image
```

Правила:

- медиа связано с текущей сценой и её canonical объектом;
- copy, CTA и media-plan переключаются атомарно;
- terminal scene не выбрасывает изображение только потому, что закончился
  таймер;
- исчезновение начинается только при реальном successor;
- если exact asset исчез, не проходит role/crop/quality gate или не декодируется,
  сцена деградирует в полноценный text-only вариант;
- mobile, low-data и reduced-motion могут не загружать растровый слой;
- мозаика не является обязательной иллюстрацией каждой реплики.

### 2.4. Будущий video-mosaic experiment

Видео внутри квадратной мозаики технически рассматривается как отдельный
эксперимент, а не как уже принятая production-функция.

Предпочтительный первый prototype:

```text
one muted playsinline video
→ one decoded frame source
→ one canvas/tile projection
→ existing Hero-talk entry/hold/exit state machine
```

Не следует начинать с десятков отдельных `<video>` элементов или обязательного
видео на телефоне. Для эксперимента требуется найти горизонтальное source video
конкретного будущего события в Telegram/VK, зафиксировать provenance, exact
source reference и hash, проверить права использования и опубликовать только в
immutable noindex lab.

## 3. Фактическая отправная точка `main`

Главная уже строится в последовательности:

```text
HomeHeroTalk
→ HomeQuickNav
→ HomeColdStartFeed
```

Текущий `HomeHeroTalk.astro` — статический skeleton с постоянным обещанием
сервиса, CTA по дате/поиску и одной карточкой события. Это не отдельный
обязательный «service promise» поверх Hero-talk. Это временное наполнение зоны,
которая продуктово предназначена самому Hero-talk.

Целевая миграция:

```text
current static HomeHeroTalk
→ shared Hero-talk renderer
→ useful static first scene
→ optional precompiled contextual/personal chain
```

Историческая typed-briefing лаборатория содержит ценные research/visual/test
решения, но сильно разошлась с актуальным `main`; она является donor evidence,
а не веткой для wholesale merge.

## 4. Три независимых слоя

Чтобы не смешивать placement, содержание и источник, каждая программа Hero-talk
описывается тремя независимыми координатами.

### 4.1. Placement — где показано

```text
home_hero
event_page_end
collection_page_end
date_listing_page_end
search_page_end
personal_feed_page_end
club_page_end
```

Все `*_page_end` являются разновидностями одного placement `page_end`.

### 4.2. Intent — какую задачу решает сообщение

```text
greeting
local_identity
service_orientation
current_context
return_delta
personalized_discovery
editorial_discovery
event_spotlight
festival_program
feature_discovery
artifact_hint
weather_context
saved_event_status
reminder_explanation
club_discovery
serendipity
lifecycle_or_safety
fallback
```

### 4.3. Origin — кто поставил программу

```text
system
catalog_signal
user_state
editorial_program
promo_campaign
lifecycle
```

Пример:

```yaml
placement: home_hero
intent: festival_program
origin: promo_campaign
campaign_id: kantata-2026
persona_pack: culture-explorer
```

Другой пример:

```yaml
placement: event_page_end
intent: feature_discovery
origin: system
capability_id: event-share
context_event_id: 7123
```

## 5. Приветствие и локальный голос

Приветствие — полноценное содержание Hero-talk, а не декоративная строка,
которую можно потерять при классификации сценариев.

Обязательные стартовые families:

### 5.1. Daypart greeting

Примеры направления:

```text
Добрый день! Что сегодня удивит?
Добрый вечер! Посмотрим, что ещё начинается сегодня?
```

Требования:

- daypart определяется в `Europe/Kaliningrad`;
- приветствие не занимает отдельную длинную бесполезную сцену;
- оно либо сразу соединено с текущей задачей, либо является первым узлом
  короткой цепочки;
- UTC сервера не выдаётся за локальное время человека.

### 5.2. Local identity greeting

Сохранённый смысловой сценарий:

```text
Мы говорим по-калининградски.
И даже можем сказать «кеска».
```

Это human-approved локальная редакционная фраза, а не утверждение о пользователе
и не inferred preference.

Требования:

- употреблять фигурные кавычки `«кеска»`;
- не повторять часто;
- не использовать как универсальный opener каждой сессии;
- связывать с региональной идентичностью сервиса или следующим осмысленным
  маршрутом;
- не превращать локальную иронию в искусственную фамильярность.

## 6. Онбоардинг внутри Hero-talk

Hero-talk не владеет самостоятельной стратегией онбоардинга. Нормативным
источником eligibility, competency state, success evidence, dismissal и
suppression является
[стратегия онбоардинга](../static-site-onboarding/README.md).

Сообщение о функции имеет:

```text
intent=feature_discovery
capability_id=<capability>
```

и показывается в `home_hero` или подходящем `page_end`.

Примеры:

### Умный поиск

```text
Можно спросить обычными словами:
«куда с ребёнком в выходные».
```

### Поделиться

```text
Есть с кем пойти?
«Поделиться» отправит ссылку на это событие.
```

### Не интересует

```text
Не ваше?
«Не интересует» уберёт событие из следующих рекомендаций.
```

### Первый артефакт

```text
На сайте спрятан первый артефакт.
Подсказка: начните со страницы выходных.
```

Ключевые правила:

- одна новая возможность за раз;
- подсказка появляется в момент релевантной задачи;
- показ не равен освоению;
- успешная продуктовая операция подавляет базовую подсказку;
- dismiss/cooldown уважаются;
- onboarding exposure, campaign click и artifact find не становятся taste
  signal;
- раздел `Что умеет сайт` хранит постоянный реестр возможностей и позволяет
  повторно запросить объяснение; его продуктовая модель разрабатывается в
  onboarding track.

## 7. Hero-talk должен быть chain-first

Hero-talk не проектируется как случайная ротация независимых слоганов. Базовая
единица планирования — **смысловая цепочка**.

Одиночная сцена допустима, но является цепочкой длины один.

### 7.1. Внутрисессионная цепочка

```text
Добрый день!
→ Пока вас не было — 12 новых событий.
→ Больше всего нового в выбранной теме «лекции».
→ Посмотреть новое в «Для меня».
```

Каждый следующий узел:

- продолжает topic anchor;
- отвечает на вопрос или обещание предыдущего;
- не меняет резко пользовательскую задачу;
- имеет понятный bridge relation;
- сохраняет один основной CTA-path.

### 7.2. Контекстная page-end цепочка

Если пользователь находится на странице конкретного события, Hero-talk должен
знать:

```text
context_event_id
occurrence/festival/club relations
page family
source route
current action state
what the user has just read or done
```

Пример после страницы события фестиваля:

```text
Вы посмотрели лекцию из программы «Кантаты».
→ В программе на этой неделе есть ещё одна встреча и два концерта.
→ Открыть программу фестиваля.
```

Пример после calendar save:

```text
Событие сохранено.
→ Можно отдельно включить напоминание за сутки.
→ Настроить уведомления.
```

Пример после события клуба:

```text
Это встреча клуба.
→ У клуба уже объявлена следующая дата.
→ Открыть клуб.
```

Page-end не должен дублировать `Похожие события`, `Смотрите дальше` или ещё один
card feed.

### 7.3. Цепочка между визитами

Hero-talk может продолжать нить прошлого и даже позапрошлого визита, но только
через bounded доказуемое состояние.

Допустимые cross-session anchors:

```text
последний квалифицированный Hero-talk node;
предпоследний node;
незавершённый open loop;
явное действие и его результат;
последний meaningful visit watermark;
активная фестивальная/editorial arc;
освоение capability;
```

Нельзя хранить или передавать полный свободный текст «разговора». Достаточен
compact thread state:

```json
{
  "thread_id": "ht_...",
  "last_node_ids": ["n3", "n2"],
  "open_loop_id": "festival-kantata-education",
  "last_action": "opened_event",
  "expires_at": "..."
}
```

Пример продолжения:

```text
В прошлый раз вы открыли программу фестиваля.
→ С тех пор добавилась ещё одна лекция.
→ Посмотреть новое.
```

Фраза «в прошлый раз» разрешена только при валидном cross-device/session
watermark и совпадающем объекте/arc.

### 7.4. Campaign arc

Редакционная промо-кампания может строить историю на нескольких визитах:

```text
первое знакомство с фестивалем
→ ключевой раздел программы
→ новое событие программы
→ скоро начало
→ что ещё можно успеть
```

Arc не обязан повторять название кампании в каждой сцене. Resolver должен
помнить, что уже было раскрыто, и не начинать рассказ заново без причины.

### 7.5. Onboarding arc

```text
eligible
→ contextual hint
→ attempted
→ result echo
→ where to find result
→ mastered/suppressed
```

Это та же Hero-talk chain model, но competency truth принадлежит onboarding
strategy.

## 8. Narrative graph contract

Предлагаемая node-модель:

```text
program_id
thread_id?
chain_id
node_id
placement_allowlist[]
intent
origin
context_entity_ids[]
topic_anchor
required_fact_ids[]
link_tokens[]
bridge_from[]
bridge_kind
open_loop_id?
resolves_loop_id?
persona_pack_allowlist[]
priority_band
frequency_cap
cooldown
safe_until
fragments_by_viewport
media_plan?
next_edges[]
```

`bridge_kind` может быть:

```text
continuation
answer
contrast
consequence
specific_example
second_option
result_echo
return_update
resolution
```

Недопустимая последовательность:

```text
погода → случайный рассказ о лайке → другой фестиваль
```

Допустимая:

```text
по прогнозу ветер
→ indoor option
→ второй indoor option
→ открыть выходные
```

## 9. «Пока вас не было»

Целевая сцена:

```text
Пока вас не было — 12 новых событий.
Посмотреть новое.
```

CTA ведёт в специальное состояние `Для меня`:

```text
/dlya-menya/?mode=new-since-visit
```

Точное URL может измениться, но контракт неизменяем:

- Hero-talk count и destination list строятся из одного `served_delta_id`;
- применяются current lifecycle, exact hide и profile projection;
- meaningful visit watermark не обновляется от backgrounding вкладки;
- cross-device watermark доступен для связанной identity;
- аноним без связанной identity остаётся device-local;
- при изменении каталога destination честно объясняет расхождение.

## 10. Персонализация и Golden personas

Hero-talk знает персональный контекст, но не генерирует уникальную реплику для
каждого человека.

Phrase packs строятся по ограниченной размерности:

```text
scenario family
× placement
× golden-persona pack
× explicit-interest overlay
× tone
× viewport
× program/campaign
```

Golden persona является мягкой смесью и внутренним инструментом выбора, а не
публичным ярлыком.

Нельзя:

```text
Вы — семейный планировщик.
Мы знаем, что вы любите лекции.
```

Допустимо:

```text
В выбранной теме «лекции» появилось новое.
Похоже на события, которые вы сохраняли.
```

До осмысленного activation используются только:

- общий каталог;
- page/session context;
- дата и город;
- редакционные программы;
- lifecycle facts.

После activation доступны:

- explicit facets;
- compatible persona pack;
- return delta;
- cross-device thread state;
- mastered-capability state;
- bounded campaign exposure.

Campaign/artifact exposure не обучает organic profile. Явный like/save остаётся
валидным сильным сигналом.

## 11. Связь с промо-кампаниями

Промо-кампания не создаёт отдельный Campaign Talk. Она добавляет Hero-talk
activity и поставляет кандидатов в общий compiler.

Пример:

```json
{
  "surface": "hero_talk",
  "config_json": {
    "placements": ["home_hero", "event_page_end"],
    "message_intents": ["festival_program", "event_spotlight"],
    "selection_policy": "diverse_program_rotation",
    "max_scenes_per_chain": 1,
    "daily_cap": 1
  }
}
```

Фестивальная кампания привязывается к живой festival/program identity, а не к
единственному замороженному event ID. Новые подходящие события могут войти в
следующий compile после обычных gates.

Первый release поддерживает только:

```text
system communication
own editorial campaigns
```

Partner/paid promotion и обязательная рекламная маркировка — отдельный legal и
product track.

Age rating конкретного события показывается небольшой круглой меткой `6+`,
`12+`, `16+` или `18+`, только из canonical facts и рядом с соответствующим
названием/CTA.

## 12. Generation-time LLM pipeline

Никакого LLM-вызова при открытии страницы нет.

```text
canonical facts and program intent
→ deterministic eligibility
→ generation brief
→ LLM Writer
→ semantic verifier
→ global editorial-style critic
→ narrative-chain critic
→ pack diversity/dedupe
→ viewport compiler
→ immutable phrase pack
→ static manifest
```

### 12.1. Deterministic owner

Код владеет:

- eligibility;
- выбором событий/фестивалей/ссылок;
- числами, датами, местами и age rating;
- lifecycle;
- persona evidence type;
- chain constraints и open-loop truth;
- caps/cooldowns;
- placement policy;
- viewport limits.

### 12.2. LLM Writer

LLM владеет:

- литературной русской формулировкой;
- глобальным friendly, добродушно-ироничным voice contract;
- смысловыми bridges;
- short/normal variants;
- единичным curiosity hook внутри разрешённого бюджета;
- связанностью цепочки без semantic expansion.

Writer не может:

- выбирать другой объект;
- менять facts;
- придумывать популярность, срочность, редкость или личное знание;
- менять CTA target;
- скрывать age/disclosure;
- продолжать cross-session thread без переданного thread evidence.

### 12.3. Стандартные fallback-фразы

Часть фраз остаётся детерминированной:

```text
Сегодня в афише N событий.
На выходные собрано N событий.
Новых событий пока нет.
Открыть «Для меня».
Показываем сохранённую афишу.
```

LLM-generated packs используются для приветствий, bridges, editorial hooks,
festival arcs, персонализированной подачи, weather→plan, feature discovery,
artifact hints и page-end continuation.

### 12.4. Воспроизводимость

- exact generation-input fingerprint;
- exact model ID;
- prompt/schema/style versions;
- source-fact hashes;
- immutable output hash;
- Writer/Verifier/Critic receipts;
- identical warm compile: `0` provider calls и `0` writes;
- forced regeneration создаёт новую version, не переписывает принятую;
- last-good pack сохраняется при отказе provider/validator.

## 13. Допустимый кликбейт

Единичный curiosity hook разрешён, если он не переступает границу.

- максимум один hook на chain;
- максимум один qualified hook за session;
- обещание раскрывается следующей сценой или CTA;
- основание доказано;
- нет ложного дефицита, срочности или угрозы потери;
- не используется для lifecycle/safety/reminder;
- дата, возраст и критические условия не скрываются;
- cooldown длиннее обычной сцены.

## 14. Page-end policy

Рекомендуемый порядок:

```text
main page content
→ canonical continuation/recommendations
→ page-end Hero-talk
→ focus-group NPS when enabled
→ footer
```

### Event page

- знает `context_event_id` и связанные festival/club/occurrence facts;
- предлагает одну следующую задачу;
- может продолжить программу события;
- может объяснить share/save/reminder;
- не дублирует recommendation cards.

### Collection/date listing

- не вставляется внутрь хронологии;
- после полного списка предлагает соседний маршрут, `Для меня`, функцию или
  одну связанную editorial continuation;
- не превращается в ещё одну подборку.

### Search

- помогает уточнить query или сохранить interest;
- empty-state может получить более заметную Hero-talk композицию;
- не утверждает, что запрос понят, если search завершился ошибкой.

### Personal feed

- объясняет `Почему это` и управление interests;
- может открыть серендипность;
- не ведёт `новое с прошлого визита` обратно в тот же уже открытый state.

## 15. Reminder/Push boundary

Hero-talk может показать save state, объяснить пользу уведомлений и открыть
preferences, но не владеет delivery.

```text
save
→ Hero-talk result echo
→ explicit enable-notifications action
→ browser permission
→ preferences
→ utility reminder transport
```

Save не является Push consent. Utility reminder и future promo Push имеют
разные purposes, caps и consent.

## 16. Метрики

Основная продуктовая метрика не равна Hero-talk CTR.

Предлагаемый primary outcome:

```text
доля eligible sessions, дошедших до конкретного event detail
через любой путь после Hero-talk exposure
```

Дополнительно:

- time to first meaningful action;
- event details/session;
- downstream save/calendar/share;
- return-delta destination completion;
- feature hint → successful operation;
- page-end continuation;
- campaign qualified exposure;
- hide/dismiss rate;
- category/feed displacement;
- Day 7/14 novelty decay;
- a11y/performance errors.

Exposure считается только после реальной квалифицированной видимости, а не при
создании скрытого node.

## 17. Open research questions

Главный открытый вопрос — как проектировать хорошие narrative chains, а не
только хорошие отдельные реплики. Он вынесен в
[deep-research-prompt.md](deep-research-prompt.md).

Исследование должно определить:

- типологию внутри- и межсессионных цепочек;
- правила semantic bridges;
- допустимую длину, ритм и memory horizon;
- open-loop/resolution contract;
- page-context continuation;
- campaign arcs;
- onboarding arcs;
- оценку литературной связности;
- автоматический chain critic;
- пользовательские и продуктовые метрики;
- границу между coherent story и навязчивой сериализацией.

## 18. Принятые решения владельца

- Hero-talk — единый продукт и общее название;
- placements: `home_hero` и `page_end`;
- typed briefing, курсор и optional tile media — определяющие признаки;
- приветствия `Добрый день` и локальный сценарий
  `Мы говорим по-калининградски / «кеска»` сохраняются;
- onboarding — message intent, связанный с отдельной onboarding strategy;
- page-end использует точный контекст страницы и объекта;
- предпочтительны смысловые цепочки, включая связь с прошлым и позапрошлым;
- текст генерируется LLM строго заранее;
- runtime выбирает готовый статический plan;
- packs относятся к Golden personas/смеси, не к уникальному человеку;
- Hero-talk знает персонализацию;
- `Пока вас не было` ведёт в `Для меня / Новое с прошлого визита`;
- один bounded clickbait допустим;
- клубы могут быть темой Hero-talk без отдельного Club Talk;
- первый promo scope — собственные редакционные кампании;
- age rating показывается компактно;
- video mosaic требует отдельного social-source lab;
- Hero-talk получает отдельные GitHub Actions gates.

### Canonical event resolver (read-only)

`hero_talk.resolver.resolve_event_packet(database_path, event_id, now=..., route_evidence=...)` resolves one raw SQLite Event snapshot, including lifecycle/silent and canonical identity/merge columns. It reuses `event_public_revision` and canonical publication span semantics with explicit Europe/Kaliningrad deadlines: timed one-day start, date-only day-end, trusted multiday end-of-day; inferred end dates do not extend a timed start. Missing identity, invalid dates and expired events fail closed. It does not establish freshness of organizer confirmation, perform semantic review, or grant campaign rights.

Links remain unresolved unless an internal `RouteReadinessEvidence` binds the exact event ID/current revision, exported slug/href and a current verification interval. This evidence must originate in a public route/inclusion verifier, not model/request JSON, outbox success or a secret-candidate build receipt. Its expiry also bounds the packet. Re-resolve at activation/use; the packet is not a durable permit. Media is intentionally unresolved: gallery approval/pixel geometry alone does not prove public encoded-object availability or usage rights. No provider/model calls, DB writes, publishing or fallback media inference occur.
