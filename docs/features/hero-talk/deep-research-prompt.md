# Deep research prompt: смысловые цепочки Hero-talk

Скопируйте этот prompt целиком в режим глубокого исследования. Исследователь
должен вернуть самостоятельный, доказательный и пригодный для переноса в
продуктовую документацию отчёт на русском языке.

---

Ты — внешний senior product researcher, conversation/narrative designer,
editorial UX architect, computational linguist, recommender-systems specialist,
motion/accessibility designer и experimentation lead для KenigEvents / «Полюбить
Калининград · Анонсы».

Проведи **глубокое критическое исследование смысловых цепочек Hero-talk**.
Не ограничивайся рекомендациями «делайте сообщения связанными». Нужна строгая
модель, по которой продукт, редактор, LLM pipeline, frontend и автотесты смогут:

1. заранее строить связные короткие повествования;
2. продолжать тему между сценами, страницами и визитами;
3. учитывать точный контекст страницы, события, кампании и пользователя;
4. не превращать афишу в чат-бот, рекламную карусель или навязчивый сериал;
5. автоматически проверять факты, литературную связность, ритм и полезность;
6. безопасно использовать LLM только на generation time;
7. публиковать конечные статические phrase packs и narrative plans.

## 1. Обязательный контекст продукта

Прежде чем отвечать, изучи переданные материалы репозитория, прежде всего:

```text
docs/features/hero-talk/README.md
docs/features/hero-talk/release-plan.md
docs/features/hero-talk/testing.md
docs/features/static-site-onboarding/README.md
docs/features/static-site-onboarding/deep-research-prompt.md
docs/features/static-site-pages/personalizaion/personalization-to-be.md
docs/features/promo-campaigns/README.md
docs/features/static-site-pages/release-plan.md
docs/features/static-site-easter-eggs/README.md
docs/features/linked-events/README.md
site/src/components/HomeHeroTalk.astro
```

Историческую typed-briefing ветку используй как research evidence, а не как
production truth. Отдельно учитывай её сильные решения:

- semantic-fragment reveal вместо медленной посимвольной печати;
- cursor semantics;
- конечные цепочки максимум из нескольких сцен;
- optional square-tile media;
- static/no-JS/reduced-motion fallback;
- deterministic facts и ссылки;
- LLM Writer только до публикации;
- cooldown, caps, action-success suppression;
- `Пока вас не было`;
- greetings `Добрый день` и
  `Мы говорим по-калининградски. И даже можем сказать «кеска»`.

Но не считай старые demo counts, event IDs, dates, screenshots и visual approvals
доказательством production-полезности.

## 2. Что такое Hero-talk

Hero-talk имеет определяющие признаки:

1. typed briefing — текст быстро появляется смысловыми фрагментами;
2. курсор отражает реальное состояние продолжения;
3. ссылки стабильны и работают с первого клика;
4. иногда вместе с текстом квадратами появляется exact source image;
5. в отдельном будущем эксперименте квадратная мозаика может показывать muted
   horizontal video;
6. первый текст полезен в static HTML;
7. runtime не вызывает LLM;
8. home hero и page-end используют один механизм;
9. page-end знает точный page/entity/action context;
10. предпочтительная единица — narrative chain, а не случайная фраза.

Не предлагай заменить Hero-talk обычным toast, tooltip, чат-ботом, видеобаннером
или статическим маркетинговым hero. Эти альтернативы можно использовать только
как controls в исследовании.

## 3. Два placement

Исследуй общую модель для:

```text
home_hero
page_end
```

Page-end включает контекстные варианты:

```text
event_page_end
collection_page_end
date_listing_page_end
search_page_end
personal_feed_page_end
club_page_end
```

Не создавай отдельные продукты `Onboarding Talk`, `Campaign Talk`, `Return
Briefing` и `Club Talk`. Это intents/programs внутри Hero-talk.

## 4. Главный предмет исследования: narrative chains

Нужно спроектировать минимум пять уровней связанности.

### A. Внутри одной сцены

Как 1–3 строки должны образовывать одну мысль:

```text
контекст
→ полезный факт
→ действие
```

### B. Между сценами одной сессии

Как строится chain длины 1–3, где следующая сцена логически вытекает из
предыдущей.

### C. Page-context chain

Как page-end учитывает точный объект страницы и уже совершённые действия.

Примеры контекста:

```text
пользователь дочитал событие фестиваля;
сохранил его;
открыл событие клуба;
посмотрел тематическую подборку;
поиск дал ноль результатов;
на странице «Для меня» закончились новые события.
```

### D. Межвизитная цепочка

Как безопасно продолжить разговор с прошлого или позапрошлого meaningful visit:

```text
последний node;
предпоследний node;
open loop;
явное действие;
изменившийся catalog/program;
```

### E. Долгая редакционная/campaign arc

Как фестиваль, сезонная тема, onboarding capability или региональная история
раскрывается несколькими короткими эпизодами на протяжении дней, не повторяя
вводную и не вызывая fatigue.

## 5. Обязательные исследовательские вопросы

### 5.1. Типология цепочек

Предложи исчерпывающую, но компактную типологию, например:

```text
orientation chain
return/update chain
recommendation chain
weather→plan chain
festival/program chain
feature-learning chain
result-echo chain
recovery chain
local-identity/greeting chain
artifact-story chain
serendipity chain
```

Для каждого типа укажи:

- user job;
- allowed opener;
- topic anchor;
- допустимые bridge relations;
- max nodes;
- memory horizon;
- terminal state;
- CTA model;
- failure/fallback;
- где допустим clickbait;
- где он запрещён.

### 5.2. Semantic bridges

Разработай таксономию связей между узлами:

```text
continuation
answer
specific example
consequence
contrast
second option
result echo
return update
resolution
```

Проверь, достаточно ли её. Для каждой связи дай хорошие и плохие русские
примеры.

Ответь:

- нужен ли явный связующий оборот в каждой сцене;
- когда continuity может быть имплицитной;
- как не превратить короткие фразы в канцелярский пересказ;
- как избежать резкой смены темы после приветствия;
- как соединять greeting с полезным контекстом;
- как использовать локальный юмор без разрушения основной задачи.

### 5.3. Open loops

Исследуй допустимые незакрытые обещания:

```text
«Угадаете, кто?»
«Есть ещё одна причина заглянуть…»
«В прошлый раз мы остановились на программе фестиваля…»
```

Определи:

- когда open loop полезен;
- максимальный срок разрешения;
- можно ли переносить его на другой визит;
- что происходит при expiry/cancellation;
- как не потерять payoff;
- как предотвращать clickbait debt;
- какие события требуют немедленного закрытия.

### 5.4. Memory model

Спроектируй минимальное bounded состояние, достаточное для связности:

```text
thread_id
last_node_ids
open_loop_id
last_action/outcome
context entity ids
catalog/profile/program revisions
expires_at
```

Критически оцени:

- нужно ли хранить последний один, два или больше nodes;
- нужен ли отдельный summary;
- допустим ли LLM-generated summary;
- что хранить local, что cross-device;
- как работать до входа;
- как мигрировать состояние после linking identity;
- как избежать хранения свободного персонального текста;
- как обрабатывать несколько вкладок и BFCache;
- когда начинать новую thread вместо продолжения старой.

### 5.5. Greeting и local identity

Отдельно исследуй families:

```text
Добрый день! Что сегодня удивит?
Мы говорим по-калининградски.
И даже можем сказать «кеска».
```

Нужно понять:

- являются ли они opener, bridge или самостоятельным editorial interlude;
- как часто их показывать;
- с чем логично продолжать;
- когда greeting уже лишний;
- как менять daypart;
- как не повторять «Добрый день» при каждом reload;
- как не делать локальную речь искусственной;
- как Golden personas могут менять стиль, не меняя смысл.

### 5.6. Onboarding chains

Не разрабатывай новую стратегию онбоардинга вместо параллельного документа.
Используй его state machine:

```text
unknown
→ eligible
→ exposed
→ attempted
→ succeeded
→ repeated
→ mastered
```

Исследуй, как Hero-talk превращает это в связный рассказ:

```text
контекстная подсказка
→ попытка
→ подтверждение результата
→ где найти результат
→ suppression/mastery
```

Обязательные capabilities:

```text
date navigation
smart search
share
like
not interested
calendar/save
saved events
For Me
Why this
identity sync
PWA
utility reminders
artifact collection
```

Отдельно проработай onboarding первого артефакта с одной точной подсказкой.

### 5.7. Page-end context

Для каждой page family предложи contextual inputs, allowed chain types и
запрещённые дубли:

```text
event detail
festival event detail
club event detail
collection
Today/Tomorrow/Weekend/date
search results
search empty
For Me
clubs
```

На event page обязательно учти:

- event ID;
- date/time/location/status;
- festival/program relation;
- club relation;
- linked occurrences;
- saved/liked/hidden state;
- ticket/registration CTA;
- what the user just did;
- existing Similar/More events blocks.

Hero-talk не должен становиться вторым recommendation feed.

### 5.8. Return delta

Проработай цепочку:

```text
Пока вас не было — N новых событий.
→ где именно появилось новое;
→ открыть «Для меня / Новое с прошлого визита».
```

Нужны единые:

```text
served_delta_id
catalog_revision
profile_revision
previous_visit watermark
destination list
```

Исследуй:

- meaningful visit definition;
- cross-device semantics;
- анонимный fallback;
- count/list drift;
- zero-delta copy;
- many-new aggregation;
- continuation из прошлого visit thread.

### 5.9. Festival/editorial campaign arcs

Проработай кампанию, которая может:

- представить фестиваль;
- затем раскрыть конкретную часть программы;
- сообщить о вновь добавленном событии;
- напомнить, что скоро начало;
- после открытия одного события продолжить в page-end;
- учитывать Golden persona mix;
- не повторять одно и то же событие;
- не занимать все Hero-talk scenes.

Рассмотри activity:

```text
promo_activity.surface = hero_talk
```

и dynamic target на festival/program identity, а не frozen event IDs.

Первый scope — собственные редакционные кампании. Paid/partner исследуй только
как будущую boundary с явной рекламной маркировкой.

### 5.10. Golden personas и phrase packs

Предложи разумную конечную матрицу phrase packs.

Исследуй:

- как квантизовать persona mixtures;
- сколько packs нужно для MVP;
- какие distinctions действительно меняют язык;
- как не создавать combinatorial explosion;
- как explicit interests перекрывают persona prior;
- как unknown mass влияет на tone;
- как один смысл сохраняется между variants;
- как персонализация объясняется человеку;
- какие claims запрещены.

Не предлагай генерацию уникального текста на пользователя.

### 5.11. LLM pipeline

Спроектируй generation-time pipeline:

```text
deterministic planner
→ Writer
→ fact verifier
→ chain-coherence critic
→ global editorial-style critic
→ diversity/dedupe
→ viewport compiler
→ immutable pack
```

Для каждого этапа укажи:

- input schema;
- output schema;
- model class;
- prompt boundaries;
- hard validators;
- retry policy;
- abstention/fallback;
- cache/fingerprint;
- evidence receipt;
- cost/rate-limit controls.

Особенно глубоко проработай **chain-coherence critic**.

Он должен оценивать как минимум:

- topic continuity;
- referent continuity;
- temporal consistency;
- bridge validity;
- open-loop resolution;
- CTA consistency;
- repetition;
- literary naturalness;
- information gain;
- persona/style compatibility;
- abrupt topic shift;
- dependency on missing previous context.

Определи, что проверяется детерминистически, что отдельной LLM-критикой, а что
только человеком.

### 5.12. Русский язык и литературное качество

Исследуй конструкции короткого связного повествования на русском:

- эллипсис;
- анафора;
- повтор anchor noun;
- местоименные ссылки;
- риторический вопрос;
- короткий контраст;
- «а ещё»;
- временные мосты;
- добродушная ирония;
- локальный колорит.

Нужны правила против:

- канцелярита;
- телеграфного рубленого текста без связи;
- одинаковых `Пойдём?`, `Заглянем?`, `Посмотрим?`;
- fake intimacy;
- рекламной истерики;
- лишних англицизмов;
- инфантильности;
- чрезмерной загадочности;
- повторения одного синтаксического шаблона.

Глобальный editorial voice разрабатывается отдельно. Исследование должно
сформулировать interface между voice contract и Hero-talk chain contract, но не
подменять будущий voice document.

### 5.13. Motion and reading

Исследуй влияние на связность:

- semantic-fragment reveal;
- cursor;
- readable hold;
- pause/interruption;
- terminal state;
- square-tile image entry/exit;
- possible video mosaic;
- mobile/reduced-motion/static mode.

Ответь:

- должен ли bridge-фрагмент появляться первым;
- можно ли скрывать часть контекста до следующей сцены;
- как motion помогает, а не разрушает referent continuity;
- как пользователь возвращается к предыдущей сцене;
- нужна ли видимая история 1–2 прошлых реплик;
- следует ли terminal Next показывать текстом;
- как не создать screen-reader spam.

### 5.14. Age rating и critical facts

Event-specific scene может иметь небольшую круглую метку `6+`, `12+`, `16+`,
`18+`.

Исследуй:

- где она находится в typed/mosaic composition;
- как привязать rating к нужному событию в multi-event chain;
- какие critical facts должны оставаться одновременно видимы;
- когда scene обязана показать `что/где/когда`;
- когда допустима teaser scene без полной логистики;
- как cancellation/postponement ломает текущую chain.

### 5.15. Допустимый clickbait

Один curiosity hook допустим при строгом бюджете.

Определи:

- taxonomy допустимых hooks;
- proof requirements;
- max frequency;
- required payoff;
- cooldown;
- forbidden domains;
- автоматическую проверку clickbait debt;
- отличие curiosity gap от misleading omission.

## 6. Внешнее исследование

Найди и критически сравни актуальные практики и исследования из следующих
областей:

- narrative UX и microcopy sequences;
- conversational design без чат-интерфейса;
- progressive disclosure;
- contextual onboarding;
- recommender explanations;
- serialized editorial storytelling;
- notification/in-app message orchestration;
- campaign frequency capping;
- computational narrative planning;
- discourse coherence и RST/SDRT-подобные модели;
- automatic dialogue/story coherence evaluation;
- LLM-as-judge reliability и multi-critic pipelines;
- accessibility moving/updating content;
- motion and reading comprehension;
- cross-session memory/privacy;
- Russian short-form editorial writing.

Приоритет источников:

1. peer-reviewed papers и proceedings;
2. официальные platform/design guidelines;
3. книги и материалы признанных practitioners;
4. продуктовые case studies с измерениями;
5. только затем качественные блоги.

Указывай точные ссылки, даты публикации и границы применимости. Не выдавай
обычный carousel или chatbot onboarding за прямой аналог Hero-talk.

## 7. Обязательные сравнения

Сравни минимум:

```text
A. случайная ротация независимых фраз
B. rule-based finite chain
C. LLM-generated chain + deterministic facts
D. graph-planned chain + LLM realization
E. editor-authored arc + LLM variants
F. полностью personalized runtime conversation — как отрицательный/дорогой control
```

Оцени:

- качество;
- воспроизводимость;
- стоимость;
- редакционный контроль;
- разнообразие;
- privacy;
- testability;
- static-site compatibility;
- campaign/onboarding integration;
- failure behavior.

Выбери target architecture и минимум один более простой baseline.

## 8. Нужные артефакты результата

Верни отчёт со следующей структурой.

### 8.1. Executive verdict

- что является сильнейшей моделью цепочек;
- что вырезать;
- что доказано, предположено и неизвестно;
- Go / Conditional Go / No-Go для chain-first architecture.

### 8.2. Canonical terminology

Дай финальный словарь:

```text
program
thread
arc
chain
node/scene
bridge
open loop
resolution
qualified exposure
thread memory
phrase pack
served plan
```

### 8.3. Chain taxonomy

Таблица типов с user job, eligibility, structure, limits и примерами.

### 8.4. Narrative graph schema

JSON Schema-like model с примерами:

- home greeting→context;
- return delta;
- event page-end;
- festival campaign arc;
- onboarding capability;
- artifact hint.

### 8.5. Memory and cross-device state

Точная state machine, payload, TTL, merge/linking, multi-tab и failure policy.

### 8.6. LLM pipeline

Полный Writer/Verifier/Chain Critic/Style Critic design с prompts/pseudocode,
versioning, caching, fallback и cost controls.

### 8.7. Quality rubric

Оценка 0–4 минимум по:

```text
factual grounding
local coherence
global coherence
bridge quality
referent clarity
temporal consistency
information gain
CTA truth
literary Russian
voice fit
non-repetition
persona appropriateness
accessibility fit
viewport fit
```

Укажи release thresholds и hard-fail dimensions.

### 8.8. Large Russian example library

Подготовь не меньше:

- 12 приветственных/local chains;
- 12 current-context chains;
- 12 return chains;
- 12 page-end event chains;
- 8 festival/campaign arcs;
- 12 onboarding chains;
- 6 artifact chains;
- 8 weather→plan chains;
- 8 club-discovery chains;
- 12 failures/fallbacks.

Для каждого примера укажи:

- context;
- facts required;
- placement;
- persona pack;
- nodes;
- bridge types;
- CTA;
- expiry/cooldown;
- почему цепочка связна;
- возможный дефект.

Не используй вымышленные текущие counts, даты, участников или погоду без явной
метки fixture/demo.

### 8.9. Experiment design

Раздели факторы:

```text
no Hero-talk
single useful static scene
independent rotating scenes
coherent chain
coherent chain + personalization
coherent chain + image mosaic
coherent chain + video mosaic
```

Primary outcome не должен быть только Hero-talk CTR. Предложи:

- downstream event discovery;
- time to useful decision;
- save/share/calendar;
- return completion;
- comprehension/recall;
- perceived coherence;
- fatigue and novelty decay;
- page/feed displacement;
- performance/accessibility guardrails.

Не назначай произвольный sample size без baseline traffic.

### 8.10. GitHub Actions test plan

Сформируй machine-testable gates:

- schema/fact/link validation;
- chain graph validity;
- unreachable/dangling/open-loop checks;
- deterministic cold/warm compile;
- LLM receipts;
- chain critic thresholds;
- lexical/syntactic diversity;
- Golden persona fixtures;
- page-context fixtures;
- cross-device state;
- browser motion/pause/first-click;
- no-JS/reduced-motion;
- image/video mosaic;
- current campaign/festival refresh;
- last-good/rollback.

Для каждого gate укажи:

```text
scenario id
inputs
assertions
evidence artifact
FAIL/WATCH policy
cost-bearing or provider-free
PR/scheduled/manual cadence
```

### 8.11. Implementation roadmap

Разбей на независимые этапы:

```text
M0 terminology/research/schema
M1 static single-scene baseline
M2 chain compiler with handwritten packs
M3 generation-time LLM pipeline
M4 page-end context
M5 onboarding integration
M6 cross-device thread memory
M7 editorial campaign arcs
M8 image/video mosaic experiment
M9 controlled production experiment
```

Для каждого этапа дай explicit cuts, release gates и rollback.

## 9. Red-team требования

Перечисли минимум 30 failure modes, включая:

- хорошая отдельная фраза, но плохая цепочка;
- потерянный референт;
- неправильное продолжение после другого устройства;
- stale open loop;
- повторный greeting;
- topic whiplash;
- campaign takeover;
- onboarding fatigue;
- false personalization;
- persona stereotype;
- count/list mismatch;
- cancellation after compile;
- clickbait without payoff;
- chain depends on hidden previous node;
- page-end repeats existing recommendations;
- motion hides bridge;
- cursor promises nonexistent continuation;
- media no longer matches copy;
- video decode stalls text;
- LLM critic accepts fluent hallucination;
- two critics share the same blind spot;
- generation quota causes silent low-quality fallback;
- warm build regenerates unnecessarily;
- thread memory leaks between accounts;
- artifact/campaign click pollutes taste profile;
- screen reader receives repeated updates;
- mobile feed is pushed below fold;
- literary voice fragments across persona packs;
- local humour becomes forced;
- page context is too specific and creepy;
- cross-session story becomes manipulative retention.

Для каждого укажи severity, detectability, mitigation и gate.

## 10. Финальный decision package

Заверши отчёт:

1. одной рекомендуемой архитектурой;
2. одной упрощённой baseline-архитектурой;
3. 15 неизменяемыми правилами хорошей цепочки;
4. 10 правил cross-session continuation;
5. target JSON model;
6. target LLM pipeline;
7. target test matrix;
8. списком решений, которые должен принять владелец продукта;
9. коротким implementation handoff для следующего кодового агента.

Отчёт должен быть критическим. Если цепочки полезны только для части intents,
прямо раздели chain-first и single-scene cases. Не расширяй систему ради
архитектурной красоты и не объявляй conversational continuity доказанной без
эксперимента.
