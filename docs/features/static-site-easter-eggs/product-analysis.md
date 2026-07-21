# Критическая продуктовая аналитика механизма пасхалок

> Дата: 2026-07-21.
> Статус: продуктовая рекомендация, не implementation approval.
> База: требования Telegram `485–519`, repo contracts, independent best-practice
> research и критический Gemini Pro consultation через `agy`; disposition
> спорных рекомендаций: [gemini-consultation-2026-07-21.md](gemini-consultation-2026-07-21.md).

## Answer-first verdict

**NARROW / продолжать в узком пилоте.** Механика соответствует региональной и
campaign-миссии KenigEvents, но только как вспомогательный способ узнать город и
найти связанные события. В сыром виде идея несёт риск оттянуть внимание от
основной задачи, превратить promo в скрытую рекламу, оптимизировать vanity
engagement и создать юридически/этически опасную prize loop.

Пилот имеет смысл, если он:

- конечный и сюжетный, а не бесконечный;
- без материального приза и share multiplier;
- прозрачно связан с кампанией;
- не ухудшает поиск/выбор события;
- даёт равноправный доступный путь;
- измеряется через incremental downstream value против holdout;
- выключается по автоматическим guardrails.

## Соотношение с общими целями

| Цель KenigEvents | Вклад пасхалок | Критическое ограничение |
|---|---|---|
| Быстро привести к жизнеспособному событию | тематическая история может открыть связанное событие/подборку | hunt не должен ухудшить time/cards-to-first-value или скрыть CTA |
| Региональная культурная ценность | короткие проверяемые истории создают дифференциацию | visual without provenance превращается в декор/ошибку |
| Непрерывный путь по поверхностям | режиссируемая глава может познакомить с поиском, календарём, «Моё» | интерфейс нельзя превращать в искусственную полосу препятствий |
| Возврат по новой полезной ценности | scheduled chapters дают понятный reason-to-return | не использовать streak, fake scarcity, punishment или daily spam |
| Память интересов и персонализация | collection state можно сохранить между устройствами | promo/egg exposure не является organic preference signal |
| Партнёрская помощь промо-кампаниям | partner co-creation и campaign narrative расширяют формат | sponsored egg требует disclosure и редакционной независимости |

### Что нельзя считать успехом

`egg_clicks`, `scroll_depth`, `time_on_site`, `collection_completion`, число
подсказок и share-intent сами по себе не доказывают продуктовую ценность. Они
могут вырасти одновременно с падением ticket/save/calendar actions.

## Продуктовые риски и решения

| Риск | Почему реален | Решение/gate |
|---|---|---|
| Cannibalization основного discovery | объект между карточками занимает viewport и внимание | holdout; non-inferiority для qualified event actions и time-to-value; не считать egg event impression |
| Скрытая реклама | партнёрский объект выглядит как редакционная находка | явное sponsorship/editorial disclosure; proposal ≠ acceptance ≠ campaign |
| Novelty-only lift | первый цикл может дать всплеск без устойчивой пользы | полный campaign cycle + минимум 7 дней; repeat campaign evidence |
| Gambling/dark pattern | variable reward, near-miss, share-to-win и FOMO меняют смысл продукта | fixed cultural unlock; известные сроки/условия; без loot boxes/streaks/fake scarcity |
| Потеря доверия | факт, IP или локация ошибочны/опасны | provenance/version; fact/IP/safety review; report + immediate pause path |
| Исключение пользователей | hover, точный клик, motion, GPS/QR/keyboard-only | WCAG AA и equivalent path; mandatory completion device-neutral |
| Семантическое загрязнение `Моё` | eggs смешиваются с календарём/избранным и badge | отдельный collection section; event badge unchanged |
| Архитектурный fake | текущий `promo_exposure.event_id` используется для не-события | first-class egg subject и dedicated/polymorphic ledger |
| Privacy/abuse | profile-level progress, prize claims, partner email | RLS/server authority, idempotency, data minimization, bounded telemetry |
| Operational burden | outdated assets, proposals, hints, unsafe reports | admin inventory, owner/SLA, pause/kill switch, expiry/freshness jobs |

## Лучшие практики и перенос в продукт

### 1. Автономия и содержательная награда

Исследования геймификации показывают в среднем небольшой эффект и сильную
зависимость от контекста. Надёжнее поддерживать autonomy/competence/relatedness,
чем наращивать очки и внешние награды. Поэтому пользователь выбирает
`искать самому | включить подсказки | скрыть`, а наградой служит знание и полезный
маршрут. Источники: [meta-analysis 2024](https://link.springer.com/article/10.1007/s11423-023-10337-7),
[Deci–Koestner–Ryan meta-analysis](https://selfdeterminationtheory.org/wp-content/uploads/2014/04/1999_DeciKoestnerRyan_Meta.pdf).

### 2. Конечный прогресс без наказания

Видимый прогресс и близость цели способны поддерживать завершение, но нельзя
рисовать фиктивно «подаренный» элемент или отнимать уже найденное. Первая единица
должна быть реально и легко найдена. Источник:
[goal-gradient/endowed progress](https://journals.sagepub.com/doi/pdf/10.1509/jmkr.43.1.39?download=true).

### 3. Surprise in content, certainty in contract

Неизвестными могут быть сюжет и визуальное открытие. Известными остаются сроки,
число глав, способ сохранения, prize/eligibility и отсутствие потери прогресса.
Запрещены near-miss, скрытые меняющиеся шансы и ложная срочность. Ориентиры:
[FTC dark patterns](https://www.ftc.gov/news-events/news/press-releases/2022/09/ftc-report-shows-rise-sophisticated-dark-patterns-designed-trick-trap-consumers),
[OECD dark commercial patterns](https://www.oecd.org/en/publications/dark-commercial-patterns_44f5e846-en.html).

### 4. Возврат по расписанию, не streak

Публичное открытие глав, catch-up и opt-in notification создают ожидание без
потери серии. Daily streak, shame copy, leaderboard и endless progression не
входят в продукт. Novelty effect требует длиннее смотреть на эксперимент:
[Microsoft experimentation note](https://www.microsoft.com/en-us/research/articles/external-validity-of-online-experiments-can-we-predict-the-future/).

### 5. Accessibility by construction

Находка — доступная button/link semantics, focus, status message, 24×24 CSS px
minimum/spacing, no color/motion/audio/hover-only cue, reduced motion и alternative
path. Источники: [WCAG 2.2 Keyboard](https://www.w3.org/WAI/WCAG22/Understanding/keyboard.html),
[Target Size](https://www.w3.org/WAI/WCAG22/Understanding/target-size-minimum.html),
[Status Messages](https://www.w3.org/WAI/WCAG22/Understanding/status-messages.html),
[Animation from Interactions](https://www.w3.org/WAI/WCAG22/Understanding/animation-from-interactions.html).

### 6. Experiment, not launch theatre

HEART и trustworthy experimentation требуют сочетать engagement с satisfaction,
retention, task success, guardrails и data quality. Решение принимается по
incremental outcome, а не по treatment-only funnel. Источники:
[Google HEART](https://research.google.com/pubs/archive/36299.pdf),
[Microsoft trustworthy experiments](https://www.microsoft.com/en-us/research/articles/patterns-of-trustworthy-experimentation-during-experiment-stage/).

### 7. Partner co-creation through moderation

Короткий intake повышает шанс предложения, но права, источники, безопасность и
редакционная ценность проверяются отдельно. Пользователь видит статус и правила,
а предложение не создаёт автоматическую публикацию.

## Режиссура как конечный автомат

> **Числа ниже — только configurable experiment candidates.** `1–2` interactions,
> `2` sessions/`3` views, `24h`, `2 hints`, `20%`, `7d`, число объектов и любые
> cadence/cooldown/cap/window не являются принятыми SLO или production defaults.
> Перед canary их заменяют параметрами, обоснованными baseline, трафиком/MDE,
> accessibility test и owner decision; при недостатке evidence используется более
> редкий fail-safe режим.

```text
latent
  ├─ hide → hidden_for_campaign
  └─ first_find → discovered

discovered
  ├─ choose_self_guided → exploring
  ├─ enable_hints → exploring
  └─ hide → hidden_for_campaign

exploring
  ├─ remaining = 1 → near_complete
  ├─ campaign_end → archived_incomplete
  └─ hide → hidden_for_campaign

near_complete
  ├─ final_find → completed
  └─ campaign_end → archived_incomplete

completed → archived_complete
```

### Автоматические правила

| Phase | Rule | Safety reason |
|---|---|---|
| `latent` | не более одного proactive signal за сессию | не перехватывать основную задачу |
| `latent` | первая простая находка в 1–2 meaningful interactions | доказать механику без grind |
| `discovered` | показать реальный `1 из N`, правила и три выбора | autonomy и честный контракт |
| `discovered` | не просить login/email сразу | trust ask только после ценности |
| `exploring` | chapters release по опубликованному schedule; late joiner получает уже открытые | fairness/catch-up |
| `exploring` | proactive hint после 2 eligible sessions или 3 eligible views без find | не спамить, но снижать frustration |
| `exploring` | ≤1 proactive hint/24h и ≤2/egg; requested hints unlimited | пользователь управляет помощью |
| `exploring` | hint ladder: тема → page family → конкретный доступный маршрут | progressive assistance |
| `near_complete` | последние 20% окна дают усиленную подсказку всем | снизить advantage раннего старта |
| `completed` | idempotent completion; fixed unlock; optional feedback/share | нет двойной выдачи/принуждения |
| `archived` | сохранить найденное и открыть story archive | прогресс не исчезает |
| any | hide действует до следующей кампании | fatigue control |
| any | unsafe/fact-error spike pause placement | trust/safety kill switch |
| any | core CTA/performance/errors cross stop threshold → pause experiment | product non-inferiority |

### Decision engine sketch

```text
if campaign not active or activity paused: no_slot
if user hid campaign: no_slot
if page/slot/accessibility alternative not eligible: no_slot
if core journey is in critical CTA: no_slot
if session/day/activity cap reached: no_slot
if chapter not released or already collected: no_slot
if safety/fact/IP/version gate not green: no_slot
mode = campaign.discovery_mode
slot = stable_verified_slot(mode, campaign, chapter, anonymous_or_profile_bucket)
hint = next_hint_only_if_threshold_met(progress, eligible_history, user_choice)
return signed versioned placement + accessible alternative
```

Это deterministic rules engine над проверенными assets/slots. Runtime LLM не
выбирает скрытый элемент, факт, eligibility или шанс на приз.

## Promo-campaign integration

### Польза существующего control plane

`promo_campaign` уже задаёт status/window/goal/priority/caps/disclosure;
`promo_activity` — поверхность, policy, profile/config и enabled state. Это
правильное место для расписания и governance новой surface.

### Где reuse заканчивается

Текущий resolver и `promo_exposure` ориентированы на реальные события; у exposure
обязателен `event_id`. Пасхалку нельзя создавать как фиктивное событие. Требуются:

- `egg_definition` + versions/provenance/freshness/IP/safety;
- `egg_collection` + chapter order/rules version;
- campaign binding;
- verified placement registry;
- private idempotent progress/find ledger;
- aggregate event stream/rollup с отдельным `subject_type=egg` либо dedicated
  exposure model.

### Surface/outcome semantics

Новая surface: `site_easter_egg`; подсказка может быть outcome или отдельной
`site_easter_egg_hint`, если caps/reporting различаются.

```text
eligible ≠ inserted ≠ viewable ≠ opened ≠ collected ≠ completed
```

`promo_exposure` должен означать доказанный viewer-facing view, а не решение
scheduler. Collection analytics не смешивается с event promo counts. Sponsored
campaign маркируется и не обучает organic interests.

## Страница, feedback и партнёрский pipeline

### Информационная архитектура

```text
Название и правила/сроки
→ найдено N из M + next chapter state
→ коллекция найденных/закрытых slots
→ архив истории и связанные события
→ [Оценить / сообщить о проблеме]
→ [Предложить пасхалку] + info@kenigevents.ru
```

В `Моё` это отдельный section/filter. Public rules/archive может жить на
`/pashalki/`; private progress загружается после identity restore и не попадает в
CDN HTML.

### Feedback taxonomy

- interest: `interesting | neutral | not_interesting`;
- difficulty: `too_easy | right | too_hard | unclear`;
- issue: `broken | outdated_fact | unsafe | inaccessible | other`;
- optional comment/contact;
- immutable `egg_version`, `campaign_id`, placement and accessibility path.

### Partner submission

Email-MVP использует `info@kenigevents.ru` с prefilled subject/template. Целевая
форма короткая на первом шаге, затем запрашивает sources, rights, safety,
accessible alternative и campaign relationship. Состояния:

```text
received → triage → needs_details → fact_ip_safety_review
→ accepted | deferred | rejected → admin campaign binding
```

`received` не равно `qualified`; `accepted` не равно `scheduled`. Partner analytics
считает `qualified proposals / 100 starts`, activation rate, decision time и
moderation minutes, а не просто отправки.

## Аналитическая модель

Канонические grains, stable placement dimensions, compact rollups, TTL, KPI
difficulty bands и post-find/dislike/motion states вынесены в
[measurement-and-state-contract.md](measurement-and-state-contract.md). Ни один
raw event list ниже не означает per-event permanent storage.

### Primary outcome

**Incremental meaningful campaign action rate (7d):** разница treatment и holdout
по доле eligible visitors, совершивших заранее определённое действие со связанным
событием/кампанией: save/calendar, ticket/register/phone/map, meaningful detail,
share подборки. Exact action set фиксируется до эксперимента.

### Secondary outcomes

- 7-day qualified campaign return uplift;
- first-find and completion funnel;
- hint-assisted find rate;
- fact-card satisfaction/learning proxy;
- partner qualified contribution and time-to-decision.

### Guardrails

- core CTA non-inferiority;
- time/cards-to-first-relevant event;
- bounce/abandonment;
- page performance/CLS/JS errors;
- `непонятно`, inaccessible, unsafe/outdated reports;
- progress merge/drift loss;
- keyboard/equivalent-path completion parity;
- notification opt-out/complaints;
- moderation/spam workload;
- telemetry validity and bot/test contamination.

### Event taxonomy

```text
egg_eligible
egg_signal_viewable
egg_opened
egg_collected
egg_fact_expanded
egg_hint_offered
egg_hint_requested
egg_collection_viewed
egg_completed
egg_hidden
egg_feedback_submitted
egg_partner_proposal_started
egg_partner_proposal_received
```

Не заявлять `proposal_received` по одному mailto click и не заявлять `share_sent`
по открытию share sheet. High-volume raw telemetry не хранится бесконечно в
Supabase; data ownership следует project ADR и retention decision.

## Эксперименты и kill criteria

| Hypothesis | Test | Ship signal | Stop/kill |
|---|---|---|---|
| H1 collection улучшает campaign value | treatment vs no-egg holdout | primary uplift и core CTA non-inferior | CTA/time-to-value ухудшены за threshold |
| H2 hint ladder снижает frustration | requested-only vs deterministic offer | find↑ и unclear/too-hard↓ | hint fatigue/abandonment↑ |
| H3 schedule даёт этичный return | all-at-once vs scheduled chapters | qualified 7d return↑ после full cycle | только launch spike, complaints↑ |
| H4 short partner intake повышает качество | email/long vs two-step | qualified rate↑, moderation minutes stable | spam/workload↑ без accepted ideas |
| H5 accessible alternative даёт parity | visual path + equivalent path | completion parity в target | mandatory path недоступен |
| H6 cultural unlock лучше chance reward | fixed story vs decorative random | satisfaction/downstream value↑ | no value or gambling-like feedback |

До запуска: baseline и A/A, one primary outcome, MDE/traffic feasibility, sample
ratio mismatch checks, full-cycle duration, pre-registered thresholds. При малом
трафике не выдавать before/after correlation за causal uplift.

## Legal/privacy/anti-abuse boundary

Это не юридическое заключение. До призовой кампании legal owner утверждает exact
rules, organizer, eligibility, prize count, selection, claim, consent, taxes,
territory/age, audit и dispute path. Полезные точки проверки:
[38-ФЗ, статья 9](https://www.consultant.ru/document/cons_doc_LAW_58968/7b11ca039c27a1a3999a4f93bed2bcdd9149f906/),
[152-ФЗ, статья 5](https://www.consultant.ru/document/cons_doc_LAW_61801/96fbc469f91f57235cc842a85e0516a99f23dc85/).

Server — authority для completion/claim. Нужны signed single-use tokens,
idempotency, object-level authorization, rate limits, audit, anomaly review и
appeal; один IP/device не является достаточным основанием вечной блокировки.
Ориентиры: [OWASP Bot Management](https://cheatsheetseries.owasp.org/cheatsheets/Bot_Management_and_Anti-Automation_Cheat_Sheet.html),
[OWASP BOLA](https://owasp.org/API-Security/editions/2023/en/0xa1-broken-object-level-authorization/).

## Release recommendation

1. **Discovery:** current documents + owner decisions + external deep research.
2. **Prototype:** `/pashalki/`/`Моё`, one feed insertion, feedback, email CTA,
   admin inventory mock; no backend/prize.
3. **Architecture gate:** data model, promo activity, ledger, privacy/a11y/security.
4. **Shadow/dry-run:** scheduler decisions and admin report without public objects.
5. **Non-prize canary:** one collection, cohort placement, holdout, stop rules.
6. **Decision:** ship/narrow/stop after full-cycle evidence.
7. **Only later:** partner form; material reward remains separate legal release.

## Kill criteria before build

Reject/defer implementation if any remains true:

- no plausible traffic/MDE for causal evaluation;
- no first-class subject/ledger without fake event IDs;
- no owner for fact/IP/safety and proposal moderation;
- no accessible equivalent completion path;
- no core-discovery non-inferiority metric;
- product case depends on prize, mandatory share or daily streak;
- admin cannot immediately pause an unsafe/outdated placement;
- mechanism requires personal raw progress in public CDN HTML.
