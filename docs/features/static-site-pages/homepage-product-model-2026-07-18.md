# Главная KenigEvents: mobile/desktop product contract

> Дата: 2026-07-18
>
> Статус: сфокусированная продуктовая рекомендация; exact UI и thresholds требуют owner acceptance и behavioral baseline
>
> Scope: только главная на mobile и desktop
>
> Внешняя критика: свежий `agy` review через `Gemini 3.1 Pro (High)`; prompt, raw response и invocation evidence — `artifacts/codex/static-home-product-20260718/`

## Решение

Главная — конечный discovery episode, а не бесконечная лента:

```text
Hero Talk
→ быстрые маршруты
→ lifecycle-aware discovery
→ state-aware text resolver
→ доступный footer
```

Эта последовательность одинакова по смыслу на mobile и desktop. Отличаются способ выбора и цена занимаемого пространства:

- mobile чаще ведёт пользователя последовательно после social entry;
- desktop чаще поддерживает сравнение, shortlist и несколько вкладок;
- inventory, lifecycle state, seen ledger и метрики у них общие.

## 1. Плоскость mobile home

```text
┌ Hero Talk ───────────────────────────────┐
│ один grounded сценарий                   │
│ один основной route, максимум secondary  │
└──────────────────────────────────────────┘

[ Сегодня ] [ Завтра ] [ Выходные ] [ Популярное ] [ Поиск ]

┌ события текущей feed edition ────────────┐
│ event cards                              │
│ optional inline trust ask                │
│ event cards / recovery                   │
└──────────────────────────────────────────┘

┌ text-only Hero Talk / Resolver ──────────┐
│ изменить тактику или завершить план       │
└──────────────────────────────────────────┘

footer: основные маршруты + trust/service links
```

### Hero Talk

- не требует ответа перед доступом к ленте;
- не заменяет быстрые маршруты;
- ведёт только в актуальный candidate set с достаточным supply;
- promo может изменить тему, но не обойти lifecycle, relevance, fatigue и hide;
- сам Hero Talk не расходует 30-card opportunity budget.

### Высота: `1/2` или `2/3`

**Рекомендованный baseline — не больше половины mobile viewport.** Это уже соответствует предыдущему lab contract `<=50svh` и оставляет доказательство, что ниже есть реальный inventory.

`60–65svh` — допустимый challenger только для:

- первого direct/brand визита;
- редкой сильной occasion/campaign;
- Hero Talk, который сам даёт grounded route/action и не блокирует scroll.

Это не постоянный layout. Recognized, regular и mature пользователь не должен на каждом визите снова платить две трети экрана за ориентацию. Для них Hero становится содержательно короче; exact compact height выбирается после prototype/behavioral evidence.

Итого:

| Состояние | Product rule |
|---|---|
| Newcomer | baseline `<=50svh`; отдельно тестировать `60–65svh` challenger |
| Recognized | `<=50svh`, resume/delta вместо повторной презентации сервиса |
| Regular | компактный актуальный shortcut |
| Mature | только материальная personal delta/сильный occasion; быстрый доступ к feed |

### Краткая навигация

`Сегодня / Завтра / Выходные / Популярное / Поиск` — escape routes, а не onboarding step. Пользователь может обойти Hero Talk одним нажатием.

Поиск следует отличать как hard-intent действие, а не прятать после `Популярного` только из-за порядка списка. Exact visual placement — UI-решение; продуктовый инвариант — он заметен до того, как пользователь исчерпал ленту.

## 2. Плоскость desktop home

```text
Hero Talk / campaign router
→ постоянно видимые быстрые маршруты
→ первая доказательная группа реальных событий
→ stable comparison feed/grid + shortlist
→ text-only resolver
→ footer
```

Desktop home не является другим продуктом. Его отличия:

- Hero занимает меньшую относительную долю первого экрана: inventory должен участвовать в first-view proof;
- несколько event detail tabs — нормальное сравнение, а не quick skip;
- порядок feed edition стабилен в пределах planning session;
- действия в дочерних вкладках атрибутируются исходному served list;
- Back/возврат восстанавливает shortlist, позицию, task context и seen-state;
- onboarding встраивается между рядами/modules и не перекрывает planning modal-окном.

Bottom resolver на desktop завершает сравнение: помогает изменить горизонт, уточнить constraint или сохранить shortlist, а не просто предлагает ещё один scroll.

## 3. Четыре состояния пользователя

Состояние не равно номеру визита, login или наличию email.

| State | Evidence | Hero job | Feed job | Допустимый ask | Success |
|---|---|---|---|---|---|
| **Newcomer** | нет доказанного prior intent/outcome | объяснить один актуальный сценарий без personal claims | fuzzy popular + current/actionable diversity | никакого trust ask на первом экране | первый `interest hit` + meaningful action |
| **Recognized** | один grounded intent episode/сильный outcome либо несколько explicit signals | восстановить прошлую задачу или показать честную дельту | unseen baseline + осторожный signal boost | personalization consent после осмысленного действия | второй qualified find, меньше cards/time-to-value |
| **Regular** | несколько успешных discovery→interest episodes в разных occasions | новое релевантное, план или текущий shortcut | relevant unseen/new-fit + adjacent novelty | identity/sync после durable value; email после найденной пользы | qualified return, shortlist/plan |
| **Mature** | профиль доказал достаточную ширину, свежесть и стабильность | материальная personal delta или смена task | context-first personalization + dedupe + recovery | только контекстный opt-in, не общий onboarding | relevant exposure `<=30` и realized value |

Текущие E2E thresholds `3 sessions / 30 valid impressions / 5 strong positives / 2 explicit negatives` остаются versioned test fixture, а не автоматическим production-сегментатором.

### Отдельные оси

- `identity_mode`: anonymous/local/Yandex/email-synced;
- `consent_state`: session-only/persistent personalization allowed/revoked;
- `activity_state`: active/dormant;
- `profile_maturity`: newcomer/recognized/regular/mature.

Login не делает профиль mature. Consent без сигналов оставляет пользователя newcomer. Inactivity не обязана стирать зрелые долгосрочные предпочтения.

## 4. Наполнение ленты

### Newcomer: fuzzy popular

Чистое `Популярное` нельзя использовать как default home ranker: оно закрепляет преимущество больших площадок, массовых жанров и источников с лучшей измеримостью.

`Fuzzy popular` означает:

```text
active/actionable eligibility
+ текущий временной context
+ нормализованный social proof
+ freshness
+ diversity по event family / venue / format
+ controlled exploration
```

Это не персонализация и не случайная мешанина. Лента не говорит `Для вас`, не повторяет одну площадку/тип подряд и не выдаёт promo reach за пользовательский интерес.

### Recognized: unseen, а не «мы уже вас знаем»

- уже валидно показанное и explicit hidden не занимает первые позиции снова;
- первые сигналы дают ограниченный boost/filter;
- cold baseline остаётся страховкой;
- текущая edition не пересортировывается после каждого action;
- один лайк не превращает всю ленту в узкую тему.

### Regular: `new-fit` + anti-bubble

`new-fit` — не просто новая строка в каталоге:

```text
(catalog-new ИЛИ materially updated)
AND unseen after current content version
AND relevant to current task/profile
AND active/actionable
```

Anti-bubble — **совместимая соседняя новизна**, а не randomness. Она сохраняет hard constraints текущей задачи — дату, город/допустимую поездку, цену, возраст/компанию — и варьирует жанр, площадку или format.

Нельзя подмешивать театр в строгий запрос `рок сегодня` только ради разнообразия. Task intent сильнее long-term taste и anti-bubble.

### Mature: исполнение обещания

- контекст текущей задачи остаётся первым множителем;
- high-confidence personal candidates идут среди первых unseen opportunities;
- fresh relevant и adjacent novelty защищают от устаревшего пузыря;
- popular остаётся resilience/social-proof input, а не основой mature feed;
- один cross-surface seen ledger дедуплицирует home/date/popular/search/related;
- при отсутствии value стратегия меняется до rank 30, но окно не сбрасывается.

30 — метрика достижения релевантности, **не запрет продолжить просмотр**. После 30 нужен честный escape hatch: расширить критерии, перейти в поиск/другую дату или смотреть глубже. Продолжение не переписывает факт P30 miss.

### Общие правила

- feed edition стабильна в пределах session;
- event, linked occurrence и duplicate family не расходуют новые opportunities повторно;
- promo disclosed/capped и не обучает organic affinity одним exposure;
- onboarding/resolver modules не считаются event impressions;
- ranking mix не получает fixed percentages до behavioral baseline.

## 5. Progressive onboarding внутри ленты

Главная не должна выдать подряд три просьбы: `разрешите персонализацию → войдите → оставьте email`.

### Порядок обмена доверием

```text
сначала реальный inventory/value
→ затем explicit feedback или durable intent
→ объяснение конкретной следующей пользы
→ один уместный trust ask
```

### Personalization consent

Не показывать на load, в Hero или перед первой карточкой.

Подходящие triggers:

- пользователь нажал like / `Не интересно` / просит подстроить следующие события;
- либо дошёл до продолжения и сам запросил ещё варианты, когда следующая batch действительно может измениться.

Первая реакция должна работать session-only там, где это возможно. Ask объясняет: `запомнить это и не показывать похожее/подстроить следующие события`, а не абстрактное `разрешите обработку данных` без product benefit.

Dismiss suppresses повтор хотя бы до следующего planning occasion; explicit deny/revoke возвращается только по user-triggered персональному действию. Exact cooldown калибруется, но один и тот же ask не повторяется в одной session.

### Yandex/email identity

Identity предлагается при durable/cross-device value:

- сохранить shortlist/plan;
- продолжить на другом устройстве;
- восстановить профиль;
- использовать authenticated smart capability;
- получать изменения конкретного сохранённого события.

Local hide/like и обычный просмотр не требуют login. После identity local state merge идемпотентен и не затирает более сильную историю.

### Search gate

**Рекомендация остаётся прежней: basic search и первая полезная выдача публичны.** Identity открывает semantic refinement, history, save и sync.

Если auth до результата пока неизменяем:

1. пользователь сначала формулирует query;
2. query сохраняется до OAuth/email verification;
3. CTA честно объясняет доступную после входа пользу;
4. callback возвращает прямо к восстановленной выдаче;
5. Hero Talk заранее раскрывает gate и не ведёт cold user в неожиданный dead end;
6. feed не содержит generic login-card `войдите ради поиска` без текущего search intent.

Нельзя показывать выдуманный `найдено N` или fake result skeleton, если система ещё не выполнила публичный search.

### Email

Recommendation consent показывается только после найденной ценности или явного запроса `сообщить, когда появится подходящее`.

Принятый contract:

- ровно три сильных distinct events;
- если трёх нет — no-send;
- Yandex/email identity не равна subscription consent;
- email ask не появляется в одной session с personalization/login ask;
- success — incremental qualified return/value, не signup или open rate.

### Fatigue invariant

- максимум один trust ask на meaningful session;
- ticket/calendar completion никогда не прерывается;
- dismiss/deny имеют bounded cooldown;
- после выполненной цели главная перестаёт онбордить и помогает сохранить/продолжить план.

## 6. Нижний text-only Hero Talk

Идея правильная, если этот блок — **Resolver**, а не повтор верхней кампании.

Один физический slot получает три режима:

| Session result | Нижний Hero Talk |
|---|---|
| Value не найдено, supply есть | задаёт один high-information constraint, меняет стратегию и предлагает bounded unseen recovery |
| Релевантного supply нет | честно сообщает об этом; другая дата/шире критерии/search/future alert |
| Value найдено | помогает сохранить shortlist, открыть следующий horizon или отдельно подписаться |

Resolver:

- text-only, без сопутствующих картинок;
- не повторяет wording/кампанию верхнего Hero;
- не сбрасывает discovery episode и P30 rank;
- не запускает бесконечный feed автоматически;
- не показывает promo как персональное спасение;
- после dismiss не возвращается в той же session.

Для mature no-value journey облегчённый rescue может появиться до конца, когда стало ясно, что первые 10/20 opportunities не сработали. Нижний slot остаётся terminal resolution. Exact trigger проверяется экспериментом; нельзя ждать footer, если upstream miss уже очевиден.

Footer после Resolver остаётся utility/trust layer: основные fixed destinations, официальные каналы, контакты и service links.

## 7. Основные failure modes

1. `2/3` Hero становится постоянным барьером для returning/mature.
2. Fuzzy popular скрыто превращается в pure engagement rank и монополию больших источников.
3. Anti-bubble нарушает date/search hard intent.
4. Consent, login и email asks идут подряд и создают onboarding wall.
5. OAuth/email callback теряет query, feed edition или scroll position.
6. Bottom Hero повторяет promo вместо диагностики miss.
7. 30 трактуется как hard stop либо сбрасывается после recovery/перехода.
8. Multi-tab desktop comparison ошибочно считается skip/повторными независимыми sessions.
9. `new` означает raw created_at, а не unseen relevant material delta.
10. Пользователь получает personal claims после одного случайного action.

## 8. P0 / P1

### P0 — доказать полезную главную

1. Hero Talk → shortcuts → lifecycle feed → text Resolver → footer как один finite contract.
2. Baseline Hero `<=50svh`; `60–65svh` только cold-user experiment.
3. Newcomer fuzzy-popular candidate policy и recognized unseen suppression.
4. Общая feed edition, valid impressions, event-family dedupe и mobile/desktop state restore.
5. Meaningful-action attribution и lifecycle evidence без ложных personal claims.
6. Один progressive ask на session; consent/auth/email contracts разделены.
7. Query/intent сохранён через auth callback, пока gate существует.
8. Minimal text-only terminal Resolver; advanced reframing/recovery может идти позже.

### P1 — learning и return

1. `new-fit since last meaningful visit`.
2. Regular/mature context-aware ranking и adjacent anti-bubble.
3. Cross-device save/shortlist/profile merge.
4. Earlier no-success rescue и bounded recovery batch.
5. Exactly-three email holdout/canary.
6. Cross-tab desktop attribution и stable comparison evidence.

## 9. Пять экспериментов

1. **Hero height, newcomer only:** `<=50svh` baseline vs `60–65svh`; downstream interest/time-to-value, не Hero CTR.
2. **Cold feed:** pure Popular vs fuzzy popular; qualified event find, category/venue concentration и supply coverage.
3. **Recognized/regular feed:** cold baseline vs unseen/new-fit + cautious profile boost; cards/time-to-second-value и false-personalization/hide.
4. **Trust ask timing:** после explicit feedback vs после user-requested continuation; next-batch improvement, abandonment, dismiss/revoke — без timeout modal.
5. **End state:** footer-only vs state-aware text Resolver; incremental value/pivot, abandonment и recovery dependency.

Search continuity — P0 correctness gate, а не повод тестировать очевидно ломающий query hard redirect.

## 10. Что принято и исправлено после Gemini Pro review

### Принято

- один Hero height для всех lifecycle states неверен;
- до `60–65svh` допустимо тестировать только newcomer/сильный occasion;
- fuzzy popular лучше чистого popularity top;
- anti-bubble не нарушает hard intent;
- auth-gated basic search разрушает activation;
- нижний text Resolver имеет самостоятельную recovery value;
- после rank 30 нужен escape hatch, но P30 miss сохраняется.

### Скорректировано

- Gemini назвал newcomer success `scroll/click/return intent`. Здесь success — interest hit и meaningful action; scroll/CTR только диагностика.
- Gemini предложил fake count/skeleton до auth. Это запрещено без реального public retrieval.
- Gemini поставил Bottom Hero целиком в P1. Minimal terminal Resolver входит в P0 skeleton; сложный conversational recovery — P1.
- Gemini предложил timeout-modal как ask experiment. Modal до доказанной пользы не нужен; тестируются два контекстных inline trigger.
- Gemini слишком поздно разрешил email только mature/после attendance. Ask допустим раньше после доказанной value, но consent остаётся отдельным и email не отправляется без трёх сильных events.
- Gemini назвал 30 hard ceiling. Это SLA window, а не запрет продолжать discovery.

## 11. Метрики

### Общие

- `relevant_exposure@30`;
- `realized_value@30`;
- P6/P12/P30 как diagnostic curve;
- unique impressions/time-to-interest;
- duplicate/seen waste;
- supply vs retrieval vs ranking vs realization miss.

### Lifecycle

- Newcomer: qualified home activation, not raw scroll.
- Recognized: second-success rate и сокращение cards/time-to-value.
- Regular: qualified return, new-fit coverage, time-to-shortlist.
- Mature: P30 reliability, diversity/anti-bubble success, downgrade/contradiction.

### Home modules

- Hero Talk: downstream value и dead-route rate.
- Progressive asks: immediate product improvement, abandonment/dismiss/revoke; не opt-in rate alone.
- Resolver: incremental value/pivot and recovery dependency; высокий dependency означает upstream ranking problem.
- Desktop: shortlist completion, cross-tab attribution, state restore.

## Открытые owner decisions

1. Принять `<=50svh` как mobile baseline и оставить `60–65svh` только newcomer challenger.
2. Разделить public basic search и authorized smart search либо явно принять измеряемую activation loss auth-gate.
3. Принять нижний text-only Resolver как постоянный P0 slot с state-dependent content.
4. Разрешить lifecycle-dependent сокращение Hero Talk для returning/mature, а не сохранять одинаковую высоту на каждом визите.

До этих решений exact layout не замораживается.
