# Product prototype: фокус-группа, благодарность и завершение

> **Статус:** продуктовая спецификация прототипа, не production implementation.
> **Требования:** R01, R09.
> **Связанные документы:** [README.md](README.md),
> [current-state-audit.md](current-state-audit.md),
> [manual-email-templates.md](manual-email-templates.md).
> **Граница:** документ описывает IA, состояния и текст интерфейса. Он не
> запускает фокус-группу, не объявляет конкурс, не создаёт право на приз, не
> отправляет письма и не меняет production.

## 1. Неподвижные продуктовые решения

1. Основная ценность участия — влияние на качество продукта, а не вознаграждение
   за положительный отзыв.
2. Базовый экран завершения благодарит за участие независимо от количества,
   длины и тона feedback, числа приглашений или публикаций в соцсетях.
3. Если отдельный prize gate когда-либо будет закрыт, **один приз — одна пара,
   то есть два приглашения на любой спектакль партнёра «Акт-Опус»**.
4. Положительная оценка, feedback, share, invite, скорость прохождения,
   количество найденных объектов и покупка **не дают multiplier и не меняют
   шанс**. В допустимой механике одна допущенная заявка verified tester имеет тот
   же вес, что любая другая.
5. Логотип партнёра показывается как атрибуция, а не как endorsement всего
   сервиса. До production используются только письменно согласованный логотип,
   spelling и правила размещения.
6. Окончание tester programme выключает tester privileges, но не удаляет
   аккаунт, явно сохранённые события и обычные пользовательские настройки.
7. Персонализация остаётся честным bounded local prototype: она объяснима,
   редактируема и не обещает cross-device профиль.

Официальный сайт партнёра сейчас использует форму `Акт.Опус`, тогда как
утверждённая формулировка требования — `«Акт-Опус»`. Поэтому exact spelling,
логотип и фраза о «любом спектакле» должны быть подтверждены партнёром до
публикации; прототип не пытается разрешить это расхождение самостоятельно.

## 2. Prototype / production boundary

| В прототипе | Только после отдельного production gate |
|---|---|
| IA и текст экранов | Публичная или cohort-публикация экранов |
| Клиентские макеты состояний | Membership/RPC/cron/operator backend |
| Описание lifecycle и end reason | Автоматический перевод реальных участников |
| Placeholder согласованного логотипа | Получение, хранение и публикация logo asset |
| Честная условная формулировка подарка | Правила, eligibility, draw, alternate, выдача |
| Визуализация локальных интересов | Durable inferred profile или cross-device sync |
| Ссылка на ручные email templates | Генератор, outbox, worker, cron или bulk send |

До подтверждения organizer, сроков, eligibility, выбора получателя, alternate,
порядка получения, privacy/retention, anti-abuse и partner terms призовой блок
остаётся `hidden` или `pending_approval`. Ни один текст `pending_approval` не
использует «вы выиграете», «розыгрыш состоится» или дату выдачи.

## 3. Information architecture

### 3.1. `/focus/` — программа

Порядок блоков:

1. `Фокус-группа · Прототип` и текущий статус программы;
2. что участник проверяет и сколько длится период;
3. что останется после завершения;
4. правила feedback без давления;
5. отдельный условный блок благодарности партнёра;
6. управление участием и связь с оператором.

Пример основного текста:

> **Помогите сделать выбор событий понятнее.**
> В режиме фокус-группы можно отмечать полезность страниц и сообщать об
> ошибках. Количество и тон отзывов не влияют на участие или возможный подарок.

### 3.2. `/focus/progress/` — прогресс участника

Показывает только понятные человеку факты:

- период: `30 июля — 30 августа`;
- состояние: `Активен`, `Приостановлен`, `Завершён` или `Вы вышли`;
- исследованные возможности без leaderboard;
- собственные обращения и их проверяемые статусы;
- CTA `Настроить «Для меня»`;
- CTA `Выйти из фокус-группы`.

Здесь нет points, рейтинга участников, «ещё один отзыв до повышения шанса»,
social-share streak и сравнения с другими людьми.

### 3.3. `/dlya-menya/` — персонализация

Верхний блок:

> **Для меня · Прототип**
> Лента меняет порядок уже опубликованных событий по выбранным вами темам и
> нескольким действиям в этом браузере. Это не полный профиль и не гарантия
> подходящего события.

Рядом всегда доступны:

- `Почему это показано`;
- `Настроить интересы`;
- `Показать обычную ленту`;
- `Очистить локальные подсказки`.

Основной редактор — **карточки категорий**. В каждой карточке один tri-state:

```text
Театр
[ Чаще ] [ Без предпочтения ] [ Реже ]
```

`Без предпочтения` — neutral default. Две колонки «интересно / неинтересно»
отклонены как primary editor: они заставляют принимать бинарное решение,
плохо объясняют neutral state и смешивают отсутствие сигнала с отрицательным
предпочтением.

### 3.4. `/focus/thanks/` — завершение и благодарность

Один route поддерживает разные end-state тексты, но одинаковую структуру:

1. нейтральное `Спасибо за участие`;
2. причина и дата завершения;
3. что выключено;
4. что сохранено;
5. CTA в обычный сайт и настройки персонализации;
6. условная partner attribution, только если она прошла отдельный gate.

## 4. Lifecycle programme: state machine

Состояние программы и причина завершения хранятся раздельно. Это не позволяет
операторскому раннему закрытию выглядеть как плановое.

### 4.1. Programme state

```text
draft
  └─ operator publishes schedule ─> scheduled
scheduled
  ├─ starts_at reached + launch gate open ─> active
  └─ operator cancels before start ─> ended(operator_cancelled)
active
  ├─ operator pauses ─> paused
  ├─ ends_at reached ─> ended(time_elapsed)
  ├─ operator closes early ─> ended(operator_closed)
  └─ operator cancels ─> ended(operator_cancelled)
paused
  ├─ operator resumes before ends_at ─> active
  ├─ ends_at reached ─> ended(time_elapsed)
  ├─ operator closes ─> ended(operator_closed)
  └─ operator cancels ─> ended(operator_cancelled)
ended
  └─ retention/audit complete ─> archived
```

`capacity reached` закрывает только admission и показывает `Места закончились`;
это не завершает программу уже активным участникам. `paused` также не является
завершением: обычные static pages продолжают работать, а новые admission,
invites, tester feedback prompts и prize applications fail closed до явного
resume.

### 4.2. Automatic end contract

По достижении `ends_at` UI и каждый privilege check трактуют программу как
`ended(time_elapsed)`, даже если housekeeping ещё не запущен. Для active
membership effective state становится `alumni`; нельзя оставлять временное окно,
в котором tester controls ещё работают.

UI copy:

> **Исследовательский период завершён.**
> Спасибо, что проверяли сайт вместе с нами. Режим тестера, новые приглашения и
> исследовательские сообщения выключены. Ваш аккаунт, сохранённые события и
> обычные настройки не удалены.

### 4.3. Operator end contract

Оператор выбирает одну явную причину:

| End reason | Когда использовать | Пользовательский заголовок |
|---|---|---|
| `operator_closed` | цели выполнены или период закончен досрочно | `Программа завершена раньше` |
| `operator_cancelled` | продолжение невозможно; обещания не выполняются | `Программа остановлена` |

Обязательны `ended_at`, internal reason, approved public explanation и operator
identity. Свободный internal comment не публикуется автоматически.

UI copy для `operator_closed`:

> **Программа завершена раньше.**
> Команда закрыла исследовательский период [дата]. Новые отзывы и приглашения в
> режиме тестера больше не принимаются. Обычный сайт и ваши сохранения доступны.

UI copy для `operator_cancelled`:

> **Программа остановлена.**
> Мы не продолжаем этот исследовательский цикл. Возможный подарок не считается
> объявленным или обещанным, если его отдельные правила не были опубликованы и
> приняты. Ваш обычный аккаунт остаётся доступен.

### 4.4. Membership state

```text
pending_verified_identity
  ├─ verified + capacity available + terms accepted ─> active
  └─ invite expired / capacity closed ─> not_activated
active
  ├─ member leaves or research consent withdrawn ─> withdrawn
  ├─ active_until/program ends ─> alumni
  └─ operator removes under approved rule ─> removed
withdrawn | alumni | removed
  └─ no implicit reactivation; a new programme needs a new explicit decision
```

| Effective member state | Tester prompts/invites | Manual focus mail | Account/saves | Ordinary personalization |
|---|---|---|---|---|
| `active` | on, subject to sampling/cap | allowed after per-send check | preserved | available as prototype |
| `paused programme` | off | only approved service notice | preserved | available |
| `alumni` | off | end confirmation only; no weekly mail | preserved | available |
| `withdrawn` | off immediately | no weekly mail; exit confirmation only if permitted | preserved | available |
| `removed` | off immediately | only approved service notice | preserved unless separate account process applies | available |

Tester state is never inferred from logo exposure, an egg QR, local storage or
editable user metadata.

## 5. Персонализация и continuity после окончания

### 5.1. Что визуализировать

Блок `Что влияет на ленту` использует четыре раздельных слоя:

1. **Ваш выбор** — tri-state `Чаще / Без предпочтения / Реже` для каждой
   категории. Это explicit preference.
2. **Индекс интереса** — отдельно рассчитанная inferred affinity, например
   `Индекс интереса: 70 из 100`. Это не вероятность клика, покупки или того,
   что событие понравится.
3. **Достаточность данных** — отдельная qualitative label
   `мало / достаточно / много сигналов` с кратким объяснением источников и
   freshness. Она не подменяется индексом интереса.
4. **Для этого события** — одна короткая причина:
   `Показано выше, потому что вы выбрали «Театр»` или
   `Это место для знакомства с новой темой`.

Индекс визуализируется нативным `<meter min="0" max="100" value="70">70 из
100</meter>` с видимой текстовой подписью, а не `progressbar`: это измерение в
известном диапазоне, не выполнение задачи. Число не показывается при
недостаточной evidence sufficiency.

Explicit override всегда сильнее inferred affinity:

- `Чаще` повышает категорию, даже если inferred index ниже;
- `Реже` понижает её, даже если inferred index выше;
- `Без предпочтения` разрешает bounded inferred rerank только при отдельном
  personalization consent;
- inferred actions никогда молча не меняют выбранный tri-state.

Не показываются психографические выводы, скрытый score, «мы знаем, что вы…»,
точные поисковые фразы, перечень просмотренных событий или выводы о возрасте,
здоровье, доходе и семье.

### 5.2. Почему именно так

- Видимая причина превращает персонализацию из «магии» в проверяемую гипотезу.
- Tri-state даёт контроль, сохраняет честный neutral state и помогает исправить
  неверный сигнал быстрее, чем новый feedback form.
- Разделение explicit choice, inferred affinity и достаточности evidence не
  выдаёт частое implicit действие за сознательное предпочтение.
- `Индекс интереса` имеет известную шкалу, но не называется confidence или
  probability и не используется как eligibility score.
- `Показать обычную ленту` даёт мгновенный контрольный вариант и честный fallback.
- Разделение explicit interests, local hints и saves предотвращает ложное
  утверждение, что прекращение персонализации удалит сохранённое.

Этот rationale следует принципам W3C о purpose limitation, contextual
transparency и контроле человека над privacy-relevant решениями:
[W3C Privacy Principles](https://www.w3.org/TR/privacy-principles/).
Google PAIR отдельно рекомендует объяснять использование данных, поддерживать
редактируемые permissions/controls и не показывать трудно интерпретируемую
numeric confidence:
[People + AI Guidebook patterns](https://pair.withgoogle.com/guidebook-v2/patterns).
Для implicit feedback классическая работа Hu, Koren и Volinsky разделяет
наблюдаемое preference и confidence в наблюдении; это поддерживает решение не
склеивать affinity с sufficiency:
[Collaborative Filtering for Implicit Feedback Datasets](https://yifanhu.net/PUB/cf.pdf),
[DOI 10.1109/ICDM.2008.22](https://doi.org/10.1109/ICDM.2008.22).
Для scalar value в известном диапазоне W3C определяет meter и прямо отделяет
его от progress:
[W3C meter](https://www.w3.org/WAI/ARIA/apg/patterns/meter/).

### 5.3. Continuity matrix

| Data/capability | Во время focus | После `alumni/withdrawn` | Честное ограничение |
|---|---|---|---|
| Auth account | identity | остаётся | не означает tester status |
| Явно сохранённые события | обычная функция | остаются | только после отдельного durable-state reconciliation |
| Явно выбранные интересы | влияют на local prototype | продолжают влиять, если пользователь их не очистил | этот браузер; cross-device не обещан |
| Локальные action hints | bounded rerank | остаются до expiry/очистки | не превращаются в durable inferred profile |
| Tester progress | status/progress UI | read-only итог или скрыт по retention | не влияет на обычную ленту |
| Tester feedback prompts | sampled | выключены | improvement обычного сайта — отдельный surface |
| Research email purpose | active condition | выключен | marketing не включается автоматически |

Окончание программы меняет privilege, а не стирает всё под одним общим
переключателем. При storage failure или отказе от персонализации сайт показывает
обычную static ленту; сохранённые события, если они доступны как отдельная
обычная функция, не должны исчезать.

UI copy после завершения:

> **«Для меня» продолжит работать как прототип.**
> Выбранные темы и локальные подсказки в этом браузере не зависят от статуса
> тестера. Можно изменить темы, показать обычную ленту или очистить подсказки.

## 6. Автоматические персональные подборки — отдельный stream

### 6.1. Product decision

В UI можно показать prototype state будущих автоматических персональных
подборок, но это **отдельный recommendation stream**, а не weekly/system/update
mail фокус-группы. Focus research consent, membership, PWA install и открытие
`/dlya-menya/` не подписывают на него автоматически.

В этой lane нет sender implementation. Не создаются purpose/outbox/worker/cron,
не отправляется письмо и не проектируется скрытый reuse ручных focus templates.

### 6.2. Eligibility

`selection_eligible` истинно только если одновременно:

1. пользователь включил отдельный explicit personalization/recommendation
   opt-in;
2. есть хотя бы один explicit `Чаще`;
3. есть минимум три интерпретируемых preference facts: explicit tri-state или
   bounded strong action с известным source и freshness;
4. evidence sufficiency не `мало`;
5. каждый выбранный candidate имеет актуальные event facts и короткое
   `Почему это`;
6. stream не paused/suppressed/unsubscribed.

Это eligibility к формированию preview, а не право на отправку. Ни `Индекс
интереса`, ни membership, ни число feedback сами по себе eligibility не дают.
Если explicit choice конфликтует с inferred affinity, explicit override wins.

### 6.3. Prototype UI states

| UI state | Текст | Поведение |
|---|---|---|
| `not_opted_in` | `Персональные подборки выключены` | CTA `Узнать и включить`; обычная static лента |
| `learning` | `Пока мало понятных сигналов` | tri-state editor; static editorial selection |
| `eligible_preview` | `Предпросмотр персональной подборки · Прототип` | on-site preview, sender отсутствует |
| `paused` | `Подборки временно приостановлены` | static/manual fallback |
| `unsubscribed` | `Автоматические подборки выключены` | не менять focus membership |

Для opt-in есть отдельный plain-language purpose:

> Разрешаю использовать выбранные категории и ограниченные действия, чтобы
> составлять персональные подборки. Это не подписка на новости фокус-группы.

Для unsubscribe отдельная команда `Отключить персональные подборки`. Она
выключает этот stream, но не удаляет аккаунт/saves, не выводит из focus group и
не меняет marketing/research purposes. Аналогично выход из focus group не
отменяет отдельный recommendation opt-in.

### 6.4. Fallback и continuity

- На сайте при отсутствии consent/signals/backend используется обычная static
  editorial selection с label `Общая подборка, не персональная`.
- До sender release допустим только prototype preview. Если оператор когда-либо
  отправляет общую подборку вручную, она относится к отдельному recommendation
  purpose, маркируется `общая, не персональная` и не использует focus-system
  templates.
- После окончания focus состояние tri-state и отдельный opt-in продолжают
  действовать в рамках их собственного retention/consent. UI теряет tester
  chrome, но не сбрасывает пользовательский выбор.
- Если state хранится только локально, новый браузер начинает с neutral editor;
  cross-device continuity не обещается.

UI copy в end screen:

> **Персональные подборки настраиваются отдельно.**
> Завершение фокус-группы не меняет ваш отдельный выбор. Сейчас доступен только
> prototype preview; автоматическая отправка ещё не запущена. Подборки можно
> отключить независимо от исследовательских сообщений.

## 7. Thank-you и conditional prize presentation

### 7.1. Всегда доступная благодарность

> **Спасибо, что были в фокус-группе.**
> Ваше участие помогло проверить, где сайт понятен, а где требует доработки.
> Мы учитываем и положительные, и критические ответы. Количество отзывов и
> публикаций не давало преимуществ.

Здесь можно показать проверяемый общий итог (`проверено 11 разделов`,
`исправлено 4 подтверждённые проблемы`) только при наличии evidence links.
Нельзя писать «благодаря вашему отзыву», если causal link не подтверждён.

### 7.2. Partner attribution

Согласованный компонент содержит:

- approved logo asset;
- видимый текст `Партнёр благодарности — «Акт-Опус»`;
- alt `Логотип партнёра «Акт-Опус»`;
- ссылку на согласованную страницу партнёра;
- нейтральное размещение после основной благодарности.

Логотип не заменяет текстовое имя и не используется как фоновый watermark.
Нельзя брать asset из поисковой выдачи или с сайта без подтверждённого права на
это конкретное использование.

### 7.3. Prize states and copy

| State | Что видно | Допустимый текст |
|---|---|---|
| `hidden` | только общая благодарность | prize copy отсутствует |
| `pending_approval` | internal prototype only | `Механика подарка ещё не объявлена` |
| `applications_open` | только после separate release gate | `Можно подать одну равновесную заявку` |
| `applications_closed` | срок и immutable snapshot | `Приём заявок завершён` |
| `selection_pending` | без намёка на победителя | `Результат ещё не подтверждён` |
| `recipient_notified` | публично только по правилам/privacy | `Результат опубликован по правилам` |
| `fulfilled` | только подтверждённый факт | `Подарок передан` |
| `cancelled` | причина из принятых правил | нельзя импровизировать обещание замены |

Условный copy после закрытия всех gates:

> **Подарок от партнёра «Акт-Опус».**
> Один подарок — одна пара: **два приглашения на любой спектакль партнёра
> «Акт-Опус»**, доступный по согласованным правилам. Одна допущенная заявка
> участвует наравне с каждой другой. Оценки, отзывы, приглашения друзей и
> публикации не увеличивают шанс.

`Любой спектакль` требует заранее согласованных исключений, срока использования,
наличия мест и порядка бронирования. Пока они не закреплены в отдельных правилах,
этот copy остаётся прототипом и не публикуется.

### 7.4. Fairness rationale

- Feedback должен измерять опыт, а не желание получить награду; multiplier за
  оценку или текст системно смещает ответы.
- Share/invite multiplier ставит в худшее положение людей с меньшей социальной
  сетью и превращает закрытое исследование в promotion.
- Speed и `8/8` наказывают поздно приглашённых участников и людей, использующих
  accessibility alternatives.
- Одна равновесная заявка на verified tester отделяет eligibility от поведения и
  делает denominator аудируемым.
- Доступная альтернатива, outage credit и immutable snapshot нужны, чтобы
  техническая ошибка или недоступный interaction не меняли шанс.

## 8. Acceptance checklist для prototype handoff

- [ ] На каждом экране явно написано `Прототип`, где поведение ещё не production.
- [ ] Один подарок везде означает ровно два приглашения, а не два приза.
- [ ] Партнёр назван и атрибутирован логотипом; spelling и asset имеют approval gate.
- [ ] Ни feedback, ни score, ни share, ни invite не меняют eligibility/odds.
- [ ] Без legal/partner/privacy gate prize state не выходит из `hidden/pending_approval`.
- [ ] Automatic и operator end визуально и аналитически различимы.
- [ ] `ends_at` прекращает privileges синхронно, даже без housekeeping.
- [ ] `alumni/withdrawn` не удаляет account, saves или обычные настройки.
- [ ] `Для меня` объясняет local-only сигналы и имеет static fallback.
- [ ] Interest visualization не раскрывает raw history и позволяет исправить/очистить сигнал.
- [ ] Primary interest editor — category cards с tri-state, не две колонки.
- [ ] Explicit choice, inferred index и evidence sufficiency показаны раздельно.
- [ ] Индекс — `<meter>` и `70 из 100`, не probability/confidence/progress.
- [ ] Recommendation stream имеет отдельные opt-in/unsubscribe и не наследуется от focus.
- [ ] Недостаток consent/signals даёт static/manual fallback, а не fake personalization.
- [ ] После focus end отдельный personalization choice сохраняет continuity.
- [ ] Email CTA ссылается только на ручной SOP; генератора и send automation нет.

## 9. Источники для product review

- Официальный сайт партнёра и актуальное публичное написание названия:
  [Театр Акт.Опус](https://actop.us/).
- Transparency, purpose limitation и user control:
  [W3C Privacy Principles](https://www.w3.org/TR/privacy-principles/).
- Feedback, control, permissions и интерпретируемая confidence:
  [Google PAIR patterns](https://pair.withgoogle.com/guidebook-v2/patterns).
- Разделение implicit preference и confidence:
  [Hu, Koren, Volinsky (2008)](https://yifanhu.net/PUB/cf.pdf).
- Scalar measurement, не progress:
  [W3C meter pattern](https://www.w3.org/WAI/ARIA/apg/patterns/meter/).

Источники подтверждают только текущую публичную идентичность партнёра и общие
design principles. Они не подтверждают prize terms, право на логотип или
юридическую готовность механики.
