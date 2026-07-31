# Product prototype: фокус-группа, благодарность и завершение

> **Статус:** продуктовая спецификация прототипа, не production implementation.
> **Требования:** R01, R09, R17–R19.
> **Связанные документы:** [README.md](README.md),
> [current-state-audit.md](current-state-audit.md),
> [manual-email-templates.md](manual-email-templates.md),
> [программа пасхалок фокус-группы](easter-egg-program.md).
> **Граница:** документ описывает IA, состояния и текст интерфейса. Он не
> запускает фокус-группу, не объявляет конкурс, не создаёт право на приз, не
> отправляет письма и не меняет production.

## 1. Неподвижные продуктовые решения

1. Основная ценность участия — влияние на качество продукта, а не вознаграждение
   за положительный отзыв.
2. Базовый экран завершения благодарит за участие независимо от количества,
   длины и тона feedback, числа приглашений или публикаций в соцсетях.
3. Последнее owner decision: если отдельный prize gate будет закрыт, **один приз
   — ровно два билета в театр**. Это не два приза; театр, доступные спектакли,
   даты, места, исключения и бронирование подтверждаются отдельными правилами.
4. Для focus prize programme результат определяется сначала по доле собранной
   коллекции пасхалок, затем по ограниченной широте исследовательского участия.
   Значение NPS, положительность, длина текста, share/invite, покупка, скорость и
   повторный spam преимущества не дают. Полный versioned контракт:
   [easter-egg-program.md](easter-egg-program.md).
5. Логотип партнёра показывается как атрибуция, а не как endorsement всего
   сервиса. До production используются только письменно согласованный логотип,
   spelling и правила размещения.
6. Окончание tester programme выключает tester privileges, но не удаляет
   аккаунт, явно сохранённые события и обычные пользовательские настройки.
7. Персонализация остаётся честным bounded local prototype: она объяснима,
   редактируема и не обещает cross-device профиль.

Официальный сайт предполагаемого партнёра сейчас использует форму `Акт.Опус`,
тогда как прежний прототип использовал `«Акт-Опус»`. Exact spelling, logo asset и
роль партнёра должны быть подтверждены до публикации. Последнее owner decision
фиксирует только награду `два билета в театр`; фраза `на любой спектакль` больше
не считается принятой и не должна появляться без отдельных условий.

## 2. Prototype / production boundary

| В прототипе | Только после отдельного production gate |
|---|---|
| IA и текст экранов | Публичная или cohort-публикация экранов |
| Клиентские макеты состояний | Membership/RPC/cron/operator backend |
| Описание lifecycle и end reason | Автоматический перевод реальных участников |
| Placeholder согласованного логотипа | Получение, хранение и публикация logo asset |
| Честная pending-формулировка `один приз — два билета` | Правила, eligibility, rank/tie-break, alternate, выдача |
| Визуализация локальных интересов | Durable inferred profile или cross-device sync |
| Ссылка на ручные email templates | Генератор, outbox, worker, cron или bulk send |

До подтверждения organizer, сроков, eligibility, collection denominator,
scoring/tie-break, alternate, получения, privacy/retention, anti-abuse и partner
terms призовой блок остаётся `hidden` или `pending_approval`, а leaderboard —
`demo`. Ни один pending-текст не использует «вы выиграете», «конкурс начался» или
дату выдачи.

## 3. Information architecture

### 3.1. `/fokus-gruppa/` — программа

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
> ошибках. Тон, значение оценки и длина текста не влияют на результат. Только
> ограниченный факт разных исследовательских действий сможет учитываться после
> публикации отдельных правил.

### 3.2. `/zakrytaya-afisha/` — hub и прогресс участника

Показывает только понятные человеку факты:

- период: `30 июля — 30 августа`;
- состояние: `Активен`, `Приостановлен`, `Завершён` или `Вы вышли`;
- исследованные возможности и собственный demo-прогресс без заявления, что
  локальные действия участвуют в конкурсе;
- собственные обращения и их проверяемые статусы;
- CTA `Настроить «Для меня»`;
- CTA `Выйти из фокус-группы`.

До публикации правил здесь нет действующего рейтинга участников или обещания
`ещё один отзыв повысит шанс`. После отдельного gate допустим объяснимый
collection-first leaderboard из [versioned контракта](easter-egg-program.md):
raw NPS/feedback не публикуются, social-share streak отсутствует.

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

### 3.4. `/fokus-gruppa/zavershenie/` — завершение и благодарность

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
invite_accepted
  ├─ terms accepted + continue without identity ─> active_local
  ├─ email/Yandex verified + terms accepted ─> active_verified
  └─ invite expired / capacity closed ─> not_activated
active_local
  ├─ later identity verification + idempotent progress merge ─> active_verified
  ├─ member leaves ─> withdrawn
  └─ active_until/program ends ─> alumni
active_verified
  ├─ member leaves ─> withdrawn
  ├─ active_until/program ends ─> alumni
  └─ operator removes under approved rule ─> removed
withdrawn | alumni | removed
  └─ no implicit reactivation; a new programme needs a new explicit decision
```

| Effective member state | Tester prompts/invites | Manual focus mail | Account/saves | Ordinary personalization |
|---|---|---|---|---|
| `active_local` | on, subject to sampling/cap | off: no verified recipient | device-local only | available as prototype |
| `active_verified` | on, subject to sampling/cap | allowed after per-send check | preserved/recoverable under server contract | available as prototype |
| `paused programme` | off | only approved service notice | preserved | available |
| `alumni` | off | end confirmation only; no weekly mail | preserved | available |
| `withdrawn` | off immediately | no weekly mail; exit confirmation only if permitted | preserved | available |
| `removed` | off immediately | only approved service notice | preserved unless separate account process applies | available |

В production tester state нельзя выводить только из logo exposure, egg QR,
localStorage или editable user metadata. Текущая ветка намеренно показывает
local participation hint как UX-прототип: он открывает макет на этом устройстве,
но не является server authorization или reward ledger.

### 4.5. PWA return contract

- focus invitation manifest сохраняет уже опубликованный stable `id`
  `/fokus-gruppa/pwa`: менять manifest identity у уже установленного PWA
  нельзя. При этом его публичное имя — `Анонсы`, icons — обычные
  product icons, а не `Lab`-брендинг. После фокус-группы это же
  установленное приложение остаётся обычной афишей и не требует
  удаления/повторной установки;
- invitation start controller стартует через
  `/fokus-gruppa/priglashenie/?launch=pwa`;
- `active_local|active_verified` при обычном PWA launch сразу перенаправляются на
  главную афиши; `confirm=1` остаётся явным путём к подтверждению;
- при отсутствии/истечении participation hint остаётся onboarding с объяснением
  восстановления, а не бесконечный redirect;
- установка и запуск происходят только после действия пользователя;
- Android получает real `beforeinstallprompt` или честный menu fallback,
  iOS — инструкцию `Поделиться → На экран Домой`;
- focus participation, персонализация и коллекция имеют разные storage keys.
  `Сбросить персонализацию` удаляет только профиль `Для меня`; явный
  `Выйти из прототипа` может удалить participation отдельно.
- локальный participation marker активируется на полные 30 дней с момента
  вступления. Он не сокращается до preview-окна и не продлевается случайными
  page views; закрытие всей программы остаётся отдельной operator/server
  командой.
- production counterpart — server-owned anonymous membership плюс rotating
  device session для PWA. Он создаётся и без email/Яндекса, имеет тот же
  `joined_at + 30 days`, восстанавливается на каждом PWA launch и не зависит от
  очистки профиля `Для меня`. Подтверждённая identity нужна только для
  cross-device/после полной очистки site data, а не для повторного входа в
  течение обычного 30-дневного участия.

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
> Мы одинаково учитываем положительные и критические ответы.

Благодарность видна каждому участнику и не зависит от prize eligibility. Общий
итог (`проверено 11 разделов`, `исправлено 4 проблемы`) показывается только с
проверяемым evidence; causal claim `благодаря вашему отзыву` без связи запрещён.

### 7.2. Partner attribution

Согласованный компонент содержит approved logo, текстовое имя, корректный alt и
ссылку на согласованную страницу. Логотип не заменяет имя, не является
endorsement всего сервиса и не берётся из поисковой выдачи без права
использования. Написание `Акт.Опус` / `Акт-Опус` и роль партнёра остаются
approval gate.

### 7.3. Последнее owner decision и pending copy

Один возможный приз означает **ровно два билета в театр**. Не обещаются `любой
спектакль`, конкретная дата, места или способ бронирования, пока они не входят в
опубликованные правила.

> **Правила готовятся.** Этот прототип показывает коллекцию и исследовательские
> действия. Сейчас они не начисляют конкурсные баллы и не создают право на два
> билета.

Прежняя модель равновесной заявки superseded. Принятый product design:
collection coverage — первый rank key, bounded participation `0…40` — второй;
NPS учитывается только как факт ответа, likes/dislikes симметричны, текст не
оценивается по sentiment/длине. Score, caps, tie draw, anti-abuse, accessibility,
legal gates и site-wide placement matrix имеют одну канонику:
[easter-egg-program.md](easter-egg-program.md).

Prize states сохраняются: `hidden → pending_approval → applications_open →
applications_closed → selection_pending → recipient_notified → fulfilled`, с
отдельным `cancelled`. До закрытия gates доступны только первые два состояния,
leaderboard помечен `demo`, а localStorage не считается membership/result
ledger.

## 8. Acceptance checklist для prototype handoff

- [ ] На каждом экране явно написано `Прототип`, где поведение ещё не production.
- [ ] Один подарок везде означает ровно два билета в театр, а не два приза.
- [ ] Партнёр назван и атрибутирован логотипом; spelling и asset имеют approval gate.
- [ ] Collection coverage является первым rank key; bounded participation — вторым.
- [ ] NPS value, sentiment, длина текста, share/invite/purchase/spam не дают advantage.
- [ ] Без legal/partner/privacy gate prize state не выходит из `hidden/pending_approval`, leaderboard остаётся `demo`.
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
