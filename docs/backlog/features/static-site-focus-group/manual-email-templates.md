# Manual email templates и send checklist

> **Статус:** шаблоны и ручной operator SOP; не email implementation.
> **Требование:** R02; end-state тексты согласованы с R09.
> **Граница:** каждое focus programme system/update письмо вручную готовит,
> проверяет и вручную отправляет уполномоченный оператор. Здесь нет генератора,
> outbox, worker, cron, scheduled send, mail-merge, API-вызова или bulk
> automation.

## 1. Что входит и не входит

В этот prototype входят ручные письма:

- подтверждение участия после отдельного подтверждения identity;
- статус/пауза программы;
- weekly update;
- статус собственного обращения;
- напоминание о плановом окончании;
- завершение по времени или решению оператора.

Не входят:

- отправка OTP/magic link и любая реализация Auth;
- автоматически персонализированный weekly digest;
- recommendation/marketing messages;
- автоматический призовой outcome;
- автоматические персональные подборки: это отдельный recommendation stream
  со своим explicit opt-in/unsubscribe, а не focus-system/update mail;
- письмо «вы победили» до отдельных правил, legal/privacy/partner approval;
- обещание даты, спектакля, мест или выдачи подарка.

Шаблон — исходник для ручного письма, а не разрешение отправить его всей базе.
Оператор каждый раз заново проверяет адресата, purpose, membership state,
suppression и содержание.

Отдельный recommendation opt-in не разрешает отправить focus update, а
focus-research purpose не разрешает отправить персональную подборку. В этой
lane recommendation sender отсутствует. При insufficient signals допустим
static on-site fallback; вручную отправленная общая подборка потребовала бы
отдельного recommendation purpose и отдельного шаблона, которого здесь нет.

## 2. Roles и минимальные данные

| Role | Ответственность |
|---|---|
| Content owner | заполняет только подтверждённые факты и evidence links |
| Programme operator | проверяет programme/member/end state |
| Privacy/consent checker | проверяет exact purpose и suppression перед send |
| Sender | вручную отправляет одно письмо одному адресату |
| Second reviewer | одобряет weekly/end/prize-adjacent формулировки |

Допустимые placeholders:

`{{first_name_or_neutral}}`, `{{programme_name}}`, `{{period}}`,
`{{end_date}}`, `{{verified_change}}`, `{{feedback_status}}`,
`{{support_address}}`, `{{leave_link}}`, `{{settings_link}}`,
`{{ordinary_site_link}}`, `{{template_version}}`.

Не подставлять raw search queries, список просмотренных событий, inferred
interests, чужие отзывы, телефон, invite token, auth link, полный recipient
profile или непроверенное утверждение «по вашему отзыву исправлено».

`tester@kenigevents.ru` можно ставить в From/Reply-To/body только после
подтверждения mailbox/alias, MX/routing, владельца, retention, response SLO и
test send/reply. До этого шаблон сохраняет `{{support_address}}`; нельзя
молча подменять его на `info@kenigevents.ru`.

## 3. Manual send state machine

```text
draft
  └─ content facts verified ─> content_reviewed
content_reviewed
  └─ recipient + purpose + suppression checked ─> recipient_checked
recipient_checked
  └─ second reviewer approval where required ─> approved
approved
  ├─ operator manually sends individual message ─> manually_sent
  └─ state/consent changed before send ─> suppressed
manually_sent
  ├─ evidence recorded ─> complete
  └─ provider reports failure ─> failed
failed
  ├─ recipient/state rechecked + one manual retry ─> manually_sent
  └─ operator stops ─> cancelled
```

Нельзя считать `approved` отправленным. Нельзя автоматически retry. Изменение
consent, membership, programme state или suppression между approval и click
`Send` возвращает письмо в `recipient_checked`.

## 4. Ручной SOP

### 4.1. Подготовка

1. Выбрать **один** шаблон и зафиксировать `template_version`.
2. Вручную заполнить placeholders из operator-approved источника.
3. Для метрик и исправлений приложить внутренний evidence link; удалить claim,
   если подтверждения нет.
4. Проверить, что subject и body описывают фактическое состояние, а не желаемое.
5. Не добавлять prize copy, кроме отдельно одобренной версии после всех gates.
6. Отправить test copy на внутренний адрес и проверить mobile/desktop, plain
   text, ссылки, имя отправителя и Reply-To.

### 4.2. Recipient check непосредственно перед send

- [ ] Адрес принадлежит нужному verified identity.
- [ ] Member/programme state соответствует шаблону.
- [ ] Для weekly/update есть точный действующий focus research purpose.
- [ ] Recommendation opt-in не принят за focus research purpose.
- [ ] Адресат не withdrawn, unsubscribed, suppressed или bounced.
- [ ] Для end/exit service notice есть утверждённое основание и это не скрытый marketing.
- [ ] `{{support_address}}` реально принимает ответы.
- [ ] В `To` один адрес; `CC/BCC` не раскрывает cohort.
- [ ] Нет raw history, inferred profile, invite/auth token и чужих данных.
- [ ] Ссылки ведут на exact approved host и не содержат recipient secret.
- [ ] Текущее время, timezone и end reason проверены.

Если хотя бы один пункт не подтверждён, письмо получает `suppressed` или
возвращается в `draft`; предположение не считается проверкой.

### 4.3. Отправка

1. Оператор открывает approved mailbox UI.
2. Создаёт **индивидуальное** письмо одному получателю.
3. Вставляет проверенные subject/body без mail merge.
4. Ещё раз сравнивает адрес и programme/member state.
5. Нажимает `Send` вручную.
6. Записывает минимальное evidence: internal member id, template/version,
   purpose, sender operator, UTC timestamp, provider message/status id если он
   видим, result. Email address и тело не копируются в общий operational log.

Даже для 200 человек цикл выполняется по одному адресату. Скрипт, CSV bulk
import, delayed/scheduled send, API или «временный» automation не допускаются
этой спецификацией.

### 4.4. После send

- Не считать отсутствие bounce доказательством доставки или прочтения.
- Bounce пометить вручную и не retry до проверки адреса/state.
- Reply передать владельцу support SLO без копирования raw текста в общий digest.
- Ошибочный адресат, утечка cohort list или отправка после withdrawal —
  stop condition: остановить оставшиеся письма, сохранить evidence и
  эскалировать по incident/privacy procedure.
- Для повторной отправки снова пройти весь recipient checklist.

## 5. Templates

Все subjects и bodies ниже — русский plain-text baseline. `{{first_name_or_neutral}}`
может быть заменён нейтральным `Здравствуйте`, если имя не было явно предоставлено.

### T01 — участие подтверждено

**Назначение:** одно ручное service message после фактической activation.

**Subject:** `Вы участвуете в фокус-группе KenigEvents`

```text
{{first_name_or_neutral}}!

Ваше участие в программе «{{programme_name}}» подтверждено.
Период: {{period}}.

В режиме тестера можно сообщать об ошибках и отмечать, насколько страницы
помогают выбрать событие. Количество, длина и тон отзывов, приглашения друзей
и публикации не дают преимуществ и не меняют шанс на возможный подарок.

Настройки участия: {{settings_link}}
Выйти из фокус-группы: {{leave_link}}
Вопросы: {{support_address}}

Это исследовательское сообщение, а не рекламная подписка.
```

### T02 — weekly update

**Назначение:** одно вручную подготовленное обновление active member; не
автоматически персонализированный digest.

**Subject:** `Что изменилось за неделю в фокус-группе KenigEvents`

```text
{{first_name_or_neutral}}!

Короткое обновление за неделю:

— Проверено командой: {{verified_change}}
— Статус ваших обращений: {{feedback_status}}
— На следующей неделе проверяем: {{next_focus}}

Мы пишем «исправлено» только для подтверждённого изменения. Если ваш отзыв не
связан с ним доказуемо, мы не приписываем изменение лично вам.

Открыть сайт: {{ordinary_site_link}}
Настройки участия: {{settings_link}}
Выйти из фокус-группы: {{leave_link}}
Вопросы: {{support_address}}
```

Если нет compact verified member facts, строка о собственных обращениях
удаляется. Нельзя заменять её raw visit/search/event history.

### T03 — статус обращения

**Назначение:** ручной ответ по конкретному обращению.

**Subject:** `Статус вашего сообщения для KenigEvents`

```text
{{first_name_or_neutral}}!

Статус вашего сообщения: {{feedback_status}}.

Что подтверждено:
{{verified_change}}

Мы не меняем сведения о событии автоматически по одному сообщению: факты
сначала проверяет оператор.

Вопросы: {{support_address}}
```

Допустимые статусы: `получено`, `проверяем`, `исправлено`, `отложено`,
`не подтверждено`. Для `исправлено` обязателен internal evidence link при review.

### T04 — программа временно приостановлена

**Subject:** `Фокус-группа KenigEvents временно приостановлена`

```text
{{first_name_or_neutral}}!

Мы временно приостановили исследовательский режим с {{pause_time}}.
Новые приглашения и feedback в режиме тестера сейчас не принимаются.
Обычные страницы сайта и ваши сохранения остаются доступны.

Мы отдельно сообщим вручную, если программу возобновят или завершат.
Вопросы: {{support_address}}
```

Пауза не продлевает срок и не возобновляется автоматически без operator decision.

### T05 — напоминание о плановом окончании

**Subject:** `Фокус-группа KenigEvents завершится {{end_date}}`

```text
{{first_name_or_neutral}}!

Исследовательский период завершится {{end_date}}.
После этого режим тестера, новые приглашения и еженедельные исследовательские
сообщения выключатся.

Ваш обычный аккаунт и явно сохранённые события не удаляются. Выбранные темы и
локальные подсказки «Для меня» могут продолжить работать в этом браузере; это
не cross-device профиль.

Настройки: {{settings_link}}
Вопросы: {{support_address}}
```

### T06 — automatic end

**Subject:** `Спасибо за участие в фокус-группе KenigEvents`

```text
{{first_name_or_neutral}}!

Исследовательский период завершился {{end_date}}.
Спасибо, что проверяли сайт вместе с нами.

Режим тестера, новые приглашения и еженедельные исследовательские сообщения
выключены. Ваш обычный аккаунт, явно сохранённые события и обычные настройки
не удалены.

Количество отзывов, оценок, приглашений и публикаций не давало преимуществ.

Открыть обычный сайт: {{ordinary_site_link}}
Настроить «Для меня»: {{personalization_link}}
Вопросы: {{support_address}}
```

### T07 — operator early end/cancellation

**Subject:** `Изменение программы фокус-группы KenigEvents`

```text
{{first_name_or_neutral}}!

Команда {{operator_end_verb}} исследовательский период {{operator_end_date}}.
Причина, которую можно сообщить участникам: {{approved_public_reason}}

Новые отзывы и приглашения в режиме тестера больше не принимаются.
Обычный сайт, ваш аккаунт и явно сохранённые события остаются доступны.

{{conditional_prize_status}}

Открыть обычный сайт: {{ordinary_site_link}}
Вопросы: {{support_address}}
```

`{{operator_end_verb}}` принимает только approved wording `завершила раньше`
или `остановила`. `{{conditional_prize_status}}` по умолчанию:

> Возможный подарок не считается объявленным или обещанным, если его отдельные
> правила не были опубликованы и приняты.

### T08 — exit confirmation

**Subject:** `Вы вышли из фокус-группы KenigEvents`

```text
{{first_name_or_neutral}}!

Ваше участие в исследовательской программе завершено.
Новые tester prompts, приглашения и weekly research messages выключены.

Обычный аккаунт и явно сохранённые события не удалены. Маркетинговая подписка
не создаётся и не изменяется этим действием.

Открыть обычный сайт: {{ordinary_site_link}}
Вопросы: {{support_address}}
```

Отправлять только как разрешённое подтверждение действия; не использовать как
повод вернуть человека в программу.

## 6. Partner/prize wording in email

В обычных weekly/system письмах prize block отсутствует. После отдельного
legal/privacy/partner/anti-abuse release gate второй reviewer может разрешить
следующий неизменяемый informational block:

```text
Партнёр благодарности — «Акт-Опус».
Один подарок — одна пара: два приглашения на любой спектакль партнёра,
доступный по опубликованным правилам. Одна допущенная заявка имеет тот же вес,
что каждая другая. Отзывы, оценки, приглашения друзей и публикации не
увеличивают шанс.
```

Email-клиентская версия логотипа необязательна: blocked images не должны убирать
текстовую атрибуцию. Если approved logo вставляется, нужен alt
`Логотип партнёра «Акт-Опус»`, согласованный asset и neutral placement.

Шаблон уведомления победителю намеренно отсутствует: до утверждения правил
невозможно честно определить eligibility, selection evidence, alternate,
сроки/наличие мест и порядок получения. Оператор не импровизирует такой текст.

## 7. Final operator checklist

### Content

- [ ] Template id/version записан.
- [ ] Subject соответствует фактическому state/reason.
- [ ] Все placeholders заполнены или удалены.
- [ ] Claims `исправлено/передано/завершено` имеют evidence.
- [ ] Нет marketing, raw history или inferred interests.
- [ ] Prize/partner block либо отсутствует, либо имеет отдельный approval.
- [ ] Формулировка пары — ровно `два приглашения`, без двусмысленности.
- [ ] Нет feedback/share/invite multiplier.

### Recipient and consent

- [ ] Один verified recipient.
- [ ] Current membership/programme state повторно проверен.
- [ ] Exact focus message purpose действует.
- [ ] Suppression/unsubscribe/bounce повторно проверены.
- [ ] Exit/end rules не использованы для скрытого reactivation.

### Transport and evidence

- [ ] From/Reply-To реально provisioned и протестирован.
- [ ] Test copy просмотрена.
- [ ] Одно индивидуальное письмо, без cohort в CC/BCC.
- [ ] Send выполнен оператором вручную.
- [ ] Minimal evidence записан без копирования email/body.
- [ ] Bounce/retry также проходит ручную повторную проверку.

## 8. Acceptance

R02 выполнено для prototype handoff, когда продукт и оператор могут подготовить
одиночное письмо по шаблону и пройти checklist, но в repository/production не
добавлены mail code, scheduled jobs, provider calls или автоматическая
персонализация. Факт наличия шаблона не означает consent, delivery или release
readiness.
