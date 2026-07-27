# Коллекция артефактов и допуск к заявке

> **Статус:** product/rules contract. Реального розыгрыша, application endpoint и
> winner selection пока нет.

## Определение

**Коллекция** — versioned ограниченный набор `N` артефактов, одновременно
доступных одной аудитории в опубликованном collection window.

Она фиксирует:

- `collection_id`, public name и rules version;
- immutable ordered membership;
- start/close/application deadlines и timezone;
- threshold percent и вычисленный integer `required_count`;
- eligibility/application/prize binding;
- одинаковую hint cadence;
- fallback/extension/kill rules.

Первая proposed collection: `8` объектов, threshold `60%`,
`required_count = ceil(8 × 0.60) = 5`.

## Почему не 100%

Threshold ниже полного набора:

- оставляет пространство для одной-двух действительно сложных находок;
- не превращает технический/safety сбой одного placement в полный запрет;
- уменьшает барьер для late joiner и accessibility paths;
- не вознаграждает скорость или obsessive completion.

Публичный интерфейс всегда показывает и процент, и точное число:
**«Нужно 5 из 8»**. Формула не меняется в ходе кампании.

## State machine

```text
DRAFT
→ SCHEDULED
→ COLLECTING
→ APPLICATION_GRACE
→ DRAW_LOCKED
→ CLAIM
→ CLOSED
→ ARCHIVED
```

Исключение:

```text
COLLECTING | APPLICATION_GRACE
→ SUSPENDED
→ resume with extension | CLOSED
```

Все `N` объектов первой коллекции доступны одновременно с начала до close. Нет
rotation, personalized release schedule или item-level expiry.

## Unlock и заявка

```text
distinct valid finds >= required_count
→ APPLICATION_ELIGIBLE
→ explicit submit + rules/privacy acknowledgement
→ APPLICATION_SUBMITTED
```

- Threshold **не** подаёт заявку автоматически.
- Первая находка и сбор доступны без обязательного email/login; контакт и
  verification запрашиваются только при добровольной подаче заявки.
- Одна действительная заявка на person/contact/collection после approved
  identity and anti-abuse design.
- `8/8`, скорость, отсутствие hints, покупки, event clicks, likes и shares не
  дают дополнительного entry или веса.
- Все действительные заявки имеют одинаковый вес.
- Пользователь может собрать остаток коллекции после submit ради истории, но не
  ради повышения odds.

До production точные organizer, prize, quantity, eligibility, geography/age,
selection, alternates, claim, cancellation, privacy/retention и appeal rules
проходят отдельное business/legal acceptance. UI не делает юридических выводов.

## Fairness и отказоустойчивость

1. Assignment и find receipts идемпотентны; reload не создаёт второй find.
2. Все обязательные placements имеют touch/keyboard/screen-reader equivalent.
3. Если страница исчезла, тот же collectible ID переезжает только в заранее
   проверенный equivalent placement; старый receipt сохраняется.
4. Если equivalent недоступен, применяется опубликованный universal credit или
   extension для всей affected audience, а не персональный make-good.
5. Outage/kill не сокращает окно незаметно; extension публикуется и audit-ится.
6. Shared IP/device, VPN или assistive tech не являются автоматической причиной
   дисквалификации.
7. Не используется invasive fingerprinting.
8. Active placement coordinates и live negative counts не публикуются партнёрам.

## Передача и social share

- QR-transfer из Telegram-идей не входит в первую reward-enabled коллекцию:
  transferable ownership ломает смысл threshold и усложняет one-person-one-entry.
- Share остаётся добровольным и не влияет на odds.
- Отдельная non-prize transferable collection возможна позже с independent
  ownership/anti-abuse contract.

## Release blockers

- approved first `registry_version` и source/IP/freshness evidence;
- durable server ledger вместо одного `localStorage`;
- application endpoint, verification, consent и deletion;
- immutable eligible-applications snapshot и auditable selection;
- published rules, organizer and prize evidence;
- keyboard/screen-reader/mobile E2E;
- outage/extension/kill rehearsal;
- owner acceptance exact branch/SHA.
