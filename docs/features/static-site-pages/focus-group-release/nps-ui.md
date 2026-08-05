# UI оценок страниц и общего NPS

> **Статус:** current auth-gated contract.
> **Родитель:** [`../focus-group.md`](../focus-group.md).

## До авторизации

Feedback block остаётся видимым, чтобы участник понимал возможность и причину
недоступности отправки.

```text
Lab · Оценка страницы
[disabled 0–10 controls]
[disabled Написать подробнее]
[disabled Добавить скриншот]

Войдите по email или через Яндекс, чтобы отправить отзыв.
[Войти]
```

Требования:

- `disabled`/`aria-disabled` соответствуют control type;
- нельзя имитировать disabled только цветом;
- draft может временно жить локально, но server write до auth запрещён;
- auth CTA сохраняет return route/anchor;
- pre-auth submit/network count = 0.

## После авторизации

### Page score

Ключ:

```text
account_subject + page_family + page_revision
```

Состояния:

- `unanswered`;
- `answered_current`;
- `revision_changed`;
- `pending`;
- `committed`;
- `failed_retryable`;
- `failed_terminal`.

### Service NPS

Отдельная сущность:

```text
account_subject + service_revision
```

Page score и service NPS не смешиваются. NPS не подменяет диагностику конкретной
страницы.

### Text / screenshot / diagnostics

Один пользовательский блок может инициировать три независимых component
receipts:

```text
feedback_text
feedback_screenshot
feedback_diagnostics
```

Успех одного компонента не маскирует failure другого.

## Revision change

При новой содержательной revision:

```text
Вы уже оценивали эту страницу — ранее: 7.
Страница обновилась. Оцените новую версию.
```

Старая row остаётся в истории; новая не создаётся автоматически.
