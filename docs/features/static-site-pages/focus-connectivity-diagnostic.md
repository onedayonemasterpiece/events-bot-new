# Диагностическая страница соединения v2

> **Статус:** implementation contract.
> **Публичный маршрут для участников:** `/fokus-gruppa/diagnostika-ustoychivost/`.
> **Индексирование:** запрещено (`noindex,nofollow,noarchive,nosnippet`).
> **Связанные документы:**
> [Yandex dependency resilience](../../operations/yandex-dependency-resilience.md),
> [autotest strategy](../../operations/static-site-autotest-strategy.md),
> [release gates](release-autotest-gates.md),
> [focus group](focus-group.md).

## 1. Решение

Новая ссылка не создаётся. Участникам передаётся прежний стабильный URL, но сама
страница получает **версию 2** интерфейса, классификации и receipt. Это сохраняет
ранее разосланные ссылки и исключает две расходящиеся диагностические страницы.

Страница отвечает только на три вопроса:

1. доступны ли с этого устройства основные функции входа и чтения данных;
2. какой маршрут реально выбрал resilient transport для Auth и Data;
3. какие отдельные Yandex-capabilities не ответили в этот момент.

Она не объявляет глобальное состояние Supabase или Yandex Cloud и не подтверждает
доставку конкретного NPS, текста, скриншота, письма или иного действия.

## 2. Наблюдение, из-за которого введён v2

На физическом iPhone участника одновременно наблюдалось:

- direct Supabase Auth — доступен;
- direct Supabase Data — доступен;
- Yandex Supabase relay Auth/Data — network error;
- resilient Auth/Data — доступны через direct;
- служебный YDB control — network error.

Корректный итог:

```text
CORE_AVAILABLE_DIRECT_YANDEX_DEGRADED
```

Пользовательский текст:

> Основные функции доступны напрямую. Резервный маршрут через Yandex и
> служебный канал YDB с этого устройства сейчас не отвечают. Это не является
> доказательством глобального сбоя Yandex Cloud.

## 3. Что страница проверяет

### 3.1 Основные функции

| Проверка | Метод | Назначение |
|---|---|---|
| Resilient Auth | read-only Auth health через `ResilientSupabaseTransport` | доказать, что вход имеет рабочий маршрут |
| Resilient Data | read-only bounded Data API request через тот же transport | доказать, что данные имеют рабочий маршрут |

Ответ transport содержит санитарный заголовок `x-ke-transport-route`. Страница
показывает фактически использованный `direct` или `relay`, а не только
предварительное решение route selector.

### 3.2 Независимые маршруты

| Проверка | Значение |
|---|---|
| Direct Auth | доступ к Supabase Auth без Yandex relay |
| Direct Data | доступ к Data API без Yandex relay |
| Relay Auth | доступ к Supabase Auth через Yandex API Gateway relay |
| Relay Data | доступ к Data API через Yandex API Gateway relay |

Ошибки одного маршрута не должны ухудшать health другого. Результат relay не
используется как общий индикатор всего Yandex Cloud.

### 3.3 Служебный канал

`YDB control` проверяется отдельно и подписывается как служебная диагностика.
Его отказ:

- не делает direct или relay route неработоспособным;
- не является подтверждением потери основного действия;
- не позволяет показывать пользователю ложный общий статус «сайт не работает»;
- является отдельным evidence для Yandex sidecar outage.

## 4. Что страница намеренно не делает

- не отправляет OTP и письма;
- не создаёт пользователя;
- не меняет профиль, лайки, скрытия, NPS или feedback;
- не загружает скриншот;
- не пишет тестовую запись в primary store;
- не проверяет Postbox, OAuth, Object Storage или inbound pipeline;
- не делает вывод о глобальной доступности провайдера;
- не говорит, что конкретное пользовательское действие доставлено.

Durable write, outbox, idempotency и component receipts проверяются отдельными
автотестами и на экранах самих действий.

## 5. Структура интерфейса

### 5.1 Вводная часть

Обязательные сообщения:

- это read-only проверка;
- писем и изменений не будет;
- результат относится к текущему устройству и сети;
- результат не равен глобальному status page провайдеров.

### 5.2 Основные функции

Первыми показываются две карточки:

- `Вход в аккаунт`;
- `Данные и персонализация`.

Именно они определяют `canContinue`. Технические direct/relay/YDB карточки не
должны визуально вытеснять главный продуктовый ответ.

### 5.3 Технические маршруты

Следующим блоком показываются четыре отдельные карточки direct/relay. В названиях
обязательно присутствуют `Supabase напрямую` и `Резерв через Yandex`; названия
`Второй маршрут` и `Контрольный канал` запрещены как двусмысленные.

### 5.4 Служебный канал

YDB располагается отдельным блоком с пояснением, что он не является владельцем
входа, профиля или обратной связи.

### 5.5 Итог

Итоговый блок содержит:

- стабильный machine code;
- понятный headline;
- объяснение без глобальных выводов;
- фактические маршруты Auth/Data;
- инструкцию, можно ли продолжать пользоваться сайтом;
- предупреждение не повторять многократно уже подтверждённое действие;
- уточнение, что эта страница не подтверждает отдельный feedback.

## 6. Коды результата

| Код | Смысл | `canContinue` |
|---|---|---:|
| `CORE_AVAILABLE_BOTH` | direct, relay, resilient core и YDB доступны | да |
| `CORE_AVAILABLE_YDB_DEGRADED` | core и оба transport route доступны, YDB не ответил | да |
| `CORE_AVAILABLE_DIRECT_YANDEX_DEGRADED` | core работает через direct, relay недоступен/частичен | да |
| `CORE_AVAILABLE_RELAY_DIRECT_DEGRADED` | core работает через relay, direct недоступен/частичен | да |
| `CORE_AVAILABLE_ROUTE_DEGRADED` | core работает, отдельный route доступен частично | да |
| `CORE_AVAILABLE_DIAGNOSTIC_INCONSISTENT` | core работает, независимые probes расходятся | да, с отправкой receipt |
| `CORE_PARTIALLY_AVAILABLE` | Auth или Data не подтвердились | нет |
| `CORE_UNAVAILABLE` | resilient Auth и Data не подтвердились | нет |
| `DEVICE_OFFLINE` | browser сообщает offline | нет |
| `CONFIGURATION_INCOMPLETE` | обязательная public config отсутствует | нет; дефект сборки |

## 7. Коммуникационный контракт

### 7.1 Когда core доступен

Страница говорит:

> Можно продолжать пользоваться сайтом. Если конкретное действие уже
> подтверждено сайтом, повторять его не нужно. Эта проверка не подтверждает
> доставку отдельного отзыва.

### 7.2 Когда core не подтверждён

Страница говорит:

> Не нажимайте отправку многократно. Скопируйте строку результата, попробуйте
> позднее или в другой сети и сообщите команде фокус-группы.

Нельзя писать `данные потеряны`, если отсутствует component receipt конкретной
операции. Нельзя писать `всё отправлено`, если диагностировалась только сеть.

## 8. Receipt v2

Короткая строка начинается с `KE5` и содержит только технические данные:

```text
KE5 ID=<id> AT=<iso> CODE=<code>
DA=<state> DD=<state> RA=<state> RD=<state>
FA=<state@route> FD=<state@route> YC=<state>
PATHA=D|R|N PATHD=D|R|N MODE=WEB|APP QUALITY=<type>
ONLINE=0|1 PWA=0|1
```

Запрещены:

- email;
- OTP;
- cookie/JWT;
- publishable/service keys;
- user agent;
- raw response body;
- bearer preview URL.

## 9. Автоматизированная проверка

### 9.1 Unit

Обязательные fixtures:

1. полевой случай `direct OK / relay NET / YDB NET / framework direct OK`;
2. reciprocal `direct down / relay OK`;
3. только YDB down при обоих рабочих routes;
4. оба client routes down;
5. browser offline;
6. incomplete public configuration;
7. capture `x-ke-transport-route`;
8. receipt redaction.

### 9.2 Static surface contract

Проверяет:

- v2 title/copy;
- явные direct/relay/YDB labels;
- отсутствие старых двусмысленных labels;
- импорт `diagnoseConnectivity`;
- отдельные Auth/Data selections;
- receipt prefix `KE5` через helper;
- отсутствие OTP/write calls.

### 9.3 Deployed browser acceptance

На immutable HTTPS candidate:

- page loads without console/page errors;
- field fixture can be deterministically reproduced with route interception;
- code and user text match fixture;
- actual route appears in core cards;
- copy button returns one sanitized line;
- layout works at 320/390/430 px and desktop;
- screenshot contains full verdict and code.

### 9.4 Mobile sample

Android Chrome and Mobile Safari verify:

- button is tappable;
- Safari/Chrome bars do not cover verdict/copy action after scroll;
- result can be copied;
- no native keyboard is required until the readonly receipt is explicitly focused;
- page remains usable with relay failure and YDB failure.

## 10. Release boundary

The page code and deterministic contracts may be merged after ordinary CI.
Перед отправкой обновлённого URL всей фокус-группе нужен один deployed browser
PASS и по одному representative Android/iOS smoke без писем и записей.

Это не заменяет release gates для:

- both client routes down;
- shared Supabase upstream outage;
- YDB projection outbox replay;
- feedback component receipts;
- Postbox/OAuth/Object Storage/inbound degradation.
