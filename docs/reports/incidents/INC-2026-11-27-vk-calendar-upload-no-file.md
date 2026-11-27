# VK calendar upload без файла, 27 ноября 2026

Статус: открыт. Исполнитель: root KenigEvents. Registry context:
`inc_af96f6510a55a8ccc1d713dc`. Причина внешнего ответа без файла не установлена.

## Наблюдения

Calendar job172383 / event11563 встретил upload timeout11:17:17 UTC,
no-file11:18:14 и timeout11:21:01. Штатный повтор создал ровно один
calendar16294 в11:31:41. Другой calendar172386 завершился с первой попытки.

После выпуска PR1114 calendar171576 / event11515 разрешил stored16159 в
public16180, затем16:09:16 получил `VK calendar upload did not return a file`.
HTTP status не вызвал исключение, JSON разобран, поле file отсутствует либо
пустое; до docs.save и wall.createComment попытка не дошла. Attempts15,
следующая обычная попытка17:09:16 UTC. Related171577 создал comment16297
один раз16:10:04; frozen11177/9628/8144 совпадают с телом публичного поста.

Это не доказательство общей причины timeout/no-file, flood/CAPTCHA,
неправильного MIME, имени файла или токена. Исходная форма ответа не сохранена.

## Минимальное изменение

В существующей no-file ветке добавлено одно предупреждение: event/post ID,
HTTP status, размер ICS в байтах, количество полей и фиксированные признаки
формы file/error/response. Значения полей, имена произвольных полей, URL,
filename, ticket, token и тело ICS не выводятся. Исключение и штатный retry,
Setting/document receipt, guid и отправка комментария не меняются.

Runtime delta: +10 строк. Новых состояний, повторов, transport, очередей,
контроллеров или ручных шагов в приложении нет. Принятие вложенного file
не добавляется: его наличие только диагностический признак.

## Проверка

Различающий offline контроль использует синтетические пустой объект,
пустой file и error-envelope с вложенным ticket. До изменения три проверки
падают из-за отсутствия warning. После изменения должны различаться только
безопасные flags; docs.save/createComment и Setting отсутствуют при no-file.
Обычный следующий вызов с валидным ticket создаёт документ/комментарий один
раз, повторный вызов ничего не отправляет. Секретные sentinel значения в
warning запрещены. Полный regression в изолированном текущем image без сети, production DB
и env mounts:116 PASS /75.53s. Before:3 FAIL на отсутствующем warning.
Независимый simplicity/privacy guard: SOURCE PASS, runtime ровно+10.

Развёртывание этой диагностики не запускалось. Приёмка: обычная попытка
календаря доставляет один корректный ICS либо сохраняет безопасную форму
повторного no-file для дальнейшего исправления причины. Наличие диагностики
не означает восстановления доставки или соблюдения задержки3–20 минут.

Retained evidence: `/home/dev/artifacts/events-bot-new/20261112T111614Z-similar-events-check/`:
`calendar-upload-boundary-source-result-20261127.json`,
`event11515-public-1612-20261127.jsonl`,
`event11515-errors-after1114-20261127.json`,
`calendar11515-log-allowlist-20261127.json`,
`vk16289-calendar-upload-diagnosis-20261127.md`.
