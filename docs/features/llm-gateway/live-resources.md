# Shared Live resources — интеграционная граница, не включённый UI

`google_ai/live_resources.py::run_live_search` делегирует существующему общему `ai-resource-control` с consumer `kenigevents` и серверной привязкой авторизованной сессии. При отсутствии пакета — явный `RESOURCE_PACKAGE_MISSING`; никакой одиночный ключ не используется. Private пакет устанавливается авторизованным deployment-контуром, не копируется в этот public репозиторий. Обычные GoogleAIClient, Edge event-search quota и Kaggle gateways остаются в прежнем canonical reserve/mark/finalize контракте.

## Текущие границы

Это новый backend integration API. Он **ещё не подключён к браузерному голосовому поиску**: новый Live route, UI микрофона и продуктовая приёмка не входят в выполненный checkpoint. Не выдавать наличие модуля за завершённое внедрение на kenigevents.ru.

Совместимый shared transport resource_guard ещё не опубликован: попытка его изменения была заблокирована и не повторялась. Общий SDK отказывает старому транспорту до резерва/Google. Новые Live-квоты в Supabase не включались; точные лимиты для каждого provider scope нужно подтвердить отдельно, не заимствовать Flash-Lite значения. Production environment не менялся, Fly/static-site deploy не выполнялся.

## Продуктовая политика из owner review

Голосовой поиск дополняет текущую выдачу/карточки, а не заменяет экран чатом. После завершённого ответа примерно 15 секунд на продолжение; при отсутствии речи продукт останавливает разговор. Refresh/уход со страницы — Stop; пропажа клиента дополнительно закрывается server liveness watchdog. Это продуктовый timeout, не TTL базы: истечение lease остаётся страховкой после краха процесса. Нельзя считать каждый PCM-фрагмент доказательством новой человеческой реплики; используйте уже существующий VAD/turn-boundary, не стройте второй VAD.

Источник владельца: voice-20260926-180531-a07ef741 в idea-hub (private provenance хранится в общем проектном ADR). Не публикуются транскрипты, user identifiers, Supabase credentials, Google keys или fingerprints.

## Проверка

Offline baseline provider-path audit завершился с exit 0 до добавления модуля. Новый модуль не импортирует Google SDK, не содержит provider URL, не обходит единый controller. Повторить audit и unit test:

```sh
python scripts/inspect/audit_google_ai_provider_paths.py
python -m unittest discover -s tests -p test_shared_live_resources.py -v
```

Следующее: доверенный session handler с auth/rate limiting, этот module как единственный ресурсный вход, готовый shared host с client watchdog и pinned private release, затем реальная четырёхпроектная конкурентная приёмка. Модуль сам по себе не авторизует пользователя: opaque session ID выдаёт backend после проверки доступа.
