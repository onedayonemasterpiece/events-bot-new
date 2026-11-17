# NFC → сообщество ВКонтакте (klgdevents)

**Адрес для NFC:** https://kenigevents.ru/vk.html  
**Адрес сообщества:** https://vk.ru/klgdevents  
**Нативная схема:** vk://vk.com/klgdevents  
**Android Intent:** intent://vk.com/klgdevents#Intent;scheme=vk;package=com.vkontakte.android;S.browser_fallback_url=https%3A%2F%2Fvk.ru%2Fklgdevents;end

## Назначение и поведение

Отдельный статический utility-адрес для общедоступной NFC-метки. В адресе, HTML, нативном deeplink и запасной ссылке фиксировано одно сообщество — klgdevents. Веб-логин VK не используется для перехода в приложение: клиент ВК имеет собственную авторизацию.

На мобильном HTML пробует открыть нативную схему; если браузер не разрешил автоматический переход, через 2700 мс показывает явную инструкцию, **не уводя пользователя** на сайт ВК и не скрывая кнопку правильного входа в приложение. Android Intent при нажатии содержит browser fallback на то же сообщество; на iOS предусмотрена отдельная веб-кнопка. Всегда видны две кнопки: «Открыть сообщество в приложении» (Android — Intent с browser fallback; iOS — vk:// схема) и «Открыть сообщество в браузере». Для отключённого JS обе ссылки есть непосредственно в HTML. На desktop автоматический переход не выполняется.

Это **best effort**, не гарантия: Chrome блокирует некоторые запуски приложения без жеста, iOS может потребовать подтверждение, а разные клиенты VK по-разному обрабатывают URI. Фактическое попадание именно в сообщество надо проверить на реальных Android/iOS с установленным VK; отсутствие установленного VK должно приводить на веб-страницу сообщества. Если VK откроется на главной — собирать ОС/версию VK и не считать нативную приёмку пройденной.

## Код и публикация

- site/public/vk.html — автономный HTML/CSS/JS, без зависимостей, query-ввода, tracking, cookies или вмешательства в страницы афиши.
- site/tests/nfc-vk-landing.test.mjs — L0 проверки Android/iOS/desktop, фиксированной цели, fallback и отсутствия веб-перехода после ухода в приложение.
- .github/workflows/nfc-vk-link-release.yml — отдельный ручной production deployment **только vk.html** в существующий website bucket. Сверка exact SHA main, резервная копия прежнего объекта, stage/readback, публичный byte-for-byte smoke и rollback при ошибке. Главный index.html, DNS, афиша и approved prelaunch-компоненты не меняются.

Запуск L0: node --test site/tests/nfc-vk-landing.test.mjs.

Публикация: GitHub Actions → NFC VK link production release на main с expected_sha равным текущему полному SHA origin/main. Приёмка только после terminal PASS с HTTP 200 и совпадением SHA256 HTML, затем ручная NFC-проверка целевого экрана приложения на Android/iPhone.

Страница является узким техническим переходником, не новым компонентом сайта; утверждённые общие компоненты дизайн-системы не модифицируются.
