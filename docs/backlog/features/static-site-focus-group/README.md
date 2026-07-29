# Фокус-группа статического сайта — продуктовый прототип

> **Статус:** page/product prototype из
> `integration/static-site-focus-group-product-20260727` интегрируется с R15
> public candidate в `integration/static-site-focus-r15-live-e2e-20260728`;
> production tester backend, cohort и rollout ещё не реализованы и не запущены.
> **Целевой запуск:** 30.07.2026, рабочее время из текущего release context —
> 18:00 `Europe/Kaliningrad` (`16:00Z`), с подтверждением при freeze.
> **Аудитория:** не публичный релиз, а закрытый исследовательский cohort до 200
> активных участий; подтверждение email/Яндекс в прототипе необязательно.
> **Период:** рекомендуемый первый цикл — 30 дней, до 30.08.2026.
> **Release truth:** `origin/main`; полная фактическая сверка —
> [current-state-audit.md](current-state-audit.md).
> **Implementation handoff:** [implementation-prompt.md](implementation-prompt.md).
> **Продуктовая механика:** [product-prototype.md](product-prototype.md).
> **Ручные письма:** [manual-email-templates.md](manual-email-templates.md).

## 0. Что реализовано 27.07.2026

В отдельной integration-ветке собран приёмочный UI-контур без production
side effects:

- `/` остаётся обычной продуктовой главной R15 и не подменяется программой;
- `/fokus-gruppa/` — программа и честная механика благодарности;
- `/fokus-gruppa/priglashenie/` — fragment intake, немедленное удаление кода,
  отдельная 30-дневная participation marker с отсчётом от вступления,
  установка постоянного приложения «Анонсы» и необязательный email/Яндекс
  identity-choice.
  Страница программы и закрытый hub сразу показывают один точный fragment URL
  и соответствующий QR, без дополнительного скрытого шага: ссылку можно
  открыть/скопировать/поделиться, а детерминированный локальный SVG скачать
  для презентации без внешнего генератора;
- `/zakrytaya-afisha/` — marker-gated hub текущих статических страниц;
- `/fokus-gruppa/kollektsiya/` — mobile-first коллекция пасхалок и демонстрация
  условного появления `FG-E12` после третьего события в календаре, а также
  отдельная non-prize миссия реальной проверки телефона и компьютера с честной
  single-device alternative;
- `/fokus-gruppa/zavershenie/` — automatic/operator end states и continuity;
- `/dlya-menya/` — consented local personalization с tri-state категориями,
  отдельным индексом интереса, объяснениями и no-send eligibility будущих
  автоматических подборок;
- reusable lab badge, NPS/page-usefulness/improvement/event-fact feedback
  specimen и SVG Repo lab icon.

Это **не закрытая production-сборка**. LocalStorage и opaque path не являются
авторизацией; focus membership/feedback остаются локальными. Экран вступления
уже вызывает общий Supabase Auth controller: email использует
`signInWithOtp()` с одним письмом, содержащим кнопку-ссылку и шестизначный
цифровой код, Яндекс — общий `custom:yandex` flow, а для уже вошедшего
пользователя `linkIdentity()`. Код вводится в одно логическое поле с
`inputmode="numeric"` и `autocomplete="one-time-code"`; шестая цифра запускает
проверку без Enter, при этом явная кнопка остаётся доступным fallback.
Трёхзначный код намеренно не используется: hosted Supabase Auth поддерживает
длину OTP только `6..10`, а минимальный безопасный OOB-код — шесть цифр.
Оба email-пути прошли live-прогон 29.07.2026 через технический Yandex Cloud
Mail Trigger receiver: код сработал только после шестой цифры, а отдельная
ссылка завершила PKCE; оба replay были отклонены. Это всё ещё не выдаётся за
готовый cohort backend. Feedback не
отправляется; подарок не объявлен. Обычный
`astro build` сохраняет публичную R15 главную, а focus mechanics доступны
только на выделенных `noindex,nofollow,noarchive` routes. Root-form production
checker считает `/fokus-gruppa/**` и `/zakrytaya-afisha/` явным семейством
приватных страниц и не требует от них `index,follow`; для всех обычных страниц
индексируемый контракт остаётся fail-closed. Эти же выделенные приватные
маршруты сохраняют page-local `no-referrer`: production checker разрешает его
только этому семейству и по-прежнему отклоняет случайную утечку candidate
privacy policy на обычные страницы. Каждая focus-страница также имеет
self-canonical URL через общий `absoluteUrl()`: в root-form proof это обычный
production URL, а в immutable candidate — тот же маршрут под текущим
`/_review/<token>` prefix. `build-production.mjs` и
`build-secret-candidate.mjs` по-прежнему владеют своими root transformations;
наличие focus routes не считается production root rollout.

### Продуктовая коррекция 29.07.2026

- onboarding обращается к новому пользователю обычными словами и не показывает
  implementation vocabulary (`fragment`, `membership`, `identity`,
  `localStorage`, `PWA`);
- это же правило действует для коллекции, обратной связи и завершения:
  пользователь видит только понятные действия, последствия и ограничения, без
  `prototype`, `handoff`, `device receipt`, `event_id`, `page family` и NPS-жаргона;
- подтверждение email/Яндекс прямо названо необязательным для использования
  афиши и важным для участия в розыгрыше двух театральных билетов;
- после нажатия `Получить код и ссылку` сразу появляется состояние отправки,
  после ответа — видимая форма шестизначного кода; через 60 секунд разрешена
  повторная отправка, а изменение адреса сбрасывает только предыдущий код;
- вошедший пользователь видит имя и кнопку `Выйти` непосредственно на экране
  вступления; logout не удаляет участие, сохранённые события или настройки;
- focus route использует тот же manifest identity, имя, иконки и start route,
  что обычное приложение `Анонсы`; отдельного `Анонсы Lab` больше нет;
- окончание 30-дневного исследования выключает только исследовательский режим.
  Аккаунт, приложение, сохранения и продуктовые настройки продолжают работать;
- явно отмеченный выбор получать итоги фокус-группы, результат розыгрыша и
  важные обновления сохраняется после исследования до отзыва. Он не
  предвыбран и не создаётся из факта входа, установки приложения или участия.

## 1. Решение

30 июля сайт не объявляется готовым для всей аудитории и не закрывает общий
public-release checklist. Запускается отдельный **режим фокус-группы**:

- ограниченный cohort с атомарным потолком `200`;
- участие можно начать без email/Яндекса; подтверждённая identity остаётся
  необязательным upgrade для восстановления и связи между устройствами;
- обычное приложение «Анонсы» как удобный путь возвращения, но не обязательное
  условие участия;
- явный feedback layer на всех ключевых page families;
- отдельный быстрый путь исправления фактов события;
- один консолидированный анализ feedback в сутки в течение месяца;
- один weekly impact review участнику;
- честно обозначенный прототип персонализации;
- пасхалки как обучение интерфейсу, но не как способ покупать положительные
  оценки;
- вертикальное видео только как маленький optional canary, если до freeze есть
  1–3 проверенных ролика и весь fail-closed contract.

Публичный anonymous launch остаётся `NO-GO`, пока не закрыты его собственные
promotion, rollback, freshness, OAuth/Search и product-acceptance gates.

## 2. Не переутяжелённая модель запуска

### P0–P1 capacity debt: Supabase Egress

На 29.07.2026 dashboard personalization-проекта показывает `3.24 GB / 5 GB`
Egress за текущий billing cycle при `55 MB` database size и `12` MAU. Суточный
график 24–28 июля уже показывает примерно `0.48–0.90 GB/day` и резкий пик
26 июля; при таком темпе free-limit может быть исчерпан ещё до роста аудитории.
Это не обычный «когда-нибудь оптимизировать», а **P0 investigation / P1
remediation** перед расширением фокус-группы. По одному billing-графику нельзя
утверждать причину; гипотеза о static-site generation/export обязана быть
проверена отдельно, а не записана как факт.

P0: снять почасовой/суточный разрез egress по Postgres/Data API/Auth/Storage/
Realtime/Edge Function, сопоставить пики с `StaticSiteBuilder`, DB-export,
build/E2E/backfill и cron run IDs, измерить количество запросов, строки и байты,
а также временно остановить очевидный runaway/repeated full export, если он
подтвердится. P1: перейти на bounded/incremental export, убрать N+1 и повторные
полные выборки, уменьшить payload, добавить caching/coalescing и alerts на
60/75/90% месячного лимита. Расширение аудитории блокируется, если расследование
не объяснило текущие `~0.5–0.9 GB/day`. Решение о Pro-плане принимается после
аудита, а не вместо устранения лишнего трафика.

### Волны

Не приглашать 200 человек одновременно:

| Волна | Размер | Когда расширять |
|---|---:|---|
| Seed | 20–30 | onboarding, feedback write, PWA, email и rollback проверены живыми людьми |
| Wave 2 | до 80 | нет P0 data/auth ошибок; daily digest не теряет обращения |
| Wave 3 | до 200 | D1 return, support load и feedback triage находятся в принятом диапазоне |

Глобальный cap не меняется от количества invite links. При заполнении cohort
новый invite получает честный waitlist/closed state, а не создаёт `201`-го
активного тестера.

### Контент и доступ

Самый реалистичный первый контур — новый immutable `noindex` candidate с
защищённым от случайной индексации prefix плюс авторизованные tester controls.
Opaque candidate URL снижает случайное распространение, но **не является
авторизацией**: пересланную ссылку можно открыть. Строгий per-user доступ ко
всему static HTML потребует отдельного Edge/CDN auth proxy и не должен скрыто
добавляться в трёхдневный MVP.

Поэтому в первой версии:

- static pages остаются readable/fail-safe;
- tester status открывает feedback, personal stats, weekly review, durable
  progress и invite action;
- release не рекламируется вне отобранной аудитории;
- candidate остаётся `noindex`, `no-referrer`, не попадает в sitemap;
- tester controls каждый раз проверяют active membership server-side.

## 3. Onboarding, QR и передача режима тестера

### Первый QR

Presentation QR ведёт не прямо в сессию, а в seed invite flow:

```text
QR / invite URL
  -> показать программу и срок
  -> принять focus-group terms + purpose-specific weekly review
  -> атомарно занять место в cohort
  -> предложить установку PWA
  -> предложить email-код / magic link ИЛИ Яндекс
  -> разрешить явно продолжить без подтверждения
  -> короткая orientation mission
```

Identity не является стеной перед продуктом. После явного продолжения
неподтверждённый участник получает локальный статус на текущем устройстве и
может пользоваться фокус-группой полные 30 дней с момента вступления; PWA
start-controller возвращает его сразу в закрытый hub. Сброс персонализации не
трогает этот отдельный статус. Никакого отдельного краткосрочного
preview-доступа в текущей механике нет.

Ограничение должно быть видно заранее: localStorage нельзя обещать как
восстановимый аккаунт. Если пользователь очистит данные браузера или сменит
устройство, только последующее подтверждение email/Яндекса и server membership
смогут восстановить статус и синхронизировать прогресс. PWA installation
browser-controlled: её нельзя вызвать или запустить без действия пользователя,
а отказ от установки не лишает участия.

### P0 continuity contract на 30 дней

В production локальная метка не может быть единственным источником доступа.
При redeem приглашения backend создаёт anonymous membership и device session
даже для варианта `Продолжить без подтверждения`:

- `active_until = joined_at + 30 days`, без idle/sliding timeout;
- PWA на каждом старте бесшумно восстанавливает эту же device session и сразу
  открывает закрытый hub, без повторного email/Яндекс;
- localStorage хранит только UX-cache, а server membership остаётся источником
  истины;
- сброс `Для меня`, коллекции или рекомендаций не удаляет membership/session;
- email/Яндекс — необязательный upgrade для восстановления после очистки данных
  и переноса на другое устройство, а не условие ежедневного входа;
- только явный выход участника, общий operator stop или истечение полных 30 дней
  закрывают доступ.

Именно этот контракт является release gate против «случайного разлогина».
Текущая page/product ветка демонстрирует тот же 30-дневный lifecycle локально,
но не выдаёт localStorage за готовую production-авторизацию.

Email и Яндекс используют один origin-scoped auth controller. Одноразовая
email-ссылка возвращается на очищенный текущий focus route; цифровой код
проверяется через `verifyOtp({ type: "email" })`; введённые адрес и код не
попадают в participation payload/localStorage. Hosted-шаблон использует один
и тот же `{{ .Token }}` в теме и крупном fallback-блоке письма, а
`{{ .ConfirmationURL }}` — в единственной основной CTA-кнопке. Срок действия —
10 минут, длина — шесть цифр. Если пользователь сначала подтвердил email,
Яндекс связывается через `linkIdentity()` с текущим `auth.uid()`, а не создаёт
второй продуктовый профиль.

Live E2E обязан выпускать два разных письма: сначала проверить автоматическое
срабатывание кода после шестой цифры и невозможность повторного использования,
затем запросить новый OTP и проверить реальную ссылку в том браузере, где был
создан PKCE verifier. Приём подтверждается техническим Yandex Cloud Mail
Trigger/Object Storage контуром; в артефакты попадают только хеши, статусы и
sanitized-разметка, но не live code, URL или адрес receiver. Это технический
E2E inbox и не замена human support mailbox.

### Тестер делится режимом

Передаётся **приглашение**, не аккаунт, сессия или прогресс:

- случайный 256-bit token; в БД только versioned HMAC;
- token передаётся во fragment URL, один раз считывается клиентом и удаляется
  через `history.replaceState`, чтобы не попадать в HTTP access log/referrer;
- короткий срок и `max_uses`; обычный default — одна успешная активация;
- inviter должен быть active tester;
- invitee получает собственный member/device id; при желании позже связывает
  его со своей подтверждённой identity;
- redeem транзакционно блокирует cohort row, проверяет global cap и расходует
  invite идемпотентно;
- sharing не увеличивает шанс получить приз;
- inviter не видит email/личность приглашённого;
- revoke, expiry, capacity close и abuse audit обязательны.

QR конкретной пасхалки не переносит tester status или ownership коллекции.

### Завершение периода

`active_until` проверяется каждым privilege RPC. После окончания:

- `active → alumni` даже если housekeeping job опоздал;
- tester feedback prompts, новые invites и weekly tester mail выключаются;
- Supabase Auth identity, явно сохранённые события, product settings и
  сделанные пользователем consent-choice остаются;
- recommendation/marketing consent не создаётся автоматически;
- участник становится обычным пользователем, а не удаляется.

### Выход из аккаунта и выход из программы

Это два разных действия. `Выйти` завершает только текущую Supabase Auth
session; 30-дневная focus participation marker и обычная персонализация не
удаляются. Отдельная кнопка `Выйти из фокус-группы` удаляет только участие и
сохраняет профиль `Для меня`. Глобальное мобильное меню, `/dlya-menya/` и
закрытый hub показывают account/logout состояние через общий auth runtime,
чтобы owner мог повторно пройти сценарий приглашения без очистки всех данных
браузера.

## 4. Зачем участник может подтвердить email или Яндекс

Email запрашивается после объяснения ценности, а не как безымянная стена.
Основная понятная человеку ценность — возможность участвовать в розыгрыше
двух театральных билетов и получить результат:

1. сохранить статус тестера и прогресс между устройствами;
2. получить личный weekly impact review: что проверено, что принято в работу,
   что исправлено и что появилось;
3. восстановить доступ после переустановки PWA/очистки браузера;
4. получить ограниченную возможность пригласить ещё одного участника;
5. получить уведомление о результате розыгрыша.

Подтверждение необязательно для входа в исследовательские страницы, но до
уведомления/получения материального приза победителю понадобится
опубликованный способ подтвердить, что result ledger принадлежит именно ему.
Это не должно менять уже набранный проверяемый результат.

На текущем экране есть один явный, не предвыбранный выбор получать итоги
фокус-группы, результат розыгрыша и важные обновления «Анонсов». Этот выбор не
истекает автоматически вместе с 30-дневным исследованием, поэтому его не надо
просить повторно на 31-й день; пользователь может снять его в любой момент.
Факт входа или участия сам по себе этот выбор не создаёт.

Отдельные будущие рекламные или персональные рассылки, которые не входят в
показанный пользователю текст, требуют своей цели и своего согласия.

- подтверждение email и focus-group identity;
- явно показанный continuing communication choice;
- любые новые рекомендации/маркетинг вне его текста — отдельный необязательный
  consent.

Никаких prechecked boxes. Отказ от email/Яндекса оставляет участие активным на
текущем устройстве. Отзыв communication choice прекращает именно письма и не
должен автоматически удалять membership, приложение, сохранения или
персонализацию. Обычная подписка не появляется из tester/install/calendar state.

`tester@kenigevents.ru` — обязательный human support/Reply-To alias or mailbox
для программы. До показа адреса должны быть подтверждены создание, MX/routing,
владелец, retention, response SLO и test send/reply. Сейчас канонический human
mailbox — `info@kenigevents.ru`; одной UI-строки недостаточно, а временная
подмена адреса не закрывает этот gate.

### Hosted email OTP contract

- тема: `{{ .Token }} — код входа в KenigEvents`, чтобы код был виден в списке
  писем без открытия;
- тело: один главный action `Войти по ссылке`, затем крупный шестизначный код
  как fallback, срок/одноразовость, запрет передачи и безопасный сценарий
  «не запрашивали — проигнорируйте»;
- HTML — узкая адаптивная карточка без внешних картинок, трекеров и критичных
  web-fonts; смысл сохраняется при отключённых стилях;
- на странице — одно поле, а не шесть независимых accessibility controls:
  цифровая мобильная клавиатура, системное OTP autocomplete, фильтрация
  нецифровых символов, auto-submit ровно один раз на полном коде, inline
  success/error и явный resend после cooldown;
- `{{ .Token }}` и `{{ .ConfirmationURL }}` живут в одинаково оформленных
  hosted `confirmation` и `magic_link` шаблонах Supabase: новый адрес сначала
  получает confirmation-flow, подтверждённый — magic-link flow; email-код и
  ссылка остаются двумя интерфейсами одной одноразовой выдачи, но E2E проверяет
  их на разных выдачах;
- канонические файлы:
  `supabase/templates/focus-magic-link.subject.txt`,
  `supabase/templates/focus-magic-link.html`,
  `scripts/configure_focus_auth_email.py`.

Hosted Auth отправляет эти шаблоны не через ограниченный default sender, а
через отдельный custom SMTP Yandex Cloud Postbox: подтверждённый домен
`kenigevents.ru`, sender `notify@kenigevents.ru`, STARTTLS `587`. SMTP API key
хранится только в hosted Auth config, ограничен `yc.postbox.send` и должен быть
заменён не позднее `29.07.2027`; секрет и технический receiver в Git/receipt не
попадают. Management API receipt проверяет hashes обоих template families,
`mailer_otp_length=6`, `mailer_otp_exp=600` и сам факт SMTP-конфигурации.

Live evidence 29.07.2026: первая выдача дала один и тот же шестизначный код в
теме и теле, пять цифр не отправили verify-запрос, шестая отправила ровно один,
создала session и повтор кода получил `403`; вторая выдача завершила PKCE на
чистом fragment-free route, а повтор ссылки вернулся `303` с error code.
Канонические sanitized receipts:
`artifacts/codex/focus-email-live-e2e-20260729/live-email-dual-path-receipt.json`,
`live-email-final-template-receipt.json`,
`live-email-code-e2e-receipt.json`,
`live-email-link-issuance-receipt.json` и
`live-email-link-replay-receipt.json`.

Визуальное исследование 29.07.2026: Pinterest funnel `60 collected / 60
self-reviewed / 10 shortlisted` по 10 query families. Приняты не конкретные
макеты, а повторяющиеся механики: код как крупнейший элемент, одна CTA,
спокойная узкая карточка, короткая security-copy, отсутствие декоративного
шума. Источник исследования:
`/home/dev/projects/pinterest-idea-library/collections/20260729-otp-email-verification-ux-references/`.

## 5. Feedback model

### Не называть всё NPS

Стандартный relationship NPS задаётся **один раз в неделю и на exit**:

> Насколько вероятно, что вы порекомендуете «Полюбить Калининград · Анонсы»
> знакомым? `0…10`

Для каждой page family измеряется не «ещё один NPS», а usefulness:

> Насколько эта страница помогла выбрать или понять событие? `0…10`

Так данные остаются интерпретируемыми. Если owner всё же называет показатель
`page NPS`, в отчёте он должен быть явно отделён от общего NPS.

### Sampling

- не чаще одного score на `tester + page_family + 7-day period`;
- только после meaningful use, а не при первом paint;
- максимум два автоматических prompt в сутки;
- always-available компактная кнопка `Предложить улучшение` не зависит от
  sampling;
- оценки с `n < 20` не сравниваются как устойчивый рейтинг page families;
- denominator, non-response и release/build version видны в отчёте.

Первая taxonomy:

`home | today | tomorrow | weekend | popular | search | for_me | festivals |
festival_detail | event_detail | collections`.

### Три разных feedback path

1. `surface_score` — usefulness `0…10`, optional reason tags.
2. `improvement` — свободное предложение до 2 000 символов.
3. `event_fact_issue` — date/time/place/price/ticket/status/media/duplicate/other.

`event_fact_issue` не ждёт следующего суточного LLM-анализа: critical signal
сразу попадает в operator triage, но никогда напрямую не меняет Fly SQLite.
Client event snapshot не является authoritative; server связывает report с
canonical `event_id`, release/build и server-known snapshot hash.

## 6. Ежедневный цикл обратной связи

Один bounded job в сутки, около `04:30 Europe/Kaliningrad`:

1. атомарно claim-ит watermark после последнего successful digest;
2. считает distribution score/NPS, page families, issue categories и statuses;
3. удаляет email/телефоны и другие прямые идентификаторы до LLM;
4. дедуплицирует близкие предложения и группирует темы;
5. отделяет factual event issues от product suggestions;
6. создаёт один компактный digest и не более одной operator task на тему;
7. человек принимает `repair | product_backlog | need_evidence | reject`;
8. повторный run с тем же input hash — no-op.

Не создавать ArtKodex task на каждый комментарий. После triage одна подтверждённая
проблема события получает один idempotent repair task с root-cause/replay/public
surface contract. LLM не редактирует события и не объявляет отзыв «внедрённым».

## 7. Weekly impact review

Один operator-approved issue в неделю, затем до 200 персонализированных outbox
rows через отдельный `focus_group_weekly` stream:

- активные дни и использованные page families;
- количество сохранений/feedback/checkpoints без сырой истории;
- статус собственных обращений: `получено | проверяем | исправлено | отложено`;
- внедрённые изменения, только когда есть accepted change ↔ feedback theme link;
- новые функции и задачи следующей недели;
- общий weekly NPS prompt;
- unsubscribe/leave-focus-group.

Не включать raw interest profile, поисковые фразы, посещённые event titles или
чужие отзывы. Не говорить «по вашему отзыву», если causal link не подтверждён.
Существующий recommendation contract «ровно три события» не переиспользуется:
нужны отдельные consent purpose, template и outbox kind, но общий identity,
suppression и NotiSend transport можно использовать.

## 8. Пасхалки и два билета

### Последнее owner decision

Один приз — **два билета в театр**. Победитель определяется
лексикографически: сначала доля собранной доступной коллекции, затем bounded
participation `0…40` и широта способов участия. NPS учитывается только как факт
ответа; `0` и `10` равноправны. Like и dislike симметричны; sentiment, длина
текста, повторы, скорость, покупки, share и invites преимущества не дают.

Полная формула, tie/audit/anti-abuse/accessibility gates и versioned карта из 12
responsive placements находятся в
[easter-egg-program.md](easter-egg-program.md). До публикации правил интерфейс
показывает `Правила готовятся`, localStorage остаётся только демонстрацией и
никаких конкурсных баллов не начисляет.

До юридической приёмки нельзя обещать розыгрыш. Российские требования к
согласию на персональные данные, электронной рекламе и публичному конкурсу
требуют отдельной проверки правил и consent: [152-ФЗ ст. 9](https://www.consultant.ru/document/cons_doc_LAW_61801/6c94959bc017ac80140621762d2ac59f6006b08c/),
[38-ФЗ ст. 18](https://www.consultant.ru/document/cons_doc_LAW_58968/f892dec1383709792452f18d36e7043306e2be0a/),
[ГК РФ ст. 1057](https://www.consultant.ru/document/cons_doc_LAW_9027/5f117a49bae27013895ad2a7821c983a7a853a20/).
Это release gate, не юридическое заключение.

## 9. Минимальный прототип персонализации

Для focus launch не нужен online ML backend. Достаточно существующего bounded
local prototype:

- explicit interest selection/cold-start;
- like, `не интересно`, share/save strong actions;
- client-side rerank конечного static candidate pool;
- разнообразие и exploration slot;
- `Почему это показано`;
- honest static fallback при no-consent/storage/backend failure.

`/dlya-menya/` маркируется `Прототип` и попадает в небольшое A/B/holdout
сравнение. Primary metric — не CTR сам по себе, а first useful event,
save/detail/ticket action и negative feedback. Full cross-device inferred profile
не является требованием 30.07.

Перед использованием durable saved state нужно устранить live migration drift:
в Supabase применена версия `20260727151208 durable_saved_events_v1_20260727`,
а R15 содержит кандидат-файл `20260727141820_durable_saved_events_v1.sql`.
Сначала hash/semantic reconcile, затем repository migration, SQL/RLS tests,
advisors, UI wiring и main merge; нельзя просто повторно применить side-branch SQL.

## 10. Вертикальные видео событий

К 30.07 полный pipeline нереалистичен. В `main` есть Telegram video intake,
`event_media_asset` и legacy Telegraph playback, но static exporter/Astro не
проецируют и не показывают видео.

Допустимый stretch canary — только 1–3 заранее source/rights-approved clips:

- versioned allowlist/sidecar с `event_id`, media id, source, dimensions,
  duration, poster, rights/approval/revision;
- poster grid: 2 mobile, 2–3 desktop;
- по tap/Enter открывается story-like dialog;
- `preload=none`, no autoplay, no sound start; video element создаётся после intent;
- contained `9:16`, prev/next/close/Escape/focus return/reduced-data;
- broken media оставляет source link и не ломает facts/CTA;
- один bounded open/start/complete/fail signal per session, без heartbeat firehose;
- отсутствуют approved assets — отсутствуют markup и bytes.

Canary не входит в 30.07 GO. Если ролики/posters/rights не готовы к 28.07 freeze,
функция остаётся post-release.

## 11. Экологичная data-модель

Fly SQLite продолжает владеть событиями, publication и operational jobs.
Supabase/Postgres владеет tester identity, consent, access, feedback и compact
personal state. Object Storage хранит большие private daily reports. YDB/Kaggle
для 200 человек и одного месяца не нужны.

Минимальные новые сущности в private `focus_group` schema:

| Entity | Назначение | Bound |
|---|---|---|
| `program` | период, status, terms, capacity/active count | единицы rows |
| `membership` | user, active/alumni/withdrawn, expiry, inviter | ≤200 active |
| `invite` | token HMAC, key version, uses, expiry/revoke | bounded quota |
| `member_page_daily` | one row per user/day/page family/release | worst case ≈72k/month |
| `feedback_item` | score/text/event issue + idempotency/status | rate-limited; raw text 45–90d |
| `feedback_daily_analysis` | one claim/result per day | ≈30 rows/month |

Общий NPS и page usefulness могут быть rows `feedback_item` с unique
`user + period + page_family + kind`, а не отдельными event logs. Большой report
body — private Object Storage; DB хранит counts, bounded theme summary и path.

Переиспользовать:

- `auth.users` и verified email;
- существующие `email_control` identity/consent/suppression/outbox, расширив
  purpose `focus_group_research` и kind `focus_group_weekly`;
- PWA state + daily aggregate, не связывая задним числом anonymous install UUID
  с email без отдельного consent;
- eventual durable saved-event contract после reconciliation.

Browser не получает прямой table access. Narrow authenticated RPC:

- проверяет `auth.uid()` и active membership;
- не использует user-editable `user_metadata` для authorization;
- revoke `PUBLIC`, fixed empty `search_path`, explicit grants;
- idempotency, rate/cap before insert;
- RLS owner isolation and `security_invoker` views.

Supabase рекомендует RLS на exposed tables и не использовать изменяемый
`raw_user_meta_data` как authorization source: [RLS documentation](https://supabase.com/docs/guides/database/postgres/row-level-security).
Email OTP/magic link поддерживаются Supabase Auth, но production требует custom
SMTP; default provider не предназначен для пользовательской рассылки:
[Auth](https://supabase.com/docs/guides/auth),
[custom SMTP](https://supabase.com/docs/guides/auth/auth-smtp).

На 29.07.2026 personalization database занимает около `55 MB`; запас до
500 MB ceiling пока достаточен. Цель — не использовать запас, а сохранить
bounded rows, retention и shed-first weak telemetry.

## 12. Метрики решения

### Cohort funnel

`invite_open → email_submit → email_verified → tester_activated → PWA_prompt →
PWA_installed → first_useful_action → D1/D7/D28`.

### Product

- overall weekly/exit NPS with response denominator;
- page usefulness by family, только при достаточном `n`;
- time/tasks to first useful event;
- search success; save/detail/ticket action; negative action;
- adoption of festivals, collections, `Для меня`, video canary;
- PWA confirmed install and standalone D1/D7;
- feedback submissions, actionable share, median triage/repair time;
- implemented theme count with evidence, not self-reported causality;
- referral activation under fixed cap.

200 people give directional product evidence, not proof of small percentage
uplifts. Qualitative clusters and repeated task failures matter more than
leaderboards of tiny page cohorts.

## 13. Release gates: что ещё реализовать

### P0 before first seed user

- [ ] Freeze one new main-reachable SHA and fresh event snapshot; build a new
  immutable focus candidate including post-R14 PWA/icon/analytics changes.
- [ ] Rehearse candidate rollback/freshness and close live OAuth/Search on the
  exact frozen candidate; do not call old R14 candidate current.
- [x] Implement optional email OTP/magic link + Yandex identity UI, anonymous
  continuation, immediate send feedback, visible numeric OTP, cooldown resend
  and obvious logout. Durable anonymous→verified progress merge remains in the
  membership backend item below.
- [ ] Implement program/membership/invite RPC, seed QR, referral, atomic cap 200,
  expiry/alumni/withdrawal and negative auth/concurrency tests.
- [ ] Publish focus terms/privacy/retention and persist the explicit continuing
  communication choice server-side; keep any purpose not named in its visible
  text separate.
- [ ] Implement shared usefulness/improvement UI plus typed event-fact issue UI,
  authenticated RPC, idempotency/rate limits and operator-visible receipt.
- [ ] Implement one daily digest claim/redaction/clustering/triage path and
  immediate critical event-issue alert; no automatic event mutation.
- [ ] Expose existing local personalization as an honest `Прототип` with static
  fallback and bounded experiment assignment.
- [ ] Verify PWA install and lifecycle writes on the exact candidate; live DB
  currently has schema but zero metric rows.
- [ ] Provision and test `tester@kenigevents.ru`; never advertise an unowned
  address or silently substitute `info@`.
- [ ] Add kill switches independently for admission, invites, feedback,
  daily analysis, weekly email, eggs and videos.
- [ ] Run mobile/desktop/keyboard/screen-reader/no-JS, RLS/abuse, email and
  end-to-end `invite → verify → activate → feedback → digest → expire` checks.

### P0 drift/integration blockers

- [ ] Reconcile the live saved-event migration version/hash with R15 before any
  new migration; commit one canonical migration + contract tests.
- [ ] Integrate from fresh `origin/main`, not the dirty root checkout.
- [ ] Do not merge stale CTA, old video-doc or old ArtKodex branches wholesale.
- [ ] Root public promotion remains separate; focus-group success does not close
  canonical lifecycle, D0/D10 Telegraph or public SEO gates.

### Required before the first weekly email

- [ ] Separate `focus_group_research` consent and `focus_group_weekly` outbox kind.
- [ ] Narrow NotiSend worker/template, support Reply-To, unsubscribe/exit,
  suppression, provider-id and authenticated delivery evidence.
- [ ] Rotate/reissue the previously exposed NotiSend key or obtain explicit
  security acceptance; seed-list canary and operator approval.
- [ ] Link shipped changes to accepted themes; generate ≤200 compact per-member
  summaries without raw history.

### Optional, non-blocking

- [ ] Non-prize artifact orientation pilot after source/IP and low-end-device gate.
- [ ] Prize application only after separate legal/rules/anti-abuse release.
- [ ] 1–3-event vertical-video canary only after approved sidecar, poster/player
  and browser gates.
- [ ] Participants/likes and stale CTA visual lab remain out of scope.

## 14. Stop/rollback rules

Pause admission immediately when any occurs:

- cap, invite or RLS bypass;
- verified identity attached to wrong membership;
- duplicate/vanished feedback or unbounded DB growth;
- event correction mutates canonical facts without human confirmation;
- email sent without the exact purpose consent or after unsubscribe;
- candidate/root rollback cannot restore last-good;
- P0 event-quality incident makes research results unreliable.

Feature-specific kill switches disable the broken experiment without taking down
static event pages. At cohort close, preserve only consent/audit/suppression and
bounded aggregates for accepted retention; delete/anonymize raw research text on
schedule.

- Secret-candidate packaging keeps the private focus routes noindex/noarchive and adds `nosnippet`; root-form production keeps the same routes private without changing the public candidate policy.
