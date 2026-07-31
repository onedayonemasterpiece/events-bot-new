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
  отдельная 30-дневная participation marker с отсчётом от вступления, focus PWA
  install/start controller и необязательный email/Яндекс identity-choice.
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
`signInWithOtp()` с одноразовой ссылкой, Яндекс — общий `custom:yandex` flow,
а для уже вошедшего пользователя `linkIdentity()`. Однако реальная email
доставка не является принятой до custom SMTP, отдельного E2E inbox и полного
mailbox browser test; это не выдаётся за готовый cohort backend. Feedback не
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

## 1. Решение

30 июля сайт не объявляется готовым для всей аудитории и не закрывает общий
public-release checklist. Запускается отдельный **режим фокус-группы**:

- ограниченный cohort с атомарным потолком `200`;
- участие можно начать без email/Яндекса; подтверждённая identity остаётся
  необязательным upgrade для восстановления и связи между устройствами;
- PWA как основной путь возвращения, но не обязательное условие участия;
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
email-ссылка возвращается на очищенный текущий focus route; введённый адрес не
попадает в participation payload/localStorage. Если пользователь сначала
подтвердил email, Яндекс связывается через `linkIdentity()` с текущим
`auth.uid()`, а не создаёт второй продуктовый профиль. Этот кодовый контракт не
заменяет обязательный E2E через custom SMTP и dedicated inbox.

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
- Supabase Auth identity, явно сохранённые события и обычные пользовательские
  consent остаются;
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

Email запрашивается после объяснения ценности, а не как безымянная стена:

1. сохранить статус тестера и прогресс между устройствами;
2. получить личный weekly impact review: что проверено, что принято в работу,
   что исправлено и что появилось;
3. восстановить доступ после переустановки PWA/очистки браузера;
4. получить ограниченную возможность пригласить ещё одного участника;
5. получить уведомление о результате prize programme, только если он будет
   отдельно юридически принят и пользователь подал заявку.

Подтверждение необязательно для входа в исследовательские страницы, но до
уведомления/получения материального приза победителю понадобится
опубликованный способ подтвердить, что result ledger принадлежит именно ему.
Это не должно менять уже набранный проверяемый результат.

Нужны раздельные цели:

- подтверждение email и focus-group identity;
- согласие на еженедельную исследовательскую коммуникацию ровно на срок cohort;
- обычные рекомендации/маркетинг — отдельный необязательный consent.

Никаких prechecked boxes. Отказ от email/Яндекса оставляет участие активным на
текущем устройстве, но weekly email отсутствует и восстановление после очистки
браузера не обещается. Отзыв research-mail consent прекращает именно письма и
не должен автоматически удалять membership, PWA, сохранения или
персонализацию. Обычная подписка не появляется из tester/PWA/calendar state.

`tester@kenigevents.ru` — обязательный human support/Reply-To alias or mailbox
для программы. До показа адреса должны быть подтверждены создание, MX/routing,
владелец, retention, response SLO и test send/reply. Сейчас канонический human
mailbox — `info@kenigevents.ru`; одной UI-строки недостаточно, а временная
подмена адреса не закрывает этот gate.

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

Текущий personalization database занимает около `38 MB`; запас порядка
`460 MB` до 500 MB ceiling достаточен. Цель — не использовать запас, а сохранить
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
- [ ] Implement optional email OTP/magic link + Yandex identity upgrade,
  anonymous continuation and idempotent anonymous→verified progress merge.
- [ ] Implement program/membership/invite RPC, seed QR, referral, atomic cap 200,
  expiry/alumni/withdrawal and negative auth/concurrency tests.
- [ ] Publish focus terms/privacy/retention and purpose-specific tester-mail
  consent; keep recommendation consent separate.
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

## Phone connectivity diagnostic

The unlinked `noindex` route `/fokus-gruppa/diagnostika/` is a narrow incident
tool, not a focus-group replacement. One tap runs three parallel bounded
`no-store` reads: Supabase Auth health, one tiny RLS-safe Supabase Data API read
and a dedicated Yandex API Gateway → YDB `GetItem`. It never sends an OTP or
changes account data.

The entire result fits one phone screenshot: understandable statuses, response
times, one opaque correlation code, local time and browser/PWA plus effective
network mode. The same code is sent as Supabase `X-Client-Info` and the Yandex
gateway query marker so the screenshot time can be compared with both provider
logs. The public receipt uses only neutral labels (`Вход по почте`, `Данные
сайта`, `Резервный канал`) and never exposes provider names. No email, OTP, JWT
or account identity appears in the result.
