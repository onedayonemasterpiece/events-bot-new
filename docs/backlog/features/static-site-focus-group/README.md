# Фокус-группа статического сайта — продуктовый прототип

> **Статус:** page/product prototype реализован в
> `integration/static-site-focus-group-product-20260727`; production tester
> backend, cohort и rollout не реализованы и не запущены.
> **Целевой запуск:** 30.07.2026, рабочее время из текущего release context —
> 18:00 `Europe/Kaliningrad` (`16:00Z`), с подтверждением при freeze.
> **Аудитория:** не публичный релиз, а закрытый исследовательский cohort до 200
> подтверждённых участников.
> **Период:** рекомендуемый первый цикл — 30 дней, до 30.08.2026.
> **Release truth:** `origin/main`; полная фактическая сверка —
> [current-state-audit.md](current-state-audit.md).
> **Implementation handoff:** [implementation-prompt.md](implementation-prompt.md).
> **Продуктовая механика:** [product-prototype.md](product-prototype.md).
> **Ручные письма:** [manual-email-templates.md](manual-email-templates.md).

## 0. Что реализовано 27.07.2026

В отдельной integration-ветке собран приёмочный UI-контур без production
side effects:

- `/` — noindex-заглушка о тестировании фокус-группой;
- `/fokus-gruppa/` — программа и честная механика благодарности;
- `/fokus-gruppa/priglashenie/` — fragment intake, немедленное удаление кода,
  bounded localStorage preview marker и честный email/Яндекс auth-choice;
- `/zakrytaya-afisha/` — marker-gated hub текущих статических страниц;
- `/fokus-gruppa/zavershenie/` — automatic/operator end states и continuity;
- `/dlya-menya/` — consented local personalization с tri-state категориями,
  отдельным индексом интереса, объяснениями и no-send eligibility будущих
  автоматических подборок;
- reusable lab badge, NPS/page-usefulness/improvement/event-fact feedback
  specimen и SVG Repo lab icon.

Это **не закрытая production-сборка**. LocalStorage и opaque path не являются
авторизацией; email/OAuth только показаны как интерфейс; feedback не
отправляется; письма не автоматизированы; подарок не объявлен. Обычный
`astro build` показывает prototype root, но `build-production.mjs` и
`build-secret-candidate.mjs` по-прежнему владеют своими root transformations.
Эта ветка намеренно не меняет их и не может считаться production root rollout.

## 1. Решение

30 июля сайт не объявляется готовым для всей аудитории и не закрывает общий
public-release checklist. Запускается отдельный **режим фокус-группы**:

- ограниченный cohort с атомарным потолком `200`;
- обязательная подтверждённая email-identity для статуса тестера;
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
  -> запросить email
  -> подтвердить email кодом ИЛИ magic link
  -> принять focus-group terms + purpose-specific weekly review
  -> атомарно занять место в cohort
  -> предложить установку PWA
  -> короткая orientation mission
```

До подтверждения email пользователь не получает durable tester membership.
PWA installation может быть предложена сразу после активации, но browser
confirmation нельзя обойти и отказ не лишает статуса тестера.

### Тестер делится режимом

Передаётся **приглашение**, не аккаунт, сессия или прогресс:

- случайный 256-bit token; в БД только versioned HMAC;
- token передаётся во fragment URL, один раз считывается клиентом и удаляется
  через `history.replaceState`, чтобы не попадать в HTTP access log/referrer;
- короткий срок и `max_uses`; обычный default — одна успешная активация;
- inviter должен быть active tester;
- invitee подтверждает собственный email и получает собственный `user_id`;
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

## 4. Почему участник оставляет и подтверждает email

Email запрашивается после объяснения ценности, а не как безымянная стена:

1. сохранить статус тестера и прогресс между устройствами;
2. получить личный weekly impact review: что проверено, что принято в работу,
   что исправлено и что появилось;
3. восстановить доступ после переустановки PWA/очистки браузера;
4. получить ограниченную возможность пригласить ещё одного участника;
5. получить уведомление о результате prize programme, только если он будет
   отдельно юридически принят и пользователь подал заявку.

Нужны раздельные цели:

- подтверждение email и focus-group identity;
- согласие на еженедельную исследовательскую коммуникацию ровно на срок cohort;
- обычные рекомендации/маркетинг — отдельный необязательный consent.

Никаких prechecked boxes. Weekly research review является прозрачным условием
участия именно в фокус-группе: отказ на onboarding не мешает пользоваться
обычным сайтом, а последующий отзыв этого consent сразу переводит tester в
`alumni` и прекращает tester mail/privileges. Это не должно затрагивать аккаунт
и обычные сохранения. Обычная подписка не появляется из
tester/PWA/calendar state.

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

### Product verdict

Не связывать шанс выигрыша с положительностью, длиной или количеством feedback.
Иначе NPS смещается, появляются формальные отзывы, а поздние/малодоступные
участники оказываются в худших условиях.

Рекомендуемый 30.07 scope: **non-prize orientation canary**. Пасхалки помогают
найти Search, Festivals, `Для меня`, сохранение и feedback; дают badge/status,
но не являются GO blocker.

Если owner сохраняет prize mechanic, минимально честный вариант:

- один приз = **пара театральных билетов**, без двусмысленности;
- уже проработанный threshold `5 из 8`, а не «найти всё»;
- после threshold — отдельная explicit application;
- feedback остаётся добровольным и не влияет на eligibility/odds;
- одна равновесная заявка на одного verified tester;
- hints, speed, 8/8, invites, social share, purchases и sentiment не повышают шанс;
- accessible alternative, outage credit/extension и immutable eligible snapshot;
- отдельные правила: organizer, сроки, возраст/территория, prize, selection,
  alternate, получение, privacy, consent, audit и публикация результата.

Компромисс «пасхалки + обязательно feedback» допустим только как отдельный
biased checkpoint, где можно выбрать `всё понятно / нечего добавить`, нет
минимальной длины или нужной оценки, а ответ исключается из unbiased NPS.

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
- [ ] Implement email OTP + magic link and verified-email tester admission.
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
