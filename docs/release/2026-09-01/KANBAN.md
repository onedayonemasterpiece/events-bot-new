<!-- GENERATED: edit checklist.toml, not this file. -->
# Запуск «Полюбить Калининград · Анонсы» — kanban

> Срез 2026-08-04 · [сводка](README.md) · [полный checklist](CHECKLIST.md)

Kanban показывает движение deliverables, а не заменяет evidence. Верхний board — только критический path; ниже находится полный board.

## Критический board

### ⛔ Заблокировано

- [`CORE-02`](CHECKLIST.md#core-02) **P0 · E2** — Аудит здоровья Smart Update/StaticSiteBuilder за последние 24 часа  
  _Далее:_ Добавить краткоживущий FLY_API_TOKEN, выполнить read-only probe и удалить временный инструмент.
- [`FG-04`](CHECKLIST.md#fg-04) **P0 · E1** — Тихая анонимная Supabase-сессия после приглашения  
  _Далее:_ Реализовать ensureFocusAnonymousSession() и явные состояния auth runtime.
- [`FG-06`](CHECKLIST.md#fg-06) **P0 · E1** — Оценка страницы, текст и скриншот доступны анонимному участнику  
  _Далее:_ Убрать login wall requireSession(); использовать anonymous auth.uid().
- [`FG-27`](CHECKLIST.md#fg-27) **P0 · E1** — Зафиксировать cutoff 31 августа в 18:00 по Калининграду  
  _Далее:_ Удалить rolling 30 days и добавить timezone/cutoff tests.
- [`FG-28`](CHECKLIST.md#fg-28) **P0 · E0** — Неизменяемый eligible snapshot, розыгрыш и резерв  
  _Далее:_ Собрать защищённый workflow, audit receipt и rehearsal.
- [`COL-01`](CHECKLIST.md#col-01) **P0 · E1** — Полный единый реестр всех канонических подборок  
  _Далее:_ Создать readiness projection v2 со статусами route/data/navigation/sitemap.
- [`SEARCH-02`](CHECKLIST.md#search-02) **P0 · E1** — Доказать причину сбоя и восстановить production-поиск  
  _Далее:_ Выполнить коррелированный live recovery run с exact static/Edge/corpus identities.
- [`P13N-04`](CHECKLIST.md#p13n-04) **P0 · E1** — Долговечная материализация анонимного профиля и связывание identity  
  _Далее:_ Связать с focus anonymous subject и политикой merge профиля.
- [`MAIL-07`](CHECKLIST.md#mail-07) **P0 · E1** — Ротировать раскрытый API-ключ NotiSend  
  _Далее:_ Запросить revoke/reissue, обновить Lockbox, проверить недействительность старого ключа.
- [`LEGAL-03`](CHECKLIST.md#legal-03) **P0 · E1** — Отдельные согласия на обработку персональных данных по целям  
  _Далее:_ Подготовить самостоятельные тексты согласий и versioned evidence; не встраивать их в пользовательское соглашение.
- [`LEGAL-08`](CHECKLIST.md#legal-08) **P0 · E1** — Публичные правила розыгрыша для участников фокус-группы  
  _Далее:_ Указать организатора, eligibility, приз, даты, метод выбора, резерв, получение и публикацию результата.
- [`LEGAL-11`](CHECKLIST.md#legal-11) **P0 · E1** — Аудит локализации и потоков данных по 152-ФЗ для Supabase/Auth/email/profile  
  _Далее:_ Определить допустимый production flow либо необходимые изменения primary storage в РФ.
- [`CORE-09`](CHECKLIST.md#core-09) **P0 · E1** — Telegraph dual-run и переход D0–D10  
  _Далее:_ Реализовать resolver, режимы, запреты create/recreate и soak metrics.
- [`OPS-02`](CHECKLIST.md#ops-02) **P0 · E1** — Hosted-сценарий отказа обоих маршрутов без потери данных  
  _Далее:_ Доказать отсутствие false success, bounded queue и exactly-once reconnect.

### 🧭 Требуется решение владельца

- [`UI-02`](CHECKLIST.md#ui-02) **P0 · E4** — Утвердить визуальный вариант публичной заглушки до запуска  
  _Далее:_ Выбрать один кандидат и зафиксировать эталонные screenshots.

### 🔎 Исследование / решение / дизайн

- [`GOV-01`](CHECKLIST.md#gov-01) **P0 · E1** — Зафиксировать единый scope публичного релиза 1 сентября  
  _Далее:_ Утвердить must-have, допустимые default-off контуры и post-launch backlog.

### 🛠 Разработка / интеграция

- [`CORE-06`](CHECKLIST.md#core-06) **P0 · E2** — Атомарная публикация root и проверенный rollback  
  _Далее:_ Закрыть inventory buckets/ALB/DNS, apply rehearsal и last-good rollback.

### ○ Очередь

- [`QA-06`](CHECKLIST.md#qa-06) **P0 · E0** — Аудит доступности и исправление критических дефектов  
  _Далее:_ Проверить клавиатуру, focus, семантику, контраст, reduced motion и screen reader.

## Полный board

<details open>
<summary><strong>⛔ Заблокировано</strong> — 52 пунктов, P0: 45</summary>

**QA, безопасность и аналитика**

- [`QA-03`](CHECKLIST.md#qa-03) `P0` `qa` `E1` — Anonymous focus release tests

**UI/UX и визуальная готовность**

- [`UI-06`](CHECKLIST.md#ui-06) `P0` `design` `E1` — Спроектировать UI полного каталога подборок
- [`UI-12`](CHECKLIST.md#ui-12) `P0` `design` `E1` — Спроектировать page score: unanswered/answered/revision changed
- [`UI-13`](CHECKLIST.md#ui-13) `P0` `design` `E1` — Спроектировать общий service NPS в participant hub
- [`UI-21`](CHECKLIST.md#ui-21) `P0` `design` `E0` — Спроектировать UI юридических согласий без consent wall

**Авторизация и identity**

- [`AUTH-06`](CHECKLIST.md#auth-06) `P0` `qa` `E1` — Yandex OAuth anonymous focus upgrade
- [`AUTH-09`](CHECKLIST.md#auth-09) `P0` `development` `E1` — Auth status never presents anonymous subject as verified login

**Инфраструктура и эксплуатация**

- [`OPS-02`](CHECKLIST.md#ops-02) `P0` `qa` `E1` — Hosted-сценарий отказа обоих маршрутов без потери данных
- [`OPS-10`](CHECKLIST.md#ops-10) `P0` `live` `E1` — Secrets and credential rotation inventory

**Персонализация и «Для меня»**

- [`P13N-04`](CHECKLIST.md#p13n-04) `P0` `development` `E1` — Долговечная материализация анонимного профиля и связывание identity

**Подборки и каталоги**

- [`COL-01`](CHECKLIST.md#col-01) `P0` `development` `E1` — Полный единый реестр всех канонических подборок
- [`COL-02`](CHECKLIST.md#col-02) `P0` `development` `E1` — Mobile/desktop menu consumes collection registry only
- [`COL-03`](CHECKLIST.md#col-03) `P0` `qa` `E1` — Build-time route integrity for catalog/navigation/sitemap
- [`COL-04`](CHECKLIST.md#col-04) `P0` `integration` `E1` — Reconcile free collection route/data/publication
- [`COL-05`](CHECKLIST.md#col-05) `P0` `decision` `E1` — Reconcile kids/family collection naming and route
- [`COL-06`](CHECKLIST.md#col-06) `P0` `integration` `E1` — Clubs publication uses six-month activity rule

**Почта, ящики и шаблоны**

- [`MAIL-07`](CHECKLIST.md#mail-07) `P0` `live` `E1` — Ротировать раскрытый API-ключ NotiSend
- [`MAIL-19`](CHECKLIST.md#mail-19) `P0` `live` `E1` — Deliverability warm-up and production canary

**Статический сайт и публикация**

- [`CORE-02`](CHECKLIST.md#core-02) `P0` `live` `E2` — Аудит здоровья Smart Update/StaticSiteBuilder за последние 24 часа
- [`CORE-08`](CHECKLIST.md#core-08) `P0` `development` `E1` — Stable URL lifecycle registry, aliases, redirects/410
- [`CORE-09`](CHECKLIST.md#core-09) `P0` `development` `E1` — Telegraph dual-run и переход D0–D10

**Умный поиск**

- [`SEARCH-02`](CHECKLIST.md#search-02) `P0` `live` `E1` — Доказать причину сбоя и восстановить production-поиск
- [`SEARCH-03`](CHECKLIST.md#search-03) `P0` `development` `E1` — Cache invalidation by catalog/corpus revision

**Фокус-группа**

- [`FG-01`](CHECKLIST.md#fg-01) `P0` `development` `E1` — Глобальный soft gate выключает обычный сайт без marker
- [`FG-02`](CHECKLIST.md#fg-02) `P0` `development` `E1` — Актуальная focus placeholder surface
- [`FG-04`](CHECKLIST.md#fg-04) `P0` `development` `E1` — Тихая анонимная Supabase-сессия после приглашения
- [`FG-05`](CHECKLIST.md#fg-05) `P0` `qa` `E0` — Reload/reinvite переиспользуют один anonymous subject
- [`FG-06`](CHECKLIST.md#fg-06) `P0` `development` `E1` — Оценка страницы, текст и скриншот доступны анонимному участнику
- [`FG-07`](CHECKLIST.md#fg-07) `P0` `development` `E1` — Page score хранится по page_family + page_revision
- [`FG-08`](CHECKLIST.md#fg-08) `P0` `development` `E1` — UI «Страница обновилась» сохраняет прежнюю оценку
- [`FG-09`](CHECKLIST.md#fg-09) `P0` `development` `E1` — Production service NPS в /zakrytaya-afisha/
- [`FG-13`](CHECKLIST.md#fg-13) `P0` `development` `E1` — Email повышает текущую anonymous identity
- [`FG-14`](CHECKLIST.md#fg-14) `P0` `qa` `E1` — Яндекс OAuth linkIdentity из anonymous focus session
- [`FG-15`](CHECKLIST.md#fg-15) `P0` `development` `E0` — Merge anonymous subject с существующим аккаунтом
- [`FG-24`](CHECKLIST.md#fg-24) `P0` `development` `E0` — Server artifact receipts под anonymous subject
- [`FG-25`](CHECKLIST.md#fg-25) `P0` `development` `E0` — Eligibility 10 из 12 считается server-side
- [`FG-26`](CHECKLIST.md#fg-26) `P0` `development` `E0` — Weighted chances 1–3 считаются server-side
- [`FG-27`](CHECKLIST.md#fg-27) `P0` `development` `E1` — Зафиксировать cutoff 31 августа в 18:00 по Калининграду
- [`FG-28`](CHECKLIST.md#fg-28) `P0` `development` `E0` — Неизменяемый eligible snapshot, розыгрыш и резерв
- [`FG-32`](CHECKLIST.md#fg-32) `P0` `qa` `E0` — Один полный live synthetic focus workflow

**Юридические и публичные документы**

- [`LEGAL-03`](CHECKLIST.md#legal-03) `P0` `design` `E1` — Отдельные согласия на обработку персональных данных по целям
- [`LEGAL-08`](CHECKLIST.md#legal-08) `P0` `design` `E1` — Публичные правила розыгрыша для участников фокус-группы
- [`LEGAL-09`](CHECKLIST.md#legal-09) `P0` `research` `E0` — Юридическая проверка prize/tax/partner-ticket obligations
- [`LEGAL-11`](CHECKLIST.md#legal-11) `P0` `research` `E1` — Аудит локализации и потоков данных по 152-ФЗ для Supabase/Auth/email/profile
- [`LEGAL-12`](CHECKLIST.md#legal-12) `P0` `research` `E0` — Cross-border transfer and foreign processor assessment

**QA, безопасность и аналитика**

- [`QA-05`](CHECKLIST.md#qa-05) `P1` `qa` `E1` — PWA system integration E2E Android+iOS

**UI/UX и визуальная готовность**

- [`UI-07`](CHECKLIST.md#ui-07) `P1` `design` `E1` — Спроектировать UI гастрономической подборки

**Инфраструктура и эксплуатация**

- [`OPS-08`](CHECKLIST.md#ops-08) `P1` `live` `E1` — YDB live RU/billing/cutover/24h observation

**Подборки и каталоги**

- [`COL-11`](CHECKLIST.md#col-11) `P1` `development` `E1` — Astro routes/navigation/sitemap for approved collections

**Статический сайт и публикация**

- [`CORE-13`](CHECKLIST.md#core-13) `P1` `qa` `E1` — Schedule/transport freshness manifest and failed-refresh drill

**Фокус-группа**

- [`FG-21`](CHECKLIST.md#fg-21) `P1` `qa` `E0` — iPhone Add to Home Screen/relaunch

**Инфраструктура и эксплуатация**

- [`OPS-05`](CHECKLIST.md#ops-05) `P2` `integration` `E1` — Weather producer/live binding/provider/bucket

</details>

<details open>
<summary><strong>🧭 Требуется решение владельца</strong> — 9 пунктов, P0: 6</summary>

**UI/UX и визуальная готовность**

- [`UI-02`](CHECKLIST.md#ui-02) `P0` `design` `E4` — Утвердить визуальный вариант публичной заглушки до запуска

**Инфраструктура и эксплуатация**

- [`OPS-12`](CHECKLIST.md#ops-12) `P0` `decision` `E0` — On-call rota and D0 runbooks

**Подборки и каталоги**

- [`COL-13`](CHECKLIST.md#col-13) `P0` `decision` `E1` — Decide whether weak-supply collections are D0 or post-launch

**Статический сайт и публикация**

- [`CORE-12`](CHECKLIST.md#core-12) `P0` `live` `E0` — Public root activation owner gate

**Управление релизом**

- [`GOV-03`](CHECKLIST.md#gov-03) `P0` `decision` `E1` — Утвердить фазовый календарь: фокус-группа → RC → freeze → D0 → D10

**Юридические и публичные документы**

- [`LEGAL-17`](CHECKLIST.md#legal-17) `P0` `decision` `E0` — Final legal GO sign-off

**Исследования и продуктовые решения**

- [`RES-04`](CHECKLIST.md#res-04) `P1` `decision` `E1` — Утвердить стратегию стандартного онбординга
- [`RES-05`](CHECKLIST.md#res-05) `P1` `decision` `E1` — Утвердить каноническую модель Hero-talk

**Коммуникации запуска**

- [`COMMS-02`](CHECKLIST.md#comms-02) `P1` `design` `E1` — Official launch press release

</details>

<details>
<summary><strong>🔎 Исследование / решение / дизайн</strong> — 21 пунктов, P0: 15</summary>

**UI/UX и визуальная готовность**

- [`UI-01`](CHECKLIST.md#ui-01) `P0` `design` `E1` — Создать полный реестр пользовательских поверхностей и UI-статусов
- [`UI-08`](CHECKLIST.md#ui-08) `P0` `design` `E1` — Утвердить UI умного поиска и auth-gate
- [`UI-11`](CHECKLIST.md#ui-11) `P0` `design` `E1` — Спроектировать soft gate и экран ожидания фокус-группы
- [`UI-14`](CHECKLIST.md#ui-14) `P0` `design` `E2` — Утвердить text/screenshot feedback UI и component receipts
- [`UI-16`](CHECKLIST.md#ui-16) `P0` `design` `E1` — Спроектировать capability-specific degraded/offline/error states

**Исследования и продуктовые решения**

- [`RES-06`](CHECKLIST.md#res-06) `P0` `decision` `E1` — Утвердить метрики запуска и фокус-группы
- [`RES-07`](CHECKLIST.md#res-07) `P0` `research` `E1` — Свести каталог подборок и их продуктовые роли
- [`RES-10`](CHECKLIST.md#res-10) `P0` `decision` `E1` — Утвердить критерии качества событий и редакционного каталога на D0

**Коммуникации запуска**

- [`COMMS-01`](CHECKLIST.md#comms-01) `P0` `decision` `E1` — Public positioning and one-sentence value proposition
- [`COMMS-07`](CHECKLIST.md#comms-07) `P0` `design` `E1` — Focus-group onboarding copy and reminder cadence

**Статический сайт и публикация**

- [`CORE-17`](CHECKLIST.md#core-17) `P0` `research` `E1` — Desktop keyboard navigation and event information reading

**Управление релизом**

- [`GOV-01`](CHECKLIST.md#gov-01) `P0` `decision` `E1` — Зафиксировать единый scope публичного релиза 1 сентября
- [`GOV-08`](CHECKLIST.md#gov-08) `P0` `decision` `E1` — Утвердить GO/NO-GO критерии и полномочия rollback

**Юридические и публичные документы**

- [`LEGAL-07`](CHECKLIST.md#legal-07) `P0` `design` `E1` — Публичные условия участия в фокус-группе
- [`LEGAL-15`](CHECKLIST.md#legal-15) `P0` `decision` `E1` — Personal-data incident and notification runbook

**UI/UX и визуальная готовность**

- [`UI-15`](CHECKLIST.md#ui-15) `P1` `design` `E2` — Утвердить PWA install UI для Android и iOS

**Исследования и продуктовые решения**

- [`RES-03`](CHECKLIST.md#res-03) `P1` `research` `E1` — Завершить исследование и выбор редакционного tone of voice
- [`RES-08`](CHECKLIST.md#res-08) `P1` `decision` `E1` — Определить D0 scope артефактов и Клуба друзей Анонсов

**Почта, ящики и шаблоны**

- [`MAIL-20`](CHECKLIST.md#mail-20) `P1` `decision` `E1` — Mailbox/support operating runbook and SLA

**Фокус-группа**

- [`FG-33`](CHECKLIST.md#fg-33) `P1` `decision` `E1` — Операторская triage-процедура feedback и screenshots

**Почта, ящики и шаблоны**

- [`MAIL-16`](CHECKLIST.md#mail-16) `P2` `design` `E1` — Recommendation email with exactly three events

</details>

<details open>
<summary><strong>🛠 Разработка / интеграция</strong> — 23 пунктов, P0: 15</summary>

**QA, безопасность и аналитика**

- [`QA-13`](CHECKLIST.md#qa-13) `P0` `integration` `E1` — KPI event schema and consent-aware analytics

**Инфраструктура и эксплуатация**

- [`OPS-01`](CHECKLIST.md#ops-01) `P0` `integration` `E2` — Capability-specific Yandex resilience contract
- [`OPS-11`](CHECKLIST.md#ops-11) `P0` `integration` `E2` — Monitoring, alerts and synthetic probes

**Персонализация и «Для меня»**

- [`P13N-03`](CHECKLIST.md#p13n-03) `P0` `integration` `E2` — Anonymous local-first profile and strong actions
- [`P13N-07`](CHECKLIST.md#p13n-07) `P0` `integration` `E1` — Profile merge and conflict policy
- [`P13N-08`](CHECKLIST.md#p13n-08) `P0` `integration` `E1` — Consent, channel and suppression state separation

**Почта, ящики и шаблоны**

- [`MAIL-05`](CHECKLIST.md#mail-05) `P0` `integration` `E2` — Transactional outbound application producers and warm-up
- [`MAIL-17`](CHECKLIST.md#mail-17) `P0` `integration` `E2` — Unsubscribe/preferences/suppression UX and backend

**Статический сайт и публикация**

- [`CORE-06`](CHECKLIST.md#core-06) `P0` `integration` `E2` — Атомарная публикация root и проверенный rollback
- [`CORE-10`](CHECKLIST.md#core-10) `P0` `integration` `E3` — Prelaunch/full-catalog robots+sitemap transition

**Умный поиск**

- [`SEARCH-07`](CHECKLIST.md#search-07) `P0` `integration` `E3` — Hosted direct/relay/cache/stage diagnostics E2E

**Фокус-группа**

- [`FG-10`](CHECKLIST.md#fg-10) `P0` `integration` `E2` — Feedback text и private screenshot работают с anonymous subject
- [`FG-12`](CHECKLIST.md#fg-12) `P0` `integration` `E2` — Anonymous personalization работает до identity verification
- [`FG-31`](CHECKLIST.md#fg-31) `P0` `integration` `E2` — Извлекаемая статистика фокус-группы

**Юридические и публичные документы**

- [`LEGAL-14`](CHECKLIST.md#legal-14) `P0` `integration` `E1` — Retention, deletion, withdrawal and data-subject request procedure

**Инфраструктура и эксплуатация**

- [`OPS-07`](CHECKLIST.md#ops-07) `P1` `integration` `E3` — YDB bounded queue/read model code
- [`OPS-14`](CHECKLIST.md#ops-14) `P1` `integration` `E2` — GitHub Actions budget and required-check inventory
- [`OPS-15`](CHECKLIST.md#ops-15) `P1` `integration` `E1` — Kaggle orchestrator/log retention and recoverability

**Коммуникации запуска**

- [`COMMS-03`](CHECKLIST.md#comms-03) `P1` `integration` `E2` — Prelaunch notification acquisition plan

**Подборки и каталоги**

- [`COL-08`](CHECKLIST.md#col-08) `P1` `integration` `E3` — Integrate gastronomy data-prep from current PR #314

**Статический сайт и публикация**

- [`CORE-16`](CHECKLIST.md#core-16) `P1` `integration` `E2` — Medallions/organization identities coverage

**Управление релизом**

- [`GOV-05`](CHECKLIST.md#gov-05) `P1` `integration` `E1` — Провести disposition открытых и устаревших PR

**Юридические и публичные документы**

- [`LEGAL-13`](CHECKLIST.md#legal-13) `P1` `integration` `E1` — Processor/vendor register and contractual safeguards

</details>

<details open>
<summary><strong>🧪 QA / hosted / live evidence</strong> — 46 пунктов, P0: 43</summary>

**QA, безопасность и аналитика**

- [`QA-07`](CHECKLIST.md#qa-07) `P0` `qa` `E2` — Performance/Core Web Vitals budget on representative devices
- [`QA-08`](CHECKLIST.md#qa-08) `P0` `qa` `E2` — RLS/ACL/security review all public Supabase surfaces
- [`QA-09`](CHECKLIST.md#qa-09) `P0` `qa` `E3` — Backup/restore and rollback drills
- [`QA-10`](CHECKLIST.md#qa-10) `P0` `qa` `E2` — Load/rate/anti-abuse tests
- [`QA-11`](CHECKLIST.md#qa-11) `P0` `qa` `E3` — Secret/PII redaction in logs and artifacts
- [`QA-15`](CHECKLIST.md#qa-15) `P0` `qa` `E2` — Data-quality checks for events, sources and media

**UI/UX и визуальная готовность**

- [`UI-03`](CHECKLIST.md#ui-03) `P0` `integration` `E4` — Довести выбранную prelaunch-заглушку до mergeable production implementation
- [`UI-04`](CHECKLIST.md#ui-04) `P0` `design` `E3` — Утвердить общий UI каталога и главной страницы на D0
- [`UI-05`](CHECKLIST.md#ui-05) `P0` `qa` `E3` — Утвердить event detail UI и все состояния события
- [`UI-09`](CHECKLIST.md#ui-09) `P0` `design` `E2` — Утвердить UI «Для меня» и правду персонализации
- [`UI-10`](CHECKLIST.md#ui-10) `P0` `qa` `E4` — Утвердить email/Yandex auth UI на desktop и mobile
- [`UI-18`](CHECKLIST.md#ui-18) `P0` `qa` `E3` — Закрыть responsive UI matrix по ключевым route families

**Авторизация и identity**

- [`AUTH-01`](CHECKLIST.md#auth-01) `P0` `qa` `E3` — No-mail authenticated session fixture
- [`AUTH-02`](CHECKLIST.md#auth-02) `P0` `qa` `E4` — Browser real-mail OTP on final target
- [`AUTH-05`](CHECKLIST.md#auth-05) `P0` `qa` `E3` — Yandex OAuth ordinary verified session
- [`AUTH-10`](CHECKLIST.md#auth-10) `P0` `qa` `E2` — Anti-abuse and rate limits for anonymous/Auth surfaces

**Инфраструктура и эксплуатация**

- [`OPS-03`](CHECKLIST.md#ops-03) `P0` `live` `E3` — Supabase migration inventory and production apply ledger
- [`OPS-09`](CHECKLIST.md#ops-09) `P0` `qa` `E4` — CDN/Object Storage/DNS/TLS final verification
- [`OPS-13`](CHECKLIST.md#ops-13) `P0` `qa` `E4` — Fly runtime health/capacity/disk guard

**Коммуникации запуска**

- [`COMMS-09`](CHECKLIST.md#comms-09) `P0` `qa` `E3` — SEO/OG/unfurl copy and images

**Персонализация и «Для меня»**

- [`P13N-05`](CHECKLIST.md#p13n-05) `P0` `qa` `E3` — Like/share/hide/calendar action consistency
- [`P13N-06`](CHECKLIST.md#p13n-06) `P0` `qa` `E2` — Authenticated «Для меня» generated journey

**Подборки и каталоги**

- [`COL-07`](CHECKLIST.md#col-07) `P0` `qa` `E3` — Verify exhibitions/festivals/popular/unusual routes

**Почта, ящики и шаблоны**

- [`MAIL-08`](CHECKLIST.md#mail-08) `P0` `qa` `E4` — Final SPF/DKIM/DMARC/DNS verification
- [`MAIL-10`](CHECKLIST.md#mail-10) `P0` `qa` `E4` — OTP email template approved and tested
- [`MAIL-21`](CHECKLIST.md#mail-21) `P0` `integration` `E2` — Prelaunch subscriber queue migration and cleanup policy

**Статический сайт и публикация**

- [`CORE-03`](CHECKLIST.md#core-03) `P0` `live` `E3` — Подтвердить current public root SHA/version и факт promotion
- [`CORE-05`](CHECKLIST.md#core-05) `P0` `qa` `E4` — Full catalog browser gate на свежем main snapshot
- [`CORE-07`](CHECKLIST.md#core-07) `P0` `qa` `E3` — Freshness/outbox/catch-up и max-staleness drill
- [`CORE-11`](CHECKLIST.md#core-11) `P0` `qa` `E4` — Stable ICS publication and non-regression
- [`CORE-15`](CHECKLIST.md#core-15) `P0` `qa` `E2` — Media deduplication and broken-image release gate

**Умный поиск**

- [`SEARCH-04`](CHECKLIST.md#search-04) `P0` `integration` `E2` — Exact-origin CORS and shared transport contract
- [`SEARCH-05`](CHECKLIST.md#search-05) `P0` `qa` `E3` — Authenticated session-fixture product journey
- [`SEARCH-06`](CHECKLIST.md#search-06) `P0` `qa` `E2` — Rolling quota, one-in-flight and global cost circuit

**Управление релизом**

- [`GOV-04`](CHECKLIST.md#gov-04) `P0` `live` `E4` — Вести точную цепочку main SHA → build → candidate → public root

**Фокус-группа**

- [`FG-03`](CHECKLIST.md#fg-03) `P0` `qa` `E2` — Invite/QR intake, marker и очистка URL fragment
- [`FG-11`](CHECKLIST.md#fg-11) `P0` `qa` `E2` — Offline/idempotent feedback outbox без потери текста
- [`FG-16`](CHECKLIST.md#fg-16) `P0` `qa` `E2` — Anonymous feedback разрешён, raffle eligibility=false
- [`FG-17`](CHECKLIST.md#fg-17) `P0` `qa` `E3` — Verified participant registration и cap=200
- [`FG-18`](CHECKLIST.md#fg-18) `P0` `qa` `E4` — Browser/Android real-mail OTP на финальном target
- [`FG-19`](CHECKLIST.md#fg-19) `P0` `qa` `E4` — iOS Safari real-mail OTP на финальном target
- [`FG-22`](CHECKLIST.md#fg-22) `P0` `qa` `E2` — Share/Calendar/Не интересно/Для меня в одном focus journey

**Юридические и публичные документы**

- [`LEGAL-10`](CHECKLIST.md#legal-10) `P0` `live` `E0` — Уведомление Роскомнадзора/актуальность сведений оператора

**Инфраструктура и эксплуатация**

- [`OPS-16`](CHECKLIST.md#ops-16) `P1` `qa` `E2` — Public diagnostic page capability truth

**Статический сайт и публикация**

- [`CORE-14`](CHECKLIST.md#core-14) `P1` `qa` `E2` — Past-event archive/CTA/SEO behavior

**Фокус-группа**

- [`FG-20`](CHECKLIST.md#fg-20) `P1` `qa` `E2` — Android PWA install/relaunch сохраняет marker и subject

</details>

<details>
<summary><strong>○ Очередь</strong> — 69 пунктов, P0: 42</summary>

**D0 — 1 сентября**

- [`D0-01`](CHECKLIST.md#d0-01) `P0` `live` `E0` — Freeze exact main SHA and dependency versions
- [`D0-02`](CHECKLIST.md#d0-02) `P0` `live` `E0` — Apply required production migrations with backups
- [`D0-03`](CHECKLIST.md#d0-03) `P0` `live` `E0` — Build final full-catalog artifact and run all blocking gates
- [`D0-04`](CHECKLIST.md#d0-04) `P0` `live` `E0` — Atomic publish full catalog
- [`D0-05`](CHECKLIST.md#d0-05) `P0` `live` `E0` — Publish full robots.txt and sitemap.xml with catalog
- [`D0-06`](CHECKLIST.md#d0-06) `P0` `live` `E0` — Production smoke: root/routes/assets/ICS/auth/search/feedback
- [`D0-07`](CHECKLIST.md#d0-07) `P0` `live` `E0` — Send one-time launch notification exactly once
- [`D0-08`](CHECKLIST.md#d0-08) `P0` `live` `E0` — Activate launch monitoring and operator room
- [`D0-09`](CHECKLIST.md#d0-09) `P0` `live` `E0` — Rollback decision and rehearsal evidence available
- [`D0-10`](CHECKLIST.md#d0-10) `P0` `live` `E0` — Publish launch communications
- [`D0-11`](CHECKLIST.md#d0-11) `P0` `live` `E0` — Record immutable release receipt
- [`D0-12`](CHECKLIST.md#d0-12) `P0` `live` `E0` — Start Telegraph D0 coexistence mode

**QA, безопасность и аналитика**

- [`QA-06`](CHECKLIST.md#qa-06) `P0` `qa` `E0` — Аудит доступности и исправление критических дефектов
- [`QA-12`](CHECKLIST.md#qa-12) `P0` `qa` `E0` — Incident/support rehearsal
- [`QA-17`](CHECKLIST.md#qa-17) `P0` `qa` `E0` — Final target exact-SHA evidence bundle
- [`QA-18`](CHECKLIST.md#qa-18) `P0` `qa` `E0` — Focus feedback data-quality/duplicate reconciliation

**UI/UX и визуальная готовность**

- [`UI-19`](CHECKLIST.md#ui-19) `P0` `qa` `E0` — Закрыть keyboard/screen-reader/focus-order аудит

**Авторизация и identity**

- [`AUTH-08`](CHECKLIST.md#auth-08) `P0` `design` `E0` — Account unlink/delete/export and data-subject flows

**Инфраструктура и эксплуатация**

- [`OPS-17`](CHECKLIST.md#ops-17) `P0` `decision` `E0` — Incident backlog and known-debt review before freeze

**Коммуникации запуска**

- [`COMMS-04`](CHECKLIST.md#comms-04) `P0` `design` `E0` — Launch-day Telegram/VK posts and visual assets
- [`COMMS-06`](CHECKLIST.md#comms-06) `P0` `design` `E0` — Support macros for launch incidents
- [`COMMS-10`](CHECKLIST.md#comms-10) `P0` `qa` `E0` — Launch notification send list and suppression reconciliation

**Персонализация и «Для меня»**

- [`P13N-09`](CHECKLIST.md#p13n-09) `P0` `integration` `E0` — Find-interest-within-30 measurement

**После запуска**

- [`POST-01`](CHECKLIST.md#post-01) `P0` `live` `E0` — D+1 health and incident review
- [`POST-04`](CHECKLIST.md#post-04) `P0` `decision` `E0` — D+10 Telegraph cutover decision
- [`POST-05`](CHECKLIST.md#post-05) `P0` `live` `E0` — Prelaunch/focus retention cleanup

**Почта, ящики и шаблоны**

- [`MAIL-09`](CHECKLIST.md#mail-09) `P0` `decision` `E0` — Canonical inventory of email purposes, From/Reply-To and templates
- [`MAIL-12`](CHECKLIST.md#mail-12) `P0` `design` `E0` — One-time launch notification template
- [`MAIL-13`](CHECKLIST.md#mail-13) `P0` `design` `E0` — Focus invite and reminder templates
- [`MAIL-14`](CHECKLIST.md#mail-14) `P0` `design` `E0` — Focus winner/alternate/thank-you templates
- [`MAIL-22`](CHECKLIST.md#mail-22) `P0` `qa` `E0` — Exact send counters and no-duplicate launch dispatch

**Умный поиск**

- [`SEARCH-09`](CHECKLIST.md#search-09) `P0` `live` `E0` — Search freshness/corpus receipt monitoring

**Управление релизом**

- [`GOV-02`](CHECKLIST.md#gov-02) `P0` `decision` `E0` — Назначить release owner и заместителя на D0

**Фокус-группа**

- [`FG-29`](CHECKLIST.md#fg-29) `P0` `design` `E0` — Winner/alternate notification и 3-day claim lifecycle
- [`FG-34`](CHECKLIST.md#fg-34) `P0` `qa` `E0` — Focus-group close rehearsal до 31 августа

**Юридические и публичные документы**

- [`LEGAL-01`](CHECKLIST.md#legal-01) `P0` `design` `E0` — Публичные сведения об операторе и контакт для обращений
- [`LEGAL-02`](CHECKLIST.md#legal-02) `P0` `design` `E0` — Политика обработки персональных данных
- [`LEGAL-04`](CHECKLIST.md#legal-04) `P0` `design` `E0` — Отдельное согласие на информационные/рекламные рассылки
- [`LEGAL-05`](CHECKLIST.md#legal-05) `P0` `design` `E0` — Cookies/localStorage/analytics notice
- [`LEGAL-06`](CHECKLIST.md#legal-06) `P0` `design` `E0` — Пользовательское соглашение/условия использования
- [`LEGAL-18`](CHECKLIST.md#legal-18) `P0` `development` `E0` — Versioned registry of legal copy and consent evidence
- [`LEGAL-20`](CHECKLIST.md#legal-20) `P0` `qa` `E0` — Public privacy/contact links present on every relevant surface

**QA, безопасность и аналитика**

- [`QA-14`](CHECKLIST.md#qa-14) `P1` `development` `E0` — Launch/focus dashboard for product and reliability metrics
- [`QA-16`](CHECKLIST.md#qa-16) `P1` `qa` `E0` — Cross-browser desktop smoke

**UI/UX и визуальная готовность**

- [`UI-17`](CHECKLIST.md#ui-17) `P1` `design` `E0` — Создать визуальную систему email-шаблонов
- [`UI-20`](CHECKLIST.md#ui-20) `P1` `design` `E0` — Вести реестр утверждённых макетов и implemented parity
- [`UI-22`](CHECKLIST.md#ui-22) `P1` `qa` `E0` — Проверить copy и визуальную иерархию пустых списков

**Авторизация и identity**

- [`AUTH-07`](CHECKLIST.md#auth-07) `P1` `design` `E0` — Account recovery and cross-device restoration

**Исследования и продуктовые решения**

- [`RES-09`](CHECKLIST.md#res-09) `P1` `research` `E0` — Определить сервисную модель поддержки пользователей

**Коммуникации запуска**

- [`COMMS-05`](CHECKLIST.md#comms-05) `P1` `design` `E0` — Partner/venue launch briefing
- [`COMMS-08`](CHECKLIST.md#comms-08) `P1` `decision` `E0` — Public status/incident communication channel

**Персонализация и «Для меня»**

- [`P13N-10`](CHECKLIST.md#p13n-10) `P1` `qa` `E0` — Longitudinal personalization mutation tests

**Подборки и каталоги**

- [`COL-09`](CHECKLIST.md#col-09) `P1` `decision` `E0` — Owner review real gastronomy candidate families
- [`COL-10`](CHECKLIST.md#col-10) `P1` `decision` `E0` — Owner gold and quality baseline for semantic collections
- [`COL-12`](CHECKLIST.md#col-12) `P1` `qa` `E0` — Collections product browser smoke on public candidate

**После запуска**

- [`POST-02`](CHECKLIST.md#post-02) `P1` `decision` `E0` — D+3 focus-group findings and prioritized backlog
- [`POST-03`](CHECKLIST.md#post-03) `P1` `live` `E0` — D+7 KPI and retention review
- [`POST-06`](CHECKLIST.md#post-06) `P1` `decision` `E0` — Release retrospective and dashboard standardization
- [`POST-07`](CHECKLIST.md#post-07) `P1` `decision` `E0` — Enable deferred features only through own gates

**Почта, ящики и шаблоны**

- [`MAIL-11`](CHECKLIST.md#mail-11) `P1` `design` `E0` — Prelaunch subscription confirmation copy
- [`MAIL-15`](CHECKLIST.md#mail-15) `P1` `design` `E0` — Transactional save/reminder/cancel/reschedule templates
- [`MAIL-18`](CHECKLIST.md#mail-18) `P1` `qa` `E0` — Email client rendering/accessibility QA

**Умный поиск**

- [`SEARCH-08`](CHECKLIST.md#search-08) `P1` `qa` `E0` — Mobile search acceptance

**Управление релизом**

- [`GOV-07`](CHECKLIST.md#gov-07) `P1` `decision` `E0` — Вести decision log, risk register и владельцев блокеров
- [`GOV-10`](CHECKLIST.md#gov-10) `P1` `decision` `E0` — Создать единый список owner-gates для визуальных, продуктовых и юридических решений

**Фокус-группа**

- [`FG-30`](CHECKLIST.md#fg-30) `P1` `design` `E0` — Thank-you mail и публичное объявление результата

**Юридические и публичные документы**

- [`LEGAL-16`](CHECKLIST.md#legal-16) `P1` `design` `E0` — Event information disclaimer, age marking and content rights
- [`LEGAL-19`](CHECKLIST.md#legal-19) `P1` `research` `E0` — Minors/child-directed data and participation rules

**Инфраструктура и эксплуатация**

- [`OPS-06`](CHECKLIST.md#ops-06) `P2` `live` `E0` — Weather seven-day canary

</details>

<details>
<summary><strong>✅ Готово</strong> — 20 пунктов, P0: 18</summary>

**QA, безопасность и аналитика**

- [`QA-01`](CHECKLIST.md#qa-01) `P0` `ready` `E2` — Central machine-readable static-site scenario registry
- [`QA-02`](CHECKLIST.md#qa-02) `P0` `ready` `E3` — Production build and browser release gates
- [`QA-04`](CHECKLIST.md#qa-04) `P0` `ready` `E4` — Mobile OTP direct/relay fault matrix

**Авторизация и identity**

- [`AUTH-03`](CHECKLIST.md#auth-03) `P0` `ready` `E4` — Android real-mail OTP direct/relay acceptance
- [`AUTH-04`](CHECKLIST.md#auth-04) `P0` `ready` `E4` — iOS native-first real-mail OTP direct/relay acceptance

**Исследования и продуктовые решения**

- [`RES-01`](CHECKLIST.md#res-01) `P0` `ready` `E1` — Канонизировать модель фокус-группы и eligibility
- [`RES-02`](CHECKLIST.md#res-02) `P0` `ready` `E1` — Сохранить честный NO-GO baseline release plan

**Персонализация и «Для меня»**

- [`P13N-01`](CHECKLIST.md#p13n-01) `P0` `ready` `E3` — P13N-00 seam and route inventory

**Почта, ящики и шаблоны**

- [`MAIL-01`](CHECKLIST.md#mail-01) `P0` `ready` `E5` — Human mailbox info@kenigevents.ru is live
- [`MAIL-02`](CHECKLIST.md#mail-02) `P0` `ready` `E4` — DMARC mailbox dmarc@kenigevents.ru is live
- [`MAIL-03`](CHECKLIST.md#mail-03) `P0` `ready` `E5` — SpaceWeb IMAP → Yandex inbound pipeline
- [`MAIL-04`](CHECKLIST.md#mail-04) `P0` `ready` `E5` — Postbox identity and feedback path

**Статический сайт и публикация**

- [`CORE-01`](CHECKLIST.md#core-01) `P0` `ready` `E5` — StaticSiteBuilder incident recovery merged and evidenced
- [`CORE-04`](CHECKLIST.md#core-04) `P0` `ready` `E3` — Generated route/runtime inventory на общих поверхностях

**Умный поиск**

- [`SEARCH-01`](CHECKLIST.md#search-01) `P0` `ready` `E1` — Canonical search contract and scenario registry

**Управление релизом**

- [`GOV-06`](CHECKLIST.md#gov-06) `P0` `ready` `E2` — Обновлять сводный checklist каждые 2–3 дня
- [`GOV-09`](CHECKLIST.md#gov-09) `P0` `ready` `E1` — Зафиксировать политику: PR/код не равны production-ready без terminal evidence

**Фокус-группа**

- [`FG-23`](CHECKLIST.md#fg-23) `P0` `ready` `E1` — Определены 12 артефактов FG-E01…FG-E12

**Инфраструктура и эксплуатация**

- [`OPS-04`](CHECKLIST.md#ops-04) `P1` `ready` `E3` — Weather consumer remains safe default-off

**Почта, ящики и шаблоны**

- [`MAIL-06`](CHECKLIST.md#mail-06) `P1` `ready` `E4` — NotiSend sender/domain verification

</details>

<details>
<summary><strong>⏸ Отложено</strong> — 2 пунктов, P0: 0</summary>

**Персонализация и «Для меня»**

- [`P13N-02`](CHECKLIST.md#p13n-02) `P2` `research` `E1` — P13N-01…P13N-06 model expansion

**Подборки и каталоги**

- [`COL-14`](CHECKLIST.md#col-14) `P2` `decision` `E1` — Gastronomy route UI publication decision

</details>

## Опциональный GitHub Project

После 1–2 циклов обновления этот источник можно синхронизировать с бесплатным GitHub Project. Рекомендуемые поля: `Status`, `Priority`, `Phase`, `Stage`, `Evidence`, `Target`, `Owner`, `Blocked by`, `Release`. До стандартизации Markdown/TOML остаются единственным источником правды, чтобы не возникло двух расходящихся досок.
