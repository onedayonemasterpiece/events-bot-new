<!-- GENERATED: edit checklist.toml, not this file. -->
# Запуск «Полюбить Калининград · Анонсы» — детальный checklist

> Срез 2026-08-04 · verdict **NO-GO** · [сводка](README.md) · [kanban](KANBAN.md) · [обновление](UPDATE.md)

Легенда: `E0` нет evidence · `E1` документ/решение · `E2` код/unit · `E3` интеграция/browser · `E4` hosted/candidate/device · `E5` production/live/soak.

<a id="stream-01"></a>
## Управление релизом

**🟠 OPEN** · P0 2/7 · blocked 0 · verify 1 · owner gate 1

<a id="gov-03"></a>
- [ ] **`GOV-03` · Утвердить фазовый календарь: фокус-группа → RC → freeze → D0 → D10**  
  `P0` · `LAUNCH_PREP` · `decision` · `E1` · **🧭 решение владельца** · target `2026-08-07`
  - **Владелец:** Product owner
  - **Следующий шаг:** Подтвердить предложенные окна и реальные даты старта фокус-группы.
  - **Источник/evidence:** `docs/release/2026-09-01/README.md`

<a id="gov-02"></a>
- [ ] **`GOV-02` · Назначить release owner и заместителя на D0**  
  `P0` · `LAUNCH_PREP` · `decision` · `E0` · **○ не начато** · target `2026-08-07`
  - **Владелец:** Product owner
  - **Следующий шаг:** Назначить ответственных за GO/NO-GO, rollback и коммуникации.
  - **Источник/evidence:** `—`

<a id="gov-01"></a>
- [ ] **`GOV-01` · Зафиксировать единый scope публичного релиза 1 сентября**  
  `P0` · `LAUNCH_PREP` · `decision` · `E1` · **🛠 в работе** · target `2026-08-07`
  - **Владелец:** Product owner
  - **Следующий шаг:** Утвердить must-have, допустимые default-off контуры и post-launch backlog.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`
  - **Примечание:** Текущий release plan шире календарного запуска и содержит исторические evidence.

<a id="gov-08"></a>
- [ ] **`GOV-08` · Утвердить GO/NO-GO критерии и полномочия rollback**  
  `P0` · `STABILIZATION` · `decision` · `E1` · **🛠 в работе** · target `2026-08-20`
  - **Владелец:** Product owner + Ops
  - **Следующий шаг:** Свести P0 gates в один подписываемый D0 decision record.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="gov-04"></a>
- [ ] **`GOV-04` · Вести точную цепочку main SHA → build → candidate → public root**  
  `P0` · `LAUNCH_PREP` · `live` · `E4` · **🧪 требует проверки** · target `2026-08-09`
  - **Владелец:** Release owner
  - **Следующий шаг:** Закрыть read-only production-аудит и записать current root/candidate identities.
  - **Источник/evidence:** `PR #322; docs/features/static-site-pages/release-plan.md`

<a id="gov-06"></a>
- [x] **`GOV-06` · Обновлять сводный checklist каждые 2–3 дня**  
  `P0` · `LAUNCH_PREP` · `ready` · `E2` · **✅ готово** · target `2026-08-04`
  - **Владелец:** Release owner
  - **Следующий шаг:** Следующее обновление — не позднее 7 августа.
  - **Источник/evidence:** `docs/release/2026-09-01/UPDATE.md`

<a id="gov-09"></a>
- [x] **`GOV-09` · Зафиксировать политику: PR/код не равны production-ready без terminal evidence**  
  `P0` · `LAUNCH_PREP` · `ready` · `E1` · **✅ готово** · target `2026-08-04`
  - **Владелец:** Release owner
  - **Следующий шаг:** Применять правило ко всем строкам этого checklist.
  - **Источник/evidence:** `docs/features/static-site-pages/release-autotest-gates.md`

<a id="gov-07"></a>
- [ ] **`GOV-07` · Вести decision log, risk register и владельцев блокеров**  
  `P1` · `LAUNCH_PREP` · `decision` · `E0` · **○ не начато** · target `2026-08-08`
  - **Владелец:** Release owner
  - **Следующий шаг:** Добавлять решения и риски в checklist source, не в отдельные несвязанные заметки.
  - **Источник/evidence:** `docs/release/2026-09-01/README.md`

<a id="gov-10"></a>
- [ ] **`GOV-10` · Создать единый список owner-gates для визуальных, продуктовых и юридических решений**  
  `P1` · `LAUNCH_PREP` · `decision` · `E0` · **○ не начато** · target `2026-08-08`
  - **Владелец:** Product owner
  - **Следующий шаг:** Назначить даты решений и запретить молчаливое продвижение кандидатов.
  - **Источник/evidence:** `docs/release/2026-09-01/CHECKLIST.md`

<a id="gov-05"></a>
- [ ] **`GOV-05` · Провести disposition открытых и устаревших PR**  
  `P1` · `LAUNCH_PREP` · `integration` · `E1` · **🛠 в работе** · target `2026-08-10`
  - **Владелец:** Release owner
  - **Следующий шаг:** Сравнить #250, #270, #287, #295 с main; закрыть superseded только после exact compare.
  - **Источник/evidence:** `PR #323`

<a id="stream-02"></a>
## Исследования и продуктовые решения

**🔵 IN PROGRESS** · P0 2/5 · blocked 0 · verify 0 · owner gate 2

<a id="res-06"></a>
- [ ] **`RES-06` · Утвердить метрики запуска и фокус-группы**  
  `P0` · `FG_PREP` · `decision` · `E1` · **🛠 в работе** · target `2026-08-08`
  - **Владелец:** Product + Analytics
  - **Следующий шаг:** Зафиксировать activation, find-interest-within-30, page score, service NPS, retention и reliability guardrails.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/README.md`

<a id="res-07"></a>
- [ ] **`RES-07` · Свести каталог подборок и их продуктовые роли**  
  `P0` · `LAUNCH_PREP` · `research` · `E1` · **🛠 в работе** · target `2026-08-10`
  - **Владелец:** Product + Editorial
  - **Следующий шаг:** Подтвердить обязательные, blocked и post-launch подборки.
  - **Источник/evidence:** `docs/features/static-site-pages/podborki.md; PR #323`

<a id="res-10"></a>
- [ ] **`RES-10` · Утвердить критерии качества событий и редакционного каталога на D0**  
  `P0` · `LAUNCH_PREP` · `decision` · `E1` · **🛠 в работе** · target `2026-08-12`
  - **Владелец:** Editorial + Product
  - **Следующий шаг:** Определить freshness, completeness, duplicates, broken media и manual-review thresholds.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="res-01"></a>
- [x] **`RES-01` · Канонизировать модель фокус-группы и eligibility**  
  `P0` · `FG_PREP` · `ready` · `E1` · **✅ готово** · target `2026-08-04`
  - **Владелец:** Product owner
  - **Следующий шаг:** Не менять продуктовый контракт без явного owner decision.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group.md`

<a id="res-02"></a>
- [x] **`RES-02` · Сохранить честный NO-GO baseline release plan**  
  `P0` · `LAUNCH_PREP` · `ready` · `E1` · **✅ готово** · target `2026-08-04`
  - **Владелец:** Release owner
  - **Следующий шаг:** Обновлять только фактами main/live.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="res-04"></a>
- [ ] **`RES-04` · Утвердить стратегию стандартного онбординга**  
  `P1` · `LAUNCH_PREP` · `decision` · `E1` · **🧭 решение владельца** · target `2026-08-10`
  - **Владелец:** Product owner
  - **Следующий шаг:** Выбрать baseline A/B и зафиксировать release boundary артефактов/клуба.
  - **Источник/evidence:** `PR #288`

<a id="res-05"></a>
- [ ] **`RES-05` · Утвердить каноническую модель Hero-talk**  
  `P1` · `LAUNCH_PREP` · `decision` · `E1` · **🧭 решение владельца** · target `2026-08-10`
  - **Владелец:** Product owner
  - **Следующий шаг:** Принять placements, цепочки и минимальный D0 scope.
  - **Источник/evidence:** `PR #291`

<a id="res-09"></a>
- [ ] **`RES-09` · Определить сервисную модель поддержки пользователей**  
  `P1` · `LAUNCH_PREP` · `research` · `E0` · **○ не начато** · target `2026-08-18`
  - **Владелец:** Product + Support
  - **Следующий шаг:** Описать каналы, SLA, типовые обращения, escalation и incident messaging.
  - **Источник/evidence:** `—`

<a id="res-03"></a>
- [ ] **`RES-03` · Завершить исследование и выбор редакционного tone of voice**  
  `P1` · `LAUNCH_PREP` · `research` · `E1` · **🛠 в работе** · target `2026-08-12`
  - **Владелец:** Editorial + Product
  - **Следующий шаг:** Получить корпуса, сравнительную оценку и утвердить editorial standard v1.
  - **Источник/evidence:** `PR #286`

<a id="res-08"></a>
- [ ] **`RES-08` · Определить D0 scope артефактов и Клуба друзей Анонсов**  
  `P1` · `LAUNCH_PREP` · `decision` · `E1` · **🛠 в работе** · target `2026-08-14`
  - **Владелец:** Product owner
  - **Следующий шаг:** Разделить onboarding value, membership и raffle claims.
  - **Источник/evidence:** `PR #288; PR #291`

<a id="stream-03"></a>
## UI/UX и визуальная готовность

**🔴 BLOCKED** · P0 0/17 · blocked 5 · verify 5 · owner gate 1

<a id="ui-06"></a>
- [ ] **`UI-06` · Спроектировать UI полного каталога подборок**  
  `P0` · `LAUNCH_PREP` · `design` · `E1` · **⛔ заблокировано** · target `2026-08-16`
  - **Владелец:** Design + Product
  - **Следующий шаг:** Начать после канонизации registry и обязательных routes.
  - **Источник/evidence:** `PR #323`
  - **Примечание:** COL-01

<a id="ui-12"></a>
- [ ] **`UI-12` · Спроектировать page score: unanswered/answered/revision changed**  
  `P0` · `FG_PREP` · `design` · `E1` · **⛔ заблокировано** · target `2026-08-08`
  - **Владелец:** Design + Product
  - **Следующий шаг:** Собрать финальный UI и copy до implementation.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/nps-ui.md`
  - **Примечание:** FG-07

<a id="ui-13"></a>
- [ ] **`UI-13` · Спроектировать общий service NPS в participant hub**  
  `P0` · `FG_PREP` · `design` · `E1` · **⛔ заблокировано** · target `2026-08-09`
  - **Владелец:** Design + Product
  - **Следующий шаг:** Утвердить placement, save/edit и optional feedback states.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/nps-ui.md`
  - **Примечание:** FG-09

<a id="ui-21"></a>
- [ ] **`UI-21` · Спроектировать UI юридических согласий без consent wall**  
  `P0` · `LAUNCH_PREP` · `design` · `E0` · **⛔ заблокировано** · target `2026-08-18`
  - **Владелец:** Design + Legal
  - **Следующий шаг:** Сначала получить purpose-by-purpose legal copy.
  - **Источник/evidence:** `LEGAL-03; LEGAL-04`
  - **Примечание:** LEGAL-03

<a id="ui-02"></a>
- [ ] **`UI-02` · Утвердить визуальный вариант публичной заглушки до запуска**  
  `P0` · `PRELAUNCH` · `design` · `E4` · **🧭 решение владельца** · target `2026-08-06`
  - **Владелец:** Product owner + Design
  - **Следующий шаг:** Выбрать один кандидат и зафиксировать эталонные screenshots.
  - **Источник/evidence:** `PR #296; PR #313; PR #318`

<a id="ui-19"></a>
- [ ] **`UI-19` · Закрыть keyboard/screen-reader/focus-order аудит**  
  `P0` · `RC` · `qa` · `E0` · **○ не начато** · target `2026-08-24`
  - **Владелец:** Accessibility + QA
  - **Следующий шаг:** Проверить landmarks, skip links, focus visibility, dialogs, carousel/details reading.
  - **Источник/evidence:** `—`

<a id="ui-01"></a>
- [ ] **`UI-01` · Создать полный реестр пользовательских поверхностей и UI-статусов**  
  `P0` · `LAUNCH_PREP` · `design` · `E1` · **🛠 в работе** · target `2026-08-08`
  - **Владелец:** Design + Frontend
  - **Следующий шаг:** Связать route/page family с design approved, implemented, QA и live states.
  - **Источник/evidence:** `docs/release/2026-09-01/CHECKLIST.md`

<a id="ui-08"></a>
- [ ] **`UI-08` · Утвердить UI умного поиска и auth-gate**  
  `P0` · `LAUNCH_PREP` · `design` · `E1` · **🛠 в работе** · target `2026-08-13`
  - **Владелец:** Design + Product
  - **Следующий шаг:** Проверить query, loading, quota, no-results, degraded и auth states.
  - **Источник/evidence:** `docs/features/static-site-pages/smart-vector-search/README.md`

<a id="ui-11"></a>
- [ ] **`UI-11` · Спроектировать soft gate и экран ожидания фокус-группы**  
  `P0` · `FG_PREP` · `design` · `E1` · **🛠 в работе** · target `2026-08-07`
  - **Владелец:** Design + Frontend
  - **Следующий шаг:** Зафиксировать locked/no-JS/invalid-invite/expired states.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/README.md`

<a id="ui-14"></a>
- [ ] **`UI-14` · Утвердить text/screenshot feedback UI и component receipts**  
  `P0` · `FG_PREP` · `design` · `E2` · **🛠 в работе** · target `2026-08-09`
  - **Владелец:** Design + Frontend
  - **Следующий шаг:** Добавить queued/sent/partial/error/retry states.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`

<a id="ui-16"></a>
- [ ] **`UI-16` · Спроектировать capability-specific degraded/offline/error states**  
  `P0` · `LAUNCH_PREP` · `design` · `E1` · **🛠 в работе** · target `2026-08-14`
  - **Владелец:** Design + Frontend
  - **Следующий шаг:** Не показывать общий «Яндекс не работает» при частичном отказе.
  - **Источник/evidence:** `docs/operations/yandex-dependency-resilience.md`

<a id="ui-18"></a>
- [ ] **`UI-18` · Закрыть responsive UI matrix по ключевым route families**  
  `P0` · `RC` · `qa` · `E3` · **🛠 в работе** · target `2026-08-24`
  - **Владелец:** QA + Frontend
  - **Следующий шаг:** Проверить 390×844, 768, 1440×900 и large desktop.
  - **Источник/evidence:** `docs/features/static-site-pages/release-autotest-gates.md`

<a id="ui-03"></a>
- [ ] **`UI-03` · Довести выбранную prelaunch-заглушку до mergeable production implementation**  
  `P0` · `PRELAUNCH` · `integration` · `E4` · **🧪 требует проверки** · target `2026-08-08`
  - **Владелец:** Frontend + Backend
  - **Следующий шаг:** Сверить migrations, final browser gates, root-only SEO и merge disposition.
  - **Источник/evidence:** `PR #296`

<a id="ui-04"></a>
- [ ] **`UI-04` · Утвердить общий UI каталога и главной страницы на D0**  
  `P0` · `LAUNCH_PREP` · `design` · `E3` · **🧪 требует проверки** · target `2026-08-14`
  - **Владелец:** Product owner + Design
  - **Следующий шаг:** Провести owner review по актуальному main-based candidate, а не историческому preview.
  - **Источник/evidence:** `PR #316; secret candidates`

<a id="ui-05"></a>
- [ ] **`UI-05` · Утвердить event detail UI и все состояния события**  
  `P0` · `LAUNCH_PREP` · `qa` · `E3` · **🧪 требует проверки** · target `2026-08-14`
  - **Владелец:** Product owner + Design
  - **Следующий шаг:** Проверить актуальное, завершённое, отменённое, перенесённое, без изображения и multi-image.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="ui-09"></a>
- [ ] **`UI-09` · Утвердить UI «Для меня» и правду персонализации**  
  `P0` · `LAUNCH_PREP` · `design` · `E2` · **🧪 требует проверки** · target `2026-08-15`
  - **Владелец:** Design + Product
  - **Следующий шаг:** Сверить static fallback, authenticated state, empty state и объяснимость.
  - **Источник/evidence:** `docs/features/static-site-pages/personalizaion/personalization-to-be.md`

<a id="ui-10"></a>
- [ ] **`UI-10` · Утвердить email/Yandex auth UI на desktop и mobile**  
  `P0` · `FG_PREP` · `qa` · `E4` · **🧪 требует проверки** · target `2026-08-10`
  - **Владелец:** Design + QA
  - **Следующий шаг:** Review актуального target после anonymous-upgrade изменений.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`

<a id="ui-07"></a>
- [ ] **`UI-07` · Спроектировать UI гастрономической подборки**  
  `P1` · `LAUNCH_PREP` · `design` · `E1` · **⛔ заблокировано** · target `2026-08-18`
  - **Владелец:** Design + Product
  - **Следующий шаг:** Определить empty/low-supply/dormant states после owner review данных.
  - **Источник/evidence:** `PR #314`
  - **Примечание:** COL-09

<a id="ui-17"></a>
- [ ] **`UI-17` · Создать визуальную систему email-шаблонов**  
  `P1` · `LAUNCH_PREP` · `design` · `E0` · **○ не начато** · target `2026-08-20`
  - **Владелец:** Design + Editorial
  - **Следующий шаг:** Определить header, typography, CTA, footer, legal/unsubscribe blocks.
  - **Источник/evidence:** `docs/operations/email-delivery.md`

<a id="ui-20"></a>
- [ ] **`UI-20` · Вести реестр утверждённых макетов и implemented parity**  
  `P1` · `LAUNCH_PREP` · `design` · `E0` · **○ не начато** · target `2026-08-09`
  - **Владелец:** Design
  - **Следующий шаг:** Для каждой surface хранить reference, owner date, implementation PR и QA evidence.
  - **Источник/evidence:** `docs/release/2026-09-01/UPDATE.md`

<a id="ui-22"></a>
- [ ] **`UI-22` · Проверить copy и визуальную иерархию пустых списков**  
  `P1` · `RC` · `qa` · `E0` · **○ не начато** · target `2026-08-22`
  - **Владелец:** Editorial + Design
  - **Следующий шаг:** Охватить нет событий, нет персональных результатов, blocked feature и stale data.
  - **Источник/evidence:** `—`

<a id="ui-15"></a>
- [ ] **`UI-15` · Утвердить PWA install UI для Android и iOS**  
  `P1` · `FG_ACTIVE` · `design` · `E2` · **🛠 в работе** · target `2026-08-16`
  - **Владелец:** Design + QA
  - **Следующий шаг:** Показать platform-specific steps и relaunch state.
  - **Источник/evidence:** `docs/features/static-site-pages/release-autotest-gates.md`

<a id="stream-04"></a>
## Фокус-группа

**🔴 BLOCKED** · P0 1/30 · blocked 18 · verify 8 · owner gate 0

<a id="fg-01"></a>
- [ ] **`FG-01` · Глобальный soft gate выключает обычный сайт без marker**  
  `P0` · `FG_PREP` · `development` · `E1` · **⛔ заблокировано** · target `2026-08-09`
  - **Владелец:** Frontend
  - **Следующий шаг:** Реализовать pre-paint locked state, inert/aria-hidden и no-JS placeholder.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`

<a id="fg-02"></a>
- [ ] **`FG-02` · Актуальная focus placeholder surface**  
  `P0` · `FG_PREP` · `development` · `E1` · **⛔ заблокировано** · target `2026-08-09`
  - **Владелец:** Frontend + Design
  - **Следующий шаг:** Собрать approved copy/visual и route tests.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/README.md`

<a id="fg-04"></a>
- [ ] **`FG-04` · Тихая анонимная Supabase-сессия после приглашения**  
  `P0` · `FG_PREP` · `development` · `E1` · **⛔ заблокировано** · target `2026-08-10`
  - **Владелец:** Frontend + Backend
  - **Следующий шаг:** Реализовать ensureFocusAnonymousSession() и явные состояния auth runtime.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`

<a id="fg-05"></a>
- [ ] **`FG-05` · Reload/reinvite переиспользуют один anonymous subject**  
  `P0` · `FG_PREP` · `qa` · `E0` · **⛔ заблокировано** · target `2026-08-11`
  - **Владелец:** Backend + QA
  - **Следующий шаг:** Добавить DB/browser evidence отсутствия новых anonymous rows.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`
  - **Примечание:** FG-04

<a id="fg-06"></a>
- [ ] **`FG-06` · Оценка страницы, текст и скриншот доступны анонимному участнику**  
  `P0` · `FG_PREP` · `development` · `E1` · **⛔ заблокировано** · target `2026-08-11`
  - **Владелец:** Frontend + Backend
  - **Следующий шаг:** Убрать login wall requireSession(); использовать anonymous auth.uid().
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`
  - **Примечание:** FG-04

<a id="fg-07"></a>
- [ ] **`FG-07` · Page score хранится по page_family + page_revision**  
  `P0` · `FG_PREP` · `development` · `E1` · **⛔ заблокировано** · target `2026-08-12`
  - **Владелец:** Backend + Frontend
  - **Следующий шаг:** Добавить revision registry, DB/RPC и bounded history.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`

<a id="fg-08"></a>
- [ ] **`FG-08` · UI «Страница обновилась» сохраняет прежнюю оценку**  
  `P0` · `FG_ACTIVE` · `development` · `E1` · **⛔ заблокировано** · target `2026-08-13`
  - **Владелец:** Frontend
  - **Следующий шаг:** Реализовать revision_changed state и browser test.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/nps-ui.md`
  - **Примечание:** FG-07

<a id="fg-09"></a>
- [ ] **`FG-09` · Production service NPS в /zakrytaya-afisha/**  
  `P0` · `FG_PREP` · `development` · `E1` · **⛔ заблокировано** · target `2026-08-12`
  - **Владелец:** Frontend + Backend
  - **Следующий шаг:** Добавить service_revision, RPC/schema, save/edit и optional text/screenshot.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`

<a id="fg-13"></a>
- [ ] **`FG-13` · Email повышает текущую anonymous identity**  
  `P0` · `FG_ACTIVE` · `development` · `E1` · **⛔ заблокировано** · target `2026-08-15`
  - **Владелец:** Auth + Backend
  - **Следующий шаг:** Реализовать upgrade без параллельного user profile и сохранить данные.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/README.md`

<a id="fg-14"></a>
- [ ] **`FG-14` · Яндекс OAuth linkIdentity из anonymous focus session**  
  `P0` · `FG_ACTIVE` · `qa` · `E1` · **⛔ заблокировано** · target `2026-08-16`
  - **Владелец:** Auth + QA
  - **Следующий шаг:** Получить fresh consent round-trip на exact target.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`
  - **Примечание:** FG-04

<a id="fg-15"></a>
- [ ] **`FG-15` · Merge anonymous subject с существующим аккаунтом**  
  `P0` · `FG_ACTIVE` · `development` · `E0` · **⛔ заблокировано** · target `2026-08-18`
  - **Владелец:** Backend
  - **Следующий шаг:** Реализовать idempotent merge/dedupe scores, artifacts, actions и profile state.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/README.md`

<a id="fg-24"></a>
- [ ] **`FG-24` · Server artifact receipts под anonymous subject**  
  `P0` · `FG_ACTIVE` · `development` · `E0` · **⛔ заблокировано** · target `2026-08-18`
  - **Владелец:** Backend
  - **Следующий шаг:** Создать durable idempotent ledger и upgrade preservation.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`

<a id="fg-25"></a>
- [ ] **`FG-25` · Eligibility 10 из 12 считается server-side**  
  `P0` · `FG_ACTIVE` · `development` · `E0` · **⛔ заблокировано** · target `2026-08-20`
  - **Владелец:** Backend
  - **Следующий шаг:** Построить projection только из receipts.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group.md`
  - **Примечание:** FG-24

<a id="fg-26"></a>
- [ ] **`FG-26` · Weighted chances 1–3 считаются server-side**  
  `P0` · `FG_CLOSE` · `development` · `E0` · **⛔ заблокировано** · target `2026-08-25`
  - **Владелец:** Backend
  - **Следующий шаг:** Реализовать base/text/screenshot projection и max=3.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group.md`
  - **Примечание:** FG-25

<a id="fg-27"></a>
- [ ] **`FG-27` · Зафиксировать cutoff 31 августа в 18:00 по Калининграду**  
  `P0` · `FG_PREP` · `development` · `E1` · **⛔ заблокировано** · target `2026-08-12`
  - **Владелец:** Backend + Frontend
  - **Следующий шаг:** Удалить rolling 30 days и добавить timezone/cutoff tests.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`

<a id="fg-28"></a>
- [ ] **`FG-28` · Неизменяемый eligible snapshot, розыгрыш и резерв**  
  `P0` · `FG_CLOSE` · `development` · `E0` · **⛔ заблокировано** · target `2026-08-27`
  - **Владелец:** Backend + Legal
  - **Следующий шаг:** Собрать защищённый workflow, audit receipt и rehearsal.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group.md`

<a id="fg-32"></a>
- [ ] **`FG-32` · Один полный live synthetic focus workflow**  
  `P0` · `RC` · `qa` · `E0` · **⛔ заблокировано** · target `2026-08-24`
  - **Владелец:** QA + Ops
  - **Следующий шаг:** Invite → anonymous feedback → upgrade → artifacts → eligibility → cleanup.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`

<a id="fg-29"></a>
- [ ] **`FG-29` · Winner/alternate notification и 3-day claim lifecycle**  
  `P0` · `FG_CLOSE` · `design` · `E0` · **○ не начато** · target `2026-08-27`
  - **Владелец:** Product + Email
  - **Следующий шаг:** Утвердить templates, expiry, reserve promotion и manual handoff.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/prize-rules.md`

<a id="fg-34"></a>
- [ ] **`FG-34` · Focus-group close rehearsal до 31 августа**  
  `P0` · `FG_CLOSE` · `qa` · `E0` · **○ не начато** · target `2026-08-26`
  - **Владелец:** Ops + Product
  - **Следующий шаг:** Пройти cutoff, snapshot, draw dry-run, notifications и rollback.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group.md`

<a id="fg-10"></a>
- [ ] **`FG-10` · Feedback text и private screenshot работают с anonymous subject**  
  `P0` · `FG_PREP` · `integration` · `E2` · **🛠 в работе** · target `2026-08-12`
  - **Владелец:** Backend + Frontend
  - **Следующий шаг:** Связать существующие v2 RPC/Storage с anonymous session.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`

<a id="fg-12"></a>
- [ ] **`FG-12` · Anonymous personalization работает до identity verification**  
  `P0` · `FG_PREP` · `integration` · `E2` · **🛠 в работе** · target `2026-08-13`
  - **Владелец:** Frontend + Backend
  - **Следующий шаг:** Доказать local-first behavior и durable linking boundary.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`

<a id="fg-31"></a>
- [ ] **`FG-31` · Извлекаемая статистика фокус-группы**  
  `P0` · `FG_ACTIVE` · `integration` · `E2` · **🛠 в работе** · target `2026-08-21`
  - **Владелец:** Analytics + Backend
  - **Следующий шаг:** Добавить subjects/linking, revisions, NPS, artifacts, chances и cohort views.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`

<a id="fg-03"></a>
- [ ] **`FG-03` · Invite/QR intake, marker и очистка URL fragment**  
  `P0` · `FG_PREP` · `qa` · `E2` · **🧪 требует проверки** · target `2026-08-10`
  - **Владелец:** Frontend + QA
  - **Следующий шаг:** Доказать exact-target journey, expiry и reinvite.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`

<a id="fg-11"></a>
- [ ] **`FG-11` · Offline/idempotent feedback outbox без потери текста**  
  `P0` · `FG_PREP` · `qa` · `E2` · **🧪 требует проверки** · target `2026-08-13`
  - **Владелец:** Frontend + QA
  - **Следующий шаг:** Провести hosted network fault → reconnect → exactly one row.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`

<a id="fg-16"></a>
- [ ] **`FG-16` · Anonymous feedback разрешён, raffle eligibility=false**  
  `P0` · `FG_PREP` · `qa` · `E2` · **🧪 требует проверки** · target `2026-08-13`
  - **Владелец:** Backend + QA
  - **Следующий шаг:** Добавить E2E с feedback success и participant rejection.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`

<a id="fg-17"></a>
- [ ] **`FG-17` · Verified participant registration и cap=200**  
  `P0` · `FG_PREP` · `qa` · `E3` · **🧪 требует проверки** · target `2026-08-14`
  - **Владелец:** Backend + QA
  - **Следующий шаг:** Провести live boundary test и cleanup test rows.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`

<a id="fg-18"></a>
- [ ] **`FG-18` · Browser/Android real-mail OTP на финальном target**  
  `P0` · `FG_ACTIVE` · `qa` · `E4` · **🧪 требует проверки** · target `2026-08-18`
  - **Владелец:** QA
  - **Следующий шаг:** Повторить после anonymous-upgrade implementation; exact 1/1/1.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`

<a id="fg-19"></a>
- [ ] **`FG-19` · iOS Safari real-mail OTP на финальном target**  
  `P0` · `FG_ACTIVE` · `qa` · `E4` · **🧪 требует проверки** · target `2026-08-18`
  - **Владелец:** QA
  - **Следующий шаг:** Повторить native-first journey после anonymous-upgrade.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`

<a id="fg-22"></a>
- [ ] **`FG-22` · Share/Calendar/Не интересно/Для меня в одном focus journey**  
  `P0` · `FG_ACTIVE` · `qa` · `E2` · **🧪 требует проверки** · target `2026-08-19`
  - **Владелец:** QA + Product
  - **Следующий шаг:** Собрать single anonymous acceptance journey.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`

<a id="fg-23"></a>
- [x] **`FG-23` · Определены 12 артефактов FG-E01…FG-E12**  
  `P0` · `FG_PREP` · `ready` · `E1` · **✅ готово** · target `2026-08-04`
  - **Владелец:** Product
  - **Следующий шаг:** Не считать local-only находки production eligibility.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group.md`

<a id="fg-21"></a>
- [ ] **`FG-21` · iPhone Add to Home Screen/relaunch**  
  `P1` · `FG_ACTIVE` · `qa` · `E0` · **⛔ заблокировано** · target `2026-08-22`
  - **Владелец:** QA
  - **Следующий шаг:** Реализовать и принять системный сценарий.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`

<a id="fg-30"></a>
- [ ] **`FG-30` · Thank-you mail и публичное объявление результата**  
  `P1` · `FG_CLOSE` · `design` · `E0` · **○ не начато** · target `2026-08-28`
  - **Владелец:** Editorial + Email
  - **Следующий шаг:** Подготовить copy без раскрытия лишних персональных данных.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group.md`

<a id="fg-33"></a>
- [ ] **`FG-33` · Операторская triage-процедура feedback и screenshots**  
  `P1` · `FG_ACTIVE` · `decision` · `E1` · **🛠 в работе** · target `2026-08-15`
  - **Владелец:** Product + Support
  - **Следующий шаг:** Определить owner/status/severity/fixed_release_sha и cadence.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group.md`

<a id="fg-20"></a>
- [ ] **`FG-20` · Android PWA install/relaunch сохраняет marker и subject**  
  `P1` · `FG_ACTIVE` · `qa` · `E2` · **🧪 требует проверки** · target `2026-08-20`
  - **Владелец:** QA
  - **Следующий шаг:** Пройти native install → launcher → standalone → relaunch.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`

<a id="stream-05"></a>
## Статический сайт и публикация

**🔴 BLOCKED** · P0 2/14 · blocked 4 · verify 5 · owner gate 1

<a id="core-02"></a>
- [ ] **`CORE-02` · Аудит здоровья Smart Update/StaticSiteBuilder за последние 24 часа**  
  `P0` · `LAUNCH_PREP` · `live` · `E2` · **⛔ заблокировано** · target `2026-08-06`
  - **Владелец:** Ops
  - **Следующий шаг:** Добавить краткоживущий FLY_API_TOKEN, выполнить read-only probe и удалить временный инструмент.
  - **Источник/evidence:** `PR #322`
  - **Примечание:** Действующий PR не прочитал production DB/logs из-за пустого секрета.

<a id="core-08"></a>
- [ ] **`CORE-08` · Stable URL lifecycle registry, aliases, redirects/410**  
  `P0` · `LAUNCH_PREP` · `development` · `E1` · **⛔ заблокировано** · target `2026-08-20`
  - **Владелец:** Backend + SEO
  - **Следующий шаг:** Создать persisted identity и two-phase cleanup plan/apply.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="core-09"></a>
- [ ] **`CORE-09` · Telegraph dual-run и переход D0–D10**  
  `P0` · `LAUNCH_PREP` · `development` · `E1` · **⛔ заблокировано** · target `2026-08-24`
  - **Владелец:** Backend + Ops
  - **Следующий шаг:** Реализовать resolver, режимы, запреты create/recreate и soak metrics.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="core-12"></a>
- [ ] **`CORE-12` · Public root activation owner gate**  
  `P0` · `D0` · `live` · `E0` · **🧭 решение владельца** · target `2026-09-01`
  - **Владелец:** Product owner + Release owner
  - **Следующий шаг:** Разрешить только после полного D0 signed checklist.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="core-06"></a>
- [ ] **`CORE-06` · Атомарная публикация root и проверенный rollback**  
  `P0` · `RC` · `integration` · `E2` · **🛠 в работе** · target `2026-08-25`
  - **Владелец:** Ops
  - **Следующий шаг:** Закрыть inventory buckets/ALB/DNS, apply rehearsal и last-good rollback.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="core-10"></a>
- [ ] **`CORE-10` · Prelaunch/full-catalog robots+sitemap transition**  
  `P0` · `PRELAUNCH` · `integration` · `E3` · **🛠 в работе** · target `2026-08-20`
  - **Владелец:** Frontend + SEO
  - **Следующий шаг:** Слить approved prelaunch contract и проверить D0 mode off.
  - **Источник/evidence:** `PR #296`

<a id="core-15"></a>
- [ ] **`CORE-15` · Media deduplication and broken-image release gate**  
  `P0` · `RC` · `qa` · `E2` · **🛠 в работе** · target `2026-08-22`
  - **Владелец:** Backend + QA
  - **Следующий шаг:** Повторить audit на fresh catalog; устранить public-visible duplicates/decode failures.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="core-17"></a>
- [ ] **`CORE-17` · Desktop keyboard navigation and event information reading**  
  `P0` · `RC` · `research` · `E1` · **🛠 в работе** · target `2026-08-19`
  - **Владелец:** Accessibility + Frontend
  - **Следующий шаг:** Завершить best-practice research, audit gaps and implement predictable focus/scroll anchors.
  - **Источник/evidence:** `—`

<a id="core-03"></a>
- [ ] **`CORE-03` · Подтвердить current public root SHA/version и факт promotion**  
  `P0` · `LAUNCH_PREP` · `live` · `E3` · **🧪 требует проверки** · target `2026-08-06`
  - **Владелец:** Ops
  - **Следующий шаг:** Сопоставить public root с exact main/candidate; не делать promotion автоматически.
  - **Источник/evidence:** `PR #323`

<a id="core-05"></a>
- [ ] **`CORE-05` · Full catalog browser gate на свежем main snapshot**  
  `P0` · `RC` · `qa` · `E4` · **🧪 требует проверки** · target `2026-08-23`
  - **Владелец:** QA
  - **Следующий шаг:** Получить terminal candidate без unexplained route/geometry failures.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="core-07"></a>
- [ ] **`CORE-07` · Freshness/outbox/catch-up и max-staleness drill**  
  `P0` · `RC` · `qa` · `E3` · **🧪 требует проверки** · target `2026-08-23`
  - **Владелец:** Backend + Ops
  - **Следующий шаг:** Доказать update during build → one successor и failure recovery.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="core-11"></a>
- [ ] **`CORE-11` · Stable ICS publication and non-regression**  
  `P0` · `RC` · `qa` · `E4` · **🧪 требует проверки** · target `2026-08-23`
  - **Владелец:** Backend + QA
  - **Следующий шаг:** Проверить all active event links, content types, hashes и rollback.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="core-01"></a>
- [x] **`CORE-01` · StaticSiteBuilder incident recovery merged and evidenced**  
  `P0` · `LAUNCH_PREP` · `ready` · `E5` · **✅ готово** · target `2026-08-04`
  - **Владелец:** Backend + Ops
  - **Следующий шаг:** Сохранить regression gates и мониторить новые симптомы.
  - **Источник/evidence:** `PR #315; PR #317; PR #319; PR #321`

<a id="core-04"></a>
- [x] **`CORE-04` · Generated route/runtime inventory на общих поверхностях**  
  `P0` · `LAUNCH_PREP` · `ready` · `E3` · **✅ готово** · target `2026-08-04`
  - **Владелец:** Frontend + QA
  - **Следующий шаг:** Сохранять как regression contract.
  - **Источник/evidence:** `PR #316`

<a id="core-13"></a>
- [ ] **`CORE-13` · Schedule/transport freshness manifest and failed-refresh drill**  
  `P1` · `RC` · `qa` · `E1` · **⛔ заблокировано** · target `2026-08-24`
  - **Владелец:** Backend + Ops
  - **Следующий шаг:** Получить актуальный rail/bus snapshot и degraded behavior.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="core-16"></a>
- [ ] **`CORE-16` · Medallions/organization identities coverage**  
  `P1` · `LAUNCH_PREP` · `integration` · `E2` · **🛠 в работе** · target `2026-08-20`
  - **Владелец:** Editorial + Frontend
  - **Следующий шаг:** Сверить high-volume venues/organizations, approved avatars and fallback.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="core-14"></a>
- [ ] **`CORE-14` · Past-event archive/CTA/SEO behavior**  
  `P1` · `RC` · `qa` · `E2` · **🧪 требует проверки** · target `2026-08-22`
  - **Владелец:** Frontend + SEO
  - **Следующий шаг:** Проверить ended/cancelled/postponed/rescheduled routes and structured data.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="stream-06"></a>
## Подборки и каталоги

**🔴 BLOCKED** · P0 0/8 · blocked 7 · verify 1 · owner gate 1

<a id="col-01"></a>
- [ ] **`COL-01` · Полный единый реестр всех канонических подборок**  
  `P0` · `LAUNCH_PREP` · `development` · `E1` · **⛔ заблокировано** · target `2026-08-12`
  - **Владелец:** Frontend + Product
  - **Следующий шаг:** Создать readiness projection v2 со статусами route/data/navigation/sitemap.
  - **Источник/evidence:** `PR #323`

<a id="col-02"></a>
- [ ] **`COL-02` · Mobile/desktop menu consumes collection registry only**  
  `P0` · `LAUNCH_PREP` · `development` · `E1` · **⛔ заблокировано** · target `2026-08-14`
  - **Владелец:** Frontend
  - **Следующий шаг:** Убрать unconditional free/clubs links и blocked links.
  - **Источник/evidence:** `PR #323`
  - **Примечание:** COL-01

<a id="col-03"></a>
- [ ] **`COL-03` · Build-time route integrity for catalog/navigation/sitemap**  
  `P0` · `LAUNCH_PREP` · `qa` · `E1` · **⛔ заблокировано** · target `2026-08-14`
  - **Владелец:** QA + Frontend
  - **Следующий шаг:** Проверять generated HTML, preview base path и canonical parity.
  - **Источник/evidence:** `PR #323`
  - **Примечание:** COL-01

<a id="col-04"></a>
- [ ] **`COL-04` · Reconcile free collection route/data/publication**  
  `P0` · `LAUNCH_PREP` · `integration` · `E1` · **⛔ заблокировано** · target `2026-08-14`
  - **Владелец:** Product + Frontend
  - **Следующий шаг:** Определить repair path и не показывать ссылку до emitted route.
  - **Источник/evidence:** `PR #323`

<a id="col-05"></a>
- [ ] **`COL-05` · Reconcile kids/family collection naming and route**  
  `P0` · `LAUNCH_PREP` · `decision` · `E1` · **⛔ заблокировано** · target `2026-08-13`
  - **Владелец:** Product + Editorial
  - **Следующий шаг:** Выбрать каноническое название и exact membership contract.
  - **Источник/evidence:** `PR #323`

<a id="col-06"></a>
- [ ] **`COL-06` · Clubs publication uses six-month activity rule**  
  `P0` · `LAUNCH_PREP` · `integration` · `E1` · **⛔ заблокировано** · target `2026-08-15`
  - **Владелец:** Backend + Frontend
  - **Следующий шаг:** Связать registry с checked relation projection.
  - **Источник/evidence:** `PR #323`

<a id="col-13"></a>
- [ ] **`COL-13` · Decide whether weak-supply collections are D0 or post-launch**  
  `P0` · `LAUNCH_PREP` · `decision` · `E1` · **🧭 решение владельца** · target `2026-08-16`
  - **Владелец:** Product owner
  - **Следующий шаг:** Зафиксировать default-off/deferred entries в scope.
  - **Источник/evidence:** `docs/features/static-site-pages/podborki.md`

<a id="col-07"></a>
- [ ] **`COL-07` · Verify exhibitions/festivals/popular/unusual routes**  
  `P0` · `RC` · `qa` · `E3` · **🧪 требует проверки** · target `2026-08-20`
  - **Владелец:** QA
  - **Следующий шаг:** Проверить route existence, content minimum, navigation and sitemap.
  - **Источник/evidence:** `PR #316`

<a id="col-11"></a>
- [ ] **`COL-11` · Astro routes/navigation/sitemap for approved collections**  
  `P1` · `LAUNCH_PREP` · `development` · `E1` · **⛔ заблокировано** · target `2026-08-23`
  - **Владелец:** Frontend
  - **Следующий шаг:** Публиковать только approved + emitted entries.
  - **Источник/evidence:** `docs/features/static-site-pages/podborki.md`
  - **Примечание:** COL-01

<a id="col-09"></a>
- [ ] **`COL-09` · Owner review real gastronomy candidate families**  
  `P1` · `LAUNCH_PREP` · `decision` · `E0` · **○ не начато** · target `2026-08-19`
  - **Владелец:** Product + Editorial
  - **Следующий шаг:** Разметить core/co-core/hard-negative и получить complete audit.
  - **Источник/evidence:** `PR #314`

<a id="col-10"></a>
- [ ] **`COL-10` · Owner gold and quality baseline for semantic collections**  
  `P1` · `LAUNCH_PREP` · `decision` · `E0` · **○ не начато** · target `2026-08-21`
  - **Владелец:** Product + Editorial
  - **Следующий шаг:** Утвердить positives, boundaries, last-good and WATCH/FAIL thresholds.
  - **Источник/evidence:** `docs/testing/static-collections-product-quality-autotests.md`

<a id="col-12"></a>
- [ ] **`COL-12` · Collections product browser smoke on public candidate**  
  `P1` · `RC` · `qa` · `E0` · **○ не начато** · target `2026-08-24`
  - **Владелец:** QA
  - **Следующий шаг:** Проверить supply, duplicates, empty states, mobile and links.
  - **Источник/evidence:** `docs/features/static-site-pages/release-autotest-gates.md`

<a id="col-08"></a>
- [ ] **`COL-08` · Integrate gastronomy data-prep from current PR #314**  
  `P1` · `LAUNCH_PREP` · `integration` · `E3` · **🛠 в работе** · target `2026-08-16`
  - **Владелец:** Backend
  - **Следующий шаг:** Перенести актуальный diff на fresh main или merge после compare.
  - **Источник/evidence:** `PR #314`

<a id="col-14"></a>
- [ ] **`COL-14` · Gastronomy route UI publication decision**  
  `P2` · `POST_LAUNCH` · `decision` · `E1` · **⏸ отложено** · target `после owner gold`
  - **Владелец:** Product owner
  - **Следующий шаг:** Не блокирует D0, пока data-prep остаётся shadow.
  - **Источник/evidence:** `PR #314`

<a id="stream-07"></a>
## Умный поиск

**🔴 BLOCKED** · P0 1/8 · blocked 2 · verify 2 · owner gate 0

<a id="search-02"></a>
- [ ] **`SEARCH-02` · Доказать причину сбоя и восстановить production-поиск**  
  `P0` · `LAUNCH_PREP` · `live` · `E1` · **⛔ заблокировано** · target `2026-08-12`
  - **Владелец:** Backend + Ops
  - **Следующий шаг:** Выполнить коррелированный live recovery run с exact static/Edge/corpus identities.
  - **Источник/evidence:** `docs/features/static-site-pages/smart-vector-search/README.md`

<a id="search-03"></a>
- [ ] **`SEARCH-03` · Cache invalidation by catalog/corpus revision**  
  `P0` · `LAUNCH_PREP` · `development` · `E1` · **⛔ заблокировано** · target `2026-08-14`
  - **Владелец:** Backend
  - **Следующий шаг:** Добавить revision fields to key and tests.
  - **Источник/evidence:** `docs/features/static-site-pages/smart-vector-search/README.md`

<a id="search-09"></a>
- [ ] **`SEARCH-09` · Search freshness/corpus receipt monitoring**  
  `P0` · `RC` · `live` · `E0` · **○ не начато** · target `2026-08-24`
  - **Владелец:** Ops + Backend
  - **Следующий шаг:** Добавить current corpus age and cold/cached canaries.
  - **Источник/evidence:** `docs/features/static-site-pages/smart-vector-search/README.md`

<a id="search-05"></a>
- [ ] **`SEARCH-05` · Authenticated session-fixture product journey**  
  `P0` · `LAUNCH_PREP` · `qa` · `E3` · **🛠 в работе** · target `2026-08-16`
  - **Владелец:** QA + Backend
  - **Следующий шаг:** Получить terminal no-mail live acceptance.
  - **Источник/evidence:** `PR #284; docs/testing/static-site-auth-session-fixture.md`

<a id="search-07"></a>
- [ ] **`SEARCH-07` · Hosted direct/relay/cache/stage diagnostics E2E**  
  `P0` · `LAUNCH_PREP` · `integration` · `E3` · **🛠 в работе** · target `2026-08-18`
  - **Владелец:** QA + Backend
  - **Следующий шаг:** Завершить PR and terminal evidence on current main.
  - **Источник/evidence:** `PR #284`

<a id="search-04"></a>
- [ ] **`SEARCH-04` · Exact-origin CORS and shared transport contract**  
  `P0` · `LAUNCH_PREP` · `integration` · `E2` · **🧪 требует проверки** · target `2026-08-14`
  - **Владелец:** Backend + Security
  - **Следующий шаг:** Сверить Edge config with same-origin direct/relay policy.
  - **Источник/evidence:** `docs/features/static-site-pages/smart-vector-search/README.md`

<a id="search-06"></a>
- [ ] **`SEARCH-06` · Rolling quota, one-in-flight and global cost circuit**  
  `P0` · `LAUNCH_PREP` · `qa` · `E2` · **🧪 требует проверки** · target `2026-08-16`
  - **Владелец:** Backend + Security
  - **Следующий шаг:** Проверить actual Edge route, not only source assertions.
  - **Источник/evidence:** `docs/features/static-site-pages/smart-vector-search/README.md`

<a id="search-01"></a>
- [x] **`SEARCH-01` · Canonical search contract and scenario registry**  
  `P0` · `LAUNCH_PREP` · `ready` · `E1` · **✅ готово** · target `2026-08-04`
  - **Владелец:** Product + Backend
  - **Следующий шаг:** Не менять baseline без incident recovery evidence.
  - **Источник/evidence:** `docs/features/static-site-pages/smart-vector-search/README.md`

<a id="search-08"></a>
- [ ] **`SEARCH-08` · Mobile search acceptance**  
  `P1` · `RC` · `qa` · `E0` · **○ не начато** · target `2026-08-23`
  - **Владелец:** QA
  - **Следующий шаг:** Проверить keyboard, long query, auth, no-results and degraded state.
  - **Источник/evidence:** `docs/features/static-site-pages/release-autotest-gates.md`

<a id="stream-08"></a>
## Персонализация и «Для меня»

**🔴 BLOCKED** · P0 1/8 · blocked 1 · verify 1 · owner gate 0

<a id="p13n-04"></a>
- [ ] **`P13N-04` · Долговечная материализация анонимного профиля и связывание identity**  
  `P0` · `FG_ACTIVE` · `development` · `E1` · **⛔ заблокировано** · target `2026-08-18`
  - **Владелец:** Backend
  - **Следующий шаг:** Связать с focus anonymous subject и политикой merge профиля.
  - **Источник/evidence:** `docs/architecture/personalization-data-ownership.md`
  - **Примечание:** FG-04

<a id="p13n-09"></a>
- [ ] **`P13N-09` · Find-interest-within-30 measurement**  
  `P0` · `FG_ACTIVE` · `integration` · `E0` · **○ не начато** · target `2026-08-20`
  - **Владелец:** Analytics + Product
  - **Следующий шаг:** Собрать reproducible multi-session E2E and metric receipts.
  - **Источник/evidence:** `docs/features/static-site-pages/personalizaion/personalization-to-be.md`

<a id="p13n-03"></a>
- [ ] **`P13N-03` · Anonymous local-first profile and strong actions**  
  `P0` · `FG_PREP` · `integration` · `E2` · **🛠 в работе** · target `2026-08-14`
  - **Владелец:** Frontend
  - **Следующий шаг:** Проверить bounds, consent and no-Supabase behavior.
  - **Источник/evidence:** `docs/architecture/personalization-data-ownership.md`

<a id="p13n-05"></a>
- [ ] **`P13N-05` · Like/share/hide/calendar action consistency**  
  `P0` · `LAUNCH_PREP` · `qa` · `E3` · **🛠 в работе** · target `2026-08-19`
  - **Владелец:** Frontend + QA
  - **Следующий шаг:** Проверить idempotency, undo, dynamic cards and personalization signals.
  - **Источник/evidence:** `PR #316`

<a id="p13n-07"></a>
- [ ] **`P13N-07` · Profile merge and conflict policy**  
  `P0` · `FG_ACTIVE` · `integration` · `E1` · **🛠 в работе** · target `2026-08-18`
  - **Владелец:** Backend + Product
  - **Следующий шаг:** Authenticated explicit actions win; preserve bounded history.
  - **Источник/evidence:** `docs/architecture/personalization-data-ownership.md`

<a id="p13n-08"></a>
- [ ] **`P13N-08` · Consent, channel and suppression state separation**  
  `P0` · `LAUNCH_PREP` · `integration` · `E1` · **🛠 в работе** · target `2026-08-20`
  - **Владелец:** Backend + Legal
  - **Следующий шаг:** Не выводить recommendation consent from auth/favorites.
  - **Источник/evidence:** `docs/architecture/personalization-data-ownership.md`

<a id="p13n-06"></a>
- [ ] **`P13N-06` · Authenticated «Для меня» generated journey**  
  `P0` · `RC` · `qa` · `E2` · **🧪 требует проверки** · target `2026-08-22`
  - **Владелец:** QA + Backend
  - **Следующий шаг:** Пройти session_fixture, non-empty/empty and continuation states.
  - **Источник/evidence:** `docs/features/static-site-pages/release-autotest-gates.md`

<a id="p13n-01"></a>
- [x] **`P13N-01` · P13N-00 seam and route inventory**  
  `P0` · `LAUNCH_PREP` · `ready` · `E3` · **✅ готово** · target `2026-08-04`
  - **Владелец:** Frontend + Backend
  - **Следующий шаг:** Сохранять behavior-neutral baseline.
  - **Источник/evidence:** `PR #316`

<a id="p13n-10"></a>
- [ ] **`P13N-10` · Longitudinal personalization mutation tests**  
  `P1` · `FG_ACTIVE` · `qa` · `E0` · **○ не начато** · target `2026-08-22`
  - **Владелец:** QA + Data
  - **Следующий шаг:** Сохранить events/logs/profile revisions across repeated browsing sessions.
  - **Источник/evidence:** `docs/operations/static-site-autotest-strategy.md`

<a id="p13n-02"></a>
- [ ] **`P13N-02` · P13N-01…P13N-06 model expansion**  
  `P2` · `POST_LAUNCH` · `research` · `E1` · **⏸ отложено** · target `после D0`
  - **Владелец:** Product + Data
  - **Следующий шаг:** Не менять ranking model в critical launch window.
  - **Источник/evidence:** `docs/features/static-site-pages/personalizaion/personalization-to-be.md`

<a id="stream-09"></a>
## Авторизация и identity

**🔴 BLOCKED** · P0 2/9 · blocked 2 · verify 3 · owner gate 0

<a id="auth-06"></a>
- [ ] **`AUTH-06` · Yandex OAuth anonymous focus upgrade**  
  `P0` · `FG_ACTIVE` · `qa` · `E1` · **⛔ заблокировано** · target `2026-08-18`
  - **Владелец:** Auth + QA
  - **Следующий шаг:** Depends on silent anonymous session and identity linking.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`
  - **Примечание:** FG-04

<a id="auth-09"></a>
- [ ] **`AUTH-09` · Auth status never presents anonymous subject as verified login**  
  `P0` · `FG_PREP` · `development` · `E1` · **⛔ заблокировано** · target `2026-08-12`
  - **Владелец:** Frontend + Auth
  - **Следующий шаг:** Добавить explicit anonymous_focus/verified/no_subject/error states.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/README.md`
  - **Примечание:** FG-04

<a id="auth-08"></a>
- [ ] **`AUTH-08` · Account unlink/delete/export and data-subject flows**  
  `P0` · `LAUNCH_PREP` · `design` · `E0` · **○ не начато** · target `2026-08-22`
  - **Владелец:** Auth + Legal
  - **Следующий шаг:** Согласовать UI/API, retention and purge receipts.
  - **Источник/evidence:** `LEGAL-14`

<a id="auth-10"></a>
- [ ] **`AUTH-10` · Anti-abuse and rate limits for anonymous/Auth surfaces**  
  `P0` · `RC` · `qa` · `E2` · **🛠 в работе** · target `2026-08-22`
  - **Владелец:** Security + Backend
  - **Следующий шаг:** Review OTP, feedback, search, subscriptions, screenshots and merge.
  - **Источник/evidence:** `docs/architecture/personalization-data-ownership.md`

<a id="auth-01"></a>
- [ ] **`AUTH-01` · No-mail authenticated session fixture**  
  `P0` · `LAUNCH_PREP` · `qa` · `E3` · **🧪 требует проверки** · target `2026-08-12`
  - **Владелец:** Auth + QA
  - **Следующий шаг:** Подтвердить merged implementation and hosted protected probe.
  - **Источник/evidence:** `PR #287; PR #316`

<a id="auth-02"></a>
- [ ] **`AUTH-02` · Browser real-mail OTP on final target**  
  `P0` · `RC` · `qa` · `E4` · **🧪 требует проверки** · target `2026-08-20`
  - **Владелец:** QA
  - **Следующий шаг:** Повторить only when final auth/mail changes land.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/status.md`

<a id="auth-05"></a>
- [ ] **`AUTH-05` · Yandex OAuth ordinary verified session**  
  `P0` · `RC` · `qa` · `E3` · **🧪 требует проверки** · target `2026-08-21`
  - **Владелец:** Auth + QA
  - **Следующий шаг:** Получить fresh owner-session consent round-trip.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="auth-03"></a>
- [x] **`AUTH-03` · Android real-mail OTP direct/relay acceptance**  
  `P0` · `RC` · `ready` · `E4` · **✅ готово** · target `2026-08-04`
  - **Владелец:** QA
  - **Следующий шаг:** Retest only after affected auth/runtime changes.
  - **Источник/evidence:** `runs 30772062840, 30772957771`

<a id="auth-04"></a>
- [x] **`AUTH-04` · iOS native-first real-mail OTP direct/relay acceptance**  
  `P0` · `RC` · `ready` · `E4` · **✅ готово** · target `2026-08-04`
  - **Владелец:** QA
  - **Следующий шаг:** Retest only after affected auth/runtime changes.
  - **Источник/evidence:** `runs 30772233868, 30773125445`

<a id="auth-07"></a>
- [ ] **`AUTH-07` · Account recovery and cross-device restoration**  
  `P1` · `RC` · `design` · `E0` · **○ не начато** · target `2026-08-23`
  - **Владелец:** Auth + Product
  - **Следующий шаг:** Определить email/Yandex recovery and anonymous limits.
  - **Источник/evidence:** `docs/architecture/personalization-data-ownership.md`

<a id="stream-10"></a>
## Почта, ящики и шаблоны

**🔴 BLOCKED** · P0 4/16 · blocked 2 · verify 3 · owner gate 0

<a id="mail-07"></a>
- [ ] **`MAIL-07` · Ротировать раскрытый API-ключ NotiSend**  
  `P0` · `LAUNCH_PREP` · `live` · `E1` · **⛔ заблокировано** · target `2026-08-12`
  - **Владелец:** Ops + Owner
  - **Следующий шаг:** Запросить revoke/reissue, обновить Lockbox, проверить недействительность старого ключа.
  - **Источник/evidence:** `docs/operations/email-delivery.md`

<a id="mail-19"></a>
- [ ] **`MAIL-19` · Deliverability warm-up and production canary**  
  `P0` · `RC` · `live` · `E1` · **⛔ заблокировано** · target `2026-08-25`
  - **Владелец:** Ops
  - **Следующий шаг:** Run only after key/template/consent gates; record inbox/spam/bounce evidence.
  - **Источник/evidence:** `docs/operations/email-delivery.md`
  - **Примечание:** MAIL-07

<a id="mail-09"></a>
- [ ] **`MAIL-09` · Canonical inventory of email purposes, From/Reply-To and templates**  
  `P0` · `LAUNCH_PREP` · `decision` · `E0` · **○ не начато** · target `2026-08-14`
  - **Владелец:** Editorial + Backend + Legal
  - **Следующий шаг:** Map every send to purpose, provider, consent/trigger, template version and owner.
  - **Источник/evidence:** `docs/operations/email-delivery.md`

<a id="mail-12"></a>
- [ ] **`MAIL-12` · One-time launch notification template**  
  `P0` · `D0` · `design` · `E0` · **○ не начато** · target `2026-08-20`
  - **Владелец:** Editorial + Legal
  - **Следующий шаг:** Approve subject, body, sender, privacy footer and idempotent send state.
  - **Источник/evidence:** `PR #296`

<a id="mail-13"></a>
- [ ] **`MAIL-13` · Focus invite and reminder templates**  
  `P0` · `FG_PREP` · `design` · `E0` · **○ не начато** · target `2026-08-12`
  - **Владелец:** Editorial + Product
  - **Следующий шаг:** Define QR/link, deadline, privacy, eligibility and support contact.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group.md`

<a id="mail-14"></a>
- [ ] **`MAIL-14` · Focus winner/alternate/thank-you templates**  
  `P0` · `FG_CLOSE` · `design` · `E0` · **○ не начато** · target `2026-08-26`
  - **Владелец:** Editorial + Legal
  - **Следующий шаг:** Approve claim deadline and privacy-safe publication copy.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/prize-rules.md`

<a id="mail-22"></a>
- [ ] **`MAIL-22` · Exact send counters and no-duplicate launch dispatch**  
  `P0` · `D0` · `qa` · `E0` · **○ не начато** · target `2026-08-28`
  - **Владелец:** Backend + QA
  - **Следующий шаг:** Rehearse pending→sent claim, ambiguous dispatch and rerun safety.
  - **Источник/evidence:** `PR #296`

<a id="mail-05"></a>
- [ ] **`MAIL-05` · Transactional outbound application producers and warm-up**  
  `P0` · `LAUNCH_PREP` · `integration` · `E2` · **🛠 в работе** · target `2026-08-23`
  - **Владелец:** Backend + Ops
  - **Следующий шаг:** Finish producer inventory, templates, claim guard, alerts and bounded canary.
  - **Источник/evidence:** `docs/operations/email-delivery.md`

<a id="mail-17"></a>
- [ ] **`MAIL-17` · Unsubscribe/preferences/suppression UX and backend**  
  `P0` · `LAUNCH_PREP` · `integration` · `E2` · **🛠 в работе** · target `2026-08-22`
  - **Владелец:** Backend + Legal
  - **Следующий шаг:** Prove purpose-specific unsubscribe and hard-bounce/complaint handling.
  - **Источник/evidence:** `docs/operations/email-delivery.md`

<a id="mail-08"></a>
- [ ] **`MAIL-08` · Final SPF/DKIM/DMARC/DNS verification**  
  `P0` · `RC` · `qa` · `E4` · **🧪 требует проверки** · target `2026-08-24`
  - **Владелец:** Ops
  - **Следующий шаг:** Capture sanitized current DNS and controlled delivery headers.
  - **Источник/evidence:** `docs/operations/email-delivery.md`

<a id="mail-10"></a>
- [ ] **`MAIL-10` · OTP email template approved and tested**  
  `P0` · `FG_PREP` · `qa` · `E4` · **🧪 требует проверки** · target `2026-08-16`
  - **Владелец:** Editorial + QA
  - **Следующий шаг:** Review subject/body/branding/spam placement on current provider route.
  - **Источник/evidence:** `docs/operations/email-delivery.md`

<a id="mail-21"></a>
- [ ] **`MAIL-21` · Prelaunch subscriber queue migration and cleanup policy**  
  `P0` · `PRELAUNCH` · `integration` · `E2` · **🧪 требует проверки** · target `2026-08-15`
  - **Владелец:** Backend + Legal
  - **Следующий шаг:** Apply migration, verify RLS/dedupe/retention before public form.
  - **Источник/evidence:** `PR #296`

<a id="mail-01"></a>
- [x] **`MAIL-01` · Human mailbox info@kenigevents.ru is live**  
  `P0` · `LAUNCH_PREP` · `ready` · `E5` · **✅ готово** · target `2026-08-04`
  - **Владелец:** Ops + Support
  - **Следующий шаг:** Maintain access, monitoring and owner coverage.
  - **Источник/evidence:** `docs/operations/email-delivery.md`

<a id="mail-02"></a>
- [x] **`MAIL-02` · DMARC mailbox dmarc@kenigevents.ru is live**  
  `P0` · `LAUNCH_PREP` · `ready` · `E4` · **✅ готово** · target `2026-08-04`
  - **Владелец:** Ops
  - **Следующий шаг:** Review aggregate reports before policy tightening.
  - **Источник/evidence:** `docs/operations/email-delivery.md`

<a id="mail-03"></a>
- [x] **`MAIL-03` · SpaceWeb IMAP → Yandex inbound pipeline**  
  `P0` · `LAUNCH_PREP` · `ready` · `E5` · **✅ готово** · target `2026-08-04`
  - **Владелец:** Ops
  - **Следующий шаг:** Keep read-only UID/cursor/DLQ canaries.
  - **Источник/evidence:** `docs/operations/email-delivery.md`

<a id="mail-04"></a>
- [x] **`MAIL-04` · Postbox identity and feedback path**  
  `P0` · `LAUNCH_PREP` · `ready` · `E5` · **✅ готово** · target `2026-08-04`
  - **Владелец:** Ops + Backend
  - **Следующий шаг:** Do not confuse feedback infra with enabled application sending.
  - **Источник/evidence:** `docs/operations/email-delivery.md`

<a id="mail-11"></a>
- [ ] **`MAIL-11` · Prelaunch subscription confirmation copy**  
  `P1` · `PRELAUNCH` · `design` · `E0` · **○ не начато** · target `2026-08-12`
  - **Владелец:** Editorial + Legal
  - **Следующий шаг:** Decide whether confirmation is sent; avoid adding unneeded mail.
  - **Источник/evidence:** `PR #296`

<a id="mail-15"></a>
- [ ] **`MAIL-15` · Transactional save/reminder/cancel/reschedule templates**  
  `P1` · `LAUNCH_PREP` · `design` · `E0` · **○ не начато** · target `2026-08-22`
  - **Владелец:** Editorial + Product
  - **Следующий шаг:** Prioritize only implemented event lifecycle producers.
  - **Источник/evidence:** `docs/operations/email-delivery.md`

<a id="mail-18"></a>
- [ ] **`MAIL-18` · Email client rendering/accessibility QA**  
  `P1` · `RC` · `qa` · `E0` · **○ не начато** · target `2026-08-25`
  - **Владелец:** QA + Design
  - **Следующий шаг:** Check plain text, dark mode, mobile widths, links, alt text and legal footer.
  - **Источник/evidence:** `—`

<a id="mail-20"></a>
- [ ] **`MAIL-20` · Mailbox/support operating runbook and SLA**  
  `P1` · `LAUNCH_PREP` · `decision` · `E1` · **🛠 в работе** · target `2026-08-20`
  - **Владелец:** Support + Ops
  - **Следующий шаг:** Assign daily coverage, escalation, password recovery and incident messages.
  - **Источник/evidence:** `docs/operations/email-delivery.md`

<a id="mail-06"></a>
- [x] **`MAIL-06` · NotiSend sender/domain verification**  
  `P1` · `LAUNCH_PREP` · `ready` · `E4` · **✅ готово** · target `2026-08-04`
  - **Владелец:** Ops
  - **Следующий шаг:** Keep recommendation stream disabled until key rotation and consent.
  - **Источник/evidence:** `docs/operations/email-delivery.md`

<a id="mail-16"></a>
- [ ] **`MAIL-16` · Recommendation email with exactly three events**  
  `P2` · `POST_LAUNCH` · `design` · `E1` · **🛠 в работе** · target `после D0`
  - **Владелец:** Editorial + Data
  - **Следующий шаг:** Keep stream default-off until consent, key rotation and product gates.
  - **Источник/evidence:** `docs/features/personal-email-announcements/README.md`

<a id="stream-11"></a>
## Юридические и публичные документы

**🔴 BLOCKED** · P0 0/17 · blocked 5 · verify 1 · owner gate 1

<a id="legal-03"></a>
- [ ] **`LEGAL-03` · Отдельные согласия на обработку персональных данных по целям**  
  `P0` · `LAUNCH_PREP` · `design` · `E1` · **⛔ заблокировано** · target `2026-08-19`
  - **Владелец:** Legal + Product
  - **Следующий шаг:** Подготовить самостоятельные тексты согласий и versioned evidence; не встраивать их в пользовательское соглашение.
  - **Источник/evidence:** `ФЗ №152-ФЗ; ФЗ №156-ФЗ от 24.06.2025`

<a id="legal-08"></a>
- [ ] **`LEGAL-08` · Публичные правила розыгрыша для участников фокус-группы**  
  `P0` · `FG_PREP` · `design` · `E1` · **⛔ заблокировано** · target `2026-08-14`
  - **Владелец:** Legal + Product
  - **Следующий шаг:** Указать организатора, eligibility, приз, даты, метод выбора, резерв, получение и публикацию результата.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group-release/prize-rules.md`

<a id="legal-09"></a>
- [ ] **`LEGAL-09` · Юридическая проверка prize/tax/partner-ticket obligations**  
  `P0` · `FG_PREP` · `research` · `E0` · **⛔ заблокировано** · target `2026-08-14`
  - **Владелец:** Qualified legal counsel
  - **Следующий шаг:** Получить письменный review; не заявлять правила завершёнными без него.
  - **Источник/evidence:** `—`

<a id="legal-11"></a>
- [ ] **`LEGAL-11` · Аудит локализации и потоков данных по 152-ФЗ для Supabase/Auth/email/profile**  
  `P0` · `LAUNCH_PREP` · `research` · `E1` · **⛔ заблокировано** · target `2026-08-18`
  - **Владелец:** Legal + Architecture
  - **Следующий шаг:** Определить допустимый production flow либо необходимые изменения primary storage в РФ.
  - **Источник/evidence:** `docs/architecture/personalization-data-ownership.md`

<a id="legal-12"></a>
- [ ] **`LEGAL-12` · Cross-border transfer and foreign processor assessment**  
  `P0` · `LAUNCH_PREP` · `research` · `E0` · **⛔ заблокировано** · target `2026-08-20`
  - **Владелец:** Legal + Architecture
  - **Следующий шаг:** Inventory Supabase, device clouds, providers, subprocessors and legal grounds.
  - **Источник/evidence:** `docs/architecture/personalization-data-ownership.md`

<a id="legal-17"></a>
- [ ] **`LEGAL-17` · Final legal GO sign-off**  
  `P0` · `STABILIZATION` · `decision` · `E0` · **🧭 решение владельца** · target `2026-08-27`
  - **Владелец:** Qualified legal counsel + Product owner
  - **Следующий шаг:** Sign only after public docs and actual flows match.
  - **Источник/evidence:** `docs/release/2026-09-01/CHECKLIST.md`

<a id="legal-01"></a>
- [ ] **`LEGAL-01` · Публичные сведения об операторе и контакт для обращений**  
  `P0` · `LAUNCH_PREP` · `design` · `E0` · **○ не начато** · target `2026-08-18`
  - **Владелец:** Legal + Product
  - **Следующий шаг:** Определить полное наименование/статус, адрес/контакт и responsible person.
  - **Источник/evidence:** `—`

<a id="legal-02"></a>
- [ ] **`LEGAL-02` · Политика обработки персональных данных**  
  `P0` · `LAUNCH_PREP` · `design` · `E0` · **○ не начато** · target `2026-08-20`
  - **Владелец:** Legal
  - **Следующий шаг:** Описать цели, категории, сроки, получателей, права, безопасность and contacts.
  - **Источник/evidence:** `docs/architecture/personalization-data-ownership.md`

<a id="legal-04"></a>
- [ ] **`LEGAL-04` · Отдельное согласие на информационные/рекламные рассылки**  
  `P0` · `LAUNCH_PREP` · `design` · `E0` · **○ не начато** · target `2026-08-20`
  - **Владелец:** Legal + Email
  - **Следующий шаг:** Разделить launch notice, transactional and recommendation purposes.
  - **Источник/evidence:** `docs/operations/email-delivery.md`

<a id="legal-05"></a>
- [ ] **`LEGAL-05` · Cookies/localStorage/analytics notice**  
  `P0` · `LAUNCH_PREP` · `design` · `E0` · **○ не начато** · target `2026-08-20`
  - **Владелец:** Legal + Frontend
  - **Следующий шаг:** Описать marker, anonymous auth, personalization state, analytics and controls.
  - **Источник/evidence:** `docs/architecture/personalization-data-ownership.md`

<a id="legal-06"></a>
- [ ] **`LEGAL-06` · Пользовательское соглашение/условия использования**  
  `P0` · `LAUNCH_PREP` · `design` · `E0` · **○ не начато** · target `2026-08-22`
  - **Владелец:** Legal + Product
  - **Следующий шаг:** Описать сервис, ограничения, ответственность, user conduct, changes and termination.
  - **Источник/evidence:** `—`

<a id="legal-18"></a>
- [ ] **`LEGAL-18` · Versioned registry of legal copy and consent evidence**  
  `P0` · `LAUNCH_PREP` · `development` · `E0` · **○ не начато** · target `2026-08-22`
  - **Владелец:** Backend + Legal
  - **Следующий шаг:** Store purpose, version, text hash, timestamp, subject and withdrawal.
  - **Источник/evidence:** `—`

<a id="legal-20"></a>
- [ ] **`LEGAL-20` · Public privacy/contact links present on every relevant surface**  
  `P0` · `RC` · `qa` · `E0` · **○ не начато** · target `2026-08-24`
  - **Владелец:** Frontend + QA
  - **Следующий шаг:** Verify footer, auth, forms, feedback, email preferences and focus hub.
  - **Источник/evidence:** `LEGAL-01; LEGAL-02; LEGAL-06`

<a id="legal-07"></a>
- [ ] **`LEGAL-07` · Публичные условия участия в фокус-группе**  
  `P0` · `FG_PREP` · `design` · `E1` · **🛠 в работе** · target `2026-08-12`
  - **Владелец:** Legal + Product
  - **Следующий шаг:** Перевести product contract в public copy; включить identity, feedback, deadlines and support.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group.md`

<a id="legal-14"></a>
- [ ] **`LEGAL-14` · Retention, deletion, withdrawal and data-subject request procedure**  
  `P0` · `LAUNCH_PREP` · `integration` · `E1` · **🛠 в работе** · target `2026-08-22`
  - **Владелец:** Legal + Backend
  - **Следующий шаг:** Map each store, TTL, purge, audit proof and user contact.
  - **Источник/evidence:** `docs/architecture/personalization-data-ownership.md`

<a id="legal-15"></a>
- [ ] **`LEGAL-15` · Personal-data incident and notification runbook**  
  `P0` · `LAUNCH_PREP` · `decision` · `E1` · **🛠 в работе** · target `2026-08-23`
  - **Владелец:** Legal + Security
  - **Следующий шаг:** Define detection, containment, evidence, notification and owner.
  - **Источник/evidence:** `docs/operations; incident reports`

<a id="legal-10"></a>
- [ ] **`LEGAL-10` · Уведомление Роскомнадзора/актуальность сведений оператора**  
  `P0` · `LAUNCH_PREP` · `live` · `E0` · **🧪 требует проверки** · target `2026-08-18`
  - **Владелец:** Legal + Ops
  - **Следующий шаг:** Проверить обязанность, текущую запись, цели/системы/локализацию and update needs.
  - **Источник/evidence:** `Роскомнадзор: реестр операторов персональных данных`

<a id="legal-16"></a>
- [ ] **`LEGAL-16` · Event information disclaimer, age marking and content rights**  
  `P1` · `LAUNCH_PREP` · `design` · `E0` · **○ не начато** · target `2026-08-23`
  - **Владелец:** Legal + Editorial
  - **Следующий шаг:** Clarify source accuracy, changes, ticket responsibility, images and age labels.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="legal-19"></a>
- [ ] **`LEGAL-19` · Minors/child-directed data and participation rules**  
  `P1` · `LAUNCH_PREP` · `research` · `E0` · **○ не начато** · target `2026-08-22`
  - **Владелец:** Legal + Product
  - **Следующий шаг:** Decide age boundaries, guardian needs and child-directed content handling.
  - **Источник/evidence:** `docs/features/static-site-pages/podborki.md`

<a id="legal-13"></a>
- [ ] **`LEGAL-13` · Processor/vendor register and contractual safeguards**  
  `P1` · `LAUNCH_PREP` · `integration` · `E1` · **🛠 в работе** · target `2026-08-22`
  - **Владелец:** Legal + Ops
  - **Следующий шаг:** Record SpaceWeb, Yandex, Supabase, NotiSend, Fly, GitHub, Kaggle and access roles.
  - **Источник/evidence:** `docs/architecture/personalization-data-ownership.md`

<a id="stream-12"></a>
## QA, безопасность и аналитика

**🔴 BLOCKED** · P0 3/15 · blocked 2 · verify 3 · owner gate 0

<a id="qa-03"></a>
- [ ] **`QA-03` · Anonymous focus release tests**  
  `P0` · `FG_PREP` · `qa` · `E1` · **⛔ заблокировано** · target `2026-08-16`
  - **Владелец:** QA
  - **Следующий шаг:** Implement anonymous_session E2E and owner-scoped receipts.
  - **Источник/evidence:** `docs/features/static-site-pages/release-autotest-gates.md`
  - **Примечание:** FG-04

<a id="qa-06"></a>
- [ ] **`QA-06` · Аудит доступности и исправление критических дефектов**  
  `P0` · `RC` · `qa` · `E0` · **○ не начато** · target `2026-08-24`
  - **Владелец:** Accessibility + QA
  - **Следующий шаг:** Проверить клавиатуру, focus, семантику, контраст, reduced motion и screen reader.
  - **Источник/evidence:** `—`

<a id="qa-12"></a>
- [ ] **`QA-12` · Incident/support rehearsal**  
  `P0` · `STABILIZATION` · `qa` · `E0` · **○ не начато** · target `2026-08-27`
  - **Владелец:** Ops + Support
  - **Следующий шаг:** Simulate auth outage, stale catalog, broken publish, mail delay and rollback.
  - **Источник/evidence:** `—`

<a id="qa-17"></a>
- [ ] **`QA-17` · Final target exact-SHA evidence bundle**  
  `P0` · `STABILIZATION` · `qa` · `E0` · **○ не начато** · target `2026-08-28`
  - **Владелец:** Release owner + QA
  - **Следующий шаг:** Collect build, browser, mobile, legal, mail, migrations and rollback evidence.
  - **Источник/evidence:** `docs/features/static-site-pages/release-autotest-gates.md`

<a id="qa-18"></a>
- [ ] **`QA-18` · Focus feedback data-quality/duplicate reconciliation**  
  `P0` · `FG_ACTIVE` · `qa` · `E0` · **○ не начато** · target `2026-08-21`
  - **Владелец:** Analytics + Backend
  - **Следующий шаг:** Verify one score/revision, idempotent text/screenshot receipts and merge integrity.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group.md`

<a id="qa-08"></a>
- [ ] **`QA-08` · RLS/ACL/security review all public Supabase surfaces**  
  `P0` · `RC` · `qa` · `E2` · **🛠 в работе** · target `2026-08-23`
  - **Владелец:** Security + Backend
  - **Следующий шаг:** Review anonymous auth, screenshots, subscriptions, feedback, merge and admin-only RPCs.
  - **Источник/evidence:** `docs/architecture/personalization-data-ownership.md`

<a id="qa-10"></a>
- [ ] **`QA-10` · Load/rate/anti-abuse tests**  
  `P0` · `RC` · `qa` · `E2` · **🛠 в работе** · target `2026-08-24`
  - **Владелец:** Security + QA
  - **Следующий шаг:** Cover OTP, search, feedback, subscriptions, images and anonymous signups.
  - **Источник/evidence:** `docs/architecture/personalization-data-ownership.md`

<a id="qa-13"></a>
- [ ] **`QA-13` · KPI event schema and consent-aware analytics**  
  `P0` · `FG_PREP` · `integration` · `E1` · **🛠 в работе** · target `2026-08-17`
  - **Владелец:** Analytics + Legal
  - **Следующий шаг:** Define SOR, minimization, TTL, idempotency and no-PII projections.
  - **Источник/evidence:** `docs/architecture/personalization-data-ownership.md`

<a id="qa-15"></a>
- [ ] **`QA-15` · Data-quality checks for events, sources and media**  
  `P0` · `RC` · `qa` · `E2` · **🛠 в работе** · target `2026-08-23`
  - **Владелец:** Editorial + Data
  - **Следующий шаг:** Set blocking thresholds and sampled owner review.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="qa-07"></a>
- [ ] **`QA-07` · Performance/Core Web Vitals budget on representative devices**  
  `P0` · `RC` · `qa` · `E2` · **🧪 требует проверки** · target `2026-08-24`
  - **Владелец:** QA + Frontend
  - **Следующий шаг:** Measure LCP/CLS/INP, JS weight, images and prelaunch/full catalog.
  - **Источник/evidence:** `—`

<a id="qa-09"></a>
- [ ] **`QA-09` · Backup/restore and rollback drills**  
  `P0` · `STABILIZATION` · `qa` · `E3` · **🧪 требует проверки** · target `2026-08-26`
  - **Владелец:** Ops + Backend
  - **Следующий шаг:** Prove Fly SQLite, Supabase migration rollback, artifact pointer and config recovery.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="qa-11"></a>
- [ ] **`QA-11` · Secret/PII redaction in logs and artifacts**  
  `P0` · `RC` · `qa` · `E3` · **🧪 требует проверки** · target `2026-08-24`
  - **Владелец:** Security + QA
  - **Следующий шаг:** Run scans on final workflows and representative failure artifacts.
  - **Источник/evidence:** `docs/features/static-site-pages/release-autotest-gates.md`

<a id="qa-01"></a>
- [x] **`QA-01` · Central machine-readable static-site scenario registry**  
  `P0` · `LAUNCH_PREP` · `ready` · `E2` · **✅ готово** · target `2026-08-04`
  - **Владелец:** QA
  - **Следующий шаг:** Keep planned/implemented/evidence states honest.
  - **Источник/evidence:** `docs/testing/static-site-autotest-scenarios.v1.yml`

<a id="qa-02"></a>
- [x] **`QA-02` · Production build and browser release gates**  
  `P0` · `LAUNCH_PREP` · `ready` · `E3` · **✅ готово** · target `2026-08-04`
  - **Владелец:** QA
  - **Следующий шаг:** Rerun on final RC.
  - **Источник/evidence:** `docs/features/static-site-pages/release-autotest-gates.md`

<a id="qa-04"></a>
- [x] **`QA-04` · Mobile OTP direct/relay fault matrix**  
  `P0` · `LAUNCH_PREP` · `ready` · `E4` · **✅ готово** · target `2026-08-04`
  - **Владелец:** QA
  - **Следующий шаг:** Retest only affected cells after final changes.
  - **Источник/evidence:** `docs/features/static-site-pages/release-autotest-gates.md`

<a id="qa-05"></a>
- [ ] **`QA-05` · PWA system integration E2E Android+iOS**  
  `P1` · `RC` · `qa` · `E1` · **⛔ заблокировано** · target `2026-08-24`
  - **Владелец:** QA
  - **Следующий шаг:** Complete install, relaunch and state persistence.
  - **Источник/evidence:** `docs/features/static-site-pages/release-autotest-gates.md`

<a id="qa-14"></a>
- [ ] **`QA-14` · Launch/focus dashboard for product and reliability metrics**  
  `P1` · `FG_ACTIVE` · `development` · `E0` · **○ не начато** · target `2026-08-21`
  - **Владелец:** Analytics
  - **Следующий шаг:** Show activation, find-within-30, NPS, errors, freshness, mail and transport receipts.
  - **Источник/evidence:** `—`

<a id="qa-16"></a>
- [ ] **`QA-16` · Cross-browser desktop smoke**  
  `P1` · `RC` · `qa` · `E0` · **○ не начато** · target `2026-08-24`
  - **Владелец:** QA
  - **Следующий шаг:** Chromium baseline plus Firefox/Safari/WebKit critical routes.
  - **Источник/evidence:** `—`

<a id="stream-13"></a>
## Инфраструктура и эксплуатация

**🔴 BLOCKED** · P0 0/9 · blocked 4 · verify 3 · owner gate 1

<a id="ops-02"></a>
- [ ] **`OPS-02` · Hosted-сценарий отказа обоих маршрутов без потери данных**  
  `P0` · `RC` · `qa` · `E1` · **⛔ заблокировано** · target `2026-08-22`
  - **Владелец:** QA + Ops
  - **Следующий шаг:** Доказать отсутствие false success, bounded queue и exactly-once reconnect.
  - **Источник/evidence:** `docs/operations/yandex-dependency-resilience.md`

<a id="ops-10"></a>
- [ ] **`OPS-10` · Secrets and credential rotation inventory**  
  `P0` · `LAUNCH_PREP` · `live` · `E1` · **⛔ заблокировано** · target `2026-08-20`
  - **Владелец:** Ops + Security
  - **Следующий шаг:** Rotate NotiSend; verify GitHub/Fly/Supabase/Yandex/Kaggle credentials and owners.
  - **Источник/evidence:** `docs/operations/email-delivery.md`

<a id="ops-12"></a>
- [ ] **`OPS-12` · On-call rota and D0 runbooks**  
  `P0` · `STABILIZATION` · `decision` · `E0` · **🧭 решение владельца** · target `2026-08-27`
  - **Владелец:** Product owner + Ops
  - **Следующий шаг:** Assign people, contact paths, escalation and rollback authority.
  - **Источник/evidence:** `—`

<a id="ops-17"></a>
- [ ] **`OPS-17` · Incident backlog and known-debt review before freeze**  
  `P0` · `STABILIZATION` · `decision` · `E0` · **○ не начато** · target `2026-08-26`
  - **Владелец:** Release owner + Ops
  - **Следующий шаг:** Classify launch blockers, accepted risk and post-launch work.
  - **Источник/evidence:** `docs/reports/incidents`

<a id="ops-01"></a>
- [ ] **`OPS-01` · Capability-specific Yandex resilience contract**  
  `P0` · `LAUNCH_PREP` · `integration` · `E2` · **🛠 в работе** · target `2026-08-16`
  - **Владелец:** Ops + Backend
  - **Следующий шаг:** Integrate residual docs/runtime and close hosted cells.
  - **Источник/evidence:** `PR #295; docs/operations/yandex-dependency-resilience.md`

<a id="ops-11"></a>
- [ ] **`OPS-11` · Monitoring, alerts and synthetic probes**  
  `P0` · `RC` · `integration` · `E2` · **🛠 в работе** · target `2026-08-23`
  - **Владелец:** Ops
  - **Следующий шаг:** Cover root, routes, auth, search, feedback, mail, build freshness and quotas.
  - **Источник/evidence:** `docs/operations/static-site-autotest-strategy.md`

<a id="ops-03"></a>
- [ ] **`OPS-03` · Supabase migration inventory and production apply ledger**  
  `P0` · `LAUNCH_PREP` · `live` · `E3` · **🧪 требует проверки** · target `2026-08-20`
  - **Владелец:** Backend + Ops
  - **Следующий шаг:** List required migrations, dry-run, backups, apply and schema checks.
  - **Источник/evidence:** `supabase/migrations`

<a id="ops-09"></a>
- [ ] **`OPS-09` · CDN/Object Storage/DNS/TLS final verification**  
  `P0` · `RC` · `qa` · `E4` · **🧪 требует проверки** · target `2026-08-25`
  - **Владелец:** Ops
  - **Следующий шаг:** Check domains, certs, cache, MIME, immutable assets and root/ICS.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="ops-13"></a>
- [ ] **`OPS-13` · Fly runtime health/capacity/disk guard**  
  `P0` · `RC` · `qa` · `E4` · **🧪 требует проверки** · target `2026-08-23`
  - **Владелец:** Ops
  - **Следующий шаг:** Review 24h CPU/memory/disk/schedulers/outboxes and alerts.
  - **Источник/evidence:** `PR #322; incident reports`

<a id="ops-08"></a>
- [ ] **`OPS-08` · YDB live RU/billing/cutover/24h observation**  
  `P1` · `LAUNCH_PREP` · `live` · `E1` · **⛔ заблокировано** · target `2026-08-24`
  - **Владелец:** Ops + Backend
  - **Следующий шаг:** Get server RU, alert, budget, exact account guard and 24h observation.
  - **Источник/evidence:** `PR #323`

<a id="ops-07"></a>
- [ ] **`OPS-07` · YDB bounded queue/read model code**  
  `P1` · `LAUNCH_PREP` · `integration` · `E3` · **🛠 в работе** · target `2026-08-18`
  - **Владелец:** Backend
  - **Следующий шаг:** Confirm complete writers/finalizers and default-off contract.
  - **Источник/evidence:** `PR #316; PR #323`

<a id="ops-14"></a>
- [ ] **`OPS-14` · GitHub Actions budget and required-check inventory**  
  `P1` · `LAUNCH_PREP` · `integration` · `E2` · **🛠 в работе** · target `2026-08-18`
  - **Владелец:** DevEx + Ops
  - **Следующий шаг:** Remove duplicate/stale jobs; keep protected expensive jobs bounded.
  - **Источник/evidence:** `.github/workflows`

<a id="ops-15"></a>
- [ ] **`OPS-15` · Kaggle orchestrator/log retention and recoverability**  
  `P1` · `LAUNCH_PREP` · `integration` · `E1` · **🛠 в работе** · target `2026-08-20`
  - **Владелец:** Data + Ops
  - **Следующий шаг:** Collect run logs/evidence, preserve separate BGE/E5 execution and operator trigger.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="ops-16"></a>
- [ ] **`OPS-16` · Public diagnostic page capability truth**  
  `P1` · `FG_ACTIVE` · `qa` · `E2` · **🛠 в работе** · target `2026-08-19`
  - **Владелец:** Frontend + Ops
  - **Следующий шаг:** Ship user-safe diagnostics for direct, relay, auth and core availability.
  - **Источник/evidence:** `docs/operations/yandex-dependency-resilience.md`

<a id="ops-04"></a>
- [x] **`OPS-04` · Weather consumer remains safe default-off**  
  `P1` · `LAUNCH_PREP` · `ready` · `E3` · **✅ готово** · target `2026-08-04`
  - **Владелец:** Frontend
  - **Следующий шаг:** Do not enable until producer/live gates pass.
  - **Источник/evidence:** `PR #316`

<a id="ops-05"></a>
- [ ] **`OPS-05` · Weather producer/live binding/provider/bucket**  
  `P2` · `POST_LAUNCH` · `integration` · `E1` · **⛔ заблокировано** · target `после D0`
  - **Владелец:** Ops
  - **Следующий шаг:** Keep out of D0 scope unless all operator gates close.
  - **Источник/evidence:** `cat-weather-new PR #4`

<a id="ops-06"></a>
- [ ] **`OPS-06` · Weather seven-day canary**  
  `P2` · `POST_LAUNCH` · `live` · `E0` · **○ не начато** · target `после producer`
  - **Владелец:** Ops + Product
  - **Следующий шаг:** Observe freshness, failures and presentation before enablement.
  - **Источник/evidence:** `docs/features/static-site-pages/weather-calendar.md`

<a id="stream-14"></a>
## Коммуникации запуска

**🟠 OPEN** · P0 0/6 · blocked 0 · verify 1 · owner gate 1

<a id="comms-04"></a>
- [ ] **`COMMS-04` · Launch-day Telegram/VK posts and visual assets**  
  `P0` · `D0` · `design` · `E0` · **○ не начато** · target `2026-08-25`
  - **Владелец:** Editorial + Design
  - **Следующий шаг:** Prepare variants, links, tracking and fallback post.
  - **Источник/evidence:** `—`

<a id="comms-06"></a>
- [ ] **`COMMS-06` · Support macros for launch incidents**  
  `P0` · `STABILIZATION` · `design` · `E0` · **○ не начато** · target `2026-08-27`
  - **Владелец:** Support + Editorial
  - **Следующий шаг:** Draft auth, missing event, wrong data, email, privacy and outage replies.
  - **Источник/evidence:** `—`

<a id="comms-10"></a>
- [ ] **`COMMS-10` · Launch notification send list and suppression reconciliation**  
  `P0` · `D0` · `qa` · `E0` · **○ не начато** · target `2026-08-28`
  - **Владелец:** Backend + Email
  - **Следующий шаг:** Freeze eligible pending rows, duplicates, withdrawals and invalid addresses.
  - **Источник/evidence:** `PR #296`

<a id="comms-01"></a>
- [ ] **`COMMS-01` · Public positioning and one-sentence value proposition**  
  `P0` · `PRELAUNCH` · `decision` · `E1` · **🛠 в работе** · target `2026-08-08`
  - **Владелец:** Editorial + Product
  - **Следующий шаг:** Approve compact 1/2/3-sentence variants and use consistently.
  - **Источник/evidence:** `PR #296`

<a id="comms-07"></a>
- [ ] **`COMMS-07` · Focus-group onboarding copy and reminder cadence**  
  `P0` · `FG_PREP` · `design` · `E1` · **🛠 в работе** · target `2026-08-12`
  - **Владелец:** Editorial + Product
  - **Следующий шаг:** Align invite, artifact hints, feedback and prize claims.
  - **Источник/evidence:** `docs/features/static-site-pages/focus-group.md`

<a id="comms-09"></a>
- [ ] **`COMMS-09` · SEO/OG/unfurl copy and images**  
  `P0` · `RC` · `qa` · `E3` · **🧪 требует проверки** · target `2026-08-24`
  - **Владелец:** Editorial + QA
  - **Следующий шаг:** Check root, event, collection and archived pages across TG/VK.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="comms-02"></a>
- [ ] **`COMMS-02` · Official launch press release**  
  `P1` · `LAUNCH_PREP` · `design` · `E1` · **🧭 решение владельца** · target `2026-08-20`
  - **Владелец:** Editorial + Product owner
  - **Следующий шаг:** Approve final facts, quotes, partner references and distribution list.
  - **Источник/evidence:** `PR #296`

<a id="comms-05"></a>
- [ ] **`COMMS-05` · Partner/venue launch briefing**  
  `P1` · `LAUNCH_PREP` · `design` · `E0` · **○ не начато** · target `2026-08-24`
  - **Владелец:** Partnerships
  - **Следующий шаг:** Confirm mentions, link format, support contact and correction process.
  - **Источник/evidence:** `—`

<a id="comms-08"></a>
- [ ] **`COMMS-08` · Public status/incident communication channel**  
  `P1` · `STABILIZATION` · `decision` · `E0` · **○ не начато** · target `2026-08-27`
  - **Владелец:** Product + Support
  - **Следующий шаг:** Choose channel and approval flow for material outages.
  - **Источник/evidence:** `—`

<a id="comms-03"></a>
- [ ] **`COMMS-03` · Prelaunch notification acquisition plan**  
  `P1` · `PRELAUNCH` · `integration` · `E2` · **🛠 в работе** · target `2026-08-14`
  - **Владелец:** Growth + Backend
  - **Следующий шаг:** Define consent copy, form launch, queue monitoring and removal date.
  - **Источник/evidence:** `PR #296`

<a id="stream-15"></a>
## D0 — 1 сентября

**🟠 OPEN** · P0 0/12 · blocked 0 · verify 0 · owner gate 0

<a id="d0-01"></a>
- [ ] **`D0-01` · Freeze exact main SHA and dependency versions**  
  `P0` · `D0` · `live` · `E0` · **○ не начато** · target `2026-08-29`
  - **Владелец:** Release owner
  - **Следующий шаг:** Record SHA, snapshot, build ID and rollback target.
  - **Источник/evidence:** `docs/features/static-site-pages/release-plan.md`

<a id="d0-02"></a>
- [ ] **`D0-02` · Apply required production migrations with backups**  
  `P0` · `D0` · `live` · `E0` · **○ не начато** · target `2026-08-31`
  - **Владелец:** Backend + Ops
  - **Следующий шаг:** Execute approved migration plan and verify schemas/RLS.
  - **Источник/evidence:** `OPS-03`

<a id="d0-03"></a>
- [ ] **`D0-03` · Build final full-catalog artifact and run all blocking gates**  
  `P0` · `D0` · `live` · `E0` · **○ не начато** · target `2026-08-31`
  - **Владелец:** QA + Ops
  - **Следующий шаг:** Require terminal exact-SHA evidence.
  - **Источник/evidence:** `QA-17`

<a id="d0-04"></a>
- [ ] **`D0-04` · Atomic publish full catalog**  
  `P0` · `D0` · `live` · `E0` · **○ не начато** · target `2026-09-01`
  - **Владелец:** Release owner + Ops
  - **Следующий шаг:** Promote only signed approved candidate.
  - **Источник/evidence:** `CORE-06`

<a id="d0-05"></a>
- [ ] **`D0-05` · Publish full robots.txt and sitemap.xml with catalog**  
  `P0` · `D0` · `live` · `E0` · **○ не начато** · target `2026-09-01`
  - **Владелец:** SEO + Ops
  - **Следующий шаг:** Verify atomically with root.
  - **Источник/evidence:** `CORE-10`

<a id="d0-06"></a>
- [ ] **`D0-06` · Production smoke: root/routes/assets/ICS/auth/search/feedback**  
  `P0` · `D0` · `live` · `E0` · **○ не начато** · target `2026-09-01`
  - **Владелец:** QA + Ops
  - **Следующий шаг:** Run immediate public probes and save evidence.
  - **Источник/evidence:** `docs/features/static-site-pages/release-autotest-gates.md`

<a id="d0-07"></a>
- [ ] **`D0-07` · Send one-time launch notification exactly once**  
  `P0` · `D0` · `live` · `E0` · **○ не начато** · target `2026-09-01`
  - **Владелец:** Email + Ops
  - **Следующий шаг:** Dispatch only approved pending rows after successful publication.
  - **Источник/evidence:** `MAIL-12; MAIL-22`

<a id="d0-08"></a>
- [ ] **`D0-08` · Activate launch monitoring and operator room**  
  `P0` · `D0` · `live` · `E0` · **○ не начато** · target `2026-09-01`
  - **Владелец:** Ops + Product
  - **Следующий шаг:** Monitor metrics, inbox and incidents with explicit owners.
  - **Источник/evidence:** `OPS-11; OPS-12`

<a id="d0-09"></a>
- [ ] **`D0-09` · Rollback decision and rehearsal evidence available**  
  `P0` · `D0` · `live` · `E0` · **○ не начато** · target `2026-08-31`
  - **Владелец:** Release owner + Ops
  - **Следующий шаг:** Keep last-good pointer and tested commands.
  - **Источник/evidence:** `CORE-06; QA-09`

<a id="d0-10"></a>
- [ ] **`D0-10` · Publish launch communications**  
  `P0` · `D0` · `live` · `E0` · **○ не начато** · target `2026-09-01`
  - **Владелец:** Editorial + Product
  - **Следующий шаг:** Publish only after public smoke passes.
  - **Источник/evidence:** `COMMS-04`

<a id="d0-11"></a>
- [ ] **`D0-11` · Record immutable release receipt**  
  `P0` · `D0` · `live` · `E0` · **○ не начато** · target `2026-09-01`
  - **Владелец:** Release owner
  - **Следующий шаг:** Store exact identities, evidence links, sends and incidents.
  - **Источник/evidence:** `docs/release/2026-09-01/README.md`

<a id="d0-12"></a>
- [ ] **`D0-12` · Start Telegraph D0 coexistence mode**  
  `P0` · `D0` · `live` · `E0` · **○ не начато** · target `2026-09-01`
  - **Владелец:** Backend + Ops
  - **Следующий шаг:** Use approved canary percentage and monitor outward links.
  - **Источник/evidence:** `CORE-09`

<a id="stream-16"></a>
## После запуска

**🟠 OPEN** · P0 0/3 · blocked 0 · verify 0 · owner gate 0

<a id="post-01"></a>
- [ ] **`POST-01` · D+1 health and incident review**  
  `P0` · `POST_LAUNCH` · `live` · `E0` · **○ не начато** · target `2026-09-02`
  - **Владелец:** Release owner + Ops
  - **Следующий шаг:** Review errors, freshness, mail, search, auth and support.
  - **Источник/evidence:** `—`

<a id="post-04"></a>
- [ ] **`POST-04` · D+10 Telegraph cutover decision**  
  `P0` · `POST_LAUNCH` · `decision` · `E0` · **○ не начато** · target `2026-09-11`
  - **Владелец:** Product + Ops
  - **Следующий шаг:** Require 72h soak, parity and no broken outward links.
  - **Источник/evidence:** `CORE-09`

<a id="post-05"></a>
- [ ] **`POST-05` · Prelaunch/focus retention cleanup**  
  `P0` · `POST_LAUNCH` · `live` · `E0` · **○ не начато** · target `по policy`
  - **Владелец:** Legal + Backend
  - **Следующий шаг:** Delete/retain according to approved purposes and evidence.
  - **Источник/evidence:** `LEGAL-14; MAIL-21`

<a id="post-02"></a>
- [ ] **`POST-02` · D+3 focus-group findings and prioritized backlog**  
  `P1` · `POST_LAUNCH` · `decision` · `E0` · **○ не начато** · target `2026-09-04`
  - **Владелец:** Product + Analytics
  - **Следующий шаг:** Publish internal synthesis with evidence and severity.
  - **Источник/evidence:** `FG-31`

<a id="post-03"></a>
- [ ] **`POST-03` · D+7 KPI and retention review**  
  `P1` · `POST_LAUNCH` · `live` · `E0` · **○ не начато** · target `2026-09-08`
  - **Владелец:** Product + Analytics
  - **Следующий шаг:** Compare against launch targets and guardrails.
  - **Источник/evidence:** `QA-14`

<a id="post-06"></a>
- [ ] **`POST-06` · Release retrospective and dashboard standardization**  
  `P1` · `POST_LAUNCH` · `decision` · `E0` · **○ не начато** · target `2026-09-12`
  - **Владелец:** Product + Release owner
  - **Следующий шаг:** Decide standard schema, optional GitHub Project and automation.
  - **Источник/evidence:** `docs/release/2026-09-01/UPDATE.md`

<a id="post-07"></a>
- [ ] **`POST-07` · Enable deferred features only through own gates**  
  `P1` · `POST_LAUNCH` · `decision` · `E0` · **○ не начато** · target `после D0`
  - **Владелец:** Product owner
  - **Следующий шаг:** No silent rollout of weather, recommendations, advanced personalization or weak collections.
  - **Источник/evidence:** `COL-14; P13N-02; OPS-05`
