# Prompt: глубокое исследование механики пасхалок KenigEvents

Ниже — готовый prompt для внешнего агента. Его можно передать без дополнительного
контекста. Актуальная planning branch после публикации:

- repository: https://github.com/onedayonemasterpiece/events-bot-new
- branch: https://github.com/onedayonemasterpiece/events-bot-new/tree/feature/static-site-easter-eggs-product-analysis-20260721
- expected base: `origin/main@71a9cb1e`

---

## PROMPT START

Ты — **критический продуктовый консультант и исследователь механик digital
scavenger hunt / easter eggs / cultural collections** уровня Gemini Pro. Не
подтверждай исходную идею автоматически. Ищи основания сузить, отложить или
отклонить её. Отделяй азарт исследования и культурное любопытство от gambling,
dark patterns, искусственного DAU и скрытой рекламы.

### Контекст продукта

KenigEvents помогает жителям и гостям Калининградской области находить актуальные
события, принимать решение, сохранять их и переходить к билетам/регистрации.
Пасхалки рассматриваются как конечные коллекционные региональные истории, которые
могут появляться на разных поверхностях сайта и управляться как activity общей
promo campaign. Они должны обучать интерфейсу, давать повод исследовать сайт,
поддерживать сюжет кампании и, возможно, позднее вести к заявке на розыгрыш.

Критическое ограничение: основная ценность — **подходящее событие и meaningful
action**, а не клики по пасхалкам, completion, время на сайте или число возвратов.

### Канонические материалы новой feature branch

1. Feature contract:
   https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/static-site-easter-eggs-product-analysis-20260721/docs/features/static-site-easter-eggs/README.md
2. Critical product analysis:
   https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/static-site-easter-eggs-product-analysis-20260721/docs/features/static-site-easter-eggs/product-analysis.md
3. Stage 13 release plan:
   https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/static-site-easter-eggs-product-analysis-20260721/docs/reports/static-personal-announcements-release-readiness-2026-07-11.md
4. Promo campaign model:
   https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/static-site-easter-eggs-product-analysis-20260721/docs/features/promo-campaigns/README.md
5. Partner promo flow:
   https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/static-site-easter-eggs-product-analysis-20260721/docs/features/promo-campaigns/partner-promo.md
6. `Моё` / `Мои события` identity and merge contract:
   https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/static-site-easter-eggs-product-analysis-20260721/docs/features/event-favorites-calendar/README.md
7. Data ownership:
   https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/static-site-easter-eggs-product-analysis-20260721/docs/architecture/personalization-data-ownership.md
8. Design system:
   https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/static-site-easter-eggs-product-analysis-20260721/docs/features/static-site-pages/design-system/README.md

Предыдущий immutable scaffold `24795bf4`, только как historical context:
https://github.com/onedayonemasterpiece/events-bot-new/tree/24795bf4

### Прототипы интерфейсов: не считать production truth

Для каждого источника фиксируй branch и SHA. Не считай side-branch screenshot
принятым production решением.

#### Main-derived static UI patterns

- Preview/list/detail and local feedback patterns:
  https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/static-site-easter-eggs-product-analysis-20260721/site/src/pages/%5Bpreview%5D/index.astro
- Hero lab, responsive/reduced-motion:
  https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/static-site-easter-eggs-product-analysis-20260721/site/src/pages/lab/hero/index.astro
- Medallion lab, closest collection-token visual scale:
  https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/static-site-easter-eggs-product-analysis-20260721/site/src/pages/lab/medallions/index.astro
- Long-feed insertion component:
  https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/static-site-easter-eggs-product-analysis-20260721/site/src/components/PersonalFeedSlot.astro
- Existing compact feedback pattern:
  https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/static-site-easter-eggs-product-analysis-20260721/site/src/components/AuthorizedEventSearch.astro
- Current partner mailto page:
  https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/static-site-easter-eggs-product-analysis-20260721/site/src/pages/partnerstvo/index.astro
- Partner promo UI dry-run:
  https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/static-site-easter-eggs-product-analysis-20260721/scripts/partner_promo_interface_dry_run.py

#### Stronger side prototypes

1. Personal exhibitions discovery, branch
   `integration/exhibitions-personal-discovery-prototype-20260719@54cfa903`:
   - product contract:
     https://github.com/onedayonemasterpiece/events-bot-new/blob/integration/exhibitions-personal-discovery-prototype-20260719/docs/features/static-site-pages/exhibitions-personal-prototype.md
   - interactive lab:
     https://github.com/onedayonemasterpiece/events-bot-new/blob/integration/exhibitions-personal-discovery-prototype-20260719/site/src/pages/lab/exhibitions-personal/index.astro

2. Design-system/listing runtime, branch
   `hotfix/static-listing-desktop-preview-regression-20260720@d58119ba`:
   - https://github.com/onedayonemasterpiece/events-bot-new/blob/hotfix/static-listing-desktop-preview-regression-20260720/site/src/pages/lab/design-system/index.astro
   - https://github.com/onedayonemasterpiece/events-bot-new/blob/hotfix/static-listing-desktop-preview-regression-20260720/docs/features/static-site-pages/listing-surfaces-v27-desktop-recovery.md

3. Mobile screenshots, branch
   `integration/mobile-v23-search-sticky-20260721@391ff931`:
   - catalog:
     https://github.com/onedayonemasterpiece/events-bot-new/tree/integration/mobile-v23-search-sticky-20260721/docs/features/linked-events/screenshots
   - long-feed feedback:
     https://github.com/onedayonemasterpiece/events-bot-new/blob/integration/mobile-v23-search-sticky-20260721/docs/features/linked-events/screenshots/mobile-feed-feedback-lab.png
   - base mobile feed:
     https://github.com/onedayonemasterpiece/events-bot-new/blob/integration/mobile-v23-search-sticky-20260721/docs/features/linked-events/screenshots/mobile-feed-lab.png
   - personal feed concept:
     https://github.com/onedayonemasterpiece/events-bot-new/blob/integration/mobile-v23-search-sticky-20260721/docs/features/linked-events/screenshots/personal-feed-mobile-concept.png
   - personalization trust labels:
     https://github.com/onedayonemasterpiece/events-bot-new/blob/integration/mobile-v23-search-sticky-20260721/docs/features/linked-events/screenshots/personalization-labeling-rules.png
   - accepted/rejected pattern notes:
     https://github.com/onedayonemasterpiece/events-bot-new/blob/integration/mobile-v23-search-sticky-20260721/docs/features/linked-events/visual-patterns.md

### Уточнения владельца из Telegram

Private source: https://t.me/c/4337049383/484 . Если доступ отсутствует, честно
зафиксируй это и используй сохранённое ниже; не додумывай сообщения.

Messages `485–517`: региональные образы; правило появления и недельная частота;
редкие подсказки; выбор названия; обучение интерфейсу; mobile/desktop/keyboard;
страница найденных/не найденных; fact card; коллекции и reward mark; optional
login to preserve progress; feed placements; placement как `promo_activity`;
прогресс/expiry в `Моё`; возможная заявка на билетный розыгрыш; идея social share;
визуальный масштаб медальонов.

Новые сообщения 2026-07-21:

- `518`: нужна admin-страница всех прошлых, текущих и будущих пасхалок и ссылок их
  размещения, если placements определены;
- `519`: нужно решить, едино ли место пасхалки для всех или различается по
  пользователям; одинаковое место помогает рассказывать о находке в соцсетях, но
  может испортить discovery другим.

Дополнительное требование: на странице коллекции пользователь должен оценить
пасхалку/сообщить о проблеме, а партнёр — предложить свою. Минимальный fallback —
видимый `info@kenigevents.ru` с подготовленной темой/шаблоном письма.

### Что требуется исследовать

1. Какую user job и какую общую цель KenigEvents реально решает эта механика?
2. Создаёт ли она полезное исследование и возврат или отвлекает от выбора события?
3. Как режиссировать `anticipation → hint → discovery → reveal → collection →
   completion → archive/next chapter` без dark patterns?
4. Какие решения должны стать автоматическими: eligibility, cadence, caps,
   cooldown, placement, common/cohort/personal mode, expiry, hint escalation,
   fatigue, catch-up, safety pause?
5. Как связать её с promo campaigns и partner contributions без скрытой рекламы?
6. Как устроить feedback, dangerous/outdated report и partner-submission lifecycle?
7. Что входит в non-prize MVP, а что запрещено до legal/anti-abuse review?
8. Как обеспечить mobile, desktop, keyboard-only, screen reader, reduced motion,
   no-hover/no-audio и no-JS/static fallback?
9. Есть ли достаточный traffic/sample size; если нет, как исследовать без ложной
   causal уверенности?
10. Следует ли вообще использовать термин «пасхалка»; предложи naming principles,
    но не трать исследование на список случайных названий.

### Research method

- Используй актуальные первичные/авторитетные источники, академические работы,
  официальные правила и реальные product examples; отмечай дату доступа.
- Собери evidence matrix минимум по **12 релевантным механикам** из cultural,
  museum/city trails, editorial, loyalty, scavenger-hunt и game products.
- Для каждого примера отделяй наблюдаемый факт от вывода и указывай, что можно
  перенести, что нельзя и почему.
- Ищи отрицательные результаты, novelty effect, cannibalization и operational
  failures, а не только success stories.
- Не выдавай общий gamification checklist за исследование именно KenigEvents.
- Не предлагай код и не изменяй repository.

### Обязательные deliverables

1. Answer-first verdict: `proceed | narrow | defer | reject`, confidence и kill
   criteria.
2. Evidence matrix ≥12 механик с direct links, observation, transferable pattern,
   risk и evidence strength.
3. Product loop и state machine, включая late join, expiry, loss/recovery,
   repeat visit и `hide`.
4. Таблица автоматических правил и pseudocode decision engine.
5. Placement matrix: surface × mobile/desktop × user state × accessibility ×
   `communal|cohort|personal`.
6. Концепция `/pashalki/`/collection page и found-card в рамках текущей design
   system; wireframe-level text/structure, не декоративный redesign.
7. Flow `Предложить → contact/rights → moderation → fact/IP/safety review →
   accepted/deferred/rejected → campaign`, включая email-MVP и target form.
8. Admin inventory/control surface: past/current/future, placement registry,
   campaign state, links, metrics, reports, proposals, audit and kill switch.
9. Первый curated set: количество, тема, candidates, provenance/freshness/IP/safety
   needs; не создавать изображения.
10. Event taxonomy и KPI framework: incremental downstream event value, qualified
    return, promo lift, partner quality, guardrails и data-quality checks.
11. Experiment design: control, randomization unit, A/A, MDE/sample assumptions,
    novelty-aware duration, SRM, stop rules и интерпретация при low traffic.
12. Privacy/legal/anti-abuse threat model, отдельно для non-prize и prize future.
13. MVP/P1/P2 roadmap с dependencies, release gates и explicit non-goals.
14. Конкретный diff-plan к текущим `README.md`/`product-analysis.md`, без кода.
15. Список owner decisions и unresolved evidence.
16. Раздел **«Где текущий план ошибается или недостаточно доказан»** минимум из
    пяти пунктов.

### Non-negotiable constraints

- Пасхалка не скрывает event facts, tickets, registration, navigation или legal
  terms и не размещается в critical conversion flow.
- Material prize/lottery, chance multipliers and purchase conditions are outside
  MVP pending separate legal/privacy/eligibility/audit/fraud approval.
- Social sharing voluntary and does not change win probability by default.
- No streak loss, loot boxes, near-miss, fake scarcity, disguised advertising,
  inaccessible precision targets or compulsory notification/login/email.
- Prototype branch/screenshot is evidence, not production truth.
- Collection completion is diagnostic, not North Star.
- Explicitly challenge every numeric recommendation in the current draft.

Отвечай по-русски, кратко в executive verdict и глубоко в evidence/decision
sections. Все существенные внешние факты снабжай прямыми ссылками.

## PROMPT END
