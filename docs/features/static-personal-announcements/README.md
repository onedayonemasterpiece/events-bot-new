# Static personal announcements

> Status: release umbrella. Full-scope public release is not established by the evidence on this page; a documentation update is not a new production verdict.
> Documentation routing updated: 2026-09-05, prepared in PR #587.
> Original readiness checklist: [2026-07-11 release audit](../../reports/static-personal-announcements-release-readiness-2026-07-11.md).
> Event-page platform/cutover slice: [static event-page release plan](../static-site-pages/release-plan.md).
> **Current cross-feature integration:** [release-integration.md](release-integration.md) — reliable transport in both directions, statistics, product decisions, personalization, voice Search and the site-wide island system.

## Purpose

This is the navigation home for the public static-site release and its personalization capabilities. Detailed facts stay in their feature homes; this page prevents F1–F17 from becoming an unowned flat checklist.

All F1–F17 are mandatory for the first public release/presentation. Canaries may be staged for risk control, but they do not reduce the final release scope. A new voice or Floating Island initiative must consume these foundations, not replace their release obligations with an isolated UI, provider call or another profile/analytics system.

## Как читать связанный план

Начинай с event-page [Плана релиза](../static-site-pages/release-plan.md) и этого umbrella, затем переходи по конкретным владельцам:

| Вопрос | Текущий владелец требований |
|---|---|
| Как соединяются контуры и текущие задачи? | [Сквозная интеграция релиза](release-integration.md) |
| Как надёжно читать/отправлять действия, когда один маршрут недоступен? | [Production integration](../unsigned-personalization/production-integration.md), [Yandex dependency resilience](../../operations/yandex-dependency-resilience.md) |
| Какие факты/метрики собираются, с каким denominator и где хранятся? | [Analytics](../static-site-pages/analytics/README.md); обратный Supabase Edge → Yandex ingest маршрут — §24 |
| Как статистика превращается в проверенное продуктовое решение? | [Product model](../../product-model/README.md), feature-specific MeasurementQuestions и reviewed evidence; не новый clickstream/dashboard framework |
| Как активируется и применяется персонализация? | Ручные [requirements](../static-site-pages/personalizaion/requirements.md), [целевой blueprint](../static-site-pages/personalizaion/personalization-to-be.md), [data ownership](../../architecture/personalization-data-ownership.md) |
| Как развивать разговорный поиск? | [Product contract](../static-site-pages/smart-vector-search/agent-assisted-event-discovery.md), [voice solution](../static-site-pages/smart-vector-search/voice-search-solution-v1.md) |
| Как спроектировать острова для всех страниц? | [Постановка нового окна](../static-site-pages/design-system/window-prompts/20260905-floating-islands-system-design.md), существующий DS pattern `pattern.detached-chrome-control-islands` |
| Что считается проверкой и релизом? | [Autotest strategy](../../operations/static-site-autotest-strategy.md), [scenario registry](../../testing/static-site-autotest-scenarios.v1.yml), [release gates](../static-site-pages/release-autotest-gates.md) |

Документы с датированными июльскими/августовскими ledger сохраняют исторические evidence и полезные ограничения. Их `Done`, `Missing`, `NO-GO`, counts или старые candidate URLs нельзя читать как свежую проверку текущего production. Принятое требование, текущий source, terminal test, deployed SHA, feature exposure и измеренный outcome — разные утверждения.

Текущее [#621](https://github.com/onedayonemasterpiece/events-bot-new/issues/621) ведёт нормализацию и один Kaggle StaticSiteBuilder/current-bucket published-preview path. Старый future/default-off two-root ALB из event-page plan не является новым обязательным provision-step этого пути. Целостность артефактов, свежесть, подтверждаемое promotion и rollback остаются обязательными; documentary update не разрешает deploy и не изменяет общий STATUS.

## Capability ownership

**Stage below = carried-forward historical checkpoint, not a newly verified status on 2026-09-05.** Current delivery/deployment/acceptance must be reconciled against the relevant source and release evidence before changing these cells. No capability has been dropped or silently marked complete.

| ID | Capability | Canonical home | Historical stage / unverified now |
|---|---|---|---|
| F1 | Smart Update effect → coalesced static rebuild | [Static pages](../static-site-pages/README.md), [event-page release plan](../static-site-pages/release-plan.md), [builder operations](../../operations/kaggle-static-site-builder.md) | partial / production disabled |
| F2 | Vector-based related events | [Semantic retrieval](../unsigned-personalization/semantic-vector-retrieval.md) | canary / partial |
| F3 | Smart authorized search | [Canonical Search](../static-site-pages/smart-vector-search/README.md); [implementation history](../unsigned-personalization/authorized-event-search.md); [conversational extension](../static-site-pages/smart-vector-search/agent-assisted-event-discovery.md) | canary / production root pending; voice extension documented, not runtime-verified |
| F4 | Email with exactly three recommendations + published personal page, delivered through NotiSend only to actively consented users admitted within the shared 200-unique-recipient ceiling | [Personal email announcements](../personal-email-announcements/README.md) | design / provider routing accepted |
| F5 | Frozen public release UI | [Release UI contract](../static-site-pages/release-ui-contract.md), [design system and live catalog](../static-site-pages/design-system/README.md) | design-system candidate implemented / immutable RC sign-off pending |
| F6 | List/detail/action personalization and its measurement | [Personalization blueprint](../static-site-pages/personalizaion/personalization-to-be.md), [analytics](../static-site-pages/analytics/README.md), [production integration](../unsigned-personalization/production-integration.md); [earlier implementation](../unsigned-personalization/README.md) | local preview; remote ingest design; closed learning/readout loop not established here |
| F7 | Yandex or verified-email identity | [Site user identity](../site-user-identity/README.md) | partial/design |
| F8 | SpaceWeb retained mailbox, read-only Yandex IMAP copy pipeline, direct Mail Trigger canary, Postbox transactional delivery, NotiSend recommendation delivery, bounce/complaint and suppression | [Email delivery](../../operations/email-delivery.md) | inbound live / outbound production gated |
| F9 | Durable favorites | [Favorites and calendar](../event-favorites-calendar/README.md) | design |
| F10 | Login/logout and profile linking | [Site user identity](../site-user-identity/README.md), [activation/link semantics](../static-site-pages/personalizaion/personalization-to-be.md) | login partial; merge design |
| F11 | Event transport schedules/cards | [Event transport](../event-transport/README.md) | implementation branch / refresh blocker |
| F12 | Calendar action backed by favorite state | [Favorites and calendar](../event-favorites-calendar/README.md) | ICS preview; durable save design |
| F13 | Catalog freshness vs canonical bot DB | [Static pages](../static-site-pages/README.md), [builder operations](../../operations/kaggle-static-site-builder.md) | partial / production blocked |
| F14 | Comment-derived discussion signals | [Event comment feedback](../event-comment-feedback/README.md) | docs/probe; production missing |
| F15 | Share with generated image | [Event sharing](../static-site-pages/event-sharing.md) | preview canvas; durable assets missing |
| F16 | Correct image focus/crop | [Image framing](../static-site-pages/image-framing.md) | renderer preview; metadata producer missing |
| F17 | Admin issue report → ArtKodex repair history | [Event issue reporting](../event-issue-reporting/README.md) | prototype branch; clean port required |

## Cross-feature authorities

- Smart Update owns canonical event meaning and event-quality prevention.
- [Event quality release monitoring](../../operations/event-quality-release-monitoring.md) owns audit cadence, incidents, root-cause closure and trend evidence.
- [Personalization data ownership](../../architecture/personalization-data-ownership.md) owns Supabase/YDB/Fly/Object Storage boundaries.
- [Production integration](../unsigned-personalization/production-integration.md) owns capability-aware product transport, confirmation and replay semantics. A feature does not create its own fallback client.
- [Analytics](../static-site-pages/analytics/README.md) owns actor/session/metric/consent/delivery definitions; [product-model](../../product-model/README.md) owns the interpretation/evidence/decision methodology. Pipeline health and social post counts alone are not product outcomes.
- [Email delivery](../../operations/email-delivery.md) owns deliverability shared by recommendation and transactional streams.
- `origin/main` remains the production source authority; unmerged branches and documentation are evidence/WIP, not deployed behavior. A main commit alone also does not prove deployment or user outcome.
- Branch refresh/supersede history: [2026-07-11 branch refresh report](../../reports/static-personal-announcements-branch-refresh-2026-07-11.md).

## Global product decisions

Current decisions and questions that affect several feature families live in [global-product-decisions.md](global-product-decisions.md). Implementation-level questions remain in their feature homes. Cross-feature dependencies and regression cases for the recovered release/voice/islands package live in [release-integration.md](release-integration.md).

## Separate post-release product tracks

- [Пасхалки о Калининграде](../static-site-easter-eggs/README.md) — Stage 13 product discovery для конечных культурных коллекций как нового `promo_activity`. Механика имеет отдельный feedback/partner intake, admin inventory, автоматическую режиссуру и holdout-аналитику. Она не блокирует первую презентацию и не разрешает production implementation до owner, accessibility, data, privacy/IP/safety и non-prize experiment gates.

First-party action-map diagnostics also retain their separate default-OFF campaign contract in the event-page release plan. A voice/island measurement question does not automatically enable raw diagnostic capture or turn it into a mandatory public-launch feature.

## Documentation completion rule

A release capability is considered properly documented only when it has:

- one canonical home or an explicit child contract under a canonical parent;
- separate and honest definition/delivery/test/deployment/measurement/owner-acceptance evidence;
- links to requirements/architecture/operations/tests/entrypoints;
- data owner, security boundary, activation and purpose-specific consent semantics;
- acceptance and rollback evidence expectations;
- a product question, observable outcome, denominator and reproducible readout/query when measurement is in scope;
- branch owner/status when implementation or a proposed documentation correction is not in `origin/main`.

The 2026-09-05 recovery updates routing and integration requirements; it does not replace a fresh end-to-end release audit or close F1–F17 by editing this page.
