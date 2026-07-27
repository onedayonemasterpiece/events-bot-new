# Static personal announcements

> Status: release umbrella; full-scope public release is not ready.
> Canonical readiness checklist: [2026-07-11 release audit](../../reports/static-personal-announcements-release-readiness-2026-07-11.md).
> Current event-page platform/cutover slice: [static event-page release plan](../static-site-pages/release-plan.md).
> **30.07.2026 decision:** this date is now a limited research release for a
> focus group of at most 200 verified participants, not the first public
> release. Its canonical product design, factual implementation audit and
> implementation handoff are in the
> [focus-group plan](../../backlog/features/static-site-focus-group/README.md).

## Purpose

This is the navigation home for the public static-site release and its personalization capabilities. Detailed facts stay in their feature homes; this page prevents F1–F17 from becoming an unowned flat checklist.

All F1–F17 remain mandatory for the eventual first public release/presentation.
The 30.07 focus-group release has its own narrower gates and cannot be used to
mark missing F1–F17 as complete.

## Capability ownership

| ID | Capability | Canonical home | Stage |
|---|---|---|---|
| F1 | Smart Update effect → coalesced static rebuild | [Static pages](../static-site-pages/README.md), [event-page release plan](../static-site-pages/release-plan.md), [builder operations](../../operations/kaggle-static-site-builder.md) | partial / production disabled |
| F2 | Vector-based related events | [Semantic retrieval](../unsigned-personalization/semantic-vector-retrieval.md) | canary / partial |
| F3 | Smart authorized search | [Authorized search](../unsigned-personalization/authorized-event-search.md) | canary / production root pending |
| F4 | Email with exactly three recommendations + published personal page, delivered through NotiSend to at most 200 actively consented users at launch | [Personal email announcements](../personal-email-announcements/README.md) | design / provider routing accepted |
| F5 | Frozen public release UI | [Release UI contract](../static-site-pages/release-ui-contract.md), [design system and live catalog](../static-site-pages/design-system/README.md) | design-system candidate implemented / immutable RC sign-off pending |
| F6 | List/detail/action personalization telemetry | [Unsigned personalization](../unsigned-personalization/README.md), [production integration](../unsigned-personalization/production-integration.md) | local preview; remote ingest design |
| F7 | Yandex or verified-email identity | [Site user identity](../site-user-identity/README.md) | partial/design |
| F8 | SpaceWeb retained mailbox, read-only Yandex IMAP copy pipeline, direct Mail Trigger canary, Postbox transactional delivery, NotiSend recommendation delivery, bounce/complaint and suppression | [Email delivery](../../operations/email-delivery.md) | inbound live / outbound production gated |
| F9 | Durable favorites | [Favorites and calendar](../event-favorites-calendar/README.md) | design |
| F10 | Login/logout and profile linking | [Site user identity](../site-user-identity/README.md) | login partial; merge design |
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
- [Email delivery](../../operations/email-delivery.md) owns deliverability shared by recommendation and transactional streams.
- `origin/main` is the only production source of truth; side branches are evidence/WIP until merged.
- Branch refresh/supersede decisions: [2026-07-11 branch refresh report](../../reports/static-personal-announcements-branch-refresh-2026-07-11.md).

## Global product decisions

Current decisions and questions that affect several feature families live in [global-product-decisions.md](global-product-decisions.md). Implementation-level questions remain in their feature homes.

## Separate post-release product tracks

- [Пасхалки о Калининграде](../static-site-easter-eggs/README.md) — Stage 13
  product discovery для конечных культурных коллекций как нового
  `promo_activity`. Механика имеет отдельный feedback/partner intake, admin
  inventory, автоматическую режиссуру и holdout-аналитику. Она не блокирует
  первую презентацию и не разрешает production implementation до owner,
  accessibility, data, privacy/IP/safety и non-prize experiment gates.

## Documentation completion rule

A release capability is considered properly documented only when it has:

- one canonical home or an explicit child contract under a canonical parent;
- honest capability-level stage (`design`, `prototype`, `canary`, `production-enabled`, `production-verified`);
- links to requirements/architecture/operations/tests/entrypoints;
- data owner and security boundary;
- acceptance and rollback evidence expectations;
- branch owner/status when implementation is not in `origin/main`.
