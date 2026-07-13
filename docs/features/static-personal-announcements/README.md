# Static personal announcements

> Status: release umbrella; full-scope public release is not ready.
> Canonical readiness checklist: [2026-07-11 release audit](../../reports/static-personal-announcements-release-readiness-2026-07-11.md).

## Purpose

This is the navigation home for the public static-site release and its personalization capabilities. Detailed facts stay in their feature homes; this page prevents F1–F17 from becoming an unowned flat checklist.

All F1–F17 are mandatory for the first public release/presentation. Canaries may be staged for risk control, but they do not reduce the final release scope.

## Capability ownership

| ID | Capability | Canonical home | Stage |
|---|---|---|---|
| F1 | Smart Update effect → coalesced static rebuild | [Static pages](../static-site-pages/README.md), [builder operations](../../operations/kaggle-static-site-builder.md) | partial / production disabled |
| F2 | Vector-based related events | [Semantic retrieval](../unsigned-personalization/semantic-vector-retrieval.md) | canary / partial |
| F3 | Smart authorized search + saved public search tags | [Authorized search](../unsigned-personalization/authorized-event-search.md) | search canary; tag save/curation/static generation missing |
| F4 | Email with exactly three recommendations + published personal page, delivered through NotiSend to at most 200 actively consented users at launch | [Personal email announcements](../personal-email-announcements/README.md) | design / provider routing accepted |
| F5 | Frozen public release UI, including responsive navigation decision | [Release UI contract](../static-site-pages/release-ui-contract.md), [responsive navigation](../static-site-pages/responsive-navigation.md) | adaptive hybrid direction selected; visual sign-off pending |
| F6 | List/detail/action personalization telemetry | [Unsigned personalization](../unsigned-personalization/README.md), [production integration](../unsigned-personalization/production-integration.md) | local preview; remote ingest design |
| F7 | Site-wide choice of Yandex identity/email or manually entered verified email | [Site user identity](../site-user-identity/README.md) | Yandex works only in search; global shell and passwordless email journey design |
| F8 | SpaceWeb retained mailbox, read-only Yandex IMAP copy pipeline, direct Mail Trigger canary, Postbox transactional delivery including D-1 event reminders, NotiSend recommendation delivery, bounce/complaint and suppression | [Email delivery](../../operations/email-delivery.md), [event notifications](../event-email-notifications/README.md) | Postbox feedback+worker live/verified; event producers, warm-up and NotiSend application flow gated |
| F9 | Durable favorites | [Favorites and calendar](../event-favorites-calendar/README.md) | design |
| F10 | Global login/logout/account state and profile linking | [Site user identity](../site-user-identity/README.md) | search-only account state; global restore/logout/forget and merge design |
| F11 | Event transport schedules/cards | [Event transport](../event-transport/README.md), [optional «Как добраться» gallery card](../event-transport/gallery-how-to-get-there-card.md) | preliminary rail+bus slice validated in refreshed draft PR #37; optional gallery prototype, release-UI integration and automatic refresh pending |
| F12 | Calendar action backed by favorite state, with visible D-1 email-reminder status | [Favorites and calendar](../event-favorites-calendar/README.md) | ICS preview; durable save/reminder UX design |
| F13 | Catalog freshness vs canonical bot DB | [Static pages](../static-site-pages/README.md), [builder operations](../../operations/kaggle-static-site-builder.md) | partial / production blocked |
| F14 | Comment-derived discussion signals | [Event comment feedback](../event-comment-feedback/README.md) | docs/probe; production missing |
| F15 | Share with generated image | [Event sharing](../static-site-pages/event-sharing.md) | preview canvas; durable assets missing |
| F16 | Correct image focus/crop | [Image framing](../static-site-pages/image-framing.md) | renderer preview; metadata producer missing |
| F17 | Admin issue report → ArtKodex repair history | [Event issue reporting](../event-issue-reporting/README.md) | prototype branch; clean port required |

## Cross-cutting visual readiness

| ID | Capability | Canonical home | Stage |
|---|---|---|---|
| M1 | Event-detail organizer/venue/festival medallions | [Event token medallions](../static-site-pages/event-token-medallions.md) | clean consolidated draft PR #38: 25 organizer/venue + 11 festival/venue-brand entries; P0 shortlist and owner visual sign-off pending |
| M1-QA | Exhaustive medallion visual cleanliness | [Medallion visual QA](../static-site-pages/medallion-visual-qa.md) | separate release gate missing: Playwright must inventory and capture every actual target page/layout with zero clipping, dirty/cut shadows, alpha mattes, overflow or unreadable assets |
| M2 | No duplicate images inside an event gallery | [Event image duplicate audit](../../operations/event-image-duplicate-audit.md) | current production baseline reported degraded; exhaustive hash-to-visual audit and root-cause burn-down pending |
| M3 | Consolidated event engagement from sources and site | [Consolidated event engagement](../post-metrics/consolidated-event-engagement.md) | TG/VK source metrics and partial counters exist; one source+site read function and ecological view/share persistence are missing |

## Cross-feature authorities

- Smart Update owns canonical event meaning and event-quality prevention.
- [Event quality release monitoring](../../operations/event-quality-release-monitoring.md) owns audit cadence, incidents, root-cause closure and trend evidence.
- [Personalization data ownership](../../architecture/personalization-data-ownership.md) owns Supabase/YDB/Fly/Object Storage boundaries.
- [Email delivery](../../operations/email-delivery.md) owns deliverability shared by recommendation and transactional streams.
- `origin/main` is the only production source of truth; side branches are evidence/WIP until merged.
- Branch refresh/supersede decisions: [2026-07-11 branch refresh report](../../reports/static-personal-announcements-branch-refresh-2026-07-11.md).

## Global product decisions

Current decisions and questions that affect several feature families live in [global-product-decisions.md](global-product-decisions.md). Implementation-level questions remain in their feature homes.

## Release sequencing constraints

- Navigation keeps one cross-device information architecture but adapts its geometry: compact mobile brand-tag disclosure, persistent desktop horizontal navigation with the shallow hybrid tag as the recommended release candidate. The exact immutable preview still needs owner sign-off.
- The gallery slide «Как добраться» is an optional F11 presentation candidate: it may summarize validated transport data after real event media, but cannot replace the accessible full schedule block or weaken the nightly refresh/fail-closed gates.
- Release galleries require zero confirmed intra-event image duplicates across the full active/future public inventory. The current baseline must be established by the [read-only SHA-to-visual audit](../../operations/event-image-duplicate-audit.md), then each confirmed failure family is closed at its ingest/Smart Update/persistence/render root cause rather than by a one-off URL cleanup.
- Popularity and counters are event-level, not separate source/site products: `/popular_posts`, daily, video selection and static pages must use the single [consolidated engagement contract](../post-metrics/consolidated-event-engagement.md) for source + site views/likes/shares, with compact current aggregates and bounded history under the Supabase storage budget.
- Medallion readiness is part of F5/UI presentation acceptance: use only clean draft PR #38, finish or explicitly owner-defer the production-backed P0 shortlist, refresh the gap audit within 48 hours of RC, then pass the separate exhaustive Playwright target-surface capture with zero visual defects before owner sign-off.
- Identity is a global static-site release capability: every generated HTML page must render the shared account controller with Yandex login, passwordless verified-email login, logout and device-local forget-email semantics; `/poisk/` may consume but may not own this state.
- Public presentation requires CDN-backed delivery of the full canonical static site and an asset audit proving that runtime images are lightweight WebP or safe SVG. The detailed gate lives in the [release checklist](../../reports/static-personal-announcements-release-readiness-2026-07-11.md#stage-2--production-static-buildpublish-platform) and [CDN delivery contract](../static-site-pages/cdn-asset-delivery.md#public-release-delivery-contract).
- Only after the public presentation, daily Telegram and VK announcements move their event links to canonical static-site pages through a channel-by-channel canary and rollback. This is tracked in [Stage 8](../../reports/static-personal-announcements-release-readiness-2026-07-11.md#stage-8--после-публичной-презентации), not as a presentation GO blocker.
- Personalization release acceptance requires the [full Playwright/Gherkin E2E and KPI contract](../unsigned-personalization/e2e-acceptance.md), including browser localStorage, DB/profile evidence and `cards_to_first_relevant <= 20` for eligible mature golden personas.
- Personalization remote writes remain release-gated by the [Supabase 500 MB storage/compaction contract](../../operations/personalization-storage-budget.md): compact current state, bounded evidence, Green-band launch and fail-safe shedding of disposable telemetry.

## Documentation completion rule

A release capability is considered properly documented only when it has:

- one canonical home or an explicit child contract under a canonical parent;
- honest capability-level stage (`design`, `prototype`, `canary`, `production-enabled`, `production-verified`);
- links to requirements/architecture/operations/tests/entrypoints;
- data owner and security boundary;
- acceptance and rollback evidence expectations;
- branch owner/status when implementation is not in `origin/main`.
