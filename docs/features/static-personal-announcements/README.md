# Static personal announcements

> Status: release umbrella; full-scope public release is not ready.
> Canonical readiness checklist: [2026-07-11 release audit](../../reports/static-personal-announcements-release-readiness-2026-07-11.md).
> Recovery/current-state audit: [2026-07-17 context recovery](../../reports/static-site-release-context-recovery-2026-07-17.md).
> Event-page platform and ten-day Telegraph cutover slice: [release plan](../static-site-pages/release-plan.md).

## Purpose

This is the navigation home for the public static-site release and its personalization capabilities. Detailed facts stay in their feature homes; this page prevents F1–F18 and explicitly named release candidates from becoming an unowned flat checklist.

F1–F13 and F15–F18 are mandatory for the first public release/presentation. F14 verified comment facts/medallion is an explicit separate post-release release and must not leak a partial collector/UI into the first RC. Cross-cutting M1–M6 gates are mandatory where their status says so, including M5 all-surface age-rating coverage and M6 card-level linked-occurrence date/time choices. Preliminary homepage candidate H1 is different: its **decision gate** is mandatory before UI freeze, while implementation joins the RC only after an explicit owner `ship` decision.

## Capability ownership

| ID | Capability | Canonical home | Stage |
|---|---|---|---|
| F1 | Smart Update effect → coalesced static rebuild | [Static pages](../static-site-pages/README.md), [builder operations](../../operations/kaggle-static-site-builder.md) | partial / production disabled |
| F2 | Automatically refreshed vector + LLM-verified related events | [Semantic retrieval](../unsigned-personalization/semantic-vector-retrieval.md#release-automation-contract) | quality canary exists; vector sync enabled, automatic strict static publication disabled/partial |
| F3 | Smart authorized search + saved public search tags | [Authorized search](../unsigned-personalization/authorized-event-search.md) | search canary; tag save/curation/static generation missing |
| F4 | Email with exactly three recommendations + published personal page, delivered through NotiSend to at most 200 actively consented users at launch | [Personal email announcements](../personal-email-announcements/README.md) | design / provider routing accepted |
| F5 | Frozen public release UI, including responsive navigation decision | [Release UI contract](../static-site-pages/release-ui-contract.md), [responsive navigation](../static-site-pages/responsive-navigation.md) | adaptive hybrid direction selected; side-branch design-system candidate exists; immutable RC visual sign-off pending |
| F6 | List/detail/action personalization telemetry | [Unsigned personalization](../unsigned-personalization/README.md), [production integration](../unsigned-personalization/production-integration.md) | local preview; remote ingest design |
| F7 | Site-wide choice of Yandex identity/email or manually entered verified email | [Site user identity](../site-user-identity/README.md) | Yandex works only in search; global shell and passwordless email journey design |
| F8 | SpaceWeb retained mailbox, read-only Yandex IMAP copy pipeline, direct Mail Trigger canary, Postbox transactional delivery including D-1 event reminders, NotiSend recommendation delivery, bounce/complaint and suppression | [Email delivery](../../operations/email-delivery.md), [event notifications](../event-email-notifications/README.md) | Postbox feedback+worker live/verified; event producers, warm-up and NotiSend application flow gated |
| F9 | Durable favorites, global count and complete saved-events page | [Favorites and calendar](../event-favorites-calendar/README.md) | design; menu badge and `/izbrannoe/` missing |
| F10 | Global login/logout/account state and profile linking | [Site user identity](../site-user-identity/README.md) | search-only account state; global restore/logout/forget and merge design |
| F11 | Event transport schedules/cards | [Event transport](../event-transport/README.md), [optional «Как добраться» gallery card](../event-transport/gallery-how-to-get-there-card.md) | preliminary rail+bus slice validated in refreshed draft PR #37; KPPK/bus provider jobs, combined atomic promotion, optional gallery prototype and release-UI integration pending |
| F12 | Calendar action backed by favorite state, with visible D-1 email-reminder status | [Favorites and calendar](../event-favorites-calendar/README.md) | ICS preview; durable save/reminder UX design |
| F13 | Catalog freshness vs canonical bot DB | [Static pages](../static-site-pages/README.md), [builder operations](../../operations/kaggle-static-site-builder.md) | partial / production blocked |
| F14 | Verified comment decision facts + «Активно обсуждают» | [Event comment feedback](../event-comment-feedback/README.md), [research/minimal implementation plan](../event-comment-feedback/probe-plan.md) | explicit post-release scope; live canary complete, 30-day shadow/typed ledger/static block/medallion missing |
| F15 | Share with generated image | [Event sharing](../static-site-pages/event-sharing.md) | preview canvas; durable assets missing |
| F16 | Correct image focus/crop | [Image framing](../static-site-pages/image-framing.md) | renderer preview; metadata producer missing |
| F17 | Admin issue report → ArtKodex repair history | [Event issue reporting](../event-issue-reporting/README.md) | prototype branch; clean port required |
| F18 | Share KenigEvents itself from the mobile menu/footer with a centrally prerendered service card; evidence-based copy behavior on Windows/macOS desktop | [Service sharing card](../static-site-pages/service-sharing.md), [desktop clipboard research](../static-site-pages/service-sharing-desktop-clipboard-research.md) | partial footer-only preview in [PR #44](https://github.com/onedayonemasterpiece/events-bot-new/pull/44) at `421d6bca`; owner-confirmed native Windows test drove the two-action refinement, while header/mobile-menu, formal Windows record + final RC retest, macOS/mobile devices, exact Pharmastaff reference, main merge and production activation remain pending |

## Preliminary homepage release candidate

| ID | Capability | Canonical home | Stage |
|---|---|---|---|
| H1 | «Городской обзор»: compact static-first editorial briefing above homepage categories/feed, with optional semantic-fragment motion | [Typed briefing research](../static-site-pages/typed-briefing-hero-research.md) | `Conditional Go`; extensive side prototype/labs exist through `02095bcd`/`b5f4797d`, but current-main clean slice, frozen RC evidence and owner `ship|defer` decision are missing |

H1 is not silently promoted to the mandatory presentation scope by appearing here. Before F5 freezes the homepage, the owner must either accept a tested branch/SHA as a release feature or explicitly defer it. A `ship` decision makes all H1 gates RC blockers; a `defer` decision preserves the ordinary category/feed homepage without holding the presentation.

## Cross-cutting release readiness

| ID | Capability | Canonical home | Stage |
|---|---|---|---|
| M1 | Event-detail organizer/venue/festival medallions | [Event token medallions](../static-site-pages/event-token-medallions.md) | clean consolidated draft PR #38: 25 organizer/venue + 11 festival/venue-brand entries; P0 shortlist and owner visual sign-off pending |
| M1-QA | Exhaustive medallion visual cleanliness | [Medallion visual QA](../static-site-pages/medallion-visual-qa.md) | separate release gate missing: Playwright must inventory and capture every actual target page/layout with zero clipping, dirty/cut shadows, alpha mattes, overflow or unreadable assets |
| M2 | No duplicate images inside an event gallery | [Event image duplicate audit](../../operations/event-image-duplicate-audit.md), [automatic event-media gate](../event-media/README.md) | baseline `79/266` was cleaned and final projection/TG/VK/Telegraph violations were `0`; automatic gate is in main, while repeat current-snapshot/static audit and 14-day observation remain open |
| M3 | Consolidated event engagement from sources and site | [Consolidated event engagement](../post-metrics/consolidated-event-engagement.md) | owned TG/VK aggregation is merged; one source+site read function, shared popular projection for every consumer and ecological site view/share persistence remain open |
| M4 | Final SEO/GEO and AI-search transparency | [SEO/GEO release optimization](../static-site-pages/seo-geo-release-optimization.md) | mandatory last pre-RC gate; starts only after immutable UI/UX acceptance and integration of all public-HTML-changing release features, then requires independent Codex + `agy` Gemini Pro + `a-opus` audits |
| M5 | Confirmed event age rating is visible on every public event representation | [Event age rating](../static-site-pages/event-age-rating.md) | canonical declared/assessed pipeline is merged in `aa95900a`; mandatory all-surface projection/renderer parity and accepted-value evidence remain open |
| M6 | Same programme in several dates/times is visible as linked occurrence choices inside every event card | [Linked events](../linked-events/README.md) | mandatory release blocker; core relation and Telegraph/detail preview exist, but static export hardcodes `other_date_ids=[]` and shared cards do not visibly expose canonical alternative slots |

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

- H1 «Городской обзор» is evaluated before F5 UI freeze, not added after SEO/GEO: first prove a static V1 against categories-first control, then semantic-fragment V2 with zero-CLS interruption and manual mobile/reduced-motion. No personalized/backend/Gemini writer path is part of that initial decision. If owner selects `ship`, integrate and refreeze the homepage before M4; otherwise record `defer` and retain the normal categories/feed entry.
- M5 age marking is a shared event fact, not detail-page decoration: once Smart Update accepts `age_restriction`, one canonical projection/formatter must show it on every event-bearing card/list/detail/search/related/personal/favorite/festival/transport/share surface and applicable ICS/structured data. Missing values never default to `0+`; source conflicts and door/audience wording fail closed into the quality workflow.
- M6 linked occurrences are a card-level navigation fact, not a `Похожие` recommendation: one programme repeated at different confirmed dates/times keeps occurrence-specific URLs/ICS/favorites/reminders, while every card visibly offers its eligible alternatives. Static export must use canonical symmetric `linked_event_ids`; the current empty projection plus title/venue inference cannot satisfy release acceptance. Same-slot duplicate rows merge instead of appearing as another date, and any relationship change triggers the F1 rebuild.
- Navigation keeps one cross-device information architecture but adapts its geometry: compact mobile brand-tag disclosure, persistent desktop horizontal navigation with the shallow hybrid tag as the recommended release candidate. The exact immutable preview still needs owner sign-off.
- F18 adds one shared service-sharing capability to that shell: on mobile it must ultimately appear both under the expanded brand tag and in the footer and share a centrally prerendered card; desktop never invokes native share. The preliminary [PR #44](https://github.com/onedayonemasterpiece/events-bot-new/pull/44) implements only the common-footer slice: one native mobile action and two explicit desktop actions (image-only card or text+URL); the header was intentionally not changed. The two-action behavior was refined after an owner-confirmed native Windows test, so Windows is preliminary evidence rather than an untested lane. Final acceptance still requires a recorded Windows rerun and full macOS/mobile matrix on the final RC, plus owner decision, `main` merge and production activation. Browser/runtime composition generation, unsupported superlatives and per-user payloads are forbidden; the parent requirements live in the [service-sharing contract](../static-site-pages/service-sharing.md).
- The gallery slide «Как добраться» is an optional F11 presentation candidate: it may summarize validated transport data after real event media, but cannot replace the accessible full schedule block or weaken the nightly refresh/fail-closed gates.
- Release galleries require zero confirmed intra-event image duplicates across the full active/future public inventory. The 2026-07-13 [SHA-to-visual baseline](../../operations/event-image-duplicate-audit.md) found confirmed duplicate refs in `79/266` eligible events and reviewed all `158/158` multi-image events; each failure family now needs production-safe cleanup/root-cause closure, public rebuild and a zero-failure repeat audit rather than a one-off URL edit.
- Popularity and counters are event-level, not separate source/site products: the global `/populyarnoe/` list, `/popular_posts`, daily, video selection and static counters must use the single [consolidated engagement contract](../post-metrics/consolidated-event-engagement.md) for source + site views/likes/shares, with compact current aggregates and bounded history under the Supabase storage budget. `/populyarnoe/` consumes a precomputed versioned order/score rather than calculating a private Astro formula; user filters may narrow it and the personalized slot remains separate.
- F2 is not complete after a manual vector/Gemma preview: every effectful Smart Update create/update must automatically reach current `related_v1` vectors, a full active/future related graph, LLM verification of changed windows, checked Kaggle artifact and atomic promotion after the 15-minute quiet window. Periodic vector/manifest reconciliation and scheduled lifecycle refresh recover missed triggers and time-only expiry.
- Medallion readiness is part of F5/UI presentation acceptance: use only clean draft PR #38, finish or explicitly owner-defer the production-backed P0 shortlist, refresh the gap audit within 48 hours of RC, then pass the separate exhaustive Playwright target-surface capture with zero visual defects before owner sign-off.
- Final SEO/GEO optimization is the last pre-RC quality stage and starts only after F5 UI/UX freeze plus integration of every release feature that can change public HTML. The immutable full-site preview is then audited independently by Codex, approved Gemini Pro through `agy`, and Opus through `a-opus`; any visible/structural fix or late feature change reopens the affected UI/UX sign-off before the final audit rerun.
- Identity is a global static-site release capability: every generated HTML page must render the shared account controller with Yandex login, passwordless verified-email login, logout and device-local forget-email semantics; `/poisk/` may consume but may not own this state.
- Public presentation requires CDN-backed delivery of the full canonical static site and an asset audit proving that runtime images are lightweight WebP or safe SVG. The detailed gate lives in the [release checklist](../../reports/static-personal-announcements-release-readiness-2026-07-11.md#stage-2--production-static-buildpublish-platform) and [CDN delivery contract](../static-site-pages/cdn-asset-delivery.md#public-release-delivery-contract).
- Only after the public presentation, daily Telegram and VK announcements move their event links to canonical static-site pages through a channel-by-channel canary and rollback. This is tracked in [Stage 8](../../reports/static-personal-announcements-release-readiness-2026-07-11.md#stage-8--после-публичной-презентации), not as a presentation GO blocker.
- Personalization release acceptance requires the [full Playwright/Gherkin E2E and KPI contract](../unsigned-personalization/e2e-acceptance.md), including browser localStorage, DB/profile evidence and `cards_to_first_relevant <= 20` for eligible mature golden personas.
- Personalization remote writes remain release-gated by the [Supabase 500 MB storage/compaction contract](../../operations/personalization-storage-budget.md): compact current state, bounded evidence, Green-band launch and fail-safe shedding of disposable telemetry.
- `Моё избранное` is a site-wide destination: after state restore its badge shows the distinct durable saved-event count only when greater than zero, and `/izbrannoe/` opens the complete lifecycle-aware saved list without embedding private data into CDN HTML.
- KPPK rail and bus refresh may run in separate provider Kaggle notebooks, but they share one versioned schema, server-side validation/fan-in, provider last-good policy and exactly one coalesced rebuild for a changed combined manifest.
- F14 is a separate post-release release: after the Region Talk clean-port audit, run the 30-day daily authority/Q-A BGE+E5 shadow, then ship the typed YDB ledger and deterministic «Важно знать» before any Smart Update prose; calibrate «Активно обсуждают» independently and keep it out of ranking initially. Region Talk source-discovery/image/publication behavior and session lanes do not transfer.

## Separate post-release releases

- [Verified comment facts and «Активно обсуждают»](../event-comment-feedback/probe-plan.md): 30-day authority/Q-A BGE+E5 shadow, typed literal fact ledger, deterministic «Важно знать», optional verified-snapshot Smart Update shadow and independently calibrated medallion through its own RC/canary/rollback.
- [Static festival section](../festivals/static-site-release.md): queue/root-cause cleanup, permanent website monitoring, a distinct festival card, stable event↔edition relations and festival index/detail pages through the standard static promotion pipeline. It is explicitly outside the first presentation GO scope and has its own UI freeze/RC/SEO-GEO evidence.
- [Operations control dashboard](../../backlog/features/operations-control-dashboard/README.md): protected read-only control centre for ingestion, video, promo, transport, static publishing, image dedup and other critical deliveries. A compact operator readiness scorecard remains a first-release reliability prerequisite; the polished web dashboard is an important later release.
- [Interest clubs](../interest-clubs/README.md): the original research-first gate has been
  superseded in part by merged implementation `98180d1e`/`6b234a52` and a live
  production canary (`6cdae545`). It remains a separate release until the
  seven-day observation, freshness, false-merge/split, rollback and stable-release
  decision are closed; this does not make it a blocker for the first static-site
  presentation.
- [Видеогайды «Как быстро найти событие»](../../backlog/features/static-site-video-guides/README.md):
  отдельный post-release content release после стабильного production UI и D10
  Telegraph cutover. Первая серия показывает быстрый путь через разделы/поиск к
  корректному событию и сохранению выбранного occurrence; каждый ролик проходит
  production-SHA, privacy, subtitles, mobile/desktop и owner-approval gates.

## Documentation completion rule

A release capability is considered properly documented only when it has:

- one canonical home or an explicit child contract under a canonical parent;
- honest capability-level stage (`design`, `prototype`, `canary`, `production-enabled`, `production-verified`);
- links to requirements/architecture/operations/tests/entrypoints;
- data owner and security boundary;
- acceptance and rollback evidence expectations;
- branch owner/status when implementation is not in `origin/main`.
