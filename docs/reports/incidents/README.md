# Инциденты

Канонический индекс production incidents и post-incident разборов. Эти записи должны использоваться как обязательный regression-check перед любыми новыми изменениями в затронутых prod-поверхностях.

## Автоматический запуск incident workflow

- Достаточно указать конкретный incident ID (`INC-*`), чтобы агент автоматически:
  - открыл `docs/operations/incident-management.md`;
  - открыл этот индекс;
  - открыл канонический incident record по ID;
  - использовал его как regression contract до closure/deploy.
- Если изменения затрагивают surface из incident record, агент обязан поднять этот record как regression-check даже без явного указания пользователя.
- Если канонического record ещё нет, его нужно создать из `TEMPLATE.md` до завершения задачи.

## Канонический шаблон

- `TEMPLATE.md` — шаблон для новых incident records.

## Активные regression contracts

- `INC-2026-08-01-guide-google-ai-package-closure.md`
  - Scope: Guide Kaggle notebook generation, staged/embedded `google_ai`
    package closure and scheduled LLM extraction.
  - Must not regress: every Python module required by the current `google_ai`
    package must reach the isolated generated notebook; a full catch-up is
    required when a scheduled scan loses all prefiltered posts to one import
    error class.

- `INC-2026-07-31-region-talk-candidate-chat-incomplete-drafts.md`
  - Scope: Region Talk grounded-draft readiness and operator-chat delivery.
  - Must not regress: incomplete rows stay out of chat/queue; legacy sent flags
    cannot suppress the first exact ready draft, while same-draft retries remain
    idempotent and use only role-scoped discovery sessions.

- `INC-2026-07-31-region-talk-deploy-interrupted-sessions.md`
  - Scope: Region Talk scheduled-run durability across Fly machine replacement.
  - Must not regress: a crashed/missing latest slot is resumed automatically;
    running/success sessions are never duplicated and recovery is bounded.

- `INC-2026-07-31-region-talk-external-commerciality-regex.md`
  - Scope: Region Talk external research commerciality evidence and generic
    social ad/promo routing.
  - Must not regress: strict noncommercial external research cannot be
    tombstoned by a generic social regex alone; social and sales/sponsored rows
    remain fail-closed and Gemini retains the final semantic decision.

- `INC-2026-07-31-region-talk-article-link-precedence.md`
  - Scope: Region Talk external article eligibility/finalizer routing and the
    daily article lane.
  - Must not regress: a current actual-image article keeps its visual gate;
    link-only/no-media-reuse is used only when no actual image exists, while
    social no-media rows remain rejected and provider RPD is never bypassed.

- `INC-2026-07-31-google-ai-parallel-limiter-bypass.md`
  - Scope: all Google AI provider calls from Codex agents, Fly, Kaggle, Edge
    Functions and probes; shared atomic admission and project-level quota scope.
  - Must not regress: production/parallel consumers cannot fall back to a
    process-local or raw-key path; every provider attempt must own a shared
    reserve/sent/finalize record, and concurrent rollout is blocked until the
    atomic migration and direct-call inventory are closed.
- `INC-2026-07-31-poster-candidate-url.md`
  - Scope: общий Smart Update poster-evidence path для scheduled source
    parsing, VK auto-import и Telegram Monitoring video persistence.
  - Must not regress: реальный `PosterCandidate` не может уронить pre-write
    grounding; exact Telegram replay создаёт event + video M:N rows, а
    сегодняшний сорванный source/VK import требует проверенного catch-up.

- `INC-2026-07-30-focus-email-otp-false-success.md`
  - Scope: focus email onboarding, Supabase Auth OTP, Postbox correlation,
    browser connectivity and focus-member activation.
  - Must not regress: a failed/timeout/rate-limited send never advances as
    success; phone diagnostics compare Supabase and Yandex read-only paths and
    closure requires separate live code/link E2E plus participant reconciliation.

- `INC-2026-07-31-false-kgd80-festival-link.md`
  - Scope: source-grounded KGD80 association in Telegram Monitoring, Smart
    Update, festival queue and all event public surfaces.
  - Must not regress: generic «80-летие Калининградской области» wording cannot
    become «80 историй о главном» without the literal campaign name/hashtag,
    `kgd80.ru` or an explicit curated festival-source binding; closure includes
    a full production association audit and repair of every published surface.

- `INC-2026-07-27-pwa-presentation-install-missing.md`
  - Scope: production root, web app manifest/icons, Android
    `beforeinstallprompt`, `?install=presentation`, Object Storage root publish.
  - Must not regress: the presentation QR target must publish an installable
    root manifest and show an honest Android install/fallback card; root publish
    cannot leave the PWA source change stranded only in an immutable preview.

- `INC-2026-07-27-future-event-source-coverage-drop.md`
  - Scope: future inventory density, parallel source-parser Kaggle status writes,
    current Philharmonia DOM/output, Qtickets freshness and truthful parse status.
  - Must not regress: enabled sources cannot disappear behind a green `ops_run`;
    all three parallel run-configs commit, official Philharmonia future cards
    reach Smart Update with parser provenance, and a missed daily slot requires a
    verified catch-up.

- `INC-2026-07-27-icae-casting-wrong-venue.md`
  - Scope: source-grounded venue/address consistency across import, Smart
    Update, canonical event rows, static pages and managed social publications.
  - Must not regress: an unrelated canonical venue such as ICAE must never be
    attached to an explicit offsite address; contradictory name/address pairs
    fail closed or receive LLM review before publication, and closure includes
    repair of every already-published surface.

- `INC-2026-07-21-faberge-tg-public-writer-gap.md`
  - Scope: grounded Telegram event public writer, exact organizer-source quote
    contract, bounded fallback/retry behavior and Faberge event `6991` repair.
  - Must not regress: Lite and strict fallback failures remain fail-closed but
    cannot silently strand valid current events; terminal failures are visible
    and recoverable, and closure includes replay of the affected future cohort.

- `INC-2026-07-20-tg-monitor-stale-s22-lease.md`
  - Scope: Guide/Telegram monitor terminal Kaggle failure reconciliation,
    exact-owner `telegram_session:s22` release and same-day Telegram catch-up.
  - Must not regress: a host-observed failed guide/Telegram kernel becomes a
    terminal ledger row and releases only its own lease; it cannot block the
    later critical Telegram slot until TTL, and a missed daily scan requires a
    verified compensating run including forum-topic sources such as
    `@klassster/8809`.

- `INC-2026-07-20-static-claim-lost-health.md`
  - Scope: deploy/restart while a static-site Kaggle job owns the coalesced
    outbox row, CAS-loss logging and `/healthz` availability.
  - Must not regress: a lost static-site CAS uses pre-rollback scalar evidence;
    it cannot lazy-load an expired ORM object or stop the outbox/health loop.

- `INC-2026-07-20-static-event-keyboard-visual-regressions.md`
  - Scope: recommendation-card crop/row agreement, real cross-document gallery
    navigation, footer shortcut ownership and production-candidate browser gates.
  - Must not regress: canonical card layout and computed `object-fit` agree on
    both discovery surfaces; a gallery recommendation Enter survives document
    navigation; visible-footer `P`/`S` works from body or stale off-screen focus;
    the generated candidate cannot publish without the real Playwright journeys.

- `INC-2026-07-20-image-geometry-pixel-drift.md`
  - Scope: Smart Update media materialization, exact pixel/geometry identity,
    transient enrichment recovery, static bbox export and safe crop consumption.
  - Must not regress: pixel-distinct posters never overwrite one managed object;
    stale geometry is invalidated and re-enqueued; only pixel-current boxes
    reach renderers, and an unsafe protected-region crop falls back to `contain`.

- `INC-2026-07-19-static-builder-root-overlay-recurrence.md`
  - Scope: Fly root writable overlay, `/tmp` Kaggle video outputs, retained static-site artifacts and scratch readiness.
  - Must not regress: terminal outputs remain bounded, active/recoverable handoffs are preserved, `/healthz`/preflight detects an unwritable root scratch filesystem before scheduled jobs dead-letter.
- `INC-2026-07-20-static-listing-desktop-preview-regression.md`
  - Scope: shared static listing CSS ownership and the desktop sticky stack for
    Today, Tomorrow, Weekend and Popular.
  - Must not regress: every built listing route contains the shared design
    system; the real header remains at viewport top and discovery rails stick
    below it; mobile Popular V26 remains unchanged.

- `INC-2026-07-19-static-site-stale-builder-lease.md`
  - Scope: host-validated StaticSiteBuilder terminal reconciliation, exact-owner
    `static_site:builder` lease release and pre-push Smart Update self-healing.
  - Must not regress: a successful published candidate cannot retain its
    exclusive lease after a lost callback or transient SQLite writer lock; a
    late receipt never releases a successor's lease; the next build reconciles
    the durable current candidate before any Kaggle push.

- `INC-2026-07-18-static-snapshot-disk-pressure.md`
  - Scope: Smart Update/Kaggle immutable snapshot lifecycle, retry/recovery
    retention, Fly `/data` readiness and deploy health checks.
  - Must not regress: terminal handoffs delete their exact snapshot pairs;
    crash leftovers stay bounded while durable active inputs are preserved;
    candidate retries cannot cross the disk-critical health floor.

- `INC-2026-07-18-dramteatr-same-day-event-glue.md`
  - Scope: Smart Update post-match identity enforcement, same-date Dramatic
    Theatre occurrences, canonical/public repair and static source/media
    projection.
  - Must not regress: a `14:30` theatre tour and an `18:00` play on the same
    venue/date remain distinct; a high-confidence
    `skip_merge_side_effects` verdict blocks mutation in enforce mode while a
    true same-event update still merges; OCR/non-identity documents do not own
    or crop the hero when a strong event-local visual exists.
- `INC-2026-07-18-social-metrics-postponed-vk-import.md`
  - Scope: Kaggle SocialMetricsCollector postponed-to-live VK resolution and
    strict Fly import of scheduled popularity batches.
  - Must not regress: a future-dated postponed row returned by `wall.getById`
    is never emitted as `published`; the collector must continue the bounded
    live-wall lookup, and one unresolved scheduled post cannot discard valid
    metrics observations from the same batch.

- `INC-2026-07-18-cherryflash-missing-true3d-bundle.md`
  - Scope: CherryFlash True3D runtime bundling, product-final output discovery,
    failed-Kaggle output recovery and daily Telegram Stories catch-up.
  - Must not regress: every invoked renderer ships in the per-session dataset;
    intro-only mp4 output cannot become `PUBLISHED_TEST`; daily closure requires
    a real `cherryflash_full_final.mp4` and verified public story fanout.

- `INC-2026-07-18-vk-captcha-publication-cadence-gap.md`
  - Scope: managed VK `vk_sync`, captcha persistence/recovery, expired post
    edits, JobOutbox pause cohorts and postponed publication cadence.
  - Must not regress: one VK captcha cannot strand the queue until 2036; only
    the marked cohort resumes after a harmless probe, with bounded spacing;
    historical/manual pauses remain untouched; expired edit windows do not
    retry indefinitely; same-day gaps require a verified catch-up and non-empty
    authenticated postponed queue.

- `INC-2026-07-17-vk-auto-provider-quota-false-reject.md`
  - Scope: manual/scheduled VK auto-import, Smart Update Google key allocation,
    provider-side 429 handling, grounding and post-create image geometry.
  - Must not regress: Smart Update uses KEY1–KEY5 as a normal pool from the
    first reserve; one provider-exhausted member cannot turn a grounded event
    into `create_bundle_grounding:llm_ungrounded`; explicit/unpooled lanes are
    never widened; recovery replay must import the original VK post and enqueue
    its geometry job.
- `INC-2026-07-17-meow-source-medallion-telegram.md`
  - Scope: `@kldevents` RichMessage medallion selection and the boundary between static-page provenance badges and Telegram event attributes.
  - Must not regress: source/aggregator-channel badges such as `MEOW Афиша` never enter Telegram graphical strips; legitimate organizer, venue, festival, program and Pushkin-card medallions remain available; affected public posts are replaced through send-first/delete-after-success and DB mappings point only to the clean replacements.
- `INC-2026-06-03-smart-update-flash-lite-rpd.md`
  - Scope: Google AI key registry/scoping, normal versus emergency pools, Smart Update quota exhaustion and bounded fallback behavior.
  - Must not regress: every runtime key used by a pool is registered; image geometry rotates only across KEY4+KEY5 from its first reservation, fails closed without the shared limiter, and bulk work stays paced/capped rather than converting ledger exhaustion into provider 429 or unrelated-lane spend.

- `INC-2026-07-15-tg-rich-medallion-rendering-gaps.md`
  - Scope: canonical `@kldevents` RichMessage publishing, manifest-backed graphical medallion strips, footer spacing and retirement of custom-emoji medallion placement.
  - Must not regress: event `6811` semantics resolve KОНБ + KGD80 + Znanie; every approved event image is preserved above one standalone bottom strip; `Подробнее` and `Max` retain a 12-space non-collapsing one-row gap; RichMessages never enqueue or receive legacy emoji medallions.

- `INC-2026-07-16-static-event-media-action-regressions.md`
  - Scope: static event medallion/media inventory, exact KAUP A/B/C design-system fidelity, invariant desktop CTA geometry, Kaliningrad-date production generation and durable Smart Update→Kaggle single-flight/adoption.
  - Must not regress: accepted transport is reproduced rather than reinterpreted and says `на Кауп`; ticket/phone variants keep the bottom calendar/share/like row; conflicting venue identities fail closed; delayed images reserve geometry; a stale Fly waiter never causes a duplicate Kaggle push; `/segodnya/` matches the receipt's Kaliningrad date.
- `INC-2026-07-15-static-production-v2-secondary-surfaces.md`
  - Scope: generated desktop recommendation geometry, rail transport media, exact accepted mobile V8 integration and full-catalog preview acceptance.
  - Must not regress: recommendation rows use one media ratio without fields, OCR crops stay at or below 20%, rail examples retain the train image, and production mobile routes reuse the accepted V8 behavior from `fd8766b1` rather than an older approximation.
- `INC-2026-07-15-static-desktop-template-regression.md`
  - Scope: exact accepted desktop event component, production media-family routing, full-catalog static generation and truthful consultant/release acceptance.
  - Must not regress: generated desktop pages must mount the accepted Continuous Editorial/Split component rather than a CSS imitation; portrait/low-resolution media must fail to Split; all-page and representative Playwright evidence must cover the actual generated preview URLs while mobile v4 stays unchanged.
- `INC-2026-07-15-fly-volume-critical.md`
  - Scope: Fly `/data` capacity, SQLite/runtime evidence retention, `/healthz` readiness and `/webhook` routing during deploys.
  - Must not regress: production must retain bounded runtime evidence without crossing the disk-critical readiness floor; every deploy must verify free space, SQLite integrity, `/healthz`, Fly checks and fresh disk-full/proxy logs; volume auto-extension stays bounded and cannot replace retention or DB/media hygiene.
- `INC-2026-07-14-ecodvor-unknown-start-time-cursor.md`
  - Scope: Telegram Monitoring parent/program versus child-activity time roles, Smart Update explicit-TBD anchor review, server time fallback and legitimate zero-event source cursor advancement.
  - Must not regress: a child activity whose source explicitly says its start time is still being clarified must keep unknown time rather than inherit the parent `14:00–17:00` window; successfully scanned legitimate `events=[]` tail messages must advance the source cursor without entering Smart Update or polluting scanned-message metrics.
- `INC-2026-07-14-synthetic-thin-source-public-copy.md`
  - Scope: sparse-source Smart Update/public copy grounding, donation-vs-registration admission, managed VK evidence loops, VK body replacement and vector admission facets.
  - Must not regress: every semantic public claim must be organizer-evidenced; public projections never become source evidence; unknown admission is neither registration nor `ticketed`; Lite remains the normal public writer with only the capped `gpt-4o` emergency lane.
- `INC-2026-07-13-tg-media-downgrade-non-cdn-posters.md`
  - Scope: Smart Update media materialization, existing-event source media repair, TelegramMonitor storage URLs, static galleries and `@kldevents` publishing.
  - Must not regress: public event gallery URLs are strictly `static.kenigevents.ru`; missing source media stays in durable retry and can never create or downgrade to a Telegram text-only event post.

- `INC-2026-07-13-runtime-logging-recurring-event-quality.md`
  - Scope: bounded permanent runtime logs, Fly volume hygiene, reversible production Telegram E2E authorization, complete future-event quality acceptance and vector/LLM evidence.
  - Must not regress: production observability must be bounded rather than disabled; logs cannot exhaust SQLite storage; every live import acceptance needs correlated UI/log/ops/vector evidence; broad future quality claims require a complete source-adjudicated denominator; known mappings such as `6774→2884` must be included before any event-local repair/public rearm.
- `INC-2026-07-12-autoretro-one-day-exhibition-location-period.md`
  - Scope: source-grounded one-day outdoor exhibition date/venue semantics, Smart Update period/location merge safety, vector-first recall and all public projections.
  - Must not regress: a one-day street vehicle exhibition must not inherit a long-running exhibition period or unrelated indoor/default venue; vector similarity is recall only and cannot authorize semantic field transfer.
- `INC-2026-07-11-cherryflash-eco-retry-storm.md`
  - Scope: CherryFlash partner watchdog retries, Kaggle selection-manifest/payload boundary, intro poster resolution and failed-kernel status evidence.
  - Must not regress: a deterministic partner render failure gets at most one recovery attempt per day/profile; a live runtime recovers from canonical `payload.json` and never falls back to fixture event `3292`.
- `INC-2026-07-11-event-vector-sidecar-sync-stalled.md`
  - Scope: regular incremental event-vector synchronization, production sidecar freshness/coverage, Smart Update/VK-import follow-up and sufficient run evidence.
  - Must not regress: ephemeral audit embeddings must not be reported as persistent production coverage; current imported events must reach both `search_v3` and `related_v1`, with freshness/coverage failures visible and attributable.
- `INC-2026-07-10-future-event-semantic-audit.md`
  - Scope: exact future canonical inventory audits, vector-first recall with LLM source/date-role adjudication, deadline/historical non-events, wrong-time/date duplicates, merge contamination, venue drift and all managed public projections.
  - Must not regress: vector similarity/identity approval must never count as semantic quality evidence; every audited row needs a current source-grounded LLM verdict, provider/missing-evidence cases fail closed, and destructive repair requires a source-confirmed survivor plus backups.
- `INC-2026-07-10-zoo-ticket-validity-non-event.md`
  - Scope: Telegram Monitoring schedule routing, operational-hours/ticket-validity date roles, Smart Update LLM eventness review, vector-quality boundary, and all public event fanout surfaces.
  - Must not regress: a venue-open/normal-mode notice with visitor/cash-desk hours and `билет действителен до <date>` must not become an event, the validity date must not become `event.date`, the hours must not become `event.time`, and identity-vector `allow_create` must never count as quality approval.
- `INC-2026-07-09-recurring-occurrence-date-drift.md`
  - Scope: Smart Update matching/merge for recurring or season rows versus exact single-date occurrences, VK auto-import source/media attachment, and public Telegram/VK/Telegraph event fanout.
  - Must not regress: a source/poster that explicitly announces one occurrence such as `10 июля 20:00` must not mutate or republish a broader `1 мая — 30 сентября` recurring row with stale date anchors; shared title/place/ticket is not enough to veto creating the occurrence or to allow merge side effects.
- `INC-2026-07-07-new-event-quality-degradation.md` — New event quality degradation after vector identity rollout; VK/Smart Update source-grounding, media and public fanout regression contract.
- `INC-2026-07-05-guide-visual-digest-stale-vk-booking-link.md`
  - Scope: guide excursions visual digest `visual_schedule`, LLM extraction of `booking_url` from multi-event schedule links, and VK/Telegram caption primary-link selection.
  - Must not regress: a future schedule occurrence must not expose a historical source wall post as the primary booking/details link; repeated-route inline links must be validated against current occurrence context or demoted to source/current post fallback.
- `INC-2026-07-05-tg-medallion-description-alias-drift.md`
  - Scope: Telegram custom-emoji medallion selection for event posts.
  - Must not regress: ordinary venue medallion aliases must match only actual location fields, not event descriptions/search digests/festival labels; curated program medallions such as KGD80 remain explicit exceptions.
- `INC-2026-07-05-tg-afisha-edit-spacing-premium-medallions.md`
  - Scope: Telegram event publishing due ordering/spacing for new posts versus existing-message edits, plus Premium/custom-emoji medallion enrichment timing.
  - Must not regress: existing Telegram post edits must not consume the scarce new-post spacing lane while no-post events wait. Its old medallion-editor requirement is superseded by `INC-2026-07-15-tg-rich-medallion-rendering-gaps`: RichMessages are complete with a graphical strip and must not enqueue custom-emoji medallions.
- `INC-2026-07-05-guide-visual-digest-phone-link.md`
  - Scope: Guide excursions `visual_schedule` Telegram caption generation and phone-only booking contacts.
  - Must not regress: Telegram visual digest captions must send phone contacts as explicit `phone_number` entities while preserving existing title/source links and leaving VK digest phone text plain.
- `INC-2026-07-05-tg-afisha-vk-dependency-backlog.md`
  - Scope: Telegram event publishing JobOutbox dependencies, `schedule_event_update_tasks`, and VK/TG fanout coupling.
  - Must not regress: `tg_event_publish` must not depend on `vk_sync`; VK media/API failures must not block valid Telegram event announcements, while Telegram spacing and daytime publish window remain enforced.
- `INC-2026-07-03-event-6045-static-defect.md`
  - Scope: Telegram Monitoring OCR date extraction/merge, Smart Update social-candidate date provenance/eventness routing, writer grounding, and public/static repair for event `6045` from `@signalkld/11052`.
  - Must not regress: record/vinyl metadata such as `LP 33 1/3 RPM` must not become an event date; social candidates with dates ungrounded in source text or poster OCR must route through LLM-first eventness review before create; non-event coffee/music promo sources must not publish active future static/Telegraph/Telegram/VK event surfaces.
- `INC-2026-07-02-static-search-92-percent-no-cards.md`
  - Scope: Static site authorized smart search `/poisk/`, personalization Supabase Edge Function `event-search`, frontend progress/rendering, card-shaped shimmer/halo loading state, LLM/vector sequencing, and production root/preview entrypoint promotion. Must not regress: reported natural-language searches must not stay stuck at 92% without cards when backend has returned or can return vector candidates; `/poisk/`/root CTA must point to the current searchable build; backend stage timings and client render evidence must be available for closure.
- `INC-2026-07-02-boyko-exhibition-smart-update-glue.md`
  - Scope: Smart Update merge-path side-effect identity for exhibition/festival sibling contexts, especially long-running exhibition candidates being glued into single-slot lecture/talk events.
  - Must not regress: after final match selection and before event/source/poster/fact/job side effects, `SMART_UPDATE_MERGE_IDENTITY_GATE` must be able to LLM-classify related-but-distinct/festival-sibling candidates and, in enforce mode, return `skipped_identity_gate` without mutating the matched event; valid same-event source updates must still merge.
- `INC-2026-07-02-kldevents-1778-vk-ocr-location-time.md`
  - Scope: VK auto-import poster OCR handoff under long-caption token budgets, Smart Update/location_reference generic venue fuzzy matching, and public `@kldevents`/`klgdevents`/Telegraph repair for source `https://vk.com/wall-169817694_32270`.
  - Must not regress: poster OCR logistics lines with date/time/venue/free-entry evidence must remain available to the LLM parse even when full OCR is trimmed for budget; `Городской парк, Пионерский` must not fuzzy-bind to a Зеленоградск culture-center venue through generic tokens; sibling events for `05/19 июля 14:00` must publish with `Городской парк, #Пионерский`.
- `INC-2026-07-02-exhibition-duplicates-static-site.md`
  - Scope: exhibition/current-future inventory duplicates, recap/spotlight/update materialization, invalid date fields, prompt/prose venue leakage, and static-site exposure amplification.
  - Must not regress: recap posts with only a thin future teaser and source-ungrounded venue/address must not auto-create public events; exhibition spotlights/roundups must attach to a canonical identity or skip, not create active duplicates with inferred ranges.
- `INC-2026-06-30-kraftmarket317-poster-only-zero-events.md`
  - Scope: Telegram Monitoring producer OCR-only poster extraction for `@kraftmarket39`, server `producer_zero_events:clear_event_signals` diagnostics, standard location reference for `Музей «Восток на Западе»`, and forced replay/public fanout for source post `https://t.me/kraftmarket39/317`.
  - Must not regress: an empty-caption Telegram post whose poster OCR contains title/date/time/venue/price/registration must enter the LLM-first extraction path instead of returning `events=[]`; clear poster-only single-event rescue must not become a blanket schedule parser for multi-time poster digests; `Музей «Восток на Западе», Клиническая 19А` must normalize consistently.
- `INC-2026-06-30-prose-location-non-event-daily-duplicate.md`
  - Scope: Telegram import/Smart Update short prose-location leaks (`В программе — ...`, `И не забывайте`), campaign/discount non-event routing, and Telegram daily scheduler restart idempotency.
  - Must not regress: short source-grounded non-location fragments must not survive as `location_name`; campaign/discount/action posts must route to LLM eventness review before create; scheduled daily announcements must have durable per-channel/day claims so releases/restarts cannot duplicate an already-sent daily slot.
- `INC-2026-06-30-generic-title-dropped-own-name.md`
  - Scope: Smart Update create-title recovery for category-only titles where the source/OCR contains a distinctive own name, plus public Telegram/VK/Telegraph repair for event `6508`.
  - Must not regress: generic titles like `Городской фестиваль` must route to LLM recovery when source headline/OCR carries a grounded own name such as `ВЕЛОДЕНЬ`; the guard must not deterministically rewrite titles and must not force recovery when no own-name evidence exists or when the current title is already distinctive.
- `INC-2026-06-29-qtickets-structured-facts-lost.md`
  - Scope: Qtickets/ticket-site parser structured fact handoff, `TheatreEvent` source text preservation, poster OCR priority, and public Telegram/VK/Telegraph fanout for parser-backed events.
  - Must not regress: ticket-page JSON-LD title, venue, address, date/end-date, price and URL must remain visible to the LLM/Smart Update boundary; poster OCR remains secondary and must not replace canonical page titles such as `FLAVA INTENSIVE (VALERA & LERA VOYNITS)` with fragments like `VALERA`.
- `INC-2026-06-29-konb-room-venue-drift.md`
  - Scope: VK auto-import/Smart Update source-grounded location handling for KОНБ (`konb39`/`wall-30777579_*`) room/floor labels and public `@kldevents`/`klgdevents`/Telegraph event surfaces.
  - Must not regress: `читальный зал`, `2 этаж`, or `4 этаж лекционный зал` from KОНБ at `Мира 9` must publish as `Научная библиотека, Мира 9, Калининград` while remaining only hall/room detail; the same generic room without KОНБ source grounding and `Дом китобоя, Мира 9` must not be auto-rewritten.
- `INC-2026-06-29-kldevents-solenaya-railway-gates.md`
  - Scope: Smart Update location canonicalization for gate-family aliases, `Солёная ворона` / `Железнодорожная 1` Зеленоградск events, and public Telegram/VK/Telegraph event surfaces.
  - Must not regress: `Железнодорожная 1, Зеленоградск` must remain `Театральная гостиная Солёная ворона`, not `Железнодорожные ворота`; Railway Gates canonicalization still requires explicit `ворота` or the real Kaliningrad gate address/landmarks.
- `INC-2026-06-29-tg-promo-compensation-repeat.md`
  - Scope: broad Telegram `tg_repost` popular amplification diversity and incident compensation posts in `@kldevents`.
  - Must not regress: `@kenigevents` popular reposts must not repeat the same normalized title inside 7 days while another forwardable candidate exists; compensation Telegram event posts must preserve direct registration/ticket links, respect media-group no-button limits, and explicitly run/verify the premium emoji editor after the standard event publisher path.
- `INC-2026-06-29-80-stories-telegram-promo-gap.md`
  - Scope: built-in `80 историй о главном` promo campaign Telegram companion activities, `tg_event_publish` self-forward/new-post behavior for `@kldevents`, `tg_repost` amplification to `@kenigevents`, and drift between VK 80 Stories refreshes and Telegram campaign coverage.
  - Must not regress: the built-in 80 Stories campaign must seed and repair code-owned `tg_event_publish`/`tg_repost` activities; `@kldevents` gets two daily self-forward-or-new-post slots aligned with VK visibility, and production-only manual activity rows must not be the only source of truth.
- `INC-2026-06-29-tg-event-publish-fresh-import-starvation.md`
  - Scope: `joboutbox` due ordering and `tg_event_publish` spacing/catch-up for Smart Update imports into `@kldevents`.
  - Must not regress: fresh Smart Update event announcements must not be starved behind old `tg_event_publish` catch-up/backlog rows either at initial `next_run_at` scheduling or at due-job execution; Telegram spacing may still enforce one post per interval, but newly imported events with completed dependencies need a freshness lane and public VK/TG divergence must be visible in evidence.
- `INC-2026-06-29-tg-premium-rock-emoji-wrong-id.md`
  - Scope: Telegram premium emoji editor for rock-concert title/category icons in `@kldevents` and daily announcements, especially the `lovekenigofficial` `🤘` document id.
  - Must not regress: rock `🤘` must use document id `5404517529362128309` (rock/guitarist symbol), not neighboring id `5393556708398225048`; existing wrong custom entities for visible `🤘` must be corrected in-place.
- `INC-2026-06-29-tg-premium-tretyakov-composite-pair.md`
  - Scope: Telegram premium emoji editor for daily Tretyakov venue markers in `@kenigevents`, especially the `lovekenigofficial` `🖼🖼` document-id pair.
  - Must not regress: Tretyakov `🖼🖼` must use the two-part composite ids `5188445640325099838,5188470637034758005`, not the small standalone thumbnail id `5188683852096234620` and not duplicated thumbnails; marker scope remains venue-only, not title/description-inferred.
- `INC-2026-06-29-tg-premium-ticket-calendar-icon.md`
  - Scope: Telegram premium emoji editor for `@kldevents` event posts, especially date rows, ticket/registration rows, and ruble/money price formatting.
  - Must not regress: generator fallback must keep date/calendar and ticket semantics distinct (`📅` date, `🎫` tickets/registration); the premium editor may convert only date/calendar `📅` to custom `🎟`; paid ticket rows must become `🎫 Билеты 💰 <number>` (or the existing ticket label/link plus `💰 <number>`) and must not keep textual `руб.`.
- `INC-2026-06-28-crumple-video-publish-only-storm.md`
  - Scope: CrumpleVideo/CherryFlash source-session resume recovery, Kaggle status ledger liveness, and publish-only compensation ledgers.
  - Must not regress: `videoannounce:<id>:publish-only:*` ledger rows must never resurrect the source render poller or repeat test/public Telegram mp4 delivery; terminal/published video sessions must not resume from fresh ledgers, while genuinely false-failed source sessions with fresh source heartbeats remain recoverable.
- `INC-2026-06-28-opening-exhibition-range-duplicate.md`
  - Scope: Telegram Monitoring / Smart Update exhibition-opening semantics, inferred exhibition `end_date`, temporal location fragments, and duplicate active exhibition inventory.
  - Must not regress: opening-only exhibition titles without an explicit source run window must remain atomic and must not receive `date + 1 month`; a real exhibition range must be represented by the exhibition title itself or source-grounded `end_date`; active inventory must not retain duplicate opening/exhibition rows for the same real `Обход 2.0`-style event.
- `INC-2026-06-28-vk-stale-event-publication.md`
  - Scope: managed `klgdevents` VK/TG fanout freshness, inferred end-date handling, and VK postponed slot reservation for one-day timed events.
  - Must not regress: inferred `end_date` must not let an already-started timed event publish; a VK postponed reservation at or after the event start must be refused; stale live/postponed managed VK posts for past timed events must be deleted or blocked before publication.
- `INC-2026-06-28-google-ai-gemma4-rpm-overrun.md`
  - Scope: Google AI LLM gateway reserve behavior, no-Supabase/local limiter fallback, and CherryFlash/video partner-filter Gemma 4 client wiring.
  - Must not regress: server-side Google AI consumers must use Supabase-backed `GoogleAIClient` with a specific consumer name; any no-Supabase fallback must fail fast through the process-local limiter instead of making unlimited direct provider calls; Gemma 4 per-key bursts must not exceed the provider `15 RPM` limit.
- `INC-2026-06-27-telegraph-footer-backfill-content-loss.md`
  - Scope: Telegraph event-page bulk maintenance/backfills, python-telegraph `get_page(..., return_html=True)` response handling, and social footer rollout.
  - Must not regress: existing Telegraph page body must never be treated as empty because HTML is in `content` instead of `content_html`; bulk footer/nav/image fixes require a canary plus public content-preservation smoke before editing all active/future pages.
- `INC-2026-05-30-active-duplicate-events-recall-gate.md`
  - Scope: Smart Update duplicate recall when exact location/time gates miss same real events; widened candidate recall, LLM dedup adjudication, and safety rails for multi-session/source-time conflicts.
  - Must not regress: after ordinary match/create, rescue, and pre-create probes fail, a same-date/date-adjacent plausible duplicate must still be visible to an LLM adjudicator; high-confidence same-event decisions may merge only when date/venue/time/source-session safety checks do not conflict; explicit same-source multi-session schedules must remain separate.
- `INC-2026-06-27-valeria-duplicate-publication.md`
  - Scope: Smart Update LLM-first duplicate matching, title-only deterministic vetoes, and managed TG/VK fanout for same real event rows.
  - Must not regress: a high-confidence LLM match with same date/venue and no explicit time conflict must not be overruled solely by `unrelated_titles`; `Валерия` and `Концерт Валерии` must merge as one event instead of producing duplicate public posts.
- `INC-2026-06-27-vk-prune-starvation.md`
  - Scope: managed `klgdevents` past-event cleanup, `vk_post_prune` capped batches, and VK recommendation hygiene.
  - Must not regress: old missing/repost-protected managed VK URLs must not starve fresh past live posts behind `VK_POST_PRUNE_LIMIT`; recent past posts without reposts/comments must be prioritized and removed from the feed.
- `INC-2026-06-26-vk-channel-draft-telegraph-cta.md`
  - Scope: VK Channel manual-copy draft CTA selection for registration-required promo events, especially `80 историй о главном`. Must not regress: Telegraph details pages must not be used as the one CTA when the event/campaign requires registration; missing direct registration/ticket links must fail/skip instead of producing a misleading draft; source-text registration URLs may be used as direct CTAs.
- `INC-2026-06-26-crumple-missing-video-output.md`
  - Scope: CrumpleVideo Kaggle notebook asset discovery, intro font loading, per-session dataset assets, final mp4 production, and fail-fast status behavior for scheduled `/v tomorrow` runs.
  - Must not regress: required intro fonts must be available either from the static Kaggle assets dataset or the session dataset; the notebook must search concrete `/kaggle/input/*` mounts rather than a single hardcoded path; if `crumple_video_final.mp4` is not produced, the notebook must fail hard instead of reporting `render_done/report_written` as a successful run.
- `INC-2026-06-26-vk-location-reference-fuzzy-park.md`
  - Scope: VK auto-import/reference location normalization and public `klgdevents`/`@kldevents` event posts where a generic municipal venue such as `Городской парк` is fuzzy-bound to an unrelated known venue in another city. Must not regress: single generic tokens (`городской`, `парк`, `центр`, `культура`, `искусство`) must not be enough to canonicalize an unknown venue; source/poster-grounded `Пионерский, городской парк` must remain Pionersky unless an explicit curated alias/address supports another venue.
- `INC-2026-06-26-tg-story-message-forward.md`
  - Scope: CherryFlash/CrumpleVideo Telegram story-to-channel feed fanout, `telegram_story_message` transport, `popular_review` target generation, CrumpleVideo `VIDEO_ANNOUNCE_STORY_TARGETS_JSON`, and Kaggle story helper/notebook sync.
  - Must not regress: production channel-feed video posts must be produced from the previous successful Telegram story via `InputMediaStory`/`messages.SendMediaRequest`, not by raw `send_file()` MP4 upload; `telegram_chat` remains allowed only for intentional raw test-post targets.
- `INC-2026-06-25-afishaengagement-no-active-activity.md`
  - Scope: Afisha Engagement public all-events fallback for `klgdevents`, production promo campaign/activity date config, runtime `afishaengagement.decision` smoke, and manual catch-up edits of VK postponed posts.
  - Must not regress: ordinary `klgdevents` VK publication preflight must have an active public Afisha Engagement fallback when rollout is intended; repeated `no_active_activity` decisions while VK posts continue are a regression; manual catch-up must verify whether VK reassigned a postponed post id and reconcile DB URLs.
- `INC-2026-06-25-outbox-unknown-jobtask-publication-outage.md`
  - Scope: shared `JobOutbox` queue, `JobTask` enum materialization, outbox worker health, stale/TTL catch-up, ICS Supabase Storage upload, ticket-site/qTickets fanout, and Telegram/VK/Telegraph publication fanout.
  - Must not regress: legacy or unknown raw `joboutbox.task` strings must not crash due/running ORM selection; one invalid page/navigation row must not block unrelated `tg_event_publish`/`vk_sync`; active current/future event-pipeline pending jobs must catch up after worker outages instead of expiring solely because the worker was blocked; health/release smoke must include runtime-log evidence for worker-loop failures, `/healthz` `job_outbox_worker_loop` status, direct Storage upload path evidence for ICS, qTickets public-fanout coverage, and public Telegram/VK resume evidence.
- `INC-2026-06-25-vk-channel-wrong-surface.md`
  - Scope: Promo `vk_channel_publish`, VK community Channel transport, public promo exposure statuses, and any fallback that uses `messages.send`/Messenger for a community Channel request.
  - Must not regress: `vk_channel_publish` must create no public exposure while VK community Channel posting lacks a verified non-Messenger API; `messages.send` peer ids and `vk.com/im?...` URLs may be used only for explicit operator manual-copy drafts (`VK_CHANNEL_DRAFT_SENT`, `public_target_count=0`) and must never be counted as VK community Channel delivery.
- `INC-2026-06-24-vk-past-actuals.md`
  - Scope: Promo VK publication/repost/story/carousel event eligibility, Afisha Engagement debug-shadow scheduling/cleanup, managed `klgdevents` VK/TG fanout gating, and VK postponed queue audits.
  - Must not regress: a managed VK/Promo/Afisha Engagement post must not publish at or after the event start for one-day timed events; date-only debug-shadow copies must publish before the event day; same-day timed events whose start has passed must be excluded from promo candidate selection and `schedule_event_update_tasks` must not enqueue new `vk_sync`/`tg_event_publish` for them.
- `INC-2026-06-24-future-event-date-default-venue-regressions.md`
  - Scope: Telegram Monitoring Gemma extraction/import date semantics, event-local offsite venue grounding versus source defaults, Smart Update writer grounding, duplicate recall after venue drift, and public `@kldevents` future inventory.
  - Must not regress: Russian compact dates (`10.05`, `30.05`) are `DD.MM`, month-word/hashtag dates (`26 июля`, `#13_июня`, `#21_июня`) remain authoritative, gate/floor/address/price/coordinate numbers never become event dates/times, retrospective reports without a future invite return `[]`, explicit offsite venue/address lines beat `source.default_location` even when extractor initially omits venue, thin/free source posts must not gain unsupported buy-ticket/theatre boilerplate, same date/title/venue source-time conflicts must match for LLM merge instead of becoming duplicate active rows, explicit same-source multi-session schedules must remain separate occurrences, and Telegraph/public text must not leak LLM editor meta such as “Вот обновленный текст”. Closure requires replaying the exact source URLs through Telegram Monitoring server import + Smart Update and verifying repaired public Telegram/Telegraph surfaces.
- `INC-2026-06-22-poll-repost-orphan-open-poll.md`
  - Scope: Poll to Repost production DB-write idempotency after Telegram `send_poll`, resolver popularity relaxation, Telethon/Fly/DB evidence for orphan public polls, and scheduler failure recovery.
  - Must not regress: a transient SQLite writer lock after a public poll is sent must not leave the poll without a durable `poll_repost_run` row; resolver must not drop a valid winning option solely because sparse popularity coverage filtered its candidates after a relaxed creation.
- `INC-2026-06-20-tg-speaker-roster-dropped.md`
  - Scope: Telegram/Smart Update lecture and public-talk imports with named speaker rosters, split-create logistics cleanup, and public Telegraph/VK/TG descriptions.
  - Must not regress: source-grounded named speaker rosters must not be collapsed into generic categories; logistics-tainted split-writer drafts should be LLM-cleaned before falling back to a generic description.
- `INC-2026-06-20-tg-forward-service-chat-leak.md`
  - Scope: forwarded/reposted Telegram message routing, TG monitoring on-demand channel/group signals, and manual add-event service replies.
  - Must not regress: forwarded-post manual add-event flow must run only in private bot chats; group/channel reposts must not receive `Festival added` / `Event added` / publication progress service messages.
- `INC-2026-06-20-tg-on-demand-scheduler-run-id.md`
  - Scope: TG monitoring on demand APScheduler entrypoint, `_job_wrapper` run_id injection, and post-deploy runtime log smoke.
  - Must not regress: scheduler jobs registered through `_job_wrapper` must accept the injected `run_id`; `tg_monitoring_on_demand` ticks must not fail with unexpected keyword argument errors.
- `INC-2026-06-18-vk-title-shortlink-public-regression.md`
  - Scope: VK auto-import title guard, poster OCR title handoff, Smart Update generic-title recovery, and source shortlink normalization for public Telegram/VK/Telegraph event posts.
  - Must not regress: deterministic VK intake must not synthesize `<event_type> — <venue>` placeholders such as `Концерт — Бар Советов`; VK intake must not run deterministic word-level title-grounding/suspicious-title checks at all; if title quality needs review, it must route through Smart Update/LLM. Poster OCR is evidence for Smart Update/review, while the LLM title remains LLM-owned unless Smart Update changes it. External source shortlinks such as `clck.ru` must be resolved before public registration links are rendered.
- `INC-2026-06-18-tg-location-prose-still-extracted.md`
  - Scope: Telegram Monitoring LLM-first venue extraction, server import location recovery, exact address/studio handling, source-default venue ownership, Telegraph source-media rehydration, and public `@kldevents` repairs.
  - Must not regress: regex/OCR helpers must not override an LLM/default known venue with prose; source-owned venues such as `sobor39`/`kldzoo` must keep their canonical defaults unless LLM-reviewed offsite evidence exists; explicit `address, studio` evidence must not be rebound to unrelated known venues like ИЦАЭ; multi-event source URLs must not rehydrate unrelated posters into another event.
- `INC-2026-06-16-cherryflash-duplicate-after-bot-send-failure.md`
  - Scope: CherryFlash/CrumpleVideo/Koenigsberg scheduled Kaggle video runs,
    server-side output download, bot test/notify delivery, Kaggle status ledger,
    and catch-up/watchdog slot eligibility.
  - Must not regress: after render output, terminal Kaggle evidence, or a public
    side effect exists, a full replacement Kaggle run is forbidden unless an
    operator explicitly overrides it; post-download bot-send failures and
    deterministic fanout blockers such as `BOOSTS_REQUIRED` must use a
    post-render terminal status and retry only narrow operations that can add
    value.
- `INC-2026-06-16-tg-event-publish-timeout-duplicate.md`
  - Scope: Telegram event publishing idempotency around Bot API write timeouts, `tg_event_publish` retry policy, and Telethon-first inspection of operator-provided Telegram links.
  - Must not regress: a Bot API timeout during new `sendMessage`/`sendPhoto`/`sendMediaGroup` for `@kldevents` must be treated as an uncertain write and must not auto-retry into a duplicate public post; `t.me` incident links must be read through Telethon first.
- `INC-2026-06-16-tg-location-pianissimo-program-fragment.md`
  - Scope: Telegram Monitoring / Smart Update venue extraction for official source posts where a short, source-grounded program/repertoire line is copied into `location_name`, plus public `@kldevents` event-publish repair for `event.id=6060`.
  - Must not regress: repertoire/program items such as `🎵 С. В. Рахманинов – Музыкальные моменты` and catalogue numbers such as `соч. 16` must trigger LLM venue review and must not survive as public venue/address fields; official Tretyakovka source context/default must recover `Филиал Третьяковской галереи, Парадная наб. 3, Калининград` when the post only says `в атриуме музея`.
- `INC-2026-06-16-tg-phone-links.md`
  - Scope: Telegram event publishing phone-only registration/ticket contacts and phone numbers in event body/captions.
  - Must not regress: `event.ticket_link=tel:+...` must become an explicit clickable Telegram `phone_number` entity with a compact Telegram-visible phone payload; body phone numbers must be linkified outside existing anchors; Kaliningrad `4012` landlines must keep natural display formatting outside the final Telegram entity payload.
- `INC-2026-06-16-vk-quality-duplicates-non-events.md`
  - Scope: VK/TG Smart Update eventness for weak digest/rubric candidates, citywide/festival duplicate recall, managed `klgdevents` VK post idempotency, and near-duplicate poster media dedupe.
  - Must not regress: rubric stubs such as `Дайджест - посмотри, приходи` must go through LLM-first eventness review and fail closed when not a concrete attendable event; same title/date/time citywide events with drifted venue text must be visible to LLM matching rather than creating duplicate active rows; existing managed/postponed VK posts must be reused instead of republished; visually near-duplicate posters must collapse before public VK publication.
- `INC-2026-06-15-konb-room-location-and-future-audit.md`
  - Scope: VK auto-import/Smart Update venue extraction for stable source-location sources, KОНБ `konb39`/`kaliningradlibrary` defaults, and future-event duplicate/generic-title audit.
  - Must not regress: room/floor labels such as `лекционный зал`, `аудитория`, or `4 этаж` must not become public venues when source context identifies a real building; KОНБ VK imports must carry `Научная библиотека, Мира 9, Калининград`; future active listings must not keep generic duplicate rows such as duplicate `Музыкальный фестиваль` donor/map notices or duplicate 80th-region concert rows.
- `INC-2026-06-15-poll-repost-missing-slots.md`
  - Scope: `poll_to_forward.py`, production/debug poll creation, popularity underfill, topic-planner fallback, question guardrails, and Fly scheduler evidence.
  - Must not regress: a sufficient raw event inventory must not disappear solely because sparse popularity coverage underfills; after an LLM planner attempt, usable inventory must fall back to bounded multi-candidate topics; full LLM unavailability must still skip rather than publish a fully deterministic poll; debug must remain a reliable visible smoke surface.
- `INC-2026-06-15-tg-promo-media-drop-and-bullet-copy.md`
  - Scope: explicit Telegram promo activity posts
    (`promo_activity.surface='tg_event_publish'`) with event media and long full
    promo bodies.
  - Must not regress: media-backed promo activity posts must not become
    text-only solely because the full body exceeds Telegram's caption limit;
    the publisher should send the image/album with a concise caption and avoid
    dumping long Smart Update bullet lists as the primary caption.

- `INC-2026-06-15-cherryflash-caption-metadata.md`
  - Scope: CherryFlash `popular_review` public Telegram/VK story-video captions
    generated from `videoannounce_session.selection_params`.
  - Must not regress: scheduled CherryFlash public posts must include the
    release/session number and target date title
    `Видеоанонс #<session_id> · <D month>`; Telegram `telegram_chat` targets
    get this exact caption and VK wall captions keep it as the title before
    the hashtag/date block.
- `INC-2026-06-15-tg-promo-markdown-leak.md`
  - Scope: explicit Telegram promo activity posts
    (`promo_activity.surface='tg_event_publish'`), full event body formatting
    in `main_part2.py`, and public `@kldevents` promo outputs.
  - Must not regress: Markdown-style section headings and bullets from
    `event.description` must not leak as literal `###`, `**`, or `*` markers in
    public Telegram promo posts; headings should be formatted with Telegram HTML
    and bullets normalized before escaping.
- `INC-2026-06-14-crumple-vk-transport-drift.md`
  - Scope: CrumpleVideo scheduled `/v tomorrow` VK wall fanout, Kaggle
    notebook embedded story helper, story-enabled session dataset helper
    bundling, and `vk:kenigeventsofficial:wall`.
  - Must not regress: CrumpleVideo VK community targets must be handled through
    the same explicit VK transports as CherryFlash (`vk_wall`/`vk_wall_story`)
    and must never fall through to Telethon username resolution; the notebook
    must prefer bundled `kaggle_common/story_publish.py` and keep embedded
    fallback in sync.
- `INC-2026-06-14-vk-publication-cta-plain-duplicate.md`
  - Scope: promo `vk_publication` and Afisha Engagement public CTA one-write
    boundary for `klgdevents` VK wall posts.
  - Must not regress: promo VK publication must choose one production variant
    before writing to VK. If public Afisha Engagement CTA preflight succeeds,
    it is the scheduled wall post and the plain `post_to_vk` call is skipped;
    if the plain fallback posts, later Afisha Engagement checks must be
    `shadow_only=True` so public CTA activities cannot create a second wall
    post for the same publication pass.
- `INC-2026-06-14-morning-import-quality-and-outbox-stale.md`
  - Scope: Telegram Monitoring location/city handoff, Smart Update unsupported-location guard, event `JobOutbox` stale handling, managed VK/TG fanout reconciliation, and Afisha Engagement CTA/plain public rollout evidence.
  - Must not regress: temporal/date words such as `Завтра`, `Сегодня`, or `14 июня` must not persist as `location_name`; inflected settlement city strings such as `посёлке Железнодорожный` must route to LLM venue-review rather than regex replacement; event-pipeline `running` jobs must retry with bounded backoff after stale runtime expiry instead of becoming 10-year dependency blockers; CTA/plain VK publication remains a single production-path decision.
- `INC-2026-06-14-afishaengagement-shadow-fallback-regression`
  - Scope: Afisha Engagement public CTA/plain selection at the VK publication
    boundary, legacy debug-shadow activity enablement, and VK postponed cleanup.
  - Must not regress: normal Smart Update VK sync must choose exactly one
    production variant per publication pass. If public CTA preflight wins, it
    creates the CTA post; if it misses or fails, the path creates only the plain
    post/update and must not schedule a marked debug-shadow copy as a side
    effect. Broad debug-shadow activities must stay disabled unless an explicit
    manual debug batch is being run.
- `INC-2026-06-14-festival-recap-logistics-false-events.md`
  - Scope: Telegram Monitoring / Smart Update non-event guards for festival recaps, attendee logistics notices, and managed VK/TG event fanout.
  - Must not regress: a post-event recap that only says the next festival dates while location/place/address is still unknown must not create an event with a fabricated venue; an operational notice for existing guests about entry/navigation/parking/queue/cloakroom must not become a new event; real future festival announcements with grounded location and invitation/ticket signals must stay importable.
- `INC-2026-06-13-tg-calendar-private-link.md`
  - Scope: Telegram event calendar CTA selection, source-post keyboard
    calendar links, and service/internal Telegram calendar asset links.
  - Must not regress: public `@kldevents` calendar buttons must not point to
    private `https://t.me/c/...` asset-channel links when a public `.ics` URL
    exists; public `https://t.me/<username>/<id>` calendar-post links may remain
    preferred.
- `INC-2026-06-13-kaggle-duplicate-videoannounce.md`
  - Scope: CherryFlash/CrumpleVideo/Koenigsberg story-video Kaggle handoff,
    live Kaggle status callbacks, heartbeat, final report handling, and shared
    Telegram session resource leases.
  - Must not regress: scheduled retries must have live evidence showing whether
    a previous Kaggle run is alive, rendering, publishing, failed, or writing
    reports before a replacement run is treated as necessary; the status
    framework must not silently suppress or deduplicate public publication
    attempts.
- `INC-2026-06-13-poll-repost-wrong-date-and-copy.md`
  - Scope: Poll to Repost candidate eligibility, LLM reply composer guardrails,
    and `@kldevents` Telegram event infoblock date-range rendering.
  - Must not regress: a recommendation for a target date must not forward an
    older start-date `@kldevents` post for a long-running event unless that
    post visibly carries the active range; LLM replies must not invent
    open-air/street format facts or use `на этом:` placeholder phrasing; Telegram
    event posts with `end_date > date` must render a visible date range in the
    infoblock and calendar button.
- `INC-2026-06-13-vk-auto-import-day-month-regex-nameerror.md`
  - Scope: VK auto-import draft extraction and `vk_intake` date-anchor guards.
  - Must not regress: `_source_text_has_absolute_date_anchor` must support both
    numeric `DD.MM` and text `DD month` anchors without runtime NameError;
    failed `vk_inbox` rows from a technical draft exception must be catchable by
    a post-deploy rerun.
- `INC-2026-06-13-vk-postponed-event-slot-late-anchor.md`
  - Scope: VK postponed slot reservation for managed event posts, especially
    `/vk_auto_import` event fanout through shared `post_to_vk`.
  - Must not regress: late promo/postponed anchors must not force fresh event
    posts to the tail of the day when morning slots are free; reservation must
    inspect active postponed timestamps and choose the first free slot at the
    configured cadence.
- `INC-2026-06-12-tg-monitoring-deploy-crash-no-watchdog.md`
  - Scope: Telegram Monitoring scheduled Kaggle handoff, critical scheduler
    watchdog registration, `ops_run` delivery evidence, `/healthz` scheduler
    health, and VK auto-import critical-slot recovery.
  - Must not regress: deploy/restart during the 23:40 Telegram Monitoring slot
    must not lose the day; `critical_scheduler_watchdog` must be registered and
    health-visible whenever `tg_monitoring`, guide full monitoring, or
    `vk_auto_import` is enabled; watchdog catch-up must inspect the last local
    scheduled slot even after local midnight, and per-slot VK auto-import
    recovery must not be masked by an earlier same-day success; Telegram
    Monitoring catch-up must defer while a `tg_monitoring` Kaggle recovery
    registry entry exists, instead of pushing a second kernel with
    `TELEGRAM_AUTH_BUNDLE_S22`.
- `INC-2026-06-12-kenigsberg-story-media-invalid-catchup-loop.md`
  - Scope: Kenigsberg `/kenigsberg` Kaggle story media profile, startup
    catch-up retry behavior, and Telegram/VK fanout terminal status handling.
  - Must not regress: Kenigsberg production story publish must use the
    story-safe H.264/`avc1` helper path instead of bypassing it with the
    CherryFlash native HEVC profile; startup catch-up must stop after repeated
    same-day failed scheduled/story sessions instead of relaunching after every
    deploy restart; closure requires terminal Telegram publish evidence or an
    explicit operator blocker before another compensation run.
- `INC-2026-06-12-kenigsberg-story-session-duplication.md`
  - Scope: Kenigsberg `/kenigsberg` Kaggle story publishing, shared remote
    Telegram session guard, Telegram/Guide monitoring Kaggle auth bundles, and
    `VIDEO_ANNOUNCE_STORY_AUTH_BUNDLE_ENV`.
  - Must not regress: two Kaggle jobs must never use the same Telethon auth
    bundle concurrently; active unknown-scope registry entries stay
    conservative and block; new remote Telegram jobs must write
    `remote_telegram_auth_scope`; story publishing should use a separate
    `TELEGRAM_AUTH_BUNDLE_STORY` when parallelism with S22 monitoring is
    required.
- `INC-2026-06-12-afishaengagement-public-canary-no-show.md`
  - Scope: Afisha Engagement public/shadow candidate resolution, VK group alias
    matching, and production public canary rates for `80 историй о главном`.
  - Must not regress: `target_group="klgdevents"` must match numeric
    `VK_EVENTS_GROUP_ID=231920894`; the 80-stories public activity at
    its configured public rate must be evaluated before the all-events 0.1
    fallback. The old shadow fallback requirement was superseded by
    `INC-2026-06-14-afishaengagement-shadow-fallback-regression`.
- `INC-2026-06-12-vk-partial-media-family-cta.md`
  - Scope: managed VK event media upload/parity and Afisha Engagement CTA
    selection for family fairs/markets.
  - Must not regress: new managed `klgdevents` event posts must fail closed on
    partial media upload instead of creating a public/postponed post with only
    part of `event.photo_urls`; transient VK `upload.php` failures must be
    retried with fresh upload-server URLs; family fairs/markets with explicit
    child/family audience signals must use `family` CTA copy rather than
    generic market repost wording.
- `INC-2026-06-12-raffle-source-publication-false-skip.md`
  - Scope: VK auto-import / Smart Update fanout from `schedule_event_update_tasks` into managed VK `vk_sync` and Telegram `tg_event_publish`, especially source posts that contain raffle/giveaway fragments alongside a real event.
  - Must not regress: a valid event whose raw source mentions a ticket raffle/giveaway must still enqueue and publish managed VK + Telegram event posts when Smart Update has produced substantial cleaned non-giveaway event copy; prize-only giveaway sources without cleaned event copy may still be skipped when a non-giveaway alternative exists.
- `INC-2026-06-12-future-event-quality-llm-first-repair.md`
  - Scope: Telegram Monitoring / VK auto-import / Smart Update future-event quality, including malformed dates, source-non-grounded venue fragments, false `Калининград Сити Джаз Клуб` defaults, `DD.MM` date markers leaking as times, and duplicate public cards across Telegraph/Telegram/VK.
  - Must not regress: active future rows must keep ISO dates only; source prose/default venue conflicts must route through LLM-first venue review instead of becoming public `location_name`; same real event clusters such as Westside `Род мужской`/`Солнцестояние`, Kantata/Agropark/Pianissimo duplicates must collapse while distinct same-time productions remain separate; confirmed bad public rows/pages/posts must be repaired or explicitly blocked by platform editing limits.
- `INC-2026-06-12-tg-event-utility-hook-quality.md`
  - Scope: Telegram event publishing intro generation for `@kldevents`,
    especially utility/service posts whose source text is practical but stored
    description/search digest may be hallucinated or entertainment-framed.
  - Must not regress: `tg_event_publish` must not force every intro to be a
    hook question; utility/service events such as tire collection/recycling
    should use source-grounded useful copy and must prefer source text over a
    conflicting entertainment-style description. The same conflicting
    description must not feed Telegram type hashtags. Closure requires repair
    evidence for `@kldevents/354` or an explicit external blocker.
- `INC-2026-06-11-tg-monitoring-recovery-after-deploy-cancel.md`
  - Scope: Telegram Monitoring scheduled Kaggle runs, `/data/kaggle_jobs.json`
    recovery registry, `kaggle_recovery`, `ops_run` terminal evidence, and Fly
    deploy/restart timing during active scheduled imports.
  - Must not regress: a local deploy cancellation of `tg_monitoring` must not be
    treated as lost output while the original Kaggle kernel is still alive; do
    not start a replacement monitoring run before checking registry/Kaggle
    status and recovery import evidence; closure requires terminal
    `recovery_import` metrics or explicit registry/log evidence explaining why
    recovery cannot proceed.
- `INC-2026-06-10-event-outbox-fanout-deadlock.md`
  - Scope: Smart Update event fanout through `JobOutbox`, `vk_sync`, `tg_event_publish`, `ics_publish`, and `tg_ics_post`.
  - Must not regress: independent event pipeline jobs must not block each other via a broad same-event prior-job rule; `vk_sync` must not wait behind calendar jobs unless explicitly configured; dependent jobs must not expire while their dependency is actively retrying with bounded backoff; ordinary VK auto-import should restore the expected Telegraph + VK + Telegram rhythm.
- `INC-2026-06-09-social-video-tg-publishing.md`
  - Scope: CherryFlash/CrumpleVideo story and VK/TG fanout ownership, `guaranteed_any_position` video promo placement, and `@kldevents` Telegram event publish slot spacing.
  - Must not regress: CherryFlash must not include the CrumpleVideo `vk:kenigeventsofficial:wall` / `crumple_official` target; CrumpleVideo must publish its official VK wall video through the shared production story target list; CherryFlash must post the rendered video into `@kenigevents` channel body after story upload; `guaranteed_any_position` promo must be mixed into stable lower positions instead of always appended; `tg_event_publish` must choose the nearest free daytime slot and must not leave a day-gap solely because late pending backlog exists.
- `INC-2026-06-08-tg-ics-bad-time-retry-storm.md`
  - Scope: calendar/ICS publication jobs (`ics_publish`, `tg_ics_post`), `schedule_event_update_tasks`, and Telegram event publish dependencies that include calendar posts.
  - Must not regress: events whose `date` or `time` cannot be parsed into a concrete calendar start must not enqueue `ics_publish`/`tg_ics_post` and `tg_event_publish` must not depend on `tg_ics_post`; already queued invalid-schedule calendar jobs must finish with `skipped_invalid_schedule` instead of raising `ValueError: bad time`/`bad date` into an infinite retry storm.
- `INC-2026-06-08-festival-vk-aggregate-regression.md`
  - Scope: `/start` -> `Добавить событие`, Telegram Monitoring single-source containment for `@kraftmarket39`, Festival Queue/Universal Festival Parser, and `sync_festival_vk_post` VK aggregate publishing.
  - Must not regress: urgent `@kraftmarket39` monitoring must run through the same Smart Update import path with a one-source Kaggle config; festival VK aggregate posts must stay disabled unless `ENABLE_FESTIVAL_VK_POSTS=1`; ordinary event-level VK/TG fanout remains separate and must not publish whole-festival aggregates to obsolete communities.
- `INC-2026-06-07-tg-event-publishing-media-calendar-dedup.md`
  - Scope: Telegram event publishing after Smart Update, `tg_event_publish`/`tg_ics_post` dependency order, calendar button target, Telegraph details link, Smart Update persisted poster dedup, and `vk_sync` managed-post idempotency.
  - Must not regress: a one-image event must publish as a single captioned media post with a publicly usable calendar button, preferring a public `event.ics_post_url` but falling back to `event.ics_url` when `ics_post_url` is a private `t.me/c` asset link; multi-image posts must carry caption on media, not a separate text post unless explicitly redesigned; captions must include `Подробнее` when Telegraph exists and must not invent placeholder price/free information; managed-storage and raw-CDN copies of the same poster must collapse to one `event.photo_urls` entry and one persisted `EventPoster` after missing `phash` backfill; Smart Update acceptance requires both a real `@kldevents` post and a real `klgdevents` wall item (`wall.getById` non-empty), not merely a stale DB URL.
- `INC-2026-06-07-guide-remote-session-stale-busy.md`
  - Scope: shared remote Telegram session guard, stale Kaggle registry entries, guide scheduled full monitoring, and `GetKernelSessionStatus` transient failures.
  - Must not regress: fresh `UNKNOWN`/status-lookup-failure Kaggle runs must still block a second `TELEGRAM_AUTH_BUNDLE_S22` session, but stale registry entries older than `REMOTE_TELEGRAM_SESSION_UNKNOWN_STALE_MINUTES` with only transient status lookup failures (`HTTP 5xx`, network, SSL, timeout) must not indefinitely suppress the daily guide full slot; closure requires same-day guide full catch-up/import/digest evidence.
- `INC-2026-06-07-future-event-quality-recurrence.md`
  - Scope: Telegram Monitoring candidate title fallback, poster OCR date/time expansion, Smart Update future public event quality, managed Telegraph pages, and `klgdevents` VK event posts.
  - Must not regress: short valid titles such as `Идиот`, `Гараж`, and `№ 13` must not be overwritten by umbrella/service lines like `завтра в театре` or `в продаже репертуар`; dotted follow-up dates such as `9.08` must not become public times like `09:08`; `time_is_default` may remain a weak internal anchor but must not render as confirmed time in VK source headers; data repair must delete bad public posts/pages/events and return Telegram/VK sources to scan state before closure.
- `INC-2026-06-06-guide-monitoring-missed-vk-festival-hashtag.md`
  - Scope: guide excursions scheduled `light/full` jobs, guide scheduled digest/VK fanout, runtime scheduler health, and VK event festival hashtags.
  - Must not regress: a public event linked to canonical festival `Кантата` must publish/search as `#Кантата` even if `event.festival` carries an inflected label such as `Кантаты`; `/healthz` must expose `guide_excursions_light` and `guide_excursions_full`; a missed critical guide `full` daily slot must be caught up by the live watchdog and closure requires same-day catch-up/digest evidence.
- `INC-2026-06-06-vk-past-klgdevents-posts.md`
  - Scope: VK outbound event publishing to `klgdevents`, `schedule_event_update_tasks` enqueue behavior for `JobTask.vk_sync`, and `job_sync_vk_source_post` before `wall.post`.
  - Must not regress: fully past events (`end_date` or, when empty, `date` strictly before today's local date) must not enqueue or execute a new managed `klgdevents` `vk_sync`; current/future events and ongoing long events (`end_date >= today`) must remain publish-eligible; already-managed `klgdevents` URLs must still suppress duplicate sync.
- `INC-2026-06-05-vk-story-forward-wall-first.md`
  - Scope: CherryFlash/Kaggle VK story fanout (`vk_wall`, `vk_wall_story`, legacy `vk_story`), `popular_review` target order for `kenigeventsofficial`/`klgdevents`, КОНБ `konb39` VK targets, and promo VK story image generation.
  - Must not regress: video announcements for these VK communities must publish a wall clip first, then upload the mp4 as a VK video story with `link_url` pointing at that wall post; promo poster stories must use the source image/poster without VK `link_url`, so VK does not render a white wall-post card; `80 историй о главном` daily VK story surfaces must remain caption-free.
- `INC-2026-06-04-80-stories-promo-vk-scheduler-gap.md`
  - Scope: Promo VK scheduler for the built-in `80 историй о главном` campaign,
    `vk_publication` cadence, `vk_repost`, new `vk_story` activity delivery,
    `/promo` reporting, VK wall/story actor selection, and same-day
    compensation discipline.
  - Must not regress: the built-in campaign must idempotently seed two daily
    `klgdevents` event posts, one daily repost into `kenigeventsofficial`, and
    two daily story cards into each target community from recent public festival
    event posts; story upload is complete only after `stories.save`; closure
    requires production evidence for tomorrow's expected VK posts/reposts/stories
    plus explicit same-day compensation evidence.
- `INC-2026-06-04-tg-monitoring-media-and-digest-quality.md`
  - Scope: Telegram Monitoring media/poster intake, `event.photo_urls`/`eventposter`, `sync_vk_source_post`, Smart Update parser defender, digest/multi-event prompt rules, and video announce poster eligibility.
  - Must not regress: Telegram-origin events may exist in DB without media, but a new managed `klgdevents` VK post must not be created with `attachments=0`; missing media must fail closed as `vk_sync_missing_media_for_telegram_event`. Prose fragments such as `мы его очень ждали` must never survive as `location_name`; without a real venue/address/meeting point the candidate must fail closed instead of creating a public event row.
- `INC-2026-06-04-tg-monitoring-vk-fanout-llm-quota-storm.md`
  - Scope: Telegram Monitoring import boundary, Smart Update fallback policy, Google AI key reserve/overflow metadata,
    `JobOutbox(vk_sync)`, VK fanout, CherryFlash S22 session usage, and promo/video/repost downstream surfaces.
  - Must not regress: a successful Telegram import or `/tg` replay must leave active future non-silent Telegram-origin
    events with pending/running/done `vk_sync` evidence or a managed VK URL; Google reserve overflow keys must be present
    both as runtime secrets and in `google_ai_api_keys`; RPD/RPM failures in mass Gemma tasks must not cause unbounded
    4o fallback spend.
- `INC-2026-06-04-kraftmarket271-tg-monitoring-tpm-import-cancel.md`
  - Scope: Telegram Monitoring producer extraction/OCR/rate-limit retry for `@kraftmarket39`, Google AI key registry and reserve fallback for `GOOGLE_API_KEY3`, server-side Telegram result import/recovery cancellation handling, scanned-message diagnostics, and `80 историй о главном` promo-campaign intake.
  - Must not regress: a clear event-like festival promo post such as `@kraftmarket39/271` (`Калининград корабельный`, 2026-07-08, registration URL on `kgd80.ru`) must not be silently recorded as `events=[]` because of TPM; rate-limit/provider failures must be distinguishable from legitimate zero-event output; cancelled import/recovery must resume or rerun until the source tail is imported or has durable diagnostics; closure requires production DB evidence that `kraftmarket39` cursor/source rows are caught up through message `271`.
- `INC-2026-06-02-vk-captcha-text-only-posts.md`
  - Scope: VK event media upload (`upload_vk_photo`, `upload_vk_photo_bytes`), `sync_vk_source_post`, `post_to_vk`, user-token captcha handling, and `vk_sync` job pause behavior.
  - Must not regress: if VK returns captcha (`code=14`) while uploading photos for an event post, `vk_sync` must pause/fail closed before `wall.post`; it must never create a `klgdevents` event post with `attachments=0` solely because the user-token photo upload path is blocked by captcha.
- `INC-2026-05-29-genai-response-repr-leak.md`
  - Scope: `google_ai/client.py` response→text extraction and empty-response guard; Smart Update description generation and grounded title recovery, `_sanitize_description_output`, `sanitize_for_vk`, VK source-post publish boundary; poster dedup (`media_dedup`, `_apply_posters`).
  - Must not regress: a stringified provider SDK response (`GenerateContentResponse` repr) must never be returned as model output or published to any surface — empty/thought-only responses must raise `empty_response` and fall back; visually near-duplicate posters (Hamming-close `phash`) must collapse to one image before publishing; a generic `<event_type> — <venue>` placeholder title must be replaced by a grounded title recovered from the source when one is available.
- `INC-2026-05-29-guide-vk-digest-missing-media.md`
  - Scope: Guide excursions VK digest fanout to `vk.com/uhtykaliningrad`, materialized guide media assets, shared VK photo upload, postponed wall id resolution, and one-post VK digest rendering.
  - Must not regress: a guide VK digest issue with media items must upload those assets to VK and publish/edit one wall post with both text and `photo...` attachments; first line must carry count + exact dates/range; postponed `wall.post` URLs must store the real wall item id, not only VK's `postponed_id`; media upload failure must fail closed instead of producing a text-only post.
- `INC-2026-05-27-dachniki-prose-venue-duplicates.md`
  - Scope: Telegram/VK/parser event location extraction, Smart Update shortlist filtering and duplicate matching, future active event Telegraph pages.
  - Must not regress: a prose fragment in `location_name` must not be trusted as a venue anchor; if source text/title/date/time prove identity with an existing event, merge without changing the public venue, otherwise fail closed instead of creating a public prose-venue card.
- `INC-2026-05-27-zhivoy-sunduk-writer-identity.md`
  - Scope: Telegram Monitor title extraction, poster OCR title priority, Smart Update rich-facts/create-bundle/split-writer prompts, public Telegraph descriptions.
  - Must not regress: caption/source attendee-facing names such as `Живой сундук` beat poster slogans like `Читайте бумажные книги!`; organizer/community/inspiration identity facts must stay source-grounded (`ОКЦ на Горького 116`, `Плоский мир Терри Пратчетта`) and must not be replaced by thematic guesses.
- `INC-2026-05-19-vk-posts-personal-author.md`
  - Scope: shared VK `post_to_vk` wall publishing for `kenigeventsofficial` daily posts and `klgdevents` event posts, especially group-token actor calls.
  - Must not regress: every new community wall post created through `post_to_vk` must send `owner_id=-<group_id>`, `from_group=1`, and `signed=0`, so VK records `from_id=-<group_id>` and normal wall/community forwarding stays available.
- `INC-2026-05-18-konb-cherryflash-render-lock-and-empty-selection.md`
  - Scope: CherryFlash `partner_konb_library_001` scheduled production slot, partner render-lock scoping, KОНБ underfilled selection recycle, and same-day compensation.
  - Must not regress: a slow `popular_review_eco` render must not block the KОНБ partner track scheduled 7 minutes later; if fresh/future KОНБ candidates underfill, eligible future KОНБ events from prior days may repeat, while same-video duplicates and same-calendar-day repeats remain blocked.
- `INC-2026-05-18-prod-startup-missing-partner-promo-module.md`
  - Scope: `main_part2.py::create_app`, optional handler imports, Fly production startup, and `/healthz`.
  - Must not regress: optional or unfinished feature modules such as partner promo must not be imported unconditionally from the production startup path; missing optional handlers must fail closed without taking down webhook serving.
- `INC-2026-05-17-konb-cherryflash-test-story-preflight.md`
  - Scope: CherryFlash `partner_konb_library_001` test/prod publish targets, Telegram channel-post test delivery, and prevention of inherited global Telegram Business fanout.
  - Must not regress: КОНБ test mode must post the rendered video to `@keniggpt` as a normal Telegram channel post, not run Telegram channel story preflight; КОНБ test/prod modes must include only explicit КОНБ targets and must not inherit `setting.video_announce_story_business_targets`.
- `INC-2026-05-17-eco-cherryflash-underfilled-event-recall.md`
  - Scope: CherryFlash `partner_eco_nature_001` candidate recall, recent popularity windows, same-day/future event-date recall, and eco LLM filter handoff.
  - Must not regress: a current/future eco/nature/local-history event with renderable media must not be invisible solely because its source post was published more than seven days earlier; event-date recall may widen candidates, but the existing `eco_prirodnaya` LLM filter must still own the semantic include/exclude decision before publication.
- `INC-2026-05-17-vk-retrospective-reschedule-wrong-postponement.md`
  - Scope: VK auto-import cancellation/postponement shortcut and lifecycle matching for posts whose `перенос` wording is retrospective context, not a cancellation of the current announced event.
  - Must not regress: a post like `Прерафаэлиты... 22 мая 19:00 ... Эта лекция - перенос несостоявшейся встречи в апреле` must stay on the normal LLM-first VK import path and must never mark an unrelated event sharing date/time as `postponed`.
- `INC-2026-05-17-kraftmarket235-tg-monitoring-extraction-miss.md`
  - Scope: TelegramMonitor producer extraction for `@kraftmarket39`, zero-event scanned message diagnostics, server import boundary, and daily/recently-added inventory for `kraftmarket39/235`.
  - Must not regress: a clear Telegram post with explicit future date/time, venue, ticket URL, and price (`8 женщин`, 2026-05-22 19:00, `Городской центр культуры и искусства`, `voroh.ru/event/1022458`) must not return `events=[]`; scanned zero-event messages must leave enough durable evidence to distinguish extractor false negatives from unscanned posts.
- `INC-2026-05-17-future-event-quality-regressions.md`
  - Scope: Telegram Monitoring / VK auto-import / parser imports / Smart Update future active event quality: prose/person-name venues, section-label titles, invalid time-from-date parses, duplicate public cards, and linked-source venue drift.
  - Must not regress: active future rows must not expose person names or prose fragments as `location_name`; compact theatre schedule bullets must not produce duplicate cards with titles like `неделя в театре` or times parsed from dates (`17.05` -> `17:05`); same real event clusters such as `GROZA`, `Общая кухня`, `Закулисье театра`, and same-ticket Yantar Hall/qTickets reposts must collapse or carry a source-grounded reason to remain separate.
- `INC-2026-05-15-cherryflash-partner-fanout-promo-filter.md`
  - Scope: CherryFlash partner Business-only story target resolution, eco/east partner auto-selection fail-closed behavior, eco classifier retry/4o fallback, and promo campaign priority / guaranteed-any-position video policy.
  - Must not regress: partner tracks must not inherit `VIDEO_ANNOUNCE_STORY_TARGETS_JSON` or publish to `@kenigevents`/`@lovekenig`; an explicit empty `story_targets_override=[]` means no Telethon fanout; scheduled partner tracks must not publish `manual_review`/`llm_error` events; eco classification must retry Gemma and then use 4o fallback before failing closed; eco promo candidates must pass the partner filter or use the explicit one-item off-filter exception after three profile-matched eco/nature/local-history events; other partner tracks must not inherit base `popular_review` promo without a documented exception; `80 историй о главном` must stay priority `1` and `guaranteed_any_position` instead of forced top-slot promo.
- `INC-2026-05-13-kenigsberg-production-story-boosts-required.md`
  - Scope: Kenigsberg production story target config, direct `@mostvkenig` fanout, Business story target gate, and same-day startup catch-up.
  - Must not regress: direct `@mostvkenig` Telethon story upload must not be the required/blocking render gate while Telegram returns `BOOSTS_REQUIRED`; Kenigsberg must not inherit the shared video-announcement Business fanout; the required target is the configured self-account story, with `@mostvkenig` attempted only as best-effort `repost_previous`; after any failed same-day scheduled/catch-up production attempt, deploy closure requires a compensation rerun and terminal publish evidence.
- `INC-2026-05-12-kenigsberg-postprocess-db-lock-and-final-copy.md`
  - Scope: Kenigsberg manual `/kenigsberg` launch blocking, Kaggle handoff/post-processing status transitions, SQLite-lock retry around video session/status writes, `/a` list-bans routing, final-copy handling and LLM-only screen splitting from `thoughts.md`, thought-pool consumption, source-video CFR frame extraction, source-range anti-repeat, beat-synced rhythm slotting, Kenigsberg notebook story-helper import gating, and manual stuck local-handoff cleanup.
  - Must not regress: a completed Kaggle run must not leave `/kenigsberg` blocked behind a misleading generic “рендерится” message; SQLite locks during heavy imports must be retried for Kenigsberg state/status writes and local-to-Kaggle handoff metadata; story text must be copied from curated `thoughts.md` entries and split into readable screens by LLM without rewriting, dropped words, or hidden long-line tails; invalid/missing LLM splits must fail before Kaggle, and Kaggle must reject overlong unsplit lines instead of slicing locally; scene slots must be derived from detected strong-beat anchors, with only 1x/2x strong-beat spans after the first partial slot; failed Kaggle runs must not consume a thought from `used_thought_ids`; source scenes must not be extended by visibly repeating the last decoded source frame; one issue must not reuse overlapping/near-adjacent source intervals; disabled production story publishing must not import/preflight the shared story helper in Kaggle; stale `local:KoenigsbergStories` rows must be auto-failed or clearable via `/kenigsberg unlock`.
- `INC-2026-05-12-kenigsberg-assist-ban-routing-and-dominant-range.md`
  - Scope: `/a` Kenigsberg action routing, direct `/kenigsberg` ban argument parsing, and generated-timeline-to-source ban mapping.
  - Must not regress: natural/direct forms such as `Kenigsberg #4 бан 4-6` must execute as `/kenigsberg ban #4 4-6`; one operator range in whole seconds must create one source ban for the dominant source segment by overlap, not all edge-overlapped segments.
- `INC-2026-05-12-kenigsberg-deterministic-text-fallback-quality.md`
  - Scope: Kenigsberg story text source, fallback policy, and Kaggle renderer text input contract.
  - Must not regress: generated story text must not use the old low-quality fallback; current publication text must come from curated `thoughts.md` entries as final copy, with LLM-only semantic splitting into validated `scene_lines`; Kaggle must still require explicit `scene_lines` instead of slicing raw text.
- `INC-2026-05-12-kenigsberg-music-range-overrun-into-vocals.md`
  - Scope: Kenigsberg renderer music selection, `MUSIC_RANGES`, audio encoding duration, and manifest/render-log music metadata.
  - Must not regress: selected audio for the full encoded story, including outro, must stay inside a configured instrumental range; unlisted or too-short tracks must be skipped/fail closed; manifest/log evidence must include selected music start/end and allowed range.
- `INC-2026-05-12-kenigsberg-command-silent-during-gemma-retry.md`
  - Scope: `/kenigsberg` manual command handler, pre-Kaggle Gemma 4 text rewrite, operator acknowledgements, and production runtime-log evidence for Kenigsberg launches.
  - Must not regress: `/kenigsberg` must send an operator-visible acknowledgement before any slow LLM/provider/Kaggle work; text rewrite must have a hard timeout and fail closed without deterministic splitting; production evidence for a command run must include either immediate ack logs or a clear handler error.
- `INC-2026-05-12-kenigsberg-winter-dataset-not-mounted.md`
  - Scope: Kenigsberg period selection in `scripts/render_kenigsberg_story.py`, server payload construction in `handlers/kenigsberg_stories_cmd.py`, and Kaggle dataset mounts for `zigomaro/koenigsberg-stories`.
  - Must not regress: the server must not preselect period/dataset or add env switches for dataset choice; the Kaggle renderer must randomly select from actually mounted video datasets, including nested layouts such as `/kaggle/input/datasets/...`; missing video/music dataset errors must include mounted `/kaggle/input` directory names.
- `INC-2026-05-12-kenigsberg-notebook-escaped-newlines.md`
  - Scope: `kaggle/KoenigsbergStories/koenigsberg_stories.ipynb`, `/kenigsberg` manual Kaggle launch, and notebook packaging for `zigomaro/koenigsberg-stories`.
  - Must not regress: Kenigsberg notebook code-cell source entries must contain real line breaks, not literal `\\n`; every code cell must compile locally before deploy; a failed first-cell Papermill `SyntaxError` is a release blocker for the manual MVP path.
- `INC-2026-05-11-zoo-lecture-premium-emoji-and-bullet-block-truncation.md`
  - Scope: `kaggle/TelegramMonitor/telegram_monitor.py::strip_custom_emoji_entities` and the new helper `_custom_emoji_fallback_is_meaningful` (Unicode pictograph classification); `smart_event_update.py::rich_facts_extract` `program_or_examples` rule; production event 4798; Kaggle TelegramMonitor kernel deploy path.
  - Must not regress: a `MessageEntityCustomEmoji` range whose Unicode fallback is a real pictograph (`🆓`, `🎟`, `📅`, etc. — Pictographs / Enclosed Alphanumerics / Misc Symbols / Dingbats / Misc Technical / Geometric Shapes blocks) must NOT be replaced by spaces; PUA / non-pictograph placeholders must still be replaced to keep entity offsets stable; the `program_or_examples` rule must explicitly enumerate common Russian lecture bullet-block headers (`О чём поговорим`, `Правда ли, что`, `Темы`, `Вопросы`, `В программе`, `Что обсудим`, `План встречи`) and forbid collapsing multi-bullet blocks into one summary fact.
- `INC-2026-05-11-event-parse-defender-and-escalation-poc.md`
  - Scope: `main.py::_event_parse_title_looks_bare`, `main.py::_event_parse_defender_check`, and the escalation block in `main.py::parse_event_via_llm` that re-routes flagged Gemma 4 outputs to `gemini-3.1-flash-lite`; env `EVENT_PARSE_DEFENDER_ESCALATION_MODEL`; `tests/test_prompt_json.py` defender tests.
  - Must not regress: bare `<event_type> — <venue>` titles must keep being flagged; quoted-programme titles (`Концерт «Скитальцы»`, `Спектакль «Жили они долго и счастливо»`) must NEVER be flagged; on flag, the second Gemma call must use the escalation model from env; on no-flag the path stays a single Gemma call (no extra cost); on escalation timeout the un-escalated Gemma output must be returned (no hard fail); adding a new defender reason requires extending `_event_parse_defender_check` AND adding a regression test pinned to the same incident.
- `INC-2026-05-11-pre-create-dup-probe-missed-identical-ticket-merge.md`
  - Scope: `smart_event_update.py::_pre_create_duplicate_probe` branch 1 (identical normalised ticket_link + overlapping date + no time conflict + related titles); the create-path shortlist construction immediately before `INSERT event`; production events 4156 and 4752 (the «Жили они долго и счастливо» 2026-05-11 20:00 duplicate pair).
  - Must not regress: when two Smart Update candidates share an identical normalised `ticket_link` and date+time and have related titles, branch 1 must merge them; if the probe returns `None`, the rejection reason must be logged so future investigations have evidence; future replay-test under `tests/replays/INC-2026-05-11-pre-create-dup-probe-missed-identical-ticket-merge/` must prove the probe merges this exact source pair after the diagnostic logging lands.
- `INC-2026-05-11-standup-excursion-meeting-point-snapped-to-zoo.md`
  - Scope: `docs/llm/prompts.md` venue-grounding rules around lines 110–115 and the new "Meeting-point override" bullet; production event 4687 («Стендап-Экскурсия по Калининграду»); excursion / walking-tour / стендап-экскурсия family across all import paths that read this prompt.
  - Must not regress: meeting-point landmarks (sculpture/памятник, остановка/bus stop, площадь, ворота, мост, фонтан, угол улиц, парк-entrance, etc.) must never be snapped to a "Known venues" entry purely by geographic address proximity; address mismatch is a hard signal that the post is about the landmark, not the nearby building; both `location_name=<landmark>` + empty `location_address` and `location_name=""` + empty `location_address` with `city="Калининград"` are acceptable shapes for excursion meeting points.
- `INC-2026-05-11-lecturer-name-and-title-dropped-from-description.md`
  - Scope: `smart_event_update.py::rich_facts_extract` (`people_org_facts` rule), `_flatten_g4_rich_facts_payload`, `split_description_writer` fact propagation, public Telegraph description for lectures/discussions; production event 4759 and Telegram source `t.me/kraftmarket39/219`.
  - Must not regress: a source post with an explicit named speaker/lecturer/host/guest/author and a job title or credentials must produce a `people_org_facts` entry that keeps both name and title in one fact; the impersonal `профессиональная позиция спикера…` paraphrase that strips name+title is forbidden; dedicated `О спикере`/`Лектор:`/`Спикер:`/`Ведущий:`/`Автор:` sections must yield a named fact whenever they contain either a name or a title; the named fact must survive the flatten step into `facts_text_clean`.
- `INC-2026-05-11-vasnetsov-30may-stochastic-title-clone.md`
  - Scope: `event_parse` Gemma 4 master prompt at `docs/llm/prompts.md`, multi-event Telegram digest splitting, `event_source` write path for duplicate imports; production events 4760/4761 and Telegram source `t.me/signalkld/10657`.
  - Must not regress: a multi-event digest with several `🎟 <date> <time>` blocks at one venue must produce per-block titles matching each block’s programme — the first block’s title must never be cloned onto the second block’s event; if a future change adds defensive block-locality wording for titles, the existing block-locality rule for venues at `docs/llm/prompts.md:199` must stay intact.
- `INC-2026-05-11-poster-near-duplicate-and-tram-photo-dropped.md`
  - Scope: `main.py::_select_eventposter_render_urls` (OCR-only group key), `main.py::_score_eventposter_against_event` (token-only scoring), poster ingest paths that populate `EventPoster.phash`; production event 4727 and its 6 `eventposter` rows; Telegraph page `Audiospektakl-Puteshestvie-nalegke-05-08`.
  - Must not regress: VK re-encoded duplicate posters with slightly different OCR (e.g. `налегке` vs `на плечах` misread on the same image) must not both reach Telegraph; `EventPoster.phash` must be populated for every new row so phash-based dedup is usable; a semantically relevant poster (tram photo for an «аудиоспектакль в театральном трамвае») must not be silently dropped by token-overlap scoring; the LLM `poster_relevance` step (or equivalent) must be invoked at least for near-threshold low-score cases.
- `INC-2026-05-11-bar-bastion-stochastic-title-fallback-and-semantic-dup.md`
  - Scope: `event_parse` master prompt title rules at `docs/llm/prompts.md` lines 56–63 (the bare `<event_type> — <venue>` ban), `smart_event_update.py::_pre_create_duplicate_probe` for tour-style cross-source reposts; production events 4486/4765/4788, Telegraph pages `Koncert--Bar-Bastion-05-10` and `Koncert--Bar-Bastion-05-10-2`.
  - Must not regress: the `<event_type> — <venue>` title fallback must remain forbidden even on tour/repost posts that carry the programme info via inline VK `[club...|...]` mentions; future semantic dup-probe extensions for same-date+same-venue+similar-time+related-programme must not collapse legitimately distinct events (e.g. afternoon kids show vs evening adult show at the same venue).
- `INC-2026-05-09-event-location-alias-free-dup-regressions.md`
  - Scope: Telegram Monitoring / VK auto-import / Smart Update venue grounding, `docs/reference/locations.md`, `docs/reference/location-aliases.md`, duplicate matching, free/ticket nuance, phone-only contact rendering, and public Telegraph/daily surfaces for the May 9 reported cards.
  - Must not regress: semantic venue identity and alias convergence must stay LLM-first; public `location_name` must never become prose, box-office text, or unrelated source default; poster/OCR address evidence must be available to venue grounding; one real event must not survive as duplicate cards; missing/zero price or registration-only wording must not imply free; phone-only contact must stay visible/actionable; source-specific partner CherryFlash enhancements must not be mixed into incident data repair.
- `INC-2026-05-08-vk-quality-false-skips.md`
  - Scope: `vk_intake.py` Gemma 4 VK draft extraction, `smart_event_update.py` non-event/online guards and source-grounded writer spelling guard, Fort 11 Doenhoff location aliases, production event 4717 Telegraph rebuild, and VK sources `wall-48383763_40430`, `wall-78248807_6006`, `wall-138053522_2532`.
  - Must not regress: concrete VK events with date/time/venue/ticket anchors must not be skipped as `non_event_notice`; offline events must not be skipped as `online_event` only because a programme mentions a stream/broadcast; Fort 11 Doenhoff must resolve to Kaliningrad; public writer output must preserve source-grounded rare proper names such as `Симуран`; one art-market/fair/holiday programme in one place/day should import as one umbrella event unless independent ticket/venue anchors prove separate events.
- `INC-2026-05-08-vk-tg-prompt-and-dup-probe.md`
  - Scope: `smart_event_update.py::_pre_create_duplicate_probe` and the deterministic matcher chain it backstops; `docs/llm/prompts.md` master prompt (title fallback, `location_address` anti-fabrication, ticket-sales-point exclusion, opaque-source rule); `main._parse_event_via_gemma` retry/timeout policy; production rows for events 4173/4200/2819/4038/4350/4396/4397/4584/4585/3824/3983/4670/4645/4664/4705.
  - Must not regress: a VK/Telegram cross-source repost with identical `ticket_link` (or identical `location_name`+time anchor) and a related title must merge into the existing event, not create a new active card; `location_address` must never contain foreign-language tokens (`asignatura`), ticket-sales points (`ТРЦ "Европа"`/`атриум "Лондон"`), or prose / curator quotes; the bare `<event_type> — <venue>` title template is forbidden — return `[]` when no name or programme theme is recoverable; events whose box-office sits inside a different building must place the canonical venue in `location_*` and never the box-office address; `event_parse` provider 5xx must not pin a row for the full `VK_AUTO_IMPORT_ROW_TIMEOUT_SEC`.
- `INC-2026-05-07-vk-auto-import-merge-regression-gemma4.md`
  - Scope: `vk_intake.py`, `vk_auto_queue.py`, `smart_event_update.py` (`_match_existing_event_by_*`, `_single_candidate_auto_match_ok`, `_llm_match_or_create_bundle`, `_ask_gemma_json`, `_ask_gemma_text`), `markup.py::unescape_public_text_escapes` / `simple_md_to_html`, env `VK_AUTO_IMPORT_PARSE_GEMMA_MODEL`, `SMART_UPDATE_MODEL`, `SMART_UPDATE_GEMMA_JSON_WALL_CLOCK_SEC`, prompt families в `docs/llm/`.
  - Must not regress: VK auto-import не должен создавать duplicate `event` row при наличии существующей `event` row с совпадающим `(date,time,location_name,ticket_link)`; HTML entities (`&quot;`, `&amp;`, `&lt;`, `&gt;`) не должны утекать в публичный `event.description` через VK source_text; миграции Gemma-backed upstream stage на новый checkpoint обязаны нести family-prompt rewrite, а не только model-id swap; vk_intake не должен fabricate `location_name`/`location_address` вне `docs/reference/locations.md` reference layer; multi-event digest посты (несколько разных событий по одной строчке без описания / времени / площадки) должны идентифицироваться на этапе vk_intake как `multi_event_digest` и не уходить в smart_update; phone-only contact ивенты должны экспонировать кликабельный `tel:` (через `ticket_link='tel:...'`); провайдерские 5xx-ответы не должны держать `_ask_gemma_json` свыше hard wall-clock cap, и неудача merge LLM не должна оставлять `vk_inbox` row в `pending`.
- `INC-2026-05-07-vk-time-reschedule-wrong-match.md`
  - Scope: VK auto-import cancellation/postponement shortcut, VK date/time parsing, event lifecycle matching, and Smart Update reports for VK transfer/time-change posts.
  - Must not regress: a notice like `8 мая время начала ... перенесено на 19.30` must parse the date from `8 мая`, must stay on the normal LLM-first VK import path, and must never mark an unrelated old event inactive through weak no-date/no-title matching.
- `INC-2026-05-05-80-stories-source-coverage.md`
  - Scope: VK crawl/date-hint admission, Telegram Monitoring festival-program extraction, `@kraftmarket39` source coverage, and future `80 историй о главном` backfill.
  - Must not regress: concrete festival event posts with title/date/time/venue/registration must reach the LLM-first import path and attach a durable `event_source`; Russian month-name dates like `16 мая 2026 г. в 16:00` must not be masked as phone-like noise before crawl admission.
- `INC-2026-05-05-event-source-media-aggregation-gap.md`
  - Scope: Telegraph event rebuild, `event_source` media rehydration, `eventposter`/`event.photo_urls`, Telegram public-page poster fallback, VK wall photo fetch.
  - Must not regress: a multi-source event page must not show only the current row's single image when attached Telegram/VK sources still expose additional unique images; duplicate repost media must be deduped.
- `INC-2026-05-05-80-stories-video-promo-gap.md`
  - Scope: CherryFlash/CrumpleVideo festival visibility and future `promo` feature.
  - Must not regress: failed/manual-test video sessions must not be counted as public festival exposure, scheduled viewer-facing CherryFlash delivery must be counted even when its legacy row status is `PUBLISHED_TEST`, and future promo/festival selection work must explicitly verify `80 историй о главном` candidate visibility after source backfill.
- `INC-2026-05-05-smart-update-gemma3-fallback-hallucination.md`
  - Scope: Smart Update model-chain configuration, Google AI provider compatibility, writer-stage fallback policy, and production backfills with LLM enabled.
  - Must not regress: a provider `NotFound` for the first-hop Smart Update model must fail closed for writer stages instead of falling through to broad fallback prose that can introduce unrelated event content.
- `INC-2026-05-05-kitoboya-garage-date.md`
  - Scope: Telegram Monitoring / VK auto-import / Smart Update exhibition date grounding, teaser handling, `course_promo` skip guard, long-running inferred-range correction, and production cleanup for the `Куплю гараж. Калининград` duplicate/date regression.
  - Must not regress: exhibition/fair teasers without an exact day/range/end date must not materialize as first-of-month or message-date event cards; later exact announcements with `кураторские экскурсии` must not be skipped as course promos; and a later source-grounded opening date must be able to correct an inferred legacy long-run exhibition row through a real import + Smart Update replay.
- `INC-2026-05-05-event-quality-regression.md`
  - Scope: Telegram Monitoring / VK auto-import / Smart Update free/location/duplicate invariants, source/default venue fallback, same-ticket/same-slot matching, rental/non-event guards, production event inventory cleanup.
  - Must not regress: zero or missing ticket price must not imply `is_free=true`; ticket giveaways or included-in-entry-ticket wording must not mark the event free; prose/unsupported locations must not be replaced by unrelated `default_location`; same real event must not survive as multiple cards when a specific ticket URL/date/place or near-identical same-slot source text proves identity.
- `INC-2026-05-05-cherryflash-disk-full.md`
  - Scope: `guide_excursions/kaggle_service.py`, `/data/guide_monitoring_results`, Fly `/data` volume, SQLite `/data/db.sqlite`, scheduled CherryFlash `popular_review`, production catch-up health checks.
  - Must not regress: Guide monitoring result bundles must not fill the production SQLite volume; `database or disk is full` / `Errno 28` must trigger disk evidence collection and same-day CherryFlash catch-up if the local slot was missed.
- `INC-2026-05-02-pre-daily-event-quality.md`
  - Scope: Telegram Monitoring / VK auto-import event-local venue grounding, literal field-placeholder cleanup, canonical ticket/program titles, and pre-daily future duplicate/location audit.
  - Must not regress: active today/future event cards must not borrow unrelated source/default venues when the event-local block names a different venue; field-name literals like `location_address` must not become public data; and one real event must not survive as multiple active cards before daily surfaces.
- `INC-2026-05-01-future-event-quality-audit.md`
  - Scope: Telegram Monitoring / VK auto-import future active event rows, prose-like `location_name`, source/default venue recovery, Smart Update duplicate merge guards, Bar Bastion future imports, `/daily`/Telegraph/month/day/video-announcement surfaces.
  - Must not regress: future active event cards must not expose prose/schedule fragments as venues, and one real future event must not survive as multiple active public cards when source posts differ only by repost, ticket URL, title wording, or doors/start time.
- `INC-2026-05-01-daily-location-drift.md`
  - Scope: Telegram Monitoring Gemma venue extraction/review, Telegram candidate grounding, Smart Update weak/default time duplicate matching, VK source default-location repair, and May 1 daily catch-up.
  - Must not regress: arbitrary prose/schedule fragments must not survive as public `location_name`; semantic venue repair must stay LLM-first rather than a growing phrase dictionary; unsupported extracted times must be weak anchors; known VK sources must not default unrelated events to `Калининград Сити Джаз Клуб`.
- `INC-2026-04-30-tg-monitoring-event-quality-regressions.md`
  - Scope: Telegram Monitoring Gemma extraction prompts/schema, schedule-rescue prompt, free/ticket semantics, Smart Update duplicate matching, production cleanup for false-free/work-hours/duplicate event rows from the 2026-04-30 batch.
  - Must not regress: missing ticket price must not mean free; ticket links/status/sale wording must not produce `is_free=true` without explicit free-entry evidence; institution work-hours/holiday-opening notices must not be imported as events by schedule rescue; same real event must not survive as multiple public cards because title/location wording drifted.
- `INC-2026-04-30-tg-monitoring-work-schedule-false-skips.md`
  - Scope: `smart_event_update.py` deterministic non-event guard `work_schedule`, Telegram Monitoring server import, `telegram_scanned_message` skip diagnostics, `/daily` recently-added inventory, production catch-up for `@kenigatom/496` and `@kraftmarket39/199`.
  - Must not regress: concrete future Telegram events at a museum/library venue or an address containing `Музейная` must not be skipped as `skipped_non_event:work_schedule` unless the source is actually a work-hours notice.
- `INC-2026-04-29-bar-bastion-city-jazz-location.md`
  - Scope: `docs/reference/locations.md`, `docs/reference/location-aliases.md`, `db.py` `vk_source` seed defaults, `location_reference.py`, `main.py` event-parse reference normalization, VK auto-import rows from `bar_bastion`.
  - Must not regress: addressless Bar Bastion posts from VK group `149955604` must not be assigned to `Калининград Сити Джаз Клуб`; Bar Bastion must normalize to `Бар Бастион, Судостроительная 6/1, Калининград`, and the production source default must stay set.
- `INC-2026-04-28-vk-smart-update-false-skips.md`
  - Scope: `smart_event_update.py` online-only guard, `festival_queue.py` festival-context routing, `docs/llm/prompts.md`, VK auto-import `persist_skipped` handling, production `vk_inbox`/`ops_run` catch-up evidence.
  - Must not regress: a concrete offline VK event must not be skipped only because it has online registration, and a single masterclass/lecture/show/ride inside a festival/cycle/program context must create/update an event instead of being routed as a whole `festival_post`.
- `INC-2026-04-27-cherryflash-missing-photo-urls.md`
  - Scope: `video_announce/popular_review.py`, `video_announce/scenario.py`, scheduled CherryFlash `popular_review`, prod sqlite event/session rows, Kaggle handoff/story publish evidence.
  - Must not regress: CherryFlash must not pick events whose persisted rows still have empty renderable `photo_urls`; source-post poster rehydration must be persisted before session items and render payload are built; older `cherryflash-session-*` datasets must not remain attached to the shared Kaggle kernel; and a missed same-day CherryFlash slot must be repaired with a compensating rerun.
- `INC-2026-04-27-prod-unresponsive-during-cherryflash-recovery.md`
  - Scope: Fly production runtime, `/healthz`, `/webhook`, `/start`, CherryFlash live/catch-up runner, and long-running production validation.
  - Must not regress: CherryFlash recovery must not continue while the serving bot is unhealthy; runtime file mirror or fallback evidence must be checked; `/healthz` and webhook readiness must be restored before same-day catch-up evidence is accepted.
- `INC-2026-04-27-tg-monitoring-sticky-skipped-post.md`
  - Scope: `source_parsing/telegram/handlers.py`, `telegram_scanned_message` idempotency, Telegram Monitoring import-only/recovery, Smart Update skipped results, `/daily` recently-added inventory, video announcement input pool.
  - Must not regress: a Telegram post with `events_extracted > events_imported` must not become permanently metrics-only just because an earlier server import marked it `skipped` without diagnostics; valid future event payloads must be retryable, while intentional/permanent skips must persist a reason/breakdown.
- `INC-2026-04-26-crumple-story-required-channel-fanout.md`
  - Scope: `VIDEO_ANNOUNCE_STORY_TARGETS_JSON`, `fly.toml`, `.env.example`, `video_announce/story_publish.py`, `kaggle/CrumpleVideo/story_publish.py`, embedded `crumple_video.ipynb`, scheduled `video_tomorrow` story status.
  - Must not regress: `me` remains the first blocking render-gate target, but production channel fanout (`@kenigevents`, `@lovekenig`) must be marked required so a missing channel story cannot finish as green `Story publish status: OK`.
- `INC-2026-04-26-prod-slow-during-vk-daily-catchup.md`
  - Scope: Fly production runtime, `/healthz`, `/webhook`, manual production catch-up/smoke commands, VK daily recovery procedure.
  - Must not regress: manual production catch-up must not run heavy full-bot workflows in a way that starves the serving machine; health/webhook degradation during validation must stop validation immediately and restore serving before continuing.
- `INC-2026-04-26-vk-daily-message-limit.md`
  - Scope: `main_part2.py::build_daily_sections_vk`, `send_daily_announcement_vk`, `post_to_vk`, `vk_scheduler`, `fly.toml` / `VK_DAILY_POST_MAX_CHARS`, VK daily publication state.
  - Must not regress: VK daily must split oversized sections before `wall.post`, must preserve event cards when possible, and must not mark the daily VK slot sent unless every chunk returns a VK post URL.
- `INC-2026-04-26-daily-location-fragments.md`
  - Scope: `source_parsing/telegram/handlers.py`, `location_reference.py`, `docs/reference/locations.md`, `docs/reference/location-aliases.md`, `main_part2.py::build_daily_posts`, Telegram Monitoring Gemma 4 location extraction/import, `/daily` publication.
  - Must not regress: prose/schedule/bio fragments must not survive as public `location_name`, known venues must recover address/city from the reference layer, and one `/daily` event card must not be split between two Telegram posts.
- `INC-2026-04-25-prod-bot-unresponsive-after-tg-monitoring-smoke.md`
  - Scope: Fly production runtime, Telegram webhook and `/start`, Telegram Monitoring post-deploy smoke/recovery/import path, scheduler heavy jobs, runtime health, and runtime evidence collection.
  - Must not regress: production smoke/validation must not make the serving bot unresponsive; `/healthz` and `/webhook` failures must trigger incident workflow immediately; runtime log mirror/rotated files must be checked before falling back to Fly logs/Kaggle/DB evidence.
- `INC-2026-04-24-crumple-story-channel-boosts-required.md`
  - Scope: `fly.toml`, CrumpleVideo story target order, `video_announce/story_publish.py`, `kaggle/CrumpleVideo/story_publish.py`, scheduled `video_tomorrow` catch-up, Telegram channel story boosts.
  - Must not regress: production CrumpleVideo must keep a Premium self-account story target (`me`) as the first blocking upload target, channel `BOOSTS_REQUIRED` must remain visible without blocking render delivery, required channel fanout must not finish green when missed, and a missed same-day scheduled slot must be repaired or explicitly blocked by Telegram capability evidence.
- `INC-2026-04-23-cherryflash-pre-handoff-loss.md`
  - Scope: `video_announce/scenario.py`, `scheduling.py`, `video_announce/poller.py`, CherryFlash scheduled `popular_review`, prod sqlite `ops_run`/`videoannounce_session`, Kaggle CherryFlash handoff evidence.
  - Must not regress: scheduled CherryFlash must not mark `ops_run` success before a real non-local Kaggle dataset/kernel handoff is persisted, local-only failed sessions for today's slot must trigger same-day catch-up, and existing remote handoffs must suppress duplicate reruns even if local status is misleading.
- `INC-2026-04-23-guide-digest-extraction-loss.md`
  - Scope: `kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py`, guide Gemma 4 screen/extract/enrich prompts, multi-date occurrence extraction, guide digest eligibility/catch-up.
  - Must not regress: multi-date posts must preserve each available dated excursion as digest-ready, sold-out/no-date/non-excursion controls must not become subscriber-visible digest cards, and a missed daily guide window must be repaired with production-equivalent monitor/catch-up evidence.
- `INC-2026-04-22-cherryflash-service-notifications-routed-to-channel.md`
  - Scope: `video_announce/poller.py`, `video_announce/scenario.py`, CherryFlash/admin notify routing, Telegram publish-vs-service destination split.
  - Must not regress: restart/service diagnostics must never leak into `test`/`main` publish channels by fallback; they must stay in operator/superadmin DM unless an explicit notify target says otherwise.
- `INC-2026-04-22-cherryflash-false-failed-after-successful-story-publish.md`
  - Scope: `video_announce/poller.py`, `video_announce/scenario.py`, CherryFlash scheduled recovery/handoff state, prod sqlite `videoannounce_session`, Kaggle CherryFlash completion evidence.
  - Must not regress: a fresh CherryFlash run must not remain locally `FAILED` after the same dataset already reached successful Kaggle/story completion, while truly stale `local:*` sessions must still fail closed instead of hanging forever.
- `INC-2026-04-21-guide-gemma4-partial-monitoring.md`
  - Scope: `kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py`, `google_ai/client.py`, `guide_excursions/service.py`, scheduled guide monitoring and auto-publish, `/guide_report` observability.
  - Must not regress: Gemma 4 guide schemas must stay provider-compatible, individual post-level LLM/provider failures must remain visible with source/post IDs, and a run-level `partial` marker must not suppress digest publication when fresh eligible guide material was successfully imported.
- `INC-2026-04-20-video-tomorrow-stuck-rendering.md`
  - Scope: scheduled `/v tomorrow`, `video_announce/scenario.py`, `video_announce/poller.py`, `scheduling.py`, runtime supervision, prod sqlite state.
  - Must not regress: scheduled `video_tomorrow` must not crash while leaving the live session orphaned in `RENDERING`, and restart recovery must never poll Kaggle against repo-local `local:*` refs.
- `INC-2026-04-20-club-znakomstv-duplicate-event-cards.md`
  - Scope: `smart_event_update.py`, `vk_intake.py`, cross-source repost merge guards, event-page rebuild path, prod event rows for the same day/venue cluster.
  - Must not regress: one real event must not survive as multiple active cards when reposts vary only by ticket URL/button wording or when one extractor takes `doors` time and another takes `start` time from the same source text.
- `INC-2026-04-19-cherryflash-story-media-invalid.md`
  - Scope: `kaggle/CrumpleVideo/story_publish.py`, `kaggle/CrumpleVideo/crumple_video.ipynb`, CherryFlash story bundle, Telegram `SendStoryRequest` media profile.
  - Must not regress: CherryFlash story publish must not pass preflight but then fail with opaque `MEDIA_FILE_INVALID`; the exact uploaded story file must be the one-pass final `720x1280 H.265/AAC` CherryFlash render, and `story_publish_report.json` must contain media diagnostics for that uploaded file without a default helper re-transcode.
- `INC-2026-04-16-prod-disk-pressure-runtime-logs.md`
  - Scope: `fly.toml`, `runtime_logging.py`, Fly prod volume hygiene, `/data` artifact retention.
  - Must not regress: the production bot must not become unavailable because `/data` filled up with runtime logs/backups and startup logging hit `Errno 28`.
- `INC-2026-04-16-cherryflash-kaggle-save-kernel-drift.md`
  - Scope: `video_announce/kaggle_client.py`, CherryFlash Kaggle launch path, `kaggle/CherryFlash/`, Kaggle `SaveKernel` response handling.
  - Must not regress: CherryFlash must not log a successful deploy when Kaggle `SaveKernel` returned an error, and fresh `cherryflash-session-*` datasets must be retried as bind-lag instead of being silently accepted as stale launch state.
- `INC-2026-04-15-gate-location-and-linked-facts-drift.md`
  - Scope: `source_parsing/telegram/handlers.py`, `smart_event_update.py`, `docs/reference/locations.md`, Telegraph event rebuild path.
  - Must not regress: gate-family venues (`Закхаймские` / `Фридландские` / `Железнодорожные`) не должны схлопываться по слову `ворота`, а linked-source sensitive facts не должны попадать в canonical fact log без подтверждения source text / OCR.
- `INC-2026-04-14-daily-delay-vk-auto-queue-lock-storm.md`
  - Scope: `ops_run.py`, `vk_review.py`, `vk_auto_queue.py`, `main.py::_vk_api`, Fly prod recovery, `/daily`, `/start`.
  - Must not regress: transient SQLite locks не должны системно останавливать scheduler recovery, проблемный VK post не должен бесконечно всплывать после rate-limit, а `/daily` shortlink failures не должны растягивать ежедневный анонс на повторные bad-token попытки.
- `INC-2026-04-10-crumple-story-prod-drift.md`
  - Scope: `/v`, `video_announce/`, `kaggle/CrumpleVideo/`, `fly.toml`, story-related env и release drift.
  - Must not regress: story publish не должен silently деградировать в mp4-only режим.
- `INC-2026-04-10-crumple-audio-source-drift.md`
  - Scope: `/v`, `video_announce/scenario.py`, Kaggle dataset assembly, audio assets и final render contract.
  - Must not regress: финальный production asset должен использовать только `The_xx_-_Intro.mp3`.
- `INC-2026-04-10-tg-monitoring-festival-bool.md`
  - Scope: `tg_monitoring`, `source_parsing/telegram/`, `smart_event_update.py`, Kaggle payload normalization.
  - Must not regress: malformed optional payload fields не должны переводить импорт в `partial` из-за типового `.strip()`/diagnostic crash.

## Правила ведения incident records

1. Один customer-visible production event — один канонический incident record.
2. Если похожий сбой повторился в другой день или в другой волне, создавай новый `INC-*` record и ссылайся на предыдущий в `Related incidents`, а не переписывай историю поверх старого.
3. Каждый incident record должен содержать automation contract:
   - affected surfaces;
   - mandatory checks before closure/deploy;
   - required evidence;
   - follow-up actions.
4. Инцидент не считается дисциплинированно закрытым, пока fix не в проде, не достижим из `origin/main`, не покрыт regression evidence и не заведены follow-up actions.
5. Для source-import / Smart Update quality incidents regression evidence обязано включать replay сырых offending source artifacts через production import path + Smart Update на prod snapshot/shadow DB. Prompt diff, unit tests или ручной SQL-аудит без такого replay не являются достаточным closure.
