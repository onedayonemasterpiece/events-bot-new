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
