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

- `INC-2026-08-22-sos-dedup-veto-location-tyunin-farm.md`
  - Scope: systemic August Smart Update vector + LLM identity regression,
    final match/distinct/retry application, occurrence-scoped replay and safe
    production duplicate repair.
  - Must not regress: an owner or `VETO_CREATE` may never fall through to
    ordinary CREATE; only source-grounded distinct evidence permits CREATE,
    uncertainty is durable retry, vectors remain recall-only, exact replay is a
    no-op and accepted-only side effects remain intact.
- `INC-2026-08-22-tyunin-farm-location-drift.md`
  - Scope: separate Tyunin Farm canonical venue/address reference defect for
    event `7717`; it is not the cause of the systemic dedup regression.
  - Must not regress: maintained venue/address data requires authoritative
    evidence plus prose/hashtag resolver fixtures; never invent an address.
- `INC-2026-08-21-tg-event-public-writer-max-tokens.md`
  - Scope: Telegram `tg_event_publish` grounded structured writer, Gemini Lite/strict 4o output budgets, `sentences[].text + evidence_quote` JSON schema cardinality, fallback budget, and publication catch-up.
  - Must not regress: provider schema must enforce one through three sentences, ordinary/promo completion budgets must cover grounded JSON rather than only visible intro text, and closure requires controlled retries plus public verification for the affected cohort, prioritizing events without a Telegram post URL.
- `INC-2026-08-15-audio-mcp-runtime-catalog-truncation.md`
  - Scope: private eventsBot ChatGPT MCP catalog ordering, audio tool discovery,
    existing `telegram:publish` authorization, and live conversation runtime.
  - Must not regress: all three `audio_transcription_*` tools must stay inside
    the bounded discovery prefix while the complete `tools/list` retains every
    existing tool; closure requires a real ChatGPT start call, not only an app
    settings scan or direct protocol smoke.
- `INC-2026-08-15-ingestion-retry-stall-and-wal-growth.md`
  - Scope: current Telegram/VK/official-source yield, Smart Update grounding
    retries and their consumer, SQLite WAL reuse, and the StaticSiteBuilder
    immutable input handoff.
  - Must not regress: every event-bearing child must reach a visible accepted
    create/merge/no-op/no-event or terminal technical receipt in the same
    product invocation; current source
    obligations must not hide behind run-level success; WAL must stay bounded
    through real ingestion; static build input must remain immutable,
    coverage-complete and capacity-safe.
- `INC-2026-08-15-mtproto-proxy-desktop-reconnect.md`
  - Scope: Telegram Desktop saved MTProto profile recovery after a VPS reboot, DD protocol health, DNS-alias profile identity, TCP `1443`, current-day logs and disk capacity.
  - Must not regress: closure requires a full DD handshake to Telegram DCs plus real Desktop recovery confirmation; a healthy listener alone is insufficient, and stale same-endpoint profiles must be bypassed with a distinct hostname without rotating the shared secret.
- `INC-2026-08-14-cherryflash-terminal-lock-and-smart-update-visibility.md`
  - Scope: closed sev1 regression contract for CherryFlash terminal-ledger
    reconciliation, Smart Update retry-worker accepted-result reporting and
    downstream static CDN delivery.
  - Must not regress: a terminal remote render must release a stale local lock
    without claiming unverified delivery; durable retry `CREATED`/`MERGED`
    results emit one bounded report, while report failure cannot alter the
    accepted result.
- `INC-2026-08-12-data-volume-ingestion.md`
  - Scope: open sev1 contract for raw-first VK volume growth, WAL checkpoint
    starvation, Kaggle launch recovery and StaticSiteBuilder snapshot ownership.
  - Must not regress: high-volume writes stop before the disk floor, current VK
    carriers progress under history, WAL remains bounded across real writes,
    catch-ups are exact-one, and static recovery/publishing uses exact-owner
    receipts and an exact-main capacity-backed canary.
- `INC-2026-08-10-smart-update-identity-terminal-loss.md`
  - Scope: open P0/sev1 configured-source ingestion recall and Smart Update
    typed identity/terminal-state contract. PR #494 merged as `69ec40342`; the
    record remains open for replay, backlog and production acceptance gates.
  - Must not regress: no pre-LLM semantic terminal, deterministic post-LLM veto,
    incomplete-evidence no-event or generic technical terminal; occurrence
    identity, accepted-only side effects and raw-boundary replay must remain
    balanced and auditable.
- `INC-2026-07-29-mtproto-proxy-desktop-disconnect.md`
  - Scope: host-level `vpn-server` MTProto container, TCP `1443`, Telegram DC connectivity, persistent application-log mount and bounded retention.
  - Must not regress: current-day proxy logs must survive container recreation; old rotations must stay bounded; closure requires a live listener, fresh Telegram DC handshake evidence, downstream connections and disk-usage evidence.
- `INC-2026-07-11-event-vector-sidecar-sync-stalled.md`
  - Scope: `related_v1`/`search_v3` projection ownership, post-Smart-Update
    coalescing, three-hour reconciliation and eligible-catalog freshness.
  - Must not regress: both document kinds must cover the eligible public
    catalogue with current hashes; vectors remain recall evidence, not merge
    authority.
- `INC-2026-07-08-prod-root-overlay-disk-full.md`
  - Scope: Fly production root writable overlay `/.fly-upper-layer`, `/tmp` Kaggle output directories, CherryFlash partner output downloads, guide/source parser temp directories, and runtime logging env drift.
  - Must not regress: production `/tmp` must not fill the Fly root overlay with `videoannounce-*` / `guide-excursions-*` bundles; `Errno 28` must trigger both `/data` and root-overlay disk evidence, `/tmp` write verification, runtime logging env verification, and same-day scheduled-job catch-up review.
- `INC-2026-07-05-guide-visual-digest-stale-vk-booking-link.md`
  - Scope: guide excursions visual digest `visual_schedule`, LLM extraction of `booking_url` from multi-event schedule links, and VK/Telegram caption primary-link selection.
  - Must not regress: a future schedule occurrence must not expose a historical source wall post as the primary booking/details link; repeated-route inline links must be validated against current occurrence context or demoted to source/current post fallback.
- `INC-2026-07-03-current-import-vector-vk-publication.md`
  - Scope: VK auto-import current-batch row failures, Smart Update vector identity-gate evidence, and managed VK postponed publication idempotency for updated/imported events.
  - Must not regress: `vk_auto_import` must not hide event-like row failures behind a green run; create-path vector identity decisions must stay auditable in `event_identity_decision_log`; updating an existing canonical event must not leave duplicate managed postponed VK posts; a VK `214 already scheduled for this time` collision must be reconciled or retried instead of leaving a fresh event without managed VK coverage.
- `INC-2026-07-02-exhibition-duplicates-static-site.md`
  - Scope: Telegram Monitoring / VK import / parsers / Smart Update exhibition identity, long-running date ranges, source-post update vs new-event classification, static site `/vystavki/` exposure, and production cleanup for duplicate/corrupt exhibition rows.
  - Must not regress: one real exhibition must not survive as multiple active public cards; roundup/spotlight/update posts must not create new exhibition cards with `date=message_date` and inferred one-month ranges; prompt/comment/code-like text must never enter public fields; `event.date`/`event.end_date` must be ISO dates or NULL; prose/emoji/operational text must not become venue/address; concerts/closure events/festival series must not be merged into exhibitions or inherit exhibition type.
- `INC-2026-06-29-kgd80-ticket-location-drift.md`
  - Scope: Telegram Monitoring hidden TextUrl/button registration-link handling, KGD80/«80 историй о главном» ticket-link specificity, chat-author fallback, current official KGD80 address verification, and production repair for events 4417/5077/5656. Must not regress: posts with both generic `kgd80.ru` and specific `kgd80.ru/sobytiya/.../?register=1` links must choose the specific registration link; generic festival-domain ticket links must yield to event-specific registration URLs; chat-author Telegram handles must not be used when a source post carries a hidden official registration URL; KGD80 future-event announcements require source/offical-page DB verification.
- `INC-2026-06-25-outbox-ics-publication-backlog.md`
  - Scope: event publication outbox, Supabase Storage ICS upload, `tg_ics_post`/`tg_event_publish` dependencies, and catch-up for events added/imported while ICS publication is failing. Must not regress: ICS uploads must use the Storage API path, repeated `ics_publish` failures must not silently park eligible Telegram event posts years in the future, and same-day publication gaps require catch-up plus public-surface verification.
- `INC-2026-06-25-vk-channel-wrong-surface.md`
  - Scope: promo `vk_channel_publish` and VK community Channel publishing for `80 историй о главном`; must not use personal-message/Favorites delivery as a substitute for the VK Messenger Channels tab.
- `INC-2026-06-24-future-event-date-default-venue-regressions.md`
  - Scope: Telegram Monitoring Gemma extraction/import date semantics, event-local offsite venue grounding versus source defaults, Smart Update writer grounding, duplicate recall after venue drift, and public `@kldevents` future inventory.
  - Must not regress: Russian compact dates (`10.05`, `30.05`) are `DD.MM`, month-word/hashtag dates (`26 июля`, `#13_июня`, `#21_июня`) remain authoritative, gate/floor/address/price/coordinate numbers never become event dates/times, retrospective reports without a future invite return `[]`, explicit offsite venue/address lines beat `source.default_location` even when extractor initially omits venue, and thin/free source posts must not gain unsupported buy-ticket/theatre boilerplate. Closure requires replaying the exact source URLs through Telegram Monitoring server import + Smart Update and verifying repaired public Telegram/Telegraph surfaces.
- `INC-2026-06-18-tg-location-prose-still-extracted.md`
  - Scope: Telegram Monitoring extraction/import venue semantics, server-side prose/person location guards, deterministic location inference/recovery, and public `@kldevents` event inventory.
  - Must not regress: Telegram Monitoring must not keep producing descriptive prose/person names as `location_name`/`location_address`; deterministic guards may prevent public garbage, but closure also requires recall evidence that event-like posts are not silently lost as `invalid:missing_location` after the guard drops prose; deterministic free-text inference must not overrule source/default known venues, exact-address normalization must not bind room/studio addresses to unrelated canonical venues, and multi-event roundup media must stay event-local before `@kldevents` publication.
- `INC-2026-06-13-kaggle-duplicate-videoannounce.md`
  - Scope: Kaggle status observability for CherryFlash/CrumpleVideo/VideoAfisha/Koenigsberg story/video publication and scheduled retry diagnostics.
  - Must not regress: Kaggle runs must emit/record enough phase and heartbeat evidence to distinguish startup, preflight, rendering, publishing, report writing, final-output failure, and resource-session conflicts; the framework must not silently deduplicate or suppress public video-announcement publication without an explicit product decision.
- `INC-2026-06-05-vk-story-forward-wall-first.md`
  - Scope: CherryFlash/Kaggle VK story fanout (`vk_wall`, `vk_wall_story`, legacy `vk_story`), `popular_review` target order for `kenigeventsofficial`/`klgdevents`, КОНБ `konb39` VK targets, and promo VK activities for `80 историй о главном`.
  - Must not regress: video announcements for these VK communities must publish a wall clip first and link/forward that wall post into stories instead of uploading the mp4 separately to stories; promo poster story forwards must use the source image/poster plus internal wall `link_url` without dragging the wall-post caption into a white story card; `80 историй о главном` must keep two daily `vk_story_forward` opportunities.
- `INC-2026-06-04-kraftmarket271-tg-monitoring-tpm-import-cancel.md`
  - Scope: Telegram Monitoring producer extraction/OCR/rate-limit retry for `@kraftmarket39`, Google AI key registry and reserve fallback for `GOOGLE_API_KEY3`, server-side Telegram result import/recovery cancellation handling, scanned-message diagnostics, and `80 историй о главном` promo-campaign intake.
  - Must not regress: a clear event-like festival promo post such as `@kraftmarket39/271` (`Калининград корабельный`, 2026-07-08, registration URL on `kgd80.ru`) must not be silently recorded as `events=[]` because of TPM; rate-limit/provider failures must be distinguishable from legitimate zero-event output; cancelled import/recovery must resume or rerun until the source tail is imported or has durable diagnostics; closure requires production DB evidence that `kraftmarket39` cursor/source rows are caught up through message `271`.
- `INC-2026-05-30-active-duplicate-events-recall-gate.md`
  - Scope: `smart_event_update.py` create path — shortlist construction (`location`/`time` pre-filters), `_pre_create_duplicate_probe`, `_llm_match_or_create_bundle`, and the `(source_type, source_url)` idempotency guard.
  - Must not regress: a genuine duplicate whose raw `location_name` is an alias/box-office/ticket-vendor variant of the same canonical venue, or whose time differs only as doors-vs-start/matinee skew, must stay in the dedup recall (shortlist) so the matcher/probe can compare it; two-ticket-vendor same-slot siblings must merge; genuinely-distinct same-venue same-day events (matinee + evening, two different shows) must NOT be collapsed.
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
