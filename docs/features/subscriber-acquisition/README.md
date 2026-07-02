# Subscriber Acquisition

Canonical requirements: [requirements.md](requirements.md).

Discovery MVP design: [mvp-discovery.md](mvp-discovery.md).

Source materials: [source/](source/).

## Implementation entrypoints

Shadow-mode MVP scaffolding is implemented in `subscriber_acquisition/` and is
wired into the bot through `main_part2.py` as `/acq*` superadmin commands,
including `/acq_surface_add` and `/acq_surfaces` candidate review cards. Core
state is stored in Fly SQLite tables with SQLModel models in `models.py` and raw
bootstrap DDL in `db.py`. The import payload contract is documented in
[`schemas/acq_discovery_result.schema.json`](schemas/acq_discovery_result.schema.json).

The MVP is intentionally review-only: it may send Telegram messages only to
`ACQ_REVIEW_CHAT_ID` review chat and has guardrails against external Telegram/VK
posting. Kaggle/session wiring uses the existing heavy-job and S22 remote-session
controls; the scanner runtime is under `kaggle/SubscriberAcquisitionDiscovery/`.
`/acq_run` uses the existing Kaggle encrypted split-dataset + status framework by
default (`ACQ_DISCOVERY_RUNNER=kaggle`) and can import an explicit result JSON.
`ACQ_DISCOVERY_RUNNER=local` is only a dev fallback that writes/imports
`acq_discovery_result.json` without any external sends. Local launchers require
the same Kaggle API dependency lane as Telegram Monitoring; `kagglesdk==0.1.30`
is pinned because newer 0.1.31+ wheels currently break Kaggle API imports before
the kernel can be pushed.

Telegram opportunity discovery is comment-first: source/channel posts are read
to discover linked discussion chats and outbound links, but review opportunities
are created only from human group/comment messages. Operator rejection feedback
is collected by pressing `Нет + причина` and replying to the review card; the
reply text is stored in acquisition feedback export. Runtime seed collection
prioritizes newly discovered and linked-discussion surfaces before older seed
surfaces, so repeated Kaggle runs walk the frontier instead of rechecking only
the original seed list. Inside a Kaggle run, Telegram scanning also performs a
bounded deterministic frontier walk: links found in scanned messages are queued
for the same run up to `ACQ_MAX_TG_FRONTIER_PER_RUN` and
`ACQ_MAX_SURFACES_PER_RUN`, without spending LLM budget on link extraction.
The server seed payload is enriched with public Kaliningrad Telegram handles from
Telega.in regional cards (`Kaliningrad_jenskiy`, `kpkld`, `gokaliningrad_ru`,
`kenig01`, `Davai_KLD`, `kaliklove`, `jobs39`, `anons39`,
`nedvizhimostkalinigrad`, `remont3939`, `autoclub_kld`,
`kaliningrad_now_ru`) and marks them as `source=telega_in` in the group map.
Discovered surfaces are region-gated for Kaliningrad Oblast before further
analysis: obvious out-of-region links such as `visitNavahrudak` are marked
`rejected_out_of_region` and are not queued for scanning. VK seeds come from
existing `vk_source` monitoring groups and the read-only scanner tries configured
VK token lanes with fallback. Human-like randomized pauses are applied between
read operations; the runtime still performs zero sends, joins, comments, or
reactions. The review chat receives a no-approval frontier summary when new surfaces are
found; this keeps discovery visible without forcing the operator to approve
analysis of every new link. Import now distinguishes seed-only queued surfaces
from actually scanned surfaces, so subsequent Kaggle runs prioritize unscanned
frontier/monitoring groups instead of restarting from the same head of the seed
list. The server also passes already analyzed `context_url` values into Kaggle
(`ACQ_SEEN_CONTEXT_URLS_JSON`) so the runtime skips repeated comment/message
analysis except for explicit retry/error cases.
For the shared S22 Telegram auth bundle the launcher also keeps a local remote
session marker/cooldown (`ACQ_REMOTE_SESSION_COOLDOWN_SECONDS`, default 600s)
and directly checks the acquisition Kaggle kernel ref before a new live TG run.
This supplements `kaggle_registry` and prevents immediate reuse of the same
Telethon auth key while a previous Kaggle kernel is still stopping or not yet
reflected in the registry.
Telegram discovery is resolver-first, not channel-count-first. Runtime seed rows
for Telegram channels start as `needs_comment_resolve`; each live Kaggle run has
a separate cheap channel-resolution budget
(`ACQ_MAX_TG_CHANNEL_RESOLVES_PER_RUN`) that calls `GetFullChannelRequest` before
spending LLM budget. If a channel has no accessible linked discussion/comments,
the scanner marks it `rejected_no_comments`. If it has a linked discussion, the
channel becomes `resolved_has_linked_discussion` with `linked_discussion_url` /
`linked_discussion_external_id` in the map, and only the linked discussion
surface is queued/scanned for reply opportunities. Review opportunities and
frontier summaries are built only from confirmed replyable surfaces:
Telegram groups/chats/linked discussions and VK communities/discussion threads.
Channel posts may be sampled only for new-link discovery, never as reply
candidates.
Seed rotation treats resolved/rejected Telegram catalog channels as already seen when building the next Kaggle payload, so subsequent runs do not re-add the same `rejected_no_comments` or `resolved_has_linked_discussion` channels from static Telega.in seeds.; the same terminal handles are passed as `ACQ_KNOWN_TERMINAL_TG_HANDLES_JSON`, so links rediscovered inside later scanned messages are skipped instead of being queued again.
Each import stores `surface_import_delta` counters in `acq_discovery_run.stats_json` so the operator can see actual map growth: created surfaces, status changes, newly replyable surfaces, newly rejected surfaces, and newly resolved channels.
The Kaggle config also passes compact Telegram seed metadata (`ACQ_TG_SEED_SURFACES_JSON`) so linked-discussion rows keep their relation/source in the runtime; numeric `t.me/c/<id>` links are resolved as channel peers rather than user ids.
VK discovery is also comment-first: the runtime reads only public wall/comment
methods, requests `filter=all` wall posts, skips posts with zero comments, reads
comments in fresh-first order (`sort=desc`), backs off on VK `too many requests`
errors, skips non-community links such as albums/apps/market/away/personal `id*`
surfaces, and reports `vk_scan` counters in the payload. VK write methods remain
blocked by the static no-send guard. To get beyond official event-source
comments, the server seed payload also includes a small Smartik Kaliningrad
public-catalog set of VK communities (`Подслушано`, `Типичный Калининград`,
`Попутчики`, `ЧС`, `KADAUTO`) as `source=smartik_kaliningrad_catalog`.
Smartik community seeds are prioritized ahead of noisy discovered VK links, and
existing VK album/app/market/away/personal rows are marked
`rejected_non_community` before the next Kaggle seed payload/import.
For these social/community VK surfaces only, a wall post that is itself a human
question/request may be treated as a reply opportunity because the acquisition
action is a public comment under that post; official event-source wall posts are
not eligible for this path.
The VK read-only scanner also checks community discussion boards
(`board.getTopics`/`board.getComments`) and additional VK social/search seeds
found by Kaliningrad queries (`kuda_go_kld`, `club_topplace`, `kuda_dety39`,
`kidsreview_kaliningrad`, `visit.kaliningrad`, route/tourism communities). Board
comments can become Gemma-gated reply opportunities with direct `topic-...`
links.
Telegram bot/service links (`*bot`, `addstickers`, `share`, etc.) are not queued
as monitoring surfaces: discovery is for public groups/chats/comment threads,
not bot accounts. If such a surface was already collected by an older run, the
server marks it `rejected_bot_or_service` before building the next Kaggle seed
payload and on result import.

Discovery opportunity topics are broader than direct event recommendations. The
MVP cheap prefilter only proposes possible comments for the expensive semantic
gate; it must not be treated as the final candidate decision. Kaggle opportunity
acceptance is LLM-first through the Gemma 4 acquisition gate
(`ACQ_ENABLE_LLM_GATE=1`, `ACQ_LLM_MODEL`, default `models/gemma-4-31b-it`,
Google key lane `GOOGLE_API_KEY3`). Gemma calls are also protected by a visible
per-run budget gate (`ACQ_MAX_LLM_CALLS_PER_RUN`, default `80`) and the runtime
reports `llm_gate` / `llm_gate_limits` counters in the Kaggle payload, including
calls used/reserved, blocked limit attempts and estimated input tokens. If the
configured Google key is absent, the runtime fails closed for opportunities
instead of showing regex-owned semantic cards. The review card stores and displays the Gemma checklist, including
whether the comment is a real current/future need and not just a post-event
thank-you/report or local logistics for the currently discussed event/post
(schedule/programme of one day, exact time/address/entrance). Venue-policy
questions addressed to a specific organizer/community (`у вас есть льготы/
скидки/билеты/доступность/пандус/можно с коляской/для инвалидов`) are rejected
as local policy unless the user explicitly asks for a city-wide search/picker of
accessible/free events.

The cheap prefilter also routes recent static-site product hooks from the
2026-07-01 docs update: organizer submission/partnership questions
(`/partnerstvo/`), site search/listing questions (`/poisk/`, `/vystavki/`,
`/populyarnoe/`), event badge/filter questions around Pushkin card,
kids/family, charity, recordings/streams, and free entry, plus trip-route
recommendation contexts from `trip-recomendation` requirements. Route retrieval
intentionally includes loose phrasing such as “что посмотреть за день”, “куда
поехать на выходных из Калининграда”, “маршрут по области”, trains, castles and
coast hints so route opportunities are not missed before Gemma does the semantic
gate. These remain shadow opportunities for review only after Gemma 4 accepts
the checklist; broad semantic acceptance is never owned by regex/keywords.

## Operator map

Use `/acq_map` or the `🗺 Карта групп XLSX` button in `/acq` to download a
clickable spreadsheet of the discovery frontier. The `groups` sheet includes
platform/type/title/url/status, `scan_state`, `reply_policy`, source, topic hint,
last/next scan timestamps, and opportunity counts. This map is for visibility;
newly discovered links do not require manual approval before future analysis.
Manual approval/rejection is reserved for concrete reply/post opportunities.

## Storage ownership note

Discovery statistics/state target is Yandex Managed Service for YDB in serverless
mode, because this workload is small append/upsert stats and YDB has a monthly
free tier for the first 1,000,000 request units and 1 GB storage. Managed
PostgreSQL remains better for heavy ad-hoc relational joins, but would require
paid cluster resources for this MVP. The created target database is
`events-bot-acq-discovery` (`/ru-central1/b1goifscr17duurhullj/etnrao7p6gh6il6b4qv9`).
When `ACQ_YDB_STATS_ENABLED=1` and `ACQ_YDB_ENDPOINT`/`ACQ_YDB_DATABASE` plus
YDB credentials are configured, imports upsert run/surface/opportunity stats into
YDB tables `acq_discovery_runs`, `acq_discovery_surfaces`, and
`acq_discovery_opportunities`.

The current `acq_*` tables in core Fly SQLite are an MVP compatibility layer, not
a final storage requirement. The storage analysis in
[`mvp-discovery.md`](mvp-discovery.md#data-ownership-analysis) recommends keeping
SQLite only while the discovery graph is small and moving discovery-state to a
separate Yandex Managed PostgreSQL store behind a storage abstraction if frontier
growth, report/XLSX exports, or concurrent Kaggle imports make the crawler graph
larger than core bot operational state. Supabase personalization storage remains
out of scope for acquisition crawling/review queues.
