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
`ACQ_MAX_SURFACES_PER_RUN`, without spending LLM budget on link extraction. VK
seeds come from existing `vk_source` monitoring groups and the read-only scanner
tries configured VK token lanes with fallback. Human-like randomized pauses are
applied between read operations; the runtime still performs zero sends, joins,
comments, or reactions. The review chat receives a no-approval frontier summary when new surfaces are
found; this keeps discovery visible without forcing the operator to approve
analysis of every new link.

## Operator map

Use `/acq_map` or the `🗺 Карта групп XLSX` button in `/acq` to download a
clickable spreadsheet of the discovery frontier. The `groups` sheet includes
platform/type/title/url/status, `scan_state`, `reply_policy`, source, topic hint,
last/next scan timestamps, and opportunity counts. This map is for visibility;
newly discovered links do not require manual approval before future analysis.
Manual approval/rejection is reserved for concrete reply/post opportunities.

## Storage ownership note

The current `acq_*` tables in core Fly SQLite are an MVP compatibility layer, not
a final storage requirement. The storage analysis in
[`mvp-discovery.md`](mvp-discovery.md#data-ownership-analysis) recommends keeping
SQLite only while the discovery graph is small and moving discovery-state to a
separate Yandex Managed PostgreSQL store behind a storage abstraction if frontier
growth, report/XLSX exports, or concurrent Kaggle imports make the crawler graph
larger than core bot operational state. Supabase personalization storage remains
out of scope for acquisition crawling/review queues.
