# Subscriber Acquisition Discovery MVP

Status: discovery/MVP design, not implemented
Date: 2026-06-29

## Goal

Launch only the **Discovery** slice first: a scheduled Kaggle job that scans
public Telegram communities/comment threads and VK communities/comment threads,
finds promising places and individual conversation moments for future subscriber
acquisition, and writes them into a manual review queue. The MVP must not
auto-post replies, DM users, harvest user profiles, or run outside the existing
Kaggle/session control framework.

## Initial seed surfaces

Normalize these Telegram URLs into `@username` seeds:

- `https://t.me/tg_kgd`
- `https://t.me/chatkalin`
- `https://t.me/kenig01chat`
- `https://t.me/zhest_kaliningrada`
- `https://t.me/pereezd_v_kaliningrad_legko`


VK is also part of the MVP, but the initial VK allowlist is intentionally empty
until product seeds are selected. MVP support should still include VK in the
schema, queue, report, review UI, and runtime config so an operator can add
starting VK communities later without redesign. Discovery may also discover VK
community links from Telegram/VK evidence, but new VK surfaces remain
`candidate` until reviewed.

For each Telegram seed:

1. Resolve entity and classify it as broadcast channel, supergroup, basic group,
   or inaccessible/private.
2. If it is a broadcast channel, inspect `GetFullChannelRequest(...).full_chat`
   for `linked_chat_id` and scan comments through the linked discussion group /
   channel post replies, not only channel posts.
3. If it is a group/supergroup, scan recent public chat messages directly.
4. Record failures as reviewable diagnostics, not as silent skips.

For VK surfaces, once seeds are provided or discovered and approved, scan public
community wall posts and comments through the existing VK API/token patterns.
VK personal walls are a post-MVP expansion unless explicitly allowlisted: the
MVP should model them as a possible `surface_type`, but not crawl them by
default.

## MVP output

The first production rollout writes **review data only**:

- candidate surfaces discovered from links/forwards/mentions in scanned messages;
- candidate conversation opportunities where a human recommendation could be
  useful;
- evidence snippets and direct links to source channel/post/comment/thread;
- LLM score/rationale and deterministic counters;
- run metrics and limit exhaustion reasons;
- conservative potential-reach estimates for each opportunity and surface;
- a human-readable Telegraph report URL for convenient link-heavy review.

Posting, reply drafting for publication, VK personal-wall monitoring, sticker
packs, and unattended approval are explicitly post-MVP.

## Data ownership decision

Use the existing **core Fly SQLite DB** for MVP data:

- acquisition discovery is tied to bot operations, Telegram/VK source state,
  event inventory, review UI, scheduler runs, and future publication safety;
- it should reuse `ops_run`, `kaggle_run_ledger`, `kaggle_registry`, and the
  existing bot admin UI/session boundaries;
- the personalization Supabase/Postgres DB is for anonymous site telemetry,
  profiles and recommendation caches, not core bot crawling/review queues.

Do **not** create a separate bot or separate database for the MVP. Revisit a
separate worker/bot only if discovery volume or policy boundaries grow enough to
need independent deployment, credentials and operator workflows.

Suggested MVP tables in core SQLite:

- `acquisition_surface`
  - `platform`, `surface_type`, `username_or_id`, `url`, `title`, `about`,
    `linked_surface_id`, `status` (`seed`, `candidate`, `approved`, `rejected`,
    `paused`, `inaccessible`), `priority`, `risk_level`, `last_scanned_at`,
    `next_scan_after`, `scan_cursor_json`, `stats_json`, `reach_estimate_json`,
    `created_by_run_id`.
- `acquisition_surface_edge`
  - `from_surface_id`, `to_surface_id`, `edge_type` (`link`, `forward`,
    `linked_discussion`, `mention`), `evidence_url`, `evidence_message_id`,
    `first_seen_at`, `last_seen_at`, `seen_count`.
- `acquisition_opportunity`
  - `surface_id`, `source_url`, `post_id`, `comment_id`, `message_date`,
    `context_text`, `intent_label`, `confidence`, `score`, `rationale`,
    `potential_views_low`, `potential_views_reason`, `matched_event_ids_json`,
    `suggested_reply_draft`, `status`
    (`new`, `reviewed`, `accepted`, `rejected`, `expired`), `dedupe_key`,
    `run_id`.
- `acquisition_discovery_run`
  - optional run summary keyed to `ops_run.id`; can be deferred if `ops_run`
    metrics/details are enough for MVP; include `telegraph_report_url` when a
    report is published.

Avoid storing unnecessary personal data. For authors, store only what is needed
for dedupe/evidence display (e.g. public display name/username if already visible
in a public comment), and prefer message links over profile snapshots.

## Runtime integration

Create a new Kaggle surface by copying the **Telegram Monitoring** pattern, not
new infrastructure:

- server launcher: `source_parsing/acquisition/service.py` (or
  `subscriber_acquisition/service.py` if a new package is preferred);
- Kaggle runtime: `kaggle/SubscriberAcquisitionDiscovery/` with a script-first
  `subscriber_acquisition_discovery.py` and generated/synced notebook wrapper,
  matching `kaggle/TelegramMonitor/telegram_monitor.py`;
- VK scanner path in the same Kaggle runtime/config, enabled only when VK token
  credentials and approved VK seeds/candidates are present;
- encrypted config/key datasets via the existing split dataset pattern;
- secrets: `TELEGRAM_AUTH_BUNDLE_S22`, `TG_API_ID`, `TG_API_HASH`, a scoped
  Google key lane such as `GOOGLE_API_KEY3` or a new acquisition lane if quota
  needs isolation;
- status dataset with `create_kaggle_run_config(..., kind="subscriber_acquisition_discovery",
  notebook="SubscriberAcquisitionDiscovery", resource_leases=["telegram_session:s22"])`
  whenever Telegram scanning is selected; VK-only future runs may omit the
  Telegram lease;
- `kaggle_status_client.py` progress events with business counters:
  `surfaces_done/surfaces_total`, `telegram_posts_scanned`,
  `vk_posts_scanned`, `comments_scanned`, `candidate_surfaces_found`,
  `opportunities_found`, `llm_calls`, `budget_remaining`,
  `telegraph_report_url`;
- `kaggle_registry` registration/recovery and `remote_telegram_session` guard.

Add the new job type to the remote Telegram session guard so it cannot overlap
`tg_monitoring`, `guide_monitoring`, story publishing that uses S22, or any
future S22 Kaggle run.

## Scheduler / idle-time behavior

The job is scheduled daily but must be opportunistic:

- add it to the heavy job set so it never overlaps current heavy Kaggle/LLM jobs;
- before pushing Kaggle, check `remote_telegram_session` and relevant
  `kaggle_resource_lease` state; if busy, close the run as `skipped` with
  `remote_telegram_session_busy`, not as success;
- run in a late/low-priority window after Telegram Monitoring, or as a watchdog
  catch-up when the heavy-job lock is free;
- keep a hard per-run wall-clock budget and carry remaining frontier items into
  `next_scan_after` instead of trying to finish the graph.

Suggested first defaults:

- once daily;
- max 5 seed/root surfaces per run across Telegram and VK;
- max 10 recent posts per channel/group/community;
- max 15 comments per post/thread;
- max depth 1 for graph expansion in the first week, then depth 2 only for
  approved/promising surfaces;
- max 25 new surface candidates and max 30 opportunity candidates per run;
- max 80 LLM classification calls per run until quota evidence says otherwise;
- publish one Telegraph review report per completed non-empty run;
- stop immediately on FloodWait above the existing safe threshold and persist
  resume state.


## Conservative potential-reach scoring

Every opportunity should be prioritized by a realistic lower-bound estimate of
how many additional people may still see a useful reply. This is a product
priority signal, not a vanity metric. Store both the numeric estimate and a short
reason string.

Use conservative inputs only:

- Telegram channel comments: start from recent post views for that channel/post
  when available, then multiply by a small comment-thread read-through factor
  and decay by post age. Prefer p10/median recent views over max views.
- Telegram group chats: estimate from recent active-message/read signals if
  available; otherwise keep `potential_views_low` small and confidence low.
- VK communities: use public post views/comment counts when available, recent
  community engagement baselines, and a comment-thread read-through factor. Do
  not assume all subscribers will see a comment reply.
- If metrics are missing or ambiguous, return a bounded low estimate rather than
  guessing.

Suggested first scoring fields:

```json
{
  "potential_views_low": 12,
  "potential_views_confidence": "low|medium|high",
  "reach_basis": "tg_post_views_p10|vk_post_views|group_activity|unknown",
  "age_decay_applied": true,
  "priority_score": 0.0
}
```

Final priority should combine reach, semantic fit, anti-spam risk, surface
quality, and freshness. High reach must not override high spam/risk.

## Discovery algorithm

### Phase 0 — preflight

- Load enabled `acquisition_surface` seeds/candidates due for scan.
- Apply global budgets: Telegram calls, messages/comments, LLM requests/tokens,
  wall-clock time, and new frontier candidates.
- Emit `preflight_ok` with budgets and selected surfaces.

### Phase 1 — surface scan

For each surface:

- fetch metadata (`title`, `about`, public username, source type, linked
  discussion group if any);
- scan recent posts/messages within cursor and day window;
- for broadcast channels with comments enabled, scan comments/replies for recent
  posts;
- for VK communities, scan public wall posts and comments for recent posts;
- extract links/mentions/forwards to Telegram channels/groups and VK communities
  as candidate surfaces;
- record deterministic engagement features: comment count, recent message count,
  median views where available, link frequency, local/Kaliningrad terms.

### Phase 2 — cheap candidate filtering

Use deterministic filters only as a narrow guardrail:

- discard inaccessible/private/non-public surfaces;
- deprioritize obvious spam, crypto, adult, unrelated geography, inactive rooms;
- dedupe by normalized platform+username/id and by source URL/message id;
- never make the broad semantic decision with regex alone.

### Phase 3 — LLM surface triage

Use Gemma through the existing Google AI client/rate limiter. Prompt output should
be strict JSON:

```json
{
  "monitoring_fit": "yes|maybe|no",
  "surface_category": "local_chat|local_news|relocation|events|venue|spam|unrelated|unknown",
  "audience_fit": 0,
  "opportunity_likelihood": 0,
  "risk_level": "low|medium|high",
  "rationale": "short grounded explanation",
  "recommended_action": "approve_candidate|keep_for_later|reject"
}
```

Gemma 4 / 26B-class model is preferred for this stage; Lite is acceptable only as
a supplementary fallback/probe and must be marked as lower-confidence in stored
metadata.

### Phase 4 — opportunity triage

For public comments/messages, ask the LLM whether the message is a sparse,
contextual chance to recommend an event. Output should include:

- intent label (`asking_where_to_go`, `looking_for_children_event`,
  `tourist_question`, `relocation_local_tip`, `event_comparison`, `other`);
- confidence and anti-spam risk;
- direct evidence quote/snippet;
- optional candidate event query terms.

Reply text generation is optional in MVP and should remain `draft_only`; no
publication action is available from the runtime.

### Phase 5 — server import/review

Server imports the Kaggle JSON into core SQLite and shows review UI:

- `/acq` or `/subs` root menu;
- surfaces: approve/reject/pause, show stats and evidence;
- opportunities: direct links to post/comment, context, score/rationale,
  matched event links if any;
- run reports: budget usage, skipped/busy status, errors, top new candidates.

## Telegraph review report

For operator convenience, each useful discovery run should publish a Telegraph
page and store its URL in `ops_run.details_json` and/or
`acquisition_discovery_run.telegraph_report_url`. The report is the primary
link-heavy artifact for external/human analysis.

Report structure:

- run summary, budgets, scanned surfaces, skipped/busy diagnostics;
- per-public monitoring potential: surface type, topic fit, recent activity,
  conservative reach estimate, event-question likelihood, native-event-topic
  opportunities, risks, recommended action;
- candidate opportunities with direct Telegram/VK links to posts/comments,
  evidence snippets, LLM rationale, potential views, and matched event ideas;
- discovered surfaces with evidence links and why they should be reviewed or
  rejected;
- sticker-strategy observations where available.

The page should avoid sensitive/user-profile dumps: link to public evidence
instead of copying large comment histories.

## Sticker strategy research

Sticker-based acquisition is not automatic posting in the MVP, but Discovery
should collect evidence for a separate strategy: where a human/operator could
quietly place relevant stickers whose pack/title points to the announcements
channel.

Analyze per Telegram chat/thread:

- whether stickers are commonly used by participants;
- whether the chat technically accepts stickers from ordinary users;
- whether recent sticker replies appear tolerated or trigger negative reactions;
- which topics/moments would make a sticker more natural than a text reply;
- anti-spam risk and expected visibility.

The MVP should only report sticker suitability; creating sticker packs and
placing stickers are follow-up tasks requiring separate content/safety approval.

## E2E / Codex loop

The MVP should support a live discovery E2E loop similar to Telegram Monitoring:

1. Run a small forced discovery job with the five seed surfaces and tiny budgets.
2. Inspect Telegram UI report and imported review rows.
3. Validate that at least one comments-enabled channel is scanned through
   comments, not only channel posts.
4. Confirm no outbound messages were sent.
5. Review false positives/false negatives and adjust prompt/schema/limits.
6. Keep run artifacts under `artifacts/codex/subscriber-acquisition-discovery/`.

## Acceptance criteria for MVP rollout

- The job can run on Kaggle using the existing encrypted dataset + status
  framework and S22 lease.
- It skips cleanly when remote Telegram session or heavy-job lock is busy.
- It scans the initial Telegram seed list, linked discussion/comment threads
  where available, and approved VK communities once configured.
- It writes reviewable surface candidates and opportunity candidates with direct
  evidence links and conservative potential-view estimates.
- It publishes a Telegraph review report for non-empty runs.
- It never posts, replies, DMs, joins private groups, or stores unnecessary user
  profile data.
- Operator can approve/reject candidate surfaces from bot UI.
- Sticker suitability is reported as research only; no sticker is sent by the
  runtime.
- `ops_run`/status ledger show business counters and terminal state.
- Documentation and changelog are updated when implementation lands.

## Estimated implementation scope

MVP implementation is medium-large but bounded because it reuses existing
Telegram Monitoring infrastructure.

- Data model + migrations + models: 0.5-1 day.
- Kaggle runtime skeleton and scanning logic: 1.5-2.5 days.
- LLM prompts/schema/rate-limit integration: 1-1.5 days.
- Server launcher/import/recovery/status: 1-1.5 days.
- Review UI `/acq` and reports: 1-2 days.
- Tests + live E2E loop + prompt tuning: 1-2 days.

Total: roughly **7-11 focused engineering days** for a safe shadow-mode MVP
with Telegram + VK-ready schema/runtime/reporting, plus 1-3 days of live
discovery calibration after first production runs. If VK API/comment scanning is
implemented fully in the first iteration rather than schema-ready + seed-ready,
reserve the upper end of the range.
