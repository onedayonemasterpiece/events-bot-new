# Subscriber Acquisition Discovery MVP

Status: shadow-mode MVP scaffolding implemented; live scanner calibration pending
Date: 2026-07-04

## Implemented shadow-mode slice

The first code slice now provides the server-side review loop and safe import
contract:

- SQLite/SQLModel tables: `acq_discovery_run`, `acq_surface`, `acq_link_target`,
  `acq_opportunity`, `acq_review_feedback`;
- `/acq`, `/acq_run`, `/acq_queue`, `/acq_surfaces`, `/acq_surface_add`,
  `/acq_report`, `/acq_export` superadmin commands;
- candidate surface review cards with approve/reject/pause actions persisted as
  `acq_review_feedback` rows;
- review cards capped by `ACQ_REVIEW_GROUP_MAX_CARDS_PER_RUN` with buttons
  `✅ Да`, `❌ Нет`, `🕒 Потом`, `🔗 Контекст`, `🎯 Куда`;
- `/acq_run` imports an explicit `ACQ_DISCOVERY_RESULTS_PATH` or
  `ACQ_DISCOVERY_FIXTURE_PATH`; when no JSON is configured it runs the shadow
  runtime through the existing Kaggle infrastructure by default
  (`ACQ_DISCOVERY_RUNNER=kaggle`) with encrypted config/key datasets,
  `kaggle_status` files, `kaggle_registry`, polling, output download, and import
  of `acq_discovery_result.json`; `ACQ_DISCOVERY_RUNNER=local` is an explicit
  dev/test fallback only; sample fixture import requires `ACQ_DISCOVERY_USE_SAMPLE=1`
  so production does not review fake evidence;
- review-card display events (`shown`), button feedback, and reply comments are persisted to `acq_review_feedback`;
- server import keeps seed-only and budget-queued surfaces separate from actually scanned surfaces, so `last_scan_at` / `next_scan_after` are updated only for touched surfaces; linked discussion groups found while resolving a channel but skipped by the replyable budget stay pending for the next Kaggle run instead of being delayed as already scanned;
- seed collection prioritizes pending discovered/linked Telegram replyable surfaces before static catalog seed groups, so each follow-up Kaggle run spends its small replyable budget on newly found comment groups first;
- seed collection includes enabled Telegram Monitoring `telegram_source` publics
  as `source=tg_monitoring` / `needs_comment_resolve`, so existing event/city
  announcement channels are checked for linked discussions instead of being
  absent from the acquisition frontier;
- importer merge policy treats the previous Kaggle resolver result as durable state: later seed-only placeholders for the same `external_id` must not overwrite `linked_discussion` type/source/title/topic metadata or the private Telegram access hash needed for numeric `t.me/c/<id>` rescans;
- the Kaggle runtime resolves numeric linked discussions with stored access metadata via Telethon `InputPeerChannel`, and seed-only diagnostic rows apply the stored `linked_discussion` metadata instead of falling back to `unknown_public`;
- VK allowlist means “safe to scan read-only”, not “human-approved to reply”: scanned VK communities are marked `comments_available` when wall comments or discussion-board comments are readable, `rejected_no_comments` when the bounded scan finds no replyable comments/boards, and `rejected_inaccessible` when the wall and boards are not readable with configured tokens;
- seed collection excludes not-yet-due scanned surfaces (`next_scan_after` in the future) from the next Kaggle payload and demotes legacy auto-approved VK prototype rows back to `candidate` unless they carry explicit human-review reply policy, so small per-run budgets keep moving to new/pending surfaces;
- each run reports `opportunity_screening` counters (`texts_screened`, matched trip/event/search/partner/badge buckets, venue-policy rejects, no-intent texts) alongside `llm_gate` counters so a zero-candidate run is explainable without weakening the candidate gate;
- seed payload includes Kaliningrad Telega.in regional-card channels/chats as `source=telega_in`, giving discovery enough new TG surfaces before relying on organic frontier links;
- optional YDB serverless stats sink writes run/surface/opportunity stats outside local SQLite when `ACQ_YDB_STATS_ENABLED=1`;
- Telegram channels without accessible linked discussion/comments are marked
  `rejected_no_comments` after scan, because reply acquisition requires a
  confirmed comment/chat surface; channels with comments are scanned through the
  linked discussion, while groups/chats are scanned directly;
- Telegram scanning is resolver-first: channel/frontier links are not treated as
  useful monitoring surfaces until commentability is resolved. Channel rows
  start as `needs_comment_resolve`; each Kaggle run uses a separate
  `ACQ_MAX_TG_CHANNEL_RESOLVES_PER_RUN` budget to check `linked_chat_id`.
  Channels with linked discussions become `resolved_has_linked_discussion` and
  store the linked chat URL/external id for XLSX/report navigation; channels
  without linked comments become `rejected_no_comments`. Candidate/review
  opportunities and frontier-summary cards are restricted to replyable
  surfaces: groups, chats, linked discussions, VK community/profile-wall
  comments and VK discussion-board topics.
- live Telegram runs use the standard `remote_telegram_session` registry guard
  plus an acquisition-specific local marker/cooldown and direct kernel-ref check
  before reusing S22, so a just-deleted/timeout Kaggle kernel cannot immediately
  start a second Telethon client on the same auth key;
- VK read-only scans prefer surfaces that are still `seed_only`, pass explicit
  VK budgets into Kaggle config, skip wall posts with no comments, fetch newest
  comments first (`sort=desc`), skip non-discovery VK links
  (`album*`/`app*`/`market*`/`away.php`) but allow explicit profile walls
  (`id*`, positive-owner `wall123_...`) as profile candidates, and expose
  `vk_scan` counters for posts/comments inspected, discovered profile-wall
  candidates and VK rate-limit backoffs; existing non-discovery VK rows are
  marked `rejected_non_community`, and Smartik Kaliningrad community seeds are
  prioritized before noisy discovered links;
- on social/community VK surfaces only, a wall post that is itself a human
  question/request may be passed to Gemma as a reply opportunity because the
  reply action would be a public comment under that post; official event-source
  wall posts remain excluded from this path;
- VK discussion boards are scanned read-only through `board.getTopics` and
  `board.getComments` with per-request comment count capped at the VK API
  limit of 100; matching board comments are linked with `topic-...` evidence
  URLs and still require Gemma acceptance before review;
- the seed payload includes extra VK social/search communities found by
  Kaliningrad discovery queries (`kuda_go_kld`, `club_topplace`, `kuda_dety39`,
  `kidsreview_kaliningrad`, `visit.kaliningrad`, route/tourism publics) so the
  crawler reaches places where people ask “куда сходить/съездить/что
  посмотреть”, not only official event-source comment threads;
- Telegram link discovery skips bot/service handles (`*bot`, `addstickers`,
  `share`, etc.) so the frontier only grows through public groups, chats,
  channels with comments, and VK communities; existing bot/service rows are
  marked `rejected_bot_or_service` before the next seed payload/import;
- Gemma 4 calls are visible and bounded: `ACQ_MAX_LLM_CALLS_PER_RUN` caps
  semantic checklist calls, and each payload reports `llm_gate` and
  `llm_gate_limits` counters so operator/status review can see how much of the
  acquisition Gemma budget was spent or blocked;
- Telegraph report renderer/publisher and JSON schema for Kaggle output import;
- conservative reach scoring, link-target selection, sticker-fit observation,
  and no-send/VK-read-only guard helpers;
- `subscriber_acquisition_discovery` is registered as a heavy Kaggle job type and
  as a scoped remote Telegram session consumer; `/acq_run` checks the remote
  Telegram session-busy guard for the selected acquisition discovery auth bundle
  before any live Telegram scan can start.

Live Telegram/VK scanning remains constrained to the Kaggle runtime path and must
run in shadow mode first. The initial `kaggle/SubscriberAcquisitionDiscovery/`
runtime writes an importable `acq_discovery_result.json` with seed surfaces and
zero outbound sends. When `ACQ_ENABLE_LIVE_TG_SCAN=1` and
`TELEGRAM_AUTH_BUNDLE_DISCOVERY` is mounted, it performs a bounded read-only
Telegram shadow scan of public seed surfaces, linked discussion chats where
resolvable, public links discovered in messages, context-chain comment/post
collection, vector retrieval and sticker-fit observations. `/acq_run` also
seeds the runtime from existing
`vk_source` monitoring groups. VK is schema/config/report-ready and has a
bounded read-only `wall.get`/`wall.getComments` scanner path, but comment scans
are active only when explicit allowlisted VK communities are provided; no VK
write methods are available in the runtime.

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


VK is also part of the MVP. The current starting VK seed set is imported from
existing `vk_source` monitoring communities so the operator does not have to
manually curate the first list. Discovery may also find VK community links from
Telegram/VK evidence; new VK surfaces are stored as `candidate` and should be
scanned in shadow mode before any reply/post decision.
For discovery coverage beyond official event-source comments, the runtime seed
payload may additionally include public Kaliningrad VK communities found in
catalog/research sources. The current MVP adds Smartik Kaliningrad catalog seeds
for `Подслушано в Калининграде`, `Типичный Калининград`, `Попутчики`,
`ЧС - Калининград и область`, and `KADAUTO`; these are still shadow candidates
and only concrete reply opportunities get operator calibration cards.

For each Telegram seed:

1. Resolve entity and classify it as broadcast channel, supergroup, basic group,
   or inaccessible/private.
2. If it is a broadcast channel, inspect `GetFullChannelRequest(...).full_chat`
   for `linked_chat_id` and scan comments through the linked discussion group /
   channel post replies, not only channel posts.
3. If it is a group/supergroup, scan recent public chat messages directly.
4. Record failures as reviewable diagnostics, not as silent skips.

For VK surfaces, once seeds are provided or discovered and approved, scan public
community/profile wall posts and comments through the existing VK API/token
patterns. Personal walls enter only as explicit `id*`/positive-wall profile
candidates discovered from public comments/post authors or optional tiny
member-list samples; vanity personal names are not auto-classified as profiles.


### Region gate

Discovery remains scoped to Kaliningrad Oblast. Link extraction is deterministic,
but obvious out-of-region Telegram/VK surfaces must not inflate the frontier. The
runtime marks such surfaces as `rejected_out_of_region` and does not enqueue them
for same-run or future scanning. Known example: `https://t.me/visitNavahrudak`
(Novogrudok/Navahrudak) is outside the target region.

### Non-event acquisition hooks

The discovery scanner should not only look for generic “where to go” event
questions. It should also surface review candidates for product hooks documented
in the recent static-site work:

- `/poisk/`, `/vystavki/`, `/populyarnoe/`: search/listing/navigation questions;
- `/partnerstvo/`: organizer and information-partnership submission questions;
- event-token medallions / future filters: Pushkin card, kids/family, charity,
  video recording/streaming, free-entry questions;
- trip recommendations: one-day route/where-to-go-outside-Kaliningrad contexts
  from `docs/features/trip-recomendation/requirements.md`, where the correct
  target is a concrete route, not the general announcements public. Retrieval
  must include looser natural questions (“что посмотреть за день”, “куда
  поехать на выходных из Калининграда”, “маршрут по области”, trains/castles/
  coast hints), while Gemma remains responsible for final semantic acceptance.

Specific-place policy questions are not acquisition candidates. For example,
“у вас есть льготы/скидки/билеты/доступность/пандус/можно с коляской/для
инвалидов?” in a venue/community thread is local venue policy and should be
rejected unless the person explicitly asks for a city-wide search, filter or
selection of accessible/free events.

These phrases are intent catalog examples for vector retrieval, not regex
prefilters. Final classification and reply wording remain LLM-first before any
public response.


### Comment semantic retrieval funnel

`acq_comment_semantic_retrieval.v1` is the next Discovery stage. It is a cheap
semantic-retrieval layer between comment collection and the Gemma gate, not a
replacement for the gate. The detailed contract, benchmark matrix and report
requirements are in
[`comment-semantic-retrieval.md`](comment-semantic-retrieval.md).

Required funnel:

```text
replyable TG/VK comments/messages
  -> light preprocessing
  -> local Kaggle embeddings
  -> score against route/event/site/organizer/badge intent ladders
  -> build surface semantic profiles and ranked comment candidates
  -> send only top candidates to Gemma/LLM acceptance gate
  -> accepted candidates become normal `acq_opportunity` rows
```

The stage must make route/POI discovery first-class. Routes are not implemented
yet, so the current MVP goal is to find where route/POI questions happen and
which surfaces should be monitored later for route replies. Route recommendation
contexts from `docs/features/trip-recomendation/requirements.md` map to
`trip_route_poi_recommendation` and should currently carry `route_target_status`
as `route_needed` or `unknown`; once route cards/posts exist, this can expand to
`matched_existing` or `published_post_found`. They should not silently fall back
to the generic events announcement channel. `https://t.me/vKalinigrad_recomendations`
is a golden calibration group for this lane.

Benchmark scope:

- compare `intfloat/multilingual-e5-base` and `BAAI/bge-m3` on the exact same
  comments, preprocessing, intent sets and scoring policies;
- dry-run up to 20 surfaces / 5,000 comments before any larger run;
- report speed, memory, score distributions, funnel reduction, candidate quality
  sample and model disagreement examples;
- do not use absolute cosine thresholds without first inspecting distributions;
- do not call external LLM for all comments.

Storage rule: bulk retrieval artifacts are written as Kaggle artifacts first;
sanitized summaries may go to YDB when enabled; core Fly SQLite receives only
small review-compatible summaries/top opportunities/pointers. Full comments,
full per-comment score tables and embeddings must not be stored in the
operational SQLite DB.


### Organizer clarification acquisition

Treat organizer-owned publics as a distinct acquisition target class, not as the
same thing as answering generic “куда сходить?” comments. In an organizer public
(for example a venue/park/festival page such as `vagonka39`), Discovery may
surface a review candidate to ask the organizer for missing practical details
about their own event.

This subfeature is still **shadow/review-only** until a separate publication
workflow is approved. The success metric is useful public interaction that
improves event information and exposes the profile/service naturally; it is not
immediate event-recommendation conversion.

#### Surface acquisition rules

Use the existing monitoring/import catalogs as the first frontier, but do not
stop there:

- **Telegram:** start from existing `telegram_source` / Telegram Monitoring
  channels and from channel handles discovered in posts, comments, forwards,
  organizer websites/about text, Telega.in regional cards, and manual
  `/acq_surface_add` rows. For this subfeature, Telegram discovery should look
  for **channels**, not ordinary chats. A channel becomes usable only after
  resolver proof that it has an accessible linked discussion **and** a recent
  non-empty human comment/reply surface. Generic chats/groups can stay in the
  broader acquisition scanner, but they are not enough to classify an
  organizer-owned event channel.
- **VK:** start from existing `vk_source` community rows and the acquisition VK
  social/catalog seeds, then expand through group links found in TG about text,
  VK post text, source event URLs and organizer profile cross-links. Explicit
  public profile-wall candidates (`id*`/positive wall links) may be scanned
  read-only under the current bounded profile policy; broad user/follower
  harvesting is still out of scope.
- **Event corpus backfill:** when an event has a source URL or known organizer
  channel/community that is not yet an acquisition surface, enqueue that
  organizer surface for commentability scanning. If the surface cannot be matched
  to a public TG channel/VK community, keep only a diagnostic row.
- **Catalog/search expansion:** add small, bounded local searches for museums,
  theatres, castles/forts, parks, festivals, concert halls, libraries, clubs,
  galleries and municipal culture offices. These searches are only for finding
  candidate surfaces; organizer ownership and event relevance still require the
  later gates.

A production inventory probe on 2026-07-03 found 52 enabled Telegram Monitoring
sources and 119 VK community `vk_source` rows. Heuristic titles suggest many of
them are organizer/venue-like (museums, theatres, parks, festivals, halls), but
current post metrics prove open Telegram comment counts for only a small subset
(`koihm`, `agropark39` at the time of the probe), while VK metrics show comments
on many more community posts. Treat this as calibration evidence, not a static
allowlist: the runtime must prove commentability from the live linked
discussion/comment surface before surfacing a public-question candidate.

#### Eligibility pipeline

1. **Organizer/post detection:** classify whether the surface is controlled by an
   event organizer, venue, festival, park, museum, theatre, club, cultural space
   or municipal culture office, and whether the post is about one concrete
   current/future event. This can use cheap metadata hints, but final eligibility
   must be LLM-owned.
2. **Reply-surface proof:** for Telegram channels, require `linked_chat_id` plus
   at least one recent human `reply_to` comment in the linked discussion; copied
   channel-post mirrors, forwarded posts, empty discussions and comments disabled
   after the post do not count. For VK, require readable wall comments or
   discussion-board comments for the concrete post/topic.
3. **Event match:** use the existing event retrieval/vector-search capability to
   find whether the post corresponds to an event already known in our event
   corpus. If no confident match exists, keep only a diagnostic/research row
   (`organizer_event_unmatched`), not a public-question candidate.
4. **Ideal-event diff:** compare the matched event against an explicit ideal
   event-card checklist: exact date/time, venue/address/meeting point,
   price/ticket status and ticket URL, age limit, duration, registration/entry
   rules, capacity/sold-out status, accessibility, kids/family constraints,
   outdoor/weather/cancellation/format notes where relevant, and
   organizer-specific nuances needed for a complete public event page.
5. **Question-pattern mining:** analyze real questions already asked in the same
   or similar organizer threads to learn realistic clarification needs. This
   builds a semantic library of question types and evidence, **not** a phrase
   template bank. It is used to avoid unnatural generic prompts, not to bypass
   semantic validation.
6. **Same-thread dedupe:** before creating a review opportunity, scan the current
   thread/post/topic for already answered or already asked equivalent questions.
   Use deterministic identity and embeddings for cheap recall, then an LLM check
   for ambiguous near-duplicates. Suppress if another participant or our account
   has already asked materially the same thing.
7. **LLM final gate:** after the cheap stages, Gemma validates whether the
   clarification is useful, non-spammy, answerable by the organizer, contextually
   timely, and suitable for a normal human public question. It should reject
   questions that exist only to manufacture engagement.

Important routing pitfall: generic third-party recommendation replies and
organizer clarification are different LLM-gated routes. Do not use a
deterministic local-logistics rejection as a prefilter before vector retrieval.
A local question such as age limit, rain plan, entry rules or accessibility can
be eligible only when the context chain shows an organizer-owned event post and
the organizer-mode LLM gate accepts it; the same text in a generic community
remains a rejection.

#### Question library and classification

The library should store semantic question classes and evidence, not final
wording. Suggested item shape:

```json
{
  "question_type": "age_limit|duration|registration_entry|ticket_status|price_includes|venue_meeting_point|accessibility|kids_family|weather_rain_plan|capacity_sold_out|format_language|stream_recording|other",
  "semantic_need": "what factual gap this question resolves",
  "eligible_when": ["matched event lacks this field", "post/thread hints that users need it", "organizer can reasonably answer"],
  "reject_when": ["already stated in post/image/comments", "event is past/expired", "field is irrelevant for this event type", "question would look like advertising or interrogation"],
  "event_fields": ["fields/gaps touched"],
  "evidence_examples": [{"platform": "tg|vk", "surface": "public handle", "context_url": "public URL", "snippet": "short public evidence"}],
  "cluster_model": "model id used for mining",
  "last_reviewed_at": "ISO timestamp"
}
```

First classification set:

- `age_limit`: ask only when age/audience constraints matter and are missing or
  contradictory; skip if it is obvious from the venue/event type or already in a
  poster.
- `duration`: for timed performances, walks, master classes and routes where the
  length affects attendance; skip for open-ended exhibitions/markets unless the
  post itself implies a session.
- `registration_entry`: when arrival, pre-registration, pass list, QR/ticket
  check or free-entry rules are unclear; skip if the post already has a complete
  registration link and rules.
- `ticket_status` / `price_includes`: when the event page lacks price, ticket
  availability, included services or booking link; skip if the post only has
  broad sales copy and no actionable missing fact.
- `venue_meeting_point`: for outdoor routes, parks, festivals, castles or large
  venues where the exact entrance/meeting point is missing; skip if asking would
  duplicate an address already shown.
- `accessibility` / `kids_family`: only when the event audience, terrain,
  stroller/wheelchair access, child participation or family constraints are
  genuinely relevant. Do not ask every organizer about accessibility by default.
- `weather_rain_plan`: for outdoor/festival/route events with visible weather
  dependency; skip indoor events and posts with explicit cancellation/weather
  rules.
- `capacity_sold_out`: when comments or ticket state suggest limited seats or
  possible sell-out; skip if ticket status is stable and recent.
- `format_language` / `stream_recording`: for lectures, tours, performances and
  hybrid events where language, online stream or recording materially changes
  attendance.

A candidate can carry several gaps, but the writer should ask **one** focused
question unless the LLM reviewer explicitly accepts a short two-part question as
more natural than two separate comments.

#### LLM-only question drafting and review

Final question text is owned by the LLM. Deterministic code may assemble facts,
URLs, diff fields, candidate type labels and safety constraints into JSON, but
must not concatenate phrase templates into the public question. No deterministic
“question template”, synonym table, regex rewrite, or fixed prefix/suffix is
allowed for `draft_question` / `published_question`.

These organizer-specific stages should use the shared `GoogleAIClient` /
LLM-gateway path instead of direct `google.genai` calls, so quota reservations,
key-lane logging, provider retries, structured-output handling and
model-specific response cleanup stay consistent with the rest of the repo. Keep
the schemas provider-simple: no `anyOf`, no nullable unions, no
`additionalProperties`; represent absence as empty strings/arrays and still
prompt for strict JSON.

The public-question writer is **Gemini Lite**: pin
`organizer_question.draft.v1` to `gemini-3.1-flash-lite` rather than a moving
`*-latest` alias. The high-confidence accept/reject decision stays independent:
the reviewer model may be a Gemma 4 26B-class lane only after a compact-schema
smoke/contract test; otherwise keep the proven 31B lane and still record it as a
separate reviewer stage/version. The reviewer must not reuse the writer response
as approval and must not publish its own wording.

Recommended generation chain and ownership:

| Stage | What it does | Owner/model | Public wording allowed? |
| --- | --- | --- | --- |
| `organizer_context.classify.v1` | Confirms organizer-owned concrete-event context and selects one useful gap/type or `do_not_ask` from the evidence pack. | LLM, default `models/gemma-4-31b-it` through `GoogleAIClient`; deterministic code only prepares facts and cheap gates. | No |
| Operator formatting notes | Captures platform/account-specific style additions before drafting: greeting/no-greeting preference, emoji policy, first-person stance, maximum length, whether links are forbidden/allowed by explicit flag, and any current operator wording constraints. | Human/operator config stored as structured constraints; deterministic code only passes it through. | No |
| `organizer_question.draft.v1` | Writes exactly one natural Russian public comment question from evidence, selected gap and operator formatting notes. | LLM writer, pinned `gemini-3.1-flash-lite` through `GoogleAIClient`; this is the **only** stage allowed to create `draft_question`. | Yes |
| `organizer_question.prepublish_review.v1` | Reviews the draft against source evidence, thread context and duplicate evidence. | Independent LLM reviewer. Prefer a different Gemma 4 lane/model such as `models/gemma-4-26b-a4b-it` after compact-schema smoke; otherwise use a separate `models/gemma-4-31b-it` call/stage with reviewer prompt/version. | No; pass/fail or revision guidance only |
| Human review card | Shows evidence, matched event, gaps, draft, model ids and reviewer verdict to the operator. | Human operator. | Human may approve/reject; any human edit must go back through LLM review before publication |
| `organizer_question.postpublish_review.v1` | After post-MVP publication, reviews the exact visible posted text and surrounding thread URL/evidence. | Independent LLM reviewer, same independence rule as prepublish review. | No |

The writer returns strict JSON with `draft_question`, `question_type`,
`grounded_gap`, `tone`, `applied_formatting_notes`, `link_intent` and
`why_this_is_natural`; no link by default, no mention of internal systems, no
“we are updating a database” framing and no hidden advertising. For the first
live-publication slice, treat organizer clarification questions as **linkless by
default**; allow a link only when the operator formatting profile explicitly
enables it for that candidate and the reviewer accepts it as natural and
non-promotional. The reviewer sees the source evidence, operator formatting
constraints and draft/published text, not the writer's private rationale, and
grades naturalness,
appropriateness, clarity, answerability, context fit, duplicate risk,
spam/self-promo risk, factual grounding, and whether a normal participant could
plausibly ask it.

Suggested reviewer schema:

```json
{
  "approve": true,
  "scores": {
    "naturalness": 0.0,
    "appropriateness": 0.0,
    "clarity": 0.0,
    "answerability": 0.0,
    "grounding": 0.0,
    "non_spam": 0.0,
    "duplicate_safety": 0.0
  },
  "fail_reasons": ["too_generic|already_answered|sounds_like_bot|not_answerable|wrong_event|too_many_questions|hidden_ad|expired_event"],
  "rewrite_allowed": false,
  "rationale": "short grounded explanation"
}
```

Fail closed unless all critical scores are high (start with `>=0.85`) and
`fail_reasons` is empty. If the reviewer suggests a rewrite, send the revised
semantic constraints back to the writer LLM; do not let reviewer text become the
public question without a fresh writer + reviewer pass.

#### Publication identity, approval and rate defaults

MVP remains shadow/review-only, so publication identity does not block discovery
launch. For the later live slice, the identity defaults are already decided:

- VK organizer-clarification questions use the currently configured VK
  token/account lane already present in the project.
- Telegram organizer-clarification questions use the currently configured
  Telegram session lane. Keep existing session-boundary rules: do not borrow the
  local E2E session for remote/Kaggle publication and do not run the same session
  concurrently in two places.
- The operator can add or update platform-specific formatting notes before the
  Gemini Lite writer call. If the operator edits the generated text after the
  writer, that exact edited text must run through
  `organizer_question.prepublish_review.v1` again before any send.
- Approve/reject ownership for MVP and the first live trial is the existing
  superadmin/operator review card flow. Automation may queue a candidate, but it
  cannot publish unless the card has an explicit human approval and a passing
  independent reviewer verdict.

Conservative first-live defaults if no stricter config is provided:

- global cap: `3` organizer clarification questions per calendar day;
- per platform cap: `2` per day for Telegram and `2` per day for VK;
- per surface/community cap: `1` question per `14` days;
- per source post/thread cap: at most one accepted question ever, unless a human
  explicitly resets the thread after new organizer context appears;
- per matched event cap: `1` organizer-clarification question per platform per
  `7` days;
- quiet hours: do not send automatically outside the configured local daytime
  window; manual approvals outside the window are queued unless the operator
  explicitly overrides.

These are safety defaults, not product targets: after the first calibrated live
batch, tune them from reviewer rejects, organizer replies, complaints/deletes
and surface-level mute/ban signals.

#### Opportunity/report fields

Organizer clarification candidates should be distinguishable from generic
recommendation opportunities, for example:

```json
{
  "opportunity_type": "organizer_clarification",
  "surface_role": "organizer_channel|organizer_vk_community",
  "source_post_url": "public organizer post URL",
  "reply_surface_url": "linked discussion/comment/topic URL",
  "commentability_proof": {"human_comments_seen": 1, "oldest_recent_comment_at": "ISO", "copied_channel_posts_excluded": true},
  "matched_event": {"event_id": 123, "confidence": 0.0, "retrieval_basis": "vector|source_url|title_date_venue"},
  "ideal_card_gaps": ["age_limit", "duration"],
  "selected_question_type": "duration",
  "same_thread_dedupe": {"status": "clear|duplicate|ambiguous", "checked_messages": 0, "nearest_existing_question": "short snippet"},
  "operator_formatting": {"profile_id": "default|platform-specific id", "notes": ["short structured note"], "links_allowed": false},
  "question_generation": {"writer_model": "gemini-3.1-flash-lite", "draft_question": "LLM-written text only", "link_intent": "none|operator_allowed"},
  "question_review": {"reviewer_model": "model id", "approve": true, "scores": {}},
  "publication_identity": {"platform_account": "current configured VK token or Telegram session lane", "human_approved_by": "operator id or empty"},
  "published_question_review": null
}
```

Run/report counters should include: organizer surfaces scanned, TG channels with
linked discussion, TG channels with **non-empty human comments**, VK communities
with readable comments/boards, organizer posts matched to known events,
unmatched diagnostics, ideal-card gaps by type, duplicates suppressed, writer
drafts created, reviewer rejects, human-approved drafts, and post-publication
review failures.

## MVP output

The first production rollout writes **review data only**:

- candidate surfaces discovered from links/forwards/mentions in scanned messages;
- candidate conversation opportunities where a human recommendation could be
  useful;
- `comment_semantic_profile` summaries for scanned surfaces;
- top semantic-retrieval candidate comments with model/intent/scoring/rank
  evidence;
- route/POI target hints (`matched_existing`, `published_post_found`,
  `route_needed`, `unknown`) for route acquisition candidates;
- evidence snippets and direct links to source channel/post/comment/thread;
- LLM score/rationale and deterministic/retrieval counters;
- run metrics, benchmark metrics and limit exhaustion reasons;
- conservative potential-reach estimates for each opportunity and surface;
- a human-readable Telegraph report URL for convenient link-heavy review.

Posting, reply drafting for publication, broad VK personal-wall/follower
monitoring, sticker packs, and unattended approval are explicitly post-MVP.
Bounded read-only checks of explicitly discovered VK profile walls are part of
the current discovery frontier.

## Data ownership analysis

Current implementation keeps small discovery/review state in the existing **core
Fly SQLite DB** only as an MVP/prototype shortcut because it is already wired to
the bot UI, `ops_run`, `kaggle_run_ledger`, `kaggle_registry`, review commands
and remote Telegram session guard.

This is not a final product decision and must not be extended to bulk comment
analytics. `acq_comment_semantic_retrieval.v1` changes the storage pressure: per-
comment scores, score distributions, benchmark samples and embeddings are
research/crawler analytics, not operational bot state. They should be artifact-
first and then, if persistence is needed, stored as sanitized summaries in the
existing YDB acquisition sink. Core SQLite should keep only small reviewed/top
opportunity rows, compact surface profile snippets, counters and artifact
pointers.

For a growing discovery graph the better long-term direction is a separate
operational discovery store, behind a repository/storage abstraction. The reasons
are product/operational rather than ideological:

| Option | Strengths | Weaknesses for discovery | Fit |
| --- | --- | --- | --- |
| Core Fly SQLite | Zero new infrastructure; easiest review UI integration; good for first E2E/prototype | Single-volume operational DB becomes a crawler graph store; weak concurrent writes/report exports; harder retention/analytics; backup/restore couples acquisition experiments with event operations; must not hold full comment retrieval tables or embeddings | Temporary compatibility layer for small review state only |
| Separate local SQLite file | Easy offline/dev cache; can be produced by Kaggle/local runs without touching production DB | Still local file lifecycle/backup problem; not good as production source of truth; easy to lose/duplicate; no shared concurrent import/report story | OK only as artifact/dev cache |
| Personalization Supabase/Postgres | Existing Postgres-like analytics surface | Wrong ownership boundary: that DB is for site users/profiles/recommendation caches, not Telegram/VK crawling/review queues | Avoid |
| YDB serverless/dedicated | Existing optional acquisition sink; good fit for append/upsert run/profile/candidate summaries; cheap/free-tier-friendly for small MVP; separates crawler analytics from critical bot operations | More custom data-access code; graph/report joins and ad-hoc analytics are less convenient than PostgreSQL; should not store full embeddings without a retention/cost decision | Preferred for sanitized retrieval summaries after dry-run |
| Yandex Managed PostgreSQL | Relational model fits complex surfaces/edges/runs/opportunities/feedback joins; SQL/XLSX/report queries stay simple; mature migrations/backups/monitoring; easy future read replicas/BI | New YC resource and secrets; cross-network access from Fly/Kaggle must be configured; costs scale with allocated cluster resources | Reconsider if YDB summary model becomes too awkward for reporting |

Recommended decision: keep the current SQLite tables as an MVP compatibility
layer for operator-visible review state, but treat bulk retrieval as artifact/YDB
owned. Do not move full comment analytics into core SQLite. Do not create a new
production local SQLite owner. Use YDB serverless for sanitized
`acq_comment_retrieval_*` run/profile/candidate summaries after the dry-run
proves value; revisit Yandex Managed PostgreSQL only if reporting joins become
more important than append/upsert ingestion.

The discovery base is **one common logical store** for all acquisition
subfeatures. Do not create a separate organizer-clarification database or a
parallel surface graph. The same `acq_surface` / `acq_opportunity` rows should
carry action-specific eligibility and status, for example
`event_recommendation_reply`, `organizer_clarification`, `partner_submission`,
`sticker_strategy`, and later `publication`. A surface can be ineligible for a
generic recommendation reply but eligible for organizer clarification; this is a
per-action eligibility/status distinction, not a separate base. In the MVP this
can live in JSON fields; if it becomes too large, add a normalized
`acq_surface_action_eligibility` / `acq_opportunity_action_state` table behind
the same storage interface.

Do **not** create a separate bot for the MVP. Revisit a separate worker/bot only
if discovery volume or policy boundaries grow enough to need independent
deployment, credentials and operator workflows.

Suggested MVP tables in core SQLite:

- `acquisition_surface`
  - `platform`, `surface_type`, `username_or_id`, `url`, `title`, `about`,
    `linked_surface_id`, `status` (`seed`, `candidate`, `approved`, `rejected`,
    `paused`, `inaccessible`), `priority`, `risk_level`, action eligibility
    (`eligibility_json` or equivalent per-action table), `last_scanned_at`,
    `next_scan_after`, `scan_cursor_json`, `stats_json`, `reach_estimate_json`,
    `created_by_run_id`.
- `acquisition_surface_edge`
  - `from_surface_id`, `to_surface_id`, `edge_type` (`link`, `forward`,
    `linked_discussion`, `mention`), `evidence_url`, `evidence_message_id`,
    `first_seen_at`, `last_seen_at`, `seen_count`.
- `acquisition_opportunity`
  - `surface_id`, `source_url`, `post_id`, `comment_id`, `message_date`,
    `context_text`, `opportunity_type` / `action_type`, `intent_label`,
    `confidence`, `score`, `rationale`, `potential_views_low`,
    `potential_views_reason`, `matched_event_ids_json`, action-specific
    eligibility/status, optional LLM draft/review evidence JSON, `status`
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
- secrets: `TELEGRAM_AUTH_BUNDLE_DISCOVERY`, `TG_API_ID`, `TG_API_HASH`, a
  scoped Google key lane such as `GOOGLE_API_KEY3` or a new acquisition lane if
  quota needs isolation. `TELEGRAM_AUTH_BUNDLE_S22` is a legacy fallback only
  when the dedicated discovery bundle is absent;
- status dataset with `create_kaggle_run_config(..., kind="subscriber_acquisition_discovery",
  notebook="SubscriberAcquisitionDiscovery", resource_leases=["telegram_session:env:TELEGRAM_AUTH_BUNDLE_DISCOVERY"])`
  whenever Telegram scanning selects that bundle; derive the lease from the
  actual auth env scope. VK-only future runs may omit the Telegram lease;
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
- extract links/frontier candidates and apply hard safety/region gates;
- never make the broad semantic decision with regex alone.

### Phase 3 — semantic retrieval and surface profiling

Before spending Gemma budget, run `acq_comment_semantic_retrieval.v1` on the
collected human comments/messages. This phase embeds comments locally on Kaggle,
scores intent ladders, writes artifact tables, and produces
`comment_semantic_profile` for every scanned surface plus ranked candidate
comments. It also emits benchmark metrics when model comparison mode is enabled.

### Phase 4 — LLM surface/opportunity gate

Use Gemma only on surface profiles and top retrieval candidates, not on every raw
comment. The current Kaggle MVP uses the same isolated monitoring key convention
(`GOOGLE_API_KEY3`, `ACQ_GOOGLE_KEY_ENV`) and native structured JSON output; the
model is configurable through `ACQ_LLM_MODEL` and defaults to the repo-proven
`models/gemma-4-31b-it`. Prompt output should be strict JSON:

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

Gemma 4 is required for high-confidence review/gating decisions. 26B-class
variants can be configured for this bounded short-comment task after smoke
coverage, but Lite/Flash/Lite-class models are not enough for final acceptance
gates. The explicit exception is the organizer-clarification **writer** stage:
it uses pinned `gemini-3.1-flash-lite` to write candidate public wording, and
that wording remains unpublishable until an independent Gemma 4 reviewer accepts
it.

### Phase 5 — opportunity triage

For top retrieval candidates, ask the LLM whether the message is a sparse,
contextual chance to recommend an event, route/POI target, site/search surface or
organizer action. Output should include:

- intent label (`asking_where_to_go`, `looking_for_children_event`,
  `tourist_question`, `relocation_local_tip`, `event_comparison`, `other`);
- a checklist: explicit question/need, future/current need rather than
  post-event praise, clear useful reply target, native short reply viability,
  low spam risk;
- confidence and anti-spam risk;
- direct evidence quote/snippet;
- optional candidate event query terms.

If the checklist is all or mostly negative — for example “спасибо
организаторам”, “погода была отличная”, “были гостями, молодцы” — the Gemma gate
must return `is_candidate=false`, and the runtime must not show an operator card.
Missing Google key or provider/schema failure also fails closed for opportunities
and is recorded in run diagnostics/stats.

Reply text generation is optional in MVP and should remain `draft_only`; no
publication action is available from the runtime.

### Phase 6 — server import/review

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
  conservative reach estimate, semantic profile, event/route/organizer signal,
  native-event-topic opportunities, risks, recommended action;
- semantic-retrieval benchmark summary when enabled: model, throughput, score
  distributions, funnel reduction and selected threshold/top-percent policy;
- candidate opportunities with direct Telegram/VK links to posts/comments,
  retrieval rank/evidence, LLM rationale, potential views, route target hints
  and matched event ideas;
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
