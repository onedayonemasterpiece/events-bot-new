# Publication queue

Status: MVP product-goal queue. Auto-publishing to public Telegram/VK remains disabled; this queue is for human/product review and local operator notifications.

The final Gemini verifier is intentionally stricter than regional relevance.
`accept` requires KO as the main subject, a grounded firsthand visit or clearly
attributed subscriber travel/photo report, personal emotion/impression or
review, and a concrete memorable/unusual/useful detail. Generic destination
cards, official route material, coordinates-only posts, ads, news and roundups
must not reach the review chat merely because their image is attractive. The
prompt version is part of the durable request fingerprint so a tightened policy
cannot replay an older, weaker verdict.

The finalizer persists both `llm_request_fingerprint` and
`llm_prompt_version`; CandidateReport must preserve them together with the
terminal status. This makes replay/idempotency auditable instead of relying on
an opaque status alone.

Eligibility evidence can exceed the compact YDB string cap. The finalizer
therefore persists `publication_eligibility_evidence_fingerprint` for the full
evidence payload and uses it together with the gate version and authoritative
source fingerprint. The readable evidence remains capped, but an unchanged
terminal tombstone is no longer rewritten, re-pruned or reported as new work
on every later finalizer run. Legacy 700-character evidence prefixes are
recognized once without a migration rewrite; a real change beyond that prefix
is detected after the durable fingerprint has been stored.
An already-current source-attestation priority for the same source and
five-post target is likewise reused rather than rotating between several
qualifying posts and getting a fresh timestamp/YDB write on every invocation.

Advertising is judged from commercial evidence in the main content. A trailing
channel signature such as `Мы ВКонтакте | Мы в MAX` is cross-platform
navigation, and `Фото:` / `Видео:` / `Источник:` is media attribution; neither
is advertising by itself. Price, booking/purchase CTA, sponsorship, promo code
or another commercial call remains ad evidence. Attribution also does not by
itself prove a firsthand visit or a subscriber report, so an impersonal route
card may still be rejected for missing story/emotion without being falsely
labelled advertising. This contract is `region_talk_final_verifier_v4`.

## Product criteria

A post can enter `publication_candidate_item` only when all of the following are true:

- the main subject is Kaliningrad Oblast (`kaliningrad_oblast_only_scope=true`, mention role `main_subject|unclear`);
- the source is not a pure Kaliningrad-local channel (`source_geo_class != kaliningrad_local`); local channels are kept for a separate future monitoring lane;
- the source looks like a nonlocal blogger/travel/media/personal source, not local/federal news or tours/ads;
- the text/vector gate does not reject the post as news, ad/promo, other-region travel, multi-region roundup, or low substance;
- for photos, RegionTalkImageDiagnostic has written actual-image scores (`image_model_input_type=actual_image`, `image_queue_status=actual_scored`) and they pass the publication thresholds (`REGION_TALK_PUBLICATION_MIN_OVERALL_MEDIA_SCORE`, default `0.66`; `REGION_TALK_PUBLICATION_MIN_POSTCARDNESS_SCORE`, default `0.55`);
- for a positively identified video (`.mp4/.mov/.m4v/.webm/.avi/.mkv`), image scores are intentionally absent: only a strict external-source + KO-only + E5/BGE text pass may enter Gemini, and the accepted link is marked `media_review_mode=operator_video_review` for manual watching in the operator chat; unsupported documents/audio do not receive this exception;
- Gemini Lite final verification accepts the text criteria through the Supabase google_ai reserve/limiter.

`RegionTalkCandidateReport` owns discovery, E5, strict current-text E5+BGE
fusion, and the signed image-queue handoff. `RegionTalkImageDiagnostic` remains
the visual scorer and writer of actual-image evidence. The local
`scripts/region_talk_publication_finalizer.py` is the **single owner** of final
Gemini publication verification and `publication_candidate_item` terminal
outcomes in the orchestrated path; CandidateReport's Gemini modes are disabled
by the orchestrator. CandidateReport may refresh ranking/source/media context,
but must preserve every `finalizer_state_version` terminal status, eligibility
attestation, retry field and delivery marker; it must never reopen a
Gemini-rejected URL as pending after a later discovery run.

### Post-level idempotent vector work

A source rescan is allowed, but it is not permission to re-encode every known
post. CandidateReport now builds a work plan before applying the per-run E5
limit. A post is actionable only when its text/model/semantic-bank contract is
new or changed, when BGE-M3 has arrived for a row that was waiting for fusion,
or when a legacy row needs one policy-fingerprint refresh. Current unchanged
rows are skipped; a current E5 row waiting for BGE is not encoded again.

The durable `post_processing_fingerprint` includes text hash, policy version,
E5/BGE model contracts and semantic-bank version/hash. Exact post links,
confirmed external-blogger evidence, fast-check and keyword hits are ordered
ahead of generic history within the actionable set. Image and Gemini retries
remain owned by their own queues. Run summaries expose new E5 work, reused
fusion work, BGE waits without E5 recomputation and unchanged skips separately.

### Source onboarding for the operator

After Gemini accepts a post, the finalizer builds/reuses the evidence-backed
source profile described in [Source onboarding profile](source-onboarding-profile.md).
It stores `source_onboarding_evidence_item` and
`source_onboarding_profile_item`, then writes the candidate-specific paragraph
and all claim/evidence references into `publication_candidate_item`. The
notifier renders `О блогере: …` only when the deterministic support and
300–600-character checks return `source_onboarding_status=ready`; otherwise the
candidate remains reviewable without an invented biography.

### Editorial onboarding writer v10

The final verifier remains the owner of the publication verdict. Accepted
candidates are rewritten by a separate LLM-first copy pipeline,
`region_talk_editorial_onboarding_writer_v10_publisher_reader_brief`:

1. **Strategy** chooses the external-source angle and an honest history mode
   from at most five actually published or exact-current `approved + clean`
   predecessors. A forced bridge is replaced by `fresh_start`.
2. **Publisher/source onboarding** is a reusable evidence-grounded LLM profile.
   Article lanes require outlet identity, intended audience and distinctive
   editorial value; social lanes retain the author/channel profile.
3. **Grounded Writer** produces exactly two Russian paragraphs. For articles,
   paragraph one cites all three publisher dimensions and gives the reader a
   compact reason to care about the outlet. Paragraph two covers one or two
   concrete details of the current material and a reason to open it.
4. Deterministic validators check 150–500 characters per paragraph, the
   550–900-character visible caption target, Russian language, banned
   clickbait/PR wording, third-person ownership and evidence-ID integrity. The
   adversative AI-cliché family built as `не …, а …` is a named hard failure,
   including punctuation/dash/line-break variants inside one sentence.
5. **Critic** returns `pass|rewrite|reject`; at most one writer retry is
   allowed. A second failure becomes `needs_grounding_review`, never an
   invented fallback.

The caption-length check uses the exact rendered text, including source
attribution, before Critic. A short or oversized otherwise-grounded draft is
therefore returned to Writer through the same single LLM-first retry. The
renderer repeats this check as a final fail-closed boundary and preserves the
complete stage audit if that secondary guard ever fires. Physical backfill
stage calls are spaced by
`REGION_TALK_DRAFT_BACKFILL_STAGE_DELAY_SECONDS` (default `5.5`) so a
multi-stage candidate stays below the conservative 13 RPM project ledger;
model-scoped overflow still prefers `gemini-3.5-flash-lite` and then
`gemini-3.1-flash-lite`.

Writer receives the exact attribution overhead plus a hard numeric caption
contract. Its normal paragraph targets are 260–420 characters; a length retry
also receives the measured visible length, a 620-character safe target and the
minimum number of grounded editorial characters to add. The stage execution
revision participates in the durable request fingerprint, so an older cached
short response cannot satisfy a newer prompt contract. If the retry still
misses, its Writer output and grounding map remain in the stage audit for
diagnosis while the candidate stays outside delivery and planning.
The Russian-language ratio ignores only exact source-owned Latin-script names
and `@handles` supplied by the candidate row. Arbitrary English prose still
fails the same threshold; this keeps channels such as `Sasha Meets Russia`
attributable without misclassifying otherwise Russian copy.

Every stage uses the existing controlled Google AI gateway, Supabase
`google_ai_reserve` limiter and durable YDB request budget. Stage prompt/model,
request and prompt fingerprints, usage, input evidence, bounded history,
strategy, grounding map and critic result are stored without truncation. The
writer never changes `llm_decision` or the original verifier verdict.

The style rule is enforced at four boundaries: Writer instruction, Critic
hard-fail, deterministic writer validation before/after the single retry, and
the shared notifier/planner readiness predicate. `public_caption()` repeats the
same check immediately before rendering. Regex only detects and rejects the
style pattern; it never rewrites meaning. Semantic rewriting remains LLM-first.
Two failed writer attempts become `needs_grounding_review` and cannot be sent
to the operator chat or occupy a daily plan slot.

Social evidence is restored from the exact Telegram/VK post using only the
role-scoped discovery identities/API. Article evidence is read from the
retained `external_publication_intake_item`; a teaser projection is no longer
treated as finished public copy. The explicit v7 prompt version is stale.
The v10 rollout invalidates earlier draft/cache fingerprints and runs a
bounded copy backfill over unpublished confirmed candidates. It builds/reuses
publisher reader briefs before regenerating article copy. Backfill archives
its old reaction projection once as `principle approved +
rewrite requested`, clears the current-review projection, and gives the new
text-plus-media revision a new fingerprint that requires fresh reactions.
Already target-published rows are excluded fail-closed by canonical URL and
candidate ID, including publication status recorded directly on the candidate.
Unversioned drafts are stale and never satisfy readiness; unversioned/older
terminal backfill rows are migration debt and become actionable under v10.
Operators may target an exact URL
with repeatable `--candidate-url`; `--force-regenerate` is allowed only together
with that explicit selector and bypasses that row's terminal/retry cooldown.

Operator delivery is fail-closed on one atomic revision: two paragraphs,
source/original links, ordered media manifest and layout. An article requires
one associated hero; a photo-led social post uses one hero or an ordered 3–6
frame album; a video-led post uses its source video. Only a material explicitly
diagnosed as having no usable media takes `link_preview_fallback`. Source media
reuse with prominent attribution is the product policy; visual checks verify
association, order and usability rather than applying a speculative rights
block.

Video is not treated as a weak image and is never assigned a fabricated visual
score. Gemini verifies only the text/product criteria. The notifier sends a
Gemini-confirmed video link to the same operator chat, where the operator makes
the visual/video-quality decision manually.

Actual-media acquisition is bounded by
`REGION_TALK_IMAGE_MAX_MEDIA_FETCH_ATTEMPTS` (default `3`). A post that returns
no downloadable image repeatedly becomes `broken_media` and leaves the active
queue; a positively identified video still follows the separate operator-video
path and is not converted into a broken photo.

## Honest funnel metrics

Discovery matches and confirmed post decisions are different grains and must
not be added together. The operator surface uses these stages:

1. `fast_check_keyword_match_sources_total` — sources where preflight found a
   literal query hit; this is not a confirmed KO post.
2. `fast_check_exact_hit_post_urls_total` / `fast_check_exact_posts_processed_unique_total`
   — exact hit links persisted and then fetched/scored.
3. `fast_check_exact_posts_dual_vectorized_total` — exact posts with both E5
   and BGE-M3.
4. `fast_check_exact_posts_dual_semantic_accept_total` — E5+BGE semantic match;
   this is not yet a publication candidate. Then
   `fast_check_exact_posts_strict_text_accepted_total` means the **complete
   publication text gate**: current freshness, external/non-spam source,
   KO-only scope, dual semantic match and no terminal text rejection. It and
   `fast_check_exact_posts_text_rejected_total` plus
   `fast_check_exact_posts_text_rejection_reasons` — the real text-gate result.
   `fast_check_exact_posts_text_pending_total` is reported separately: waiting
   for BGE or another current processing stage is not a rejection.
5. `fast_check_exact_posts_image_queue_total` and
   `fast_check_exact_posts_video_manual_review_total` — separate photo and
   manual-video paths.
6. `fast_check_exact_posts_publication_queue_total`,
   `publication_confirmed_total`, `publication_sent_total` — Gemini and chat
   conversion.

The phrase “strict gate passed” must never be presented as “ready for
publication”. Ready means Gemini-confirmed and not yet sent; sent is a separate
terminal ledger.

## Exact-text restoration after asynchronous media scoring

The media worker may finish hours or runs after E5+BGE. A missing post body at
that point is therefore **not** a content rejection and is not a terminal
`filtered_before_llm` outcome. The finalizer records
`publication_status=text_restore_pending` /
`publication_candidate_status=awaiting_text_restore`, spends no Gemini budget,
and reopens the exact Telegram URL as a priority-0 `post_link_queue_item`.
The handoff has a dedicated durable
`publication_text_restore_requested=true` marker plus request reason/time/run;
`priority_reason` remains a backward-compatible ordering hint rather than the
only product-state carrier. CandidateReport also joins the authoritative
`publication_candidate_item` by canonical post URL on every exact-link load.
This self-repairs older or concurrently rewritten keyword rows: a pending
publication with durable full text takes a CPU/YDB-only local rebuild, while a
row without text re-enters the same bounded exact Telegram fetch lane.
CandidateReport performs the refetch through DISCOVERY1, its cached entity and
the normal human-like request governor; the finalizer does not use public-web
HTML as a hidden fallback. Existing FloodWait/cooldown evidence on an active
exact-link retry is preserved.

The exact-link selector reserves up to five existing network slots per run for
these near-final `text_restore_pending` rows before ordinary fresh keyword
links. This is an ordering rule, not a larger Telegram budget: total exact
calls, cached-entity preference, pacing and cooldown behavior are unchanged.
After a successful restore, CandidateReport must rebuild the current policy,
fusion and candidate-memory projection even when the text hash and both E5/BGE
vectors are already current. That pass is `reuse_e5_bge_text_restore`: it
reuses both durable vectors and does not load or run either encoder. If BGE
does not yet match the restored full-text hash, the row takes
`reuse_e5_wait_bge_text_restore`: CandidateReport reuses the current E5
semantic scores, rewrites the same stable E5 ledger row with the lossless
active body, and performs **no E5 inference**. The isolated BGE notebook then
enriches that body, after which the next CandidateReport pass performs the
normal dual fusion above. An ordinary previously processed row may remain a
passive `wait_bge_existing_e5`; a near-final text restore may not, because its
body is the required durable input to the external worker. The operator metrics
`posts_planned_e5_payload_refresh_for_bge_text_restore` and
`posts_e5_payload_refresh_for_bge_text_restore_written` distinguish planning
from a confirmed YDB write. The refresh preserves the matched E5 row's YDB PK
even when exact-link and source-history routes produced different `post_id`
values.

Candidate memory identity is canonical post URL, not a fetch-route-specific
`post_id`, so source-history and exact-link fetches cannot create a second product row for
the same public post. This reuse also applies when candidate memory is being
bootstrapped from a legacy processed-post row rather than an already-migrated
`candidate_memory_item`.

Gemini/operator/delivery verdicts are monotonic. A contradictory stale
`text_restore_pending` projection cannot reopen a `sent_to_chat`,
`llm_rejected`, tombstoned or revoked post, and terminal source/bad-link rows
remain terminal. This veto is URL-level across duplicate historical
publication rows: any durable delivered/Gemini/operator/tombstone/revoked row
blocks an older pending projection. Historical rows where the normalized
status lagged behind the provider result are covered too: durable
`llm_decision=accept|reject` is terminal evidence and cannot be reopened. Any
already-persisted restore marker on that terminal URL is actively cleared and
its exact link returns to the fetched terminal state, so the historical bug
cannot keep scheduling the same post. The queue loader persists this cleanup
before excluding the fetched row from its returned work batch and reports
`terminal_restore_suppressions_persisted`; an in-memory-only cleanup is not a
completed repair. The
only legacy terminal spelling
intentionally migrated is
`no_text_for_gemini + filtered_before_llm`, because that combination was
created by the historical premature-compaction bug before Gemini ran.

Exact-link and source-history observations are collapsed by canonical platform
post identity before vector planning. The richest active text (with explicit
publication restore taking precedence) is scored once, so route-specific
`post_id` values cannot create two E5 rows in the same run. Metrics expose raw
observations, unique posts used for vector planning and collapsed duplicates
separately.

Source-onboarding evidence also treats the current restored candidate as the
authoritative authored-post excerpt for its URL. An older compacted memory row
with the same URL is removed from that evidence pack rather than hiding the
restored body and producing a misleading identity-only profile.
The Kaggle CandidateReport launcher defaults to the shared YDB backend; an
offline JSON run now requires an explicit `REGION_TALK_STATE_BACKEND=json`, so
a successful sandbox report cannot be mistaken for product-funnel progress.

Legacy `no_text_for_gemini` rows are migrated into this retry contour. Working
text remains lossless only while the candidate is active and is deleted after
the actual Gemini/operator terminal verdict. This keeps YDB compact without
allowing storage compaction to terminate an otherwise eligible post before
Gemini sees it.

The legacy field `drop_gate` is not by itself proof of a text rejection. It is
also used for downstream media outcomes (`image_fetch_gate`,
`image_postcardness_gate`, `candidate_score_gate`). A post waiting for its image
or routed to manual video review therefore counts as having passed the text
gate, while remaining **not yet** a publication candidate.

For current funnel metrics, `processed_post_item` is authoritative over an
older `candidate_memory_item`. Candidate memory is historical/operational and
must not turn a newly deferred or rejected current verdict back into an
acceptance.

Operator feedback is durable and idempotent through
`scripts/region_talk_operator_feedback.py`. A reject tombstones exact-link,
candidate, media and publication projections; a review request records human
calibration evidence but does not bypass freshness/source/Gemini safeguards.
`approve_visual` is narrower still: it is accepted only for a complete decoded
gallery with safety score `>=0.95`, is bound to the current media-manifest hash,
and bypasses only the uncalibrated legacy visual-score threshold. Source,
text/vector, rights and final Gemini verification remain unchanged.

Keyword source metrics likewise distinguish
`keyword_sources_with_preliminary_candidates_total` from
`keyword_sources_with_confirmed_ko_posts_total` and the narrower
`keyword_external_sources_with_confirmed_ko_posts_total`. The legacy
`publics_keyword_with_ko_candidates_total` remains only for compatibility and
must be labelled broad/legacy because `candidate_posts_found` is not confirmed
KO evidence.

If a strong actual-image row is blocked only because its source has fewer than
five sampled posts and therefore has no durable external/local verdict, the
finalizer marks that existing source row with
`publication_source_evidence_priority=true`. The next CandidateReport scan
selects this bounded evidence-completion lane before ordinary backlog. It does
not weaken the source gate: after the scan, the finalizer recomputes the same
fail-closed source/text/vector/image eligibility.

## Ranking

`publication_score` combines:

- actual image quality / visual score;
- postcardness;
- text-story/emotion/usefulness evidence;
- nonlocal source value;
- semantic anti-overlap with already confirmed/sent/published candidates.

The old same-source/place/content-type penalty is only a fallback when vectors
are missing. Target diversity is a real semantic anti-vector: store vectors for
confirmed/sent/published rows in `publication_semantic_history_item`, compute
the candidate's maximum cosine similarity to that history, and rank strong
candidates by:

```text
publication_rank_score = quality_score - diversity_weight * max_similarity_to_history
```

This makes the next selected post meaningfully farther from what is already in
the shortlist, while the full text/source/image/Gemini gates still decide
whether it is eligible at all.

### Daily article + social plan

`scripts/region_talk_publication_plan.py` builds the durable public-content
selection plan with policy `region_talk_daily_pair_antivector_v1`:

- exactly one `article` slot and one `social` slot per Kaliningrad day;
- articles are editorial/academic external publications; social rows are
  Telegram/VK posts;
- article slots normally carry one verified publisher-associated hero plus the
  prominent original link. Only an explicit terminal missing/unextractable
  media result may use the system-link-preview fallback. Social slots keep the
  strict source-image/album/video-review gate;
- each lane is ranked against actual target-publication history plus all
  earlier selections in that lane, so long-range diversity is recalculated
  rather than frozen when new candidates arrive;
- the social choice is also compared with that day's article. An alternative
  below the pair-similarity threshold is preferred; unavoidable relaxation is
  explicit in `pair_diversity_relaxed`;
- future `planned`/`vacant` slots are overwritten on every run. Only
  `locked`/`published` slots are immutable;
- an elapsed planned slot is frozen, and a first-ever plan created after the
  first daily slot starts on the next day instead of manufacturing a past due
  publication; the frozen slot also consumes its candidate identity, so that
  same article/post cannot be selected again later in the recalculated horizon;
- a missing lane is persisted as `vacant`, never silently filled with a second
  item from the other lane;
- a confirmed row is not publication-ready until it has a complete grounded
  Telegram/VK draft, explicit attribution and one to three support points;
  legacy rows without that package are excluded from the public schedule
  instead of reaching a slot with no usable copy;
- planner and operator-chat delivery use the same strict readiness predicate:
  `publication_draft_fact_points_json` must decode to a non-empty list, not
  merely contain a non-empty string such as `[]`;
- the same selected content is intended for cross-posting to Telegram and VK;
  this is two content items per day, not four unrelated platform posts.

The planner reads the actual target publication log/schedule, not operator-chat
delivery, when reconstructing `publication_semantic_history_item`. Sending a
candidate to the review chat therefore does not falsely mark it as publicly
published. The scheduled runner executes the planner after each autonomous
discovery/finalization session, so the next 14 days are recalculated five
times per day as candidates arrive under the current production cadence.

The implemented on-demand operator view uses
`scripts/region_talk_review_queue.py` policy
`region_talk_mmr_adjacency_v1`. It is an iterative MMR order, not one static
penalty: each next row is compared with recent durable history and the rows
already placed in the current snapshot. An explicit previous-neighbour
threshold keeps a near-duplicate out of the next slot whenever an alternative
exists. If no alternative exists, the snapshot records
`adjacency_relaxed=true` rather than hiding the homogeneous tail. Only equal
model/encoder/dimension/encoding vector contracts are comparable; missing or
incompatible vectors use a visible source/topic/content fallback.

## Grounded final-post draft

For every newly Gemini-accepted social post, editorial article or academic
publication, final verification also returns a bounded fact-grounded draft for
Telegram and VK. It is persisted before working source text is compacted.
`publication_draft_status=ready_for_operator_review` requires a title, explicit
source attribution, canonical original URL, both platform texts and one to
three internal claim/support pairs. The operator candidate message includes the
Telegram draft. This is the input to the future target-channel publisher; it
does not bypass diversity ordering, source-media association/attribution or
manual release gates.
For an article whose source media is terminally absent or unextractable, the
eligibility evidence explicitly says
`publication_external_article_link_preview_fallback` and
`terminal_media_fallback_system_link_preview`; this is not visual approval.
The fallback is unavailable while source-media extraction or browser
materialization is merely pending, and `rights_policy` alone cannot trigger it.

Historical external articles accepted before the grounded-draft rollout may be
repaired without another semantic verdict only when their strict intake row
already contains an evidence-backed `editorial_pack`. The planner copies those
sentences verbatim into Telegram/VK layout, verifies every used copy surface
against existing evidence IDs, preserves the caveat and original link, and
records `region_talk_external_evidence_projection_v1`. This mechanical
projection is unavailable to social posts and fails closed on a missing or
broken support reference. Social rows without a current grounded draft remain
outside the schedule until the LLM-first writer has produced one.

Legacy confirmed social rows are repaired by
`scripts/region_talk_publication_draft_backfill.py` in bounded batches. For a
Telegram URL it fetches the exact original message through an explicitly
selected `telethon_discovery1` or `telethon_discovery2` identity; for a VK URL
it uses the existing read-token chain and exact `wall.getById` identity. The
source body is held only for the current writer call and is not restored into
the compact terminal publication row. Gemini uses the same final-verifier /
grounded-writer prompt and the shared Supabase limiter, plus a separate daily
durable fingerprint budget. The original `llm_confirmed` or `sent_to_chat`
verdict is monotonic: the backfill may add draft fields or a review/retry
status, but never replace that historical verdict.

Writer history accepts a public `published` row or an operator-approved clean
row whose `operator_review_fingerprint` still equals the current exact
draft-plus-media fingerprint. Stale reactions never become narrative context.
The last three stored editorial plans also enforce the anti-template rule: if
two used an explicit transition/scale shift, the next plan is deterministically
forced to `fresh_start`.

The orchestrator exposes separate Telegram and VK actionable counts. Telegram
defaults to `DISCOVERY2` because the continuously launched CandidateReport
normally owns `DISCOVERY1`; either discovery transport is valid only while its
mapped Kaggle notebook is verified idle. A per-auth-bundle local file lock also
prevents two local agents from connecting the same StringSession after the
same idle observation. Provider errors retry durably; three source-fetch
failures become manual `source_text_unavailable`, while a semantic disagreement
becomes manual review rather than an infinite paid retry loop.

## YDB state

CandidateReport writes:

- `publication_goal` in compact `latest_state`;
- row-level `publication_candidate_item:<id>` rows;
- XLSX sheets `04p_publication_queue` and `04q_publication_confirmed`.

Goal fields include:

- `publication_goal_id`;
- `target_confirmed` (default `20`);
- `llm_budget_max` (default `100`);
- `confirmed_count`;
- `sent_count`;
- `llm_calls_used_total` / `llm_budget_remaining` (includes same-run final
  verifier calls that created accepted rows, not only extra publication-queue
  verification calls);
- `goal_status=running|complete|llm_budget_exhausted`.

Local finalizer calls additionally use durable YDB rows:

- `region_talk_llm_budget_item:<budget_id>` — cumulative reserved requests,
  hard-clamped to `100`;
- `region_talk_llm_request_item:<budget_id>:<fingerprint>` — deterministic
  request identity and completed result replay.

Only terminal provider results (`llm_gate_status=ok`) are replayed. Errors,
rate limits and unknown responses keep the same reservation retryable rather
than becoming a permanent cached failure. The local finalizer verifies that
the official `google-genai` SDK is importable **before** creating a Region Talk
budget reservation. Gemini request identity includes stable source
classification but excludes monotonic `posts_scanned`/KO/candidate counters,
so a deeper scan with the same external/local verdict cannot spend another
request by changing an unrelated source fingerprint.

The shared Supabase `google_ai_reserve` remains the cross-service RPM/TPM/RPD
authority. The Region Talk ledger is an extra product-run ceiling. Internal
provider retries are set to one attempt; retryable rows are retried by the
durable finalizer state instead.

All Region Talk Gemini consumers (final verifier/onboarding, visual
adjudicator and optional grounded external research) disable direct reserve and
process-local limiter fallbacks. If Supabase, `google_ai_reserve`, the
scoped `GOOGLE_API_KEY3` registration or both stable Flash-Lite model rows are
unavailable, the request fails/defer-closes before the provider call. The
default order is `gemini-3.5-flash-lite` followed by
`gemini-3.1-flash-lite`; each has an independent conservative
`13 RPM / 240000 TPM / 450 RPD` registry row. A model-scoped quota block uses
the next row immediately. Provider failures remain single-send failures for
the Region Talk product budget. YDB's
`DurableGeminiBudget` adds a cumulative 100-request ceiling and fingerprint
replay for finalizer/onboarding and image-VLM work; it does not replace the
shared RPM/TPM/RPD reservation.

## Operator Telegram notification

### Reaction review contract

Every newly rendered candidate carries a visible legend and is reviewed on two
independent axes:

| Exact allowlisted operator reactions | Decision | Copy state | Eligible when gate is enabled |
|---|---|---|---|
| `❤️` / `❤` or `👍` only | `approved` | `clean` | yes |
| `✍️` / `✍` only | `pending` | `rewrite_requested` | no |
| positive + `✍️` | `approved` | `rewrite_requested` | no, rewrite first |
| `👎` (without positive) | `rejected` | clean or rewrite | no |
| positive + `👎` | `conflict` | clean or rewrite | no |

`scripts/region_talk_reaction_sync.py` reads reactions only through
`telethon_discovery2`, under the same local lease and verified-idle guard as
ImageDiagnostic's role-scoped session. `REGION_TALK_OPERATOR_REVIEWER_IDS` is a
required comma/space-separated allowlist of numeric Telegram user IDs.
Telegram's aggregate emoji counts are never authority: the synchronizer uses
`messages.getMessageReactionsList`, follows every page, and binds only exact
reactors from that allowlist. A missing page, looping offset or count mismatch
aborts before any revision is written. This is what makes a completely removed
reaction reversible without mistaking a partial page for removal.
For a valid delivery whose current Telegram `Message.reactions` field is absent
([Telegram API contract](https://core.telegram.org/api/reactions)),
the synchronizer records a complete empty observation without invoking
`messages.getMessageReactionsList`: Telegram can return `MSG_ID_INVALID` for
that list call even though the reaction-less message itself exists. If the
last reaction disappears between the snapshot and list call, one fresh message
read must confirm the absent reactions field before the empty revision is
accepted; a missing message still fails closed.

Review identity is
`region_talk_operator_review_payload_v1`: canonical post URL, exact grounded
draft fingerprint, and the ordered media/presentation manifest. Carousel order
is semantic. Any changed Telegram/VK copy, selected image, carousel order or
presentation mode gets another delivery identity and requires fresh reactions;
the earlier event remains historical and cannot approve the new revision.

The synchronizer defaults to dry-run. `--execute` writes an idempotent current
state, an immutable observation-revision event when exact reactions changed,
and a projection onto `publication_candidate_item` only when the observed
fingerprint still equals the candidate's current fingerprint. It never
tombstones or deletes a candidate/source.

Public-plan enforcement is rollout-gated. Local/dev keeps
`REGION_TALK_REACTION_GATE_ENABLED=0` as the safe migration default, while
production enables it after the historical-review migration. With the gate on,
only exact-current `approved + clean` rows are admitted; pending, rejected,
conflict, rewrite-requested and stale-fingerprint rows all fail closed. The
one-time historical projection (`principle approved + rewrite requested`) is
audit context for regenerating copy, never approval of the new revision.

Use `scripts/region_talk_goal_notify.py`, never on Kaggle. It supports Bot API
and role-scoped `telethon_discovery1` / `telethon_discovery2` transports;
production defaults to `telethon_discovery2`. Neither
`TELEGRAM_AUTH_BUNDLE_E2E` nor generic `TELEGRAM_SESSION` is a notifier input,
and the scheduled wrapper removes both from child environments. Before
Telethon connects, the notifier rechecks the mapped Kaggle kernel and fails
closed for active or unverified status. The selected account must already be a
member of the numeric `REGION_TALK_NOTIFY_CHAT_ID`. The
notifier sends only complete draft-ready unsent `llm_confirmed` candidates to
the operator chat and deduplicates
them by canonical post URL plus numeric chat id in
`publication_delivery_item`. After Telegram acceptance it records chat and
message ids and timestamps both in the delivery ledger and candidate row.
Finalizer/CandidateReport writers preserve these sent markers.

Telethon delivery uses the durable ledger's stable `random_id`, allowing a
crash-ambiguous `sending` row to be retried idempotently. This transport also
keeps the MTProto entity surface available for future target-channel custom
premium emoji. The discovery identity is never connected merely to decorate a
candidate: custom-emoji entities are optional and belong to the future public
publisher after the same session-idle/lease gate. Bot API does not accept a caller-supplied idempotency key, so a pre-existing ambiguous
`status=sending` is stopped for operator reconciliation instead of risking a
duplicate. A successful Bot API response is persisted before the candidate is
marked sent.

The small per-run send limit is applied only after scanning the durable
publication ledger. YDB primary-key order is not a readiness order, so older
tombstones cannot hide a later confirmed unsent candidate from notification.

For an explicit, read-only ranked snapshot use:

```bash
python3 scripts/region_talk_goal_notify.py --queue --limit 20 --dry-run
```

`--dry-run` performs no Telegram connection. For the production-equivalent
transport use `--transport telethon_discovery2`; it verifies ImageDiagnostic is
idle before connecting. An explicit `--transport bot_api` check first validates
`getMe` and the pinned chat through `getChat`.

Removing `--dry-run` sends the snapshot to the same pinned operator chat. This
mode does not set `sent_to_chat` and does not alter the candidate delivery
ledger. Each snapshot exposes rank/quality/max-similarity, fallback status and
unavoidable adjacency relaxation.

Before delivery, the notifier reconstructs the authoritative source with the
same monotonic merge semantics as the finalizer: scan/KO/candidate counters use
their maximum and terminal local/spam classification cannot be erased by a
later sparse `source_status_item` or `online_source_item`. Otherwise a valid
Gemini-confirmed row can fail its source fingerprint check merely because a
live progress overlay contains zero counters.

CandidateReport also reconciles source locality from the durable candidate
ledger before image/publication handoff. This durable-ledger lane treats seven
strict dual-accepted KO rows spread over at least 42 days as persistent
regional-author evidence, not as a
single visitor's trip burst. The source and its unsent candidate projections
are routed to the local-region audit lane unless an authoritative
`confirmed_external` evidence record explicitly overrides that inference.
This closes the observed `vk:krasivo_s_evgo` failure mode (22 scanned/0 KO in a
sparse status row while seven accepted KO candidates plus one vector-rejected
row spanned 49 days). Candidate-ledger repairs carry explicit run lineage and
are mandatory in the bounded source-queue handoff even when that source was
not selected for the current history batch. A terminal locality correction
also outranks routine current-run scan rollups when the bounded handoff is
full. Gemini's
post-level approval can never override this source-level terminal decision.
The broader recent-history source classifier keeps the more conservative
eight-post threshold; the seven-post boundary is limited to already
dual-accepted durable KO evidence.

An external-blogger evidence row overrides persistent-local evidence only when
`region_relation_status` explicitly says that the author is a non-local
visitor. `confirmed_external` with an unresolved relation still keeps the
source in the priority research cohort, but it is not authoritative enough to
erase seven durable KO posts across six weeks. This distinction is required
for rows such as `vk:krasivo_s_evgo`, which entered the research table before
the author's locality was established.

The orchestrator reconstructs one live source view from the canonical
`source_queue_item` plus sparse status/online overlays.  Terminal local/spam
classification fields from the canonical queue are monotonic during this
merge: later historical overlays may add larger scan counters or current fetch
diagnostics, but cannot replace `rejected_local_region_source` / local-region
scope with `processed_found_ko_candidate` / `unknown`.  This prevents already
tombstoned media from reappearing as a phantom Gemini-finalizer backlog.

Per-run post throughput distinguishes posts first admitted in that run
(`first_seen_run_id`) from durable posts merely refreshed/re-evaluated by the
same run. The CandidateReport heartbeat owns the actual deep-history source
count and fetched/E5-scored workload. Source-overlay rows sharing a
`last_scan_run_id` remain visible as a separate technical diagnostic and are
not labelled as the number of deeply read sources.

The bot never joins an invite link. `REGION_TALK_NOTIFY_CHAT_ID` pins the
expected numeric chat and fails closed on a wrong target; the prepared chat is
currently pinned by default as `-5563945596`. Every notifier result,
including a zero-row dry run, reports the resolved numeric chat and delivery
account ids so operators can pin them without sending a test message.

Example:

```bash
python scripts/region_talk_goal_notify.py \
  --env-file /home/dev/projects/events-bot-new/.env \
  --expected-chat-id '-5563945596' \
  --limit 20
```

For launch progress messages:

```bash
python scripts/region_talk_goal_notify.py --message 'Region Talk: CandidateReport run started …'
```

## Queue rules for future public publishing

- Exactly 1 article and 1 social post per day.
- Cross-post each selected item to both Telegram and VK where possible and allowed.
- Do not publish the same source more than once per day.
- Max 1–2 posts per source per 7 days.
- Recalculate unlocked future slots whenever new candidates arrive.
- Use semantic anti-vector history separately for articles and social posts.
- Keep the article/social pair and adjacent days semantically different.
- Candidates can expire.
- Dry-run mode is required.
- Auto-publish is disabled by default until explicitly configured.

Future publisher state machine: `pending → locked → published|failed|skipped|cancelled`.

Idempotency:

- same `candidate_id + target_platform` cannot publish twice unless explicit `force_republish` is set and logged;
- every publish attempt writes/updates queue state and then publication log;
- partial success must be visible (e.g. Telegram published, VK failed).

Telegram/VK publication cannot be transactionally rolled back with YDB. Deletion/edit can be attempted later, but ledger must preserve original API responses and deletion status.
