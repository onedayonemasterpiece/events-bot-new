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

## Product criteria

A post can enter `publication_candidate_item` only when all of the following are true:

- the main subject is Kaliningrad Oblast (`kaliningrad_oblast_only_scope=true`, mention role `main_subject|unclear`);
- the source is not a pure Kaliningrad-local channel (`source_geo_class != kaliningrad_local`); local channels are kept for a separate future monitoring lane;
- the source looks like a nonlocal blogger/travel/media/personal source, not local/federal news or tours/ads;
- the text/vector gate does not reject the post as news, ad/promo, other-region travel, multi-region roundup, or low substance;
- RegionTalkImageDiagnostic has written actual-image scores (`image_model_input_type=actual_image`, `image_queue_status=actual_scored`);
- image scores pass publication thresholds (`REGION_TALK_PUBLICATION_MIN_OVERALL_MEDIA_SCORE`, default `0.66`; `REGION_TALK_PUBLICATION_MIN_POSTCARDNESS_SCORE`, default `0.55`);
- Gemini Lite final verification accepts the text criteria through the Supabase google_ai reserve/limiter.

`RegionTalkCandidateReport` owns discovery, E5, strict current-text E5+BGE
fusion, and the signed image-queue handoff. `RegionTalkImageDiagnostic` remains
the visual scorer and writer of actual-image evidence. The local
`scripts/region_talk_publication_finalizer.py` is the **single owner** of final
Gemini publication verification and `publication_candidate_item` terminal
outcomes in the orchestrated path; CandidateReport's Gemini modes are disabled
by the orchestrator.

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

The shared Supabase `google_ai_reserve` remains the cross-service RPM/TPM/RPD
authority. The Region Talk ledger is an extra product-run ceiling. Internal
provider retries are set to one attempt; retryable rows are retried by the
durable finalizer state instead.

## Local Telegram notification

Use `scripts/region_talk_goal_notify.py` locally, never on Kaggle. It uses
`TELEGRAM_AUTH_BUNDLE_E2E`/`TELEGRAM_SESSION` and sends unsent `llm_confirmed`
links to the operator chat. Delivery is deduplicated by canonical post URL plus
numeric chat id in `publication_delivery_item`. A deterministic Telegram
`random_id` is persisted before sending and reused after a crash; the notifier
also takes a local exclusive lock. After Telegram acceptance it records chat,
random id, message id and timestamps both in the delivery ledger and candidate
row. Finalizer/CandidateReport writers preserve these sent markers.

Invite links are checked without joining first. A one-time join requires the
explicit `--allow-join-chat` flag. `REGION_TALK_NOTIFY_CHAT_ID` can pin the
expected numeric peer and fail closed on a wrong target; the prepared chat is
currently pinned by default as `-5563945596`. Every notifier result,
including a zero-row dry run, reports the resolved numeric chat and delivery
account ids so operators can pin them without sending a test message.

Example:

```bash
python scripts/region_talk_goal_notify.py \
  --env-file /home/dev/projects/events-bot-new/.env \
  --chat 'https://t.me/+kfaIRh98oHVkYWFi' \
  --limit 20
```

For launch progress messages:

```bash
python scripts/region_talk_goal_notify.py --message 'Region Talk: CandidateReport run started …'
```

## Queue rules for future public publishing

- Max 4 posts per day total.
- Publish to both Telegram and VK where possible and allowed.
- Do not publish the same source more than once per day.
- Max 1–2 posts per source per 7 days.
- Prefer diverse sources and topics.
- Avoid same topic/location back-to-back.
- Candidates can expire.
- Dry-run mode is required.
- Auto-publish is disabled by default until explicitly configured.

Future publisher state machine: `pending → locked → published|failed|skipped|cancelled`.

Idempotency:

- same `candidate_id + target_platform` cannot publish twice unless explicit `force_republish` is set and logged;
- every publish attempt writes/updates queue state and then publication log;
- partial success must be visible (e.g. Telegram published, VK failed).

Telegram/VK publication cannot be transactionally rolled back with YDB. Deletion/edit can be attempted later, but ledger must preserve original API responses and deletion status.
