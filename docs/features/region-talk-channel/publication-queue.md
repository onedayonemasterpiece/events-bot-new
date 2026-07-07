# Publication queue

Status: MVP product-goal queue. Auto-publishing to public Telegram/VK remains disabled; this queue is for human/product review and local operator notifications.

## Product criteria

A post can enter `publication_candidate_item` only when all of the following are true:

- the main subject is Kaliningrad Oblast (`kaliningrad_oblast_only_scope=true`, mention role `main_subject|unclear`);
- the source is not a pure Kaliningrad-local channel (`source_geo_class != kaliningrad_local`); local channels are kept for a separate future monitoring lane;
- the source looks like a nonlocal blogger/travel/media/personal source, not local/federal news or tours/ads;
- the text/vector gate does not reject the post as news, ad/promo, other-region travel, multi-region roundup, or low substance;
- RegionTalkImageDiagnostic has written actual-image scores (`image_model_input_type=actual_image`, `image_queue_status=actual_scored`);
- image scores pass publication thresholds (`REGION_TALK_PUBLICATION_MIN_OVERALL_MEDIA_SCORE`, default `0.66`; `REGION_TALK_PUBLICATION_MIN_POSTCARDNESS_SCORE`, default `0.55`);
- Gemini Lite final verification accepts the text criteria through the Supabase google_ai reserve/limiter.

The final queue is produced by `RegionTalkCandidateReport`, because it is the only notebook that has the full text/vector/source/YDB state and can join the image scores written by `RegionTalkImageDiagnostic`. ImageDiagnostic remains the visual scorer and writer of image evidence.

## Ranking

`publication_score` combines:

- actual image quality / visual score;
- postcardness;
- text-story/emotion/usefulness evidence;
- nonlocal source value;
- a diversity penalty for same source/place/content overlap with already confirmed candidates.

This is an MVP anti-overlap penalty; it is intentionally isolated so it can later be replaced by nearest-neighbour vector diversity over confirmed/published posts.

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

## Local Telegram notification

Use `scripts/region_talk_goal_notify.py` locally, never on Kaggle. It uses `TELEGRAM_AUTH_BUNDLE_E2E`/`TELEGRAM_SESSION` and sends unsent `llm_confirmed` `publication_candidate_item` links to the operator chat, then marks rows as `sent_to_chat` in YDB.

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
