# LLM/VLM verifier and post-writer contract

Status: implemented origin-aware contract. Use Gemini Flash-Lite / current configured lite/flash-lite model via env for final verifier/post writer. Do not call it for every post.

The external-publication research and intake contract is canonical in
[`external-publications.md`](external-publications.md). Final-verifier policy
`region_talk_final_verifier_v7_grounded_draft` selects rules by
`content_origin_type` and, only for an accepted row, produces the first
operator-reviewable Telegram/VK draft in the same grounded call:

- `external_social` keeps the firsthand visit/subscriber report and
  emotion/review requirements;
- `editorial_publication|academic_publication` instead require attributed
  original analysis/research or expert basis, a concrete broad-reader insight
  and a memorable/useful detail. They do not require a first-person visit;
- external publications with `sharp_negative_region_image=true` cannot be
  accepted. Constructive-neutral problem analysis may pass only when balanced,
  evidence-based and useful;
- news/politics/military/ad/local-source and main-subject gates remain common
  fail-closed exclusions.

## Call policy

Call verifier only for:

- top candidates entering `favorites`/report;
- candidates being queued;
- every candidate before any future autopublish.

Never call verifier for the raw corpus. Cache by `post_id`, `text_hash`, selected media hashes, `semantic_bank_version`, `verifier_policy_version` and `model_id`.

Each verifier call must be bounded. CandidateReport sets
`REGION_TALK_LLM_CALL_TIMEOUT_SECONDS=60` by default and propagates the same
value to `GOOGLE_AI_PROVIDER_TIMEOUT_SEC` unless an explicit provider timeout is
already configured. If the Gemini/Lite call exceeds that window, the row is
recorded as `llm_gate_status=error` with a timeout reason and the notebook must
continue to write YDB/report state instead of hanging without new heartbeats.
The prompt is deliberately slim: `REGION_TALK_LLM_PROMPT_TEXT_MAX_CHARS`
defaults to 1800, falls back from `text` to `text_excerpt`/summary fields when
row-level YDB image rows do not carry full text, and sends only the compact
image/vector evidence fields required by the final decision.
The v7 wire contract uses compact enum/field notation so a social prompt with
the configured 900-character test excerpt remains below 3.5 KB; this changes
no keys, enum members, grounding rules or acceptance semantics. A proportional
UTF-8 byte cap (`REGION_TALK_LLM_PROMPT_TEXT_MAX_BYTES`) complements the
character cap so Cyrillic input cannot silently double the complete payload.
When actual-image evidence is missing, the event name is
`final_verifier_deferred_until_image_scoring` and its payload must make clear
that this is non-blocking (`blocking_wait=false`, `llm_calls=0`,
`next_action=run_region_talk_image_diagnostic`).

The v7 verifier+draft response defaults to a bounded 1200 output tokens
(`REGION_TALK_LLM_MAX_OUTPUT_TOKENS`, clamped to `700..1800`) so both platform
drafts and claim/support evidence fit without turning the top-candidate call
into an unbounded writer request.

## Input

- source metadata and `rights_policy`;
- original post URL;
- text excerpt / normalized text, not unbounded raw payload;
- selected media thumbnails/URLs or local image files;
- image scoring report;
- semantic matches and candidate score;
- target platforms (`telegram`, `vk`);
- desired tone: concise, careful attribution, no hype.

## Tasks

1. Confirm the post is about Kaliningrad Oblast.
2. Confirm it is not news/trash/politics/incident/ad-only.
3. Confirm source is suitable and external/mixed.
4. Confirm selected images are strong enough and safe.
5. Confirm summary does not overclaim.
6. Extract positive, neutral/useful and constructive concern points.
7. Decide publication readiness.
8. Draft platform-specific text for future Telegram/VK use.

For editorial/academic origin, task 4 evaluates a direct article image only as
visual evidence under `score_only_no_reuse`; it never converts `link_only`
rights into media-reuse permission. Task 6 must preserve scientific scope,
uncertainty and limitations when applicable.

## Output JSON

```json
{
  "decision": "approve|reject|needs_review",
  "publication_readiness": "report_only|manual_review|ready_for_queue|ready_for_autopublish",
  "reason_short": "...",
  "region_relevance_confirmed": true,
  "non_news_confirmed": true,
  "media_strong_confirmed": true,
  "evidence_or_expert_basis": true,
  "public_interest_insight": true,
  "sharp_negative_region_image": false,
  "rights_warning": null,
  "positive_points": ["..."],
  "neutral_points": ["..."],
  "concern_points": ["..."],
  "telegram_text": "...",
  "vk_text": "...",
  "title_short": "...",
  "source_attribution": "...",
  "image_report_short": "...",
  "risk_flags": []
}
```

The durable YDB projection uses flat fields
`publication_draft_status`, `publication_draft_title`,
`publication_draft_source_attribution`,
`publication_draft_telegram_text`, `publication_draft_vk_text`,
`publication_draft_fact_points_json` and
`publication_draft_prompt_version`. `fact_points` contains one to three
`claim → support_excerpt` pairs derived from the supplied source text. The
deterministic normalizer always appends explicit `Источник:` and `Оригинал:`
lines and the canonical URL; it does not invent missing body text. A complete
result is `ready_for_operator_review`, never `ready_for_autopublish`.

Rejected/review rows store no draft. A partial accept response becomes
`needs_grounding_review` and is excluded from any future target-channel
publisher until an explicit writer/review pass completes. CandidateReport and
the finalizer preserve the draft fields across later asynchronous refreshes.

## Writing rules

- Do not copy full original text.
- Do not invent facts.
- Do not imply source endorsed our channel.
- Always include source attribution and original link.
- Keep useful concerns when present, but do not sensationalize.
- Avoid “лучший”, “все в восторге” and similar overclaims unless directly justified, and still prefer restrained wording.
- A generated draft is a review artifact, not permission to reuse source media
  or publish it automatically.
