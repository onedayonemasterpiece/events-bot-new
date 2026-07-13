# Probe / evaluation plan — event-comment-feedback

Status: draft. First implementation script name: `scripts/probe_event_comment_feedback.py` (documented only in MVP-0; not implemented here).

Prerequisite: the [Region Talk reuse audit and skill-first gate](region-talk-reuse-audit.md) is accepted. The stale F14 probe branch may supply test evidence, but it is not the integration base.

## Goal

Validate whether fixed phrase-bank vector matching plus group-level verifier can produce safe, specific, useful public comment-discussion signals for static event pages.

## Inputs

- Production/static event snapshot with future active events.
- `EventSource`/source URL snapshot and legacy `Event.source_post_url/source_vk_post_url` fallback.
- Small manually curated comment fixtures and/or limited real public comments from approved sources.
- Phrase bank v1 with prototypes and hard negatives.
- Optional verifier cache/model only for medium/high-risk groups.

## Probe output

Each run should emit a JSON/Markdown report containing:

- `run_id`;
- event id, title, date, venue;
- source URLs/platform keys;
- `comments_seen_count`, `comments_used_count`;
- candidate phrase groups;
- proposed public items;
- representative internal evidence snippets (redacted, not public export);
- vector scores: positive, negative, next-best, margin;
- risk flags and conflict flags;
- LLM/cache decision when applicable;
- suppression reason;
- final static JSON preview for publishable items.

Artifacts belong under `artifacts/codex/<run_id>/` unless a curated summary is intentionally promoted into docs.

## Metrics

| Metric | Meaning |
|---|---|
| `phrase_precision` | Share of proposed phrase groups judged correct. |
| `false_positive_rate` | Unsupported phrase publications / proposed publications. |
| `unsafe_phrase_publication_rate` | Privacy/compliance/factual-overclaim failures. |
| `overclaim_rate` | Stronger wording than evidence supports. |
| `sarcasm_failure_rate` | Sarcasm/negation accepted as positive evidence. |
| `ticket_conflict_rate` | Ticket/sold-out conflicts not downgraded/suppressed. |
| `spam_resale_leak_rate` | Resale/spam counted as demand evidence. |
| `llm_calls_per_1000_comments` | Cost scaling guard. |
| `llm_calls_per_event_with_feedback` | Practical event-level cost. |
| `verifier_cache_hit_rate` | Cache effectiveness. |
| `vector_only_share` | Share of items published without LLM. |
| `red_phrase_precision` | Precision for concern/high-risk/red items. |

## Suggested acceptance gate

- `unsafe_phrase_publication_rate = 0`.
- Red/high-risk phrase precision `>= 0.90` on manual review.
- Green/gray low-risk phrase precision `>= 0.80–0.85`.
- No public raw comments.
- No public author names/user ids.
- No generated `public_sentence` outside phrase bank.
- No per-comment LLM architecture.
- No public page runtime dependency on YDB/LLM/VK/TG.
- `llm_calls_per_1000_comments` reflects group-level verification, not raw comment volume.

## Probe stages

1. **Region Talk transfer fixtures**: adapt the accepted failure corpus for stable identity/dedup, stale overlay, cursor retry, false-green run, cooldown, vector false positive, raw-text leak and delivery reconciliation.
2. **Fixture smoke**: run on hand-written comments covering every risk class, ticket conflicts, sarcasm, resale spam and practical questions.
3. **Golden events**: choose 20–40 future events with known active TG/VK source discussions.
4. **Vector-only audit**: disable verifier and confirm only low-risk strict phrases publish; compare one- vs dual-model cost/quality only if the adoption matrix marks dual-model evaluation relevant.
5. **Verifier audit**: enable verifier for medium/high-risk groups and inspect downgrade/reject behavior.
6. **YDB/funnel audit**: verify online row identity, compact state, terminal/retry/freshness metrics and that a heartbeat without accepted public state cannot be green.
7. **Static export audit**: validate manifest schema, forbidden-field absence, changed-hash handoff and checked build delivery.
8. **Human review**: manually review top N proposed items and all red/high-risk items.
9. **Cost/cache report**: rerun unchanged evidence and require zero provider calls on cache hit.

## Manual review rubric

For each proposed item answer:

- Is the public sentence supported by multiple comments?
- Is it too strong, too vague or factual when evidence is only a question?
- Is there sarcasm/negation?
- Are spam/resale comments excluded?
- Does it expose private info or a quote?
- Does canonical event data contradict the phrase?
- Would a user understand this as discussion context, not a review/rating?

## Stop conditions

- Any raw comment/name/user id appears in public manifest.
- Any verifier generates a new public phrase.
- Any implementation path calls LLM per comment.
- Any public page needs YDB/LLM/VK/TG at view time.
- Sold-out/high-risk phrases are approved without verifier/manual review.
