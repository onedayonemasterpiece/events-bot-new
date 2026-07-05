# LLM verifier contract — event-comment-feedback

Status: draft. The verifier is optional and group-level. It validates whether already selected phrase groups are safe to publish; it does **not** discover comments or generate public copy.

## Forbidden

- LLM per comment.
- LLM-generated public summaries.
- LLM-generated `public_sentence` outside phrase bank.
- LLM up-classification to stronger/riskier phrase ids.
- LLM confirming statistics it did not see.
- LLM making factual claims such as “tickets are sold out” from a question or rumor.

## Allowed decisions

- `approve`: candidate phrase is semantically supported and safe.
- `reject`: not supported, unsafe, too weak, sarcastic, spammy or overclaiming.
- `downgrade`: choose a safer allowed phrase id from a configured downgrade map.
- `needs_review`: human/manual review required.

The verifier can only choose from supplied phrase ids and allowed downgrade ids. It cannot invent text.

## Input shape

One request should verify up to 10–16 phrase groups. Group multiple events only if the prompt remains compact; target 3–8 events per call.

```json
{
  "schema_version": "event-comment-feedback-verifier-input-v1",
  "phrase_bank_version": "event-comment-feedback-phrase-bank-v1",
  "verifier_policy_version": "comment-feedback-verifier-policy-v1",
  "events": [
    {
      "event_id": "1842",
      "facts": {
        "title": "Название события",
        "date": "2026-07-20",
        "venue": "Площадка",
        "ticket_status": "available",
        "event_type": "concert",
        "short_description": "Короткое описание"
      },
      "candidate_groups": [
        {
          "phrase_id": "sold_out_disappointment",
          "public_sentence": "В комментариях расстраиваются, что билеты быстро закончились",
          "risk_class": "high",
          "tone": "concern",
          "stats": {
            "evidence_count": 3,
            "unique_authors_count": 3,
            "sources_count": 1,
            "confidence": 0.81
          },
          "representative_comments": [
            "нет билетов, очень жаль",
            "не успели купить"
          ],
          "risk_flags": ["ticket_status_conflict_possible"],
          "conflict_flags": ["canonical_ticket_status_available"],
          "allowed_downgrades": ["sold_out_discussion", "ticket_availability_question"]
        }
      ]
    }
  ]
}
```

Representative comments are internal verifier input only. They must be redacted, compact and never included in static export.

## Output shape

```json
{
  "schema_version": "event-comment-feedback-verifier-output-v1",
  "events": [
    {
      "event_id": "1842",
      "decisions": [
        {
          "phrase_id": "sold_out_disappointment",
          "decision": "downgrade",
          "approved_phrase_id": "sold_out_discussion",
          "risk": "medium"
        }
      ]
    }
  ]
}
```

`reason` is optional and stored only in YDB audit/debug. Production static output does not need verifier reasoning.

## Cache key

```text
hash(
  event_id,
  phrase_id,
  event_facts_fingerprint,
  evidence_fingerprint,
  phrase_bank_version,
  verifier_policy_version,
  model_id
)
```

Do not call verifier on cache hit. Static rebuild with unchanged evidence should make zero provider calls.

## LLM budget

- Max 10–16 phrase groups per call.
- Max 3–8 compact events per call.
- Max 1–2 calls for a very active event.
- Normal event should be 0–1 calls.
- Provider unavailability switches to degraded no-LLM mode: only high-confidence low-risk vector phrases may be newly published; medium/high-risk new groups are suppressed or kept as `needs_review`.

## Downgrade policy examples

| Candidate | Risk | Allowed downgrade | Reason |
|---|---:|---|---|
| `sold_out_disappointment` | high | `sold_out_discussion` | Comment evidence discusses availability but does not prove sold-out. |
| `high_demand_from_ticket_friction` | medium | `ticket_interest_high` | There is interest in tickets, but demand/friction signal is too strong. |
| `organizer_quality` | medium | `organizer_trust` or reject | Quality claim may rely on past event; avoid overclaim. |
| `artist_loved` | medium | `lineup_interest` | Artist-specific love unsupported, but program interest supported. |
| `accessibility_concern` | high | `accessibility_questions` | Concern not proven; questions are present. |

## Prompt guardrails

The verifier prompt must say:

- You validate aggregate support for fixed public phrases.
- You must not generate public sentences.
- You must not infer facts not present in event facts/evidence.
- Questions do not prove factual state.
- Sarcasm/negation should reject or downgrade.
- Spam/resale comments are not demand evidence.
- If unsure, use `needs_review` or `reject`.

## Acceptance gates

- No verifier output contains a new public phrase.
- No decision approves a phrase id absent from the supplied candidate/downgrade set.
- No per-comment LLM calls in logs/audit.
- `llm_calls_per_1000_comments` stays tied to phrase groups, not comment count.
