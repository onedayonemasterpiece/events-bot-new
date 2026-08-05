# INC-2026-08-04: caller outcome boundary follow-up

## Scope

This focused follow-up closes the production corruption path left after PR #334:

1. Smart Update may return `review_required` or `skipped_identity_gate` with the
   matched Event ID for operator evidence.
2. Legacy callers treated any non-null Event ID as success.
3. Those callers could then update ticket fields, rebuild Telegraph, recompute
   linked events, enqueue publication work, or mark a VK inbox row imported.

## Contract

The public Smart Update boundary classifies every result as:

- `accepted_changed`: `created`, `merged`;
- `accepted_no_change`: `skipped_nochange`, `skipped_same_source_url`,
  `noop_exact_source_replay`;
- `not_accepted`: every other or unknown status.

A `not_accepted` result may retain `event_id` as diagnostic evidence, but
callers must classify the outcome before using that ID. The official parser and
VK persist paths now return or raise before ticket mutation, linked-event
recompute, Telegraph rebuild, publication scheduling or successful import
state. Unknown future statuses fail closed until explicitly added to the
contract.

This is a safety boundary, not the final operator UX. A later caller migration
must consume the typed outcome directly and route `review_required` into durable
review instead of a generic failed/deferred state.

## LLM-first invariant

Semantic identity remains an LLM-first decision. Deterministic code adds only
one narrow impossibility rail: two different explicit occurrence identities
issued by the same ticket vendor cannot be the same event card, even if the
model returns `allow_merge`.

Cross-vendor ticket IDs may describe the same event and remain an LLM decision.
Generic ticket landing pages, similar titles, venues and event types also remain
semantic evidence for the LLM; they are not converted into deterministic merge
rules.

## Legacy source ownership

A canonical identity-bearing source cannot be attached to a second event.
Unclassified legacy (`source_role IS NULL` or blank) ownership on another event
now forces review instead of being silently taken over. It is not
mass-classified.

## Validation

Focused contract tests cover:

- parser `review_required` with a diagnostic Event ID and zero caller-side
  ticket, linked-event, Telegraph or publication effects;
- VK identity rejection that cannot become a successful persisted import;
- same-vendor conflicting ticket occurrences overriding an erroneous LLM
  `allow_merge`;
- cross-vendor ticket IDs remaining LLM-first;
- legacy unknown source ownership forcing review while `context_only` remains
  shareable;
- unknown future result statuses failing closed.

The focused test suite, Python compilation and `git diff --check` passed before
the final product commit.

## Deferred architecture work

The following is deliberately not mixed into this P0 patch:

- explicit source role from every producer;
- a first-class multi-event source envelope;
- durable review state in VK/parser callers;
- raw-packet vs derived-candidate fingerprints;
- exhaustive caller migration away from the compatibility `event_id` boundary.
