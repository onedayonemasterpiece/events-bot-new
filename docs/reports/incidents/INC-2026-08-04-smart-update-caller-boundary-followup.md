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

A `not_accepted` result moves the candidate match from `event_id` to the
non-authorizing `matched_event_id`. Existing callers therefore stop before
their success path. The append-only identity decision log remains the durable
source of evidence. Unknown future statuses fail closed until explicitly added
to the contract.

This is a safety boundary, not the final operator UX. A later caller migration
must consume the typed outcome directly and route `review_required` into durable
review instead of a generic failed/deferred state.

## LLM-first invariant

Semantic identity remains an LLM-first decision. Deterministic code adds only
one narrow impossibility rail: two different explicit ticket occurrence
identities cannot be the same event card, even if the model returns
`allow_merge`.

Generic ticket landing pages, similar titles, venues and event types remain
semantic evidence for the LLM; they are not converted into deterministic merge
rules.

## Legacy source ownership

A canonical identity-bearing source cannot be attached to a second event.
Unclassified legacy (`source_role IS NULL` or blank) ownership on another event
now forces review instead of being silently taken over. It is not
mass-classified.

## Deferred architecture work

The following is deliberately not mixed into this P0 patch:

- explicit source role from every producer;
- a first-class multi-event source envelope;
- durable review state in VK/parser callers;
- raw-packet vs derived-candidate fingerprints;
- exhaustive caller migration away from the compatibility `event_id` boundary.
