# Parser merge gate: fixed first-day production cohort

Requested follow-up to the November 5 evening review of PR 834. Read-only production ledger capture: November 6, 2026 at 14:08:23 UTC. No code, configuration, canonical Event, Job or provider writes.

## Window and result

The fixed window is November 5 14:03:00 inclusive through November 6 14:03:00 exclusive UTC (16:03 to 16:03 Europe/Kaliningrad). It contains 17 logical `merge_identity_gate` invocations over 16 distinct candidate states. Every invocation correlates with exactly one final decision using candidate, attempt, source and owner; missing or ambiguous correlations: zero.

| Final verdict | Invocations | Share of 17 |
| --- | ---: | ---: |
| FINAL_MATCH | 11 | 64.71% |
| FINAL_RETRY | 2 | 11.76% |
| FINAL_DISTINCT | 4 | 23.53% |

The two retries preserve native ticket conflicts: candidate 27006 / gate 19919 / owner 10506 uses fresh ticket 52541 against historical owner binding 52506; candidate 27008 / gate 19923 / owner 10578 uses fresh 52878 against historical 52616. Both reasons are `specific_ticket_occurrence_conflict`. These unresolved historical bindings are not grounds to relax the native guard or retime/replay either source.

## Exact FINAL_DISTINCT provenance

The following is persisted canonical/source/native provenance, not a new independent live DOM validation. Each terminal created Event has the candidate's exact native source anchor; the compared owner's distinct binding remains present.

| Gate / candidate | Compared owner | Candidate and terminal Event | Reason and distinction |
| --- | --- | --- | --- |
| 19921 / 27007 | 10577, «Горизонт памяти», November 21 15:00, ticket 52581 | «Архитектор: История Алексея Германа и его фильмов», November 21 15:00, ticket 52456 → 10606 | `specific_ticket_occurrence_conflict`: different title and ticket product in the same slot |
| 19926 / 27010 | 10598, «Искусство в объективе», festival November 20–22 | «Калининградская фотография», November 22 16:30, ticket 52621 → 10608 | `festival_sibling_not_same_event`: specific ticketed lecture versus festival umbrella |
| 19928 / 27011 | Same festival owner 10598 | «Остров перемен: до стекла и бетона», November 22 17:30, ticket 52626 → 10609 | `festival_sibling_not_same_event`: another specific lecture versus umbrella |
| 19930 / 27012 | 10609, preceding lecture, November 22 17:30, ticket 52626 | «Плоскость, обретающая объём», November 22 17:30, ticket 52631 → 10610 | `specific_ticket_occurrence_conflict`: different workshop and native product despite the same slot |

No same-native duplicate of the compared owner is established in these four cases. This does not certify the absence of duplicates elsewhere in production, and does not authorize cleanup of the old duplicate pairs or their shared public references.

## Interpretation and limits

- These are logical gate invocations, not unique Events, provider requests, tokens, retries within a provider call, or quota usage. No before/after provider-cost comparison was measured; 17 calls alone does not demonstrate quota harm.
- Exact-input no-op paths and unchanged parser rows bypass this gate. The separate attempt cohort contains 22 CREATED, 11 MERGED and two RETRY_SCHEDULED outcomes; it is not the denominator of the verdict percentages.
- Only production `parser:%` decision rows are included. Isolated shadow diagnostics are excluded. The closed window uses gate timestamps; a currently running parser may finish later. Final correlations are as observed at capture.
- Deployment boundary is recorded to the minute. The subsequent live qualification receipt is at November 5 14:04:21 UTC; no exact first-second exposure is asserted.
- The four distinct outcomes are supported by exact source identities. There is no evidence here requiring a new conditional gate, retry, normalizer or relaxation of native/date/identity protections.

## Inspectable retained evidence

Managed retained task directory: `/home/dev/artifacts/events-bot-new/20261102T233947Z-intake-recovery-20261103`.

- `parser-gate-24h-read.py` and `parser-gate-24h-1408.json`: fixed-window query, correlation and full 17-row cohort.
- `parser-gate-distinct-read.py` and `parser-gate-distinct-provenance-1410.json`: four candidate payloads, compared owners, terminal Events and exact source anchors.
- `guide-transactions-delivery-receipt.json`: prior deployment/live-check boundary.

No production behavior change is proposed by this measurement. The ongoing normal parser 11687 remains independent and must reach its own terminal receipt acceptance.
