# Lane qtickets_occurrence Results

> Integration correction: the worker result below records its original lane
> output. Final integration removed the arbitrary 14-day cutoff. New Qtickets
> parser dumps treat every explicit cross-date JSON-LD product boundary as
> `vendor_schedule_end_date`, while the existing LLM may derive a genuine
> continuous multi-day `end_date` from explicit prose without another call.
> Legacy dumps without the marker are preserved rather than heuristically
> reclassified. Exact-attach occurrence/source-fact and pair-correlated ledger
> coverage were also added after this lane. See the integration report.

## Status

committed

## Requirement IDs

- R2 — Qtickets occurrence identity contract

## Branch

`agent/august-repair-contracts/qtickets-occurrence`

## Worktree

`/home/dev/.codex/worktrees/events-bot-new/sol-contracts-qtickets`

## Base SHA

`0af3f0f8b4673417853a5cd2b6be13b67b21fded`

## Implementation Head SHA

`9e3000ab72df52dc71dc8b980f85366aceda9078`

The final lane head also contains the reporting-only commit for this file.

## Outcome

- Qtickets URLs are now product/series recall signals, not occurrence-global identity.
- Parser occurrence keys use canonical product URL plus concrete date/time for Qtickets; schedule-end and producer-order drift do not change a slot key.
- Same product + same date/time remains replay/match eligible; different date or separately sold time is structurally distinct.
- The semantic adjudicator remains the normal-path decision owner. Its existing call count is unchanged; a deterministic post-verdict safety rail rejects only an impossible Qtickets date/time merge.
- Long Qtickets JSON-LD ranges are separated into `vendor_schedule_end_date`; this metadata is retained in source text, candidate state fingerprinting, and `EventSourceFact` notes instead of extending a one-day occurrence.
- Short real multi-day ranges remain `end_date`; the FLAVA 2026-07-04..07 fixture proves this preservation.
- Contradictory August corpus entries were moved from same-event positives to `distinct_occurrence` hard negatives, including the branded Baltic Odyssey Qtickets series.
- No provider, production DB, content, or social mutation was performed.

## Files Changed

- `kaggle/ParseQtickets/parse_qtickets.py`
- `source_parsing/qtickets.py`
- `source_parsing/parser.py`
- `source_parsing/handlers.py`
- `smart_event_update.py`
- `smart_update_identity.py`
- `tests/test_qtickets_occurrence_contract.py`
- `tests/test_qtickets_structured_facts.py`
- `tests/test_ingestion_completion_regressions.py`
- `tests/test_smart_update_identity_incident_replay.py`
- `tests/replays/INC-2026-08-22-sos-dedup-veto-location-tyunin-farm/dedup_cases.json`
- `.codex/lanes/qtickets_occurrence/RESULTS.md`

## Commands Run

1. Red test before implementation:
   - `/home/dev/.venvs/events-bot-image-geometry/bin/python -m pytest -q tests/test_qtickets_occurrence_contract.py`
   - Result: **2 failed, 5 passed**.
   - Failures proved schedule/order drift changed the occurrence key and vendor schedule end leaked into `event.end_date`.
2. Dedicated lane suite after implementation:
   - `/home/dev/.venvs/events-bot-image-geometry/bin/python -m pytest -q tests/test_qtickets_occurrence_contract.py`
   - Result: **10 passed**.
3. Historical classification and adversarial verdict gate:
   - targeted August positive/negative corpus, adversarial SAME_EVENT rejection, and corpus manifest tests.
   - Result: **20 passed**.
4. Full directly relevant regression set:
   - `/home/dev/.venvs/events-bot-image-geometry/bin/python -m pytest -q tests/test_qtickets_occurrence_contract.py tests/test_qtickets_structured_facts.py tests/test_ingestion_completion_regressions.py tests/test_smart_update_identity_incident_replay.py tests/test_source_parsing_existing_parser_attach.py tests/test_smart_update_occurrence_stability.py tests/test_smart_update_candidate_state_keys.py`
   - Result: **84 passed**.
5. Syntax/static validation:
   - `/home/dev/.venvs/events-bot-image-geometry/bin/python -m compileall -q source_parsing/qtickets.py source_parsing/handlers.py source_parsing/parser.py smart_event_update.py smart_update_identity.py kaggle/ParseQtickets/parse_qtickets.py tests/test_qtickets_occurrence_contract.py`
   - `git diff --check`
   - Result: **passed**.

## Tests / Verification Coverage

- Historical pairs `7604/7707`, `7580/7833`, and `7805/8253` have distinct occurrence keys and executable distinct/create outcomes.
- Exact replay of both occurrences returns `NOOP_EXACT_REPLAY` and creates no duplicate row.
- Same URL/date/time can match; same URL/different date cannot match a legacy polluted range.
- Same Qtickets product with different date/time rejects even an adversarial high-confidence LLM `SAME_EVENT` verdict.
- Historical qTickets corpus decisions emit `FINAL_DISTINCT` through the existing adjudicator path.
- Dedicated provider counter proves the occurrence/replay path adds **zero** model calls; the historical adjudicator test still uses exactly its existing single semantic call.
- Product schedule metadata is persisted as two source-fact notes for two independent occurrences.
- True short multi-day Qtickets range remains an event range.

## Risks

- The parser uses a bounded 14-day structural cutoff to distinguish a plausible multi-day occurrence from a long product schedule/sales horizon when handling legacy or current JSON-LD. The source text still labels the separated value explicitly for the existing semantic extraction pass. A future legitimate Qtickets event longer than 14 days would require source-contract evidence or a native occurrence-duration field rather than treating the product horizon as canonical duration.
- Older accepted EventSource rows without occurrence keys are not migrated by this lane; the repaired production manifest and subsequent normal reingestion own data convergence.
- Canonical docs and `CHANGELOG.md` were forbidden in this worker lane and remain integration-owner responsibilities.

## Merge Notes

- Cherry-pick implementation commit `9e3000ab72df52dc71dc8b980f85366aceda9078`, then the following report commit.
- No schema migration is required; added fields are in-memory candidate/parser metadata and existing JSON/source-fact storage.
