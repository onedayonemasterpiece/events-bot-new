# Current UI Decoder v1 — transport lane

## Scope

Implemented only the bounded AS-IS transport decoder lane in
`scripts/current_ui_resource_graph/v1/transport.mjs`. No Astro, CSS, runtime UI,
Penpot, token, merge/split, or normalization change was made.

## Delivered

- Separate reviewed surfaces for rail, bus, and Kaup; the legacy broad
  `family.transport` classification is rejected.
- Closed rail axes for outbound/return presence, `explicit`, `forecast`, and
  `schedule_cutoff`, estimated end, next-day return, and warning states.
- Closed bus axes for outbound/return group presence and estimated boarding.
- Closed Kaup axes for compact/regular, trip/no-trip, estimated/tight,
  public-return availability, official-transfer disclosure, production-off
  baseline, and controlled QA treatments.
- Exact marker allowlists and an explicit denial list for deployment endpoint
  and key attributes.
- Source-derived boundary pairs for rail, bus, Kaup, and the desktop consumer.
- Bounded state/specimen records, source-to-wrapper-to-treatment links, exact
  real-route representatives, and pinned structural-scan counts.
- Component-scoped capture requirements without retaining full HTML, endpoint
  values, bearer URLs, or keys.
- All records retain `decision: NOT_MERGED` and
  `normalization_allowed: false`.

## Evidence claim limits

- No browser specimen or private candidate corpus was run in this lane.
- No row emitted here claims `production-observed`.
- QA treatments are `controlled-specimen-only`, with their source
  implementations separately labelled `experiment-off`.
- `focus_group` and `live` enum modes remain `source-only` and do not enter the
  specimen plan.

## Verification

```text
uv run --with pytest==8.1.1 pytest -q --confcutdir=tests/fixtures \
  tests/test_current_ui_decoder_v1_transport.py
9 passed

node --check scripts/current_ui_resource_graph/v1/transport.mjs
git diff --check
```
