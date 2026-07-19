# North transport lane results

## Scope

Restored the reviewed preferred-boarding contract for Romanovo buses that serve `Северный вокзал`, and applied it to both the generic bus suggestion and exact-venue KAUP suggestion without redesigning the KAUP component.

## Implementation

- Added route-level `preferred_boarding` data: North stop, Kaliningrad locality, estimated terminal `+15 min` offset and map URL.
- Added shared `busBoarding.ts` arithmetic. It keeps official terminal departures as provenance, displays the downstream North departure, subtracts the same offset from remaining ride time, and therefore keeps destination arrival invariant.
- Generic bus rows now expose raw terminal time in `data-terminal-departure`, display the estimated North time and use North as the public origin.
- KAUP options now retain raw `16:30` / `17:55`, display `≈16:45` / `≈18:10`, keep Romanovo at `≈17:35` / `≈19:00`, and keep KAUP at `≈18:28` / `≈19:53`.
- Corrected generated-page checks, canonical docs, changelog and incident regression contract.

## Validation

- Direct transport unit invocation: PASS (`tests/test_static_site_bus_boarding.py`).
- Preview build `preview-20260717t-static-event-v10-north-test`: PASS, 373 pages / 303 event pages.
- `npm run check:preview`: PASS, 303 events.
- `npm run check:production-desktop`: PASS, 303 event pages.
- `npm run check:bus-directory`: PASS, 17 localities / 26 venues / 21 stops.
- `git diff --check`: PASS.

The full pytest runner was unavailable in the base shell because its global environment lacks project dependency `aiogram`; the new isolated test itself was executed directly and passed.
