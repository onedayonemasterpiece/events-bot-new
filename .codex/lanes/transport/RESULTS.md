# Transport lane results (R04)

## Scope restored

- Preserved the three accepted treatments from `1b4f2ccc` without redesign:
  `departure_board_v1`, `route_strips_v1`, `next_departure_queue_v1`.
- Replaced the rejected legacy compact-list fallback with the accepted departure-board treatment. This fallback now covers off mode, no JavaScript, automation, elapsed schedules and failed/ineligible assignment; it can also be selected as arm A without hiding itself.
- Retained stable SHA-256 browser-subject allocation, session query override `ke-exp-transport`, exposure/qualified-action analytics, the full 1–20 trip range and the Kaliningrad North boarding-time projection.
- The one-trip `next_departure_queue_v1` treatment no longer says “Ещё 0 рейсов”; multi-trip treatments keep bounded disclosure.
- Corrected official-transfer copy to cover both an autobus and a minibus.

## Evidence

- `pytest`: `10 passed` for `tests/test_static_site_transport_experiment.py` and `tests/test_static_site_bus_boarding.py`.
- Full Astro build: `375 page(s) built`, including production event `4671`.
- Built event `4671` contains both desktop/mobile instances of all three treatment markers, visible accepted board fallback, `data-experiment-mode="qa"`, official-transfer autobus/minibus copy and the North boarding projection.
- Local Playwright at `390×844`:
  - forced `departure_board_v1`: rendered/visible arm A, zero horizontal overflow;
  - forced `route_strips_v1`: rendered/visible arm B, 2 trips, zero horizontal overflow;
  - forced `next_departure_queue_v1`: rendered/visible arm C, 2 trips, zero horizontal overflow;
  - no override on elapsed event: accepted departure-board fallback, zero horizontal overflow.
- Local Playwright at `1366×900`: forced route strips visible on both event surfaces, zero horizontal overflow.

No documentation, changelog, content-repair or media-router files were changed in this lane.
