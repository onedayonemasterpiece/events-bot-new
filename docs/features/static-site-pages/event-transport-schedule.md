# Static event transport schedule

> Status: data/validation/ICS foundation ready for controlled canary; old PR #37 UI is not part of this implementation and production scheduling remains off.

## Static consumer boundary

The static builder may consume only the immutable manifest named by `combined/current.json`. It must not read a half-written provider output, call a provider in Astro/browser runtime, or fall back to random routes by city.

Eligibility and selection are build-time, fail-closed checks implemented in `transport_refresh/selection.py`:

1. reject Kaliningrad and events without named venue coordinates;
2. require an exact local service date equal to the event occurrence date;
3. require exact reviewed city and venue name/alias binding (and event id when restricted);
4. keep outbound services arriving before the start;
5. keep return services departing after an explicit end;
6. when the end is unknown, do not promise a return;
7. return at most two deterministic options per direction.

This foundation does not own the final event-detail composition. A later presentation lane may consume these results without changing their validation boundary.

## Calendar boundary

`transport_refresh/ics.py` separates:

- occurrence-specific event ICS: event start/end, event venue, event UID;
- transport ICS: one selected leg, exact stops and times, route/provider context, stable transport UID and `VALARM` 30 minutes before departure.

Only a selected visible transport action should create a transport ICS. A transport file must never replace the event calendar file.

## Controlled nightly schedule (documented, not active)

After a successful controlled canary, the proposed UTC schedule is:

| Time | Job | Lease |
|---|---|---|
| `01:10` | KPPK refresh | `transport_schedule:kppk:refresh` |
| `01:20` | bus refresh | `transport_schedule:bus:refresh` |

Each waited runner downloads and server-validates its result. Fan-in after either run is safe because it reads the other provider's fresh last-good. The static rebuild coalesce key is always `static_site_build:prod` and is used only on a changed combined content hash.

Do **not** add these jobs to `scheduling.py`, cron or `fly.toml` until the canary checklist in [Event transport guidance](../event-transport/README.md#production-gate-and-rollback) passes with real sources.

## Acceptance matrix

Contract tests cover:

- independent provider contracts and provider-specific mode;
- exact-date/timezone, route, stop and binding validation;
- outside-Kaliningrad and no-random-Kaliningrad selection;
- arrival before start and promised return after explicit end;
- named stop/venue coordinates;
- distinct event/transport ICS and transport alarm;
- timeout, empty, invalid, stale last-good and recovery;
- immutable deterministic fan-in;
- changed hash → one coalesced rebuild callback; unchanged hash → zero;
- status heartbeat/resource-lease/report instrumentation in both Kaggle jobs.

The tests use synthetic explicit-date records. They are not evidence that either live provider adapter has passed canary.
