# Event transport guidance

> Status: **presentation candidate in `integration/event-transport-schedule`; not in `origin/main`**. The branch has active uncommitted WIP and must not be updated or merged until that work is committed/pushed or explicitly discarded.

## Product contract

For eligible events outside Kaliningrad, the event page may show source-backed public-transport guidance:

- suitable outbound and return rail/bus options;
- schedule/source freshness and timezone;
- one summary transport card in the event media/content flow;
- separate calendar action for an actual transport leg;
- no invented event duration or return feasibility;
- hide/fail safe when schedule evidence is missing or stale.

Current branch scope is narrower than the release requirement: rail examples for Svetlogorsk/Zelenogradsk and one Romanovo bus case. Gusev, Chernyakhovsk, Sovetsk, Ozyorsk, Zheleznodorozhny and the full provider/city matrix remain open.

## Ownership

- Fly SQLite owns canonical event/city/time facts.
- The transport refresh pipeline owns normalized source snapshots/history; its final storage lane must be explicitly selected before implementation.
- Static builder consumes an immutable validated transport snapshot and generates HTML/JSON/ICS.
- Browser runtime is not a journey planner and does not call schedule providers.

## Release blocker

A manually reviewed committed snapshot is insufficient for the public presentation. Required production path:

1. nightly/manual Kaggle `transport_schedule_refresh` with resource lease and status ledger;
2. source URL, fetched/effective time, timezone, route/trip/stop identity and service-calendar validation;
3. bounded-diff checks and empty/partial fail-safe;
4. atomic last-known-good publication and stale-age alert;
5. one coalesced static rebuild when validated content hash changes;
6. release manifest records transport snapshot id/hash/time;
7. full city/provider, weekday/weekend/holiday and unknown-end acceptance matrix.

## Branch governance

Keep `integration/event-transport-schedule`; after its current dirty WIP is safely committed/pushed, merge `origin/main` into it without force-push, run feature tests/build/check and open a PR. Move the canonical contract to this feature home during integration; historical presentation QA belongs in reports.

## Related documentation

- [Static event pages](../static-site-pages/README.md)
- [Favorites/calendar](../event-favorites-calendar/README.md)
- [Kaggle static-site builder](../../operations/kaggle-static-site-builder.md)
