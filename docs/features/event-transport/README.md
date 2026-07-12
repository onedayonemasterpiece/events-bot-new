# Event transport guidance

> Status: **preliminary rail+bus presentation implementation consolidated in `integration/event-transport-schedule`; not yet in `origin/main`**. The original handoff is pushed at `dc46c348`; refresh merge `4577b334` incorporates `main@c6396331` without force-push, and draft PR [#37](https://github.com/onedayonemasterpiece/events-bot-new/pull/37) is the single integration review surface. Integration with the newly selected release UI remains required.

## Product contract

For eligible events outside Kaliningrad, the event page may show source-backed public-transport guidance:

- suitable outbound and return rail/bus options;
- schedule/source freshness and timezone;
- one summary transport card in the event content flow, with an optional generated gallery slide «Как добраться» governed separately;
- separate calendar action for an actual transport leg;
- no invented event duration or return feasibility;
- hide/fail safe when schedule evidence is missing or stale.

The preliminary branch now contains:

- source-backed rail blocks for the enabled Svetlogorsk/Zelenogradsk scope;
- an official-Avtovokzal-backed Romanovo/Сказочное Холмогорье bus block;
- prepared official rail and regional bus locality/venue directories for broader coverage;
- event and transport-leg ICS with bounded type-prefixed filenames and stable UIDs;
- static preview validators for visible rows, directories, artifact ceilings and orphan-free calendar output.

Prepared directory coverage is not the same as public schedule enablement. Gusev, Chernyakhovsk, Sovetsk, Ozyorsk, Zheleznodorozhny and other localities still require exact-date validated schedules/last-mile rules before their blocks may render.

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

## New release UI integration

The transport implementation is now a reusable feature slice, not the owner of the final page composition. The release-UI task must:

1. port the rail/bus blocks and transport-leg calendar actions into the chosen immutable UI baseline without duplicating schedule logic/data;
2. keep the full schedule block as the accessible canonical surface and separately prototype the optional generated gallery slide «Как добраться» without forking schedule selection;
3. retain fail-closed matching, source/date freshness, unknown-end warnings, keyboard/touch targets and no-JS/static fallback;
4. run the same transport directory/preview/ICS validators after the UI merge;
5. prove mobile/desktop visual baselines for an enabled rail event, enabled bus event, unsupported locality and stale/partial schedule;
6. if the gallery candidate is included, prove at most one derived card after genuine media, SVG/WebP+CDN compliance, no hero/OG/JSON-LD use and complete no-card fail-closed cases.

Until that integration is signed off, the preliminary block must not be copied into another UI branch as forked markup or a second schedule selector.

## Branch governance

Keep `integration/event-transport-schedule` as the single transport integration source. Its former dirty-WIP blocker is closed: `dc46c348` was pushed, `main@c6396331` was merged without force-push as `4577b334`, the directory/full-preview gates passed, and draft PR [#37](https://github.com/onedayonemasterpiece/events-bot-new/pull/37) exposes both the reusable transport slice and the explicit automatic-refresh/new-UI blockers. Historical presentation QA belongs in reports; do not create another competing transport implementation branch.

## Related documentation

- [Static event pages](../static-site-pages/README.md)
- [Favorites/calendar](../event-favorites-calendar/README.md)
- [Kaggle static-site builder](../../operations/kaggle-static-site-builder.md)
- [Optional gallery card «Как добраться»](gallery-how-to-get-there-card.md)
- [Detailed preliminary renderer/schedule contract](../static-site-pages/event-transport-schedule.md)
- [Rail multimodal directory](../static-site-pages/rail-multimodal-directory.md)
- [Bus transport directory](../static-site-pages/bus-transport-directory.md)
