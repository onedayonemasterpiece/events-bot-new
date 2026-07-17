# Event transport guidance

> Status: **rail+bus slice is integrated with the selected desktop/mobile release UI in `integration/static-site-transport-mobile-real-events-20260715`; production-root promotion is still pending**. The original transport history remains in draft PR [#37](https://github.com/onedayonemasterpiece/events-bot-new/pull/37), but the current acceptance surface is the full-catalog preview documented below.

## Product contract

For eligible events outside Kaliningrad, the event page may show source-backed public-transport guidance:

- suitable outbound and return rail/bus options;
- schedule/source freshness and timezone;
- one summary transport card in the event media/content flow;
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

Completed in the 2026-07-15 integration candidate:

- one shared rail/bus selector and renderer, without forked lab markup;
- placement after `Коротко` facts in both desktop and mobile reading flows;
- responsive container-query composition reviewed by Gemini 3.1 Pro (High);
- current-data build with `282` public future/ongoing events and static pgvector
  related navigation;
- public rail event `5789` and bus event `6710` accepted through Playwright on
  desktop and mobile;
- transport and event `.ics` public MIME/HTTP checks passed.

Review URL:
<https://kenigevents.ru/preview-20260715t-production-transport-mobile-real-events-v1/__preview/>.

The remaining P0 is the automated schedule refresh/last-known-good release
lane below; the UI integration itself is no longer the blocker.

The integration retains the original acceptance invariants: no duplicated
schedule logic/data, fail-closed matching, source/date freshness, unknown-end
warnings, keyboard/touch targets, no-JS/static fallback and the same
directory/preview/ICS validators. Future UI work must reuse this slice rather
than copy the markup or create another selector.

## Branch governance

`integration/event-transport-schedule` and draft PR [#37](https://github.com/onedayonemasterpiece/events-bot-new/pull/37)
are historical source/handoff references. The combined review branch is
`integration/static-site-transport-mobile-real-events-20260715`; merge only the
combined branch after acceptance, and do not create a competing transport
implementation.

## Related documentation

- [Static event pages](../static-site-pages/README.md)
- [Favorites/calendar](../event-favorites-calendar/README.md)
- [Kaggle static-site builder](../../operations/kaggle-static-site-builder.md)
- [Detailed preliminary renderer/schedule contract](../static-site-pages/event-transport-schedule.md)
- [Rail multimodal directory](../static-site-pages/rail-multimodal-directory.md)
- [Bus transport directory](../static-site-pages/bus-transport-directory.md)
