# Home finite feed — shared ranking adapter

`HomeColdStartFeed` v2, variant `shared-ranked-finite-feed`, is the home consumer of
`AdaptiveEventCardGrid` / `EventCard` / `MediaFrame`, not a separate recommender.
The page passes the entire eligible, occurrence-collapsed pool. The consumer uses
`toPersonalFeedCandidate`, the strict `parseLegacyProfileV1` contract and existing
`legacyRankEventDetailRelatedV1` diversity for the static general order. A compatible
profile with at least three shared explicit signals activates
`legacyRankPersonalFeedV1` **over the full pool before the 30-card budget**. These
quarantined legacy weights are an applied compatibility baseline, not a claim that
the target/shadow presenter or personalization quality has been accepted.

SSR emits up to 30 ordinary shared cards and the full display/feature manifest.
No-JS and unavailable shared materializer preserve the general SSR cards. Hydration
uses the existing `KenigEventsCreateEventCard` and common framing planner; the
shared card-host port registers home candidates and served-list identity for the
same profile, action, calendar/share and logging owners as other shared cards.
No home fetch, provider, cloud profile, feedback store, CSS card geometry or
pagination is introduced. General label is «Общая подборка»; personal label is
«С учётом ваших интересов», only after a profile-ranked suffix is actually applied.

The observed prefix is immutable under later ranking signals. Only its unseen
suffix is reconciled. Explicit hidden/not-interested IDs expand through shared
`occurrence_member_ids`; hidden slots retain their identity and exact position so
Undo can restore them, while replacement cards and Undo never exceed 30 visible
cards. The common grid owns diagnostics/reflow; the home runtime publishes
`data-home-stable-prefix="true"` only after a successful shared-host reconciliation.
Neither Hero scene animation nor a live scene change is an input to reranking.

A bounded six-hour session snapshot stores IDs/order, mode and time, scoped to
preview pathname, local profile owner and eligible source identity. Return uses
that order plus the existing history-entry scroll position. Storage denial does
not block the static feed or modify the profile. This is UI continuity, not
cross-device profile synchronization or an authorization boundary.

## Validation and remaining acceptance

- `site/tests/home-feed-normalization.test.mjs`: shared full-pool recall (candidate
  beyond initial popularity 30), diversity without deletion, family hide/Undo,
  observed-prefix/return stability, empty/corrupt profile and finite budget.
- `site/tests/favorites-home-contract.test.mjs`: home checks updated to actual shared
  ranking; unrelated favorites/durable-save assertions preserved.
- `site/tests/home-feed-runtime.browser.mjs`: **synthetic DOM / mocked UI only**;
  exercises materialization, preserved node identity, family hide/Undo, return and
  denied storage. Does not substitute for real EventCard/media/Auth acceptance.
- Integrated home preview must verify current-corpus general/personal cards,
  shared actions, three desktop / one mobile columns, ordinary remainder and
  actual return scroll. Final evidence and source pairing belong to the parent
  home assembly record; this lane does not deploy or claim production acceptance.

Product follow-up: exact home editorial admission/balance (popular and intimate,
varied formats) still needs owner review. Current shared diversity prevents a
popularity-only top-30 cutoff; it is not a new AI editorial selection pipeline.
