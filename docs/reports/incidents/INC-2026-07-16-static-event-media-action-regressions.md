# INC-2026-07-16 Static event media and action regressions

Status: open
Severity: sev2
Service: KenigEvents static event pages
Opened: 2026-07-16
Closed: —
Owners: static-site integration
Related incidents: `INC-2026-07-15-static-production-v2-secondary-surfaces`, `INC-2026-07-15-static-desktop-template-regression`, `INC-2026-07-13-tg-media-downgrade-non-cdn-posters`
Related docs: `docs/features/static-site-pages/event-token-medallions.md`, `docs/features/event-media/README.md`, `docs/features/unsigned-personalization/personal-feed-architecture.md`

## Summary

The full-catalog static preview exposed several user-visible regressions around an otherwise accepted desktop/mobile event template: the Dramatic Theatre venue medallion was absent, event `4671` exposed two visually duplicated versions of the same poster, narrow mobile event docks expanded an arbitrary secondary action according to event-id parity, dynamically appended cards could collapse while their image was loading, and the desktop full-screen gallery lacked the terminal related-event recommendation already present on mobile.

## User / Business Impact

- a known venue lost its fast-recognition medallion;
- visitors could see a cropped duplicate of the Epidemia poster in one gallery;
- equivalent mobile events showed inconsistent secondary CTA labels for no product reason;
- slow or failed personalized-card media could cause a visible layout jump;
- desktop and mobile gallery journeys ended differently, weakening discovery continuity.

## Detection

- detected by user review of the public noindex full-catalog preview;
- source/DB inspection showed that the duplicate poster was legacy approved-ledger debt rather than a renderer-only duplicate;
- generated-page acceptance did not assert medallion inventory parity, adaptive action semantics, dynamic-card media geometry, or desktop/mobile gallery-ending parity.

## Timeline

- 2026-07-15: full-catalog v3 preview published after accepted desktop/mobile integration.
- 2026-07-16: user reported the missing theatre medallion, duplicated Epidemia poster, CTA inconsistencies and discovery gaps.
- 2026-07-16: incident opened; canonical data and shared-component repair started in an isolated worktree.
- 2026-07-17: backed up and reconciled Epidemia EventPoster row `8622` against canonical row `7824`, rebuilt its Telegraph projection, and exported a fresh 303-event future/ongoing catalog.
- 2026-07-17: published noindex preview `preview-20260717t-static-personalization-v4`; public HTTP and browser acceptance covered medallions, desktop calendar states, duplicate removal, stable failed-image geometry, personal-feed lazy loading/dedup, gallery parity and desktop/mobile transport.
- 2026-07-17: completed the repaired full vector sync and replaced the temporary sparse catalog with noindex preview `preview-20260717t-static-personalization-v5-vector`: `303` events, `40` pgvector/HNSW candidates per event and no underfilled chains.

## Root Cause

1. The accepted `dramteatr39` medallion asset and manifest item existed only in an older medallion integration branch and never reached the current production integration base.
2. EventPoster rows `7824` and `8622` are crop/overlay variants of the same poster. Both were bulk-approved before approved-versus-approved pair reconciliation existed; the current automatic gate only compares pending candidates against approved rows. The static exporter and renderers intentionally remove only exact URL duplicates, so they correctly preserved both approved canonical rows.
3. The accepted mobile V8 dock was later given an event-id parity rule that randomly expanded `calendar` or `share` text at narrow widths.
4. Dynamically inserted cards did not reserve their media aspect ratio until image error/load handling ran.
5. The mobile gallery appended a terminal related-event slide, while the desktop gallery generated image slides only.

## Contributing Factors

- accepted branch assets were copied selectively without a manifest-inventory regression check;
- historical approved media was not re-run through the current visual pair-review contract;
- lab/production iteration introduced event-specific presentation behavior without a user-state or product rule;
- browser acceptance focused on initially rendered cards, not delayed/failed media.

## Automation Contract

### Treat as regression guard when

- changing event medallions, event media approval/projection, static gallery export, mobile/desktop CTA rendering, dynamic recommendation cards or gallery navigation;
- generating or publishing a full-catalog static preview.

### Affected surfaces

- `site/src/data/organizerMedallions.json` and organizer assets;
- production `eventposter`/event gallery projection for event `4671`;
- `site/src/components/CalendarLink.astro`, `DesktopEventActionPanel.astro`, `MobileEventProductionStyles.astro`;
- `site/src/layouts/EventLayout.astro`, `site/src/components/DesktopEventPage.astro`;
- full-catalog static generation and noindex preview publication.

### Mandatory checks before closure or deploy

- event `5756` renders the Dramatic Theatre medallion from the accepted local SVG asset;
- event `4671` exports and renders exactly one member of poster pair `7824`/`8622`, with the duplicate ledger/projection repair backed up and auditable;
- narrow mobile secondary actions do not choose an expanded label by event id;
- desktop secondary calendar action expands only for never/stale use and remains accessible when visually compact;
- delayed, invalid and successful dynamically inserted card images keep one stable reserved media frame;
- desktop and mobile full-screen galleries both expose one terminal related-event recommendation;
- previous desktop template, related-card crop, transport and mobile V8 regression contracts remain green across the full future-event catalog.

### Required evidence

- source commit and pushed branch;
- production DB before/after rows and backup-table name for the narrow media repair;
- complete build route/page counts and static checks;
- public noindex HTTP and Playwright evidence on representative desktop/mobile routes;
- release note that production root was not promoted without explicit approval.

## Immediate Mitigation

- keep the affected build under a noindex preview prefix;
- repair only the confirmed duplicate poster row and projection after a production backup;
- restore the accepted medallion asset instead of inventing a replacement.

## Corrective Actions

- restore the accepted theatre medallion manifest entry and SVG provenance;
- reconcile the legacy approved duplicate in canonical data and add generated-page assertions;
- replace random mobile CTA expansion with stable icon-only secondary actions;
- persist bounded calendar-use recency separately from saved-event expiry state;
- reserve dynamic-card media geometry and add desktop gallery recommendation parity.

## Follow-up Actions

- [ ] Add a bounded approved-versus-approved historical media reconciliation batch routed through the existing VLM pair-review contract.
- [ ] Promote the accepted static integration through `origin/main` only after product acceptance of the noindex preview.

## Release And Closure Evidence

- deployed SHA: recorded by the final branch commit for this preview; production root was not promoted
- deploy path: `https://kenigevents.ru/preview-20260717t-static-personalization-v5-vector/__preview/` (noindex preview only; production root unchanged)
- generated catalog: `303` future/ongoing event pages; `event_pgvector_related_chain_v2_two_doc` over `supabase_pgvector_hnsw_cosine_v1`, semantic embeddings enabled, exactly `40` candidates for every event and `0` underfilled chains
- regression checks: Node source/build contracts `7/7`, preview check, desktop full-catalog contract (`303` pages), bus and rail directory checks, desktop-gallery contract and `git diff --check` passed
- post-deploy verification: public HTTP `200` for the v5 index, events `5756`/`4671`/`3103`, personal-feed JSON and theatre SVG; prior Playwright acceptance on the identical application code confirmed one visible `dramteatr39` medallion, calendar fresh/regular/stale modes, ten unique Epidemia gallery assets with removed row absent, fixed failed-image geometry, lazy six-card `Для вас` chunk with no overlap, desktop terminal recommendation and desktop/mobile rail transport

## Prevention

- this record is a mandatory regression contract for future static-site catalog generation, event-media projection and secondary action work.
