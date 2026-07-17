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

The full-catalog static preview exposed several user-visible regressions around an otherwise accepted desktop/mobile event template: the Dramatic Theatre venue medallion was absent, event `4671` exposed two visually duplicated versions of the same poster, narrow mobile event docks expanded an arbitrary secondary action according to event-id parity, dynamically appended cards could collapse while their image was loading, and the desktop full-screen gallery lacked the terminal related-event recommendation already present on mobile. A later full-catalog review also found role-blind desktop hero selection, cropped OCR in fullscreen, missing grouped portrait viewing, incomplete KAUP transport, an irrelevant next-morning train suggestion and a desktop phone CTA that attempted to dial instead of exposing the number.

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
- 2026-07-17: published desktop-only v6 remediation for events `5756`, `4671`, `3103`, `4783` and `6851`; the only shared mobile change is replacing the retired fullscreen brand imitation with `AnnouncementsLockup`.
- 2026-07-17: follow-up review found that v6 still used an ambiguous KAUP route label, offered train `6724` without a realistic Янтарь-холл exit/walk buffer, admitted low-resolution images into the grouped viewer despite a strong set, and allowed the constrained phone panel to wrap/overflow.
- 2026-07-17: published noindex v7 product-polish preview with distinct pedestrian/car KAUP routes, venue-access-aware return selection, quality-admitted grouped media and measured component-responsive telephone actions.
- 2026-07-17: user acceptance rejected v7 because the KAUP information architecture still obscured the bus origin/journey order and the telephone panel's real child geometry still broke despite the earlier browser claim; the stored `a-opus` output contained no final verdict and is not valid acceptance evidence.
- 2026-07-17: published v8 desktop correction with an explicit terminal-address-first bus journey and a height-stable one-row telephone CTA; repeated public Playwright geometry and Gemini 3.1 Pro (High) visual review passed without material blockers.
- 2026-07-17: pre-handoff mobile browser QA found that v8 rendered the corrected KAUP journey only on the desktop surface, so the v8 links were deliberately not sent for mobile review. A compact phone variant and generated-page contract were added for v9 before Telegram handoff.

## Root Cause

1. The accepted `dramteatr39` medallion asset and manifest item existed only in an older medallion integration branch and never reached the current production integration base.
2. EventPoster rows `7824` and `8622` are crop/overlay variants of the same poster. Both were bulk-approved before approved-versus-approved pair reconciliation existed; the current automatic gate only compares pending candidates against approved rows. The static exporter and renderers intentionally remove only exact URL duplicates, so they correctly preserved both approved canonical rows.
3. The accepted mobile V8 dock was later given an event-id parity rule that randomly expanded `calendar` or `share` text at narrow widths.
4. Dynamically inserted cards did not reserve their media aspect ratio until image error/load handling ran.
5. The mobile gallery appended a terminal related-event slide, while the desktop gallery generated image slides only.
6. Desktop routing trusted the first exported media mode and scanned alternate landscapes only for a strict identity poster, so a classified non-identity document could force the split family even when safe horizontal event photos existed.
7. Fullscreen CSS treated legacy/derived `visual_only` as permission to cover; it did not require classified `event_photo` plus explicit safe-crop evidence.
8. The efficient viewer gate required at least five portrait images and a 60% portrait share; a real 12-image gallery with four strong portraits therefore fell back to the ordinary one-at-a-time viewer.
9. KAUP had no exact-venue transport component, and the rail exporter missed an explicit duration written with a dash rather than the older colon-only form.
10. Desktop telephone CTA reused a mobile `tel:` action instead of a visible copyable number.
11. The v6 acceptance script asserted phone copy content but not child geometry at the user's effective `1536×864` viewport; CSS grids remained viewport-responsive while the action component's actual container was narrower.
12. Return selection filtered directly from event end and omitted venue exit/walk/boarding time. Media grouping tested orientation/count but ignored imported technical quality when strong alternatives existed.
13. The v7 phone panel still relied on CSS-grid auto-placement. Its empty
    live-status paragraph consumed a grid cell and moved the metrics row below
    the phone CTA; the browser gate asserted only containment/no overflow, not
    same-row centres or height stability after copying.
14. The v7 KAUP renderer exposed correct routes but organized them as a
    decorative pseudo-map and multiple button-like labels. It omitted the exact
    Kaliningrad bus-terminal address and put the final walking action before
    the departures, so correct source data did not form an actionable journey.
15. The first v8 correction was mounted inside `DesktopEventPage` only. The
    established mobile renderer did not invoke the exact-venue component, and
    the v8 gate checked desktop KAUP plus generic mobile transport rather than
    asserting that event `4671` exposed the exact journey on the phone surface.

## Contributing Factors

- accepted branch assets were copied selectively without a manifest-inventory regression check;
- historical approved media was not re-run through the current visual pair-review contract;
- lab/production iteration introduced event-specific presentation behavior without a user-state or product rule;
- browser acceptance focused on initially rendered cards, not delayed/failed media.
- consultant review covered the component concept, while the automated gate did not replay the exact constrained phone specimen or assert bounding rectangles; this allowed a conceptually valid adaptive label to pass with invalid geometry.
- source-backed transport facts were accepted independently from information
  architecture; no gate asserted the journey order or rejected decorative raw
  coordinates/pseudo-map chrome.

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
- event `5756` uses Editorial with a classified safe horizontal photo while its non-identity document remains contained in fullscreen;
- event `4671` shows exact-venue KAUP transfer, bus and map guidance sourced from KAUP/Автовокзал and distinguishes the pedestrian route from the Romanovo stop from the separate Kaliningrad car route;
- event `4671` names `Калининградский автовокзал · ул. Железнодорожная, 7`, orders the independent flow as origin → schedule → final walking leg → no-return warning → car, uses standard mode/pin icons, and exposes neither raw coordinates nor the rejected pseudo-map/route schematic;
- event `4671` exposes that same factual journey on the established mobile surface as a flat compact block; only official-transfer boarding fine print starts collapsed, while origin, two departures, final walk, return risk and car remain visible; phone widths `320` and `390` have no horizontal overflow and every link/disclosure target is at least `44px` high;
- event `3103` applies the 30-minute Янтарь-холл exit/walk/boarding buffer, shows `6726`/`6728`, excludes unsafe `6724`, and shows no next-morning wait inside the desktop renderer;
- event `4783` opens the grouped multi-portrait viewer with the seven technically strong images, excludes five materially weak renditions while that strong set exists, and discloses `7 из 12`;
- event `6851` exposes and copies the formatted telephone number with visible success feedback;
- event `6851` keeps admission, the one-line phone number and all action controls on the same visual row at `1536×864` and `1920×1080`; its calendar is icon-only and the panel height is stable after copy success;
- desktop OCR/documents are contained in fullscreen, click/backdrop close works, and both responsive galleries use the shared accepted lockup;
- insufficient-feedback placeholder copy is absent.

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
- require venue-access buffers after exact event ends, separate ambiguous multimodal route CTAs, apply quality admission with a weak-only fallback, and accept action panels by measured component geometry rather than content assertions alone.

## Follow-up Actions

- [ ] Add a bounded approved-versus-approved historical media reconciliation batch routed through the existing VLM pair-review contract.
- [ ] Promote the accepted static integration through `origin/main` only after product acceptance of the noindex preview.

## Release And Closure Evidence

- deployed SHA: `fc413efb` on pushed branch `fix/static-site-v4-personalization-media-20260716`; production root was not promoted
- deploy path: `https://kenigevents.ru/preview-20260717t-static-personalization-v6-desktop-fixes/__preview/` (noindex preview only; production root unchanged)
- generated catalog: `303` future/ongoing event pages; `event_pgvector_related_chain_v2_two_doc` over `supabase_pgvector_hnsw_cosine_v1`, semantic embeddings enabled, exactly `40` candidates for every event and `0` underfilled chains
- regression checks: preview check, desktop full-catalog contract (`303` pages), bus and rail directory checks, `12` targeted pytest checks and `git diff --check` passed; the preview check also verifies exact visible-link/transport-ICS parity after the desktop/mobile shortlist union
- post-deploy verification: public HTTP `200` for the v6 index, all five regression event pages and the recovered desktop train ICS; public Playwright passed `26/26` checks with zero console errors, covering Editorial routing and event-photo hero for `5756`, contained/click-dismissable documents and posters, KAUP transfer/bus/map facts, only evening returns `6724`/`6726`, the 12-image grouped viewer with group navigation, visible/copyable phone number, absent insufficient-feedback placeholder and preserved mobile `accepted-v8` with the shared lockup
- v7 product-polish source: pushed commit `79ff2c25c18e0f0ed8f739c578a720350f506dac` on `fix/static-site-v4-personalization-media-20260716`
- v7 preview: `https://kenigevents.ru/preview-20260717t-static-personalization-v7-product-polish/__preview/` (noindex only; production root unchanged)
- v7 build and static gates: `373` total pages / `303` event pages; preview, production-desktop (`303`), rail (`13` source pages / `9` routes / `17` locality policies / `10` patterns), bus (`17` localities / `26` venues / `21` stops), four targeted pytest regressions and `git diff --check` passed
- v7 public evidence: HTTP `200` for the preview index and all four regression pages; Playwright at `1536×864`, `1920×1080` and mobile `390×844` verified no horizontal overflow, one-line copyable telephone geometry, compact/comfortable calendar adaptation, separate KAUP `rtt=pd` and `rtt=auto` routes, return trains `6726`/`6728` after the `20:10` safe-ready threshold, seven admitted images out of twelve and preserved mobile `accepted-v8`
- v7 external-review correction: the retained `a-opus`/agy output stopped after an introductory sentence and contained no verdict, so it is explicitly excluded from acceptance evidence rather than represented as a completed review
- v8 desktop correction: pushed SHA `b089add9` on `fix/static-site-v4-personalization-media-20260716`; immutable noindex preview `preview-20260717t-static-personalization-v8-transport-cta`; repeated public Playwright at `1536×864` and `1920×1080` returned `failures: []`, including exact KAUP order/icons and telephone same-row centres, icon-only calendar, nowrap/fit and copy-state height stability; Gemini 3.1 Pro (High) visual review returned PASS with no material blockers
- v9 mobile handoff candidate: `preview-20260717t-static-personalization-v9-mobile-handoff`; local build produced `373` total routes / `303` event pages, preview and production-desktop contracts passed, and Playwright returned `failures: []` on desktop `1536×864`/`1920×1080` plus mobile `320×780`/`390×844`; the final source SHA, immutable public verification and Telegram delivery receipt are pending publication

## Prevention

- this record is a mandatory regression contract for future static-site catalog generation, event-media projection and secondary action work.
