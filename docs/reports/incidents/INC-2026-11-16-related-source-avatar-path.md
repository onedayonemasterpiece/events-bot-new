# INC-2026-11-16 Related source avatar outside application directory

Status: open
Severity: sev2
Service: VK first-publication relation cards
Opened: 2026-11-16
Closed: —
Owner: KenigEvents (codex)
Related incidents: INC-2026-11-12-related-vector-build-claim

## Impact and detection

At10:39UTC initial job159664/event11083 failed before publication with
`/data/source_avatars/-231920894.webp is not in the subpath of /app`.
A ready related selection could not be rendered for the first carousel.
Existing posts and comments must not be edited or recreated.

## Root cause and correction

The shared medallion resolver legitimately returns curated repository assets
and downloaded source avatars on the persistent data mount. VK presentation
forced every path through Path.relative_to(ROOT), which rejects external
assets. Preserve the resolver path directly. The existing renderer already
opens ROOT / path; pathlib supports both absolute and relative asset paths.
No new path policy, asset copy, fallback, queue or retry is needed.

## Automation contract

Treat as regression guard when changing medallion resolution, frozen relation
presentation or card asset loading. Affected surfaces: vk_event_relations.py
presentation and render_card. Preserve source identity and carousel ordering.
Mandatory before deploy: existing shared-graph test must cover both curated
assets and a source avatar outside ROOT through presentation and actual card
render; full relation suite; exact-main release and isolated preflight.
Before closure: ordinary initial159664 must recover through the existing worker,
then inspect its new carousel once. Do not manually replay or rewrite old posts.

## Evidence and release

Retained task directory20261112T111614Z-similar-events-check contains baseline
external-avatar-baseline-20261116.log: external case FAIL, curated case PASS.
Production delta: one replaced expression, net0, budget4 touched lines.
Deployment and user-visible recovery remain open.
