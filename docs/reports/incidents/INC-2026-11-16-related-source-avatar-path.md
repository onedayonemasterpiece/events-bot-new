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

## Pre-release visual acceptance extension

The path-only PR1029 merged as e81d4f246 and its image was built, but root did
not switch production: cached publisher avatar was visibly square and resolver
preferred source_vk_post_url over canonical source_post_url. Preserve the
original source identity in TG/VK and site medallions; publisher is a fallback
only when original source is absent. Source link lists remain intact.

Carry the existing source_owner metadata to VK rendering, with legacy string
entries still supported. Only source avatars use a common alpha-preserving
round_avatar_image helper. The encoder produces a round shared CDN resource;
TG/VK also handle old raw cached avatars. Curated assets, including Pushkin,
are not cropped. No cache migration, new state/version/flag or download replay.
The helper remains in source_avatars.py, already copied into Kaggle staging.

Observed baseline: two encoder cases fail square-corner checks. Exact fixed
source:38 existing source/avatar/relation tests PASS plus3 new role/site
integration controls PASS. Source32touched/net+8 within budget50/net+18.
Path-only image remains BUILT, NOT DEPLOYED. Final combined rollout and ordinary
initial159664 recovery remain open.

## 27 November: selected venue and source portrait duplicate one identity

Root inspected actual postponed VK16321: peer11006 displayed the curated
`dom-kitoboya` logo and source portrait `vk-source--148784347` side by side.
Retained provider metadata binds signed owner -148784347 to «Дом китобоя» and
canonical profile `https://vk.com/club148784347`. The shared resolver suppressed
all source portraits only when a selected item had reason `source_identity`;
the same selected venue had reason `location_alias`, so slug dedup missed it.
Site detail and listing consumers independently appended the same source portrait.

Correction uses an existing exact profile alias in the common manifest. Each
consumer omits only the source owner already represented by a selected curated
item. It preserves a different owner, positive personal-page identity, two
different source owners and the portrait when no curated item was selected.
No event meaning, ranking, assets, downloads, state or recovery path changes.
The failure is mechanical exact owner identity, not semantic venue inference.

Distinguishing baseline: Python2FAIL/3PASS; site1FAIL/5PASS. Mandatory checks:
source-avatar and external-path relation suites, shared manifest asset tests,
site listing/detail identity tests and normal new-card visual readback.
Code/tests, main release and subsequent product verification are separate;
deployment for this correction has not run. Existing VK posts/comments remain
untouched, including postponed16321.
