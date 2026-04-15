# INC-2026-04-15 Gate Location And Linked Facts Drift

Status: monitoring
Severity: sev1
Service: Telegram Monitoring + Smart Event Update + Telegraph event pages
Opened: 2026-04-15
Closed: —
Owners: events-bot runtime / import pipeline owner
Related incidents: `INC-2026-04-10-tg-monitoring-festival-bool`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/reference/locations.md`, `docs/operations/release-governance.md`

## Summary

Customer-visible Telegraph event pages were published with the wrong gate venue and hallucinated linked-source facts. The incident was surfaced on `Весенний Экодвор` (`event_id=3927`) and a separate screenshot showed the same venue family drift on `Арт-пространство Ворота` / gate spaces in Kaliningrad.

## User / Business Impact

- Organizers saw incorrect event cards on public Telegraph pages.
- Venue mistakes directly affect attendance and navigation.
- False age/music/group-limit claims degrade trust in the product and create avoidable support load.

## Detection

- The incident was reported by event owners with a canonical source post (`https://t.me/ecodvor39/762`) and a broken Telegraph page (`https://telegra.ph/Vesennij-EHkodvor-04-15`).
- Additional operator evidence showed another gate-space collision: `Арт-пространство Ворота` was rendered as `Фридландские ворота`.
- Observability gap: source logs existed, but no guard blocked unsupported venue guesses or unsupported sensitive facts before they became canonical.

## Timeline

- 2026-04-15 UTC: organizer reported wrong data in the public `Весенний Экодвор` Telegraph card.
- 2026-04-15 UTC: investigation of live DB snapshot identified `event_id=3927` and traced source chain through `signalkld/10431` plus linked `ecodvor39/*` posts.
- 2026-04-15 UTC: source-log review showed the initial venue drift and later linked-source hallucinated facts (`12+`, `4 человека`, music program).
- 2026-04-15 UTC: scope expanded after screenshot evidence confirmed a broader `ворота` disambiguation problem across three Kaliningrad spaces.

## Root Cause

1. Telegram Monitoring accepted an extracted venue even when the same message text / poster OCR explicitly grounded a different venue.
2. Smart Update accepted sensitive linked-source facts from LLM merge output without verifying that they were actually present in that linked candidate’s source text / OCR.
3. The gate-space alias layer treated the `ворота` family too loosely and did not encode all three canonical Kaliningrad gate venues in the location reference.

## Contributing Factors

- The public fact-first narrative trusted `event_source_fact`, so once bad facts entered the canonical log they propagated into Telegraph text.
- `Закхаймские ворота` had evolved a second brand surface (`Арт-пространство Ворота`), increasing ambiguity around the token `ворота`.
- `docs/reference/locations.md` did not explicitly contain all three relevant gate spaces.

## Automation Contract

### Treat as regression guard when

- changes touch `source_parsing/telegram/handlers.py`;
- changes touch `smart_event_update.py` fact extraction / merge / location canonicalization;
- changes touch venue normalization, gate aliases, `docs/reference/locations.md`, or Telegraph rebuild paths.

### Affected surfaces

- `source_parsing/telegram/handlers.py`
- `smart_event_update.py`
- `docs/reference/locations.md`
- Telegraph event rebuild path (`/rebuild_event ... --regen-desc`)
- live prod DB event rows and `event_source_fact`

### Mandatory checks before closure or deploy

- targeted tests for gate-location disambiguation and sensitive fact grounding;
- clean-worktree release-governance checks;
- post-deploy verification that the affected Telegraph card no longer shows the wrong venue or hallucinated facts;
- confirmation that the deployed SHA is reachable from `origin/main`.

### Required evidence

- deployed SHA;
- test output for the targeted regression suite;
- post-deploy Telegraph verification for `Весенний Экодвор`;
- confirmation that `origin/main` contains the deployed fix.

## Immediate Mitigation

- Added venue grounding before Smart Update so unsupported extractor venues can be replaced by text/OCR-grounded venues from the same post.
- Added sensitive fact grounding so unsupported age/group-size/music/duration claims are dropped before they reach `event_source_fact`.
- Added all three gate venues to the canonical venue set.

## Corrective Actions

- Introduced explicit canonical handling for `Закхаймские`, `Фридландские`, and `Железнодорожные` gates.
- Removed the unsafe assumption that bare `Ворота` should default to Zakheim.
- Added regression tests for both venue drift and linked-source sensitive-fact drift.

## Follow-up Actions

- [ ] Recheck nearby recent gate-space events in prod for legacy drift and rebuild any affected Telegraph cards.
- [ ] Consider adding an operator-facing warning when a gate-family venue is inferred only from a weak token and not from address/OCR.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks:
- post-deploy verification:

## Prevention

- The venue layer now requires explicit gate-family grounding instead of generic `ворота` collapsing.
- The fact layer now rejects unsupported sensitive claims before fact-first rebuild.
- The incident record itself becomes a mandatory regression contract for future gate-location or linked-source import changes.
