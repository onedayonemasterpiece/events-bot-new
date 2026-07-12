# Event quality release monitoring

> Status: release workflow.
> Prevention owner: **Smart Update and its import/extraction/match-merge pipeline**.

## Purpose

This workflow prepares the public static-site release by continuously measuring event quality, opening incidents, fixing root causes and proving that recurring defect families are reduced to an accepted near-zero level.

It is not a second semantic gate and must not replace LLM-first Smart Update decisions with broad regex/keyword logic in the static exporter.

## Cadence

- **Daily:** inspect every new/changed canonical event and its public projections.
- **Regular full inventory:** audit the complete active/future catalog at an agreed cadence before release.
- **Release cutoff:** freeze the exact active/future inventory and complete a full source-grounded audit.
- **Canary:** continue the same cadence and measure new/reopened incidents during the stability window.

## Workflow

1. Select new/changed or full active/future rows from canonical Fly SQLite.
2. Compare canonical fields with source text, linked sources, OCR/media and lifecycle evidence.
3. Inspect affected public surfaces: Telegram, authenticated VK, Telegraph, static HTML/JSON/listings/search/ICS.
4. Classify duplicates, wrong/prose/default location, wrong date/time, invalid eventness, media/source drift and projection mismatch.
5. Create or update the relevant `INC-*` record; do not create duplicate incident families.
6. Trace the root cause to extraction, import boundary, Smart Update shortlist/match/merge/writer, reference normalization or publication path.
7. Fix prevention first or alongside data/public repair.
8. Run closure-grade replay through the real import boundary and `smart_event_update.py` on snapshot/shadow DB.
9. Recheck canonical DB plus every affected public surface.
10. Record counts/rates and monitor for recurrence.

## Required release metrics

At minimum track by import batch and day:

- active duplicate clusters found;
- wrong/prose/default location incidents;
- wrong date/time incidents;
- invalid/non-event rows reaching a public projection;
- newly opened, reopened, mitigated and closed quality incidents;
- root causes closed with passing replay;
- time from detection to mitigation and prevention closure.

Numerical GO thresholds and the required stable window are global product/release decisions. The release owner must not replace missing thresholds with an informal “looks clean”.

## Projection safety boundary

Static export may fail closed on narrow structural conditions such as invalid ISO dates, `silent`, merged/review/cancelled/inactive state, missing required identifiers or explicitly quarantined rows. These checks prevent obviously unsafe projection; they do not decide event meaning, venue semantics or duplicate identity.

## Mandatory regression families

Use the current incident index and at least the contracts linked from the [static personal announcements release checklist](../reports/static-personal-announcements-release-readiness-2026-07-11.md#stage-1--стабилизация-качества-smart-update-и-incident-burn-down).

## Release evidence

- exact catalog snapshot/hash and row count;
- per-defect counts/rates and trend;
- incident IDs and statuses;
- replay commands/results and DB diffs;
- current Telegram/VK/Telegraph/static/ICS evidence;
- remaining risks, owner and deadline;
- approved stability-window result.
