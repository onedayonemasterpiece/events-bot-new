# INC-2026-09-06-islands-preview-stale-catalog Historical catalog presented in a newly dated island preview

Status: open
Severity: sev3
Service: isolated KenigEvents floating-island review preview (not production root)
Opened: 2026-09-06
Closed: —
Owners: static-site preview delivery
Related docs: `docs/features/static-site-pages/astro-preview.md`

## Summary / User Impact

The user accepted solid7 floating-island mechanics as the working base, then noticed July events in its September review URL. The public preview is a new UI build, but its catalog and eligibility reference are both frozen at 2026-07-23. It must not be described as a current September event catalog. No production DB/date corruption, production-root defect or Telegram/VK publication defect has been established by this investigation.

Accepted mechanics baseline (preserve during data refresh):
https://kenigevents.ru/preview-islands-b-20260906-solid7/populyarnoe/
Source snapshot and receipt: `artifacts/codex/islands-b-solid-20260906/source/`, `source-receipt.json`.

## Detection / Timeline

- 2026-09-06 UTC: user approves mechanics as a base and reports July dates.
- 2026-09-06 02:56 UTC: inspect source metadata, fetch public HTML, execute the existing eligibility predicate against historical and current references.

## Root Cause

1. Repeated local UI builds copied `site/src/data/preview-events.json` from the same historical snapshot: generated_at `2026-07-23T06:55:06.777309+00:00`, current_date `2026-07-23`, 288 events.
2. `getCurrentDate()` in `site/src/lib/events.ts` returns the snapshot's date, unless an explicit build override exists. The UI build commands supplied a new preview ID but no current-date/reference override and no refreshed catalog.
3. Both Popular entry points filter with that historical date/time. Example: event 6870, «Музыкальный фестиваль КВН», 2026-07-24, is eligible against the supplied July reference and ineligible against September 6. This is a stale build input/reference, not evidence that the event's recorded date is wrong.
4. Previous acceptance covered mechanics, rendered cards and warm/cold personalization, but not catalog age/reference freshness. The historical content was not adequately disclosed to the reviewer.

## Evidence

`artifacts/codex/islands-data-age-20260906/evidence.json` and `public.html`.
- All 288 inputs: 284 eligible against the snapshot reference, 43 eligible against September 6 using the same old input set. These are input-pool counts, not displayed-card counts.
- Old July examples 6870, 5374, 6941, 6864 are rejected by the existing predicate with the current reference.
- Merely advancing the date would remove expired entries but would NOT add events imported after July 23 or refresh status/social metrics. It is not a fresh-catalog repair.
- Prior snapshot-freshness family reviewed: `INC-2026-05-30-active-duplicate-events-recall-gate.md` documents a stale local snapshot undercount. Its production dedup/LLM contract is not implicated by this isolated preview export reuse.

## Automation Contract

### Treat as regression guard when
- Rebuilding a user-facing preview from copied fixtures/snapshots, especially a newly dated preview prefix.
- Claiming catalog freshness or current-event eligibility after UI-only builds.

### Affected surfaces
- Historical preview-events input; build.current_date/generated_at; PUBLIC_STATIC_SITE_CURRENT_DATE / PUBLIC_STATIC_SITE_REFERENCE_ISO overrides.
- Popular desktop/mobile eligibility and personalized candidate pools.

### Mandatory checks before closure or refreshed preview delivery
- Obtain and identify a genuinely current export; do not relabel the July snapshot as fresh.
- Verify current Kaliningrad reference date/time and snapshot generation provenance separately from UI build timestamp.
- Assert elapsed one-off events are absent, while valid continuing/recurring events are evaluated by the existing eligibility contract rather than banning all old start dates.
- Recheck desktop/mobile and warm-profile recommendations on actual rendered updated data.
- Preserve the accepted solid7 island mechanism and publish only a new scoped preview unless separately authorized.

### Required evidence
- Export provenance/timestamp, effective reference, eligible/output IDs, regression results and public readback.
- Production SHA/back-merge requirements apply only if a later task changes production; this report does not authorize production promotion.

## Immediate Mitigation / Corrective Actions

Confirmed and disclosed the historical-data cause; recorded accepted UI baseline. No data, event dates, accepted preview source or published objects changed in this diagnostic turn. Fresh-catalog delivery remains open; no closure/deployment claim.

## Follow-up / Prevention

- [ ] Refresh the one-page preview from a current export with a current eligibility reference, without Kaggle or changing approved motion.
- [ ] Add a freshness preflight or conspicuous historical-data labeling for intentionally historical UI review builds.
