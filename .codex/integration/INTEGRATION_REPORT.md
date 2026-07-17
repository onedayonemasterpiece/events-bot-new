# Integration report — popular-all-owned-channels

## Integrated scope

| Requirement | Status | Integration evidence |
|---|---|---|
| R01 | Done | Exact `231828790` event repost loader/exporter; structural exclusion tests |
| R02 | Done | Manifest v2 + Kaggle grouped postponed/live resolver + Fly evidence revalidation |
| R03 | Done | Exact result coverage, deterministic atomic slot claim, terminal failure ledger, exception-safe dataset cleanup |
| R04 | Done | Four-key snapshots unchanged, 90-day rolling backfill/cleanup, minified ephemeral JSON, no raw wall persistence, owned-family max |

## Integration decisions

- The separate env `SOCIAL_METRICS_VK_OFFICIAL_GROUP_ID` is authoritative for
  metrics; the overloaded production `VK_AFISHA_GROUP_ID` is not reused.
- A 90-day ledger-backed backfill is allowed; arbitrary wall history is not.
- Match text is carried only as a bounded transient result field so Fly can run
  the canonical matcher; it is discarded after validation and never stored.
- Resolution mapping and snapshots are independently idempotent. If snapshot
  persistence fails after mapping, the next interval sees the published target
  and repairs the metric without another wall scan.

## Verification

- Combined collector/status/exporter and existing VK popularity matcher suite:
  `42 passed`.
- `a-gemini` (`Gemini 3.1 Pro (High)`) acceptance review: **APPROVE**;
  no blockers, explicitly accepted exact attribution, owned-family collapse,
  Fly evidence revalidation, atomic slot claim and failure cleanup.
- Pending final CI/release evidence.
