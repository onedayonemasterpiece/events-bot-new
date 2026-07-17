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

## Production release

- PR: `#57`; merge SHA: `0804df65bb86b901df21f715aa5f2c9989d102ed`.
- Manual exact-main Fly deploy: release `v1686`, image
  `deployment-01KXQRPXP1G30SRRABNE9SDJRC`, machine version `1686`.
- Real canary `social-metrics:991268`: `224` due metric targets plus `272`
  postponed/live candidates; imported `192` collected observations and resolved
  `154 published`, `114 missing`, `4 ambiguous`, `0 resolver errors`.
- Exact official-group backfill: all `36/36` ledger-backed posts collected in one
  VK group batch; four bounded rows/post (`144` total = `36 collected` + `108
  skipped_late`). No row-bound violations.
- Current/future `klgdevents` metric coverage increased to `172/303` events.
  Official group has zero current/future exact repost mappings at this instant,
  so it correctly changes no current ranking yet; future exact reposts are now
  collected automatically.
- Status regression evidence: `kernel_started`, `preflight_ok`, multiple `alive`,
  terminal `report_written`, and both resource releases recorded. All four
  private temporary Kaggle datasets were absent after cleanup.
- Post-deploy: `/healthz` HTTP 200 `ready=true`; Fly checks `1/1`; SQLite
  `quick_check=ok`; `/data` about `920 MiB` free on 2 GiB; runtime mirror grew
  within the 64 MiB budget; no fresh disk-full, proxy no-candidate or
  `AuthKeyDuplicatedError` matches.
