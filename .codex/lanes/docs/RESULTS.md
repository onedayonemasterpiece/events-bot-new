# Region Talk external-publication documentation lane

## Completed

- Copied the three approved historical JSON payloads and
  `external-publication-research-results.md` from
  `origin/agent/region-talk-external-research-results-20260801` without
  modifying JSON bytes. SHA-256 values match the source registry:
  `59b1d7cc43fff8eabe53f4f8b84b700d1c5ebc60f326b9f3f8c2d208999bc2cf`,
  `c040269f09bd72f16cf74fe2f721d9b8375ede82bd3742ce989e406747384cb0`, and
  `e662b449811a0887dd2fa0ebe33903d8caffed3231323ee9e8fbfc55b027bad7`.
- Read workflow commit `a9c9d43e` and its importer contract. Reproduced dry
  validation against that importer in an isolated `/tmp` copy: the first
  historical file has 5 semantic row rejections, the second has 1, and the
  2026-08-01 input is clean (20 valid, 0 rejected, 63 planned YDB rows).
- Added the canonical guarded-import runbook; updated Region Talk README, YDB
  schema pointer, routes, and `[Unreleased]` changelog.

## Constraints recorded

- The first two immutable historical inputs must not be dispatched. Their
  corrections require new successor request IDs and an explicit reviewed
  workflow-allowlist update; historical payload bytes remain audit evidence.
- Import is YDB staging only and does not publish to Telegram or VK.
