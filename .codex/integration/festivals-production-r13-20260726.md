# R13 Festivals Production Integration Report

| Lane | Requirement IDs | Branch | Status | Head SHA | Merge/cherry-pick | Evidence |
|---|---|---|---|---|---|---|
| R13-FEST-DB | R13-02/03 | integration/festivals-production-r13-20260726 | integrated | pending | serial implementation | `.codex/lanes/R13-FEST-DB/RESULTS.md` |
| R13-PROD-GEN | R13-04/05 | integration/festivals-production-r13-20260726 | integrated | pending | serial implementation | `.codex/lanes/R13-PROD-GEN/RESULTS.md` |
| R13-TODAY-SORT | R13-01 | integration/festivals-production-r13-20260726 | integrated | pending | serial implementation | `.codex/lanes/R13-TODAY-SORT/RESULTS.md` |
| R13-INTEGRATION | R13-01..05 | integration/festivals-production-r13-20260726 | implementation-complete; release pending | `0abe04ab` + final follow-up | owns final commit/release | `.codex/lanes/R13-INTEGRATION/RESULTS.md` |

## Integration order

1. Base on the checked R12 integration branch.
2. Cherry-pick the eight bounded festival-donor commits through `940fea2e`.
3. Add the core SQLite calendar model/backfill and DB-only exporter projection.
4. Add production route/manifest/browser gates and the Today chronological regression.
5. Synchronize canonical docs and changelog, then validate and release from clean `origin/main`.

## Final local acceptance

- Production artifact: 275 event pages / 1212 files, all production checks green.
- Secret candidate: 1217 files, all static checks green.
- Chromium gate: crop, loaded media, keyboard, footer and festival calendar all
  green; `/festivali/` renders all 21 DB-projected entries without broken images
  or horizontal overflow on desktop/mobile.
- Search public aliases survive both release profiles without exposing
  secret/service-role credentials.
- Gemini Pro external acceptance remains blocked before model execution by the
  provider's account-eligibility response; it was not replaced with a lower
  model class.
