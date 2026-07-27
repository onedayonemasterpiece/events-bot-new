# R13 Festivals Production Integration Report

| Lane | Requirement IDs | Branch | Status | Head SHA | Merge/cherry-pick | Evidence |
|---|---|---|---|---|---|---|
| R13-FEST-DB | R13-02/03 | integration/festivals-production-r13-20260726 | released | `f93fdd2c` | serial implementation | `.codex/lanes/R13-FEST-DB/RESULTS.md` |
| R13-PROD-GEN | R13-04/05 | integration/festivals-production-r13-20260726 | released | `f93fdd2c` | serial implementation | `.codex/lanes/R13-PROD-GEN/RESULTS.md` |
| R13-TODAY-SORT | R13-01 | integration/festivals-production-r13-20260726 | released | `f93fdd2c` | serial implementation | `.codex/lanes/R13-TODAY-SORT/RESULTS.md` |
| R13-INTEGRATION | R13-01..05 | integration/festivals-production-r13-20260726 | immutable review released; visual acceptance pending | `709eda27` | production candidate + host recovery | `.codex/lanes/R13-INTEGRATION/RESULTS.md` |

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

## Production-generation release

- Production DB backup was taken before the idempotent backfill and retained as
  a verified gzip; the original SHA-256 is
  `0f21ae4702e65a8be6b5dfb7ab48fdb641f568f3317dbd2746874a054cda9630`.
- Production SQLite contains exactly 21 public 2026 festival editions with
  unique slugs and display order. Status coverage is `9 announced`,
  `4 date-pending`, `8 program-pending`; the 58 legacy festival rows are
  unchanged.
- Kaggle build `production-secret-20260727T004208-0af7c1de` produced 246 event
  pages and 1131 candidate objects from page-source SHA `f93fdd2c…`. Every
  production and secret-candidate gate passed; related freshness was the only
  explicitly optional degraded check.
- A concurrent Smart Update writer collided with the first final receipt write
  after all objects had already been uploaded and verified. Host SHA
  `709eda27…` adds bounded fresh-session receipt retries and exact create-only
  object adoption. The assertion-guarded recovery then committed the current
  receipt without a second Kaggle build or any overwrite.
- Current noindex candidate:
  <https://kenigevents.ru/_review/qjjOTwpZHmmmBBv7lbHcSmluPCtWhXIa4mz1ZxYn9l4/>.
  Root and stable ICS mutation flags are both false.
- Final public Playwright QA passed the 19-page matrix on desktop/mobile,
  Festivals 21-card inventory, three club detail routes, partnership cleanup,
  Search editability, Today chronology and event `6667` free medallion.
- The complete 22-link review matrix was sent to Telegram reply thread `548`;
  read-back receipt is message `692`.
