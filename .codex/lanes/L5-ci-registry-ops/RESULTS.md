# Lane L5 CI / Registry / Operations Results

## Status

implemented; live release acceptance pending

## Requirement IDs

- R04
- R07
- R08

## Delivered

- Frozen all four Search variants and their request/cache/provider/persona,
  platform, blocking and evidence policies in the central registry.
- Added an independent no-mail Search workflow: cached every 30 minutes, cold
  vector every three hours, bounded LLM four times daily, Android/iOS nightly,
  and an immediate blocking post-deploy dispatch.
- The workflow resolves only the latest durable accepted secret candidate via
  the production read-only resolver, masks the bearer path, verifies exact SHA,
  and obtains a fresh session through a protected GitHub OIDC broker.
- Browser acceptance performs `auth.getUser`, exactly one owner-filtered RLS
  read and owner receipt RPC verification. Android/iOS use distinct personas,
  callbacks in the platform browser, native keyboard intent and real touch
  scrolling. No mailbox/Focus credential is referenced.
- Repeated scheduled failure creates/updates one sanitized GitHub issue;
  post-deploy failure is immediately blocking; recovery closes the issue.
- Candidate packaging now requires complete authoritative projection coverage
  and binds catalog/corpus/search-document revisions to target metadata.

## Validation

- Search harness/workflow: 10/10 Node tests PASS.
- Auth fixture: 12/12 Node tests PASS.
- Search UI: 15/15 selected tests PASS (worker full set 30/30).
- Edge: 24/24 Node tests PASS.
- Broker/corpus/export: 36 Python tests PASS in the integrated tree.
- Static release contract: 17/17 Node tests PASS.
- `bash -n`, Node syntax, Python compile and `git diff --check`: PASS.
- Broader static handoff/public-gate suite: 51 PASS, one known environment-only
  failure because `/dev/shm` was below the runner's scratch threshold; the same
  test and cause were independently recorded by L4.

## Live handoff

Migration, Edge/broker deployment, exact-main StaticSiteBuilder regeneration,
terminal browser/Android/iOS receipts and stability counters are owned by the
integration release phase; they must not be reported as PASS merely because
the workflow and harness exist.
