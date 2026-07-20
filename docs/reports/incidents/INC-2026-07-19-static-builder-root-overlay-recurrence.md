# INC-2026-07-19 Static Builder Blocked by Fly Root Overlay Recurrence

Status: monitoring
Severity: sev1
Service: Smart Update static candidate builder / Fly app `events-bot-new-wngqia`
Opened: 2026-07-19
Closed: —
Owners: operations / static-site pipeline
Related incidents: `INC-2026-07-08-prod-root-overlay-disk-full`, `INC-2026-07-18-static-snapshot-disk-pressure`, `INC-2026-07-19-static-site-stale-builder-lease`
Related docs: `docs/operations/kaggle-static-site-builder.md`, `docs/operations/runtime-logs.md`, `docs/features/static-site-pages/release-plan.md`

## Summary

Smart Update could not refresh the immutable secret static-site candidate because
Fly's ephemeral root writable overlay reached `100%` while the persistent
`/data` volume remained healthy. Seven attempts failed before Kaggle submission;
the last static outbox row exhausted its retries with `FileNotFoundError: No
usable temporary directory`. The checked secret candidate stayed available and
the production root was not changed.

## User / Business Impact

- production event data after the last successful 05:55 UTC candidate was not reflected in a newer review build;
- the requested keyboard-navigation candidate could not be generated safely;
- other Kaggle-backed jobs were at risk of failing temporary output downloads;
- `/healthz` stayed green because it observed `/data`, not the exhausted root scratch filesystem.

## Detection

- redacted 24-hour static diagnostics showed eleven failed histories and current job `38162` exhausted at attempt four;
- runtime logs identified `FileNotFoundError` before status-ledger/Kaggle creation;
- `df` showed `/` and `/.fly-upper-layer` at `100%` with zero bytes free, while `/data` retained about `1.55 GiB`;
- direct upper-layer inventory found about `6.45 GiB` under `/tmp/videoannounce-*` and `1.84 GiB` under `/app/artifacts/codex/static-site-builder`.

## Timeline

- 2026-07-19 05:55 UTC — last checked secret candidate adopted successfully.
- 2026-07-19 14:02–17:11 UTC — seven claimed retry histories failed before Kaggle submission.
- 2026-07-19 17:11 UTC — outbox job `38162` exhausted attempt four.
- 2026-07-19 20:31 UTC — incident triage confirmed root overlay exhaustion, idle static claim, released builder lease and healthy `/data`.
- 2026-07-19 20:44 UTC — terminal session inventory isolated published video sessions `920`/`921`, failed sessions `919`/`922`, and four successful static output directories; no deletion was performed during read-only triage.

## Root Cause

1. `video_announce/poller.py` downloaded full Kaggle output trees, including thousands of intermediate frames, into `/tmp/videoannounce-<id>` and did not remove local output after terminal publication/failure.
2. the production static builder downloaded and extracted every successful candidate under `/app/artifacts/codex/static-site-builder` without bounded terminal retention;
3. both paths share the bounded Fly root overlay;
4. readiness measured only `/data`, so it did not detect an unusable Python temporary directory before jobs claimed retries.

## Contributing Factors

- four terminal video directories consumed about `6.45 GiB`; published sessions `920`/`921` alone account for about `3.85 GiB`;
- four retained successful static outputs consumed about `1.84 GiB`, approximately `458–460 MiB` each;
- current release receipts and Object Storage are durable, but full local copies were retained as if they were the release authority;
- current Fly rootfs persistence is `never`, so a clean deploy resets the overlay, but the application lacked a guard against rebuilding the same accumulation.

## Automation Contract

### Treat as regression guard when

- changing static-site Kaggle runner output paths/retention, video output download/cleanup, Fly health disk checks or any Smart Update static build lifecycle;
- handling `Errno 28`, missing usable temporary directory or root-overlay pressure.

### Affected surfaces

- `/tmp/videoannounce-*` and `video_announce/poller.py`;
- `/app/artifacts/codex/static-site-builder`, static runner/host finish/recovery paths;
- root `/`, persistent `/data`, `runtime_disk.py`, `/healthz`;
- static outbox/history/state, Kaggle ledger and `static_site:builder` lease.

### Mandatory checks before closure or deploy

- preserve active/recoverable handoffs and delete only assertion-proven terminal outputs;
- `df -h / /.fly-upper-layer /tmp /data` shows writable headroom;
- an actual temporary-file create/fsync/remove probe passes;
- `/healthz` reports both persistent and scratch disk state, returns 503 for a critical/unwritable root scratch path and 200 after recovery;
- `PRAGMA quick_check=ok`, runtime mirror present and no fresh ENOSPC/tempfile errors;
- terminal static output retention is bounded while current candidate counts/hashes remain diagnosable after full files are removed;
- terminal video cleanup never removes live/publish-blocked recovery inputs;
- a compensating full secret candidate succeeds from current `origin/main`, advances only the internal review receipt and leaves root/current/stable ICS unchanged;
- exact builder lease is released and the failed daily static refresh is caught up.

### Required evidence

- before/after bytes and exact assertion-approved removed paths;
- fixed SHA reachable from `origin/main` and deployed machine/image;
- focused static/video/disk tests and incident regression suites;
- redacted 24-hour build diagnostics, runtime-log evidence, current-candidate counts and root-isolation checks.

## Immediate Mitigation

- 2026-07-19: assertion-gated deletion removed only fully published sessions
  `920`/`921`; failed `919`/`922` were preserved. Root scratch recovered and a
  real tempfile fsync probe, `/data` SQLite quick check and `/healthz` passed
  (local evidence: `artifacts/codex/keyboard-navigation-production-20260719/root-overlay-cleanup-20260719.json`);
- do not enqueue another static build until root/tempfile preflight passes;
- preserve failed video sessions `919` and `922` until explicit recovery/supersede decisions.

## Corrective Actions

- deployed from main on 2026-07-20: bound/delete
  terminal static runner outputs after durable receipt/history adoption, reject
  output symlink/traversal and defer capacity without consuming retry budget;
- deployed from main on 2026-07-20: delete terminal
  published video frame trees, publish-only/log side trees and reconcile a
  failed cleanup on startup while preserving actionable recovery;
- deployed from main on 2026-07-20: root-scratch
  capacity/tempfile checks in health and large-download preflight.

## Follow-up Actions

- [ ] Decide recovery/supersede for failed video sessions `919` and `922` before deleting their local evidence.
- [ ] Add alerting for root scratch warning/critical floors independent from `/data`.
- [ ] Close only after a fresh Smart Update/operator candidate and at least one subsequent terminal job leave bounded root usage.

## Release And Closure Evidence

- fixed SHA is reachable from `origin/main` through merge `11cbef1751d5c7af1705d1c42e9a3f615d31a263` (PR #99); manual Fly release `v1721` deployed that clean tree, and the concurrent bbox release `v1722` advanced to descendant `ae2336cb6e6b2518213c702db7a3ced92dc2434a` without losing the fixes;
- the startup catch-up generated `production-secret-20260720T130141-937a3d18` from repo SHA `11cbef17`: 248 event pages, 853 pages and 930 files; all production and secret-candidate gates passed and 936 create-only objects were verified;
- the internal review receipt advanced at `2026-07-20T11:22:41Z`; root and stable ICS hashes remained byte-identical, and the static claim/lease returned idle;
- `/healthz` after deploy reports persistent `/data` and scratch `/tmp` separately, including a successful real tempfile probe; `PRAGMA quick_check=ok` and the runtime file mirror remained available;
- both live Chromium V7 fixtures (Split `6408`, Editorial `6593`) passed the complete gallery, focus, Cyrillic/Latin shortcut, consent, canonical related/continuation card, rerender and teardown regression;
- local task cleanup removed about 0.75 GiB of stale build/test output and raised devserver free space from about 1.9 GiB to 2.8 GiB. Object Storage currently has 22 immutable review prefixes; pruning the oldest 12 is deliberately deferred while the descendant `v1722` startup rebuild is active, so no running prefix can be mistaken for retention garbage.

The incident stays in monitoring until the active descendant candidate becomes
terminal and the explicit failed-video recovery decision in Follow-up Actions is
made; neither item invalidates the completed secret candidate above.

## Prevention

Treat Fly root scratch as a capacity-constrained production resource. Durable
release truth belongs in the DB receipt, checked manifest hashes and immutable
Object Storage candidate, not unbounded local copies of full build trees.
