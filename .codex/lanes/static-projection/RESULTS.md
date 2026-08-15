# Static projection lane results

## Outcome

Implemented the bounded first stage of the StaticSiteBuilder storage redesign. Fly now materializes an immutable, static-only SQLite projection under `/tmp`, closes the production read transaction before compaction/hash/remote work, and hands Kaggle a hash-named/content-addressed private input. The kernel reads that mounted input in place and removes any SQLite file from `/kaggle/working`, so Kaggle output cannot contain `events.sqlite` or another database copy. Downloaded candidate archives are checked and published from `/tmp`; only bounded semantic caches, receipts, and the runner lock remain on `/data`.

The projection contract contains only explicitly allowlisted exporter relations and fails closed on unknown/operational relations, manifest/hash/size/schema/row-count mismatch, failed SQLite `quick_check`, or a projection above 256 MiB. In particular it excludes `joboutbox`, `ops_run`, `vk_inbox`, `vk_source_packet`, `kaggle_run_event`, `kaggle_run_ledger`, and `resource_lease`.

Direct Kaggle-to-Yandex candidate staging is intentionally not part of this bounded incident change. The transitional candidate/root archives still return through Fly for validation and Yandex publication, but their staging is ephemeral `/tmp`, not persistent `/data`. Kaggle remains an untrusted builder and cannot promote the public root.

## Important implementation details

- Projection identity: `static_site_projection_snapshot_v1` manifest + `static_site_projection_sqlite_v1` content schema, SHA-256, byte size, exact table inventory, source/copied row counts, and SQLite `quick_check`.
- The live DB is opened read-only, copied in one bounded transaction, and closed before `VACUUM`, hash, Kaggle upload, wait, output download, or Yandex calls.
- Production dataset filenames are `static-projection-<sha256>.sqlite` and matching manifest; never `events.sqlite`.
- Dataset refs are derived from the SHA-256 of the complete staged payload. Retry adoption requires the exact already-committed dataset ref and expected snapshot identity; it never replaces a mismatching remote dataset.
- If Fly restarts after the private dataset is durable, recovery may adopt it without the vanished `/tmp` file only from the exact manifest/snapshot hash already committed to the job handoff.
- Runner dataset staging uses a hard link when projection and dataset scratch share a filesystem, avoiding another allocation. A compact-file copy is only a portability fallback.
- Durable semantic cache files are individually capped at 64 MiB. Output, projection, and runner scratch roots are explicitly configured under `/tmp` in `fly.toml`.

## Validation

- Python compilation: changed Python modules passed `python3 -m py_compile`.
- Focused regression suite: **112 passed in 8.82s**.
  - `tests/test_static_site_projection_snapshot.py`
  - `tests/test_static_site_projection_handoff.py`
  - existing static release/handoff/debounce/search-snapshot/semantic-cache tests
- Focused tests cover exporter-visible parity, operational-table absence, max-size rejection, hash/row-count/quick-check validation, closed source reader/WAL checkpoint behavior, no `events.sqlite`, no SQLite Kaggle output, no `/data` output default, content-addressed input, and exact adoption after ephemeral scratch loss.
- Broader static suite: **369 passed, 2 unrelated failures**. Both remaining failures are pre-existing `tests/test_static_collection_upstream_capture.py` VK `PosterMedia` capture/schema assertions; this lane did not modify their code or schema. One initially exposed legacy recovery compatibility failure in this lane and was fixed before the final focused run.
- Local production-like materializer canary against tracked `db_prod_data.sqlite`: source **21,397,504 B**, projection **7,372,800 B** (34.46%), **0.566 s**, `quick_check=ok`, table counts `event=1716`, `eventposter=2245`, `user=9`.
- `git diff --check`: clean.

## Required live canary before incident closure

1. Merge to a clean `origin/main` SHA and deploy that exact SHA; do not deploy this side branch directly.
2. Record pre-run `/healthz`, Fly `/data` and root `/tmp` free space, DB/WAL sizes, `PRAGMA quick_check`, current static job/claim/lease, and confirm no unintended active builder.
3. Rearm exactly one supported static build. Verify exactly one Kaggle input push/run and a durable handoff containing exact repo SHA, source identity, vector/package identities, projection id/hash/size/counts, dataset ref, kernel ref, run id, and candidate token.
4. Verify projection materialization completes below 256 MiB, source/copied row counts agree, `quick_check=ok`, and the source read transaction is gone before upload/wait. Monitor WAL during the full remote run and prove it is not pinned by StaticSiteBuilder.
5. Inspect the private Kaggle input: it contains `static-projection-<sha256>.sqlite`, never `events.sqlite`, and its dataset ref equals the recorded content-addressed payload identity. Inspect Kaggle output: no `*.sqlite`, `*.sqlite-wal`, or `*.sqlite-shm` files.
6. Record `/data` and `/tmp` before/peak/after. Confirm no new `/data/static_site_snapshots`, `/data/static_site_builder/output-*`, or persistent runner staging; each retained cache is <=64 MiB; terminal `/tmp` projection/output is removed.
7. Validate downloaded report/archive identities and exporter parity, publish only the secret candidate, and run the existing candidate/public object and browser probes. Root promotion must remain disabled unless it receives a separate explicit approval.
8. Exercise one safe exact-adoption path (or inspect equivalent restart evidence): no second dataset push/kernel, exact dataset/ref identities preserved, successful terminal reconciliation, claim/lease released, and no duplicate publication.
9. Re-check health, DB/WAL/space, job terminal state, current candidate receipt, Yandex candidate objects, and public root state. The public-root freshness problem is separate from this storage redesign until a candidate is explicitly promoted.

## Follow-up reviewer gate closure

A separate follow-up commit closes the reviewer block on column-level disclosure and bounded execution:

- Every relation now has an explicit ordered exporter-required column allowlist. `user` is reduced to `user_id,is_partner,organization`; `event_source` excludes raw source text, source/candidate fingerprints and keys, canonical/role identity state, and Smart Update candidate ids. Optional product tables similarly omit evidence, trace, analysis and editor-only payloads.
- The immutable manifest carries the exact ordered column inventory per table. Fly validation and the Kaggle kernel independently compare it with `PRAGMA table_xinfo` and fail closed on additions, omissions or ordering changes.
- A SQLite progress handler enforces the projection transaction deadline inside long VM scans. Row counting uses `SUM(1)` rather than SQLite's `COUNT(*)` fast path so it has an interrupt boundary; Python fetch checks remain a second boundary.
- Kernel cleanup is recursive for `*.sqlite`, `*.sqlite-wal` and `*.sqlite-shm`. Final Fly output validation independently performs the same recursive zero-SQLite assertion before result acceptance.
- Parity coverage now constructs and compares full catalog events/archive/ledger, source records, participant/video/image/geometry/metrics/publication/promo/poll data, both interest-club products, and festival timeline across source and projection. Private columns are populated in every optional fixture and asserted absent.
