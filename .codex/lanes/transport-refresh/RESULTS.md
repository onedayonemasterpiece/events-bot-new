# Transport refresh lane results

## Recovery / branch safety

- Worktree: `/home/dev/.codex/worktrees/events-bot-new/static-release-transport-refresh`
- Branch: `agent/static-release/transport-refresh`
- Crash-worktree base/current before preservation: `d169004376c309dc487fa6b48a7aae4a8ed7dea3`
- Named recovery checkpoint created before refactoring: `206072cf533c2ec70b495cd4f714d693fca528dd` (`chore(transport): preserve pre-existing lane work`)
- Recovery inventory: 20 lane files (shared package, two kernels, three runners/publisher, tests, feature/operations docs, this result record); no reset/clean/delete was used.
- `origin/main` later advanced to `55fb6828`; this crash lane was intentionally not rebased. Root integration will cherry-pick accepted commits onto the fresh integration base.

## TR-1..TR-9 matrix

| ID | Requirement | Status | Evidence / qualification |
|---|---|---|---|
| TR-1 | Separate KPPK Kaggle CPU job | **Done foundation / Partial live source** | independent kernel+wrapper, private input and provider lease; controlled payload run `controlled-20260717-kppk-v3` completed. Official KPPK HTML/PDF adapter is not implemented. |
| TR-2 | Separate bus Kaggle CPU job | **Done foundation / Partial live source** | independent kernel+wrapper, private input and provider lease; controlled payload run `controlled-20260717-bus-v1` completed. Official bus HTML adapter is not implemented. |
| TR-3 | Shared versioned transport schema | **Done** | `kenigevents.transport_provider.v1`; shared validator/normalizer, route/stops/date/tz/time/binding/source/validity hashes. |
| TR-4 | Validated fan-in, per-provider LKG, explicit invalid/partial/stale | **Done** | immutable provider attempts + `status.json`, combined `status.json`; attempt vs serving health is separate; timeout/invalid/partial/stale recovery tests. |
| TR-5 | Immutable combined manifest + safe current pointer | **Done** | file+directory fsync, atomic rename, allowlisted pointer paths, snapshot/content hash fail-closed reader. |
| TR-6 | Changed hash exactly one coalesced rebuild; unchanged zero; multiple updates merge | **Done locally** | durable pending/ack `combined/rebuild.json`; enqueue failure unchanged retry; pending-key merge and running-owner single deferred follow-up tested against actual `main.enqueue_job`. No production build was enqueued. |
| TR-7 | Schedule/default activation off | **Done** | no transport scheduler/`fly.toml` flag; test asserts absence. Nightly times remain documentation only. |
| TR-8 | Controlled provider/Kaggle canary | **Partial** | both private CPU kernels completed with synthetic reviewed JSON payloads after local gates; downloaded manifests/fan-in evidence below. No real official-source adapter, status DB/callback, or production publish path was present, so this is mechanics evidence only. |
| TR-9 | Do not change transport cards/listing/event-detail composition | **Done** | no `site/` or UI component/page file changed; prior PR #37 UI was not copied. |

## Retry-safe fan-in / outbox behavior

- The desired combined content hash is persisted before `combined/current.json` moves.
- SQLite enqueue acknowledgment is persisted only after the callback succeeds.
- An enqueue exception leaves the desired hash pending; an unchanged later provider refresh retries it.
- Multiple pending provider updates merge through `static_site_build:prod`.
- If that key is already running, `main.enqueue_job` now creates exactly one +15 minute deferred coalesced `static_site_build` follow-up; later updates re-arm the same pending row.

## Controlled Kaggle mechanics evidence (not production)

Local gates passed before either successful run. The package import failure from the first two KPPK probes was investigated from Kaggle logs, then against official Kaggle CLI kernel/dataset-source documentation. The accepted fix mounts the shared Python package through the unique private input dataset, verifies an allowlisted file manifest and SHA-256 for every mounted `.py`, and explicitly rewrites staged kernel metadata to the authenticated runtime owner. No unchanged third guess was attempted.

| Provider | Run id | Kernel ref | Result | Snapshot hash | Content hash |
|---|---|---|---|---|---|
| KPPK | `controlled-20260717-kppk-v3` | `zigomaro/kenigevents-transport-kppk-refresh` | `COMPLETE`, 1 service | `3ad401f5d5d6318f53ab83bdbb1fa2d0c13e3f4e1a5009cb25ecb792e62f80b7` | `859e284c88564c2137e06e1b972f3aa991e74c04323c95eb65eb7f8a2fffb4b1` |
| bus | `controlled-20260717-bus-v1` | `zigomaro/kenigevents-transport-bus-refresh` | `COMPLETE`, 1 service | `d448532f29d4bd2c842678d884f9277801efd0fe605ec43ea92a743d9d5c9b5a` | `0e8aa8cf544391e1fc17075727c8c1fe87dc955547a2f2065985cd4d1d21d754` |

Controlled fan-in produced:

- combined id `transport-28b103cec10c768ed5c4`;
- combined semantic hash `28b103cec10c768ed5c49a962956982cfb159e7248720b3a382bdfda0f4d191a`;
- immutable snapshot hash `564d4fd035be83ea6f41207ba0973728f413c1d4ba47eef29ceea19e0874fe2f`;
- rebuild intent intentionally `pending`, because no publish DB was supplied and no build was created.

Local uncommitted evidence (ignored by git):
`artifacts/codex/transport-refresh/controlled-canary-20260717/CANARY_SUMMARY.json` plus downloaded provider manifests/results/kernel logs and fan-in state.

Both terminal runs reported successful deletion of their unique private input datasets; a separate post-run account check found zero leftover controlled datasets. Status datasets were intentionally skipped because status DB/callback were absent. Thus there is no production `kaggle_run_ledger`, heartbeat callback, resource-acquire callback, static build id, or public manifest URL to claim.

## Tests

- `TMPDIR=/dev/shm PYTHONDONTWRITEBYTECODE=1 /home/dev/.codex/venvs/events-bot-new/bin/pytest -q -p no:cacheprovider tests/test_transport_refresh.py` — **16 passed**.
- `... pytest ... tests/test_transport_refresh.py tests/test_job_outbox_depends.py` — **18 passed** (transport plus adjacent outbox regression).
- `... pytest ... tests/test_kaggle_status.py tests/test_kaggle_notebook_status_instrumentation.py` — **19 passed**; the process then hung during interpreter shutdown and was terminated with `SIGTERM` after the green summary (PID `151447`, ~11 minutes). This is recorded separately per the Kaggle-status runbook.
- Python compile validation for package, kernels, publisher/runners and `main.py` — passed.
- both provider wrapper `--help` checks — passed.
- `git diff --check` — passed.

## Production / activation truth

- Production applied: **No**.
- Scheduler enabled: **No**.
- Real official-provider ingestion: **Blocked** on concrete reviewed KPPK/bus HTML/PDF adapters and source/licensing approval.
- Production canary/status ledger: **Blocked** on configured status DB, callback, manifest root and publish DB in the actual runtime.
- UI integration: excluded by scope.

Next activation steps: implement/review official adapters; run both kernels with real dated records and production-style status callback/leases; review immutable combined manifest without DB; run changed then unchanged publish-DB pair; verify one build id and status ledger; only then add default-off schedule config and separately approve activation.
