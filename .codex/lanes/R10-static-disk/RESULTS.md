# R10-static-disk results

## Lane contract

- Requirement: `R10`
- Base SHA: `bd661f84`
- Implementation SHA: `be3bd77848351e325c9f78761ece8fae05873808`
- Branch: `agent/keyboard-navigation-production/R10-static-disk`
- Forbidden surfaces (`site/`, `docs/`, `CHANGELOG.md`,
  `video_announce/poller.py`): untouched

## Outcome

The static builder no longer depends on an unbounded repository-root output
tree:

- `STATIC_SITE_ARTIFACT_ROOT` is shared by the host finisher/recovery path and
  Kaggle runner; Fly config places it at `/data/static_site_builder`.
- Kaggle staging uses `STATIC_SITE_SCRATCH_DIR` under the same persistent root
  instead of an implicit system `TemporaryDirectory`.
- terminal cleanup recognizes only exact `output-production-*` directories;
  unknown root entries, preview/operator paths and symlinks are never deleted.
- the exact build id from a readable active durable handoff is preserved. A
  malformed/missing active handoff fails closed and skips retention.
- normal and adopted successful outputs are deleted only after the immutable
  success/current-candidate receipt commits. A retryable remote handoff keeps
  its output; non-recoverable terminal output is removed.
- the default retained terminal full-output count is zero. Retention remains
  configurable through `STATIC_SITE_OUTPUT_KEEP_LATEST_TERMINAL`.

Before full files are removed, allow-listed numeric `counts` are copied from
the validated build result into both the outbox build receipt and durable
static-site success history/state evidence. The existing redacted diagnostic
collector reads these counts without needing a local archive or manifest.
Only six numeric count families are accepted; candidate tokens, URLs and
arbitrary result fields cannot enter this diagnostic payload. Terminal
handoff bearer data is cleared after success becomes durable (or after a
non-recoverable terminal failure), never before recoverability is resolved.

Runtime readiness now reports two independent filesystems:

- `disk`: persistent `/data` capacity (existing contract);
- `scratch_disk`: `/tmp` capacity plus a real create/write/flush/fsync/close/
  remove probe.

A critical/unknown/unwritable scratch result makes `/healthz` return `503`.
The Smart Update static job and runner independently repeat the root scratch
probe before Kaggle submission, and also probe the configured `/data` work
directory. Error evidence contains only status and exception class names.

## Production configuration changes

`fly.toml` now sets:

```text
RUNTIME_SCRATCH_PATH=/tmp
RUNTIME_SCRATCH_WARN_FREE_MB=1024
RUNTIME_SCRATCH_CRITICAL_FREE_MB=512
STATIC_SITE_ARTIFACT_ROOT=/data/static_site_builder
STATIC_SITE_SCRATCH_DIR=/data/static_site_builder/tmp
STATIC_SITE_STORAGE_WARN_FREE_MB=1536
STATIC_SITE_STORAGE_CRITICAL_FREE_MB=1024
STATIC_SITE_OUTPUT_KEEP_LATEST_TERMINAL=0
```

The same knobs and defaults are present in `.env.example`.

## Validation

- `python -m py_compile` via
  `/home/dev/.codex/venvs/events-bot-new/bin/python` — passed for
  `runtime_disk.py`, `static_site_release.py`, runner, `main.py` and
  `main_part2.py`.
- focused static/release/handoff/diagnostics/disk/health selection —
  `51 passed, 1 deselected`.
- static debounce and Kaggle status regression suites — `28 passed`.
- `git diff --check` — passed.

New regression coverage proves:

- tempfile probes are removed after fsync and probe errors force critical;
- `/healthz` returns `503` for unwritable scratch and `200` after recovery;
- active output identity preservation and malformed-identity fail-closed
  behavior;
- recognized-only bounded deletion with unknown paths and symlinks retained;
- configurable shared artifact/scratch resolution;
- safe numeric count extraction and diagnostics after the full output tree is
  deleted.

One pre-existing unrelated assertion in
`test_running_vk_sync_stale_retries_instead_of_terminal_dependency_block`
failed because the local SQLite driver returned an offset-naive datetime for
comparison with an aware datetime; it does not exercise this lane. The local
pytest process also remained in interpreter shutdown after printing completed
results because of an existing background thread, so it was interrupted only
after the summaries above were emitted.

## Risks and integration handoff

- `/tmp` remains an intentional global readiness gate even though static
  output and builder working scratch move to `/data`. This prevents a green
  service and retry consumption while other Python/runtime operations cannot
  create tempfiles; static builds resume only after root cleanup or a clean
  deploy restores `/tmp`.
- `/data` is finite. The 1024 MiB critical floor is deliberately higher than
  one observed approximately 460 MiB output, while the 1536 MiB warning is
  visible but does not by itself fail readiness. Operators should expand the
  volume if normal snapshot + staging + output growth approaches that floor.
- Moving the configured root does not delete legacy
  `/app/artifacts/codex/static-site-builder` files in an already-full running
  machine. The integrator must recover root headroom by assertion-approved
  cleanup or a clean deploy; clean Fly rootfs replacement is expected to
  remove legacy overlay files.
- This lane performed no production deletion, deploy, DB mutation, Kaggle run
  or compensating Smart Update catch-up. Incident closure still requires the
  live `df`, tempfile, `/healthz`, SQLite quick-check, runtime-log, fresh secret
  candidate, root-isolation, exact lease-release and catch-up evidence listed
  in the canonical incident record.
