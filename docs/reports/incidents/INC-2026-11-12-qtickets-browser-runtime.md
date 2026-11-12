# Qtickets browser runtime missing system libraries

Status: open. Registry: inc_08fb0623433bb50f278f2578. Severity: sev2. Service: Qtickets source parser / Kaggle.

## Observed impact and cause

Normal parser operation 11980, run 0c32718ecbca4c76b2568922dffcd470, reached remote Qtickets ERROR at 12:16:22 UTC. Exact new-account target richss777/parse-qtickets version 6 failed before collecting events: Chromium could not load libatk-1.0.so.0. The actual log is retained under /data/incident_recovery/qtickets11980-readonly-output/parse-qtickets.log on RunCoveer. Other parser sources and social metrics completed; this is not general Kaggle authentication failure.

ensure_playwright returned success on an importable Python package. Its missing-package branch ran a handwritten apt package list with unchecked return codes. A failed dependency install could still report HAS_PLAYWRIGHT=True. Replace that list with Playwright's checked install --with-deps chromium command, including the already-importable case. No event interpretation, source URL, date, identity, fact, retry, timeout, account or model change. Production delta -11 lines, budget +5.

The two later releases did not change Qtickets, Telegram monitor or their metadata. Telegram target absence and prepared BGE artifact absence are separate migration issues. Preserve independently delivered related-event work and the calendar feature in any future release.

## Treat as regression guard when

Changing Qtickets notebook startup, browser dependencies or Kaggle runtime preparation.

## Mandatory checks before closure or deploy

- Actual startup function: importable package still prepares browser/system dependencies; missing package installs Python package first; failed dependency install cannot return ready. All three fail on the original and pass on the fix.
- Existing Qtickets structured-fact and occurrence identity contracts remain green.
- Qualified exact merged main, actual runtime ancestry and existing release governance.
- Actual new-account remote Chromium startup and nonempty Qtickets output, then ordinary exact import receipts. Green unit tests alone are not recovery.
- Do not replay already accepted Estrada/Yantarhall rows from operation11980 or interfere with active parse11983.

## Required evidence

Retained task /home/dev/artifacts/events-bot-new/20261112T101438Z-vk-calendar-rollout-20261112: parser11980-boundary.log, qtickets-failure-sanitized-extract.log, qtickets-bootstrap-before.log and final controls. Remote output contains execution diagnostics, not a source-data repair authorization. Incident remains open until actual source import succeeds.

## November 12 qualification and actual source recovery

PR893 merged after all four required checks passed. Exact main df9ba64d92e230d7909fbb6c139f0fd15dab41a3 is fully qualified, image runcoveer/events:qtickets-browser-deps-20261112, immutable sha256:d837939f17b1415248188f153b839f0cf59629fed7fc2da82f99ca5a9bc3bd3e. All4474 source files matched, actual db31 ancestry and unchanged dependency inputs proved. Initialized isolated actual startup and inherited calendar controls passed; wrong SHA rejected before initialization; network-none HTTP503 ready=false/DBskipped at156.7seconds; owned preflight removed. Browser package management is mocked in isolated controls, not actual Kaggle acceptance. Steady runtime has not yet switched; do not claim delivery or closure.

A one-time source-only recovery stages the exact merged Qtickets notebook in a private operator directory and invokes the existing normal remote runner against the confirmed failed target. SaveKernel accepted version7 with no invalid inputs. This launches no whole-parser replay and imports no already accepted source rows. Exclusive remote intent /data/operator_experiments/qtickets-browser-20261112/recovery-intent.json; reconcile it instead of repeating the driver. Normal source receipts and actual output remain required. Preserve active local resume operation11983 and the newer related-event changes; retain all original native/date/identity guards.
