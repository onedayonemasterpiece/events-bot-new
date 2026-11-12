# Qtickets browser runtime missing system libraries

Status: open. Severity: sev2. Service: Qtickets source parser / Kaggle.

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
