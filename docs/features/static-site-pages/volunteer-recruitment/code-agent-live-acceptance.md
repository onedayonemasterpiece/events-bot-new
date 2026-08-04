# Code-agent task: integrate and live-accept Volunteer Monitor

Repository: `onedayonemasterpiece/events-bot-new`
Base: fresh `main` after the docs-only PR #331 decision.
Input: the volunteer-monitor implementation patch/bundle.

## Goal

Integrate the read-only `Добро.рф` source skeleton, run the real direct GitHub canary first, and repair only evidence-backed selector/extraction defects. Then run the optional Kaggle canary. Do not implement SQLite apply, BGE matching or public UI in this task.

## Required sequence

1. Integrate the new source package, fixtures, workflow and status documentation.
2. Run `pytest` and `compileall`.
3. Merge or temporarily install the workflow on the default branch: GitHub `workflow_dispatch` is not available from an arbitrary unmerged branch through the UI.
4. Set repository variables:
   - `VOLUNTEER_MONITOR_PERMISSION_REFERENCE`;
   - `VOLUNTEER_KAGGLE_KERNEL_SLUG=eventsbot/kenigevents-volunteer-monitor`.
5. Run `Volunteer monitor smoke` with `executor=direct`.
6. Inspect `discovery-receipt.json`, failure HTML and failure-only screenshot. Repair current selectors for:
   - `Калининградская область`;
   - `С доступными вакансиями`;
   - `Показать еще` terminal traversal;
   - canonical `/event/<id>` extraction.
7. Require a real open-page and closed-page state proof. Never convert an unproven selector failure into zero supply.
8. Ask the owner to create GitHub Environment `volunteer-monitor-canary` and add one environment secret `KAGGLE_API_TOKEN`. Do not request the token in chat, print it, or commit `kaggle.json`.
9. Run `executor=kaggle`, download output, and verify the receipt/result SHA-256 contract.
10. Record exact workflow run IDs, artifact names, current selectors and live counts in the implementation-status document.

## Definition of done

- fixture tests green;
- direct live canary terminal `PASS` or truthful `WARN_NO_LIVE_SUPPLY` with proven filters;
- Kaggle canary terminal PASS;
- fixture/direct/Kaggle output schema identical;
- current real source demonstrates `OPEN` and `CLOSED` parsing;
- region filter proof is retained;
- no PII appears in persisted excerpts/artifacts beyond failure HTML subject to 14-day private retention;
- production DB, festival queue and public site remain unchanged.
