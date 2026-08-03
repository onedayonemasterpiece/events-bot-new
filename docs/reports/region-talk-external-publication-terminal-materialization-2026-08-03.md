# Region Talk external-publication terminal materialization receipt

Date: 2026-08-03

- Source PR: #237
- Materialization workflow run: `30808382823`
- Materialized branch commit: `ff9e3309f1ef66ab6131f6a96b8d8aa9d82b4cb0`
- Predecessor identities reconciled by the checked-in plan: **54**
- Terminal `candidate_report`: **43**
- Terminal `manual_review_required`: **0**
- Terminal transitions: **32** retained candidates, **11** completed-research promotions, **5** exclusions, **6** unresolved leads
- Publisher dossiers: **32** sources covering all **43** retained candidates
- Current importer validation: **15/15 + 13/13 + 15/15**, `rejected=0`, `conflicts=0`
- Focused regression contracts: **61 passed**
- YDB effect of the materialization workflow: **none**

This receipt intentionally triggers the ordinary pull-request CI on the durable, materialized diff. YDB reconciliation and import remain separate protected post-merge operations and require their own machine-readable receipts.
