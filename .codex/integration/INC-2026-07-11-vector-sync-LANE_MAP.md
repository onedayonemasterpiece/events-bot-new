# Lane map: INC-2026-07-11 vector sidecar sync

- requirement_id: R01
  description: Trace why production event-search documents and embeddings stopped after 2026-07-02.
  owner: vector_pipeline_trace + prod_logs_kaggle
  dependencies: []
  validation: Code-path map plus production scheduler/ops evidence identifies the first broken handoff.
  status: in_progress
- requirement_id: R02
  description: Add durable regular incremental embedding synchronization.
  owner: integrator
  dependencies: [R01]
  validation: Targeted tests and a production run update only missing/changed documents and embeddings.
  status: pending
- requirement_id: R03
  description: Catch production sidecar up to the current actionable event inventory.
  owner: integrator
  dependencies: [R02, R07]
  validation: Post-run freshness and coverage query, with failures and skips accounted for.
  status: pending
- requirement_id: R04
  description: Make vector sync and Smart Update evidence sufficient for incident verification.
  owner: prod_logs_kaggle + integrator
  dependencies: [R01]
  validation: Stage/run identifiers, counts, failures, latency and freshness are queryable from retained evidence.
  status: pending
- requirement_id: R05
  description: Run a live Telegram-UI VK auto-import E2E limited to 1-3 real candidates.
  owner: vk_e2e_map + integrator
  dependencies: [R04, R07]
  validation: UI response, runtime/ops evidence and resulting canonical rows agree.
  status: pending
- requirement_id: R06
  description: Debug the Smart Update path step by step for the E2E candidates.
  owner: vk_e2e_map + integrator
  dependencies: [R05]
  validation: Extraction, identity recall, LLM decision, merge/create, jobs and vector follow-up are individually evidenced.
  status: pending
- requirement_id: R07
  description: Ship tests, canonical docs, changelog, origin/main integration and production deploy.
  owner: integrator
  dependencies: [R02, R04]
  validation: Clean tests; deployed SHA reachable from origin/main; incident regression contracts pass.
  status: pending
- requirement_id: R08
  description: Classify residual findings as fixed incidents or explicit technical debt.
  owner: integrator
  dependencies: [R03, R06]
  validation: Incident closure record contains evidence and owned follow-ups.
  status: pending

## Integration order

1. Complete all three read-only discovery lanes.
2. Implement R02/R04 serially in the clean integration worktree.
3. Test and merge/push to `origin/main`; deploy only that clean SHA.
4. Run R03 compensating catch-up.
5. Run mutating R05/R06 live E2E serially and repair any regression found.
6. Close R08 only after post-deploy evidence is recorded.
