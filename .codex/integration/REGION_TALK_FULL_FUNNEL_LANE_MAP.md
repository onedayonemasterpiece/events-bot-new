# Region Talk Full-Funnel Debug Lane Map

```yaml
mode: mixed
repo: events-bot-new
base_ref: 85f29b8469b090dd8b631251878b609dcbe4a0c4
base_branch: agent/region-talk/bge-m3-enrichment-test
integration_branch: agent/region-talk/bge-m3-enrichment-test
global_constraints:
  - Discovery, fast-check, keyword and similar-channel methods remain enabled.
  - No scheduler/cron rollout in this phase.
  - Gemini requests share the existing global limiter and must not exceed 100 for the live debug run.
  - Only strict source/text/image eligible rows may reach Gemini or the operator chat.
  - Telegram delivery target is the existing prepared chat identified by https://t.me/+kfaIRh98oHVkYWFi.
  - CandidateReport stays E5-only; BGE-M3 stays an isolated CPU notebook.
verification_owner: INTEGRATE
stop_conditions:
  - target Telegram chat cannot be resolved from the approved role-scoped session or durable configuration
  - global Gemini budget cannot be proved before calls
  - publication safety would require weakening local/multiregion/ad/source/image gates
  - Telegram FloodWait or session-role conflict requires bypassing cooldown
dependency_graph:
  L1_DISCOVERY: []
  L2_FINALIZER: []
  L3_DELIVERY: []
  L4_METRICS: []
  INTEGRATE: [L1_DISCOVERY, L2_FINALIZER, L3_DELIVERY, L4_METRICS]
requirements:
  R01: fast-check and keyword-discovered publics are measurably prioritized into normal scanning
  R02: eligible posts reach strict Gemini verification
  R03: Gemini-accepted posts are delivered exactly once to the prepared operator chat
  R04: optimize operations per accepted candidate without weakening quality
  R05: debug through live runs now, without scheduling
  R06: enforce a shared Gemini budget ceiling of 100 requests
  R07: define and validate a path to more than three chat candidates per day
lanes:
  - id: L1_DISCOVERY
    role: code_mapper
    effort: medium
    requirement_ids: [R01]
    target: map fast-check/keyword priority, cursor movement, dedup and conversion metrics
    execution_mode: parallel
    expected_output: verified code paths, metric evidence, concrete bottlenecks
    verification_scope: inspection_only
    status: planned
  - id: L2_FINALIZER
    role: code_mapper
    effort: high
    requirement_ids: [R02, R06]
    target: audit strict finalizer eligibility, Gemini limiter/budget and retry semantics
    execution_mode: parallel
    expected_output: call contract, budget proof, blocking defects and bounded fixes
    verification_scope: inspection_only
    status: planned
  - id: L3_DELIVERY
    role: code_mapper
    effort: high
    requirement_ids: [R03]
    target: audit prepared-chat resolution, idempotent send and delivery evidence
    execution_mode: parallel
    expected_output: exact config/code path, dedup contract and safe live verification plan
    verification_scope: inspection_only
    status: planned
  - id: L4_METRICS
    role: code_mapper
    effort: high
    requirement_ids: [R04, R07]
    target: build full-funnel conversion/efficiency diagnosis and >3/day acceptance forecast
    execution_mode: parallel
    expected_output: stage conversion table, bottleneck ranking, measurable acceptance gates
    verification_scope: inspection_only
    status: planned
  - id: INTEGRATE
    role: merge_reviewer
    effort: extra-high
    requirement_ids: [R05]
    target: serial implementation, docs/changelog, tests, baseline, live runs, final product reflection
    depends_on: [L1_DISCOVERY, L2_FINALIZER, L3_DELIVERY, L4_METRICS]
    execution_mode: serial_after_dependency
    writable_files:
      - scripts/region_talk_orchestrator.py
      - scripts/region_talk_publication_finalizer.py
      - scripts/region_talk_goal_notify.py
      - kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py
      - relevant tests
      - docs/features/region-talk-channel/*
      - CHANGELOG.md
    verification_scope: full_local_and_live
    status: in_progress
```

## Execution matrix

| ID | Area | Dependencies | Conflict risk | Lane | Done when |
|---|---|---|---|---|---|
| R01 | discovery priority | none | medium | L1 | fast-check/keyword hits demonstrably enter near-term source/post processing |
| R02 | Gemini verification | R01, dual vectors, image score | high | L2 | strict eligible row reaches Gemini with auditable result |
| R03 | Telegram delivery | R02 | high | L3 | accepted URL is sent once and durable send evidence prevents duplicates |
| R04 | efficiency | all metrics | medium | L4 | operations per candidate and top bottleneck are measured and improved |
| R05 | live debugging | L1-L4 | high | controlled live run produces full funnel evidence; no scheduler changes |
| R06 | shared LLM budget | R02 | high | preflight and post-run counters prove no more than 100 requests |
| R07 | >3/day outcome | R01-R06 | medium | forecast is based on observed stage conversion and explicit capacity targets |
