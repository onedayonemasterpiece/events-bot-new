# Region Talk P0 Lane Map

```yaml
mode: mixed
repo: events-bot-new
base_ref: 7c8fdc5bd2c1db590dec773f0563bbc8fc2647e8
base_branch: agent/region-talk/bge-m3-enrichment-test
integration_branch: agent/region-talk/bge-m3-enrichment-test
global_constraints:
  - Telegram keyword, hashtag, and similar discovery remain enabled.
  - Manual source inflow remains enabled and is attributed separately.
  - E5 remains in CandidateReport; BGE-M3 remains an isolated CPU worker.
  - No public-web fallback masks Telethon cooldowns.
  - Production publication remains disabled; only the operator shortlist is built.
verification_owner: integration lane
stop_conditions:
  - queue migration cannot prove row preservation
  - Telethon contract would require bypassing FloodWait/cooldown
  - authoritative source eligibility cannot be joined safely
lanes:
  - id: L1
    role: worker
    requirement_ids: [R02, R03]
    target: CandidateReport queue/cache/exact/shared eligibility
    execution_mode: parallel
    branch: agent/region-talk/l1-candidate-report
    worktree: /home/dev/projects/events-bot-new-region-talk-l1
    writable_files:
      - kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py
      - tests/test_region_talk_candidate_report.py
    status: committed_and_integrated
  - id: L2
    role: worker
    requirement_ids: [R01, R02, R05]
    target: orchestrator actions and complete metrics
    execution_mode: parallel
    branch: agent/region-talk/l2-orchestrator
    worktree: /home/dev/projects/events-bot-new-region-talk-l2-orchestrator
    writable_files:
      - scripts/region_talk_orchestrator.py
      - tests/test_region_talk_orchestrator.py
    status: committed_and_integrated
  - id: L3
    role: worker
    requirement_ids: [R03]
    target: publication finalizer safety and retries
    execution_mode: parallel
    branch: agent/region-talk/L3
    worktree: /home/dev/projects/events-bot-new-region-talk-l3
    writable_files:
      - scripts/region_talk_publication_finalizer.py
      - tests/test_region_talk_publication_finalizer.py
    status: committed_and_integrated
  - id: L4
    role: worker
    requirement_ids: [R03]
    target: ImageDiagnostic signed eligibility enforcement
    execution_mode: parallel
    branch: agent/region-talk/l4-image-eligibility
    worktree: /home/dev/projects/events-bot-new-region-talk-l4-image-diagnostic
    writable_files:
      - kaggle/RegionTalkImageDiagnostic/region_talk_image_diagnostic.py
      - tests/test_region_talk_image_diagnostic.py
    status: committed_and_integrated
  - id: INTEGRATE
    role: merge_reviewer
    requirement_ids: [R01, R02, R03, R04, R05]
    target: serial integration, docs, changelog, regression tests, live baseline/run
    depends_on: [L1, L2, L3, L4]
    execution_mode: serial_after_dependency
    status: in_progress_full_suite_194_ok_final_review_running
```
