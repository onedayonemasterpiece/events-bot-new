mode: serial_integrator
repo: events-bot-new
base_ref: origin/main
base_branch: main
integration_branch: agent/region-talk/cliche-ban-20260801
global_constraints:
  - LLM-first semantic rewrite; deterministic detector is a fail-closed style guard only
  - no publishing API calls
  - production YDB writes only after dry-run inventory and code tests
  - preserve unrelated work in all existing worktrees
verification_owner: /root
stop_conditions:
  - missing Region Talk YDB credentials
  - candidate rewrite cannot preserve grounding/evidence contract
lanes:
  - id: W01
    role: worker
    requirement_ids: [R02, R03]
    target: prompt, verifier, deterministic final-text gate, tests, canonical docs, changelog
    depends_on: [read_only_code_map, opus_prompt_review_attempted_blocked]
    execution_mode: serial_after_dependency
    branch: agent/region-talk/cliche-ban-20260801
    worktree: /home/dev/.codex/worktrees/events-bot-new/region-talk-cliche-ban-20260801
    writable_files:
      - scripts/region_talk_publication_draft_backfill.py
      - scripts/region_talk_goal_notify.py
      - scripts/region_talk_orchestrator.py
      - tests/test_region_talk_publication_draft_backfill.py
      - tests/test_region_talk_goal_notify.py
      - docs/features/region-talk-channel/*
      - CHANGELOG.md
      - .codex/lanes/region-talk-cliche-ban/*
      - .codex/integration/*
    forbidden_files: [production credentials, unrelated modules]
    expected_output: committed code/docs/tests with fail-closed cliché guard
    verification_scope: targeted
    status: completed
  - id: W02
    role: integrator
    requirement_ids: [R01]
    target: inventory and controlled YDB candidate-text backfill including Archi.ru
    depends_on: [W01]
    execution_mode: serial_after_dependency
    branch: agent/region-talk/cliche-ban-20260801
    worktree: /home/dev/.codex/worktrees/events-bot-new/region-talk-cliche-ban-20260801
    writable_files: [artifacts/codex/region-talk-cliche-backfill/*, YDB candidate/editorial rows selected by dry-run]
    forbidden_files: [published Telegram/VK posts, source evidence, unrelated YDB rows]
    expected_output: dry-run inventory, backed-up mutations, post-write verification
    verification_scope: targeted
    status: partial_quota_blocked
