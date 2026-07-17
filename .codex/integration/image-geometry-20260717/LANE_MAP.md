# Image geometry lane map

```yaml
mode: serial_integrator_with_parallel_read_only_discovery
repo: events-bot-new
base_ref: origin/main
base_sha: d169004376c309dc487fa6b48a7aae4a8ed7dea3
integration_branch: integration/image-geometry-20260717
global_constraints:
  - no provider/backfill before smoke gate
  - no Fly image-download traffic for historical backfill
  - normalized coordinates only; downstream crop remains separate
  - quota-aware KEY4+KEY5 pool from first reservation, not fallback
  - paced queue, RPD reserve floor, checkpointing
verification_owner: /root
status: complete
stop_conditions:
  - provider 429 or unexpected quota-group collision
  - invalid schema/empty response rate above smoke allowance
  - visual QA shows systematic bad face/value boxes
lanes:
  - id: R01
    role: planner
    execution_mode: read_only_parallel
    target: EventPoster schema and Smart Update hook mapping
    effort: high
  - id: R02
    role: planner
    execution_mode: read_only_parallel
    target: normal quota-aware key pool design
    effort: extra-high
  - id: R03-R04
    role: planner
    execution_mode: read_only_parallel
    target: local/Kaggle backfill and visual QA patterns
    effort: high
  - id: R01-R05-integrator
    role: worker+integrator+reviewer
    execution_mode: serial_after_dependency
    branch: integration/image-geometry-20260717
    worktree: /home/dev/.codex/worktrees/events-bot-new/image-geometry-20260717
    writable_files: schema/models/smart-update/google-ai/scripts/kaggle/tests/docs/changelog
    verification_scope: full_local+live_smoke+bounded_backfill
    result: completed
```
