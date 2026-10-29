# Events Bot worktree audit — 2026-10-29

The owner requested cleanup after project discovery returned multiple Events Bot checkouts. The repository had 13 registered worktrees before this task. Seven completed worktrees were removed using `git worktree remove`, without force. Their commits remain in Git and, for unique commits, on the named origin branches. Only generated Python/pytest caches were discarded. No credentials, runtime state, or dirty source files were removed.

## Removed worktrees

| Checkout | Preserved branch / state |
| --- | --- |
| `/home/dev/projects/events-bot-cherryflash-20261028` | `docs/cherryflash-token-blocked-20261029`; unique commits retained on origin branch |
| `/home/dev/projects/events-bot-new-gpt4o-route-20261027` | `docs/runcoveer-vk-route-incident-20261028`; contained in origin/main |
| `/home/dev/projects/events-bot-new-kaggle-retention-20261020` | `fix/kaggle-retention-stable-pagination-20261020`; contained in origin/main |
| `/home/dev/projects/events-bot-new-scheduler-delay-20261014` | `d431b9f86b87999cf27f74b99857d23aedc28f16`; contained in origin/main |
| `/home/dev/projects/worktrees/events-guide-links-20261018` | `feature/incident-guide-links-20261018`; unique commits retained on origin branch |
| `/home/dev/projects/worktrees/events-vk-recovery-20261023` | `fix/vk-recovery-evidence-20261023`; unique commits retained on origin branch |
| `/home/dev/projects/worktrees/events-vk-revert-20261023` | `fix/revert-identity-retry-20261023`; contained in origin/main |

## Preserved unfinished work

These six checkouts contain uncommitted work. Their retention is deliberate: merging, discarding or publishing those changes requires a separate assessment of each task. They are not alternate production release sources. Production remains the single RunCoveer Events container; its observed image revision before this task was `b156fc067749e920383e7559e9e7efca804a1461`, contained in `origin/main`.

| Checkout | Tracked changed files | Scope |
| --- | --- |
| `/home/dev/projects/events-bot-new` | 8 | Preview registry plus event-quality incident changes |
| `/home/dev/projects/events-bot-new-islands-home` | 2 | Interest club data and a local preview launcher |
| `/home/dev/projects/events-bot-new-partner-mcp` | 63 | Partner event/promo MCP implementation and local test environment |
| `/home/dev/projects/events-bot-new-partner-mcp-r2` | 0 | Nine untracked incident inspection/build scripts |
| `/home/dev/projects/events-bot-new-preview-registry` | 80 | Design-system and static-site UI work |
| `/home/dev/projects/events-bot-new-vk-auto-storage-storm` | 21 | Static-site UI/search work |

Audit inventory, separate staged/working patches and removal receipts are retained in:
`/home/dev/artifacts/events-bot/20261029T144150Z-reminders-worktree-audit-20261029`. Untracked source remains in each original checkout. The inventory contains filenames, not credential values.

The temporary reminder-removal worktree is removed after merge and live verification. Existing lifecycle instructions in `docs/operations/repository-workflow.md` already require removal after completion; this audit does not introduce another cleanup mechanism.
