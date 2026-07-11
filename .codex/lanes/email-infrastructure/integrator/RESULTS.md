# Email infrastructure integration Results

## Requirement closure

| ID | State | Evidence |
|---|---|---|
| R01 | Done | Clean linked worktree and main-based integration branch; stale dirty checkout was never used for deploy or provider mutation. |
| R02 | Done | Temporary Postbox key hygiene repaired and all task-created temporary keys removed. |
| R03 | Done | Canonical routing, data ownership, 200-user cap and release gates documented. |
| R04 | Done | SpaceWeb mailboxes and DNS verified without NS/site regression. |
| R05 | Done | Direct-trigger and retained-mailbox inbound paths are live, private, idempotent and DLQ-backed. |
| R06 | Done | Live additive Supabase control plane and receipt RPC; all outbound switches disabled. |
| R07 | Partial | Postbox identity/seed real-ID proof passed; Spam placement and missing event destination block production sending. |
| R08 | Partial / blocked | Domain/sender/cap ready; NotiSend API activation is still pending externally. |
| R09 | Partial | Both inbound paths and Postbox seed were exercised from the required kgd80 sender; provider failure/unsubscribe production drills remain outbound gates. |
| R10 | In progress | Integration changes must pass final checks, be pushed and merged to `origin/main`; no Fly/core deploy is required because no existing bot runtime is enabled or changed. |

## Operational posture

- Inbound foundation: enabled and verified.
- Human mailbox: enabled and retained at SpaceWeb.
- Transactional outbound: disabled.
- Recommendation outbound: disabled; zero admitted users.
- Fly/core production application: unchanged.

Detailed redacted evidence is stored only in ignored
`artifacts/codex/email-infrastructure-20260711/`.
