# Integration report — static-site resilient transport and ecological egress

| Lane | Requirement IDs | Branch | Status | Head SHA | Merge/cherry-pick | Evidence |
|---|---|---|---|---|---|---|
| M1 dynamic/browser map | R01,R02,R05,R07 | read-only | completed | — | n/a | Direct bypass and storage inventories received |
| M2 static egress map | R04 | read-only | completed | — | n/a | Wide related RPC identified as 3.10 GB root cause |
| M3 security map | R03 | read-only | completed | — | n/a | Relay/RPC/Auth threat model received |
| M4 P0/P1 gap map | R06 | read-only | completed | — | n/a | Deployment and closure gaps classified |
| W1 client transport/storage | R01,R02,R05,R07 | `agent/static-site-resilient-egress/W1` | completed | `1b9a55f1` | `e42292f0`, evidence `003357f8` | 41/41 client, 53/53 focus, 466-page donor build |
| W2 compact egress | R04 | `agent/static-site-resilient-egress/W2` | completed | `ba3e6f53` | `6aee6cbf`, evidence `37ae6521` | 55/55 core, 39/39 adjacent, 466-page donor build |
| W3 security hardening | R03 | `agent/static-site-resilient-egress/W3` | completed | `8fbbc8ce` | `d9a574cd`, evidence `051ab436` | 8/8 Python, 5/5 Deno, local SQL contracts |
| W4 atomic root publisher | R04,R06 | `agent/static-site-resilient-egress/W4` | completed, default-off | `d1fed032` | `975a5e97` + evidence | 57/57; live infrastructure absent |
| I1 integration/release | R06 | `integration/static-site-resilient-egress-20260731` | running | — | current | pending |

## Acceptance reminders

- no direct runtime Supabase access outside the shared client and deliberate diagnostic probes;
- unsafe/cost-bearing POST is never blindly repeated across routes;
- idempotent local actions have bounded storage and server deduplication before replay;
- non-auth browser state stays within its tested byte budget and old preview cache keys are removed;
- full related rebuild response bytes remain below the compact ceiling;
- relay rejects unknown services, functions, RPCs and methods before upstream;
- SQL grants/RLS/caps and Auth live link/code/Yandex journeys have independent evidence;
- root rollout remains blocked until atomic current/previous promotion and rollback are proven.
