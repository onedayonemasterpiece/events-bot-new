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
| I1 integration/release | R06 | `integration/static-site-resilient-egress-20260731` | partial: data plane live, root rollout gated | `98c9ff25` | current | Supabase schema + `event-search` + exact relay live; 466-page build; atomic root infrastructure absent |

## Integration evidence — 2026-07-31

- Production Supabase migration ledger matches the repository through
  `20260731193000`; compact related retrieval, capped saved-event writes,
  service-only Search internals, focus participant admission and idempotent
  feedback are applied.
- `event-search` is deployed. The permanent Yandex relay is active without a
  service account or request logging and exposes only the reviewed method/path
  set.
- A live CORS regression was found during integration: the fixed upstream
  reflected an arbitrary caller `Origin`. All 29 integrations now replace it
  with `https://kenigevents.ru`. Chromium accepts the production origin and
  rejects `https://example.com`; unsupported routes return `404` at Gateway.
- Validation: resilient client `42/42`, focus product `71/71`, Python release /
  security / migration suite `88/88`, Edge Function `18/18`, Astro `466`
  pages.
- Root publication remains default-off: the two complete root buckets and ALB
  promotion/rollback resources required by W4 do not yet exist. No unsafe
  in-place root overlay was performed.
- Incident closure is not claimed: separate live email-code and magic-link
  issuances plus the final root/current focus release remain open gates.

## Acceptance reminders

- no direct runtime Supabase access outside the shared client and deliberate diagnostic probes;
- unsafe/cost-bearing POST is never blindly repeated across routes;
- idempotent local actions have bounded storage and server deduplication before replay;
- non-auth browser state stays within its tested byte budget and old preview cache keys are removed;
- full related rebuild response bytes remain below the compact ceiling;
- relay rejects unknown services, functions, RPCs and methods before upstream;
- SQL grants/RLS/caps and Auth live link/code/Yandex journeys have independent evidence;
- root rollout remains blocked until atomic current/previous promotion and rollback are proven.
