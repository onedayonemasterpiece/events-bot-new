# Google TTS quota gateway execution matrix

| ID | Requirement | Area | Dependencies | Lane | Done when |
|---|---|---|---|---|---|
| R01 | Register a 10 requests/day TTS limit | Supabase Google AI limiter | Existing model-limit table/RPC | serial-integrator | Both supported TTS model IDs have RPD 10 |
| R02 | Account for the already-used request | Supabase counters/audit | R01, registered `GOOGLE_API_KEY` row | serial-integrator | Historical success is represented once and daily usage is at least 1 |
| R03 | Prevent TTS without shared limit control | Python gateway/CLI | Existing reserve/mark/finalize lifecycle | serial-integrator | Missing limiter/registry/RPC blocks before provider |
| R04 | Generate through several registered keys | Python gateway/CLI | R01, normal key-pool selection | serial-integrator | Atomic reserve chooses one eligible key before one provider call |
| R05 | Create a reusable Codex skill | `.codex/skills` | R03/R04 CLI contract | serial-integrator | Skill validates and bans raw-key/direct API paths |
| R06 | Tests, canonical docs, changelog, delivery | tests/docs/git | R01-R05 | serial-integrator | Targeted tests pass; docs/changelog match; branch is pushed |

Dependency order: `R01 -> R03 -> R04 -> R05 -> R06`; `R02` depends on `R01` and the live registry.

