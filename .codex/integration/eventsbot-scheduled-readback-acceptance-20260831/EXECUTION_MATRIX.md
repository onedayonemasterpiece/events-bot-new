# Scheduled readback acceptance delta — execution matrix

Base: exact fetched `origin/main` `ab289db1750d60242fde37f07af305a6e67b84fa`.
Branch: `fix/eventsbot-scheduled-readback-acceptance-20260831`.

| ID | Requirement | Area | Dependencies | Conflict risk | Primary lane | Parallelizable | Done when |
|---|---|---|---|---|---|---|---|
| R1 | Telegram durable schedule envelope, raw scheduled/live reconciliation, logical albums, exact cardinality, bounded pending, and no resend after unknown | Telegram adapter/provider/runtime | R4 receipt identity | high: shared schemas/runtime | acceptance-integrator | discovery only | terminal outcomes and readback evidence match the requested contract; timeout/live/collision/album/zero/duplicate tests pass |
| R2 | `social_scheduled_items_list` with provider scheduled queues, opaque exact projection, ordered safe media digests, trust, catalogue and minimal read scopes | public schemas/tools + Telegram/VK adapters | R1/R5 digest helpers | high: shared public schemas | acceptance-integrator | discovery only | closed schema and both provider projections/scopes pass contract/runtime tests |
| R3 | Namespace-safe Telegram/VK scheduled deletion plus provider absence readback | Telegram/VK adapters | R1/R2 scheduled bindings | medium | acceptance-integrator | discovery only | scheduled delete tests prove exact namespace and absence without ordinary/text-only deletion |
| R4 | Terminal-only, retry-safe, same-logical-action numbered retry with complete receipt and single-flight | runtime/provider wrappers + schemas | R1/R5 terminal semantics | high: shared operation ledger | acceptance-integrator | discovery only | retry/idempotency/concurrency tests prove the full receipt and fail-closed rules |
| R5 | Diagnose and correct VK `media_upload_response_invalid` without assuming gzip; preserve safe diagnostics and pre-wall retry safety; cover four realistic PNGs | VK transport/adapter/provider ledger | production evidence | high: external API boundary | acceptance-integrator | discovery only | root cause is evidence-backed; realistic four-image multipart regression and invalid-receipt boundary tests pass |
| R6 | Full Private Events MCP suite, compileall, docs/changelog, PR/CI/merge/release gates and exact SHA/health evidence | tests/docs/release | R1–R5 | high | acceptance-integrator | serial after R1–R5 | full suite and compileall pass; branch is pushed and PR opened; deploy only after clean exact-main merge and green CI |

## Dependency order

`production evidence + exact-main audit` → `failing acceptance tests` →
`shared schema/runtime changes` → `provider adapter changes` →
`targeted tests` → `full suite/docs/changelog` → `push/PR/CI` →
`exact-main deploy/health only if every release gate is satisfied`.

No historical carousel publication, provider retry, deletion, or other social
provider mutation is permitted by this task.
