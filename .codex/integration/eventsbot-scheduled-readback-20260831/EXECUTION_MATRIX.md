# EVENTSBOT-MCP-20260831-SCHEDULED-READBACK execution matrix

Base: `origin/main` at `64f75d10f7aff33fa616cee212878bd9d03673b1`.

| ID | Requirement | Area | Likely files | Dependencies | Conflict risk | Primary lane | Parallelizable | Done when |
|---|---|---|---|---|---|---|---|---|
| R1 | Eliminate nested-timeout cancellation race and finalize every claimed provider operation | runtime / Telegram ledger | `social_workspace_runtime.py`, `private_events_mcp_telegram_adapter.py`, `private_events_mcp_workspace_providers.py` | provider deadline contract | high | telegram-ledger, integrated serially with tools-runtime | partly | cancellation cannot leave an expired null-result claim |
| R2 | Persist sanitized/encrypted Telegram reconciliation evidence | provider store | `private_events_mcp_workspace_providers.py`, Telegram adapter | R1 intent contract | high | telegram-ledger | no | restart has sufficient encrypted exact intent and bounded attempt state |
| R3 | Provider-backed Telegram scheduled/live reconciliation with bounded convergence | Telegram adapter | `private_events_mcp_telegram_adapter.py` | R2 | high | telegram-ledger | no | exact/zero-window/ambiguous/terminal paths pass |
| R4 | Add `social_scheduled_items_list` using existing schedule scopes | public contract/runtime/providers | `social_workspace.py`, `social_workspace_runtime.py`, `social_workspace_tools.py`, adapters | provider logical list API | medium | tools-runtime | yes after provider interface agreed | bounded/redacted logical publications returned without new consent |
| R5 | Exact scheduled cancellation and absence verification | Telegram/VK adapters | Telegram/VK adapters and tests | R4 queue reads | medium | telegram-ledger + vk-diagnostics, integration acceptance serial | partly | scheduled namespaces used and exact absence proved |
| R6 | Retry-safe bounded action retry with single-flight attempts | runtime/store/tools | social workspace contract/runtime/tools | stable action/preparation/operation links | high | tools-runtime | yes | retry accepted only for terminal retry-safe failure and concurrent retry prevented |
| R7 | Diagnose/fix VK four-image upload validation and add safe observability | VK transport/adapter | VK adapter/transport/upload/provider store | sanitized production evidence | medium | vk-diagnostics | yes | concrete cause recorded; response shapes/observability/readback tests pass |
| R8 | Failing-before/passing-after Telegram, VK, runtime/tool tests plus full suite/compile checks | tests/verification | relevant `tests/test_private_events_mcp_*` | R1-R7 | high | each worker targeted; integrator full suite | partly | targeted + complete suite + compileall pass |
| O1 | Incident, release, read-only queue reconciliation and machine-usable operational handoff | docs/ops | incident record, private MCP ops docs, changelog, production read-only probes | R1-R8 merged and deployed exact main for READY | high | docs-ops + integrator | partly | incident gates and exact queue counts recorded without Codex publication |

Dependency order: `R1 -> R2 -> R3 -> R5`; provider list interfaces feed `R4`; `R6`
shares runtime schema with `R4` and is integrated in the same lane; `R7` is independent
until final queue/readback acceptance; `R8/O1` close only after integration.
