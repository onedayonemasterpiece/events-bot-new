# Partner EventsBot application boundary

Current runtime: MCP client → authenticated MCP adapter → shared application
commands → canonical EventsBot DB/Smart Update/JobOutbox/promo services.

`actors.ActorContext` is constructed by the trusted transport, never from model
arguments. `CommandPolicy` resolves current server-side grants inside canonical
transactions. `EventCommandService` and `PromoCommandService` accept this actor
and closed domain requests without `ToolCallContext`. Existing create uses
`EventCreateRequest` and `EventCreateRuntime`; authorization, Smart Update
receipts and accepted portfolio assignment remain server-owned.

A future optional flow is:

```text
partner web session
→ authenticated BFF/session
→ live/conversational model
→ allowlisted function-call executor
→ same EventsBot application command
→ prepare / commit / review / status
→ structured result
→ model explains result to user
```

This is planned compatibility, not an implemented web/live product. Google Live
API is one potential provider; no provider types enter domain commands. No BFF,
voice session, WebSocket, VAD/STT, model router or conversational UI is included.

The model provider is never the security principal. Its tool arguments cannot
choose tenant/organization, add scopes, skip review or invoke publication
adapters. The future executor constructs the same authenticated actor, applies
the same schemas and current policy, and calls the same services. Durable
operation state survives model/session/process restarts. Provider execution
stays in existing workers; changing the conversational provider does not change
event or promo semantics.
