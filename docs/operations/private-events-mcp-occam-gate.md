# EventsBot MCP implementation safety gate

The mandatory pre-implementation Occam audit for event operations is:

`docs/reports/private-events-mcp-occam-preimplementation-audit-20260902.md`

No event-operation implementation may start from the full revision-3 scope as a
single change. The first allowed increments are:

1. read-only publication queue observability without schema or worker changes;
2. owner-only event creation through the existing Smart Update and standard
   `JobOutbox`, default-off and `smart_rewrite` only.

Lifecycle mutations, automatic notices, derived status images, promo and partner
OAuth are later independently gated changes. The canonical product contract
remains `docs/operations/private-events-mcp-event-operations-to-be.md`.
