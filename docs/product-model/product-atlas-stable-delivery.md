# Product Atlas — Git и Penpot MCP delivery

> **Status:** Git SoT candidate materialized; Penpot materialization pending.  
> **Decision date:** 2026-08-25.  
> **Canonical model:** [`atlas/v1/README.md`](atlas/v1/README.md).

## Current delivery path

```text
reviewed versioned Git Product Atlas SoT
→ fail-closed source/relation validation
→ explicit scoped Penpot MCP task
→ exact target/file/page verification
→ bounded mutation
→ MCP read-back
→ versioned Git receipt
→ owner review
```

The historical Product Atlas plugin publication, manifest and installation URL are superseded. They remain available only in Git history and are not a supported installation or synchronization path.

## Git delivery evidence

A Product Atlas Git revision is reviewable only when it contains:

- exact product, corrected UI SoT and planning locks;
- complete product entity registries;
- route/archetype/region/state foreign-key linkage;
- explicit unresolved and `not_modeled` ledger;
- typed `binding_pending` for unpublished Penpot bindings;
- fail-closed validator and repository test;
- no fabricated Penpot identity.

A Draft PR is evidence of a candidate revision, not publication in `main`, owner acceptance, deployment or user/owner outcome.

## Penpot entry gate

Penpot MCP materialization is blocked until:

1. the exact Git Product Atlas revision is accepted for the requested scope;
2. the target Product Atlas file and page are resolved by MCP read, not assumed from a link;
3. linked UI identities are stable enough for the scope;
4. a bounded dry-run plan identifies all changed and preserved objects;
5. comment preservation and rollback/read-back procedure are defined;
6. no unresolved entity is silently promoted to accepted;
7. every absent native binding remains `binding_pending`.

## MCP acceptance evidence

A completed materialization receipt must record:

```yaml
receipt_schema_version: product-atlas-mcp-receipt.v1
product_model_sha: <Git commit>
product_source_lock_sha256: <hash>
ui_sot_sha: <Git commit>
ui_manifest_sha256: <hash>
operation_scope:
  file_id: <read back from MCP>
  page_ids: []
  entity_ids: []
operation:
  mode: create | reconcile | patch
  dry_run_hash: <hash>
read_back:
  completed: true
  object_ids: []
  relation_count: 0
  unresolved_binding_count: 0
comments:
  preserved: true
  unresolved_thread_ids: []
review:
  status: candidate | accepted | rejected
  reviewer: <owner or delegated reviewer>
```

No file/page/object ID is written before real MCP read-back.

## Prohibited delivery modes

- Product Atlas plugin or plugin manifest;
- automatic/background Penpot synchronization;
- live production DB or raw analytics connection;
- direct ingestion of raw action-map summaries;
- mutation on plugin/MCP open;
- guessed file/page/board/shape UUIDs;
- treating a successful write without read-back as completed delivery.

## Current state

- Product Atlas Git SoT v1: candidate;
- corrected UI SoT lock: resolved;
- product-to-UI semantic linkage: materialized in Git;
- Penpot mutations in this task: `0`;
- native Product Atlas Penpot bindings: `binding_pending`;
- supported next delivery mechanism: explicit Penpot MCP task after entry gate.