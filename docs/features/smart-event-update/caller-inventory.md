# Smart Update production caller inventory

AST import/alias/callback discovery was frozen on then-latest `origin/main`
`d7731ab4235b325e9ca52d13c45fba83eaf5de0b` before the automatic terminal
state-machine change. After integration rebased onto `origin/main`
`96784bd572c03b965f303366c4ff0bb85d1b9a3f`, the inventory and AST acceptance
contract were rerun; intervening main commits added no Smart Update caller.
Seven call expressions form six direct production boundaries.

## Direct boundaries and indirect entrypoints

| boundary | direct call | indirect entrypoints |
| --- | --- | --- |
| official source parsers | `source_parsing/handlers.py::_smart_event_update_with_lock_retry` invokes the injected facade from `add_new_event_via_queue` | parse commands, scheduled/changed/resume parsing, Kaggle recovery, and Dom Iskusstv/Philharmonia/Pyramida/QTickets wrappers |
| Telegram Monitoring | `source_parsing/telegram/handlers.py::process_telegram_results` poster bridge and primary import | server import/reimport, scheduled monitor, commands, on-demand, festival TG intake, and incomplete-scan recovery |
| VK | `vk_intake.py::persist_event_and_pages` | VK auto scheduler, manual VK intake, legacy wrapper, and static-collection replay adapter |
| ticket sites | `ticket_sites_queue.py::_smart_update_from_theatre_event` | Pyramida, Dom Iskusstv, and QTickets scheduled/manual queue |
| festival VK | `festival_queue.py::_process_vk_item` | festival queue scheduler/manual command |
| manual/forwarded bot intake | `main.py::add_events_from_text` and `main.py::handle_add_event_raw` | add-event worker and forwarded-message path |

Non-production direct callers are the Smart Update benchmark and upstream VK
smoke scripts. Baseline tests contained 61 direct calls across 16 files.

## Pre-fix terminal leaks

The audit found these public-boundary violations:

- source-binding wrapper/race, ambiguous replay, create binding, and merge
  binding returned `review_required`, sometimes with the conflicting owner in
  `event_id`;
- create veto/gate exception, the final duplicate probe, and merge gate returned
  `skipped_identity_gate`;
- an upsert carrying `source_role=context_only` returned
  `skipped_context_only` instead of using explicit intent;
- a vanished merge Event returned generic `error`;
- no-change strings represented more than exact accepted replay;
- parser and VK adapters collapsed identity outcomes into generic
  `failed`/`skipped`/`rejected` queue states.

Diagnostic ID truthiness could increment festival creation counts, drive main
result rendering/edit controls, gate Telegram notices, and populate a ticket
queue Event ID. Exact no-op was not a ticket-queue success. Telegram primary
could finalize an identity retry as scanned/skipped, while poster-bridge
completion did not require an accepted result.

## Migration gate

Every direct boundary must consume `SmartUpdateTerminalOutcome` and supply a
stable producer child ordinal or occurrence key when available. Only
`SmartUpdateResult.is_accepted` permits Event/publication side effects. A
diagnostic ID is never copied downstream. `ATTACH_CONTEXT` is the only
provenance-only operation.

The AST regression test must fail when production callers:

- branch on `SmartUpdateResult.status` or compare Smart Update status strings;
- use bare `result.event_id` truthiness as acceptance;
- synthesize review/generic-failed terminals after a typed outcome;
- call the core from an unlisted production boundary.

PR #338 is intentionally excluded: it keeps forbidden terminals and introduces
a human-review queue/UI into a product without that operator workflow.

## Post-migration disposition

The six production boundaries now use the typed facade. Parser, Telegram, VK,
ticket, festival, and manual/forwarded adapters supply stable producer ordinals
where their envelope can contain siblings. `CREATED`, `MERGED`, and exact
`NOOP_EXACT_REPLAY` are accepted; only those results can schedule projections,
emit notices, increment accepted counters, or store an imported Event pointer.
`RETRY_SCHEDULED` is left to the default-on durable worker, and
`REJECTED_PRODUCT_POLICY` retains its explicit permanent reason.

Telegram exact replay re-arms the same idempotent completion work as an
accepted no-change import. VK roundup children are resolved independently: an
unresolved child defers the carrier without erasing accepted sibling mappings,
and a later keyed replay cannot re-create those siblings. Ticket exact replay
is a successful queue completion.
