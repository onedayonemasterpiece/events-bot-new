# Smart Update: accepted rename, stale fact and bounded correction

Snapshot: 4 November 2026, 08:12 UTC / 10:12 Europe/Kaliningrad. Supplement to the [Opus audit](smart-update-opus-critical-review-20261103.md). This records a live mitigation and a tested review concern; it does not claim generic rename prevention or complete incident recovery.

## Actual failure and producer

Official source `https://vk.com/wall-194927034_5002` was accepted as MERGED into existing Event 9779 by operation `evt_op_oMI4xiGzlVrNd502ZMu49M9VoV7c6f-Q`. Its canonical title became `🎵 Концерт «Песни о любви»`; November 27, 18:00, Zelenogradsk, Railway Street 1 were preserved. Source 13915467 anchors that official evidence. The saved before-state title was exactly `Верни мне музыку`.

After the media reset, the actual Telegraph page had the new title but retained the old name in its body. Historical fact 194844, source 13552284, remained `added`:

> Событие представляет собой концерт под названием «Верни мне музыку».

The MCP text gate did not receive current `title`, while its `locked_facts` included that historical fact. An exact read-only reproduction returned `conflict / locked_facts`, surfaced as `PUBLIC_TEXT_FACT_CONFLICT`. The failed typed preparation left no prepared operation or Event/job writes. This is a factual conflict in the writer inputs, not exhausted provider quota.

Relevant normal producers/consumers:

- `smart_event_update.py`: historical `added/duplicate` source facts feed `facts_before_list` and the fact-first writer after an accepted merge.
- `private_events_mcp/event_commands.py`: `SQLiteEventCommandStore.prepare` loads active source facts; `canonical_text_gate` checks public prose against them, but its `keys` omit `title`.
- `models.py`: `EventSourceFact` already has `added`, `duplicate`, `conflict` and `note` statuses. No new table/state is necessary for this correction.

## Applied bounded mitigation

A read-only plan verified the exact old fact, current canonical title/date/time/city, accepted official operation and source anchor. One transaction then changed **only fact 194844** from `added` to the existing `conflict` status and appended provenance note 209511 under official source 13915467. The original fact text and source remain stored. All 33 existing fact rows were compared before/after; every other field and fact was preserved. This transaction made zero Event, outbox, provider or intake writes.

The existing typed description preparation was retried with the same stable key `INC-2026-11-04-pesni-description-provenance-9779` after this proven input correction. It passed the unchanged real LLM consistency gate and was auto-approved. Commit was sent once and read back accepted:

- Operation: `evt_op_wdKH5AMYswuguIAIsfg80Aye4_lax90C`.
- Canonical revision: `e82ba58c0d985328e4b8368900be96621410500d57811aece938c9205850dae3`.
- Scope: description only, new name and official concert/dinner details; no date, venue, lifecycle or identity change.
- Public surfaces: reconciliation queued, **not yet accepted as current public content**. Existing uneditable VK post remains failed; no replacement or manual requeue.

The Nov14 disputed-date case and decision 19099 were not changed or replayed. This correction of one disproved historical title fact does not authorize accepting ambiguous date conflicts or disabling locked facts.

## Review correction: the suggested note helper replaces a whole source batch

The morning audit suggests adding a single rename note through `_record_source_facts`. That helper first deletes **all** `EventSourceFact` rows for `(event_id, source_id)` and then writes the supplied batch. It is appropriate for complete source iteration replacement, not an append-only note call.

A real helper call in an isolated initialized SQLite database proves the risk:

```json
{
  "before": [["Цена: 3000 ₽", "added"], ["Время: 18:00", "added"]],
  "after_note_only_call": [["Название обновлено", "note"]],
  "production_writes": 0,
  "provider_calls": 0
}
```

The diagnostic transaction was rolled back. Calling it with only a note in the proposed helper would lose unrelated source facts. Keep the existing helper's replacement semantics; do not introduce an append interpretation without examining its existing callers. The bounded live mitigation above inserts one note directly and preserves the original source batch.

The proposed generic substring rule also needs a composite-fact negative control: a sentence can contain an old event title alongside a date, venue or price. Marking the whole sentence conflicting must not silently erase valid logistics. This is a review concern, not a claim that such a composite fact was found for 9779. The actual 9779 fact is title-only and was checked exactly.

## Remaining prevention and acceptance

The normal rename producer still needs a minimal, LLM-first contract that keeps historical fact provenance and current authoritative naming consistent for both public writer and edit gate. Adding `title` to the gate payload alone does not establish safe fact supersession. Do not ship broad lexical fact dropping, a repair worker, automatic historical replays or a new fact-history table as a shortcut.

Before accepting prevention, use the actual accepted-rename fixture and verify the normal writer inputs/body plus MCP consistency gate. Opposite controls must retain disputed-date refusal, protect composite logistics, preserve facts from the accepted current source and leave semantic-title-mismatch refusals unchanged. A plan or this one-event mitigation is not a passing full normal-pipeline replay.

## Retained receipts

Root: `/home/dev/artifacts/events-bot-new/20261102T233947Z-intake-recovery-20261103/`.

- `morning-receipts-0732.json`: actual historical facts and official source anchors.
- `pesni-text-gate-readonly-0726.py/.jsonl`: unchanged real gate and refusal.
- `reconcile-9779-title-fact.py`, `9779-title-fact-plan.json`, `9779-title-fact-applied.json`: exact guarded correction and fact preservation.
- `9779-description-accepted.json`: typed acceptance, distinct from publication.
- `record-facts-note-control.py/.json`: actual batch-replacement counterexample.

No credentials, actor endpoints or private preparation references are required to reproduce the reasoning above. This public document supplies the evidence the auditor could not read from the retained directory; private runtime permissions were not broadened.
