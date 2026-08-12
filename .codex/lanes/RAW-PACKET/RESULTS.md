# Lane RAW-PACKET Results

## Status
committed

## Requirement IDs
- R1: shared pure sanitized recursive VK source envelope
- R2: complete ordered attachment/media inventory with bounded OCR selection
- R3: immutable durable envelope schema, completeness fields, and semantic revision hashes
- R4: edit-sensitive/counter-insensitive revision idempotency
- R5: shared crawl, continuation, fresh-fetch, personal-wall, and legacy derivation
- R6: deleted/access-denied complete-packet replay and incomplete/missing retry behavior
- R7: honest census/recovery replayability classification and normal-processor-only plan
- R8: secret denylist, access-key preservation boundary, security/parity/regression validation
- A-G: outer/nested text and attachment evidence, preview ingestion, fallback, revision matrix, parity, one-fetch, and visual-only EvidenceManifest counts

## Branch
`agent/smart-update-final-code/raw-packet`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/smart-update-raw-packet`

## Base SHA
`09be094fa815a09346552208d99ea1525de748b8`

## Head SHA
Implementation commit: `4db9aa918cc40eff9ed86672f0a476175b0fa1b0`

## Files changed
- `vk_source_envelope.py`
- `main.py`
- `main_part2.py`
- `vk_intake.py`
- `vk_auto_queue.py`
- `db.py`
- `models.py`
- `scripts/ops/smart_update_loss_census.py`
- `scripts/ops/recover_smart_update_identity_losses.py`
- `tests/test_vk_source_envelope.py`
- `tests/test_vk_raw_first_llm_contract.py`
- `tests/test_vk_crawl_continuation.py`
- `tests/test_vk_auto_queue_import.py`
- `tests/test_vkrev_fetch_photos.py`
- `tests/test_smart_update_loss_census.py`
- `tests/test_recover_smart_update_identity_losses.py`

## Commands run
- `python3 -m py_compile` on every changed production/script Python module
- Owned focused pytest suite: envelope, raw-first, continuation, auto queue, legacy media facade, census, and recovery
- Adjacent pytest suite: VK history/future/default-time/shortpost/miss/review/rate-limit and source-parse/caller typed contracts
- `git diff --check`

## Tests / verification
- Focused owned suite: `143 passed`
- Adjacent VK legacy/review suite: `92 passed, 1 skipped` (pre-existing unmarked async test warning)
- Source parse/verifier/caller suite: `71 passed`
- Combined final command: `271 passed, 1 skipped` in 42.01s
- Final targeted post-review command: `14 passed`
- `py_compile`: passed
- `git diff --check`: passed

## Risks
- Historical packets remain explicitly `replayable_legacy_incomplete`; deleted legacy packets are retried rather than promoted to terminal semantic conclusions.
- The envelope retains sanitized raw provider attachment structures and therefore increases packet size; OCR/media selection remains bounded and records omissions without deleting inventory.
- Attachment `access_key` is retained only in protected packet/inventory data for media replay and is excluded from semantic revision projection and application logs/receipts.
- Recovery remains a queue/state transition only. It does not call a model or insert Events.

## Merge notes
- Cherry-pick implementation commit `4db9aa918cc40eff9ed86672f0a476175b0fa1b0` and the following RESULTS-only commit.
- No files in the forbidden source-parse-contract, Smart Update implementation, documentation, or CHANGELOG scopes were changed.
- User-wall fresh fetch uses positive signed owner IDs and canonical `wall<user_id>_<post_id>` URLs; group behavior remains backward-compatible.
