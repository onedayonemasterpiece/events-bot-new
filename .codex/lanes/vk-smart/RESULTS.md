# VK / Smart Update linear terminal lane

## Outcome

Implemented the 2026-08-15 owner decision that supersedes the automatic-retry part of `INC-2026-08-10`: one Smart Update call now ends as accepted, typed product rejection, or visible `FAILED_TECHNICAL`. New product calls never enqueue background Smart Update retry work; the scheduler switch is default-off and retained only for an explicit legacy drain.

### Runtime fixes

- location grounding schema is closed to `keep | repair | reject_missing_location`;
- grounded `keep` no longer falls through only because numeric confidence is below `0.9`;
- semantic identity unknown takes one inline distinct-create resolution;
- remaining complete-evidence semantic vetoes are converted to closed product terminals; technical results become `FAILED_TECHNICAL`;
- finished technical attempts have `retry_exhausted=1`, no `next_retry_at`, a completion timestamp and no claim;
- expired legacy claims are selectable even when `retry_attempts == max_attempts`; abandoned open attempts close visibly as interrupted technical failures;
- legacy rehydration failure is terminalized instead of remaining claimed/due;
- `vk_auto_queue` persists `failed_technical`, reports the typed reason and explicitly states that no automatic retry was scheduled, including partial multi-child carriers;
- successful blank OCR counts as processed evidence even though no OCR text enters the prompt;
- exact parse replay stores `parse_key=NULL`, so repeated replay cannot violate the unique successful-parse index.

## SQLite migration

`Database.init()` transactionally rebuilds `smart_update_candidate_state` and `smart_update_attempt` only when their old CHECK constraints do not contain `FAILED_TECHNICAL`. IDs/payloads/attempts are copied verbatim with foreign keys temporarily disabled; enforcement is restored afterwards. Direct old-schema migration×2 and normal `Database.init()`×2 are covered, including `PRAGMA foreign_key_check`.

## Focused validation

- `378 passed, 9 warnings`:
  `tests/test_smart_update*.py tests/test_vk_auto_queue_import.py tests/test_vk_raw_first_llm_contract.py tests/test_source_parse_contract.py`
- `5 passed`: `tests/test_smart_update_linear_terminal_contract.py` (including scheduler-default-off and init×2).
- `1 passed`: the focused VK failed-technical caller regression after its final reporting change.
- `python3 -m py_compile` passed for changed runtime modules.
- `git diff --check` passed.

Warnings are the pre-existing Python 3.12 sqlite datetime-adapter deprecation in parser-occurrence tests.

## Raw regressions

- `wall-32547811_11187`: exact date/time, “в Чеховке” and `Московский проспект, 39`; an LLM `keep` at confidence `0.62` is accepted because the quote/address are grounded.
- Negative Day-city/festival-context carrier returns `reject_missing_location`, not a guessed venue and not a retry.
- An unchanged semantic fingerprint invoked twice leaves two closed rejection attempt receipts and zero `RETRY_SCHEDULED` attempts.
- A blank-success OCR image has `attachment_count=ocr_blocks_available=ocr_blocks_included=1`, zero unavailable attachments and complete evidence.
- Two exact receipt replays create two `exact_replay` rows with null parse keys and no provider parse.

## Approved consultant blocker

Both allowed Opus paths were attempted and no lower-class model was substituted:

- `a-opus` / `Claude Opus 4.6 (Thinking)`: interactive OAuth required, authentication timed out;
- `claude --agent Opus`: `Not logged in · Please run /login`.

Redacted evidence is in ignored artifacts:
`artifacts/codex/INC-2026-08-15-vk-smart/{opus-prompt.txt,opus-response.txt,CONSULTANT_STATUS.md}`.

## Exact production legacy catch-up selector

Run only after the exact-main deploy/migration and with the periodic worker disabled. Capture this immutable id set first:

```sql
SELECT id,candidate_key,source_type,canonical_source_url,reason,retry_attempts,max_attempts
FROM smart_update_candidate_state
WHERE current_outcome='RETRY_SCHEDULED'
ORDER BY id;
```

On the stopped/replaced old runtime, make only that captured set due and release expired claims:

```sql
UPDATE smart_update_candidate_state
SET next_retry_at=CURRENT_TIMESTAMP,
    claimed_by=NULL,
    claim_expires_at=NULL,
    updated_at=CURRENT_TIMESTAMP
WHERE id IN (<captured comma-separated ids>)
  AND current_outcome='RETRY_SCHEDULED';
```

Invoke `retry_due_smart_update_candidates(db, limit=25)` manually in bounded batches until `claimed=0`. Because the new facade cannot return a newly scheduled retry, every selected row finishes in that invocation as accepted, product-rejected or `FAILED_TECHNICAL`. If the process dies mid-batch, wait for/clear the expired lease and re-run only remaining ids; exact replay protects already committed domain writes.

Closure SQL:

```sql
SELECT COUNT(*) AS legacy_retry_left
FROM smart_update_candidate_state
WHERE current_outcome='RETRY_SCHEDULED';

SELECT COUNT(*) AS open_attempts
FROM smart_update_attempt
WHERE finished_at IS NULL;

SELECT current_outcome,COUNT(*)
FROM smart_update_candidate_state
WHERE id IN (<captured comma-separated ids>)
GROUP BY current_outcome;
```

Required gates: both first counts are zero; every captured id is accounted for; accepted ids have Event/EventSource linkage; each `FAILED_TECHNICAL` reason is present in ops/operator reporting and is not silently rearmed.

## Integration risks / follow-up

- TG/parser callers synthesize their own retry states; that lane must map Smart Update `FAILED_TECHNICAL` to its visible terminal and alert rather than enqueueing again.
- `RETRY_SCHEDULED` remains in the SQLite CHECK/enum only for provisional attempt registration and the one-time legacy drain. Removing it entirely requires a separate crash-safe `PROCESSING` schema transition.
- Accepted domain write followed by failed state acknowledgement remains an unavoidable two-authority crash window; exact input replay/legacy drain is the recovery rail, not a polling product queue.
