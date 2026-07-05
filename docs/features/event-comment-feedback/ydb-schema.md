# YDB schema draft — event-comment-feedback

Status: draft for follow-up implementation. Persistent storage for this feature is a **YDB sidecar**. Do not add SQLite tables for comments, embeddings, phrase matches, verifier cache or public feedback.

YDB sidecar is not the canonical event DB. Canonical event rows, source rows, ticket status, publication state and static build scheduling stay in the existing core contour unless a separate migration is approved.

## Retention policy

- Raw/normalized comments: short retention, default proposal 60 days, configurable by source/risk.
- Embeddings: same retention as normalized comment unless needed for a still-active event; delete/recompute on model/doc-version changes.
- Phrase matches/groups/run audit: keep 90–180 days for QA/regression.
- Public feedback current state: keep while event is active plus short stale window.
- Verifier cache: TTL by policy version and evidence/event fingerprints, suggested 30–90 days.

YDB TTL deletion is asynchronous. Every query that reads expirable state must also filter `retention_expires_at`, `expires_at` or `status/stale` explicitly instead of assuming the row disappeared at the TTL moment.

## Table naming and key conventions

- `event_id` and `event_source_id` may start as `Utf8` to tolerate exported snapshots; if implementation commits to core SQLite integer ids, use `Uint64` and keep JSON export numeric.
- `platform` values: `telegram | vk`.
- `platform_post_key` examples: `vk:{owner_id}:{post_id}`, `tg:{channel_id}:{message_id}`.
- `comment_key` examples: `vk:{owner_id}:{post_id}:{comment_id}`, `tg:{discussion_chat_id}:{thread_top_id}:{message_id}`.
- Store vectors as `String`/`Bytes` blob or JSON only as an MVP cache format. YDB is not required to execute ANN search.

## 1. `event_comment_source_state`

Purpose: incremental crawl state for an event source.

| Field | Type | Notes |
|---|---|---|
| `event_id` | `Utf8`/`Uint64` | Core event id from snapshot. |
| `event_source_id` | `Utf8`/`Uint64` | `event_source.id` where available. |
| `platform` | `Utf8` | `telegram` or `vk`. |
| `source_url` | `Utf8` | Public source URL if available. |
| `platform_post_key` | `Utf8` | Stable post key. |
| `source_fingerprint` | `Utf8` | Hash of source identity + event relation. |
| `last_seen_comment_key` | `Utf8?` | Incremental cursor. |
| `last_seen_comment_created_at` | `Timestamp?` | Cursor aid. |
| `last_known_comment_count` | `Uint32?` | Precheck signal only. |
| `last_fetch_at` | `Timestamp?` | Any fetch attempt. |
| `last_successful_fetch_at` | `Timestamp?` | Successful fetch. |
| `next_fetch_after` | `Timestamp?` | Throttle. |
| `fetch_status` | `Utf8` | `ok|skipped|error|unavailable|forbidden`. |
| `fetch_error_code` | `Utf8?` | Redacted. |
| `fetch_error_message` | `Utf8?` | Redacted/truncated. |
| `comments_capability` | `Utf8` | `available|no_comments|not_supported|forbidden|unknown`. |
| `updated_at` | `Timestamp` | Last state mutation. |

Primary key: `(platform, platform_post_key)`.

Indexes/query needs:

- `idx_event_comment_source_state_event_id(event_id)`;
- `idx_event_comment_source_state_next_fetch(next_fetch_after)`;
- `idx_event_comment_source_state_fetch_status(fetch_status)`.

## 2. `event_source_comment`

Purpose: normalized comment corpus.

| Field | Type | Notes |
|---|---|---|
| `comment_key` | `Utf8` | Primary key. |
| `event_id` | `Utf8`/`Uint64` | Event relation snapshot. |
| `event_source_id` | `Utf8`/`Uint64` | Source relation snapshot. |
| `platform` | `Utf8` | `telegram|vk`. |
| `platform_post_key` | `Utf8` | Parent source post. |
| `platform_comment_id` | `Utf8` | Platform-local id. |
| `parent_comment_key` | `Utf8?` | Thread/reply relation. |
| `author_hash` | `Utf8?` | Non-reversible hash; no raw user id. |
| `text` | `Utf8` | Raw-ish text after allowed redaction, backend only. |
| `text_normalized` | `Utf8` | Matching input. |
| `text_hash` | `Utf8` | Dedup/re-embed key. |
| `language` | `Utf8?` | Optional. |
| `created_at` | `Timestamp?` | Platform timestamp. |
| `fetched_at` | `Timestamp` | Fetch time. |
| `edited_at` | `Timestamp?` | If known. |
| `is_deleted` | `Bool` | Tombstone. |
| `is_empty` | `Bool` | Empty after normalization. |
| `has_links` | `Bool` | Guardrail. |
| `raw_payload_ref` | `Utf8?` | Optional private artifact/object ref. |
| `raw_payload_json` | `Json?`/`Utf8?` | Avoid unless needed; never exported public. |
| `retention_expires_at` | `Timestamp?` | TTL/filter field. |

Primary key: `(comment_key)`.

Indexes/query needs: `event_id`, `platform_post_key`, `text_hash`, `retention_expires_at`.

`author_hash` must not be reversible without a separate secret. Names, avatars and direct user ids do not enter static export.

## 3. `event_comment_embedding`

Purpose: embedding cache for normalized comments.

| Field | Type | Notes |
|---|---|---|
| `comment_key` | `Utf8` | FK-like reference. |
| `embedding_model` | `Utf8` | Exact provider/model id. |
| `embedding_dim` | `Uint32` | Dimension. |
| `embedding_document_version` | `Utf8` | Input formatting version. |
| `text_hash` | `Utf8` | Recompute guard. |
| `embedding_vector_blob` | `String`/`Bytes?` | Preferred compact binary. |
| `embedding_vector_json` | `Json?`/`Utf8?` | Debug/portable alternative. |
| `created_at` | `Timestamp` | Cache creation. |
| `retention_expires_at` | `Timestamp?` | Optional TTL. |

Primary key: `(comment_key, embedding_model, embedding_document_version)`.

Important: YDB stores vectors/cache; MVP matching loads phrase prototypes/comments into Kaggle/Python memory and uses numpy/faiss/sklearn.

## 4. `event_feedback_phrase`

Purpose: versioned public phrase bank.

| Field | Type | Notes |
|---|---|---|
| `phrase_id` | `Utf8` | Stable id. |
| `phrase_bank_version` | `Utf8` | e.g. `event-comment-feedback-phrase-bank-v1`. |
| `category` | `Utf8` | Product group. |
| `signal_type` | `Utf8` | Machine-friendly signal. |
| `tone` | `Utf8` | `positive|neutral|concern`. |
| `icon` | `Utf8` | `smile_green|neutral_gray|sad_red`. |
| `risk_class` | `Utf8` | `low|medium|high`. |
| `public_sentence` | `Utf8?` | Null for internal-only classes. |
| `display_priority` | `Uint32` | UI ordering. |
| `is_active` | `Bool` | Soft disable. |
| `min_evidence_count` | `Uint32` | Per-phrase threshold. |
| `min_unique_authors` | `Uint32` | Per-phrase threshold. |
| `min_sources` | `Uint32?` | Optional source diversity. |
| `vector_only_allowed` | `Bool` | Publication path. |
| `requires_llm_verification` | `Bool` | Publication path. |
| `created_at` | `Timestamp` | Version row creation. |
| `updated_at` | `Timestamp` | Latest metadata change. |

Primary key: `(phrase_bank_version, phrase_id)`.

## 5. `event_feedback_phrase_prototype`

Purpose: prototypes and hard negatives for vector matching.

| Field | Type | Notes |
|---|---|---|
| `phrase_bank_version` | `Utf8` | Phrase bank version. |
| `phrase_id` | `Utf8` | Phrase id. |
| `prototype_id` | `Utf8` | Stable row id. |
| `prototype_kind` | `Utf8` | `positive|hard_negative|downgrade_hint|spam_negative`. |
| `prototype_text` | `Utf8` | Russian prototype text. |
| `weight` | `Double` | Optional. |
| `embedding_model` | `Utf8` | Prototype embedding model. |
| `embedding_dim` | `Uint32` | Dimension. |
| `embedding_vector_blob` | `String`/`Bytes?` | Compact vector cache. |
| `embedding_vector_json` | `Json?`/`Utf8?` | Portable/debug alternative. |
| `created_at` | `Timestamp` | Created. |

Primary key: `(phrase_bank_version, phrase_id, prototype_id)`.

## 6. `event_comment_phrase_match`

Purpose: per-comment phrase candidates for a run.

| Field | Type | Notes |
|---|---|---|
| `run_id` | `Utf8` | Pipeline run id. |
| `event_id` | `Utf8`/`Uint64` | Event. |
| `comment_key` | `Utf8` | Comment. |
| `phrase_bank_version` | `Utf8` | Phrase bank. |
| `phrase_id` | `Utf8` | Candidate phrase. |
| `positive_score` | `Double` | Cosine/normalized. |
| `negative_score` | `Double` | Hard-negative max. |
| `next_best_phrase_id` | `Utf8?` | Confusion signal. |
| `next_best_score` | `Double?` | Confusion signal. |
| `margin` | `Double` | Acceptance margin. |
| `rank` | `Uint32` | Top-K rank. |
| `match_status` | `Utf8` | `candidate|accepted_vector|rejected_vector|rejected_spam|rejected_duplicate`. |
| `created_at` | `Timestamp` | Created. |

Primary key: `(run_id, event_id, comment_key, phrase_id)`.

## 7. `event_comment_phrase_group`

Purpose: aggregated event/phrase evidence.

| Field | Type | Notes |
|---|---|---|
| `run_id` | `Utf8` | Pipeline run. |
| `event_id` | `Utf8`/`Uint64` | Event. |
| `phrase_bank_version` | `Utf8` | Phrase bank. |
| `phrase_id` | `Utf8` | Phrase. |
| `evidence_count` | `Uint32` | Accepted comments. |
| `weighted_evidence_count` | `Double` | Author/source capped. |
| `unique_authors_count` | `Uint32` | Distinct `author_hash`. |
| `sources_count` | `Uint32` | Distinct source posts. |
| `avg_positive_score` | `Double` | Average evidence score. |
| `avg_margin` | `Double` | Average margin. |
| `max_negative_score` | `Double` | Max hard-negative. |
| `risk_class` | `Utf8` | Copied from phrase. |
| `vector_group_score` | `Double` | Combined confidence. |
| `candidate_status` | `Utf8` | `suppressed_weak|vector_publishable|needs_llm|needs_manual_review|rejected`. |
| `representative_comment_keys_json` | `Json`/`Utf8` | Internal evidence keys only. |
| `risk_flags_json` | `Json`/`Utf8` | E.g. sarcasm, low diversity. |
| `conflict_flags_json` | `Json`/`Utf8` | E.g. ticket conflict. |
| `created_at` | `Timestamp` | Created. |

Primary key: `(run_id, event_id, phrase_id)`.

## 8. `event_comment_feedback_verification_cache`

Purpose: cache LLM/group verifier decisions.

| Field | Type | Notes |
|---|---|---|
| `cache_key` | `Utf8` | Hash key. |
| `event_id` | `Utf8`/`Uint64` | Event. |
| `phrase_bank_version` | `Utf8` | Phrase bank. |
| `phrase_id` | `Utf8` | Candidate phrase. |
| `event_facts_fingerprint` | `Utf8` | Title/date/venue/ticket/etc hash. |
| `evidence_fingerprint` | `Utf8` | Evidence keys/snippets/stats hash. |
| `verifier_policy_version` | `Utf8` | Prompt/schema policy. |
| `model_id` | `Utf8` | Verifier model. |
| `decision` | `Utf8` | `approve|reject|downgrade|needs_review`. |
| `approved_phrase_id` | `Utf8?` | For downgrade. |
| `risk` | `Utf8` | Result risk. |
| `raw_response_json` | `Json`/`Utf8?` | Internal audit only. |
| `created_at` | `Timestamp` | Created. |
| `expires_at` | `Timestamp` | TTL/filter. |

Primary key: `(cache_key)`.

## 9. `event_comment_feedback_current`

Purpose: current public state per event.

| Field | Type | Notes |
|---|---|---|
| `event_id` | `Utf8`/`Uint64` | Primary key. |
| `schema_version` | `Utf8` | `event-comment-feedback-v1`. |
| `generated_at` | `Timestamp` | Export generation. |
| `phrase_bank_version` | `Utf8` | Phrase bank. |
| `build_policy_version` | `Utf8` | Publication policy. |
| `comments_seen_count` | `Uint32` | Raw comments seen. |
| `comments_used_count` | `Uint32` | Evidence comments used. |
| `sources_count` | `Uint32` | Sources used. |
| `items_json` | `Json`/`Utf8` | Public allowlisted items. |
| `status` | `Utf8` | `published|suppressed|stale|error`. |
| `suppression_reason` | `Utf8?` | Compact reason. |
| `updated_at` | `Timestamp` | Latest update. |

Primary key: `(event_id)`.

## 10. `event_comment_feedback_run`

Purpose: pipeline audit.

| Field | Type | Notes |
|---|---|---|
| `run_id` | `Utf8` | Primary key. |
| `started_at` | `Timestamp` | Start. |
| `finished_at` | `Timestamp?` | Finish. |
| `status` | `Utf8` | `running|success|partial|failed|degraded`. |
| `input_event_count` | `Uint32` | Selected events. |
| `input_source_count` | `Uint32` | Selected sources. |
| `fetched_comment_count` | `Uint32` | Total fetched. |
| `new_comment_count` | `Uint32` | New/changed. |
| `embedded_comment_count` | `Uint32` | Embedded in run. |
| `vector_match_count` | `Uint32` | Candidate matches. |
| `phrase_group_count` | `Uint32` | Groups produced. |
| `llm_call_count` | `Uint32` | Provider calls. |
| `llm_cache_hit_count` | `Uint32` | Cache hits. |
| `exported_event_count` | `Uint32` | Public states exported. |
| `error_json` | `Json`/`Utf8?` | Redacted errors. |
| `artifact_refs_json` | `Json`/`Utf8?` | Probe/export artifacts. |

Primary key: `(run_id)`.

## Query/index checklist

- Due fetch sources by `next_fetch_after` and `fetch_status`.
- Load comments by `event_id` and `platform_post_key`.
- Load embeddings by `comment_key + model + doc_version`.
- Load groups by `run_id/event_id`.
- Load current public state by `event_id` for export.
- Search verifier cache by `cache_key` only.

## Export allowlist

Only `event_comment_feedback_current.items_json` after validation may feed static JSON. Never export rows from raw comment, embedding, match, group or verifier tables directly.
