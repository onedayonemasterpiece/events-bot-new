---
name: region-talk-vector-selection
description: Use in events-bot-new for Region Talk / Kaliningrad-best-post-monitoring text candidate quality, semantic vector gates, dual-model embedding enrichment with intfloat/multilingual-e5-base and BAAI/bge-m3, false-positive audits for non-Kaliningrad posts, news, ads, afisha/event announcements, local паблики priority, and LLM-late final verification policy.
---

# Region Talk Vector Selection

Use this skill when auditing or changing Region Talk candidate selection. The product rule is: **mass filtering is vector-first; LLM is only a thin final verifier/tie-breaker**.

## Non-negotiables

- Treat regex/keyword/place lexicon as evidence, recall, safety fallback, or observability only. Do not let broad regex rules own product semantics.
- Use dual-model enrichment, not model comparison: run/plan both `intfloat/multilingual-e5-base` and `BAAI/bge-m3`, union/fuse evidence, and keep per-model fields.
- Positive vector classes must require the post itself to be about Kaliningrad Oblast, not just contain a place token.
- Negative vector classes must catch: other-region primary topic, multi-region roundup, news/report, afisha/event announcement, ad/promo, local institution PR, and low-substance chat/test/internal bot output.
- LLM calls are allowed only for compact final candidates via Supabase-controlled limits; never use broad LLM to compensate for weak vector filtering.

## Workflow

1. Inspect latest XLSX sheets: `04a_final_shortlist`, `04_review_queue`, `06a_candidate_memory`, `08_dropped_posts`, `14d_llm_usage_by_stage`.
2. For false positives, classify by vector category, not keyword reason:
   - `other_region_travel` / `ambiguous_place_homonym_not_ko`
   - `multi_region_roundup`
   - `news_report` / `event_announcement`
   - `ad_or_promo` / `official_tourism_promo`
   - `local_institution_pr_event_report`
   - `low_substance`
3. Patch `kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py` around `semantic_bank_v1`, `dual_model_semantic_scores`, `text_vector_gate`, and candidate-memory shortlist filtering.
4. Keep `REGION_TALK_ENABLE_EARLY_LLM=false`; verify `14d_llm_usage_by_stage` still shows broad/review LLM disabled.
5. Add tests with representative false positives from the XLSX.

## Reference

Read `references/vector-selection-contract.md` before making substantive scoring changes.
