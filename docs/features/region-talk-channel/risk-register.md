# Risk register — Region Talk Channel

| Risk | Impact | Mitigation |
|---|---|---|
| Legal/media reuse risk | Reposting or modifying source images can violate rights. | `rights_policy`, link-only fallback, no autopublish with unknown rights, source attribution. |
| VK image publishing token risk | Community token may post text but fail photo upload. | Validate API path in dry-run, consider limited user-token policy, fallback text/link or Telegram-only. |
| Telegram source reading risk | Bot API cannot read arbitrary public channel history. | Separate acquisition modes; use authorized/manual/web-preview modes only after policy review; respect session boundaries. |
| Full autonomy risk | Pipeline publishes bad/unsafe post. | Dry-run, XLSX favorites report, manual approval, strict verifier gates, canary. |
| Weak image risk | Good text but weak photo lowers channel quality. | Strong-media gate; image report; weak-media debug only. |
| News/trash contamination | Channel drifts into news/incident feed. | Disqualifier classes, source filters, verifier, manual review. |
| Source monoculture | Same few channels dominate. | Source novelty boost, source/day cooldown, 7-day caps, diversity bonus. |
| Duplicates | Same post/route/photo appears multiple times. | URL canonicalization, text hash, pHash, semantic duplicate detection. |
| Overclaiming in generated post | LLM exaggerates sentiment or facts. | Structured output, conservative prompt, source attribution, no invented facts. |
| Kaggle/YDB reliability | Partial run publishes without ledger update. | Transactional queue states, idempotency, locks, retry-safe states. |
| Secrets leakage | Tokens appear in notebook/logs/artifacts. | Kaggle secrets/encrypted datasets, redacted logs, no raw token print. |
| Model cost drift | VLM/LLM calls grow with all posts/images. | Cascade cheap filters → vector scoring → top candidates only → cached verifier. |
| Source-local creator mislabeled as an external visitor | Locally produced KO posts can reach Gemini and the operator chat even when each post is individually good. | Reconcile monotonic source counters from durable strict-KO candidates; treat 8+ KO rows over 42+ days as persistent local-author evidence unless authoritative external evidence exists; terminalize before image/publication handoff. |
| Broad query activity mistaken for useful discovery | Thousands of matches/RPCs can grow while no new current-year KO URL enters the funnel. | Report distinct new URLs and manually/LLM-verified KO yield per 100 RPC by query structure; use rolling 365-day truncation and exact visible-toponym guards. |
| Long-tail cursor starvation | A 200+ term bank exists but sources repeatedly receive only its first two terms. | Persist cursor, reserve two continuation sources per run after confirmed evidence is drained, retain fresh-source capacity, and expose query cursor/wave/RPC counters. |
