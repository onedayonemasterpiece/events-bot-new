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
