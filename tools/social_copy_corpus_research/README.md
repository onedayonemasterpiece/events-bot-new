# Social copy corpus research

One-off, bounded collection of public Telegram posts and VK community posts for the editorial-style-system study dated 2026-08-06.

## Safety and scope

- Target: up to 100 recent text-bearing posts per configured source.
- Telegram: public `t.me/s/<handle>` preview pages. Shared Telethon sessions are detected but never opened.
- VK: official API `v5.199` with an existing read token supplied through GitHub Secrets.
- Request behavior: stable user agent, bounded random pauses, exponential backoff, no proxy rotation, CAPTCHA bypass, fake reactions, typing events or read-receipt simulation.
- Full post text exists only in a one-day GitHub Actions artifact. Durable outputs contain provenance, post URLs/IDs, timestamps, hashes and derived features rather than a redistributable text dump.

## Run

```bash
python tools/social_copy_corpus_research/collect.py \
  --sources tools/social_copy_corpus_research/sources.json \
  --out social-copy-corpus-output \
  --target 100
```

The corresponding workflow is `.github/workflows/social-copy-corpus-research.yml`.
