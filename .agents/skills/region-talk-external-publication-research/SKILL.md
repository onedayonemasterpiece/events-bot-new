---
name: region-talk-external-publication-research
description: Use for every Region Talk request involving external publications, journals, publisher/source onboarding, candidate copy, or source-profile enrichment.
---

# Region Talk external publication research

## Mandatory local read

Before researching or changing this pipeline, open:

- `docs/features/region-talk-channel/AGENTS.md`
- `docs/features/region-talk-channel/external-publication-research.prompt.txt`
- `docs/features/region-talk-channel/external-publication-research.schema.json`
- `docs/features/region-talk-channel/publisher-profile-research.prompt.txt`
- `docs/features/region-talk-channel/publisher-profile-enrichment.schema.json`
- `docs/features/region-talk-channel/source-profile-recovery-plan.md`

Do not ask the user to reattach the prompt when these files exist in the repository.

## Route

- New external article discovery → execute the canonical external-publication prompt and live registry contract.
- Known publisher/source enrichment → use the publisher-profile prompt and separate sidecar schema.
- Social Telegram/VK onboarding → use the bounded source capture contract in the recovery plan.
- Copy review/regeneration → verify that current material evidence and reusable source profile evidence are both present.

## Safety

- Historical research-result JSON is immutable.
- New publisher sidecars must not match the candidate auto-import glob.
- Local edition/byline check is mandatory.
- No profile result grants candidate approval or autopublish.
- Live YDB updates require the dedicated guarded importer and strong re-read.
