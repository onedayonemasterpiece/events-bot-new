# Region Talk agent entrypoint

This directory is the canonical local entrypoint for any request about Region Talk / «О Калининграде говорят», its external-publication research, source onboarding, candidate copy, or publishing queue.

Before acting, read the relevant local contracts:

1. `README.md`
2. `source-onboarding-profile.md`
3. `source-profile-recovery-plan.md`
4. `external-publication-research.prompt.txt`
5. `external-publication-research.schema.json`
6. `publisher-profile-research.prompt.txt`
7. `publisher-profile-enrichment.schema.json`
8. `publication-queue.md`
9. `editorial-visual-product.md`

Rules:

- The local external research prompt must be consulted before every new external-publication research request; do not rely on a copied prompt from chat.
- The live registry and schema URLs declared by that prompt must still be fetched at execution time.
- Historical `region-talk-external-research-result-*.json` files are immutable.
- Publisher profile enrichment uses a separate schema/file mask and grants no publication permission.
- A federal brand does not prove that a specific article is external; check byline, section, bureau, and local edition.
- Social source onboarding must not be inferred from one current post.
- Preserve manual-review and operator gates; no autopublish.
