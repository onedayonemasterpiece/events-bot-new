# Region Talk cliché-ban integration report

The change was implemented serially on top of current `origin/main`; no second
Region Talk pipeline or product fork was introduced. Existing Strategy →
Grounded Writer → Critic ownership remains intact. Deterministic code only
rejects the style pattern; semantic rewriting remains LLM-first.

Core code/doc changes were merged through PR #184 and deployed as Fly v1855.
The production-discovered exact-force cooldown defect was fixed through PR
#185. Deployment of that operational follow-up is intentionally deferred until
the active Region Talk catch-up exits, per the session-interruption regression
contract. The core future-copy guard is already live.

Production backfill attempted every confirmed candidate without touching
target-published identities. Archi.ru now has v9 copy and a new operator review
message. The remaining retry/review/media tail is explicit and fail-closed.
