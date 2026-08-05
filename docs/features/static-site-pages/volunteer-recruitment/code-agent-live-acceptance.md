# Code-agent task: provision the one unavailable Kaggle credential

Repository: `onedayonemasterpiece/events-bot-new`.

The Volunteer Monitor code, direct live canary, selector debugging, run analysis
and documentation are handled in PR #335. **Do not modify code, workflows,
branches, PRs or documentation.**

Perform only these GitHub settings operations:

1. Create GitHub Environment `volunteer-monitor-canary` if it does not exist.
2. Add environment secret `KAGGLE_API_TOKEN` from the Kaggle token securely
   available in your execution environment.
3. Set repository variable:
   `VOLUNTEER_KAGGLE_KERNEL_SLUG=eventsbot/kenigevents-volunteer-monitor`.
4. Set repository variable:
   `VOLUNTEER_KAGGLE_CANARY_ENABLED=true`.
5. Verify only that the secret name and variables exist. Never print, echo,
   return or commit the secret value; never create or commit `kaggle.json`.

If the Kaggle token is not securely available, stop and report only:
`KAGGLE_API_TOKEN is required`. Do not request it in chat and do not substitute
another credential.
