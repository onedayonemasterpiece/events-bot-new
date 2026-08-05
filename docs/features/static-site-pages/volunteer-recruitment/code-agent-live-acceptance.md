# Code-agent task: replace one rejected Kaggle secret value

Repository: `onedayonemasterpiece/events-bot-new`.

Do not modify code, workflows, branches, PRs, variables or documentation.
Do not create another secret name.

Perform only this operation:

1. In GitHub Environment `volunteer-monitor-canary`, replace the **value** of
   existing secret `KAGGLE_API_TOKEN` with the exact raw current Kaggle access
   token generated in Kaggle account settings.

The value must contain only the token itself: no secret name, quotes, JSON,
filename, explanation, GitHub token or legacy `kaggle.json` wrapper.

Never print, echo, return or commit the value. Verify only that the secret update
succeeded. Do not run or edit anything else.

If a current Kaggle access token is not securely available, report only:

```text
A current raw KAGGLE_API_TOKEN is required
```
