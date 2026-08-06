# Penpot API smoke for the design-system review loop

> Status: experimental integration proof. The production design-system source of truth remains Git.

## Purpose

This contour proves the narrow workflow requested for the emerging LoveKGD design system:

```text
Git/design-system definition
→ generated visual specimen in Penpot
→ owner comment over the specimen
→ later pull of unresolved comments
→ short reproducible ChatGPT prompt
→ candidate implementation and secret preview
→ owner sign-off
→ versioned release
```

Penpot is a visual review surface in this contract. The owner is not expected to edit generated design-system elements. No MCP server or Codex bridge is required.

## Current smoke scope

Workflow: `.github/workflows/penpot-design-system-api-smoke.yml`.

Adapter: `scripts/integrations/penpot_api_smoke_test.mjs`.

The workflow uses the repository secret `PENPOT_INTEGRATION_TOKEN` to:

1. authenticate through the Penpot personal-access-token API;
2. discover the intended team and project without printing the token or profile data;
3. create or reuse the dedicated file `00 — LoveKGD API smoke test`;
4. add one isolated frame with a button specimen carrying stable ID `core.button.smoke`;
5. create one marked comment thread over the button;
6. reread the file and fail if the generated frame is absent;
7. emit a direct workspace URL and an immutable JSON receipt.

The workflow never modifies another Penpot file. Repeated runs reuse the marked frame and comment thread instead of duplicating them.

## Safety boundary

- The PAT is read only from GitHub Actions secrets and is never written to an artifact or log.
- The live write is confined to the dedicated smoke-test file.
- A generated Penpot visual is a candidate/review projection, not a released design-system definition.
- Automatic writes from Penpot to production code are out of scope.
- The next slice may read unresolved comments and create a review bundle, but it must not auto-merge design changes.

## Run and evidence

The initial branch push runs the smoke automatically. After merge, manual executions require the explicit `allow_write=true` workflow input.

Terminal evidence consists of:

- workflow conclusion `success`;
- `penpot-api-smoke-<run-id>` artifact;
- `docs/reports/penpot-api-smoke-2026-08-06.md` with the exact Penpot revision and direct page URL.

## Follow-up acceptance

The owner opens the exact Penpot page and leaves a reply or a new comment over the generated button. The next implementation slice must:

1. pull open comment threads from the same file;
2. bind them to `core.button.smoke`;
3. generate a short prompt containing a secret review-bundle URL;
4. leave the visual and production state unchanged until that prompt is deliberately executed and approved.

This temporary workflow lives in `events-bot-new` because that repository currently owns the secret. After the proof succeeds, the adapter, workflow contract and secret should move to `onedayonemasterpiece/lovekgd-design-system` as the canonical home.
