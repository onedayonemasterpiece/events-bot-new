# Autopresenter prototype agent

One headed Playwright agent for the single `tomorrow-mobile` prototype scenario.
It expects the local relay and Astro presenter stage to be running.

```bash
AUTOPRESENTER_RELAY_URL=http://127.0.0.1:8787 \
AUTOPRESENTER_STAGE_URL=http://127.0.0.1:4321/internal/presenter-stage/ \
NODE_PATH="$(npm root -g)" npm start
```

Defaults are the values above. The loader accepts either a project-local Playwright
package or the centrally installed package. Set `AUTOPRESENTER_HEADLESS=1` only for
CI diagnostics; the supported prototype mode is headed. Optional
`AUTOPRESENTER_ARTIFACT_DIR` enables 1920x1080 WebM capture and a completion PNG.

Stage shortcuts are handled without the control UI: `Space` or `Right Arrow` runs,
`Esc` stops, and `R` resets. The stage owns `F` fullscreen behavior.
