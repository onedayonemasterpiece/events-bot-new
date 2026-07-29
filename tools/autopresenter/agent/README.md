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
`AUTOPRESENTER_ARTIFACT_DIR` enables 1920x1080 WebM capture and a pre-click PNG
of the final concrete event description.

The mobile interaction contract deliberately hides every mouse cursor. Real
Playwright taps are shown as a tap circle. Horizontal touch navigation is shown
as a directional swipe trail: the finger moves left while the event content is
presented as moving rightward toward its description. The scenario opens `/zavtra/`, chooses the
deterministic mobile event with the fewest rail images (then the lowest numeric
event ID), reveals and dwells on its `О событии` rail, opens the event detail,
and completes only after its mobile description is visibly highlighted. The
completion detail carries the selected event ID/title and both description
checkpoints for public E2E evidence.

The dormant desktop interaction contract is separate: when a future desktop
scenario selects `desktop` interaction mode, the shell shows currently pressed
keyboard keys and a `presenter:desktop-ui-response` label. Mobile scenarios
never render that keyboard overlay.

Stage shortcuts are handled without the control UI: `Space` or `Right Arrow` runs,
`Esc` stops, and `R` resets. The stage owns `F` fullscreen behavior.
