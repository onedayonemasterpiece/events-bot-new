# Autopresenter prototype agent

One headed Playwright agent for exactly three explicit scenarios:

- `tomorrow-mobile` (the fallback when `command.scenario` is omitted);
- `tomorrow-rail-like`;
- `weekend-amber-artifact`.

It expects the local relay and Astro presenter stage to be running. Unknown scenario
IDs fail instead of being interpreted as a scenario DSL.

```bash
AUTOPRESENTER_RELAY_URL=http://127.0.0.1:8787 \
AUTOPRESENTER_STAGE_URL=http://127.0.0.1:4321/internal/presenter-stage/ \
NODE_PATH="$(npm root -g)" npm start
```

Defaults are the values above. The loader accepts either a project-local Playwright
package or the centrally installed package. Set `AUTOPRESENTER_HEADLESS=1` only for
CI diagnostics; the supported prototype mode is headed. Optional
`AUTOPRESENTER_ARTIFACT_DIR` enables 1920x1080 WebM capture and a final PNG named
after the completed scenario.

The mobile interaction contract deliberately hides every mouse cursor. Real
Playwright taps are shown as a tap circle. Horizontal touch navigation is shown
as a directional swipe trail: the finger moves left while the event content is
presented as moving rightward. Vertical navigation uses sampled wheel input with
visible intermediate movement; tap helpers never scroll. The explicit like scenario
uses only the armed rail-edge gesture, and the amber scenario uses only real rail
drags and taps. Runs are paced to at least 12 seconds. Each explicit scenario has
its own timeout policy entry: the short tomorrow scenario remains bounded at 30
seconds, and the gesture-heavy rail/artifact scenarios at 120 seconds. The policy
admits a future explicit scene lasting up to one hour.

The headed browser, BrowserContext, stage page, and window are created once and
stay alive across Run, Stop, Reset, and scene switches. A new Run cooperatively
stops the active scene within a bounded deadline and starts the requested explicit
scenario in that same window. Scenario isolation clears the embedded site's local
and session storage and reloads its iframe instead of recreating the context.
Only Shutdown, SIGINT, or SIGTERM closes the presentation environment.

The dormant desktop interaction contract is separate: when a future desktop
scenario selects `desktop` interaction mode, the shell shows currently pressed
keyboard keys and a `presenter:desktop-ui-response` label. Mobile scenarios
never render that keyboard overlay.

Stage shortcuts are handled without the control UI: `Space` or `Right Arrow` runs,
`Esc` stops, and `R` resets. The stage owns `F` fullscreen behavior.
