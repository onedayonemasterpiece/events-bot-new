import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { SCENE_ACCEPTANCE_CONTRACT } from "../presentation-contract.mjs";

const agent = await readFile(new URL("../agent.mjs", import.meta.url), "utf8");
const stage = await readFile(
  new URL("../../../../site/src/pages/internal/presenter-stage/index.astro", import.meta.url),
  "utf8",
);

function digest(value) {
  return createHash("sha256").update(value).digest("hex");
}

function sliceBetween(source, start, end) {
  const from = source.indexOf(start);
  const to = source.indexOf(end, from);
  assert.ok(from >= 0 && to > from, `freeze markers missing: ${start} → ${end}`);
  return source.slice(from, to);
}

test("accepted scene implementations cannot change incidentally during draft iterations", () => {
  const expected = SCENE_ACCEPTANCE_CONTRACT.sourceSha256;
  const methods = {
    "tomorrow-mobile": ["  async runTomorrowMobile", "\n  async runTomorrowRailLike"],
    "tomorrow-rail-like": ["  async runTomorrowRailLike", "\n  async runWeekendAmberArtifact"],
    "weekend-amber-artifact": ["  async runWeekendAmberArtifact", "\n  async selectTomorrowEvent"],
    "outro-qr": ["  async runOutroQr", "\n  async openTomorrowFromHome"],
  };
  for (const [scene, markers] of Object.entries(methods)) {
    assert.equal(
      digest(sliceBetween(agent, ...markers)),
      expected[scene],
      `${scene} changed without explicitly reopening its acceptance`,
    );
  }

  const outroStart = stage.lastIndexOf("<section", stage.indexOf('class="outro-scene'));
  const outroEnd = stage.indexOf("</section>", outroStart) + "</section>".length;
  assert.ok(outroStart >= 0 && outroEnd > outroStart);
  assert.equal(
    digest(stage.slice(outroStart, outroEnd)),
    expected["outro-qr-stage"],
    "accepted outro composition changed without explicitly reopening its acceptance",
  );
});
