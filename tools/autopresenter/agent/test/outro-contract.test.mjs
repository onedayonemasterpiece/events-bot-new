import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  DEFAULT_PRESENTER_SCENE_ID,
  OUTRO_QR_ASSET,
  OUTRO_SCENE_ID,
  OUTRO_VISUAL_ACCEPTANCE,
  resolvePresenterSceneId,
} from "../outro-contract.mjs";

const stageSource = await readFile(
  new URL("../../../../site/src/pages/internal/presenter-stage/index.astro", import.meta.url),
  "utf8",
);

test("declares one explicit QR outro while live site remains the default", () => {
  assert.equal(DEFAULT_PRESENTER_SCENE_ID, "live-site");
  assert.equal(OUTRO_SCENE_ID, "outro-qr");
  assert.equal(resolvePresenterSceneId(undefined), "live-site");
  assert.equal(resolvePresenterSceneId("outro-qr"), "outro-qr");
  assert.throws(() => resolvePresenterSceneId("slides"), /unsupported presenter scene/u);
  assert.match(stageSource, /data-presenter-scene="live-site"/u);
  assert.match(stageSource, /window\.addEventListener\('presenter:scene'/u);
  assert.match(stageSource, /detail\.id/u);
});

test("pins the immutable CDN survey image and its intrinsic dimensions", () => {
  assert.deepEqual(OUTRO_QR_ASSET, {
    url: "https://static.kenigevents.ru/assets/autopresenter/scenario-20260730/qr-survey-nikiforov-916b6fee58256c4f2111887bf70c502070a55e45a667650dfccdb1495016ccd9.png",
    width: 1155,
    height: 1155,
    sha256: "916b6fee58256c4f2111887bf70c502070a55e45a667650dfccdb1495016ccd9",
  });
  assert.ok(stageSource.includes(OUTRO_QR_ASSET.url));
  assert.match(stageSource, /rel="preload" as="image" href=\{outroQrUrl\} fetchpriority="high"/u);
  assert.match(stageSource, /width="1155"\s+height="1155"/u);
  assert.match(stageSource, /loading="eager"/u);
  assert.match(stageSource, /fetchpriority="high"/u);
});

test("records and implements the premium fullscreen visual acceptance", () => {
  assert.deepEqual(OUTRO_VISUAL_ACCEPTANCE.imageEntrance, {
    properties: ["opacity", "transform"],
    transform: "soft-zoom",
    easing: "ease-in-out",
    reducedMotion: "near-instant",
  });
  assert.deepEqual(OUTRO_VISUAL_ACCEPTANCE.forbidden, [
    "status-card",
    "dashboard-labels",
    "instruction-clutter",
    "phone-frame",
    "explanatory-side-panel",
  ]);
  assert.match(stageSource, />Как вам\?</u);
  assert.match(stageSource, /Оцените событие — это займёт минуту\./u);
  assert.match(stageSource, /@keyframes outro-qr-enter/u);
  assert.match(stageSource, /opacity: 0;[\s\S]*scale\(\.82\)/u);
  assert.match(stageSource, /animation: outro-qr-enter 940ms cubic-bezier/u);
  assert.match(stageSource, /@media \(prefers-reduced-motion: reduce\)/u);
});
