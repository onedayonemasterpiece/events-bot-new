import assert from "node:assert/strict";
import test from "node:test";

import {
  INTRO_LOOP_RUNTIME_MS,
  INTRO_MUSIC_ASSET,
  INTRO_SCENE_ID,
  LECTURE_ASSETS,
  LECTURE_SCENE_IDS,
  SCENE_ACCEPTANCE_CONTRACT,
  SERVICE_SCENE_IDS,
  WEEKEND_DESKTOP_SCENE_ID,
  ZNANIE_LOGO_ASSET,
} from "../presentation-contract.mjs";

test("presentation scenes remain explicit and the intro defaults to fifty minutes", () => {
  assert.equal(INTRO_SCENE_ID, "intro-loop");
  assert.deepEqual(
    LECTURE_SCENE_IDS,
    ["lecture-01", "lecture-02", "lecture-03", "lecture-04", "lecture-05", "lecture-06", "lecture-07"],
  );
  assert.equal(WEEKEND_DESKTOP_SCENE_ID, "weekend-desktop");
  assert.equal(INTRO_LOOP_RUNTIME_MS, 50 * 60 * 1_000);
  assert.ok(SERVICE_SCENE_IDS.includes("service-search-live"));
  assert.deepEqual(
    SCENE_ACCEPTANCE_CONTRACT.frozen,
    ["tomorrow-mobile", "tomorrow-rail-like", "weekend-amber-artifact", "outro-qr"],
  );
});

test("intro and lecture media use immutable content-addressed Yandex CDN URLs", () => {
  for (const asset of [ZNANIE_LOGO_ASSET, INTRO_MUSIC_ASSET, ...LECTURE_ASSETS]) {
    assert.match(asset.url, /^https:\/\/static\.kenigevents\.ru\/assets\/autopresenter\/scenario-20260730\//u);
    assert.match(asset.url, new RegExp(`${asset.sha256.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&")}\\.[a-z0-9]+$`, "u"));
    assert.match(asset.sha256, /^[a-f0-9]{64}$/u);
  }
  assert.deepEqual(
    LECTURE_ASSETS.map((asset) => asset.messageId),
    [821, 822, 823, 824, 825, 826, 830],
  );
});
