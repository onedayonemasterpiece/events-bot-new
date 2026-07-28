import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const source = await readFile(new URL("../agent.mjs", import.meta.url), "utf8");

test("implements exactly the tomorrow-mobile target and ready marker", () => {
  assert.match(source, /const SCENARIO = "tomorrow-mobile"/);
  assert.match(source, /data-presenter-id="nav-tomorrow"/);
  assert.match(source, /data-presenter-id="tomorrow-page-ready"/);
  assert.doesNotMatch(source, /desktop|typing|qr-code|infographic/i);
});

test("uses Playwright locator actions and never DOM activation", () => {
  assert.match(source, /await target\.scrollIntoViewIfNeeded\(\)/);
  assert.match(source, /await target\.boundingBox\(\)/);
  assert.match(source, /await target\.hover\(/);
  assert.match(source, /await target\.click\(/);
  assert.doesNotMatch(source, /element\.click|node\.click|\.evaluate\([^)]*=>[^;]*\.click/s);
});

test("cursor and ripple overlays cannot intercept pointer input", () => {
  const pointerNoneCount = source.match(/pointerEvents: "none"/g)?.length || 0;
  assert.ok(pointerNoneCount >= 2);
  assert.match(source, /data-autopresenter-cursor/);
  assert.match(source, /autopresenterRipple/);
});

test("poll, idempotent ack, TTL and bounded hard stop contracts are explicit", () => {
  assert.match(source, /\/api\/commands\/next/);
  assert.match(source, /after_seq/);
  assert.match(source, /ackCache/);
  assert.match(source, /isExpired\(command\)/);
  assert.match(source, /hardStopMs/);
  assert.match(source, /hardRecoverContext/);
  assert.match(source, /agent confirmed stopped/);
});

test("local fallback keys cover run, stop and reset", () => {
  assert.match(source, /event\.code === "Space" \|\| event\.code === "ArrowRight"/);
  assert.match(source, /event\.code === "Escape"/);
  assert.match(source, /event\.code === "KeyR"/);
});

test("stage status is visible and reset closes extra tabs", () => {
  assert.match(source, /new CustomEvent\("presenter:status"/);
  assert.match(source, /this\.context\.pages\(\)/);
  assert.match(source, /extraPage !== this\.page/);
});

test("visual evidence is fixed at 1920x1080", () => {
  assert.match(source, /width: 1920, height: 1080/);
  assert.match(source, /deviceScaleFactor: 1/);
  assert.match(source, /tomorrow-mobile-1920x1080\.png/);
  assert.match(source, /recordVideo/);
  assert.match(source, /this\.shutdownPromise/);
  assert.match(source, /await this\.context\?\.close/);
});
