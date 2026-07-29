import assert from "node:assert/strict";
import { getEventListeners } from "node:events";
import test from "node:test";
import { abortableDelay } from "../abort-utils.mjs";

test("sequential scenario delays release their AbortSignal listeners", async () => {
  const controller = new AbortController();
  for (let index = 0; index < 25; index += 1) {
    await abortableDelay(0, controller.signal);
    assert.equal(getEventListeners(controller.signal, "abort").length, 0);
  }
});

test("aborting a delay rejects once and releases its listener", async () => {
  const controller = new AbortController();
  const delay = abortableDelay(60_000, controller.signal);
  assert.equal(getEventListeners(controller.signal, "abort").length, 1);
  controller.abort();
  await assert.rejects(delay, { name: "AbortError", message: "scenario stopped" });
  assert.equal(getEventListeners(controller.signal, "abort").length, 0);
});
