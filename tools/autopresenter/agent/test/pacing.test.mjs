import assert from "node:assert/strict";
import test from "node:test";
import { buildVerticalWheelTrajectory, PACING } from "../pacing.mjs";

function assertMonotonicIntermediate(trajectory, distance) {
  assert.ok(trajectory.length >= 2);
  assert.notEqual(trajectory[0].cumulativeY, distance);
  const direction = Math.sign(distance);
  let previous = 0;
  for (const step of trajectory) {
    assert.equal(Math.sign(step.deltaY), direction);
    assert.ok(
      direction > 0
        ? step.cumulativeY > previous
        : step.cumulativeY < previous,
    );
    previous = step.cumulativeY;
  }
  assert.equal(trajectory.at(-1).cumulativeY, distance);
}

test("vertical wheel trajectory is monotonic and contains intermediate movement", () => {
  assertMonotonicIntermediate(buildVerticalWheelTrajectory(1_700), 1_700);
  assertMonotonicIntermediate(buildVerticalWheelTrajectory(-1_700), -1_700);
});

test("vertical trajectory duration stays inside the pacing bounds", () => {
  const short = buildVerticalWheelTrajectory(12);
  const typical = buildVerticalWheelTrajectory(1_700);
  const long = buildVerticalWheelTrajectory(20_000);
  assert.equal(short.at(-1).atMs, PACING.verticalMinDurationMs);
  assert.ok(typical.at(-1).atMs > PACING.verticalMinDurationMs);
  assert.ok(typical.at(-1).atMs < PACING.verticalMaxDurationMs);
  assert.equal(long.at(-1).atMs, PACING.verticalMaxDurationMs);
  for (const trajectory of [short, typical, long]) {
    assert.ok(trajectory.every((step) => step.delayMs > 0));
    assert.ok(
      trajectory.slice(0, -1).every(
        (step) => step.delayMs <= PACING.verticalSampleMs + Number.EPSILON,
      ),
    );
  }
});

test("zero distance has no synthetic wheel movement", () => {
  assert.deepEqual(buildVerticalWheelTrajectory(0), []);
});
