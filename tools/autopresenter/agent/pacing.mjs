export const PACING = Object.freeze({
  scenarioTypicalMinMs: 12_000,
  verticalVelocityPxPerSecond: 850,
  verticalMinDurationMs: 650,
  verticalMaxDurationMs: 2_600,
  verticalSampleMs: 80,
  verticalFinalCorrectionPx: 120,
  settleSampleMs: 50,
  settleStableSamples: 3,
  settleMaxMs: 1_200,
  tapLeadMs: 360,
  routeDwellMs: 650,
  railSteps: 18,
  railStepMs: 34,
});

function clamp(value, minimum, maximum) {
  return Math.max(minimum, Math.min(maximum, value));
}

function smoothstep(progress) {
  return progress * progress * (3 - 2 * progress);
}

export function buildVerticalWheelTrajectory(
  distancePx,
  {
    velocityPxPerSecond = PACING.verticalVelocityPxPerSecond,
    minDurationMs = PACING.verticalMinDurationMs,
    maxDurationMs = PACING.verticalMaxDurationMs,
    sampleMs = PACING.verticalSampleMs,
  } = {},
) {
  const distance = Number(distancePx);
  if (!Number.isFinite(distance)) throw new TypeError("distancePx must be finite");
  if (distance === 0) return Object.freeze([]);
  if (!(velocityPxPerSecond > 0)) throw new RangeError("velocityPxPerSecond must be positive");
  if (!(sampleMs > 0)) throw new RangeError("sampleMs must be positive");
  if (!(minDurationMs > 0 && maxDurationMs >= minDurationMs)) {
    throw new RangeError("duration bounds are invalid");
  }

  const durationMs = clamp(
    (Math.abs(distance) / velocityPxPerSecond) * 1_000,
    minDurationMs,
    maxDurationMs,
  );
  const sampleCount = Math.max(2, Math.ceil(durationMs / sampleMs));
  const steps = [];
  let previousCumulative = 0;
  let previousAtMs = 0;

  for (let index = 1; index <= sampleCount; index += 1) {
    const progress = index / sampleCount;
    const cumulative = index === sampleCount ? distance : distance * smoothstep(progress);
    const atMs = index === sampleCount ? durationMs : durationMs * progress;
    steps.push(
      Object.freeze({
        atMs,
        delayMs: atMs - previousAtMs,
        deltaY: cumulative - previousCumulative,
        cumulativeY: cumulative,
      }),
    );
    previousCumulative = cumulative;
    previousAtMs = atMs;
  }
  return Object.freeze(steps);
}
