import { OUTRO_SCENE_ID } from "./outro-contract.mjs";

export const INTERACTION_VISUAL_CONTRACTS = Object.freeze({
  mobile: Object.freeze({
    pointerVisual: "tap-circle",
    gestureVisual: "swipe-trail-with-direction",
    forbiddenVisuals: Object.freeze(["mouse-cursor"]),
  }),
  desktop: Object.freeze({
    inputVisual: "currently-pressed-keyboard-keys",
    responseVisual: "ui-response",
  }),
});

export const TOMORROW_MOBILE_CONTRACT = Object.freeze({
  id: "tomorrow-mobile",
  surface: "mobile",
  completion: "concrete-event-detail-description-visible-after-horizontal-rail-gesture",
});

export const TOMORROW_RAIL_LIKE_CONTRACT = Object.freeze({
  id: "tomorrow-rail-like",
  surface: "mobile",
  eventId: 5296,
  eventTitle: "Концерт «Фестиваль Pianissimo: Жуан Нету Виейра»",
  completion: "gesture-like-persisted-after-reload",
});

export const WEEKEND_AMBER_ARTIFACT_CONTRACT = Object.freeze({
  id: "weekend-amber-artifact",
  surface: "mobile",
  snapshotEventId: 6591,
  completion: "artifact-collected-and-detail-dialog-visible-after-reload",
});

export const OUTRO_QR_CONTRACT = Object.freeze({
  id: OUTRO_SCENE_ID,
  surface: "stage",
  completion: "fullscreen-survey-qr-loaded-and-visible",
});

export const DEFAULT_SCENARIO_ID = TOMORROW_MOBILE_CONTRACT.id;
export const SCENARIO_IDS = Object.freeze([
  TOMORROW_MOBILE_CONTRACT.id,
  TOMORROW_RAIL_LIKE_CONTRACT.id,
  WEEKEND_AMBER_ARTIFACT_CONTRACT.id,
  OUTRO_QR_CONTRACT.id,
]);
export const LONG_SCENE_TIMEOUT_CEILING_MS = 60 * 60 * 1_000;
export const SCENARIO_TIMEOUT_POLICY = Object.freeze({
  [TOMORROW_MOBILE_CONTRACT.id]: 120_000,
  [TOMORROW_RAIL_LIKE_CONTRACT.id]: 120_000,
  [WEEKEND_AMBER_ARTIFACT_CONTRACT.id]: 120_000,
  [OUTRO_QR_CONTRACT.id]: 30_000,
});

export function resolveScenarioId(value) {
  const requested = String(value || "").trim();
  if (!requested) return DEFAULT_SCENARIO_ID;
  if (SCENARIO_IDS.includes(requested)) return requested;
  throw new Error(
    `unsupported scenario "${requested}"; expected one of ${SCENARIO_IDS.join(", ")}`,
  );
}

export function resolveScenarioTimeoutMs(
  scenarioId,
  policy = SCENARIO_TIMEOUT_POLICY,
) {
  const timeoutMs = policy?.[scenarioId];
  if (
    !Number.isSafeInteger(timeoutMs) ||
    timeoutMs <= 0 ||
    timeoutMs > LONG_SCENE_TIMEOUT_CEILING_MS
  ) {
    throw new Error(
      `scenario "${scenarioId}" needs an explicit timeout between 1ms and ` +
        `${LONG_SCENE_TIMEOUT_CEILING_MS}ms`,
    );
  }
  return timeoutMs;
}

export function selectDeterministicMobileEvent(candidates) {
  const eligible = candidates
    .map((candidate) => ({
      eventId: String(candidate.eventId || "").trim(),
      title: String(candidate.title || "").trim(),
      galleryCount: Number(candidate.galleryCount),
    }))
    .filter(
      (candidate) =>
        /^\d+$/.test(candidate.eventId) &&
        candidate.title &&
        Number.isFinite(candidate.galleryCount) &&
        candidate.galleryCount >= 0,
    );

  eligible.sort(
    (left, right) =>
      left.galleryCount - right.galleryCount ||
      Number(left.eventId) - Number(right.eventId) ||
      left.title.localeCompare(right.title, "ru"),
  );
  return eligible[0] || null;
}
