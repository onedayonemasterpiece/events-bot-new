import { OUTRO_SCENE_ID } from "./outro-contract.mjs";
import {
  FOCUS_INVITATION_SCENE_ID,
  EXTRA_LECTURE_SCENE_IDS,
  INTRO_SCENE_ID,
  JOKE_DATABASE_SCENE_IDS,
  LECTURE_SCENE_IDS,
  MARKET_SCENE_IDS,
  SERVICE_SCENE_IDS,
  STATIC_PRESENTATION_SCENE_IDS,
  WEEKEND_DESKTOP_SCENE_ID,
} from "./presentation-contract.mjs";

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

export const TOMORROW_MOBILE_CONTRACT = Object.freeze({ id: "tomorrow-mobile", surface: "mobile", completion: "concrete-event-detail-description-visible-after-horizontal-rail-gesture" });
export const TOMORROW_RAIL_LIKE_CONTRACT = Object.freeze({ id: "tomorrow-rail-like", surface: "mobile", eventId: 5297, eventTitle: "Фестиваль Pianissimo: Игорь Сидоров", completion: "gesture-like-persisted-after-reload" });
export const WEEKEND_AMBER_ARTIFACT_CONTRACT = Object.freeze({ id: "weekend-amber-artifact", surface: "mobile", snapshotEventId: 7164, completion: "artifact-collected-and-detail-dialog-visible-after-reload" });
export const OUTRO_QR_CONTRACT = Object.freeze({ id: OUTRO_SCENE_ID, surface: "stage", completion: "fullscreen-survey-qr-loaded-and-visible" });
export const INTRO_LOOP_CONTRACT = Object.freeze({ id: INTRO_SCENE_ID, surface: "stage", completion: "fifty-minute-logical-randomized-two-line-hero-talk-loop" });
export const WEEKEND_DESKTOP_CONTRACT = Object.freeze({ id: WEEKEND_DESKTOP_SCENE_ID, surface: "desktop", completion: "meaning-first-then-live-weekend-page-at-fhd-and-natural-scroll" });
export const SEARCH_AUTH_SETUP_SCENE_ID = "service-search-auth-setup";
export const LECTURE_SCENE_CONTRACTS = Object.freeze(
  [...LECTURE_SCENE_IDS, ...EXTRA_LECTURE_SCENE_IDS]
    .map((id) => Object.freeze({ id, surface: "stage", completion: "held-until-another-explicit-command" })),
);
export const SERVICE_SCENE_CONTRACTS = Object.freeze(SERVICE_SCENE_IDS.map((id) => Object.freeze({ id, surface: id.endsWith("-mobile") || id.endsWith("-live") ? "mobile" : id.endsWith("-desktop") ? "desktop" : "stage", completion: "explicit-scene-visible-and-held" })));

export const DEFAULT_SCENARIO_ID = TOMORROW_MOBILE_CONTRACT.id;
export const SCENARIO_IDS = Object.freeze([
  INTRO_LOOP_CONTRACT.id,
  ...LECTURE_SCENE_IDS,
  ...EXTRA_LECTURE_SCENE_IDS,
  ...MARKET_SCENE_IDS,
  ...JOKE_DATABASE_SCENE_IDS,
  TOMORROW_MOBILE_CONTRACT.id,
  TOMORROW_RAIL_LIKE_CONTRACT.id,
  WEEKEND_AMBER_ARTIFACT_CONTRACT.id,
  ...SERVICE_SCENE_IDS,
  SEARCH_AUTH_SETUP_SCENE_ID,
  WEEKEND_DESKTOP_CONTRACT.id,
  OUTRO_QR_CONTRACT.id,
]);
export const LONG_SCENE_TIMEOUT_CEILING_MS = 60 * 60 * 1_000;
export const SCENARIO_TIMEOUT_POLICY = Object.freeze(Object.fromEntries(SCENARIO_IDS.map((id) => [id, id === INTRO_LOOP_CONTRACT.id ? LONG_SCENE_TIMEOUT_CEILING_MS : id === SEARCH_AUTH_SETUP_SCENE_ID ? 10 * 60 * 1_000 : [TOMORROW_MOBILE_CONTRACT.id, TOMORROW_RAIL_LIKE_CONTRACT.id, WEEKEND_AMBER_ARTIFACT_CONTRACT.id, WEEKEND_DESKTOP_CONTRACT.id, "service-search-live", "service-nps", "service-medallions-desktop", "service-medallions-mobile", "service-transport-rail", "service-navigation-exhibitions", "service-navigation-festivals", FOCUS_INVITATION_SCENE_ID].includes(id) ? 120_000 : 30_000])));

export function isStaticPresentationScenario(id) { return STATIC_PRESENTATION_SCENE_IDS.includes(id); }
export function resolveScenarioId(value) {
  const requested = String(value || "").trim();
  if (!requested) return DEFAULT_SCENARIO_ID;
  if (SCENARIO_IDS.includes(requested)) return requested;
  throw new Error(`unsupported scenario "${requested}"; expected one of ${SCENARIO_IDS.join(", ")}`);
}
export function resolveScenarioTimeoutMs(scenarioId, policy = SCENARIO_TIMEOUT_POLICY) {
  const timeoutMs = policy?.[scenarioId];
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0 || timeoutMs > LONG_SCENE_TIMEOUT_CEILING_MS) throw new Error(`scenario "${scenarioId}" needs an explicit timeout between 1ms and ${LONG_SCENE_TIMEOUT_CEILING_MS}ms`);
  return timeoutMs;
}
export function selectDeterministicMobileEvent(candidates) {
  const eligible = candidates.map((candidate) => ({ eventId: String(candidate.eventId || "").trim(), title: String(candidate.title || "").trim(), galleryCount: Number(candidate.galleryCount) })).filter((candidate) => /^\d+$/.test(candidate.eventId) && candidate.title && Number.isFinite(candidate.galleryCount) && candidate.galleryCount >= 0);
  eligible.sort((left, right) => left.galleryCount - right.galleryCount || Number(left.eventId) - Number(right.eventId) || left.title.localeCompare(right.title, "ru"));
  return eligible[0] || null;
}
