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
