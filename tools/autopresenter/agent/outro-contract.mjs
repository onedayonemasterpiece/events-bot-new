export const DEFAULT_PRESENTER_SCENE_ID = "live-site";
export const OUTRO_SCENE_ID = "outro-qr";

export const OUTRO_QR_ASSET = Object.freeze({
  url: "https://static.kenigevents.ru/assets/autopresenter/scenario-20260730/qr-survey-nikiforov-916b6fee58256c4f2111887bf70c502070a55e45a667650dfccdb1495016ccd9.png",
  width: 1155,
  height: 1155,
  sha256: "916b6fee58256c4f2111887bf70c502070a55e45a667650dfccdb1495016ccd9",
});

export const OUTRO_VISUAL_ACCEPTANCE = Object.freeze({
  format: "fullscreen-typographic-qr",
  headline: "Как вам?",
  supportLine: "Оцените событие — это займёт минуту.",
  accent: "coral-on-premium-dark",
  imageEntrance: Object.freeze({
    properties: Object.freeze(["opacity", "transform"]),
    transform: "soft-zoom",
    easing: "ease-in-out",
    reducedMotion: "near-instant",
  }),
  required: Object.freeze([
    "large-scannable-qr",
    "reserved-square-image-space",
    "eager-preloaded-cdn-image",
  ]),
  forbidden: Object.freeze([
    "status-card",
    "dashboard-labels",
    "instruction-clutter",
    "phone-frame",
    "explanatory-side-panel",
  ]),
});

export function resolvePresenterSceneId(value) {
  const requested = String(value || "").trim();
  if (!requested) return DEFAULT_PRESENTER_SCENE_ID;
  if (requested === DEFAULT_PRESENTER_SCENE_ID || requested === OUTRO_SCENE_ID) {
    return requested;
  }
  throw new Error(
    `unsupported presenter scene "${requested}"; expected ${DEFAULT_PRESENTER_SCENE_ID} or ${OUTRO_SCENE_ID}`,
  );
}
