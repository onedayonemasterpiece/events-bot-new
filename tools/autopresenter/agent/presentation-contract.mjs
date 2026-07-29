export const INTRO_SCENE_ID = "intro-loop";
export const WEEKEND_DESKTOP_SCENE_ID = "weekend-desktop";

export const INTRO_LOOP_RUNTIME_MS = 50 * 60 * 1_000;

export const FOCUS_PREVIEW_BASE_URL =
  "https://kenigevents.ru/preview-20260729-focus-simple-r15-a5cc0256";
export const FOCUS_INVITATION_URL =
  `${FOCUS_PREVIEW_BASE_URL}/fokus-gruppa/priglashenie/#invite=focus-group-2026-announcements`;
export const FOCUS_INVITATION_SCENE_ID = "service-focus-group";
export const FOCUS_NPS_URL = `${FOCUS_PREVIEW_BASE_URL}/segodnya/`;

export const ZNANIE_LOGO_ASSET = Object.freeze({
  url: "https://static.kenigevents.ru/assets/autopresenter/scenario-20260730/znanie-logo-b97bf38f1b152a8eb3bbae79cb38df24cc2543ec2538d6f0d58863c9698072a9.svg",
  sha256: "b97bf38f1b152a8eb3bbae79cb38df24cc2543ec2538d6f0d58863c9698072a9",
});

export const INTRO_MUSIC_ASSET = Object.freeze({
  url: "https://static.kenigevents.ru/assets/autopresenter/scenario-20260730/echo-sax-end-6d7494c0d24c1815ac72a120e96b23782a2e92ef1ce26fb67769693d057fd08a.mp3",
  sha256: "6d7494c0d24c1815ac72a120e96b23782a2e92ef1ce26fb67769693d057fd08a",
});

const lectureSources = [
  [821, "2f3c1b7d9a1c7094c77da009867c25a62cb233110185ab7a1a020b61356bdc26"],
  [822, "d85cadfd1dd4aad0c0e8f5fb68482624a1a4d617585fae9737fd430fac9513d1"],
  [823, "61de6c5363792bc2fb5e50d6182c90f926fa7021c12d9bd80f96dff5b9d62bdc"],
  [824, "19c1f686d1b04f0b99463d24a16625827f0ff3e45bccbffb7c6e3b9d8f5d561b"],
  [825, "1734e48aa4ef2b20c19c975c76d91b507fe98999d49655bf71749e0b8fd9f43b"],
  [826, "252137528a929f8165c441b6fccdc853169fd4c4a249c0870f3b9b26a15ecadb"],
  [830, "cdcb0a381189715a8cebf90122000b7ddad74350390df87fd881fed658eff582"],
];

export const LECTURE_SCENES = Object.freeze(
  lectureSources.map(([messageId, sha256], index) =>
    Object.freeze({
      id: `lecture-${String(index + 1).padStart(2, "0")}`,
      messageId,
      url: `https://static.kenigevents.ru/assets/autopresenter/scenario-20260730/lecture-${messageId}-${sha256}.webp`,
      sha256,
    }),
  ),
);
export const LECTURE_SCENE_IDS = Object.freeze(LECTURE_SCENES.map(({ id }) => id));
export const LECTURE_ASSETS = LECTURE_SCENES;

export const SERVICE_SCENE_IDS = Object.freeze([
  "service-wordmark",
  "service-needs",
  "service-medallions",
  "service-medallions-desktop",
  "service-medallions-mobile",
  "service-joke",
  "service-search-concept",
  "service-search-live",
  "service-disruption",
  "service-taste",
  "service-feedback",
  FOCUS_INVITATION_SCENE_ID,
  "service-nps",
  "service-future-celebrity",
]);

export const STATIC_PRESENTATION_SCENE_IDS = Object.freeze([
  ...LECTURE_SCENE_IDS,
  "service-wordmark",
  "service-needs",
  "service-medallions",
  "service-joke",
  "service-search-concept",
  "service-disruption",
  "service-taste",
  "service-feedback",
]);

// Explicitly accepted scenes stay closed to incidental redesign during draft iterations.
export const SCENE_ACCEPTANCE_CONTRACT = Object.freeze({
  version: "2026-07-29.telegram-850",
  frozen: Object.freeze(["outro-qr"]),
  sourceSha256: Object.freeze({
    "tomorrow-mobile": "c4c6c6845fd129ef8701e34c8463e05570a5b3e7e110e1b12994704d7e03e776",
    "tomorrow-rail-like": "9c989b823a8a9967dddd4dcd634ec8cc8610f5ba841ab9633ccde0c500e8dba1",
    "weekend-amber-artifact": "bbc133dda3526fd9ecb790a537776e1a400a1f82100f775645681d6b48691465",
    "outro-qr": "625cc68566bb112c809d14873d4a446b8fb7cd250a6a769467944a6f2a44ad55",
    "outro-qr-stage": "7f9cdd99fb2cb1c2c8d581d9e9b1f2c7efac83b4a9e6bc1bcfaef75652478c31",
  }),
  reopened: Object.freeze({
    "intro-loop": "Hero Talk behavior and timer feedback",
    lecture: "Telegram 843–849: seven held scenes, varied layouts and themes",
    "tomorrow-mobile": "Telegram 803/2026-07-29: current-preview menu/selectors and pacing regression",
    "tomorrow-rail-like": "Telegram 803/2026-07-29: current event contract and missing rail gesture",
    "weekend-amber-artifact": "Telegram 803/2026-07-29: artifact actions absent after Weekend transition",
    "weekend-desktop": "Telegram 850: meaning-first then live site",
    pwa: "Telegram 840/844: stable shelf and sticky timer",
  }),
  draftVerification: "targeted-new-or-reopened-scenes-only",
  finalVerification: "one-full-regression-gate",
});
