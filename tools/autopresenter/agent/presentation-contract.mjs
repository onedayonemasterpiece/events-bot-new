export const INTRO_SCENE_ID = "intro-loop";
export const WEEKEND_DESKTOP_SCENE_ID = "weekend-desktop";

export const INTRO_LOOP_RUNTIME_MS = 50 * 60 * 1_000;

export const FOCUS_PREVIEW_BASE_URL =
  "https://kenigevents.ru/preview-20260729-focus-simple-r15-a5cc0256";
export const FOCUS_INVITATION_URL =
  `${FOCUS_PREVIEW_BASE_URL}/fokus-gruppa/priglashenie/#invite=focus-group-2026-announcements`;
export const FOCUS_INVITATION_SCENE_ID = "service-focus-group";
export const FOCUS_PAGE_RATING_URL = `${FOCUS_PREVIEW_BASE_URL}/segodnya/`;

export const ZNANIE_LOGO_ASSET = Object.freeze({
  url: "https://static.kenigevents.ru/assets/autopresenter/scenario-20260730/znanie-logo-b97bf38f1b152a8eb3bbae79cb38df24cc2543ec2538d6f0d58863c9698072a9.svg",
  sha256: "b97bf38f1b152a8eb3bbae79cb38df24cc2543ec2538d6f0d58863c9698072a9",
});

export const INTRO_MUSIC_ASSET = Object.freeze({
  url: "https://static.kenigevents.ru/assets/autopresenter/scenario-20260730/echo-sax-end-6d7494c0d24c1815ac72a120e96b23782a2e92ef1ce26fb67769693d057fd08a.mp3",
  sha256: "6d7494c0d24c1815ac72a120e96b23782a2e92ef1ce26fb67769693d057fd08a",
});

export const CAT_KEYBOARD_ASSET = Object.freeze({
  url: "https://static.kenigevents.ru/assets/autopresenter/scenario-20260730/cat-keyboard-unsplash-dbbe5f90f00dc4e4bb483d3374626a668aa1d395ea1e352ec1661b8b7ebc9e79.webp",
  sha256: "dbbe5f90f00dc4e4bb483d3374626a668aa1d395ea1e352ec1661b8b7ebc9e79",
  source: "https://unsplash.com/photos/a-white-cat-sitting-on-top-of-a-computer-keyboard-ek1GsWCSY50",
  author: "Cheung Gnaiq",
  license: "Unsplash License",
});

export const FRIENDS_CLUB_VIDEO_ASSET = Object.freeze({
  url: "https://static.kenigevents.ru/assets/autopresenter/scenario-20260730/friends-club-darya-7cb34fb872eb528a4938f4e7af3cd8d2ebf1850246cb0cf9e2b44e7b17b05ac6.mp4",
  sha256: "7cb34fb872eb528a4938f4e7af3cd8d2ebf1850246cb0cf9e2b44e7b17b05ac6",
  source: "https://t.me/c/4337049383/803/871",
  contentType: "video/mp4",
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
export const EXTRA_LECTURE_SCENE_IDS = Object.freeze([
  "lecture-convenience-emergence",
  "lecture-usability-measurement",
]);
export const MARKET_SCENE_IDS = Object.freeze([
  "market-01-primary",
  "market-02-substitutes",
  "market-03-dynamics",
  "market-04-position",
]);
export const EXPANDED_SERVICE_SCENE_IDS = Object.freeze([
  "service-navigation-map",
  "service-social-proof",
  "service-artifacts-explained",
  "service-artifact-desktop",
  "service-laws",
  "service-keyboard-concept",
  "service-keyboard-day",
  "service-keyboard-event",
  "service-fast-find",
  "service-share-friends",
  "service-calendar-memory",
  "service-community-curator",
  "service-location-artifact",
  "service-friends-club",
]);

export const SERVICE_SCENE_IDS = Object.freeze([
  "service-wordmark",
  "service-needs",
  "service-medallions",
  "service-medallions-desktop",
  "service-medallions-mobile",
  "service-joke",
  "service-search-concept",
  "service-search-live",
  "service-personalization",
  "service-disruption",
  "service-taste",
  "service-feedback",
  FOCUS_INVITATION_SCENE_ID,
  "service-nps",
  "service-future-celebrity",
  "service-transport-rail",
  "service-transport-bus",
  ...EXPANDED_SERVICE_SCENE_IDS,
]);

export const STATIC_PRESENTATION_SCENE_IDS = Object.freeze([
  ...LECTURE_SCENE_IDS,
  ...EXTRA_LECTURE_SCENE_IDS,
  ...MARKET_SCENE_IDS,
  "service-wordmark",
  "service-needs",
  "service-medallions",
  "service-joke",
  "service-search-concept",
  "service-personalization",
  "service-disruption",
  "service-taste",
  "service-feedback",
  "service-transport-bus",
  ...EXPANDED_SERVICE_SCENE_IDS,
]);

// Explicitly accepted scenes stay closed to incidental redesign during draft iterations.
export const SCENE_ACCEPTANCE_CONTRACT = Object.freeze({
  version: "2026-07-29.iteration-e",
  frozen: Object.freeze([
    "tomorrow-mobile",
    "outro-qr",
  ]),
  sourceSha256: Object.freeze({
    "tomorrow-mobile": "c4c6c6845fd129ef8701e34c8463e05570a5b3e7e110e1b12994704d7e03e776",
    "tomorrow-rail-like": "a74cd05d72f5de9204a8e53ab50996fb3100e7ceb167df6a43e1ae10d353f3d7",
    "weekend-amber-artifact": "5c1904c1ebdac2fd99e4f6f7072a6b6443f1339077dae7d4bf68de5f3c1bb5f0",
    "outro-qr": "625cc68566bb112c809d14873d4a446b8fb7cd250a6a769467944a6f2a44ad55",
    "outro-qr-stage": "7f9cdd99fb2cb1c2c8d581d9e9b1f2c7efac83b4a9e6bc1bcfaef75652478c31",
  }),
  reopened: Object.freeze({
    "intro-loop": "Hero Talk behavior and timer feedback",
    lecture: "Telegram 843–849: seven held scenes, varied layouts and themes",
    "weekend-desktop": "Telegram 850: meaning-first then live site",
    pwa: "Telegram 840/844 and base: stable shelf, sticky timer, long PWA name and rating terminology",
    "tomorrow-rail-like": "latest owner run: readiness gate hid the rail and like gesture",
    "weekend-amber-artifact": "latest owner run: artifact journey was not observable",
  }),
  draftVerification: "targeted-new-or-reopened-scenes-only",
  finalVerification: "one-full-regression-gate",
});
