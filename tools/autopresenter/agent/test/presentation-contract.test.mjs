import assert from "node:assert/strict";
import test from "node:test";

import {
  CAT_KEYBOARD_ASSET,
  FRIENDS_CLUB_VIDEO_ASSET,
  INTRO_LOOP_RUNTIME_MS,
  INTRO_MUSIC_ASSET,
  INTRO_MUSIC_ASSETS,
  INTRO_SCENE_ID,
  LECTURE_UI_UX_ASSET,
  EXTRA_LECTURE_SCENE_IDS,
  JOKE_DATABASE_SCENE_IDS,
  LECTURE_ASSETS,
  LECTURE_SCENE_ORDER,
  LECTURE_SCENE_IDS,
  LECTURE_UI_REFERENCE_ASSETS,
  MANUAL_PAGE_SCENES,
  MANUAL_PAGE_SCENE_IDS,
  MARKET_SCENE_IDS,
  SCENE_ACCEPTANCE_CONTRACT,
  SHARE_PROOF_ASSETS,
  SERVICE_SCENE_IDS,
  WEEKEND_DESKTOP_SCENE_ID,
  ZNANIE_LOGO_ASSET,
} from "../presentation-contract.mjs";

test("presentation scenes remain explicit and the intro defaults to fifty minutes", () => {
  assert.equal(INTRO_SCENE_ID, "intro-loop");
  assert.deepEqual(
    LECTURE_SCENE_IDS,
    ["lecture-01", "lecture-02", "lecture-03", "lecture-04", "lecture-05", "lecture-06", "lecture-07"],
  );
  assert.equal(WEEKEND_DESKTOP_SCENE_ID, "weekend-desktop");
  assert.deepEqual(
    EXTRA_LECTURE_SCENE_IDS,
    [
      "lecture-ui-ux-path",
      "lecture-convenience-emergence",
      "lecture-usability-measurement",
      "lecture-good-ui",
      "lecture-poor-ui",
    ],
  );
  assert.deepEqual(
    LECTURE_SCENE_ORDER,
    [
      "lecture-01",
      "lecture-02",
      "lecture-ui-ux-path",
      "lecture-03",
      "lecture-04",
      "lecture-05",
      "lecture-06",
      "lecture-convenience-emergence",
      "lecture-usability-measurement",
      "lecture-07",
      "lecture-good-ui",
      "lecture-poor-ui",
    ],
  );
  assert.deepEqual(
    MARKET_SCENE_IDS,
    ["market-01-primary", "market-02-substitutes", "market-03-dynamics", "market-04-position"],
  );
  assert.deepEqual(
    JOKE_DATABASE_SCENE_IDS,
    Array.from({ length: 9 }, (_, index) => `joke-db-${String(index + 1).padStart(2, "0")}`),
  );
  assert.equal(INTRO_LOOP_RUNTIME_MS, 50 * 60 * 1_000);
  assert.equal(MANUAL_PAGE_SCENES.length, 30);
  assert.equal(MANUAL_PAGE_SCENE_IDS.length, 30);
  assert.deepEqual(
    MANUAL_PAGE_SCENES.slice(0, 4).map(({ id }) => id),
    [
      "manual-page-home-mobile",
      "manual-page-home-desktop",
      "manual-page-mobile-menu-mobile",
      "manual-page-mobile-menu-desktop",
    ],
  );
  assert.ok(MANUAL_PAGE_SCENES.every(({ url }) =>
    url.startsWith("https://kenigevents.ru/preview-20260730-hero-talk-date-donor-r2/")));
  assert.deepEqual(
    [...new Set(MANUAL_PAGE_SCENES.map(({ mode }) => mode))],
    ["mobile", "desktop"],
  );
  assert.ok(SERVICE_SCENE_IDS.includes("service-search-live"));
  assert.ok(SERVICE_SCENE_IDS.includes("service-personalization"));
  assert.ok(SERVICE_SCENE_IDS.includes("service-transport-rail"));
  assert.ok(SERVICE_SCENE_IDS.includes("service-transport-bus"));
  assert.ok(SERVICE_SCENE_IDS.includes("service-navigation-exhibitions"));
  assert.ok(SERVICE_SCENE_IDS.includes("service-navigation-festivals"));
  assert.deepEqual(
    SCENE_ACCEPTANCE_CONTRACT.frozen,
    ["tomorrow-mobile", "outro-qr"],
  );
  assert.ok(Object.hasOwn(SCENE_ACCEPTANCE_CONTRACT.reopened, "intro-loop"));
  assert.ok(Object.hasOwn(SCENE_ACCEPTANCE_CONTRACT.reopened, "lecture"));
  assert.ok(Object.hasOwn(SCENE_ACCEPTANCE_CONTRACT.reopened, "pwa"));
  assert.ok(Object.hasOwn(SCENE_ACCEPTANCE_CONTRACT.reopened, "tomorrow-rail-like"));
  assert.ok(Object.hasOwn(SCENE_ACCEPTANCE_CONTRACT.reopened, "weekend-amber-artifact"));
});

test("intro and lecture media use immutable content-addressed Yandex CDN URLs", () => {
  for (const asset of [
    ZNANIE_LOGO_ASSET,
    ...INTRO_MUSIC_ASSETS,
    LECTURE_UI_UX_ASSET,
    ...LECTURE_ASSETS,
    ...LECTURE_UI_REFERENCE_ASSETS,
  ]) {
    assert.match(asset.url, /^https:\/\/static\.kenigevents\.ru\/assets\/autopresenter\/scenario-20260730\//u);
    assert.ok(
      asset.url.includes(asset.sha256) || asset.url.includes(asset.sha256.slice(0, 16)),
      `${asset.url} is content-addressed`,
    );
    assert.match(asset.sha256, /^[a-f0-9]{64}$/u);
  }
  assert.equal(INTRO_MUSIC_ASSET, INTRO_MUSIC_ASSETS[0]);
  assert.equal(INTRO_MUSIC_ASSETS.length, 5);
  assert.deepEqual(
    INTRO_MUSIC_ASSETS.map(({ id }) => id),
    [
      "echo-sax-end",
      "maslov-nutcracker-march",
      "dave-brubeck-take-five",
      "maslov-nutcracker-waltz",
      "herbie-hancock-cantaloupe-island",
    ],
  );
  assert.match(LECTURE_UI_UX_ASSET.sourceSha256, /^[a-f0-9]{64}$/u);
  assert.deepEqual(
    LECTURE_ASSETS.map((asset) => asset.messageId),
    [821, 822, 823, 824, 825, 826, 830],
  );
  assert.deepEqual(
    LECTURE_UI_REFERENCE_ASSETS.map(({ source }) => source),
    [
      "https://t.me/c/4337049383/803/890",
      "https://t.me/c/4337049383/803/891",
      "https://t.me/c/4337049383/803/893",
    ],
  );
});

test("cat interruption uses a content-addressed sourced CDN asset", () => {
  assert.match(CAT_KEYBOARD_ASSET.url, new RegExp(`${CAT_KEYBOARD_ASSET.sha256}\\.webp$`, "u"));
  assert.match(CAT_KEYBOARD_ASSET.source, /^https:\/\/unsplash\.com\/photos\//u);
  assert.equal(CAT_KEYBOARD_ASSET.license, "Unsplash License");
});

test("friends club video is a content-addressed Telegram source asset", () => {
  assert.match(FRIENDS_CLUB_VIDEO_ASSET.url, new RegExp(`${FRIENDS_CLUB_VIDEO_ASSET.sha256}\\.mp4$`, "u"));
  assert.equal(FRIENDS_CLUB_VIDEO_ASSET.source, "https://t.me/c/4337049383/803/871");
  assert.equal(FRIENDS_CLUB_VIDEO_ASSET.contentType, "video/mp4");
  assert.ok(SERVICE_SCENE_IDS.includes("service-friends-club"));
  assert.ok(SERVICE_SCENE_IDS.includes("service-laws"));
  assert.ok(SERVICE_SCENE_IDS.includes("service-keyboard-event"));
});

test("share proof screenshots are pinned to the Yandex CDN with Telegram provenance", () => {
  assert.deepEqual(
    SHARE_PROOF_ASSETS.map(({ source }) => source),
    [
      "https://t.me/c/4337049383/803/885",
      "https://t.me/c/4337049383/803/886",
    ],
  );
  for (const asset of SHARE_PROOF_ASSETS) {
    assert.match(asset.url, /^https:\/\/static\.kenigevents\.ru\/assets\/autopresenter\//u);
    assert.match(asset.sha256, /^[a-f0-9]{64}$/u);
  }
});
