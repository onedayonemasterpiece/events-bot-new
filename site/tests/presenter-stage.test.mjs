import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const source = await readFile(
  new URL('../src/pages/internal/presenter-stage/index.astro', import.meta.url),
  'utf8',
);

test('presenter stage keeps explicit scenes in one page without a scenario DSL', () => {
  for (const scene of ['live-site', 'intro-loop', 'service-wordmark', 'service-search-live', 'weekend-desktop', 'outro-qr']) {
    assert.match(source, new RegExp(`data-presenter-scene-id="${scene}"`, 'u'));
  }
  assert.match(source, /window\.addEventListener\('presenter:scene'/u);
  assert.match(source, /window\.addEventListener\('presenter:stop'/u);
  assert.match(source, /const scenes = new Map/u);
  assert.match(source, /scenes\.forEach\(\(scene, id\) => \{ scene\.hidden = id !== sceneId; \}\)/u);
  assert.doesNotMatch(source, /scenario queue|scene editor|scene schema/iu);
});

test('intro follows the Hero Talk semantic-fragment reveal and keeps two explicit lines', () => {
  assert.match(source, /data-presenter-id="intro-thought"/u);
  assert.match(source, /data-intro-line="one"/u);
  assert.match(source, /data-intro-line="two"/u);
  assert.match(source, /Добро пожаловать!/u);
  assert.match(source, /Почему одни сервисы удобны,/u);
  assert.match(source, /Сегодня вы — наша фокус-группа\./u);
  assert.match(source, /const introRoutes = \[/u);
  assert.match(source, /\['welcome', 'topic', 'format'/u);
  assert.match(source, /splitSemanticChunks/u);
  assert.match(source, /className = 'hero-fragment is-active'/u);
  assert.match(source, /if \(!reduceMotion\.matches && !await pause\(280, token\)\)/u);
  assert.match(source, /aria-live="off"/u);
  assert.match(source, /countdownPhrase/u);
  assert.match(source, /data-presenter-id="intro-music"/u);
  assert.match(source, /introMusic\.volume = \.15 \* progress/u);
  assert.match(source, /introMusic\.pause\(\)/u);
  assert.match(source, /introMusic\.currentTime = 0/u);
  assert.match(source, /\.intro-thought \{[\s\S]*font-size: clamp\(54px, 4\.35vw, 86px\)/u);
});

test('lecture is seven independently held frames with varied layouts and transparent branding', () => {
  for (const messageId of [821, 822, 823, 824, 825, 826, 830]) {
    assert.match(source, new RegExp(`messageId: ${messageId}`, 'u'));
  }
  assert.match(source, /Интерфейс видят\. Опыт проживают\./u);
  assert.match(source, /В сложных системах удобство — безопасность\./u);
  assert.match(source, /Понять → спроектировать → проверить\./u);
  assert.match(source, /data-presenter-scene-id=\{`lecture-\$\{String\(index \+ 1\)/u);
  assert.match(source, /brand-plate brand-plate--lecture/u);
  assert.match(source, /brand-logo--on-\$\{slide\.theme\}/u);
  assert.match(source, /layout: 'horizontal'/u);
  assert.match(source, /layout: 'cinema'/u);
  assert.doesNotMatch(source, /startLecture|lectureState = 'complete'|slideIntervalMs/u);
  assert.match(source, /@keyframes lecture-image-enter \{[\s\S]*scale\(\.9\)[\s\S]*scale\(1\)/u);
});

test('desktop weekend scene presents meaning first and then reveals the clean FHD site', () => {
  const start = source.indexOf('class="desktop-scene"');
  const end = source.indexOf('</section>', start);
  const markup = source.slice(start, end);
  assert.ok(start > 0 && end > start);
  assert.match(markup, /data-presenter-id="desktop-site-frame"/u);
  assert.match(markup, /width="1920"/u);
  assert.match(markup, /height="1080"/u);
  assert.match(markup, /data-presenter-id="desktop-meaning"/u);
  assert.match(markup, /Сначала мысль\. Затем/u);
  assert.match(source, /data-desktop-phase', 'meaning'/u);
  assert.match(source, /data-desktop-phase', 'site'/u);
  assert.match(source, /\.desktop-scene \{ position: absolute; inset: 0;/u);
  assert.match(source, /\.desktop-scene iframe \{ width: 100%; height: 100%;/u);
});

test('outro remains the accepted restrained fullscreen QR scene', () => {
  const outroStart = source.indexOf('class="outro-scene');
  const outroEnd = source.indexOf('</section>', outroStart);
  const outroMarkup = source.slice(outroStart, outroEnd);
  assert.ok(outroStart > 0 && outroEnd > outroStart);
  assert.match(outroMarkup, /<h1 id="outro-heading">Как вам\?<\/h1>/u);
  assert.match(outroMarkup, /Оцените событие — это займёт минуту\./u);
  assert.match(outroMarkup, /data-presenter-id="outro-qr-image"/u);
  assert.doesNotMatch(outroMarkup, /phone-shell|stage-status|dashboard|instruction/iu);
  assert.match(source, /width="1155"\s+height="1155"/u);
  assert.match(source, /@keyframes outro-qr-enter \{[\s\S]*scale\(\.82\)[\s\S]*opacity: 1/u);
});

test('all presentation media are immutable Yandex CDN assets and reduced motion is safe', () => {
  const urls = [...source.matchAll(/https:\/\/static\.kenigevents\.ru\/assets\/autopresenter\/scenario-20260730\/[^'"]+/gu)].map((match) => match[0]);
  assert.ok(urls.length >= 10);
  for (const url of urls) assert.match(url, /-[a-f0-9]{64}\.(?:png|svg|mp3|webp)$/u);
  assert.match(source, /@media \(prefers-reduced-motion: reduce\)/u);
  assert.match(source, /if \(!reduceMotion\.matches && !await pause\(280, token\)\)/u);
  assert.match(source, /\.hero-fragment\.is-active::after \{ display: none; \}/u);
  assert.match(source, /animation-duration: \.01ms !important/u);
});

test('service presentation uses the requested focus preview and explicit input-driven search', () => {
  assert.match(source, /preview-20260729-focus-simple-r15-a5cc0256/u);
  assert.match(source, /fokus-gruppa\/priglashenie\/#invite=focus-group-2026-announcements/u);
  assert.match(source, /data-presenter-scene-id="service-medallions-desktop"/u);
  assert.match(source, /data-presenter-scene-id="service-medallions-mobile"/u);
  assert.match(source, /data-presenter-scene-id="service-future-celebrity"/u);
  assert.match(source, /service-search-live/u);
});
