import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const source = await readFile(
  new URL('../src/pages/internal/presenter-stage/index.astro', import.meta.url),
  'utf8',
);

test('presenter stage keeps explicit scenes in one page without a scenario DSL', () => {
  for (const scene of ['live-site', 'intro-loop', 'service-wordmark', 'service-comfort', 'service-needs', 'service-search-live', 'weekend-desktop', 'outro-qr']) {
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
  assert.match(source, /Сегодня задача со звёздочкой\./u);
  assert.match(source, /как устроен этот инструмент\./u);
  assert.match(source, /const introRoutes = \[/u);
  assert.match(source, /\['welcome', 'topic', 'challenge', 'format'/u);
  assert.match(source, /splitSemanticChunks/u);
  assert.match(source, /className = 'hero-fragment'/u);
  assert.match(source, /node\.append\(document\.createTextNode\(' '\)\)/u);
  assert.match(source, /requestAnimationFrame\(\(\) => requestAnimationFrame\(resolve\)\)/u);
  assert.match(source, /fragment\.classList\.add\('is-active'\)/u);
  assert.match(source, /seededUnit/u);
  assert.match(source, /cadence = 190/u);
  assert.match(source, /const route = \[\.\.\.pickRoute\(\)\]/u);
  assert.match(source, /return \['Вот-вот', 'начинаем'\]/u);
  assert.match(source, /aria-live="off"/u);
  assert.match(source, /countdownPhrase/u);
  assert.match(source, /dataIntroDensity|dataset\.introDensity/u);
  assert.match(source, /\.intro-scene\[data-intro-density="compact"\]/u);
  assert.match(source, /data-presenter-id="intro-music"/u);
  assert.match(source, /introMusic\.volume = \.15 \* progress/u);
  assert.match(source, /introMusic\.pause\(\)/u);
  assert.match(source, /introMusic\.currentTime = 0/u);
  assert.match(source, /\.intro-thought \{[\s\S]*font-size: clamp\(54px, 4\.35vw, 86px\)/u);
});

test('lecture keeps seven source frames and adds two independently held conceptual frames', () => {
  for (const messageId of [821, 822, 823, 824, 825, 826, 830]) {
    assert.match(source, new RegExp(`messageId: ${messageId}`, 'u'));
  }
  assert.match(source, /Интерфейс видят\. Опыт проживают\./u);
  assert.match(source, /В сложных системах удобство — безопасность\./u);
  assert.match(source, /Понять → спроектировать → проверить\./u);
  assert.match(source, /data-presenter-scene-id="lecture-convenience-emergence"/u);
  assert.match(source, /data-presenter-scene-id="lecture-usability-measurement"/u);
  assert.match(source, /Можно ли сразу стать удобным\?/u);
  assert.match(source, /Можно ли удобство измерить\?/u);
  assert.match(source, /desire-path-ludwell-cc-by-sa-2\.jpg/u);
  assert.match(source, /data-presenter-scene-id=\{`lecture-\$\{String\(index \+ 1\)/u);
  assert.match(source, /brand-plate brand-plate--lecture/u);
  assert.match(source, /brand-logo--on-\$\{slide\.theme\}/u);
  assert.match(source, /layout: 'horizontal'/u);
  assert.match(source, /layout: 'cinema'/u);
  assert.match(source, /const typeLecture = async \(scene, token\)/u);
  assert.match(source, /scene\.dataset\.lectureState = 'typing'/u);
  assert.match(source, /scene\.dataset\.lectureState = 'media'/u);
  assert.match(source, /scene\.dataset\.lectureState = 'complete'/u);
  assert.doesNotMatch(source, /slideIntervalMs/u);
  assert.match(source, /\.lecture-visual \{[\s\S]*border: 0; border-radius: 0; background: transparent; box-shadow: none;/u);
  assert.match(source, /\.lecture-slide\[data-lecture-index="6"\][\s\S]*minmax\(50vw/u);
  assert.match(source, /@keyframes lecture-image-enter \{[\s\S]*scale\(\.88\)[\s\S]*scale\(1\.025\)/u);
});

test('market chapter uses interactive source-structure charts without invented market shares', () => {
  for (const scene of ['market-01-primary', 'market-02-substitutes', 'market-03-dynamics', 'market-04-position']) {
    assert.match(source, new RegExp(`data-presenter-scene-id="${scene}"`, 'u'));
  }
  assert.match(source, /const marketPlayers = \[/u);
  assert.match(source, /const marketSubstitutes = \[/u);
  assert.match(source, /const marketCapabilities = \[/u);
  assert.match(source, /data-market-focus/u);
  assert.match(source, /data-market-scrubber/u);
  assert.match(source, /scrubber\.addEventListener\('input'/u);
  assert.match(source, /scene\.style\.setProperty\('--market-progress'/u);
  assert.match(source, /data-market-state="idle"/u);
  assert.match(source, /startMarketScene/u);
  assert.match(source, /Качественный вывод исследования/u);
  assert.match(source, /Вместе — ни у кого\./u);
  assert.doesNotMatch(source, /market share|доля рынка|600 000|0,1%|7×/iu);
  assert.doesNotMatch(source, /--value:\.(?:92|66|81|78)/u);
});

test('expanded author scenario has explicit visual scenes instead of empty placeholders', () => {
  for (const scene of [
    'service-navigation-map',
    'service-social-proof',
    'service-artifacts-explained',
    'service-artifact-desktop',
    'service-laws',
    'service-keyboard-concept',
    'service-keyboard-day',
    'service-keyboard-event',
    'service-fast-find',
    'service-share-friends',
    'service-calendar-memory',
    'service-community-curator',
    'service-location-artifact',
    'service-friends-club',
  ]) assert.match(source, new RegExp(`id: '${scene}'|data-presenter-scene-id=\"${scene}\"`, 'u'));
  assert.match(source, /data-presenter-id="friends-club-video"/u);
  assert.match(source, /createFocusInviteQrSvg\(friendsClubUrl\)/u);
  assert.match(source, /friends-club-darya-[a-f0-9]{64}\.mp4/u);
  assert.match(source, /data-visual-state="idle"/u);
  assert.match(source, /activeScene\.hasAttribute\('data-visual-state'\)/u);
});

test('new navigation and joke blocks are explicit runnable presentation scenes', () => {
  assert.match(source, /data-presenter-scene-id="service-navigation-exhibitions"/u);
  assert.match(source, /data-presenter-id="exhibitions-desktop-frame"/u);
  assert.match(source, /data-presenter-scene-id="service-navigation-festivals"/u);
  assert.match(source, /data-presenter-id="festivals-desktop-frame"/u);
  assert.match(source, /data-presenter-id="festivals-mobile-frame"/u);
  assert.match(source, /data-festival-phase="desktop"/u);
  assert.match(source, /activeScene\.dataset\.festivalPhase = 'desktop'/u);
  assert.match(source, /const jokeDatabase = \[/u);
  assert.match(source, /Калининградский прогноз погоды/u);
  assert.match(source, /Куршской косе кабаны/u);
  assert.match(source, /data-tts-required=\{joke\.ttsRequired/u);
  assert.match(source, /id: `joke-db-\$\{String\(index \+ 1\)/u);
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
  assert.match(source, /@keyframes outro-qr-enter \{[\s\S]*scale\(\.84\)[\s\S]*opacity: 1/u);
});

test('all presentation media are immutable Yandex CDN assets and reduced motion is safe', () => {
  const urls = [...source.matchAll(/https:\/\/static\.kenigevents\.ru\/assets\/autopresenter\/scenario-20260730\/[^'"]+/gu)].map((match) => match[0]);
  assert.ok(urls.length >= 10);
  for (const url of urls) {
    assert.match(url, /-(?:[a-f0-9]{16}|[a-f0-9]{64})\.(?:jpg|png|svg|mp3|mp4|wav|webp)$/u);
  }
  assert.match(source, /@media \(prefers-reduced-motion: reduce\)/u);
  assert.match(source, /if \(reduceMotion\.matches\)/u);
  assert.match(source, /\.hero-fragment\.is-active::after \{ display: none; \}/u);
  assert.match(source, /animation-duration: \.01ms !important/u);
});

test('service presentation uses the requested focus preview and explicit input-driven search', () => {
  assert.match(source, /preview-20260730-hero-talk-date-donor-r2/u);
  assert.match(source, /fokus-gruppa\/priglashenie\/#invite=focus-group-2026-announcements/u);
  assert.match(source, /data-presenter-scene-id="service-medallions-desktop"/u);
  assert.match(source, /data-presenter-scene-id="service-medallions-mobile"/u);
  assert.match(source, /data-presenter-scene-id="service-future-celebrity"/u);
  assert.match(source, /data-presenter-scene-id="service-personalization"/u);
  assert.match(source, /data-presenter-scene-id="service-transport-rail"/u);
  assert.match(source, /data-presenter-scene-id="service-transport-bus"/u);
  assert.match(source, /service-search-live/u);
  assert.match(source, /pwa-icon-leather-7015488739e0296f\.png/u);
  assert.match(source, /data-presenter-scene-id="service-comfort"/u);
  assert.match(source, /@keyframes comfort-mark-to-corner/u);
  assert.match(source, /03\.3 · Что нужно человеку/u);
  assert.match(source, /artifactWeekendUrl = 'https:\/\/kenigevents\.ru\/_review\/[^']+\/artefakty\/'/u);
  assert.match(source, /class="wordmark-plain"[\s\S]*>Анонсы</u);
  assert.match(source, /class="wordmark-vector" src=\{announcementsWordmarkUrl\}/u);
  assert.match(source, /class="wordmark-o-glyph"/u);
  assert.match(source, /transform: scaleX\(3\.25\)/u);
  assert.match(source, /triatlon-pokoleniy-kaliningrad-6865\//u);
  assert.match(source, /typeInto\(first, 'Максим, самое время для шутки'/u);
  assert.match(source, /pause\(7000, token\)/u);
  assert.match(source, /мой электронный мозг не создан для шуток/u);
  assert.match(source, /jokeAudio\.play\(\)/u);
  assert.match(source, /errorAudio\.play\(\)/u);
  assert.match(source, /cat-keyboard-unsplash-[a-f0-9]{64}\.webp/u);
  assert.match(source, /data-presenter-id="error-cat-image"/u);
  assert.match(source, /scene\.dataset\.errorPhase = 'second-cue'/u);
  assert.match(source, /scene\.dataset\.errorPhase = 'cat'/u);
  assert.match(source, /const startServiceWordSequence = async/u);
  assert.match(source, /data-service-copy-state="idle"/u);
  assert.match(source, /class="service-word"/u);
  assert.match(source, /words\[index\]\?\.classList\.add\('is-visible'\)/u);
  assert.match(source, /reference4-v8\/search-thin\.svg/u);
  assert.match(source, /<Icon name="share" className="need-icon need-icon--share"/u);
  assert.doesNotMatch(source, /share-network-thin/u);
  assert.match(source, /const medallions = \[[\s\S]*world-ocean-museum[\s\S]*mumod/u);
  assert.match(source, /data-medallion-state="idle"/u);
  assert.match(source, /createFocusInviteQrSvg\(focusInvitationUrl\)/u);
  assert.match(source, /focusNpsUrl = `\$\{focusPreviewBase\}\/segodnya\/`/u);
  assert.match(source, /Татьяна Удовенко[\s\S]*Андрей Бойко[\s\S]*Светлана Соколова/u);
  assert.match(source, /data-focus-phase="meaning"/u);
  assert.match(source, /@font-face \{ font-family: "Cygre"/u);
  assert.match(source, /\/brand\/announcements-o-expanded\.svg/u);
  assert.match(source, /03\.7 · Медальоны/u);
  assert.match(source, /<h2>Умный поиск\.<\/h2>/u);
  assert.match(source, /Присоединяйтесь<br \/>к фокус-группе\./u);
  assert.match(source, /startErrorCue/u);
});
