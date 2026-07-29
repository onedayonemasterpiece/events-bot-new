import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const source = await readFile(
  new URL('../src/pages/internal/presenter-stage/index.astro', import.meta.url),
  'utf8',
);

test('presenter stage defaults to the real live mobile site and switches only on an explicit scene event', () => {
  assert.match(source, /data-presenter-scene="live-site"/u);
  assert.match(source, /data-presenter-scene-id="live-site"/u);
  assert.match(source, /data-presenter-scene-id="outro-qr"/u);
  assert.match(source, /window\.addEventListener\('presenter:scene'/u);
  assert.match(source, /const OUTRO_SCENE_ID = 'outro-qr'/u);
  assert.match(source, /detail && typeof detail\.id === 'string'/u);
  assert.match(source, /liveSiteScene\.hidden = showOutro/u);
  assert.match(source, /outroScene\.hidden = !showOutro/u);
  assert.doesNotMatch(source, /scenario queue|scene editor|scene schema/iu);
});

test('outro is a restrained fullscreen typographic QR scene without presenter chrome', () => {
  const outroStart = source.indexOf('class="outro-scene"');
  const outroEnd = source.indexOf('</section>', outroStart);
  assert.ok(outroStart > 0 && outroEnd > outroStart);
  const outroMarkup = source.slice(outroStart, outroEnd);

  assert.match(outroMarkup, /<h1 id="outro-heading">Как вам\?<\/h1>/u);
  assert.match(outroMarkup, /Оцените событие — это займёт минуту\./u);
  assert.match(outroMarkup, /data-presenter-id="outro-qr-image"/u);
  assert.doesNotMatch(outroMarkup, /phone-shell|stage-status|dashboard|instruction/iu);
  assert.match(source, /\.outro-scene \{[\s\S]*position: absolute;[\s\S]*inset: 0;/u);
  assert.match(source, /--stage-accent: #ff7657/u);
});

test('QR loading, layout stability, entrance motion and reduced motion are explicit', () => {
  assert.match(source, /<link rel="preload" as="image" href=\{outroQrUrl\} fetchpriority="high" \/>/u);
  assert.match(source, /width="1155"\s+height="1155"/u);
  assert.match(source, /loading="eager"/u);
  assert.match(source, /\.outro-qr-wrap \{[\s\S]*aspect-ratio: 1;/u);
  assert.match(source, /@keyframes outro-qr-enter \{[\s\S]*opacity: 0;[\s\S]*scale\(\.82\)[\s\S]*opacity: 1;[\s\S]*scale\(1\)/u);
  assert.match(source, /animation: outro-qr-enter 940ms cubic-bezier\(\.42, 0, \.24, 1\) 90ms both/u);
  assert.match(source, /@media \(prefers-reduced-motion: reduce\)[\s\S]*animation-duration: \.01ms !important/u);
});
