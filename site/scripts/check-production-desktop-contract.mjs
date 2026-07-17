import fs from 'node:fs';
import path from 'node:path';

const distBase = path.resolve('dist');
const previewBuildId = process.env.PREVIEW_BUILD_ID
  || (fs.existsSync(distBase) ? fs.readdirSync(distBase).find((name) => name.startsWith('preview-')) : undefined);
const distRoot = previewBuildId
  ? path.join(distBase, previewBuildId, 'sobytiya')
  : path.join(distBase, 'sobytiya');
if (!fs.existsSync(distRoot)) {
  throw new Error(`${path.relative(process.cwd(), distRoot)} is missing; run npm run build or build:preview first`);
}

const eventFiles = fs.readdirSync(distRoot)
  .map((slug) => ({ slug, file:path.join(distRoot, slug, 'index.html') }))
  .filter(({ file }) => fs.existsSync(file));

const expectedSpecimens = new Map([
  ['blogerskiy-avtobus-splav-na-baydarkah-kaliningrad-6815', ['split', 'split-portrait-or-square-visual']],
  ['spektakl-garazh-kaliningrad-5658', ['editorial', 'editorial-primary-qualified-landscape']],
  ['epidemiya-ognennaya-rukopis-kaliningrad-4671', ['editorial', 'editorial-with-classified-identity-poster']],
  ['zhenitba-i-ekskursiya-zakulise-teatra-kaliningrad-5756', ['editorial', 'editorial-replaces-non-identity-document-with-classified-photo']],
]);

const requiredRoutingFamilies = new Set([
  'split:split-resolution-constrained-landscape',
  ...[...expectedSpecimens.values()].map(([family, reason]) => `${family}:${reason}`),
]);

const counts = new Map();
const failures = [];
for (const { slug, file } of eventFiles) {
  const html = fs.readFileSync(file, 'utf8');
  const family = html.match(/data-desktop-family="([^"]+)"/)?.[1];
  const reason = html.match(/data-presentation-reason="([^"]+)"/)?.[1];
  const surface = html.match(/data-event-surface="([^"]+)"/)?.[1];
  const mobileRevision = html.match(/data-mobile-review-revision="([^"]+)"/)?.[1];
  const mobileVariant = html.match(/data-mobile-review-variant="([^"]+)"/)?.[1];
  const mobileParallax = html.match(/data-mobile-parallax-profile="([^"]+)"/)?.[1];
  const key = `${family || 'missing'}:${reason || 'missing'}`;
  counts.set(key, (counts.get(key) || 0) + 1);

  if (!html.includes('data-desktop-clean-event')) failures.push(`${slug}: accepted desktop component is missing`);
  if (!['editorial', 'split'].includes(family)) failures.push(`${slug}: unsupported desktop family ${family || 'missing'}`);
  if (surface !== 'production') failures.push(`${slug}: desktop surface is ${surface || 'missing'}, expected production`);
  if (!html.includes('data-production-mobile-event')) failures.push(`${slug}: accepted mobile fallback is missing`);
  if (mobileRevision !== 'v4') failures.push(`${slug}: mobile revision changed from v4 to ${mobileRevision || 'missing'}`);
  if (mobileVariant !== 'accepted-v8') failures.push(`${slug}: mobile variant changed from accepted-v8 to ${mobileVariant || 'missing'}`);
  if (mobileParallax !== 'photo-continuous-crop') failures.push(`${slug}: mobile parallax changed from photo-continuous-crop to ${mobileParallax || 'missing'}`);
  if (html.includes('data-lab-media-treatment="document-natural"')) failures.push(`${slug}: related cards regressed to unequal document-natural media heights`);
  const expected = expectedSpecimens.get(slug);
  if (expected && (family !== expected[0] || reason !== expected[1])) {
    failures.push(`${slug}: routed to ${family}/${reason}, expected ${expected[0]}/${expected[1]}`);
  }
  if (slug === 'spektakl-garazh-kaliningrad-5658' && !html.includes('/p/thumb/v1/')) {
    failures.push(`${slug}: accepted compact rail lost its immutable thumbnail derivatives`);
  }
  const desktopHtml = html.slice(html.indexOf('<main class="desktop-clean-event'), html.indexOf('</main>', html.indexOf('<main class="desktop-clean-event')) + 7);
  const mobileHtml = html.slice(html.indexOf('<main class="mobile-event-production'), html.indexOf('</main>', html.indexOf('<main class="mobile-event-production')) + 7);
  if (slug === 'zhenitba-i-ekskursiya-zakulise-teatra-kaliningrad-5756') {
    if (!desktopHtml.includes('data-selected-media-policy="visual_only"')) failures.push(`${slug}: classified horizontal photo was not selected as the desktop hero`);
    if (!desktopHtml.includes('data-desktop-gallery-fit="contain"')) failures.push(`${slug}: non-photo document is no longer protected by contain in the desktop gallery`);
  }
  if (slug === 'epidemiya-ognennaya-rukopis-kaliningrad-4671') {
    for (const marker of [
      'data-kaup-transport',
      'data-kaup-official-transfer',
      'data-kaup-public-bus',
      '600 ₽ туда и обратно',
      'data-kaup-bus-origin',
      'Калининградский автовокзал',
      'ул. Железнодорожная, 7',
      'data-kaup-last-mile',
      'Открыть пеший маршрут',
      'data-kaup-car-route',
      'Открыть маршрут',
      'rtt=pd',
    ]) {
      if (!desktopHtml.includes(marker)) failures.push(`${slug}: KAUP desktop transport is missing ${marker}`);
    }
    for (const rejectedMarker of ['kaup-transport__map-mark', 'kaup-transport__route', '>54.8781']) {
      if (desktopHtml.includes(rejectedMarker)) failures.push(`${slug}: KAUP transport still exposes rejected map decoration ${rejectedMarker}`);
    }
    const originIndex = desktopHtml.indexOf('data-kaup-bus-origin');
    const scheduleIndex = desktopHtml.indexOf('<ol', originIndex);
    const lastMileIndex = desktopHtml.indexOf('data-kaup-last-mile');
    const returnWarningIndex = desktopHtml.indexOf('kaup-transport__warning--strong');
    const carIndex = desktopHtml.indexOf('data-kaup-car-route');
    if (!(originIndex >= 0 && scheduleIndex > originIndex && lastMileIndex > scheduleIndex && returnWarningIndex > lastMileIndex && carIndex > returnWarningIndex)) {
      failures.push(`${slug}: KAUP travel flow is not ordered as origin → schedule → last mile → return warning → car`);
    }
    for (const marker of ['data-kaup-compact', 'Трансфер · 600 ₽ туда и обратно', 'Точки посадки и условия', 'Калининградский автовокзал', 'ул. Железнодорожная, 7', 'Автобус № 119']) {
      if (!mobileHtml.includes(marker)) failures.push(`${slug}: compact mobile KAUP journey is missing ${marker}`);
    }
    if (!mobileHtml.includes('<details class="kaup-transport__transfer-details"')) {
      failures.push(`${slug}: compact mobile KAUP transfer details are not progressively disclosed`);
    }
  }
  if (slug === '15-avgusta-v-yantar-holl-spektakl-papa-svetlogorsk-3103') {
    for (const train of ['data-train-number="6726"', 'data-train-number="6728"']) {
      if (!desktopHtml.includes(train)) failures.push(`${slug}: safe explicit-duration evening return is missing ${train}`);
    }
    if (desktopHtml.includes('data-train-number="6724"')) failures.push(`${slug}: unsafe 19:57 return is still suggested before the venue-access buffer`);
    if (!desktopHtml.includes('data-return-access-minutes="30"')) failures.push(`${slug}: Yantar Hall return-access buffer is missing`);
    if (!desktopHtml.includes('около 15 минут пешком до Светлогорска-2')) failures.push(`${slug}: Yantar Hall walk/exit rationale is missing`);
    if (desktopHtml.includes('Первый поезд 16 августа')) failures.push(`${slug}: desktop still suggests waiting for a next-morning train despite evening returns`);
  }
  if (slug === 'myuzikl-alye-parusa-kaliningrad-4783') {
    const efficientItems = desktopHtml.match(/data-efficient-viewer-item/g)?.length || 0;
    if (!desktopHtml.includes('data-split-efficient-viewer="true"') || efficientItems !== 7) {
      failures.push(`${slug}: quality-admitted grouped multi-portrait viewer should contain 7 items, got ${efficientItems}`);
    }
    if (!desktopHtml.includes('Показаны 7 из 12 изображений в лучшем качестве')) failures.push(`${slug}: grouped viewer does not disclose quality filtering`);
  }
  if (slug === 'kinopokaz-fatalnaya-chechetka-kaliningrad-6851') {
    if (!desktopHtml.includes('data-desktop-phone-copy') || !desktopHtml.includes('+7 911 868-89-55')) {
      failures.push(`${slug}: desktop phone CTA does not expose the copyable number`);
    }
    if (!desktopHtml.includes('data-desktop-action-panel') || !desktopHtml.includes('data-primary-action-kind="phone"')) {
      failures.push(`${slug}: phone action panel lacks the component-responsive geometry contract`);
    }
    if (desktopHtml.includes('data-phone-copy-status')) {
      failures.push(`${slug}: phone action panel still contains the layout-shifting status row`);
    }
  }
  if (html.includes('Отзывов недостаточно, чтобы уверенно выделить повторяющиеся впечатления.')) {
    failures.push(`${slug}: insufficient-feedback placeholder is still rendered`);
  }
  if (html.includes('<div class="hero-gallery"') && !html.includes('data-desktop-gallery-dismiss="true"')) {
    failures.push(`${slug}: desktop gallery click-to-close contract is missing`);
  }
}

for (const slug of expectedSpecimens.keys()) {
  if (!eventFiles.some((item) => item.slug === slug)) failures.push(`${slug}: required real-event specimen is absent`);
}

for (const key of requiredRoutingFamilies) {
  if (!counts.has(key)) failures.push(`fresh event set has no representative for ${key}`);
}

if (failures.length) {
  console.error(`Production desktop contract failed (${failures.length}):`);
  failures.forEach((failure) => console.error(`- ${failure}`));
  process.exit(1);
}

console.log(`Production desktop contract passed for ${eventFiles.length} event pages.`);
for (const [key, count] of [...counts].sort((left, right) => left[0].localeCompare(right[0]))) {
  console.log(`- ${key}: ${count}`);
}
