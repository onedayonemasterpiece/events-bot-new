import fs from 'node:fs';
import path from 'node:path';

const distBase = path.resolve('dist');
const productionFamily = ['production', 'secret_candidate'].includes(String(process.env.PUBLIC_SITE_MODE || ''));
// Kaggle historically exports PREVIEW_BUILD_ID for every static-site kernel.
// A production-profile checker must still inspect the root-form dist tree.
const previewBuildId = productionFamily
  ? undefined
  : process.env.PREVIEW_BUILD_ID
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

// Slugs are editorial data and may change after Smart Update. Event ids are the
// stable identities for the accepted real-event specimens.
const expectedSpecimens = new Map([
  [6815, ['split', 'split-low-resolution-portrait-viewer']],
  [5658, ['editorial', 'editorial-primary-qualified-landscape']],
  [4671, ['editorial', 'editorial-with-classified-identity-poster']],
  // The source-consistent classified event photo owns Editorial; the
  // non-identity document remains contained rather than becoming the hero.
  // Smart Update repaired 5756's canonical primary from the rejected document
  // to an event photo. Both source-consistent states must stay Editorial: the
  // old occurrence replaces the document, while the repaired occurrence may
  // promote a stronger landscape photo from the same classified media set.
  [5756, ['editorial', [
    'editorial-replaces-non-identity-document-with-classified-photo',
    'editorial-promotes-qualified-landscape-photo',
  ]]],
]);

const templateContract = JSON.parse(fs.readFileSync(path.resolve('src/data/eventTemplateContract.json'), 'utf8'));

const counts = new Map();
const failures = [];
for (const { slug, file } of eventFiles) {
  const eventId = Number(slug.match(/-(\d+)$/u)?.[1] || 0);
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
  if (!html.includes(`data-event-template-contract="${templateContract.contract_id}"`)) failures.push(`${slug}: accepted template contract marker is missing`);
  if (!html.includes(`data-event-template-source="${templateContract.accepted_source_sha}"`)) failures.push(`${slug}: accepted template source marker is missing`);
  if (!['editorial', 'split'].includes(family)) failures.push(`${slug}: unsupported desktop family ${family || 'missing'}`);
  if (surface !== 'production') failures.push(`${slug}: desktop surface is ${surface || 'missing'}, expected production`);
  if (!html.includes('data-production-mobile-event')) failures.push(`${slug}: accepted mobile fallback is missing`);
  if (mobileRevision !== 'v4') failures.push(`${slug}: mobile revision changed from v4 to ${mobileRevision || 'missing'}`);
  if (mobileVariant !== 'accepted-v8') failures.push(`${slug}: mobile variant changed from accepted-v8 to ${mobileVariant || 'missing'}`);
  if (mobileParallax !== 'photo-continuous-crop') failures.push(`${slug}: mobile parallax changed from photo-continuous-crop to ${mobileParallax || 'missing'}`);
  if (html.includes('data-lab-media-treatment="document-natural"')) failures.push(`${slug}: related cards regressed to unequal document-natural media heights`);
  const expected = expectedSpecimens.get(eventId);
  const expectedReasons = expected ? (Array.isArray(expected[1]) ? expected[1] : [expected[1]]) : [];
  if (expected && (family !== expected[0] || !expectedReasons.includes(reason))) {
    failures.push(`${slug}: routed to ${family}/${reason}, expected ${expected[0]}/${expectedReasons.join('|')}`);
  }
  if (eventId === 5658 && !html.includes('/p/thumb/v1/')) {
    failures.push(`${slug}: accepted compact rail lost its immutable thumbnail derivatives`);
  }
  const desktopHtml = html.slice(html.indexOf('<main class="desktop-clean-event'), html.indexOf('</main>', html.indexOf('<main class="desktop-clean-event')) + 7);
  const mobileHtml = html.slice(html.indexOf('<main class="mobile-event-production'), html.indexOf('</main>', html.indexOf('<main class="mobile-event-production')) + 7);
  for (const imageTag of desktopHtml.match(/<img class="hero-gallery__image"[^>]+>/gu) || []) {
    const textMode = imageTag.match(/data-image-text-mode="([^"]+)"/u)?.[1];
    const fit = imageTag.match(/data-desktop-gallery-fit="([^"]+)"/u)?.[1];
    const protectedFit = imageTag.match(/data-protected-crop-fit="([^"]+)"/u)?.[1];
    const protectedReason = imageTag.match(/data-protected-crop-reason="([^"]+)"/u)?.[1];
    const semanticStatus = imageTag.match(/data-media-semantic-status="([^"]+)"/u)?.[1];
    const mediaRole = imageTag.match(/data-media-role="([^"]+)"/u)?.[1];
    if (!['contain', 'cover'].includes(fit)) failures.push(`${slug}: desktop gallery has unsupported fit ${fit || 'missing'}`);
    if (fit !== protectedFit) failures.push(`${slug}: desktop gallery fit ${fit || 'missing'} disagrees with protected fit ${protectedFit || 'missing'}`);
    if (textMode === 'visual_only' && fit !== 'cover') failures.push(`${slug}: desktop gallery non-OCR image must fill with cover`);
    if (textMode !== 'visual_only' && fit !== 'contain') failures.push(`${slug}: desktop gallery document/unknown image must contain`);
    if (fit === 'cover' && !['protected_regions_fit', 'visual_only_gallery_fill'].includes(protectedReason)) failures.push(`${slug}: desktop gallery cover lacks the non-OCR fill contract`);
    if (fit === 'contain' && !protectedReason) failures.push(`${slug}: desktop gallery contain lacks a fail-closed reason`);
  }
  const selectedMode = desktopHtml.match(/data-selected-media-policy="([^"]+)"/u)?.[1];
  const selectedRole = desktopHtml.match(/data-selected-media-role="([^"]+)"/u)?.[1];
  const selectedSemanticStatus = desktopHtml.match(/data-selected-media-semantic-status="([^"]+)"/u)?.[1];
  const heroFit = desktopHtml.match(/data-hero-render-fit="([^"]+)"/u)?.[1];
  const heroTag = desktopHtml.match(/<img class="desktop-prototype__media-image"[^>]*data-clean-hero-image[^>]+>/u)?.[0] || '';
  const heroProtectedFit = heroTag.match(/data-protected-crop-fit="([^"]+)"/u)?.[1];
  const heroProtectedReason = heroTag.match(/data-protected-crop-reason="([^"]+)"/u)?.[1];
  if (!['contain', 'cover'].includes(heroFit)) failures.push(`${slug}: selected hero has unsupported fit ${heroFit || 'missing'}`);
  if (heroTag && heroFit !== heroProtectedFit) failures.push(`${slug}: selected hero fit ${heroFit || 'missing'} disagrees with protected fit ${heroProtectedFit || 'missing'}`);
  if (selectedMode === 'visual_only' && heroFit !== 'cover') failures.push(`${slug}: selected non-OCR hero must fill with cover`);
  if (selectedMode !== 'visual_only' && heroFit !== 'contain') failures.push(`${slug}: selected document/unknown hero must contain`);
  if (heroFit === 'cover' && !['protected_regions_fit', 'visual_only_gallery_fill'].includes(heroProtectedReason)) failures.push(`${slug}: selected hero cover lacks the non-OCR fill contract`);
  if (heroTag && heroFit === 'contain' && !heroProtectedReason) failures.push(`${slug}: selected hero contain lacks a fail-closed reason`);
  if (eventId === 5658 && (!desktopHtml.includes('data-editorial-crop="bounded-cover"') || heroFit !== 'cover' || heroProtectedReason !== 'visual_only_gallery_fill')) {
    failures.push(`${slug}: wide no-OCR photograph must keep editorial routing and fill the responsive hero`);
  }
  if (eventId === 5756) {
    if (!desktopHtml.includes('data-selected-media-policy="visual_only"')) failures.push(`${slug}: classified horizontal photo was not selected as the desktop hero`);
    const galleryTags = desktopHtml.match(/<img class="hero-gallery__image"[^>]+>/gu) || [];
    const galleryContainsDocument = galleryTags.some((tag) => !tag.includes('data-image-text-mode="visual_only"'));
    if (galleryContainsDocument && !desktopHtml.includes('data-desktop-gallery-fit="contain"')) {
      failures.push(`${slug}: non-photo document is no longer protected by contain in the desktop gallery`);
    }
  }
  if (eventId === 4671) {
    for (const marker of [
      'data-kaup-transport',
      'data-kaup-official-transfer',
      'data-kaup-public-bus',
      '600 ₽ туда и обратно',
      'data-kaup-bus-origin',
      'Северный вокзал',
      'Северный вокзал · Калининград',
      'data-kaup-last-mile',
      'Открыть пеший маршрут',
      'data-kaup-car-route',
      'Открыть маршрут',
      'rtt=pd',
    ]) {
      if (!desktopHtml.includes(marker)) failures.push(`${slug}: KAUP desktop transport is missing ${marker}`);
    }
    for (const rejectedMarker of ['kaup-transport__map-mark', 'kaup-transport__route', '>54.8781', 'Калининградский автовокзал', 'ул. Железнодорожная, 7']) {
      if (desktopHtml.includes(rejectedMarker)) failures.push(`${slug}: KAUP transport still exposes rejected map decoration ${rejectedMarker}`);
    }
    const originIndex = desktopHtml.indexOf('data-kaup-bus-origin');
    const scheduleIndex = desktopHtml.indexOf('data-transport-baseline', originIndex);
    const lastMileIndex = desktopHtml.indexOf('data-kaup-last-mile');
    const returnWarningIndex = desktopHtml.indexOf('kaup-transport__warning--strong');
    const carIndex = desktopHtml.indexOf('data-kaup-car-route');
    if (!(originIndex >= 0 && scheduleIndex > originIndex && lastMileIndex > scheduleIndex && returnWarningIndex > lastMileIndex && carIndex > returnWarningIndex)) {
      failures.push(`${slug}: KAUP travel flow is not ordered as origin → schedule → last mile → return warning → car`);
    }
    for (const marker of ['data-kaup-compact', 'Официальный трансфер · 600 ₽ туда и обратно', 'Точки посадки и условия', 'Северный вокзал · Калининград', 'Автобус № 119']) {
      if (!mobileHtml.includes(marker)) failures.push(`${slug}: compact mobile KAUP journey is missing ${marker}`);
    }
    for (const [surface, surfaceHtml] of [['desktop', desktopHtml], ['mobile', mobileHtml]]) {
      for (const [terminal, north, romanovo, venue] of [
        ['16:30', '16:45', '17:35', '18:28'],
        ['17:55', '18:10', '19:00', '19:53'],
      ]) {
        // Accepted A uses table cells while the reviewed B/C arms use route
        // strips/queue rows. Assert the source-parity chain without coupling
        // release acceptance to the rejected compact-list tag names.
        const rowPattern = new RegExp(`data-terminal-departure="${terminal}"[^>]*>≈?\\s*${north}<\\/(?:td|strong)>[\\s\\S]{0,700}?${romanovo}[\\s\\S]{0,700}?${venue}`, 'u');
        if (!rowPattern.test(surfaceHtml)) failures.push(`${slug}: ${surface} KAUP route must keep terminal ${terminal}, estimate North ${north}, Romanovo ${romanovo} and venue ${venue}`);
      }
      if (surfaceHtml.includes('Калининградский автовокзал') || surfaceHtml.includes('ул. Железнодорожная, 7')) {
        failures.push(`${slug}: ${surface} KAUP route regressed from North boarding to the bus terminal`);
      }
    }
    if (!mobileHtml.includes('<details class="kaup-transport__transfer-details"')) {
      failures.push(`${slug}: compact mobile KAUP transfer details are not progressively disclosed`);
    }
  }
  if (eventId === 3103) {
    for (const train of ['data-train-number="6726"', 'data-train-number="6728"']) {
      if (!desktopHtml.includes(train)) failures.push(`${slug}: safe explicit-duration evening return is missing ${train}`);
    }
    if (desktopHtml.includes('data-train-number="6724"')) failures.push(`${slug}: unsafe 19:57 return is still suggested before the venue-access buffer`);
    if (!desktopHtml.includes('data-return-access-minutes="30"')) failures.push(`${slug}: Yantar Hall return-access buffer is missing`);
    if (!desktopHtml.includes('около 15 минут пешком до Светлогорска-2')) failures.push(`${slug}: Yantar Hall walk/exit rationale is missing`);
    if (desktopHtml.includes('Первый поезд 16 августа')) failures.push(`${slug}: desktop still suggests waiting for a next-morning train despite evening returns`);
  }
  if (eventId === 4783) {
    const efficientItems = desktopHtml.match(/data-efficient-viewer-item/g)?.length || 0;
    if (!desktopHtml.includes('data-split-efficient-viewer="true"') || efficientItems !== 7) {
      failures.push(`${slug}: quality-admitted grouped multi-portrait viewer should contain 7 items, got ${efficientItems}`);
    }
    if (!desktopHtml.includes('Показаны 7 из 12 изображений в лучшем качестве')) failures.push(`${slug}: grouped viewer does not disclose quality filtering`);
  }
  if (eventId === 6851) {
    if (!desktopHtml.includes('data-desktop-phone-copy') || !desktopHtml.includes('data-phone-display="+7 911 868-89-55"') || !desktopHtml.includes('>Показать телефон</span>')) {
      failures.push(`${slug}: desktop phone CTA does not preserve branded reveal-and-copy semantics`);
    }
    if (!desktopHtml.includes('data-desktop-action-panel') || !desktopHtml.includes('data-primary-action-kind="phone"')) {
      failures.push(`${slug}: phone action panel lacks the component-responsive geometry contract`);
    }
    if (desktopHtml.includes('data-phone-copy-status')) {
      failures.push(`${slug}: phone action panel still contains the layout-shifting status row`);
    }
    const phoneControl = desktopHtml.match(/<button[^>]*data-desktop-phone-copy[\s\S]*?<\/button>/u)?.[0] || '';
    if (phoneControl.includes('icon--phone')) failures.push(`${slug}: branded desktop phone CTA still has the rejected phone icon`);
    if (!phoneControl.includes('icon--copy') || !phoneControl.includes('data-desktop-phone-status') || !phoneControl.includes('data-desktop-phone-toast')) {
      failures.push(`${slug}: branded desktop phone CTA lacks copy icon plus visible and live feedback`);
    }
    if (phoneControl.includes('Скопировать номер</small>')) failures.push(`${slug}: desktop phone CTA restored the rejected helper line`);
  }
  if (html.includes('Отзывов недостаточно, чтобы уверенно выделить повторяющиеся впечатления.')) {
    failures.push(`${slug}: insufficient-feedback placeholder is still rendered`);
  }
  if (html.includes('<div class="hero-gallery"') && !html.includes('data-desktop-gallery-dismiss="true"')) {
    failures.push(`${slug}: desktop gallery click-to-close contract is missing`);
  }
}

for (const [eventId, [family, reasons]] of expectedSpecimens) {
  if (!eventFiles.some(({ slug }) => Number(slug.match(/-(\d+)$/u)?.[1] || 0) === eventId)) continue;
  const acceptedReasons = Array.isArray(reasons) ? reasons : [reasons];
  if (!acceptedReasons.some((reason) => counts.has(`${family}:${reason}`))) {
    failures.push(`fresh event set has no representative for ${family}:${acceptedReasons.join('|')}`);
  }
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
