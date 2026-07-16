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
  ['kontsert-festival-pianissimo-maksim-miloslavskiy-kaliningrad-5294', ['split', 'split-resolution-constrained-landscape']],
  ['blogerskiy-avtobus-splav-na-baydarkah-kaliningrad-6815', ['split', 'split-portrait-or-square-visual']],
  ['spektakl-garazh-kaliningrad-5658', ['editorial', 'editorial-primary-qualified-landscape']],
  ['epidemiya-ognennaya-rukopis-kaliningrad-4671', ['editorial', 'editorial-with-classified-identity-poster']],
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
}

for (const slug of expectedSpecimens.keys()) {
  if (!eventFiles.some((item) => item.slug === slug)) failures.push(`${slug}: required real-event specimen is absent`);
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
