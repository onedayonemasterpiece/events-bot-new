import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import eventsPayload from '../src/data/preview-events.json' with { type:'json' };
import relatedPayload from '../src/data/preview-related.json' with { type:'json' };
import overrides from '../src/data/listingMediaOverrides.json' with { type:'json' };
import {
  relatedCardPrimaryImageAsset,
  resolveRelatedCardMediaTreatment,
} from '../src/lib/relatedCardLayout.mjs';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath, encoding = 'utf8') => readFile(path.join(siteRoot, relativePath), encoding);
const sha256 = (buffer) => createHash('sha256').update(buffer).digest('hex');

const events = eventsPayload.events || [];
const goblin = events.find((event) => event.title === 'Гоблинское сражение');
const acceptanceRelated = relatedPayload.related?.['6529']?.chain || [];

test('event 6529 Goblin recommendation uses a source-keyed reviewed 5:4 asset', async () => {
  assert.equal(goblin?.id, 6835);
  assert.ok(acceptanceRelated.some((item) => Number(item.event_id) === goblin.id),
    'the exact Goblin battle card must remain an event 6529 recommendation canary');

  const reviewed = relatedCardPrimaryImageAsset({
    ...goblin,
    image_text_mode:'unknown',
    image_media_role:'unknown_document',
    image_assets:goblin.image_assets?.map((asset) => ({
      ...asset,
      image_text_mode:'unknown',
      media_role:'unknown_document',
      media_semantic_status:'error',
      safe_crop:false,
    })),
  });
  assert.equal(reviewed.src, '/assets/card-media/goblin-battle-reviewed-5x4.webp');
  assert.equal(reviewed.original_src, goblin.image_url);
  assert.equal(reviewed.image_text_mode, 'visual_only');
  assert.equal(reviewed.media_role, 'event_photo');
  assert.equal(reviewed.listing_crop_evidence, 'source-still-reviewed-no-ocr-20260723');
  assert.deepEqual([reviewed.width, reviewed.height], [1000, 800]);

  const decision = resolveRelatedCardMediaTreatment(goblin, 5 / 4);
  assert.equal(decision.mediaKind, 'visual');
  assert.equal(decision.fit, 'cover');
  assert.equal(decision.coverCrop, 0);

  const metadata = JSON.parse(await read('public/assets/card-media/goblin-battle-reviewed-5x4.metadata.json'));
  const derivative = await read('public/assets/card-media/goblin-battle-reviewed-5x4.webp', null);
  assert.equal(sha256(derivative), metadata.output_sha256);
  assert.deepEqual(metadata.crop_box_px, [0, 245, 691, 798]);
  assert.match(metadata.diagnosis, /no baked bands and no OCR/u);
  const override = overrides.items.find((item) => item.sourceSrc === goblin.image_url);
  assert.equal(override?.sourceSha256, metadata.source_sha256);
});

test('reviewed source correction cannot weaken OCR or spill to another source', () => {
  const ocr = {
    id:999001,
    image_url:'https://static.kenigevents.ru/p/example-ocr.webp',
    image_text_mode:'ocr_text',
    image_assets:[{
      src:'https://static.kenigevents.ru/p/example-ocr.webp',
      width:800,
      height:1000,
      image_text_mode:'ocr_text',
      media_role:'event_identity_poster',
      media_semantic_status:'classified',
    }],
  };
  const untouched = relatedCardPrimaryImageAsset(ocr);
  assert.equal(untouched.src, ocr.image_url);
  assert.equal(untouched.image_text_mode, 'ocr_text');
  const decision = resolveRelatedCardMediaTreatment(ocr, 800 / 1000);
  assert.equal(decision.mediaKind, 'document');
  assert.equal(decision.coverCrop, 0);
});

test('reviewed no-OCR portrait 6821 fills recommendation cards despite stale classifier error', () => {
  const sourceSrc = 'https://static.kenigevents.ru/p/dh16/1a/1a120c130c4b046304e504c4078d0f8c0fc00a401a403240e2c0c98249034403.webp';
  const event = {
    id:6821,
    image_url:sourceSrc,
    image_text_mode:'unknown',
    image_assets:[{
      src:sourceSrc,
      width:861,
      height:1024,
      image_text_mode:'unknown',
      media_role:'unknown_document',
      media_semantic_status:'error',
      safe_crop:false,
    }],
  };
  const reviewed = relatedCardPrimaryImageAsset(event);
  assert.equal(reviewed.image_text_mode, 'visual_only');
  assert.equal(reviewed.listing_crop_evidence, 'source-still-reviewed-no-ocr-20260726');
  assert.equal(reviewed.recommended_object_position, '50% 40%');
  const decision = resolveRelatedCardMediaTreatment(event, 5 / 4);
  assert.equal(decision.mediaKind, 'visual');
  assert.equal(decision.mediaTreatment, 'visual-cover');
  assert.equal(decision.fit, 'cover');
});

test('server and runtime card branches use OCR mode so every non-OCR image covers', async () => {
  const card = await read('src/components/EventCard.astro');
  const layout = await read('src/layouts/EventLayout.astro');

  assert.match(card, /imageTextMode !== 'visual_only'/u);
  assert.match(card, /data-card-media-review=\{primaryImageAsset\?\.listing_crop_evidence\}/u);
  assert.match(card, /'is-image-loading'/u);
  assert.match(card, /const imageLoadHandler = `const shell=this\.closest\('\.event-card__media-shell'\);/u);
  assert.match(card, /shell\?\.classList\.remove\('is-image-loading'\);shell\?\.classList\.add\('is-image-loaded'\)/u);
  assert.match(layout, /data\.image_text_mode !== 'visual_only'/u);
  assert.match(layout, /documentMedia \? 'contain' : 'cover'/u);
  assert.match(layout, /\.event-card__media-shell\.is-image-loaded \.event-card__image-fallback \{ display: none; \}/u);
  assert.match(layout, /KenigEventsRelatedCardPrimaryImageAsset/u);
  assert.match(layout, /reviewedAsset\?\.listing_crop_evidence/u);
});

test('desktop tag uses cleaned blank leather, live lockup, complete edging and terracotta fallback', async () => {
  const layout = await read('src/layouts/EventLayout.astro');
  const metadata = JSON.parse(await read('public/assets/ui/desktop-head-leather-r5.metadata.json'));
  const asset = await read('public/assets/ui/desktop-head-leather-r5.webp', null);
  const master = await read('src/assets/ui/desktop-head-leather-r5-master.webp', null);

  assert.deepEqual(metadata.output_dimensions_px, [960, 352]);
  assert.equal(metadata.output_aspect_ratio, '30:11');
  assert.equal(metadata.chosen_source, 'docs/features/static-site-pages/references/head-skin-desctop (2).png');
  assert.equal(metadata.rejected_source, 'docs/features/static-site-pages/references/head-skin-desctop (1).png');
  assert.match(metadata.selection, /without baked raster lettering[\s\S]*live AnnouncementsLockup[\s\S]*sharp/u);
  assert.match(metadata.cleanup_policy, /stitched outer edge[\s\S]*offset dark backing[\s\S]*pale horizontal extraction remnant/u);
  assert.match(metadata.alpha_mask, /rounded-rectangle silhouette[\s\S]*no hard clipping/u);
  assert.equal(sha256(asset), metadata.output_sha256);
  assert.equal(sha256(master), metadata.master_sha256);

  assert.match(layout, /--desktop-brand-tag-skin:url\('\$\{withBase\('\/assets\/ui\/desktop-head-leather-r5\.webp'\)\}'\)/u);
  assert.match(layout, /<AnnouncementsLockup variant="desktop" class="site-header__desktop-lockup" \/>/u);
  assert.match(layout, /\.site-header__brand-tag,\s*\.hero-gallery__brand\s*\{[\s\S]*background-color:#98401f;[\s\S]*background-image:/u);
  assert.match(layout, /linear-gradient\(90deg,[\s\S]*rgb\(152,64,31\) 0,[\s\S]*rgba\(152,64,31,.72\) 1px,[\s\S]*transparent 3px,[\s\S]*transparent calc\(100% - 3px\)[\s\S]*var\(--desktop-brand-tag-skin\)/u);
  assert.match(layout, /background-size:100% 100%,100% 100%/u);
  assert.match(layout, /border:1px solid transparent/u);
  assert.doesNotMatch(layout, /border:1px solid rgba\(104,39,19,.34\)/u);
  assert.match(layout, /0 18px 32px rgba\(68,30,16,.13\)/u);
});
