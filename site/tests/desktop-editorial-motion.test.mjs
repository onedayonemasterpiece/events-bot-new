import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import {
  buildDesktopEventPresentation,
  resolveEditorialSideMotionGeometry,
} from '../src/lib/desktopEventPresentation.ts';

const editorialAsset = (src) => ({
  src,
  width:1600,
  height:900,
  image_text_mode:'visual_only',
  media_semantic_status:'classified',
  media_role:'event_photo',
  safe_crop:true,
  recommended_hero_fit:'cover',
  quality_score:14,
});

const editorialEvent = (assets) => ({
  id:9001,
  image_url:assets[0].src,
  image_text_mode:'visual_only',
  image_assets:assets,
});

test('one-photo and multi-photo Editorial presentations share continuous motion', () => {
  const onePhoto = buildDesktopEventPresentation(editorialEvent([
    editorialAsset('one.webp'),
  ]));
  const multiPhoto = buildDesktopEventPresentation(editorialEvent([
    editorialAsset('one.webp'),
    editorialAsset('two.webp'),
  ]));

  assert.equal(onePhoto.candidate, 'editorial');
  assert.equal(multiPhoto.candidate, 'editorial');
  assert.equal(onePhoto.editorialMotion, multiPhoto.editorialMotion);
  assert.equal(onePhoto.editorialMotion, 'continuous');
  assert.equal(onePhoto.editorialRail, false);
  assert.equal(multiPhoto.editorialRail, true);
});

test('Editorial CTA geometry remains active when the optional rail is absent', () => {
  const onePhoto = resolveEditorialSideMotionGeometry({
    holdTop:640,
    stickyTop:81,
  });
  const multiPhoto = resolveEditorialSideMotionGeometry({
    holdTop:640,
    stickyTop:81,
    railBottom:140,
  });

  assert.deepEqual(onePhoto, {
    holdTop:640,
    dockTop:82,
    maxTravel:558,
    stickyTop:81,
  });
  assert.deepEqual(multiPhoto, {
    holdTop:640,
    dockTop:152,
    maxTravel:488,
    stickyTop:81,
  });
  assert.ok(onePhoto.maxTravel > 0, 'rail-less Editorial CTA must enter the shared motion state machine');
});

test('Desktop Editorial runtime does not gate layout or motion updates on rail presence', async () => {
  const page = await readFile(new URL('../src/components/DesktopEventPage.astro', import.meta.url), 'utf8');
  const motion = page.slice(
    page.indexOf('const updateEditorialSideMotion'),
    page.indexOf('const measureEditorialCrop'),
  );

  assert.match(motion, /railBottom:editorialRail\?\.getBoundingClientRect\(\)\.bottom/u);
  assert.doesNotMatch(motion, /!editorialRail/u);
  assert.match(motion, /editorialSide\.dataset\.ctaPhase = phase/u);
  assert.match(motion, /applyEditorialReleaseHeight\(editorialSideGeometry\.maxTravel\)/u);
});
