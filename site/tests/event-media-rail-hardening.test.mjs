import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');

test('EventMediaRail normalizes displayable media, srcset candidates and rejected input census', async () => {
  const source = await read('src/components/EventMediaRail.astro');

  assert.match(source, /const normalizedMediaUrl = \(value: string \| null \| undefined\) =>/u);
  assert.match(source, /if \(\/\^\(\?:data\|blob\):\/iu\.test\(raw\)\) return raw;/u);
  assert.match(source, /return eventImageUrl\(raw\) \|\| raw;/u);
  assert.match(source, /const normalizedSrcset = \(value: string \| null \| undefined\) =>/u);
  assert.match(source, /const candidates = raw\.split\(','\)\.map/u);
  assert.match(source, /\(\\S\+\)\(\?:\\s\+\(\\d\+\(\?:\\\.\\d\+\)\?\[wx\]\)\)\?/u);
  assert.match(source, /return candidates\.length \? candidates\.join\(', '\) : undefined;/u);
  assert.match(source, /const normalizedThumbnailSources = \(asset: EventImageAsset\)/u);
  assert.match(source, /src: normalizedMediaUrl\(item\.src\)/u);
  assert.match(source, /\.filter\(\(item\): item is \{ src: string; width: number; height: number \} => Boolean\(item\.src && item\.width && item\.height\)\)/u);
  assert.match(source, /const thumbnailSrc = normalizedMediaUrl\(item\.thumbnailSrc\) \|\| src;/u);
  assert.match(source, /thumbnailSrcset: normalizedSrcset\(item\.thumbnailSrcset\)/u);
  assert.match(source, /const normalizedItems = normalizedCandidates\.filter\(\(item\) => Boolean\(item\.src && item\.thumbnailSrc\)\);/u);
  assert.match(source, /const rejectedCount = normalizedCandidates\.length - normalizedItems\.length;/u);
  assert.match(source, /data-event-media-rail-input-total=\{normalizedCandidates\.length\}/u);
  assert.match(source, /data-event-media-rail-total=\{normalizedItems\.length\}/u);
  assert.match(source, /data-event-media-rail-rejected=\{rejectedCount \|\| undefined\}/u);
});

test('resolved rail media never upgrades contradictory or unknown semantics to cover', async () => {
  const source = await read('src/components/EventMediaRail.astro');

  assert.match(source, /const normalizedImageTextMode = [\s\S]*value === 'visual_only' \|\| value === 'ocr_text' \? value : 'unknown'/u);
  assert.match(source, /const contradictoryVisualKind = requestedKind === 'visual' && imageTextMode !== 'visual_only';/u);
  assert.match(source, /const kind: EventMediaRailFrameKind = contradictoryVisualKind[\s\S]*imageTextMode === 'ocr_text' \? 'document' : 'unknown'/u);
  assert.match(source, /const fit: EventMediaRailFrameFit = requestedCover && kind === 'visual' && imageTextMode === 'visual_only'[\s\S]*\? 'cover'[\s\S]*: 'contain';/u);
  assert.match(source, /contradictoryVisualKind[\s\S]*'resolved_visual_kind_mismatch_fail_closed'/u);
  assert.match(source, /requestedCover && fit !== 'cover'[\s\S]*'resolved_cover_request_fail_closed'/u);
  assert.doesNotMatch(source, /item\.fit === 'cover' && kind === 'visual' \? 'cover' : 'contain'/u,
    'visual kind alone must not authorize crop when imageTextMode is OCR or unknown');
});

test('gallery and resolved items publish normalized image-text modes and conservative roles', async () => {
  const source = await read('src/components/EventMediaRail.astro');

  assert.match(source, /imageTextMode: normalizedImageTextMode\(asset\.image_text_mode\)/u);
  assert.match(source, /imageTextMode,/u);
  assert.match(source, /mediaRole: item\.mediaRole \|\| 'unknown_document'/u);
  assert.match(source, /mediaRole: asset\.media_role \|\| 'unknown_document'/u);
  assert.match(source, /data-image-text-mode=\{usesResolvedItems \? item\.imageTextMode : undefined\}/u);
  assert.match(source, /data-media-frame-fit=\{item\.fit\}/u);
  assert.match(source, /data-media-frame-crop-reason=\{item\.cropReason\}/u);
});
