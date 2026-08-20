import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import { resolveBoundedDocumentFrame } from '../src/lib/compactMediaFraming.mjs';

test('very tall classified OCR listing media uses the universal 20% bounded frame', () => {
  const frame = resolveBoundedDocumentFrame({
    ratio:906 / 1280,
    imageTextMode:'ocr_text',
    semanticStatus:'classified',
    dimensionsKnown:true,
  });
  assert.ok(Math.abs(frame.targetRatio - 0.884765625) < 1e-12);
  assert.equal(frame.fit, 'cover');
  assert.equal(frame.mediaTreatment, 'document-safe-cover');
  assert.equal(frame.cropReason, 'document_bounded_cover');
  assert.ok(Math.abs(frame.coverCrop - 0.2) < 1e-12);
  assert.equal(frame.verticalRetention, 0.8);
});

test('unknown, semantic-error and unknown-dimension media remain fail-closed', () => {
  for (const input of [
    { ratio:906 / 1280, imageTextMode:'unknown', semanticStatus:'classified', dimensionsKnown:true },
    { ratio:906 / 1280, imageTextMode:'ocr_text', semanticStatus:'error', dimensionsKnown:true },
    { ratio:906 / 1280, imageTextMode:'ocr_text', semanticStatus:'classified', dimensionsKnown:false },
  ]) {
    const frame = resolveBoundedDocumentFrame(input);
    assert.equal(frame.targetRatio, 906 / 1280);
    assert.equal(frame.fit, 'contain');
    assert.equal(frame.mediaTreatment, 'document-contain');
    assert.equal(frame.coverCrop, null);
    assert.equal(frame.verticalRetention, 1);
  }
});

test('ordinary OCR does not gain arbitrary width', () => {
  const ordinary = resolveBoundedDocumentFrame({ ratio:0.9, imageTextMode:'ocr_text', semanticStatus:'classified', dimensionsKnown:true });
  assert.equal(ordinary.targetRatio, 0.9);
  assert.equal(ordinary.coverCrop, 0);
});

test('Listing projection, markup and CSS expose the source-generic treatment contract', async () => {
  const [projection, component, css] = await Promise.all([
    readFile(new URL('../src/lib/listingPresentation.ts', import.meta.url), 'utf8'),
    readFile(new URL('../src/components/listings/ListingEventCard.astro', import.meta.url), 'utf8'),
    readFile(new URL('../src/styles/design-system.css', import.meta.url), 'utf8'),
  ]);
  assert.match(projection, /resolveBoundedDocumentFrame/u);
  assert.match(projection, /outputRatio = isVisualOnly \? visualRatio : documentFrame\.targetRatio/u);
  for (const attribute of ['data-listing-media-treatment', 'data-listing-fit', 'data-listing-cover-crop', 'data-listing-crop-reason']) {
    assert.match(component, new RegExp(attribute));
  }
  assert.match(css, /data-listing-media-treatment="document-safe-cover"\]\[data-listing-fit="cover"[\s\S]*object-fit:\s*cover/u);
  assert.doesNotMatch(projection + component + css, /7491|f42be320/u, 'runtime framing must not depend on an event id or asset hash');
});
