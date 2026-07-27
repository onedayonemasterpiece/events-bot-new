export const EVENT_SHARE_IMAGE_WIDTH = 1080;
export const EVENT_SHARE_IMAGE_HEIGHT = 1350;

export function normalizedEventShareImageTextMode(value) {
  return value === 'visual_only' || value === 'ocr_text' ? value : 'unknown';
}

export function shouldComposeEventShareImage(value) {
  return normalizedEventShareImageTextMode(value) === 'visual_only';
}

export function eventShareImagePayload(input = {}) {
  return Object.freeze({
    title: String(input.title || '').trim().slice(0, 280),
    dateTime: String(input.dateTime || '').trim().slice(0, 120),
    place: String(input.place || '').trim().slice(0, 180),
    admission: String(input.admission || '').trim().slice(0, 100),
    brandUrl: String(input.brandUrl || '').trim(),
  });
}

function roundedRect(ctx, x, y, width, height, radius) {
  ctx.beginPath();
  if (typeof ctx.roundRect === 'function') {
    ctx.roundRect(x, y, width, height, radius);
    return;
  }
  const r = Math.max(0, Math.min(radius, width / 2, height / 2));
  ctx.moveTo(x + r, y);
  ctx.arcTo(x + width, y, x + width, y + height, r);
  ctx.arcTo(x + width, y + height, x, y + height, r);
  ctx.arcTo(x, y + height, x, y, r);
  ctx.arcTo(x, y, x + width, y, r);
  ctx.closePath();
}

export function wrapEventShareText(ctx, text, maxWidth, maxLines) {
  const words = String(text || '').split(/\s+/u).filter(Boolean);
  const lines = [];
  let line = '';
  let consumed = 0;
  for (const word of words) {
    const candidate = line ? `${line} ${word}` : word;
    if (!line || ctx.measureText(candidate).width <= maxWidth) {
      line = candidate;
      consumed += 1;
      continue;
    }
    lines.push(line);
    if (lines.length >= maxLines) break;
    line = word;
    consumed += 1;
  }
  if (line && lines.length < maxLines) lines.push(line);
  if (consumed < words.length && lines.length) {
    let last = lines[lines.length - 1];
    while (last.length > 6 && ctx.measureText(`${last}…`).width > maxWidth) last = last.slice(0, -1);
    lines[lines.length - 1] = `${last.trim()}…`;
  }
  return lines;
}

function drawCover(ctx, image, width, height) {
  const sourceWidth = Number(image?.width || 0);
  const sourceHeight = Number(image?.height || 0);
  if (!(sourceWidth > 0 && sourceHeight > 0)) throw new Error('share_source_dimensions_invalid');
  const scale = Math.max(width / sourceWidth, height / sourceHeight);
  const drawWidth = sourceWidth * scale;
  const drawHeight = sourceHeight * scale;
  ctx.drawImage(image, (width - drawWidth) / 2, (height - drawHeight) / 2, drawWidth, drawHeight);
}

export function drawEventShareImage(ctx, sourceImage, input, brandImage = null) {
  const payload = eventShareImagePayload(input);
  const width = EVENT_SHARE_IMAGE_WIDTH;
  const height = EVENT_SHARE_IMAGE_HEIGHT;
  ctx.fillStyle = '#241c17';
  ctx.fillRect(0, 0, width, height);
  drawCover(ctx, sourceImage, width, height);

  const topVeil = ctx.createLinearGradient(0, 0, 0, 420);
  topVeil.addColorStop(0, 'rgba(20,13,9,.68)');
  topVeil.addColorStop(1, 'rgba(20,13,9,0)');
  ctx.fillStyle = topVeil;
  ctx.fillRect(0, 0, width, 420);
  const bottomVeil = ctx.createLinearGradient(0, 620, 0, height);
  bottomVeil.addColorStop(0, 'rgba(15,11,9,0)');
  bottomVeil.addColorStop(.42, 'rgba(15,11,9,.48)');
  bottomVeil.addColorStop(1, 'rgba(15,11,9,.96)');
  ctx.fillStyle = bottomVeil;
  ctx.fillRect(0, 620, width, height - 620);

  roundedRect(ctx, 56, 52, 398, 174, 18);
  ctx.fillStyle = '#98401f';
  ctx.fill();
  ctx.fillStyle = '#fffaf2';
  ctx.font = '700 25px system-ui, -apple-system, "Segoe UI", sans-serif';
  ctx.fillText('ПОЛЮБИТЬ КАЛИНИНГРАД', 82, 96);
  if (brandImage?.width > 0 && brandImage?.height > 0) {
    const maxWidth = 320;
    const maxHeight = 82;
    const scale = Math.min(maxWidth / brandImage.width, maxHeight / brandImage.height);
    ctx.drawImage(brandImage, 82, 119, brandImage.width * scale, brandImage.height * scale);
  } else {
    ctx.font = '950 70px system-ui, -apple-system, "Segoe UI", sans-serif';
    ctx.fillText('Анонсы', 78, 188);
  }

  ctx.fillStyle = '#fff';
  ctx.font = '900 72px system-ui, -apple-system, "Segoe UI", sans-serif';
  const titleLines = wrapEventShareText(ctx, payload.title, 944, 4);
  const titleTop = 920 - Math.max(0, titleLines.length - 2) * 76;
  titleLines.forEach((line, index) => ctx.fillText(line, 68, titleTop + index * 78));

  const facts = [payload.dateTime, payload.place, payload.admission].filter(Boolean);
  let factY = Math.max(1090, titleTop + titleLines.length * 78 + 26);
  ctx.font = '800 32px system-ui, -apple-system, "Segoe UI", sans-serif';
  for (const fact of facts.slice(0, 3)) {
    const lines = wrapEventShareText(ctx, fact, 880, 2);
    const boxHeight = 54 + (lines.length - 1) * 38;
    roundedRect(ctx, 68, factY, 944, boxHeight, 15);
    ctx.fillStyle = 'rgba(255,250,242,.92)';
    ctx.fill();
    ctx.fillStyle = '#34251c';
    lines.forEach((line, index) => ctx.fillText(line, 94, factY + 38 + index * 38));
    factY += boxHeight + 12;
    if (factY > height - 38) break;
  }
}

async function bitmapFromBlob(blob, createImageBitmapImpl) {
  if (!(blob instanceof Blob)) throw new Error('share_source_blob_invalid');
  if (typeof createImageBitmapImpl !== 'function') throw new Error('create_image_bitmap_unavailable');
  return createImageBitmapImpl(blob);
}

async function loadBrandBitmap(brandUrl, fetchImpl, createImageBitmapImpl) {
  if (!brandUrl || typeof fetchImpl !== 'function') return null;
  try {
    const response = await fetchImpl(brandUrl, { mode:'same-origin', credentials:'same-origin' });
    if (!response.ok) return null;
    return await bitmapFromBlob(await response.blob(), createImageBitmapImpl);
  } catch {
    return null;
  }
}

export async function composeEventShareImage({
  sourceBlob,
  imageTextMode,
  title,
  dateTime,
  place,
  admission,
  brandUrl,
  documentLike = globalThis.document,
  fetchImpl = globalThis.fetch,
  createImageBitmapImpl = globalThis.createImageBitmap,
} = {}) {
  if (!shouldComposeEventShareImage(imageTextMode)) return null;
  const canvas = documentLike?.createElement?.('canvas');
  if (!canvas?.getContext) throw new Error('share_canvas_unavailable');
  canvas.width = EVENT_SHARE_IMAGE_WIDTH;
  canvas.height = EVENT_SHARE_IMAGE_HEIGHT;
  const ctx = canvas.getContext('2d');
  if (!ctx) throw new Error('share_canvas_context_unavailable');
  const sourceImage = await bitmapFromBlob(sourceBlob, createImageBitmapImpl);
  const brandImage = await loadBrandBitmap(brandUrl, fetchImpl, createImageBitmapImpl);
  try {
    drawEventShareImage(ctx, sourceImage, { title, dateTime, place, admission, brandUrl }, brandImage);
    return await new Promise((resolve, reject) => canvas.toBlob(
      (blob) => blob ? resolve(blob) : reject(new Error('share_canvas_blob_failed')),
      'image/png',
      .94,
    ));
  } finally {
    sourceImage?.close?.();
    brandImage?.close?.();
  }
}
