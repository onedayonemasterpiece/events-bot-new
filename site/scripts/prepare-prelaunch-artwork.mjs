import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteDir = dirname(scriptDir);
const sourceRoot = join(siteDir, 'src', 'assets', 'prelaunch-approved');
const targetDir = join(siteDir, 'public', 'assets', 'prelaunch');

const assets = [
  {
    name: 'desktop',
    width: 1738,
    height: 905,
    bytes: 82_732,
    sha256: '3e975fcd07d025f33c948b32758164905d3abc4b1bc91da5e84819604b712061',
    target: 'prelaunch-scene-desktop.webp',
  },
  {
    name: 'mobile',
    width: 853,
    height: 1844,
    bytes: 54_130,
    sha256: 'c6ae402fd938807b821f0c78d16f1184bb16f25e73efbce94a4e55758aa5c94f',
    target: 'prelaunch-scene-mobile.webp',
  },
];

function read24(buffer, offset) {
  return buffer[offset] | (buffer[offset + 1] << 8) | (buffer[offset + 2] << 16);
}

function webpDimensions(buffer) {
  if (buffer.length < 30 || buffer.toString('ascii', 0, 4) !== 'RIFF' || buffer.toString('ascii', 8, 12) !== 'WEBP') {
    throw new Error('Approved prelaunch asset is not a WebP RIFF file');
  }
  const declared = buffer.readUInt32LE(4) + 8;
  if (declared !== buffer.length) {
    throw new Error(`Approved prelaunch asset is truncated: declared=${declared}, actual=${buffer.length}`);
  }

  let offset = 12;
  while (offset + 8 <= buffer.length) {
    const type = buffer.toString('ascii', offset, offset + 4);
    const length = buffer.readUInt32LE(offset + 4);
    const payload = offset + 8;
    if (payload + length > buffer.length) throw new Error(`Invalid WebP ${type} chunk length`);

    if (type === 'VP8X') {
      return { width: read24(buffer, payload + 4) + 1, height: read24(buffer, payload + 7) + 1 };
    }
    if (type === 'VP8 ') {
      if (buffer[payload + 3] !== 0x9d || buffer[payload + 4] !== 0x01 || buffer[payload + 5] !== 0x2a) {
        throw new Error('Invalid lossy WebP frame header');
      }
      return {
        width: buffer.readUInt16LE(payload + 6) & 0x3fff,
        height: buffer.readUInt16LE(payload + 8) & 0x3fff,
      };
    }
    if (type === 'VP8L') {
      if (buffer[payload] !== 0x2f) throw new Error('Invalid lossless WebP frame header');
      const bits = buffer.readUInt32LE(payload + 1);
      return { width: (bits & 0x3fff) + 1, height: ((bits >> 14) & 0x3fff) + 1 };
    }
    offset = payload + length + (length % 2);
  }
  throw new Error('WebP image chunk is missing');
}

function decodeAsset(asset) {
  const sourceDir = join(sourceRoot, asset.name);
  if (!existsSync(sourceDir)) throw new Error(`Approved prelaunch source directory is missing: ${sourceDir}`);
  const parts = readdirSync(sourceDir)
    .filter((name) => /^part-\d+\.b64part$/u.test(name))
    .sort();
  if (!parts.length) throw new Error(`Approved prelaunch source parts are missing: ${sourceDir}`);

  const encoded = parts.map((name) => readFileSync(join(sourceDir, name), 'utf8').trim()).join('');
  if (!/^[A-Za-z0-9+/]+={0,2}$/u.test(encoded)) throw new Error(`Approved prelaunch source is not canonical base64: ${asset.name}`);
  const buffer = Buffer.from(encoded, 'base64');
  const hash = createHash('sha256').update(buffer).digest('hex');
  const dimensions = webpDimensions(buffer);

  if (buffer.length !== asset.bytes) throw new Error(`Approved ${asset.name} bytes mismatch: ${buffer.length}`);
  if (hash !== asset.sha256) throw new Error(`Approved ${asset.name} sha256 mismatch: ${hash}`);
  if (dimensions.width !== asset.width || dimensions.height !== asset.height) {
    throw new Error(`Approved ${asset.name} dimensions mismatch: ${dimensions.width}x${dimensions.height}`);
  }

  mkdirSync(targetDir, { recursive: true });
  const target = join(targetDir, asset.target);
  writeFileSync(target, buffer);
  console.log(`Prepared approved prelaunch ${asset.name}: ${asset.width}x${asset.height}, ${buffer.length} bytes, sha256=${hash}`);
}

for (const asset of assets) decodeAsset(asset);
