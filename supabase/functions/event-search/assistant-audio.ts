/** Lossless-boundary PCM transport preparation, not an ASR or VAD.
 * Preserve the actual capture rate: changing a WAV header is NOT resampling.
 * The receiver must decode/assemble these segments in order before interpretation.
 */
export type AudioPart = { index: number; firstFrame: number; frameCount: number;
  sampleRate: number; bytes: Uint8Array };
export type WireBudget = { maxWireBytes: number; envelopeBytes: number; encoding: 'binary' | 'base64' };
function integer(n: number, minimum: number, maximum: number, name: string): void {
  if (!Number.isSafeInteger(n) || n < minimum || n > maximum) throw new Error(`invalid_${name}`);
}
export function maxWavBytes(budget: WireBudget): number {
  integer(budget.maxWireBytes, 64, 2 * 1024 * 1024, 'wire_budget');
  integer(budget.envelopeBytes, 0, budget.maxWireBytes, 'envelope');
  if (!['binary', 'base64'].includes(budget.encoding)) throw new Error('invalid_encoding');
  const room = budget.maxWireBytes - budget.envelopeBytes;
  const bytes = budget.encoding === 'base64' ? Math.floor(room / 4) * 3 : room;
  if (bytes < 46) throw new Error('audio_envelope_too_small');
  return bytes;
}
/** Output is mono signed PCM16 WAV. Non-finite input is rejected before creating any parts. */
export function segmentPcm16(samples: Float32Array, sampleRate: number, budget: WireBudget): AudioPart[] {
  if (!(samples instanceof Float32Array)) throw new Error('invalid_pcm');
  integer(sampleRate, 8000, 96000, 'sample_rate');
  integer(samples.length, 1, 4 * 1024 * 1024, 'sample_count');
  for (const sample of samples) if (!Number.isFinite(sample)) throw new Error('non_finite_pcm');
  const framesPerPart = Math.floor((maxWavBytes(budget) - 44) / 2);
  if (Math.ceil(samples.length / framesPerPart) > 256) throw new Error('too_many_parts');
  const result: AudioPart[] = [];
  for (let firstFrame = 0; firstFrame < samples.length; firstFrame += framesPerPart) {
    const frameCount = Math.min(framesPerPart, samples.length - firstFrame);
    const bytes = new Uint8Array(44 + frameCount * 2), view = new DataView(bytes.buffer);
    const ascii = (offset: number, value: string) => [...value].forEach((c, i) => view.setUint8(offset + i, c.charCodeAt(0)));
    ascii(0, 'RIFF'); view.setUint32(4, bytes.length - 8, true); ascii(8, 'WAVE');
    ascii(12, 'fmt '); view.setUint32(16, 16, true); view.setUint16(20, 1, true);
    view.setUint16(22, 1, true); view.setUint32(24, sampleRate, true);
    view.setUint32(28, sampleRate * 2, true); view.setUint16(32, 2, true); view.setUint16(34, 16, true);
    ascii(36, 'data'); view.setUint32(40, frameCount * 2, true);
    for (let j = 0; j < frameCount; j++) {
      const sample = Math.max(-1, Math.min(1, samples[firstFrame + j]!));
      view.setInt16(44 + j * 2, Math.round(sample < 0 ? sample * 32768 : sample * 32767), true);
    }
    result.push({ index: result.length, firstFrame, frameCount, sampleRate, bytes });
  }
  return result;
}
export function wireBytes(part: AudioPart, budget: WireBudget): number {
  return budget.envelopeBytes + (budget.encoding === 'base64' ? 4 * Math.ceil(part.bytes.byteLength / 3) : part.bytes.byteLength);
}
/** Validate continuity AND actual WAV bytes; do not trust the caller's declared duration/rate. */
export function validateAudioParts(parts: readonly AudioPart[], totalFrames: number, budget: WireBudget): void {
  integer(totalFrames, 1, 4 * 1024 * 1024, 'sample_count');
  if (!parts.length || parts.length > 256) throw new Error('invalid_parts');
  const maxBytes = maxWavBytes(budget);
  let nextFrame = 0, rate = 0;
  for (const [index, part] of parts.entries()) {
    integer(part.sampleRate, 8000, 96000, 'sample_rate');
    integer(part.frameCount, 1, totalFrames, 'frame_count');
    if (part.index !== index || part.firstFrame !== nextFrame) throw new Error('audio_gap_or_overlap');
    if (rate && part.sampleRate !== rate) throw new Error('mixed_sample_rates');
    rate = part.sampleRate;
    if (!(part.bytes instanceof Uint8Array) || part.bytes.byteLength !== 44 + part.frameCount * 2 || part.bytes.byteLength > maxBytes) throw new Error('audio_size_mismatch');
    const v = new DataView(part.bytes.buffer, part.bytes.byteOffset, part.bytes.byteLength);
    const text = (from: number, n: number) => String.fromCharCode(...part.bytes.subarray(from, from+n));
    if (text(0,4) !== 'RIFF' || text(8,4) !== 'WAVE' || text(12,4) !== 'fmt ' || text(36,4) !== 'data' ||
        v.getUint32(4,true) !== part.bytes.byteLength-8 || v.getUint32(16,true) !== 16 ||
        v.getUint16(20,true) !== 1 || v.getUint16(22,true) !== 1 || v.getUint32(24,true) !== rate ||
        v.getUint32(28,true) !== rate*2 || v.getUint16(32,true) !== 2 || v.getUint16(34,true) !== 16 ||
        v.getUint32(40,true) !== part.frameCount*2) throw new Error('invalid_wav_header');
    if (wireBytes(part,budget) > budget.maxWireBytes) throw new Error('audio_wire_limit');
    nextFrame += part.frameCount;
  }
  if (nextFrame !== totalFrames) throw new Error('audio_incomplete');
}
