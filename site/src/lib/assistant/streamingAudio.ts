import { maxWavBytes, segmentPcm16, type AudioPart, type WireBudget } from './audioSegments.ts';

/** Constant-memory framing. A part boundary never ends the logical utterance.
 * Unlike the batch helper, total recording duration is not held in one array.
 */
export class StreamingPcm16 {
  private buffer: Float32Array;
  private used = 0;
  private emittedFrames = 0;
  private index = 0;
  private closed = false;
  readonly sampleRate: number;
  private budget: WireBudget;
  constructor(sampleRate: number, budget: WireBudget, maxPartFrames?: number) {
    this.sampleRate = sampleRate; this.budget = budget;
    if (!Number.isSafeInteger(sampleRate) || sampleRate < 8000 || sampleRate > 96000) throw new Error('invalid_sample_rate');
    if (maxPartFrames !== undefined && (!Number.isSafeInteger(maxPartFrames) || maxPartFrames < 1)) throw new Error('invalid_checkpoint_frames');
    this.buffer = new Float32Array(Math.min(Math.floor((maxWavBytes(budget) - 44) / 2), maxPartFrames ?? Infinity));
  }
  get frames(): number { return this.emittedFrames + this.used; }
  push(pcm: Float32Array, firstFrame = this.frames): AudioPart[] {
    if (this.closed) throw new Error('capture_closed');
    if (!(pcm instanceof Float32Array) || firstFrame !== this.frames) throw new Error('audio_gap_or_overlap');
    if (!Number.isSafeInteger(this.frames + pcm.length)) throw new Error('invalid_sample_count');
    for (const sample of pcm) if (!Number.isFinite(sample)) throw new Error('non_finite_pcm');
    const parts: AudioPart[] = [];
    let offset = 0;
    while (offset < pcm.length) {
      const take = Math.min(this.buffer.length - this.used, pcm.length - offset);
      this.buffer.set(pcm.subarray(offset, offset + take), this.used);
      this.used += take; offset += take;
      if (this.used === this.buffer.length) parts.push(this.emit());
    }
    return parts;
  }
  private emit(): AudioPart {
    const part = segmentPcm16(this.buffer.subarray(0, this.used), this.sampleRate, this.budget)[0]!;
    part.index = this.index++; part.firstFrame = this.emittedFrames;
    this.emittedFrames += this.used; this.used = 0;
    return part;
  }
  finish(): AudioPart[] {
    if (this.closed) return [];
    this.closed = true;
    return this.used ? [this.emit()] : [];
  }
}
