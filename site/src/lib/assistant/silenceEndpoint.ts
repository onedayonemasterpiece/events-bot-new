/** Local energy endpoint, not a speech recognizer or an audio filter.
 * Observation uses captured sample time, never wall-clock gaps or timers.
 * Ambiguous/noisy audio stays open for the manual Stop control.
 */
export const SILENCE_ENDPOINT_MS = 1500;
const WINDOW_MS = 20;
const SPEECH_EVIDENCE_MS = 180;
const SPEECH_RMS = 0.004;
// Keep the donor WL conservative -60 dBFS floor. Never adapt this upward
// to a noisy room: doing so can mistake quiet words for the end of speech.
const SILENCE_RMS = 0.001;

export class SilenceEndpointDetector {
  private sampleRate = 0;
  private windowFrames = 0;
  private frames = 0;
  private energy = 0;
  private speechMs = 0;
  private quietMs = 0;
  private armed = false;
  private ended = false;

  /** Conservative energy evidence, not semantic speech recognition. */
  hasSpeechEvidence():boolean { return this.armed; }

  reset(): void {
    this.sampleRate = 0; this.windowFrames = 0; this.frames = 0; this.energy = 0;
    this.speechMs = 0; this.quietMs = 0; this.armed = false; this.ended = false;
  }

  /** Reads but never mutates PCM. Missing/invalid analysis resets evidence,
   * requiring new sound instead of interpreting an unobserved gap as silence.
   * Energy cannot distinguish speech from every non-speech sound; no claim of
   * semantic VAD is made. Short/very quiet inputs remain manually stoppable.
   */
  push(pcm: Float32Array | null, sampleRate: number): boolean {
    if (!(pcm instanceof Float32Array) || !pcm.length || !Number.isSafeInteger(sampleRate)
      || sampleRate < 8000 || sampleRate > 96000) { this.reset(); return false; }
    for (const sample of pcm) if (!Number.isFinite(sample)) { this.reset(); return false; }
    if (sampleRate !== this.sampleRate) {
      this.reset(); this.sampleRate = sampleRate;
      this.windowFrames = Math.round(sampleRate * WINDOW_MS / 1000);
    }
    if (this.ended) return true;
    for (const sample of pcm) {
      this.energy += sample * sample; this.frames++;
      if (this.frames < this.windowFrames) continue;
      const rms = Math.sqrt(this.energy / this.frames);
      const duration = this.frames * 1000 / sampleRate;
      this.frames = 0; this.energy = 0;
      if (rms >= SPEECH_RMS) {
        this.speechMs += duration; this.quietMs = 0;
        if (this.speechMs >= SPEECH_EVIDENCE_MS) this.armed = true;
      } else {
        this.speechMs = 0;
        if (this.armed && rms < SILENCE_RMS) this.quietMs += duration;
        else this.quietMs = 0;
      }
      if (this.armed && this.quietMs >= SILENCE_ENDPOINT_MS) { this.ended = true; return true; }
    }
    return false;
  }
}
