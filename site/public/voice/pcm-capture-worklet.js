/* Continuous foreground capture, deliberately no energy gate/VAD.
 * Blocks are PCM transport units, never ASR requests or utterance boundaries.
 */
class KenigVoicePcmProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.buffer = new Float32Array(4096);
    this.used = 0;
    this.frames = 0;
    this.stopped = false;
    this.port.onmessage = ({ data }) => {
      if (data?.type !== 'stop' || this.stopped) return;
      this.flush();
      this.stopped = true;
      this.port.postMessage({ type: 'stopped', frames: this.frames, sampleRate });
    };
  }
  flush() {
    if (!this.used) return;
    const pcm = this.buffer.slice(0, this.used);
    this.port.postMessage({ type: 'pcm', firstFrame: this.frames,
      sampleRate, pcm }, [pcm.buffer]);
    this.frames += this.used;
    this.used = 0;
  }
  process(inputs) {
    if (this.stopped) return false;
    const channels = inputs[0];
    if (!channels?.length) return true;
    // Render quantum is not assumed to be 128. Keep even quiet negative words.
    for (let frame = 0; frame < channels[0].length; frame++) {
      let mono = 0;
      for (const channel of channels) mono += channel[frame] / channels.length;
      this.buffer[this.used++] = mono;
      if (this.used === this.buffer.length) this.flush();
    }
    // The output stays silent; microphone audio is never played back.
    return true;
  }
}
registerProcessor('kenig-voice-pcm', KenigVoicePcmProcessor);
