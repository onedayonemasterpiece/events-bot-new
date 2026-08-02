import WebSocket from 'ws';

import { extractSingleOtp } from './otp-message-parser.mjs';
import { shortHash } from './redaction.mjs';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const pattern = (value) => String(value || '').trim() ? new RegExp(String(value), 'iu') : null;

function headers(message) {
  return new Map((message?.headers || []).map((item) => [String(item?.name || '').toLowerCase(), (item?.values || []).map(String)]));
}

export class YandexMailTriggerSocket {
  constructor(config) {
    this.url = String(config.url || '').trim();
    this.expectedFrom = pattern(config.expectedFrom);
    this.expectedSubject = pattern(config.expectedSubject);
    this.events = [];
    if (!/^wss:\/\//u.test(this.url)) throw new Error('yandex_mail_websocket_configuration_invalid');
  }

  async connect() {
    this.connectedAt = Date.now();
    this.socket = new WebSocket(this.url, { handshakeTimeout: 15_000 });
    this.socket.on('message', (data) => {
      try {
        const event = JSON.parse(String(data));
        for (const message of event?.messages || []) this.events.push({ arrivedAt: Date.now(), message });
      } catch { /* malformed frames cannot become matching OTP mail */ }
    });
    await new Promise((resolve, reject) => {
      this.socket.once('open', resolve);
      this.socket.once('error', reject);
    }).catch((error) => { throw new Error(`yandex_mail_websocket_connect:${String(error?.message || error).slice(0, 120)}`); });
  }

  async checkpoint() { return this.connectedAt; }

  matchingMessages({ checkpoint, recipient }) {
    const matches = [];
    for (const event of this.events.filter((item) => item.arrivedAt >= Number(checkpoint || 0))) {
      const map = headers(event.message);
      const to = (map.get('to') || []).join(' ');
      const from = (map.get('from') || []).join(' ');
      const subject = (map.get('subject') || []).join(' ');
      if (!to.toLowerCase().includes(String(recipient).toLowerCase())) continue;
      if (this.expectedFrom && !this.expectedFrom.test(from)) continue;
      if (this.expectedSubject && !this.expectedSubject.test(subject)) continue;
      let otp;
      try { otp = extractSingleOtp({ text: event.message?.message || '' }); } catch { continue; }
      matches.push({ otp, receivedAt: event.message?.received_at || null,
        messageIdHash: shortHash((map.get('message-id') || [String(event.arrivedAt)])[0]) });
    }
    return matches;
  }

  async waitForSingleOtp({ checkpoint, recipient, timeoutMs = 120_000, pollMs = 500 }) {
    const started = Date.now(); let firstMatchAt = 0;
    while (Date.now() - started < timeoutMs) {
      const matches = this.matchingMessages({ checkpoint, recipient });
      if (matches.length > 1) throw new Error('mail_duplicate_messages');
      if (matches.length === 1) {
        if (!firstMatchAt) firstMatchAt = Date.now();
        if (Date.now() - firstMatchAt >= 1_500) return { ...matches[0], matchingMessageCount: 1, deliveryLatencyMs: Date.now() - started };
      }
      await sleep(pollMs);
    }
    throw new Error('mail_delivery_timeout');
  }

  async close() {
    if (!this.socket || this.socket.readyState === WebSocket.CLOSED) return;
    await new Promise((resolve) => { this.socket.once('close', resolve); this.socket.close(); setTimeout(resolve, 1_000); });
  }
}
