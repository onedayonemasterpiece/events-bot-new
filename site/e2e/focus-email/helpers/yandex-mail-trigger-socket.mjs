import WebSocket from 'ws';

import { extractSingleOtp } from './otp-message-parser.mjs';
import { shortHash } from './redaction.mjs';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const pattern = (value) => String(value || '').trim() ? new RegExp(String(value), 'iu') : null;

function headers(message) {
  return new Map((message?.headers || []).map((item) => [String(item?.name || '').toLowerCase(), (item?.values || []).map(String)]));
}

function messageOtp(message, subject) {
  // Yandex Mail Trigger exposes Subject headers and the message body as
  // separate fields. The protected provider contract puts the OTP in the
  // already-decoded Subject, so it is the canonical source. Multipart/raw MIME
  // bodies may contain unrelated six-digit fragments and must not make that
  // exact subject ambiguous. Body parsing is a fallback for subject-less
  // provider contracts only.
  try {
    return extractSingleOtp({ text: subject });
  } catch (error) {
    if (error?.message !== 'otp_missing') throw error;
  }
  const map = headers(message);
  const contentType = (map.get('content-type') || []).join(' ').toLowerCase();
  const body = String(message?.message || '');
  return contentType.includes('text/html')
    ? extractSingleOtp({ html: body })
    : extractSingleOtp({ text: body });
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

  async checkpoint() {
    this.checkpointAt = this.connectedAt;
    return this.checkpointAt;
  }

  safeDiagnostics({ checkpoint = this.checkpointAt, recipient } = {}) {
    const diagnostics = {
      matching_message_count: 0,
      received_event_count: 0,
      recipient_match_count: 0,
      sender_match_count: 0,
      subject_match_count: 0,
      otp_parse_count: 0,
    };
    for (const event of this.events.filter((item) => item.arrivedAt >= Number(checkpoint || 0))) {
      diagnostics.received_event_count += 1;
      const map = headers(event.message);
      const to = (map.get('to') || []).join(' ');
      const from = (map.get('from') || []).join(' ');
      const subject = (map.get('subject') || []).join(' ');
      if (!to.toLowerCase().includes(String(recipient || '').toLowerCase())) continue;
      diagnostics.recipient_match_count += 1;
      if (this.expectedFrom && !this.expectedFrom.test(from)) continue;
      diagnostics.sender_match_count += 1;
      if (this.expectedSubject && !this.expectedSubject.test(subject)) continue;
      diagnostics.subject_match_count += 1;
      try { messageOtp(event.message, subject); } catch { continue; }
      diagnostics.otp_parse_count += 1;
    }
    diagnostics.matching_message_count = diagnostics.otp_parse_count;
    return diagnostics;
  }

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
      try { otp = messageOtp(event.message, subject); } catch { continue; }
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
