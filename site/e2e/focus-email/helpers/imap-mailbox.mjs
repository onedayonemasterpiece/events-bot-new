import { ImapFlow } from 'imapflow';

import { parseOtpMessage } from './otp-message-parser.mjs';
import { shortHash } from './redaction.mjs';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function optionalPattern(value) {
  if (!String(value || '').trim()) return null;
  return new RegExp(String(value), 'iu');
}

export class FocusOtpMailbox {
  constructor(config) {
    this.config = {
      host: String(config.host || ''),
      port: Number(config.port || 993),
      secure: config.secure !== false,
      username: String(config.username || ''),
      password: String(config.password || ''),
      expectedFrom: optionalPattern(config.expectedFrom),
      expectedSubject: optionalPattern(config.expectedSubject),
    };
    if (!this.config.host || !this.config.username || !this.config.password || this.config.port !== 993 || !this.config.secure) {
      throw new Error('imap_configuration_invalid');
    }
    this.client = new ImapFlow({
      host: this.config.host,
      port: this.config.port,
      secure: true,
      auth: { user: this.config.username, pass: this.config.password },
      logger: false,
      emitLogs: false,
    });
  }

  async connect() {
    await this.client.connect();
    await this.client.mailboxOpen('INBOX', { readOnly: true });
  }

  async checkpoint() {
    const status = await this.client.status('INBOX', { uidNext: true });
    return Math.max(1, Number(status.uidNext || 1));
  }

  async matchingMessages({ checkpoint, recipient }) {
    const checkpointUid = Math.max(1, Number(checkpoint || 1));
    const uids = (await this.client.search({ uid: `${checkpointUid}:*` }, { uid: true }))
      .filter((uid) => Number(uid) >= checkpointUid);
    const matches = [];
    for (const uid of uids) {
      const message = await this.client.fetchOne(uid, { source: true, internalDate: true }, { uid: true });
      if (!message?.source) continue;
      let parsed;
      try { parsed = await parseOtpMessage(message.source); } catch { continue; }
      const addresses = parsed.to.map((item) => item.toLowerCase());
      if (!addresses.includes(String(recipient).toLowerCase())) continue;
      if (this.config.expectedFrom && !parsed.from.some((item) => this.config.expectedFrom.test(item))) continue;
      if (this.config.expectedSubject && !this.config.expectedSubject.test(parsed.subject)) continue;
      matches.push({
        uid,
        otp: parsed.otp,
        receivedAt: message.internalDate instanceof Date ? message.internalDate : parsed.date,
        messageIdHash: shortHash(parsed.messageId || `uid:${uid}`),
      });
    }
    return matches;
  }

  async waitForSingleOtp({ checkpoint, recipient, timeoutMs = 120_000, pollMs = 2_000 }) {
    const started = Date.now();
    let firstMatchAt = 0;
    while (Date.now() - started < timeoutMs) {
      const matches = await this.matchingMessages({ checkpoint, recipient });
      if (matches.length > 1) throw new Error('mail_duplicate_messages');
      if (matches.length === 1) {
        if (!firstMatchAt) firstMatchAt = Date.now();
        // One extra poll catches providers that enqueue a duplicate moments
        // after the first copy without marking any message as read.
        if (Date.now() - firstMatchAt >= Math.max(1_500, pollMs)) {
          return { ...matches[0], matchingMessageCount: 1, deliveryLatencyMs: Date.now() - started };
        }
      }
      await sleep(pollMs);
    }
    throw new Error('mail_delivery_timeout');
  }

  async close() {
    await this.client.logout().catch(() => undefined);
  }
}
