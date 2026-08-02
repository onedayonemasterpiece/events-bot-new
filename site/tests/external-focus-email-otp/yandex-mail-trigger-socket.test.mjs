import assert from 'node:assert/strict';
import test from 'node:test';

import { YandexMailTriggerSocket } from '../../e2e/focus-email/helpers/yandex-mail-trigger-socket.mjs';

const recipient = 'fixed-trigger-recipient@serverless.yandexcloud.net';
const message = (subject) => ({
  received_at: '2026-08-02 10:58:16 +0000 UTC',
  headers: [
    { name: 'To', values: [`KenigEvents E2E <${recipient}>`] },
    { name: 'From', values: ['KenigEvents <notify@kenigevents.ru>'] },
    { name: 'Subject', values: [subject] },
    { name: 'Message-Id', values: ['<safe-fixture@example.test>'] },
  ],
  message: '<div>Одноразовый код: <b>123456</b></div>',
});

test('Yandex Mail Trigger adapter accepts the current SMTP and staged hook subjects', () => {
  const socket = new YandexMailTriggerSocket({
    url: 'wss://example.test/protected',
    expectedFrom: '(?:notify@kenigevents\\.ru|events@news\\.kenigevents\\.ru)',
    expectedSubject: '(?:^[0-9]{6} — код входа в KenigEvents$|^Код [0-9]{6} — вход в Анонсы$)',
  });
  socket.checkpointAt = 1;
  socket.events = [
    { arrivedAt: 2, message: message('123456 — код входа в KenigEvents') },
    { arrivedAt: 3, message: message('Код 123456 — вход в Анонсы') },
  ];
  assert.equal(socket.matchingMessages({ checkpoint: 1, recipient }).length, 2);
  assert.deepEqual(socket.safeDiagnostics({ recipient }), {
    matching_message_count: 2,
    received_event_count: 2,
    recipient_match_count: 2,
    sender_match_count: 2,
    subject_match_count: 2,
    otp_parse_count: 2,
  });
});

test('safe diagnostics identify a subject-contract mismatch without retaining mail data', () => {
  const socket = new YandexMailTriggerSocket({
    url: 'wss://example.test/protected',
    expectedFrom: 'notify@kenigevents\\.ru',
    expectedSubject: '^Код [0-9]{6} — вход в Анонсы$',
  });
  socket.checkpointAt = 1;
  socket.events = [{ arrivedAt: 2, message: message('123456 — код входа в KenigEvents') }];
  assert.deepEqual(socket.safeDiagnostics({ recipient }), {
    matching_message_count: 0,
    received_event_count: 1,
    recipient_match_count: 1,
    sender_match_count: 1,
    subject_match_count: 0,
    otp_parse_count: 0,
  });
});
