import { FocusOtpMailbox } from './imap-mailbox.mjs';
import { YandexMailTriggerSocket } from './yandex-mail-trigger-socket.mjs';

const required = (env, name) => {
  const value = String(env[name] || '').trim();
  if (!value) throw new Error(`missing_configuration:${name}`);
  return value;
};

export function createMailbox(env = process.env) {
  const adapter = String(env.E2E_MAIL_ADAPTER || 'imap').trim().toLowerCase();
  const common = { expectedFrom: required(env, 'E2E_EXPECTED_FROM_PATTERN'), expectedSubject: required(env, 'E2E_EXPECTED_SUBJECT_PATTERN') };
  if (adapter === 'yandex-websocket') return new YandexMailTriggerSocket({ ...common, url: required(env, 'E2E_YANDEX_MAIL_WS_URL') });
  if (adapter !== 'imap') throw new Error(`mail_adapter_invalid:${adapter}`);
  return new FocusOtpMailbox({ ...common, host: required(env, 'E2E_IMAP_HOST'), port: Number(env.E2E_IMAP_PORT || 993),
    secure: String(env.E2E_IMAP_SECURE || 'true').toLowerCase() === 'true', username: required(env, 'E2E_IMAP_USERNAME'), password: required(env, 'E2E_IMAP_PASSWORD') });
}
