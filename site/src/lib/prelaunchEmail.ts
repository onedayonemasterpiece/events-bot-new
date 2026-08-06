export type PrelaunchEmailValidationReason =
  | 'empty'
  | 'length'
  | 'unsafe_character'
  | 'at_count'
  | 'local_length'
  | 'local_syntax'
  | 'domain_length'
  | 'domain_syntax'
  | 'tld_syntax';

export type PrelaunchEmailValidationResult =
  | { ok: true; email: string }
  | { ok: false; reason: PrelaunchEmailValidationReason };

const LOCAL_ALLOWED = /^[a-z0-9.!#$%&'*+/=?^_`{|}~-]+$/u;
const DOMAIN_LABEL = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/u;
const ASCII_VISIBLE = /^[\x21-\x7e]+$/u;
const EXPLICITLY_UNSAFE = /[<>"\\()[\]{},;:\u007f]/u;
const ORDINARY_TLD = /^[a-z]{2,63}$/u;
const PUNYCODE_TLD = /^xn--[a-z0-9-]{2,59}$/u;

/**
 * Deliberately conservative browser/server contract for the launch form.
 *
 * We support ordinary ASCII mailbox addresses, plus-tags and punycode domains.
 * Quoted local parts, comments and raw Unicode domains are rejected rather than
 * normalized ambiguously. The value is still sent as a JSON RPC parameter and
 * is never interpolated into SQL or HTML.
 */
export function normalizePrelaunchEmail(input: unknown): PrelaunchEmailValidationResult {
  const value = String(input ?? '').trim().toLowerCase();
  if (!value) return { ok: false, reason: 'empty' };
  if (value.length < 5 || value.length > 254) return { ok: false, reason: 'length' };
  if (!ASCII_VISIBLE.test(value) || EXPLICITLY_UNSAFE.test(value)) {
    return { ok: false, reason: 'unsafe_character' };
  }

  const at = value.indexOf('@');
  if (at <= 0 || at !== value.lastIndexOf('@')) return { ok: false, reason: 'at_count' };

  const local = value.slice(0, at);
  const domain = value.slice(at + 1);
  if (local.length > 64) return { ok: false, reason: 'local_length' };
  if (!LOCAL_ALLOWED.test(local)
    || local.startsWith('.')
    || local.endsWith('.')
    || local.includes('..')) {
    return { ok: false, reason: 'local_syntax' };
  }

  if (!domain || domain.length > 253) return { ok: false, reason: 'domain_length' };
  const labels = domain.split('.');
  if (labels.length < 2 || labels.some((label) => !DOMAIN_LABEL.test(label))) {
    return { ok: false, reason: 'domain_syntax' };
  }

  const tld = labels.at(-1) || '';
  if (!ORDINARY_TLD.test(tld) && !PUNYCODE_TLD.test(tld)) {
    return { ok: false, reason: 'tld_syntax' };
  }

  return { ok: true, email: `${local}@${domain}` };
}
