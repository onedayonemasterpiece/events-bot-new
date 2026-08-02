import { simpleParser } from 'mailparser';

const SIX_DIGITS = /(?<!\d)(\d{6})(?!\d)/gu;

function htmlToText(value) {
  return String(value || '')
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/giu, ' ')
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/giu, ' ')
    .replace(/<[^>]+>/gu, ' ')
    .replace(/&nbsp;/giu, ' ')
    .replace(/&#(?:x0*)?([0-9a-f]+);/giu, (_match, code) => {
      const radix = /[a-f]/iu.test(code) ? 16 : 10;
      return String.fromCodePoint(Number.parseInt(code, radix));
    });
}

export function extractSingleOtp({ text = '', html = '' }) {
  const candidates = new Set();
  for (const source of [String(text || ''), htmlToText(html)]) {
    for (const match of source.matchAll(SIX_DIGITS)) candidates.add(match[1]);
  }
  if (candidates.size !== 1) throw new Error(candidates.size ? 'otp_ambiguous' : 'otp_missing');
  return [...candidates][0];
}

export async function parseOtpMessage(source) {
  const mail = await simpleParser(source, { skipHtmlToText: true, skipTextToHtml: true });
  const otp = extractSingleOtp({ text: mail.text || '', html: typeof mail.html === 'string' ? mail.html : '' });
  return {
    otp,
    subject: String(mail.subject || ''),
    messageId: String(mail.messageId || ''),
    date: mail.date instanceof Date ? mail.date : null,
    from: mail.from?.value?.map((item) => item.address || '').filter(Boolean) || [],
    to: mail.to?.value?.map((item) => item.address || '').filter(Boolean) || [],
  };
}
