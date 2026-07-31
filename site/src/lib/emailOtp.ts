export const EMAIL_OTP_LENGTH = 6;

export function normalizeEmailOtp(value: string): string {
  return String(value || '')
    .replace(/\D/gu, '')
    .slice(0, EMAIL_OTP_LENGTH);
}

export function isCompleteEmailOtp(value: string): boolean {
  return normalizeEmailOtp(value).length === EMAIL_OTP_LENGTH;
}

export function canSubmitEmailOtp(
  value: string,
  options: { inFlight?: boolean; lastSubmitted?: string } = {},
): boolean {
  const normalized = normalizeEmailOtp(value);
  return normalized.length === EMAIL_OTP_LENGTH
    && !options.inFlight
    && normalized !== normalizeEmailOtp(options.lastSubmitted || '');
}
