const STORAGE_PUBLIC_PREFIXES = [
  'https://storage.yandexcloud.net/kenigevents/',
  'http://storage.yandexcloud.net/kenigevents/',
  'https://storage.yandexcloud.net/kenigevents.ru/',
  'http://storage.yandexcloud.net/kenigevents.ru/',
  'https://kenigevents.ru.storage.yandexcloud.net/',
  'http://kenigevents.ru.storage.yandexcloud.net/',
];

const ASSET_BASE_URL = (import.meta.env.PUBLIC_ASSET_BASE_URL || '').replace(/\/+$/u, '');

function normalizePath(path: string): string {
  return `/${path.replace(/^\/+/, '')}`;
}

export function toAssetPath(input: string | null | undefined): string | null {
  const raw = String(input || '').trim();
  if (!raw) return null;

  for (const prefix of STORAGE_PUBLIC_PREFIXES) {
    if (raw.startsWith(prefix)) return normalizePath(raw.slice(prefix.length));
  }

  if (/^https?:\/\//iu.test(raw)) return raw;

  return normalizePath(raw);
}

export function assetUrl(input: string | null | undefined): string | null {
  const raw = String(input || '').trim();
  if (!raw) return null;
  if (!ASSET_BASE_URL) return raw;

  for (const prefix of STORAGE_PUBLIC_PREFIXES) {
    if (raw.startsWith(prefix)) return `${ASSET_BASE_URL}${normalizePath(raw.slice(prefix.length))}`;
  }

  if (/^https?:\/\//iu.test(raw)) return raw;

  return `${ASSET_BASE_URL}${normalizePath(raw)}`;
}

export function eventImageUrl(input: string | null | undefined): string | null {
  return assetUrl(input);
}
