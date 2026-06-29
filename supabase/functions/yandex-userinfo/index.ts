// Yandex OAuth userinfo adapter for Supabase custom OAuth provider.
// Supabase Auth's generic OAuth2 provider expects OIDC-like JSON claims
// (`sub`, `email`, `name`). Yandex JSON userinfo returns `id` and
// `default_email`, so this public Edge Function transforms the response.

const YANDEX_USERINFO_URL = "https://login.yandex.ru/info?format=json";

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

function extractToken(header: string | null): string {
  const value = (header || "").trim();
  if (!value) return "";
  const match = value.match(/^(?:Bearer|OAuth)\s+(.+)$/i);
  return (match ? match[1] : value).trim();
}

function stringValue(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function firstEmail(raw: Record<string, unknown>): string {
  const defaultEmail = stringValue(raw.default_email);
  if (defaultEmail) return defaultEmail;
  const emails = raw.emails;
  if (Array.isArray(emails)) {
    for (const item of emails) {
      const email = stringValue(item);
      if (email) return email;
    }
  }
  return "";
}

function avatarUrl(raw: Record<string, unknown>): string {
  const isEmpty = raw.is_avatar_empty === true || raw.is_avatar_empty === "true";
  const id = stringValue(raw.default_avatar_id);
  if (isEmpty || !id) return "";
  return `https://avatars.yandex.net/get-yapic/${encodeURIComponent(id)}/islands-200`;
}

function mapYandexUser(raw: Record<string, unknown>): Record<string, unknown> {
  const sub = stringValue(raw.id);
  const email = firstEmail(raw);
  const name = stringValue(raw.real_name) || stringValue(raw.display_name) || stringValue(raw.login);
  const picture = avatarUrl(raw);
  const mapped: Record<string, unknown> = {
    sub,
    email,
    email_verified: Boolean(email),
    name,
    preferred_username: stringValue(raw.login),
    given_name: stringValue(raw.first_name),
    family_name: stringValue(raw.last_name),
  };
  if (picture) mapped.picture = picture;
  return mapped;
}

async function fetchYandexUser(token: string): Promise<Response> {
  const headers = { Authorization: `OAuth ${token}` };
  let upstream = await fetch(YANDEX_USERINFO_URL, { headers });
  // Some OAuth clients accept Bearer while official Yandex examples use OAuth.
  // Keep a fallback for compatibility without exposing the token in logs.
  if (!upstream.ok && upstream.status === 401) {
    upstream = await fetch(YANDEX_USERINFO_URL, { headers: { Authorization: `Bearer ${token}` } });
  }
  return upstream;
}

Deno.serve(async (request) => {
  if (request.method === "OPTIONS") return new Response(null, { status: 204 });
  if (request.method !== "GET") return jsonResponse({ error: "method_not_allowed" }, 405);

  const token = extractToken(request.headers.get("authorization"));
  if (!token) return jsonResponse({ error: "missing_yandex_token" }, 401);

  const upstream = await fetchYandexUser(token);
  if (!upstream.ok) {
    return jsonResponse({ error: "yandex_userinfo_failed", status: upstream.status }, 502);
  }

  let raw: Record<string, unknown>;
  try {
    raw = await upstream.json();
  } catch (_error) {
    return jsonResponse({ error: "invalid_yandex_userinfo_json" }, 502);
  }

  const mapped = mapYandexUser(raw);
  if (!mapped.sub) return jsonResponse({ error: "missing_yandex_subject" }, 502);
  return jsonResponse(mapped);
});
