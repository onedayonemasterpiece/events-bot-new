// Authenticated static-site event follow/calendar notification enqueue.
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.108.2";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/u;

function env(name: string, fallback = ""): string {
  return Deno.env.get(name) || fallback;
}

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...CORS_HEADERS, "Content-Type": "application/json; charset=utf-8" },
  });
}

function bearerToken(header: string | null): string | null {
  const match = /^Bearer\s+(.+)$/i.exec(header || "");
  return match ? match[1] : null;
}

function text(value: unknown, max = 1000): string {
  return String(value || "").trim().slice(0, max);
}

async function sha256Hex(value: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return Array.from(new Uint8Array(digest)).map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

function parseDate(value: unknown): string | null {
  const raw = text(value, 40);
  return /^\d{4}-\d{2}-\d{2}$/u.test(raw) ? raw : null;
}

function parseTimestamp(value: unknown): string | null {
  const raw = text(value, 80);
  if (!raw) return null;
  const dt = new Date(raw);
  return Number.isFinite(dt.getTime()) ? dt.toISOString() : null;
}

function reminderRunAt(startsAt: string | null): string | null {
  if (!startsAt) return null;
  const dt = new Date(startsAt);
  return Number.isFinite(dt.getTime()) ? new Date(dt.getTime() - 24 * 60 * 60 * 1000).toISOString() : null;
}

function startsInLessThan24h(startsAt: string | null, now: Date): boolean {
  if (!startsAt) return false;
  const dt = new Date(startsAt);
  return Number.isFinite(dt.getTime()) && dt.getTime() - now.getTime() < 24 * 60 * 60 * 1000;
}

function escapeHtml(value: string): string {
  return value.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function emailPayload(kind: string, event: Record<string, unknown>, queuedAt: string): Record<string, unknown> {
  const title = text(event.title, 500);
  const dateLine = [text(event.start_date, 20), text(event.display_time, 80)].filter(Boolean).join(" · ") || "дата уточняется";
  const place = text(event.venue_name, 500) || text(event.city, 200) || "место уточняется";
  const lines = [
    `Событие: ${title}`,
    `Когда: ${dateLine}`,
    `Где: ${place}`,
    text(event.location_address, 500) ? `Адрес: ${text(event.location_address, 500)}` : "",
    `Страница события: ${text(event.event_url, 2000)}`,
    text(event.source_url, 2000) ? `Источник/организатор: ${text(event.source_url, 2000)}` : "",
    text(event.ticket_link, 2000) ? `Билеты/регистрация: ${text(event.ticket_link, 2000)}` : "",
  ].filter(Boolean);
  const subject = kind === "event_reminder_24h" ? `Напоминание: завтра ${title}` : `Событие добавлено в календарь: ${title}`;
  const intro = kind === "event_reminder_24h" ? "Напоминаем о событии примерно за 24 часа до начала." : "Вы добавили событие в календарь и согласились получать уведомления по нему.";
  return {
    kind,
    event,
    email: {
      subject,
      text: [intro, ...lines, "Управлять уведомлениями можно на странице события."].join("\n\n"),
      html: `<p>${escapeHtml(intro)}</p><ul>${lines.map((line) => `<li>${escapeHtml(line)}</li>`).join("")}</ul><p>Управлять уведомлениями можно на странице события.</p>`,
    },
    queued_at: queuedAt,
  };
}

Deno.serve(async (request) => {
  if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS_HEADERS });
  if (request.method !== "POST") return jsonResponse({ error: "method_not_allowed" }, 405);

  const supabaseUrl = env("SUPABASE_URL") || env("PERSONALIZATION_SUPABASE_URL");
  const anonKey = env("SUPABASE_ANON_KEY") || env("PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY");
  const serviceKey = env("SUPABASE_SERVICE_ROLE_KEY") || env("PERSONALIZATION_SUPABASE_SECRET_KEY") || env("PERSONALIZATION_SUPABASE_SERVICE_KEY");
  if (!supabaseUrl || !anonKey || !serviceKey) return jsonResponse({ error: "supabase_env_missing" }, 500);

  const token = bearerToken(request.headers.get("authorization"));
  if (!token) return jsonResponse({ error: "auth_required" }, 401);

  const authClient = createClient(supabaseUrl, anonKey, {
    global: { headers: { Authorization: `Bearer ${token}` } },
    auth: { persistSession: false, autoRefreshToken: false },
  });
  const { data: userResult, error: userError } = await authClient.auth.getUser(token);
  const user = userResult?.user;
  if (userError || !user) return jsonResponse({ error: "auth_required" }, 401);

  let body: Record<string, unknown>;
  try {
    body = await request.json();
  } catch (_error) {
    return jsonResponse({ error: "invalid_json" }, 400);
  }
  if (body.notification_consent !== true) return jsonResponse({ error: "notification_consent_required" }, 400);

  const metadata = (user.user_metadata || {}) as Record<string, unknown>;
  const oauthEmail = text(user.email || metadata.email || metadata.default_email, 320).toLowerCase();
  const manualEmail = text(body.notification_email, 320).toLowerCase();
  const notificationEmail = oauthEmail || manualEmail;
  if (!EMAIL_RE.test(notificationEmail)) return jsonResponse({ error: "notification_email_required" }, 400);
  const recipientHash = await sha256Hex(`:${notificationEmail}`);
  const emailSource = oauthEmail ? "yandex_oauth" : "manual";
  const now = new Date();
  const queuedAt = now.toISOString();

  const src = (body.event || {}) as Record<string, unknown>;
  const eventId = Number(src.id || src.event_id || body.event_id);
  if (!Number.isFinite(eventId) || eventId <= 0) return jsonResponse({ error: "event_id_required" }, 400);
  const event = {
    event_id: Math.trunc(eventId),
    title: text(src.title, 500),
    event_url: text(src.event_url || src.url, 2000),
    starts_at: parseTimestamp(src.starts_at),
    start_date: parseDate(src.start_date),
    display_time: text(src.display_time, 80),
    venue_name: text(src.venue_name || src.location_name, 500),
    location_address: text(src.location_address || src.address, 500),
    city: text(src.city, 200),
    source_url: text(src.source_url, 2000),
    ticket_link: text(src.ticket_link || src.ticket_href, 2000),
    lifecycle_status: text(src.lifecycle_status, 40) || "active",
  };
  if (!event.title || !event.event_url) return jsonResponse({ error: "event_snapshot_required" }, 400);

  const service = createClient(supabaseUrl, serviceKey, { auth: { persistSession: false, autoRefreshToken: false } });
  const profile = await service.from("user_notification_profiles").upsert({
    user_id: user.id,
    notification_email: notificationEmail,
    notification_email_hash: recipientHash,
    email_verified: Boolean(oauthEmail),
    email_source: emailSource,
    updated_at: queuedAt,
    metadata: { provider: "custom:yandex", oauth_email_present: Boolean(oauthEmail) },
  }, { onConflict: "user_id" });
  if (profile.error) return jsonResponse({ error: "profile_upsert_failed", detail: profile.error.message }, 500);

  const follow = await service.from("event_follows").upsert({
    user_id: user.id,
    event_id: event.event_id,
    notification_email: notificationEmail,
    notification_email_hash: recipientHash,
    notification_consent_at: queuedAt,
    calendar_added_at: queuedAt,
    unsubscribed_at: null,
    event_url: event.event_url,
    event_title: event.title,
    starts_at: event.starts_at,
    start_date: event.start_date,
    display_time: event.display_time,
    venue_name: event.venue_name,
    location_address: event.location_address,
    city: event.city,
    source_url: event.source_url,
    ticket_link: event.ticket_link,
    lifecycle_status: event.lifecycle_status,
    source_snapshot: event,
    updated_at: queuedAt,
  }, { onConflict: "user_id,event_id" });
  if (follow.error) return jsonResponse({ error: "follow_upsert_failed", detail: follow.error.message }, 500);

  const outboxRows: Record<string, unknown>[] = [{
    kind: "calendar_confirmation",
    event_id: event.event_id,
    user_id: user.id,
    recipient_email: notificationEmail,
    recipient_email_hash: recipientHash,
    payload_json: emailPayload("calendar_confirmation", event, queuedAt),
    status: "pending",
    next_run_at: queuedAt,
    idempotency_key: `calendar_confirmation:${user.id}:${event.event_id}:calendar-follow-v1`,
    dry_run: true,
  }];
  const deliveryRows: Record<string, unknown>[] = [{
    event_id: event.event_id,
    user_id: user.id,
    kind: "calendar_confirmation",
    status: "queued",
    recipient_email_hash: recipientHash,
    reason: "event_follow",
    dry_run: true,
    metadata: { ydb_projection_required: true },
  }];

  const remindAt = reminderRunAt(event.starts_at);
  if (!event.starts_at || startsInLessThan24h(event.starts_at, now) || !remindAt) {
    deliveryRows.push({
      event_id: event.event_id,
      user_id: user.id,
      kind: "event_reminder_24h",
      status: "skipped",
      recipient_email_hash: recipientHash,
      reason: event.starts_at ? "starts_in_less_than_24h" : "missing_start_time",
      dry_run: true,
      metadata: { ydb_projection_required: true },
    });
  } else {
    outboxRows.push({
      kind: "event_reminder_24h",
      event_id: event.event_id,
      user_id: user.id,
      recipient_email: notificationEmail,
      recipient_email_hash: recipientHash,
      payload_json: emailPayload("event_reminder_24h", event, queuedAt),
      status: "pending",
      next_run_at: remindAt,
      idempotency_key: `event_reminder_24h:${user.id}:${event.event_id}:${event.starts_at}`,
      dry_run: true,
    });
    deliveryRows.push({ event_id: event.event_id, user_id: user.id, kind: "event_reminder_24h", status: "queued", recipient_email_hash: recipientHash, reason: "event_follow", dry_run: true, metadata: { ydb_projection_required: true } });
  }

  const outbox = await service.from("email_outbox").upsert(outboxRows, { onConflict: "idempotency_key" });
  if (outbox.error) return jsonResponse({ error: "outbox_enqueue_failed", detail: outbox.error.message }, 500);
  const delivery = await service.from("email_delivery_events").insert(deliveryRows);
  if (delivery.error) return jsonResponse({ error: "delivery_event_failed", detail: delivery.error.message }, 500);
  return jsonResponse({ ok: true, event_id: event.event_id, queued: outboxRows.map((row) => row.kind), email_source: emailSource, email_verified: Boolean(oauthEmail), dry_run: true });
});
