import { createClient } from "https://esm.sh/@supabase/supabase-js@2.108.2";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
};

type JsonRecord = Record<string, unknown>;

function env(name: string, fallback = ""): string {
  return Deno.env.get(name) || fallback;
}

function json(body: JsonRecord, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...CORS_HEADERS, "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" },
  });
}

function bearerToken(header: string | null): string | null {
  const match = /^Bearer\s+(.+)$/i.exec(header || "");
  return match ? match[1] : null;
}

function cleanText(value: unknown, max = 2000): string {
  return String(value || "")
    .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]+/gu, " ")
    .replace(/\s+/gu, " ")
    .trim()
    .slice(0, max);
}

function cleanUrl(value: unknown): string | null {
  const raw = cleanText(value, 1200);
  if (!raw) return null;
  try {
    const url = new URL(raw);
    return ["http:", "https:"].includes(url.protocol) ? url.toString() : null;
  } catch (_) {
    return null;
  }
}

function cleanSourceUrls(value: unknown, fallback: string | null): string[] {
  const values = Array.isArray(value) ? value : [];
  const urls = values.map(cleanUrl).filter((item): item is string => Boolean(item));
  if (fallback && !urls.includes(fallback)) urls.unshift(fallback);
  return urls.slice(0, 12);
}

function isAdmin(user: JsonRecord): boolean {
  const allowedIds = env("EVENT_ISSUE_REPORT_ADMIN_USER_IDS") || env("EVENT_ISSUE_REPORT_ADMIN_USER_ID");
  const allowedEmails = env("EVENT_ISSUE_REPORT_ADMIN_EMAILS") || env("EVENT_ISSUE_REPORT_ADMIN_EMAIL");
  const userId = String(user.id || "");
  const email = String(user.email || "").trim().toLowerCase();
  const idSet = new Set(allowedIds.split(",").map((item) => item.trim()).filter(Boolean));
  const emailSet = new Set(allowedEmails.split(",").map((item) => item.trim().toLowerCase()).filter(Boolean));
  return (userId && idSet.has(userId)) || (email && emailSet.has(email));
}

async function authenticatedUser(request: Request): Promise<{ user?: JsonRecord; error?: Response }> {
  const supabaseUrl = env("SUPABASE_URL") || env("PERSONALIZATION_SUPABASE_URL");
  const anonKey = env("SUPABASE_ANON_KEY") || env("PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY");
  if (!supabaseUrl || !anonKey) return { error: json({ error: "supabase_env_missing" }, 500) };
  const token = bearerToken(request.headers.get("Authorization"));
  if (!token) return { error: json({ error: "auth_required" }, 401) };
  const supabase = createClient(supabaseUrl, anonKey, {
    global: { headers: { Authorization: `Bearer ${token}` } },
    auth: { persistSession: false, autoRefreshToken: false },
  });
  const { data, error } = await supabase.auth.getUser(token);
  if (error || !data?.user) return { error: json({ error: "auth_required" }, 401) };
  return { user: data.user as unknown as JsonRecord };
}

function serviceClient() {
  const supabaseUrl = env("SUPABASE_URL") || env("PERSONALIZATION_SUPABASE_URL");
  const serviceKey = env("SUPABASE_SERVICE_ROLE_KEY") || env("PERSONALIZATION_SUPABASE_SECRET_KEY") || env("PERSONALIZATION_SUPABASE_SERVICE_KEY") || env("SUPABASE_SERVICE_KEY");
  if (!supabaseUrl || !serviceKey) return null;
  return createClient(supabaseUrl, serviceKey, {
    global: { headers: { Authorization: `Bearer ${serviceKey}` } },
    auth: { persistSession: false, autoRefreshToken: false },
  });
}

function cleanEventIdParam(url: URL): number | null {
  const value = Number(url.searchParams.get("event_id") || url.searchParams.get("static_event_id") || "");
  if (!Number.isFinite(value) || value <= 0) return null;
  return Math.trunc(value);
}

function reportPublicFields(row: JsonRecord): JsonRecord {
  return {
    id: row.id,
    status: row.status,
    event_id: row.event_id,
    static_event_id: row.static_event_id,
    event_slug: row.event_slug,
    report_text: row.report_text,
    artkodex_task_id: row.artkodex_task_id,
    artkodex_thread_url: row.artkodex_thread_url,
    processing_error: row.processing_error,
    created_at: row.created_at,
    processed_at: row.processed_at,
    updated_at: row.updated_at,
  };
}

Deno.serve(async (request: Request) => {
  if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS_HEADERS });
  if (!["GET", "POST"].includes(request.method)) return json({ error: "method_not_allowed" }, 405);

  const auth = await authenticatedUser(request);
  if (auth.error) return auth.error;
  const user = auth.user!;
  if (!isAdmin(user)) return json({ error: "admin_required" }, 403);
  if (request.method === "GET") {
    const eventId = cleanEventIdParam(new URL(request.url));
    if (!eventId) return json({ admin: true, reports: [] });
    const service = serviceClient();
    if (!service) return json({ error: "service_env_missing" }, 500);
    const table = env("EVENT_ISSUE_REPORTS_TABLE", "event_issue_reports");
    const { data, error } = await service
      .from(table)
      .select("id,status,event_id,static_event_id,event_slug,report_text,artkodex_task_id,artkodex_thread_url,processing_error,created_at,processed_at,updated_at")
      .or(`event_id.eq.${eventId},static_event_id.eq.${eventId}`)
      .order("created_at", { ascending: false })
      .limit(20);
    if (error) {
      console.error(JSON.stringify({ event: "event_issue_report_select_failed", message: error.message }));
      return json({ error: "select_failed" }, 500);
    }
    return json({ admin: true, reports: (data || []).map((row) => reportPublicFields(row as JsonRecord)) });
  }

  let body: JsonRecord = {};
  try {
    body = await request.json();
  } catch (_) {
    return json({ error: "invalid_json" }, 400);
  }
  const reportText = cleanText(body.report_text, 2000);
  if (reportText.length < 12) return json({ error: "report_too_short" }, 400);
  const eventId = Number(body.event_id);
  if (!Number.isFinite(eventId) || eventId <= 0) return json({ error: "invalid_event_id" }, 400);

  const sourceUrl = cleanUrl(body.source_url);
  const row = {
    status: "submitted",
    event_id: Math.trunc(eventId),
    static_event_id: Number.isFinite(Number(body.static_event_id)) ? Math.trunc(Number(body.static_event_id)) : null,
    event_slug: cleanText(body.event_slug, 240),
    event_title: cleanText(body.event_title, 500),
    event_url: cleanUrl(body.event_url),
    source_url: sourceUrl,
    source_urls: cleanSourceUrls(body.source_urls, sourceUrl),
    telegraph_url: cleanUrl(body.telegraph_url),
    event_date: cleanText(body.event_date, 32),
    event_time: cleanText(body.event_time, 80),
    venue_name: cleanText(body.venue_name, 240),
    address: cleanText(body.address, 500),
    city: cleanText(body.city, 120),
    report_text: reportText,
    reported_by_user_id: String(user.id || ""),
    reporter_email: cleanText(user.email, 320),
    reporter_provider: cleanText(((user.app_metadata as JsonRecord | undefined)?.provider), 120),
    reporter_metadata: {
      providers: (user.app_metadata as JsonRecord | undefined)?.providers || [],
    },
  };

  const service = serviceClient();
  if (!service) return json({ error: "service_env_missing" }, 500);
  const table = env("EVENT_ISSUE_REPORTS_TABLE", "event_issue_reports");
  const { data, error } = await service.from(table).insert(row).select("id,status,created_at").single();
  if (error) {
    console.error(JSON.stringify({ event: "event_issue_report_insert_failed", message: error.message }));
    return json({ error: "insert_failed" }, 500);
  }
  return json({ ok: true, report: reportPublicFields(data as JsonRecord), id: data.id, status: data.status, created_at: data.created_at });
});
