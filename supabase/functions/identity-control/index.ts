// Site-wide identity/device control-plane. Deploy with --no-verify-jwt; every
// authenticated action validates the bearer token explicitly before service RPCs.
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.108.2";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu;
const DEVICE_SECRET_RE = /^[A-Za-z0-9_-]{43}$/u;

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { ...CORS_HEADERS, "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" } });
}
function value(name: string): string { return Deno.env.get(name) || ""; }
function bearer(request: Request): string | null { return /^Bearer\s+(.+)$/iu.exec(request.headers.get("authorization") || "")?.[1] || null; }
async function sha256Hex(input: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(input));
  return Array.from(new Uint8Array(digest), (v) => v.toString(16).padStart(2, "0")).join("");
}
function savedRows(input: unknown): Array<Record<string, unknown>> {
  if (!Array.isArray(input) || input.length > 500) throw new Error("saved_payload_invalid");
  return input.map((row) => {
    if (!row || typeof row !== "object") throw new Error("saved_payload_invalid");
    const item = row as Record<string, unknown>;
    const eventId = Number(item.event_id);
    const occurrenceKey = String(item.occurrence_key || "").trim();
    if (!Number.isSafeInteger(eventId) || eventId <= 0 || occurrenceKey.length < 1 || occurrenceKey.length > 160) throw new Error("saved_payload_invalid");
    const starts = item.occurrence_starts_at == null ? null : new Date(String(item.occurrence_starts_at));
    if (starts && !Number.isFinite(starts.getTime())) throw new Error("saved_payload_invalid");
    return { event_id: eventId, occurrence_key: occurrenceKey, occurrence_starts_at: starts?.toISOString() || null, saved: item.saved !== false };
  });
}

Deno.serve(async (request) => {
  if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS_HEADERS });
  if (request.method !== "POST") return json({ error: "method_not_allowed" }, 405);
  // This boundary is personalization-only. Never fall back to the repository's
  // legacy Supabase project credentials.
  const supabaseUrl = value("PERSONALIZATION_SUPABASE_URL");
  const publishableKey = value("PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY");
  const serviceKey = value("PERSONALIZATION_SUPABASE_SECRET_KEY");
  if (!supabaseUrl || !publishableKey || !serviceKey) return json({ error: "server_config_missing" }, 503);

  let body: Record<string, unknown>;
  try { body = await request.json(); } catch { return json({ error: "invalid_json" }, 400); }
  const action = String(body.action || "");
  const deviceId = String(body.device_id || "");
  const deviceSecret = String(body.device_secret || "");
  const consentVersion = String(body.consent_version || "").trim();
  if (!UUID_RE.test(deviceId) || !DEVICE_SECRET_RE.test(deviceSecret)) return json({ error: "device_proof_invalid" }, 400);
  const credentialHash = await sha256Hex(`ke-device-v1:${deviceId}:${deviceSecret}`);
  const service = createClient(supabaseUrl, serviceKey, { auth: { persistSession: false, autoRefreshToken: false } });

  if (action === "materialize_device") {
    if (consentVersion.length < 1 || consentVersion.length > 80) return json({ error: "consent_required" }, 400);
    let saves: Array<Record<string, unknown>>;
    try { saves = savedRows(body.saved_occurrences || []); } catch { return json({ error: "saved_payload_invalid" }, 400); }
    const { error } = await service.rpc("personalization_materialize_device_v1", {
      p_device_id: deviceId, p_credential_hash_hex: credentialHash, p_consent_version: consentVersion, p_saved: saves,
    });
    if (error) return json({ error: "materialize_failed" }, 409);
    return json({ ok: true, device_id: deviceId, materialized_saved_count: saves.filter((row) => row.saved !== false).length });
  }

  const token = bearer(request);
  if (!token) return json({ error: "auth_required" }, 401);
  const auth = createClient(supabaseUrl, publishableKey, { global: { headers: { Authorization: `Bearer ${token}` } }, auth: { persistSession: false, autoRefreshToken: false } });
  const { data: userData, error: userError } = await auth.auth.getUser(token);
  if (userError || !userData.user || userData.user.is_anonymous) return json({ error: "auth_required" }, 401);
  const userId = userData.user.id;

  if (action === "merge_device") {
    const requestId = String(body.request_id || "");
    if (!UUID_RE.test(requestId) || consentVersion.length < 1 || consentVersion.length > 80) return json({ error: "merge_request_invalid" }, 400);
    const { data, error } = await service.rpc("personalization_merge_device_v1", {
      p_user_id: userId, p_device_id: deviceId, p_credential_hash_hex: credentialHash, p_consent_version: consentVersion, p_request_id: requestId,
    });
    if (error) return json({ error: error.code === "23505" ? "device_account_conflict" : "merge_failed" }, error.code === "23505" ? 409 : 400);
    return json({ ok: true, merge: Array.isArray(data) ? data[0] : data });
  }
  if (action === "unlink_device") {
    const { data, error } = await service.rpc("personalization_unlink_device_v1", {
      p_user_id: userId, p_device_id: deviceId, p_credential_hash_hex: credentialHash,
    });
    if (error) return json({ error: "unlink_failed" }, 400);
    return json({ ok: true, unlinked: Boolean(data) });
  }
  if (action === "delete_profile") {
    // Marks data for purge first. Deleting auth.users remains a separately audited,
    // recent-auth service operation so an old bearer token cannot silently erase an account.
    const { data, error } = await service.rpc("personalization_mark_profile_deleting_v1", { p_user_id: userId });
    if (error) return json({ error: "delete_profile_failed" }, 400);
    return json({ ok: true, profile_id: data, auth_user_delete_required: true }, 202);
  }
  return json({ error: "unsupported_action" }, 400);
});
