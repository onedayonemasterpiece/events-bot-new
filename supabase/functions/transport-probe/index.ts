const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...CORS_HEADERS, "Content-Type": "application/json; charset=utf-8" },
  });
}

Deno.serve(async (request) => {
  if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS_HEADERS });
  if (request.method !== "POST") return json({ error: "method_not_allowed" }, 405);

  const declaredLength = Number(request.headers.get("content-length") || 0);
  if (Number.isFinite(declaredLength) && declaredLength > 1024) return json({ error: "request_too_large" }, 413);

  let payload: { nonce?: unknown };
  try {
    const text = await request.text();
    if (new TextEncoder().encode(text).byteLength > 1024) return json({ error: "request_too_large" }, 413);
    payload = JSON.parse(text) as { nonce?: unknown };
  } catch {
    return json({ error: "invalid_json" }, 400);
  }

  const nonce = String(payload.nonce || "");
  if (!/^[A-Za-z0-9_-]{16,80}$/u.test(nonce)) return json({ error: "invalid_nonce" }, 400);
  return json({ nonce, schema: 1 });
});
