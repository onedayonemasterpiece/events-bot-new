# Focus-group connectivity control

This is a deliberately small, read-only control for diagnosing the focus-group
email incident. A browser can compare:

1. Supabase Auth health;
2. one tiny RLS-safe Supabase Data API read;
3. this Yandex API Gateway → YDB `GetItem`.

The control does not send an email, verify an OTP, create a user or write
telemetry. It reads only the dedicated
`focus_connectivity_probe[probe_id=primary]` row. The API Gateway runs as the
dedicated `focus-connectivity-probe` service account with `ydb.viewer`; it has
no static keys. CORS is restricted to `https://kenigevents.ru`.

## Privacy boundary

The page and endpoint must not receive or persist email, Auth user id, JWT,
OTP, full user agent or IP-derived identity. A copied diagnostic receipt may
contain only UTC time, page origin, online/effective connection type, bounded
status/error class, elapsed milliseconds and response byte count.

## Provisioning and verification

The non-secret resource contract is recorded in
[`desired-state.json`](desired-state.json). The document table lives in the
existing deletion-protected `kenigevents-email-events` serverless database but
does not reuse its mail-event tables.

Before using the endpoint:

1. verify the dedicated table contains `primary / ready / schema_version=1`;
2. verify the service account has only the required read role and zero static
   keys;
3. create or update the API Gateway from [`openapi.yaml`](openapi.yaml);
4. test allowed-origin `GET` and `OPTIONS`;
5. test that another `Origin` receives no usable CORS permission;
6. run the hidden browser page on Wi-Fi, mobile data and the reported VPN mode.

This is an incident diagnostic, not an authentication proxy. Moving OTP behind
Yandex is a separate decision and is not justified until the browser probes
show that the direct Supabase route fails while this control remains reachable.
