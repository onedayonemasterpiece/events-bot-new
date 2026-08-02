# Protected focus OTP receipt boundary

This boundary receives the routine focus-group Auth OTP without provisioning a
human mailbox and without granting GitHub access to the shared inbound bucket.

```text
Supabase Auth / NotiSend
  -> generated Yandex Mail Trigger address
  -> Mail Trigger batch of one
  -> API Gateway WebSocket broadcast on an unguessable path
  -> one protected GitHub Actions connection
  -> OTP held only in runner memory
```

The checked-in identifiers and stable resource names are in
[`desired-state.json`](desired-state.json). The generated recipient and
WebSocket path are secrets and are deliberately absent. The trigger stores no
message or attachment. `external-e2e` contains the WSS URL as a secret and the
fixed generated recipient as an Environment variable. The corresponding
Supabase Auth identity is pre-created once so ordinary sign-in is a returning
identity and follows the NotiSend route without consuming a new recipient on
every run.

Operational checks:

```bash
yc --profile focus-e2e serverless trigger get a1sa03t99oa787mg1vf0
yc --profile focus-e2e serverless api-gateway get d5dl9aj1pi4l08s30c3n
```

Require both resources to be `ACTIVE`, the trigger gateway/path/service account
to match desired state, and a successful WSS handshake before a protected run.
Never print the generated address, path, incoming event or OTP. Rotation creates
a new random gateway path, updates the trigger and GitHub secret atomically, and
then removes the old path only after a handshake check.
